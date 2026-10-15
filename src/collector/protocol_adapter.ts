import { BiosignalTcpClient, type TcpClientOptions } from './tcp_client.ts';
import type { SensorId, MockResponse, LossRange, RawSample } from '../types.ts';
import type { StorageEngine } from '../storage/db.ts';

export interface SessionTransitionEvent {
  oldSessionId: string | null;
  newSessionId: string;
}

export interface IntegrityErrorEvent {
  sessionId: string;
  sensor: SensorId;
  index: number;
  existingValues: number[];
  incomingValues: number[];
  reason: string;
}

export interface ReportedLossEvent {
  sessionId: string;
  sensor: SensorId;
  lossRanges: LossRange[];
}

export type AdapterReadResult =
  | { success: true; response: MockResponse; isDuplicate?: boolean; commitLatencyMs?: number }
  | { success: false; error: string; code: string; recoverable: boolean };

export class BiosignalProtocolAdapter {
  private client: BiosignalTcpClient;
  private storage: StorageEngine | null = null;
  private currentSessionId: string | null = null;
  private expectedIndices: Map<SensorId, number> = new Map([[1, 0], [2, 0]]);

  private sessionTransitionListeners: Array<(ev: SessionTransitionEvent) => void> = [];
  private integrityErrorListeners: Array<(ev: IntegrityErrorEvent) => void> = [];
  private reportedLossListeners: Array<(ev: ReportedLossEvent) => void> = [];
  private sessionEnded: Map<string, boolean> = new Map();
  private pendingLossReasonsFallback: Map<string, string> = new Map();

  constructor(opts: TcpClientOptions, storage?: StorageEngine) {
    this.client = new BiosignalTcpClient(opts);
    if (storage) {
      this.storage = storage;
    }
  }

  public attachStorage(storage: StorageEngine) {
    this.storage = storage;
  }

  public onSessionTransition(cb: (ev: SessionTransitionEvent) => void) {
    this.sessionTransitionListeners.push(cb);
  }

  public onIntegrityError(cb: (ev: IntegrityErrorEvent) => void) {
    this.integrityErrorListeners.push(cb);
  }

  public onReportedLoss(cb: (ev: ReportedLossEvent) => void) {
    this.reportedLossListeners.push(cb);
  }

  public async connect(): Promise<void> {
    await this.client.connect();
  }

  public disconnect(): void {
    this.client.disconnect();
  }

  public getClient(): BiosignalTcpClient {
    return this.client;
  }

  public getSessionId(): string | null {
    return this.currentSessionId;
  }

  public getExpectedIndex(sensor: SensorId): number {
    return this.expectedIndices.get(sensor) || 0;
  }

  public setExpectedIndex(sensor: SensorId, idx: number) {
    this.expectedIndices.set(sensor, idx);
  }

  /**
   * Compare existing sample values with incoming duplicate values.
   */
  public static compareSampleValues(existing: number[], incoming: number[]): 'identical' | 'conflict' {
    if (existing.length !== incoming.length) return 'conflict';
    for (let i = 0; i < existing.length; i++) {
      if (existing[i] !== incoming[i]) return 'conflict';
    }
    return 'identical';
  }

  /**
   * Request next sequential batch for a sensor with automatic retry and gap reconciliation.
   */
  public async readNext(sensor: SensorId): Promise<AdapterReadResult> {
    const expected = this.expectedIndices.get(sensor) || 0;
    return this.readWithRetry(sensor, expected);
  }

  /**
   * Request specific index for a sensor.
   */
  public async readFromIndex(sensor: SensorId, fromIndex: number): Promise<AdapterReadResult> {
    return this.readWithRetry(sensor, fromIndex);
  }

  private async readWithRetry(
    sensor: SensorId,
    targetIndex: number,
    retryCount: number = 0
  ): Promise<AdapterReadResult> {
    const maxRetries = 3;
    const result = await this.client.readSensor(sensor, targetIndex);

    if (!result.ok) {
      if (retryCount < maxRetries) {
        if (result.code === 'CRC_MISMATCH') {
          // Corrupt reply: do not trust metadata, re-request from expected index
          await new Promise((r) => setTimeout(r, 50 * (retryCount + 1)));
          return this.readWithRetry(sensor, targetIndex, retryCount + 1);
        }

        if (result.error?.includes('timeout') || result.error?.includes('closed')) {
          // Timeout or connection dropped: close and reconnect, then re-read with from_index = expected_next
          this.client.disconnect();
          try {
            await this.client.connect();
          } catch {}
          await new Promise((r) => setTimeout(r, 50 * (retryCount + 1)));
          const expectedNext = this.expectedIndices.get(sensor) || 0;
          return this.readWithRetry(sensor, expectedNext, retryCount + 1);
        }
      }

      // Exhausted retries or unrecoverable error
      if (result.code === 'CRC_MISMATCH') {
        const expectedNext = this.expectedIndices.get(sensor) || 0;
        const key = `${sensor}:${expectedNext}`;
        this.pendingLossReasonsFallback.set(key, 'crc_unrecoverable');
        const sessId = this.currentSessionId || (this.storage ? this.storage.getSessionIds()[0] : null);
        if (this.storage && sessId) {
          this.storage.setPendingLossReason(sessId, sensor, expectedNext, 'crc_unrecoverable');
        }
      }

      return {
        success: false,
        error: result.error,
        code: result.code,
        recoverable: retryCount < maxRetries,
      };
    }

    const resp = result.response;

    // Track session_end
    if (resp.session_end) {
      this.sessionEnded.set(resp.session_id, true);
    }

    // Detect and handle session transition
    if (this.currentSessionId !== null && resp.session_id !== this.currentSessionId) {
      const oldSession = this.currentSessionId;
      this.currentSessionId = resp.session_id;
      this.expectedIndices.set(1, 0);
      this.expectedIndices.set(2, 0);

      // Section 6.3: "restart, which starts a new session. Unread data of the old session is then lost and must be listed, with 'to_index': null when its end is unknown."
      if (this.storage && !this.sessionEnded.get(oldSession)) {
        for (const s of [1, 2] as const) {
          const prog = this.storage.getProgress(oldSession, s);
          const fromIdx = prog.resolvedWatermark + 1;
          const losses = this.storage.getLosses(oldSession, s);
          const hasOpenLoss = losses.some((l) => l.from_index === fromIdx && l.to_index === null);
          if (!hasOpenLoss) {
            this.storage.recordLoss(oldSession, s, fromIdx, null, 'session_restart_tail_lost', 'pipeline');
          }
        }
      }

      for (const listener of this.sessionTransitionListeners) {
        listener({ oldSessionId: oldSession, newSessionId: resp.session_id });
      }
    } else if (this.currentSessionId === null) {
      this.currentSessionId = resp.session_id;
      for (const listener of this.sessionTransitionListeners) {
        listener({ oldSessionId: null, newSessionId: resp.session_id });
      }
    }

    // Check for reported loss ranges
    if (resp.lost.length > 0) {
      for (const listener of this.reportedLossListeners) {
        listener({
          sessionId: resp.session_id,
          sensor: resp.sensor,
          lossRanges: resp.lost,
        });
      }
    }

    // Flush any pending loss reasons for this session
    if (this.storage && this.pendingLossReasonsFallback.size > 0) {
      for (const [key, reason] of this.pendingLossReasonsFallback.entries()) {
        const [sStr, idxStr] = key.split(':');
        const s = parseInt(sStr, 10) as SensorId;
        const idx = parseInt(idxStr, 10);
        this.storage.setPendingLossReason(resp.session_id, s, idx, reason);
      }
      this.pendingLossReasonsFallback.clear();
    }

    // 5. If storage engine is attached, commit atomically before advancing transport progress
    let commitLatencyMs = 0;
    if (this.storage) {
      const t0 = performance.now();
      const persistRes = this.storage.persistValidatedReply(resp);
      commitLatencyMs = Math.round(performance.now() - t0);
      if (!persistRes.success) {
        return {
          success: false,
          error: persistRes.error || 'Durable commit failed',
          code: 'STORAGE_COMMIT_FAILED',
          recoverable: false,
        };
      }
    }

    // Check if reply is an identical duplicate or behind expected
    const expected = this.expectedIndices.get(sensor) || 0;
    const isDuplicate = resp.first_index + resp.count <= expected;

    // Update transport-level expected watermark if forward progress is made
    const maxReached = resp.first_index + resp.count;
    if (maxReached > expected) {
      this.expectedIndices.set(sensor, maxReached);
    }

    return {
      success: true,
      response: resp,
      isDuplicate,
      commitLatencyMs,
    };
  }
}
