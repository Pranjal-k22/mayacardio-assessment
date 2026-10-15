import * as net from 'node:net';
import type { SensorId, MockResponse, RawValidationResult } from '../types.ts';
import { validateRawResponse } from './raw_parser.ts';

export interface TcpClientOptions {
  host: string;
  port: number;
  connectTimeoutMs?: number;
  readTimeoutMs?: number;
  maxLineLength?: number;
  maxRetries?: number;
}

export interface SensorProgress {
  sessionId: string;
  expectedIndex: number;
  totalReceived: number;
  totalLossReported: number;
}

export type ValidatedReplyHandler = (
  response: MockResponse,
  rawBytes: Buffer
) => Promise<void> | void;

export type SessionRestartHandler = (
  oldSessionId: string,
  newSessionId: string,
  unreadTailPerSensor: Map<SensorId, number>
) => Promise<void> | void;

export class BiosignalTcpClient {
  private options: Required<TcpClientOptions>;
  private socket: net.Socket | null = null;
  private buffer: Buffer = Buffer.alloc(0);
  private isConnecting: boolean = false;
  private isDestroyed: boolean = false;

  private currentSessionId: string | null = null;
  private progress: Map<SensorId, number> = new Map(); // next expected index per sensor

  // Buffered incoming complete lines and line waiters
  private availableLines: Buffer[] = [];
  private lineWaiters: Array<{
    resolve: (line: Buffer) => void;
    reject: (err: Error) => void;
    timer: NodeJS.Timeout;
  }> = [];

  // FIX 2: Reply queues for decoupled sensor responses
  private queuedReplies: Map<SensorId, RawValidationResult[]> = new Map([
    [1, []],
    [2, []],
  ]);

  // FIX 2: Bounded fingerprint cache for dropping exact duplicates
  private seenFingerprints: Set<string> = new Set();
  private fingerprintQueue: string[] = [];
  private readonly maxFingerprints: number = 10000;
  private duplicateRepliesDropped: number = 0;

  constructor(opts: TcpClientOptions) {
    this.options = {
      host: opts.host || '127.0.0.1',
      port: opts.port || 7100,
      connectTimeoutMs: opts.connectTimeoutMs || 3000,
      readTimeoutMs: opts.readTimeoutMs || 4000,
      maxLineLength: opts.maxLineLength || 512 * 1024, // 512 KB line limit
      maxRetries: opts.maxRetries || 3,
    };
  }

  public getSessionId(): string | null {
    return this.currentSessionId;
  }

  public getExpectedIndex(sensor: SensorId): number {
    return this.progress.get(sensor) || 0;
  }

  public setProgress(sessionId: string, sensor: SensorId, nextIndex: number) {
    this.currentSessionId = sessionId;
    this.progress.set(sensor, nextIndex);
  }

  public getDuplicateRepliesDropped(): number {
    return this.duplicateRepliesDropped;
  }

  public resetDuplicateCount(): void {
    this.duplicateRepliesDropped = 0;
  }

  private isDuplicate(fp: string): boolean {
    return this.seenFingerprints.has(fp);
  }

  private recordFingerprint(fp: string): void {
    if (this.seenFingerprints.has(fp)) return;
    this.seenFingerprints.add(fp);
    this.fingerprintQueue.push(fp);
    if (this.fingerprintQueue.length > this.maxFingerprints) {
      const oldest = this.fingerprintQueue.shift()!;
      this.seenFingerprints.delete(oldest);
    }
  }

  /**
   * Connect or ensure connection to the mock service.
   */
  public async connect(): Promise<void> {
    this.isDestroyed = false;

    if (this.socket && !this.socket.destroyed) {
      return;
    }

    if (this.isConnecting) {
      while (this.isConnecting) {
        await new Promise((r) => setTimeout(r, 50));
      }
      if (this.socket && !this.socket.destroyed) return;
    }

    this.isConnecting = true;

    return new Promise((resolve, reject) => {
      const socket = new net.Socket();

      const connectTimer = setTimeout(() => {
        socket.destroy();
        this.isConnecting = false;
        reject(new Error(`TCP connect timeout after ${this.options.connectTimeoutMs}ms`));
      }, this.options.connectTimeoutMs);

      socket.connect(this.options.port, this.options.host, () => {
        clearTimeout(connectTimer);
        this.socket = socket;
        this.isConnecting = false;
        this.buffer = Buffer.alloc(0);

        socket.removeAllListeners('error');
        socket.on('data', (chunk) => this.onData(chunk));
        socket.on('close', () => this.onClose());
        socket.on('error', (err) => this.onError(err));

        resolve();
      });

      socket.on('error', (err) => {
        clearTimeout(connectTimer);
        this.isConnecting = false;
        reject(err);
      });
    });
  }

  public disconnect(): void {
    this.isDestroyed = true;
    this.closeSocket();
  }

  private closeSocket(): void {
    this.rejectAllWaiters(new Error('Client disconnected or socket closed'));
    if (this.socket) {
      this.socket.removeAllListeners();
      this.socket.destroy();
      this.socket = null;
    }
    this.buffer = Buffer.alloc(0);
    this.availableLines = [];
  }

  private rejectAllWaiters(err: Error): void {
    for (const waiter of this.lineWaiters) {
      clearTimeout(waiter.timer);
      waiter.reject(err);
    }
    this.lineWaiters = [];
  }

  private onData(chunk: Buffer): void {
    this.buffer = Buffer.concat([this.buffer, chunk]);

    if (this.buffer.length > this.options.maxLineLength) {
      const err = new Error(`Buffer exceeded maximum allowed line length (${this.options.maxLineLength} bytes)`);
      this.rejectAllWaiters(err);
      this.disconnect();
      return;
    }

    this.drainLines();
  }

  private drainLines(): void {
    let newlineIdx: number;
    while ((newlineIdx = this.buffer.indexOf(0x0a)) !== -1) { // 0x0a is '\n'
      let line = this.buffer.subarray(0, newlineIdx);
      this.buffer = this.buffer.subarray(newlineIdx + 1);

      // Handle CRLF: strip trailing \r (0x0d)
      if (line.length > 0 && line[line.length - 1] === 0x0d) {
        line = line.subarray(0, line.length - 1);
      }

      if (line.length === 0) continue; // ignore empty lines

      if (this.lineWaiters.length > 0) {
        const waiter = this.lineWaiters.shift()!;
        clearTimeout(waiter.timer);
        waiter.resolve(line);
      } else {
        this.availableLines.push(line);
      }
    }
  }

  private readNextLine(timeoutMs: number): Promise<Buffer> {
    if (this.availableLines.length > 0) {
      return Promise.resolve(this.availableLines.shift()!);
    }

    if (!this.socket || this.socket.destroyed) {
      return Promise.reject(new Error('Socket is not connected'));
    }

    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        const idx = this.lineWaiters.findIndex((w) => w.resolve === resolve);
        if (idx !== -1) {
          this.lineWaiters.splice(idx, 1);
        }
        const err: any = new Error(`Read timeout after ${timeoutMs}ms`);
        err.isTimeout = true;
        reject(err);
      }, timeoutMs);

      this.lineWaiters.push({ resolve, reject, timer });
    });
  }

  private onClose(): void {
    this.rejectAllWaiters(new Error('Connection closed by remote host'));
    this.socket = null;
  }

  private onError(err: Error): void {
    this.rejectAllWaiters(err);
    if (this.socket) {
      this.socket.removeAllListeners();
      this.socket.destroy();
      this.socket = null;
    }
  }

  /**
   * Send a command and read exactly one complete line response.
   */
  public async sendCommand(cmdObj: any): Promise<Buffer> {
    await this.connect();
    await this.writeCommand(cmdObj);
    return this.readNextLine(this.options.readTimeoutMs);
  }

  private async writeCommand(cmdObj: any): Promise<void> {
    await this.connect();
    const cmdStr = JSON.stringify(cmdObj) + '\n';
    return new Promise((resolve, reject) => {
      if (!this.socket || this.socket.destroyed) {
        return reject(new Error('Socket is not connected'));
      }
      this.socket.write(cmdStr, 'utf-8', (err) => {
        if (err) reject(err);
        else resolve();
      });
    });
  }

  private updateSessionAndProgress(resp: MockResponse): void {
    if (this.currentSessionId !== null && resp.session_id !== this.currentSessionId) {
      this.currentSessionId = resp.session_id;
      this.progress.set(1, 0);
      this.progress.set(2, 0);
      this.queuedReplies.get(1)!.length = 0;
      this.queuedReplies.get(2)!.length = 0;
      this.seenFingerprints.clear();
      this.fingerprintQueue = [];
    } else if (this.currentSessionId === null) {
      this.currentSessionId = resp.session_id;
    }

    const currentProg = this.progress.get(resp.sensor) ?? 0;
    const maxReached = resp.first_index + resp.count;
    if (maxReached > currentProg) {
      this.progress.set(resp.sensor, maxReached);
    }
  }

  private popMatchingQueuedReply(
    sensor: SensorId,
    overrideFromIndex?: number
  ): RawValidationResult | null {
    const queue = this.queuedReplies.get(sensor);
    if (!queue || queue.length === 0) return null;

    const expected = overrideFromIndex !== undefined ? overrideFromIndex : (this.progress.get(sensor) ?? 0);

    while (queue.length > 0) {
      const item = queue.shift()!;
      const resp = item.response;

      let isConsistent = false;
      if (overrideFromIndex !== undefined) {
        isConsistent = resp.first_index === overrideFromIndex || resp.first_index >= overrideFromIndex;
      } else {
        isConsistent = resp.first_index >= expected || (resp.first_index + resp.count > expected);
      }

      if (isConsistent) {
        this.updateSessionAndProgress(resp);
        return item;
      } else {
        this.duplicateRepliesDropped++;
      }
    }

    return null;
  }

  private async fetchMatchingReply(
    sensor: SensorId,
    expected: number,
    overrideFromIndex?: number
  ): Promise<RawValidationResult> {
    await this.connect();

    const cmd: any = { cmd: 'read', sensor };
    if (overrideFromIndex !== undefined) {
      cmd.from_index = overrideFromIndex;
    } else if (expected !== undefined) {
      cmd.from_index = expected;
    }

    await this.writeCommand(cmd);

    const deadline = Date.now() + this.options.readTimeoutMs;

    while (true) {
      const remainingMs = deadline - Date.now();
      if (remainingMs <= 0) {
        const err: any = new Error(`Read timeout after ${this.options.readTimeoutMs}ms waiting for sensor ${sensor}`);
        err.isTimeout = true;
        throw err;
      }

      const rawLine = await this.readNextLine(remainingMs);

      // Validate schema and CRC without enforcing expectedSensor upfront
      const validation = validateRawResponse(rawLine);

      if (!validation.ok) {
        // Corrupt CRC or malformed line: return immediately for retry handling
        return validation;
      }

      const resp = validation.response;
      const fp = `${resp.session_id}:${resp.sensor}:${resp.first_index}:${resp.count}:${resp.crc32}`;

      // Check sensor ID
      if (resp.sensor !== sensor) {
        // A reply for the other sensor is queued or dropped, NEVER fatal!
        if (resp.count > 0 && this.isDuplicate(fp)) {
          this.duplicateRepliesDropped++;
          continue;
        }
        if (resp.count > 0) {
          this.recordFingerprint(fp);
        }

        const otherQueue = this.queuedReplies.get(resp.sensor as SensorId);
        if (otherQueue && otherQueue.length < 500) {
          otherQueue.push(validation);
        }
        // Continue consuming lines until the requested sensor arrives
        continue;
      }

      // Sensor matches requested sensor: check consistency
      let isConsistent = false;
      if (overrideFromIndex !== undefined) {
        isConsistent = resp.first_index === overrideFromIndex || resp.first_index >= overrideFromIndex;
      } else {
        isConsistent = resp.first_index >= expected || (resp.first_index + resp.count > expected);
      }

      if (!isConsistent) {
        // Stale duplicate reply for this sensor
        this.duplicateRepliesDropped++;
        continue;
      }

      // Check if this is an explicit re-read of past data requested by caller
      const currentProg = this.progress.get(sensor) ?? 0;
      const isExplicitPastReRead = overrideFromIndex !== undefined && overrideFromIndex < currentProg && resp.first_index === overrideFromIndex;

      if (resp.count > 0 && !isExplicitPastReRead && this.isDuplicate(fp)) {
        // Drop exact duplicate
        this.duplicateRepliesDropped++;
        continue;
      }

      // Found matching consistent reply
      if (resp.count > 0) {
        this.recordFingerprint(fp);
      }
      this.updateSessionAndProgress(resp);
      return validation;
    }
  }

  /**
   * Request next data for sensor with validation, retry on CRC mismatch, and gap recovery.
   * Consumes lines until one has matching sensor and consistent first_index.
   * Replies for other sensor are queued; exact duplicates are dropped.
   * On timeout, closes and reconnects, then re-reads with from_index = expected_next.
   * Never throws "Sensor ID mismatch" out of the read loop.
   */
  public async readSensor(
    sensor: SensorId,
    overrideFromIndex?: number
  ): Promise<RawValidationResult> {
    // 1. Check if a valid matching reply is already queued for this sensor
    const queued = this.popMatchingQueuedReply(sensor, overrideFromIndex);
    if (queued) {
      return queued;
    }

    const expected = overrideFromIndex !== undefined ? overrideFromIndex : (this.progress.get(sensor) ?? 0);

    try {
      return await this.fetchMatchingReply(sensor, expected, overrideFromIndex);
    } catch (err: any) {
      if (err.isTimeout) {
        this.closeSocket();
        return {
          ok: false,
          code: 'TRUNCATED_OR_MALFORMED',
          error: `Network/Read error: ${err.message}`,
        };
      }

      return {
        ok: false,
        code: 'TRUNCATED_OR_MALFORMED',
        error: `Network/Read error: ${err.message}`,
      };
    }
  }
}
