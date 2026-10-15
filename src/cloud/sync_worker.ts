import type { StorageEngine } from '../storage/db.ts';
import type { CloudApiClient } from './client.ts';
import { ChunkCodec } from './chunk_codec.ts';
import type { ChunkPayload } from '../../cloud/worker/src/types.ts';
import type { WindowResult } from '../types.ts';

import type { NetworkMonitor } from '../network/network_monitor.ts';

export interface SyncWorkerOptions {
  storage: StorageEngine;
  client: CloudApiClient;
  sessionId: string;
  networkMonitor?: NetworkMonitor;
  backlogConcurrency?: number;
  backlogBatchSize?: number;
}

export interface SyncStats {
  liveChunksSynced: number;
  historicalChunksSynced: number;
  totalBytesSent: number;
  lastLiveLatencyMs?: number;
}

export class SyncWorker {
  private storage: StorageEngine;
  private client: CloudApiClient;
  private sessionId: string;
  private networkMonitor: NetworkMonitor | null = null;
  private isRunning: boolean = false;
  private liveBusy: boolean = false;
  private stats: SyncStats = {
    liveChunksSynced: 0,
    historicalChunksSynced: 0,
    totalBytesSent: 0,
  };

  constructor(opts: SyncWorkerOptions) {
    this.storage = opts.storage;
    this.client = opts.client;
    this.sessionId = opts.sessionId;
    if (opts.networkMonitor) {
      this.networkMonitor = opts.networkMonitor;
    }
  }

  public getStats(): SyncStats {
    return { ...this.stats };
  }

  /**
   * Synchronize a live chunk with highest priority.
   * Ensures chunk is processed online in the cloud within 5 seconds.
   */
  public async syncLiveChunk(chunkIdx: number): Promise<{ ok: boolean; results?: WindowResult[]; latencyMs: number }> {
    const startTime = Date.now();
    this.liveBusy = true;

    try {
      const payload = await this.buildChunkPayload(chunkIdx, 'online');
      const payloadBytes = JSON.stringify(payload).length;

      const resp = await this.client.ingestChunk(payload);
      this.stats.totalBytesSent += payloadBytes;

      // Positive health signal to network monitor
      if (this.networkMonitor) {
        this.networkMonitor.recordSuccessfulIngestion();
      }

      // Cloud commit is confirmed!
      this.storage.markChunkSynced(this.sessionId, chunkIdx, 'online');
      this.stats.liveChunksSynced++;

      const latencyMs = Date.now() - startTime;
      this.stats.lastLiveLatencyMs = latencyMs;

      // Save authoritative online results locally if returned
      if (resp.results && resp.results.length > 0) {
        for (const res of resp.results) {
          this.storage.saveProcessingResult(res, [], res.window_idx, -1);
        }
      }

      return { ok: true, results: resp.results, latencyMs };
    } catch (err: any) {
      if (this.networkMonitor) {
        this.networkMonitor.recordFailedIngestion(err?.message || 'Live chunk ingest failed');
      }
      throw err;
    } finally {
      this.liveBusy = false;
    }
  }

  /**
   * Reconcile inventory with the cloud in bounded pages.
   * Updates local progress table for any chunks already held in the cloud.
   */
  public async reconcileInventory(maxChunkIdx: number): Promise<{ reconciled: number; cloudTotal: number }> {
    let fromChunk = 0;
    let reconciled = 0;
    let cloudTotal = 0;

    while (fromChunk <= maxChunkIdx) {
      const inv = await this.client.getInventory(this.sessionId, fromChunk, maxChunkIdx, 1000);
      cloudTotal += inv.chunks.length;

      for (const c of inv.chunks) {
        if (!this.storage.isChunkSynced(this.sessionId, c)) {
          this.storage.markChunkSynced(this.sessionId, c, 'offline');
          reconciled++;
        }
      }

      if (inv.chunks.length === 0 || (inv.max_chunk !== undefined && inv.max_chunk >= maxChunkIdx)) {
        break;
      }

      fromChunk = (inv.max_chunk ?? fromChunk) + 1;
    }

    return { reconciled, cloudTotal };
  }

  /**
   * Run historical backlog synchronization.
   * Iterates through unsynced historical chunks oldest-first without blocking the live lane.
   */
  public async syncHistoricalBacklog(options?: {
    maxChunkIdx?: number;
    onProgress?: (synced: number, total: number) => void;
  }): Promise<{ syncedCount: number; durationMs: number }> {
    const start = Date.now();
    let syncedCount = 0;

    // First find highest stored chunk
    let maxChunk = options?.maxChunkIdx;
    if (maxChunk === undefined) {
      const p1 = this.storage.getProgress(this.sessionId, 1);
      const p2 = this.storage.getProgress(this.sessionId, 2);
      const maxWatermark = Math.min(p1.resolvedWatermark, p2.resolvedWatermark);
      maxChunk = maxWatermark >= 0 ? Math.floor(maxWatermark / 800) : -1;
    }

    if (maxChunk < 0) {
      return { syncedCount: 0, durationMs: Date.now() - start };
    }

    // Reconcile with cloud first to avoid re-uploading already stored chunks
    await this.reconcileInventory(maxChunk);

    while (true) {
      const unsynced = this.storage.getUnsyncedChunks(this.sessionId, maxChunk, 50);
      if (unsynced.length === 0) break;

      for (const chunkIdx of unsynced) {
        // Yield if live lane needs to transmit!
        while (this.liveBusy) {
          await new Promise((resolve) => setTimeout(resolve, 10));
        }

        const payload = await this.buildChunkPayload(chunkIdx, 'offline');
        const payloadBytes = JSON.stringify(payload).length;

        const resp = await this.client.ingestChunk(payload);
        this.stats.totalBytesSent += payloadBytes;

        if (this.networkMonitor) {
          this.networkMonitor.recordSuccessfulIngestion();
        }

        // Mark synced on verified commit
        this.storage.markChunkSynced(this.sessionId, chunkIdx, 'offline');
        this.stats.historicalChunksSynced++;
        syncedCount++;

        if (options?.onProgress) {
          options.onProgress(syncedCount, unsynced.length);
        }

        // Cooperatively yield event loop between chunks to prevent writer starvation
        await new Promise((resolve) => setImmediate(resolve));
      }
    }

    const durationMs = Date.now() - start;
    return { syncedCount, durationMs };
  }

  /**
   * Construct a compact, reversible, digested ChunkPayload from SQLite.
   */
  public async buildChunkPayload(chunkIdx: number, mode: 'online' | 'offline'): Promise<ChunkPayload> {
    const data = this.storage.getChunkData(this.sessionId, chunkIdx);
    const { fromIndex, toIndex, s1Map, s2Map, losses, offlineResults } = data;

    const s1Encoded = ChunkCodec.encodeSensor(1, fromIndex, s1Map);
    const s2Encoded = ChunkCodec.encodeSensor(2, fromIndex, s2Map);

    // Build context if live mode: include preceding 800 samples of Sensor 1 Channel 2 and lookahead sample
    let context: ChunkPayload['context'] = undefined;
    if (mode === 'online') {
      const historyStart = Math.max(0, fromIndex - 800);
      const historyMap = this.storage.getSampleRange(this.sessionId, 1, historyStart, fromIndex - 1);
      const s1History: Array<{ index: number; values: number[] }> = [];
      for (const [idx, vals] of historyMap.entries()) {
        s1History.push({ index: idx, values: vals });
      }

      // Check for lookahead sample (toIndex + 1)
      const lookaheadMap = this.storage.getSampleRange(this.sessionId, 1, toIndex + 1, toIndex + 1);
      const lookahead: Array<{ index: number; values: number[] }> = [];
      if (lookaheadMap.has(toIndex + 1)) {
        lookahead.push({ index: toIndex + 1, values: lookaheadMap.get(toIndex + 1)! });
      }

      context = {
        s1_history: s1History.length > 0 ? s1History : undefined,
        lookahead: lookahead.length > 0 ? lookahead : undefined,
      };
    }

    const digest = await ChunkCodec.computeDigest({
      sessionId: this.sessionId,
      chunkIdx,
      fromIndex,
      toIndex,
      s1Encoded: s1Encoded.encoded,
      s2Encoded: s2Encoded.encoded,
      s1Mask: s1Encoded.maskHex,
      s2Mask: s2Encoded.maskHex,
      lost: losses,
    });

    return {
      session_id: this.sessionId,
      chunk_idx: chunkIdx,
      from_index: fromIndex,
      to_index: toIndex,
      mode,
      s1_encoded: s1Encoded.encoded,
      s2_encoded: s2Encoded.encoded,
      s1_mask: s1Encoded.maskHex,
      s2_mask: s2Encoded.maskHex,
      lost: losses,
      digest,
      offline_results: mode === 'offline' && offlineResults.length > 0 ? offlineResults : undefined,
      context,
    };
  }
}
