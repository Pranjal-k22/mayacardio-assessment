import { parentPort, workerData } from 'node:worker_threads';
import { StorageEngine } from '../storage/db.ts';
import { BiosignalProtocolAdapter } from './protocol_adapter.ts';
import type { SensorId } from '../types.ts';

export interface CollectorWorkerConfig {
  dbPath: string;
  host: string;
  port: number;
  connectTimeoutMs?: number;
  readTimeoutMs?: number;
}

export interface CollectorTelemetryMessage {
  type: 'session_active' | 'watermark' | 'metrics' | 'error' | 'stopped';
  sessionId?: string;
  sensor?: SensorId;
  watermark?: number;
  readLoopGapMs?: number;
  p99CommitLatencyMs?: number;
  maxCommitLatencyMs?: number;
  commitLatencies?: number[];
  maxOverallLoopGapMs?: number;
  error?: string;
}

if (!parentPort) {
  throw new Error('collector_worker.ts must be run as a worker_thread');
}

const config: CollectorWorkerConfig = workerData;

const storage = new StorageEngine({ dbPath: config.dbPath });
const adapter = new BiosignalProtocolAdapter(
  {
    host: config.host,
    port: config.port,
    connectTimeoutMs: config.connectTimeoutMs ?? 3000,
    readTimeoutMs: config.readTimeoutMs ?? 4000,
  },
  storage
);

let isRunning = true;
let currentSessionId: string | null = null;

// Track metrics
let lastLoopTime = Date.now();
let maxLoopGapMs = 0;
let overallMaxLoopGapMs = 0;
const recentCommitLatencies: number[] = [];
const allCommitLatencies: number[] = [];

adapter.onSessionTransition((ev) => {
  currentSessionId = ev.newSessionId;
  parentPort?.postMessage({
    type: 'session_active',
    sessionId: ev.newSessionId,
  } as CollectorTelemetryMessage);
});

parentPort.on('message', (msg) => {
  if (msg?.type === 'stop') {
    isRunning = false;
  }
});

async function runCollector() {
  try {
    await adapter.connect();
  } catch (err: any) {
    parentPort?.postMessage({
      type: 'error',
      error: `Initial mock connect failed: ${err.message}`,
    } as CollectorTelemetryMessage);
  }

  let lastMetricsReport = Date.now();

  while (isRunning) {
    const loopStart = Date.now();
    const gap = loopStart - lastLoopTime;
    if (gap > maxLoopGapMs) maxLoopGapMs = gap;
    if (gap > overallMaxLoopGapMs) overallMaxLoopGapMs = gap;
    lastLoopTime = loopStart;

    try {
      const r1 = await adapter.readNext(1);
      const r2 = await adapter.readNext(2);

      if (r1.success && r1.commitLatencyMs !== undefined) {
        recentCommitLatencies.push(r1.commitLatencyMs);
        allCommitLatencies.push(r1.commitLatencyMs);
        if (recentCommitLatencies.length > 500) recentCommitLatencies.shift();
      }
      if (r2.success && r2.commitLatencyMs !== undefined) {
        recentCommitLatencies.push(r2.commitLatencyMs);
        allCommitLatencies.push(r2.commitLatencyMs);
        if (recentCommitLatencies.length > 500) recentCommitLatencies.shift();
      }

      if (r1.success && r1.response.session_id) {
        if (!currentSessionId) {
          currentSessionId = r1.response.session_id;
          parentPort?.postMessage({
            type: 'session_active',
            sessionId: currentSessionId,
          } as CollectorTelemetryMessage);
        }
      }

      // Periodically report telemetry to parent
      const now = Date.now();
      if (now - lastMetricsReport >= 1000) {
        lastMetricsReport = now;
        const sorted = [...recentCommitLatencies].sort((a, b) => a - b);
        const p99 = sorted.length > 0 ? sorted[Math.floor(sorted.length * 0.99)] : 0;
        const maxCommit = sorted.length > 0 ? sorted[sorted.length - 1] : 0;

        parentPort?.postMessage({
          type: 'metrics',
          sessionId: currentSessionId ?? undefined,
          readLoopGapMs: maxLoopGapMs,
          p99CommitLatencyMs: p99,
          maxCommitLatencyMs: maxCommit,
        } as CollectorTelemetryMessage);
        maxLoopGapMs = 0;
      }

      // Cooperative brief pause if buffer is empty
      if (r1.success && r2.success && r1.response.count === 0 && r2.response.count === 0) {
        await new Promise((r) => setTimeout(r, 10));
      }

    } catch (err: any) {
      parentPort?.postMessage({
        type: 'error',
        error: err.message,
      } as CollectorTelemetryMessage);
      await new Promise((r) => setTimeout(r, 50));
    }
  }

  // Drain any remaining buffered samples so both sensors finish at the exact same index
  try {
    const exp1 = adapter.getExpectedIndex(1);
    const exp2 = adapter.getExpectedIndex(2);
    if (exp1 < exp2) {
      const rTail1 = await adapter.readNext(1);
      if (rTail1.success && rTail1.commitLatencyMs !== undefined) {
        allCommitLatencies.push(rTail1.commitLatencyMs);
      }
    } else if (exp2 < exp1) {
      const rTail2 = await adapter.readNext(2);
      if (rTail2.success && rTail2.commitLatencyMs !== undefined) {
        allCommitLatencies.push(rTail2.commitLatencyMs);
      }
    }
  } catch {}

  adapter.disconnect();
  storage.close();
  parentPort?.postMessage({
    type: 'stopped',
    commitLatencies: allCommitLatencies,
    maxOverallLoopGapMs: overallMaxLoopGapMs,
  } as CollectorTelemetryMessage);
}

runCollector().catch((err) => {
  parentPort?.postMessage({ type: 'error', error: err.message } as CollectorTelemetryMessage);
});
