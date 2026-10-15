#!/usr/bin/env node
import * as path from 'node:path';
import * as fs from 'node:fs';
import { fileURLToPath } from 'node:url';
import { StorageEngine } from '../src/storage/db.ts';
import { CloudApiClient } from '../src/cloud/client.ts';
import { IntegrityAuditor } from '../src/audit/integrity_audit.ts';
import { BiosignalProtocolAdapter } from '../src/collector/protocol_adapter.ts';
import { WindowProcessingEngine } from '../src/processing/engine.ts';
import { NetworkMonitor } from '../src/network/network_monitor.ts';
import { HttpHealthProber } from '../src/network/http_prober.ts';
import { OfflineProcessingCoordinator } from '../src/network/offline_coordinator.ts';
import { SyncWorker } from '../src/cloud/sync_worker.ts';
import type { PipelineStatus, RawSample } from '../src/types.ts';

/**
 * Automatically load .env file if present in working directory or parent directory.
 */
function loadEnvFile(): void {
  const envPaths = [
    path.resolve('.env'),
    path.resolve(process.cwd(), '.env'),
    path.resolve(path.dirname(process.argv[1] || ''), '..', '.env'),
  ];
  for (const p of envPaths) {
    if (fs.existsSync(p)) {
      try {
        const content = fs.readFileSync(p, 'utf8');
        for (const line of content.split(/\r?\n/)) {
          const trimmed = line.trim();
          if (!trimmed || trimmed.startsWith('#')) continue;
          const eqIdx = trimmed.indexOf('=');
          if (eqIdx > 0) {
            const key = trimmed.substring(0, eqIdx).trim();
            const val = trimmed.substring(eqIdx + 1).trim();
            if (process.env[key] === undefined) {
              process.env[key] = val;
            }
          }
        }
        break;
      } catch {}
    }
  }
}

loadEnvFile();

interface ParsedArgs {
  command?: string;
  session?: string;
  from?: 'device' | 'cloud';
  dbPath?: string;
  port?: number;
  host?: string;
  cloudUrl?: string;
  authToken?: string;
  help?: boolean;
}

function parseArgs(args: string[]): ParsedArgs {
  const parsed: ParsedArgs = {};
  let i = 0;

  if (args.length > 0 && !args[0].startsWith('-')) {
    parsed.command = args[0];
    i = 1;
  }

  while (i < args.length) {
    const arg = args[i];
    if (arg === '--help' || arg === '-h') {
      parsed.help = true;
      i++;
    } else if (arg === '--session' || arg === '-s') {
      parsed.session = args[i + 1];
      i += 2;
    } else if (arg === '--from') {
      const val = args[i + 1];
      if (val === 'device' || val === 'cloud') {
        parsed.from = val;
      }
      i += 2;
    } else if (arg === '--db') {
      parsed.dbPath = args[i + 1];
      i += 2;
    } else if (arg === '--port' || arg === '-p') {
      parsed.port = parseInt(args[i + 1], 10);
      i += 2;
    } else if (arg === '--host') {
      parsed.host = args[i + 1];
      i += 2;
    } else if (arg === '--cloud-url') {
      parsed.cloudUrl = args[i + 1];
      i += 2;
    } else if (arg === '--token' || arg === '--auth-token') {
      parsed.authToken = args[i + 1];
      i += 2;
    } else {
      i++;
    }
  }

  return parsed;
}

function printHelp(): void {
  const helpText = `Usage: pipeline <command> [options]

Commands:
  status                                 Report current mode, sample counts, and backlog
  export --session <id> --from <target>  Export canonical samples and losses ('device' or 'cloud')
  results --session <id>                 Export authoritative window processing results
  audit --session <id>                   Perform comprehensive data and loss integrity audit
  start                                  Start the continuous biosignal ingestion and sync pipeline

Options:
  --session, -s <id> Session identifier
  --from <target>    Export source: 'device' or 'cloud'
  --db <path>        SQLite database file path (default: ./pipeline.db or env STORAGE_DB_PATH)
  --port, -p <port>  Mock service TCP port (default: 7100 or env MOCK_TCP_PORT)
  --host <host>      Mock service TCP host (default: 127.0.0.1 or env MOCK_TCP_HOST)
  --cloud-url <url>  Cloudflare Worker URL (default: env CLOUD_BASE_URL)
  --token <token>    Cloudflare authentication Bearer token (default: env CLOUD_AUTH_TOKEN)
  -h, --help         Show this help text
`;
  process.stdout.write(helpText);
}

function getDbPath(parsed: ParsedArgs): string {
  if (parsed.dbPath) return parsed.dbPath;
  if (process.env.STORAGE_DB_PATH) return process.env.STORAGE_DB_PATH;
  if (fs.existsSync('pipeline.db')) return 'pipeline.db';
  if (fs.existsSync(path.resolve('data', 'mayacardio.db'))) return path.resolve('data', 'mayacardio.db');
  return 'pipeline.db';
}

function getCloudClient(parsed: ParsedArgs): CloudApiClient {
  const baseUrl = parsed.cloudUrl || process.env.CLOUD_BASE_URL || process.env.CLOUD_SYNC_URL || 'https://mayacardio.example.workers.dev';
  const authToken = parsed.authToken || process.env.CLOUD_AUTH_TOKEN || '';
  return new CloudApiClient({
    baseUrl,
    authToken,
    timeoutMs: 5000,
  });
}

async function handleStatus(parsed: ParsedArgs): Promise<number> {
  const dbPath = getDbPath(parsed);
  if (!fs.existsSync(dbPath)) {
    // If DB does not exist yet, report clean zero state offline
    const status: PipelineStatus = {
      mode: 'offline',
      samples_saved: { '1': 0, '2': 0 },
      samples_in_cloud: 0,
      backlog: 0,
    };
    process.stdout.write(JSON.stringify(status) + '\n');
    return 0;
  }

  let storage: StorageEngine | null = null;
  try {
    storage = new StorageEngine({ dbPath });
    const sessionId = parsed.session || storage.getLatestSessionId() || undefined;

    const counts = storage.getSampleCounts(sessionId);
    const confirmedCloudPositions = storage.getConfirmedCloudSampleCount(sessionId);

    // Backlog: unique stored sample positions saved locally minus confirmed in cloud
    const savedPositions = Math.max(counts['1'], counts['2']);
    const backlog = Math.max(0, savedPositions - confirmedCloudPositions);

    // Determine mode: attempt lightweight probe if cloud url is configured, else offline
    let mode: 'online' | 'offline' = 'offline';
    try {
      const client = getCloudClient(parsed);
      const h = await client.health();
      if (h.ok) mode = 'online';
    } catch {
      mode = 'offline';
    }

    const status: PipelineStatus = {
      mode,
      samples_saved: counts,
      samples_in_cloud: confirmedCloudPositions,
      backlog,
    };

    process.stdout.write(JSON.stringify(status) + '\n');
    return 0;
  } catch (err: any) {
    process.stderr.write(`Error retrieving status: ${err.message}\n`);
    return 1;
  } finally {
    if (storage) storage.close();
  }
}

async function handleExportDevice(sessionId: string, parsed: ParsedArgs): Promise<number> {
  const dbPath = getDbPath(parsed);
  if (!fs.existsSync(dbPath)) {
    process.stderr.write(`Error: Database file '${dbPath}' not found\n`);
    return 1;
  }

  let storage: StorageEngine | null = null;
  try {
    storage = new StorageEngine({ dbPath });
    if (!storage.sessionExists(sessionId)) {
      process.stderr.write(`Error: Session '${sessionId}' not found in database\n`);
      return 1;
    }

    // 1. Stream samples in index order (sensor 1 then sensor 2 at each index)
    // To ensure O(1) memory, fetch by bounded index ranges
    const maxIdxRow = (storage as any).db
      .prepare('SELECT MAX(sample_idx) as m FROM samples WHERE session_id = ?')
      .get(sessionId) as any;
    const maxStoredIdx = maxIdxRow?.m !== null && maxIdxRow?.m !== undefined ? maxIdxRow.m : -1;

    let fromIdx = 0;
    const step = 1000;
    while (fromIdx <= maxStoredIdx) {
      const toIdx = fromIdx + step - 1;
      const s1Batch = storage.getSamplesRange(sessionId, 1, fromIdx, toIdx);
      const s2Batch = storage.getSamplesRange(sessionId, 2, fromIdx, toIdx);

      const indices = new Set<number>();
      for (const s of s1Batch) indices.add(s.index);
      for (const s of s2Batch) indices.add(s.index);
      const sortedIndices = Array.from(indices).sort((a, b) => a - b);

      const s1Map = new Map<number, RawSample>(s1Batch.map((s) => [s.index, s]));
      const s2Map = new Map<number, RawSample>(s2Batch.map((s) => [s.index, s]));

      for (const idx of sortedIndices) {
        const s1 = s1Map.get(idx);
        if (s1) {
          process.stdout.write(
            JSON.stringify({ sensor: 1, index: s1.index, values: s1.values }) + '\n'
          );
        }
        const s2 = s2Map.get(idx);
        if (s2) {
          process.stdout.write(
            JSON.stringify({ sensor: 2, index: s2.index, values: s2.values }) + '\n'
          );
        }
      }

      fromIdx += step;
    }

    // 2. Stream all loss records in ascending order (guaranteeing coverage up to high watermark)
    const losses = storage.getExportLosses(sessionId);
    for (const l of losses) {
      process.stdout.write(
        JSON.stringify({
          sensor: l.sensor,
          from_index: l.from_index,
          to_index: l.to_index,
          reason: l.reason,
        }) + '\n'
      );
    }

    return 0;
  } catch (err: any) {
    process.stderr.write(`Error during device export: ${err.message}\n`);
    return 1;
  } finally {
    if (storage) storage.close();
  }
}

async function handleExportCloud(sessionId: string, parsed: ParsedArgs): Promise<number> {
  const client = getCloudClient(parsed);
  try {
    // 1. Check session inventory to verify existence and get total chunk count
    const inv = await client.getInventory(sessionId, 0, 1);
    if (!inv || inv.total_chunks === 0) {
      process.stderr.write(`Error: Session '${sessionId}' not found in cloud\n`);
      return 1;
    }

    // 2. Stream sample pages as they arrive, collect loss lines (bounded requests for Worker CPU limits)
    const pageChunkLimit = 50;
    const collectedLosses = new Map<string, any>();

    for (let c = 0; c < inv.total_chunks; c += pageChunkLimit) {
      const fromIndex = c * 800;
      const toIndex = (c + pageChunkLimit) * 800 - 1;
      const pageNdjson = await client.export(sessionId, fromIndex, toIndex);

      if (!pageNdjson || pageNdjson.trim() === '') {
        continue;
      }

      const lines = pageNdjson.trim().split('\n');
      for (const rawLine of lines) {
        const line = rawLine.trim();
        if (!line) continue;

        let record: any;
        try {
          record = JSON.parse(line);
        } catch {
          continue;
        }

        if (record.values !== undefined) {
          // Stream sample line immediately
          process.stdout.write(line + '\n');
        } else if (record.reason !== undefined) {
          // Collect loss line for deferred emission
          const key = `${record.sensor}:${record.from_index}:${record.to_index ?? 'null'}:${record.reason}`;
          if (!collectedLosses.has(key)) {
            collectedLosses.set(key, record);
          }
        }
      }
    }

    // 3. Print all loss lines after the last sample line, canonically sorted
    const sortedLosses = Array.from(collectedLosses.values()).sort((a, b) => {
      if (a.sensor !== b.sensor) return a.sensor - b.sensor;
      return a.from_index - b.from_index;
    });

    for (const l of sortedLosses) {
      process.stdout.write(JSON.stringify(l) + '\n');
    }

    return 0;
  } catch (err: any) {
    process.stderr.write(`Error during cloud export: ${err.message}\n`);
    return 1;
  }
}

async function handleResults(sessionId: string, parsed: ParsedArgs): Promise<number> {
  const dbPath = getDbPath(parsed);
  if (!fs.existsSync(dbPath)) {
    process.stderr.write(`Error: Database file '${dbPath}' not found\n`);
    return 1;
  }

  let storage: StorageEngine | null = null;
  try {
    storage = new StorageEngine({ dbPath });
    if (!storage.sessionExists(sessionId)) {
      process.stderr.write(`Error: Session '${sessionId}' not found in database\n`);
      return 1;
    }

    let fromWindow = 0;
    const batchSize = 1000;
    while (true) {
      const results = storage.getProcessingResults(sessionId, fromWindow, batchSize);
      if (results.length === 0) break;

      for (const res of results) {
        process.stdout.write(JSON.stringify(res) + '\n');
      }

      if (results.length < batchSize) break;
      fromWindow = results[results.length - 1].window_idx + 1;
    }

    return 0;
  } catch (err: any) {
    process.stderr.write(`Error retrieving results: ${err.message}\n`);
    return 1;
  } finally {
    if (storage) storage.close();
  }
}

async function handleAudit(sessionId: string, parsed: ParsedArgs): Promise<number> {
  const dbPath = getDbPath(parsed);
  if (!fs.existsSync(dbPath)) {
    process.stderr.write(`Error: Database file '${dbPath}' not found\n`);
    return 1;
  }

  let storage: StorageEngine | null = null;
  try {
    storage = new StorageEngine({ dbPath });
    let client: CloudApiClient | undefined;
    try {
      client = getCloudClient(parsed);
    } catch {}

    const auditor = new IntegrityAuditor(storage, client);
    const report = await auditor.auditSession(sessionId);

    process.stdout.write(JSON.stringify(report) + '\n');
    return report.pass ? 0 : 1;
  } catch (err: any) {
    process.stderr.write(`Error during audit: ${err.message}\n`);
    return 1;
  } finally {
    if (storage) storage.close();
  }
}

import { Worker } from 'node:worker_threads';

async function handleStart(parsed: ParsedArgs): Promise<number> {
  const dbPath = getDbPath(parsed);
  const host = parsed.host || process.env.MOCK_TCP_HOST || '127.0.0.1';
  const port = parsed.port || parseInt(process.env.MOCK_TCP_PORT || '7100', 10);
  const cloudUrl = parsed.cloudUrl || process.env.CLOUD_BASE_URL || process.env.CLOUD_SYNC_URL || 'https://mayacardio.example.workers.dev';
  const authToken = parsed.authToken || process.env.CLOUD_AUTH_TOKEN || '';

  process.stdout.write(`[PIPELINE] Initializing MayaCardio pipeline on ${host}:${port} (db: ${dbPath})...\n`);
  const storage = new StorageEngine({ dbPath });

  const client = new CloudApiClient({
    baseUrl: cloudUrl,
    authToken,
    timeoutMs: 5000,
  });

  const prober = new HttpHealthProber({
    healthUrl: `${cloudUrl.replace(/\/+$/, '')}/health`,
    customHeaders: authToken ? { Authorization: `Bearer ${authToken}` } : {},
  });
  const monitor = new NetworkMonitor({ prober, probeIntervalMs: 2000, probeTimeoutMs: 1500 });
  const engine = new WindowProcessingEngine({ storage });
  const coordinator = new OfflineProcessingCoordinator({
    networkMonitor: monitor,
    storage,
    processingEngine: engine,
  });

  let syncWorker: SyncWorker | null = null;
  let activeSessionId: string | null = parsed.session || null;

  if (activeSessionId) {
    syncWorker = new SyncWorker({ storage, client, sessionId: activeSessionId, networkMonitor: monitor });
  }

  // Launch isolated collector thread so mock reads are NEVER blocked by sync or processing
  const collectorScript = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '../src/collector/collector_worker.ts');
  const collector = new Worker(collectorScript, {
    execArgv: ['--experimental-strip-types'],
    workerData: {
      dbPath,
      host,
      port,
      connectTimeoutMs: 3000,
      readTimeoutMs: 4000,
    },
  });

  collector.on('message', (msg: any) => {
    if (msg?.type === 'session_active' && msg.sessionId) {
      if (activeSessionId !== msg.sessionId) {
        activeSessionId = msg.sessionId;
        syncWorker = new SyncWorker({ storage, client, sessionId: activeSessionId, networkMonitor: monitor });
        nextChunkIdxToSync = 0;
        process.stdout.write(`[PIPELINE] Session active: ${activeSessionId}\n`);
      }
    } else if (msg?.type === 'error') {
      process.stderr.write(`[COLLECTOR] ${msg.error}\n`);
    }
  });

  monitor.start();
  coordinator.start();

  let isRunning = true;
  const cleanup = () => {
    if (!isRunning) return;
    isRunning = false;
    process.stdout.write('\n[PIPELINE] Graceful shutdown completed.\n');
    collector.postMessage({ type: 'stop' });
    monitor.stop();
    coordinator.stop();
    storage.close();
    setTimeout(() => process.exit(0), 500);
  };

  process.on('SIGINT', cleanup);
  process.on('SIGTERM', cleanup);

  let lastSyncTime = Date.now();
  let nextChunkIdxToSync = 0;
  let isBacklogSyncing = false;

  while (isRunning) {
    try {
      if (activeSessionId) {
        const mode = monitor.getState() === 'ONLINE' ? 'online' : 'offline';
        engine.processAllAvailableWindows(activeSessionId, mode);

        if (mode === 'online' && syncWorker) {
          const now = Date.now();
          if (now - lastSyncTime >= 2000) {
            lastSyncTime = now;
            const p1 = storage.getProgress(activeSessionId, 1);
            const p2 = storage.getProgress(activeSessionId, 2);
            const minWatermark = Math.min(p1.resolvedWatermark, p2.resolvedWatermark);
            const maxReadyChunk = minWatermark >= 0 ? Math.floor(minWatermark / 800) - 1 : -1;

            // Live lane synchronization (highest priority)
            while (nextChunkIdxToSync <= maxReadyChunk) {
              await syncWorker.syncLiveChunk(nextChunkIdxToSync);
              nextChunkIdxToSync++;
            }

            // Historical backlog catch-up in bounded pages with cooperative yielding
            if (!isBacklogSyncing && maxReadyChunk >= 0) {
              const unsynced = storage.getUnsyncedChunks(activeSessionId, maxReadyChunk, 20);
              if (unsynced.length > 0) {
                isBacklogSyncing = true;
                setImmediate(async () => {
                  try {
                    for (const c of unsynced) {
                      if (!isRunning || monitor.getState() !== 'ONLINE') break;
                      const payload = await syncWorker!.buildChunkPayload(c, 'offline');
                      await client.ingestChunk(payload);
                      storage.markChunkSynced(activeSessionId!, c, 'offline');
                      await new Promise((r) => setTimeout(r, 10)); // Cooperative yield
                    }
                  } catch {
                  } finally {
                    isBacklogSyncing = false;
                  }
                });
              }
            }
          }
        }
      }
    } catch {
    }
    await new Promise((r) => setTimeout(r, 200));
  }

  return 0;
}

export async function main(argv: string[] = process.argv.slice(2)): Promise<number> {
  const parsed = parseArgs(argv);

  if (parsed.help || !parsed.command) {
    printHelp();
    return parsed.help ? 0 : 1;
  }

  switch (parsed.command) {
    case 'start':
    case 'run':
      return await handleStart(parsed);

    case 'status':
      return await handleStatus(parsed);

    case 'export':
      if (!parsed.session) {
        process.stderr.write("Error: Missing required option '--session <id>'\n");
        return 1;
      }
      if (!parsed.from) {
        process.stderr.write("Error: Missing or invalid option '--from <device|cloud>'\n");
        return 1;
      }
      if (parsed.from === 'device') {
        return await handleExportDevice(parsed.session, parsed);
      } else {
        return await handleExportCloud(parsed.session, parsed);
      }

    case 'results':
      if (!parsed.session) {
        process.stderr.write("Error: Missing required option '--session <id>'\n");
        return 1;
      }
      return await handleResults(parsed.session, parsed);

    case 'audit':
      if (!parsed.session) {
        process.stderr.write("Error: Missing required option '--session <id>'\n");
        return 1;
      }
      return await handleAudit(parsed.session, parsed);

    default:
      process.stderr.write(`Error: Unknown command '${parsed.command}'\n`);
      printHelp();
      return 1;
  }
}

// Execute when run directly as CLI
const isCli = Boolean(
  process.argv[1] &&
  (path.resolve(process.argv[1]) === fileURLToPath(import.meta.url) ||
   import.meta.url.endsWith(process.argv[1].replace(/\\/g, '/')))
);

if (isCli) {
  main().then((code) => {
    process.exit(code);
  });
}
