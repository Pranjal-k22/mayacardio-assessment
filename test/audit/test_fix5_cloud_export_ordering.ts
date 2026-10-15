import * as http from 'node:http';
import * as fs from 'node:fs';
import * as path from 'node:path';
import assert from 'node:assert';
import { StorageEngine } from '../../src/storage/db.ts';
import { SyncWorker } from '../../src/cloud/sync_worker.ts';
import { CloudApiClient } from '../../src/cloud/client.ts';
import worker, { type Env } from '../../cloud/worker/src/index.ts';
import { D1DatabaseAdapter } from '../../test/d1_adapter.ts';
import { verifyRampExport } from './independent_verifier.ts';
import type { SensorId, MockResponse } from '../../src/types.ts';
import { crc32 } from 'node:zlib';

const CLOUD_PORT = 8789;
const TMP_DIR = path.resolve('scratch', 'fix5_test');
if (!fs.existsSync(TMP_DIR)) {
  fs.mkdirSync(TMP_DIR, { recursive: true });
}

const DB_PATH = path.join(TMP_DIR, 'fix5_pipeline.db');
const DEVICE_EXPORT_PATH = path.join(TMP_DIR, 'fix5_device_export.ndjson');
const CLOUD_EXPORT_PATH = path.join(TMP_DIR, 'fix5_cloud_export.ndjson');

function cleanFiles() {
  for (const ext of ['', '-wal', '-shm']) {
    const f = DB_PATH + ext;
    if (fs.existsSync(f)) fs.unlinkSync(f);
  }
  if (fs.existsSync(DEVICE_EXPORT_PATH)) fs.unlinkSync(DEVICE_EXPORT_PATH);
  if (fs.existsSync(CLOUD_EXPORT_PATH)) fs.unlinkSync(CLOUD_EXPORT_PATH);
}

function createRampBatch(
  sessionId: string,
  sensor: SensorId,
  firstIndex: number,
  count: number,
  lost: Array<{ from_index: number; to_index: number | null; reason: string }> = []
): MockResponse {
  const data: number[][] = [];
  const MOD = 8388608;

  for (let i = 0; i < count; i++) {
    const idx = firstIndex + i;
    if (sensor === 1) {
      data.push([
        (3 * idx + 0) % MOD,
        (3 * idx + 1) % MOD,
        (3 * idx + 2) % MOD,
      ]);
    } else {
      data.push([
        (2 * idx + 0) % MOD,
        (2 * idx + 1) % MOD,
      ]);
    }
  }

  const dataSlice = Buffer.from(JSON.stringify(data));
  const crc = crc32(dataSlice).toString(16).padStart(8, '0');

  return {
    session_id: sessionId,
    sensor,
    first_index: firstIndex,
    count,
    lost,
    session_end: false,
    crc32: crc,
    data,
  };
}

async function startMockCloudServer(): Promise<{ server: http.Server; d1: D1DatabaseAdapter }> {
  const d1 = new D1DatabaseAdapter(':memory:');
  await d1.exec(`
    CREATE TABLE IF NOT EXISTS chunks (
      session_id TEXT NOT NULL,
      chunk_idx INTEGER NOT NULL,
      from_index INTEGER NOT NULL,
      to_index INTEGER NOT NULL,
      mode TEXT NOT NULL,
      s1_encoded TEXT NOT NULL,
      s2_encoded TEXT NOT NULL,
      s1_mask TEXT NOT NULL,
      s2_mask TEXT NOT NULL,
      lost_json TEXT NOT NULL,
      results_json TEXT NOT NULL,
      digest TEXT NOT NULL,
      created_at INTEGER NOT NULL,
      PRIMARY KEY (session_id, chunk_idx)
    ) WITHOUT ROWID;

    CREATE TABLE IF NOT EXISTS cloud_incidents (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      session_id TEXT NOT NULL,
      chunk_idx INTEGER NOT NULL,
      incident_type TEXT NOT NULL,
      details TEXT NOT NULL,
      created_at INTEGER NOT NULL
    );
  `);

  const env: Env = {
    DB: d1,
    AUTH_TOKEN: 'fix5-test-token',
  };

  const server = http.createServer(async (req, res) => {
    const url = `http://127.0.0.1:${CLOUD_PORT}${req.url}`;
    const chunks: Buffer[] = [];
    for await (const chunk of req) {
      chunks.push(chunk);
    }
    const body = chunks.length > 0 ? Buffer.concat(chunks) : undefined;

    const webReq = new Request(url, {
      method: req.method,
      headers: req.headers as any,
      body: req.method !== 'GET' && req.method !== 'HEAD' ? body : undefined,
    });

    try {
      const webRes = await worker.fetch(webReq, env);
      res.statusCode = webRes.status;
      webRes.headers.forEach((val, key) => {
        res.setHeader(key, val);
      });
      const resBody = await webRes.arrayBuffer();
      res.end(Buffer.from(resBody));
    } catch (err: any) {
      res.statusCode = 500;
      res.end(err.message || 'Worker Internal Error');
    }
  });

  await new Promise<void>((resolve) => {
    server.listen(CLOUD_PORT, '127.0.0.1', () => resolve());
  });

  return { server, d1 };
}

async function runFix5AcceptanceTest() {
  console.log('=== FIX 5 ACCEPTANCE TEST: CLOUD EXPORT ORDERING & PARITY ===');
  cleanFiles();

  const { server, d1 } = await startMockCloudServer();
  const sessionId = 'fix5_cloud_export_sess';
  const storage = new StorageEngine({ dbPath: DB_PATH });

  try {
    // 1. Ingest 2 full chunks (1600 sample positions) with multiple loss records
    console.log('1. Seeding local database with samples and losses...');
    // Batch 1: Sensor 1 (0..399), Sensor 2 (0..199 and loss 200..249, 250..399)
    storage.persistValidatedReply(createRampBatch(sessionId, 1, 0, 400));
    storage.persistValidatedReply(createRampBatch(sessionId, 2, 0, 200));
    storage.persistValidatedReply(createRampBatch(sessionId, 2, 250, 150, [
      { from_index: 200, to_index: 249, reason: 'buffer_overflow' }
    ]));

    // Batch 2: Sensor 1 (400..799), Sensor 2 (400..799)
    storage.persistValidatedReply(createRampBatch(sessionId, 1, 400, 400));
    storage.persistValidatedReply(createRampBatch(sessionId, 2, 400, 400));

    // Batch 3: Unreported gap in Sensor 1 (skipping 800..849, receiving 850..1199)
    storage.persistValidatedReply(createRampBatch(sessionId, 1, 850, 350));
    storage.persistValidatedReply(createRampBatch(sessionId, 2, 800, 400));

    // Batch 4: Samples 1200..1499 (count 300)
    storage.persistValidatedReply(createRampBatch(sessionId, 1, 1200, 300));
    storage.persistValidatedReply(createRampBatch(sessionId, 2, 1200, 300));

    // Session restart tail lost within chunk 1 bounds (at index 1500..null)
    storage.recordLoss(sessionId, 1, 1500, null, 'session_restart_tail_lost', 'pipeline');
    storage.recordLoss(sessionId, 2, 1500, null, 'session_restart_tail_lost', 'pipeline');

    console.log('   [PASS] Local database seeded with sample rows and loss records within chunk bounds');

    // 2. Synchronize to cloud
    console.log('2. Synchronizing backlog to cloud worker...');
    const client = new CloudApiClient({
      baseUrl: `http://127.0.0.1:${CLOUD_PORT}`,
      authToken: 'fix5-test-token',
    });

    const syncWorker = new SyncWorker({
      storage,
      client,
      sessionId,
      batchSizeChunks: 50,
    });

    const syncResult = await syncWorker.syncHistoricalBacklog();
    console.log(`   [PASS] Catch-up completed: ${syncResult.syncedCount} chunks synchronized to cloud`);
    assert(syncResult.syncedCount >= 2, 'Must have synced at least 2 full chunks');

    // 3. Export from device
    console.log('3. Generating export from device...');
    const devFd = fs.openSync(DEVICE_EXPORT_PATH, 'w');
    const maxIdxRow = (storage as any).db
      .prepare('SELECT MAX(sample_idx) as m FROM samples WHERE session_id = ?')
      .get(sessionId) as any;
    const maxStoredIdx = maxIdxRow?.m !== null && maxIdxRow?.m !== undefined ? maxIdxRow.m : -1;

    let fromIdx = 0;
    const step = 500;
    while (fromIdx <= maxStoredIdx) {
      const toIdx = fromIdx + step - 1;
      const s1 = storage.getSamplesRange(sessionId, 1, fromIdx, toIdx);
      const s2 = storage.getSamplesRange(sessionId, 2, fromIdx, toIdx);

      const indices = Array.from(new Set([...s1.map((s) => s.index), ...s2.map((s) => s.index)])).sort((a, b) => a - b);
      const s1Map = new Map(s1.map((s) => [s.index, s]));
      const s2Map = new Map(s2.map((s) => [s.index, s]));

      for (const idx of indices) {
        const samp1 = s1Map.get(idx);
        if (samp1) {
          fs.writeSync(devFd, JSON.stringify({ sensor: 1, index: samp1.index, values: samp1.values }) + '\n');
        }
        const samp2 = s2Map.get(idx);
        if (samp2) {
          fs.writeSync(devFd, JSON.stringify({ sensor: 2, index: samp2.index, values: samp2.values }) + '\n');
        }
      }
      fromIdx += step;
    }

    const deviceLosses = storage.getExportLosses(sessionId);
    for (const l of deviceLosses) {
      fs.writeSync(
        devFd,
        JSON.stringify({
          sensor: l.sensor,
          from_index: l.from_index,
          to_index: l.to_index,
          reason: l.reason,
        }) + '\n'
      );
    }
    fs.closeSync(devFd);

    // 4. Export from cloud using the same streaming and loss-ordering logic as bin/pipeline.ts
    console.log('4. Generating export from cloud...');
    const cloudFd = fs.openSync(CLOUD_EXPORT_PATH, 'w');
    const inv = await client.getInventory(sessionId, 0, 1);
    assert(inv.total_chunks > 0, 'Cloud inventory must contain chunks');

    const pageChunkLimit = 50;
    const collectedCloudLosses = new Map<string, any>();

    for (let c = 0; c < inv.total_chunks; c += pageChunkLimit) {
      const fromIndex = c * 800;
      const toIndex = (c + pageChunkLimit) * 800 - 1;
      const pageNdjson = await client.export(sessionId, fromIndex, toIndex);

      if (!pageNdjson || pageNdjson.trim() === '') continue;

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
          // Sample line: stream immediately
          fs.writeSync(cloudFd, line + '\n');
        } else if (record.reason !== undefined) {
          // Loss line: collect and print at end
          const key = `${record.sensor}:${record.from_index}:${record.to_index ?? 'null'}:${record.reason}`;
          if (!collectedCloudLosses.has(key)) {
            collectedCloudLosses.set(key, record);
          }
        }
      }
    }

    const sortedLosses = Array.from(collectedCloudLosses.values()).sort((a, b) => {
      if (a.sensor !== b.sensor) return a.sensor - b.sensor;
      return a.from_index - b.from_index;
    });

    for (const l of sortedLosses) {
      fs.writeSync(cloudFd, JSON.stringify(l) + '\n');
    }
    fs.closeSync(cloudFd);

    // 5. Line-for-line parity comparison
    console.log('5. Comparing Device Export and Cloud Export line-for-line...');
    const devLines = fs.readFileSync(DEVICE_EXPORT_PATH, 'utf-8').trim().split('\n');
    const cloudLines = fs.readFileSync(CLOUD_EXPORT_PATH, 'utf-8').trim().split('\n');

    console.log(`   Device Export Lines: ${devLines.length}`);
    console.log(`   Cloud Export Lines:  ${cloudLines.length}`);

    assert.strictEqual(devLines.length, cloudLines.length, 'Total export line counts must match exactly');

    let firstLossDevIdx = -1;
    let firstLossCloudIdx = -1;

    for (let i = 0; i < devLines.length; i++) {
      const dObj = JSON.parse(devLines[i]);
      const cObj = JSON.parse(cloudLines[i]);

      if (dObj.reason !== undefined && firstLossDevIdx === -1) {
        firstLossDevIdx = i;
      }
      if (cObj.reason !== undefined && firstLossCloudIdx === -1) {
        firstLossCloudIdx = i;
      }

      assert.deepStrictEqual(
        cObj,
        dObj,
        `Line ${i} must match exactly between cloud export and device export`
      );
    }

    assert(firstLossDevIdx > 0, 'Must have loss lines in device export');
    assert.strictEqual(
      firstLossDevIdx,
      firstLossCloudIdx,
      'Loss lines must start at the exact same line index (losses last)'
    );
    console.log(`   [PASS] 100% Line-for-line exact parity verified across all ${devLines.length} lines`);
    console.log(`   [PASS] All samples strictly precede losses (losses start at line ${firstLossDevIdx})`);

    // 6. Verify both exports with independent Ramp verifier
    console.log('6. Running independent Ramp verifier on both export files...');
    const devReport = await verifyRampExport(DEVICE_EXPORT_PATH);
    const cloudReport = await verifyRampExport(CLOUD_EXPORT_PATH);

    assert(devReport.pass, `Device export verifier must PASS: ${JSON.stringify(devReport.errors)}`);
    assert(cloudReport.pass, `Cloud export verifier must PASS: ${JSON.stringify(cloudReport.errors)}`);
    assert.strictEqual(devReport.unaccountedGaps.length, 0);
    assert.strictEqual(cloudReport.unaccountedGaps.length, 0);
    assert.strictEqual(devReport.orderingViolation, false);
    assert.strictEqual(cloudReport.orderingViolation, false);

    console.log('\n=== FIX 5 ACCEPTANCE TEST PASSED 100% ===');
  } finally {
    storage.close();
    server.close();
    cleanFiles();
  }
}

runFix5AcceptanceTest().catch((err) => {
  console.error('\n[FATAL FIX 5 ERROR]:', err);
  process.exit(1);
});
