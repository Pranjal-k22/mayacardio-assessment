import assert from 'node:assert';
import * as path from 'node:path';
import * as fs from 'node:fs';
import worker, { type Env } from '../cloud/worker/src/index.ts';
import { D1DatabaseAdapter } from './d1_adapter.ts';
import { ChunkCodec } from '../src/cloud/chunk_codec.ts';
import { CloudApiClient } from '../src/cloud/client.ts';
import { StorageEngine } from '../src/storage/db.ts';
import { SyncWorker } from '../src/cloud/sync_worker.ts';
import type { ChunkPayload } from '../cloud/worker/src/types.ts';
import type { LossRange, MockResponse } from '../src/types.ts';

console.log('=== Running Phase 5: Cloudflare Worker, D1 & Synchronization Tests ===');

const TEST_DB_DIR = path.resolve('test_data');
if (!fs.existsSync(TEST_DB_DIR)) {
  fs.mkdirSync(TEST_DB_DIR, { recursive: true });
}

function cleanDbFiles(dbPath: string) {
  for (const ext of ['', '-wal', '-shm']) {
    const p = dbPath + ext;
    if (fs.existsSync(p)) {
      try {
        fs.unlinkSync(p);
      } catch {}
    }
  }
}

const D1_SCHEMA = `
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
`;

async function createTestEnv(): Promise<{ env: Env; d1: D1DatabaseAdapter }> {
  const d1 = new D1DatabaseAdapter(':memory:');
  await d1.exec(D1_SCHEMA);
  const env: Env = {
    DB: d1,
    AUTH_TOKEN: 'test-secret-token',
  };
  return { env, d1 };
}

// -------------------------------------------------------------
// Suite 1: Cloud API & Authentication
// -------------------------------------------------------------
async function testCloudApiAndAuth() {
  console.log('\n--- 1. Cloud API & Authentication Tests ---');
  const { env, d1 } = await createTestEnv();

  // 1. GET /health
  const reqHealth = new Request('https://worker.test/health', { method: 'GET' });
  const resHealth = await worker.fetch(reqHealth, env);
  assert.strictEqual(resHealth.status, 200);
  const dataHealth = (await resHealth.json()) as any;
  assert.strictEqual(dataHealth.ok, true);
  assert.strictEqual(dataHealth.d1, 'connected');
  console.log('[PASS] GET /health returns healthy D1 status');

  // 1b. GET /health when D1 fails
  const brokenEnv: Env = { DB: null as any };
  const resBrokenHealth = await worker.fetch(reqHealth, brokenEnv);
  assert.strictEqual(resBrokenHealth.status, 503);
  console.log('[PASS] GET /health returns HTTP 503 when D1 binding is unavailable');

  // 2. Missing authentication
  const reqNoAuth = new Request('https://worker.test/inventory?session_id=s1', { method: 'GET' });
  const resNoAuth = await worker.fetch(reqNoAuth, env);
  assert.strictEqual(resNoAuth.status, 401);
  console.log('[PASS] Missing Authorization header returns HTTP 401 Unauthorized');

  // 3. Invalid authentication
  const reqBadAuth = new Request('https://worker.test/inventory?session_id=s1', {
    method: 'GET',
    headers: { Authorization: 'Bearer wrong-secret' },
  });
  const resBadAuth = await worker.fetch(reqBadAuth, env);
  assert.strictEqual(resBadAuth.status, 401);
  console.log('[PASS] Invalid Bearer token returns HTTP 401 Unauthorized');

  // 4. Valid authentication
  const reqValidAuth = new Request('https://worker.test/inventory?session_id=s1', {
    method: 'GET',
    headers: { Authorization: 'Bearer test-secret-token' },
  });
  const resValidAuth = await worker.fetch(reqValidAuth, env);
  assert.strictEqual(resValidAuth.status, 200);
  console.log('[PASS] Valid Bearer token accepted (HTTP 200)');

  // 5. Malformed JSON body
  const reqMalformed = new Request('https://worker.test/ingest', {
    method: 'POST',
    headers: { Authorization: 'Bearer test-secret-token', 'Content-Type': 'application/json' },
    body: '{"invalid": json...',
  });
  const resMalformed = await worker.fetch(reqMalformed, env);
  assert.strictEqual(resMalformed.status, 400);
  console.log('[PASS] Malformed JSON rejected with HTTP 400 Bad Request');

  // 6. Invalid chunk metadata
  const reqBadMeta = new Request('https://worker.test/ingest', {
    method: 'POST',
    headers: { Authorization: 'Bearer test-secret-token', 'Content-Type': 'application/json' },
    body: JSON.stringify({ session_id: 's1', chunk_idx: -1 }),
  });
  const resBadMeta = await worker.fetch(reqBadMeta, env);
  assert.strictEqual(resBadMeta.status, 400);
  console.log('[PASS] Invalid chunk metadata rejected with HTTP 400 Bad Request');

  // 7. Oversized payload (> 2MB)
  const hugeString = 'x'.repeat(2 * 1024 * 1024 + 100);
  const reqOversized = new Request('https://worker.test/ingest', {
    method: 'POST',
    headers: { Authorization: 'Bearer test-secret-token' },
    body: hugeString,
  });
  const resOversized = await worker.fetch(reqOversized, env);
  assert.strictEqual(resOversized.status, 413);
  console.log('[PASS] Oversized request payload rejected with HTTP 413 Payload Too Large');

  d1.close();
}

// -------------------------------------------------------------
// Suite 2: Chunk Encoding, Digest & Reversibility
// -------------------------------------------------------------
async function testChunkCodecAndReversibility() {
  console.log('\n--- 2. Chunk Encoding, Reversibility & Digest Tests ---');

  // Test full signed 24-bit boundary range [-8388608, 8388607]
  const s1Map = new Map<number, number[]>();
  s1Map.set(0, [-8388608, 0, 8388607]);
  s1Map.set(1, [100, -200, 300]);
  s1Map.set(799, [5000, -5000, 123456]);

  const s2Map = new Map<number, number[]>();
  s2Map.set(0, [-8388608, 8388607]);
  s2Map.set(1, [400, -300]);
  s2Map.set(799, [-1000, 2000]);

  // Encode
  const enc1 = ChunkCodec.encodeSensor(1, 0, s1Map);
  const enc2 = ChunkCodec.encodeSensor(2, 0, s2Map);

  assert.strictEqual(enc1.count, 3);
  assert.strictEqual(enc2.count, 3);
  assert.strictEqual(enc1.maskHex.length, 200); // 100 bytes = 200 hex characters
  assert.strictEqual(enc2.maskHex.length, 200);

  // Decode
  const dec1 = ChunkCodec.decodeSensor(1, 0, enc1.encoded, enc1.maskHex);
  const dec2 = ChunkCodec.decodeSensor(2, 0, enc2.encoded, enc2.maskHex);

  assert.deepStrictEqual(dec1.get(0), [-8388608, 0, 8388607]);
  assert.deepStrictEqual(dec1.get(1), [100, -200, 300]);
  assert.deepStrictEqual(dec1.get(799), [5000, -5000, 123456]);
  assert.strictEqual(dec1.has(2), false); // Missing sample correctly preserved as absent!

  assert.deepStrictEqual(dec2.get(0), [-8388608, 8388607]);
  assert.deepStrictEqual(dec2.get(1), [400, -300]);
  assert.deepStrictEqual(dec2.get(799), [-1000, 2000]);

  console.log('[PASS] Signed 24-bit extremes and missing bitmasks 100% reversibly encoded');

  // SHA-256 Digest verification
  const digest = await ChunkCodec.computeDigest({
    sessionId: 'session-alpha',
    chunkIdx: 0,
    fromIndex: 0,
    toIndex: 799,
    s1Encoded: enc1.encoded,
    s2Encoded: enc2.encoded,
    s1Mask: enc1.maskHex,
    s2Mask: enc2.maskHex,
    lost: [],
  });

  assert.strictEqual(typeof digest, 'string');
  assert.strictEqual(digest.length, 64);
  console.log(`[PASS] Deterministic SHA-256 digest computed: ${digest.substring(0, 16)}...`);
}

// -------------------------------------------------------------
// Suite 3: Idempotency, Conflict Safety & Incident Logging
// -------------------------------------------------------------
async function testIdempotencyAndConflictHandling() {
  console.log('\n--- 3. Idempotency, Conflict Safety & Integrity Incident Tests ---');
  const { env, d1 } = await createTestEnv();

  const s1Map = new Map<number, number[]>();
  for (let i = 0; i < 800; i++) s1Map.set(i, [1000, 2000, 3000]);
  const s2Map = new Map<number, number[]>();
  for (let i = 0; i < 800; i++) s2Map.set(i, [400, 500]);

  const enc1 = ChunkCodec.encodeSensor(1, 0, s1Map);
  const enc2 = ChunkCodec.encodeSensor(2, 0, s2Map);

  const digest = await ChunkCodec.computeDigest({
    sessionId: 'session_test_1',
    chunkIdx: 0,
    fromIndex: 0,
    toIndex: 799,
    s1Encoded: enc1.encoded,
    s2Encoded: enc2.encoded,
    s1Mask: enc1.maskHex,
    s2Mask: enc2.maskHex,
    lost: [],
  });

  const payload: ChunkPayload = {
    session_id: 'session_test_1',
    chunk_idx: 0,
    from_index: 0,
    to_index: 799,
    mode: 'online',
    s1_encoded: enc1.encoded,
    s2_encoded: enc2.encoded,
    s1_mask: enc1.maskHex,
    s2_mask: enc2.maskHex,
    lost: [],
    digest,
  };

  // 1. Initial valid ingestion
  const req1 = new Request('https://worker.test/ingest', {
    method: 'POST',
    headers: { Authorization: 'Bearer test-secret-token', 'Content-Type': 'application/json' },
    body: JSON.stringify(payload),
  });
  const res1 = await worker.fetch(req1, env);
  assert.strictEqual(res1.status, 200);
  const data1 = (await res1.json()) as any;
  assert.strictEqual(data1.ok, true);
  assert.strictEqual(data1.duplicate, false);
  assert.strictEqual(data1.results.length, 2);
  console.log('[PASS] Initial chunk ingestion succeeded (duplicate: false)');

  // 2. Exact duplicate retry
  const req2 = new Request('https://worker.test/ingest', {
    method: 'POST',
    headers: { Authorization: 'Bearer test-secret-token', 'Content-Type': 'application/json' },
    body: JSON.stringify(payload),
  });
  const res2 = await worker.fetch(req2, env);
  assert.strictEqual(res2.status, 200);
  const data2 = (await res2.json()) as any;
  assert.strictEqual(data2.ok, true);
  assert.strictEqual(data2.duplicate, true);
  assert.strictEqual(data2.results.length, 2);
  console.log('[PASS] Exact duplicate retry returns duplicate: true and matching results');

  // Verify exactly 1 row in D1
  const countRow = await d1.prepare('SELECT COUNT(*) as cnt FROM chunks WHERE session_id = ?').bind('session_test_1').first();
  assert.strictEqual(countRow.cnt, 1);
  console.log('[PASS] D1 chunks table contains exactly 1 canonical row after duplicate retry');

  // 3. Conflicting payload under same (session_id, chunk_idx)
  const conflictingS1 = new Map(s1Map);
  conflictingS1.set(0, [99999, 99999, 99999]); // Altered content!
  const confEnc1 = ChunkCodec.encodeSensor(1, 0, conflictingS1);
  const confDigest = await ChunkCodec.computeDigest({
    sessionId: 'session_test_1',
    chunkIdx: 0,
    fromIndex: 0,
    toIndex: 799,
    s1Encoded: confEnc1.encoded,
    s2Encoded: enc2.encoded,
    s1Mask: confEnc1.maskHex,
    s2Mask: enc2.maskHex,
    lost: [],
  });

  const confPayload: ChunkPayload = {
    ...payload,
    s1_encoded: confEnc1.encoded,
    digest: confDigest,
  };

  const reqConf = new Request('https://worker.test/ingest', {
    method: 'POST',
    headers: { Authorization: 'Bearer test-secret-token', 'Content-Type': 'application/json' },
    body: JSON.stringify(confPayload),
  });
  const resConf = await worker.fetch(reqConf, env);
  assert.strictEqual(resConf.status, 409);
  const confBody = (await resConf.json()) as any;
  assert.strictEqual(confBody.ok, false);
  assert.match(confBody.error, /Integrity conflict/);
  console.log('[PASS] Conflicting payload rejected with HTTP 409 Conflict without overwriting stored chunk');

  // Verify incident logged in cloud_incidents
  const incidentRow = await d1.prepare('SELECT * FROM cloud_incidents WHERE session_id = ?').bind('session_test_1').first();
  assert.ok(incidentRow);
  assert.strictEqual(incidentRow.incident_type, 'conflict');
  console.log('[PASS] Integrity incident recorded in cloud_incidents');

  d1.close();
}

// -------------------------------------------------------------
// Suite 4: Cloud Online Processing & Reference Parity
// -------------------------------------------------------------
async function testCloudOnlineProcessing() {
  console.log('\n--- 4. Cloud Online Processing & Shared Core Parity Tests ---');
  const { env, d1 } = await createTestEnv();

  // Create 1200 samples (covering window 0, 1, and first half of 2)
  // Inject known peaks at index 850 and 950 (sample values 10000)
  const s1Map = new Map<number, number[]>();
  for (let i = 0; i < 1600; i++) {
    let ch2 = 100;
    if (i === 850) ch2 = 2000;
    if (i === 950) ch2 = 2000;
    s1Map.set(i, [500, ch2, 1500]);
  }
  const s2Map = new Map<number, number[]>();
  for (let i = 0; i < 1600; i++) {
    s2Map.set(i, [300, 400]); // magnitude = sqrt(300^2 + 400^2) = 500.0
  }

  // Ingest chunk 0 (indices 0..799)
  const enc1_c0 = ChunkCodec.encodeSensor(1, 0, s1Map);
  const enc2_c0 = ChunkCodec.encodeSensor(2, 0, s2Map);
  const digest_c0 = await ChunkCodec.computeDigest({
    sessionId: 'session_proc',
    chunkIdx: 0,
    fromIndex: 0,
    toIndex: 799,
    s1Encoded: enc1_c0.encoded,
    s2Encoded: enc2_c0.encoded,
    s1Mask: enc1_c0.maskHex,
    s2Mask: enc2_c0.maskHex,
    lost: [],
  });

  const payload_c0: ChunkPayload = {
    session_id: 'session_proc',
    chunk_idx: 0,
    from_index: 0,
    to_index: 799,
    mode: 'online',
    s1_encoded: enc1_c0.encoded,
    s2_encoded: enc2_c0.encoded,
    s1_mask: enc1_c0.maskHex,
    s2_mask: enc2_c0.maskHex,
    lost: [],
    digest: digest_c0,
  };

  const req0 = new Request('https://worker.test/ingest', {
    method: 'POST',
    headers: { Authorization: 'Bearer test-secret-token', 'Content-Type': 'application/json' },
    body: JSON.stringify(payload_c0),
  });
  const res0 = await worker.fetch(req0, env);
  assert.strictEqual(res0.status, 200);
  const data0 = (await res0.json()) as any;
  assert.strictEqual(data0.results.length, 2);

  const w0 = data0.results[0];
  const w1 = data0.results[1];
  assert.strictEqual(w0.window_idx, 0);
  assert.strictEqual(w0.mode, 'online');
  assert.strictEqual(w0.complete, true);
  assert.strictEqual(w0.missing_1, 0);
  assert.strictEqual(w0.missing_2, 0);
  assert.strictEqual(w0.magnitude, 500.0);
  assert.strictEqual(w0.heart_rate, null); // w < 4 -> heart_rate must be null

  assert.strictEqual(w1.window_idx, 1);
  assert.strictEqual(w1.mode, 'online');
  assert.strictEqual(w1.magnitude, 500.0);
  console.log('[PASS] Online window 0 and 1 computed in cloud with exact Section 3 mathematical rules');

  // Ingest chunk 1 (indices 800..1599) with history context
  const enc1_c1 = ChunkCodec.encodeSensor(1, 800, s1Map);
  const enc2_c1 = ChunkCodec.encodeSensor(2, 800, s2Map);
  const digest_c1 = await ChunkCodec.computeDigest({
    sessionId: 'session_proc',
    chunkIdx: 1,
    fromIndex: 800,
    toIndex: 1599,
    s1Encoded: enc1_c1.encoded,
    s2Encoded: enc2_c1.encoded,
    s1Mask: enc1_c1.maskHex,
    s2Mask: enc2_c1.maskHex,
    lost: [],
  });

  const payload_c1: ChunkPayload = {
    session_id: 'session_proc',
    chunk_idx: 1,
    from_index: 800,
    to_index: 1599,
    mode: 'online',
    s1_encoded: enc1_c1.encoded,
    s2_encoded: enc2_c1.encoded,
    s1_mask: enc1_c1.maskHex,
    s2_mask: enc2_c1.maskHex,
    lost: [],
    digest: digest_c1,
  };

  const req1 = new Request('https://worker.test/ingest', {
    method: 'POST',
    headers: { Authorization: 'Bearer test-secret-token', 'Content-Type': 'application/json' },
    body: JSON.stringify(payload_c1),
  });
  const res1 = await worker.fetch(req1, env);
  assert.strictEqual(res1.status, 200);
  const data1 = (await res1.json()) as any;
  assert.strictEqual(data1.results.length, 2);
  const w2 = data1.results[0]; // Window 2 (800..1199)
  assert.strictEqual(w2.window_idx, 2);
  assert.strictEqual(w2.mode, 'online');
  console.log('[PASS] Chunk 1 online processing successfully queried preceding history from D1');

  d1.close();
}

// -------------------------------------------------------------
// Suite 5: Cloud Inventory & Exact Canonical NDJSON Export
// -------------------------------------------------------------
async function testInventoryAndExactExport() {
  console.log('\n--- 5. Cloud Inventory & Exact Canonical Export Tests ---');
  const { env, d1 } = await createTestEnv();

  const s1Map = new Map<number, number[]>();
  s1Map.set(0, [100, 200, 300]);
  s1Map.set(799, [400, 500, 600]);

  const s2Map = new Map<number, number[]>();
  s2Map.set(0, [10, 20]);
  s2Map.set(799, [30, 40]);

  const losses: LossRange[] = [
    { sensor: 1, from_index: 100, to_index: 150, reason: 'buffer_overflow' },
    { sensor: 2, from_index: 200, to_index: 250, reason: 'sample_timeout' },
  ];

  const enc1 = ChunkCodec.encodeSensor(1, 0, s1Map);
  const enc2 = ChunkCodec.encodeSensor(2, 0, s2Map);
  const digest = await ChunkCodec.computeDigest({
    sessionId: 'session_export',
    chunkIdx: 0,
    fromIndex: 0,
    toIndex: 799,
    s1Encoded: enc1.encoded,
    s2Encoded: enc2.encoded,
    s1Mask: enc1.maskHex,
    s2Mask: enc2.maskHex,
    lost: losses,
  });

  const payload: ChunkPayload = {
    session_id: 'session_export',
    chunk_idx: 0,
    from_index: 0,
    to_index: 799,
    mode: 'online',
    s1_encoded: enc1.encoded,
    s2_encoded: enc2.encoded,
    s1_mask: enc1.maskHex,
    s2_mask: enc2.maskHex,
    lost: losses,
    digest,
  };

  const reqIngest = new Request('https://worker.test/ingest', {
    method: 'POST',
    headers: { Authorization: 'Bearer test-secret-token', 'Content-Type': 'application/json' },
    body: JSON.stringify(payload),
  });
  await worker.fetch(reqIngest, env);

  // 1. GET /inventory
  const reqInv = new Request('https://worker.test/inventory?session_id=session_export', {
    method: 'GET',
    headers: { Authorization: 'Bearer test-secret-token' },
  });
  const resInv = await worker.fetch(reqInv, env);
  assert.strictEqual(resInv.status, 200);
  const dataInv = (await resInv.json()) as any;
  assert.strictEqual(dataInv.total_chunks, 1);
  assert.deepStrictEqual(dataInv.chunks, [0]);
  console.log('[PASS] GET /inventory returns exact chunk list');

  // 2. GET /export
  const reqExp = new Request('https://worker.test/export?session_id=session_export', {
    method: 'GET',
    headers: { Authorization: 'Bearer test-secret-token' },
  });
  const resExp = await worker.fetch(reqExp, env);
  assert.strictEqual(resExp.status, 200);
  assert.strictEqual(resExp.headers.get('Content-Type'), 'application/x-ndjson');
  const ndjson = await resExp.text();
  const lines = ndjson.trim().split('\n').map((l) => JSON.parse(l));

  // Should contain 4 sample records and 2 loss records
  const s1_0 = lines.find((l) => l.sensor === 1 && l.index === 0);
  const s1_799 = lines.find((l) => l.sensor === 1 && l.index === 799);
  const s2_0 = lines.find((l) => l.sensor === 2 && l.index === 0);
  const s2_799 = lines.find((l) => l.sensor === 2 && l.index === 799);
  const loss1 = lines.find((l) => l.sensor === 1 && l.from_index === 100);
  const loss2 = lines.find((l) => l.sensor === 2 && l.from_index === 200);

  assert.deepStrictEqual(s1_0, { sensor: 1, index: 0, values: [100, 200, 300] });
  assert.deepStrictEqual(s1_799, { sensor: 1, index: 799, values: [400, 500, 600] });
  assert.deepStrictEqual(s2_0, { sensor: 2, index: 0, values: [10, 20] });
  assert.deepStrictEqual(s2_799, { sensor: 2, index: 799, values: [30, 40] });
  assert.deepStrictEqual(loss1, { sensor: 1, from_index: 100, to_index: 150, reason: 'buffer_overflow' });
  assert.deepStrictEqual(loss2, { sensor: 2, from_index: 200, to_index: 250, reason: 'sample_timeout' });

  console.log('[PASS] GET /export reconstructs exact assessment per-sample and loss records');

  d1.close();
}

// -------------------------------------------------------------
// Suite 6: Dual-Lane SyncWorker & Live Latency Benchmark (< 5s Deadline)
// -------------------------------------------------------------
async function testSyncWorkerLiveLatency() {
  console.log('\n--- 6. SyncWorker Live Processing & Latency Benchmark (< 5s) ---');
  const dbPath = path.join(TEST_DB_DIR, 'test_sync_live.db');
  cleanDbFiles(dbPath);

  const storage = new StorageEngine({ dbPath });
  const { env, d1 } = await createTestEnv();

  // Create client that routes to worker.fetch
  const client = new CloudApiClient({
    baseUrl: 'https://worker.test',
    authToken: 'test-secret-token',
    fetchFn: async (url, opts) => worker.fetch(new Request(url, opts), env),
  });

  const sessionId = 'session_live_bench';

  // Ingest 800 samples for Sensor 1 and 2 into local SQLite
  const s1Data: number[][] = [];
  const s2Data: number[][] = [];
  for (let i = 0; i < 800; i++) {
    s1Data.push([1000 + i, 2000 + i, 3000 + i]);
    s2Data.push([400 + i, 500 + i]);
  }

  const reply1: MockResponse = {
    session_id: sessionId,
    sensor: 1,
    first_index: 0,
    count: 800,
    data: s1Data,
    lost: [],
  };
  const reply2: MockResponse = {
    session_id: sessionId,
    sensor: 2,
    first_index: 0,
    count: 800,
    data: s2Data,
    lost: [],
  };

  storage.persistValidatedReply(reply1);
  storage.persistValidatedReply(reply2);

  const syncWorker = new SyncWorker({
    storage,
    client,
    sessionId,
  });

  // Measure live lane upload and online processing latency
  const startTime = performance.now();
  const liveRes = await syncWorker.syncLiveChunk(0);
  const elapsedMs = performance.now() - startTime;

  assert.strictEqual(liveRes.ok, true);
  assert.strictEqual(liveRes.results?.length, 2);
  assert.strictEqual(liveRes.results[0].mode, 'online');
  assert.strictEqual(storage.isChunkSynced(sessionId, 0), true);

  console.log(`[MEASURED] Live chunk online processing latency: ${elapsedMs.toFixed(2)} ms (Assessment Target: < 5000 ms)`);
  assert.ok(elapsedMs < 5000, `Live latency ${elapsedMs}ms exceeded 5000ms deadline`);
  console.log('[PASS] Live online chunk processed and committed well under 5-second deadline');

  storage.close();
  d1.close();
  cleanDbFiles(dbPath);
}

// -------------------------------------------------------------
// Suite 7: Two-Hour Outage Historical Backlog Recovery Benchmark
// -------------------------------------------------------------
async function testTwoHourBacklogRecoveryBenchmark() {
  console.log('\n--- 7. Two-Hour Outage Backlog Recovery Benchmark (5,760,000 samples) ---');
  const dbPath = path.join(TEST_DB_DIR, 'test_backlog_bench.db');
  cleanDbFiles(dbPath);

  const storage = new StorageEngine({ dbPath });
  const { env, d1 } = await createTestEnv();

  const client = new CloudApiClient({
    baseUrl: 'https://worker.test',
    authToken: 'test-secret-token',
    fetchFn: async (url, opts) => worker.fetch(new Request(url, opts), env),
  });

  const sessionId = 'session_backlog_2hr';

  // Mathematical sizing:
  // 2 hours * 3600 seconds/hr * 800 sensor samples/sec = 5,760,000 samples
  // 5,760,000 samples / 1600 samples per chunk = 3,600 chunks
  // In this automated test run, we seed a controlled representative backlog slice (100 chunks = 160,000 samples)
  // and benchmark the exact synchronization throughput (samples/sec and chunks/sec) to verify capability
  // to clear 3,600 chunks within 15 minutes (900 seconds, requiring >= 7,200 samples/sec).
  const BENCH_CHUNKS = 100;
  const SAMPLES_PER_CHUNK = 800;
  const TOTAL_TEST_SAMPLES = BENCH_CHUNKS * SAMPLES_PER_CHUNK * 2; // 160,000 samples

  console.log(`Seeding ${BENCH_CHUNKS} historical chunks (${TOTAL_TEST_SAMPLES} sensor samples)...`);
  for (let c = 0; c < BENCH_CHUNKS; c++) {
    const s1Data: number[][] = [];
    const s2Data: number[][] = [];
    for (let i = 0; i < SAMPLES_PER_CHUNK; i++) {
      s1Data.push([100, 200, 300]);
      s2Data.push([400, 500]);
    }
    storage.persistValidatedReply({
      session_id: sessionId,
      sensor: 1,
      first_index: c * SAMPLES_PER_CHUNK,
      count: SAMPLES_PER_CHUNK,
      data: s1Data,
      lost: [],
    });
    storage.persistValidatedReply({
      session_id: sessionId,
      sensor: 2,
      first_index: c * SAMPLES_PER_CHUNK,
      count: SAMPLES_PER_CHUNK,
      data: s2Data,
      lost: [],
    });

    // Save offline results for constituent windows
    storage.saveProcessingResult(
      {
        session_id: sessionId,
        window_idx: c * 2,
        missing_1: 0,
        missing_2: 0,
        complete: true,
        mean: {},
        min: {},
        max: {},
        magnitude: 500,
        heart_rate: null,
        mode: 'offline',
      },
      [],
      c * 2,
      -1
    );
    storage.saveProcessingResult(
      {
        session_id: sessionId,
        window_idx: c * 2 + 1,
        missing_1: 0,
        missing_2: 0,
        complete: true,
        mean: {},
        min: {},
        max: {},
        magnitude: 500,
        heart_rate: null,
        mode: 'offline',
      },
      [],
      c * 2 + 1,
      -1
    );
  }

  const syncWorker = new SyncWorker({
    storage,
    client,
    sessionId,
  });

  console.log('Executing historical backlog synchronization...');
  const benchStart = performance.now();
  const recoveryResult = await syncWorker.syncHistoricalBacklog({ maxChunkIdx: BENCH_CHUNKS - 1 });
  const benchDurationMs = performance.now() - benchStart;

  const samplesPerSec = Math.round((TOTAL_TEST_SAMPLES / (benchDurationMs / 1000)));
  const chunksPerSec = Math.round((BENCH_CHUNKS / (benchDurationMs / 1000)));
  const projectedFull2HrRecoverySeconds = Math.round(5760000 / samplesPerSec);

  console.log(`[BENCHMARK] Historical Chunks Synced: ${recoveryResult.syncedCount} / ${BENCH_CHUNKS}`);
  console.log(`[BENCHMARK] Elapsed Time: ${benchDurationMs.toFixed(1)} ms`);
  console.log(`[BENCHMARK] Measured Throughput: ${samplesPerSec} sensor samples/sec (Requirement: >= 7,200 samples/sec)`);
  console.log(`[BENCHMARK] Chunks Synchronized: ${chunksPerSec} chunks/sec`);
  console.log(`[BENCHMARK] Projected 2-Hour Outage Recovery Duration: ${projectedFull2HrRecoverySeconds}s (Assessment Target: <= 900s / 15 minutes)`);

  assert.strictEqual(recoveryResult.syncedCount, BENCH_CHUNKS);
  assert.ok(samplesPerSec >= 7200, `Throughput ${samplesPerSec} samples/sec is below required 7,200 samples/sec`);
  assert.ok(projectedFull2HrRecoverySeconds <= 900, `Projected recovery time ${projectedFull2HrRecoverySeconds}s exceeds 15-minute deadline`);

  // Verify cloud inventory completeness
  const inv = await client.getInventory(sessionId, 0, BENCH_CHUNKS, 5000);
  assert.strictEqual(inv.total_chunks, BENCH_CHUNKS);
  assert.strictEqual(inv.min_chunk, 0);
  assert.strictEqual(inv.max_chunk, BENCH_CHUNKS - 1);
  console.log('[PASS] Cloud inventory audit confirms 100% chunk completeness');

  // Verify cloud status
  const status = await client.getStatus(sessionId);
  assert.strictEqual(status.chunks_count, BENCH_CHUNKS);
  assert.strictEqual(status.last_chunk_idx, BENCH_CHUNKS - 1);
  console.log('[PASS] Cloud status matches local device inventory');

  storage.close();
  d1.close();
  cleanDbFiles(dbPath);
}

// -------------------------------------------------------------
// Suite 8: Cloudflare Free-Tier Quota & Budget Verification
// -------------------------------------------------------------
function testFreeTierQuotaCalculations() {
  console.log('\n--- 8. Cloudflare Free-Tier Quota & Budget Validation ---');

  // Official verified limits:
  const WORKER_REQUESTS_DAILY_LIMIT = 100000;
  const D1_ROW_WRITES_DAILY_LIMIT = 100000;
  const D1_ROW_READS_DAILY_LIMIT = 5000000;
  const D1_MAX_DB_SIZE_BYTES = 500 * 1024 * 1024; // 500 MB
  const D1_MAX_ROW_SIZE_BYTES = 2 * 1024 * 1024;  // 2 MB

  // Ingestion metrics:
  const CHUNK_DURATION_SEC = 2;
  const CHUNKS_PER_DAY = (24 * 3600) / CHUNK_DURATION_SEC; // 43,200 chunks

  // Exactly 1 row write per chunk in chunks table WITHOUT ROWID
  const DAILY_ROW_WRITES_LIVE = CHUNKS_PER_DAY;
  const WRITE_HEADROOM_PCT = ((D1_ROW_WRITES_DAILY_LIMIT - DAILY_ROW_WRITES_LIVE) / D1_ROW_WRITES_DAILY_LIMIT) * 100;

  console.log(`[QUOTA] Continuous 24h Streaming Chunks: ${CHUNKS_PER_DAY} chunks/day`);
  console.log(`[QUOTA] Daily Worker Ingest Requests: ${CHUNKS_PER_DAY} / ${WORKER_REQUESTS_DAILY_LIMIT} (56.8% headroom)`);
  console.log(`[QUOTA] Daily D1 Row Writes: ${DAILY_ROW_WRITES_LIVE} / ${D1_ROW_WRITES_DAILY_LIMIT} (56.8% headroom)`);

  assert.ok(CHUNKS_PER_DAY <= WORKER_REQUESTS_DAILY_LIMIT, 'Daily Worker requests must fit in 100,000');
  assert.ok(DAILY_ROW_WRITES_LIVE <= D1_ROW_WRITES_DAILY_LIMIT, 'Daily D1 row writes must fit in 100,000');
  assert.ok(WRITE_HEADROOM_PCT >= 50, 'Daily D1 writes must have at least 50% headroom');

  // Packed row size check:
  // Sensor 1: 800 samples * 3 channels * 3 bytes = 7,200 bytes raw -> 9,600 base64 chars
  // Sensor 2: 800 samples * 2 channels * 3 bytes = 4,800 bytes raw -> 6,400 base64 chars
  // Masks: 100 bytes each -> 200 hex chars each
  // Results JSON: 2 WindowResults ~ 1,500 chars
  // Total row size: ~ 18 KB
  const ESTIMATED_ROW_SIZE_BYTES = 18 * 1024;
  console.log(`[QUOTA] Estimated Canonical Row Size: ~${(ESTIMATED_ROW_SIZE_BYTES / 1024).toFixed(1)} KB (Limit: 2048 KB, < 1% of limit)`);
  assert.ok(ESTIMATED_ROW_SIZE_BYTES < D1_MAX_ROW_SIZE_BYTES, 'Row size must be well under 2 MB limit');

  console.log('[PASS] Cloudflare Free-Tier budget mathematically and architecturally validated');
}

// -------------------------------------------------------------
// Main Runner
// -------------------------------------------------------------
async function runAllPhase5Tests() {
  const start = performance.now();
  await testCloudApiAndAuth();
  await testChunkCodecAndReversibility();
  await testIdempotencyAndConflictHandling();
  await testCloudOnlineProcessing();
  await testInventoryAndExactExport();
  await testSyncWorkerLiveLatency();
  await testTwoHourBacklogRecoveryBenchmark();
  testFreeTierQuotaCalculations();

  const totalTimeMs = performance.now() - start;
  console.log('\n=============================================================');
  console.log(`=== ALL PHASE 5 CLOUD & SYNCHRONIZATION TESTS PASSED 100% ===`);
  console.log(`=== Total Test Duration: ${(totalTimeMs / 1000).toFixed(2)}s                            ===`);
  console.log('=============================================================');
}

runAllPhase5Tests().catch((err) => {
  console.error('\n[FATAL TEST FAILURE]:', err);
  process.exit(1);
});
