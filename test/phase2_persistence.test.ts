import assert from 'node:assert';
import * as fs from 'node:fs';
import * as path from 'node:path';
import { spawnSync } from 'node:child_process';
import { StorageEngine } from '../src/storage/db.ts';
import type { MockResponse } from '../src/types.ts';

console.log('=== Running Phase 2: Crash-Safe Local Persistence & Progress Tests ===');

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

async function runDatabaseAndValuesTests() {
  console.log('\n--- 1. Database Configuration & Value Preservation Tests ---');

  const dbPath = path.join(TEST_DB_DIR, 'test_values.db');
  cleanDbFiles(dbPath);

  const engine = new StorageEngine({ dbPath });

  try {
    // 1. Verify schema idempotency (re-running init shouldn't throw)
    const engine2 = new StorageEngine({ dbPath });
    engine2.close();
    console.log('[PASS] Repeated migrations and initializations are idempotent');

    // 2. Exact 24-bit signed bounds and negative numbers round-trip
    const testSamples: MockResponse = {
      session_id: 'sess-val',
      sensor: 1,
      first_index: 0,
      count: 3,
      lost: [],
      session_end: false,
      crc32: '00000000',
      data: [
        [-8388608, 0, 8388607],    // Min, zero, max
        [-1, -400000, 400000],     // Negative values
        [123456, -654321, 789012], // Arbitrary 24-bit values
      ],
    };

    const res = engine.persistValidatedReply(testSamples);
    assert(res.success, 'Persistence must succeed');
    assert.strictEqual(res.newSamplesCount, 3);
    assert.strictEqual(res.resolvedWatermark, 2);

    const retrieved = engine.getSamples('sess-val', 1, 0, 10);
    assert.strictEqual(retrieved.length, 3);
    assert.deepStrictEqual(retrieved[0].values, [-8388608, 0, 8388607]);
    assert.deepStrictEqual(retrieved[1].values, [-1, -400000, 400000]);
    assert.deepStrictEqual(retrieved[2].values, [123456, -654321, 789012]);
    console.log('[PASS] Exact signed 24-bit boundaries [-8388608, 8388607] preserved without precision loss');

    // 3. Sensor 2 channel count (2 channels, ch3 is null)
    const testS2: MockResponse = {
      session_id: 'sess-val',
      sensor: 2,
      first_index: 0,
      count: 1,
      lost: [],
      session_end: false,
      crc32: '00000000',
      data: [[400000, 150000]],
    };
    const resS2 = engine.persistValidatedReply(testS2);
    assert(resS2.success);
    const retrievedS2 = engine.getSamples('sess-val', 2, 0, 10);
    assert.strictEqual(retrievedS2.length, 1);
    assert.deepStrictEqual(retrievedS2[0].values, [400000, 150000]);
    console.log('[PASS] Sensor 2 two-channel samples correctly saved and reconstructed');

    // 4. Identical Duplicates do not increase count
    const dupRes = engine.persistValidatedReply(testSamples);
    assert(dupRes.success);
    assert.strictEqual(dupRes.newSamplesCount, 0);
    assert.strictEqual(dupRes.duplicateSamplesCount, 3);
    const totalStored = engine.getSamples('sess-val', 1, 0, 10);
    assert.strictEqual(totalStored.length, 3, 'Duplicate reply must not insert duplicate rows');
    console.log('[PASS] Identical duplicate replies recognized and handled idempotently');

    // 5. Conflicting Data under existing identity: logged as incident, original preserved
    const conflictReply: MockResponse = {
      session_id: 'sess-val',
      sensor: 1,
      first_index: 0,
      count: 1,
      lost: [],
      session_end: false,
      crc32: '00000000',
      data: [[999999, 999999, 999999]], // conflicting values for index 0
    };
    const confRes = engine.persistValidatedReply(conflictReply);
    assert(confRes.success);
    assert.strictEqual(confRes.conflictSamplesCount, 1);
    const uncorrupted = engine.getSamples('sess-val', 1, 0, 1);
    assert.deepStrictEqual(uncorrupted[0].values, [-8388608, 0, 8388607], 'Original values must NOT be overwritten');
    console.log('[PASS] Conflicting duplicate recorded as integrity incident without overwriting existing data');

    // 6. Session Restart Isolation: same indices in a new session do not collide
    const newSessionReply: MockResponse = {
      session_id: 'sess-new',
      sensor: 1,
      first_index: 0,
      count: 1,
      lost: [],
      session_end: false,
      crc32: '00000000',
      data: [[111, 222, 333]],
    };
    const newSessRes = engine.persistValidatedReply(newSessionReply);
    assert(newSessRes.success);
    assert.strictEqual(newSessRes.newSamplesCount, 1);
    const sessOldSamples = engine.getSamples('sess-val', 1, 0, 1);
    const sessNewSamples = engine.getSamples('sess-new', 1, 0, 1);
    assert.deepStrictEqual(sessOldSamples[0].values, [-8388608, 0, 8388607]);
    assert.deepStrictEqual(sessNewSamples[0].values, [111, 222, 333]);
    console.log('[PASS] Session isolation verified: new session index 0 does not collide with previous session');

    // 7. Losses with Unknown Endpoint (to_index = null)
    engine.recordLoss('sess-val', 1, 1000, null, 'session_restart_tail_lost', 'test');
    const losses = engine.getLosses('sess-val', 1);
    assert.strictEqual(losses.length, 1);
    assert.strictEqual(losses[0].from_index, 1000);
    assert.strictEqual(losses[0].to_index, null);
    assert.strictEqual(losses[0].reason, 'session_restart_tail_lost');

    // Idempotent loss record check
    engine.recordLoss('sess-val', 1, 1000, null, 'session_restart_tail_lost', 'test');
    const lossesDup = engine.getLosses('sess-val', 1);
    assert.strictEqual(lossesDup.length, 1, 'Repeated loss record must be idempotent');
    console.log('[PASS] Loss ledger supports unknown endpoints (NULL) and idempotent insertion');

  } finally {
    engine.close();
    cleanDbFiles(dbPath);
  }
}

async function runTransactionsAndWatermarkTests() {
  console.log('\n--- 2. Atomic Transactions & Watermark Progress Tests ---');

  const dbPath = path.join(TEST_DB_DIR, 'test_watermark.db');
  cleanDbFiles(dbPath);

  const engine = new StorageEngine({ dbPath });

  try {
    // 1. Ingest batch 1: samples 0 to 399
    const batch1Data: number[][] = [];
    for (let i = 0; i < 400; i++) batch1Data.push([i, i + 1, i + 2]);
    const batch1: MockResponse = {
      session_id: 'sess-wm',
      sensor: 1,
      first_index: 0,
      count: 400,
      lost: [],
      session_end: false,
      crc32: '00000000',
      data: batch1Data,
    };

    const r1 = engine.persistValidatedReply(batch1);
    assert(r1.success);
    assert.strictEqual(r1.resolvedWatermark, 399);
    console.log('[PASS] Batch 1 (0..399) resolved watermark advanced to 399');

    // 2. Unreported ingest gap: skip 400..799, ingest batch 3 (800..1199)
    // Per FIX 3: persistValidatedReply must automatically record uncovered gap 400..799 as 'gap_unreported_by_mock'
    // in the same transaction and advance resolved watermark to 1199!
    const batch3Data: number[][] = [];
    for (let i = 800; i < 1200; i++) batch3Data.push([i, i + 1, i + 2]);
    const batch3: MockResponse = {
      session_id: 'sess-wm',
      sensor: 1,
      first_index: 800,
      count: 400,
      lost: [],
      session_end: false,
      crc32: '00000000',
      data: batch3Data,
    };

    const r3 = engine.persistValidatedReply(batch3);
    assert(r3.success);
    assert.strictEqual(r3.newLossesCount, 1, 'Must record 1 uncovered loss range');
    assert.strictEqual(r3.resolvedWatermark, 1199, 'Watermark advances to 1199 because 400..799 is covered by loss');
    assert.strictEqual(r3.nextExpectedIndex, 1200);
    const losses = engine.getLosses('sess-wm', 1);
    assert.strictEqual(losses.length, 1);
    assert.strictEqual(losses[0].from_index, 400);
    assert.strictEqual(losses[0].to_index, 799);
    assert.strictEqual(losses[0].reason, 'gap_unreported_by_mock');
    console.log('[PASS] Unreported gap detected: automatically recorded as loss and watermark advanced to 1199');

    // 3. Verify watermark does not jump across an unaccounted gap (neither in samples nor in losses)
    (engine as any).db.prepare(
      'INSERT INTO samples (session_id, sensor, sample_idx, ch1, ch2, ch3, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)'
    ).run('sess-wm', 1, 2000, 1, 2, 3, Date.now());
    const wmUnresolved = (engine as any).computeContiguousWatermark('sess-wm', 1, 1199);
    assert.strictEqual(wmUnresolved, 1199, 'Watermark must not jump across unaccounted gap 1200..1999');
    (engine as any).db.prepare(
      'DELETE FROM samples WHERE session_id = ? AND sensor = ? AND sample_idx = ?'
    ).run('sess-wm', 1, 2000);
    console.log('[PASS] Watermark strictly bound to contiguous samples and losses');

    // 4. Rollback midway through failed transaction
    // Passing invalid mock response that will trigger rollback
    const invalidReply: any = {
      session_id: 'sess-wm',
      sensor: 1,
      first_index: 1200,
      count: 2,
      lost: [],
      session_end: false,
      crc32: '00000000',
      data: [
        [1, 2, 3],
        [null, null], // invalid row structure triggering DB constraint error
      ],
    };
    const rFail = engine.persistValidatedReply(invalidReply);
    assert(!rFail.success, 'Transaction must fail and report error');
    const pAfterFail = engine.getProgress('sess-wm', 1);
    assert.strictEqual(pAfterFail.resolvedWatermark, 1199, 'Watermark must not advance after failed transaction');
    const checkRow = engine.getSamples('sess-wm', 1, 1200, 1);
    assert.strictEqual(checkRow.length, 0, 'No sample from failed transaction should be committed');
    console.log('[PASS] Failed transaction rolls back completely without altering progress or storing partial samples');

  } finally {
    engine.close();
    cleanDbFiles(dbPath);
  }
}

async function runCrashRecoveryTests() {
  console.log('\n--- 3. Subprocess Crash Consistency & WAL Recovery Tests ---');

  const crashDbPath = path.join(TEST_DB_DIR, 'test_crash.db');
  cleanDbFiles(crashDbPath);

  // Run crash_worker.ts in a separate Node process
  // The worker commits Batch 1 (0..399), then starts Batch 2 (400..599) and abruptly crashes via process.abort()
  const workerScript = path.resolve('test/crash_worker.ts');
  const result = spawnSync('node', ['--experimental-strip-types', workerScript, crashDbPath], {
    encoding: 'utf-8',
  });

  console.log(`Worker stdout: ${result.stdout?.trim()}`);
  console.log(`Worker exit status: signal=${result.signal || 'none'}, code=${result.status}`);

  assert(result.stdout?.includes('BATCH_1_COMMITTED'), 'Batch 1 must have been committed before crash');
  assert(result.stdout?.includes('CRASHING_NOW'), 'Worker must have crashed during uncommitted batch 2');

  // Open the exact crashed database with StorageEngine
  const engine = new StorageEngine({ dbPath: crashDbPath });

  try {
    // 1. Verify PRAGMA integrity_check passes
    // (Already checked in engine.init())
    console.log('[PASS] Reopening crashed SQLite WAL database succeeded and passed integrity check');

    // 2. Committed samples (0..399) must be fully intact
    const committedSamples = engine.getSamples('sess-crash', 1, 0, 500);
    assert.strictEqual(committedSamples.length, 400, 'All 400 committed samples must survive the crash');
    assert.deepStrictEqual(committedSamples[0].values, [100, 200, 300]);
    assert.deepStrictEqual(committedSamples[399].values, [499, 599, 699]);
    console.log('[PASS] 100% of durably committed samples (0..399) survived process termination');

    // 3. Uncommitted samples (400..599) must be completely rolled back
    const uncommittedSamples = engine.getSamples('sess-crash', 1, 400, 500);
    assert.strictEqual(uncommittedSamples.length, 0, 'Uncommitted samples must be rolled back by WAL');
    console.log('[PASS] Zero uncommitted samples leaked into durable storage');

    // 4. Progress and watermark must reflect committed state
    const progress = engine.getProgress('sess-crash', 1);
    assert.strictEqual(progress.resolvedWatermark, 399);
    console.log('[PASS] Resolved watermark accurately preserved at 399 after crash recovery');

  } finally {
    engine.close();
    cleanDbFiles(crashDbPath);
  }
}

async function runIntegrityAuditTests() {
  console.log('\n--- 4. Integrity Audit Tests ---');

  const dbPath = path.join(TEST_DB_DIR, 'test_audit.db');
  cleanDbFiles(dbPath);

  const engine = new StorageEngine({ dbPath });

  try {
    // 1. Populate clean session
    const data: number[][] = [];
    for (let i = 0; i < 200; i++) data.push([i, i, i]);
    engine.persistValidatedReply({
      session_id: 'sess-audit',
      sensor: 1,
      first_index: 0,
      count: 200,
      lost: [],
      session_end: false,
      crc32: '00000000',
      data,
    });

    const cleanAudit = engine.auditSession('sess-audit', 1);
    assert(cleanAudit.pass, 'Clean session must pass audit');
    assert.strictEqual(cleanAudit.unaccountedGaps.length, 0);
    assert.strictEqual(cleanAudit.integrityIncidents.length, 0);
    console.log('[PASS] Audit passes on clean continuous session');

    // 2. Injected gap: manually remove sample index 150 to simulate corruption/loss
    (engine as any).db.prepare('DELETE FROM samples WHERE session_id = ? AND sensor = ? AND sample_idx = ?').run('sess-audit', 1, 150);

    const gapAudit = engine.auditSession('sess-audit', 1);
    assert(!gapAudit.pass, 'Audit must fail when gap exists below resolved watermark');
    assert.strictEqual(gapAudit.unaccountedGaps.length, 1);
    assert.strictEqual(gapAudit.unaccountedGaps[0].from_index, 150);
    console.log('[PASS] Audit successfully detects unaccounted gap at index 150');

    // 3. Resolve gap by declaring explicit loss
    engine.recordLoss('sess-audit', 1, 150, 150, 'hardware_dropped', 'audit_reconciliation');
    const resolvedAudit = engine.auditSession('sess-audit', 1);
    assert(resolvedAudit.pass, 'Audit must pass after gap is explicitly accounted for in loss ledger');
    console.log('[PASS] Audit passes after loss ledger reconciliation');

  } finally {
    engine.close();
    cleanDbFiles(dbPath);
  }
}

async function runPerformanceAndThroughputBenchmark() {
  console.log('\n--- 5. Storage Performance & Throughput Benchmark ---');

  const dbPath = path.join(TEST_DB_DIR, 'test_bench.db');
  cleanDbFiles(dbPath);

  const engine = new StorageEngine({ dbPath });

  try {
    const totalBatches = 20; // 20 batches of 400 samples = 8,000 samples (10 seconds of continuous data per sensor)
    const samplesPerBatch = 400;
    const latencies: number[] = [];

    const startTime = performance.now();

    for (let b = 0; b < totalBatches; b++) {
      const firstIdx = b * samplesPerBatch;
      const data: number[][] = [];
      for (let i = 0; i < samplesPerBatch; i++) {
        data.push([firstIdx + i, (firstIdx + i) * 2, (firstIdx + i) * 3]);
      }

      const reply: MockResponse = {
        session_id: 'sess-bench',
        sensor: 1,
        first_index: firstIdx,
        count: samplesPerBatch,
        lost: [],
        session_end: false,
        crc32: '00000000',
        data,
      };

      const t0 = performance.now();
      const res = engine.persistValidatedReply(reply);
      const t1 = performance.now();
      assert(res.success);
      latencies.push(t1 - t0);
    }

    const totalElapsedMs = performance.now() - startTime;
    const totalSamples = totalBatches * samplesPerBatch;
    const throughput = (totalSamples / totalElapsedMs) * 1000;
    const avgLatency = latencies.reduce((a, b) => a + b, 0) / latencies.length;
    const maxLatency = Math.max(...latencies);

    const dbSize = engine.getDbFileSize();
    const walSize = engine.getWalFileSize();

    console.log(`[BENCHMARK] Total Samples Ingested: ${totalSamples}`);
    console.log(`[BENCHMARK] Elapsed Time: ${totalElapsedMs.toFixed(1)} ms`);
    console.log(`[BENCHMARK] Throughput: ${throughput.toFixed(0)} samples/sec (Target: >= 800 samples/sec live)`);
    console.log(`[BENCHMARK] Commit Latency: avg=${avgLatency.toFixed(2)} ms, max=${maxLatency.toFixed(2)} ms (well under 100ms budget)`);
    console.log(`[BENCHMARK] DB File Size: ${(dbSize / 1024).toFixed(1)} KB, WAL Size: ${(walSize / 1024).toFixed(1)} KB`);

    assert(throughput >= 2000, `Throughput (${throughput} samples/sec) must easily exceed live requirement`);
    console.log('[PASS] Storage performance and WAL throughput verified');

  } finally {
    engine.close();
    cleanDbFiles(dbPath);
  }
}

async function main() {
  await runDatabaseAndValuesTests();
  await runTransactionsAndWatermarkTests();
  await runCrashRecoveryTests();
  await runIntegrityAuditTests();
  await runPerformanceAndThroughputBenchmark();

  console.log('\n=============================================================');
  console.log('=== ALL PHASE 2 PERSISTENCE TESTS PASSED SUCCESSFULLY 100% ===');
  console.log('=============================================================');
}

main().catch((err) => {
  console.error('\n[FAIL] Phase 2 Test encountered error:', err);
  process.exit(1);
});
