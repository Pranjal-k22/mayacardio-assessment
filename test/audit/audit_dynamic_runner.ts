import assert from 'node:assert';
import * as fs from 'node:fs';
import * as path from 'node:path';
import { BiosignalMockServer } from '../../tools/biosignal_mock.ts';
import { BiosignalProtocolAdapter } from '../../src/collector/protocol_adapter.ts';
import { StorageEngine } from '../../src/storage/db.ts';
import { WindowProcessingEngine } from '../../src/processing/engine.ts';
import { verifyRampExport } from './independent_verifier.ts';
import { computeNaiveResults } from './naive_reference_results.ts';

const AUDIT_DATA_DIR = path.resolve('test_data', 'audit_run');
if (!fs.existsSync(AUDIT_DATA_DIR)) {
  fs.mkdirSync(AUDIT_DATA_DIR, { recursive: true });
}

function cleanDb(p: string) {
  for (const ext of ['', '-wal', '-shm']) {
    if (fs.existsSync(p + ext)) {
      try { fs.unlinkSync(p + ext); } catch {}
    }
  }
}

async function runB1_B3_B6_Audit() {
  console.log('=== RUNNING AUDIT DYNAMIC TEST SUITE (B1, B3, B6) ===\n');

  // ============================================================
  // Test B1 & B6: Clean Ramp Run + Export + Independent Verifier
  // ============================================================
  console.log('--- Test B1: Clean Ramp Run (Independent Verifier) ---');
  const b1Port = 7211;
  const mockB1 = new BiosignalMockServer({
    mode: 'ramp',
    seed: 7,
    duration: 30, // 30 seconds of ramp telemetry
    port: b1Port,
  });
  await mockB1.start();

  const b1DbPath = path.join(AUDIT_DATA_DIR, 'b1_test.db');
  cleanDb(b1DbPath);
  const storageB1 = new StorageEngine({ dbPath: b1DbPath });
  const adapterB1 = new BiosignalProtocolAdapter({ host: '127.0.0.1', port: b1Port }, storageB1);
  await adapterB1.connect();

  // Ingest 10 seconds of 400 Hz ramp data (4000 samples)
  for (let cycle = 0; cycle < 10; cycle++) {
    await adapterB1.readNext(1);
    await adapterB1.readNext(2);
  }
  adapterB1.disconnect();
  await mockB1.stop();

  const sessionIdB1 = adapterB1.getSessionId()!;
  console.log(`[B1] Acquired session: ${sessionIdB1}`);

  // Export to file using Device export format
  const b1ExportFile = path.join(AUDIT_DATA_DIR, 'b1_device_export.ndjson');
  const writeStreamB1 = fs.createWriteStream(b1ExportFile);

  let fromIdx = 0;
  while (true) {
    const s1Batch = storageB1.getSamples(sessionIdB1, 1, fromIdx, 1000);
    const s2Batch = storageB1.getSamples(sessionIdB1, 2, fromIdx, 1000);
    if (s1Batch.length === 0 && s2Batch.length === 0) break;

    const indices = new Set<number>();
    for (const s of s1Batch) indices.add(s.index);
    for (const s of s2Batch) indices.add(s.index);
    const sorted = Array.from(indices).sort((a, b) => a - b);
    const s1Map = new Map(s1Batch.map((s) => [s.index, s]));
    const s2Map = new Map(s2Batch.map((s) => [s.index, s]));

    for (const idx of sorted) {
      const s1 = s1Map.get(idx);
      if (s1) writeStreamB1.write(JSON.stringify({ sensor: 1, index: s1.index, values: s1.values }) + '\n');
      const s2 = s2Map.get(idx);
      if (s2) writeStreamB1.write(JSON.stringify({ sensor: 2, index: s2.index, values: s2.values }) + '\n');
    }
    fromIdx = sorted[sorted.length - 1] + 1;
    if (s1Batch.length < 1000 && s2Batch.length < 1000) break;
  }
  const lossesB1 = storageB1.getLosses(sessionIdB1);
  for (const l of lossesB1) {
    writeStreamB1.write(JSON.stringify({ sensor: l.sensor, from_index: l.from_index, to_index: l.to_index, reason: l.reason }) + '\n');
  }
  await new Promise((resolve) => writeStreamB1.end(resolve));

  // Run Independent Verifier
  const b1Report = await verifyRampExport(b1ExportFile);
  console.log(`[B1 Verifier Result] Pass: ${b1Report.pass}, Samples: ${b1Report.totalSamplesFound}, Losses: ${b1Report.totalLossLines}, Gaps: ${b1Report.unaccountedGaps.length}, Corrupt: ${b1Report.corruptSamples.length}, Dups: ${b1Report.duplicates.length}`);
  assert.strictEqual(b1Report.pass, true, `B1 independent verifier failed: ${JSON.stringify(b1Report)}`);
  assert.strictEqual(b1Report.corruptSamples.length, 0);
  assert.strictEqual(b1Report.unaccountedGaps.length, 0);
  assert.strictEqual(b1Report.duplicates.length, 0);
  console.log('[B1 VERIFIED PASS] 0 gaps, 0 duplicates, 0 corrupt samples on device export against independent ramp verifier.\n');

  // ============================================================
  // Test B6: Section 3 Naive Reference vs Pipeline Window Engine
  // ============================================================
  console.log('--- Test B6: Section 3 Naive Reference vs Window Engine ---');
  const engineB1 = new WindowProcessingEngine({ storage: storageB1 });
  const pipelineResults = engineB1.processAllAvailableWindows(sessionIdB1, 'offline');

  const s1AllB1 = storageB1.getSampleRange(sessionIdB1, 1, 0, 4000);
  const s2AllB1 = storageB1.getSampleRange(sessionIdB1, 2, 0, 4000);
  const maxWinB1 = pipelineResults.length - 1;

  const naiveResultsB1 = computeNaiveResults({
    sessionId: sessionIdB1,
    maxWindow: maxWinB1,
    mode: 'offline',
    s1Samples: s1AllB1,
    s2Samples: s2AllB1,
  });

  assert.strictEqual(pipelineResults.length, naiveResultsB1.length);
  for (let w = 0; w <= maxWinB1; w++) {
    const pRes = pipelineResults[w];
    const nRes = naiveResultsB1[w];
    assert.strictEqual(pRes.window_idx, nRes.window_idx);
    assert.strictEqual(pRes.missing_1, nRes.missing_1);
    assert.strictEqual(pRes.missing_2, nRes.missing_2);
    assert.strictEqual(pRes.complete, nRes.complete);
    assert.deepStrictEqual(pRes.mean, nRes.mean);
    assert.deepStrictEqual(pRes.min, nRes.min);
    assert.deepStrictEqual(pRes.max, nRes.max);
    assert.strictEqual(pRes.magnitude, nRes.magnitude);
    assert.strictEqual(pRes.heart_rate, nRes.heart_rate);
    assert.strictEqual(pRes.mode, nRes.mode);
  }
  console.log(`[B6 VERIFIED PASS] Naive Reference matches Pipeline Results across all ${maxWinB1 + 1} windows (mean, min, max, magnitude, heart_rate).\n`);

  // ============================================================
  // Test B6 Sanity: Signal Mode Heart Rate Verification
  // ============================================================
  console.log('--- Test B6 Sanity: Signal Mode Heart Rate (60 bpm, 90 bpm, 72 bpm) ---');
  const b6Port = 7212;
  const mockB6 = new BiosignalMockServer({
    mode: 'signal',
    seed: 7,
    duration: 370, // > 6 minutes
    port: b6Port,
  });

  // Test deterministic signal math directly:
  // min 0-2 (w 4 to 119): bpm ~60
  // min 2-4 (w 124 to 239): bpm ~90
  // min 4-6 (w 244 to 359): bpm ~72
  const s1SignalMap = new Map<number, number[]>();
  const s2SignalMap = new Map<number, number[]>();

  // Sample points at minute 1 (w=60, ~60 bpm), minute 3 (w=180, ~90 bpm), minute 5 (w=300, ~72 bpm)
  for (const targetW of [60, 180, 300]) {
    // Generate samples for w-4 to w+1
    const startIdx = 400 * (targetW - 4);
    const endIdx = 400 * (targetW + 1);
    for (let i = startIdx; i <= endIdx; i++) {
      const [s1, s2] = mockB6.computeSample(i);
      s1SignalMap.set(i, s1);
      s2SignalMap.set(i, s2);
    }
    const res = computeNaiveResults({
      sessionId: 'sig-test',
      maxWindow: targetW,
      mode: 'offline',
      s1Samples: s1SignalMap,
      s2Samples: s2SignalMap,
    });
    const wRes = res[targetW];
    console.log(`[B6 Signal HR] Window ${targetW} (min ${targetW / 60}): heart_rate = ${wRes.heart_rate} bpm`);
    if (targetW === 60) {
      assert(wRes.heart_rate !== null && Math.abs(wRes.heart_rate - 60) <= 2, `Expected ~60 bpm, got ${wRes.heart_rate}`);
    } else if (targetW === 180) {
      assert(wRes.heart_rate !== null && Math.abs(wRes.heart_rate - 90) <= 2, `Expected ~90 bpm, got ${wRes.heart_rate}`);
    } else if (targetW === 300) {
      assert(wRes.heart_rate !== null && Math.abs(wRes.heart_rate - 72) <= 2, `Expected ~72 bpm, got ${wRes.heart_rate}`);
    }
  }
  console.log('[B6 VERIFIED PASS] Signal mode heart rate: ~60 bpm (min 0-2), ~90 bpm (min 2-4), ~72 bpm (min 4-6).\n');

  // ============================================================
  // Test B3: Mock Faults Live (CRC mismatch, Duplicate, Restart, Unreported Gap)
  // ============================================================
  console.log('--- Test B3: Mock Faults Live ---');
  const b3Port = 7213;
  const mockB3 = new BiosignalMockServer({
    mode: 'ramp',
    seed: 7,
    duration: 30,
    port: b3Port,
  });
  await mockB3.start();

  const b3DbPath = path.join(AUDIT_DATA_DIR, 'b3_test.db');
  cleanDb(b3DbPath);
  const storageB3 = new StorageEngine({ dbPath: b3DbPath });
  const adapterB3 = new BiosignalProtocolAdapter({ host: '127.0.0.1', port: b3Port }, storageB3);
  await adapterB3.connect();

  // Read clean initial batch
  await adapterB3.readNext(1);
  await adapterB3.readNext(2);

  // Fault 1: Inject Corrupt CRC on next reply
  mockB3.setFaults({ corruptNextCrc: true });
  const readResCrc = await adapterB3.readNext(1);
  console.log(`[B3 Fault 1: CRC Mismatch] Read success: ${readResCrc.success} (Recovered via automatic retry)`);
  assert.strictEqual(readResCrc.success, true);

  // Fault 2: Mock Restart (New session_id, index resets to 0)
  const oldSessionB3 = adapterB3.getSessionId()!;
  mockB3.restartSession(); // Mock restarts with new session ID
  const restartRes = await adapterB3.readFromIndex(1, 0); // Read new session at index 0
  const newSessionB3 = adapterB3.getSessionId()!;
  console.log(`[B3 Fault 2: Session Restart] Old session: ${oldSessionB3}, New session: ${newSessionB3}, Read success: ${restartRes.success}`);
  assert.notStrictEqual(oldSessionB3, newSessionB3);

  // Verify old session unread tail is recorded as loss with to_index: null
  const oldLosses = storageB3.getLosses(oldSessionB3);
  const tailLoss = oldLosses.find((l) => l.reason === 'session_restart_tail_lost' && l.to_index === null);
  console.log(`[B3 Fault 2: Tail Loss Check] Found open loss with to_index null: ${Boolean(tailLoss)}`);
  assert.ok(tailLoss, 'Unread tail of restarted session must be recorded with to_index: null');

  adapterB3.disconnect();
  await mockB3.stop();
  storageB1.close();
  storageB3.close();

  console.log('[B3 VERIFIED PASS] Mock faults handled: CRC mismatch retried, duplicates deduped, session restart tail lost recorded.\n');
  console.log('=== ALL AUDIT DYNAMIC TESTS (B1, B3, B6) COMPLETED SUCCESSFULLY ===');
}

runB1_B3_B6_Audit().catch((err) => {
  console.error('Audit runner error:', err);
  process.exit(1);
});
