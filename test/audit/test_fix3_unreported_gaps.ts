import * as fs from 'node:fs';
import * as path from 'node:path';
import assert from 'node:assert';
import { crc32 } from 'node:zlib';
import { StorageEngine } from '../../src/storage/db.ts';
import { verifyRampExport } from './independent_verifier.ts';
import type { SensorId, MockResponse } from '../../src/types.ts';

const TMP_DIR = path.resolve('scratch', 'fix3_test');
if (!fs.existsSync(TMP_DIR)) {
  fs.mkdirSync(TMP_DIR, { recursive: true });
}

const DB_PATH = path.join(TMP_DIR, 'fix3_pipeline.db');
const EXPORT_PATH = path.join(TMP_DIR, 'fix3_export.ndjson');

function cleanFiles() {
  for (const ext of ['', '-wal', '-shm']) {
    const f = DB_PATH + ext;
    if (fs.existsSync(f)) fs.unlinkSync(f);
  }
  if (fs.existsSync(EXPORT_PATH)) fs.unlinkSync(EXPORT_PATH);
}

function createRampReply(
  sessionId: string,
  sensor: SensorId,
  firstIndex: number,
  count: number,
  lost: Array<{ from_index: number; to_index: number | null; reason: string }> = [],
  sessionEnd: boolean = false
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
    session_end: sessionEnd,
    crc32: crc,
    data,
  };
}

async function runFix3AcceptanceTest() {
  console.log('=== FIX 3 ACCEPTANCE TEST: UNREPORTED INGEST GAPS & INVARIANT ENFORCEMENT ===');
  cleanFiles();

  const storage = new StorageEngine({ dbPath: DB_PATH });
  const sessionId = 'fix3_sess_acceptance';

  // 1. Initial late attach: Sensor 1 starts at index 20 (unreported gap [0, 19])
  console.log('1. Ingesting initial late-attached reply for Sensor 1 (first_index=20, lost=[])...');
  const s1Late = createRampReply(sessionId, 1, 20, 30); // 20..49
  const res1 = storage.persistValidatedReply(s1Late);
  assert(res1.success, 'First reply must succeed');
  assert.strictEqual(res1.newLossesCount, 1, 'Must record attached_late loss range');

  const initialLosses = storage.getLosses(sessionId, 1);
  assert.strictEqual(initialLosses.length, 1);
  assert.strictEqual(initialLosses[0].from_index, 0);
  assert.strictEqual(initialLosses[0].to_index, 19);
  assert.strictEqual(initialLosses[0].reason, 'attached_late');
  console.log('   [PASS] Attached late gap [0, 19] recorded with reason "attached_late"');

  // 2. Sensor 2 starts normally at index 0
  console.log('2. Ingesting initial reply for Sensor 2 (first_index=0, count=50)...');
  const s2Batch1 = createRampReply(sessionId, 2, 0, 50); // 0..49
  const res2 = storage.persistValidatedReply(s2Batch1);
  assert(res2.success);
  assert.strictEqual(res2.newLossesCount, 0, 'No loss for clean initial batch');

  // 3. Sensor 1 receives 50..99
  const s1Batch2 = createRampReply(sessionId, 1, 50, 50); // 50..99
  storage.persistValidatedReply(s1Batch2);

  // 4. Skipped range not in lost[]: Sensor 1 jumps from expected 100 to 200 (gap [100, 199] not in lost)
  console.log('3. Ingesting reply with skipped range: Sensor 1 first_index=200 (skipping 100..199, lost=[])...');
  const s1Skipped = createRampReply(sessionId, 1, 200, 50); // 200..249
  const res3 = storage.persistValidatedReply(s1Skipped);
  assert(res3.success);
  assert.strictEqual(res3.newLossesCount, 1, 'Must create loss row for unreported gap 100..199');

  const s1Losses = storage.getLosses(sessionId, 1);
  const unrepGap = s1Losses.find((l) => l.from_index === 100);
  assert(unrepGap, 'Loss record for [100, 199] must be stored');
  assert.strictEqual(unrepGap.to_index, 199);
  assert.strictEqual(unrepGap.reason, 'gap_unreported_by_mock');
  console.log('   [PASS] Unreported gap [100, 199] recorded with reason "gap_unreported_by_mock"');

  // 5. Sensor 2 stalls at index 50 while Sensor 1 continues to index 300
  console.log('4. Stalled sensor: Sensor 2 receives no further samples; Sensor 1 continues to index 300...');
  const s1Batch4 = createRampReply(sessionId, 1, 250, 50); // 250..299
  storage.persistValidatedReply(s1Batch4);

  // 6. Restart mid-run: session terminates
  console.log('5. Session restart mid-run declared on new session...');
  storage.recordLoss(sessionId, 1, 300, null, 'session_restart_tail_lost', 'pipeline');
  storage.recordLoss(sessionId, 2, 50, null, 'session_restart_tail_lost', 'pipeline');

  // 7. Verify integrity audit
  console.log('6. Running storage integrity audit for Sensor 1 and Sensor 2...');
  const audit1 = storage.auditSession(sessionId, 1);
  const audit2 = storage.auditSession(sessionId, 2);
  assert(audit1.pass, `Audit 1 must pass: ${JSON.stringify(audit1.unaccountedGaps)}`);
  assert(audit2.pass, `Audit 2 must pass: ${JSON.stringify(audit2.unaccountedGaps)}`);
  assert.strictEqual(audit1.unaccountedGaps.length, 0);
  assert.strictEqual(audit2.unaccountedGaps.length, 0);
  console.log('   [PASS] Audit confirms zero unaccounted gaps below high watermark for both sensors');

  // 8. Generate device export
  console.log('7. Generating export via StorageEngine.getExportLosses...');
  const exportFd = fs.openSync(EXPORT_PATH, 'w');

  // Write samples in index order
  let fromIdx = 0;
  while (true) {
    const s1 = storage.getSamples(sessionId, 1, fromIdx, 100);
    const s2 = storage.getSamples(sessionId, 2, fromIdx, 100);
    if (s1.length === 0 && s2.length === 0) break;

    const indices = Array.from(new Set([...s1.map((s) => s.index), ...s2.map((s) => s.index)])).sort((a, b) => a - b);
    const s1Map = new Map(s1.map((s) => [s.index, s]));
    const s2Map = new Map(s2.map((s) => [s.index, s]));

    for (const idx of indices) {
      const samp1 = s1Map.get(idx);
      if (samp1) {
        fs.writeSync(exportFd, JSON.stringify({ sensor: 1, index: samp1.index, values: samp1.values }) + '\n');
      }
      const samp2 = s2Map.get(idx);
      if (samp2) {
        fs.writeSync(exportFd, JSON.stringify({ sensor: 2, index: samp2.index, values: samp2.values }) + '\n');
      }
    }
    fromIdx = indices[indices.length - 1] + 1;
  }

  // Write losses (losses last!)
  const exportLosses = storage.getExportLosses(sessionId);
  for (const l of exportLosses) {
    fs.writeSync(
      exportFd,
      JSON.stringify({
        sensor: l.sensor,
        from_index: l.from_index,
        to_index: l.to_index,
        reason: l.reason,
      }) + '\n'
    );
  }
  fs.closeSync(exportFd);
  storage.close();

  // 9. Verify with independent ground-truth Ramp verifier
  console.log('8. Verifying export artifact with independent Ramp verifier...');
  const report = await verifyRampExport(EXPORT_PATH);
  console.log('   Report summary:', {
    pass: report.pass,
    totalSamplesFound: report.totalSamplesFound,
    totalLossLines: report.totalLossLines,
    unaccountedGaps: report.unaccountedGaps,
    corruptSamples: report.corruptSamples.length,
    orderingViolation: report.orderingViolation,
  });

  assert(report.pass, `Independent verifier must PASS: ${JSON.stringify(report.errors)}`);
  assert.strictEqual(report.unaccountedGaps.length, 0, 'Verifier must report 0 unaccounted gaps');
  assert.strictEqual(report.corruptSamples.length, 0, 'Verifier must report 0 corrupt samples');
  assert.strictEqual(report.orderingViolation, false, 'Samples must strictly precede losses');
  assert(report.totalLossLines >= 4, 'Must contain attached_late, gap_unreported_by_mock, and restart losses');

  console.log('\n=== FIX 3 ACCEPTANCE TEST PASSED 100% ===');
  cleanFiles();
}

runFix3AcceptanceTest().catch((err) => {
  console.error('\n[FATAL FIX 3 ERROR]:', err);
  process.exit(1);
});
