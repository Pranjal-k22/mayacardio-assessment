import assert from 'node:assert';
import * as path from 'node:path';
import * as fs from 'node:fs';
import { main } from '../bin/pipeline.ts';
import { StorageEngine } from '../src/storage/db.ts';
import { IntegrityAuditor } from '../src/audit/integrity_audit.ts';
import { D1DatabaseAdapter } from './d1_adapter.ts';
import worker, { type Env } from '../cloud/worker/src/index.ts';
import { CloudApiClient } from '../src/cloud/client.ts';
import { SyncWorker } from '../src/cloud/sync_worker.ts';
import type { MockResponse, LossRange } from '../src/types.ts';

console.log('=== Running Phase 6: CLI, Exports, Loss Accounting & Audit Tests ===');

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

/**
 * Capture stdout and stderr of an async function
 */
async function captureOutput(fn: () => Promise<number>): Promise<{ code: number; stdout: string; stderr: string }> {
  let stdoutData = '';
  let stderrData = '';

  const origStdoutWrite = process.stdout.write;
  const origStderrWrite = process.stderr.write;

  process.stdout.write = (chunk: any) => {
    stdoutData += chunk.toString();
    return true;
  };
  process.stderr.write = (chunk: any) => {
    stderrData += chunk.toString();
    return true;
  };

  try {
    const code = await fn();
    return { code, stdout: stdoutData, stderr: stderrData };
  } finally {
    process.stdout.write = origStdoutWrite;
    process.stderr.write = origStderrWrite;
  }
}

// -------------------------------------------------------------
// Suite 1: CLI Parsing, Help and Argument Validation
// -------------------------------------------------------------
async function testCliParsingAndValidation() {
  console.log('\n--- 1. CLI Parsing, Help & Argument Validation Tests ---');

  // 1. Help flag
  const resHelp = await captureOutput(() => main(['--help']));
  assert.strictEqual(resHelp.code, 0);
  assert.match(resHelp.stdout, /Usage: pipeline/);
  console.log('[PASS] --help displays usage and returns code 0');

  // 2. Missing command
  const resNoCmd = await captureOutput(() => main([]));
  assert.strictEqual(resNoCmd.code, 1);
  console.log('[PASS] Missing command returns exit code 1');

  // 3. Unknown command
  const resBadCmd = await captureOutput(() => main(['unknown-cmd']));
  assert.strictEqual(resBadCmd.code, 1);
  assert.match(resBadCmd.stderr, /Unknown command/);
  console.log('[PASS] Unknown command writes error to stderr and returns code 1');

  // 4. Export missing --session
  const resExpNoSess = await captureOutput(() => main(['export', '--from', 'device']));
  assert.strictEqual(resExpNoSess.code, 1);
  assert.match(resExpNoSess.stderr, /Missing required option '--session <id>'/);
  console.log('[PASS] Export without --session fails with code 1');

  // 5. Export missing --from
  const resExpNoFrom = await captureOutput(() => main(['export', '--session', 's1']));
  assert.strictEqual(resExpNoFrom.code, 1);
  assert.match(resExpNoFrom.stderr, /Missing or invalid option '--from <device\|cloud>'/);
  console.log('[PASS] Export without --from fails with code 1');

  // 6. Results missing --session
  const resResNoSess = await captureOutput(() => main(['results']));
  assert.strictEqual(resResNoSess.code, 1);
  assert.match(resResNoSess.stderr, /Missing required option '--session <id>'/);
  console.log('[PASS] Results without --session fails with code 1');

  // 7. Audit missing --session
  const resAudNoSess = await captureOutput(() => main(['audit']));
  assert.strictEqual(resAudNoSess.code, 1);
  assert.match(resAudNoSess.stderr, /Missing required option '--session <id>'/);
  console.log('[PASS] Audit without --session fails with code 1');
}

// -------------------------------------------------------------
// Suite 2: `pipeline status` Specification Compliance
// -------------------------------------------------------------
async function testPipelineStatus() {
  console.log('\n--- 2. Pipeline Status Specification Compliance Tests ---');
  const dbPath = path.join(TEST_DB_DIR, 'test_status_cli.db');
  cleanDbFiles(dbPath);

  // 1. Status with absent database
  const resAbsent = await captureOutput(() => main(['status', '--db', dbPath]));
  assert.strictEqual(resAbsent.code, 0);
  const linesAbsent = resAbsent.stdout.trim().split('\n');
  assert.strictEqual(linesAbsent.length, 1, 'Must emit exactly one JSON line');
  const jsonAbsent = JSON.parse(linesAbsent[0]);
  assert.deepStrictEqual(jsonAbsent, {
    mode: 'offline',
    samples_saved: { '1': 0, '2': 0 },
    samples_in_cloud: 0,
    backlog: 0,
  });
  console.log('[PASS] Status on empty/absent database outputs exactly 1 JSON line with zero counts');

  // 2. Populate SQLite database with samples and synced chunks
  const storage = new StorageEngine({ dbPath });
  const sessionId = 'session_status_test';

  // Seed 1,600 samples on Sensor 1 and Sensor 2 (covers 2 chunks: chunk 0 and chunk 1)
  const s1Data: number[][] = [];
  const s2Data: number[][] = [];
  for (let i = 0; i < 1600; i++) {
    s1Data.push([100, 200, 300]);
    s2Data.push([400, 500]);
  }

  storage.persistValidatedReply({
    session_id: sessionId,
    sensor: 1,
    first_index: 0,
    count: 1600,
    data: s1Data,
    lost: [],
  });
  storage.persistValidatedReply({
    session_id: sessionId,
    sensor: 2,
    first_index: 0,
    count: 1600,
    data: s2Data,
    lost: [],
  });

  // Mark chunk 0 as synced in cloud (800 sample positions)
  storage.markChunkSynced(sessionId, 0, 'online');
  storage.close();

  // 3. Status with populated database (offline)
  const resPopulated = await captureOutput(() =>
    main(['status', '--db', dbPath, '--session', sessionId, '--cloud-url', 'http://127.0.0.1:1'])
  );
  assert.strictEqual(resPopulated.code, 0);
  const linesPopulated = resPopulated.stdout.trim().split('\n');
  assert.strictEqual(linesPopulated.length, 1, 'Must output exactly one JSON line');
  const jsonPop = JSON.parse(linesPopulated[0]);

  assert.strictEqual(jsonPop.mode, 'offline');
  assert.strictEqual(jsonPop.samples_saved['1'], 1600);
  assert.strictEqual(jsonPop.samples_saved['2'], 1600);
  assert.strictEqual(jsonPop.samples_in_cloud, 800);
  assert.strictEqual(jsonPop.backlog, 800);
  console.log('[PASS] Populated status verified: 1600 saved, 800 in cloud, 800 backlog (1 exact JSON line)');

  cleanDbFiles(dbPath);
}

// -------------------------------------------------------------
// Suite 3: Device Sample & Loss Export (`pipeline export --from device`)
// -------------------------------------------------------------
async function testDeviceExport() {
  console.log('\n--- 3. Device Sample & Loss Export Tests ---');
  const dbPath = path.join(TEST_DB_DIR, 'test_export_device.db');
  cleanDbFiles(dbPath);

  const storage = new StorageEngine({ dbPath });
  const sessionId = 'session_export_dev';

  // Seed samples for Sensor 1 and Sensor 2 at index 0 and 1
  storage.persistValidatedReply({
    session_id: sessionId,
    sensor: 1,
    first_index: 0,
    count: 2,
    data: [
      [720, 1200, 1560],
      [725, 1205, 1565],
    ],
    lost: [],
  });
  storage.persistValidatedReply({
    session_id: sessionId,
    sensor: 2,
    first_index: 0,
    count: 2,
    data: [
      [310, 450],
      [315, 455],
    ],
    lost: [],
  });

  // Seed two loss records: one bounded, one with unknown endpoint (null)
  storage.recordLoss(sessionId, 1, 2, null, 'gap_detected');
  storage.recordLoss(sessionId, 2, 2, 4399, 'buffer_overflow');
  storage.close();

  // 1. Export known session
  const resExp = await captureOutput(() =>
    main(['export', '--session', sessionId, '--from', 'device', '--db', dbPath])
  );
  assert.strictEqual(resExp.code, 0);

  const lines = resExp.stdout.trim().split('\n');
  assert.strictEqual(lines.length, 6, 'Must emit 4 sample lines followed by 2 loss lines');

  // Verify all lines parse as valid JSON
  const parsedRecords = lines.map((l) => JSON.parse(l));

  // Samples first (in index order, sensor 1 then sensor 2)
  assert.deepStrictEqual(parsedRecords[0], { sensor: 1, index: 0, values: [720, 1200, 1560] });
  assert.deepStrictEqual(parsedRecords[1], { sensor: 2, index: 0, values: [310, 450] });
  assert.deepStrictEqual(parsedRecords[2], { sensor: 1, index: 1, values: [725, 1205, 1565] });
  assert.deepStrictEqual(parsedRecords[3], { sensor: 2, index: 1, values: [315, 455] });

  // Losses next
  assert.deepStrictEqual(parsedRecords[4], {
    sensor: 1,
    from_index: 2,
    to_index: null,
    reason: 'gap_detected',
  });
  assert.deepStrictEqual(parsedRecords[5], {
    sensor: 2,
    from_index: 2,
    to_index: 4399,
    reason: 'buffer_overflow',
  });
  console.log('[PASS] Device export emits canonical samples then losses in deterministic order with null preserved');

  // 2. Export unknown session
  const resUnknown = await captureOutput(() =>
    main(['export', '--session', 'non_existent_session', '--from', 'device', '--db', dbPath])
  );
  assert.strictEqual(resUnknown.code, 1);
  assert.match(resUnknown.stderr, /Session 'non_existent_session' not found/);
  console.log('[PASS] Unknown session export fails with exit code 1 and message on stderr');

  cleanDbFiles(dbPath);
}

// -------------------------------------------------------------
// Suite 4: Cloud Export Through Client Interface
// -------------------------------------------------------------
async function testCloudExport() {
  console.log('\n--- 4. Cloud Sample & Loss Export Tests ---');
  const d1 = new D1DatabaseAdapter(':memory:');
  await d1.exec(D1_SCHEMA);
  const env: Env = { DB: d1, AUTH_TOKEN: 'test-token' };

  const dbPath = path.join(TEST_DB_DIR, 'test_cloud_export_local.db');
  cleanDbFiles(dbPath);
  const storage = new StorageEngine({ dbPath });
  const sessionId = 'session_cloud_exp';

  // Seed sample data locally
  storage.persistValidatedReply({
    session_id: sessionId,
    sensor: 1,
    first_index: 0,
    count: 800,
    data: Array(800).fill([100, 200, 300]),
    lost: [],
  });
  storage.persistValidatedReply({
    session_id: sessionId,
    sensor: 2,
    first_index: 0,
    count: 800,
    data: Array(800).fill([400, 500]),
    lost: [{ from_index: 50, to_index: 70, reason: 'sensor_dropout' }],
  });

  // Sync to cloud worker via SyncWorker
  const client = new CloudApiClient({
    baseUrl: 'https://worker.test',
    authToken: 'test-token',
    fetchFn: async (url, opts) => worker.fetch(new Request(url, opts), env),
  });

  const syncWorker = new SyncWorker({ storage, client, sessionId });
  await syncWorker.syncLiveChunk(0);

  // Ingested into cloud! Now query cloud export
  const exportNdjson = await client.export(sessionId, 0, 1);
  const lines = exportNdjson.trim().split('\n').map((l) => JSON.parse(l));

  // Verify sample count and schema
  assert.strictEqual(lines.length, 5); // Index 0 (s1, s2), Index 1 (s1, s2) + 1 loss record
  const s1_0 = lines.find((l) => l.sensor === 1 && l.index === 0);
  assert.deepStrictEqual(s1_0, { sensor: 1, index: 0, values: [100, 200, 300] });

  const loss = lines.find((l) => l.reason === 'sensor_dropout');
  assert.ok(loss);
  assert.strictEqual(loss.sensor, 2);
  assert.strictEqual(loss.from_index, 50);
  assert.strictEqual(loss.to_index, 70);

  console.log('[PASS] Cloud export reconstructs exact canonical samples and loss records');

  storage.close();
  d1.close();
  cleanDbFiles(dbPath);
}

// -------------------------------------------------------------
// Suite 5: Authoritative Window Results (`pipeline results`)
// -------------------------------------------------------------
async function testPipelineResults() {
  console.log('\n--- 5. Authoritative Window Results Tests ---');
  const dbPath = path.join(TEST_DB_DIR, 'test_results_cli.db');
  cleanDbFiles(dbPath);

  const storage = new StorageEngine({ dbPath });
  const sessionId = 'session_results_test';

  // Seed two authoritative window results: one online, one offline
  storage.saveProcessingResult(
    {
      session_id: sessionId,
      window_idx: 0,
      mode: 'online',
      missing_1: 0,
      missing_2: 0,
      complete: true,
      mean: { s1_ch1: 100.0, s1_ch2: 200.0, s1_ch3: 300.0, s2_ch1: 400.0, s2_ch2: 500.0 },
      min: { s1_ch1: 50, s1_ch2: 150, s1_ch3: 250, s2_ch1: 350, s2_ch2: 450 },
      max: { s1_ch1: 150, s1_ch2: 250, s1_ch3: 350, s2_ch1: 450, s2_ch2: 550 },
      magnitude: 640.3,
      heart_rate: null, // null preserved!
    },
    [],
    0,
    -1
  );

  storage.saveProcessingResult(
    {
      session_id: sessionId,
      window_idx: 1,
      mode: 'offline',
      missing_1: 5,
      missing_2: 0,
      complete: false,
      mean: { s1_ch1: 102.0, s1_ch2: 204.0, s1_ch3: 306.0, s2_ch1: 408.0, s2_ch2: 510.0 },
      min: { s1_ch1: 60, s1_ch2: 160, s1_ch3: 260, s2_ch1: 360, s2_ch2: 460 },
      max: { s1_ch1: 160, s1_ch2: 260, s1_ch3: 360, s2_ch1: 460, s2_ch2: 560 },
      magnitude: 650.0,
      heart_rate: 72.5,
    },
    [450],
    1,
    450
  );
  storage.close();

  // Test pipeline results CLI
  const res = await captureOutput(() =>
    main(['results', '--session', sessionId, '--db', dbPath])
  );
  assert.strictEqual(res.code, 0);

  const lines = res.stdout.trim().split('\n');
  assert.strictEqual(lines.length, 2);

  const r0 = JSON.parse(lines[0]);
  const r1 = JSON.parse(lines[1]);

  assert.strictEqual(r0.window_idx, 0);
  assert.strictEqual(r0.mode, 'online');
  assert.strictEqual(r0.heart_rate, null);
  assert.strictEqual(r0.complete, true);

  assert.strictEqual(r1.window_idx, 1);
  assert.strictEqual(r1.mode, 'offline');
  assert.strictEqual(r1.missing_1, 5);
  assert.strictEqual(r1.heart_rate, 72.5);

  console.log('[PASS] Pipeline results emits window results in deterministic order with exact nulls preserved');

  cleanDbFiles(dbPath);
}

// -------------------------------------------------------------
// Suite 6: Comprehensive Loss Accounting & Integrity Audit
// -------------------------------------------------------------
async function testComprehensiveIntegrityAudit() {
  console.log('\n--- 6. Comprehensive Loss Accounting & Integrity Audit Tests ---');
  const dbPath = path.join(TEST_DB_DIR, 'test_audit_cli.db');
  cleanDbFiles(dbPath);

  const storage = new StorageEngine({ dbPath });
  const sessionId = 'session_audit_test';

  // 1. Clean session test
  storage.persistValidatedReply({
    session_id: sessionId,
    sensor: 1,
    first_index: 0,
    count: 400,
    data: Array(400).fill([100, 200, 300]),
    lost: [],
  });
  storage.persistValidatedReply({
    session_id: sessionId,
    sensor: 2,
    first_index: 0,
    count: 400,
    data: Array(400).fill([400, 500]),
    lost: [],
  });

  const auditor = new IntegrityAuditor(storage);
  const cleanReport = await auditor.auditSession(sessionId);
  assert.strictEqual(cleanReport.pass, true);
  assert.strictEqual(cleanReport.findings.length, 0);
  console.log('[PASS] Clean continuous session passes integrity audit with zero findings');

  // 2. Injected defect: Missing index below watermark (Unaccounted gap)
  // Forcefully advance progress watermark beyond stored samples without a loss record
  (storage as any).db
    .prepare('UPDATE progress SET resolved_watermark = 500 WHERE session_id = ? AND sensor = 1')
    .run(sessionId);

  const gapReport = await auditor.auditSession(sessionId);
  assert.strictEqual(gapReport.pass, false);
  const gapFinding = gapReport.findings.find((f) => f.type === 'UNACCOUNTED_GAP');
  assert.ok(gapFinding, 'Should identify unaccounted gap below watermark');
  assert.strictEqual(gapFinding.sensor, 1);
  assert.strictEqual(gapFinding.index, 400);
  console.log('[PASS] Auditor detects unaccounted gap below resolved watermark');

  // 3. Injected defect: Sample overlapping declared loss
  storage.recordLoss(sessionId, 1, 100, 150, 'buffer_overflow');
  const collisionReport = await auditor.auditSession(sessionId);
  const collisionFinding = collisionReport.findings.find((f) => f.type === 'SAMPLE_LOSS_COLLISION');
  assert.ok(collisionFinding, 'Should detect sample and loss collision at index 100');
  console.log('[PASS] Auditor detects sample/loss collision where sample exists inside declared loss range');

  // 4. Injected defect: Overlapping losses with conflicting reasons
  storage.recordLoss(sessionId, 1, 120, 180, 'crc_error');
  const conflictReport = await auditor.auditSession(sessionId);
  const conflictFinding = conflictReport.findings.find((f) => f.type === 'CONFLICTING_LOSS_REASONS');
  assert.ok(conflictFinding, 'Should detect conflicting loss reasons on overlapping ranges');
  console.log('[PASS] Auditor detects overlapping loss records with conflicting reasons');

  // 5. Test CLI audit execution
  const resCliAudit = await captureOutput(() =>
    main(['audit', '--session', sessionId, '--db', dbPath])
  );
  assert.strictEqual(resCliAudit.code, 1); // Fails due to errors
  const auditJson = JSON.parse(resCliAudit.stdout.trim());
  assert.strictEqual(auditJson.pass, false);
  assert.ok(auditJson.findings.length >= 3);
  console.log('[PASS] pipeline audit command outputs machine-readable JSON report and exits code 1 on defects');

  // 6. Test Idempotency: Running audit multiple times does not change findings or state
  const repeatReport = await auditor.auditSession(sessionId);
  assert.strictEqual(repeatReport.findings.length, conflictReport.findings.length);
  console.log('[PASS] Audit is strictly idempotent and read-only');

  storage.close();
  cleanDbFiles(dbPath);
}

// -------------------------------------------------------------
// Main Runner
// -------------------------------------------------------------
async function runAllPhase6Tests() {
  const start = performance.now();
  await testCliParsingAndValidation();
  await testPipelineStatus();
  await testDeviceExport();
  await testCloudExport();
  await testPipelineResults();
  await testComprehensiveIntegrityAudit();

  const totalTimeMs = performance.now() - start;
  console.log('\n=============================================================');
  console.log(`=== ALL PHASE 6 CLI & AUDIT TESTS PASSED 100%             ===`);
  console.log(`=== Total Test Duration: ${(totalTimeMs / 1000).toFixed(2)}s                            ===`);
  console.log('=============================================================');
}

runAllPhase6Tests().catch((err) => {
  console.error('\n[FATAL TEST FAILURE]:', err);
  process.exit(1);
});
