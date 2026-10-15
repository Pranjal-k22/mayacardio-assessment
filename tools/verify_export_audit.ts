import * as fs from 'node:fs';
import * as path from 'node:path';
import { execSync } from 'node:child_process';
import { StorageEngine } from '../src/storage/db.ts';
import { BiosignalMockServer } from './biosignal_mock.ts';

const TEST_DIR = path.resolve('data');
if (!fs.existsSync(TEST_DIR)) {
  fs.mkdirSync(TEST_DIR, { recursive: true });
}

const DB_PATH = path.join(TEST_DIR, 'test_export_audit.db');
const EXPORT_VALID = path.join(TEST_DIR, 'export_audit_valid.ndjson');
const EXPORT_CORRUPT = path.join(TEST_DIR, 'export_audit_corrupt.ndjson');
const EXPORT_DUP = path.join(TEST_DIR, 'export_audit_dup.ndjson');
const EXPORT_GAP = path.join(TEST_DIR, 'export_audit_gap.ndjson');

// Cleanup previous runs
for (const f of [DB_PATH, `${DB_PATH}-wal`, `${DB_PATH}-shm`, EXPORT_VALID, EXPORT_CORRUPT, EXPORT_DUP, EXPORT_GAP]) {
  if (fs.existsSync(f)) fs.unlinkSync(f);
}

console.log('=== MayaCardio Real Export & Ground Truth Audit Verification ===');

// 1. Initialize DB and insert known Section 6.1 ramp samples (400 samples each)
const storage = new StorageEngine({ dbPath: DB_PATH });
const sessionId = 'audit-session-2026';
const mock = new BiosignalMockServer({ mode: 'ramp', seed: 42, duration: 3600, port: 7100 });

const s1Samples: number[][] = [];
const s2Samples: number[][] = [];
for (let i = 0; i < 400; i++) {
  const [s1, s2] = mock.computeSample(i);
  s1Samples.push(s1);
  s2Samples.push(s2);
}

storage.persistValidatedReply({
  session_id: sessionId,
  sensor: 1,
  first_index: 0,
  count: 400,
  data: s1Samples,
  lost: [],
});

storage.persistValidatedReply({
  session_id: sessionId,
  sensor: 2,
  first_index: 0,
  count: 400,
  data: s2Samples,
  lost: [],
});

// Explicit loss record
storage.persistValidatedReply({
  session_id: sessionId,
  sensor: 1,
  first_index: 500,
  count: 0,
  data: [],
  lost: [{ from_index: 400, to_index: 499, reason: 'buffer_overflow' }],
});

storage.close();
console.log('[SETUP] Seeded 800 Section 6.1 ramp samples + 1 explicit loss range in SQLite.');

// 2. Run real CLI export via bin/pipeline.ts
console.log('\n--- Test Case 1: Real Device Export via CLI ---');
const exportCmd = `node --experimental-strip-types bin/pipeline.ts export --session ${sessionId} --from device --db ${DB_PATH}`;
console.log(`Executing: ${exportCmd} > ${EXPORT_VALID}`);
const exportOutput = execSync(exportCmd, { encoding: 'utf-8' });
fs.writeFileSync(EXPORT_VALID, exportOutput, 'utf-8');
const exportLines = exportOutput.trim().split('\n');
console.log(`Exported Lines: ${exportLines.length} (Expected: 801 lines = 800 samples + 1 loss)`);

// 3. Run check_ground_truth CLI on valid export
console.log('\n--- Test Case 2: Ground-Truth Verification on Valid Export ---');
const checkCmdValid = `node --experimental-strip-types tools/check_ground_truth.ts ${EXPORT_VALID} ramp 42`;
console.log(`Executing: ${checkCmdValid}`);
let checkOutputValid = '';
let exitCodeValid = 0;
try {
  checkOutputValid = execSync(checkCmdValid, { encoding: 'utf-8' });
} catch (e: any) {
  exitCodeValid = e.status;
  checkOutputValid = e.stdout;
}
console.log(`Exit Code: ${exitCodeValid}`);
console.log(`Output:\n${checkOutputValid.trim()}`);

// 4. Test corrupted sample detection
console.log('\n--- Test Case 3: Ground-Truth Verification on Tampered/Corrupted Export ---');
const corruptContent = exportOutput.replace(
  JSON.stringify({ sensor: 1, index: 100, values: mock.computeSample(100)[0] }),
  JSON.stringify({ sensor: 1, index: 100, values: [999999, 999999, 999999] })
);
fs.writeFileSync(EXPORT_CORRUPT, corruptContent, 'utf-8');
const checkCmdCorrupt = `node --experimental-strip-types tools/check_ground_truth.ts ${EXPORT_CORRUPT} ramp 42`;
console.log(`Executing: ${checkCmdCorrupt}`);
let checkOutputCorrupt = '';
let exitCodeCorrupt = 0;
try {
  checkOutputCorrupt = execSync(checkCmdCorrupt, { encoding: 'utf-8' });
} catch (e: any) {
  exitCodeCorrupt = e.status;
  checkOutputCorrupt = e.stdout || e.stderr;
}
console.log(`Exit Code: ${exitCodeCorrupt}`);
console.log(`Output:\n${checkOutputCorrupt.trim()}`);

// 5. Test duplicate sample detection
console.log('\n--- Test Case 4: Ground-Truth Verification on Duplicate Export ---');
const dupLine = JSON.stringify({ sensor: 2, index: 50, values: mock.computeSample(50)[1] }) + '\n';
const dupContent = dupLine + exportOutput;
fs.writeFileSync(EXPORT_DUP, dupContent, 'utf-8');
const checkCmdDup = `node --experimental-strip-types tools/check_ground_truth.ts ${EXPORT_DUP} ramp 42`;
console.log(`Executing: ${checkCmdDup}`);
let checkOutputDup = '';
let exitCodeDup = 0;
try {
  checkOutputDup = execSync(checkCmdDup, { encoding: 'utf-8' });
} catch (e: any) {
  exitCodeDup = e.status;
  checkOutputDup = e.stdout || e.stderr;
}
console.log(`Exit Code: ${exitCodeDup}`);
console.log(`Output:\n${checkOutputDup.trim()}`);

// 6. Test unaccounted gap detection
console.log('\n--- Test Case 5: Ground-Truth Verification on Unaccounted Gap Export ---');
const gapContent = exportLines.filter((l) => !l.includes('"index":150')).join('\n') + '\n';
fs.writeFileSync(EXPORT_GAP, gapContent, 'utf-8');
const checkCmdGap = `node --experimental-strip-types tools/check_ground_truth.ts ${EXPORT_GAP} ramp 42`;
console.log(`Executing: ${checkCmdGap}`);
let checkOutputGap = '';
let exitCodeGap = 0;
try {
  checkOutputGap = execSync(checkCmdGap, { encoding: 'utf-8' });
} catch (e: any) {
  exitCodeGap = e.status;
  checkOutputGap = e.stdout || e.stderr;
}
console.log(`Exit Code: ${exitCodeGap}`);
console.log(`Output:\n${checkOutputGap.trim()}`);

console.log('\n=== Export & Ground Truth Audit Verification Complete ===');
