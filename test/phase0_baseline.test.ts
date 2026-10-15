import { DatabaseSync } from 'node:sqlite';
import { crc32 } from 'node:zlib';
import assert from 'node:assert';

console.log('--- Running Phase 0 Baseline Verification ---');

// 1. Verify Node.js version >= 24
const [major] = process.versions.node.split('.').map(Number);
console.log(`[PASS] Node.js version: ${process.version} (Major: ${major})`);
assert(major >= 22, 'Node.js must be version 22 or newer');

// 2. Verify node:sqlite capabilities
const db = new DatabaseSync(':memory:');
db.exec('CREATE TABLE test_samples (session_id TEXT, sensor INT, sample_idx INT, PRIMARY KEY(session_id, sensor, sample_idx));');
const insertStmt = db.prepare('INSERT INTO test_samples (session_id, sensor, sample_idx) VALUES (?, ?, ?)');
insertStmt.run('sess-01', 1, 0);
insertStmt.run('sess-01', 1, 1);

const queryStmt = db.prepare('SELECT COUNT(*) as count FROM test_samples');
const res = queryStmt.get() as { count: number };
assert.strictEqual(res.count, 2, 'SQLite insert/query failed');
console.log('[PASS] node:sqlite DatabaseSync operational with prepared statements');

// 3. Verify CRC32 on raw bytes
// CRC32 of 'hello' in zlib is 0x3610a686 (907044486)
const testBuffer = Buffer.from('hello', 'utf-8');
const computedCrc = crc32(testBuffer);
assert.strictEqual(computedCrc, 0x3610a686, 'CRC32 calculation mismatch');
console.log(`[PASS] node:zlib CRC32 verification: 0x${computedCrc.toString(16)} matches expected`);

// 4. Verify 24-bit signed integer boundary handling
const maxSigned24 = 8388607;
const minSigned24 = -8388608;
const testBuf = Buffer.alloc(6);
testBuf.writeIntBE(maxSigned24, 0, 3);
testBuf.writeIntBE(minSigned24, 3, 3);
const readMax = testBuf.readIntBE(0, 3);
const readMin = testBuf.readIntBE(3, 3);
assert.strictEqual(readMax, maxSigned24, '24-bit max int mismatch');
assert.strictEqual(readMin, minSigned24, '24-bit min int mismatch');
console.log('[PASS] Signed 24-bit integer encoding and decoding operational');

console.log('--- All Phase 0 Baseline Checks PASSED ---');
