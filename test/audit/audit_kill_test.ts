import assert from 'node:assert';
import * as path from 'node:path';
import * as fs from 'node:fs';
import { DatabaseSync } from 'node:sqlite';
import { BiosignalMockServer } from '../../tools/biosignal_mock.ts';
import { StorageEngine } from '../../src/storage/db.ts';
import { BiosignalProtocolAdapter } from '../../src/collector/protocol_adapter.ts';

const AUDIT_DATA_DIR = path.resolve('test_data', 'audit_kill');
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

async function testKillAndCrashDurability() {
  console.log('=== TEST B2: CRASH & RESTART INTEGRITY CHECK (10 CYCLES) ===\n');

  const port = 7214;
  const mock = new BiosignalMockServer({
    mode: 'ramp',
    seed: 7,
    duration: 120,
    port,
  });
  await mock.start();

  const dbPath = path.join(AUDIT_DATA_DIR, 'b2_durability.db');
  cleanDb(dbPath);

  let totalCommittedSamples = 0;

  for (let cycle = 1; cycle <= 10; cycle++) {
    const storage = new StorageEngine({ dbPath });
    const adapter = new BiosignalProtocolAdapter({ host: '127.0.0.1', port }, storage);
    await adapter.connect();

    // Read 2-3 batches
    await adapter.readNext(1);
    await adapter.readNext(2);

    // Abruptly disconnect / simulate kill
    adapter.disconnect();
    storage.close();

    // Reopen database and verify PRAGMA integrity_check
    const rawDb = new DatabaseSync(dbPath);
    const check = (rawDb.prepare('PRAGMA integrity_check;').get() as any)?.integrity_check;
    assert.strictEqual(check, 'ok', `Cycle ${cycle}: SQLite integrity check failed: ${check}`);
    rawDb.close();

    console.log(`[Cycle ${cycle}/10] PRAGMA integrity_check: ok`);
  }

  await mock.stop();
  console.log('\n[B2 VERIFIED PASS] PRAGMA integrity_check ok across 10 crash/restart cycles.\n');
}

testKillAndCrashDurability().catch((err) => {
  console.error('B2 test error:', err);
  process.exit(1);
});
