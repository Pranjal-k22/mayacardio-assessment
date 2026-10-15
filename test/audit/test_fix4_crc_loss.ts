import * as net from 'node:net';
import * as fs from 'node:fs';
import * as path from 'node:path';
import assert from 'node:assert';
import { crc32 } from 'node:zlib';
import { StorageEngine } from '../../src/storage/db.ts';
import { BiosignalProtocolAdapter } from '../../src/collector/protocol_adapter.ts';
import { verifyRampExport } from './independent_verifier.ts';
import type { SensorId } from '../../src/types.ts';

const TEST_PORT = 7218;
const TMP_DIR = path.resolve('scratch', 'fix4_test');
if (!fs.existsSync(TMP_DIR)) {
  fs.mkdirSync(TMP_DIR, { recursive: true });
}

const DB_PATH = path.join(TMP_DIR, 'fix4_pipeline.db');
const EXPORT_PATH = path.join(TMP_DIR, 'fix4_export.ndjson');

function cleanFiles() {
  for (const ext of ['', '-wal', '-shm']) {
    const f = DB_PATH + ext;
    if (fs.existsSync(f)) fs.unlinkSync(f);
  }
  if (fs.existsSync(EXPORT_PATH)) fs.unlinkSync(EXPORT_PATH);
}

function generateRampChunk(
  sessionId: string,
  sensor: SensorId,
  fromIndex: number,
  count: number,
  forceBadCrc: boolean = false
): string {
  const data: number[][] = [];
  const MOD = 8388608;
  for (let i = 0; i < count; i++) {
    const idx = fromIndex + i;
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
  const validCrc = crc32(dataSlice).toString(16).padStart(8, '0');
  const crc = forceBadCrc ? 'deadbeef' : validCrc;

  const obj = {
    session_id: sessionId,
    sensor,
    first_index: fromIndex,
    count,
    lost: [],
    session_end: false,
    crc32: crc,
    data,
  };

  return JSON.stringify(obj);
}

function startCorruptingServer(sessionId: string): Promise<net.Server> {
  return new Promise((resolve) => {
    let corruptSensor1Count = 0;

    const server = net.createServer((socket) => {
      let buf = '';

      socket.on('data', (chunk) => {
        buf += chunk.toString('utf-8');
        let newlineIdx: number;

        while ((newlineIdx = buf.indexOf('\n')) !== -1) {
          const line = buf.slice(0, newlineIdx).trim();
          buf = buf.slice(newlineIdx + 1);
          if (!line) continue;

          let req: any;
          try {
            req = JSON.parse(line);
          } catch {
            continue;
          }

          const sensor: SensorId = req.sensor;
          const fromIdx: number = req.from_index || 0;

          // If Sensor 1 requests index 40, return corrupt CRC on first 4 attempts
          if (sensor === 1 && fromIdx === 40 && corruptSensor1Count < 4) {
            corruptSensor1Count++;
            const corruptReply = generateRampChunk(sessionId, sensor, 40, 20, true);
            socket.write(corruptReply + '\n');
            continue;
          }

          // If Sensor 1 retries or moves on, mock drops 40..59 and serves 60..79
          let actualFrom = fromIdx;
          let count = 20;
          if (sensor === 1 && fromIdx < 60 && corruptSensor1Count >= 4) {
            actualFrom = 60; // range 40..59 left the mock buffer
          }

          const reply = generateRampChunk(sessionId, sensor, actualFrom, count, false);
          socket.write(reply + '\n');
        }
      });
    });

    server.listen(TEST_PORT, '127.0.0.1', () => {
      resolve(server);
    });
  });
}

async function runFix4AcceptanceTest() {
  console.log('=== FIX 4 ACCEPTANCE TEST: EXHAUSTED CRC RETRIES RECORDED AS LOSS ===');
  cleanFiles();

  const sessionId = 'fix4_session_crc';
  const server = await startCorruptingServer(sessionId);
  const storage = new StorageEngine({ dbPath: DB_PATH });

  const adapter = new BiosignalProtocolAdapter(
    { host: '127.0.0.1', port: TEST_PORT, readTimeoutMs: 1000 },
    storage
  );

  await adapter.connect();

  try {
    // 1. Initial valid reads: Sensor 1 (0..19, 20..39), Sensor 2 (0..19, 20..39)
    console.log('1. Reading initial batches (0..39) for Sensor 1 and Sensor 2...');
    let r1 = await adapter.readNext(1);
    assert(r1.success && r1.response.first_index === 0);
    let r2 = await adapter.readNext(2);
    assert(r2.success && r2.response.first_index === 0);

    r1 = await adapter.readNext(1);
    assert(r1.success && r1.response.first_index === 20);
    r2 = await adapter.readNext(2);
    assert(r2.success && r2.response.first_index === 20);
    console.log('   [PASS] Initial batches stored cleanly');

    // 2. Next read for Sensor 1: requests index 40. Server replies with corrupt CRC on all retries!
    console.log('2. Requesting index 40 for Sensor 1 (server injects persistent corrupt CRC)...');
    const corruptResult = await adapter.readNext(1);
    assert(!corruptResult.success, 'Read must fail when CRC retries are exhausted');
    assert.strictEqual(corruptResult.code, 'CRC_MISMATCH');
    console.log('   [PASS] Adapter cleanly rejected corrupt reply and exhausted bounded retries');

    // 3. Verify pending reason was persisted in storage
    const pendingRow = (storage as any).db.prepare(
      'SELECT reason FROM pending_loss_reasons WHERE session_id = ? AND sensor = 1 AND from_index = 40'
    ).get(sessionId);
    assert(pendingRow, 'Pending loss reason row must be persisted in database');
    assert.strictEqual(pendingRow.reason, 'crc_unrecoverable');
    console.log('   [PASS] Pending loss reason "crc_unrecoverable" persisted at index 40');

    // 4. Subsequent read: mock moves forward to index 60 (range 40..59 left buffer)
    console.log('3. Subsequent read for Sensor 1: receiving index 60 (gap 40..59 revealed)...');
    const nextValid = await adapter.readNext(1);
    assert(nextValid.success, 'Subsequent read must succeed');
    assert.strictEqual(nextValid.response.first_index, 60);

    // 5. Verify that FIX 3 recorded the gap [40, 59] using the persisted reason "crc_unrecoverable"!
    const losses = storage.getLosses(sessionId, 1);
    const crcLoss = losses.find((l) => l.from_index === 40);
    assert(crcLoss, 'Loss row for range [40, 59] must exist');
    assert.strictEqual(crcLoss.to_index, 59);
    assert.strictEqual(crcLoss.reason, 'crc_unrecoverable', 'Loss reason must be "crc_unrecoverable"');
    console.log('   [PASS] Loss row created for [40, 59] with reason "crc_unrecoverable"');

    // 6. Verify no fabricated values exist for range 40..59
    const samplesInGap = (storage as any).db.prepare(
      'SELECT COUNT(*) as cnt FROM samples WHERE session_id = ? AND sensor = 1 AND sample_idx >= 40 AND sample_idx <= 59'
    ).get(sessionId).cnt;
    assert.strictEqual(samplesInGap, 0, 'Zero samples must be fabricated for missing range');
    console.log('   [PASS] Zero fabricated values exist in database for missing range');

    // 7. Sensor 2 reads 40..59 and 60..79 cleanly
    r2 = await adapter.readNext(2);
    assert(r2.success && r2.response.first_index === 40);
    r2 = await adapter.readNext(2);
    assert(r2.success && r2.response.first_index === 60);

    // Close session gracefully
    storage.recordLoss(sessionId, 1, 80, null, 'session_restart_tail_lost', 'pipeline');
    storage.recordLoss(sessionId, 2, 80, null, 'session_restart_tail_lost', 'pipeline');

    // 8. Generate device export artifact
    console.log('4. Generating export and verifying with independent Ramp verifier...');
    const exportFd = fs.openSync(EXPORT_PATH, 'w');

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

    // 9. Verify export with independent ramp verifier
    const report = await verifyRampExport(EXPORT_PATH);
    console.log('   Verifier summary:', {
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

    console.log('\n=== FIX 4 ACCEPTANCE TEST PASSED 100% ===');
  } finally {
    adapter.disconnect();
    storage.close();
    server.close();
    cleanFiles();
  }
}

runFix4AcceptanceTest().catch((err) => {
  console.error('\n[FATAL FIX 4 ERROR]:', err);
  process.exit(1);
});
