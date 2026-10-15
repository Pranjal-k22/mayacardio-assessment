import * as net from 'node:net';
import * as fs from 'node:fs';
import * as path from 'node:path';
import assert from 'node:assert';
import { crc32 } from 'node:zlib';
import { StorageEngine } from '../../src/storage/db.ts';
import { BiosignalProtocolAdapter } from '../../src/collector/protocol_adapter.ts';
import { verifyRampExport } from './independent_verifier.ts';
import type { SensorId } from '../../src/types.ts';

const TEST_PORT = 7215;
const CLEAN_PORT = 7216;
const TMP_DIR = path.resolve('scratch', 'fix2_test');

if (!fs.existsSync(TMP_DIR)) {
  fs.mkdirSync(TMP_DIR, { recursive: true });
}

function generateRampChunk(sessionId: string, sensor: SensorId, fromIndex: number, count: number = 20): string {
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
  const crc = crc32(dataSlice).toString(16).padStart(8, '0');

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

function startMockServer(port: number, injectDuplicates: boolean): Promise<net.Server> {
  return new Promise((resolve) => {
    let prevReply: string | null = null;
    const sessionId = 'fix2_session_01';

    const server = net.createServer((socket) => {
      let buf = '';
      socket.on('data', (chunk) => {
        const str = chunk.toString('utf-8');
        buf += str;
        let newlineIdx: number;
        while ((newlineIdx = buf.indexOf('\n')) !== -1) {
          const line = buf.slice(0, newlineIdx).trim();
          buf = buf.slice(newlineIdx + 1);
          if (!line) continue;

          try {
            const req = JSON.parse(line);
            if (req.cmd === 'read') {
              const sensor: SensorId = req.sensor;
              const fromIndex: number = req.from_index ?? 0;
              const reply = generateRampChunk(sessionId, sensor, fromIndex, 20);

              if (injectDuplicates) {
                // Send original reply
                socket.write(reply + '\n');
                // Inject duplicate immediately after original
                socket.write(reply + '\n');

                // Inject duplicate delayed from previous request if available
                if (prevReply) {
                  socket.write(prevReply + '\n');
                }
                prevReply = reply;
              } else {
                socket.write(reply + '\n');
              }
            }
          } catch (err: any) {
            console.error(`[SERVER ${port}] Error parsing: ${err.message}`);
          }
        }
      });
      socket.on('error', (err) => {
        console.error(`[SERVER ${port}] Socket error: ${err.message}`);
      });
    });

    server.on('error', (err) => {
      console.error(`[SERVER ${port}] Server error: ${err.message}`);
    });

    server.listen(port, '127.0.0.1', () => {
      console.log(`[SERVER ${port}] Listening`);
      resolve(server);
    });
  });
}

async function runTest() {
  console.log('=== FIX 2 ACCEPTANCE TEST: Duplicate Reply Desync ===');

  const cleanDbPath = path.join(TMP_DIR, 'clean.db');
  const dupDbPath = path.join(TMP_DIR, 'dup.db');
  if (fs.existsSync(cleanDbPath)) fs.unlinkSync(cleanDbPath);
  if (fs.existsSync(dupDbPath)) fs.unlinkSync(dupDbPath);

  // 1. Run Clean Baseline (no duplicates injected)
  console.log('\n[1/3] Running clean baseline acquisition...');
  const cleanServer = await startMockServer(CLEAN_PORT, false);
  const cleanStorage = new StorageEngine({ dbPath: cleanDbPath });
  const cleanAdapter = new BiosignalProtocolAdapter(
    { host: '127.0.0.1', port: CLEAN_PORT, readTimeoutMs: 2000 },
    cleanStorage
  );

  const NUM_ROUNDS = 25; // 25 rounds * 2 sensors * 20 samples = 500 samples per sensor (1000 total)
  for (let i = 0; i < NUM_ROUNDS; i++) {
    const r1 = await cleanAdapter.readNext(1);
    assert(r1.success, `Clean read S1 round ${i} failed: ${r1.error}`);
    const r2 = await cleanAdapter.readNext(2);
    assert(r2.success, `Clean read S2 round ${i} failed: ${r2.error}`);
  }
  cleanAdapter.disconnect();
  cleanStorage.close();
  await new Promise<void>((r) => cleanServer.close(() => r()));
  console.log(`[PASS] Clean baseline collected ${NUM_ROUNDS * 20} samples per sensor.`);

  // 2. Run Duplicate Injection Run
  console.log('\n[2/3] Running duplicate-injected acquisition (immediate + delayed duplicates)...');
  const dupServer = await startMockServer(TEST_PORT, true);
  const dupStorage = new StorageEngine({ dbPath: dupDbPath });
  const dupAdapter = new BiosignalProtocolAdapter(
    { host: '127.0.0.1', port: TEST_PORT, readTimeoutMs: 2000 },
    dupStorage
  );

  for (let i = 0; i < NUM_ROUNDS; i++) {
    const r1 = await dupAdapter.readNext(1);
    assert(r1.success, `Duplicate-injected read S1 round ${i} failed: ${r1.error}`);
    const r2 = await dupAdapter.readNext(2);
    assert(r2.success, `Duplicate-injected read S2 round ${i} failed: ${r2.error}`);
  }

  const droppedDuplicates = dupAdapter.getClient().getDuplicateRepliesDropped();
  console.log(`[METRIC] Duplicate replies dropped and counted: ${droppedDuplicates}`);
  assert(droppedDuplicates > 0, 'Must have dropped injected duplicate replies');

  dupAdapter.disconnect();
  dupStorage.close();
  await new Promise<void>((r) => dupServer.close(() => r()));

  // 3. Compare Exports & Verify with Independent Ramp Verifier
  console.log('\n[3/3] Exporting and verifying canonical output...');
  const cleanExportPath = path.join(TMP_DIR, 'clean_export.txt');
  const dupExportPath = path.join(TMP_DIR, 'dup_export.txt');

  // Export clean
  const cleanReadEngine = new StorageEngine({ dbPath: cleanDbPath });
  const cleanSamples = [
    ...cleanReadEngine.getSamples('fix2_session_01', 1, 0, 100000),
    ...cleanReadEngine.getSamples('fix2_session_01', 2, 0, 100000),
  ];
  const cleanLosses = cleanReadEngine.getLosses('fix2_session_01');
  cleanReadEngine.close();

  const cleanLines = [
    ...cleanSamples.map((s) => JSON.stringify({ sensor: s.sensor, index: s.index, values: s.values })),
    ...cleanLosses.map((l) => JSON.stringify({ sensor: l.sensor, from_index: l.from_index, to_index: l.to_index, reason: l.reason })),
  ];
  fs.writeFileSync(cleanExportPath, cleanLines.join('\n') + '\n', 'utf-8');

  // Export dup
  const dupReadEngine = new StorageEngine({ dbPath: dupDbPath });
  const dupSamples = [
    ...dupReadEngine.getSamples('fix2_session_01', 1, 0, 100000),
    ...dupReadEngine.getSamples('fix2_session_01', 2, 0, 100000),
  ];
  const dupLosses = dupReadEngine.getLosses('fix2_session_01');
  dupReadEngine.close();

  const dupLines = [
    ...dupSamples.map((s) => JSON.stringify({ sensor: s.sensor, index: s.index, values: s.values })),
    ...dupLosses.map((l) => JSON.stringify({ sensor: l.sensor, from_index: l.from_index, to_index: l.to_index, reason: l.reason })),
  ];
  fs.writeFileSync(dupExportPath, dupLines.join('\n') + '\n', 'utf-8');

  // Verify line-for-line parity
  assert.strictEqual(cleanLines.length, dupLines.length, 'Clean and duplicate export line count must match');
  for (let i = 0; i < cleanLines.length; i++) {
    assert.strictEqual(dupLines[i], cleanLines[i], `Line ${i} mismatch between clean and duplicate runs`);
  }
  console.log('[PASS] Line-for-line parity: Duplicate-injected export is IDENTICAL to clean export.');

  // Verify with independent ramp verifier
  const report = await verifyRampExport(dupExportPath);
  console.log(`[VERIFIER] Pass: ${report.pass}, Samples: ${report.totalSamplesFound}, Dups: ${report.duplicates.length}, Gaps: ${report.unaccountedGaps.length}, Corrupt: ${report.corruptSamples.length}`);
  assert(report.pass, 'Independent verifier must PASS on duplicate-injected export');
  assert.strictEqual(report.duplicates.length, 0, 'Zero duplicate samples in export');
  assert.strictEqual(report.unaccountedGaps.length, 0, 'Zero unaccounted gaps in export');
  assert.strictEqual(report.corruptSamples.length, 0, 'Zero corrupt samples in export');

  console.log('\n=============================================================');
  console.log('=== FIX 2 ACCEPTANCE TEST PASSED 100%                     ===');
  console.log('=============================================================');
}

runTest().catch((err) => {
  console.error('[FAIL] FIX 2 test failed:', err);
  process.exit(1);
});
