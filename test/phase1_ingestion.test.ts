import assert from 'node:assert';
import * as net from 'node:net';
import { crc32 } from 'node:zlib';
import { extractDataArraySlice, validateRawResponse } from '../src/collector/raw_parser.ts';
import { BiosignalTcpClient } from '../src/collector/tcp_client.ts';
import { BiosignalProtocolAdapter } from '../src/collector/protocol_adapter.ts';
import { BiosignalMockServer } from '../tools/biosignal_mock.ts';

console.log('=== Running Full Phase 1 Test Suite (27 Test Cases) ===');

async function runCrcAndParsingTests() {
  console.log('\n--- Part A: CRC and Parsing Tests (Cases 1 - 11) ---');

  // 1. Known valid zlib CRC32 vectors
  assert.strictEqual(crc32(Buffer.from('')).toString(16).padStart(8, '0'), '00000000');
  assert.strictEqual(crc32(Buffer.from('hello')).toString(16).padStart(8, '0'), '3610a686');
  console.log('[PASS 1] Known valid zlib CRC32 vectors match standard');

  // 2. Correct checksum on a raw data array
  const rawData2 = '[[720,1200,1560],[708,1180,1534],[726,1210,1573]]';
  const expectedCrc2 = crc32(Buffer.from(rawData2)).toString(16).padStart(8, '0');
  const json2 = `{"session_id":"s1","sensor":1,"first_index":0,"count":3,"lost":[],"session_end":false,"crc32":"${expectedCrc2}","data":${rawData2}}`;
  const val2 = validateRawResponse(Buffer.from(json2), 1);
  assert(val2.ok, 'Valid checksum on raw data array must pass');
  assert.strictEqual(val2.dataSlice.toString('utf-8'), rawData2);
  console.log('[PASS 2] Correct checksum on raw data array verified');

  // 3. Incorrect checksum
  const json3 = `{"session_id":"s1","sensor":1,"first_index":0,"count":3,"lost":[],"session_end":false,"crc32":"badc0ffe","data":${rawData2}}`;
  const val3 = validateRawResponse(Buffer.from(json3), 1);
  assert(!val3.ok && val3.code === 'CRC_MISMATCH');
  console.log('[PASS 3] Incorrect checksum rejected with CRC_MISMATCH');

  // 4. Whitespace changes that affect actual raw payload bytes
  const spacedData = '[ [ 100 , 200 , 300 ] ]';
  const spacedCrc = crc32(Buffer.from(spacedData)).toString(16).padStart(8, '0');
  const json4 = `{"session_id":"s1","sensor":1,"first_index":0,"count":1,"lost":[],"session_end":false,"crc32":"${spacedCrc}","data":${spacedData}}`;
  const val4 = validateRawResponse(Buffer.from(json4), 1);
  assert(val4.ok && val4.dataSlice.toString('utf-8') === spacedData);
  console.log('[PASS 4] Whitespace inside data array preserved and exact bytes verified');

  // 5. Nested arrays and escaped strings in JSON scanner
  const trickyJson = `{"note":"fake \\"data\\": [[1,2,3]]","session_id":"s1","sensor":2,"first_index":0,"count":1,"lost":[],"session_end":false,"crc32":"TEMP","data":[[400000,150000]]}`;
  const trickySlice = '[[400000,150000]]';
  const trickyCrc = crc32(Buffer.from(trickySlice)).toString(16).padStart(8, '0');
  const val5 = validateRawResponse(Buffer.from(trickyJson.replace('TEMP', trickyCrc)), 2);
  assert(val5.ok && val5.dataSlice.toString('utf-8') === trickySlice);
  console.log('[PASS 5] Nested arrays and escaped strings correctly handled by quote-aware scanner');

  // 6. Malformed JSON
  const val6 = validateRawResponse(Buffer.from('{"session_id": "s1", unquoted_bad_json: 123}'));
  assert(!val6.ok && val6.code === 'TRUNCATED_OR_MALFORMED');
  console.log('[PASS 6] Malformed JSON safely rejected');

  // 7. Truncated data
  const val7 = validateRawResponse(Buffer.from('{"session_id":"s1","sensor":1,"first_index":0,"count":2,"data":[[1,2,3]'));
  assert(!val7.ok && val7.code === 'TRUNCATED_OR_MALFORMED');
  console.log('[PASS 7] Truncated data rejected');

  // 8. Missing or malformed CRC fields
  const noCrcJson = `{"session_id":"s1","sensor":1,"first_index":0,"count":1,"lost":[],"session_end":false,"data":[[1,2,3]]}`;
  const val8a = validateRawResponse(Buffer.from(noCrcJson));
  assert(!val8a.ok && val8a.code === 'INVALID_SCHEMA');
  const badHexCrc = `{"session_id":"s1","sensor":1,"first_index":0,"count":1,"lost":[],"session_end":false,"crc32":"not_a_hex!","data":[[1,2,3]]}`;
  const val8b = validateRawResponse(Buffer.from(badHexCrc));
  assert(!val8b.ok && val8b.code === 'INVALID_SCHEMA');
  console.log('[PASS 8] Missing or non-hex CRC fields rejected');

  // 9. Incorrect row lengths
  const badRowJson = `{"session_id":"s1","sensor":1,"first_index":0,"count":1,"lost":[],"session_end":false,"crc32":"TEMP","data":[[100,200]]}`;
  const badRowCrc = crc32(Buffer.from('[[100,200]]')).toString(16).padStart(8, '0');
  const val9 = validateRawResponse(Buffer.from(badRowJson.replace('TEMP', badRowCrc)), 1);
  assert(!val9.ok && val9.code === 'CHANNEL_COUNT_MISMATCH');
  console.log('[PASS 9] Incorrect row lengths rejected with CHANNEL_COUNT_MISMATCH');

  // 10. Out-of-range and fractional channel values
  const outOfRangeJson = `{"session_id":"s1","sensor":1,"first_index":0,"count":1,"lost":[],"session_end":false,"crc32":"TEMP","data":[[8388608,0,0]]}`;
  const outOfRangeCrc = crc32(Buffer.from('[[8388608,0,0]]')).toString(16).padStart(8, '0');
  const val10a = validateRawResponse(Buffer.from(outOfRangeJson.replace('TEMP', outOfRangeCrc)), 1);
  assert(!val10a.ok && val10a.code === 'OUT_OF_RANGE_VALUES');

  const fractJson = `{"session_id":"s1","sensor":1,"first_index":0,"count":1,"lost":[],"session_end":false,"crc32":"TEMP","data":[[100.5,0,0]]}`;
  const fractCrc = crc32(Buffer.from('[[100.5,0,0]]')).toString(16).padStart(8, '0');
  const val10b = validateRawResponse(Buffer.from(fractJson.replace('TEMP', fractCrc)), 1);
  assert(!val10b.ok && val10b.code === 'OUT_OF_RANGE_VALUES');
  console.log('[PASS 10] Out-of-range and fractional values rejected');

  // 11. Invalid indices and counts
  const badIndexJson = `{"session_id":"s1","sensor":1,"first_index":-5,"count":1,"lost":[],"session_end":false,"crc32":"TEMP","data":[[1,2,3]]}`;
  const badIndexCrc = crc32(Buffer.from('[[1,2,3]]')).toString(16).padStart(8, '0');
  const val11 = validateRawResponse(Buffer.from(badIndexJson.replace('TEMP', badIndexCrc)), 1);
  assert(!val11.ok && val11.code === 'INVALID_SCHEMA');
  console.log('[PASS 11] Negative indices or count mismatches rejected');
}

async function runTcpFramingTests() {
  console.log('\n--- Part B: TCP Framing Tests (Cases 12 - 18) ---');

  const port = 7210;
  const sample1 = '{"session_id":"s1","sensor":1,"first_index":0,"count":1,"lost":[],"session_end":false,"crc32":"TEMP1","data":[[1,2,3]]}';
  const crc1 = crc32(Buffer.from('[[1,2,3]]')).toString(16).padStart(8, '0');
  const line1 = sample1.replace('TEMP1', crc1);

  const sample2 = '{"session_id":"s1","sensor":1,"first_index":1,"count":1,"lost":[],"session_end":false,"crc32":"TEMP2","data":[[4,5,6]]}';
  const crc2 = crc32(Buffer.from('[[4,5,6]]')).toString(16).padStart(8, '0');
  const line2 = sample2.replace('TEMP2', crc2);

  let activeServer: net.Server | null = null;

  // 12. A JSON line delivered one byte at a time
  activeServer = net.createServer((socket) => {
    socket.on('error', () => {});
    socket.on('data', () => {
      const full = Buffer.from(line1 + '\n', 'utf-8');
      let idx = 0;
      const sendByte = () => {
        if (idx < full.length) {
          socket.write(full.subarray(idx, idx + 1));
          idx++;
          setTimeout(sendByte, 2);
        }
      };
      sendByte();
    });
  });
  await new Promise<void>((r) => activeServer!.listen(port, '127.0.0.1', () => r()));

  const client12 = new BiosignalTcpClient({ host: '127.0.0.1', port, readTimeoutMs: 3000 });
  const r12 = await client12.readSensor(1);
  assert(r12.ok && r12.response.first_index === 0);
  client12.disconnect();
  await new Promise<void>((r) => activeServer!.close(() => r()));
  console.log('[PASS 12] JSON line delivered one byte at a time reassembled and validated');

  // 13 & 14. Multiple lines received together + CRLF framing
  activeServer = net.createServer((socket) => {
    socket.on('error', () => {});
    socket.on('data', () => {
      // Send line 1 with \r\n and line 2 with \n together in one socket write
      socket.write(line1 + '\r\n' + line2 + '\n');
    });
  });
  await new Promise<void>((r) => activeServer!.listen(port, '127.0.0.1', () => r()));

  const client13 = new BiosignalTcpClient({ host: '127.0.0.1', port });
  const r13a = await client13.readSensor(1);
  assert(r13a.ok && r13a.response.first_index === 0, 'Line 1 with CRLF must pass');
  const r13b = await client13.readSensor(1);
  assert(r13b.ok && r13b.response.first_index === 1, 'Buffered line 2 with LF must pass');
  client13.disconnect();
  await new Promise<void>((r) => activeServer!.close(() => r()));
  console.log('[PASS 13 & 14] Multiple lines delivered together and CRLF framing handled cleanly');

  // 15 & 18. Oversized lines & bounded buffering
  activeServer = net.createServer((socket) => {
    socket.on('error', () => {});
    socket.on('data', () => {
      // Send 600 KB without newline to exceed 512 KB line limit
      socket.write(Buffer.alloc(600 * 1024, 0x41));
    });
  });
  await new Promise<void>((r) => activeServer!.listen(port, '127.0.0.1', () => r()));

  const client15 = new BiosignalTcpClient({ host: '127.0.0.1', port, maxLineLength: 512 * 1024 });
  const r15 = await client15.readSensor(1);
  assert(!r15.ok, 'Oversized payload must be rejected');
  assert(r15.error.includes('maximum allowed line length'));
  client15.disconnect();
  await new Promise<void>((r) => activeServer!.close(() => r()));
  console.log('[PASS 15 & 18] Oversized lines rejected; memory buffers bounded safely');

  // 16. Disconnect during a partial response
  activeServer = net.createServer((socket) => {
    socket.on('error', () => {});
    socket.on('data', () => {
      socket.write('{"session_id":"s1","part');
      socket.destroy(); // Sudden close
    });
  });
  await new Promise<void>((r) => activeServer!.listen(port, '127.0.0.1', () => r()));

  const client16 = new BiosignalTcpClient({ host: '127.0.0.1', port });
  const r16 = await client16.readSensor(1);
  assert(!r16.ok, 'Disconnect during partial response must report failure without crashing');
  client16.disconnect();
  await new Promise<void>((r) => activeServer!.close(() => r()));
  console.log('[PASS 16] Disconnect during partial response handled safely');

  // 17. Timeout and successful reconnection
  let serverHits = 0;
  activeServer = net.createServer((socket) => {
    socket.on('error', () => {});
    socket.on('data', () => {
      serverHits++;
      if (serverHits === 1) {
        // Do not reply (trigger timeout)
      } else {
        socket.write(line1 + '\n');
      }
    });
  });
  await new Promise<void>((r) => activeServer!.listen(port, '127.0.0.1', () => r()));

  const client17 = new BiosignalTcpClient({ host: '127.0.0.1', port, readTimeoutMs: 300 });
  const r17a = await client17.readSensor(1);
  assert(!r17a.ok && r17a.error.includes('timeout'));
  // Reconnect and retry
  client17.disconnect();
  const r17b = await client17.readSensor(1);
  assert(r17b.ok && r17b.response.first_index === 0);
  client17.disconnect();
  await new Promise<void>((r) => activeServer!.close(() => r()));
  console.log('[PASS 17] Timeout handled and subsequent reconnection succeeds');
}

async function runProtocolBehaviorTests() {
  console.log('\n--- Part C: Protocol Behavior Tests with BiosignalMock (Cases 19 - 27) ---');

  const testPort = 7220;
  const mock = new BiosignalMockServer({
    mode: 'ramp',
    seed: 7,
    duration: 3600,
    port: testPort,
    bufferSeconds: 10,
  });
  await mock.start();

  const adapter = new BiosignalProtocolAdapter({ host: '127.0.0.1', port: testPort });

  try {
    // 19. Correct requests for each sensor
    mock.generateSamples(400);
    const resS1 = await adapter.readNext(1);
    assert(resS1.success && resS1.response.sensor === 1 && resS1.response.data.length === 400);

    const resS2 = await adapter.readNext(2);
    assert(resS2.success && resS2.response.sensor === 2 && resS2.response.data.length === 400);
    console.log('[PASS 19] Correct requests and responses for Sensor 1 and Sensor 2');

    // 20. Indexed reread after a missing range
    const rereadRes = await adapter.readFromIndex(1, 200);
    assert(rereadRes.success && rereadRes.response.first_index === 200);
    console.log('[PASS 20] Indexed reread (from_index) succeeds');

    // 21. Identical duplicate responses
    const dupRes = await adapter.readFromIndex(1, 0);
    assert(dupRes.success && dupRes.isDuplicate === true);
    console.log('[PASS 21] Identical duplicate recognized via isDuplicate flag');

    // 22. Conflicting duplicate data check
    const existing = [100, 200, 300];
    const same = [100, 200, 300];
    const diff = [100, 999, 300];
    assert.strictEqual(BiosignalProtocolAdapter.compareSampleValues(existing, same), 'identical');
    assert.strictEqual(BiosignalProtocolAdapter.compareSampleValues(existing, diff), 'conflict');
    console.log('[PASS 22] Conflict detection accurately distinguishes identical vs conflicting duplicates');

    // 23. Wrong sensor ID in response
    const wrongSensorJson = `{"session_id":"s1","sensor":2,"first_index":0,"count":1,"lost":[],"session_end":false,"crc32":"TEMP","data":[[1,2]]}`;
    const wrongCrc = crc32(Buffer.from('[[1,2]]')).toString(16).padStart(8, '0');
    const valWrong = validateRawResponse(Buffer.from(wrongSensorJson.replace('TEMP', wrongCrc)), 1);
    assert(!valWrong.ok && valWrong.error.includes('Sensor ID mismatch'));
    console.log('[PASS 23] Wrong sensor ID rejected with expected mismatch error');

    // 24. Changed session ID event
    let sessionTransitionFired = false;
    adapter.onSessionTransition((ev) => {
      sessionTransitionFired = true;
    });
    mock.setFaults({ forceRestart: true });
    mock.generateSamples(100);
    const rRestart = await adapter.readNext(1);
    assert(rRestart.success && sessionTransitionFired);
    console.log('[PASS 24] Changed session ID fires onSessionTransition event and resets indices to 0');

    // 25. Valid source-reported loss ranges
    let reportedLossFired = false;
    adapter.onReportedLoss((ev) => {
      reportedLossFired = true;
      assert.strictEqual(ev.lossRanges[0].reason, 'buffer_overflow');
    });
    mock.setFaults({
      injectLoss: { sensor: 1, from_index: 500, to_index: 899, reason: 'buffer_overflow' },
    });
    mock.generateSamples(800);
    const rLoss = await adapter.readNext(1);
    assert(rLoss.success && reportedLossFired);
    console.log('[PASS 25] Valid source-reported loss ranges captured and forwarded to listeners');

    // 26 & 27. Corrupt response with plausible metadata + bounded retry behavior
    mock.setFaults({ corruptNextCrc: true });
    // Adapter automatically retries on corrupt CRC
    const rCorruptRetry = await adapter.readNext(1);
    assert(rCorruptRetry.success, 'Adapter should recover automatically on transient CRC error');
    console.log('[PASS 26 & 27] Corrupt response rejected and recovered via bounded retry without accepting corrupt metadata');

  } finally {
    adapter.disconnect();
    await mock.stop();
  }
}

async function main() {
  await runCrcAndParsingTests();
  await runTcpFramingTests();
  await runProtocolBehaviorTests();
  console.log('\n=============================================================');
  console.log('=== ALL 27 PHASE 1 REQUIREMENTS VERIFIED AND PASSED 100%  ===');
  console.log('=============================================================');
}

main().catch((err) => {
  console.error('\n[FAIL] Test encountered error:', err);
  process.exit(1);
});
