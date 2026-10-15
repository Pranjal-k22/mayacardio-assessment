import assert from 'node:assert';
import * as path from 'node:path';
import * as fs from 'node:fs';
import worker, { type Env } from '../../cloud/worker/src/index.ts';
import { D1DatabaseAdapter } from '../d1_adapter.ts';
import { ChunkCodec } from '../../src/cloud/chunk_codec.ts';
import { StorageEngine } from '../../src/storage/db.ts';

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

async function testCloudExportFormat() {
  console.log('=== TEST CLOUD EXPORT FORMAT & ORDERING ===\n');

  const d1 = new D1DatabaseAdapter(':memory:');
  await d1.exec(D1_SCHEMA);
  const env: Env = { DB: d1, AUTH_TOKEN: 'token' };

  const sessionId = 'parity_session_01';

  // Chunk 0: 800 samples, with a loss record
  const s1_0 = new Map<number, number[]>();
  const s2_0 = new Map<number, number[]>();
  for (let i = 0; i < 800; i++) {
    s1_0.set(i, [100, 200, 300]);
    s2_0.set(i, [400, 500]);
  }
  const enc1_0 = ChunkCodec.encodeSensor(1, 0, s1_0);
  const enc2_0 = ChunkCodec.encodeSensor(2, 0, s2_0);
  const loss0 = [{ sensor: 1, from_index: 2000, to_index: 2100, reason: 'hardware_fault' }];

  const digest0 = await ChunkCodec.computeDigest({
    sessionId,
    chunkIdx: 0,
    fromIndex: 0,
    toIndex: 799,
    s1Encoded: enc1_0.encoded,
    s2Encoded: enc2_0.encoded,
    s1Mask: enc1_0.maskHex,
    s2Mask: enc2_0.maskHex,
    lost: loss0,
  });

  // Ingest Chunk 0
  const req0 = new Request('https://worker.test/ingest', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', Authorization: 'Bearer token' },
    body: JSON.stringify({
      session_id: sessionId,
      chunk_idx: 0,
      from_index: 0,
      to_index: 799,
      mode: 'offline',
      s1_encoded: enc1_0.encoded,
      s2_encoded: enc2_0.encoded,
      s1_mask: enc1_0.maskHex,
      s2_mask: enc2_0.maskHex,
      lost: loss0,
      digest: digest0,
      offline_results: [],
    }),
  });
  const res0 = await worker.fetch(req0, env);
  if (res0.status !== 200) {
    console.log('Worker ingest failed with status:', res0.status, await res0.text());
  }
  assert.strictEqual(res0.status, 200);

  // Ingest Chunk 1: 800 samples (indices 800..1599)
  const s1_1 = new Map<number, number[]>();
  const s2_1 = new Map<number, number[]>();
  for (let i = 800; i < 1600; i++) {
    s1_1.set(i, [110, 210, 310]);
    s2_1.set(i, [410, 510]);
  }
  const enc1_1 = ChunkCodec.encodeSensor(1, 800, s1_1);
  const enc2_1 = ChunkCodec.encodeSensor(2, 800, s2_1);

  const digest1 = await ChunkCodec.computeDigest({
    sessionId,
    chunkIdx: 1,
    fromIndex: 800,
    toIndex: 1599,
    s1Encoded: enc1_1.encoded,
    s2Encoded: enc2_1.encoded,
    s1Mask: enc1_1.maskHex,
    s2Mask: enc2_1.maskHex,
    lost: [],
  });

  const req1 = new Request('https://worker.test/ingest', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', Authorization: 'Bearer token' },
    body: JSON.stringify({
      session_id: sessionId,
      chunk_idx: 1,
      from_index: 800,
      to_index: 1599,
      mode: 'offline',
      s1_encoded: enc1_1.encoded,
      s2_encoded: enc2_1.encoded,
      s1_mask: enc1_1.maskHex,
      s2_mask: enc2_1.maskHex,
      lost: [],
      digest: digest1,
      offline_results: [],
    }),
  });
  const res1 = await worker.fetch(req1, env);
  assert.strictEqual(res1.status, 200);

  // Now request export from cloud worker
  const reqExp = new Request(`https://worker.test/export?session_id=${sessionId}`, {
    method: 'GET',
    headers: { Authorization: 'Bearer token' },
  });
  const resExp = await worker.fetch(reqExp, env);
  const ndjson = await resExp.text();
  const lines = ndjson.trim().split('\n');

  console.log(`Cloud export returned ${lines.length} lines.`);

  // Find index of first loss line vs index of chunk 1 sample line
  let firstLossLineIdx = -1;
  let sampleAfterLossIdx = -1;

  for (let i = 0; i < lines.length; i++) {
    const parsed = JSON.parse(lines[i]);
    if (parsed.from_index !== undefined) {
      if (firstLossLineIdx === -1) firstLossLineIdx = i;
    }
    if (parsed.values !== undefined && firstLossLineIdx !== -1) {
      sampleAfterLossIdx = i;
      console.log(`[ORDERING VIOLATION] Found sample line at line ${i} AFTER loss line at line ${firstLossLineIdx}!`);
      console.log(`Loss line ${firstLossLineIdx}: ${lines[firstLossLineIdx]}`);
      console.log(`Sample line ${i}: ${lines[i]}`);
      break;
    }
  }

  assert.notStrictEqual(sampleAfterLossIdx, -1, 'Expected ordering violation: cloud worker interleaves chunk samples and chunk losses');
  console.log('\n[CONFIRMED DEFECT] Cloud export violates spec format rule: loss line appeared before sample lines of later chunks!');
}

testCloudExportFormat().catch((e) => {
  console.error(e);
  process.exit(1);
});
