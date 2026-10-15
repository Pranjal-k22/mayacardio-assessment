import assert from 'node:assert';
import * as path from 'node:path';
import * as fs from 'node:fs';
import { CloudApiClient } from '../../src/cloud/client.ts';
import { ChunkCodec } from '../../src/cloud/chunk_codec.ts';

function loadEnv(): { url: string; token: string } {
  const envContent = fs.readFileSync('.env', 'utf-8');
  let url = '';
  let token = '';
  for (const line of envContent.split(/\r?\n/)) {
    const trimmed = line.trim();
    if (trimmed.startsWith('CLOUD_BASE_URL=')) url = trimmed.split('=')[1].trim();
    if (trimmed.startsWith('CLOUD_AUTH_TOKEN=')) token = trimmed.split('=')[1].trim();
  }
  return { url, token };
}

async function testLiveCloudLatency() {
  console.log('=== TEST B7: LIVE CLOUDFLARE WORKER LATENCY ===\n');

  const { url, token } = loadEnv();
  console.log(`Connecting to live worker: ${url}`);

  const client = new CloudApiClient({
    baseUrl: url,
    authToken: token,
    timeoutMs: 8000,
  });

  const health = await client.health();
  console.log('Cloud health:', health);
  assert.strictEqual(health.ok, true, 'Live Cloudflare Worker health check failed');

  const sessionId = `live_audit_${Date.now().toString(16)}`;
  console.log(`Created live test session: ${sessionId}`);

  const latencies: number[] = [];

  // Ingest 5 consecutive 2-second chunks (10 windows)
  for (let c = 0; c < 5; c++) {
    const fromIndex = c * 800;
    const toIndex = fromIndex + 799;

    const s1 = new Map<number, number[]>();
    const s2 = new Map<number, number[]>();
    for (let i = fromIndex; i <= toIndex; i++) {
      s1.set(i, [(3 * i) % 8388608, (3 * i + 1) % 8388608, (3 * i + 2) % 8388608]);
      s2.set(i, [(2 * i) % 8388608, (2 * i + 1) % 8388608]);
    }

    const enc1 = ChunkCodec.encodeSensor(1, fromIndex, s1);
    const enc2 = ChunkCodec.encodeSensor(2, fromIndex, s2);

    const digest = await ChunkCodec.computeDigest({
      sessionId,
      chunkIdx: c,
      fromIndex,
      toIndex,
      s1Encoded: enc1.encoded,
      s2Encoded: enc2.encoded,
      s1Mask: enc1.maskHex,
      s2Mask: enc2.maskHex,
      lost: [],
    });

    const start = Date.now();
    const res = await client.ingestChunk({
      session_id: sessionId,
      chunk_idx: c,
      from_index: fromIndex,
      to_index: toIndex,
      mode: 'online',
      s1_encoded: enc1.encoded,
      s2_encoded: enc2.encoded,
      s1_mask: enc1.maskHex,
      s2_mask: enc2.maskHex,
      lost: [],
      digest,
    });
    const elapsed = Date.now() - start;
    latencies.push(elapsed);
    console.log(`[Chunk ${c}] Synced in ${elapsed} ms. Cloud results count: ${res.results?.length ?? 0}`);
  }

  latencies.sort((a, b) => a - b);
  const p50 = latencies[Math.floor(latencies.length * 0.5)];
  const p95 = latencies[Math.floor(latencies.length * 0.95)];
  const max = latencies[latencies.length - 1];

  console.log(`\n[Live Latency Summary] p50: ${p50} ms | p95: ${p95} ms | max: ${max} ms (Deadline: < 5000 ms)`);
  assert(max < 5000, `Live latency exceeded 5s: max was ${max} ms`);
  console.log('[B7 VERIFIED PASS] Live Cloudflare Worker latency is within 5 s deadline.\n');
}

testLiveCloudLatency().catch((e) => {
  console.error('B7 live test error:', e);
  process.exit(1);
});
