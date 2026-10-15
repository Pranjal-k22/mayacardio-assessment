import type { LossRange, WindowResult } from '../../../src/types.ts';
import { ChunkCodec } from '../../../src/cloud/chunk_codec.ts';
import {
  computeWindowResult,
  PeakDetector,
  calculateHeartRate,
  WINDOW_SIZE,
  PEAK_HISTORY_SIZE,
  HR_WINDOW_LOOKBACK_WINDOWS,
  HR_WINDOW_LOOKBACK_SAMPLES,
} from '../../../src/processing/core.ts';
import type {
  ChunkPayload,
  IngestResponse,
  InventoryResponse,
  CloudStatusResponse,
} from './types.ts';

export interface Env {
  DB: any; // Cloudflare D1Database or D1Adapter
  AUTH_TOKEN?: string;
  CLOUD_AUTH_TOKEN?: string;
}

const DEFAULT_AUTH_TOKEN = 'mayacardio-secret-token';
const MAX_PAYLOAD_BYTES = 2 * 1024 * 1024; // 2 MB limit per request

function jsonResponse(data: unknown, status: number = 200): Response {
  return new Response(JSON.stringify(data), {
    status,
    headers: {
      'Content-Type': 'application/json',
      'Access-Control-Allow-Origin': '*',
      'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
      'Access-Control-Allow-Headers': 'Content-Type, Authorization',
    },
  });
}

function verifyAuth(request: Request, env: Env): boolean {
  const expectedToken = env.CLOUD_AUTH_TOKEN || env.AUTH_TOKEN || DEFAULT_AUTH_TOKEN;
  const authHeader = request.headers.get('Authorization');
  if (!authHeader) return false;
  const parts = authHeader.split(' ');
  if (parts.length !== 2 || parts[0].toLowerCase() !== 'bearer') return false;
  return parts[1] === expectedToken;
}

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const url = new URL(request.url);
    const pathname = url.pathname;

    // CORS preflight
    if (request.method === 'OPTIONS') {
      return new Response(null, {
        status: 204,
        headers: {
          'Access-Control-Allow-Origin': '*',
          'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
          'Access-Control-Allow-Headers': 'Content-Type, Authorization',
        },
      });
    }

    // 1. GET /health (Unauthenticated or safe prober)
    if (pathname === '/health' && request.method === 'GET') {
      try {
        if (!env.DB) {
          return jsonResponse({ ok: false, error: 'D1 binding missing' }, 503);
        }
        await env.DB.prepare('SELECT 1').first();
        return jsonResponse({
          ok: true,
          status: 'healthy',
          d1: 'connected',
          timestamp: Date.now(),
        });
      } catch (err: any) {
        return jsonResponse(
          {
            ok: false,
            status: 'unhealthy',
            d1: 'error',
            error: err?.message || 'D1 check failed',
          },
          503
        );
      }
    }

    // Authenticate all remaining endpoints
    if (!verifyAuth(request, env)) {
      return jsonResponse({ ok: false, error: 'Unauthorized' }, 401);
    }

    // 2. POST /ingest
    if (pathname === '/ingest' && request.method === 'POST') {
      return handleIngest(request, env);
    }

    // 3. GET /inventory
    if (pathname === '/inventory' && request.method === 'GET') {
      return handleInventory(url, env);
    }

    // 4. GET /export
    if (pathname === '/export' && request.method === 'GET') {
      return handleExport(url, env);
    }

    // 5. GET /results
    if (pathname === '/results' && request.method === 'GET') {
      return handleResults(url, env);
    }

    // 6. GET /status
    if (pathname === '/status' && request.method === 'GET') {
      return handleStatus(url, env);
    }

    return jsonResponse({ ok: false, error: 'Not found' }, 404);
  },
};

async function handleIngest(request: Request, env: Env): Promise<Response> {
  let bodyText: string;
  try {
    bodyText = await request.text();
  } catch (err: any) {
    return jsonResponse({ ok: false, error: 'Failed to read request body' }, 400);
  }

  if (bodyText.length > MAX_PAYLOAD_BYTES) {
    return jsonResponse({ ok: false, error: 'Payload exceeds 2 MB limit' }, 413);
  }

  let payload: ChunkPayload;
  try {
    payload = JSON.parse(bodyText);
  } catch (err: any) {
    return jsonResponse({ ok: false, error: 'Malformed JSON payload' }, 400);
  }

  // Schema Validation
  if (
    !payload ||
    typeof payload.session_id !== 'string' ||
    !payload.session_id.trim() ||
    typeof payload.chunk_idx !== 'number' ||
    payload.chunk_idx < 0 ||
    typeof payload.from_index !== 'number' ||
    typeof payload.to_index !== 'number' ||
    payload.to_index !== payload.from_index + 799 ||
    typeof payload.s1_encoded !== 'string' ||
    typeof payload.s2_encoded !== 'string' ||
    typeof payload.s1_mask !== 'string' ||
    typeof payload.s2_mask !== 'string' ||
    !Array.isArray(payload.lost) ||
    typeof payload.digest !== 'string' ||
    (payload.mode !== 'online' && payload.mode !== 'offline')
  ) {
    return jsonResponse({ ok: false, error: 'Invalid chunk payload schema' }, 400);
  }

  // Validate digest
  const expectedDigest = await ChunkCodec.computeDigest({
    sessionId: payload.session_id,
    chunkIdx: payload.chunk_idx,
    fromIndex: payload.from_index,
    toIndex: payload.to_index,
    s1Encoded: payload.s1_encoded,
    s2Encoded: payload.s2_encoded,
    s1Mask: payload.s1_mask,
    s2Mask: payload.s2_mask,
    lost: payload.lost,
  });

  if (expectedDigest !== payload.digest) {
    return jsonResponse({ ok: false, error: 'Content digest mismatch' }, 400);
  }

  // Idempotency & Conflict Check against D1
  const existing = await env.DB.prepare(
    'SELECT digest, results_json FROM chunks WHERE session_id = ? AND chunk_idx = ?'
  )
    .bind(payload.session_id, payload.chunk_idx)
    .first();

  if (existing) {
    if (existing.digest === payload.digest) {
      // Identical retry -> duplicate-safe success
      let results: WindowResult[] = [];
      try {
        results = JSON.parse(existing.results_json);
      } catch {}
      const res: IngestResponse = {
        ok: true,
        session_id: payload.session_id,
        chunk_idx: payload.chunk_idx,
        duplicate: true,
        results,
      };
      return jsonResponse(res, 200);
    } else {
      // Conflict: same (session_id, chunk_idx) but different payload
      try {
        await env.DB.prepare(
          'INSERT INTO cloud_incidents (session_id, chunk_idx, incident_type, details, created_at) VALUES (?, ?, ?, ?, ?)'
        )
          .bind(
            payload.session_id,
            payload.chunk_idx,
            'conflict',
            JSON.stringify({
              stored_digest: existing.digest,
              incoming_digest: payload.digest,
            }),
            Date.now()
          )
          .run();
      } catch {}

      return jsonResponse(
        {
          ok: false,
          error: 'Integrity conflict: chunk already exists with different payload',
          session_id: payload.session_id,
          chunk_idx: payload.chunk_idx,
        },
        409
      );
    }
  }

  // Decode chunk samples
  const s1Map = ChunkCodec.decodeSensor(1, payload.from_index, payload.s1_encoded, payload.s1_mask);
  const s2Map = ChunkCodec.decodeSensor(2, payload.from_index, payload.s2_encoded, payload.s2_mask);

  // Validate sample values (bounds check for 24-bit signed ints)
  for (const [idx, vals] of s1Map.entries()) {
    if (vals.length !== 3) {
      return jsonResponse({ ok: false, error: `Invalid sensor 1 channel count at ${idx}` }, 400);
    }
    for (const v of vals) {
      if (v < -8388608 || v > 8388607) {
        return jsonResponse({ ok: false, error: `Sensor 1 value out of range at ${idx}: ${v}` }, 400);
      }
    }
  }

  for (const [idx, vals] of s2Map.entries()) {
    if (vals.length !== 2) {
      return jsonResponse({ ok: false, error: `Invalid sensor 2 channel count at ${idx}` }, 400);
    }
    for (const v of vals) {
      if (v < -8388608 || v > 8388607) {
        return jsonResponse({ ok: false, error: `Sensor 2 value out of range at ${idx}: ${v}` }, 400);
      }
    }
  }

  let finalResults: WindowResult[] = [];

  if (payload.mode === 'offline') {
    // Synchronized offline window results preserved with mode: 'offline'
    finalResults = payload.offline_results || [];
  } else {
    // Online processing execution in the Cloudflare Worker!
    finalResults = await performCloudOnlineProcessing(payload, s1Map, s2Map, env);
  }

  // Persist canonical chunk in D1
  const resultsJson = JSON.stringify(finalResults);
  const lostJson = JSON.stringify(payload.lost);
  const now = Date.now();

  await env.DB.prepare(
    `INSERT INTO chunks (
      session_id, chunk_idx, from_index, to_index, mode,
      s1_encoded, s2_encoded, s1_mask, s2_mask,
      lost_json, results_json, digest, created_at
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
  )
    .bind(
      payload.session_id,
      payload.chunk_idx,
      payload.from_index,
      payload.to_index,
      payload.mode,
      payload.s1_encoded,
      payload.s2_encoded,
      payload.s1_mask,
      payload.s2_mask,
      lostJson,
      resultsJson,
      payload.digest,
      now
    )
    .run();

  const response: IngestResponse = {
    ok: true,
    session_id: payload.session_id,
    chunk_idx: payload.chunk_idx,
    duplicate: false,
    results: finalResults,
  };

  return jsonResponse(response, 200);
}

/**
 * Execute online processing for the 2 constituent windows of a 2-second chunk.
 */
async function performCloudOnlineProcessing(
  payload: ChunkPayload,
  s1Map: Map<number, number[]>,
  s2Map: Map<number, number[]>,
  env: Env
): Promise<WindowResult[]> {
  const w0 = payload.chunk_idx * 2;
  const w1 = w0 + 1;
  const baseIdx = payload.from_index;

  // Build full S1 map for candidate evaluation:
  // Merge context history if provided
  if (payload.context?.s1_history) {
    for (const h of payload.context.s1_history) {
      if (!s1Map.has(h.index)) {
        s1Map.set(h.index, h.values);
      }
    }
  }

  // Merge lookahead if provided
  if (payload.context?.lookahead) {
    for (const l of payload.context.lookahead) {
      if (!s1Map.has(l.index)) {
        s1Map.set(l.index, l.values);
      }
    }
  }

  // If earlier samples are needed for 800-sample peak history or 2000-sample HR lookback,
  // query preceding chunks from D1 if available.
  const minRequiredIdx = Math.max(0, baseIdx - HR_WINDOW_LOOKBACK_SAMPLES);
  if (!s1Map.has(minRequiredIdx) && payload.chunk_idx > 0) {
    try {
      const minChunkIdx = Math.max(0, payload.chunk_idx - 3);
      const prevRows = await env.DB.prepare(
        'SELECT chunk_idx, from_index, s1_encoded, s1_mask FROM chunks WHERE session_id = ? AND chunk_idx >= ? AND chunk_idx < ? ORDER BY chunk_idx ASC'
      )
        .bind(payload.session_id, minChunkIdx, payload.chunk_idx)
        .all();

      if (prevRows?.results) {
        for (const row of prevRows.results) {
          const prevMap = ChunkCodec.decodeSensor(1, row.from_index, row.s1_encoded, row.s1_mask);
          for (const [idx, vals] of prevMap.entries()) {
            if (!s1Map.has(idx)) {
              s1Map.set(idx, vals);
            }
          }
        }
      }
    } catch {}
  }

  // Peak detection helper
  const getS1Ch2 = (index: number): number | undefined => {
    const s = s1Map.get(index);
    return s && s.length >= 2 ? s[1] : undefined;
  };

  const peakDetector = new PeakDetector();

  // Find all accepted peaks up to window 1 end
  const startEvalIdx = Math.max(0, (w0 - HR_WINDOW_LOOKBACK_WINDOWS) * WINDOW_SIZE);
  const endEvalIdx = (w1 + 1) * WINDOW_SIZE - 1;

  const allAcceptedPeaks = peakDetector.processCandidates(
    startEvalIdx,
    endEvalIdx,
    getS1Ch2
  );

  // Function to check 5-second range completeness for window w
  const checkHrRangeComplete = (w: number): boolean => {
    if (w < HR_WINDOW_LOOKBACK_WINDOWS) return false;
    const rStart = (w - HR_WINDOW_LOOKBACK_WINDOWS) * WINDOW_SIZE;
    const rEnd = (w + 1) * WINDOW_SIZE - 1;
    for (let i = rStart; i <= rEnd; i++) {
      if (!s1Map.has(i)) return false;
    }
    return true;
  };

  // Compute Window Result for W0
  const isHr0Complete = checkHrRangeComplete(w0);
  const res0 = computeWindowResult({
    sessionId: payload.session_id,
    windowIdx: w0,
    mode: 'online',
    s1Samples: s1Map,
    s2Samples: s2Map,
    acceptedPeaks: allAcceptedPeaks,
    isHrRangeComplete: isHr0Complete,
  });

  // Compute Window Result for W1
  const isHr1Complete = checkHrRangeComplete(w1);
  const res1 = computeWindowResult({
    sessionId: payload.session_id,
    windowIdx: w1,
    mode: 'online',
    s1Samples: s1Map,
    s2Samples: s2Map,
    acceptedPeaks: allAcceptedPeaks,
    isHrRangeComplete: isHr1Complete,
  });

  return [res0, res1];
}

async function handleInventory(url: URL, env: Env): Promise<Response> {
  const sessionId = url.searchParams.get('session_id');
  if (!sessionId) {
    return jsonResponse({ ok: false, error: 'Missing session_id parameter' }, 400);
  }

  const fromChunk = url.searchParams.has('from_chunk')
    ? parseInt(url.searchParams.get('from_chunk')!, 10)
    : 0;
  const toChunk = url.searchParams.has('to_chunk')
    ? parseInt(url.searchParams.get('to_chunk')!, 10)
    : 2147483647;
  const limit = url.searchParams.has('limit')
    ? Math.min(10000, parseInt(url.searchParams.get('limit')!, 10))
    : 5000;

  const rows = await env.DB.prepare(
    'SELECT chunk_idx FROM chunks WHERE session_id = ? AND chunk_idx >= ? AND chunk_idx <= ? ORDER BY chunk_idx ASC LIMIT ?'
  )
    .bind(sessionId, fromChunk, toChunk, limit)
    .all();

  const chunks: number[] = rows?.results ? rows.results.map((r: any) => r.chunk_idx) : [];
  const minChunk = chunks.length > 0 ? chunks[0] : undefined;
  const maxChunk = chunks.length > 0 ? chunks[chunks.length - 1] : undefined;

  const res: InventoryResponse = {
    ok: true,
    session_id: sessionId,
    chunks,
    total_chunks: chunks.length,
    min_chunk: minChunk,
    max_chunk: maxChunk,
  };

  return jsonResponse(res, 200);
}

async function handleExport(url: URL, env: Env): Promise<Response> {
  const sessionId = url.searchParams.get('session_id');
  if (!sessionId) {
    return jsonResponse({ ok: false, error: 'Missing session_id parameter' }, 400);
  }

  const fromIndex = url.searchParams.has('from_index')
    ? parseInt(url.searchParams.get('from_index')!, 10)
    : 0;
  const toIndex = url.searchParams.has('to_index')
    ? parseInt(url.searchParams.get('to_index')!, 10)
    : 2147483647;
  const limitChunks = url.searchParams.has('limit_chunks')
    ? parseInt(url.searchParams.get('limit_chunks')!, 10)
    : 5000;

  const fromChunk = Math.floor(fromIndex / 800);
  const toChunk = Math.floor(toIndex / 800);

  const rows = await env.DB.prepare(
    'SELECT chunk_idx, from_index, to_index, s1_encoded, s2_encoded, s1_mask, s2_mask, lost_json FROM chunks WHERE session_id = ? AND chunk_idx >= ? AND chunk_idx <= ? ORDER BY chunk_idx ASC LIMIT ?'
  )
    .bind(sessionId, fromChunk, toChunk, limitChunks)
    .all();

  if (!rows?.results || rows.results.length === 0) {
    return new Response('', {
      status: 200,
      headers: { 'Content-Type': 'application/x-ndjson' },
    });
  }

  const sampleLines: string[] = [];
  const lossLines: string[] = [];
  const lossFingerprints = new Set<string>();

  for (const row of rows.results) {
    const s1Map = ChunkCodec.decodeSensor(1, row.from_index, row.s1_encoded, row.s1_mask);
    const s2Map = ChunkCodec.decodeSensor(2, row.from_index, row.s2_encoded, row.s2_mask);

    const start = Math.max(fromIndex, row.from_index);
    const end = Math.min(toIndex, row.to_index);

    for (let idx = start; idx <= end; idx++) {
      const s1 = s1Map.get(idx);
      if (s1) {
        sampleLines.push(JSON.stringify({ sensor: 1, index: idx, values: s1 }));
      }
      const s2 = s2Map.get(idx);
      if (s2) {
        sampleLines.push(JSON.stringify({ sensor: 2, index: idx, values: s2 }));
      }
    }

    try {
      const losses: any[] = JSON.parse(row.lost_json);
      for (const loss of losses) {
        const fp = `${loss.sensor}:${loss.from_index}:${loss.to_index ?? 'null'}:${loss.reason}`;
        if (!lossFingerprints.has(fp)) {
          lossFingerprints.add(fp);
          lossLines.push(
            JSON.stringify({
              sensor: loss.sensor,
              from_index: loss.from_index,
              to_index: loss.to_index,
              reason: loss.reason,
            })
          );
        }
      }
    } catch {}
  }

  const allLines = [...sampleLines, ...lossLines];
  return new Response(allLines.join('\n') + (allLines.length > 0 ? '\n' : ''), {
    status: 200,
    headers: {
      'Content-Type': 'application/x-ndjson',
      'Access-Control-Allow-Origin': '*',
    },
  });
}

async function handleResults(url: URL, env: Env): Promise<Response> {
  const sessionId = url.searchParams.get('session_id');
  if (!sessionId) {
    return jsonResponse({ ok: false, error: 'Missing session_id parameter' }, 400);
  }

  const fromWindow = url.searchParams.has('from_window')
    ? parseInt(url.searchParams.get('from_window')!, 10)
    : 0;
  const toWindow = url.searchParams.has('to_window')
    ? parseInt(url.searchParams.get('to_window')!, 10)
    : 2147483647;

  const fromChunk = Math.floor(fromWindow / 2);
  const toChunk = Math.floor(toWindow / 2);

  const rows = await env.DB.prepare(
    'SELECT results_json FROM chunks WHERE session_id = ? AND chunk_idx >= ? AND chunk_idx <= ? ORDER BY chunk_idx ASC'
  )
    .bind(sessionId, fromChunk, toChunk)
    .all();

  const allResults: WindowResult[] = [];
  if (rows?.results) {
    for (const row of rows.results) {
      try {
        const results: WindowResult[] = JSON.parse(row.results_json);
        for (const res of results) {
          if (res.window_idx >= fromWindow && res.window_idx <= toWindow) {
            allResults.push(res);
          }
        }
      } catch {}
    }
  }

  return jsonResponse({ ok: true, session_id: sessionId, results: allResults }, 200);
}

async function handleStatus(url: URL, env: Env): Promise<Response> {
  const sessionId = url.searchParams.get('session_id');
  if (!sessionId) {
    return jsonResponse({ ok: false, error: 'Missing session_id parameter' }, 400);
  }

  const statRow = await env.DB.prepare(
    'SELECT COUNT(*) as chunks_count, MIN(chunk_idx) as min_chunk, MAX(chunk_idx) as max_chunk FROM chunks WHERE session_id = ?'
  )
    .bind(sessionId)
    .first();

  const chunksCount = statRow?.chunks_count ?? 0;
  const lastChunkIdx = statRow?.max_chunk ?? -1;
  const lastWindowIdx = lastChunkIdx >= 0 ? (lastChunkIdx + 1) * 2 - 1 : -1;

  const res: CloudStatusResponse = {
    ok: true,
    session_id: sessionId,
    chunks_count: chunksCount,
    samples_count: chunksCount * 800,
    last_chunk_idx: lastChunkIdx,
    last_window_idx: lastWindowIdx,
  };

  return jsonResponse(res, 200);
}
