import http from 'node:http';
import worker, { type Env } from '../cloud/worker/src/index.ts';
import { D1DatabaseAdapter } from '../test/d1_adapter.ts';

const port = parseInt(process.argv[2] || process.env.PORT || '8787', 10);
const d1 = new D1DatabaseAdapter(':memory:');

await d1.exec(`
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
`);

const env: Env = {
  DB: d1,
  AUTH_TOKEN: process.env.CLOUD_AUTH_TOKEN || 'test-secret-token',
};

const server = http.createServer(async (req, res) => {
  const url = `http://127.0.0.1:${port}${req.url}`;
  const chunks: Buffer[] = [];
  for await (const chunk of req) {
    chunks.push(chunk);
  }
  const body = chunks.length > 0 ? Buffer.concat(chunks) : undefined;

  const webReq = new Request(url, {
    method: req.method,
    headers: req.headers as any,
    body: req.method !== 'GET' && req.method !== 'HEAD' ? body : undefined,
  });

  try {
    const webRes = await worker.fetch(webReq, env);
    res.statusCode = webRes.status;
    webRes.headers.forEach((val, key) => {
      res.setHeader(key, val);
    });
    const resBody = await webRes.arrayBuffer();
    res.end(Buffer.from(resBody));
  } catch (err: any) {
    res.statusCode = 500;
    res.end(err.message || 'Worker Internal Error');
  }
});

server.listen(port, '127.0.0.1', () => {
  console.log(`[MOCK_CLOUD] Listening on http://127.0.0.1:${port}`);
});
