-- MayaCardio D1 Database Schema
-- Optimized for Cloudflare D1 Free Tier (100k daily row writes, 500 MB DB limit)

-- Canonical 2-second chunks table (1 chunk = 2 one-second windows = 1600 total sensor samples)
-- Exactly 1 row write per 2-second chunk -> 43,200 writes/day for continuous 24h streaming (43.2% of 100k limit)
CREATE TABLE IF NOT EXISTS chunks (
  session_id TEXT NOT NULL,
  chunk_idx INTEGER NOT NULL,
  from_index INTEGER NOT NULL,
  to_index INTEGER NOT NULL,
  mode TEXT NOT NULL,         -- 'online' | 'offline'
  s1_encoded TEXT NOT NULL,   -- Base64 packed signed 24-bit samples (800 samples x 3 channels)
  s2_encoded TEXT NOT NULL,   -- Base64 packed signed 24-bit samples (800 samples x 2 channels)
  s1_mask TEXT NOT NULL,      -- 100-byte availability bitmask for Sensor 1
  s2_mask TEXT NOT NULL,      -- 100-byte availability bitmask for Sensor 2
  lost_json TEXT NOT NULL,    -- Explicit loss ranges covered by chunk
  results_json TEXT NOT NULL, -- JSON array of WindowResult objects for constituent windows (2 per chunk)
  digest TEXT NOT NULL,       -- SHA-256 digest of canonical raw payload for idempotency & conflict verification
  created_at INTEGER NOT NULL,
  PRIMARY KEY (session_id, chunk_idx)
) WITHOUT ROWID;

-- Integrity incident audit table for recording rejected conflicting payloads
CREATE TABLE IF NOT EXISTS cloud_incidents (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  session_id TEXT NOT NULL,
  chunk_idx INTEGER NOT NULL,
  incident_type TEXT NOT NULL,
  details TEXT NOT NULL,
  created_at INTEGER NOT NULL
);
