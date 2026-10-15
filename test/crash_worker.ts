import { DatabaseSync } from 'node:sqlite';

const dbPath = process.argv[2];
if (!dbPath) {
  console.error('Error: missing dbPath');
  process.exit(1);
}

const db = new DatabaseSync(dbPath);
db.exec('PRAGMA journal_mode = WAL;');
db.exec('PRAGMA synchronous = FULL;');

db.exec(`
  CREATE TABLE IF NOT EXISTS samples (
    session_id TEXT,
    sensor INT,
    sample_idx INT,
    ch1 INT,
    ch2 INT,
    ch3 INT,
    created_at INT,
    PRIMARY KEY(session_id, sensor, sample_idx)
  );
  CREATE TABLE IF NOT EXISTS progress (
    session_id TEXT,
    sensor INT,
    resolved_watermark INT,
    next_expected_index INT,
    updated_at INT,
    PRIMARY KEY(session_id, sensor)
  );
`);

// 1. Durably commit batch 1: samples 0 through 399
db.exec('BEGIN IMMEDIATE;');
for (let i = 0; i < 400; i++) {
  db.prepare('INSERT INTO samples VALUES (?, ?, ?, ?, ?, ?, ?);').run('sess-crash', 1, i, 100 + i, 200 + i, 300 + i, Date.now());
}
db.prepare('INSERT OR REPLACE INTO progress VALUES (?, ?, ?, ?, ?);').run('sess-crash', 1, 399, 400, Date.now());
db.exec('COMMIT;');

console.log('BATCH_1_COMMITTED');

// 2. Start batch 2: insert 200 samples and crash BEFORE commit
db.exec('BEGIN IMMEDIATE;');
for (let i = 400; i < 600; i++) {
  db.prepare('INSERT INTO samples VALUES (?, ?, ?, ?, ?, ?, ?);').run('sess-crash', 1, i, 100 + i, 200 + i, 300 + i, Date.now());
}

// Abrupt crash during active transaction
console.log('CRASHING_NOW');
process.abort(); // Triggers immediate abort / SIGABRT crash
