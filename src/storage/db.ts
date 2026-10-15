import { DatabaseSync } from 'node:sqlite';
import * as fs from 'node:fs';
import type { SensorId, MockResponse, RawSample, LossRange, StoredLossRecord, WindowResult, ProcessingCheckpoint } from '../types.ts';

export interface StorageOptions {
  dbPath: string;
  busyTimeoutMs?: number;
}

export interface PersistResult {
  success: boolean;
  newSamplesCount: number;
  duplicateSamplesCount: number;
  conflictSamplesCount: number;
  newLossesCount: number;
  resolvedWatermark: number;
  nextExpectedIndex: number;
  error?: string;
}

export interface AuditResult {
  pass: boolean;
  sessionId: string;
  sensor: SensorId;
  totalSamples: number;
  totalLossRanges: number;
  resolvedWatermark: number;
  highestStoredIndex: number;
  unaccountedGaps: Array<{ from_index: number; to_index: number }>;
  integrityIncidents: Array<{ index: number; incidentType: string; details: string }>;
}

export class StorageEngine {
  private db: DatabaseSync;
  private dbPath: string;
  private busyTimeoutMs: number;

  constructor(opts: StorageOptions) {
    this.dbPath = opts.dbPath;
    this.busyTimeoutMs = opts.busyTimeoutMs ?? 5000;
    this.db = new DatabaseSync(this.dbPath);
    this.init();
  }

  private init() {
    // 1. Configure and enforce durability pragmas
    this.db.exec('PRAGMA journal_mode = WAL;');
    this.db.exec('PRAGMA synchronous = FULL;');
    this.db.exec('PRAGMA foreign_keys = ON;');
    this.db.exec(`PRAGMA busy_timeout = ${this.busyTimeoutMs};`);

    // Verify active pragmas
    const jMode = (this.db.prepare('PRAGMA journal_mode;').get() as any)?.journal_mode;
    const sync = (this.db.prepare('PRAGMA synchronous;').get() as any)?.synchronous;
    if (jMode !== 'wal') {
      throw new Error(`Failed to set WAL journal mode, active mode: ${jMode}`);
    }
    // sync 2 is FULL
    if (sync !== 2) {
      throw new Error(`Failed to set synchronous=FULL, active setting: ${sync}`);
    }

    // 2. Execute migrations
    this.migrate();

    // 3. Run integrity check
    const check = (this.db.prepare('PRAGMA integrity_check;').get() as any)?.integrity_check;
    if (check !== 'ok') {
      throw new Error(`SQLite integrity check failed on startup: ${check}`);
    }
  }

  private migrate() {
    this.db.exec(`
      CREATE TABLE IF NOT EXISTS sessions (
        session_id TEXT PRIMARY KEY,
        created_at INTEGER NOT NULL,
        last_seen_at INTEGER NOT NULL
      );

      CREATE TABLE IF NOT EXISTS samples (
        session_id TEXT NOT NULL,
        sensor INTEGER NOT NULL,
        sample_idx INTEGER NOT NULL,
        ch1 INTEGER NOT NULL,
        ch2 INTEGER NOT NULL,
        ch3 INTEGER, -- NULL for Sensor 2
        created_at INTEGER NOT NULL,
        PRIMARY KEY (session_id, sensor, sample_idx)
      );

      CREATE TABLE IF NOT EXISTS losses (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        session_id TEXT NOT NULL,
        sensor INTEGER NOT NULL,
        from_index INTEGER NOT NULL,
        to_index INTEGER, -- NULL when genuinely unknown endpoint
        reason TEXT NOT NULL,
        source TEXT NOT NULL,
        created_at INTEGER NOT NULL
      );

      CREATE UNIQUE INDEX IF NOT EXISTS uq_losses ON losses (
        session_id,
        sensor,
        from_index,
        COALESCE(to_index, -1),
        reason
      );

      CREATE TABLE IF NOT EXISTS progress (
        session_id TEXT NOT NULL,
        sensor INTEGER NOT NULL,
        resolved_watermark INTEGER NOT NULL, -- highest contiguous resolved index (-1 if none)
        next_expected_index INTEGER NOT NULL,
        updated_at INTEGER NOT NULL,
        PRIMARY KEY (session_id, sensor)
      );

      CREATE TABLE IF NOT EXISTS integrity_incidents (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        session_id TEXT NOT NULL,
        sensor INTEGER NOT NULL,
        sample_idx INTEGER NOT NULL,
        incident_type TEXT NOT NULL,
        existing_data TEXT,
        incoming_data TEXT,
        details TEXT,
        created_at INTEGER NOT NULL
      );

      CREATE TABLE IF NOT EXISTS processing_results (
        session_id TEXT NOT NULL,
        window_idx INTEGER NOT NULL,
        mode TEXT NOT NULL,
        missing_1 INTEGER NOT NULL,
        missing_2 INTEGER NOT NULL,
        complete INTEGER NOT NULL,
        result_json TEXT NOT NULL,
        created_at INTEGER NOT NULL,
        PRIMARY KEY (session_id, window_idx)
      );

      CREATE TABLE IF NOT EXISTS detected_peaks (
        session_id TEXT NOT NULL,
        peak_idx INTEGER NOT NULL,
        created_at INTEGER NOT NULL,
        PRIMARY KEY (session_id, peak_idx)
      );

      CREATE TABLE IF NOT EXISTS processing_checkpoints (
        session_id TEXT PRIMARY KEY,
        last_processed_window INTEGER NOT NULL,
        last_accepted_peak INTEGER NOT NULL,
        updated_at INTEGER NOT NULL
      );

      CREATE TABLE IF NOT EXISTS cloud_sync_progress (
        session_id TEXT NOT NULL,
        chunk_idx INTEGER NOT NULL,
        mode TEXT NOT NULL,
        synced_at INTEGER NOT NULL,
        PRIMARY KEY (session_id, chunk_idx)
      );

      CREATE TABLE IF NOT EXISTS pending_loss_reasons (
        session_id TEXT NOT NULL,
        sensor INTEGER NOT NULL,
        from_index INTEGER NOT NULL,
        reason TEXT NOT NULL,
        created_at INTEGER NOT NULL,
        PRIMARY KEY (session_id, sensor, from_index)
      );
    `);
  }

  /**
   * Persist a validated Phase 1 reply atomically.
   */
  public persistValidatedReply(reply: MockResponse): PersistResult {
    const now = Date.now();
    let newSamplesCount = 0;
    let duplicateSamplesCount = 0;
    let conflictSamplesCount = 0;
    let newLossesCount = 0;

    const findSampleStmt = this.db.prepare(
      'SELECT ch1, ch2, ch3 FROM samples WHERE session_id = ? AND sensor = ? AND sample_idx = ?'
    );
    const insertSampleStmt = this.db.prepare(
      'INSERT INTO samples (session_id, sensor, sample_idx, ch1, ch2, ch3, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)'
    );
    const insertIncidentStmt = this.db.prepare(
      'INSERT INTO integrity_incidents (session_id, sensor, sample_idx, incident_type, existing_data, incoming_data, details, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)'
    );
    const insertLossStmt = this.db.prepare(
      'INSERT OR IGNORE INTO losses (session_id, sensor, from_index, to_index, reason, source, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)'
    );
    const upsertSessionStmt = this.db.prepare(`
      INSERT INTO sessions (session_id, created_at, last_seen_at)
      VALUES (?, ?, ?)
      ON CONFLICT(session_id) DO UPDATE SET last_seen_at = excluded.last_seen_at
    `);
    const getProgressStmt = this.db.prepare(
      'SELECT resolved_watermark, next_expected_index FROM progress WHERE session_id = ? AND sensor = ?'
    );
    const upsertProgressStmt = this.db.prepare(`
      INSERT INTO progress (session_id, sensor, resolved_watermark, next_expected_index, updated_at)
      VALUES (?, ?, ?, ?, ?)
      ON CONFLICT(session_id, sensor) DO UPDATE SET
        resolved_watermark = excluded.resolved_watermark,
        next_expected_index = excluded.next_expected_index,
        updated_at = excluded.updated_at
    `);

    const maxRetries = 3;
    for (let attempt = 0; attempt < maxRetries; attempt++) {
      newSamplesCount = 0;
      duplicateSamplesCount = 0;
      conflictSamplesCount = 0;
      newLossesCount = 0;

      try {
        this.db.exec('BEGIN IMMEDIATE;');

        // 1. Track Session

      upsertSessionStmt.run(reply.session_id, now, now);

      // 2. Insert Samples & Detect Duplicates/Conflicts
      for (let r = 0; r < reply.data.length; r++) {
        const sampleIdx = reply.first_index + r;
        const row = reply.data[r];
        const ch1 = row[0];
        const ch2 = row[1];
        const ch3 = reply.sensor === 1 ? row[2] : null;

        const existing = findSampleStmt.get(reply.session_id, reply.sensor, sampleIdx) as any;

        if (!existing) {
          // New sample
          insertSampleStmt.run(reply.session_id, reply.sensor, sampleIdx, ch1, ch2, ch3, now);
          newSamplesCount++;
        } else {
          // Identity collision: compare values
          const match =
            existing.ch1 === ch1 &&
            existing.ch2 === ch2 &&
            (reply.sensor === 2 || existing.ch3 === ch3);

          if (match) {
            duplicateSamplesCount++;
          } else {
            // Integrity Conflict: record incident without overwriting stored sample
            conflictSamplesCount++;
            insertIncidentStmt.run(
              reply.session_id,
              reply.sensor,
              sampleIdx,
              'CONFLICTING_SAMPLE',
              JSON.stringify(existing),
              JSON.stringify(row),
              `Conflicting sample values received at index ${sampleIdx}`,
              now
            );
          }
        }
      }

      // 3. Detect and Record Unreported Ingest Gaps in the same transaction
      const progressRow = getProgressStmt.get(reply.session_id, reply.sensor) as any;
      let currentWatermark = progressRow ? progressRow.resolved_watermark : -1;
      let currentExpected = progressRow ? progressRow.next_expected_index : 0;

      if (reply.first_index > currentExpected) {
        const gapStart = currentExpected;
        const gapEnd = reply.first_index - 1;

        // Subdivide gap by intervals covered in reply.lost
        const coveredIntervals: Array<{ from: number; to: number }> = [];
        for (const lr of reply.lost) {
          const from = Math.max(gapStart, lr.from_index);
          const to = lr.to_index === null ? gapEnd : Math.min(gapEnd, lr.to_index);
          if (from <= to) {
            coveredIntervals.push({ from, to });
          }
        }
        coveredIntervals.sort((a, b) => a.from - b.from);

        // Merge overlapping covered intervals
        const mergedCovered: Array<{ from: number; to: number }> = [];
        for (const int of coveredIntervals) {
          if (mergedCovered.length === 0) {
            mergedCovered.push({ ...int });
          } else {
            const prev = mergedCovered[mergedCovered.length - 1];
            if (int.from <= prev.to + 1) {
              prev.to = Math.max(prev.to, int.to);
            } else {
              mergedCovered.push({ ...int });
            }
          }
        }

        // Find uncovered slices in [gapStart, gapEnd]
        let cursor = gapStart;
        for (const cov of mergedCovered) {
          if (cov.from > cursor) {
            const uStart = cursor;
            const uEnd = cov.from - 1;
            const reason = uStart === 0 ? 'attached_late' : (this.getAndClearPendingLossReason(reply.session_id, reply.sensor, uStart) || 'gap_unreported_by_mock');
            insertLossStmt.run(reply.session_id, reply.sensor, uStart, uEnd, reason, 'pipeline', now);
            newLossesCount++;
          }
          cursor = Math.max(cursor, cov.to + 1);
        }
        if (cursor <= gapEnd) {
          const uStart = cursor;
          const uEnd = gapEnd;
          const reason = uStart === 0 ? 'attached_late' : (this.getAndClearPendingLossReason(reply.session_id, reply.sensor, uStart) || 'gap_unreported_by_mock');
          insertLossStmt.run(reply.session_id, reply.sensor, uStart, uEnd, reason, 'pipeline', now);
          newLossesCount++;
        }
      }

      // 4. Persist Valid Reported Losses Idempotently
      for (const lr of reply.lost) {
        const prevChanges = (this.db.prepare('SELECT changes() as c;').get() as any)?.c;
        insertLossStmt.run(
          reply.session_id,
          reply.sensor,
          lr.from_index,
          lr.to_index,
          lr.reason,
          'mock_reported',
          now
        );
        const postChanges = (this.db.prepare('SELECT changes() as c;').get() as any)?.c;
        if (postChanges > 0) {
          newLossesCount++;
        }
      }

      // 5. Advance Contiguous Resolved Watermark

      const newWatermark = this.computeContiguousWatermark(
        reply.session_id,
        reply.sensor,
        currentWatermark
      );

      const updatedExpected = Math.max(currentExpected, reply.first_index + reply.count);

      upsertProgressStmt.run(
        reply.session_id,
        reply.sensor,
        newWatermark,
        updatedExpected,
        now
      );

      this.db.exec('COMMIT;');

      return {
        success: true,
        newSamplesCount,
        duplicateSamplesCount,
        conflictSamplesCount,
        newLossesCount,
        resolvedWatermark: newWatermark,
        nextExpectedIndex: updatedExpected,
      };
    } catch (err: any) {
      try {
        this.db.exec('ROLLBACK;');
      } catch {
        // Ignore rollback failure if already rolled back
      }
      const isBusy = err?.message?.toLowerCase().includes('busy') || err?.code === 'SQLITE_BUSY';
      if (isBusy && attempt < maxRetries - 1) {
        Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, 10);
        continue;
      }
      return {
        success: false,
        newSamplesCount: 0,
        duplicateSamplesCount: 0,
        conflictSamplesCount: 0,
        newLossesCount: 0,
        resolvedWatermark: -1,
        nextExpectedIndex: 0,
        error: err.message,
      };
    }
  }

  return {
    success: false,
    newSamplesCount: 0,
    duplicateSamplesCount: 0,
    conflictSamplesCount: 0,
    newLossesCount: 0,
    resolvedWatermark: -1,
    nextExpectedIndex: 0,
    error: 'Max lock retries exhausted',
  };
}

  /**
   * Compute contiguous resolved watermark from a starting point.
   */
  public computeContiguousWatermark(
    sessionId: string,
    sensor: SensorId,
    startWatermark: number
  ): number {
    const lookaheadLimit = 10000;
    const startIdx = startWatermark + 1;

    // Fetch sample indices in ascending order
    const sampleRows = this.db
      .prepare(
        'SELECT sample_idx FROM samples WHERE session_id = ? AND sensor = ? AND sample_idx >= ? ORDER BY sample_idx LIMIT ?'
      )
      .all(sessionId, sensor, startIdx, lookaheadLimit) as any[];

    const sampleSet = new Set<number>();
    for (const r of sampleRows) {
      sampleSet.add(r.sample_idx);
    }

    // Fetch loss ranges
    const lossRows = this.db
      .prepare(
        'SELECT from_index, to_index FROM losses WHERE session_id = ? AND sensor = ? AND (to_index IS NULL OR to_index >= ?) ORDER BY from_index'
      )
      .all(sessionId, sensor, startIdx) as any[];

    let current = startIdx;
    while (true) {
      if (sampleSet.has(current)) {
        current++;
        continue;
      }

      // Check if covered by a loss record
      const lossMatch = lossRows.find(
        (lr) => current >= lr.from_index && (lr.to_index === null || current <= lr.to_index)
      );

      if (lossMatch) {
        if (lossMatch.to_index === null) {
          // Open-ended loss (e.g. session restart tail lost): session watermark terminates here
          break;
        }
        current = lossMatch.to_index + 1;
        continue;
      }

      // Neither present nor lost: unresolved gap!
      break;
    }

    return current - 1;
  }

  /**
   * Explicitly record an unrecoverable loss and advance watermark.
   */
  public recordLoss(
    sessionId: string,
    sensor: SensorId,
    fromIndex: number,
    toIndex: number | null,
    reason: string,
    source: string = 'pipeline'
  ): void {
    const now = Date.now();
    try {
      this.db.exec('BEGIN IMMEDIATE;');

      this.db
        .prepare(
          'INSERT OR IGNORE INTO losses (session_id, sensor, from_index, to_index, reason, source, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)'
        )
        .run(sessionId, sensor, fromIndex, toIndex, reason, source, now);

      const progressRow = this.db
        .prepare(
          'SELECT resolved_watermark, next_expected_index FROM progress WHERE session_id = ? AND sensor = ?'
        )
        .get(sessionId, sensor) as any;

      let currentWatermark = progressRow ? progressRow.resolved_watermark : -1;
      let currentExpected = progressRow ? progressRow.next_expected_index : 0;

      const newWatermark = this.computeContiguousWatermark(sessionId, sensor, currentWatermark);

      this.db
        .prepare(`
          INSERT INTO progress (session_id, sensor, resolved_watermark, next_expected_index, updated_at)
          VALUES (?, ?, ?, ?, ?)
          ON CONFLICT(session_id, sensor) DO UPDATE SET
            resolved_watermark = excluded.resolved_watermark,
            updated_at = excluded.updated_at
        `)
        .run(sessionId, sensor, newWatermark, currentExpected, now);

      this.db.exec('COMMIT;');
    } catch (err) {
      try {
        this.db.exec('ROLLBACK;');
      } catch {}
      throw err;
    }
  }

  /**
   * Reconstruct progress from durable records on startup.
   */
  public getProgress(
    sessionId: string,
    sensor: SensorId
  ): { resolvedWatermark: number; nextExpectedIndex: number } {
    const row = this.db
      .prepare(
        'SELECT resolved_watermark, next_expected_index FROM progress WHERE session_id = ? AND sensor = ?'
      )
      .get(sessionId, sensor) as any;

    if (row) {
      return {
        resolvedWatermark: row.resolved_watermark,
        nextExpectedIndex: row.next_expected_index,
      };
    }

    // Compute from stored samples and losses
    const watermark = this.computeContiguousWatermark(sessionId, sensor, -1);
    const maxIdxRow = this.db
      .prepare(
        'SELECT MAX(sample_idx) as max_idx FROM samples WHERE session_id = ? AND sensor = ?'
      )
      .get(sessionId, sensor) as any;
    const nextExpected = maxIdxRow?.max_idx !== null && maxIdxRow?.max_idx !== undefined ? maxIdxRow.max_idx + 1 : 0;

    return { resolvedWatermark: watermark, nextExpectedIndex: nextExpected };
  }

  /**
   * Run an integrity audit for a session and sensor.
   */
  public auditSession(sessionId: string, sensor: SensorId): AuditResult {
    const countRow = this.db
      .prepare(
        'SELECT COUNT(*) as cnt, MAX(sample_idx) as max_idx FROM samples WHERE session_id = ? AND sensor = ?'
      )
      .get(sessionId, sensor) as any;

    const totalSamples = countRow?.cnt || 0;
    const highestStoredIndex = countRow?.max_idx !== null && countRow?.max_idx !== undefined ? countRow.max_idx : -1;

    const lossRows = this.db
      .prepare(
        'SELECT from_index, to_index FROM losses WHERE session_id = ? AND sensor = ? AND (to_index IS NULL OR to_index >= 0)'
      )
      .all(sessionId, sensor) as any[];
    const totalLossRanges = lossRows.length;

    let maxRecordedLoss = -1;
    for (const l of lossRows) {
      if (l.to_index !== null) {
        maxRecordedLoss = Math.max(maxRecordedLoss, l.to_index);
      } else {
        maxRecordedLoss = Math.max(maxRecordedLoss, l.from_index);
      }
    }

    const progress = this.getProgress(sessionId, sensor);
    const highestExpected = progress.nextExpectedIndex - 1;
    const highWatermark = Math.max(highestStoredIndex, highestExpected, maxRecordedLoss);

    const incidents = (this.db
      .prepare(
        'SELECT sample_idx, incident_type, details FROM integrity_incidents WHERE session_id = ? AND sensor = ?'
      )
      .all(sessionId, sensor) as any[]).map((r) => ({
      index: r.sample_idx,
      incidentType: r.incident_type,
      details: r.details,
    }));

    // Find any gaps between 0 and highWatermark (invariant: every index below high watermark is stored or in a loss range)
    const unaccountedGaps: Array<{ from_index: number; to_index: number }> = [];
    if (highWatermark >= 0) {
      const sampleRows = this.db
        .prepare(
          'SELECT sample_idx FROM samples WHERE session_id = ? AND sensor = ? AND sample_idx <= ?'
        )
        .all(sessionId, sensor, highWatermark) as any[];

      const storedSet = new Set(sampleRows.map((r) => r.sample_idx));

      let current = 0;
      while (current <= highWatermark) {
        if (storedSet.has(current)) {
          current++;
          continue;
        }

        const match = lossRows.find((l) => current >= l.from_index && (l.to_index === null || current <= l.to_index));
        if (match) {
          if (match.to_index === null) {
            break;
          }
          current = match.to_index + 1;
          continue;
        }

        const gapStart = current;
        while (current <= highWatermark && !storedSet.has(current) && !lossRows.some((l) => current >= l.from_index && (l.to_index === null || current <= l.to_index))) {
          current++;
        }
        unaccountedGaps.push({ from_index: gapStart, to_index: current - 1 });
      }
    }

    const pass = unaccountedGaps.length === 0 && incidents.length === 0;

    return {
      pass,
      sessionId,
      sensor,
      totalSamples,
      totalLossRanges,
      resolvedWatermark: progress.resolvedWatermark,
      highestStoredIndex,
      unaccountedGaps,
      integrityIncidents: incidents,
    };
  }

  /**
   * Stream / fetch samples in index order.
   */
  public getSamples(
    sessionId: string,
    sensor: SensorId,
    fromIndex: number = 0,
    limit: number = 1000
  ): RawSample[] {
    const rows = this.db
      .prepare(
        'SELECT sample_idx, ch1, ch2, ch3 FROM samples WHERE session_id = ? AND sensor = ? AND sample_idx >= ? ORDER BY sample_idx LIMIT ?'
      )
      .all(sessionId, sensor, fromIndex, limit) as any[];

    return rows.map((r) => ({
      session_id: sessionId,
      sensor,
      index: r.sample_idx,
      values: sensor === 1 ? [r.ch1, r.ch2, r.ch3] : [r.ch1, r.ch2],
    }));
  }

  /**
   * Fetch samples in an index range [fromIndex, toIndex] in index order.
   */
  public getSamplesRange(
    sessionId: string,
    sensor: SensorId,
    fromIndex: number,
    toIndex: number
  ): RawSample[] {
    const rows = this.db
      .prepare(
        'SELECT sample_idx, ch1, ch2, ch3 FROM samples WHERE session_id = ? AND sensor = ? AND sample_idx >= ? AND sample_idx <= ? ORDER BY sample_idx'
      )
      .all(sessionId, sensor, fromIndex, toIndex) as any[];

    return rows.map((r) => ({
      session_id: sessionId,
      sensor,
      index: r.sample_idx,
      values: sensor === 1 ? [r.ch1, r.ch2, r.ch3] : [r.ch1, r.ch2],
    }));
  }

  /**
   * Fetch loss records for export.
   */
  public getLosses(sessionId: string, sensor?: SensorId): StoredLossRecord[] {
    let query = 'SELECT sensor, from_index, to_index, reason, source FROM losses WHERE session_id = ?';
    const params: any[] = [sessionId];
    if (sensor !== undefined) {
      query += ' AND sensor = ?';
      params.push(sensor);
    }
    query += ' ORDER BY from_index';

    const rows = this.db.prepare(query).all(...params) as any[];
    return rows.map((r) => ({
      session_id: sessionId,
      sensor: r.sensor as SensorId,
      from_index: r.from_index,
      to_index: r.to_index,
      reason: r.reason,
      source: r.source,
    }));
  }

  public setPendingLossReason(
    sessionId: string,
    sensor: SensorId,
    fromIndex: number,
    reason: string
  ): void {
    const now = Date.now();
    this.db.prepare(`
      INSERT INTO pending_loss_reasons (session_id, sensor, from_index, reason, created_at)
      VALUES (?, ?, ?, ?, ?)
      ON CONFLICT(session_id, sensor, from_index) DO UPDATE SET reason = excluded.reason
    `).run(sessionId, sensor, fromIndex, reason, now);
  }

  public getAndClearPendingLossReason(
    sessionId: string,
    sensor: SensorId,
    fromIndex: number
  ): string | null {
    const row = this.db.prepare(
      'SELECT reason FROM pending_loss_reasons WHERE session_id = ? AND sensor = ? AND from_index = ?'
    ).get(sessionId, sensor, fromIndex) as any;
    if (row) {
      this.db.prepare(
        'DELETE FROM pending_loss_reasons WHERE session_id = ? AND sensor = ? AND from_index = ?'
      ).run(sessionId, sensor, fromIndex);
      return row.reason;
    }
    return null;
  }

  /**
   * Fetch all loss records for export, guaranteeing the invariant check:
   * for every session/sensor, every index below high watermark is stored or in a loss range;
   * emits "unaccounted_gap" loss lines if any holes exist.
   */
  public getExportLosses(sessionId: string, sensor?: SensorId): StoredLossRecord[] {
    const sensorsToScan: SensorId[] = sensor !== undefined ? [sensor] : [1, 2];
    const allLosses: StoredLossRecord[] = [];

    for (const s of sensorsToScan) {
      const recorded = this.getLosses(sessionId, s);
      const sampleMaxRow = this.db.prepare(
        'SELECT MAX(sample_idx) as m FROM samples WHERE session_id = ? AND sensor = ?'
      ).get(sessionId, s) as any;
      const highestSample = sampleMaxRow?.m !== null && sampleMaxRow?.m !== undefined ? sampleMaxRow.m : -1;

      const progress = this.getProgress(sessionId, s);
      const highestExpected = progress.nextExpectedIndex - 1;

      let maxRecordedLoss = -1;
      for (const l of recorded) {
        if (l.to_index !== null) {
          maxRecordedLoss = Math.max(maxRecordedLoss, l.to_index);
        } else {
          maxRecordedLoss = Math.max(maxRecordedLoss, l.from_index);
        }
      }

      const highWatermark = Math.max(highestSample, highestExpected, maxRecordedLoss);

      if (highWatermark < 0) {
        allLosses.push(...recorded);
        continue;
      }

      // Collect sample set
      const sampleRows = this.db.prepare(
        'SELECT sample_idx FROM samples WHERE session_id = ? AND sensor = ? AND sample_idx <= ?'
      ).all(sessionId, s, highWatermark) as any[];
      const sampleSet = new Set<number>();
      for (const r of sampleRows) {
        sampleSet.add(r.sample_idx);
      }

      // Collect covered ranges
      const coveredRanges: Array<{ from: number; to: number }> = [];
      for (const l of recorded) {
        const to = l.to_index === null ? highWatermark : l.to_index;
        coveredRanges.push({ from: l.from_index, to });
      }

      // Detect any holes in [0, highWatermark]
      const synthesizedLosses: StoredLossRecord[] = [];
      let current = 0;
      while (current <= highWatermark) {
        if (sampleSet.has(current)) {
          current++;
          continue;
        }

        const lossMatch = coveredRanges.find(
          (r) => current >= r.from && current <= r.to
        );
        if (lossMatch) {
          current = lossMatch.to + 1;
          continue;
        }

        // Neither present in samples nor inside a loss range: hole found!
        const holeStart = current;
        while (current <= highWatermark && !sampleSet.has(current) && !coveredRanges.some((r) => current >= r.from && current <= r.to)) {
          current++;
        }
        const holeEnd = current - 1;

        synthesizedLosses.push({
          session_id: sessionId,
          sensor: s,
          from_index: holeStart,
          to_index: holeEnd,
          reason: 'unaccounted_gap',
          source: 'pipeline',
        });
      }

      allLosses.push(...recorded, ...synthesizedLosses);
    }

    allLosses.sort((a, b) => {
      if (a.sensor !== b.sensor) return a.sensor - b.sensor;
      return a.from_index - b.from_index;
    });

    return allLosses;
  }

  public getSessionIds(): string[] {
    const rows = this.db.prepare('SELECT session_id FROM sessions ORDER BY created_at DESC').all() as any[];
    return rows.map((r) => r.session_id);
  }

  public getLatestSessionId(): string | null {
    const row = this.db.prepare('SELECT session_id FROM sessions ORDER BY last_seen_at DESC LIMIT 1').get() as any;
    if (row) return row.session_id;
    const maxSample = this.db.prepare('SELECT session_id FROM samples ORDER BY created_at DESC LIMIT 1').get() as any;
    return maxSample ? maxSample.session_id : null;
  }

  public sessionExists(sessionId: string): boolean {
    const row = this.db.prepare('SELECT 1 FROM sessions WHERE session_id = ?').get(sessionId);
    if (row) return true;
    const sRow = this.db.prepare('SELECT 1 FROM samples WHERE session_id = ? LIMIT 1').get(sessionId);
    if (sRow) return true;
    const lRow = this.db.prepare('SELECT 1 FROM losses WHERE session_id = ? LIMIT 1').get(sessionId);
    if (lRow) return true;
    const rRow = this.db.prepare('SELECT 1 FROM processing_results WHERE session_id = ? LIMIT 1').get(sessionId);
    return !!rRow;
  }

  public getSampleCounts(sessionId?: string): { '1': number; '2': number } {
    if (sessionId) {
      const row1 = this.db.prepare('SELECT COUNT(*) as c FROM samples WHERE session_id = ? AND sensor = 1').get(sessionId) as any;
      const row2 = this.db.prepare('SELECT COUNT(*) as c FROM samples WHERE session_id = ? AND sensor = 2').get(sessionId) as any;
      return { '1': row1?.c || 0, '2': row2?.c || 0 };
    }
    const row1 = this.db.prepare('SELECT COUNT(*) as c FROM samples WHERE sensor = 1').get() as any;
    const row2 = this.db.prepare('SELECT COUNT(*) as c FROM samples WHERE sensor = 2').get() as any;
    return { '1': row1?.c || 0, '2': row2?.c || 0 };
  }

  public getConfirmedCloudSampleCount(sessionId?: string): number {
    let query = 'SELECT COUNT(*) as c FROM cloud_sync_progress';
    const params: any[] = [];
    if (sessionId) {
      query += ' WHERE session_id = ?';
      params.push(sessionId);
    }
    const row = this.db.prepare(query).get(...params) as any;
    const chunksCount = row?.c || 0;
    // Each 2-second chunk covers 800 paired sample positions
    return chunksCount * 800;
  }

  public getDbFileSize(): number {
    try {
      const stats = fs.statSync(this.dbPath);
      return stats.size;
    } catch {
      return 0;
    }
  }

  public getWalFileSize(): number {
    try {
      const stats = fs.statSync(`${this.dbPath}-wal`);
      return stats.size;
    } catch {
      return 0;
    }
  }

  /**
   * Save a window processing result, any newly accepted peaks, and checkpoint atomically.
   */
  public saveProcessingResult(
    result: WindowResult,
    newPeaks: number[],
    checkpointWindow: number,
    checkpointPeak: number
  ): void {
    const now = Date.now();
    try {
      this.db.exec('BEGIN IMMEDIATE;');

      this.db
        .prepare(`
          INSERT INTO processing_results (
            session_id, window_idx, mode, missing_1, missing_2, complete, result_json, created_at
          ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
          ON CONFLICT(session_id, window_idx) DO NOTHING
        `)
        .run(
          result.session_id,
          result.window_idx,
          result.mode,
          result.missing_1,
          result.missing_2,
          result.complete ? 1 : 0,
          JSON.stringify(result),
          now
        );

      const insertPeakStmt = this.db.prepare(
        'INSERT OR IGNORE INTO detected_peaks (session_id, peak_idx, created_at) VALUES (?, ?, ?)'
      );
      for (const p of newPeaks) {
        insertPeakStmt.run(result.session_id, p, now);
      }

      this.db
        .prepare(`
          INSERT INTO processing_checkpoints (session_id, last_processed_window, last_accepted_peak, updated_at)
          VALUES (?, ?, ?, ?)
          ON CONFLICT(session_id) DO UPDATE SET
            last_processed_window = MAX(excluded.last_processed_window, processing_checkpoints.last_processed_window),
            last_accepted_peak = CASE
              WHEN excluded.last_processed_window >= processing_checkpoints.last_processed_window THEN excluded.last_accepted_peak
              ELSE processing_checkpoints.last_accepted_peak
            END,
            updated_at = excluded.updated_at
        `)
        .run(result.session_id, checkpointWindow, checkpointPeak, now);

      this.db.exec('COMMIT;');
    } catch (err) {
      try {
        this.db.exec('ROLLBACK;');
      } catch {}
      throw err;
    }
  }

  public getProcessingResult(sessionId: string, windowIdx: number): WindowResult | null {
    const row = this.db
      .prepare('SELECT result_json FROM processing_results WHERE session_id = ? AND window_idx = ?')
      .get(sessionId, windowIdx) as any;

    if (!row) return null;
    return JSON.parse(row.result_json);
  }

  public getProcessingResults(
    sessionId: string,
    fromWindow: number = 0,
    limit: number = 1000
  ): WindowResult[] {
    const rows = this.db
      .prepare(
        'SELECT result_json FROM processing_results WHERE session_id = ? AND window_idx >= ? ORDER BY window_idx LIMIT ?'
      )
      .all(sessionId, fromWindow, limit) as any[];

    return rows.map((r) => JSON.parse(r.result_json));
  }

  public getDetectedPeaks(sessionId: string, fromIndex: number = 0, toIndex?: number): number[] {
    let query = 'SELECT peak_idx FROM detected_peaks WHERE session_id = ? AND peak_idx >= ?';
    const params: any[] = [sessionId, fromIndex];
    if (toIndex !== undefined) {
      query += ' AND peak_idx <= ?';
      params.push(toIndex);
    }
    query += ' ORDER BY peak_idx';

    const rows = this.db.prepare(query).all(...params) as any[];
    return rows.map((r) => r.peak_idx);
  }

  public getProcessingCheckpoint(sessionId: string): ProcessingCheckpoint | null {
    const row = this.db
      .prepare(
        'SELECT session_id, last_processed_window, last_accepted_peak, updated_at FROM processing_checkpoints WHERE session_id = ?'
      )
      .get(sessionId) as any;

    if (!row) return null;
    return {
      session_id: row.session_id,
      last_processed_window: row.last_processed_window,
      last_accepted_peak: row.last_accepted_peak,
      updated_at: row.updated_at,
    };
  }

  public saveProcessingCheckpoint(checkpoint: ProcessingCheckpoint): void {
    this.db
      .prepare(`
        INSERT INTO processing_checkpoints (session_id, last_processed_window, last_accepted_peak, updated_at)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(session_id) DO UPDATE SET
          last_processed_window = MAX(excluded.last_processed_window, processing_checkpoints.last_processed_window),
          last_accepted_peak = CASE
            WHEN excluded.last_processed_window >= processing_checkpoints.last_processed_window THEN excluded.last_accepted_peak
            ELSE processing_checkpoints.last_accepted_peak
          END,
          updated_at = excluded.updated_at
      `)
      .run(
        checkpoint.session_id,
        checkpoint.last_processed_window,
        checkpoint.last_accepted_peak,
        checkpoint.updated_at
      );
  }

  public getSampleRange(
    sessionId: string,
    sensor: SensorId,
    fromIndex: number,
    toIndex: number
  ): Map<number, number[]> {
    const rows = this.db
      .prepare(
        'SELECT sample_idx, ch1, ch2, ch3 FROM samples WHERE session_id = ? AND sensor = ? AND sample_idx >= ? AND sample_idx <= ? ORDER BY sample_idx'
      )
      .all(sessionId, sensor, fromIndex, toIndex) as any[];

    const map = new Map<number, number[]>();
    for (const r of rows) {
      map.set(r.sample_idx, sensor === 1 ? [r.ch1, r.ch2, r.ch3] : [r.ch1, r.ch2]);
    }
    return map;
  }

  /**
   * Check whether all indices in [fromIndex, toIndex] are stored as valid samples (no loss, no gap).
   */
  public isRangeFullyAvailable(
    sessionId: string,
    sensor: SensorId,
    fromIndex: number,
    toIndex: number
  ): boolean {
    if (toIndex < fromIndex) return true;
    const expectedCount = toIndex - fromIndex + 1;
    const row = this.db
      .prepare(
        'SELECT COUNT(*) as cnt FROM samples WHERE session_id = ? AND sensor = ? AND sample_idx >= ? AND sample_idx <= ?'
      )
      .get(sessionId, sensor, fromIndex, toIndex) as any;

    return (row?.cnt || 0) === expectedCount;
  }

  /**
   * Check whether window `windowIdx` is resolved on both sensors.
   * If needNextSample is true, sensor 1 watermark must reach at least 400 * windowIdx + 400
   * so the candidate at the window boundary (399) can be finalized.
   */
  public isWindowResolved(
    sessionId: string,
    windowIdx: number,
    needNextSample: boolean = true
  ): boolean {
    const s1Req = 400 * windowIdx + (needNextSample ? 400 : 399);
    const s2Req = 400 * windowIdx + 399;

    const s1Prog = this.getProgress(sessionId, 1);
    const s2Prog = this.getProgress(sessionId, 2);

    return s1Prog.resolvedWatermark >= s1Req && s2Prog.resolvedWatermark >= s2Req;
  }

  /**
   * Mark a chunk as successfully committed and acknowledged in the cloud.
   */
  public markChunkSynced(sessionId: string, chunkIdx: number, mode: 'online' | 'offline'): void {
    const now = Date.now();
    this.db
      .prepare(`
        INSERT INTO cloud_sync_progress (session_id, chunk_idx, mode, synced_at)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(session_id, chunk_idx) DO UPDATE SET
          mode = excluded.mode,
          synced_at = excluded.synced_at
      `)
      .run(sessionId, chunkIdx, mode, now);
  }

  /**
   * Check if a chunk is marked as synced locally.
   */
  public isChunkSynced(sessionId: string, chunkIdx: number): boolean {
    const row = this.db
      .prepare('SELECT 1 FROM cloud_sync_progress WHERE session_id = ? AND chunk_idx = ?')
      .get(sessionId, chunkIdx);
    return !!row;
  }

  /**
   * Get list of synced chunk indices for a session.
   */
  public getSyncedChunks(sessionId: string, fromChunk: number = 0, limit: number = 5000): number[] {
    const rows = this.db
      .prepare(
        'SELECT chunk_idx FROM cloud_sync_progress WHERE session_id = ? AND chunk_idx >= ? ORDER BY chunk_idx ASC LIMIT ?'
      )
      .all(sessionId, fromChunk, limit) as any[];
    return rows.map((r) => r.chunk_idx);
  }

  /**
   * Identify unsynced chunk indices up to maxChunkIdx.
   */
  public getUnsyncedChunks(sessionId: string, maxChunkIdx: number, limit: number = 1000): number[] {
    const syncedRows = this.db
      .prepare(
        'SELECT chunk_idx FROM cloud_sync_progress WHERE session_id = ? AND chunk_idx <= ?'
      )
      .all(sessionId, maxChunkIdx) as any[];

    const syncedSet = new Set<number>(syncedRows.map((r) => r.chunk_idx));
    const unsynced: number[] = [];

    for (let c = 0; c <= maxChunkIdx; c++) {
      if (!syncedSet.has(c)) {
        unsynced.push(c);
        if (unsynced.length >= limit) break;
      }
    }

    return unsynced;
  }

  /**
   * Extract raw sample data, losses, and any offline window results for a chunk.
   */
  public getChunkData(sessionId: string, chunkIdx: number): {
    fromIndex: number;
    toIndex: number;
    s1Map: Map<number, number[]>;
    s2Map: Map<number, number[]>;
    losses: LossRange[];
    offlineResults: WindowResult[];
  } {
    const fromIndex = chunkIdx * 800;
    const toIndex = fromIndex + 799;

    const s1Map = this.getSampleRange(sessionId, 1, fromIndex, toIndex);
    const s2Map = this.getSampleRange(sessionId, 2, fromIndex, toIndex);

    // Find losses intersecting [fromIndex, toIndex]
    const lossRows = this.db
      .prepare(
        `SELECT sensor, from_index, to_index, reason FROM losses
         WHERE session_id = ? AND (
           (from_index >= ? AND from_index <= ?) OR
           (to_index >= ? AND to_index <= ?) OR
           (from_index <= ? AND (to_index IS NULL OR to_index >= ?))
         ) ORDER BY from_index`
      )
      .all(sessionId, fromIndex, toIndex, fromIndex, toIndex, fromIndex, toIndex) as any[];

    const losses: LossRange[] = lossRows.map((r) => ({
      sensor: r.sensor as SensorId,
      from_index: r.from_index,
      to_index: r.to_index,
      reason: r.reason,
    }));

    // Find any offline results for window w0 = chunkIdx * 2 and w1 = chunkIdx * 2 + 1
    const offlineResults: WindowResult[] = [];
    const res0 = this.getProcessingResult(sessionId, chunkIdx * 2);
    if (res0) offlineResults.push(res0);
    const res1 = this.getProcessingResult(sessionId, chunkIdx * 2 + 1);
    if (res1) offlineResults.push(res1);

    return {
      fromIndex,
      toIndex,
      s1Map,
      s2Map,
      losses,
      offlineResults,
    };
  }

  public close(): void {
    try {
      this.db.close();
    } catch {}
  }
}
