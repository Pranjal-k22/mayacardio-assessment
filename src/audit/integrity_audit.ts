import type { StorageEngine } from '../storage/db.ts';
import type { CloudApiClient } from '../cloud/client.ts';
import type { SensorId } from '../types.ts';

export type AuditSeverity = 'ERROR' | 'WARNING' | 'INFO';

export interface AuditFinding {
  severity: AuditSeverity;
  type: string;
  sensor?: SensorId;
  index?: number;
  fromIndex?: number;
  toIndex?: number | null;
  details: string;
}

export interface SessionAuditReport {
  sessionId: string;
  pass: boolean;
  totalSamples: { '1': number; '2': number };
  totalLossRecords: { '1': number; '2': number };
  watermark: { '1': number; '2': number };
  highestStoredIndex: { '1': number; '2': number };
  processing: {
    checkpointWindow: number;
    highestResultWindow: number;
    resultsCount: number;
    consistent: boolean;
  };
  cloudSync: {
    syncedChunksLocal: number;
    syncedChunksCloud?: number;
    inventoryMatches?: boolean;
  };
  findings: AuditFinding[];
}

export class IntegrityAuditor {
  private storage: StorageEngine;
  private client?: CloudApiClient;

  constructor(storage: StorageEngine, client?: CloudApiClient) {
    this.storage = storage;
    this.client = client;
  }

  /**
   * Run comprehensive audit for a specific session.
   * Completely read-only and idempotent.
   */
  public async auditSession(sessionId: string): Promise<SessionAuditReport> {
    const findings: AuditFinding[] = [];

    // Verify session existence
    if (!this.storage.sessionExists(sessionId)) {
      findings.push({
        severity: 'ERROR',
        type: 'SESSION_NOT_FOUND',
        details: `Session '${sessionId}' does not exist in local storage.`,
      });
      return {
        sessionId,
        pass: false,
        totalSamples: { '1': 0, '2': 0 },
        totalLossRecords: { '1': 0, '2': 0 },
        watermark: { '1': -1, '2': -1 },
        highestStoredIndex: { '1': -1, '2': -1 },
        processing: { checkpointWindow: -1, highestResultWindow: -1, resultsCount: 0, consistent: true },
        cloudSync: { syncedChunksLocal: 0 },
        findings,
      };
    }

    // 1. Audit Sensor 1 and Sensor 2
    const totalSamples: { '1': number; '2': number } = { '1': 0, '2': 0 };
    const totalLossRecords: { '1': number; '2': number } = { '1': 0, '2': 0 };
    const watermark: { '1': number; '2': number } = { '1': -1, '2': -1 };
    const highestStoredIndex: { '1': number; '2': number } = { '1': -1, '2': -1 };

    for (const s of [1, 2] as SensorId[]) {
      const p = this.storage.getProgress(sessionId, s);
      watermark[String(s) as '1' | '2'] = p.resolvedWatermark;

      const losses = this.storage.getLosses(sessionId, s);
      totalLossRecords[String(s) as '1' | '2'] = losses.length;

      // Check highest stored index and total count
      const samples = this.storage.getSamples(sessionId, s, 0, 1000000);
      totalSamples[String(s) as '1' | '2'] = samples.length;
      const maxIdx = samples.length > 0 ? samples[samples.length - 1].index : -1;
      highestStoredIndex[String(s) as '1' | '2'] = maxIdx;

      const sampleIndexSet = new Set<number>(samples.map((sm) => sm.index));

      // 1a. Validate Loss Range Bounds
      for (const lr of losses) {
        if (lr.from_index < 0) {
          findings.push({
            severity: 'ERROR',
            type: 'INVALID_LOSS_BOUNDS',
            sensor: s,
            fromIndex: lr.from_index,
            details: `Loss range has negative from_index: ${lr.from_index}`,
          });
        }
        if (lr.to_index !== null && lr.to_index < lr.from_index) {
          findings.push({
            severity: 'ERROR',
            type: 'INVALID_LOSS_BOUNDS',
            sensor: s,
            fromIndex: lr.from_index,
            toIndex: lr.to_index,
            details: `Loss range to_index (${lr.to_index}) is less than from_index (${lr.from_index})`,
          });
        }

        // 1b. Check for Sample overlapping declared loss
        const start = lr.from_index;
        const end = lr.to_index ?? start;
        for (let i = start; i <= end; i++) {
          if (sampleIndexSet.has(i)) {
            findings.push({
              severity: 'ERROR',
              type: 'SAMPLE_LOSS_COLLISION',
              sensor: s,
              index: i,
              fromIndex: lr.from_index,
              toIndex: lr.to_index,
              details: `Sample ${i} exists on disk but is also declared in loss range [${lr.from_index}, ${lr.to_index}]`,
            });
            break; // report once per loss range
          }
        }
      }

      // 1c. Check for Overlapping Loss Records with Conflicting Reasons
      for (let i = 0; i < losses.length; i++) {
        for (let j = i + 1; j < losses.length; j++) {
          const l1 = losses[i];
          const l2 = losses[j];
          const end1 = l1.to_index ?? l1.from_index;
          const end2 = l2.to_index ?? l2.from_index;
          const overlaps = Math.max(l1.from_index, l2.from_index) <= Math.min(end1, end2);
          if (overlaps && l1.reason !== l2.reason) {
            findings.push({
              severity: 'ERROR',
              type: 'CONFLICTING_LOSS_REASONS',
              sensor: s,
              fromIndex: Math.max(l1.from_index, l2.from_index),
              details: `Overlapping loss records have conflicting reasons: '${l1.reason}' vs '${l2.reason}'`,
            });
          }
        }
      }

      // 1d. Check Watermark Contiguity (No unaccounted gaps below watermark)
      if (p.resolvedWatermark >= 0) {
        for (let i = 0; i <= p.resolvedWatermark; i++) {
          if (!sampleIndexSet.has(i)) {
            const coveredByLoss = losses.some(
              (lr) => i >= lr.from_index && (lr.to_index === null || i <= lr.to_index)
            );
            if (!coveredByLoss) {
              findings.push({
                severity: 'ERROR',
                type: 'UNACCOUNTED_GAP',
                sensor: s,
                index: i,
                details: `Index ${i} is below resolved watermark (${p.resolvedWatermark}) but is neither stored nor declared lost.`,
              });
            }
          }
        }
      }
    }

    // 2. Audit Recorded Integrity Incidents
    const legacyAudit1 = this.storage.auditSession(sessionId, 1);
    const legacyAudit2 = this.storage.auditSession(sessionId, 2);
    for (const inc of legacyAudit1.integrityIncidents.concat(legacyAudit2.integrityIncidents)) {
      findings.push({
        severity: 'ERROR',
        type: 'INTEGRITY_INCIDENT',
        index: inc.index,
        details: `${inc.incidentType}: ${inc.details}`,
      });
    }

    // 3. Audit Processing Checkpoint Consistency
    const cp = this.storage.getProcessingCheckpoint(sessionId);
    const results = this.storage.getProcessingResults(sessionId, 0, 100000);
    const cpWindow = cp?.last_processed_window ?? -1;
    let maxResultWindow = -1;
    const windowSet = new Set<number>();

    for (const r of results) {
      if (r.window_idx > maxResultWindow) maxResultWindow = r.window_idx;
      windowSet.add(r.window_idx);
    }

    let processingConsistent = true;
    if (cpWindow > maxResultWindow && maxResultWindow >= 0) {
      findings.push({
        severity: 'ERROR',
        type: 'CHECKPOINT_INCONSISTENCY',
        details: `Processing checkpoint last_processed_window (${cpWindow}) exceeds highest stored result window (${maxResultWindow}).`,
      });
      processingConsistent = false;
    }

    // Check for gaps in processing results up to maxResultWindow
    for (let w = 0; w <= maxResultWindow; w++) {
      if (!windowSet.has(w)) {
        findings.push({
          severity: 'WARNING',
          type: 'MISSING_WINDOW_RESULT',
          details: `Window ${w} processing result is missing between 0 and max window ${maxResultWindow}.`,
        });
        processingConsistent = false;
      }
    }

    // 4. Audit Cloud Synchronization Consistency
    const syncedChunks = this.storage.getSyncedChunks(sessionId, 0, 100000);
    const cloudSyncReport: SessionAuditReport['cloudSync'] = {
      syncedChunksLocal: syncedChunks.length,
    };

    if (this.client) {
      try {
        const inv = await this.client.getInventory(sessionId, 0, 100000, 10000);
        cloudSyncReport.syncedChunksCloud = inv.total_chunks;

        const cloudChunkSet = new Set<number>(inv.chunks);
        let inventoryMatches = true;

        for (const localC of syncedChunks) {
          if (!cloudChunkSet.has(localC)) {
            findings.push({
              severity: 'ERROR',
              type: 'CLOUD_INVENTORY_DISCREPANCY',
              details: `Chunk ${localC} is marked synced locally but absent from cloud inventory.`,
            });
            inventoryMatches = false;
          }
        }

        cloudSyncReport.inventoryMatches = inventoryMatches;
      } catch (err: any) {
        findings.push({
          severity: 'INFO',
          type: 'CLOUD_AUDIT_UNAVAILABLE',
          details: `Cloud inventory could not be checked: ${err.message}`,
        });
      }
    }

    const hasErrors = findings.some((f) => f.severity === 'ERROR');

    return {
      sessionId,
      pass: !hasErrors,
      totalSamples,
      totalLossRecords,
      watermark,
      highestStoredIndex,
      processing: {
        checkpointWindow: cpWindow,
        highestResultWindow: maxResultWindow,
        resultsCount: results.length,
        consistent: processingConsistent,
      },
      cloudSync: cloudSyncReport,
      findings,
    };
  }
}
