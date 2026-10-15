import type { WindowResult, SensorId, ProcessingCheckpoint } from '../types.ts';
import { StorageEngine } from '../storage/db.ts';
import {
  WINDOW_SIZE,
  PEAK_HISTORY_SIZE,
  HR_WINDOW_LOOKBACK_WINDOWS,
  PeakDetector,
  computeWindowResult,
} from './core.ts';

export interface ProcessingEngineOptions {
  storage: StorageEngine;
  /**
   * Scope for heart rate 5-second missingness check.
   * Per Phase 0 AMB-01 provisional assumption, defaults to 'sensor1_ch2' (ECG channel).
   */
  hrMissingScope?: 'sensor1_ch2' | 'both_sensors';
}

/**
 * Coordinates durable processing using StorageEngine and pure core functions.
 */
export class WindowProcessingEngine {
  private storage: StorageEngine;
  private hrMissingScope: 'sensor1_ch2' | 'both_sensors';

  constructor(opts: ProcessingEngineOptions) {
    this.storage = opts.storage;
    this.hrMissingScope = opts.hrMissingScope ?? 'sensor1_ch2';
  }

  /**
   * Process the next available window for a session.
   * Returns null if the next window is not yet fully resolved in storage.
   */
  public processNextWindow(
    sessionId: string,
    mode: 'online' | 'offline' = 'offline'
  ): WindowResult | null {
    const checkpoint = this.storage.getProcessingCheckpoint(sessionId);
    const nextWindow = checkpoint ? checkpoint.last_processed_window + 1 : 0;
    const priorPeak = checkpoint ? checkpoint.last_accepted_peak : -Infinity;

    // Check if next window already has a persisted authoritative result
    const existing = this.storage.getProcessingResult(sessionId, nextWindow);
    if (existing) {
      return existing;
    }

    // Check if next window is resolved in storage
    const isResolved = this.storage.isWindowResolved(sessionId, nextWindow, true);
    if (!isResolved) {
      return null;
    }

    const windowStart = nextWindow * WINDOW_SIZE;
    const windowEnd = windowStart + WINDOW_SIZE - 1;

    // 1. Fetch sensor 1 and sensor 2 samples for window
    const s1WindowSamples = this.storage.getSampleRange(sessionId, 1, windowStart, windowEnd);
    const s2WindowSamples = this.storage.getSampleRange(sessionId, 2, windowStart, windowEnd);

    // 2. Run peak detector on Sensor 1 Channel 2 candidates in this window
    // Peak detector requires history back to windowStart - 800 and next sample windowEnd + 1
    const peakHistoryStart = Math.max(0, windowStart - PEAK_HISTORY_SIZE);
    const peakHistoryEnd = windowEnd + 1;
    const s1HistorySamples = this.storage.getSampleRange(
      sessionId,
      1,
      peakHistoryStart,
      peakHistoryEnd
    );

    const getValue = (idx: number): number | undefined => {
      const row = s1HistorySamples.get(idx);
      if (!row) return undefined;
      return row[1]; // Sensor 1 Channel 2 (ECG)
    };

    const detector = new PeakDetector({
      lastAcceptedPeak: priorPeak < 0 ? -Infinity : priorPeak,
    });

    const newPeaks = detector.processCandidates(windowStart, windowEnd, getValue);
    const updatedPeakState = detector.getState();
    const finalPeak =
      updatedPeakState.lastAcceptedPeak === -Infinity
        ? priorPeak
        : updatedPeakState.lastAcceptedPeak;

    // 3. Heart rate calculation
    let isHrRangeComplete = false;
    let peaksInHrInterval: number[] = [];

    if (nextWindow >= HR_WINDOW_LOOKBACK_WINDOWS) {
      const hrStart = WINDOW_SIZE * (nextWindow - HR_WINDOW_LOOKBACK_WINDOWS); // 400 * (w - 4)
      const hrEnd = WINDOW_SIZE * (nextWindow + 1) - 1; // 400 * (w + 1) - 1

      const s1Complete = this.storage.isRangeFullyAvailable(sessionId, 1, hrStart, hrEnd);
      if (this.hrMissingScope === 'both_sensors') {
        const s2Complete = this.storage.isRangeFullyAvailable(sessionId, 2, hrStart, hrEnd);
        isHrRangeComplete = s1Complete && s2Complete;
      } else {
        isHrRangeComplete = s1Complete;
      }

      // Fetch all detected peaks in [hrStart, hrEnd]
      const existingPeaks = this.storage.getDetectedPeaks(sessionId, hrStart, hrEnd);
      // Combine with new peaks in this window (avoid duplicates)
      const peakSet = new Set<number>([...existingPeaks, ...newPeaks]);
      peaksInHrInterval = Array.from(peakSet).sort((a, b) => a - b);
    }

    // 4. Compute pure WindowResult
    const result = computeWindowResult({
      sessionId,
      windowIdx: nextWindow,
      mode,
      s1Samples: s1WindowSamples,
      s2Samples: s2WindowSamples,
      acceptedPeaks: peaksInHrInterval,
      isHrRangeComplete,
    });

    // 5. Atomically save result, newly detected peaks, and checkpoint
    this.storage.saveProcessingResult(result, newPeaks, nextWindow, finalPeak);

    return result;
  }

  /**
   * Process all available resolved windows in loop until none left.
   */
  public processAllAvailableWindows(
    sessionId: string,
    mode: 'online' | 'offline' = 'offline'
  ): WindowResult[] {
    const results: WindowResult[] = [];
    while (true) {
      const res = this.processNextWindow(sessionId, mode);
      if (!res) break;
      results.push(res);
    }
    return results;
  }
}
