import type { WindowResult, ChannelKey, SensorId } from '../types.ts';

export const WINDOW_SIZE = 400; // 400 samples/second
export const PEAK_HISTORY_SIZE = 800; // 800 preceding samples
export const REFRACTORY_PERIOD = 100; // minimum 100 samples between accepted peaks
export const HR_WINDOW_LOOKBACK_WINDOWS = 4; // w - 4 to w (5 seconds total)
export const HR_WINDOW_LOOKBACK_SAMPLES = 2000; // 5 seconds * 400 samples/sec

/**
 * Standard round-half-up to 1 decimal place per MayaCardio specification.
 * Preserves null for unavailable values.
 */
export function roundToOneDecimal(val: number | null): number | null {
  if (val === null || !Number.isFinite(val)) return null;
  return Math.round((val + Number.EPSILON) * 10) / 10;
}

/**
 * Channel statistics: arithmetic mean, minimum, and maximum over available values.
 * Returns null for all statistics if values array is empty.
 */
export function calculateChannelStats(values: number[]): {
  mean: number | null;
  min: number | null;
  max: number | null;
} {
  if (values.length === 0) {
    return { mean: null, min: null, max: null };
  }

  let sum = 0;
  let min = values[0];
  let max = values[0];

  for (let i = 0; i < values.length; i++) {
    const v = values[i];
    sum += v;
    if (v < min) min = v;
    if (v > max) max = v;
  }

  const rawMean = sum / values.length;
  return {
    mean: roundToOneDecimal(rawMean),
    min,
    max,
  };
}

/**
 * Sensor 2 magnitude:
 * For each available sensor 2 sample: sqrt(ch1^2 + ch2^2).
 * Then compute arithmetic mean of those per-sample magnitudes.
 * Returns null if no sensor 2 samples are available.
 */
export function calculateSensor2Magnitude(
  s2Samples: Array<[number, number]>
): number | null {
  if (s2Samples.length === 0) {
    return null;
  }

  let sumMag = 0;
  for (let i = 0; i < s2Samples.length; i++) {
    const [ch1, ch2] = s2Samples[i];
    const mag = Math.sqrt(ch1 * ch1 + ch2 * ch2);
    sumMag += mag;
  }

  const meanMag = sumMag / s2Samples.length;
  return roundToOneDecimal(meanMag);
}

/**
 * Four-condition peak detector state.
 */
export interface PeakDetectorState {
  lastAcceptedPeak: number; // index of last accepted peak, or -Infinity if none
}

/**
 * Deque item for sliding maximum
 */
interface DequeItem {
  index: number;
  value: number;
}

/**
 * High-performance, streaming 4-condition peak detector on Sensor 1 Channel 2.
 * Independent of wall-clock time, I/O, or OS APIs.
 */
export class PeakDetector {
  private lastAcceptedPeak: number;
  // Sliding maximum deque of [index, value] for preceding 800 samples
  private deque: DequeItem[] = [];
  // Tracks how many consecutive available samples immediately precede candidate i
  private consecutiveAvailableCount: number = 0;

  constructor(initialState?: Partial<PeakDetectorState>) {
    this.lastAcceptedPeak = initialState?.lastAcceptedPeak ?? -Infinity;
  }

  public getState(): PeakDetectorState {
    return {
      lastAcceptedPeak: this.lastAcceptedPeak,
    };
  }

  public setState(state: PeakDetectorState): void {
    this.lastAcceptedPeak = state.lastAcceptedPeak;
    this.deque = [];
    this.consecutiveAvailableCount = 0;
  }

  /**
   * Reset the detector state completely.
   */
  public reset(lastAcceptedPeak: number = -Infinity): void {
    this.lastAcceptedPeak = lastAcceptedPeak;
    this.deque = [];
    this.consecutiveAvailableCount = 0;
  }

  /**
   * Evaluate a sequence of candidate indices for a window.
   *
   * @param startIndex First candidate index in range (inclusive)
   * @param endIndex Last candidate index in range (inclusive)
   * @param getValue Function returning the sample value for index i, or undefined if missing/unavailable
   * @returns Array of accepted peak indices in ascending order
   */
  public processCandidates(
    startIndex: number,
    endIndex: number,
    getValue: (index: number) => number | undefined
  ): number[] {
    const acceptedPeaks: number[] = [];

    // Pre-populate history before startIndex if needed
    this.primeHistory(startIndex, getValue);

    for (let i = startIndex; i <= endIndex; i++) {
      const isPeak = this.evaluateCandidate(i, getValue);
      if (isPeak) {
        this.lastAcceptedPeak = i;
        acceptedPeaks.push(i);
      }
      this.advanceCandidate(i, getValue);
    }

    return acceptedPeaks;
  }

  /**
   * Pre-populate sliding deque and consecutive count up to startIndex.
   */
  public primeHistory(
    startIndex: number,
    getValue: (index: number) => number | undefined
  ): void {
    this.deque = [];
    this.consecutiveAvailableCount = 0;

    const historyStart = Math.max(0, startIndex - PEAK_HISTORY_SIZE);
    for (let k = historyStart; k < startIndex; k++) {
      const val = getValue(k);
      if (val !== undefined) {
        this.consecutiveAvailableCount++;
        // Maintain monotonic decreasing deque
        while (this.deque.length > 0 && this.deque[this.deque.length - 1].value <= val) {
          this.deque.pop();
        }
        this.deque.push({ index: k, value: val });
      } else {
        // Gap encountered: breaks consecutive history
        this.consecutiveAvailableCount = 0;
        this.deque = [];
      }
    }

    // Prune entries outside [startIndex - 800, startIndex - 1]
    const minPrecedingIdx = startIndex - PEAK_HISTORY_SIZE;
    while (this.deque.length > 0 && this.deque[0].index < minPrecedingIdx) {
      this.deque.shift();
    }
  }

  /**
   * Evaluate whether candidate index `i` is a peak according to the 4 conditions.
   */
  public evaluateCandidate(
    i: number,
    getValue: (index: number) => number | undefined
  ): boolean {
    // Condition 1:
    // a) Candidate must have index >= 800 (since 800 preceding samples are required)
    if (i < PEAK_HISTORY_SIZE) {
      return false;
    }

    // b) Candidate sample itself must be available
    const xi = getValue(i);
    if (xi === undefined) {
      return false;
    }

    // c) Sample immediately after it (i + 1) must be available
    const nextVal = getValue(i + 1);
    if (nextVal === undefined) {
      return false;
    }

    // d) All 800 preceding samples (i - 800 to i - 1) must be available
    if (this.consecutiveAvailableCount < PEAK_HISTORY_SIZE) {
      return false;
    }

    // Condition 2: Candidate is strictly greater than both immediate neighbors
    const prevVal = getValue(i - 1);
    if (prevVal === undefined) {
      return false;
    }
    if (!(xi > prevVal && xi > nextVal)) {
      return false;
    }

    // Condition 3: Candidate is strictly greater than half the maximum of preceding 800 samples
    if (this.deque.length === 0) {
      return false;
    }
    const maxPreceding = this.deque[0].value;
    const threshold = maxPreceding / 2;
    if (!(xi > threshold)) {
      return false;
    }

    // Condition 4: Candidate is at least 100 samples after the previous accepted peak
    if (this.lastAcceptedPeak !== -Infinity && i - this.lastAcceptedPeak < REFRACTORY_PERIOD) {
      return false;
    }

    return true;
  }

  /**
   * Advance state from candidate i to i + 1:
   * Sample i becomes the newest element of the preceding 800 lookback window for i + 1.
   */
  public advanceCandidate(
    i: number,
    getValue: (index: number) => number | undefined
  ): void {
    const val = getValue(i);
    if (val !== undefined) {
      this.consecutiveAvailableCount++;
      // Push (i, val) to monotonic deque
      while (this.deque.length > 0 && this.deque[this.deque.length - 1].value <= val) {
        this.deque.pop();
      }
      this.deque.push({ index: i, value: val });
    } else {
      // Missing sample at i breaks consecutive run
      this.consecutiveAvailableCount = 0;
      this.deque = [];
    }

    // Elements before (i + 1) - 800 leave the preceding lookback window
    const minPrecedingIdx = (i + 1) - PEAK_HISTORY_SIZE;
    while (this.deque.length > 0 && this.deque[0].index < minPrecedingIdx) {
      this.deque.shift();
    }
  }
}

/**
 * Calculate heart rate for window `w`.
 *
 * Interval: [400 * (w - 4), 400 * (w + 1) - 1] (inclusive 5 seconds).
 *
 * Conditions for null:
 * 1. w < 4
 * 2. Any sample in those 5 seconds is missing (isRangeComplete === false)
 * 3. Fewer than 2 accepted peaks exist in the interval
 *
 * If valid:
 * heart_rate = 60 / mean_gap_seconds = (60 * 400 * (count - 1)) / (last_peak - first_peak)
 * Rounded to 1 decimal place.
 */
export function calculateHeartRate(params: {
  windowIdx: number;
  acceptedPeaks: number[];
  isRangeComplete: boolean;
}): number | null {
  const { windowIdx, acceptedPeaks, isRangeComplete } = params;

  // Condition 1: w < 4
  if (windowIdx < HR_WINDOW_LOOKBACK_WINDOWS) {
    return null;
  }

  // Condition 2: Any sample in the 5-second interval is missing
  if (!isRangeComplete) {
    return null;
  }

  // Inclusive interval endpoints
  const rangeStart = WINDOW_SIZE * (windowIdx - HR_WINDOW_LOOKBACK_WINDOWS); // 400 * (w - 4)
  const rangeEnd = WINDOW_SIZE * (windowIdx + 1) - 1; // 400 * (w + 1) - 1

  // Filter accepted peaks in interval
  const peaksInInterval: number[] = [];
  for (let i = 0; i < acceptedPeaks.length; i++) {
    const p = acceptedPeaks[i];
    if (p >= rangeStart && p <= rangeEnd) {
      peaksInInterval.push(p);
    }
  }

  // Condition 3: Fewer than 2 peaks in the interval
  if (peaksInInterval.length < 2) {
    return null;
  }

  const k = peaksInInterval.length;
  const firstPeak = peaksInInterval[0];
  const lastPeak = peaksInInterval[k - 1];

  const peakSpanSamples = lastPeak - firstPeak;
  if (peakSpanSamples <= 0) {
    return null;
  }

  // heart_rate = 60 * 400 * (k - 1) / (lastPeak - firstPeak)
  const rawHeartRate = (60 * WINDOW_SIZE * (k - 1)) / peakSpanSamples;
  return roundToOneDecimal(rawHeartRate);
}

/**
 * Pure function to compute the complete WindowResult for window `w`.
 */
export function computeWindowResult(params: {
  sessionId: string;
  windowIdx: number;
  mode: 'online' | 'offline';
  s1Samples: Map<number, number[]>; // idx -> [ch1, ch2, ch3]
  s2Samples: Map<number, number[]>; // idx -> [ch1, ch2]
  acceptedPeaks: number[];
  isHrRangeComplete: boolean;
}): WindowResult {
  const {
    sessionId,
    windowIdx,
    mode,
    s1Samples,
    s2Samples,
    acceptedPeaks,
    isHrRangeComplete,
  } = params;

  const startIdx = windowIdx * WINDOW_SIZE;
  const endIdx = startIdx + WINDOW_SIZE - 1;

  // 1. Missing samples and completeness
  let s1Count = 0;
  const s1Ch1Vals: number[] = [];
  const s1Ch2Vals: number[] = [];
  const s1Ch3Vals: number[] = [];

  for (let i = startIdx; i <= endIdx; i++) {
    const s1 = s1Samples.get(i);
    if (s1 !== undefined) {
      s1Count++;
      s1Ch1Vals.push(s1[0]);
      s1Ch2Vals.push(s1[1]);
      s1Ch3Vals.push(s1[2]);
    }
  }

  let s2Count = 0;
  const s2Ch1Vals: number[] = [];
  const s2Ch2Vals: number[] = [];
  const s2Pairs: Array<[number, number]> = [];

  for (let i = startIdx; i <= endIdx; i++) {
    const s2 = s2Samples.get(i);
    if (s2 !== undefined) {
      s2Count++;
      s2Ch1Vals.push(s2[0]);
      s2Ch2Vals.push(s2[1]);
      s2Pairs.push([s2[0], s2[1]]);
    }
  }

  const missing_1 = WINDOW_SIZE - s1Count;
  const missing_2 = WINDOW_SIZE - s2Count;
  const complete = missing_1 === 0 && missing_2 === 0;

  // 2. Channel aggregations
  const s1Ch1Stats = calculateChannelStats(s1Ch1Vals);
  const s1Ch2Stats = calculateChannelStats(s1Ch2Vals);
  const s1Ch3Stats = calculateChannelStats(s1Ch3Vals);
  const s2Ch1Stats = calculateChannelStats(s2Ch1Vals);
  const s2Ch2Stats = calculateChannelStats(s2Ch2Vals);

  const mean: Record<string, number | null> = {
    s1_ch1: s1Ch1Stats.mean,
    s1_ch2: s1Ch2Stats.mean,
    s1_ch3: s1Ch3Stats.mean,
    s2_ch1: s2Ch1Stats.mean,
    s2_ch2: s2Ch2Stats.mean,
  };

  const min: Record<string, number | null> = {
    s1_ch1: s1Ch1Stats.min,
    s1_ch2: s1Ch2Stats.min,
    s1_ch3: s1Ch3Stats.min,
    s2_ch1: s2Ch1Stats.min,
    s2_ch2: s2Ch2Stats.min,
  };

  const max: Record<string, number | null> = {
    s1_ch1: s1Ch1Stats.max,
    s1_ch2: s1Ch2Stats.max,
    s1_ch3: s1Ch3Stats.max,
    s2_ch1: s2Ch1Stats.max,
    s2_ch2: s2Ch2Stats.max,
  };

  // 3. Sensor 2 magnitude
  const magnitude = calculateSensor2Magnitude(s2Pairs);

  // 4. Heart rate
  const heart_rate = calculateHeartRate({
    windowIdx,
    acceptedPeaks,
    isRangeComplete: isHrRangeComplete,
  });

  return {
    session_id: sessionId,
    window_idx: windowIdx,
    missing_1,
    missing_2,
    complete,
    mean,
    min,
    max,
    magnitude,
    heart_rate,
    mode,
  };
}
