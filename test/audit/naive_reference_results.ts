export interface NaiveWindowResult {
  session_id: string;
  window_idx: number;
  missing_1: number;
  missing_2: number;
  complete: boolean;
  mean: Record<string, number | null>;
  min: Record<string, number | null>;
  max: Record<string, number | null>;
  magnitude: number | null;
  heart_rate: number | null;
  mode: 'online' | 'offline';
}

function round1(val: number | null): number | null {
  if (val === null || !Number.isFinite(val)) return null;
  return Math.round((val + Number.EPSILON) * 10) / 10;
}

/**
 * Slow, naive, independent reference implementation of Section 3.
 * Completely independent of src/processing/core.ts.
 */
export function computeNaiveResults(params: {
  sessionId: string;
  maxWindow: number;
  mode: 'online' | 'offline';
  s1Samples: Map<number, number[]>; // index -> [ch1, ch2, ch3]
  s2Samples: Map<number, number[]>; // index -> [ch1, ch2]
}): NaiveWindowResult[] {
  const { sessionId, maxWindow, mode, s1Samples, s2Samples } = params;
  const results: NaiveWindowResult[] = [];

  // Track accepted peaks chronologically
  const acceptedPeaks: number[] = [];
  let lastAcceptedPeak = -Infinity;

  // Find all peaks from index 0 up to 400 * (maxWindow + 1)
  const maxEvalIndex = 400 * (maxWindow + 1);

  for (let i = 0; i <= maxEvalIndex; i++) {
    // Condition 1: 800 samples before it and 1 sample after it must be available
    if (i < 800) continue;

    const sCurrent = s1Samples.get(i);
    const sNext = s1Samples.get(i + 1);
    const sPrev = s1Samples.get(i - 1);
    if (!sCurrent || !sNext || !sPrev) continue;

    const xi = sCurrent[1]; // Sensor 1 Channel 2 (ECG)
    const nextVal = sNext[1];
    const prevVal = sPrev[1];

    // Check all 800 preceding samples are available and find their max
    let allPrecedingAvailable = true;
    let maxPreceding = -Infinity;

    for (let k = i - 800; k <= i - 1; k++) {
      const s = s1Samples.get(k);
      if (!s) {
        allPrecedingAvailable = false;
        break;
      }
      const val = s[1];
      if (val > maxPreceding) {
        maxPreceding = val;
      }
    }

    if (!allPrecedingAvailable) continue;

    // Condition 2: Larger than both neighbours (strict >)
    if (!(xi > prevVal && xi > nextVal)) continue;

    // Condition 3: Larger than half of max of 800 before it (2*xi > maxPreceding or xi > maxPreceding/2)
    if (!(2 * xi > maxPreceding)) continue;

    // Condition 4: At least 100 samples after previous accepted peak
    if (lastAcceptedPeak !== -Infinity && i - lastAcceptedPeak < 100) continue;

    // Accepted peak!
    lastAcceptedPeak = i;
    acceptedPeaks.push(i);
  }

  // Now compute window results for each window w from 0 to maxWindow
  for (let w = 0; w <= maxWindow; w++) {
    const start = 400 * w;
    const end = start + 399;

    let s1Count = 0;
    const s1_ch1: number[] = [];
    const s1_ch2: number[] = [];
    const s1_ch3: number[] = [];

    for (let idx = start; idx <= end; idx++) {
      const s1 = s1Samples.get(idx);
      if (s1) {
        s1Count++;
        s1_ch1.push(s1[0]);
        s1_ch2.push(s1[1]);
        s1_ch3.push(s1[2]);
      }
    }

    let s2Count = 0;
    const s2_ch1: number[] = [];
    const s2_ch2: number[] = [];
    const s2Mags: number[] = [];

    for (let idx = start; idx <= end; idx++) {
      const s2 = s2Samples.get(idx);
      if (s2) {
        s2Count++;
        s2_ch1.push(s2[0]);
        s2_ch2.push(s2[1]);
        s2Mags.push(Math.sqrt(s2[0] * s2[0] + s2[1] * s2[1]));
      }
    }

    const missing_1 = 400 - s1Count;
    const missing_2 = 400 - s2Count;
    const complete = missing_1 === 0 && missing_2 === 0;

    const calcStats = (vals: number[]) => {
      if (vals.length === 0) return { mean: null, min: null, max: null };
      let sum = 0;
      let min = vals[0];
      let max = vals[0];
      for (const v of vals) {
        sum += v;
        if (v < min) min = v;
        if (v > max) max = v;
      }
      return { mean: round1(sum / vals.length), min, max };
    };

    const st_s1_1 = calcStats(s1_ch1);
    const st_s1_2 = calcStats(s1_ch2);
    const st_s1_3 = calcStats(s1_ch3);
    const st_s2_1 = calcStats(s2_ch1);
    const st_s2_2 = calcStats(s2_ch2);

    const mean = {
      s1_ch1: st_s1_1.mean,
      s1_ch2: st_s1_2.mean,
      s1_ch3: st_s1_3.mean,
      s2_ch1: st_s2_1.mean,
      s2_ch2: st_s2_2.mean,
    };

    const min = {
      s1_ch1: st_s1_1.min,
      s1_ch2: st_s1_2.min,
      s1_ch3: st_s1_3.min,
      s2_ch1: st_s2_1.min,
      s2_ch2: st_s2_2.min,
    };

    const max = {
      s1_ch1: st_s1_1.max,
      s1_ch2: st_s1_2.max,
      s1_ch3: st_s1_3.max,
      s2_ch1: st_s2_1.max,
      s2_ch2: st_s2_2.max,
    };

    let magnitude: number | null = null;
    if (s2Mags.length > 0) {
      let sum = 0;
      for (const m of s2Mags) sum += m;
      magnitude = round1(sum / s2Mags.length);
    }

    // Heart rate rule
    let heart_rate: number | null = null;
    if (w >= 4) {
      const hrRangeStart = 400 * (w - 4);
      const hrRangeEnd = 400 * (w + 1) - 1;

      // Check if any sample missing in those 5s (Sensor 1 Channel 2 ECG)
      let anyMissingIn5s = false;
      for (let k = hrRangeStart; k <= hrRangeEnd; k++) {
        if (!s1Samples.has(k)) {
          anyMissingIn5s = true;
          break;
        }
      }

      if (!anyMissingIn5s) {
        // Peaks in [400(w-4), 400(w+1)-1]
        const peaksIn5s = acceptedPeaks.filter((p) => p >= hrRangeStart && p <= hrRangeEnd);
        if (peaksIn5s.length >= 2) {
          const k = peaksIn5s.length;
          const firstP = peaksIn5s[0];
          const lastP = peaksIn5s[k - 1];
          const span = lastP - firstP;
          if (span > 0) {
            const rawHr = (60 * 400 * (k - 1)) / span;
            heart_rate = round1(rawHr);
          }
        }
      }
    }

    results.push({
      session_id: sessionId,
      window_idx: w,
      missing_1,
      missing_2,
      complete,
      mean,
      min,
      max,
      magnitude,
      heart_rate,
      mode,
    });
  }

  return results;
}
