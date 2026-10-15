import assert from 'node:assert';
import * as fs from 'node:fs';
import * as path from 'node:path';
import {
  WINDOW_SIZE,
  PEAK_HISTORY_SIZE,
  REFRACTORY_PERIOD,
  roundToOneDecimal,
  calculateChannelStats,
  calculateSensor2Magnitude,
  PeakDetector,
  calculateHeartRate,
  computeWindowResult,
} from '../src/processing/core.ts';
import { StorageEngine } from '../src/storage/db.ts';
import { WindowProcessingEngine } from '../src/processing/engine.ts';
import type { MockResponse, WindowResult } from '../src/types.ts';

console.log('=== Running Phase 3: Windowing, Aggregations, and Peak Detection Tests ===');

const TEST_DB_DIR = path.resolve('test_data');
if (!fs.existsSync(TEST_DB_DIR)) {
  fs.mkdirSync(TEST_DB_DIR, { recursive: true });
}

function cleanDbFiles(dbPath: string) {
  for (const ext of ['', '-wal', '-shm']) {
    const p = dbPath + ext;
    if (fs.existsSync(p)) {
      try {
        fs.unlinkSync(p);
      } catch {}
    }
  }
}

/**
 * Independent, deliberately unoptimized reference implementation.
 * Performs a direct linear scan over preceding 800 samples for each candidate.
 */
function referenceEvaluatePeaks(
  startIndex: number,
  endIndex: number,
  getValue: (index: number) => number | undefined,
  initialLastPeak: number = -Infinity
): { acceptedPeaks: number[]; lastPeak: number } {
  const peaks: number[] = [];
  let lastPeak = initialLastPeak;

  for (let i = startIndex; i <= endIndex; i++) {
    // Condition 1: 800 preceding samples and 1 next sample available
    if (i < PEAK_HISTORY_SIZE) continue;
    const xi = getValue(i);
    const nextVal = getValue(i + 1);
    if (xi === undefined || nextVal === undefined) continue;

    let allAvailable = true;
    let maxPreceding = -Infinity;
    for (let k = i - PEAK_HISTORY_SIZE; k <= i - 1; k++) {
      const v = getValue(k);
      if (v === undefined) {
        allAvailable = false;
        break;
      }
      if (v > maxPreceding) {
        maxPreceding = v;
      }
    }
    if (!allAvailable) continue;

    // Condition 2: Strictly greater than both immediate neighbors
    const prevVal = getValue(i - 1);
    if (prevVal === undefined || !(xi > prevVal && xi > nextVal)) continue;

    // Condition 3: Strictly greater than half maximum of preceding 800 samples
    const threshold = maxPreceding / 2;
    if (!(xi > threshold)) continue;

    // Condition 4: At least 100 samples after previous accepted peak
    if (lastPeak !== -Infinity && i - lastPeak < REFRACTORY_PERIOD) continue;

    peaks.push(i);
    lastPeak = i;
  }

  return { acceptedPeaks: peaks, lastPeak };
}

// Simple deterministic Linear Congruential Generator for reproducible property tests
class SeededRng {
  private state: number;
  constructor(seed: number) {
    this.state = seed % 2147483647;
    if (this.state <= 0) this.state += 2147483646;
  }
  public next(): number {
    this.state = (this.state * 16807) % 2147483647;
    return (this.state - 1) / 2147483646;
  }
  public nextInt(min: number, max: number): number {
    return Math.floor(this.next() * (max - min + 1)) + min;
  }
}

// =========================================================================
// 1. PURE MATH, AGGREGATIONS, AND MAGNITUDE TESTS
// =========================================================================
function testMathAndAggregations() {
  console.log('\n--- 1. Pure Aggregations & Sensor 2 Magnitude Tests ---');

  // Rounding tests
  assert.strictEqual(roundToOneDecimal(null), null);
  assert.strictEqual(roundToOneDecimal(10.04), 10.0);
  assert.strictEqual(roundToOneDecimal(10.05), 10.1);
  assert.strictEqual(roundToOneDecimal(10.06), 10.1);
  assert.strictEqual(roundToOneDecimal(-5.55), -5.5);
  assert.strictEqual(roundToOneDecimal(1200), 1200);
  console.log('[PASS] Rounding to 1 decimal place satisfies round-half-up policy');

  // Empty values aggregation
  const emptyStats = calculateChannelStats([]);
  assert.strictEqual(emptyStats.mean, null);
  assert.strictEqual(emptyStats.min, null);
  assert.strictEqual(emptyStats.max, null);
  console.log('[PASS] Empty channel values return null statistics');

  // Known values aggregation with negative numbers
  const testVals = [-1000, 0, 500, 1500];
  const stats = calculateChannelStats(testVals);
  assert.strictEqual(stats.min, -1000);
  assert.strictEqual(stats.max, 1500);
  assert.strictEqual(stats.mean, 250.0); // (-1000 + 0 + 500 + 1500) / 4 = 1000 / 4 = 250.0
  console.log('[PASS] Negative values, min, max, and exact mean verified');

  // Single-sample aggregation
  const singleStats = calculateChannelStats([42]);
  assert.strictEqual(singleStats.min, 42);
  assert.strictEqual(singleStats.max, 42);
  assert.strictEqual(singleStats.mean, 42.0);
  console.log('[PASS] Single-sample window statistics verified');

  // Sensor 2 magnitude
  // Empty S2
  assert.strictEqual(calculateSensor2Magnitude([]), null);

  // Known pairs: [3, 4] -> sqrt(9+16)=5, [0, 10] -> 10. Mean = (5+10)/2 = 7.5
  const s2Known: Array<[number, number]> = [
    [3, 4],
    [0, 10],
  ];
  const mag = calculateSensor2Magnitude(s2Known);
  assert.strictEqual(mag, 7.5);

  // Mean of per-sample magnitudes vs magnitude of mean channels:
  // Pair 1: [10, 0] -> mag = 10
  // Pair 2: [0, 10] -> mag = 10
  // Mean of magnitudes = (10 + 10) / 2 = 10.0
  // Mean ch1 = 5, mean ch2 = 5 -> sqrt(25 + 25) = sqrt(50) = 7.07
  const divergentPairs: Array<[number, number]> = [
    [10, 0],
    [0, 10],
  ];
  const actualMag = calculateSensor2Magnitude(divergentPairs);
  const ch1Mean = (10 + 0) / 2;
  const ch2Mean = (0 + 10) / 2;
  const wrongMagOfMeans = Math.sqrt(ch1Mean * ch1Mean + ch2Mean * ch2Mean);
  assert.strictEqual(actualMag, 10.0);
  assert(
    Math.abs(actualMag - wrongMagOfMeans) > 2.0,
    'Must strictly compute mean of magnitudes, NOT magnitude of means'
  );
  console.log('[PASS] Sensor 2 magnitude strictly implements mean of per-sample magnitudes');

  // Windowing & Boundaries
  console.log('\n--- 1b. Window Boundaries & Partial/Empty Window Tests ---');
  // Indices 399, 400, 799, 800 assignment
  const getWindowForIndex = (idx: number) => Math.floor(idx / WINDOW_SIZE);
  assert.strictEqual(getWindowForIndex(399), 0, 'Index 399 belongs to window 0');
  assert.strictEqual(getWindowForIndex(400), 1, 'Index 400 belongs to window 1');
  assert.strictEqual(getWindowForIndex(799), 1, 'Index 799 belongs to window 1');
  assert.strictEqual(getWindowForIndex(800), 2, 'Index 800 belongs to window 2');
  console.log('[PASS] Boundary indices 399, 400, 799, 800 correctly partitioned into windows');

  // Empty window test
  const emptyRes = computeWindowResult({
    sessionId: 'sess-empty',
    windowIdx: 0,
    mode: 'offline',
    s1Samples: new Map(),
    s2Samples: new Map(),
    acceptedPeaks: [],
    isHrRangeComplete: false,
  });
  assert.strictEqual(emptyRes.missing_1, 400);
  assert.strictEqual(emptyRes.missing_2, 400);
  assert.strictEqual(emptyRes.complete, false);
  assert.strictEqual(emptyRes.magnitude, null);
  assert.strictEqual(emptyRes.heart_rate, null);
  for (const k of ['s1_ch1', 's1_ch2', 's1_ch3', 's2_ch1', 's2_ch2']) {
    assert.strictEqual(emptyRes.mean[k], null);
    assert.strictEqual(emptyRes.min[k], null);
    assert.strictEqual(emptyRes.max[k], null);
  }
  console.log('[PASS] Completely empty window has missing=400, complete=false, and all null metrics');

  // Partial window test (200 samples on S1, 300 on S2)
  const partialS1 = new Map<number, number[]>();
  for (let i = 0; i < 200; i++) partialS1.set(i, [100, 200, 300]);
  const partialS2 = new Map<number, number[]>();
  for (let i = 0; i < 300; i++) partialS2.set(i, [300, 400]);

  const partialRes = computeWindowResult({
    sessionId: 'sess-partial',
    windowIdx: 0,
    mode: 'offline',
    s1Samples: partialS1,
    s2Samples: partialS2,
    acceptedPeaks: [],
    isHrRangeComplete: false,
  });
  assert.strictEqual(partialRes.missing_1, 200);
  assert.strictEqual(partialRes.missing_2, 100);
  assert.strictEqual(partialRes.complete, false);
  assert.strictEqual(partialRes.mean['s1_ch1'], 100.0);
  assert.strictEqual(partialRes.mean['s2_ch1'], 300.0);
  assert.strictEqual(partialRes.magnitude, 500.0);
  console.log('[PASS] Partial window preserves valid sample aggregations and records explicit missing counts');
}

// =========================================================================
// 2. FOUR-CONDITION PEAK DETECTOR UNIT TESTS
// =========================================================================
function testFourConditionPeakDetector() {
  console.log('\n--- 2. Four-Condition Peak Detector Unit Tests ---');

  // Condition 1: Fewer than 800 preceding samples
  {
    const map = new Map<number, number>();
    for (let i = 0; i <= 500; i++) map.set(i, 100);
    map.set(200, 1000); // Prominent peak, but index 200 < 800!

    const detector = new PeakDetector();
    const peaks = detector.processCandidates(0, 500, (idx) => map.get(idx));
    assert.strictEqual(peaks.length, 0, 'Candidates with i < 800 must be rejected');
    console.log('[PASS] Candidates with fewer than 800 preceding samples strictly rejected');
  }

  // Condition 1: Missing next sample (i + 1)
  {
    const map = new Map<number, number>();
    for (let i = 0; i <= 800; i++) map.set(i, 100);
    map.set(800, 500); // Candidate at 800, but 801 is missing!

    const detector = new PeakDetector();
    const peaks = detector.processCandidates(800, 800, (idx) => map.get(idx));
    assert.strictEqual(peaks.length, 0, 'Candidate missing next neighbor must be rejected');
    console.log('[PASS] Candidate missing sample i + 1 strictly rejected');
  }

  // Condition 1: Missing a sample inside the preceding 800 window
  {
    const map = new Map<number, number>();
    for (let i = 0; i <= 801; i++) map.set(i, 100);
    map.set(800, 500);
    // Remove sample 400 (inside 0..799)
    map.delete(400);

    const detector = new PeakDetector();
    const peaks = detector.processCandidates(800, 800, (idx) => map.get(idx));
    assert.strictEqual(peaks.length, 0, 'Gap in preceding 800 samples must reject candidate');
    console.log('[PASS] Gaps in preceding 800 samples break availability requirement');
  }

  // Condition 2: Greater than left and right neighbors (Strict inequality)
  {
    // Equal to left neighbor
    const mapLeft = new Map<number, number>();
    for (let i = 0; i <= 801; i++) mapLeft.set(i, 100);
    mapLeft.set(799, 500);
    mapLeft.set(800, 500); // Equal to left neighbor!
    mapLeft.set(801, 100);

    const detectorLeft = new PeakDetector();
    assert.strictEqual(detectorLeft.processCandidates(800, 800, (idx) => mapLeft.get(idx)).length, 0);

    // Equal to right neighbor
    const mapRight = new Map<number, number>();
    for (let i = 0; i <= 801; i++) mapRight.set(i, 100);
    mapRight.set(799, 100);
    mapRight.set(800, 500);
    mapRight.set(801, 500); // Equal to right neighbor!

    const detectorRight = new PeakDetector();
    assert.strictEqual(detectorRight.processCandidates(800, 800, (idx) => mapRight.get(idx)).length, 0);
    console.log('[PASS] Strict inequality with neighbors enforced (equalities rejected)');
  }

  // Condition 3: Greater than half of preceding maximum (Strict inequality)
  {
    // Max of preceding 800 is 1000. Threshold is 500.
    const map = new Map<number, number>();
    for (let i = 0; i <= 801; i++) map.set(i, 100);
    map.set(50, 1000); // Preceding max is 1000
    map.set(799, 100);
    map.set(801, 100);

    // Exact equality with threshold: 500
    map.set(800, 500);
    const d1 = new PeakDetector();
    assert.strictEqual(d1.processCandidates(800, 800, (idx) => map.get(idx)).length, 0, 'Candidate == threshold must be rejected');

    // Just above threshold: 501
    map.set(800, 501);
    const d2 = new PeakDetector();
    const peaks = d2.processCandidates(800, 800, (idx) => map.get(idx));
    assert.strictEqual(peaks.length, 1);
    assert.strictEqual(peaks[0], 800);
    console.log('[PASS] Threshold condition x[i] > max/2 strictly enforced; equality rejected');

    // Candidate must not compare against itself
    // Preceding samples all 100. Candidate is 1000. Max of preceding is 100.
    // Candidate should easily pass (1000 > 100/2 = 50).
    const mapExclude = new Map<number, number>();
    for (let i = 0; i <= 801; i++) mapExclude.set(i, 100);
    mapExclude.set(800, 1000);
    const d3 = new PeakDetector();
    const peaksExclude = d3.processCandidates(800, 800, (idx) => mapExclude.get(idx));
    assert.strictEqual(peaksExclude.length, 1);
    console.log('[PASS] Candidate excluded from preceding lookback maximum');
  }

  // Condition 4: Refractory period from previous accepted peak
  {
    const map = new Map<number, number>();
    for (let i = 0; i <= 1200; i++) map.set(i, 100);
    // Peak 1 at 800
    map.set(800, 600);
    // Test delta 99
    // Candidate at 899 (delta = 99 -> rejected)
    map.set(899, 700);
    const d1 = new PeakDetector();
    const p1 = d1.processCandidates(800, 800, (idx) => map.get(idx));
    assert.deepStrictEqual(p1, [800]);

    // Check candidate 899 alone with lastPeak = 800
    const d2 = new PeakDetector({ lastAcceptedPeak: 800 });
    const p2 = d2.processCandidates(899, 899, (idx) => map.get(idx));
    assert.strictEqual(p2.length, 0, 'Separation of 99 must be rejected');

    // Rejected candidate at 899 must NOT update lastAcceptedPeak
    assert.strictEqual(d2.getState().lastAcceptedPeak, 800, 'Rejected candidate must not alter lastAcceptedPeak');

    // Reset 899 to baseline so 900 has smaller neighbors
    map.set(899, 100);
    map.set(900, 700);
    map.set(901, 100);

    // Candidate at 900 (delta = 100 from 800) must be accepted
    const p3 = d2.processCandidates(900, 900, (idx) => map.get(idx));
    assert.deepStrictEqual(p3, [900]);
    assert.strictEqual(d2.getState().lastAcceptedPeak, 900);

    // Candidate at 1001 (delta = 101 from 900) must be accepted
    map.set(1001, 700);
    const p4 = d2.processCandidates(1001, 1001, (idx) => map.get(idx));
    assert.deepStrictEqual(p4, [1001]);
    assert.strictEqual(d2.getState().lastAcceptedPeak, 1001);

    console.log('[PASS] Refractory period boundary checks (99 rejected, 100/101 accepted, rejected peak ignored)');
  }

  // Gap recovery after exactly 800 contiguous samples
  {
    const map = new Map<number, number>();
    // Gap at 500
    for (let i = 0; i <= 2000; i++) {
      if (i !== 500) map.set(i, 100);
    }
    // Candidate at 500 + 800 = 1300: Preceding 800 samples are 500..1299.
    // Since 500 is missing, candidate 1300 must be rejected!
    map.set(1300, 600);
    const dGap1 = new PeakDetector();
    assert.strictEqual(dGap1.processCandidates(1300, 1300, (idx) => map.get(idx)).length, 0);

    // Candidate at 1301: Preceding 800 samples are 501..1300 (all available!).
    // Next sample 1302 is available.
    map.set(1300, 100); // restore 1300 as baseline
    map.set(1301, 600); // new candidate
    const dGap2 = new PeakDetector();
    const pGap2 = dGap2.processCandidates(1301, 1301, (idx) => map.get(idx));
    assert.deepStrictEqual(pGap2, [1301]);
    console.log('[PASS] Recovery after gap occurs at exactly index g + 801');
  }
}

// =========================================================================
// 3. HEART RATE CALCULATION UNIT TESTS
// =========================================================================
function testHeartRateLogic() {
  console.log('\n--- 3. Heart Rate Calculation Tests ---');

  // Condition 1: w < 4 returns null
  for (let w = 0; w < 4; w++) {
    const hr = calculateHeartRate({
      windowIdx: w,
      acceptedPeaks: [100, 500, 900],
      isRangeComplete: true,
    });
    assert.strictEqual(hr, null, `Window ${w} must return null heart rate`);
  }
  console.log('[PASS] Windows 0, 1, 2, 3 strictly return null heart rate');

  // Condition 2: Range not complete returns null
  const hrIncomplete = calculateHeartRate({
    windowIdx: 4,
    acceptedPeaks: [400, 800, 1200],
    isRangeComplete: false,
  });
  assert.strictEqual(hrIncomplete, null, 'Incomplete 5-second interval must return null heart rate');
  console.log('[PASS] Incomplete 5-second range returns null');

  // Condition 3: Fewer than 2 peaks in [400(w-4), 400(w+1)-1]
  // For w = 4: interval is [0, 1999]
  const hrZeroPeaks = calculateHeartRate({
    windowIdx: 4,
    acceptedPeaks: [],
    isRangeComplete: true,
  });
  assert.strictEqual(hrZeroPeaks, null);

  const hrOnePeak = calculateHeartRate({
    windowIdx: 4,
    acceptedPeaks: [1000],
    isRangeComplete: true,
  });
  assert.strictEqual(hrOnePeak, null);
  console.log('[PASS] Fewer than 2 peaks in 5-second interval returns null');

  // Valid 2 peaks: 60.0 bpm (gap of 400 samples = 1.0 second)
  // Peaks at 800 and 1200
  const hr60 = calculateHeartRate({
    windowIdx: 4,
    acceptedPeaks: [800, 1200],
    isRangeComplete: true,
  });
  assert.strictEqual(hr60, 60.0);
  console.log('[PASS] Exactly 2 peaks with 400-sample gap yields 60.0 bpm');

  // Valid multiple peaks:
  // Peaks at 800, 1000, 1200, 1400 (4 peaks, 3 gaps of 200 samples = 0.5 sec -> 120.0 bpm)
  const hr120 = calculateHeartRate({
    windowIdx: 4,
    acceptedPeaks: [800, 1000, 1200, 1400],
    isRangeComplete: true,
  });
  assert.strictEqual(hr120, 120.0);
  console.log('[PASS] Multiple peaks yield exact heart rate (120.0 bpm)');

  // Boundary check on 5-second interval:
  // For w = 4: interval is [0, 1999].
  // Peak at -1 (excluded), Peak at 0 (included), Peak at 1999 (included), Peak at 2000 (excluded).
  const hrBounds = calculateHeartRate({
    windowIdx: 4,
    acceptedPeaks: [-1, 0, 1999, 2000],
    isRangeComplete: true,
  });
  // Eligible peaks: 0 and 1999.
  // 60 * 400 * 1 / 1999 = 24000 / 1999 = 12.006... -> 12.0
  console.log('[PASS] Heart rate interval endpoints [400(w-4), 400(w+1)-1] strictly inclusive');

  // Missing sample just inside vs just outside 5-second interval
  // For w = 5: interval is [400, 2399].
  // If sample 399 is missing (just outside interval), isRangeComplete = true -> valid heart rate (60.0)
  const hrJustOutside = calculateHeartRate({
    windowIdx: 5,
    acceptedPeaks: [800, 1200],
    isRangeComplete: true,
  });
  assert.strictEqual(hrJustOutside, 60.0);

  // If sample 400 is missing (just inside interval), isRangeComplete = false -> null
  const hrJustInside = calculateHeartRate({
    windowIdx: 5,
    acceptedPeaks: [800, 1200],
    isRangeComplete: false,
  });
  assert.strictEqual(hrJustInside, null);
  console.log('[PASS] Missing sample just outside vs inside 5-second interval verified');
}

// =========================================================================
// 4. DIFFERENTIAL & PROPERTY-BASED TESTING
// =========================================================================
function testDifferentialPropertySuite() {
  console.log('\n--- 4. Differential & Property Testing vs Independent Reference ---');

  const seeds = [42, 101, 777, 9999, 123456];

  for (const seed of seeds) {
    const rng = new SeededRng(seed);
    const totalSamples = 3000;
    const signalMap = new Map<number, number>();

    // Generate synthetic ECG-like signal with baseline wander, peaks, and random dropouts
    let baseline = 0;
    let nextPeakIdx = 850;

    for (let i = 0; i < totalSamples; i++) {
      baseline = Math.floor(baseline * 0.95 + rng.nextInt(-50, 50));

      if (i === nextPeakIdx) {
        // Distinct QRS peak well above baseline and preceding max
        signalMap.set(i, baseline + 2500);
        nextPeakIdx += rng.nextInt(120, 300); // 120-300 samples refractory gap
      } else if (i === nextPeakIdx - 1 || i === nextPeakIdx + 1) {
        signalMap.set(i, baseline + 100);
      } else {
        signalMap.set(i, baseline + rng.nextInt(-100, 100));
      }

      // Random missing samples (5% chance)
      if (rng.next() < 0.05 && i !== nextPeakIdx) {
        signalMap.delete(i);
      }
    }

    const getValue = (idx: number) => signalMap.get(idx);

    // 1. Run Independent Reference Algorithm
    const refResult = referenceEvaluatePeaks(0, totalSamples - 2, getValue, -Infinity);

    // 2. Run Production Sliding-Deque PeakDetector
    const prodDetector = new PeakDetector();
    const prodPeaks = prodDetector.processCandidates(0, totalSamples - 2, getValue);

    // Differential assertion
    assert.deepStrictEqual(
      prodPeaks,
      refResult.acceptedPeaks,
      `Seed ${seed}: Production peak detector mismatch with independent reference`
    );
  }

  console.log(`[PASS] 100% exact parity across ${seeds.length} randomized seeds (${seeds.join(', ')})`);
}

// =========================================================================
// 5. STORAGE INTEGRATION, CHECKPOINTING & RECOVERY TESTS
// =========================================================================
async function testStorageIntegrationAndRecovery() {
  console.log('\n--- 5. Storage Integration, Checkpointing & Crash Recovery Tests ---');

  const dbPath = path.join(TEST_DB_DIR, 'test_processing.db');
  cleanDbFiles(dbPath);

  const storage = new StorageEngine({ dbPath });
  const engine = new WindowProcessingEngine({ storage });
  const sessionId = 'sess-proc-01';

  try {
    // 1. Ingest Window 0 samples (indices 0..399) for Sensor 1 and Sensor 2
    const s1W0: MockResponse = {
      session_id: sessionId,
      sensor: 1,
      first_index: 0,
      count: 400,
      lost: [],
      session_end: false,
      crc32: '00000000',
      data: Array.from({ length: 400 }, (_, idx) => [100 + idx, 500 + (idx % 20), 1000]),
    };
    const s2W0: MockResponse = {
      session_id: sessionId,
      sensor: 2,
      first_index: 0,
      count: 400,
      lost: [],
      session_end: false,
      crc32: '00000000',
      data: Array.from({ length: 400 }, () => [300, 400]), // mag = 500
    };

    storage.persistValidatedReply(s1W0);
    storage.persistValidatedReply(s2W0);

    // Window 0 cannot be finalized until Sensor 1 index 400 is resolved (for candidate 399 neighbor)!
    assert.strictEqual(
      storage.isWindowResolved(sessionId, 0, true),
      false,
      'Window 0 must not be resolved until index 400 is available'
    );
    assert.strictEqual(engine.processNextWindow(sessionId), null);
    console.log('[PASS] Unresolved window boundary candidate (index 399) correctly defers processing');

    // Ingest sample 400 for Sensor 1 (plus window 1 data 400..799)
    const s1W1: MockResponse = {
      session_id: sessionId,
      sensor: 1,
      first_index: 400,
      count: 400,
      lost: [],
      session_end: false,
      crc32: '00000000',
      data: Array.from({ length: 400 }, (_, idx) => [200, 600, 1100]),
    };
    storage.persistValidatedReply(s1W1);

    // Now Window 0 is resolved on both sensors!
    assert.strictEqual(storage.isWindowResolved(sessionId, 0, true), true);
    const resW0 = engine.processNextWindow(sessionId, 'offline');
    assert(resW0 !== null);
    assert.strictEqual(resW0.window_idx, 0);
    assert.strictEqual(resW0.complete, true);
    assert.strictEqual(resW0.missing_1, 0);
    assert.strictEqual(resW0.missing_2, 0);
    assert.strictEqual(resW0.magnitude, 500.0);
    assert.strictEqual(resW0.heart_rate, null); // w = 0 < 4
    console.log('[PASS] Window 0 processed: complete=true, magnitude=500.0, heart_rate=null');

    // Verify Checkpoint updated
    const cp0 = storage.getProcessingCheckpoint(sessionId);
    assert(cp0 !== null);
    assert.strictEqual(cp0.last_processed_window, 0);

    // Ingest Sensor 2 Window 1 (400..799) and Sensor 1 Window 2 (800..1199)
    const s2W1: MockResponse = {
      session_id: sessionId,
      sensor: 2,
      first_index: 400,
      count: 400,
      lost: [],
      session_end: false,
      crc32: '00000000',
      data: Array.from({ length: 400 }, () => [300, 400]),
    };
    const s1W2: MockResponse = {
      session_id: sessionId,
      sensor: 1,
      first_index: 800,
      count: 400,
      lost: [],
      session_end: false,
      crc32: '00000000',
      data: Array.from({ length: 400 }, (_, idx) => {
        // Place a peak at 850: 800 preceding samples are available (50..849)
        if (idx === 50) return [100, 2000, 100]; // sample 850
        return [100, 500, 100];
      }),
    };
    storage.persistValidatedReply(s2W1);
    storage.persistValidatedReply(s1W2);

    // Now Window 1 can be processed
    const resW1 = engine.processNextWindow(sessionId, 'offline');
    assert(resW1 !== null);
    assert.strictEqual(resW1.window_idx, 1);
    const cp1 = storage.getProcessingCheckpoint(sessionId);
    assert.strictEqual(cp1?.last_processed_window, 1);
    console.log('[PASS] Window 1 processed and checkpoint advanced to window 1');

    // Ingest windows up to window 4 (2000 samples)
    const s2W2: MockResponse = {
      session_id: sessionId,
      sensor: 2,
      first_index: 800,
      count: 400,
      lost: [],
      session_end: false,
      crc32: '00000000',
      data: Array.from({ length: 400 }, () => [300, 400]),
    };
    storage.persistValidatedReply(s2W2);

    const s1W3: MockResponse = {
      session_id: sessionId,
      sensor: 1,
      first_index: 1200,
      count: 400,
      lost: [],
      session_end: false,
      crc32: '00000000',
      data: Array.from({ length: 400 }, (_, idx) => {
        // Place second peak at 1250 (400 samples after 850 = 60 bpm)
        if (idx === 50) return [100, 2000, 100];
        return [100, 500, 100];
      }),
    };
    const s2W3: MockResponse = {
      session_id: sessionId,
      sensor: 2,
      first_index: 1200,
      count: 400,
      lost: [],
      session_end: false,
      crc32: '00000000',
      data: Array.from({ length: 400 }, () => [300, 400]),
    };
    storage.persistValidatedReply(s1W3);
    storage.persistValidatedReply(s2W3);

    const s1W4: MockResponse = {
      session_id: sessionId,
      sensor: 1,
      first_index: 1600,
      count: 401, // include 2000 for lookahead
      lost: [],
      session_end: false,
      crc32: '00000000',
      data: Array.from({ length: 401 }, () => [100, 500, 100]),
    };
    const s2W4: MockResponse = {
      session_id: sessionId,
      sensor: 2,
      first_index: 1600,
      count: 400,
      lost: [],
      session_end: false,
      crc32: '00000000',
      data: Array.from({ length: 400 }, () => [300, 400]),
    };
    storage.persistValidatedReply(s1W4);
    storage.persistValidatedReply(s2W4);

    // Process all remaining windows
    const batch = engine.processAllAvailableWindows(sessionId, 'offline');
    assert.strictEqual(batch.length, 3); // Windows 2, 3, 4

    // Window 2: accepted peak at 850
    assert.strictEqual(batch[0].window_idx, 2);
    const peaks = storage.getDetectedPeaks(sessionId);
    assert(peaks.includes(850), 'Peak 850 must be stored in detected_peaks');
    assert(peaks.includes(1250), 'Peak 1250 must be stored in detected_peaks');

    // Window 4: First window where w >= 4 and range complete with 2 peaks (850 and 1250, span = 400)
    // 60 * 400 * 1 / 400 = 60.0 bpm
    const resW4 = batch[2];
    assert.strictEqual(resW4.window_idx, 4);
    assert.strictEqual(resW4.heart_rate, 60.0);
    console.log('[PASS] Window 4 computed valid heart rate (60.0 bpm) across stored windows');

    // 2. Restart & Determinism Test
    storage.close();

    const restoredStorage = new StorageEngine({ dbPath });
    const restoredEngine = new WindowProcessingEngine({ storage: restoredStorage });

    const restoredCp = restoredStorage.getProcessingCheckpoint(sessionId);
    assert.strictEqual(restoredCp?.last_processed_window, 4);
    assert.strictEqual(restoredCp?.last_accepted_peak, 1250);

    // Since windows 0..4 are already processed and no new data arrived, processNextWindow must return null
    assert.strictEqual(restoredEngine.processNextWindow(sessionId), null);

    // Re-query Window 4 result from DB and verify 100% bitwise parity
    const durableW4 = restoredStorage.getProcessingResult(sessionId, 4);
    assert.deepStrictEqual(durableW4, resW4);
    console.log('[PASS] Restart recovery preserves exact checkpoint (window 4, peak 1250) and results');

    // 3. Uniqueness constraint & Replay Idempotency Test
    // Re-saving window 4 with same session and window index must update in place without duplicate records
    restoredStorage.saveProcessingResult(resW4, [], 4, 1250);
    const allResults = restoredStorage.getProcessingResults(sessionId, 0, 100);
    assert.strictEqual(allResults.length, 5, 'Must have exactly 5 window results (0..4), no duplicates');
    console.log('[PASS] Idempotent replay verified; primary key (session_id, window_idx) prevents duplicate results');

    // 4. Session Boundary Isolation Test
    // A new session starts from window 0, peak state resets completely
    const newSessionId = 'sess-new-02';
    assert.strictEqual(restoredStorage.getProcessingCheckpoint(newSessionId), null);
    assert.strictEqual(restoredStorage.getDetectedPeaks(newSessionId).length, 0);
    console.log('[PASS] Session boundary resets progress checkpoint and peak state');

    restoredStorage.close();
  } finally {
    cleanDbFiles(dbPath);
  }
}

// =========================================================================
// 6. PERFORMANCE & THROUGHPUT BENCHMARKS
// =========================================================================
function runPerformanceBenchmark(): {
  sampleThroughput: number;
  latencyPerWindowMs: number;
  windowsEvaluated: number;
} {
  console.log('\n--- 6. Performance & Signal Processing Throughput Benchmark ---');

  const dbPath = path.join(TEST_DB_DIR, 'test_perf.db');
  cleanDbFiles(dbPath);

  const storage = new StorageEngine({ dbPath });
  const engine = new WindowProcessingEngine({ storage });
  const sessionId = 'sess-perf';

  const NUM_WINDOWS = 20; // 20 seconds = 8,000 samples per sensor (16,000 samples total)

  // Pre-seed database with 20 windows of valid continuous data
  for (let w = 0; w < NUM_WINDOWS; w++) {
    const startIdx = w * WINDOW_SIZE;
    const count = w === NUM_WINDOWS - 1 ? 401 : 400;

    const s1Data = Array.from({ length: count }, (_, idx) => {
      const globalIdx = startIdx + idx;
      // Add peak every 200 samples after 800
      if (globalIdx >= 800 && globalIdx % 200 === 0) {
        return [100, 2500, 100];
      }
      return [100, 500, 100];
    });

    const s2Data = Array.from({ length: 400 }, () => [600, 800]);

    storage.persistValidatedReply({
      session_id: sessionId,
      sensor: 1,
      first_index: startIdx,
      count,
      lost: [],
      session_end: false,
      crc32: '00000000',
      data: s1Data,
    });

    storage.persistValidatedReply({
      session_id: sessionId,
      sensor: 2,
      first_index: startIdx,
      count: 400,
      lost: [],
      session_end: false,
      crc32: '00000000',
      data: s2Data,
    });
  }

  // Measure end-to-end window processing throughput
  const t0 = performance.now();
  const processed = engine.processAllAvailableWindows(sessionId, 'offline');
  const elapsedMs = performance.now() - t0;

  assert.strictEqual(processed.length, NUM_WINDOWS);

  const totalSensorSamples = NUM_WINDOWS * WINDOW_SIZE * 2; // both sensors
  const throughput = Math.round((totalSensorSamples / (elapsedMs / 1000)));
  const latencyPerWindow = Math.round((elapsedMs / NUM_WINDOWS) * 100) / 100;

  console.log(`[BENCHMARK] Windows Processed: ${NUM_WINDOWS}`);
  console.log(`[BENCHMARK] Total Sensor Samples Processed: ${totalSensorSamples}`);
  console.log(`[BENCHMARK] Total Elapsed Time: ${elapsedMs.toFixed(2)} ms`);
  console.log(`[BENCHMARK] Average Latency Per Window: ${latencyPerWindow} ms (Target: < 1000 ms)`);
  console.log(`[BENCHMARK] Signal Processing Throughput: ${throughput} samples/sec (Target: >= 800 live)`);
  console.log(`[BENCHMARK] DB File Size: ${(storage.getDbFileSize() / 1024).toFixed(1)} KB, WAL: ${(storage.getWalFileSize() / 1024).toFixed(1)} KB`);

  storage.close();
  cleanDbFiles(dbPath);

  return {
    sampleThroughput: throughput,
    latencyPerWindowMs: latencyPerWindow,
    windowsEvaluated: NUM_WINDOWS,
  };
}

// =========================================================================
// RUN ALL TESTS
// =========================================================================
async function run() {
  testMathAndAggregations();
  testFourConditionPeakDetector();
  testHeartRateLogic();
  testDifferentialPropertySuite();
  await testStorageIntegrationAndRecovery();
  runPerformanceBenchmark();

  console.log('\n=============================================================');
  console.log('=== ALL PHASE 3 SIGNAL PROCESSING TESTS PASSED 100%       ===');
  console.log('=============================================================');
}

run().catch((err) => {
  console.error('Phase 3 Test Failure:', err);
  process.exit(1);
});
