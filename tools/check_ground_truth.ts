import * as fs from 'node:fs';
import * as readline from 'node:readline';
import { BiosignalMockServer } from './biosignal_mock.ts';

export interface GroundTruthResult {
  pass: boolean;
  totalSamplesExpected: number;
  totalSamplesFound: number;
  totalLossRanges: number;
  unaccountedGaps: Array<{ sensor: number; from_index: number; to_index: number }>;
  corruptedSamples: Array<{ sensor: number; index: number; expected: number[]; actual: number[] }>;
  duplicateSamples: Array<{ sensor: number; index: number; count: number }>;
  errors: string[];
}

export async function checkExportAgainstGroundTruth(
  exportFilePath: string,
  mode: 'signal' | 'ramp',
  seed: number,
  expectedSessions?: string[]
): Promise<GroundTruthResult> {
  const result: GroundTruthResult = {
    pass: true,
    totalSamplesExpected: 0,
    totalSamplesFound: 0,
    totalLossRanges: 0,
    unaccountedGaps: [],
    corruptedSamples: [],
    duplicateSamples: [],
    errors: [],
  };

  if (!fs.existsSync(exportFilePath)) {
    result.pass = false;
    result.errors.push(`Export file does not exist: ${exportFilePath}`);
    return result;
  }

  const mock = new BiosignalMockServer({ mode, seed, duration: 3600, port: 7100 });

  // Detect encoding: handle PowerShell UTF-16 LE redirection (0xff 0xfe) as well as UTF-8
  let fileEncoding: BufferEncoding = 'utf-8';
  try {
    const fd = fs.openSync(exportFilePath, 'r');
    const header = Buffer.alloc(2);
    fs.readSync(fd, header, 0, 2, 0);
    fs.closeSync(fd);
    if (header[0] === 0xff && header[1] === 0xfe) {
      fileEncoding = 'utf16le';
    }
  } catch {}

  const fileStream = fs.createReadStream(exportFilePath, { encoding: fileEncoding });
  const rl = readline.createInterface({ input: fileStream, crlfDelay: Infinity });

  // Map of (sensor, index) -> sample values
  const s1Samples = new Map<number, number[]>();
  const s2Samples = new Map<number, number[]>();
  const duplicateCounts = new Map<string, number>();

  const losses: Array<{ sensor: number; from_index: number; to_index: number | null; reason: string }> = [];

  for await (const line of rl) {
    let trimmed = line.trim();
    if (!trimmed) continue;
    if (trimmed.charCodeAt(0) === 0xfeff) {
      trimmed = trimmed.slice(1).trim();
      if (!trimmed) continue;
    }

    let parsed: any;
    try {
      parsed = JSON.parse(trimmed);
    } catch (e: any) {
      result.errors.push(`Malformed JSON line: ${trimmed}`);
      result.pass = false;
      continue;
    }

    if (parsed.values !== undefined) {
      // Sample record: {"sensor": 1, "index": 120000, "values": [720, 1200, 1560]}
      const sensor = parsed.sensor;
      const idx = parsed.index;
      const values = parsed.values;

      const key = `${sensor}:${idx}`;
      const existingCount = duplicateCounts.get(key) || 0;
      duplicateCounts.set(key, existingCount + 1);

      if (existingCount > 0) {
        result.duplicateSamples.push({ sensor, index: idx, count: existingCount + 1 });
        result.pass = false;
      }

      const map = sensor === 1 ? s1Samples : s2Samples;
      map.set(idx, values);
      result.totalSamplesFound++;

      // Verify value correctness against deterministic math
      const [expectedS1, expectedS2] = mock.computeSample(idx);
      const expectedValues = sensor === 1 ? expectedS1 : expectedS2;

      let match = values.length === expectedValues.length;
      if (match) {
        for (let c = 0; c < values.length; c++) {
          if (values[c] !== expectedValues[c]) {
            match = false;
            break;
          }
        }
      }

      if (!match) {
        result.corruptedSamples.push({ sensor, index: idx, expected: expectedValues, actual: values });
        result.pass = false;
      }
    } else if (parsed.reason !== undefined) {
      // Loss record: {"sensor": 2, "from_index": 4000, "to_index": 4399, "reason": "buffer_overflow"}
      losses.push(parsed);
      result.totalLossRanges++;
    }
  }

  // Audit for silent gaps in Sensor 1 and Sensor 2
  for (const sensor of [1, 2]) {
    const map = sensor === 1 ? s1Samples : s2Samples;
    if (map.size === 0) continue;

    const indices = Array.from(map.keys()).sort((a, b) => a - b);
    const minIdx = indices[0];
    const maxIdx = indices[indices.length - 1];

    for (let i = minIdx; i <= maxIdx; i++) {
      if (!map.has(i)) {
        // Is this index covered by an explicit loss record?
        const covered = losses.some(
          (l) => l.sensor === sensor && i >= l.from_index && (l.to_index === null || i <= l.to_index)
        );

        if (!covered) {
          result.unaccountedGaps.push({ sensor, from_index: i, to_index: i });
          result.pass = false;
        }
      }
    }
  }

  if (result.corruptedSamples.length > 0 || result.unaccountedGaps.length > 0 || result.duplicateSamples.length > 0) {
    result.pass = false;
  }

  return result;
}

if (process.argv[1] && (process.argv[1].endsWith('check_ground_truth.ts') || process.argv[1].includes('check_ground_truth'))) {
  const args = process.argv.slice(2);
  if (args.length < 1 || args.includes('--help') || args.includes('-h')) {
    console.log('Usage: node --experimental-strip-types tools/check_ground_truth.ts <export-file.ndjson> [mode: signal|ramp] [seed]');
    process.exit(args.includes('--help') || args.includes('-h') ? 0 : 1);
  }
  const file = args[0];
  const mode = (args[1] || 'ramp') as 'signal' | 'ramp';
  const seed = args[2] ? parseInt(args[2], 10) : 42;
  checkExportAgainstGroundTruth(file, mode, seed).then((res) => {
    console.log(JSON.stringify(res, null, 2));
    process.exit(res.pass ? 0 : 1);
  }).catch((err) => {
    console.error('Ground truth check failed:', err);
    process.exit(1);
  });
}
