import * as fs from 'node:fs';
import * as readline from 'node:readline';

export interface AuditVerificationReport {
  pass: boolean;
  totalSamplesFound: number;
  totalLossLines: number;
  orderingViolation: boolean;
  duplicates: Array<{ sensor: number; index: number; count: number }>;
  corruptSamples: Array<{ sensor: number; index: number; expected: number[]; actual: number[] }>;
  unaccountedGaps: Array<{ sensor: number; fromIndex: number; toIndex: number }>;
  invalidLossLines: Array<{ line: string; reason: string }>;
  errors: string[];
}

/**
 * Independent ground-truth verifier for Ramp mode data.
 * Pure independent implementation: does NOT import biosignal_mock or check_ground_truth.
 */
export async function verifyRampExport(filePath: string): Promise<AuditVerificationReport> {
  const report: AuditVerificationReport = {
    pass: true,
    totalSamplesFound: 0,
    totalLossLines: 0,
    orderingViolation: false,
    duplicates: [],
    corruptSamples: [],
    unaccountedGaps: [],
    invalidLossLines: [],
    errors: [],
  };

  if (!fs.existsSync(filePath)) {
    report.pass = false;
    report.errors.push(`File does not exist: ${filePath}`);
    return report;
  }

  // Handle potential UTF-16 LE BOM from PowerShell redirection
  let encoding: BufferEncoding = 'utf-8';
  try {
    const fd = fs.openSync(filePath, 'r');
    const header = Buffer.alloc(2);
    fs.readSync(fd, header, 0, 2, 0);
    fs.closeSync(fd);
    if (header[0] === 0xff && header[1] === 0xfe) {
      encoding = 'utf16le';
    }
  } catch {}

  const fileStream = fs.createReadStream(filePath, { encoding });
  const rl = readline.createInterface({ input: fileStream, crlfDelay: Infinity });

  const s1Samples = new Map<number, number[]>();
  const s2Samples = new Map<number, number[]>();
  const sampleCounts = new Map<string, number>();
  const losses: Array<{ sensor: number; from_index: number; to_index: number | null; reason: string }> = [];

  let seenLossLine = false;

  for await (const rawLine of rl) {
    let line = rawLine.trim();
    if (!line) continue;
    if (line.charCodeAt(0) === 0xfeff) line = line.slice(1).trim();
    if (!line) continue;

    let obj: any;
    try {
      obj = JSON.parse(line);
    } catch (err: any) {
      report.pass = false;
      report.errors.push(`Unparseable JSON line: ${line}`);
      continue;
    }

    if (obj.values !== undefined) {
      // Sample line
      if (seenLossLine) {
        // Spec rule: sample lines {"sensor":1,"index":120000,"values":[...]} THEN loss lines!
        report.orderingViolation = true;
        report.pass = false;
        report.errors.push(`Sample line found after loss line (Ordering violation): ${line}`);
      }

      const sensor = obj.sensor;
      const index = obj.index;
      const values = obj.values;

      if (sensor !== 1 && sensor !== 2) {
        report.pass = false;
        report.errors.push(`Invalid sensor ${sensor} at index ${index}`);
        continue;
      }

      const key = `${sensor}:${index}`;
      const count = (sampleCounts.get(key) || 0) + 1;
      sampleCounts.set(key, count);
      if (count > 1) {
        report.duplicates.push({ sensor, index, count });
        report.pass = false;
      }

      const map = sensor === 1 ? s1Samples : s2Samples;
      map.set(index, values);
      report.totalSamplesFound++;

      // Verify Ramp ground truth:
      // Sensor 1: [(3*i + 0) % 8388608, (3*i + 1) % 8388608, (3*i + 2) % 8388608]
      // Sensor 2: [(2*i + 0) % 8388608, (2*i + 1) % 8388608]
      let expected: number[];
      if (sensor === 1) {
        expected = [
          (3 * index + 0) % 8388608,
          (3 * index + 1) % 8388608,
          (3 * index + 2) % 8388608,
        ];
      } else {
        expected = [
          (2 * index + 0) % 8388608,
          (2 * index + 1) % 8388608,
        ];
      }

      if (!Array.isArray(values) || values.length !== expected.length) {
        report.corruptSamples.push({ sensor, index, expected, actual: values });
        report.pass = false;
      } else {
        let mismatch = false;
        for (let c = 0; c < expected.length; c++) {
          if (values[c] !== expected[c]) {
            mismatch = true;
            break;
          }
        }
        if (mismatch) {
          report.corruptSamples.push({ sensor, index, expected, actual: values });
          report.pass = false;
        }
      }
    } else if (obj.from_index !== undefined) {
      // Loss line: {"sensor":2,"from_index":4000,"to_index":4399,"reason":"..."}
      seenLossLine = true;
      report.totalLossLines++;

      if (obj.sensor !== 1 && obj.sensor !== 2) {
        report.invalidLossLines.push({ line, reason: `Invalid sensor: ${obj.sensor}` });
        report.pass = false;
      }
      if (!Number.isInteger(obj.from_index) || obj.from_index < 0) {
        report.invalidLossLines.push({ line, reason: `Invalid from_index: ${obj.from_index}` });
        report.pass = false;
      }
      if (obj.to_index !== null && (!Number.isInteger(obj.to_index) || obj.to_index < obj.from_index)) {
        report.invalidLossLines.push({ line, reason: `Invalid to_index: ${obj.to_index}` });
        report.pass = false;
      }
      if (typeof obj.reason !== 'string' || obj.reason.trim().length === 0) {
        report.invalidLossLines.push({ line, reason: `Missing or empty reason: ${obj.reason}` });
        report.pass = false;
      }

      losses.push({
        sensor: obj.sensor,
        from_index: obj.from_index,
        to_index: obj.to_index,
        reason: obj.reason,
      });
    } else {
      report.pass = false;
      report.errors.push(`Unrecognized JSON record format: ${line}`);
    }
  }

  // Check for unaccounted gaps below the highest observed index for each sensor
  for (const sensor of [1, 2] as const) {
    const map = sensor === 1 ? s1Samples : s2Samples;
    const indices = Array.from(map.keys()).sort((a, b) => a - b);
    if (indices.length === 0) continue;

    const maxIndex = indices[indices.length - 1];
    const sensorLosses = losses.filter((l) => l.sensor === sensor);

    let gapStart: number | null = null;

    for (let i = 0; i <= maxIndex; i++) {
      if (map.has(i)) {
        if (gapStart !== null) {
          report.unaccountedGaps.push({ sensor, fromIndex: gapStart, toIndex: i - 1 });
          report.pass = false;
          gapStart = null;
        }
        continue;
      }

      // Sample missing at i: is it covered by a loss range?
      const isCovered = sensorLosses.some(
        (l) => i >= l.from_index && (l.to_index === null || i <= l.to_index)
      );

      if (isCovered) {
        if (gapStart !== null) {
          report.unaccountedGaps.push({ sensor, fromIndex: gapStart, toIndex: i - 1 });
          report.pass = false;
          gapStart = null;
        }
        continue;
      }

      // Missing and NOT covered
      if (gapStart === null) {
        gapStart = i;
      }
    }

    if (gapStart !== null) {
      report.unaccountedGaps.push({ sensor, fromIndex: gapStart, toIndex: maxIndex });
      report.pass = false;
    }
  }

  return report;
}
