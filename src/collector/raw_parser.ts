import { crc32 } from 'node:zlib';
import type { MockResponse, RawValidationResult, SensorId } from '../types.ts';

const MIN_SIGNED_24 = -8388608;
const MAX_SIGNED_24 = 8388607;

/**
 * Quote-aware byte scanner to extract the exact slice of the top-level `data` array
 * from the raw UTF-8 Buffer without JSON re-serialization.
 */
export function extractDataArraySlice(rawBuf: Buffer): { start: number; end: number; slice: Buffer } | null {
  let inString = false;
  let escape = false;
  let braceDepth = 0;
  let bracketDepth = 0;

  let currentKey = '';
  let collectingKey = false;
  let afterColon = false;
  let dataArrayStart = -1;

  for (let i = 0; i < rawBuf.length; i++) {
    const byte = rawBuf[i];
    const char = String.fromCharCode(byte);

    if (inString) {
      if (escape) {
        escape = false;
        if (collectingKey) currentKey += char;
      } else if (char === '\\') {
        escape = true;
      } else if (char === '"') {
        inString = false;
        collectingKey = false;
      } else {
        if (collectingKey) currentKey += char;
      }
      continue;
    }

    // Outside string
    if (char === '"') {
      inString = true;
      if (braceDepth === 1 && bracketDepth === 0 && !afterColon) {
        currentKey = '';
        collectingKey = true;
      }
      continue;
    }

    if (char === '{') {
      braceDepth++;
      continue;
    }

    if (char === '}') {
      braceDepth--;
      continue;
    }

    if (braceDepth === 1 && bracketDepth === 0) {
      if (char === ':') {
        afterColon = true;
        continue;
      }
      if (char === ',') {
        afterColon = false;
        currentKey = '';
        continue;
      }
    }

    if (char === '[') {
      bracketDepth++;
      if (braceDepth === 1 && bracketDepth === 1 && afterColon && currentKey.trim() === 'data') {
        dataArrayStart = i;
      }
      continue;
    }

    if (char === ']') {
      if (braceDepth === 1 && bracketDepth === 1 && dataArrayStart !== -1) {
        const dataArrayEnd = i;
        const slice = rawBuf.subarray(dataArrayStart, dataArrayEnd + 1);
        return { start: dataArrayStart, end: dataArrayEnd, slice };
      }
      bracketDepth--;
      continue;
    }
  }

  return null;
}

/**
 * Validate a raw newline-delimited JSON line according to MayaCardio Section 6 specs.
 */
export function validateRawResponse(
  rawBuf: Buffer,
  expectedSensor?: SensorId
): RawValidationResult {
  if (rawBuf.length === 0) {
    return { ok: false, code: 'TRUNCATED_OR_MALFORMED', error: 'Empty payload buffer', rawBytes: rawBuf };
  }

  let text: string;
  let parsed: any;
  try {
    text = rawBuf.toString('utf-8').trim();
    parsed = JSON.parse(text);
  } catch (err: any) {
    return {
      ok: false,
      code: 'TRUNCATED_OR_MALFORMED',
      error: `Malformed JSON: ${err.message}`,
      rawBytes: rawBuf,
    };
  }

  // 1. Extract exact raw data slice
  const extracted = extractDataArraySlice(rawBuf);
  if (!extracted) {
    return {
      ok: false,
      code: 'INVALID_SCHEMA',
      error: 'Could not extract top-level data array slice from payload',
      rawBytes: rawBuf,
    };
  }

  if (typeof parsed.crc32 !== 'string' || !/^[0-9a-fA-F]{1,8}$/.test(parsed.crc32.trim())) {
    return {
      ok: false,
      code: 'INVALID_SCHEMA',
      error: 'crc32 field must be a valid hex string',
      rawBytes: rawBuf,
    };
  }

  const computedCrcInt = crc32(extracted.slice);
  const computedCrcHex = computedCrcInt.toString(16).padStart(8, '0').toLowerCase();
  const reportedCrcHex = parsed.crc32.trim().toLowerCase().padStart(8, '0');

  if (computedCrcHex !== reportedCrcHex) {
    return {
      ok: false,
      code: 'CRC_MISMATCH',
      error: `CRC32 mismatch: reported ${reportedCrcHex}, computed ${computedCrcHex}`,
      rawBytes: rawBuf,
    };
  }

  // 3. Validate Schema Fields
  if (!parsed.session_id || typeof parsed.session_id !== 'string') {
    return {
      ok: false,
      code: 'INVALID_SCHEMA',
      error: 'Missing or invalid session_id',
      rawBytes: rawBuf,
    };
  }

  if (parsed.sensor !== 1 && parsed.sensor !== 2) {
    return {
      ok: false,
      code: 'INVALID_SCHEMA',
      error: `Invalid sensor ID: ${parsed.sensor}`,
      rawBytes: rawBuf,
    };
  }

  if (expectedSensor !== undefined && parsed.sensor !== expectedSensor) {
    return {
      ok: false,
      code: 'INVALID_SCHEMA',
      error: `Sensor ID mismatch: expected ${expectedSensor}, got ${parsed.sensor}`,
      rawBytes: rawBuf,
    };
  }

  if (!Number.isInteger(parsed.first_index) || parsed.first_index < 0) {
    return {
      ok: false,
      code: 'INVALID_SCHEMA',
      error: `Invalid first_index: ${parsed.first_index}`,
      rawBytes: rawBuf,
    };
  }

  if (!Number.isInteger(parsed.count) || parsed.count < 0) {
    return {
      ok: false,
      code: 'INVALID_SCHEMA',
      error: `Invalid count: ${parsed.count}`,
      rawBytes: rawBuf,
    };
  }

  if (!Array.isArray(parsed.data) || parsed.data.length !== parsed.count) {
    return {
      ok: false,
      code: 'INVALID_SCHEMA',
      error: `data length (${parsed.data?.length}) does not match count (${parsed.count})`,
      rawBytes: rawBuf,
    };
  }

  if (!Array.isArray(parsed.lost)) {
    return {
      ok: false,
      code: 'INVALID_SCHEMA',
      error: 'lost field must be an array',
      rawBytes: rawBuf,
    };
  }

  // Validate loss ranges structure
  for (const lr of parsed.lost) {
    if (!Number.isInteger(lr.from_index) || lr.from_index < 0) {
      return { ok: false, code: 'INVALID_SCHEMA', error: 'Invalid from_index in loss range', rawBytes: rawBuf };
    }
    if (lr.to_index !== null && (!Number.isInteger(lr.to_index) || lr.to_index < lr.from_index)) {
      return { ok: false, code: 'INVALID_SCHEMA', error: 'Invalid to_index in loss range', rawBytes: rawBuf };
    }
    if (!lr.reason || typeof lr.reason !== 'string') {
      return { ok: false, code: 'INVALID_SCHEMA', error: 'Missing reason in loss range', rawBytes: rawBuf };
    }
  }

  if (typeof parsed.session_end !== 'boolean') {
    return {
      ok: false,
      code: 'INVALID_SCHEMA',
      error: 'session_end field must be a boolean',
      rawBytes: rawBuf,
    };
  }

  // 4. Validate Channel Counts & 24-bit Signed Range
  const expectedChannels = parsed.sensor === 1 ? 3 : 2;

  for (let r = 0; r < parsed.data.length; r++) {
    const row = parsed.data[r];
    if (!Array.isArray(row) || row.length !== expectedChannels) {
      return {
        ok: false,
        code: 'CHANNEL_COUNT_MISMATCH',
        error: `Row ${r} on sensor ${parsed.sensor} has ${row?.length} channels, expected ${expectedChannels}`,
        rawBytes: rawBuf,
      };
    }

    for (let c = 0; c < expectedChannels; c++) {
      const val = row[c];
      if (!Number.isInteger(val) || val < MIN_SIGNED_24 || val > MAX_SIGNED_24) {
        return {
          ok: false,
          code: 'OUT_OF_RANGE_VALUES',
          error: `Sample ${parsed.first_index + r} channel ${c} value ${val} out of signed 24-bit range [${MIN_SIGNED_24}, ${MAX_SIGNED_24}]`,
          rawBytes: rawBuf,
        };
      }
    }
  }

  const response: MockResponse = {
    session_id: parsed.session_id,
    sensor: parsed.sensor as SensorId,
    first_index: parsed.first_index,
    count: parsed.count,
    lost: parsed.lost,
    session_end: Boolean(parsed.session_end),
    crc32: reportedCrcHex,
    data: parsed.data,
  };

  return {
    ok: true,
    response,
    rawBytes: rawBuf,
    dataSlice: extracted.slice,
    computedCrcHex,
  };
}
