import type { LossRange, RawSample, SensorId } from '../types.ts';
import * as zlib from 'node:zlib';

export const CHUNK_DURATION_SECONDS = 2;
export const SAMPLES_PER_SECOND = 400;
export const SAMPLES_PER_CHUNK = CHUNK_DURATION_SECONDS * SAMPLES_PER_SECOND; // 800 samples per sensor
export const WINDOWS_PER_CHUNK = 2;

export function uint8ArrayToBase64(bytes: Uint8Array): string {
  if (typeof Buffer !== 'undefined') {
    return Buffer.from(bytes.buffer, bytes.byteOffset, bytes.byteLength).toString('base64');
  }
  let binary = '';
  for (let i = 0; i < bytes.length; i++) {
    binary += String.fromCharCode(bytes[i]);
  }
  return btoa(binary);
}

export function base64ToUint8Array(base64: string): Uint8Array {
  if (typeof Buffer !== 'undefined') {
    const buf = Buffer.from(base64, 'base64');
    return new Uint8Array(buf.buffer, buf.byteOffset, buf.byteLength);
  }
  const binary = atob(base64);
  const bytes = new Uint8Array(binary.length);
  for (let i = 0; i < binary.length; i++) {
    bytes[i] = binary.charCodeAt(i);
  }
  return bytes;
}

export function writeInt24(buf: Uint8Array, offset: number, val: number): void {
  let v = val;
  if (v < 0) {
    v = (1 << 24) + v;
  }
  buf[offset] = (v >> 16) & 0xff;
  buf[offset + 1] = (v >> 8) & 0xff;
  buf[offset + 2] = v & 0xff;
}

export function readInt24(buf: Uint8Array, offset: number): number {
  let val = (buf[offset] << 16) | (buf[offset + 1] << 8) | buf[offset + 2];
  if (val & 0x800000) {
    val -= 1 << 24; // Sign extension
  }
  return val;
}

export interface EncodedSensorData {
  encoded: string; // Base64
  maskHex: string; // Hex bitmask (100 bytes = 800 bits)
  count: number;   // Number of available valid samples
}

export class ChunkCodec {
  /**
   * Encode 800 expected samples of a sensor into a compact binary base64 string and availability bitmask.
   */
  public static encodeSensor(
    sensor: SensorId,
    fromIndex: number,
    samplesMap: Map<number, number[]>
  ): EncodedSensorData {
    const channelCount = sensor === 1 ? 3 : 2;
    const bytesPerSample = channelCount * 3;
    const totalDataBytes = SAMPLES_PER_CHUNK * bytesPerSample;

    const dataBuf = new Uint8Array(totalDataBytes);
    const maskBytes = new Uint8Array(Math.ceil(SAMPLES_PER_CHUNK / 8)); // 100 bytes

    let availableCount = 0;

    for (let i = 0; i < SAMPLES_PER_CHUNK; i++) {
      const idx = fromIndex + i;
      const sample = samplesMap.get(idx);

      if (sample && sample.length >= channelCount) {
        availableCount++;
        // Set bit in bitmask
        const byteIdx = Math.floor(i / 8);
        const bitIdx = 7 - (i % 8);
        maskBytes[byteIdx] |= (1 << bitIdx);

        // Write signed 24-bit integer values
        const offset = i * bytesPerSample;
        for (let c = 0; c < channelCount; c++) {
          writeInt24(dataBuf, offset + c * 3, sample[c]);
        }
      }
    }

    // Convert bitmask to hex string
    let maskHex = '';
    for (let i = 0; i < maskBytes.length; i++) {
      maskHex += maskBytes[i].toString(16).padStart(2, '0');
    }

    // Compress data buffer with gzip before Base64 encoding to reduce daily volume
    const compressedBuf = zlib.gzipSync(dataBuf);

    return {
      encoded: uint8ArrayToBase64(compressedBuf),
      maskHex,
      count: availableCount,
    };
  }

  /**
   * Decode a sensor's encoded payload back into a Map of sample_idx -> values.
   */
  public static decodeSensor(
    sensor: SensorId,
    fromIndex: number,
    encodedBase64: string,
    maskHex: string
  ): Map<number, number[]> {
    const channelCount = sensor === 1 ? 3 : 2;
    const bytesPerSample = channelCount * 3;
    const rawBytes = base64ToUint8Array(encodedBase64);

    // Transparently decompress gzipped payload (magic bytes 0x1f, 0x8b) or accept uncompressed fallback
    let dataBuf: Uint8Array;
    if (rawBytes.length >= 2 && rawBytes[0] === 0x1f && rawBytes[1] === 0x8b) {
      const decompressed = zlib.gunzipSync(rawBytes);
      dataBuf = new Uint8Array(decompressed.buffer, decompressed.byteOffset, decompressed.byteLength);
    } else {
      dataBuf = rawBytes;
    }

    const maskBytes = new Uint8Array(maskHex.length / 2);
    for (let i = 0; i < maskHex.length; i += 2) {
      maskBytes[i / 2] = parseInt(maskHex.substring(i, i + 2), 16);
    }

    const map = new Map<number, number[]>();

    for (let i = 0; i < SAMPLES_PER_CHUNK; i++) {
      const byteIdx = Math.floor(i / 8);
      const bitIdx = 7 - (i % 8);
      const isAvailable = (maskBytes[byteIdx] & (1 << bitIdx)) !== 0;

      if (isAvailable) {
        const offset = i * bytesPerSample;
        const vals: number[] = [];
        for (let c = 0; c < channelCount; c++) {
          vals.push(readInt24(dataBuf, offset + c * 3));
        }
        map.set(fromIndex + i, vals);
      }
    }

    return map;
  }

  /**
   * Calculate deterministic SHA-256 digest of chunk contents.
   */
  public static async computeDigest(data: {
    sessionId: string;
    chunkIdx: number;
    fromIndex: number;
    toIndex: number;
    s1Encoded: string;
    s2Encoded: string;
    s1Mask: string;
    s2Mask: string;
    lost: LossRange[];
  }): Promise<string> {
    const canonicalText = JSON.stringify({
      session_id: data.sessionId,
      chunk_idx: data.chunkIdx,
      from_index: data.fromIndex,
      to_index: data.toIndex,
      s1_encoded: data.s1Encoded,
      s2_encoded: data.s2Encoded,
      s1_mask: data.s1Mask,
      s2_mask: data.s2Mask,
      lost: data.lost,
    });

    const msgBuffer = new TextEncoder().encode(canonicalText);
    const hashBuffer = await crypto.subtle.digest('SHA-256', msgBuffer);
    const hashArray = Array.from(new Uint8Array(hashBuffer));
    return hashArray.map((b) => b.toString(16).padStart(2, '0')).join('');
  }
}
