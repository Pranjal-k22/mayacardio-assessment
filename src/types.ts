export type SensorId = 1 | 2;

export interface RawSample {
  session_id: string;
  sensor: SensorId;
  index: number;
  values: number[];
}

export interface LossRange {
  from_index: number;
  to_index: number | null;
  reason: string;
  source?: string;
}

export interface StoredLossRecord extends LossRange {
  session_id: string;
  sensor: SensorId;
}

export interface MockResponse {
  session_id: string;
  sensor: SensorId;
  first_index: number;
  count: number;
  lost: LossRange[];
  session_end: boolean;
  crc32: string;
  data: number[][];
}

export interface ValidationSuccess {
  ok: true;
  response: MockResponse;
  rawBytes: Buffer;
  dataSlice: Buffer;
  computedCrcHex: string;
}

export interface ValidationFailure {
  ok: false;
  error: string;
  code:
    | 'TRUNCATED_OR_MALFORMED'
    | 'CRC_MISMATCH'
    | 'INVALID_SCHEMA'
    | 'OUT_OF_RANGE_VALUES'
    | 'CHANNEL_COUNT_MISMATCH';
  rawBytes?: Buffer;
}

export type RawValidationResult = ValidationSuccess | ValidationFailure;

export interface WindowResult {
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

export type ChannelKey = 's1_ch1' | 's1_ch2' | 's1_ch3' | 's2_ch1' | 's2_ch2';

export interface ProcessingCheckpoint {
  session_id: string;
  last_processed_window: number;
  last_accepted_peak: number;
  updated_at: number;
}

export interface PipelineStatus {
  mode: 'online' | 'offline';
  samples_saved: { '1': number; '2': number };
  samples_in_cloud: number;
  backlog: number;
}
