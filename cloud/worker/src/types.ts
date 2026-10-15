import type { LossRange, WindowResult } from '../../../src/types.ts';

export interface ChunkPayload {
  session_id: string;
  chunk_idx: number;
  from_index: number;
  to_index: number;
  mode: 'online' | 'offline';
  s1_encoded: string;
  s2_encoded: string;
  s1_mask: string;
  s2_mask: string;
  lost: LossRange[];
  digest: string;
  offline_results?: WindowResult[];
    context?: {
    s1_history?: Array<{ index: number; values: number[] }>;
    lookahead?: Array<{ index: number; values: number[] }>;
  };
}

export interface IngestResponse {
  ok: boolean;
  session_id: string;
  chunk_idx: number;
  duplicate?: boolean;
  results?: WindowResult[];
  error?: string;
}

export interface InventoryResponse {
  ok: boolean;
  session_id: string;
  chunks: number[];
  total_chunks: number;
  min_chunk?: number;
  max_chunk?: number;
}

export interface CloudStatusResponse {
  ok: boolean;
  session_id: string;
  chunks_count: number;
  samples_count: number;
  last_chunk_idx: number;
  last_window_idx: number;
}
