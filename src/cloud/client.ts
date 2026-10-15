import type { ChunkPayload, IngestResponse, InventoryResponse, CloudStatusResponse } from '../../cloud/worker/src/types.ts';
import type { WindowResult } from '../types.ts';

export interface CloudClientOptions {
  baseUrl: string;
  authToken?: string;
  timeoutMs?: number;
  fetchFn?: typeof fetch;
}

export class CloudApiClient {
  private baseUrl: string;
  private authToken: string;
  private timeoutMs: number;
  private fetchFn: typeof fetch;

  constructor(opts: CloudClientOptions) {
    this.baseUrl = opts.baseUrl.replace(/\/+$/, '');
    this.authToken = opts.authToken || process.env.CLOUD_AUTH_TOKEN || 'mayacardio-secret-token';
    this.timeoutMs = opts.timeoutMs ?? 5000;
    this.fetchFn = opts.fetchFn || fetch;
  }

  private async request(path: string, options: RequestInit = {}): Promise<Response> {
    const url = `${this.baseUrl}${path}`;
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), this.timeoutMs);

    const headers: Record<string, string> = {
      Accept: 'application/json',
      Authorization: `Bearer ${this.authToken}`,
      ...((options.headers as Record<string, string>) || {}),
    };

    try {
      const response = await this.fetchFn(url, {
        ...options,
        headers,
        signal: controller.signal,
      });
      return response;
    } finally {
      clearTimeout(timeout);
    }
  }

  public async health(): Promise<{ ok: boolean; status?: string; d1?: string; error?: string }> {
    try {
      const res = await this.request('/health', {
        method: 'GET',
        headers: { Accept: 'application/json' },
      });
      const data = await res.json();
      return data;
    } catch (err: any) {
      return { ok: false, error: err.message };
    }
  }

  public async ingestChunk(payload: ChunkPayload): Promise<IngestResponse> {
    const res = await this.request('/ingest', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload),
    });

    const body = await res.json();
    if (!res.ok) {
      const err = new Error(body.error || `Ingest failed with status ${res.status}`);
      (err as any).status = res.status;
      (err as any).response = body;
      throw err;
    }

    return body as IngestResponse;
  }

  public async getInventory(
    sessionId: string,
    fromChunk: number = 0,
    toChunk?: number,
    limit?: number
  ): Promise<InventoryResponse> {
    const params = new URLSearchParams({ session_id: sessionId, from_chunk: String(fromChunk) });
    if (toChunk !== undefined) params.set('to_chunk', String(toChunk));
    if (limit !== undefined) params.set('limit', String(limit));

    const res = await this.request(`/inventory?${params.toString()}`, { method: 'GET' });
    if (!res.ok) {
      throw new Error(`Inventory query failed with status ${res.status}`);
    }
    return (await res.json()) as InventoryResponse;
  }

  public async export(
    sessionId: string,
    fromIndex?: number,
    toIndex?: number
  ): Promise<string> {
    const params = new URLSearchParams({ session_id: sessionId });
    if (fromIndex !== undefined) params.set('from_index', String(fromIndex));
    if (toIndex !== undefined) params.set('to_index', String(toIndex));

    const res = await this.request(`/export?${params.toString()}`, {
      method: 'GET',
      headers: { Accept: 'application/x-ndjson' },
    });
    if (!res.ok) {
      throw new Error(`Export query failed with status ${res.status}`);
    }
    return await res.text();
  }

  public async getResults(
    sessionId: string,
    fromWindow?: number,
    toWindow?: number
  ): Promise<{ ok: boolean; results: WindowResult[] }> {
    const params = new URLSearchParams({ session_id: sessionId });
    if (fromWindow !== undefined) params.set('from_window', String(fromWindow));
    if (toWindow !== undefined) params.set('to_window', String(toWindow));

    const res = await this.request(`/results?${params.toString()}`, { method: 'GET' });
    if (!res.ok) {
      throw new Error(`Results query failed with status ${res.status}`);
    }
    return (await res.json()) as { ok: boolean; results: WindowResult[] };
  }

  public async getStatus(sessionId: string): Promise<CloudStatusResponse> {
    const params = new URLSearchParams({ session_id: sessionId });
    const res = await this.request(`/status?${params.toString()}`, { method: 'GET' });
    if (!res.ok) {
      throw new Error(`Status query failed with status ${res.status}`);
    }
    return (await res.json()) as CloudStatusResponse;
  }
}
