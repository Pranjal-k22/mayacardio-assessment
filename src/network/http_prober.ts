import type { HealthProber, HealthProbeResult, MonotonicClock } from './network_monitor.ts';
import { SystemMonotonicClock } from './network_monitor.ts';

export interface HttpHealthProberOptions {
  healthUrl: string;
  clock?: MonotonicClock;
  expectedStatusField?: string;
  customHeaders?: Record<string, string>;
}

/**
 * Real HTTP/HTTPS health prober enforcing strict end-to-end request deadlines.
 * Cancels pending connections, TLS handshakes, and body reading upon timeout.
 */
export class HttpHealthProber implements HealthProber {
  private healthUrl: string;
  private clock: MonotonicClock;
  private expectedStatusField: string;
  private customHeaders: Record<string, string>;

  constructor(opts: HttpHealthProberOptions) {
    this.healthUrl = opts.healthUrl;
    this.clock = opts.clock ?? new SystemMonotonicClock();
    this.expectedStatusField = opts.expectedStatusField ?? 'status';
    this.customHeaders = opts.customHeaders ?? {};
  }

  public async probe(timeoutMs: number): Promise<HealthProbeResult> {
    const startTime = this.clock.now();
    const controller = new AbortController();
    let timedOut = false;

    const timeoutTimer = setTimeout(() => {
      timedOut = true;
      try {
        controller.abort();
      } catch {}
    }, timeoutMs);

    try {
      const response = await fetch(this.healthUrl, {
        method: 'GET',
        signal: controller.signal,
        headers: {
          'Accept': 'application/json, text/plain',
          'User-Agent': 'MayaCardio-Pipeline/1.0',
          ...this.customHeaders,
        },
      });

      const bodyText = await response.text();
      const latencyMs = Math.round((this.clock.now() - startTime) * 100) / 100;
      clearTimeout(timeoutTimer);

      if (!response.ok) {
        return {
          ok: false,
          latencyMs,
          statusCode: response.status,
          error: `HTTP error ${response.status}: ${response.statusText}`,
        };
      }

      // Validate response payload
      let isHealthy = false;
      try {
        const json = JSON.parse(bodyText);
        const val = json[this.expectedStatusField];
        isHealthy = val === 'ok' || val === 'healthy' || json.healthy === true || json.ok === true;
      } catch {
        const trimmed = bodyText.trim().toLowerCase();
        isHealthy = trimmed === 'ok' || trimmed === 'healthy';
      }

      if (!isHealthy) {
        return {
          ok: false,
          latencyMs,
          statusCode: response.status,
          error: `Unhealthy payload received: ${bodyText.slice(0, 100)}`,
        };
      }

      return {
        ok: true,
        latencyMs,
        statusCode: response.status,
      };
    } catch (err: any) {
      clearTimeout(timeoutTimer);
      const latencyMs = Math.round((this.clock.now() - startTime) * 100) / 100;
      const isTimeout =
        timedOut ||
        err.name === 'TimeoutError' ||
        err.name === 'AbortError' ||
        err.code === 20;

      return {
        ok: false,
        latencyMs,
        error: isTimeout ? `Probe timed out after ${timeoutMs}ms` : (err.message || 'Network probe failed'),
      };
    }
  }
}
