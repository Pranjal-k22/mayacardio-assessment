export type MonitorState = 'ONLINE' | 'OFFLINE';

export interface NetworkTransitionEvent {
  from: MonitorState;
  to: MonitorState;
  timestampMonotonic: number;
  reason: string;
  consecutiveFailures: number;
  consecutiveSuccesses: number;
}

export interface HealthProbeResult {
  ok: boolean;
  latencyMs: number;
  statusCode?: number;
  error?: string;
}

export interface HealthProber {
  probe(timeoutMs: number): Promise<HealthProbeResult>;
}

export interface MonotonicClock {
  now(): number;
}

export class SystemMonotonicClock implements MonotonicClock {
  public now(): number {
    return performance.now();
  }
}

export interface NetworkMonitorOptions {
  prober: HealthProber;
  clock?: MonotonicClock;
  probeIntervalMs?: number; // default: 2000 ms
  probeTimeoutMs?: number; // default: 1500 ms
  failuresToOffline?: number; // default: 2
  successesToOnline?: number; // default: 2
  initialState?: MonitorState; // default: 'OFFLINE' (conservative startup)
  suppressProbeThresholdMs?: number; // default: 1500 ms
}

export interface MonitorMetrics {
  state: MonitorState;
  mode: 'online' | 'offline';
  consecutiveFailures: number;
  consecutiveSuccesses: number;
  totalProbes: number;
  totalTransitions: number;
  lastTransitionMonotonic: number;
  lastObservationMonotonic: number;
  lastSuccessfulObservationMonotonic: number;
  isProbing: boolean;
  isRunning: boolean;
}

/**
 * State machine managing network reachability transitions between ONLINE and OFFLINE.
 * Uses monotonic clock exclusively to prevent system wall-clock adjustments from corrupting deadlines.
 */
export class NetworkMonitor {
  private prober: HealthProber;
  private clock: MonotonicClock;
  private probeIntervalMs: number;
  private probeTimeoutMs: number;
  private failuresToOffline: number;
  private successesToOnline: number;
  private suppressProbeThresholdMs: number;

  private state: MonitorState;
  private consecutiveFailures: number = 0;
  private consecutiveSuccesses: number = 0;
  private totalProbes: number = 0;
  private totalTransitions: number = 0;

  private lastTransitionMonotonic: number = 0;
  private lastObservationMonotonic: number = 0;
  private lastSuccessfulObservationMonotonic: number = 0;

  private isProbing: boolean = false;
  private isRunning: boolean = false;
  private timerId: any = null;

  private transitionListeners: Array<(ev: NetworkTransitionEvent) => void> = [];

  constructor(opts: NetworkMonitorOptions) {
    this.prober = opts.prober;
    this.clock = opts.clock ?? new SystemMonotonicClock();
    this.probeIntervalMs = opts.probeIntervalMs ?? 2000;
    this.probeTimeoutMs = opts.probeTimeoutMs ?? 1500;
    this.failuresToOffline = opts.failuresToOffline ?? 2;
    this.successesToOnline = opts.successesToOnline ?? 2;
    this.suppressProbeThresholdMs = opts.suppressProbeThresholdMs ?? 2000;

    // Conservative initial state per specifications
    this.state = opts.initialState ?? 'OFFLINE';
    this.lastTransitionMonotonic = this.clock.now();
  }

  public getState(): MonitorState {
    return this.state;
  }

  public getMode(): 'online' | 'offline' {
    return this.state === 'ONLINE' ? 'online' : 'offline';
  }

  public getMetrics(): MonitorMetrics {
    return {
      state: this.state,
      mode: this.getMode(),
      consecutiveFailures: this.consecutiveFailures,
      consecutiveSuccesses: this.consecutiveSuccesses,
      totalProbes: this.totalProbes,
      totalTransitions: this.totalTransitions,
      lastTransitionMonotonic: this.lastTransitionMonotonic,
      lastObservationMonotonic: this.lastObservationMonotonic,
      lastSuccessfulObservationMonotonic: this.lastSuccessfulObservationMonotonic,
      isProbing: this.isProbing,
      isRunning: this.isRunning,
    };
  }

  public onTransition(listener: (ev: NetworkTransitionEvent) => void): () => void {
    this.transitionListeners.push(listener);
    return () => {
      this.transitionListeners = this.transitionListeners.filter((l) => l !== listener);
    };
  }

  /**
   * Start scheduled probing loop.
   */
  public start(): void {
    if (this.isRunning) return;
    this.isRunning = true;
    this.scheduleNextProbe(0);
  }

  /**
   * Stop scheduled probing loop and cancel pending timers.
   */
  public stop(): void {
    this.isRunning = false;
    if (this.timerId !== null) {
      clearTimeout(this.timerId);
      this.timerId = null;
    }
  }

  /**
   * Record a verified successful cloud ingestion acknowledging data reception.
   * Resets failure counter and counts as a positive health observation.
   */
  public recordSuccessfulIngestion(timestampMonotonic?: number): void {
    const now = timestampMonotonic ?? this.clock.now();
    this.lastObservationMonotonic = now;
    this.lastSuccessfulObservationMonotonic = now;

    this.consecutiveFailures = 0;
    this.consecutiveSuccesses++;

    if (this.state === 'OFFLINE' && this.consecutiveSuccesses >= this.successesToOnline) {
      this.transitionTo('ONLINE', `Cloud reachability verified via ${this.consecutiveSuccesses} consecutive successful ingestions`);
    }
  }

  /**
   * Record a confirmed ingestion error / network drop during transmission.
   */
  public recordFailedIngestion(error?: string): void {
    const now = this.clock.now();
    this.lastObservationMonotonic = now;

    this.consecutiveSuccesses = 0;
    this.consecutiveFailures++;

    if (this.state === 'ONLINE' && this.consecutiveFailures >= this.failuresToOffline) {
      this.transitionTo('OFFLINE', `Cloud connection lost via ingestion error: ${error || 'Unknown network error'}`);
    }
  }

  /**
   * Execute a single health probe synchronously or on-demand.
   * Serializes concurrent calls to prevent overlapping probe requests.
   */
  public async probeOnce(): Promise<HealthProbeResult> {
    if (this.isProbing) {
      return { ok: false, latencyMs: 0, error: 'Probe already in progress' };
    }

    this.isProbing = true;
    this.totalProbes++;
    const probeStartTime = this.clock.now();

    try {
      const res = await this.prober.probe(this.probeTimeoutMs);
      this.handleObservationResult(res.ok, res.error ? `Probe error: ${res.error}` : 'Health probe ok');
      return res;
    } catch (err: any) {
      const errorMsg = err.message || 'Probe thrown exception';
      this.handleObservationResult(false, `Probe exception: ${errorMsg}`);
      return { ok: false, latencyMs: this.clock.now() - probeStartTime, error: errorMsg };
    } finally {
      this.isProbing = false;
    }
  }

  /**
   * Schedule the next probe tick using monotonic clock timing.
   */
  private scheduleNextProbe(delayMs: number): void {
    if (!this.isRunning) return;
    if (this.timerId !== null) {
      clearTimeout(this.timerId);
      this.timerId = null;
    }

    this.timerId = setTimeout(async () => {
      this.timerId = null;
      if (!this.isRunning) return;

      const cycleStartTime = this.clock.now();

      // Check if probe can be suppressed due to very recent successful ingestion
      const timeSinceSuccess = cycleStartTime - this.lastSuccessfulObservationMonotonic;
      if (this.state === 'ONLINE' && timeSinceSuccess < this.suppressProbeThresholdMs) {
        // Redundant check skipped; reschedule for remainder of interval
        const nextDelay = Math.max(50, this.probeIntervalMs - timeSinceSuccess);
        this.scheduleNextProbe(nextDelay);
        return;
      }

      await this.probeOnce();

      if (!this.isRunning) return;
      const elapsed = this.clock.now() - cycleStartTime;
      const nextDelay = Math.max(0, this.probeIntervalMs - elapsed);
      this.scheduleNextProbe(nextDelay);
    }, delayMs);
  }

  /**
   * Process observation outcome and evaluate state machine transitions.
   */
  private handleObservationResult(ok: boolean, details: string): void {
    const now = this.clock.now();
    this.lastObservationMonotonic = now;

    if (ok) {
      this.lastSuccessfulObservationMonotonic = now;
      this.consecutiveFailures = 0;
      this.consecutiveSuccesses++;

      if (this.state === 'OFFLINE' && this.consecutiveSuccesses >= this.successesToOnline) {
        this.transitionTo('ONLINE', `${this.consecutiveSuccesses} consecutive successful health observations`);
      }
    } else {
      this.consecutiveSuccesses = 0;
      this.consecutiveFailures++;

      if (this.state === 'ONLINE' && this.consecutiveFailures >= this.failuresToOffline) {
        this.transitionTo('OFFLINE', `${this.consecutiveFailures} consecutive failed health observations (${details})`);
      }
    }
  }

  /**
   * Transition state and notify listeners safely.
   */
  private transitionTo(newState: MonitorState, reason: string): void {
    if (this.state === newState) return; // Idempotent guard

    const oldState = this.state;
    this.state = newState;
    this.lastTransitionMonotonic = this.clock.now();
    this.totalTransitions++;

    const event: NetworkTransitionEvent = {
      from: oldState,
      to: newState,
      timestampMonotonic: this.lastTransitionMonotonic,
      reason,
      consecutiveFailures: this.consecutiveFailures,
      consecutiveSuccesses: this.consecutiveSuccesses,
    };

    // Safe listener delivery
    for (const listener of this.transitionListeners) {
      try {
        listener(event);
      } catch (err) {
        // Listener error must never crash the monitor or abort acquisition
        console.error('Error in network transition listener:', err);
      }
    }
  }
}
