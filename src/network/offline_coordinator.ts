import type { NetworkMonitor, NetworkTransitionEvent } from './network_monitor.ts';
import type { WindowProcessingEngine } from '../processing/engine.ts';
import type { StorageEngine } from '../storage/db.ts';
import type { WindowResult } from '../types.ts';

export interface OfflineCoordinatorOptions {
  networkMonitor: NetworkMonitor;
  storage: StorageEngine;
  processingEngine: WindowProcessingEngine;
  pollIntervalMs?: number; // default: 1000 ms to poll for newly completed windows while offline
}

export interface ReconnectionEvent {
  transitionEvent: NetworkTransitionEvent;
  offlineResultsPreserved: number;
  timestampMonotonic: number;
}

/**
 * Coordinates offline window processing during network disconnects.
 * Completely decoupled from TCP acquisition to ensure continuous, zero-loss collection.
 */
export class OfflineProcessingCoordinator {
  private networkMonitor: NetworkMonitor;
  private storage: StorageEngine;
  private processingEngine: WindowProcessingEngine;
  private pollIntervalMs: number;

  private isRunning: boolean = false;
  private pollTimer: any = null;

  private unsubscribeMonitor: (() => void) | null = null;
  private reconnectionListeners: Array<(ev: ReconnectionEvent) => void> = [];

  constructor(opts: OfflineCoordinatorOptions) {
    this.networkMonitor = opts.networkMonitor;
    this.storage = opts.storage;
    this.processingEngine = opts.processingEngine;
    this.pollIntervalMs = opts.pollIntervalMs ?? 1000;
  }

  public start(): void {
    if (this.isRunning) return;
    this.isRunning = true;

    // Listen to network state transitions
    this.unsubscribeMonitor = this.networkMonitor.onTransition((ev) => {
      this.handleTransition(ev);
    });

    // If initial state is OFFLINE, trigger immediate processing check
    if (this.networkMonitor.getState() === 'OFFLINE') {
      this.startOfflinePolling();
    }
  }

  public stop(): void {
    this.isRunning = false;
    this.stopOfflinePolling();
    if (this.unsubscribeMonitor) {
      this.unsubscribeMonitor();
      this.unsubscribeMonitor = null;
    }
  }

  public onReconnection(listener: (ev: ReconnectionEvent) => void): () => void {
    this.reconnectionListeners.push(listener);
    return () => {
      this.reconnectionListeners = this.reconnectionListeners.filter((l) => l !== listener);
    };
  }

  private currentProcessingPromise: Promise<WindowResult[]> | null = null;

  /**
   * Process all currently eligible, resolved windows across active sessions.
   * If a pass is already in flight, joins and awaits the active promise.
   * Yields to Node.js event loop between window commits to protect collector throughput.
   */
  public async processEligibleWindows(): Promise<WindowResult[]> {
    if (this.currentProcessingPromise) {
      return this.currentProcessingPromise;
    }

    this.currentProcessingPromise = this.doProcessEligibleWindows();
    try {
      return await this.currentProcessingPromise;
    } finally {
      this.currentProcessingPromise = null;
    }
  }

  private async doProcessEligibleWindows(): Promise<WindowResult[]> {
    const processedResults: WindowResult[] = [];
    try {
      const sessionIds = this.storage.getSessionIds();

      for (const sessionId of sessionIds) {
        while (this.isRunning) {
          const res = this.processingEngine.processNextWindow(sessionId, 'offline');
          if (!res) {
            // No more resolved windows available for this session
            break;
          }
          processedResults.push(res);

          // Cooperative yield to event loop so incoming TCP packets / collector writes are serviced immediately
          await new Promise<void>((resolve) => setImmediate(resolve));
        }
      }
    } catch (err) {
      console.error('Error during offline window processing:', err);
    }

    return processedResults;
  }

  private handleTransition(ev: NetworkTransitionEvent): void {
    if (ev.to === 'OFFLINE') {
      // Switched to OFFLINE: initiate offline processing
      this.startOfflinePolling();
      // Trigger immediate pass
      this.processEligibleWindows().catch(() => {});
    } else if (ev.to === 'ONLINE') {
      // Switched to ONLINE: pause offline processing claiming
      this.stopOfflinePolling();

      // Notify Phase 5 synchronization listeners that network is restored
      const event: ReconnectionEvent = {
        transitionEvent: ev,
        offlineResultsPreserved: this.countOfflineResults(),
        timestampMonotonic: ev.timestampMonotonic,
      };

      for (const listener of this.reconnectionListeners) {
        try {
          listener(event);
        } catch (err) {
          console.error('Error in reconnection listener:', err);
        }
      }
    }
  }

  private startOfflinePolling(): void {
    if (this.pollTimer !== null) return;
    const poll = async () => {
      if (!this.isRunning || this.networkMonitor.getState() !== 'OFFLINE') {
        this.stopOfflinePolling();
        return;
      }
      await this.processEligibleWindows();
      if (this.isRunning && this.networkMonitor.getState() === 'OFFLINE') {
        this.pollTimer = setTimeout(poll, this.pollIntervalMs);
      } else {
        this.pollTimer = null;
      }
    };

    this.pollTimer = setTimeout(poll, this.pollIntervalMs);
  }

  private stopOfflinePolling(): void {
    if (this.pollTimer !== null) {
      clearTimeout(this.pollTimer);
      this.pollTimer = null;
    }
  }

  private countOfflineResults(): number {
    try {
      const sessionIds = this.storage.getSessionIds();
      let total = 0;
      for (const s of sessionIds) {
        const results = this.storage.getProcessingResults(s, 0, 100000);
        total += results.filter((r) => r.mode === 'offline').length;
      }
      return total;
    } catch {
      return 0;
    }
  }
}
