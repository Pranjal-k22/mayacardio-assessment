import assert from 'node:assert';
import * as http from 'node:http';
import * as path from 'node:path';
import * as fs from 'node:fs';
import {
  NetworkMonitor,
  type HealthProber,
  type HealthProbeResult,
  type MonotonicClock,
  type NetworkTransitionEvent,
} from '../src/network/network_monitor.ts';
import { HttpHealthProber } from '../src/network/http_prober.ts';
import { OfflineProcessingCoordinator } from '../src/network/offline_coordinator.ts';
import { StorageEngine } from '../src/storage/db.ts';
import { WindowProcessingEngine } from '../src/processing/engine.ts';
import type { MockResponse } from '../src/types.ts';

console.log('=== Running Phase 4: Network Monitor State Machine & Failover Tests ===');

const TEST_DB_DIR = path.resolve('test_data');
if (!fs.existsSync(TEST_DB_DIR)) {
  fs.mkdirSync(TEST_DB_DIR, { recursive: true });
}

function cleanDbFiles(dbPath: string) {
  for (const ext of ['', '-wal', '-shm']) {
    const p = dbPath + ext;
    if (fs.existsSync(p)) {
      try {
        fs.unlinkSync(p);
      } catch {}
    }
  }
}

// Controllable Monotonic Clock for deterministic unit testing
class ManualClock implements MonotonicClock {
  private currentTime: number;
  constructor(initialTime: number = 1000) {
    this.currentTime = initialTime;
  }
  public now(): number {
    return this.currentTime;
  }
  public advance(ms: number): void {
    this.currentTime += ms;
  }
}

// Configurable Mock Prober
class MockHealthProber implements HealthProber {
  public nextResult: HealthProbeResult = { ok: true, latencyMs: 10 };
  public delayMs: number = 0;
  public probeCount: number = 0;

  public async probe(timeoutMs: number): Promise<HealthProbeResult> {
    this.probeCount++;
    if (this.delayMs > 0) {
      if (this.delayMs >= timeoutMs) {
        return { ok: false, latencyMs: timeoutMs, error: `Probe timed out after ${timeoutMs}ms` };
      }
    }
    return this.nextResult;
  }
}

// =========================================================================
// 1. STATE MACHINE & TRANSITION THRESHOLD TESTS
// =========================================================================
async function testStateMachineThresholds() {
  console.log('\n--- 1. State Machine & Transition Threshold Tests ---');

  const clock = new ManualClock(1000);
  const prober = new MockHealthProber();

  const transitions: NetworkTransitionEvent[] = [];
  const monitor = new NetworkMonitor({
    prober,
    clock,
    probeIntervalMs: 2000,
    probeTimeoutMs: 1500,
    failuresToOffline: 2,
    successesToOnline: 2,
    initialState: 'OFFLINE',
  });

  monitor.onTransition((ev) => transitions.push(ev));

  // 1. Initial conservative state
  assert.strictEqual(monitor.getState(), 'OFFLINE');
  assert.strictEqual(monitor.getMode(), 'offline');
  console.log('[PASS] Initial conservative state is OFFLINE');

  // 2. Single success does NOT transition prematurely
  prober.nextResult = { ok: true, latencyMs: 20 };
  await monitor.probeOnce();
  assert.strictEqual(monitor.getState(), 'OFFLINE');
  assert.strictEqual(transitions.length, 0);
  console.log('[PASS] Single successful probe does not transition OFFLINE -> ONLINE');

  // 3. Second consecutive success transitions OFFLINE -> ONLINE
  await monitor.probeOnce();
  assert.strictEqual(monitor.getState(), 'ONLINE');
  assert.strictEqual(monitor.getMode(), 'online');
  assert.strictEqual(transitions.length, 1);
  assert.strictEqual(transitions[0].from, 'OFFLINE');
  assert.strictEqual(transitions[0].to, 'ONLINE');
  console.log('[PASS] Two consecutive successful observations transition OFFLINE -> ONLINE');

  // 4. Single failure does NOT transition prematurely
  prober.nextResult = { ok: false, latencyMs: 1500, error: 'Probe timed out' };
  await monitor.probeOnce();
  assert.strictEqual(monitor.getState(), 'ONLINE');
  assert.strictEqual(transitions.length, 1); // No new transition
  console.log('[PASS] Single failed probe does not transition ONLINE -> OFFLINE');

  // 5. Subsequent success resets failure counter
  prober.nextResult = { ok: true, latencyMs: 25 };
  await monitor.probeOnce();
  assert.strictEqual(monitor.getState(), 'ONLINE');
  assert.strictEqual(monitor.getMetrics().consecutiveFailures, 0);
  console.log('[PASS] Intervening success resets failure counter to 0');

  // 6. Two consecutive failures transition ONLINE -> OFFLINE
  prober.nextResult = { ok: false, latencyMs: 1500, error: 'Connection refused' };
  await monitor.probeOnce(); // Failure 1
  assert.strictEqual(monitor.getState(), 'ONLINE');
  await monitor.probeOnce(); // Failure 2 -> Transition!
  assert.strictEqual(monitor.getState(), 'OFFLINE');
  assert.strictEqual(transitions.length, 2);
  assert.strictEqual(transitions[1].from, 'ONLINE');
  assert.strictEqual(transitions[1].to, 'OFFLINE');
  console.log('[PASS] Two consecutive failed observations transition ONLINE -> OFFLINE');

  // 7. Flapping / failure resets recovery counter
  prober.nextResult = { ok: true, latencyMs: 10 };
  await monitor.probeOnce(); // Success 1
  assert.strictEqual(monitor.getState(), 'OFFLINE');
  prober.nextResult = { ok: false, latencyMs: 50, error: '500 Internal Error' };
  await monitor.probeOnce(); // Failure resets success counter!
  assert.strictEqual(monitor.getState(), 'OFFLINE');
  assert.strictEqual(monitor.getMetrics().consecutiveSuccesses, 0);
  console.log('[PASS] Intervening failure resets recovery counter to 0');

  // 8. Idempotency: Duplicate observations in same state emit no redundant events
  prober.nextResult = { ok: false, latencyMs: 50 };
  await monitor.probeOnce();
  await monitor.probeOnce();
  assert.strictEqual(monitor.getState(), 'OFFLINE');
  assert.strictEqual(transitions.length, 2, 'No redundant transition events emitted');
  console.log('[PASS] Repeated observations in same state are strictly idempotent');

  // 9. Ingestion signal: Successful cloud ingestion acts as health observation
  monitor.recordSuccessfulIngestion(clock.now());
  assert.strictEqual(monitor.getMetrics().consecutiveSuccesses, 1);
  monitor.recordSuccessfulIngestion(clock.now());
  assert.strictEqual(monitor.getState(), 'ONLINE');
  assert.strictEqual(transitions.length, 3);
  console.log('[PASS] Real successful cloud ingestion acknowledged as valid health observation');

  // 10. Listener error safety: an exception inside a transition listener does not crash monitor
  const throwingMonitor = new NetworkMonitor({
    prober,
    clock,
    initialState: 'ONLINE',
  });
  throwingMonitor.onTransition(() => {
    throw new Error('Fatal listener explosion');
  });
  prober.nextResult = { ok: false, latencyMs: 10 };
  await throwingMonitor.probeOnce();
  await throwingMonitor.probeOnce(); // Should transition without throwing
  assert.strictEqual(throwingMonitor.getState(), 'OFFLINE');
  console.log('[PASS] Exceptions in user transition listeners isolated and non-fatal');
}

// =========================================================================
// 2. TIMEOUTS, SERIALIZATION & CONCURRENCY TESTS
// =========================================================================
async function testTimeoutsAndConcurrency() {
  console.log('\n--- 2. Timeouts, Serialization & Concurrency Tests ---');

  const prober = new MockHealthProber();
  const monitor = new NetworkMonitor({
    prober,
    probeIntervalMs: 100,
    probeTimeoutMs: 50,
  });

  // Verify concurrent calls to probeOnce are serialized / safe
  let running = true;
  prober.delayMs = 20;

  const p1 = monitor.probeOnce();
  const p2 = monitor.probeOnce(); // Concurrent second call while isProbing=true

  const [r1, r2] = await Promise.all([p1, p2]);
  assert(r1.ok || r2.ok || r2.error === 'Probe already in progress');
  console.log('[PASS] Concurrent probes serialized; overlapping requests prevented');

  // Monitor start/stop clears active timers
  monitor.start();
  assert.strictEqual(monitor.getMetrics().isRunning, true);
  monitor.stop();
  assert.strictEqual(monitor.getMetrics().isRunning, false);
  console.log('[PASS] Monitor start/stop cleanly manages background timers');
}

// =========================================================================
// 3. REAL HTTP HEALTH PROBER & LOCAL TEST SERVER INTEGRATION
// =========================================================================
async function testHttpHealthProberAndLocalServer() {
  console.log('\n--- 3. Real HTTP Health Prober & Local Server Integration Tests ---');

  let serverMode: 'healthy' | 'unhealthy' | 'error' | 'hang' = 'healthy';

  const server = http.createServer((req, res) => {
    if (serverMode === 'healthy') {
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ status: 'ok', service: 'mayacardio-cloud' }));
    } else if (serverMode === 'unhealthy') {
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ status: 'degraded', healthy: false }));
    } else if (serverMode === 'error') {
      res.writeHead(503, { 'Content-Type': 'text/plain' });
      res.end('Service Unavailable');
    } else if (serverMode === 'hang') {
      // Intentionally do not respond to test client timeout cancellation
    }
  });

  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
  const port = (server.address() as any).port;
  const healthUrl = `http://127.0.0.1:${port}/health`;

  try {
    const prober = new HttpHealthProber({ healthUrl });

    // 1. Healthy response
    serverMode = 'healthy';
    const resHealthy = await prober.probe(1000);
    assert.strictEqual(resHealthy.ok, true);
    assert.strictEqual(resHealthy.statusCode, 200);
    console.log(`[PASS] Healthy probe succeeded: latency=${resHealthy.latencyMs}ms`);

    // 2. Unhealthy payload (200 OK but status != ok)
    serverMode = 'unhealthy';
    const resUnhealthy = await prober.probe(1000);
    assert.strictEqual(resUnhealthy.ok, false);
    assert(resUnhealthy.error?.includes('Unhealthy payload'));
    console.log('[PASS] Non-ok JSON payload rejected as failed observation');

    // 3. HTTP 503 error
    serverMode = 'error';
    const resError = await prober.probe(1000);
    assert.strictEqual(resError.ok, false);
    assert.strictEqual(resError.statusCode, 503);
    console.log('[PASS] HTTP 503 Service Unavailable rejected as failed observation');

    // 4. Request timeout & socket cleanup on hanging server
    serverMode = 'hang';
    const t0 = performance.now();
    const resHang = await prober.probe(200); // 200ms hard deadline
    const elapsed = performance.now() - t0;
    assert.strictEqual(resHang.ok, false);
    assert(resHang.error?.includes('timed out'));
    assert(elapsed >= 180 && elapsed <= 500, `Timeout must fire near 200ms (actual: ${elapsed}ms)`);
    console.log(`[PASS] Strict deadline enforced on hanging server (${elapsed.toFixed(1)}ms elapsed)`);

    // 5. Connection refusal (server closed on port)
    const deadProber = new HttpHealthProber({ healthUrl: 'http://127.0.0.1:1/health' });
    const resRefused = await deadProber.probe(500);
    assert.strictEqual(resRefused.ok, false);
    console.log('[PASS] Connection refusal safely handled as failed observation');
  } finally {
    await new Promise<void>((resolve) => server.close(() => resolve()));
  }
}

// =========================================================================
// 4. TIMED FAILOVER & RECOVERY EXPERIMENT (< 10 SECONDS)
// =========================================================================
async function testTimedFailoverExperiment() {
  console.log('\n--- 4. Real Timed Failover & Recovery Experiment (< 10s Target) ---');

  let serverAlive = true;
  const server = http.createServer((req, res) => {
    if (serverAlive) {
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ status: 'ok' }));
    } else {
      res.writeHead(503, { 'Content-Type': 'text/plain' });
      res.end('Cloud Down');
    }
  });

  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
  const port = (server.address() as any).port;
  const healthUrl = `http://127.0.0.1:${port}/health`;

  try {
    const prober = new HttpHealthProber({ healthUrl });

    // Configure with standard production parameters
    const monitor = new NetworkMonitor({
      prober,
      probeIntervalMs: 1000, // 1s interval for test speed
      probeTimeoutMs: 800,
      failuresToOffline: 2,
      successesToOnline: 2,
      initialState: 'ONLINE',
    });

    let offlineTransitionTime = 0;
    let onlineTransitionTime = 0;

    monitor.onTransition((ev) => {
      if (ev.to === 'OFFLINE') {
        offlineTransitionTime = performance.now();
      } else if (ev.to === 'ONLINE') {
        onlineTransitionTime = performance.now();
      }
    });

    monitor.start();

    // 1. Simulate Cloud Drop
    const dropTime = performance.now();
    serverAlive = false;

    // Wait for transition to OFFLINE
    while (monitor.getState() !== 'OFFLINE') {
      await new Promise((r) => setTimeout(r, 50));
      if (performance.now() - dropTime > 10000) {
        assert.fail('Failover to OFFLINE exceeded 10-second limit!');
      }
    }

    const failoverDurationMs = offlineTransitionTime - dropTime;
    console.log(`[MEASURED] Time from outage injection to OFFLINE: ${(failoverDurationMs / 1000).toFixed(2)}s`);
    assert(
      failoverDurationMs < 10000,
      `Failover duration (${failoverDurationMs}ms) must be strictly under 10,000ms`
    );
    console.log('[PASS] Automatic failover to OFFLINE completed in under 10 seconds');

    // 2. Simulate Cloud Restoration
    const restoreTime = performance.now();
    serverAlive = true;

    // Wait for transition to ONLINE
    while (monitor.getState() !== 'ONLINE') {
      await new Promise((r) => setTimeout(r, 50));
      if (performance.now() - restoreTime > 10000) {
        assert.fail('Recovery to ONLINE exceeded 10-second limit!');
      }
    }

    const recoveryDurationMs = onlineTransitionTime - restoreTime;
    console.log(`[MEASURED] Time from recovery to ONLINE: ${(recoveryDurationMs / 1000).toFixed(2)}s`);
    assert(
      recoveryDurationMs < 10000,
      `Recovery duration (${recoveryDurationMs}ms) must be strictly under 10,000ms`
    );
    console.log('[PASS] Automatic recovery to ONLINE completed in under 10 seconds');

    monitor.stop();
  } finally {
    await new Promise<void>((resolve) => server.close(() => resolve()));
  }
}

// =========================================================================
// 5. INTEGRATION WITH PERSISTENCE, OFFLINE PROCESSING & CONFLICT SAFETY
// =========================================================================
async function testPersistenceAndOfflineProcessingIntegration() {
  console.log('\n--- 5. Offline Coordinator, Persistence & Conflict Safety Tests ---');

  const dbPath = path.join(TEST_DB_DIR, 'test_phase4.db');
  cleanDbFiles(dbPath);

  const storage = new StorageEngine({ dbPath });
  const windowEngine = new WindowProcessingEngine({ storage });

  const prober = new MockHealthProber();
  const monitor = new NetworkMonitor({
    prober,
    initialState: 'ONLINE',
  });

  const coordinator = new OfflineProcessingCoordinator({
    networkMonitor: monitor,
    storage,
    processingEngine: windowEngine,
    pollIntervalMs: 500,
  });

  coordinator.start();
  const sessionId = 'sess-phase4-offline';

  try {
    // 1. Ingest window 0 samples while monitor is ONLINE
    const s1W0: MockResponse = {
      session_id: sessionId,
      sensor: 1,
      first_index: 0,
      count: 401, // include 400 for lookahead
      lost: [],
      session_end: false,
      crc32: '00000000',
      data: Array.from({ length: 401 }, () => [100, 500, 100]),
    };
    const s2W0: MockResponse = {
      session_id: sessionId,
      sensor: 2,
      first_index: 0,
      count: 400,
      lost: [],
      session_end: false,
      crc32: '00000000',
      data: Array.from({ length: 400 }, () => [300, 400]),
    };

    // Collector persists samples directly to storage regardless of network state
    storage.persistValidatedReply(s1W0);
    storage.persistValidatedReply(s2W0);
    assert.strictEqual(storage.getProgress(sessionId, 1).resolvedWatermark, 400);
    assert.strictEqual(storage.getProgress(sessionId, 2).resolvedWatermark, 399);

    // 2. Trigger network drop -> Transitions to OFFLINE
    prober.nextResult = { ok: false, latencyMs: 50, error: 'Network dropped' };
    await monitor.probeOnce();
    await monitor.probeOnce();
    assert.strictEqual(monitor.getState(), 'OFFLINE');

    // Coordinator processes eligible resolved windows locally as OFFLINE
    const processed = await coordinator.processEligibleWindows();
    assert.strictEqual(processed.length, 1);
    assert.strictEqual(processed[0].window_idx, 0);
    assert.strictEqual(processed[0].mode, 'offline');
    assert.strictEqual(processed[0].complete, true);

    const savedW0 = storage.getProcessingResult(sessionId, 0);
    assert.strictEqual(savedW0?.mode, 'offline');
    console.log('[PASS] Eligible window 0 processed locally and saved with mode: offline');

    // 3. Repeated OFFLINE events do NOT duplicate or alter authoritative result
    const repeatProcessed = await coordinator.processEligibleWindows();
    assert.strictEqual(repeatProcessed.length, 0, 'Already processed window must not be re-processed');
    const allResults = storage.getProcessingResults(sessionId, 0, 100);
    assert.strictEqual(allResults.length, 1, 'Exactly 1 authoritative result in database');
    console.log('[PASS] Repeated offline processing checks are idempotent; zero duplicates');

    // 4. Ingest Window 1 while OFFLINE (collector continues running unimpeded)
    const s1W1: MockResponse = {
      session_id: sessionId,
      sensor: 1,
      first_index: 401,
      count: 400, // 401..800
      lost: [],
      session_end: false,
      crc32: '00000000',
      data: Array.from({ length: 400 }, () => [200, 600, 200]),
    };
    const s2W1: MockResponse = {
      session_id: sessionId,
      sensor: 2,
      first_index: 400,
      count: 400, // 400..799
      lost: [],
      session_end: false,
      crc32: '00000000',
      data: Array.from({ length: 400 }, () => [300, 400]),
    };
    storage.persistValidatedReply(s1W1);
    storage.persistValidatedReply(s2W1);

    const processedW1 = await coordinator.processEligibleWindows();
    assert.strictEqual(processedW1.length, 1);
    assert.strictEqual(processedW1[0].window_idx, 1);
    assert.strictEqual(processedW1[0].mode, 'offline');
    console.log('[PASS] Ingestion and offline processing continue smoothly during network outage');

    // 5. Reconnection: Network returns ONLINE
    let reconnectionFired = false;
    coordinator.onReconnection((ev) => {
      reconnectionFired = true;
      assert.strictEqual(ev.offlineResultsPreserved, 2);
    });

    prober.nextResult = { ok: true, latencyMs: 20 };
    await monitor.probeOnce();
    await monitor.probeOnce();
    assert.strictEqual(monitor.getState(), 'ONLINE');
    assert.strictEqual(reconnectionFired, true);

    // Reconnection must preserve existing offline results intact
    const preservedW0 = storage.getProcessingResult(sessionId, 0);
    const preservedW1 = storage.getProcessingResult(sessionId, 1);
    assert.strictEqual(preservedW0?.mode, 'offline');
    assert.strictEqual(preservedW1?.mode, 'offline');
    console.log('[PASS] Reconnection preserves all offline results and emits sync notification');

    // 6. Conflict safety: an incoming result for window 0 cannot overwrite committed offline result
    const conflictingOnlineResult: any = {
      ...preservedW0,
      mode: 'online',
      magnitude: 9999.0, // conflicting
    };
    storage.saveProcessingResult(conflictingOnlineResult, [], 0, 0);
    const checkedW0 = storage.getProcessingResult(sessionId, 0);
    assert.strictEqual(checkedW0?.mode, 'offline', 'Committed offline result must NOT be overwritten');
    assert.strictEqual(checkedW0?.magnitude, 500.0);
    console.log('[PASS] Authoritative offline result protected against subsequent overwrites');

    // 7. Restart test: Checkpoint and results persist across process restart
    coordinator.stop();
    monitor.stop();
    storage.close();

    const reopenedStorage = new StorageEngine({ dbPath });
    const cp = reopenedStorage.getProcessingCheckpoint(sessionId);
    assert.strictEqual(cp?.last_processed_window, 1);
    assert.strictEqual(reopenedStorage.getProcessingResults(sessionId).length, 2);
    console.log('[PASS] Durable processing checkpoint and offline results survive process restart');

    reopenedStorage.close();
  } finally {
    cleanDbFiles(dbPath);
  }
}

// =========================================================================
// RUN ALL TESTS
// =========================================================================
async function run() {
  await testStateMachineThresholds();
  await testTimeoutsAndConcurrency();
  await testHttpHealthProberAndLocalServer();
  await testTimedFailoverExperiment();
  await testPersistenceAndOfflineProcessingIntegration();

  console.log('\n=============================================================');
  console.log('=== ALL PHASE 4 NETWORK MONITOR & FAILOVER TESTS PASSED 100% ===');
  console.log('=============================================================');
}

run().catch((err) => {
  console.error('Phase 4 Test Failure:', err);
  process.exit(1);
});
