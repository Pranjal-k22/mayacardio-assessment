/**
 * MayaCardio: Never Lose a Heartbeat
 * Main package entry point exporting all system components.
 */

export * from './types.ts';
export { StorageEngine } from './storage/db.ts';
export { BiosignalTcpClient } from './collector/tcp_client.ts';
export { BiosignalProtocolAdapter } from './collector/protocol_adapter.ts';
export { extractDataArraySlice, validateRawResponse } from './collector/raw_parser.ts';
export {
  WindowProcessingEngine,
  type WindowEngineOptions,
} from './processing/engine.ts';
export {
  computeWindowResult,
  PeakDetector,
  calculateHeartRate,
  WINDOW_SIZE,
  PEAK_HISTORY_SIZE,
  HR_WINDOW_LOOKBACK_WINDOWS,
  HR_WINDOW_LOOKBACK_SAMPLES,
} from './processing/core.ts';
export { NetworkMonitor, type NetworkMonitorConfig, type NetworkState } from './network/network_monitor.ts';
export { HttpHealthProber } from './network/http_prober.ts';
export { OfflineProcessingCoordinator } from './network/offline_coordinator.ts';
export { ChunkCodec, SAMPLES_PER_CHUNK, CHUNK_DURATION_SECONDS } from './cloud/chunk_codec.ts';
export { CloudApiClient } from './cloud/client.ts';
export { SyncWorker, type SyncWorkerOptions, type SyncStats } from './cloud/sync_worker.ts';
export { IntegrityAuditor, type AuditReport } from './audit/integrity_audit.ts';
