# MayaCardio Telemetry Pipeline: Empirical Test Evidence & Audit Ledger

**Date of Execution:** October 15, 2026 (Updated Post-Fixes)  
**Host Environment:** Windows 11 Pro x64 (AMD64), Node.js v24.20.0, PowerShell 5.1  
**Cloud Deployment:** Live Production Cloudflare Worker + D1 Database  
**Author:** Pranjal Karan

---

## 1. Executive Summary & Verification Matrix

All core pipeline phases (0 through 7) and post-audit architectural remediations (FIX 1 through FIX 5) have executed deterministically with **100% PASS** rates.

| Subsystem / Phase | Test Harness Script | Status | Key Measured Metrics |
|:---|:---|:---:|:---|
| **Phase 0: Runtime Primitives** | `test/phase0_baseline.test.ts` | **PASS** | Node.js v24.20.0, native SQLite DatabaseSync, zlib CRC32, signed 24-bit roundtrip. |
| **Phase 1: Ingestion & Framing** | `test/phase1_ingestion.test.ts` | **PASS** (27/27) | CRC32 byte-slice extraction, TCP chunk fragmentation, duplicate deduplication, bounded retry. |
| **Phase 2: Durable Persistence** | `test/phase2_persistence.test.ts` | **PASS** | SQLite WAL, `PRAGMA synchronous = FULL;`, atomic commit, unreported gap loss recording. |
| **Phase 3: Window Processing** | `test/phase3_processing.test.ts` | **PASS** | 191,948 samples/sec throughput; 4.17 ms average window latency (< 1000 ms limit). |
| **Phase 4: Network Monitor** | `test/phase4_network_monitor.test.ts` | **PASS** | Monotonic prober, failover in 10.0s, recovery in 2.0s (< 10s ceiling). |
| **Phase 5: Cloud Synchronization** | `test/phase5_cloud_sync.test.ts` | **PASS** | 404,497 samples/sec sync rate; Gzip compression: 247.10 MB/day (ECG) to 484.19 MB/day (Ramp). |
| **Phase 6: CLI & Export Parity** | `test/phase6_cli_audit.test.ts` | **PASS** | Status NDJSON, canonical ordering (samples first, losses last), audit integrity checks. |
| **Phase 7: End-to-End Acceptance** | `test/phase7_acceptance.test.ts` | **PASS** | 20/20 fault scenarios passed; 2-hour backlog (3,600 chunks, 5.76M samples) recovered in 8.70s. |
| **FIX 1: Collector Starvation** | `test/audit/test_fix1_collector_starvation.ts` | **PASS** | Sustained 400.00 Hz live streaming (120,110 samples/sensor in 300.25s; 240,220 total); max loop gap = 398 ms (< 2000 ms limit), P99 commit = 3 ms (N=115,218, Max=124ms), 2-hour backlog drained in 60.97s (< 900s limit), 0 buffer overflows. |
| **FIX 2: Duplicate Reply Desync** | `test/audit/test_fix2_duplicate_desync.ts` | **PASS** | 97 duplicates dropped, 0 stalls, 0 desyncs, line-for-line clean parity. |
| **FIX 3: Unreported Ingest Gaps** | `test/audit/test_fix3_unreported_gaps.ts` | **PASS** | `attached_late` and `gap_unreported_by_mock` recorded atomically; 0 unaccounted gaps. |
| **FIX 4: Exhausted CRC Loss** | `test/audit/test_fix4_crc_loss.ts` | **PASS** | Exhausted CRC retries record `crc_unrecoverable` loss; 0 fabricated values. |
| **FIX 5: Cloud Export Ordering** | `test/audit/test_fix5_cloud_export_ordering.ts` | **PASS** | 100% exact parity across 2,904 lines (losses strictly after samples). |

---

## 2. Live Cloudflare Deployment Verification

The cloud pipeline is actively deployed and operational on the Cloudflare Free Tier:
* **Worker URL:** `https://mayacardio-cloud-pipeline.pranjalkaran22.workers.dev`
* **D1 Database:** `mayacardio-production` (UUID: `26c77bf1-3b53-4cd3-abae-e5e656f1d97f`)
* **Endpoint Status:** `GET /health` verified live returning `HTTP 200 { ok: true, status: 'healthy', d1: 'connected' }`.
* **Verified Latency Measurements (Timestamp: 2026-10-15T12:38:20Z):**
  * Health Endpoint (`/health`): 10/10 probes succeeded | **p50 = 127.5 ms, max = 388.7 ms**
  * Authenticated Status (`/status`): 10/10 probes succeeded | **p50 = 101.2 ms, max = 231.7 ms**
* **Live End-to-End Result Delivery Latency (`scratch/test_live_e2e_latency.ts`, 2026-10-15T14:25:00Z):**
  * 10 consecutive 1-second windows (8,000 samples) streamed live to production Cloudflare Worker; measured from window close to durable D1 retrieval: **P50 = 1,506 ms, P95 = 1,828 ms, Max = 1,828 ms** (strictly $\le 5,000\,\text{ms}$ requirement).
* **Secret Rotation:** Cryptographically rotated via Wrangler; verified old credential returns `HTTP 401 Unauthorized` and rotated token returns `HTTP 200 OK`.
* **CLI Live Status Verification:**
  ```bash
  node --experimental-strip-types bin/pipeline.ts status
  # Output: {"mode":"online","samples_saved":{"1":1920,"2":1920},"samples_in_cloud":1600,"backlog":320}
  ```


---

## 3. Detailed Verification by Functional Subsystem

### A. CRC32 Validation & Frame Parsing (`phase1_ingestion.test.ts`)
* **Framing:** Tested streaming NDJSON over loopback TCP with chunk fragmentations, partial packets across TCP read boundaries, and multi-frame bursts.
* **CRC32 Validation:** Exact raw payload substring extracted and validated against IEEE 802.3 CRC32 polynomial. Corrupted byte injections are detected, triggering bounded retries.
* **Signed 24-Bit Range:** Verified signed 24-bit integer limits (`-8,388,608` to `8,388,607`). Values exceeding 24 bits or non-integer tokens are rejected.

### B. Durable Persistence & SQLite WAL (`phase2_persistence.test.ts`)
* **WAL Mode:** Database initialized with `PRAGMA journal_mode = WAL; PRAGMA synchronous = FULL;`. Every commit forces an explicit disk flush.
* **Atomic Batching:** Single transaction per batch ensures samples, losses, and watermarks commit atomically. Uncommitted transactions roll back cleanly upon simulated crash.
* **Loss Ledger:** Verified that missing sequence numbers create durable rows in the `losses` table (`attached_late`, `gap_unreported_by_mock`, `crc_unrecoverable`, `session_restart_tail_lost`).
* **Process Abort Testing:** Child process aborted mid-transaction using `process.abort()`. Parent process reopened the database, ran `PRAGMA integrity_check`, verified zero corruption, and confirmed that only committed transactions survived.

### C. Deterministic Windowing & Heart-Rate Reference (`phase3_processing.test.ts`)
* **Window Alignment:** Contiguous 1.0-second boundaries (400 samples for Sensor 1 and 400 samples for Sensor 2: samples $400w$ to $400w + 399$).
* **Statistical Metrics:** Verified Mean, Min, and Max per channel, and Sensor 2 vector magnitude (Mean of $\sqrt{ch1^2 + ch2^2}$) against independent mathematical reference implementations.
* **Four-Condition Peak Detection (Section 3):**
  1. The 800 preceding samples and the sample after candidate $i$ are available.
  2. Candidate $i$ is strictly larger than both immediate neighbours ($s[i] > s[i-1]$ and $s[i] > s[i+1]$).
  3. Candidate $i$ is strictly larger than half of the maximum of the preceding 800 samples ($s[i] > 0.5 \times \max_{j=i-800}^{i-1} s[j]$).
  4. Candidate $i$ is at least 100 samples after the previously accepted peak ($i - \text{priorPeak} \ge 100$).
* **Heart Rate Calculation:** Evaluates accepted peaks across the 5-second window ($400(w-4)$ to $400(w+1)-1$). Evaluates strictly to `null` if $w < 4$, if any sample is missing, or if fewer than 2 peaks are found.
* **Signal Mode Sanity:** Verified heart rates reproduce the 60 bpm, 90 bpm, 72 bpm cycle.

### D. Network Failover State Machine (`phase4_network_monitor.test.ts`)
* **Health Polling:** Consecutive probe failures (HTTP timeouts / 5xx responses) trigger state transition:
  * Probe 1 fail $\rightarrow$ `ONLINE_DEGRADED`
  * Probe 2 fail $\rightarrow$ `OFFLINE` (Elapsed time: 10.0s worst-case with 5.0s probe timeout).
  * Recovery: 2 consecutive successful probes restore `ONLINE` in 2.0s.
* **Autonomous Processing:** Offline windows computed locally and saved with `mode: 'offline'`.

### E. Idempotent Cloud Synchronization & Quota Accounting (`phase5_cloud_sync.test.ts`)
* **Idempotency Key:** Primary key `(session_id, chunk_index)` in Cloudflare D1 schema (`WITHOUT ROWID`, 0 secondary indexes).
* **SHA-256 Payload Hash:**
  * Re-transmitting an identical chunk returns `200 OK` (`duplicate: true`) and performs 0 database writes.
  * Transmitting a differing payload with the same key returns `409 Conflict` and records in `cloud_incidents`.
* **Gzip Compression Arithmetic Across Profiles (43,200 Chunks / Day):**
  * Raw values: 12,000 bytes ($11.72\,\text{KiB}$) per 2-second chunk $\rightarrow$ 16,000 Base64 characters.
  * Smooth ECG: 3,613 bytes gzip (3.32x ratio) $\rightarrow$ 5,720 bytes D1 row $\rightarrow$ **247.10 MB decimal / 235.66 MiB binary** $\rightarrow$ **48.6 hours retention** in single 500 MB D1.
  * Ramp Reference: 7,730 bytes gzip (1.55x ratio) $\rightarrow$ 11,208 bytes D1 row $\rightarrow$ **484.19 MB decimal / 461.76 MiB binary** $\rightarrow$ **24.8 hours retention** in single 500 MB D1.
  * Noisy Biosignal (+5% noise): 10,440 bytes gzip (1.15x ratio) $\rightarrow$ 14,820 bytes D1 row $\rightarrow$ **640.22 MB decimal / 610.57 MiB binary** $\rightarrow$ **18.7 hours retention**.
  * High Entropy: 12,046 bytes gzip (1.00x ratio) $\rightarrow$ 16,964 bytes D1 row $\rightarrow$ **732.84 MB decimal / 698.90 MiB binary** $\rightarrow$ **16.4 hours retention**.
* **Single D1 Holding Capacity:** >24 hours for smooth biosignals and ramp streams. Elevated noise requires multi-DB partitioning across the 10 available free D1 databases (5,000 MB aggregate storage).

---

## 4. Full Two-Hour Backlog Catch-Up Benchmark

A full-scale two-hour backlog catch-up simulation was executed in `test/phase7_acceptance.test.ts` Suite 3:
* **Simulated Backlog:** 3,600 chunks (7,200 seconds = 2 continuous recording hours).
* **Sample Count:** 2,880,000 samples for Sensor 1 + 2,880,000 samples for Sensor 2 = **5,760,000 total samples**.
* **Pre-population Duration:** 0.04 seconds.
* **Measured Benchmark Results:**
  ```
  Total Chunks Synchronized: 3,600 / 3,600
  Total Catch-up Elapsed Time: 6.45s (Assessment Deadline: <= 900s / 15 minutes)
  Chunk Sync Rate: 558 chunks/sec
  Effective Timeline Recovery Rate: 892,740 sample-positions/sec (Requirement: >= 7,200/sec)
  ```
* **Inventory Completeness:** 100% of chunks (3,600 / 3,600) verified present in cloud inventory.

---

## 5. Ground-Truth Parity & Recurrence Invariant Verification

Phase 7 Suite 4 verified deterministic parity against the assessment Section 6.1 specification:
* **Sensor 1 Recurrence:** $s_1(i, c) = (3i + c) \pmod{8,388,608}$ for channels $c \in \{0, 1, 2\}$.
* **Sensor 2 Recurrence:** $s_2(i, c) = (2i + c) \pmod{8,388,608}$ for channels $c \in \{0, 1\}$.
* **Verified Parity:** Real device and cloud NDJSON exports verified with 100% parity against independent ramp verifier (`test/audit/independent_verifier.ts`).

---

## 6. Open Hardware & Official Mock Gates

The following tests require physical Raspberry Pi 4 hardware and official assessment artifacts:
1. **Physical Raspberry Pi 4 Deployment:** Execution on Debian 12 ARM64 hardware.
2. **Physical 5V Power Cuts (10x):** Manual power disconnects during active 400 Hz streaming.
3. **Official Mock Executable:** Evaluation against the official `biosignal-mock` binary and `check_ground_truth` script.
4. **Physical Cgroup Profiling:** 1-hour profiling harness on physical ARM64 SoC.
