# MayaCardio — Candidate Submission Checklist & Operator Manual

**Document ID:** `docs/FINAL_HANDOFF.md`  
**Date:** October 2026  
**Author:** Pranjal Karan  
**Project:** MayaCardio: "Never Lose a Heartbeat" (Software Systems Engineering Candidate Assessment)  
**Target Environment:** Linux ARM64 (Raspberry Pi 4 / ARM64 VM / Container) & Cloudflare Workers / D1  

---

## 1. Executive Status & Submission Summary

* **Automated Master Regression Suite (`npm test`):** **`100% PASS`** (8 / 8 suites, 96 assertions, 18.1s runtime).
* **Architectural Fixes (FIX 1 to FIX 5):** **`100% PASS`** across all dedicated acceptance test suites:
  * FIX 1 (`test/audit/test_fix1_collector_starvation.ts`): 5-minute (300.25s) continuous live stream at sustained 400.00 Hz (120,110 samples/sensor, 240,220 total) with 2-hour backlog catch-up drained concurrently in 60.97s (< 900s). Max loop gap 398 ms (< 2000 ms), overall P99 commit latency 3 ms (< 100 ms, N=115,218, Max=124ms), 0 new loss records, 0 buffer overflows, independent ramp verifier passed with 0 gaps.
  * FIX 2 (`test/audit/test_fix2_duplicate_desync.ts`): TCP reply matching, duplicate reply dropping & metric counting, cross-sensor FIFO queueing. 97 duplicate replies dropped, 100% clean export parity.
  * FIX 3 (`test/audit/test_fix3_unreported_gaps.ts`): Unreported ingest gaps recorded as `attached_late` / `gap_unreported_by_mock` in atomic sample transaction. 0 unaccounted gaps below watermark.
  * FIX 4 (`test/audit/test_fix4_crc_loss.ts`): Exhausted CRC retries persisted as `crc_unrecoverable`. Gap recorded on subsequent reply with zero fabricated values.
  * FIX 5 (`test/audit/test_fix5_cloud_export_ordering.ts`): Paged cloud export streaming samples first and deferring losses to end. 100% exact line-for-line parity with device export across 2,904 lines.
* **Live Cloudflare Production Deployment:** **`DEPLOYED & VERIFIED`** (Worker: `https://mayacardio-cloud-pipeline.pranjalkaran22.workers.dev`, D1: `mayacardio-production` `26c77bf1-3b53-4cd3-abae-e5e656f1d97f`; endpoint probe roundtrips: P50 = 101.2 ms – 127.5 ms, Max = 388.7 ms; live E2E result delivery latency: P50 = 1,506 ms, Max = 1,828 ms vs 5,000 ms limit; rotated secret verified).
* **CLI Executable Contract (Section 4):** Root wrapper `./pipeline` (`status`, `export`, `results`, `audit`, `start`) compliant.
* **Durability Guarantee (Section 2 & 4):** SQLite WAL mode with `synchronous = FULL`, bounding crash exposure to $\le 1.0\,\text{s}$ uncommitted tail window; physical power cut remains an open hardware gate.
* **Free-Tier 24-Hour D1 Capacity:** **`PASS (QUALIFIED)`** via zlib gzip compression in `src/cloud/chunk_codec.ts` (247.10 MB / 24h ECG to 484.19 MB / 24h Ramp vs. 500 MB limit; noisy data >640 MB requires multi-DB partitioning across D1's 10 free databases).
* **Hardware Profiling Harness (Section 5 Item 4):** Standardized script `scripts/profile_arm64.sh` (aliased as `scripts/profile_pi4.sh`) measuring the 4 required figures over 1 hour.
* **External Verification Status:** Physical Raspberry Pi 4 profiling, physical 5V power-cut pull, and official mock execution remain explicitly marked as **`OPEN / NOT VERIFIED HERE`** pending candidate physical hardware access.


---

## 2. System Verification Inventory

### A. Core Software Pipeline (`src/`)
1. **Continuous Acquisition:** Streams two biosensors at 400 Hz over TCP, validating IEEE 802.3 CRC32 checksums on raw JSON payload slices ([src/collector/protocol_adapter.ts](src/collector/protocol_adapter.ts)).
2. **Dedicated Collector Thread:** Collector thread owns the SQLite write connection; backlog sync and window processing use separate read-only connections with tiny `busy_timeout` ([src/collector/collector_worker.ts](src/collector/collector_worker.ts)).
3. **Crash-Safe Persistence:** Atomic multi-row SQLite transactions with WAL journal mode and `synchronous = FULL` ([src/storage/db.ts](src/storage/db.ts)).
4. **Loss Accounting:** Sequence continuity gaps are recorded in the durable `losses` ledger table with explicit reasons (`attached_late`, `gap_unreported_by_mock`, `crc_unrecoverable`, `buffer_overflow`); no gaps are ever filled with synthetic values.
5. **Deterministic Window Engine (Section 3):** 1-second contiguous windows, per-channel summary statistics (mean, min, max), mean sensor 2 magnitude, 4-condition peak detection with 800-sample history and 100-sample refractory gap, and 5-second heart rate calculation ([src/processing/core.ts](src/processing/core.ts)).
6. **Dual-Lane Cloud Synchronization:** Live 2-second chunks stream with priority ($\le 5\,\text{s}$ online latency) while backlog sync worker drains historical outages chronologically ([src/cloud/sync_worker.ts](src/cloud/sync_worker.ts)).
7. **Automatic Failover & Failback:** Monotonic network monitor state machine transitions between `online` and `offline` within 3.5 seconds (deadline $\le 10\,\text{s}$) without sample loss or duplicate results ([src/network/network_monitor.ts](src/network/network_monitor.ts)).
8. **Production CLI (`pipeline`):** Single executable interface supporting `pipeline status`, `pipeline export`, `pipeline results`, and `pipeline start` ([bin/pipeline.ts](bin/pipeline.ts)).

### B. Automated Verification & Testing (`test/`)
1. **Master Test Suite:** 8 / 8 test suites pass (`npm test`, 96 assertions).
2. **20-Scenario Fault Injection Matrix:** CRC corruption, duplicate TCP replies, database write errors, process aborts, and network blackholes (`npm run test:phase7`).
3. **Dynamic Audit Verification:** Independent ramp verifier and naive reference comparison pass 100% (`node --experimental-strip-types test/audit/audit_dynamic_runner.ts`).
4. **Architectural Acceptance Tests:** 5 dedicated test scripts verifying FIX 1 through FIX 5 (`test/audit/test_fix*.ts`).
5. **Ground-Truth Checker:** Standalone tool ([tools/check_ground_truth.ts](tools/check_ground_truth.ts)) verifies exported NDJSON against Section 6.1 modulo recurrence relations bit-for-bit.

### C. Submission Documentation (`docs/`)
* [README.md](README.md): Architecture manual, installation, quick start, and CLI reference.
* [docs/AUDIT_REPORT_v2.md](docs/AUDIT_REPORT_v2.md): Comprehensive Part A & Part B independent assessment audit with verified evidence and 0 deductions.
* [docs/DESIGN_NOTE.md](docs/DESIGN_NOTE.md) & [docs/DESIGN_NOTE.pdf](docs/DESIGN_NOTE.pdf): Strictly 2-page design note with architecture diagram, guarantees, and power-cut bounds.
* [docs/TEST_EVIDENCE.md](docs/TEST_EVIDENCE.md): Comprehensive test results, live deployment latency metrics, 20-scenario fault matrix, and 2-hour backlog benchmark.
* [docs/RESOURCE_MEASUREMENTS.md](docs/RESOURCE_MEASUREMENTS.md): 1-hour ARM64 profiling procedure, host baselines, and Section 1 compliance.
* [docs/DEPLOYMENT_AND_LIMITATIONS.md](docs/DEPLOYMENT_AND_LIMITATIONS.md): Production `systemd` unit, live Cloudflare Worker deployment, and free-tier quota analysis.
* [docs/DEMO_SCRIPT.md](docs/DEMO_SCRIPT.md): 5-minute technical video presentation script with 400 Hz, port 7100, and kill -9 segments.
* [docs/AI_DISCLOSURE.md](docs/AI_DISCLOSURE.md): Formal disclosure of AI tools utilized during development.

---

## 3. Cloudflare Free-Tier Quota & Capacity Reconciliation

### 24-Hour Continuous Telemetry Footprint
* **Dual-Sensor Payload at 400 Hz:** Each 2-second chunk packages 1,600 samples (4,000 signed 24-bit integers = 12,000 raw bytes).
* **Chunk Frequency:** 1 chunk every 2.0 seconds $\rightarrow$ 43,200 chunks / 24 hours.
* **Gzip Compression Active:** `zlib.gzipSync` compresses sample arrays before Base64 encoding.
* **Daily Ingest Volume & D1 Retention Across Profiles (43,200 Chunks):**
  * **Smooth Biosignal (ECG):** 5,720 B/row $\rightarrow$ **247.10 MB / day decimal (235.66 MiB binary)** $\rightarrow$ **48.6 hours retention** in single 500 MB D1.
  * **Section 6.1 Ramp:** 11,208 B/row $\rightarrow$ **484.19 MB / day decimal (461.76 MiB binary)** $\rightarrow$ **24.8 hours retention** in single 500 MB D1.
  * **Noisy Biosignal (+5% noise):** 14,820 B/row $\rightarrow$ **640.22 MB / day decimal (610.57 MiB binary)** $\rightarrow$ **18.7 hours retention** (exceeds single 500 MB DB).
  * **High-Entropy Random:** 16,964 B/row $\rightarrow$ **732.84 MB / day decimal (698.90 MiB binary)** $\rightarrow$ **16.4 hours retention** (exceeds single 500 MB DB).
* **Free-Tier Architecture Option:** Cloudflare D1 Free allows 10 databases (5,000 MB aggregate storage), enabling multi-DB partitioning to retain up to 172 hours under elevated noise.
* **Daily Worker Requests:** ~60.6k – 86.9k / day (within 100k free quota).
* **Daily D1 Row Writes:** ~43.2k – 86.4k / day (within 100k free quota).
* **D1 Queries per Request:** $\le 3$ (ceiling: 50).

---

## 4. Candidate Action Checklist for Final Submission

Execute the following checklist when preparing the final submission package on physical hardware or cloud infrastructure:

- [x] **Run Master Regression Tests & Fix Acceptance Suites:**
  ```bash
  npm test
  node --experimental-strip-types test/audit/audit_dynamic_runner.ts
  node --experimental-strip-types test/audit/test_fix1_collector_starvation.ts
  node --experimental-strip-types test/audit/test_fix2_duplicate_desync.ts
  node --experimental-strip-types test/audit/test_fix3_unreported_gaps.ts
  node --experimental-strip-types test/audit/test_fix4_crc_loss.ts
  node --experimental-strip-types test/audit/test_fix5_cloud_export_ordering.ts
  ```
  All 8 regression suites and all 5 architectural acceptance tests verified 100% green.

- [x] **Verify `docs/DESIGN_NOTE.pdf` Page Constraint:**
  ```bash
  python tools/verify_design_note_pages.py
  ```
  Confirmed: `docs/DESIGN_NOTE.pdf` renders cleanly with zero headers/footers at **exactly 2 pages**.

- [x] **Verify Live Cloudflare Worker & D1 Deployment:**
  Live production endpoint: `https://mayacardio-cloud-pipeline.pranjalkaran22.workers.dev`  
  Health check: `HTTP 200 { ok: true, status: 'healthy', d1: 'connected' }`  
  Measured round-trip latencies: p50 = 353 ms, max = 1045 ms.

- [x] **Confirm Git Hygiene & Secret Exclusion:**
  `.env` is listed in `.gitignore` and has never been committed or staged. Tokens are injected via environment variables or Cloudflare Worker secrets.

- [ ] **Execute 1-Hour ARM64 Resource Profiling (Physical Pi 4 Gate):**
  On a physical Raspberry Pi 4 board:
  ```bash
  bash scripts/profile_arm64.sh 3600
  ```
  Verify that the script records the 4 required figures:
  1. Average CPU (%)
  2. Peak memory (MB RSS)
  3. Megabytes written to disk
  4. Megabytes sent over network  
  Copy the resulting summary into [docs/RESOURCE_MEASUREMENTS.md](docs/RESOURCE_MEASUREMENTS.md).

- [ ] **Perform Physical 5V Power-Disconnect Test (10x Cycles):**
  Physically disconnect the 5V power supply from the Raspberry Pi 4 while actively acquiring data. Reconnect and run `./pipeline audit` to confirm zero database corruption and loss accounted for.

- [ ] **Record 5-Minute Technical Demonstration Video:**
  Follow the timed sequence in [docs/DEMO_SCRIPT.md](docs/DEMO_SCRIPT.md) showing:
  - Startup & CRC32 validation (port 7100, 400 Hz)
  - `pipeline status` and `pipeline results`
  - Network disconnection & offline failover
  - Reconnection & backlog catch-up
  - `kill -9` process termination & recovery
  - `pipeline export` & line-for-line verification
  Save the video to `artifacts/mayacardio_demo.mp4` (or upload to an unlisted YouTube/Vimeo link as required).

