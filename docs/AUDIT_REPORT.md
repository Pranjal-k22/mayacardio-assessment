> [!WARNING]
> # HISTORICAL PRE-REMEDIATION AUDIT — NOT THE CURRENT ACCEPTANCE VERDICT
> This document is retained strictly as an immutable historical record of early audit findings and identified architectural defects prior to FIX 1–FIX 5 remediations.
> For the authoritative, post-remediation audit and verified acceptance freeze, see [docs/AUDIT_REPORT_v2.md](file:///c:/Users/pc/Desktop/mayacardio/docs/AUDIT_REPORT_v2.md).

# Comprehensive Independent Audit Report: MayaCardio Telemetry Pipeline
**Assessment:** "Never Lose a Heartbeat" (Edge-to-Cloud Biosignal Pipeline: Raspberry Pi 4 + Free Cloud)  
**Audit Date:** October 15, 2026 (Historical Pre-Remediation Baseline)  
**Auditor:** Independent Systems Auditor (Antigravity Evaluation Engine)  
**Host Environment:** Windows 11 Pro x64 (AMD64), Node.js v24.20.0, PowerShell 5.1  
**Target Architecture:** Raspberry Pi 4 Model B (ARM64, $\le 4$ cores, 2 GB RAM) + Cloudflare Free Tier (Workers + D1)

---

## 1. Executive Summary & Final Verdict

### Final Verdict: **NOT SUBMITTABLE**

While MayaCardio demonstrates strong software architecture, zero third-party dependencies, clean TypeScript code, and an active Cloudflare Worker deployment, it contains **critical blocking defects** that violate core assessment invariants:
1. **Collector Starvation during Backlog Recovery:** The pipeline daemon runs cloud synchronization in the **same loop thread** as the TCP collector without isolation. During recovery after an outage, synchronous live uploads block TCP reads; because the mock buffer holds only 10 seconds of data, incoming telemetry overflows and is dropped.
2. **Missing Loss Rows on Ingest Gaps:** When `first_index > expected_next` and the gap is not in `lost[]`, `persistValidatedReply` does **not** insert a row into the `losses` ledger at ingest time. The gap never appears in `pipeline export`, causing a **silent loss violation**.
3. **Cloud Export Ordering Violation:** Cloud export interleaves sample lines and loss lines per chunk, violating the specification requiring all sample lines to precede loss lines.
4. **TCP Desynchronization on Duplicate Replies:** Injected duplicate replies remain in the TCP stream buffer, causing subsequent reads to mismatch sensor IDs and fail indefinitely.
5. **Hardware & Official Mock Gaps:** Physical Raspberry Pi 4 power pulls, cgroup measurements, and official mock evaluation are absent from the repository.

---

## 2. Assessment Criteria & Scorecard

*Note: Per assessment ground rules, each verdict is strictly one of `PASS`, `FAIL`, or `NOT VERIFIABLE HERE`. Deductions of 5 points are assessed for each silent loss or unflagged wrong value scenario.*

| # | Criterion | Points | Verdict | Evidence | Fix Needed |
|:---|:---|:---:|:---:|:---|:---|
| **1** | **Device Ingestion & Framing** (400 Hz dual-sensor, signed 24-bit, CRC32 raw slice, bounded retries) | 12 | **FAIL** | Raw CRC32 parsing works (`src/collector/raw_parser.ts:140`), but duplicate TCP reply corrupts stream framing (`test/audit/audit_dynamic_runner.ts`), and unrecoverable errors drop without recording loss (`protocol_adapter.ts:124`). | Drain unexpected socket data; record loss record if retries exhausted. |
| **2** | **Device Data Safety & Durability** (SQLite WAL, `synchronous=FULL`, atomic commit before watermark, zero silent loss) | 13 | **FAIL** | SQLite WAL `synchronous=FULL` active (`src/storage/db.ts:46`), but gaps without `lost[]` are NOT written to `losses` table (`db.ts:205-300`), resulting in silent gaps in `export`. | Insert loss row in `persistValidatedReply` when `reply.first_index > currentExpected`. |
| **3** | **Crash & Power Durability** (Crash recovery, PRAGMA integrity_check, new session unread tail loss) | 10 | **NOT VERIFIED (needs official mock)** | PRAGMA integrity_check passed 10/10 cycles (`test/audit/audit_kill_test.ts`); session restart loss works; but physical power pulls not conducted. | Execute 10 physical power cuts on Raspberry Pi 4. |
| **4** | **Online Mode Live Streaming** (Result in cloud $\le 5\,\text{s}$ per second of data, live lane priority) | 12 | **PASS** | Live Cloudflare Worker latency measured at **p50 = 353 ms, max = 1045 ms** (`test/audit/measure_live_latency.ts`) vs 5,000 ms target. | None for latency; fix collector isolation in daemon. |
| **5** | **Offline Mode & Network Switching** (Autonomous local processing, reachability probe, transition $\le 10\,\text{s}$) | 13 | **PASS** | Monotonic network monitor transitions within 10.0 s blackhole, 2.2 s restore (`src/network/network_monitor.ts:90-94`). Local offline results match cloud bit-for-bit. | Align `probeTimeoutMs` in `bin/pipeline.ts` to match `<3.5 s` doc claim. |
| **6** | **Catch-Up & Cloud Parity** (Sync 2-hour backlog $\le 15\,\text{min}$, cloud inventory reconciliation, export parity) | 20 | **FAIL** | `syncHistoricalBacklog` never called in daemon (`bin/pipeline.ts:440-475`); cloud export interleaves chunk losses (`test/audit/test_cloud_export_parity.ts`); backlog benchmark simulated in-process only (`test/d1_adapter.ts`). | Move backlog sync to background worker; emit all sample lines before loss lines in cloud export. |
| **7** | **Section 3 Results Exactness** (Contiguous 1s windows, 5-channel mean/min/max, magnitude, 4-condition peak HR) | 10 | **PASS** | Independent naive reference perfectly matches window engine (`test/audit/audit_dynamic_runner.ts`); signal HR reproduces 60/90/72 bpm cycle. | None. |
| **8** | **Small-Device Limits, Resource Reporting & Design Note** ($\le 4$ cores, $\le 2$ GB RAM, exact 2-page PDF, code quality) | 10 | **NOT VERIFIABLE HERE** | Zero npm dependencies; 2-page PDF verified (`tools/verify_design_note_pages.py`); but ARM64 figures measured in Docker, not physical Pi 4. | Run 1-hour profiling harness on physical Raspberry Pi 4. |
| **TOTAL** | | **100** | | | |

---

## 3. Score Breakdown & Deductions

### Verified Base Points
- **Criteria 4 (Online Mode Live Streaming):** 12 / 12
- **Criteria 5 (Offline Mode & Network Switching):** 13 / 13
- **Criteria 7 (Section 3 Results Exactness):** 10 / 10
- **Total Verified Base Points:** **35 / 100**

### Penalties & Deductions (-5 pts per silent loss / unflagged wrong value, max -30 pts)
1. **Deduction 1 (-5 pts): Silent Unaccounted Gap on Ingest**
   - *Failure Scenario:* Sensor mock experiences an unlisted gap (`first_index > expected_next` with empty `lost[]`). `persistValidatedReply` in `src/storage/db.ts:205-300` inserts incoming samples and stalls the contiguous watermark, but **writes no loss record** to SQLite. Running `pipeline export --from device` completely omits the missing range without any loss line, concealing the drop from downstream consumers.
2. **Deduction 2 (-5 pts): Collector Dropping Data During Backlog Sync**
   - *Failure Scenario:* After a 2-hour network outage, the device reconnects. `bin/pipeline.ts:456-470` enters a synchronous `while (nextChunkIdxToSync <= maxReadyChunk)` loop executing HTTP PUT requests. While uploading, `adapter.readNext` is never called. Because `tools/biosignal_mock.ts:55` maintains a 10-second ring buffer (4,000 samples), 1,440,000 incoming samples are dropped unread.
3. **Deduction 3 (-5 pts): Unrecoverable Ingestion Failure Dropped Silently**
   - *Failure Scenario:* If CRC corruption persists beyond 3 retries (`src/collector/protocol_adapter.ts:118-129`), the function returns `{ success: false, recoverable: false }`. `bin/pipeline.ts` catches and discards the error; no loss entry is written to SQLite, leaving an unflagged hole.
4. **Deduction 4 (-5 pts): Cloud Export Format Ordering Defect**
   - *Failure Scenario:* In `cloud/worker/src/index.ts:498-530`, chunk loss records are emitted immediately after chunk samples. When chunk 0 contains a loss and chunk 1 contains samples, loss records appear prior to sample lines (verified by `test/audit/test_cloud_export_parity.ts`), violating the required format `sample lines then loss lines`.

**Total Deductions Assessed:** **-20 points**  
**Net Verified Score:** **15 / 100**

---

## 4. Part A: Comprehensive Static Code Analysis

### A1. Sample Rates and Windowing Boundaries
- **400 Hz / 400 Samples Everywhere:** `src/processing/core.ts:3` (`WINDOW_SIZE = 400`), `src/cloud/chunk_codec.ts:5` (`SAMPLES_PER_SECOND = 400`), `src/storage/db.ts:820, 828` (`400 * windowIdx`), `tools/biosignal_mock.ts:55` (`bufferCapacity = bufferSeconds * 400`).
- **Any 500/300 Hz?** No occurrences of 500 Hz or 300 Hz exist in code or docs. All documentation accurately states 400 Hz.
- **Verdict:** **PASS**

### A2. CRC-32 Extraction, Verification & Recovery
- **Raw Byte Slice Extraction:** `src/collector/raw_parser.ts:11-93` implements `extractDataArraySlice`, scanning raw UTF-8 byte buffers quote-by-quote to isolate the `data` array slice from `[` to `]` without re-serialization.
- **Checksum Computation:** `src/collector/raw_parser.ts:140` calls `crc32(extracted.slice)`.
- **Lowercase Hex Comparison:** `src/collector/raw_parser.ts:141-144` compares `computedCrcHex` with reported CRC formatted as 8-digit lowercase hex.
- **Reply Discarding on Mismatch:** `src/collector/tcp_client.ts:268-271` returns validation failure before processing `session_id`, `lost`, or `data`.
- **Bounded Retry:** `src/collector/protocol_adapter.ts:114` bounds retries to 3 attempts.
- **Defect:** If unrecoverable after 3 retries, `protocol_adapter.ts:124-129` does **not** write a loss record with reason to SQLite; the error is dropped.
- **Malformed Line Handling:** `raw_parser.ts:108-118` safely traps JSON parsing exceptions with code `TRUNCATED_OR_MALFORMED`.
- **Verdict:** **FAIL** (Unrecoverable corruption drops without loss ledger recording).

### A3. Payload Validation
- **Length vs Count:** `src/collector/raw_parser.ts:199-206` verifies `parsed.data.length === parsed.count`.
- **Channel Count:** `src/collector/raw_parser.ts:240-251` enforces 3 channels for Sensor 1 and 2 channels for Sensor 2.
- **Signed 24-Bit Range:** `src/collector/raw_parser.ts:254-263` enforces `val >= -8388608 && val <= 8388607`.
- **first_index Sanity:** `src/collector/raw_parser.ts:181-188` verifies non-negative integer.
- **Verdict:** **PASS**

### A4. Session Handling & Key Isolation
- **Dynamic Session ID:** `src/collector/protocol_adapter.ts:140-167` takes `session_id` directly from mock replies.
- **Composite Primary Keys:** All database tables include `session_id` in their composite primary keys (`src/storage/db.ts:79, 90, 110, 130, 142, 149, 156`), preventing duplicate collisions across sessions.
- **Session Restart Tail Loss:** `src/collector/protocol_adapter.ts:147-157` queries the old session's watermark and records an explicit loss with `to_index: null` and reason `'session_restart_tail_lost'`.
- **Verdict:** **PASS**

### A5. Gap Handling & Watermarking
- **Watermark Invariant:** `src/storage/db.ts:323-372` (`computeContiguousWatermark`) advances strictly through contiguous stored samples or documented loss ranges. Below `resolvedWatermark`, no gaps exist.
- **Defect:** `src/storage/db.ts:205-290` (`persistValidatedReply`) does not write a loss row when `reply.first_index > currentExpected`. Samples above the gap are committed, but the gap itself is never inserted into `losses` and never appears in `pipeline export`.
- **Verdict:** **FAIL**

### A6. Duplicate Replies & Conflict Handling
- **Identical Duplicates:** `src/storage/db.ts:227-234` compares stored channels against incoming channels; matching rows increment `duplicateSamplesCount` without modifying storage.
- **Conflicting Samples:** `src/storage/db.ts:234-248` logs conflicting values to `integrity_incidents` table and preserves original canonical data.
- **Verdict:** **PASS**

### A7. Durability & SQLite Pragmas
- **WAL & synchronous:** `src/storage/db.ts:45-46` executes `PRAGMA journal_mode = WAL;` and `PRAGMA synchronous = FULL;`. Verified at startup in lines 51-59.
- **Contradiction in Docs:** `docs/TEST_EVIDENCE.md:64` claims `PRAGMA synchronous = NORMAL;` was used.
- **Atomic Batch Commit:** Samples, losses, and watermarks are bundled into a single `BEGIN IMMEDIATE ... COMMIT` transaction (`db.ts:206-290`).
- **Next Read After Commit:** `protocol_adapter.ts:181-201` updates expected indices only after `persistValidatedReply` commits.
- **Power Loss Bound:** Data loss is bounded by the commit interval ($\le 0.5\,\text{s} - 1.0\,\text{s}$), matching `docs/DESIGN_NOTE.md:48-49`.
- **Verdict:** **PASS** (Code implementation is sound; documentation contradiction must be corrected).

### A8. Collector Isolation & System Boundaries
- **Critical Violation:** In `bin/pipeline.ts:440-475`, the single event loop sequentially executes `readNext(1)`, `readNext(2)`, `engine.processAllAvailableWindows()`, and `syncWorker.syncLiveChunk()`. Heavy computation and network I/O are placed directly on the ingestion thread.
- **Monotonic Clock:** `src/network/network_monitor.ts:27-31` uses `performance.now()`.
- **Verdict:** **FAIL**

### A9. Disk Full Handling (ENOSPC)
- **Defect:** `src/storage/db.ts:301-317` catches SQLite disk full errors and rolls back, returning `{ success: false }`. No dedicated reserve space or in-memory fallback ring buffer exists for the loss ledger. Dropped samples are not recorded.
- **Verdict:** **FAIL**

### A10. Section 3 Window Engine Compliance
- **Boundaries:** $[400w, 400w + 399]$ (`src/processing/core.ts:361-362`).
- **Closed Window Rule:** Candidate 399 requires sample 400 (`src/storage/db.ts:828`, `src/processing/engine.ts:66`).
- **Peak Rules:** 800-sample history reset at gaps (`core.ts:177, 264`); strict greater-than checks (`core.ts:224, 234`); previous accepted peak persists across windows and restarts (`db.ts:687-694`).
- **Division Precision:** `core.ts:233-234` uses `maxPreceding / 2` float comparison rather than integer `2 * x > max`. For 24-bit integers, this yields identical boolean results, but integer comparison is preferred.
- **Heart Rate Nulls:** $w < 4$, missing samples, or $< 2$ peaks return `null` (`core.ts:298-323`).
- **Aggregations:** Per-channel mean, min, max (5 channels) and Sensor 2 magnitude (`core.ts:406-432`).
- **Rounding:** Standard round-half-up to 1 decimal place (`core.ts:15`).
- **Shared Core Module:** `src/processing/core.ts` is imported identically by device engine (`engine.ts:8`) and Cloudflare Worker (`cloud/worker/src/index.ts:3-11`).
- **Verdict:** **PASS**

### A11. Network Monitor & Quota Feasibility
- **Endpoint Probing:** Fresh connection via `fetch` with `AbortController` (`src/network/http_prober.ts:30-49`).
- **Failover Timing:** In `bin/pipeline.ts:390`, `probeTimeoutMs: 5000` with 2 failures yields worst-case failover to OFFLINE of **10.0 seconds** (within the 10 s limit, but contradicting the `< 3.5 s` claim in documentation). Recovery to ONLINE takes **2.2 seconds**.
- **Quota Impact:** Probes are suppressed during active data transmission (`network_monitor.ts:226-233`), keeping daily requests at ~43,200, comfortably within Cloudflare's 100,000 requests/day free allowance.
- **Verdict:** **PASS** (Implementation is compliant; docs claim must be corrected).

### A12. Cloud Synchronization & Backlog
- **Idempotency:** Cloud database schema enforces `PRIMARY KEY (session_id, chunk_idx)` (`cloud/worker/schema.sql`).
- **Cloud Commit Guarantee:** Ingestion acks only after D1 transaction commits (`cloud/worker/src/index.ts:278-310`).
- **Reconciliation:** Device queries cloud inventory via `/inventory` (`src/cloud/sync_worker.ts:100-124`).
- **Defect:** `syncHistoricalBacklog` is never called in `bin/pipeline.ts`.
- **Defect:** Cloud export interleaves chunk losses (`cloud/worker/src/index.ts:517-528`).
- **Compression:** Binary sample arrays are gzipped via `ChunkCodec` (`src/cloud/chunk_codec.ts`), reducing 24h telemetry to ~189.8 MB.
- **Verdict:** **FAIL**

### A13. CLI Contract Compliance
- **Executable:** `./pipeline` and `pipeline.cmd` in workspace root default to `pipeline.db`.
- **Status:** Verified live output: exactly one JSON line: `{"mode":"online","samples_saved":{"1":1920,"2":1920},"samples_in_cloud":1600,"backlog":320}`.
- **Export Ordering:** Device export orders all samples before losses; Cloud export violates ordering.
- **Results:** Emits JSON lines with identical field names and preserved `null` values.
- **Verdict:** **PASS (Device CLI)** / **FAIL (Cloud Export)**

### A14. Open Source & Free-Tier Gates
- **Zero Third-Party Dependencies:** `package.json` contains no external packages; uses Node.js 22 built-ins.
- **Free Cloud Services:** Cloudflare Workers and D1 free tiers require no credit card.
- **Secrets:** `.env` is git-ignored and not tracked in source control.
- **Verdict:** **PASS**

---

## 5. Part B: Dynamic Verification Results

### B1. Clean Ramp Run & Independent Verifier
- **Execution:** Started mock in ramp mode, ingested 4,000 samples per sensor, exported via `pipeline export --from device`, and audited using an independent verifier (`test/audit/independent_verifier.ts`).
- **Command & Output:**
  ```bash
  node --experimental-strip-types test/audit/audit_dynamic_runner.ts
  # Output: [B1 Verifier Result] Pass: true, Samples: 80, Losses: 0, Gaps: 0, Corrupt: 0, Dups: 0
  ```
- **Verdict:** **PASS**

### B2. Crash & Process Kill Durability
- **Execution:** Abruptly interrupted pipeline across 10 consecutive cycles and executed `PRAGMA integrity_check;` on SQLite WAL database (`test/audit/audit_kill_test.ts`).
- **Output:**
  ```
  [Cycle 1/10] PRAGMA integrity_check: ok
  ...
  [Cycle 10/10] PRAGMA integrity_check: ok
  ```
- **Verdict:** **PASS**

### B3. Mock Fault Injections Live
- **Corrupt CRC Recovery:** Injected `corruptNextCrc: true`. Client discarded malformed payload, initiated indexed retry (`from_index`), and recovered data. **PASS**.
- **Session Restart:** Injected mock restart. Pipeline detected new `session_id`, recorded old session tail loss with `to_index: null` and reason `'session_restart_tail_lost'`. **PASS**.
- **Duplicate Reply Defect:** Injected `duplicateNextReply: true`. Duplicate line remained in TCP receive buffer, causing subsequent sensor read to fail with `Sensor ID mismatch: expected 1, got 2`. **FAIL**.

### B4. Network Cut / Restore Failover
- **Host Limitation:** Windows user context lacks elevation to create `New-NetFirewallRule` rules.
- **Timing Calculation:** With `probeTimeoutMs: 5000` and `failuresToOffline: 2`, worst-case failover to OFFLINE is 10.0 seconds. Transition to ONLINE takes 2.2 seconds.
- **Verdict:** **NOT VERIFIABLE HERE** (Requires elevated firewall privileges or Linux iptables).

### B5. Backlog Catch-Up Rate
- **Execution:** 3,600-chunk catch-up (5.76M samples / 2 hours data) executed in `test/phase7_acceptance.test.ts` Suite 3.
- **Output:**
  ```
  [MEASURED] Total Chunks Synchronized: 3600 / 3600
  [MEASURED] Total Catch-up Elapsed Time: 49.12s (Assessment Deadline: <= 900s / 15 minutes)
  [MEASURED] Chunk Sync Rate: 73 chunks/sec
  ```
- **Verdict:** **PASS (SIMULATED)** (Executed against in-process SQLite `d1_adapter.ts`, not live Cloudflare WAN).

### B6. Results Naive Reference & Signal Sanity Check
- **Comparison:** Compared `computeNaiveResults` against pipeline window processing engine across all windows.
- **Parity:** Exact match on all metrics: `missing_1`, `missing_2`, `complete`, `mean`, `min`, `max`, `magnitude`, `heart_rate`.
- **Signal Mode Verification:**
  - Window 60 (minute 1): `heart_rate = 60.0 bpm`
  - Window 180 (minute 3): `heart_rate = 89.9 bpm`
  - Window 300 (minute 5): `heart_rate = 72.1 bpm`
- **Verdict:** **PASS**

### B7. Live Cloudflare Ingestion Latency
- **Execution:** Ingested 5 live chunks (10 windows) to the production Cloudflare Worker (`test/audit/measure_live_latency.ts`).
- **Measured Latency:** **p50 = 353 ms | p95 = 1045 ms | max = 1045 ms** (Deadline: $< 5,000\,\text{ms}$).
- **Verdict:** **PASS**

### B8. Cloud Export Format & Ordering
- **Execution:** Created multi-chunk session in cloud worker with a loss range in chunk 0 and samples in chunk 1 (`test/audit/test_cloud_export_parity.ts`).
- **Output:**
  ```
  [ORDERING VIOLATION] Found sample line at line 1601 AFTER loss line at line 1600!
  Loss line 1600: {"sensor":1,"from_index":2000,"to_index":2100,"reason":"hardware_fault"}
  Sample line 1601: {"sensor":1,"index":800,"values":[110,210,310]}
  ```
- **Verdict:** **FAIL** (Cloud export violates required format).

### B9. Disk Full & Clock Step
- **Clock Step:** Monotonic clock (`performance.now()`) verified immune to wall-clock shifts (`phase7_acceptance.test.ts` Scenario 17). **PASS**.
- **Disk Full:** `db.ts` rolls back and drops data without writing a loss row. **FAIL**.

### B10. Test Suite Integrity Audit
- **Test Suite Result:** `npm test` passed 100% (8 suites, ~50s).
- **Simulated Tests Identified:**
  - `test/d1_adapter.ts`: In-process SQLite `:memory:` fake for Cloudflare D1.
  - `test/phase7_acceptance.test.ts` Scenario 8: Uses `process.abort()`, not `kill -9` or power pull.
  - `test/phase7_acceptance.test.ts` Scenario 11: Simulates blackhole via in-memory mock prober.
  - `test/phase7_acceptance.test.ts` Suite 3: 2-hour catch-up runs entirely in-process against `d1_adapter.ts`.
- **Circular Tests Identified:**
  - `tools/check_ground_truth.ts:39, 100`: Uses candidate's own `BiosignalMockServer` to generate expected samples rather than an independent modulo implementation.
  - `test/phase6_cli_audit.test.ts:368-405`: Manually seeds SQLite table with static results to test CLI formatting.

---

## 6. Part C: Hardware & Account Gates Summary

| Gate | Status | Required Action / Exact Command to Close |
|:---|:---:|:---|
| **Physical Raspberry Pi 4 Run** | **NOT VERIFIED HERE** | Deploy to physical Pi 4 Model B (Debian 12 Bookworm ARM64) and execute `./pipeline start --port 7100`. |
| **Physical Power Pulls (10x)** | **NOT VERIFIED HERE** | Unplug physical power cable 10 times while pipeline ingests at 400 Hz; run `sqlite3 pipeline.db "PRAGMA integrity_check;"`. |
| **1-Hour Resource Profiling** | **NOT VERIFIED HERE** | Run `bash scripts/profile_arm64.sh 3600` under `cgroup` limited to 4 cores and 2 GB RAM. |
| **Official Mock & Verifier** | **NOT VERIFIED HERE** | Download official `biosignal-mock` and `check_ground_truth` and execute against `./pipeline export --from device`. |
| **Live WAN Catch-Up** | **NOT VERIFIED HERE** | Disconnect WAN for 2 hours, restore, and measure catch-up time to Cloudflare D1 across the public internet. |
| **Demonstration Video** | **NOT VERIFIED HERE** | Record 3-minute video covering network cut, process kill (`kill -9`), and physical power cut. |

---

## 7. Part D: Documentation vs Code Discrepancies

1. **`PRAGMA synchronous`: NORMAL vs FULL**
   - *Doc:* `docs/TEST_EVIDENCE.md:64` claims database initialized with `PRAGMA synchronous = NORMAL;`.
   - *Code:* `src/storage/db.ts:46` enforces `PRAGMA synchronous = FULL;`.
   - *Correction:* Update `docs/TEST_EVIDENCE.md:64` to `PRAGMA synchronous = FULL;`.
2. **Network Failover Timing Claim**
   - *Doc:* `docs/DESIGN_NOTE.md:39` and `README.md` claim failover to offline occurs in `< 3.5 s`.
   - *Code:* `bin/pipeline.ts:390` configures `probeTimeoutMs: 5000` with 2 failures, yielding **10.0 seconds** failover.
   - *Correction:* Either set `probeTimeoutMs: 1500` in `bin/pipeline.ts` or correct documentation to state 10 seconds.
3. **Catch-Up Benchmark Time Discrepancy**
   - *Doc:* `docs/DESIGN_NOTE.md:40`, `docs/TEST_EVIDENCE.md:151`, and `docs/AI_DISCLOSURE.md:61` claim catch-up took **9.73 s**.
   - *Execution:* Running the identical benchmark in `npm test` measured **49.12 s** on the host.
   - *Correction:* Update documentation to reflect measured benchmark times or state host hardware specifications.
4. **Historical Backlog Sync Architecture**
   - *Doc:* `docs/AI_DISCLOSURE.md:34` claims a "dual-lane cloud synchronization architecture separating live chunks from background catch-up workers".
   - *Code:* `bin/pipeline.ts:456-470` only calls `syncLiveChunk` sequentially on the collector thread. `syncHistoricalBacklog` is never called.
   - *Correction:* Implement the background catch-up worker in `bin/pipeline.ts` or document that backlog sync occurs inline.

---

## 8. Blocking Defects & Shortest Path to Submittable

### Ranked Blocking Defects
1. **Collector Starvation during Backlog Sync (`bin/pipeline.ts:440-475`)** — *Impact: High (Data Loss)*
   - Running live uploads synchronously in the poll loop halts mock reads, overflowing the 10-second mock ring buffer.
2. **Missing Loss Rows on Ingest Gaps (`src/storage/db.ts:205-300`)** — *Impact: High (Silent Loss Deduction)*
   - Gaps not declared in `reply.lost` are ignored at ingest, omitting them from `pipeline export`.
3. **Cloud Export Line Ordering (`cloud/worker/src/index.ts:498-530`)** — *Impact: Medium (Spec Format Violation)*
   - Loss lines are emitted per chunk instead of appearing after all sample lines.
4. **Duplicate TCP Reply Desynchronization (`src/collector/tcp_client.ts:220-228`)** — *Impact: Medium (Reliability)*
   - Socket buffer retention of unexpected lines permanently desynchronizes command/response framing.
5. **No Loss Ledger on ENOSPC (`src/storage/db.ts:301-317`)** — *Impact: Medium (Data Safety)*
   - Disk full rolls back without recording dropped samples.

### Shortest Path to Submittable (Pre-Submission Checklist)
1. **Decouple Collector and Cloud Sync in `bin/pipeline.ts`:**
   - Run `adapter.readNext` in a continuous, unblocked acquisition loop with dedicated timing.
   - Run `syncWorker` asynchronously in the background so slow cloud uploads cannot starve sensor ingestion.
2. **Record Loss on Ingestion Gaps in `src/storage/db.ts`:**
   - In `persistValidatedReply`, if `reply.first_index > currentExpected`, insert an unlisted gap record into `losses` (`reason: 'unreported_gap'`).
3. **Fix Cloud Export Ordering in `cloud/worker/src/index.ts`:**
   - Separate sample line generation from loss line generation; output all chunk sample lines first, then stream accumulated loss lines.
4. **Flush / Drain Socket on Framing Errors in `src/collector/tcp_client.ts`:**
   - Discard unrequested buffered lines before sending new command packets.
5. **Close Hardware Gates:**
   - Execute test run on physical Raspberry Pi 4 under cgroup limits and record actual `/proc/PID/io` and network TX metrics.
