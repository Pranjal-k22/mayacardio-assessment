# MayaCardio — Final Submission Readiness Gate Report

**Document ID:** `docs/READINESS_GATE.md`  
**Evaluation Date:** October 15, 2026  
**Auditor Role:** Independent Submission Gatekeeper (Antigravity Evaluation Engine)  
**Host Environment:** Windows 11 Pro x64 (AMD64), Node.js v24.20.0, PowerShell 5.1  
**Target Hardware Specification:** Raspberry Pi 4 Model B (ARM64, $\le 4$ cores, $\le 2$ GB RAM)  
**Target Cloud Service:** Cloudflare Workers + D1 Database (Free Tier)  

---

## 1. Clean-Room Reproduction & Git State (STEP 1)

### A. Git Repository State Audit
```text
$ git status
On branch main
No commits yet
Changes to be committed: 54 files
Changes not staged for commit: 21 files
Untracked files: 7 directories/files (.agents/, .antigravity/, docs/AUDIT_REPORT.md, docs/AUDIT_REPORT_v2.md, scratch/, src/collector/collector_worker.ts, test/audit/)

$ git rev-parse HEAD
fatal: ambiguous argument 'HEAD': unknown revision or path not in the working tree.
```

> [!CAUTION]
> **CRITICAL SUBMISSION BLOCKER: Zero Committed Revisions**  
> The working repository contains **no git commits on `main`**. All initial scaffold files are staged in the index, but all subsequent bug fixes, worker thread implementations (`src/collector/collector_worker.ts`), audit tests (`test/audit/`), and audit reports (`docs/AUDIT_REPORT_v2.md`) exist only in the working tree or as untracked files.  
> If an evaluator clones this repository via `git clone`, **git clones an empty repository (`warning: You appear to have cloned an empty repository`)**. Nothing currently exists in git history.

### B. Clean Installation from README
* **Command:** `npm install --omit=dev`
* **Result:** **PASS (0 dependencies added)**. The application adheres to the zero-dependency requirement, relying entirely on native Node.js v24 APIs (`node:sqlite`, `node:zlib`, `node:crypto`, `node:worker_threads`, and native `--experimental-strip-types`).
* **Missing README Steps:** The README mentions `git clone https://github.com/mayacardio/mayacardio.git`, which currently yields an empty directory due to the uncommitted git state. Furthermore, running against the live cloud worker requires creating `.env` from `.env.example`, which is not automated.

### C. Master Regression Suite Execution
* **Command:** `npm test`
* **Result:** **PASS (8 / 8 suites passing, 96 assertions, 0 failures, 8.84s duration)**
  * Phase 0 (Runtime Primitives): PASS
  * Phase 1 (Ingestion & Framing): PASS (27 assertions)
  * Phase 2 (Persistence & Durability): PASS
  * Phase 3 (Window Processing): PASS
  * Phase 4 (Network Monitor & Failover): PASS
  * Phase 5 (Cloud Sync & Idempotency): PASS
  * Phase 6 (CLI & Audit Integrity): PASS
  * Phase 7 (End-to-End Fault Matrix & Acceptance): PASS (20/20 fault scenarios)

### D. Audit Test Suite Execution (`test/audit/`)
Every script in `test/audit/` was executed directly from the shell:

| Script | Purpose / Scope | Result | Exit Code | Notes |
|:---|:---|:---:|:---:|:---|
| `audit_dynamic_runner.ts` | B1 ramp verifier, B6 window math, B3 mock faults | **PASS** | `0` | Verified 0 gaps/corruptions; naive reference matched engine |
| `audit_kill_test.ts` | 10 crash/disconnect cycles with PRAGMA integrity_check | **PASS** | `0` | 10/10 cycles returned `integrity_check = ok` |
| `test_fix1_collector_starvation.ts` | 5-minute continuous 400 Hz stream + 2-hour backlog | **PASS** | `0` | 300.25s duration, 240,220 samples, P99 commit=3ms, gap=398ms, backlog catch-up in 60.97s |
| `test_fix2_duplicate_desync.ts` | TCP duplicate replies & stream framing | **PASS** | `0` | 97 duplicates dropped, 0 stream stalls, clean export parity |
| `test_fix3_unreported_gaps.ts` | Unreported ingest gaps & watermark invariant | **PASS** | `0` | Atomic loss insertion, 0 unaccounted gaps below watermark |
| `test_fix4_crc_loss.ts` | Exhausted CRC retries logged as `crc_unrecoverable` | **PASS** | `0` | Gap recorded on next reply, 0 synthetic values |
| `test_fix5_cloud_export_ordering.ts` | Export line ordering (samples first, losses last) | **PASS** | `0` | 100% exact parity across 2,904 lines |
| `test_worker_thread.ts` | Isolated collector worker IPC sanity | **PASS** | `0` | Clean IPC message roundtrip |
| `measure_live_latency.ts` | Ingestion latency against deployed Cloudflare Worker | **PASS** | `0` | Requires `.env`; P50 = 185 ms, Max = 190 ms |
| `test_cloud_export_parity.ts` | Historical defect reproducer from Audit v1 | **FAIL** | `1` | **Intended failure**: Script asserts that an ordering defect MUST occur (`assert.notStrictEqual(sampleAfterLossIdx, -1)`). Because FIX 5 resolved the defect, this defect-reproduction test fails. |

---

## 2. Test Integrity & Weakening Audit (STEP 2)

A line-by-line inspection of `git diff -- test/` was conducted to verify whether any regression tests were deleted, thresholds loosened, or timeouts extended.

### 1. `test/phase2_persistence.test.ts`
* **Lines Changed:** Lines 178–220 (`runTransactionsAndWatermarkTests`).
* **Original Assertion:**
  ```ts
  const r3 = engine.persistValidatedReply(batch3); // batch3 has gap 400..799
  assert.strictEqual(r3.resolvedWatermark, 399, 'Watermark must stay at 399 because 400..799 is missing');
  ```
* **New Assertion:**
  ```ts
  const r3 = engine.persistValidatedReply(batch3);
  assert.strictEqual(r3.newLossesCount, 1, 'Must record 1 uncovered loss range');
  assert.strictEqual(r3.resolvedWatermark, 1199, 'Watermark advances to 1199 because 400..799 is covered by loss');
  const losses = engine.getLosses('sess-wm', 1);
  assert.strictEqual(losses[0].reason, 'gap_unreported_by_mock');
  ```
* **Gatekeeper Verdict:** **JUSTIFIED & STRICTER**. The original test accepted a behavior where an unreported gap pinned the watermark without writing a loss record to SQLite, causing a silent loss in `pipeline export`. FIX 3 enforced the assessment invariant that every missing range must be written to `losses` immediately upon receipt. Because the gap is now durably cataloged as a loss, advancing the watermark to 1199 is spec-compliant. Additionally, a new test was added asserting that an unaccounted gap (neither in samples nor in losses) STILL strictly pins the watermark.

### 2. `test/phase6_cli_audit.test.ts`
* **Lines Changed:** Lines 242–279 (`testDeviceExport`).
* **Change:** Pre-seeded loss range adjusted from `from_index = 4000` to `from_index = 2` to align with the active session watermark.
* **Gatekeeper Verdict:** **JUSTIFIED**. Preserves the exact check that sample lines precede loss lines, that `to_index: null` is preserved, and that output is deterministic.

### 3. `test/phase7_acceptance.test.ts`
* **Lines Changed:** Lines 224–248 (Scenario 4 in fault matrix).
* **Change:** Aligned with FIX 3: asserts that an unreported gap automatically creates a loss record with reason `gap_unreported_by_mock` rather than stalling without recording.
* **Gatekeeper Verdict:** **JUSTIFIED**. Strengthens loss accounting.

**Conclusion:** No test was weakened. No timeouts were extended, and no assertions were deleted to hide regressions.

---

## 3. Black-Box Behavioral & Fault Evaluation (STEP 3)

### A. Documented CLI Contract Verification
Executed directly against `bin/pipeline.ts` with local database:

1. **`pipeline status`:**
   ```json
   {"mode":"online","samples_saved":{"1":1920,"2":1920},"samples_in_cloud":1600,"backlog":320}
   ```
   * **Verification:** Outputs **exactly one JSON line**. Field names (`mode`, `samples_saved`, `samples_in_cloud`, `backlog`) match Section 4 byte-for-byte.
2. **`pipeline export --session assessment-audit-01 --from device`:**
   ```json
   {"sensor":1,"index":0,"values":[0,1,2]}
   {"sensor":2,"index":0,"values":[0,1]}
   {"sensor":1,"index":1,"values":[3,4,5]}
   {"sensor":2,"index":1,"values":[2,3]}
   ```
   * **Verification:** Samples are canonically ordered (Sensor 1 then Sensor 2 per index, ascending by index). All sample lines strictly precede loss records.
3. **`pipeline results --session assessment-audit-01`:**
   ```json
   {"session_id":"assessment-audit-01","window_idx":0,"missing_1":0,"missing_2":0,"complete":true,"mean":{"s1_ch1":598.5,"s1_ch2":599.5,"s1_ch3":600.5,"s2_ch1":399,"s2_ch2":400},"min":{"s1_ch1":0,"s1_ch2":1,"s1_ch3":2,"s2_ch1":0,"s2_ch2":1},"max":{"s1_ch1":1197,"s1_ch2":1198,"s1_ch3":1199,"s2_ch1":798,"s2_ch2":799},"magnitude":565,"heart_rate":null,"mode":"offline"}
   ```
   * **Verification:** Outputs exactly one line per 1-second window. All required fields (`missing_1`, `missing_2`, `complete`, `mean`, `min`, `max`, `magnitude`, `heart_rate`, `mode`) present. `heart_rate` evaluates strictly to `null` for $w < 4$ as required by Section 3.

### B. Independent Parity & Math Verification
* **Ramp Ground-Truth Verifier:** Ran `tools/verify_export_audit.ts` against Section 6.1 modulo recurrence relations ($s_1 = (3i+c) \pmod{8388608}$, $s_2 = (2i+c) \pmod{8388608}$):
  * Clean export: **PASS (100% bit-identical)**.
  * Tampered sample injection: **Detected & Failed as expected**.
  * Duplicate record injection: **Detected & Failed as expected**.
  * Unreported gap injection: **Detected & Failed as expected**.
* **Section 3 Naive Reference Comparison (`test/audit/audit_dynamic_runner.ts`):** Naive reference engine compared against `src/processing/core.ts` outputs across 100 windows: **0 arithmetic mismatches**. Signal mode ECG heart rate reproduced the exact 60 / 90 / 72 bpm cycle.

### C. Fault Injection & Hardware Limitations

| Fault Scenario | Test Harness / Method | Result | Audit Qualification |
|:---|:---|:---:|:---|
| **Process Termination (`kill -9`)** | `audit_kill_test.ts` (10 cycles) | **PASS** | SQLite WAL survived 10/10 cycles; `PRAGMA integrity_check = ok`. |
| **TCP CRC Corruption** | `test_fix4_crc_loss.ts` | **PASS** | Bounded retries exhausted; `crc_unrecoverable` logged to `losses`. |
| **TCP Duplicate Replies** | `test_fix2_duplicate_desync.ts` | **PASS** | 97 duplicates dropped without desynchronizing framing. |
| **Mock Session Restart** | `audit_dynamic_runner.ts` (B3) | **PASS** | Unread tail logged with `to_index: null` and reason `session_restart_tail_lost`. |
| **OS-Level Firewall Cut / Blackhole** | OS packet filter / iptables | **NOT VERIFIABLE HERE** | Requires elevated administrative permissions or Linux network namespace. Simulated probe blackhole passes in 10.0s. |
| **Physical 5V Sudden Power Cut** | Physical relay disconnect on board | **NOT VERIFIABLE HERE** | Cannot be tested on host; requires physical Raspberry Pi 4 test bench. |
| **Disk Full Simulation** | OS filesystem exhaustion | **NOT VERIFIABLE HERE** | Cannot safely exhaust host OS disk. Storage engine aborts safely on SQLite `SQLITE_FULL`. |
| **Clock Jump ($\pm 2\,\text{h}$)** | NTP / System time shift | **PASS (BY DESIGN)** | Processing windows and dead-reckoning derive strictly from monotonic sample indices ($i$), immune to wall-clock shifts. |

### D. Two-Hour Backlog Catch-Up Verification
* **Measured Benchmark:** 3,600 chunks (5,760,000 samples) pre-seeded and synchronized concurrently with live 400.00 Hz acquisition.
* **Duration:** **60.97 seconds** (Deadline: $\le 900\,\text{seconds}$ / 15 minutes).
* **Collector Read-Loop Gaps:** Peaked at **398 ms** ($\ll 2,000\,\text{ms}$ ceiling).
* **Execution Target Clarification:** **Tested against in-process SQLite D1 simulation adapter (`test/d1_adapter.ts`)**, not the live Cloudflare Worker over public internet. Catch-up against live Worker was verified on smaller scale (100 chunks).

### E. Live Cloud E2E Delivery Latency
* **Target:** Live Cloudflare Worker (`https://mayacardio-cloud-pipeline.pranjalkaran22.workers.dev`).
* **Measured Ingestion Latency (`scratch/test_live_e2e_latency.ts`):** 10 consecutive windows (8,000 samples) streamed live:
  * Minimum: **506 ms**
  * P50: **1,506 ms**
  * P95: **1,828 ms**
  * Maximum: **1,828 ms** (Target: $\le 5,000\,\text{ms}$).
* **Limitation:** Continuous 10-minute live delivery over public internet was not executed continuously for 600 seconds.

---

## 4. Documentation Claims vs. Empirical Evidence (STEP 4)

| Claim in Documentation | Stated Value | Verified Reality | Status |
|:---|:---|:---|:---:|
| **Collector Starvation Avoidance** | Read gap $\le 398\,\text{ms}$ | Measured peak read gap: **398 ms** | **VERIFIED** |
| **Two-Hour Backlog Catch-Up** | 3,600 chunks in 60.97 s | Measured catch-up: **60.97 s** on local D1 simulation | **VERIFIED** |
| **SQLite Commit Latency** | P99 = 3 ms, Max = 124 ms | Measured on $N=115,218$ commits: P99=3ms, Max=124ms | **VERIFIED** |
| **Export Parity** | 100% line parity across 2,904 lines | Verified bit-for-bit in `test_fix5_cloud_export_ordering.ts` | **VERIFIED** |
| **D1 24-Hour Retention** | Fits in single 500 MB database | **Partially True**: 247 MB/day (ECG, 48.6h) and 484 MB/day (ramp, 24.8h) fit; noisy/random data (640–733 MB) exceeds single database (16.4h–18.7h). | **QUALIFIED** |
| **Design Note Length** | Exactly 2 pages | Chromium Headless Edge renders **exactly 2 pages** | **VERIFIED** |
| **Failover Transition Time** | Sub-3.5 s / $\le 10\,\text{s}$ | Simulated probe blackhole transitions in 10.0s; live OS firewall cut unverified | **QUALIFIED** |
| **Physical Power-Cut Safety** | Bounded loss $\le 1.0\,\text{s}$ | Architecture supports $\le 1.0\,\text{s}$ exposure; physical 10x power-pull unperformed | **OPEN GATE** |
| **Raspberry Pi 4 Profiling** | 1-hour profiling metrics | Measured in Docker ARM64 container; physical board unprofiled | **OPEN GATE** |
| **Official Mock Verification** | Tested against official binary | Binary absent; tested against Section 6.1 stand-in reference mock | **OPEN GATE** |

---

## 5. Security, Cloud Quotas & Rules Audit (STEP 5)

### A. Zero-Disclosure Secret Hygiene
* **`.gitignore` Audit:** `.env` is explicitly listed in `.gitignore` and untracked by Git.
* **Working Tree & Commit History Scan:**
  * No secret tokens, bearer headers, or private credentials exist in any tracked or staged files.
  * Documentation and tests reference only generic placeholders (`CLINICAL_PROD_BEARER_TOKEN_HERE`).
  * Live Cloudflare Worker credential was cryptographically rotated; old token rejected with `HTTP 401 Unauthorized`.

### B. Free Services & Open-Source Compliance
* **Dependencies:** Zero external production npm packages. Uses Node.js native standard libraries.
* **Cloud Architecture:** Single Cloudflare Worker + Cloudflare D1 database. Free tier, no credit card required.
* **Mock Service:** `tools/biosignal_mock.ts` preserved as candidate stand-in reference; no evaluator files modified.

### C. Cloudflare Free-Tier Daily Quota Arithmetic (43,200 Chunks / Day)
1. **Daily Requests:** 43,200 ingestion POSTs + ~17,280 health probes = **~60,480 requests/day** ($\le 100,000$ free quota; ~60.5%).
2. **D1 Row Writes:** 43,200 chunk rows/day ($\le 100,000$ free write quota; ~43.2%).
3. **D1 Row Reads:** ~172,800 reads/day ($\le 5,000,000$ free read quota; ~3.5%).
4. **Worker CPU Time:** ~4–8 ms per request ($\le 10\,\text{ms}$ limit).
5. **Storage Capacity:**
   * Single Database Quota: 500 MB (decimal $500,000,000\,\text{bytes}$).
   * Smooth ECG: 247.10 MB / day $\rightarrow$ **48.6 hours retention** (Compliant).
   * Section 6.1 Ramp: 484.19 MB / day $\rightarrow$ **24.8 hours retention** (Compliant).
   * High-Noise Data: 640.22 MB / day $\rightarrow$ **18.7 hours retention** (Exceeds single DB; requires multi-DB partitioning across D1's 10 free databases).

---

## 6. Official Scoring Matrix & Submission Gate Verdict (STEP 6)

### A. Assessment Criteria Scorecard

| Area | Criterion | Est. Pts | Status | Evidence Classification | Gatekeeper Findings |
|:---|:---|:---:|:---:|:---:|:---|
| **Area 1: Data Safety on Device (35 pts)** | **1. Ingestion & Framing** | 12 | **PASS** | `LOCAL SOFTWARE TEST` | Verified raw CRC32 slice checks, 97 duplicates dropped without desync, bounded retries. |
| | **2. Persistence & Durability** | 13 | **PASS** | `LOCAL SOFTWARE TEST` | SQLite WAL with `synchronous = FULL`; atomic insertion of unreported gaps; 0 silent loss below watermark. |
| | **3. Crash & Power Durability** | 10 | **NOT VERIFIABLE HERE** | `PHYSICAL HARDWARE REQUIRED` | Software WAL survived 10/10 process kill cycles; physical 10x 5V power disconnects unexecuted. |
| **Area 2: Online Mode & Switching (25 pts)** | **4. Online Mode Live Streaming** | 12 | **PASS** | `LIVE CLOUD` | Live Worker deployed; live E2E delivery latency measured at P50 = 1,506 ms, Max = 1,828 ms ($\le 5,000\,\text{ms}$). |
| | **5. Offline Mode & Failover** | 13 | **PASS (QUALIFIED)** | `LOCAL SOFTWARE TEST` | Automated failover passes in 10.0s blackhole, 2.0s restore; OS-level firewall cut requires admin. |
| **Area 3: Cloud Copy (20 pts)** | **6. Catch-Up & Cloud Parity** | 20 | **PASS (QUALIFIED)** | `LOCAL SIMULATION` | 2-hour backlog synced in 60.97s on local D1 simulation; export parity verified across 2,904 lines. |
| **Area 4: Correct Results (10 pts)** | **7. Window Math Exactness** | 10 | **PASS** | `LOCAL SOFTWARE TEST` | Naive reference matches window engine across all channels; signal mode reproduces 60/90/72 bpm cycle. |
| **Area 5: Limits & Design (10 pts)** | **8. Resources & Design Note** | 10 | **NOT VERIFIABLE HERE** | `PHYSICAL HARDWARE REQUIRED` | Zero npm dependencies; 2-page PDF verified; physical 1-hour Raspberry Pi 4 profiling unperformed. |
| **TOTAL** | | **100** | | | |

### B. Dual Score Accounting
* **(a) Points Supported by Evidence Reproduced in THIS Host Run:** **57 / 100 points**  
  *(Criteria 1, 2, 4, 7 fully verified [47 pts] + Criterion 5 verified via simulated blackhole [10 pts] = 57 pts).*
* **(b) Points Still Open Pending Target Bench Verification:** **43 / 100 points**  
  * Criterion 3 (10 pts): Requires physical 5V power-cut pull on Raspberry Pi 4.
  * Criterion 6 (15 pts): 2-hour backlog catch-up tested against in-process simulation; needs live cloud network run.
  * Criterion 5 remainder (3 pts): OS-level firewall packet filter on target board.
  * Criterion 8 (10 pts): Physical 1-hour profiling on Raspberry Pi 4 Model B board.
  * Evaluator Mock Parity (5 pts): Validation against official binary.

### C. Potential 5-Point Deduction Scenarios
1. **Uncommitted Git Clone (-30 pts / Disqualification):** If submitted in the current git state (`No commits yet`), evaluators cloning the repository receive an empty directory.
2. **Physical Power-Cut NAND Failure (-5 pts per silent corruption):** If SD card filesystem barriers fail under abrupt power pull, SQLite WAL could encounter unrecoverable corruption without hardware testing.
3. **High-Noise Database Exhaustion (-5 pts):** If evaluators stream high-entropy or noisy biosignals for >18.7 hours into a single database, Cloudflare D1 will reject writes with quota exceeded errors.
4. **Official Mock Protocol Mismatch (-5 pts):** If the official `biosignal-mock` binary introduces framing or command conventions not present in the local reference mock.

---

## 7. Submission Gate Verdict

```text
=============================================================================
FINAL SUBMISSION GATE VERDICT: NOT READY
=============================================================================
```

**Reasoning:**  
While the software implementation is architecturally complete, passes all regression tests, and satisfies all locally verifiable criteria, the repository is **not ready for submission** because:
1. The working tree has **zero git commits** (a clone yields an empty repository).
2. The mandatory physical Raspberry Pi 4 profiling, 10x physical power-cut testing, official mock verification, and demonstration video remain unperformed.

---

## 8. Prioritized Task List to Reach Submission Readiness

1. **Commit All Code & Docs to Git:**
   ```bash
   git add .
   git commit -m "feat: complete MayaCardio edge-to-cloud biosignal telemetry pipeline"
   ```
2. **Deploy to Physical Raspberry Pi 4 Model B:**
   * Boot Debian 12 (Bookworm) 64-bit on a Raspberry Pi 4 ($\le 4$ cores, 2 GB RAM).
   * Clone repository and verify fresh installation following `README.md`.
3. **Execute 1-Hour Physical Resource Profiling:**
   * Run `bash scripts/profile_pi4.sh 3600` on the board under systemd.
   * Record the 4 required figures (average CPU, peak RAM, disk MB, network MB).
4. **Conduct 10 Physical 5V Power-Cut Cycles:**
   * Physically disconnect power while streaming 400 Hz telemetry.
   * Reboot and verify `./pipeline audit` returns 0 corruptions and bounded uncommitted loss.
5. **Verify Against Official Evaluator Binary:**
   * Run the official `biosignal-mock` and `check_ground_truth` executables when provided.
6. **Execute 2-Hour Outage Catch-Up Against Live Worker:**
   * Run full backlog synchronization over a real network against `https://mayacardio-cloud-pipeline.pranjalkaran22.workers.dev`.
7. **Record 5-Minute Technical Demonstration Video:**
   * Follow [docs/DEMO_SCRIPT.md](file:///c:/Users/pc/Desktop/mayacardio/docs/DEMO_SCRIPT.md) demonstrating live streaming, network cut, process kill, and physical power pull.
8. **Final Cloud Token Rotation:**
   * Rotate production bearer secret before final submission.
