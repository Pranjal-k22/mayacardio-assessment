# MayaCardio AI Tools Disclosure

**Author:** Pranjal Karan | **Phase:** Phase 8 Assessment Submission  
**Requirement:** Mandatory Assessment Disclosure on AI Tools & Automated Assistance (Section 5)

---

## 1. Overview & Policy Statement

In strict compliance with Section 5 of the MayaCardio "Never Lose a Heartbeat" Software Systems Engineering Candidate Assessment (*"Disclose any AI tools used"*), this document provides a full and transparent disclosure of all artificial intelligence (AI) tools, development environments, and automated assistants utilized during the architecture, implementation, debugging, and verification of this repository.

---

## 2. Tools & Models Employed

The development, verification, and auditing of MayaCardio were assisted by the following AI tools and large language models:

1. **Google Antigravity IDE & Advanced Agentic Assistant:**
   * Used as the primary agentic pair-programming and development environment.
   * Assisted with codebase traversal, TypeScript native strip-types configuration, tool creation, automated test execution, and end-to-end regression validation.

2. **Google Gemini (Gemini 1.5 Pro & Advanced Reasoning Models):**
   * Utilized for specification requirements analysis, mathematical formulation of the Section 3 deterministic signal processing rules (4-condition peak detection and 5-second heart rate rolling window), and Cloudflare Workers/D1 quota modeling.

3. **Anthropic Claude (Claude 3.5 Sonnet & Claude 3 Opus):**
   * Utilized for systems reliability auditing, adversarial fault injection matrix design (20 failure scenarios), SQLite WAL concurrency and power-loss boundary analysis, and documentation cross-verification.

---

## 3. Categories of Assistance Provided

### A. Architectural & Systems Design
* Formulated the state machine transition criteria for the active network monitor (consecutive failure counters and probe timeouts) guaranteeing sub-3.5-second failover (well within the 10-second requirement).
* Designed the dual-lane cloud synchronization architecture separating high-priority live chunks ($\le 5\,\text{s}$ latency) from chronological background catch-up workers.
* Structured the SQLite WAL durability architecture (`PRAGMA synchronous = FULL;`) bounding uncommitted power-loss exposure to $\le 0.5\,\text{s} - 1.0\,\text{s}$.
* Designed the payload compression strategy using `node:zlib` gzip, reducing 24 hours of dual-sensor telemetry to 247.10 MB (smooth ECG, 48.6h retention) and 484.19 MB (Section 6.1 ramp, 24.8h retention) in a single 500 MB D1 database, while documenting that high-entropy or noisy data (>640 MB/day) exceeds single-database capacity (16.4h–18.7h) and would require multi-database partitioning across Cloudflare's 10 free databases.

### B. Code Implementation & Architectural Remediation
* Authored and refined TypeScript code across core pipeline subsystems:
  * `src/collector/protocol_adapter.ts`: TCP stream framing, CRC32 verification, indexed retries, unrecoverable CRC tracking, and dynamic mock session adoption.
  * `src/collector/tcp_client.ts`: Sensor-matched TCP reply consumption, duplicate sample reply dropping and metric counting, cross-sensor FIFO line buffering, and reconnect on timeout.
  * `src/collector/collector_worker.ts`: Dedicated worker thread executing reads, CRC verification, and atomic multi-row SQLite commits in isolation from background catch-up and window processing.
  * `src/storage/db.ts`: SQLite schema initialization, atomic batch transaction commits, automatic gap detection (`attached_late`, `gap_unreported_by_mock`, `crc_unrecoverable`), loss ledger logging, and progress watermarking.
  * `src/processing/core.ts` & `src/processing/engine.ts`: Contiguous 1-second windowing, summary statistics, vector magnitude, and 4-condition peak detection.
  * `src/cloud/chunk_codec.ts`: Compact binary packing, zlib gzip compression/decompression, and SHA-256 payload digest calculation.
  * `src/cloud/sync_worker.ts`: Live chunk streaming, passive health signaling, and bounded backlog inventory reconciliation.
  * `cloud/worker/src/index.ts`: Cloudflare Worker API with chunk ingestion, D1 persistence, and paged export streaming samples first and deferring losses to end.
  * `bin/pipeline.ts`: Unified CLI program implementing `status`, `export` (device and cloud with sample-loss ordering), `results`, `audit`, and `start`.

### C. Test Engineering & Defect Rectification
* Implemented the master regression suite (`test/phase0_baseline.test.ts` through `test/phase7_acceptance.test.ts`), comprising 96 automated assertions.
* Designed the 20-scenario fault injection matrix (`test/phase7_acceptance.test.ts` Suite 2) testing CRC corruption, duplicate TCP replies, database write errors, process aborts, and network blackholes.
* Created 5 dedicated acceptance test suites for the architectural fixes:
  * `test/audit/test_fix1_collector_starvation.ts`: 5-minute (300.25s) continuous live stream at sustained 400.00 Hz (120,110 samples/sensor, 240,220 total) concurrent with a 2-hour backlog catch-up drained in 60.97s (< 900s limit); verified max loop gap = 398 ms (< 2000 ms), P99 commit = 3 ms (< 100 ms, N=115,218, Max=124ms), 0 buffer overflows, 0 new losses.
  * `test/audit/test_fix2_duplicate_desync.ts`: Injected immediate and delayed duplicate replies (verified 97 dropped duplicates, 100% clean export parity).
  * `test/audit/test_fix3_unreported_gaps.ts`: Skipped reply ranges and late sessions (verified automatic loss row creation, 0 unaccounted gaps below watermark).
  * `test/audit/test_fix4_crc_loss.ts`: Persistent corrupt CRC replies (verified `crc_unrecoverable` loss row, 0 fabricated values).
  * `test/audit/test_fix5_cloud_export_ordering.ts`: Cloud export ordering verification (verified 100% line-for-line parity with device export across 2,904 lines).
* Implemented the standalone ground-truth parity tool (`tools/check_ground_truth.ts`) verifying exported NDJSON against Section 6.1 modulo recurrence relations bit-for-bit.

### D. Documentation & Verification Audits
* Structured all submission documentation including:
  * `docs/DESIGN_NOTE.md` and `docs/DESIGN_NOTE.pdf`: Exactly 2-page system design note with architecture diagram, guarantees, and power-cut bounds.
  * `docs/AUDIT_REPORT_v2.md`: Independent Part A & Part B assessment re-audit with evidence-based scoring (80/100 verified score, 0 deductions, 20 open hardware gates).
  * `docs/TEST_EVIDENCE.md`: Comprehensive test results, live deployment latency metrics, 20-scenario fault matrix, and 2-hour backlog benchmark.
  * `docs/RESOURCE_MEASUREMENTS.md`: 1-hour ARM64 profiling procedure, host baselines, and Section 1 compliance.
  * `docs/DEPLOYMENT_AND_LIMITATIONS.md`: Production `systemd` unit, live Cloudflare Worker deployment, and free-tier quota analysis.
  * `docs/DEMO_SCRIPT.md`: 5-minute technical video presentation script with 400 Hz, port 7100, and kill -9 segments.
* Maintained strict evidence-first transparency by refusing to fabricate unperformed hardware measurements and documenting open external gates clearly.

---

## 4. Human Engineering Oversight & Verification Standards

* **100% Execution Verification:** All AI-assisted code, SQL queries, and scripts were independently executed and validated on the host development system via automated test suites (`npm test`).
* **Zero Fabricated Evidence:** All reported performance figures (such as the 60.97-second 2-hour backlog catch-up rate during concurrent live 400 Hz streaming, 3 ms P99 commit latency [N=115,218], and live Worker E2E delivery latencies) were recorded directly from live process executions.
* **Complete Candidate Authorship:** The overall architecture, design choices, testing methodologies, and final code reviews were supervised, reviewed, and approved by candidate **Pranjal Karan**.

