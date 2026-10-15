# MayaCardio System Design Note: High-Reliability Biosignal Telemetry

**Author:** Pranjal Karan | **Scope:** Edge Ingestion, Local Durability & Cloud Sync | **Constraint:** Max 2 Pages

---

### 1. Architecture & System Overview
MayaCardio implements an unattended bedside biosignal telemetry pipeline on a Raspberry Pi-class board (ARM64, $\le 4$ cores, 2 GB RAM) and free cloud services (Cloudflare Workers + D1). The system continuously ingests two biosensors at 400 samples/sec (Sensor 1: 3 channels, Sensor 2: 2 channels), persists all data crash-safely, computes authoritative 1-second window analytics, and synchronizes backlogs idempotently without data loss.

*Core Engineering Decisions:*
* **Language & Runtime:** TypeScript on Node.js v22+ LTS with native `node:sqlite` WAL mode for zero external runtime dependencies and deterministic execution.
* **Collector Isolation (FIX 1):** Ingestion runs in an isolated thread/worker owning write connections, decoupled from backlog upload and window processing, keeping read gaps $\le 398\,\text{ms}$ ($\ll 2000\,\text{ms}$ ceiling) during 5-minute continuous streaming at sustained 400 Hz.
* **Storage Engine:** SQLite 3 with `PRAGMA journal_mode = WAL;` and `PRAGMA synchronous = FULL;`. Every committed transaction forces an explicit disk flush.
* **Honest Loss Accounting (FIX 3 & 4):** Ingestion gaps (`attached_late`, `gap_unreported_by_mock`, `crc_unrecoverable`) are atomically recorded to SQLite; zero silent holes exist below high watermark.
* **Idempotent Synchronization (FIX 2 & 5):** 2-second canonical chunks keyed by `(session_id, chunk_index)` and verified via SHA-256 digests ensure at-least-once uploads never duplicate records. Cloud export streams samples first, losses last, matching device export 100%.

```mermaid
graph LR
    subgraph Edge Node [Raspberry Pi 4 / ARM64 Board]
        TCP[TCP Mock: 7100] -->|NDJSON + CRC32| RX[Isolated Collector]
        RX -->|Atomic Batch Commit| DB[(SQLite WAL Database)]
        DB --> W1[1s Window Engine]
        DB --> SYNC[Decoupled Sync Worker]
    end
    subgraph Cloud [Cloudflare Workers & D1]
        SYNC -->|POST /api/ingest Gzip| WRK[Worker Engine]
        WRK -->|Idempotent Upsert| D1[(Cloudflare D1 Database)]
    end
    W1 --> SYNC
```

---

### 2. What is Guaranteed and What is Not
* **What is Guaranteed:**
  * **Zero Silent Loss:** Every sample accepted over TCP is committed to SQLite WAL before watermark progression. Unreported gaps and exhausted CRC retries are logged to `losses` with auditable reasons.
  * **Crash Consistency (Software):** Committed data survives abrupt process crashes (`SIGKILL`, `process.abort()`) with zero SQLite database corruption under WAL mode.
  * **Deterministic Mathematical Parity:** Window results computed locally in offline mode match cloud results bit-for-bit (Section 3 per-channel stats, sensor 2 magnitude, and 4-condition peak detection).
  * **Sub-10s Failover:** Network loss triggers transition to offline within 10.0s (2 failed probes $\times$ 5.0s); network return restores online mode within 2.0s without data loss or duplication.
  * **Rapid Catch-Up:** A 2-hour offline backlog (3,600 chunks, 5.76M samples) synchronizes completely in **60.97 seconds** concurrent with 400 Hz real-time acquisition (well within 15-minute deadline).
  * **Export Parity:** `pipeline export --from cloud` and `--from device` achieve 100% exact line-for-line parity with all sample lines strictly preceding loss lines.
* **What is Not Guaranteed:**
  * Telemetry lost inside the sensor mock before transmission (e.g. mock buffer overflow) cannot be fabricated; it is cataloged in `losses`.
  * Physical power-cut durability depends on OS write flushes and storage hardware; this requires target bench testing and is not proven by software configuration alone.
  * Physical Pi 4 hardware profiling, physical power pulls, and official mock evaluation remain open external validation gates.

---

### 3. Maximum Data Loss on a Power Cut & Crash Recovery
* **Expected Uncommitted Exposure:** Data loss on an abrupt process termination is bounded by the **uncommitted batch commit interval ($\le 0.5\,\text{s}$ to $1.0\,\text{s}$)**.
* **Durability Architecture & Verification Limits:** Ingestion batches incoming samples within atomic `BEGIN IMMEDIATE ... COMMIT` transactions under `PRAGMA synchronous = FULL;`. Process termination crash testing (`process.abort()` and `SIGKILL`) verifies that committed SQLite WAL data remains intact with zero database corruption. However, physical power-loss survival depends on kernel barrier flushes and hardware storage write-buffer durability; physical 10x power-pull testing remains an unverified hardware gate until conducted on the physical Raspberry Pi 4 bench.
* **Simultaneous Board Power Loss:** When both the mock and pipeline lose power simultaneously on the bedside board:
  * On reboot, the mock begins a new session with `session_id` starting at index 0.
  * The pipeline dynamically adopts the new session ID and, as mandated by Section 6.3, logs the unread tail of the interrupted session with `to_index: null` and reason `session_restart_tail_lost`.

---

### 4. Ingestion, Framing & Sample Integrity
* **Framing, Deduplication & Checksums:** Packets arrive as NDJSON over TCP port 7100. The protocol adapter extracts the exact byte slice of `data` and verifies it against `crc32`. Corrupted packets trigger bounded indexed retries; exhausted retries persist pending reason `crc_unrecoverable`. Duplicate replies are deduplicated and dropped without desynchronizing TCP framing.
* **Sample Indexing:** Channels are signed 24-bit integers (`[-8,388,608, 8,388,607]`). Samples are addressed by monotonic sequence indices $i \ge 0$, establishing absolute temporal identity independent of system clock drift.

---

### 5. Deterministic Processing & Cloud Synchronization
* **Window Engine (Section 3):** Contiguous 1-second windows ($400w$ to $400w + 399$). Each window computes per-channel mean, min, max, sensor 2 magnitude (mean of $\sqrt{\text{ch1}^2 + \text{ch2}^2}$), and heart rate via 4-condition peak detection (800-sample history, larger than immediate neighbours, $> 0.5 \times$ max lookback, $\ge 100$ samples gap). Heart rate evaluates strictly to `null` if $w < 4$, if samples are missing, or if fewer than 2 peaks exist.
* **Passive Health Signalling:** Successful live chunk uploads act as positive health signals, suppressing redundant `GET /health` probes and saving 43,200 Worker requests/day.
* **Idempotent Ingestion:** Cloud endpoints enforce `UNIQUE(session_id, chunk_index)`. Re-transmitting an identical chunk returns `200 OK` (deduplicated, 0 row writes); conflicting payloads return `409 Conflict`.

---

### 6. Subsystem Verification & Constraints Summary

| Subsystem / Requirement | Status & Implementation Reality | Trade-Off / Technical Verification |
|:---|:---|:---|
| **Cloudflare D1 Storage** | **PASS (247-484 MB / 24h)** | Gzip reduces 24h volume to 247.10 MB (ECG, 48.6h) and 484.19 MB (ramp, 24.8h) in 500 MB D1; noisy data (>640 MB) requires multi-DB partitioning. |
| **2-Hour Backlog Catch-Up** | **PASS (60.97s Concurrent)** | Synchronized 3,600 chunks (5,760,000 samples) in 60.97s concurrent with 400.00 Hz live streaming vs 900s deadline. |
| **Crash & Durability** | **PASS (WAL + sync=FULL)** | Process aborts verified with zero corruption; uncommitted exposure $\le 1.0\,\text{s}$; physical power-pull gate open. |
| **Collector Starvation Avoidance** | **PASS (Max gap 398 ms)** | Dedicated collector worker; P99 commit = 3 ms (N=115,218, Max=124ms) across 300.25s continuous 400 Hz test with 2-hour catch-up. |
| **Export Order & Parity** | **PASS (100% Bit-for-bit)** | Cloud export buffers losses and streams samples first; line-for-line parity verified across 2,904 lines. |
| **Section 6.1 Math Parity** | **PASS (100% Bit-for-bit)** | Standalone `tools/check_ground_truth.ts` verifies device/cloud NDJSON exports against Section 6.1 modulo recurrence. |

