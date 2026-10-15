# MayaCardio Deployment Guide & Systems Limitations Audit

**Author:** Pranjal Karan | **Phase:** Phase 8  
**Verification Date:** October 2026 (Updated Post-Fixes) | **Target Production Platforms:** Raspberry Pi 4 Model B (Linux ARM64) & Cloudflare Workers / D1

---

## 1. Executive Status Matrix of Validation Gates

| Subsystem / Requirement Gate | Assessment Status | Tested Failure Model / Evidence | Required Action to Close Gate |
|:---|:---:|:---|:---|
| **Official Assessment Mock Binary** | **UNVERIFIED** | Official binary not in repository; verified against Section 6.1 math recurrence and stand-in mock. | Run against official mock binary when provided. |
| **Live Cloudflare Deployment** | **PASS (LIVE DEPLOYED)** | Live Worker deployed at `https://mayacardio-cloud-pipeline.pranjalkaran22.workers.dev`; D1 db connected; live latency p50 = 353 ms, max = 1045 ms. | Gate satisfied in production. |
| **ARM64 1-Hour Resource Profiling** | **NOT MEASURED** | Host is Windows 11 x64; no physical ARM64 run executed. | Run `scripts/profile_pi4.sh` on physical Pi 4. |
| **Physical 5V Sudden Power-Cut** | **UNVERIFIED** | Software `process.abort()` verified clean SQLite WAL recovery. | Execute 10-cycle 5V relay disconnect test on Pi 4. |
| **Software Crash / Abort Safety** | **PASS** | Verified via `process.abort()` child process testing. | Gate satisfied for OS-level process crashes. |
| **Local D1 Simulation Fidelity** | **PASS** | In-process SQLite adapter replicates Workers/D1 schemas & HTTP codes. | Gate satisfied for local automated testing. |
| **2-Hour Backlog Catch-Up Rate** | **PASS** | 3,600 chunks synchronized in 6.45s (892,740 samples/s) on simulation. | Verified against 15-minute deadline. |
| **24-Hour Free-Tier Cloud Capacity** | **PASS (QUALIFIED)** | 24h telemetry is 247.10 MB (ECG, 48.6h) to 484.19 MB (Ramp, 24.8h) via gzip; fits 500 MB limit. Noisy data (>640 MB) requires multi-DB partitioning. | Gate satisfied in code & qualified. |

---

## 2. Edge Device Deployment (Raspberry Pi 4 & ARM64 Environments)

### A. System Requirements & Platform Compliance (Section 1)
In accordance with Section 1 of the assessment, MayaCardio supports:
* **Target Hardware:** Raspberry Pi 4 Model B (2 GB or 4 GB RAM, Quad-core Cortex-A72 @ 1.5 GHz).
* **ARM64 Virtual Machine / Emulator:** QEMU `aarch64` or cloud ARM64 VM (e.g. AWS Graviton `t4g.small` limited to $\le 4$ cores / 2 GB RAM).
* **ARM64 Docker Container:**
  ```bash
  docker run --rm -it --platform linux/arm64 --cpus=4 --memory=2g \
    -v $(pwd):/app -w /app node:22 bash scripts/profile_arm64.sh
  ```
* **Operating System:** Raspberry Pi OS 64-bit (Debian 12 Bookworm, kernel 6.6+ `aarch64`).
* **Runtime:** Node.js v22 LTS or v24 LTS (`arm64` binary).

### B. Production Systemd Service Unit (`/etc/systemd/system/mayacardio.service`)
```ini
[Unit]
Description=MayaCardio Telemetry Ingestion & Durability Pipeline
After=network.target time-sync.target
Wants=time-sync.target

[Service]
Type=simple
User=pi
Group=pi
WorkingDirectory=/opt/mayacardio
Environment=NODE_ENV=production
Environment=PIPELINE_DB=/var/lib/mayacardio/pipeline.db
Environment=PORT=7100
Environment=CLOUD_SYNC_URL=https://mayacardio-cloud-pipeline.pranjalkaran22.workers.dev
EnvironmentFile=-/etc/mayacardio.env

# Dynamic session adoption from incoming mock replies (Section 6.2)
ExecStart=/usr/bin/node --experimental-strip-types bin/pipeline.ts start \
  --db ${PIPELINE_DB} \
  --port ${PORT} \
  --cloud-url ${CLOUD_SYNC_URL}

Restart=always
RestartSec=3s
KillMode=mixed
TimeoutStopSec=10s

# Section 1 Raspberry Pi-Class Resource Constraints (max 4 cores, 2 GB RAM)
CPUQuota=400%
MemoryMax=2G
LimitNOFILE=65536
Nice=-10
ProtectSystem=full
ProtectHome=read-only
ReadWritePaths=/var/lib/mayacardio

[Install]
WantedBy=multi-user.target
```

### C. Installation Steps
```bash
sudo mkdir -p /opt/mayacardio /var/lib/mayacardio /etc
sudo chown -R pi:pi /opt/mayacardio /var/lib/mayacardio
echo "CLOUD_AUTH_TOKEN=CLINICAL_PROD_BEARER_TOKEN_HERE" | sudo tee /etc/mayacardio.env > /dev/null
sudo chmod 0600 /etc/mayacardio.env
cd /opt/mayacardio
npm install --omit=dev
sudo cp deploy/mayacardio.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now mayacardio.service
```

---

## 3. Cloud Infrastructure Deployment (Cloudflare Workers & D1)

### A. Active Production Deployment Details
The production cloud pipeline is fully deployed on the Cloudflare Free Tier:
* **Cloudflare Worker URL:** `https://mayacardio-cloud-pipeline.pranjalkaran22.workers.dev`
* **Cloudflare D1 Database:** `mayacardio-production`
* **Health Check Endpoint:** `GET /health` returns `HTTP 200 { ok: true, status: 'healthy', d1: 'connected' }`.
* **Reachability Probing Latency (Probes on 2026-10-15T12:38:20Z):** Measured at **p50 = 127.5 ms, max = 388.7 ms** (`/health`) and **p50 = 101.2 ms, max = 231.7 ms** (`/status`).
* **Live End-to-End Result Delivery Latency (2026-10-15T14:25:00Z):** Measured from window close to durable D1 retrieval at **P50 = 1,506 ms, Max = 1,828 ms** across 10 consecutive windows (8,000 samples), well within the assessment $\le 5,000\,\text{ms}$ ceiling.
* **Credential Rotation:** Token rotated via Wrangler secret put; old credential rejected with `HTTP 401 Unauthorized` and new token verified with `HTTP 200 OK`.

| Resource Parameter | Cloudflare Free-Tier Allocation | MayaCardio Telemetry Demand | Evidence Classification & Audit Status |
|:---|:---|:---|:---:|
| **Daily Worker Requests** | 100,000 requests / day | Ingestion (43.2k) + Health/Ops (17k–43k) = 60.5k–86.4k/day | **PASS (ESTIMATED COMPLIANT)** (60.5%–86.4% of quota) |
| **Worker CPU Time** | 10 ms CPU execution / invocation | ~1.8 ms – 3.2 ms on host Node.js; ~4–8 ms on live Worker | **PASS (LIVE VERIFIED)** (Below 10 ms ceiling) |
| **D1 Rows Written** | 100,000 row writes / day | Base chunk rows (43.2k) + potential incidents | **PASS (ESTIMATED COMPLIANT)** (43.2%–86.4% of quota) |
| **D1 Rows Read** | 5,000,000 row reads / day | 43.2k idempotency reads + ~130k context reads | **PASS (ESTIMATED COMPLIANT)** (~3.5% of daily quota) |
| **D1 Query Subrequests** | **Max 50 D1 queries / invocation** | Max 2–3 D1 queries per Worker invocation | **PASS (SOURCE AUDIT)** (All endpoints verified) |
| **D1 Databases** | Up to 10 databases per account | 1 database currently used | **PASS (SOURCE AUDIT)** |
| **D1 Database Storage** | **500 MB per database limit** | **247.10 MB / day (Smooth ECG) to 484.19 MB / day (Ramp)** | **PASS (GZIP IMPLEMENTED)** (24.8h–48.6h retention) |

---

## 4. Cloud Storage Capacity & Compression Resolution

### A. Raw Sample & Encoding Byte-Count Verification
* **Official Sensor Telemetry Spec (Section 6.1):** Dual sensors at 400 samples/second each.
* **Chunk Frequency:** 1 chunk every 2.0 seconds $\rightarrow 1,800\,\text{chunks/hour} \rightarrow 43,200\,\text{chunks/24-hour day}$.
* **Dual-Sensor Raw Values:**
  * Sensor 1 (400 Hz): $800\,\text{samples} \times 3\,\text{channels} \times 3\,\text{bytes} = 7,200\,\text{bytes}$ raw 24-bit binary.
  * Sensor 2 (400 Hz): $800\,\text{samples} \times 2\,\text{channels} \times 3\,\text{bytes} = 4,800\,\text{bytes}$ raw 24-bit binary.
  * Total Raw Binary Samples: Exactly **12,000 bytes** ($11.72\,\text{KiB}$) per 2-second chunk.
* **Base64 Expansion:**
  * Raw 12,000 bytes expand to **16,000 Base64 characters** ($12,000 \times 4 / 3$) before framing or availability masks.
  * Availability Masks: $2 \times 100\,\text{bytes} = 400\,\text{hex characters}$ (800 bits per sensor).
  * Uncompressed JSON Request: $\approx 16,645\,\text{bytes}$ per chunk.

### B. Implemented Compression Solution (`src/cloud/chunk_codec.ts`)
Binary sample arrays are compressed via `zlib.gzipSync` prior to Base64 encoding and transparently decompressed via `zlib.gunzipSync` in the Cloudflare Worker upon ingestion and export.

### C. Empirical Storage Measurements Across Biosignal Profiles (43,200 Chunks / Day)

| Signal Profile | Raw Sample Bytes | Gzip Binary Size | Base64 String | D1 Stored Row | Daily Storage (Decimal MB) | Daily Storage (Binary MiB) | 500 MB D1 Free Retention | Parity Check |
|:---|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|
| **Smooth Biosignal (ECG/Sine)** | 12,000 B | 3,613 B (3.32x) | 4,820 chars | 5,720 B | **247.10 MB** | **235.66 MiB** | **48.6 hours** (50.9h binary) | 100% PASS |
| **Section 6.1 Ramp Data** | 12,000 B | 7,730 B (1.55x) | 10,308 chars | 11,208 B | **484.19 MB** | **461.76 MiB** | **24.8 hours** (26.0h binary) | 100% PASS |
| **Noisy Biosignal (ECG + 5% Noise)** | 12,000 B | 10,440 B (1.15x) | 13,920 chars | 14,820 B | **640.22 MB** | **610.57 MiB** | **18.7 hours** (19.7h binary) | 100% PASS |
| **High-Entropy Random (Worst-Case)** | 12,000 B | 12,046 B (1.00x) | 16,064 chars | 16,964 B | **732.84 MB** | **698.90 MiB** | **16.4 hours** (17.2h binary) | 100% PASS |

*Notes on Units:* Decimal MB uses $10^6$ bytes ($1,000,000\,\text{bytes}$); Binary MiB uses $2^{20}$ bytes ($1,048,576\,\text{bytes}$). Cloudflare D1 quotas are specified as 500 MB.

### D. Honest Retention Bounds & Free-Tier Limitations
* **Physiological ECG & Smooth Biosignals:** Gzip compression reduces daily footprint to **247.10 MB decimal (235.66 MiB binary)**, comfortably satisfying 24-hour retention (>48.6 continuous hours in a single 500 MB D1 database).
* **Section 6.1 Ramp Reference:** Gzip compression yields **484.19 MB decimal (461.76 MiB binary)**, fitting within the single 500 MB database for 24.8 hours.
* **High-Noise / High-Entropy Limitation:** Under elevated noise or incompressible data, daily volume reaches 640.2 MB to 732.8 MB, exhausting a single 500 MB database in 16.4 to 18.7 hours.
* **Architectural Free-Tier Mitigation:** Cloudflare D1 Free provides **10 databases (5,000 MB aggregate storage)**. Future continuous 24-hour retention under extreme noise can partition across databases without exceeding the free plan. Multi-database partitioning is documented as an architectural option and is not required for standard physiological data.

---

## 5. Security & Operational Assumptions

* **Transport Encryption:** Edge-to-cloud sync requires HTTPS (TLS 1.3). Plaintext HTTP is strictly rejected in production.
* **Access Control:** All sync endpoints require bearer authentication matching `CLOUD_AUTH_TOKEN`.
* **Local Storage Protection:** SQLite database files on the edge node must be created with `chmod 0600` permissions accessible only to the dedicated `pi` service user.
* **Clock Monotonicity:** Processing algorithms rely on hardware sample counting rather than wall-clock timestamps to remain immune to NTP jumps.
