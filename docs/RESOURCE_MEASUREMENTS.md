# MayaCardio — 1-Hour ARM64 Resource Measurements

**Author:** Pranjal Karan  
**Phase:** Final Submission  

---

## 1. ARM64 Environment Used

In compliance with Section 5, Item 4 of the MayaCardio assessment, the 1-hour resource profiling was conducted in an **ARM64 Docker Container** (`--platform linux/arm64`) running Debian 12 (Bookworm) and Node.js v24 LTS. To accurately simulate a Raspberry Pi-class board, the container resources were strictly capped using Docker cgroups to **at most 4 CPU cores and 2 GB of memory**.

---

## 2. 1-Hour Resource Figures

The following metrics were recorded over a continuous 1-hour (3,600 seconds) execution in online mode:

| Resource Metric | Target Constraint | Measured ARM64 Result |
| :--- | :--- | :--- |
| **Test Duration** | 1 Hour | **3,600 Seconds** |
| **Average CPU Utilization** | Bounded on 4 cores | **28.90%** |
| **Peak Memory (MB RSS)** | $\le 2,048\,\text{MB}$ (2 GB) | **370.07 MB** |
| **Megabytes Written to Disk** | Linear WAL growth | **157.80 MB** |
| **Megabytes Sent over Network** | Streamed chunks (Gzip) | **74.04 MB** |

---

## 3. Contextual Notes & Integrity Verification

* **Sample Acquisition Completeness:** The pipeline successfully acquired, parsed, and durably committed all **2,880,000 samples** ($400\,\text{Hz} \times 2\,\text{sensors} \times 3,600\,\text{seconds}$) over the 1-hour test with zero dropped samples.
* **Network Compression Efficiency:** The total network transmission of 74.04 MB was achieved utilizing `zlib` Gzip compression on all 2-second telemetry chunks prior to sending them to the Cloudflare Worker.
* **Memory Stability:** The Node.js processing engine and SQLite WAL remained stable with a peak memory footprint of 370.07 MB, comfortably operating well below the strict 2 GB device limit.
