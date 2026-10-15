# MayaCardio AI Tools Disclosure

**Author:** Pranjal Karan  
**Requirement:** Mandatory Assessment Disclosure on AI Tools & Automated Assistance (Section 5)

---

## 1. Executive Statement & Engineering Ownership

In accordance with Section 5 of the MayaCardio "Never Lose a Heartbeat" Candidate Assessment, this document outlines the extent and nature of automated assistance used while building this project.

**Core Authorship Statement:**  
This project was conceived, architected, and coded by me (**Pranjal Karan**). I am responsible for over 70% of the active engineering effort—including the end-to-end data pipeline, the SQLite WAL persistence layer, the network state machine, the Cloudflare Worker endpoints, and all 96 automated regression assertions.

Artificial Intelligence tools accounted for approximately **25–30%** of my time, acting strictly as an **interactive technical reference, sounding board, and conceptual tutor**. At no point was an AI model instructed or permitted to generate the application codebase or write the pipeline logic for me. Every single line of TypeScript, SQL, and configuration was authored, debugged, and verified directly by my hands.

---

## 2. Tools Consulted

I treated AI models the same way an engineer consults official documentation, RFC specifications, or StackOverflow:

1. **Google Gemini (1.5 Pro):** Consulted for requirements clarification—specifically for discussing the mathematical formulation of Section 3's 4-condition peak detection and double-checking cloud storage arithmetic (uncompressed vs. gzipped D1 row limits).
2. **Anthropic Claude (3.5 Sonnet):** Consulted as a conceptual sounding board when troubleshooting SQLite concurrency errors (`SQLITE_BUSY`) and understanding Node.js event-loop starvation during intensive TCP stream ingestion.
3. **Google Antigravity IDE:** Used as my local workspace environment for running tests, managing git commits, and inspecting file structures.

---

## 3. How AI Was Used (and How It Was NOT Used)

To ensure complete academic and professional honesty, here is the exact breakdown of how AI was utilized:

### What AI Helped Me With (Conceptual & Advisory ~30%):
* **Clarifying Dense Requirements:** The Section 3 signal processing specification has strict conditions (800-sample lookback, relative amplitudes, refractory periods, and 5-second rolling windows). I discussed these rules with AI to verify my interpretation of edge cases (such as index bounds $w < 4$) before implementing the algorithms in TypeScript.
* **Storage Arithmetic Sanity Checks:** Calculating the 24-hour telemetry byte footprint across raw binary, Base64 strings, and gzip compression to ensure Cloudflare's 500 MB free D1 limit would not be breached during continuous operation.
* **Troubleshooting Concurrency Concepts:** When multiple components competed for SQLite write transactions, discussing thread isolation concepts helped me realize the collector read-loop needed its own dedicated write connection in a separate worker thread.
* **Markdown Formatting:** Polishing the layout of documentation tables and evidence matrices to ensure clean readability for the evaluators.

### What AI Did NOT Do:
* **No Autonomous Code Generation:** The pipeline was not created by asking an AI to "build this system." I wrote the application logic, the network monitor, the FIFO TCP buffering, the sync coordinator, and the database schemas.
* **No Black-Box Logic:** There is no code in this repository that I do not completely understand, cannot explain in technical detail, or could not recreate on a whiteboard.
* **No Automated Verification:** All testing, fault injection (killing processes, cutting ports via `iptables`, pulling power cables), benchmarking, and terminal commands were manually executed and validated by me.

---

## 4. Final Verification & Accountability

I hold 100% engineering ownership and accountability for this submission. The engineering tradeoffs, data safety guarantees, and architectural designs described in `DESIGN_NOTE.pdf` represent my independent technical decisions as a software systems engineer.
