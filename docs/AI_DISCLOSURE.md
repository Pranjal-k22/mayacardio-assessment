# MayaCardio AI Tools Disclosure

**Author:** Pranjal Karan | **Phase:** Phase 8 Assessment Submission  
**Requirement:** Mandatory Assessment Disclosure on AI Tools & Automated Assistance (Section 5)

---

## 1. Overview & Policy Statement

In compliance with Section 5 of the MayaCardio "Never Lose a Heartbeat" Software Systems Engineering Candidate Assessment, this document provides a transparent disclosure of the artificial intelligence (AI) tools utilized during the development of this repository.

I am the primary author, engineer, and architect of this project, responsible for approximately 70% of the core engineering effort. AI tools provided approximately 30% of the effort, serving strictly as thought partners, debugging assistants, and conceptual guides rather than writing the core pipeline logic for me. All architectural decisions, subsystem designs, and committed source code were conceived, authored, and verified by me.

---

## 2. Tools & Models Employed

The following tools were consulted as assistive resources during development:

1. **Google Antigravity IDE & Advanced Agentic Assistant:** Used as my primary development environment for codebase traversal, rapid test execution, and terminal management.
2. **Google Gemini (1.5 Pro):** Consulted for conceptual breakdowns of the Section 3 mathematical requirements (specifically the 4-condition peak detection logic) and for calculating Cloudflare D1 Free Tier byte-size storage limits.
3. **Anthropic Claude (3.5 Sonnet):** Used as a debugging partner to troubleshoot SQLite WAL concurrency locks and to help analyze TCP network framing behavior during the fault-injection phases.

---

## 3. Categories of AI Assistance

Rather than relying on AI to author the core application logic, I leveraged these tools to accelerate my own engineering workflow in the following specific areas:

### A. Conceptual Understanding & Quota Math
* **Requirement Analysis:** I used AI to break down the dense logic of the 5-second heart rate rolling window and peak detection rules into manageable pseudo-code before writing the actual TypeScript implementation myself.
* **Storage Feasibility:** I consulted AI to verify my arithmetic regarding Base64 expansion and `zlib` compression ratios to ensure my 24-hour continuous telemetry data would safely fit within Cloudflare's 500 MB D1 database limit.

### B. Debugging & Optimization
* **Concurrency Locks:** When facing SQLite `SQLITE_BUSY` errors during simultaneous edge ingestion and cloud synchronization, I used AI to help me understand Node.js event loop blocking. This guided my decision to isolate the collector into a dedicated worker thread.
* **Network Framing:** I used AI to help debug TCP desynchronization issues when the mock sent unexpected duplicate replies, allowing me to refine my FIFO line-buffer logic.

### C. Scaffolding & Formatting
* **Test Harness Scaffolding:** AI tools were used to generate boilerplate syntax for the Mocha/Chai regression testing files, which I then populated with the 96 custom assertions used to validate the pipeline.
* **Markdown Formatting:** AI assisted in formatting the structured markdown tables in `TEST_EVIDENCE.md` and `RESOURCE_MEASUREMENTS.md` to ensure clean readability for the evaluators.

---

## 4. Human Authorship & Engineering Ownership

The core architecture, implementation, and final engineering decisions in this repository are entirely my own.

* **Code Authorship:** I authored the TypeScript logic, the SQLite WAL transaction boundaries, the Cloudflare Worker routing, and the chunk synchronization algorithms.
* **Execution & Verification:** All tests, fault injections (network cuts, process kills, and power disconnects), and physical hardware profiling runs were executed manually by me.
* **Final Accountability:** I personally reviewed, refined, and validated every line of code committed to this repository. The engineering tradeoffs, architectural guarantees, and system behaviors described in the documentation represent my direct decisions and full responsibility.
