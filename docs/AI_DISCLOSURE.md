# MayaCardio — AI Tools Disclosure

**Author:** Pranjal Karan  
**Role:** Author & Systems Architect  

---

## 1. Authorship Statement

I am the sole author and architect of MayaCardio. The design decisions, system architecture, database schemas, and application logic in this repository were conceived, implemented, and verified entirely by me.

AI tools were not used to write the pipeline or generate code. Instead, they served as assistive research resources—much like consulting engineering documentation, technical specifications, or a sounding board.

---

## 2. Tools & Advisory Scope

During development, I consulted AI assistants (Google Gemini, Anthropic Claude, and Antigravity IDE) strictly for advisory guidance:

* **Requirements Clarification:** Discussing edge cases in the Section 3 signal processing rules (such as boundary conditions for the 4-condition peak detection and rolling windows) before writing the TypeScript algorithms myself.
* **Storage Feasibility Checks:** Sanity-checking arithmetic around Base64 expansion and gzip compression ratios to ensure 24-hour telemetry safely fits within Cloudflare D1 free-tier limits.
* **Concurrency Brainstorming:** Exploring Node.js event-loop and SQLite write-lock behaviors when diagnosing concurrency bottlenecks, which informed my decision to isolate the ingestion collector into its own worker thread.

---

## 3. Engineering Ownership

* **Codebase & Architecture:** Every line of TypeScript, SQL, and system configuration was written, debugged, and reviewed directly by me.
* **Verification & Testing:** All regression suites, fault-injection tests (network cuts, process crashes, and power disconnects), and benchmarks were executed and validated manually on my development machine and test environments.
* **Accountability:** I hold full engineering responsibility for the implementation, performance characteristics, and guarantees described throughout this submission.
