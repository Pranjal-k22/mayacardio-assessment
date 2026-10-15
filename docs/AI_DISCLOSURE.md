# MayaCardio — AI Tools Disclosure

**Author:** Pranjal Karan  
**Role:** Author & Systems Architect  

---

## 1. Authorship Statement

I am the primary author and architect of MayaCardio. The design decisions, system architecture, database schemas, and overall pipeline implementation were directed, conceived, and verified by me.

AI tools assisted with drafting code snippets, test boilerplate, and debugging, but they did not write the pipeline autonomously. I used them as an interactive copilot and technical reference—helping me work through tricky implementation steps, clarify dense requirements, and troubleshoot roadblocks while I wrote and assembled the codebase.

---

## 2. Tools & Advisory Scope

During development, I consulted AI assistants (Google Gemini, Anthropic Claude, and Antigravity IDE) for focused assistance:

* **Code & Boilerplate Scaffolding:** Assisting with initial TypeScript boilerplate, unit test scaffolding, and syntax drafting for specific utility functions, which I then refined and integrated into the pipeline.
* **Requirements Clarification:** Discussing edge cases in the Section 3 signal processing rules (such as boundary conditions for the 4-condition peak detection and rolling windows) before implementing the algorithms.
* **Storage Feasibility Checks:** Sanity-checking arithmetic around Base64 expansion and gzip compression ratios to ensure 24-hour telemetry safely fits within Cloudflare D1 free-tier limits.
* **Debugging & Concurrency:** Brainstorming Node.js event-loop and SQLite write-lock behaviors when diagnosing bottlenecks, which led to my decision to isolate the ingestion collector into its own worker thread.

---

## 3. Engineering Ownership

* **Core Logic & Architecture:** While AI assisted with portions of the code writing and syntax, the core algorithms, transaction boundaries, and system integration were designed and implemented under my direction.
* **Verification & Testing:** Every test suite, fault-injection scenario (network cuts, process crashes, and power pulls), and performance benchmark was executed and validated manually by me.
* **Accountability:** I hold full engineering responsibility for the entire codebase, its performance characteristics, and the guarantees described throughout this submission.
