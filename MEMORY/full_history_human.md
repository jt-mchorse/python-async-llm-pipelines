# Session History (human-readable)

Chronological log of work sessions. Most recent first below the divider.

---

## 2026-05-14 — Issue #1: async batching with bounded concurrency
**Duration:** ~45 min · **Branch:** `session/2026-05-14-1445-issue-01`

- Shipped `async_pipelines.process(items, fn, *, concurrency, return_exceptions=False)` — `asyncio.TaskGroup` + `asyncio.Semaphore`-bounded fan-out, results returned in input order (D-003).
- Shipped `async_pipelines.stream(producer, fn, *, concurrency, queue_size, return_exceptions=False)` — bounded `asyncio.Queue` for backpressure on unbounded async sources.
- Wrapper is runtime-dep-free (D-002): only stdlib `asyncio`. Anthropic/OpenAI/httpx are not required deps.
- 16 tests, 97% coverage. Covers ordering, concurrency-ceiling enforcement via semaphore measurement, fail-fast cancellation propagation through TaskGroup, partial-failure mode, backpressure on the bounded queue.
- README backfilled with real Quickstart (10-line runnable `process()` snippet + a streaming snippet).
- D-002 (runtime-dep-free) and D-003 (input-order results) recorded in MEMORY (separate `memory:` commit).

**Why this work, this session:** Issue #1 is the foundation for #2 (concurrent tool dispatch) and #4 (1000-doc benchmark). The wrapper has to exist before there's anything to benchmark.

**Open questions / blockers:** The 5–20× win benchmark (#4) needs a real LLM-call workload — deferred until either the Anthropic SDK adapter or a swappable provider interface is in place.

**Next session:** Issue #2 (concurrent tool-call dispatch) — sits cleanly on top of the `process()` shape shipped here.
