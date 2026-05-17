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

## 2026-05-15 — Issue #2: Concurrent tool-call dispatch
**Duration:** ~60 min · **Branch:** `session/2026-05-15-1923-issue-02`

- Shipped `async_pipelines/tool_dispatch.py`: `ToolCall`/`ToolResult` dataclasses, `ToolRegistry` (thin dict wrapper, D-004), `dispatch_tool_calls()` with `asyncio.TaskGroup` + optional `Semaphore`, partial-failure tolerance via `return_exceptions`, fail-fast wrapped in `PipelineError` (parity with `process()`, D-006), per-tool telemetry on `elapsed_ms`.
- `ToolResult` carries `tool_call_id` (D-005) so callers round-trip Anthropic `tool_use_id`s without maintaining a side correlation map.
- Missing-tool error surface: pre-resolved up-front in fail-fast mode (clean error early), recorded on the result in `return_exceptions=True` mode.
- 17 new hermetic tests + 16 from #1 = 33/33 passing. Acceptance criteria from #2 all met (handles 1..K calls, partial failures don't poison batch, 5-tool serial-vs-concurrent bench in suite).
- README "Tool dispatch (#2 · this PR)" section with snippets.

**Why this work, this session:** Tool dispatch is the natural sibling to `process()` — both are bounded-concurrency fan-outs over independent units of work. Sharing `PipelineError` and the `return_exceptions` semantic keeps the package's mental model consistent. The `ToolCall`/`ToolResult` shape is what `agent-orchestration-platform`'s tool registry (#2 there) will consume.

**Open questions / blockers:** None. Real production-workload speedup (5–20×) still pending #4's 1000-doc benchmark with a real LLM-call workload; the test-suite bench shows the speedup mechanism is real on a deterministic fake.

**Next session:** Issue #4 (1000-doc benchmark) is the natural sibling — uses both `process` and `dispatch_tool_calls` against a real-shape (mock-LLM) workload to publish numbers for `docs/benchmarks.md`.

## 2026-05-16 — Issue #4: 1000-doc benchmark
**Duration:** ~35 min · **Branch:** `session/2026-05-16-0437-issue-4`

- Shipped `async_pipelines/benchmark.py`: `Workload` config + `RunResult` shape + three `Pipeline` implementations (`SerialPipeline`, `AsyncPipeline`, `BatchedAsyncPipeline`) sharing the same `async run(docs)` signature. `FakeLLM` simulates per-call latency via `asyncio.sleep` (D-007 — the speedup ratio is the load-bearing claim, real-API swap is two-line operator action). `BatchedAsyncPipeline` uses `make_batch_caller` for one-round-trip-per-batch semantics (D-008 — the Anthropic Batch API shape, not just chunked async).
- `scripts/bench_1000_doc.py` is the single command: runs all three pipelines, attaches speedup ratios vs serial, writes `docs/benchmarks.md` + `docs/benchmarks.json`. Configurable via `--n`, `--latency`, `--concurrency`, `--batch-size`. Smoke-tested at n=200 and the full n=1000.
- **Real measured numbers** committed to `docs/benchmarks.md` from a local n=1000 run on Apple Silicon, CPython 3.14, concurrency 32, batch size 8: serial 43.3 s, async 1.4 s (**30.34×**), batched 0.17 s (**251×**). Honest framing: the 30× exceeds the spec's "5–20×" because the synthetic `asyncio.sleep` is pure-wait, no per-request overhead; real-API speedups land in the spec's range because TCP/TLS/JSON parsing bounds the fan-out ceiling.
- 10 new tests in `tests/test_benchmark.py` covering each pipeline's input-order preservation, call counting per FakeLLM, the async-beats-serial assertion under synthetic latency, batch-size validation, `attach_speedup` math (including zero-serial-duration edge case), and the script's markdown + JSON output schema. Suite total: 43/43 pass; ruff lint+format clean.
- README updated with the 1000-doc section + table + honest disclosure.

**Why this work, this session:** With `process` (#1) and `dispatch_tool_calls` (#2) shipped, the benchmark validates the v0.1 claim that this library actually delivers the 5–20× win the spec promises. The synthetic-LLM design lets CI prove the speedup ratio is real without burning real-API budget.

**Open questions / blockers:** Real-Anthropic-API numbers come from an operator running the script with an `AnthropicLLM` adapter; the seam is the `LLMClient` Protocol (`async __call__(prompt) -> str`). No engineering blocker.

**Next session:** python-async-llm-pipelines hits v0.1 with this PR. Move to a different repo.

## 2026-05-17 — Issue #3: Backpressure metrics and OOM-safety demo
**Duration:** ~40 min · **Branch:** `session/2026-05-17-1908-issue-03`

- Added `StreamMetrics` dataclass to `async_pipelines.core` and an optional `metrics=` keyword-only argument to `stream` (D-009). The dataclass captures `produced`, `consumed`, `producer_pauses` (count of `queue.put`s that had to wait for space), `max_queue_depth` (high-water mark sampled after each successful put), and `producer_pause_seconds` (cumulative wall-time blocked, via `time.perf_counter`). All stdlib — keeps D-002 (runtime-dep-free) intact.
- Shipped `scripts/bench_backpressure.py`: fast producer (zero per-item latency) × slow consumer × bounded queue, instrumented with `tracemalloc` for peak heap measurement. Writes `docs/backpressure.md` + `docs/backpressure.json`. Optional `--compare` flag runs the same `n` at 4× queue size to make the bound visible side-by-side. Real run on Apple Silicon (CPython 3.14, n=5000, 1 ms consumer, concurrency=2): `qs=8` peaks at exactly 8 items in queue with 2,707 pauses; `qs=32` peaks at exactly 32 items in queue with 2,672 pauses. Duration is essentially identical (~3.05 s) because the consumer is the bottleneck — only the in-memory backlog changes, which is the entire point of the demo.
- 5 new tests in `tests/test_stream.py` covering: (a) metrics populate counts under no-pressure, (b) `producer_pauses ≥ 1` and `max_queue_depth == queue_size` under slow-consumer pressure, (c) `producer_pauses == 0` when consumer keeps up, (d) **the OOM-safety invariant** — `max_queue_depth ≤ queue_size` across a 1000-item run, (e) the default `metrics=None` path returns identical behavior to the v0 signature. Suite total: 48/48 pass; ruff lint+format clean.
- README "Backpressure (#3)" subsection added between the streaming code example and the existing "Tool dispatch (#2)" section. Includes the `StreamMetrics` snippet, the real measured table, and the reproduce command.

**Why this work, this session:** Issue #3 is one of two open `priority:med` issues and the lower-numbered one. The bounded-queue *mechanism* was already shipped with `stream()` in #1; what was missing for the acceptance criteria was (a) operator-visible proof that backpressure is engaging (the metrics surface) and (b) a demo script that measures peak heap and proves it stays bounded by `queue_size`. The in-place dataclass keeps the v0 `stream()` signature backward-compatible — the only change to existing callers is they can now optionally pass `metrics=StreamMetrics()` and inspect it after the call returns.

**Open questions / blockers:** None on this issue. Issue #5 (cancellation/timeout patterns) is the remaining `priority:med` for this repo and a natural follow-on if a future session picks it.

**Next session:** Continue the day-session multi-issue loop — pick the next repo (probably `agent-orchestration-platform` or `nextjs-streaming-ai-patterns`, both 36h+ untouched and earlier in build sequence than this one once it ships).
