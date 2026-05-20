# Session History (human-readable)

Chronological log of work sessions. Most recent first below the divider.

---

## 2026-05-19 — Issue #16: snapshot test for README + docs/benchmarks.md bench tables
**Duration:** ~25 min · **Branch:** `session/2026-05-19-1531-issue-16` · **PR:** #17

- Added `tests/test_bench_table_snapshot.py` (7 tests). The repo had three sources of truth for the 1000-doc benchmark numbers — `docs/benchmarks.json` (committed raw values), the README's table, and `docs/benchmarks.md`'s rendered table — with nothing asserting they agree. The test renders the JSON's three pipeline runs into the expected row cells and asserts each cell appears in both the README and `benchmarks.md` rows for that pipeline.
- Captured the renderer rules in the test: the README uses `:.3f` for duration, `:.1f` for docs/s, and a **conditional** speedup format (`:.2f×` for ratios <100, `:.0f×` for ratios ≥100 — only the batched 251.21× triggers the integer form today). `docs/benchmarks.md` uses uniform `:.2f` for speedup. Both formats are documented in the test docstring so a future PR that re-renders the table sees the contract before changing it.
- A pipeline-name set guard catches silent additions / drops in the committed JSON. Parametrized over the three pipelines for both renderings → 7 tests total.
- The wall-clock values themselves aren't deterministic across machines, so the test deliberately does **not** re-run the bench; that would be a CI flake. Locking JSON→README and JSON→MD is the right level of strictness — re-running the bench is the documented regen path.
- Tamper-verified by editing the README's `serial 43.311 → 43.999`; the `test_readme_row_matches_bench_json[serial]` parametrized case fired with the regen hint pointing at `scripts/bench_1000_doc.py`. Reverted to green.

**Why this work, this session:** Continuation of the portfolio-wide drift-lock pattern. The existing `test_readme_snapshot.py` here covers structural invariants but not the numeric cells of the benchmark table. With three renderings of the same JSON living in three places, the highest-leverage missing test was the JSON-to-renderer drift-lock — this one closes that gap.

**Open questions / blockers:** None — PR ready for review.

**Next session:** Drift-locks now cover both the README structural invariants (#13) and the benchmark table cells (#16). Remaining gap is `docs/backpressure.{json,md}` — same pattern would apply but it's a separate file pair; viable follow-up if anyone files it.

## 2026-05-19 — Issue #13: drop "This PR ships" framing + snapshot test
**Duration:** ~25 min · **Branch:** `session/2026-05-19-issue-13`

- Rewrote `What this is` third+fourth paragraphs from "This PR ships the first primitive" / "Everything beyond #1 is staged in follow-up issues" (both true on 2026-05-15) to a five-bullet present-tense layer picture (#1 process+stream, #2 tool_dispatch, #3 backpressure metrics, #4 1000-doc bench, #5 per-item timeouts).
- Replaced Demo section's "*60-second demo pending — depends on issue [#4].*" with today's two-command hermetic demo path (`pytest` + `scripts/bench_1000_doc.py`) and named the captured-asset follow-up as #14.
- `tests/test_readme_snapshot.py` (4 tests) locks: every (#N) ref appears in "What this is", `This PR ships` does not appear in the README, every relative file path resolves, Demo section names a follow-up + mentions the bench script + doesn't carry the gating phrase.

**Why this work, this session:** Same drift pattern the autonomous loop has been fixing across the portfolio this week; python-async-llm-pipelines was the last priority:med candidate after the 2026-05-18 cycle.

**Open questions / blockers:** None.

**Next session:** Continues with Phase A; #14 is priority:low demo capture.

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

## 2026-05-18 — Issue #5: Cancellation and timeout patterns
**Duration:** ~30 min · **Branch:** `session/2026-05-18-issue-05` · **PR:** #10

- Added a `timeout: float | None = None` kwarg to both `process()` and `stream()`. Each `fn(item)` call is wrapped in `asyncio.wait_for` when the kwarg is set; on expiry the wrapper raises the new typed `PipelineTimeoutError` (subclass of `PipelineError`) with `.index` and `.timeout_s`. Default `timeout=None` is byte-identical with the pre-#5 path.
- 12 new tests in `tests/test_timeouts.py`. The load-bearing two pin the "no orphaned tasks" invariant from both directions: `finally`-block counters under an internal deadline expiry, and under external `task.cancel()`. Both assert `finished == started`. Plus exception-shape (subclass + attributes), fail-fast vs `return_exceptions` paths on `process` and `stream`, invalid-timeout validation, and a parametrize that keeps `timeout=None` on the original code path.
- README: new `Timeouts & cancellation` section with two snippets (one per failure-handling policy) and the structured-cancel invariant in plain English.

**Why this work, this session:** Issue #5 was the next med-priority item earliest in build sequence with a contained scope and clear acceptance criteria — exactly the right shape to lead a multi-issue night session.

**Open questions / blockers:** None.

**Next session:** Move on to mcp-server-cookbook #4 (internal-tools bridge MCP server).

## 2026-05-18 — Issue #11: Architecture doc covers all 5 shipped primitives
**Duration:** ~20 min · **Branch:** `session/2026-05-18-1544-issue-11` · **PR:** [#12](https://github.com/jt-mchorse/python-async-llm-pipelines/pull/12) (ready)

- Rewrote `docs/architecture.md`: integrated all-green pipeline lifecycle mermaid + one section per shipped primitive (#1 process/stream, #2 concurrent tool dispatch, #3 backpressure metrics, #4 1000-doc benchmark, #5 per-item timeouts). Each section has a prose statement of what it does, a mermaid of its own slice, the relevant D-NNN references back to MEMORY (D-002 through D-010), and a "composes with" line.
- README Architecture block: code-shape signatures refreshed to include the current `per_item_timeout` and `metrics` kwargs; "shipped vs pending" stub replaced with a one-line pointer at the doc.
- Mermaid hygiene consistent with the rest of the portfolio's architecture-doc PRs this session — labels with parens are fully double-quoted.

**Why this work, this session:** Every original priority:high issue is closed; the last `priority:med` items (#3 backpressure, #5 cancellation/timeouts) shipped on 2026-05-17/18. The architecture doc still labelled #2 and #4 as pending and had no mention of #3 or #5. Filling that gap is the cleanest move toward v0.1.

**Open questions / blockers:** None — PR ready for review.

**Next session:** Pick up the next zero-open-issue repo in §8 build sequence (agent-orchestration-platform), or wait for in-flight PRs to merge first.

## 2026-05-20 — Issue #18: lock async_pipelines public surface
**Duration:** ~20 min · **Branch:** `session/2026-05-20-0326-issue-18`

- Added `tests/test_public_surface.py` (5 standalone + 3 parametrized = 8 test items) and `__version__ = "0.0.1"` on the package. First variant in the portfolio-wide pattern to combine the README-quickstart-imports axis (locking 7 names quoted across five `from async_pipelines import …` snippets at lines 88, 107, 129, 188, 231) with the README-dotted-path axis (locking the line-29 quoted `async_pipelines.tool_dispatch.dispatch_tool_calls`). Six orthogonal axes total.
- Tamper-verified four of six: bad version, drop `"StreamMetrics"` from `__all__`, in-process delete of `dispatch_tool_calls` from `tool_dispatch`, alias-rename `process as process2` (which fires three axes simultaneously: bound-and-non-none, readme-quickstart, anchor[core]).
- Full suite 79/79 (was 71; +8 new), `__init__.py` at 100%.

**Why this work, this session:** Seventh strike of the portfolio-wide public-surface hygiene pattern. The README's import + dotted-path footprint here is the largest in the portfolio (5 + 1 quoted references); fitting them into one suite proves the pattern's two axis variants compose without redundancy.

**Open questions / blockers:** None — PR ready for review.

**Next session:** Only `mcp-server-cookbook` (Python example) remains as a portable target in this pattern.
