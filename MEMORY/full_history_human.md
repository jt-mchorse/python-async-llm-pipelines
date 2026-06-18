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

## 2026-05-21 — Issue #14: 60-second demo capture script
**Duration:** ~28 min · **Branch:** `session/2026-05-21-1929-issue-14` · **PR:** #20

- Added `scripts/capture_demo.sh` driving the two surfaces from the README's Demo section: `pytest --ignore=tests/test_capture_demo_smoke.py` (then `python scripts/bench_1000_doc.py --n 200 --concurrency 32 --batch-size 8 --out <tmp>.md`) followed by `cat` so the speedup table is on camera. `--n 200` (not 1000) keeps the bench under ~10s so the full demo fits 60s; the bench output's own "Synthetic LLM disclosure" already says the *ratios* are load-bearing under FakeLLM, not absolute durations. Per-run tempdir trapped on EXIT/INT/TERM. `CAPTURE_PACE_SECONDS` honored, `CAPTURE_DEMO_N` for take variation.
- Added `tests/test_capture_demo_smoke.py` (3 tests) that runs the script with `PACE=0` and asserts the pytest "N passed" summary appears + no " failed" substring; the bench markdown header signature matches what `test_bench_table_snapshot.py` locks separately; every pipeline (`serial` / `async` / `async+batched`) has a data row in the rendered table; script exists and is executable.
- Two non-obvious gotchas discovered and documented inline in the script header:
  1. The inner `pytest` invocation must `--ignore` the smoke test file, otherwise the outer pytest run that invokes the capture script recursively re-enters it.
  2. `pyproject.toml`'s `addopts="-ra -q"` plus an explicit `-q` on the command line becomes `-qq`, which silences the "N passed in Xs" summary line. The script omits the second `-q` accordingly.
- Fixed the README Demo block's `bench_1000_doc.py` invocation to include `--out /tmp/bench.md`. Without it, the default `--out` is `docs/benchmarks.md` — a casual reader running the README example will mutate the committed snapshot the snapshot test locks. Found teeth-first this session when an accidental default-flags run broke `test_bench_table_snapshot.py` mid-debug. Added a paragraph pointing at the capture script and the smoke test. 82/82 tests pass, ruff clean.

**Why this work, this session:** Eighth repo to land the `scripts/capture_demo.sh` pattern this week. Issue #14 was the explicit owner of the README's "pending 60s demo" claim and was sitting at `priority:low` — closing it cleanly closes the last quality-bar gap in this repo's v0.1 story.

**Open questions / blockers:** None. The real-API third surface (`AnthropicLLM`) is deliberately out of scope; needs an API key and can't be hermetic. The capture script's epilogue points operators at the swap.

**Next session:** Continue the multi-issue loop on the remaining stale repos. agent-orchestration-platform #16 is the next in §8 build order.

## 2026-05-22 — README claimed `per_item_timeout=`, real kwarg is `timeout=` (#21)

**Duration:** ~20 min. **Issue:** [#21](https://github.com/jt-mchorse/python-async-llm-pipelines/issues/21). **PR:** TBD.

Two README locations claimed the per-item-timeout kwarg was named `per_item_timeout`: the bullet at L45 in the "Today five primitives ship" list, and the architecture-block function-signature lines at L67-68. The actual signature in `async_pipelines/core.py` has been `timeout: float | None = None` since #5 landed. The prose section's code examples lower in the README (`process(..., timeout=5.0)`) were correct all along; only the upper two locations drifted. A reader who hit either of those first and copy-pasted `per_item_timeout=5.0` got `TypeError: process() got an unexpected keyword argument 'per_item_timeout'`.

Pure prose fix; no code change. The architecture block also got reordered to match the actual signature parameter order (`concurrency`, `return_exceptions`, then `timeout`; `metrics` last on `stream`). A new snapshot test (`tests/test_readme_kwarg_consistency.py`, 4 tests) does two things: parses the architecture-block one-line signature lines via regex and asserts every named kwarg there is a real `inspect.signature(process).parameters` / `inspect.signature(stream).parameters` entry; and scans every ` ```python ` code-fenced block for `process(...)` / `stream(...)` call sites, balance-parses the argument list at depth zero (so a nested call doesn't false-positive a kwarg from the outer one), and asserts every keyword used is a real parameter. So both drift surfaces — function signature claim *and* call-site claim — are locked in CI.

Why prioritized: this was the fourth post-v0.1 silent-surface-drift fix today, matching the pattern across `embedding-model-shootout` #17 (word-bigrams vs character-bigrams), `chunking-strategies-lab` #19 (unenforced late-chunking embedder constraint), and `vector-search-at-scale` #19 (silently-ignored concurrency on `run_benchmark`). Different shapes but the same family: documented contracts that drift from code or that don't fail loud. Closing them as a batch braces the portfolio against handoff §10's longest rule. Open questions: none. The kwarg-consistency test pattern is portable; the same shape would be useful in any portfolio repo that has both an architecture-block sig fence and prose code examples.

## 2026-05-23 — Architecture-doc drift lock (#24)

**Duration:** ~15 min. **Issue:** [#24](https://github.com/jt-mchorse/python-async-llm-pipelines/issues/24). **PR:** [#25](https://github.com/jt-mchorse/python-async-llm-pipelines/pull/25).

Test-only lock; doc was already in steady state. Dual-axis (`#NN` + `D-NNN`) like `rag-production-kit` PR #30 from earlier this same session. Four invariants pinned, tamper-verified each. **Why this work, this session:** Fourth of five sister issues in this night sweep. **Next session:** Final repo — `agent-orchestration-platform`, which has real drift (`this PR` / `Pending downstream` framing left over from pre-shipping state).

## 2026-05-24 — Issue #26: `dispatch_tool_calls` per-tool timeout (parity with `process`/`stream`)

**Duration:** ~20 min. **Issue:** [#26](https://github.com/jt-mchorse/python-async-llm-pipelines/issues/26). **Branch:** `session/2026-05-24-0356-issue-26`.

`async_pipelines.process()` and `stream()` both accepted a `timeout: float | None` parameter that bounded per-item wall clock. `dispatch_tool_calls()` did not — a misbehaving tool that never returned could stall the whole batch indefinitely with no recourse beyond cancelling the `TaskGroup` from outside. For real agent loops calling unreliable backends (HTTP, subprocess, downstream LLMs), per-tool timeout is a basic safety knob; the parity gap was unprincipled.

Added `timeout: float | None = None` to `dispatch_tool_calls`. None preserves current behavior. Positive float wraps each tool invocation in `asyncio.wait_for`; expiry raises `PipelineTimeoutError(index=idx, timeout_s=timeout)` (already a public class) and follows the existing `return_exceptions` policy — propagates when False (wrapped by the TaskGroup error funnel into PipelineError), attaches to the matching `ToolResult.error_repr` when True. Validation at dispatch entry: `timeout <= 0` raises `ValueError`, matching `process()`.

The plumbing required passing the tool's index through `_invoke_tool` and `_run_with_telemetry` so `PipelineTimeoutError` carries the correct identifier — five new tests pin the shape: slow tool propagates the error class through the chain; `return_exceptions=True` attaches `PipelineTimeoutError` to the matching ToolResult while fast tools' values still populate; `timeout=0` and `timeout=-0.1` raise ValueError; `timeout=None` is a no-op regression guard.

README signature line for `dispatch_tool_calls` now shows the new kwarg; added one paragraph under the dispatch example referencing the parity with `process`/`stream` and the `PipelineTimeoutError` shape.

**Why this work, this session:** Eighth issue in the night-session multi-issue loop, and the first non-CLI-parity fix tonight — this is a real safety surface gap, not a doc/UX improvement. Same shape of work (read for asymmetry, fix, test, lock) at the library layer instead of the CLI layer.

**Open questions / blockers:** none — PR ready for review.

**Next session:** Continue to build-sequence #9 (`agent-orchestration-platform`).

## 2026-05-24 — Issue #28: constructor-time concurrency validation + stale `__init__` docstring

**Duration:** ~12 min. **Issue:** [#28](https://github.com/jt-mchorse/python-async-llm-pipelines/issues/28). **Branch:** `session/2026-05-24-1537-issue-28`.

Two related polish gaps surfaced from reading the benchmark module after #26's tool-dispatch timeout fix:

1. **Constructor-time validation parity.** `BatchedAsyncPipeline.__init__` already validated `batch_size >= 1` at construction time, but neither async pipeline validated `concurrency >= 1`. `process()` itself enforces `concurrency > 0` at call-time, so a pipeline constructed with `concurrency=0` only blew up when the operator finally called `run()` — making the failure point inconsistent with the existing `batch_size` validation behavior. Both async pipelines now raise `ValueError("concurrency must be >= 1; got {n}")` at construction, matching the existing guard's shape.

2. **Stale public-API docstring.** `async_pipelines/__init__.py` line 17 listed the pre-#26 `dispatch_tool_calls(..., concurrency)` signature; `timeout` was added by #26 but the module docstring wasn't updated. Backfilled now.

Five new tests in `tests/test_benchmark.py`: AsyncPipeline rejects zero and negative concurrency; BatchedAsyncPipeline rejects zero concurrency; both construct cleanly at minimum valid values (concurrency=1, batch_size=1). The existing `test_batched_pipeline_rejects_zero_batch_size` is preserved as the regression-pin for the pre-existing guard.

**Why this work, this session:** Fifth Phase B+C target of a 180-minute day session, after `llm-eval-harness` #37, `prompt-regression-suite` #32, `mcp-server-cookbook` #31, and `embedding-model-shootout` #26. First constructor-validation polish rather than CLI/naming parity — same shape of fix (close a half-implemented capability or restore symmetry), different surface.

**Open questions / blockers:** none — PR ready for review.

**Next session:** Continue the day-session loop. Remaining candidates: `chunking-strategies-lab` (run_matrix already polished), `vector-search-at-scale` (recent dry/no-dry parity fix), `rag-production-kit` (today's #33 already filled the `--suite` filter), `agent-orchestration-platform` (retry-cap landed; might have a docs / public-surface gap), `nextjs-streaming-ai-patterns` / `ai-app-integration-tests` (TS frontends, less touched).

## 2026-05-25 — Issue #30: Workload validates fields at construction
**Duration:** ~15 min · **Branch:** `session/2026-05-24-issue-30`

- `Workload` at `async_pipelines/benchmark.py:32` is a frozen dataclass with `n_docs`, `llm_call_seconds`, `concurrency`, `batch_size`. Three of the four were guarded only downstream (concurrency / batch_size at pipeline `__init__` per #28/#29; latency by `asyncio.sleep`). `n_docs` was completely unguarded — `Workload(n_docs=0)` produced an empty `docs` list at `bench_1000_doc.py:43`, near-zero `duration_seconds`, and the speedup math then divided by zero or yielded `inf`. A silently bogus benchmark could have landed in the published markdown.
- Added `__post_init__` raising `ValueError(f"{field} must be ...; got {value}")` for `n_docs < 1`, `llm_call_seconds < 0`, `concurrency < 1`, `batch_size < 1`. Defense-in-depth on the last three is intentional: surfaces the failure at the Workload site where the operator misconfigured, not inside an inner pipeline factory call — same "proximate failure" rationale as today's `rag-production-kit` PR #34 (Retriever construct-time `k_rrf`).
- Eleven new test cases in `tests/test_benchmark.py` under a `#30` block: parametrized over each field × bad-values (3 + 2 + 2 + 2 = 9 cases), plus zero-latency acceptance (instant-LLM smoke test is meaningful), plus all-ones minimum-valid pin (future contract-tightening regression guard). Full suite 119/119 (was 108 after #28).

**Why this work, this session:** Sister to today's portfolio-wide constructor-validation parity sweep. Seventh Phase B+C target of the 180-min day session after `llm-eval-harness` #40, `llm-cost-optimizer` #34, `rag-production-kit` #36, `embedding-model-shootout` #29, `chunking-strategies-lab` #27, `vector-search-at-scale` #27. The pattern is now portfolio-wide: every operator-supplied dataclass / constructor with numeric or load-bearing string fields validates them at construction.

**Open questions / blockers:** none — PR ready for review.

**Next session:** Within the 15-min cleanup buffer of the 180-min cap. Wrap with the final report after this PR.

## 2026-05-25 — Issue #32: isinstance(int) + math.isfinite guards across core, benchmark, tool_dispatch
**Duration:** ~25 min · **Branch:** `session/2026-05-24-issue-32`

- Three public entry points (`process`, `stream`, `dispatch_tool_calls`) and `Workload` had sign-only checks on integer (concurrency, queue_size, n_docs, batch_size) and finite (timeout, llm_call_seconds) parameters. NaN, +/-Infinity, fractional, and bool slipped through. The worst cases: `concurrency = NaN` propagates into `asyncio.Semaphore(NaN)` raising a cryptic deep TypeError at acquire; `timeout = +Infinity` makes `asyncio.wait_for` never fire — silent disable; `NaN llm_call_seconds` skews published benchmark throughput numbers because the simulated-latency sleep becomes platform-dependent.
- Tightened each entry point to `isinstance(int)` (bool excluded explicitly) for count fields and `math.isfinite` for timeout/llm_call_seconds. Error messages updated from "must be positive" to "must be a positive int" or "must be a finite positive number" so callers can grep the new contract. Six pre-existing tests pinning the old strings updated via bulk sed.
- New parametrized tests in `tests/test_process.py` (process concurrency + timeout) and `tests/test_benchmark.py` (Workload four fields). Test count 146.

**Why this work, this session:** Twelfth (and final) Phase B+C target in the 360-min night session. Second PR in python-async-llm-pipelines tonight; the first was via the Phase A fixup-merge of #31 (Workload sign-only `__post_init__`). The two together cover both the construction surface and the runtime-entry surface, completing the portfolio's contract-tightening arc.

**Open questions / blockers:** none — PR ready for review.

**Next session:** The portfolio finiteness/integer-extension sweep is now at full coverage across all 12 repos (12 Phase B+C PRs + 7 Phase A fixup-merges tonight = 19 substantive items). Next sessions can pivot to a different harm class or pick up the demo-capture operator-required follow-ups.

## 2026-05-26 — Issue #34: AsyncPipeline + BatchedAsyncPipeline constructors complete the #32 sweep
**Duration:** ~20 min · **Branch:** `session/2026-05-25-2330-issue-34`

- `AsyncPipeline.__init__` and `BatchedAsyncPipeline.__init__` were the two remaining sign-only `< 1` construction sites in `async_pipelines/benchmark.py` after #32 (`Workload.__post_init__` + `process()` + `stream()` + `dispatch_tool_calls()`). Tightened both with `isinstance(int) + reject bool` above the existing `< 1` check, mirroring `Workload.__post_init__` from #32 (same file). `SerialPipeline.__init__` reviewed and skipped — it takes no numeric parameters.
- Closed the broken-eager-validation-contract failure mode: `AsyncPipeline(..., concurrency=True/1.5/NaN)` previously slipped past the construction-time check (`True < 1` is False, etc.), surfaced from `process()`'s #32-tightened validator with the error pointing at the wrong site. The documented eager-validation guarantee ("a misconfigured workload spec should surface at construction not at the first run()") was effectively false for non-int types. PR makes the comment true.
- Five new parametrize blocks in `tests/test_benchmark.py` following `_BAD_INT`: three type-reject (AsyncPipeline.concurrency, BatchedAsyncPipeline.concurrency, BatchedAsyncPipeline.batch_size), two acceptance pins over `[1, 2, 4, 8, 32]`. Existing `rejects_zero` / `rejects_negative` tests continue to pass unchanged (preservation pin). 25 new collected cases; full suite 146 → 171. Ruff clean.

**Why this work, this session:** Fifth Phase B+C target in the 360-min night session. Direct continuation of #32 — the same sweep but at the public pipeline class constructors, which is where users actually see the error. Picked via build-sequence #8 after `vector-search-at-scale#32` (#4).

**Open questions / blockers:** none — PR ready for review.

**Next session:** Continue the loop. `agent-orchestration-platform` (build #9, TypeScript) and `mcp-server-cookbook` (build #10, TypeScript) are next. The TypeScript validation pattern is different (numeric coercion is more permissive at the type system level, but `typeof === 'number'` + `Number.isInteger` + `!Number.isFinite` is the established shape there).

## 2026-05-26 — Issue #36: Add `async_pipelines/io_utils.atomic_write_text`, route bench scripts through it
**Duration:** ~22 min · **Branch:** `session/2026-05-26-1938-issue-36`

- Four production write sites in this repo's bench scripts used `Path.write_text` without atomicity: `bench_1000_doc.py` writes the markdown rendered into the README's "Benchmark Results" section plus a companion JSON for downstream tooling, and `bench_backpressure.py` does the same shape. A signal between the implicit `open(..., "w")` truncate and `close()` flush leaves the destination zero-length or partial — README front-page failure on GitHub, `JSONDecodeError` for the tooling.
- New `async_pipelines/io_utils.py:atomic_write_text(path, text, encoding="utf-8")` matches the portfolio standard. Both bench scripts updated to import and use it.
- Tests: 6 unit + 2 integration. Integration tests use `importlib.util` to load each script as a module — required pattern detail: the module must be registered in `sys.modules` *before* `exec_module` runs so dataclasses inside the script can find their own `__module__` during class creation (otherwise dataclasses raises AttributeError walking sys.modules). D-011 codifies the helper's placement.

**Why this work, this session:** Fourth Phase B issue of today's DAY session. Closes the python-side atomic-write gap in the four-of-twelve cohort not in the morning arc. Portfolio atomic-write coverage now at 9 of 12 repos.

**Open questions / blockers:** none — PR ready for review.

**Next session:** Two more candidates remain — `chunking-strategies-lab` (2 sites in run_matrix.py) and `vector-search-at-scale` (5 sites across load.py, harness.py, hnsw_grid.py, cost_table.py). `nextjs-streaming-ai-patterns` has no on-disk write paths to harden. Today's budget has room for both remaining sweeps to reach 12-of-12 saturation.

## 2026-05-26 — Issue #38: README decision-range upper-bound lock
**Duration:** ~6 min · **Branch:** `session/2026-05-26-2332-issue-38`

- Bumped README range citation D-010 → D-011 (catching the drift from #36).
- Added `tests/test_readme_decision_range.py` so future drift fires loud.

**Why this work, this session:** Smoking gun — D-011 had landed in #36 (the `async_pipelines.io_utils` package-level helper) but the README's `D-002…D-010` bound was stale. The architecture-doc-lock already caught the doc-side omission in Phase A; the new readme-lock now traps the sister drift class.

**Open questions / blockers:** none.
**Next session:** Continue to agent-orchestration-platform (TypeScript repo — translate the test).

## 2026-05-27 — Issue #40: drop stale "· this PR" from two README section headers + banned-phrase lock
**Duration:** ~12 min · **Branch:** `session/2026-05-27-0330-issue-40`

- Two section headers in `README.md` carried PR-time framing for shipped surface: `Tool dispatch (#2 · this PR)` (line 223), `1000-doc benchmark (#4 · this PR)` (line 268). The benchmark even has a real numbers table in `docs/benchmarks.md` — these are not PRs in flight.
- Rewrote both to steady-state form.
- New lock: `tests/test_readme_banned_phrases.py`. **Tightened pattern**: `BANNED_PHRASES = ("· this pr",)` (middle dot + space + "this pr"), not the bare `"this pr"` used in the prior three repos. Reason: this README contains the legitimate prose "Bounded queue applies backpressure to this producer." — substring match on "this pr" would false-positive there. The middle-dot pattern matches the exact section-header drift shape (`## Foo (#N · this PR)`) and doesn't collide with any prose.
- Lock test 3/3 pass. Full suite 183/183 pass. Verified loud-fail on synthetic reintroduction.

**Why this work, this session:** Iteration 6 of an autonomous NIGHT session, fourth (and final known) repo in the README banned-phrase lock propagation arc.

**Open questions / blockers:** Pattern divergence — three prior repos (`prompt-regression-suite`, `llm-cost-optimizer`, `embedding-model-shootout`) shipped with `BANNED_PHRASES = ("this pr",)`. They don't currently have the false-positive collision, but could acquire one in the future. Worth a follow-up to tighten them for portfolio uniformity.

**Next session:** Touch-up iteration to align the three prior locks to the tightened pattern.

## 2026-05-27 — Issue #42: CONTRIBUTING.md cadence-wording propagation
**Duration:** ~3 min · **PR:** #43

- Replaced pre-D-008 `~60-minute session cap` line with D-008 (180/360 min, multi-issue loop) and D-004 (Phase A PR auto-merge) wording, matching the bootstrap template post-portfolio-ops#3.

**Why this work, this session:** Iteration in the autonomous NIGHT session propagation arc for portfolio-ops#3.

**Open questions / blockers:** none.

**Next session:** continue portfolio propagation.

## 2026-06-01 — Issue #44: Workload/RunResult to_dict + dump_benchmark_json observability parity
**Duration:** ~18 min · **Branch:** `session/2026-06-01-2327-issue-44`

- Shipped `Workload.to_dict()` and `RunResult.to_dict()` on the two benchmark dataclasses, pinning the JSON contract so future internal-only fields don't leak into the JSON surface that downstream consumers parse. `RunResult.to_dict` preserves `speedup_vs_serial=None` for the serial baseline (consumers route on `None`) and shallow-copies `extra` so the frozen dataclass can't be mutated through the returned dict.
- Shipped `async_pipelines.benchmark.dump_benchmark_json(path, *, workload, results)` — package-level helper that writes the combined `{"workload": ..., "results": [...]}` payload atomically via `atomic_write_text` (D-011). JSON shape is byte-identical to the pre-#44 inline output that `scripts/bench_1000_doc.py` produced.
- Refactored `scripts/bench_1000_doc.py` to use the new helper and dropped its `dataclasses.asdict` import. `scripts/bench_backpressure.py` is intentionally untouched: its `BackpressureResult` and `StreamMetrics` shapes are different dataclasses outside this issue's `Workload`/`RunResult` scope.
- `tests/test_benchmark_dump.py` is 11 cases — including a load-bearing byte-identical-to-pre-#44 shape lock that constructs the old `asdict` payload in the test and byte-compares against the new helper's output. The atomic-write contract test monkey-patches `os.replace` to raise mid-write, then asserts no partial file remains at the destination path.
- README "1000-doc benchmark (#4)" gains one sentence about the pinned JSON surface; `docs/architecture.md` §4 gains a "JSON observability surface (#44)" paragraph cross-referencing the three sister-repo PRs that established the pattern earlier in this session.
- Live-tested with `scripts/bench_1000_doc.py --n 50 --latency 0.001 --concurrency 4 --batch-size 2`: real speedup numbers (serial 1×, async 3.8×, async+batched 7×) with byte-identical JSON shape. Full suite 194 / 194 pass, ruff clean.

**Why this work, this session:** Third iteration of the day-session loop. After the validate-pattern propagation in iterations 1-2 (emb-shootout #46, chunking-lab #38), the OTHER pattern that landed today in Phase A — the observability-parity to_dict + dump_aggregate_json shape from rag-kit #51 / cost-optimizer #51/#53 — generalizes cleanly to async-pipelines' benchmark JSON output. The fail-fast-read class of repos was already saturated; this was the next-highest-leverage gap.

**Open questions / blockers:** None — ready for review.

**Next session:** Continue the day-session loop. Remaining untouched-since-2026-05-27 candidates: `vector-search-at-scale` (no JSONL inputs, no obvious analog), `agent-orchestration-platform`, `mcp-server-cookbook`, `nextjs-streaming-ai-patterns`, `ai-app-integration-tests`. Check `agent-orchestration-platform` next per build sequence.

## 2026-06-02 — Issue #46: StreamMetrics.to_dict + BackpressureResult.to_dict (close last asdict gaps)
**Duration:** ~18 min · **Branch:** `session/2026-06-02-0403-issue-46`

- Follow-on to #45's package-level `Workload.to_dict` / `RunResult.to_dict` / `dump_benchmark_json`. Closed the two remaining `asdict` surfaces:
  - `async_pipelines/core.py`: `StreamMetrics.to_dict` pins the 5-field **public** contract and explicitly excludes the private `_started_monotonic` field. Pre-#46 `asdict(m)` silently leaked that internal timing checkpoint into the `scripts/bench_backpressure.py` JSON, which downstream operators reading the bench output would see as a confusing field with no documented meaning.
  - `scripts/bench_backpressure.py`: `BackpressureResult.to_dict` (7-field contract, `metrics` shallow-copied) replaces both `metrics=asdict(m)` (L96) and `[asdict(r) for r in results]` (L166). `asdict` import dropped.
- `scripts/bench_1000_doc.py` was already wired through `dump_benchmark_json` by #45 — no work needed there. (My initial grep showed stale asdict references but that was pre-rebase against the latest main.)
- 7 new tests: 3 in `test_stream.py` (StreamMetrics sorted-keys pin, `_started_monotonic` exclusion regression net, value round-trip) + 4 in new `test_bench_backpressure.py` (BackpressureResult sorted-keys pin, value round-trip, metrics shallow-copy guard, JSON round-trip). 201/201 pass (was 194). Ruff check + format clean.
- `grep -rn asdict scripts/ async_pipelines/` returns only documentation references (the inline comments documenting the prior asdict shape). No source-level asdict serialization remains.

**Why this work, this session:** Iteration 7 of the night session loop. After completing the observability-parity arc across `llm-cost-optimizer` (via #54), audit of `python-async-llm-pipelines` showed two parallel asdict surfaces that #45's package-level work hadn't covered. This PR completes both levels (package + script) of the arc for this repo, matching the sister-repo posture in `llm-cost-optimizer` after #54.

**Open questions / blockers:** none — ready for review.

**Next session:** Observability-parity arc fully saturated across both package and script levels in this repo. Future iterations can pivot to operator-blocked items (demo capture) or novel parity opportunities outside the asdict / to_dict arc.

## 2026-06-17 — Issue #48: Workflow YAML-parseability lock
**Duration:** ~7 min · **Branch:** `session/2026-06-17-1926-issue-48`

Added `tests/test_workflows_yaml_parseable.py` (3 tests for `ci.yml`)
and pulled `pyyaml>=6.0` into `dev` extras.

**Why this work, this session:** Ninth hop of the `portfolio-ops#30`
propagation arc.

**Open questions / blockers:** none — PR #49 open.

**Next session:** continue propagation to the remaining 3 repos.

## 2026-06-18 — Issue #50: timeout-minutes guard + lock test
**Duration:** ~10 min · **Branch:** `session/2026-06-18-0329-issue-50`

- Added `timeout-minutes: 15` to every job in `ci.yml` (`lint`, `test`, `memory-check`).
- Added `tests/test_workflows_timeout_minutes.py` — 10 new tests (1 smoke + 3 jobs × 3 parametrized invariants).

**Why this work, this session:** seventh hop in the portfolio-wide timeout-minutes propagation arc.

**Open questions / blockers:** none.

**Next session:** continue propagation. Four repos remain: agent-orchestration-platform (TS), mcp-server-cookbook (TS), ai-app-integration-tests (TS), portfolio-ops itself.
