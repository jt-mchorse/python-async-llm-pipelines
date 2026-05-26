# Core Decisions

Strategic decisions for this repo, with reasoning. Append-only — superseded decisions are marked, not removed.

## D-001 — Scope locked to portfolio handoff §2 (2026-05-10)
**Decision:** Scope of this repo is fixed by the portfolio handoff document, section 2.

**Why:** The handoff spec was deliberated; ad-hoc scope expansion within a session is the failure mode this prevents.

**Alternatives considered:** None — this is a baseline.

**Reversibility:** Expensive. Scope changes require a deliberate revisit and a new decision entry.

**Related issues:** —

## D-002 — Wrapper is runtime-dep-free (2026-05-14)
**Decision:** `async_pipelines` requires no third-party runtime dependencies. It uses stdlib `asyncio` only. Provider SDKs (Anthropic, OpenAI), HTTP clients (`httpx`), and serialization libraries are not required to use the wrapper.

**Why:** The wrapper's value is the *concurrency primitive*, which is decoupled from any particular provider. Bundling a specific SDK would force every downstream consumer to install that SDK even if they call a different provider, which is the failure mode this decision avoids. The concrete provider adapters (e.g., an Anthropic-SDK adapter that wraps `process()` into a `messages.create` call shape) can live behind optional extras or in separate companion packages.

**Alternatives considered:**
- Bundle the Anthropic SDK as a required dep — rejected; locks out OpenAI/Google/local-model users.
- Bundle `httpx` as a required dep — rejected; the wrapper doesn't make HTTP calls itself; it calls the caller's `fn`.

**Reversibility:** Cheap. Adding a required dep is a one-line edit; the API surface doesn't change.

**Related issues:** #1, #2, #4.

## D-003 — `process()` returns input-order, `stream()` returns completion-order (2026-05-14)
**Decision:** `process(items, fn, ...)` returns its result list in input order. `stream(producer, fn, ...)` returns results in completion order, not producer order.

**Why:** The two primitives have different consumer mental models. `process` callers usually need to correlate outputs back to specific inputs (e.g., "give me the embedding for each of these documents, in the order I gave them"); requiring them to track the index themselves is a worse default than just preserving order. `stream` callers, by contrast, are draining an unbounded source — there is rarely a meaningful "producer order" to honor, and the bounded-queue semantics that make backpressure work would be defeated if completion ordering had to wait for indexed slot fills.

The mismatch is intentional: it forces the user to pick the right primitive for the shape of their input.

**Alternatives considered:**
- `asyncio.as_completed`-style unordered iterator for both — rejected for `process`; the typical LLM workload wants outputs correlated back to inputs without a side dict.
- Input-order results for `stream` as well — rejected; would defeat backpressure (a slow item would block the result-list slot it occupies, preventing later items from being delivered).

**Reversibility:** Cheap. Either primitive's return shape can be changed (or augmented with a flag) without breaking the other.

**Related issues:** #1, #4.

## D-004 — `ToolRegistry` is a thin dict wrapper; tools are async callables (2026-05-15)
**Decision:** `ToolRegistry` is a `dict[str, AsyncCallable]` with `register(name, fn)`, decorator-form `tool(name)`, `get(name)`. No abstract base class, no schema-first registration.

**Why:** Tools are just async functions taking a dict. ABC ceremony for a one-method shape is noise. Decorator form reads naturally and matches the rest of the portfolio's single-method-Protocol seams. Schema-first validation is a per-tool concern; the dispatcher doesn't need to know.

**Alternatives considered:**
- Abstract `Tool` base class — rejected: ceremony for a one-function shape.
- DI container — rejected: massive overkill.
- OpenAPI/JSON-Schema-first registry — rejected: per-tool validation belongs in the tool.

**Reversibility:** Cheap. Adding optional `schema` to `register()` is backwards-compatible.

**Related issues:** #2

## D-005 — `ToolResult` carries `tool_call_id` (2026-05-15)
**Decision:** Every `ToolResult` carries the `tool_call_id` of the `ToolCall` that produced it.

**Why:** Anthropic's `tool_use` / `tool_result` round-trip requires the `tool_use_id` echoed back in the next turn. Without an id on the result, every caller reconstructs the correlation map themselves — error-prone boilerplate. With the id, callers do `ToolResult` → `{"type": "tool_result", "tool_use_id": r.tool_call_id, ...}` directly.

**Alternatives considered:**
- Results in input order with no id — rejected: breaks if any caller filters or reorders.
- Results keyed by position — rejected: same problem.

**Reversibility:** Cheap.

**Related issues:** #2

## D-006 — Default fail-fast; `return_exceptions=True` opt-in for partial tolerance (2026-05-15)
**Decision:** `dispatch_tool_calls()` defaults to fail-fast (parity with `process()` D-003). `return_exceptions=True` opts into partial tolerance.

**Why:** Two-mode design keeps the package's defaults consistent — every primitive fails fast by default, with the partial-tolerance opt-in available. Issue #2's acceptance criterion ("partial failures don't poison the batch") is satisfied by the opt-in. Defaulting to partial tolerance would invert the safety posture: callers who didn't deliberately handle errors would silently get incomplete results.

**Alternatives considered:**
- Default partial tolerance, opt-in fail-fast — rejected: silent failures are a worse default.
- Two separate functions — rejected: API surface bloat.

**Reversibility:** Cheap.

**Related issues:** #2

## D-007 — Benchmark ships with `FakeLLM` for CI; real-API is an operator swap (2026-05-16)
**Decision:** The 1000-doc benchmark in `scripts/bench_1000_doc.py` defaults to a `FakeLLM` that simulates per-call latency via `asyncio.sleep`. The committed `docs/benchmarks.md` numbers are real measurements from this synthetic configuration. Real-Anthropic-API numbers come from the operator swapping in an `AnthropicLLM` adapter that conforms to the `LLMClient` Protocol and re-running the same script.

**Why:** The spec's "5–20× win" is a *speedup ratio*, not an absolute latency. The ratio is reproducible under deterministic synthetic latency — and what's load-bearing about the claim is the *shape* of the curve (serial scales linearly, async scales sub-linearly, batched scales asymptotically with batch size). Burning Anthropic API budget on N=1000 calls just to confirm the ratio would (a) hit rate limits, (b) cost real dollars per CI run, and (c) introduce non-determinism. The honest path: ship the synthetic harness with real measured numbers and a documented swap.

**Alternatives considered:**
- Require `ANTHROPIC_API_KEY` in CI — rejected: burns budget, hits rate limits, non-deterministic.
- No benchmark at all — rejected: misses the v0.1 claim the issue body explicitly asks for.
- Ship fabricated numbers — rejected: violates the no-fabricated-benchmarks rule (handoff §10).

**Reversibility:** Cheap. The script's `_run_all` helper instantiates `FakeLLM`s; swapping that for `AnthropicLLM(...)` is two lines.

**Related issues:** #4

## D-008 — `BatchedAsyncPipeline` uses one round trip per batch (2026-05-16)
**Decision:** The batched pipeline collects `batch_size` documents into one consolidated LLM call per batch. `make_batch_caller(llm)` returns a callable that takes a list of items and (in the synthetic case) does **one** `asyncio.sleep` regardless of batch size, simulating the Anthropic Batch API shape where one request processes N inputs. A real implementation swaps this for the actual batch endpoint.

**Why:** Without one-round-trip-per-batch semantics, "batched" is just "chunked async" and the third pipeline is identical to the second. The batched pipeline exists in the matrix specifically to model the Batch-API workload (nightly evals, offline jobs) where the per-request overhead amortizes across many inputs. The synthetic seam captures that asymmetry correctly.

**Alternatives considered:**
- "Batched" = chunked async with per-item calls inside the chunk — rejected: redundant with `AsyncPipeline`, doesn't model the real win.
- One `messages.create` per item inside a batch — same redundancy.

**Reversibility:** Cheap. The batch caller is a one-function wrapper; replacing it is a single source change.

**Related issues:** #4

## D-009 — `StreamMetrics` is an in-place dataclass passed via a keyword-only argument, default `None` (2026-05-17)
**Decision:** `stream`'s backpressure observability surface (#3) is a stdlib `dataclass` named `StreamMetrics` exposed at the package's top level. `stream` takes an optional keyword-only `metrics: StreamMetrics | None = None`; when non-None, the function writes counters and timings into it in-place during the run. `metrics=None` (the default) keeps the v0 signature backward-compatible and skips the instrumentation entirely.

**Why:** Three forces compose. (1) **Backward compatibility.** `stream` already shipped in #1 returning `list[R | BaseException]`; changing the return shape to a `(results, metrics)` tuple would silently break both existing callsites in `tests/test_stream.py` and any operator who copied the snippet from the README. An optional in-place dataclass is the smallest change that adds the new capability. (2) **D-002 alignment.** The wrapper is committed to runtime-dep-free; `dataclass` is stdlib so the metrics surface doesn't pull in `pydantic`, `attrs`, or anything else. (3) **The semantic fits.** Backpressure metrics are a snapshot of the *single* call to `stream` — they make no sense returned alongside results because they're a property of the *pipeline*, not of any individual output. Threading them through a passed-in collector matches what observability libraries (statsd, prometheus client) do for the same reason.

**Alternatives considered:**
- Return a `(results, metrics)` tuple from `stream` — rejected: breaks the two existing tests and the README snippet for zero capability benefit beyond what the in-place collector gives.
- Put `StreamMetrics` in a separate `metrics.py` module — rejected: it's a 30-line dataclass and only `stream` writes to it. Splitting them across modules adds an import edge for no readability win.
- Per-event callback API (`metrics_callback=callable`) — rejected: overkill for a demo repo, harder to reason about (callback timing relative to `queue.put` is non-obvious), and the dataclass surfaces *exactly* the four numbers operators care about (pauses, max depth, pause time, totals) without further abstraction.

**Reversibility:** Cheap. `StreamMetrics` is one dataclass and the instrumentation is a handful of lines inside `_produce`/`_consume`. Removing it is a smaller change than adding it was.

**Related issues:** #3

## D-010 — Per-item timeout is a kwarg on `process` / `stream`, not a separate decorator (2026-05-18)

**Decision:** Per-item timeouts are configured via `process(..., timeout=N)` and `stream(..., timeout=N)`, raising `PipelineTimeoutError` (subclass of `PipelineError`). Not a separate `timed(fn, timeout)` decorator the caller wraps `fn` with.

**Why:** The timeout has to interact with the *existing* fail-fast / opt-in partial-tolerance policy from D-003 + D-006. Putting it on the primitive means the same `return_exceptions=True` toggle that lets per-item exceptions survive also lets per-item timeouts survive — one mental model, one knob, one API surface. A `timed(fn, timeout)` decorator would force callers to wrap *twice* (once for the timeout, once for `process`) and would either silently swallow the timeout or raise a generic `TimeoutError` with no item-index correlation. The kwarg shape also mirrors `asyncio.wait_for(coro, timeout)` directly — caller intuition transfers. `timeout=None` is the default and is byte-identical with the pre-#5 path: no `wait_for` wrapping, no overhead, no regression risk for existing callsites.

**Alternatives considered:**
- Separate `timed(fn, timeout)` decorator — rejected: doubles the API surface; doesn't compose with `return_exceptions` without re-implementing exception classification inside the decorator.
- Batch-level deadline (`process(..., batch_deadline=N)`) — rejected: the use case for #5 is *"one slow item shouldn't fail the batch"*, which is per-item by definition. A batch-level deadline is trivially recoverable from the outside (`asyncio.wait_for(process(...), batch_deadline)`) without baking it into the primitive.
- Throw `asyncio.TimeoutError` directly — rejected: no carried `index`, no carried `timeout_s`. Breaks the correlation contract every other failure mode in this repo upholds (e.g., `ToolResult.tool_call_id` from D-005).

**Reversibility:** Cheap. `timeout=None` is the original code path. Removing the feature is "delete the `if timeout is None` branches and the kwarg" — about 20 lines.

**Related issues:** #5

## D-011 — Atomic-write helper lives in `async_pipelines/io_utils.py` (2026-05-26)
**Decision:** The atomic-write helper for this repo lives at `async_pipelines/io_utils.py`, exposing public `atomic_write_text(path, text, encoding="utf-8")`. Pattern matches the 2026-05-26 portfolio atomic-write arc: `rag_kit/io_utils.atomic_write_text` (rag-production-kit#44/#45), `eval_harness/io_utils.atomic_write_text` (llm-eval-harness#51, D-015), `emb_shootout/io_utils.atomic_write_text` (embedding-model-shootout#37, D-009), `prompt_regression/io.atomic_write_text` (prompt-regression-suite#40).

**Why:** Four production write sites in `scripts/` needed atomicity. Placing the helper at the package level (rather than file-private to one script) lets both bench scripts (and any future caller) reach it, and centralizes the test surface — one `io_utils.os.replace` to monkey-patch rather than one per call site.

**Alternatives considered:**
- File-private helper per script — rejected; ~25-line duplication across two scripts that can drift, and fragments the test surface.
- Inline the pattern at each call site — rejected; same drift hazard, no central seam.
- Separate distribution package — rejected; over-engineering for one repo with one consumer.

**Reversibility:** Cheap. Stable API; future evolution is localized.

**Related issues:** #36
