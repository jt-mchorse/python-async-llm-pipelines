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
