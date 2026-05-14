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
