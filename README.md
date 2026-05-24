# python-async-llm-pipelines
> Performance patterns for concurrent LLM workloads: bounded async batching, concurrent tool dispatch, backpressure, structured concurrency benchmarks.

![CI](https://github.com/jt-mchorse/python-async-llm-pipelines/actions/workflows/ci.yml/badge.svg)
![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)

## What this is

A pipeline that calls an LLM once per document, serially, on a 10,000-document
workload is bottlenecked entirely on network latency. The default Python
shape for that loop — a synchronous `for doc in docs:` — leaves most of
the speedup on the floor. Replacing it with a structured-concurrency
fan-out, bounded by the provider's rate limit, is one of the
highest-leverage refactors in any LLM-heavy codebase. This repo is the
reference for the patterns that actually work in production.

Today five primitives ship, each one closing the issue named in
parentheses:

- **`async_pipelines.process(items, fn, concurrency=N)`** (#1) — a
  thin, dep-free wrapper around `asyncio.TaskGroup` and
  `asyncio.Semaphore` that fans out a finite list through an async
  function, capping concurrent in-flight calls, and returns results
  **in the input order**. The fail-fast path uses TaskGroup's
  structured cancellation; the `return_exceptions=True` path keeps the
  batch alive when one bad document shouldn't lose 999 others.
- **`async_pipelines.stream(...)`** (#1, sibling) — unbounded-source
  variant with an `asyncio.Queue`-bounded backpressure path.
- **`async_pipelines.tool_dispatch.dispatch_tool_calls(...)`** (#2) —
  concurrent tool-call dispatcher with a `ToolRegistry` Protocol so the
  same wrapper handles parallel tool execution without reimplementing
  the fan-out logic.
- **Bounded `stream()` with backpressure metrics** (#3) — `metrics=...`
  parameter records queue depth, wait time at the producer, and stall
  counts so an operator can see *which* end of the pipeline is the
  bottleneck instead of guessing.
- **1000-document benchmark** (#4) — `scripts/bench_1000_doc.py`
  produces a serial-vs-async-vs-async+batched table on a synthetic
  workload. Result on the committed run: ~30× speedup at
  `concurrency=32`, which is the theoretical upper bound for the
  `FakeLLM` (pure-wait `asyncio.sleep`). Real-API speedups land in the
  5–20× spec range; the script is the seam an operator swaps for their
  own `LLMClient` Protocol implementation.
- **Per-item timeouts + cooperative cancellation** (#5) — `timeout`
  parameter on both `process()` and `stream()` raises
  `PipelineTimeoutError` at the per-item level without poisoning the
  rest of the batch; structured-concurrency teardown ensures no
  orphaned tasks.

The wrapper itself stays runtime-dep-free (D-002). The architecture
diagram below names the five layers and the D-002…D-010 decisions
behind each one.

[#1]: https://github.com/jt-mchorse/python-async-llm-pipelines/issues/1
[#2]: https://github.com/jt-mchorse/python-async-llm-pipelines/issues/2
[#3]: https://github.com/jt-mchorse/python-async-llm-pipelines/issues/3
[#4]: https://github.com/jt-mchorse/python-async-llm-pipelines/issues/4
[#5]: https://github.com/jt-mchorse/python-async-llm-pipelines/issues/5
[D-002]: MEMORY/core_decisions_human.md
[D-003]: MEMORY/core_decisions_human.md

## Architecture

```
async_pipelines/
├── core.py
│   ├── process(items, fn, *, concurrency, return_exceptions=False, timeout=None) -> list
│   └── stream(producer, fn, *, concurrency, queue_size, return_exceptions=False, timeout=None, metrics=None) -> list
└── tool_dispatch.py    ← #2
    ├── ToolCall, ToolResult, ToolRegistry
    └── dispatch_tool_calls(tool_calls, *, registry, return_exceptions, concurrency, timeout=None) -> list[ToolResult]
```

See **[`docs/architecture.md`](docs/architecture.md)** for the integrated pipeline lifecycle, per-layer detail across all five shipped primitives (process/stream #1, tool dispatch #2, backpressure metrics #3, 1000-doc benchmark #4, per-item timeouts #5), and the D-002…D-010 design decisions behind each one.

## Quickstart

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -e '.[dev]'
ruff check . && ruff format --check . && pytest
```

The wrapper in 10 lines:

```python
import asyncio
from async_pipelines import process

async def call_llm(doc: str) -> str:
    # Replace with your real provider call.
    await asyncio.sleep(0.5)
    return doc.upper()

async def main():
    docs = [f"doc-{i}" for i in range(100)]
    # 10 concurrent in-flight calls, results returned in input order.
    results = await process(docs, call_llm, concurrency=10)
    print(len(results), "results, first:", results[0])

asyncio.run(main())
```

The bounded-stream variant:

```python
from async_pipelines import stream

async def items_from_kafka():
    while True:
        # Bounded queue applies backpressure to this producer.
        yield await consumer.next()

results = await stream(items_from_kafka(), call_llm, concurrency=10, queue_size=50)
```

## Backpressure (#3)

`stream`'s bounded `asyncio.Queue` is the backpressure mechanism: when
the consumer pool can't drain as fast as the producer emits, the
producer's `queue.put` *blocks* until a consumer pulls. Peak items in
memory are bounded by `queue_size` regardless of how many items the
producer would emit — that's the OOM-safety invariant for pointing this
at an unbounded source.

Pass an optional `StreamMetrics` to observe the backpressure signal:

```python
from async_pipelines import StreamMetrics, stream

metrics = StreamMetrics()
results = await stream(
    items_from_kafka(),
    call_llm,
    concurrency=10,
    queue_size=50,
    metrics=metrics,
)
print(
    f"produced={metrics.produced} consumed={metrics.consumed} "
    f"pauses={metrics.producer_pauses} "
    f"max_depth={metrics.max_queue_depth} "
    f"pause_s={metrics.producer_pause_seconds:.2f}"
)
```

`producer_pauses` is the count of `queue.put`s that had to wait for
room; `producer_pause_seconds` is the cumulative wall-time blocked. A
non-zero pause count is the operator's signal that fan-out has more
headroom than the consumer can use — either grow `concurrency` or
accept the queue as the rate limiter.

Real numbers from `scripts/bench_backpressure.py` (Apple Silicon,
CPython 3.14, 5000 items × 1 ms consumer sleep, concurrency 2):

| queue_size | duration_s | peak_heap_kb | producer_pauses | max_queue_depth |
| ---------: | ---------: | -----------: | --------------: | --------------: |
| 8 | 3.051 | 201.7 | 2707 | **8** |
| 32 | 3.080 | 198.2 | 2672 | **32** |

`max_queue_depth` matches `queue_size` exactly in both rows — the
producer fills the queue and waits for the consumer, never queueing
ahead. Full report in [`docs/backpressure.md`](docs/backpressure.md);
raw JSON alongside in `docs/backpressure.json`.

```bash
python scripts/bench_backpressure.py --n 5000 --queue-size 8 \
    --consumer-ms 1 --concurrency 2 --compare
```

## Timeouts & cancellation (#5)

Real LLM workloads have one item that hangs. The provider returns 200 OK
and then stops streaming. The rate limiter pauses for 90 seconds. A
single misbehaving doc shouldn't keep the whole batch on the runway —
and the batch shouldn't leak tasks when it gives up.

Both primitives accept an optional `timeout` (seconds). It's a *per-item*
deadline, enforced via `asyncio.wait_for` around each `fn(item)` call.
When the deadline fires, the wrapper raises `PipelineTimeoutError`
(a subclass of `PipelineError`) carrying the failing item's index and
the timeout that fired. The exception then follows the existing
`return_exceptions` policy: fail-fast by default (the batch is cancelled
via TaskGroup), or in-line at the offending index when
`return_exceptions=True`.

```python
from async_pipelines import process, PipelineTimeoutError

async def call_llm(doc: str) -> str:
    ...  # may hang

# Fail-fast: one slow item cancels the batch and PipelineTimeoutError
# propagates with .index and .timeout_s on it.
try:
    results = await process(docs, call_llm, concurrency=10, timeout=5.0)
except* PipelineTimeoutError as eg:
    for exc in eg.exceptions:
        print(f"item {exc.index} timed out after {exc.timeout_s}s")

# Or keep the batch alive — slow items land as exceptions in-line.
results = await process(
    docs, call_llm, concurrency=10, timeout=5.0, return_exceptions=True
)
ok = [r for r in results if not isinstance(r, BaseException)]
slow = [r for r in results if isinstance(r, PipelineTimeoutError)]
```

**Cancellation is structured.** Whether the deadline fires or the outer
task is cancelled from somewhere else, every in-flight `fn(item)`
observes `CancelledError` and runs its `finally` block before the
`process()` call returns. The regression tests in
[`tests/test_timeouts.py`](tests/test_timeouts.py) instrument both paths
with started/finished counters and assert `finished == started` —
the load-bearing "no orphaned tasks" invariant. The same property holds
for `stream()`; consumers get an index assigned at consumption time
(not producer time, since `stream` returns in completion order per
[D-003]).

`timeout=None` is the default and is byte-identical with the pre-#5
shape — no `wait_for` wrapping, no overhead. The new behavior is opt-in.

## Tool dispatch (#2 · this PR)

When the model returns multiple `tool_use` blocks, calling them serially
throws away the whole point of having more than one. `dispatch_tool_calls`
runs them concurrently inside an `asyncio.TaskGroup`, with optional
bounded concurrency, partial-failure tolerance, and per-tool telemetry.

```python
from async_pipelines import ToolCall, ToolRegistry, dispatch_tool_calls

registry = ToolRegistry()

@registry.tool("web_fetch")
async def web_fetch(args: dict) -> dict:
    ...  # your real implementation

@registry.tool("file_read")
async def file_read(args: dict) -> str:
    ...

# Translate the model's tool_use blocks into ToolCall objects (preserve ids):
calls = [
    ToolCall(id="toolu_01", name="web_fetch", arguments={"url": "..."}),
    ToolCall(id="toolu_02", name="file_read", arguments={"path": "..."}),
]

# Concurrent dispatch; partial failures don't poison the batch.
results = await dispatch_tool_calls(calls, registry=registry, return_exceptions=True)
for r in results:
    print(r.tool_call_id, r.ok, r.elapsed_ms, "ms")
```

Default behavior is fail-fast (parity with `process`); pass
`return_exceptions=True` for the partial-tolerance mode. The
`tool_call_id` round-trips through `ToolResult` so callers can map results
straight back to the model's `tool_use_id`s when constructing the next
turn's messages.

Bound a misbehaving tool with `timeout` (parity with `process()` and
`stream()`): `await dispatch_tool_calls(calls, registry=registry, timeout=2.0)`
wraps each tool invocation in `asyncio.wait_for`; expiry raises
`PipelineTimeoutError(index=…, timeout_s=…)` (propagates by default,
attached to the matching `ToolResult.error_repr` under
`return_exceptions=True`).

## 1000-doc benchmark (#4 · this PR)

`scripts/bench_1000_doc.py` runs all three pipelines on a workload of
N=1000 documents × 2 LLM calls per doc, with the default `FakeLLM`
simulating 20 ms per call. Real measured numbers on Apple Silicon,
CPython 3.14, concurrency 32, batch size 8:

| pipeline | duration (s) | docs/s | speedup vs serial |
| -------- | -----------: | -----: | ----------------: |
| serial | 43.311 | 23.1 | 1.00× |
| async | 1.427 | 700.5 | **30.34×** |
| async+batched | 0.172 | 5800.1 | **251×** |

The full report (with the host + date provenance lines) lives in
[`docs/benchmarks.md`](docs/benchmarks.md); raw JSON is alongside in
`docs/benchmarks.json`.

**Honest framing on the numbers.** The spec's range of "5–20× win"
assumes real-API I/O, which has per-request overhead (TCP, TLS, JSON
parsing) that bounds fan-out. The synthetic FakeLLM's `asyncio.sleep`
is pure-wait, so the speedup ratio is the theoretical upper bound —
30× is what `process(items, fn, concurrency=32)` can do when each
"call" is literally just a sleep. Real production speedups land in the
spec's range, sometimes higher (Batch API workloads). The script is
the seam where an operator swaps `FakeLLM` for an `AnthropicLLM`
adapter (anything matching the `LLMClient` Protocol) and re-runs to
get their workload's real numbers — same table shape, same reproducer.

```bash
python scripts/bench_1000_doc.py --n 1000 --concurrency 32 --batch-size 8
```

## Demo

Today's hermetic demo is two commands on a fresh clone, both runnable
without an API key:

```bash
# The full primitive surface (process, stream, tool_dispatch,
# backpressure, per-item timeouts) covered by unit tests.
pytest

# The 1000-document serial-vs-async-vs-async+batched bench table.
# --out keeps the run from mutating the committed docs/benchmarks.md
# that test_bench_table_snapshot.py locks; omit it only when you
# intend to refresh the committed snapshot.
python scripts/bench_1000_doc.py --n 1000 --concurrency 32 --batch-size 8 \
  --out /tmp/bench.md
```

The first prints the test summary; the second prints the
serial-vs-async-vs-batched comparison table that demonstrates the ~30×
speedup the synthetic `FakeLLM` allows. For the deterministic 60-second
recording, `bash scripts/capture_demo.sh` runs both surfaces with a
smaller `--n 200` workload (the *ratios* are the load-bearing claim;
absolute durations scale with the synthetic per-call latency) and a
per-run tempdir so re-records can't trip the snapshot test — see
[#14], and the smoke test at `tests/test_capture_demo_smoke.py` that
pins each surface's distinctive output so the demo can't bitrot.

[#14]: https://github.com/jt-mchorse/python-async-llm-pipelines/issues/14

## Why these decisions

See [`MEMORY/core_decisions_human.md`](MEMORY/core_decisions_human.md).

## License

MIT
