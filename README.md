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

This PR ships the first primitive: **`async_pipelines.process(items, fn,
concurrency=N)`** — a thin, dep-free wrapper around `asyncio.TaskGroup`
and `asyncio.Semaphore` that fans out a finite list through an async
function, capping concurrent in-flight calls, and returns results **in
the input order**. The fail-fast path uses TaskGroup's structured
cancellation; the `return_exceptions=True` path keeps the batch alive
when one bad document shouldn't lose 999 others. The companion
**`stream()`** primitive handles unbounded sources with an
`asyncio.Queue`-bounded backpressure path.

Everything beyond #1 is staged in follow-up issues: concurrent
tool-call dispatch ([#2]), the 1000-document serial-vs-async benchmark
that proves the 5–20× win ([#4]), and an Anthropic-SDK adapter that
wraps the wrapper into the actual provider's call shape. The wrapper
itself stays runtime-dep-free ([D-002]).

[#2]: https://github.com/jt-mchorse/python-async-llm-pipelines/issues/2
[#4]: https://github.com/jt-mchorse/python-async-llm-pipelines/issues/4
[D-002]: MEMORY/core_decisions_human.md

## Architecture

```
async_pipelines/
├── core.py
│   ├── process(items, fn, *, concurrency, return_exceptions=False) -> list
│   └── stream(producer, fn, *, concurrency, queue_size, return_exceptions=False) -> list
└── tool_dispatch.py    ← #2
    ├── ToolCall, ToolResult, ToolRegistry
    └── dispatch_tool_calls(tool_calls, *, registry, return_exceptions, concurrency) -> list[ToolResult]
```

See [`docs/architecture.md`](docs/architecture.md) for the mermaid of
shipped (#1, #2) vs pending (#4) layers.

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

## Benchmarks / Results

*The serial-vs-async-vs-batched 1000-document benchmark is pending
issue [#4]. The wrapper's concurrency-ceiling enforcement is unit-tested
here (`test_concurrency_ceiling_is_enforced` and
`test_concurrency_speedup_is_real`), but the published 5–20× win
number requires a real LLM-call workload that isn't in scope for this
PR.*

## Demo

*60-second demo pending — depends on issue [#4].*

## Why these decisions

See [`MEMORY/core_decisions_human.md`](MEMORY/core_decisions_human.md).

## License

MIT
