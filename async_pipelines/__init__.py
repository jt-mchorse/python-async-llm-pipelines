"""python-async-llm-pipelines: structured concurrency for LLM workloads.

Issue #1 surface:

    from async_pipelines import process, stream, PipelineError

Layers shipped here (#1):
- ``process(items, fn, *, concurrency, return_exceptions=False)`` — bounded
  fan-out over a finite input list; returns results in input order.
- ``stream(producer, fn, *, concurrency, queue_size)`` — bounded
  fan-out over an unbounded source, with `asyncio.Queue`-based
  backpressure on the producer.
- ``PipelineError`` — wraps the first exception when fail-fast.

Later issues:
- #2: concurrent tool-call dispatch.
- #4: 1000-doc benchmark (serial vs async vs async+batched).
"""

from .core import PipelineError, process, stream

__all__ = ["PipelineError", "process", "stream"]
