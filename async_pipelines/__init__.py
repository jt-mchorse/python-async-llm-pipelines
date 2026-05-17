"""python-async-llm-pipelines: structured concurrency for LLM workloads.

Public surface:

    from async_pipelines import process, stream, PipelineError
    # Tool dispatch (#2):
    from async_pipelines import (
        ToolCall, ToolResult, ToolRegistry, dispatch_tool_calls,
    )

Shipped layers:
- #1: ``process(items, fn, *, concurrency, return_exceptions=False)`` —
      bounded fan-out over a finite input list; returns results in input order.
- #1: ``stream(producer, fn, *, concurrency, queue_size)`` — bounded
      fan-out over an unbounded source, with `asyncio.Queue`-based
      backpressure on the producer.
- #2: ``dispatch_tool_calls(tool_calls, *, registry, return_exceptions, concurrency)`` —
      runs the model's parallel tool_use blocks concurrently inside an
      `asyncio.TaskGroup`, with optional bounded concurrency, partial-failure
      tolerance, and per-tool telemetry.

Later issues:
- #4: 1000-doc benchmark (serial vs async vs async+batched).
"""

from .benchmark import (
    AsyncPipeline,
    BatchedAsyncPipeline,
    FakeLLM,
    LLMClient,
    RunResult,
    SerialPipeline,
    Workload,
    attach_speedup,
    make_batch_caller,
    run_pipeline,
)
from .core import PipelineError, StreamMetrics, process, stream
from .tool_dispatch import (
    ToolCall,
    ToolNotFoundError,
    ToolRegistry,
    ToolResult,
    dispatch_tool_calls,
)

__all__ = [
    # Core (#1)
    "PipelineError",
    "StreamMetrics",
    "process",
    "stream",
    # Tool dispatch (#2)
    "ToolCall",
    "ToolNotFoundError",
    "ToolRegistry",
    "ToolResult",
    "dispatch_tool_calls",
    # Benchmark (#4)
    "AsyncPipeline",
    "BatchedAsyncPipeline",
    "FakeLLM",
    "LLMClient",
    "RunResult",
    "SerialPipeline",
    "Workload",
    "attach_speedup",
    "make_batch_caller",
    "run_pipeline",
]
