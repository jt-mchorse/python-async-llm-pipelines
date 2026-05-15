"""Concurrent tool-call dispatch.

When a model returns multiple `tool_use` blocks, calling them serially throws
away the whole point of having more than one — they're independent (or the
model wouldn't have requested them in parallel). `dispatch_tool_calls` runs
them concurrently inside an `asyncio.TaskGroup` with optional bounded
concurrency, then maps results back to the original `tool_call_id`s so the
caller can hand them straight back to the model.

Two failure modes:

- Default fail-fast: the first tool that raises propagates, wrapped in
  `PipelineError`. Same semantics as `async_pipelines.process()` (D-003 from #1).
- `return_exceptions=True`: each tool's exception is captured on the
  corresponding `ToolResult` and the batch completes. This is the
  "partial failures don't poison the batch" mode the issue calls for.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass, field
from typing import Any

from .core import PipelineError

# A tool function is async, takes a single dict of arguments, and returns
# anything JSON-serializable (whatever shape the model expects back).
ToolFn = Callable[[dict[str, Any]], Awaitable[Any]]


# ----------------------------------------------------------------------
# Types
# ----------------------------------------------------------------------


@dataclass(frozen=True)
class ToolCall:
    """One tool invocation request from the model."""

    id: str
    name: str
    arguments: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class ToolResult:
    """One tool invocation result.

    `ok=True` → `value` is the tool's return.
    `ok=False` → `error_repr` is `repr(exception)`; `value` is None.
    """

    tool_call_id: str
    name: str
    ok: bool
    value: Any = None
    error_repr: str | None = None
    elapsed_ms: float = 0.0


class ToolNotFoundError(KeyError):
    """Raised when a `ToolCall.name` isn't registered in the dispatcher's `ToolRegistry`."""


# ----------------------------------------------------------------------
# Registry
# ----------------------------------------------------------------------


class ToolRegistry:
    """Name → async-callable map."""

    def __init__(self) -> None:
        self._tools: dict[str, ToolFn] = {}

    def register(self, name: str, fn: ToolFn) -> None:
        if not name or not isinstance(name, str):
            raise ValueError(f"tool name must be a non-empty string; got {name!r}")
        if name in self._tools:
            raise ValueError(f"tool {name!r} already registered")
        self._tools[name] = fn

    def tool(self, name: str) -> Callable[[ToolFn], ToolFn]:
        """Decorator form: `@registry.tool("web_fetch")`."""

        def decorator(fn: ToolFn) -> ToolFn:
            self.register(name, fn)
            return fn

        return decorator

    def get(self, name: str) -> ToolFn:
        if name not in self._tools:
            raise ToolNotFoundError(f"tool {name!r} not registered")
        return self._tools[name]

    def names(self) -> list[str]:
        return list(self._tools.keys())

    def __contains__(self, name: object) -> bool:
        return isinstance(name, str) and name in self._tools

    def __len__(self) -> int:
        return len(self._tools)


# ----------------------------------------------------------------------
# Dispatch
# ----------------------------------------------------------------------


async def dispatch_tool_calls(
    tool_calls: Sequence[ToolCall],
    *,
    registry: ToolRegistry,
    return_exceptions: bool = False,
    concurrency: int | None = None,
) -> list[ToolResult]:
    """Run every `ToolCall` against `registry` concurrently.

    Args:
      tool_calls: the model's requested tool invocations.
      registry: name → async-callable lookup.
      return_exceptions: if True, capture each tool's exception on the
        corresponding ToolResult and continue. If False (default), the first
        exception propagates wrapped in PipelineError.
      concurrency: optional upper bound on simultaneous in-flight calls.
        None (default) means unbounded — each tool runs as soon as it's
        scheduled.

    Returns: ToolResult list in input order; `tool_call_id` matches each input.
    """
    if concurrency is not None and concurrency <= 0:
        raise ValueError(f"concurrency must be positive or None; got {concurrency}")

    if not tool_calls:
        return []

    # Pre-resolve all tool functions so a missing name fails up-front rather
    # than mid-batch (cleaner error surface; same fail-fast spirit).
    resolved: list[tuple[ToolCall, ToolFn]] = []
    for call in tool_calls:
        try:
            fn = registry.get(call.name)
        except ToolNotFoundError:
            if not return_exceptions:
                raise
            resolved.append((call, _make_missing_tool_stub(call.name)))
            continue
        resolved.append((call, fn))

    semaphore = asyncio.Semaphore(concurrency) if concurrency is not None else None
    results: list[ToolResult | None] = [None] * len(resolved)

    async def _run_one(idx: int, call: ToolCall, fn: ToolFn) -> None:
        result = await _invoke_tool(call, fn, semaphore, return_exceptions=return_exceptions)
        results[idx] = result

    try:
        async with asyncio.TaskGroup() as tg:
            for idx, (call, fn) in enumerate(resolved):
                tg.create_task(_run_one(idx, call, fn))
    except* Exception as eg:
        # Default fail-fast: surface the first non-exception-recorded failure
        # via PipelineError (parity with async_pipelines.process()).
        first = eg.exceptions[0]
        if isinstance(first, PipelineError):
            raise first from eg
        raise PipelineError(repr(first)) from first

    # All slots populated by now (TaskGroup awaited every spawn).
    return [r for r in results if r is not None]


def _make_missing_tool_stub(name: str) -> ToolFn:
    async def _missing(args: dict[str, Any]) -> Any:
        raise ToolNotFoundError(f"tool {name!r} not registered")

    return _missing


async def _invoke_tool(
    call: ToolCall,
    fn: ToolFn,
    semaphore: asyncio.Semaphore | None,
    *,
    return_exceptions: bool,
) -> ToolResult:
    if semaphore is not None:
        async with semaphore:
            return await _run_with_telemetry(call, fn, return_exceptions=return_exceptions)
    return await _run_with_telemetry(call, fn, return_exceptions=return_exceptions)


async def _run_with_telemetry(call: ToolCall, fn: ToolFn, *, return_exceptions: bool) -> ToolResult:
    start = time.perf_counter()
    try:
        value = await fn(call.arguments)
    except Exception as e:
        elapsed_ms = (time.perf_counter() - start) * 1000.0
        if not return_exceptions:
            raise  # let TaskGroup collect it; outer raises PipelineError
        return ToolResult(
            tool_call_id=call.id,
            name=call.name,
            ok=False,
            value=None,
            error_repr=repr(e),
            elapsed_ms=elapsed_ms,
        )
    elapsed_ms = (time.perf_counter() - start) * 1000.0
    return ToolResult(
        tool_call_id=call.id,
        name=call.name,
        ok=True,
        value=value,
        error_repr=None,
        elapsed_ms=elapsed_ms,
    )
