"""Concurrent tool-call dispatch.

When a model returns multiple `tool_use` blocks, calling them serially throws
away the whole point of having more than one — they're independent (or the
model wouldn't have requested them in parallel). `dispatch_tool_calls` runs
them concurrently inside an `asyncio.TaskGroup` with optional bounded
concurrency, then maps results back to the original `tool_call_id`s so the
caller can hand them straight back to the model.

Two failure modes:

- Default fail-fast: the first tool that raises propagates wrapped in
  `PipelineError`, with the original exception as `__cause__`. An
  unregistered `ToolCall.name` is the exception to that: it raises
  `ToolNotFoundError` bare, before any tool runs.
  The *policy* mirrors `async_pipelines.process()` (D-003 from #1); the
  *exception shape* does not — `process` lets TaskGroup's `ExceptionGroup`
  through with the original exception inside, so `except* ValueError`
  works there and not here (#90).
- `return_exceptions=True`: each tool's exception is captured on the
  corresponding `ToolResult` and the batch completes. This is the
  "partial failures don't poison the batch" mode the issue calls for.
"""

from __future__ import annotations

import asyncio
import copy
import time
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass, field
from typing import Any

from .core import PipelineError, PipelineTimeoutError, _require_timeout_seconds

# A tool function is async, takes a single dict of arguments, and returns
# anything JSON-serializable (whatever shape the model expects back).
ToolFn = Callable[[dict[str, Any]], Awaitable[Any]]


# ----------------------------------------------------------------------
# Types
# ----------------------------------------------------------------------


@dataclass(frozen=True)
class ToolCall:
    """One tool invocation request from the model.

    **The record of a request, and it stays that.** `frozen=True` says this
    object cannot change, and `arguments` is its one mutable field -- the same
    shape `#100` fixed on `RunResult.extra`. Measured before this (#102)::

        src = {"nested": {"k": "original"}}
        call = ToolCall(id="t1", name="search", arguments=src)
        src["nested"]["k"] = "MUTATED"; src["added"] = "AFTER"
        -> call.arguments == {'nested': {'k': 'MUTATED'}, 'added': 'AFTER'}

    A *new top-level key* on a frozen object after construction.

    Deep, not shallow: `#100` measured that shallow was "exactly the half that
    failed". `object.__setattr__` because the dataclass is frozen; this is the
    documented way to normalize a field in a frozen `__post_init__`.

    The matching egress copy lives in `_run_with_telemetry`, which hands `fn` a
    per-invocation copy rather than this dict -- see there for why that one is
    the half that makes results independent of `concurrency`.

    This docstring used to end the sentence above with "which is this package's
    only other frozen dataclass with a mutable field". There are **three**, and
    the third is `ToolResult` immediately below (#106). A scoped, true-sounding
    count reads as a completed enumeration, so nobody re-counts -- which is how
    a class in the same file stayed invisible.
    """

    id: str
    name: str
    arguments: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "arguments", copy.deepcopy(self.arguments))


@dataclass(frozen=True)
class ToolResult:
    """One tool invocation result.

    `ok=True` → `value` is the tool's return.
    `ok=False` → `error_repr` is `repr(exception)`; `value` is None.

    **`value` is the tool's own object, not a snapshot of it, and that is
    currently unresolved rather than decided (#106).** This is the third frozen
    dataclass in the package with a mutable field, and the only one that does
    not deep-copy it -- `RunResult.extra` (#100) and `ToolCall.arguments`
    (#102) both do. Measured consequences::

        a tool returning state it retains -> the frozen record gains a new
            top-level key after construction, verbatim the shape #102 fixed
        ToolResult(..., value=src)        -> aliases the caller's object
        three calls to one tool returning
            one object                    -> rs[0].value is rs[1].value

    It is stated here rather than fixed because the two obvious answers pull
    opposite ways and both cost something real. `arguments` is model-supplied
    JSON: small and always copyable. `value` is `Any`, so a `deepcopy` can
    *fail* on a connection or a file handle -- turning a successful call into a
    crash at the record boundary -- and costs a full copy per result on the hot
    path of a benchmark harness, in the repo whose spine is performance.

    `tests/test_frozen_mutable_field_policy.py` pins the current behaviour and
    requires every frozen dataclass with a mutable field to be classified, so
    whichever way #106 goes it lands as a diff to an assertion.
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
    timeout: float | None = None,
) -> list[ToolResult]:
    """Run every `ToolCall` against `registry` concurrently.

    Args:
      tool_calls: the model's requested tool invocations.
      registry: name → async-callable lookup.
      return_exceptions: if True, capture each tool's exception on the
        corresponding ToolResult and continue — including an unregistered
        `ToolCall.name`, which lands as a failed `ToolResult` carrying a
        `ToolNotFoundError` repr. If False (default), a *tool* that raises
        propagates wrapped in `PipelineError`, with the original exception
        as `__cause__` — but an *unregistered* `ToolCall.name` raises
        `ToolNotFoundError` bare, before any tool runs, because that check
        happens outside the TaskGroup that does the wrapping. Catch both,
        or catch `Exception` (#90).

        Note both differ from `process`/`stream`, which let TaskGroup's
        `ExceptionGroup` through with the original exception inside — so
        `except* ValueError` catches a failing `fn` there but not here.
      concurrency: optional upper bound on simultaneous in-flight calls.
        None (default) means unbounded — each tool runs as soon as it's
        scheduled.
      timeout: optional per-tool deadline in seconds. None (default) means
        no per-tool deadline. Positive float: each tool invocation is
        wrapped in `asyncio.timeout`; on expiry a `PipelineTimeoutError`
        is raised (carrying the tool's index in `tool_calls`) and follows
        the existing `return_exceptions` policy. Parity with
        `process()` / `stream()`'s `timeout` shape.

    Returns: ToolResult list in input order; `tool_call_id` matches each input.
    """
    # Integer + finite guards (#32). See async_pipelines/core.py:120 for the
    # full harm rationale; the same shape applies to dispatch_tool_calls.
    if concurrency is not None and (
        not isinstance(concurrency, int) or isinstance(concurrency, bool) or concurrency <= 0
    ):
        raise ValueError(f"concurrency must be a positive int or None; got {concurrency!r}")
    timeout = _require_timeout_seconds(timeout)

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
                # Raises bare, NOT wrapped in PipelineError like the TaskGroup
                # handler below — this branch is outside the TaskGroup, so
                # nothing wraps it. That is deliberate and pinned by
                # `test_dispatch_unknown_tool_fail_fast_raises_immediately`;
                # the docstring above used to claim otherwise, which is the
                # part that was wrong. A caller following that claim and
                # writing `except PipelineError` misses this path, and since
                # `ToolNotFoundError` subclasses `KeyError`, an unrelated
                # `except KeyError` upstack can swallow it. Whether that is
                # the right trade is a maintainer call, filed as #90.
                raise
            resolved.append((call, _make_missing_tool_stub(call.name)))
            continue
        resolved.append((call, fn))

    semaphore = asyncio.Semaphore(concurrency) if concurrency is not None else None
    results: list[ToolResult | None] = [None] * len(resolved)

    async def _run_one(idx: int, call: ToolCall, fn: ToolFn) -> None:
        result = await _invoke_tool(
            idx,
            call,
            fn,
            semaphore,
            return_exceptions=return_exceptions,
            timeout=timeout,
        )
        results[idx] = result

    try:
        async with asyncio.TaskGroup() as tg:
            for idx, (call, fn) in enumerate(resolved):
                tg.create_task(_run_one(idx, call, fn))
    except* Exception as eg:
        # Default fail-fast: surface the first non-exception-recorded failure
        # via PipelineError.
        #
        # This is deliberately NOT the shape `process`/`stream` raise, despite
        # what this comment used to claim. They let TaskGroup's `ExceptionGroup`
        # through with the original exception inside, so `except* ValueError`
        # catches a failing `fn` there; here the type survives only as
        # `__cause__`, and `except* ValueError` catches nothing. Whether the
        # dispatcher should stop wrapping and match them is a public-API
        # behaviour question left to the maintainer (#90) — the current shape is
        # pinned by tests either way, and both are documented in the docstring
        # above rather than asserted to be the same.
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
    idx: int,
    call: ToolCall,
    fn: ToolFn,
    semaphore: asyncio.Semaphore | None,
    *,
    return_exceptions: bool,
    timeout: float | None,
) -> ToolResult:
    if semaphore is not None:
        async with semaphore:
            return await _run_with_telemetry(
                idx, call, fn, return_exceptions=return_exceptions, timeout=timeout
            )
    return await _run_with_telemetry(
        idx, call, fn, return_exceptions=return_exceptions, timeout=timeout
    )


async def _run_with_telemetry(
    idx: int,
    call: ToolCall,
    fn: ToolFn,
    *,
    return_exceptions: bool,
    timeout: float | None,
) -> ToolResult:
    start = time.perf_counter()
    # A per-invocation copy, not `call.arguments` itself (#102). `fn` is
    # caller-supplied, and a tool that writes to its argument dict is an
    # ordinary thing -- `args.setdefault("limit", 10)`, normalizing a field in
    # place, accumulating. Handed the live dict, such a tool mutated the
    # `ToolCall`, and because the calls run concurrently the *interleaving*
    # decided what each one saw. Six calls sharing one dict, a tool doing
    # `args["n"] += 1`:
    #
    #     concurrency=1 -> results=[1, 2, 3, 4, 5, 6]
    #     concurrency=2 -> results=[2, 2, 4, 4, 6, 6]
    #     concurrency=6 -> results=[6, 6, 6, 6, 6, 6]
    #
    # `concurrency` is documented as "optional upper bound on simultaneous
    # in-flight calls" -- a throughput knob. It was deciding the answers, in a
    # library whose whole subject is running things concurrently without
    # changing what they mean.
    #
    # It does not need a shared dict to bite. With each `ToolCall` built from
    # its own literal, `calls[0].arguments` still came back mutated, so the
    # record of the request no longer described the request. `ToolResult`
    # carries `elapsed_ms` and `error_repr` so a run is reconstructable
    # afterwards; a `ToolCall` that silently records post-mutation state
    # defeats that.
    #
    # Copied per invocation rather than once outside the try, so the retry-shaped
    # second `await fn(...)` below starts from the same arguments as the first.
    try:
        if timeout is None:
            value = await fn(copy.deepcopy(call.arguments))
        else:
            # Attribute only the deadline's own firing to PipelineTimeoutError;
            # preserve fn's own TimeoutError (a downstream tool/socket timeout),
            # which wait_for() + a bare `except TimeoutError` could not tell
            # apart from the deadline firing (#66).
            try:
                async with asyncio.timeout(timeout) as cm:
                    value = await fn(copy.deepcopy(call.arguments))
            except TimeoutError as exc:
                if cm.expired():
                    raise PipelineTimeoutError(index=idx, timeout_s=timeout) from exc
                raise
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
