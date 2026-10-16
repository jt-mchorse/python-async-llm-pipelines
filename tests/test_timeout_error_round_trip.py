"""`PipelineTimeoutError` survives `copy` and `pickle` (#190).

Its `__init__` is keyword-only, and `BaseException.__reduce__` rebuilds as
`cls(*self.args)` with `args == (message,)`. So all three mechanisms raised
`TypeError: __init__() takes 1 positional argument but 2 were given`. Measured
on `main`: a `return_exceptions=True` result list holding one timed-out item,
returned from a `ProcessPoolExecutor` worker, raised `BrokenProcessPool` in the
parent. The pool spawn is not repeated here; `pickle` is what the pool uses.
"""

from __future__ import annotations

import asyncio
import copy
import pickle
from collections.abc import Callable
from typing import Any

import pytest

from async_pipelines import (
    PipelineError,
    PipelineTimeoutError,
    ToolCall,
    ToolRegistry,
    dispatch_tool_calls,
    process,
)


def _pickle_round_trip(obj: Any) -> Any:
    return pickle.loads(pickle.dumps(obj))


ROUND_TRIPS: list[tuple[str, Callable[[Any], Any]]] = [
    ("copy.copy", copy.copy),
    ("copy.deepcopy", copy.deepcopy),
    ("pickle", _pickle_round_trip),
]


def _assert_same_error(got: object, want: PipelineTimeoutError) -> None:
    assert type(got) is type(want)
    assert isinstance(got, PipelineTimeoutError)
    assert got.index == want.index
    assert got.timeout_s == want.timeout_s
    assert str(got) == str(want)
    assert got.args == want.args


@pytest.mark.parametrize(("name", "round_trip"), ROUND_TRIPS, ids=[n for n, _ in ROUND_TRIPS])
def test_constructed_error_round_trips(name: str, round_trip: Callable[[Any], Any]) -> None:
    exc = PipelineTimeoutError(index=3, timeout_s=0.25)
    _assert_same_error(round_trip(exc), exc)


@pytest.mark.parametrize(("name", "round_trip"), ROUND_TRIPS, ids=[n for n, _ in ROUND_TRIPS])
def test_subclass_round_trips_as_itself(name: str, round_trip: Callable[[Any], Any]) -> None:
    exc = _NamedTimeout(index=1, timeout_s=2.0)
    exc.note = "kept"
    got = round_trip(exc)
    _assert_same_error(got, exc)
    assert got.note == "kept"  # instance `__dict__` travels too


class _NamedTimeout(PipelineTimeoutError):
    """Module-level so `pickle` can find it by name."""


async def _slow_at_one(x: int) -> int:
    await asyncio.sleep(5 if x == 1 else 0)
    return x


async def test_return_exceptions_result_list_deep_copies_and_pickles() -> None:
    """The value the issue's pool returned: a results list with a timed-out slot."""
    results = await process(
        [0, 1, 2], _slow_at_one, concurrency=4, timeout=0.05, return_exceptions=True
    )
    assert isinstance(results[1], PipelineTimeoutError)
    for _, round_trip in ROUND_TRIPS:
        got = round_trip(results)
        assert got[0] == 0
        assert got[2] == 2
        _assert_same_error(got[1], results[1])


async def test_fail_fast_exception_group_pickles() -> None:
    """The fail-fast shape: TaskGroup's ExceptionGroup carrying the error."""
    with pytest.raises(ExceptionGroup) as info:
        await process([0, 1, 2], _slow_at_one, concurrency=4, timeout=0.05)
    got = _pickle_round_trip(info.value)
    [inner] = got.exceptions
    [want] = info.value.exceptions
    _assert_same_error(inner, want)


async def test_dispatch_raised_timeout_pickles() -> None:
    """`dispatch_tool_calls` raises its own deadline bare, with an owner tag (#155)."""
    registry = ToolRegistry()

    @registry.tool("slow")
    async def slow(args: dict[str, Any]) -> None:
        await asyncio.sleep(5)

    with pytest.raises(PipelineTimeoutError) as info:
        await dispatch_tool_calls([ToolCall(id="t1", name="slow")], registry=registry, timeout=0.05)
    _assert_same_error(_pickle_round_trip(info.value), info.value)


def test_base_pipeline_error_control() -> None:
    """The base class already round-tripped; the fix must not be needed there."""
    exc = PipelineError("boom")
    for _, round_trip in ROUND_TRIPS:
        got = round_trip(exc)
        assert type(got) is PipelineError
        assert str(got) == "boom"
