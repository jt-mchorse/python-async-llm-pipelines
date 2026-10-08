"""A CancelledError that `fn` raises on its own is a failure, not a vanishing (#163).

`asyncio.TaskGroup` ignores a child that ends in `CancelledError`. When `fn`
raises one while nobody cancelled the pipeline (it awaited a future or task
that something else cancelled), the item's failure disappeared. Measured on
`main` with item 1 awaiting a cancelled future, and no exception raised either
way:

    process(range(5), fn, concurrency=1)            -> [0, None, None, None, None]
    dispatch_tool_calls(5 calls)                    -> c1 missing
    dispatch_tool_calls(5 calls, concurrency=1)     -> only c0
    stream(5 items, fn, concurrency=1)              -> hangs forever

A real cancellation keeps propagating as `CancelledError`, whether it comes
from the outer task or from the TaskGroup cancelling a failed item's siblings
(#36). The controls at the bottom pin that.
"""

from __future__ import annotations

import asyncio

import pytest

from async_pipelines import (
    PipelineError,
    ToolCall,
    ToolRegistry,
    dispatch_tool_calls,
    process,
    stream,
)

_N = 5


async def _fn(x: int) -> int:
    if x == 1:
        fut = asyncio.get_running_loop().create_future()
        fut.cancel()
        await fut  # raises CancelledError; this task was not cancelled
    await asyncio.sleep(0.01)
    return x * 10


async def _producer():
    for i in range(_N):
        yield i


def _registry() -> ToolRegistry:
    reg = ToolRegistry()

    @reg.tool("t")
    async def t(args: dict) -> int:
        return await _fn(args["x"])

    return reg


_CALLS = [ToolCall(id=f"c{i}", name="t", arguments={"x": i}) for i in range(_N)]


def _is_stray(e: BaseException) -> bool:
    return isinstance(e, PipelineError) and isinstance(e.__cause__, asyncio.CancelledError)


# --- fail-fast: the failure propagates ----------------------------------------


@pytest.mark.parametrize("concurrency", [1, _N])
async def test_process_fail_fast_raises_instead_of_returning_none(concurrency: int) -> None:
    with pytest.raises(ExceptionGroup) as ei:
        await asyncio.wait_for(process(range(_N), _fn, concurrency=concurrency), 5)
    assert any(_is_stray(e) for e in ei.value.exceptions)
    assert "index 1" in str(ei.value.exceptions[0])


@pytest.mark.parametrize("concurrency", [1, _N])
async def test_stream_fail_fast_raises_instead_of_hanging(concurrency: int) -> None:
    with pytest.raises(ExceptionGroup) as ei:
        await asyncio.wait_for(stream(_producer(), _fn, concurrency=concurrency, queue_size=2), 5)
    assert any(_is_stray(e) for e in ei.value.exceptions)


@pytest.mark.parametrize("concurrency", [None, 1])
async def test_dispatch_fail_fast_raises_instead_of_dropping_calls(
    concurrency: int | None,
) -> None:
    with pytest.raises(PipelineError) as ei:
        await asyncio.wait_for(
            dispatch_tool_calls(_CALLS, registry=_registry(), concurrency=concurrency), 5
        )
    assert _is_stray(ei.value.__cause__)


# --- return_exceptions: collected at the item, nothing lost -------------------


@pytest.mark.parametrize("concurrency", [1, _N])
async def test_process_collects_it_at_its_index(concurrency: int) -> None:
    out = await asyncio.wait_for(
        process(range(_N), _fn, concurrency=concurrency, return_exceptions=True), 5
    )
    assert out[0] == 0
    assert out[2:] == [20, 30, 40]
    assert _is_stray(out[1])


@pytest.mark.parametrize("concurrency", [1, _N])
async def test_stream_collects_it_and_finishes(concurrency: int) -> None:
    out = await asyncio.wait_for(
        stream(_producer(), _fn, concurrency=concurrency, queue_size=2, return_exceptions=True),
        5,
    )
    assert len(out) == _N
    assert sorted(r for r in out if isinstance(r, int)) == [0, 20, 30, 40]
    assert sum(_is_stray(r) for r in out) == 1


@pytest.mark.parametrize("concurrency", [None, 1])
async def test_dispatch_returns_a_result_for_every_call(concurrency: int | None) -> None:
    rs = await asyncio.wait_for(
        dispatch_tool_calls(
            _CALLS, registry=_registry(), concurrency=concurrency, return_exceptions=True
        ),
        5,
    )
    assert [r.tool_call_id for r in rs] == [c.id for c in _CALLS]
    assert [r.ok for r in rs] == [True, False, True, True, True]
    assert "CancelledError" in (rs[1].error_repr or "")


# --- controls: a real cancellation is still a cancellation -------------------


async def test_control_outer_cancellation_still_propagates_as_cancelled_error() -> None:
    async def slow(x: int) -> int:
        await asyncio.sleep(10)
        return x

    for coro_fn in (
        lambda: process(range(_N), slow, concurrency=2, return_exceptions=True),
        lambda: stream(_producer(), slow, concurrency=2, queue_size=2, return_exceptions=True),
    ):
        task = asyncio.create_task(coro_fn())
        await asyncio.sleep(0.02)
        task.cancel()
        # Bounded: a fix that relabels EVERY CancelledError turns this
        # cancellation into collected results and the call runs on for 10 s
        # per item instead of stopping.
        done, _ = await asyncio.wait({task}, timeout=2)
        assert task in done, "cancelling the pipeline did not stop it"
        with pytest.raises(asyncio.CancelledError):
            await task


async def test_control_siblings_cancelled_by_a_failure_are_not_relabelled() -> None:
    async def fn(x: int) -> int:
        if x == 0:
            await asyncio.sleep(0.01)
            raise ValueError("boom")
        await asyncio.sleep(10)
        return x

    with pytest.raises(ExceptionGroup) as ei:
        await process(range(_N), fn, concurrency=_N)
    assert [type(e) for e in ei.value.exceptions] == [ValueError]
