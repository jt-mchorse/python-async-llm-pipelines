"""A cancel already pending when a pipeline starts is a cancellation (#215).

#197 (the #196 fix) asks whether the caller's `cancelling()` count rose above
where it stood on entry. A cancel requested but not yet delivered when the
pipeline starts has already raised the count, so delivering it never moves the
count past the baseline, and each child's `CancelledError` was relabelled
"fn raised CancelledError, but the pipeline was not cancelled". Measured on
origin/main ace58d1, identical on 3.11.14, 3.12.12 and 3.14:

    asyncio.current_task().cancel(); then the call
        raw TaskGroup / asyncio.gather / stream  -> CancelledError
        process                                  -> ExceptionGroup[PipelineError]
        dispatch_tool_calls                      -> PipelineError

The realistic shape is a retry loop around a fail-fast call, cancelled while
attempt 1's group is aborting. 3.13+ re-arm the cancel before attempt 2 starts
(pending on entry); 3.11/3.12's `_TaskGroup` re-arms it one loop step later
(#211), after attempt 2 has entered, with ``uncancel()`` + ``cancel()`` that
leave the count unchanged. Both ended in `PipelineError` for process/dispatch.

`stream` already raised `CancelledError` in both shapes, and so did every
primitive under `return_exceptions=True`; those arms are controls.
So are the #196 arms at the bottom: a count leaked *and delivered* before the
call is still not a cancellation.
"""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import AsyncIterator, Awaitable

import pytest

from async_pipelines import PipelineError, process, stream
from async_pipelines.tool_dispatch import ToolCall, ToolRegistry, dispatch_tool_calls

PRIMITIVES = pytest.mark.parametrize("primitive", ["process", "stream", "dispatch"])


async def _source(n: int) -> AsyncIterator[int]:
    for i in range(n):
        yield i


async def _run(primitive: str, fn, **kw) -> object:  # type: ignore[no-untyped-def]
    if primitive == "process":
        return await process(range(3), fn, concurrency=3, **kw)
    if primitive == "stream":
        return await stream(_source(3), fn, concurrency=3, queue_size=3, **kw)
    registry = ToolRegistry()

    @registry.tool("t")
    async def t(args: dict) -> object:  # type: ignore[type-arg]
        return await fn(int(args["n"]))

    calls = [ToolCall(id=f"c{i}", name="t", arguments={"n": i}) for i in range(3)]
    return await dispatch_tool_calls(calls, registry=registry, concurrency=3, **kw)


async def _bounded(aw: Awaitable[object]) -> object:
    # A regression here could be a hang; fail it as one.
    return await asyncio.wait_for(aw, 5)


async def _slow(n: int) -> int:
    await asyncio.sleep(0.05)
    return n


@PRIMITIVES
async def test_a_cancel_pending_at_entry_is_a_cancellation(primitive: str) -> None:
    async def caller() -> object:
        asyncio.current_task().cancel()  # type: ignore[union-attr]
        return await _run(primitive, _slow)

    task = asyncio.create_task(caller())
    with pytest.raises(asyncio.CancelledError):
        await _bounded(task)
    assert task.cancelled()


@PRIMITIVES
async def test_a_cancel_pending_at_entry_is_not_collected_by_return_exceptions(
    primitive: str,
) -> None:
    # Control: with `return_exceptions` the old code stored the relabelled
    # children as results, but the group still re-raised the caller's cancel,
    # so this was already a CancelledError. Kept so the fix cannot regress it.
    async def caller() -> object:
        asyncio.current_task().cancel()  # type: ignore[union-attr]
        return await _run(primitive, _slow, return_exceptions=True)

    task = asyncio.create_task(caller())
    with pytest.raises(asyncio.CancelledError):
        await _bounded(task)
    assert task.cancelled()


async def _item(n: int) -> int:
    if n == 0:
        await asyncio.sleep(0.05)
        raise ValueError("item 0 failed")  # a transient failure
    try:
        await asyncio.sleep(10)
    except asyncio.CancelledError:
        await asyncio.sleep(0.2)  # a slow cancellation cleanup
        raise
    return n


def _transient(eg: BaseExceptionGroup) -> bool:  # type: ignore[type-arg]
    return any(
        isinstance(e, ValueError) or isinstance(e.__cause__, ValueError) for e in eg.exceptions
    )


@PRIMITIVES
async def test_a_retry_loop_cancelled_during_the_teardown_is_cancelled(primitive: str) -> None:
    # Attempt 1 fails at 50 ms; the caller is cancelled at 100 ms, while the
    # group is aborting; the loop retries, so attempt 2 starts with the cancel
    # pending (3.13+) or re-armed after it entered (3.11/3.12).
    attempts: list[int] = []

    async def with_retries() -> object:
        for i in range(3):
            attempts.append(i)
            try:
                return await _run(primitive, _item)
            except* (ValueError, PipelineError) as eg:
                if not _transient(eg):
                    raise
        return "gave up"

    task = asyncio.create_task(with_retries())
    await asyncio.sleep(0.1)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await _bounded(task)
    assert task.cancelled()
    assert attempts == [0, 1]


# --- controls: #196's leaked count is still not a cancellation -----------------


async def _own_cancel_absorbed() -> None:
    # Raises the count on every interpreter and delivers it: nothing pending.
    asyncio.current_task().cancel()  # type: ignore[union-attr]
    with contextlib.suppress(asyncio.CancelledError):
        await asyncio.sleep(0)


async def _cancelled_future() -> None:
    fut = asyncio.get_running_loop().create_future()
    fut.cancel()
    await fut  # a CancelledError nobody cancelled the pipeline for (#163)


async def _stray_at_1(n: int) -> int:
    if n == 1:
        await _cancelled_future()
    return n


@pytest.mark.parametrize("primitive", ["process", "dispatch"])
async def test_a_delivered_count_at_entry_still_reports_a_stray_cancellation(
    primitive: str,
) -> None:
    async def caller() -> object:
        await _own_cancel_absorbed()
        assert asyncio.current_task().cancelling() == 1  # type: ignore[union-attr]
        return await _run(primitive, _stray_at_1, return_exceptions=True)

    out = await _bounded(asyncio.create_task(caller()))
    if primitive == "process":
        assert out[0] == 0  # type: ignore[index]
        assert out[2] == 2  # type: ignore[index]
        assert isinstance(out[1], PipelineError)  # type: ignore[index]
        assert "fn raised CancelledError" in str(out[1])  # type: ignore[index]
    else:
        assert [r.ok for r in out] == [True, False, True]  # type: ignore[attr-defined]
        assert "fn raised CancelledError" in out[1].error_repr  # type: ignore[index]


@PRIMITIVES
async def test_a_delivered_count_at_entry_lets_the_pipeline_finish(primitive: str) -> None:
    async def caller() -> object:
        await _own_cancel_absorbed()
        return await _run(primitive, _slow)

    out = await _bounded(asyncio.create_task(caller()))
    if primitive == "dispatch":
        out = [r.value for r in out]  # type: ignore[attr-defined]
    assert sorted(out) == [0, 1, 2]  # type: ignore[call-overload]
