"""An expired per-item deadline is a `PipelineTimeoutError` even after `fn` raised its task's cancel count (#200).

`asyncio.timeout` turns its own cancellation into `TimeoutError` only when the
task's `cancelling()` count, after one uncancel, is back where it was on entry.
On Python 3.11/3.12 an `asyncio.TaskGroup` inside `fn` that handles a child's
failure leaves the count at 1 (fixed in 3.13). The deadline's cancellation then
escaped as a bare `CancelledError`, and `_stray_cancellation_as_error` relabelled
it. Measured on 3.11.14, `timeout=0.2`, `fn(1)` handling a nested TaskGroup
failure and then hanging:

    process / stream / dispatch_tool_calls -> PipelineError("fn raised CancelledError ...")

Each case runs in two shapes, as in `test_nested_taskgroup_cancel_count.py`:
the real nested TaskGroup (which raises the count only on 3.11/3.12) and `fn`
cancelling its own task and absorbing it (which raises the count on every
interpreter, so those arms are red against the old code wherever they run).
"""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import AsyncIterator, Awaitable, Callable

import pytest

from async_pipelines import PipelineError, PipelineTimeoutError, process, stream
from async_pipelines.tool_dispatch import ToolCall, ToolRegistry, dispatch_tool_calls

DEADLINE = 0.05


async def _fails() -> None:
    raise ValueError("tool down")


async def _nested_taskgroup_failure_handled() -> None:
    try:
        async with asyncio.TaskGroup() as tg:
            tg.create_task(_fails())
    except* ValueError:
        pass


async def _own_cancel_absorbed() -> None:
    task = asyncio.current_task()
    assert task is not None
    task.cancel()
    # Absorbed and never uncancelled: the count stays raised.
    with contextlib.suppress(asyncio.CancelledError):
        await asyncio.sleep(0)


async def _nothing() -> None:
    return None


RAISERS: dict[str, Callable[[], Awaitable[None]]] = {
    "nested-taskgroup": _nested_taskgroup_failure_handled,
    "own-cancel-absorbed": _own_cancel_absorbed,
    # Control: the count is never raised, so the deadline was always a
    # PipelineTimeoutError. Green before and after the fix.
    "control-no-raise": _nothing,
}
PARAMS = pytest.mark.parametrize("raise_count", list(RAISERS.values()), ids=list(RAISERS))


def _hangs_at_one(raise_count: Callable[[], Awaitable[None]]) -> Callable[[int], Awaitable[int]]:
    async def fn(x: int) -> int:
        if x == 1:
            await raise_count()
            await asyncio.sleep(10)  # past the deadline
        return x

    return fn


async def _source(n: int) -> AsyncIterator[int]:
    for i in range(n):
        yield i


async def _bounded(aw: Awaitable[object]) -> object:
    return await asyncio.wait_for(aw, 5)


@PARAMS
async def test_process_reports_the_deadline(raise_count) -> None:  # type: ignore[no-untyped-def]
    out = await _bounded(
        process(
            range(3),
            _hangs_at_one(raise_count),
            concurrency=3,
            timeout=DEADLINE,
            return_exceptions=True,
        )
    )
    assert out[0] == 0  # type: ignore[index]
    assert out[2] == 2  # type: ignore[index]
    err = out[1]  # type: ignore[index]
    assert isinstance(err, PipelineTimeoutError), repr(err)
    assert (err.index, err.timeout_s) == (1, DEADLINE)


@PARAMS
async def test_process_fail_fast_raises_the_deadline(raise_count) -> None:  # type: ignore[no-untyped-def]
    with pytest.raises(BaseExceptionGroup) as info:
        await _bounded(
            process(range(3), _hangs_at_one(raise_count), concurrency=3, timeout=DEADLINE)
        )
    leaves = info.value.exceptions
    assert [type(e) for e in leaves] == [PipelineTimeoutError], leaves


@PARAMS
async def test_stream_reports_the_deadline(raise_count) -> None:  # type: ignore[no-untyped-def]
    out = await _bounded(
        stream(
            _source(3),
            _hangs_at_one(raise_count),
            concurrency=3,
            queue_size=3,
            timeout=DEADLINE,
            return_exceptions=True,
        )
    )
    errors = [r for r in out if isinstance(r, BaseException)]  # type: ignore[attr-defined]
    assert sorted(r for r in out if not isinstance(r, BaseException)) == [0, 2]  # type: ignore[attr-defined]
    assert [type(e) for e in errors] == [PipelineTimeoutError], errors


@PARAMS
async def test_dispatch_reports_the_deadline(raise_count) -> None:  # type: ignore[no-untyped-def]
    registry = ToolRegistry()
    fn = _hangs_at_one(raise_count)

    @registry.tool("t")
    async def t(args: dict) -> int:  # type: ignore[type-arg]
        return await fn(int(args["n"]))

    calls = [ToolCall(id=f"c{i}", name="t", arguments={"n": i}) for i in range(3)]
    results = await _bounded(
        dispatch_tool_calls(calls, registry=registry, timeout=DEADLINE, return_exceptions=True)
    )
    assert [r.ok for r in results] == [True, False, True]  # type: ignore[attr-defined]
    assert results[1].error_repr.startswith("PipelineTimeoutError("), results[1].error_repr  # type: ignore[index]


# Controls: what the fix must not change.


async def test_a_stray_cancellation_under_a_deadline_is_still_fns_failure() -> None:
    # The deadline did not fire, so a CancelledError from `fn` stays #163's case.
    async def fn(x: int) -> int:
        if x == 1:
            await _own_cancel_absorbed()
            fut = asyncio.get_running_loop().create_future()
            fut.cancel()
            await fut
        return x

    out = await _bounded(process(range(3), fn, concurrency=3, timeout=5, return_exceptions=True))
    assert isinstance(out[1], PipelineError)  # type: ignore[index]
    assert not isinstance(out[1], PipelineTimeoutError)  # type: ignore[index]
    assert "fn raised CancelledError" in str(out[1])  # type: ignore[index]


@pytest.mark.parametrize("entry", ["process", "stream"])
async def test_a_real_caller_cancellation_still_propagates(entry: str) -> None:
    started = asyncio.Event()

    async def fn(x: int) -> int:
        await _own_cancel_absorbed()
        started.set()
        await asyncio.sleep(10)
        return x

    if entry == "process":
        coro = process(range(2), fn, concurrency=2, timeout=5, return_exceptions=True)
    else:
        coro = stream(
            _source(2), fn, concurrency=2, queue_size=2, timeout=5, return_exceptions=True
        )
    task = asyncio.create_task(coro)
    await started.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(task, 5)
