"""`_TaskGroup` re-arms a live cancel the caller brought in, not only a new one (#219).

#206 made the 3.11/3.12 backport re-arm only a cancel that raised the count
*while the group was open*, so a count leaked earlier (#196) is not re-armed.
But the entry count also holds a live cancel in the two shapes #215 taught
`_CallerCancelProbe`: one pending on entry (``_must_cancel``), and one a previous
group's deferred re-arm delivers after entry (a retry with no await in
between). When an item fails before its first await, the group swallows that
cancel and the count never rises past the baseline, so nothing re-armed it.
Measured on origin/main 8f0d57c, the caller handling the failure:

    retry after a teardown cancel, attempt 2's item 0 raises before any await
        3.11.14 / 3.12.12  -> caller ran to completion, task.cancelled() False
        3.14.7             -> CancelledError
    current_task().cancel(); the call; await sleep(0.2)
        3.11.14 / 3.12.12  -> process/dispatch: the await completed
        3.14.7             -> CancelledError

The arms run on every interpreter: 3.13+ do this natively, so they are red
only on the 3.11/3.12 CI runs. `stream` raises the cancel out of the call
itself in both shapes, so its arms are controls; so are the #206 leaked-count
arms and the fails-after-an-await / no-retry shapes at the bottom. The retry
inside the caller's own ``asyncio.timeout`` is red too: the deadline was lost.
"""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import AsyncIterator, Awaitable, Callable

import pytest

from async_pipelines import PipelineError, process, stream
from async_pipelines.tool_dispatch import ToolCall, ToolRegistry, dispatch_tool_calls

PRIMITIVES = pytest.mark.parametrize("primitive", ["process", "stream", "dispatch"])


async def _source(n: int) -> AsyncIterator[int]:
    for i in range(n):
        yield i


async def _bounded(aw: Awaitable[object]) -> object:
    # A regression here could be a hang; fail it as one.
    return await asyncio.wait_for(aw, 5)


async def _handled(primitive: str, fn: Callable[[int], Awaitable[int]]) -> None:
    """The README's handled fail-fast call for one primitive."""
    try:
        if primitive == "process":
            await process(range(3), fn, concurrency=3)
        elif primitive == "stream":
            await stream(_source(3), fn, concurrency=3, queue_size=3)
        else:
            registry = ToolRegistry()

            @registry.tool("t")
            async def t(args: dict) -> int:  # type: ignore[type-arg]
                return await fn(int(args["n"]))

            calls = [ToolCall(id=f"c{i}", name="t", arguments={"n": i}) for i in range(3)]
            await dispatch_tool_calls(calls, registry=registry, concurrency=3)
    except* (ValueError, PipelineError):
        pass


async def _fails_at_once(n: int) -> int:
    if n == 0:
        raise ValueError("item 0 failed before any await")
    await asyncio.sleep(0.05)
    return n


async def _fails_after_an_await(n: int) -> int:
    if n == 0:
        await asyncio.sleep(0)
        raise ValueError("item 0 failed after one await")
    await asyncio.sleep(0.05)
    return n


async def _slow_cleanup(n: int) -> int:
    if n == 0:
        await asyncio.sleep(0.05)
        raise ValueError("attempt 1 failed")
    try:
        await asyncio.sleep(10)
    except asyncio.CancelledError:
        await asyncio.sleep(0.2)  # a slow cancellation cleanup
        raise
    return n


async def _callers_own_handled_taskgroup_failure() -> None:
    # The #196 leak: leaves the caller at cancelling() == 1 on 3.11/3.12, with
    # nothing pending. 3.13+ leak nothing.
    async def fails() -> None:
        raise ValueError("tool down")

    try:
        async with asyncio.TaskGroup() as tg:
            tg.create_task(fails())
    except* ValueError:
        pass


# --- a cancel pending on entry --------------------------------------------------


@PRIMITIVES
async def test_a_cancel_pending_on_entry_survives_a_failure_before_the_first_await(
    primitive: str,
) -> None:
    seen: list[str] = []

    async def caller() -> None:
        asyncio.current_task().cancel()  # type: ignore[union-attr]
        await _handled(primitive, _fails_at_once)
        seen.append("returned")
        await asyncio.sleep(0.2)
        seen.append("cancel lost")

    task = asyncio.create_task(caller())
    with pytest.raises(asyncio.CancelledError):
        await _bounded(task)
    assert task.cancelled()
    assert "cancel lost" not in seen


@PRIMITIVES
async def test_a_pending_cancel_on_top_of_a_leaked_count_is_still_re_armed(
    primitive: str,
) -> None:
    # The baseline already holds the #196 leak; the pending cancel is one more.
    seen: list[str] = []

    async def caller() -> None:
        await _callers_own_handled_taskgroup_failure()
        asyncio.current_task().cancel()  # type: ignore[union-attr]
        await _handled(primitive, _fails_at_once)
        seen.append("returned")
        await asyncio.sleep(0.2)
        seen.append("cancel lost")

    task = asyncio.create_task(caller())
    with pytest.raises(asyncio.CancelledError):
        await _bounded(task)
    assert task.cancelled()
    assert "cancel lost" not in seen


# --- a re-arm that lands after entry (the retry shape) --------------------------


@PRIMITIVES
async def test_a_retry_failing_before_its_first_await_keeps_the_teardown_cancel(
    primitive: str,
) -> None:
    # Attempt 1 fails at 50 ms; the caller is cancelled at 100 ms, while the
    # group is aborting; attempt 2 starts at once and its item 0 raises before
    # any await, so attempt 1's deferred re-arm lands inside attempt 2's group.
    seen: list[str] = []

    async def caller() -> None:
        await _handled(primitive, _slow_cleanup)
        seen.append("attempt 1 handled")
        await _handled(primitive, _fails_at_once)
        seen.append("attempt 2 handled")
        await asyncio.sleep(1)
        seen.append("cancel lost")

    task = asyncio.create_task(caller())
    await asyncio.sleep(0.1)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await _bounded(task)
    assert task.cancelled()
    assert "cancel lost" not in seen


@PRIMITIVES
async def test_a_retry_inside_the_callers_timeout_is_still_a_timeout(primitive: str) -> None:
    # #210's owner: the deadline fires during attempt 1's teardown, attempt 2
    # fails before its first await. The re-armed cancel must reach the
    # timeout's own block and become TimeoutError there, not leak past it.
    seen: list[str] = []

    async def caller() -> str:
        with contextlib.suppress(TimeoutError):
            async with asyncio.timeout(0.1):
                await _handled(primitive, _slow_cleanup)
                await _handled(primitive, _fails_at_once)
                await asyncio.sleep(1)
                seen.append("deadline lost")
        await asyncio.sleep(0.05)
        return "finished"

    assert await _bounded(asyncio.create_task(caller())) == "finished"
    assert seen == []


# --- controls -------------------------------------------------------------------


@PRIMITIVES
async def test_a_leaked_count_with_nothing_pending_is_still_not_re_armed(primitive: str) -> None:
    # #206: the baseline drops only for a live cancel, never for the leak.
    async def caller() -> str:
        await _callers_own_handled_taskgroup_failure()
        await _handled(primitive, _fails_at_once)
        await asyncio.sleep(0)
        await _handled(primitive, _fails_at_once)  # a second group: no stale signal
        await asyncio.sleep(0.05)
        return "finished"

    assert await _bounded(asyncio.create_task(caller())) == "finished"


@PRIMITIVES
async def test_a_handled_failure_before_the_first_await_alone_is_not_a_cancel(
    primitive: str,
) -> None:
    async def caller() -> str:
        await _handled(primitive, _fails_at_once)
        await asyncio.sleep(0.05)
        return "finished"

    assert await _bounded(asyncio.create_task(caller())) == "finished"


@PRIMITIVES
async def test_a_retry_failing_after_an_await_keeps_the_teardown_cancel(primitive: str) -> None:
    seen: list[str] = []

    async def caller() -> None:
        await _handled(primitive, _slow_cleanup)
        await _handled(primitive, _fails_after_an_await)
        await asyncio.sleep(1)
        seen.append("cancel lost")

    task = asyncio.create_task(caller())
    await asyncio.sleep(0.1)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await _bounded(task)
    assert seen == []


@PRIMITIVES
async def test_without_a_retry_the_teardown_cancel_reaches_the_next_await(primitive: str) -> None:
    seen: list[str] = []

    async def caller() -> None:
        await _handled(primitive, _slow_cleanup)
        await asyncio.sleep(1)
        seen.append("cancel lost")

    task = asyncio.create_task(caller())
    await asyncio.sleep(0.1)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await _bounded(task)
    assert seen == []
