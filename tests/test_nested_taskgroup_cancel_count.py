"""A raised `cancelling()` count inside `fn` is not a pipeline cancellation (#196).

On Python 3.11/3.12 an `asyncio.TaskGroup` inside `fn` whose child fails, and
which `fn` handles (`except* ValueError`), cancels the enclosing task to wake its
`__aexit__` and never uncancels it (fixed in 3.13). #194's guard and #163's
classifier both read the child task's `cancelling()`, so measured on 3.11.14 and
3.12.12:

    stream(5 items), fn handles a nested TaskGroup failure -> hung (3.14: [0..4])
    process(return_exceptions=True), same plus a stray
      CancelledError from fn at item 1                    -> [0, None, None]

The pipeline now asks whether *it* is stopping: its fail-fast latch, or the
caller's task cancelled since entry. Each case runs in two shapes: the real
nested TaskGroup (which only raises the count on 3.11/3.12) and `fn` cancelling
its own task and absorbing it (which raises the count on every interpreter, so
the arms are red against the old guard wherever CI or a developer runs them).
"""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import AsyncIterator, Awaitable, Callable

import pytest

from async_pipelines import PipelineError, process, stream
from async_pipelines.tool_dispatch import ToolCall, ToolRegistry, dispatch_tool_calls


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


RAISERS: dict[str, Callable[[], Awaitable[None]]] = {
    "nested-taskgroup": _nested_taskgroup_failure_handled,
    "own-cancel-absorbed": _own_cancel_absorbed,
}
PARAMS = pytest.mark.parametrize("raise_count", list(RAISERS.values()), ids=list(RAISERS))


async def _cancelled_future() -> None:
    fut = asyncio.get_running_loop().create_future()
    fut.cancel()
    await fut  # a CancelledError nobody cancelled the pipeline for (#163's shape)


async def _source(n: int) -> AsyncIterator[int]:
    for i in range(n):
        yield i


async def _bounded(aw: Awaitable[object]) -> object:
    # A regression here is a hang; fail it as one instead of stalling the suite.
    return await asyncio.wait_for(aw, 5)


@PARAMS
async def test_stream_completes_when_fn_raises_the_count(raise_count) -> None:  # type: ignore[no-untyped-def]
    async def fn(x: int) -> int:
        await raise_count()
        return x

    out = await _bounded(stream(_source(5), fn, concurrency=1, queue_size=1))
    assert sorted(out) == [0, 1, 2, 3, 4]  # type: ignore[call-overload]


@PARAMS
async def test_stream_completes_when_the_source_raises_the_count(raise_count) -> None:  # type: ignore[no-untyped-def]
    async def source() -> AsyncIterator[int]:
        for i in range(5):
            await raise_count()
            yield i

    async def fn(x: int) -> int:
        return x

    out = await _bounded(stream(source(), fn, concurrency=1, queue_size=1))
    assert sorted(out) == [0, 1, 2, 3, 4]  # type: ignore[call-overload]


@PARAMS
async def test_process_still_reports_a_stray_cancellation_after_the_count_rose(
    raise_count,
) -> None:  # type: ignore[no-untyped-def]
    async def fn(x: int) -> int:
        await raise_count()
        if x == 1:
            await _cancelled_future()
        return x

    out = await _bounded(process(range(3), fn, concurrency=1, return_exceptions=True))
    assert out[0] == 0  # type: ignore[index]
    assert isinstance(out[1], PipelineError)  # type: ignore[index]
    assert "fn raised CancelledError" in str(out[1])  # type: ignore[index]
    assert out[2] == 2  # type: ignore[index]


@PARAMS
async def test_process_without_a_stray_returns_every_item(raise_count) -> None:  # type: ignore[no-untyped-def]
    async def fn(x: int) -> int:
        await raise_count()
        return x

    assert await _bounded(process(range(4), fn, concurrency=2)) == [0, 1, 2, 3]


@PARAMS
async def test_dispatch_reports_a_stray_cancellation_after_the_count_rose(
    raise_count,
) -> None:  # type: ignore[no-untyped-def]
    registry = ToolRegistry()

    @registry.tool("t")
    async def t(args: dict) -> int:  # type: ignore[type-arg]
        await raise_count()
        if args["n"] == 1:
            await _cancelled_future()
        return int(args["n"])

    calls = [ToolCall(id=f"c{i}", name="t", arguments={"n": i}) for i in range(3)]
    results = await _bounded(
        dispatch_tool_calls(calls, registry=registry, concurrency=1, return_exceptions=True)
    )
    assert [r.tool_call_id for r in results] == ["c0", "c1", "c2"]  # type: ignore[attr-defined]
    assert [r.ok for r in results] == [True, False, True]  # type: ignore[attr-defined]
    assert "fn raised CancelledError" in results[1].error_repr  # type: ignore[index]


async def test_a_real_caller_cancellation_still_stops_stream_when_fn_absorbs_it() -> None:
    # #194's case, kept: `fn` swallows the pipeline's cancellation and returns.
    started = asyncio.Event()

    async def fn(x: int) -> int:
        started.set()
        try:
            await asyncio.sleep(10)
        except asyncio.CancelledError:
            return -1
        return x

    task = asyncio.create_task(stream(_source(100), fn, concurrency=2, queue_size=1))
    await started.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(task, 5)


# --- #204: the count the library's OWN TaskGroup leaves on the caller ---------
#
# Above, the library stopped *reading* a leaked count. Here it stops *leaving*
# one: after the README's handled fail-fast failure, 3.11/3.12 left the caller
# at `cancelling() == 1` with nobody cancelling it. Measured on 3.11.14/3.12.12
# for all three primitives: the caller's own `asyncio.timeout` then raised
# CancelledError instead of TimeoutError, and an external cancel landing during
# the teardown was lost (the next await completed). 3.13+ already behave; the
# arms are red on the interpreters CI runs and green on 3.13+ either way.


async def _item(n: int, *, fail_at: float = 0.05, cleanup: float = 0.0) -> int:
    if n == 0:
        await asyncio.sleep(fail_at)
        raise ValueError("item 0 failed")
    try:
        await asyncio.sleep(10)
    except asyncio.CancelledError:
        await asyncio.sleep(cleanup)  # a slow cancellation cleanup
        raise
    return n


async def _items(n: int) -> AsyncIterator[int]:
    for i in range(n):
        yield i


def _handled(primitive: str, **kw: float) -> Callable[[], Awaitable[None]]:
    """The README's handled fail-fast call for one primitive."""

    async def fn(n: int) -> int:
        return await _item(n, **kw)

    async def run() -> None:
        if primitive == "dispatch":
            registry = ToolRegistry()

            @registry.tool("t")
            async def t(args: dict) -> int:  # type: ignore[type-arg]
                return await fn(int(args["n"]))

            calls = [ToolCall(id=f"c{i}", name="t", arguments={"n": i}) for i in range(3)]
            with contextlib.suppress(PipelineError):
                await dispatch_tool_calls(calls, registry=registry, concurrency=3)
            return
        try:
            if primitive == "process":
                await process(range(3), fn, concurrency=3)
            else:
                await stream(_items(3), fn, concurrency=3, queue_size=3)
        except* ValueError:
            pass

    return run


PRIMITIVES = pytest.mark.parametrize("primitive", ["process", "stream", "dispatch"])


@PRIMITIVES
async def test_the_callers_own_deadline_is_still_a_timeout_after_a_handled_failure(
    primitive: str,
) -> None:
    async def caller() -> int:
        async with asyncio.timeout(0.3):
            await _handled(primitive)()
            count = asyncio.current_task().cancelling()  # type: ignore[union-attr]
            await asyncio.sleep(5)
        return count

    task = asyncio.create_task(caller())
    with pytest.raises(TimeoutError):
        await task
    assert not task.cancelled()


@PRIMITIVES
async def test_a_handled_failure_leaves_the_callers_count_where_it_was(primitive: str) -> None:
    async def caller() -> int:
        await _handled(primitive)()
        return asyncio.current_task().cancelling()  # type: ignore[union-attr]

    assert await asyncio.create_task(caller()) == 0


@PRIMITIVES
async def test_an_external_cancel_during_the_teardown_reaches_the_callers_next_await(
    primitive: str,
) -> None:
    # Item 0 fails at 50 ms; the others take 200 ms to clean up; the caller is
    # cancelled at 100 ms, while the group is aborting.
    seen: list[int] = []

    async def caller() -> None:
        await _handled(primitive, fail_at=0.05, cleanup=0.2)()
        seen.append(asyncio.current_task().cancelling())  # type: ignore[union-attr]
        await asyncio.sleep(1)
        seen.append(-1)  # the cancel was lost

    task = asyncio.create_task(caller())
    await asyncio.sleep(0.1)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert seen == [1]


@PRIMITIVES
async def test_an_external_cancel_before_the_failure_is_still_a_cancel(primitive: str) -> None:
    # Control: the backport must not erase a real cancel (a restore-to-baseline
    # fix would). Cancelled at 10 ms, before item 0 fails at 50 ms.
    async def caller() -> None:
        await _handled(primitive, fail_at=0.05)()

    task = asyncio.create_task(caller())
    await asyncio.sleep(0.01)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert task.cancelled()


@PRIMITIVES
async def test_without_a_failure_the_count_and_results_are_untouched(primitive: str) -> None:
    async def ok(n: int) -> int:
        return n

    async def caller() -> object:
        if primitive == "process":
            out: object = await process(range(3), ok, concurrency=3)
        elif primitive == "stream":
            out = await stream(_items(3), ok, concurrency=3, queue_size=3)
        else:
            registry = ToolRegistry()
            registry.register("t", lambda a: ok(int(a["n"])))
            calls = [ToolCall(id=f"c{i}", name="t", arguments={"n": i}) for i in range(3)]
            out = [r.value for r in await dispatch_tool_calls(calls, registry=registry)]
        assert asyncio.current_task().cancelling() == 0  # type: ignore[union-attr]
        return out

    assert sorted(await asyncio.create_task(caller())) == [0, 1, 2]  # type: ignore[arg-type]
