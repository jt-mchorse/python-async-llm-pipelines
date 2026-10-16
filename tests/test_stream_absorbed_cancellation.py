"""`stream` stays cancellable when `fn` or the source absorbs a cancellation (#194).

A `fn` that catches `CancelledError` to clean up and return a fallback ends its
own task in `process` and `dispatch_tool_calls`, which run one task per item.
`stream` runs two loops. Its consumer went back to `queue.get()` after such a
`fn`, and its producer pulled the next item from a source that had absorbed
one. Each then blocked on a queue that the already-cancelled other side would
never serve, and the task hung for good: `TaskGroup` cancels its children once,
so cancelling `stream` again did nothing. Measured on `main`, 3.11 and 3.12:

    process, fn absorbs                  -> cancelled
    stream, fn absorbs                   -> hung after 5 cancels
    stream, source absorbs               -> hung after 5 cancels
    asyncio.timeout(0.05) around stream  -> hung 2 s past its deadline

Every arm is bounded by `asyncio.wait`, not `wait_for`. `wait_for` cancels the
task on expiry and then waits for it, which hangs exactly as the bug does.
`_outcome` cancels any stuck children directly, so an arm run against the
unfixed code fails instead of hanging the session.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Awaitable, Callable

import pytest

from async_pipelines import process, stream

_BOUND_S = 2.0


async def _absorbing_fn(x: int) -> object:
    """Cleans up and returns a fallback when cancelled, rather than re-raising."""
    try:
        await asyncio.sleep(10)
        return x
    except asyncio.CancelledError:
        return "fallback"


async def _fails_first(x: int) -> object:
    if x == 0:
        await asyncio.sleep(0.01)
        raise ValueError("item 0 failed")
    return await _absorbing_fn(x)


async def _fast(x: int) -> int:
    await asyncio.sleep(0.001)
    return x


async def _slow(x: int) -> int:
    await asyncio.sleep(10)
    return x


async def _source(n: int = 100) -> AsyncIterator[int]:
    for i in range(n):
        await asyncio.sleep(0.001)
        yield i


async def _absorbing_source() -> AsyncIterator[int]:
    """Absorbs one cancellation (a fetch retried on cancel), then carries on."""
    absorbed = False
    for i in range(1000):
        try:
            await asyncio.sleep(0.01)
        except asyncio.CancelledError:
            if absorbed:
                raise
            absorbed = True
        yield i


async def _absorbing_source_that_then_ends() -> AsyncIterator[int]:
    """Absorbs the cancellation and ends, so the producer's sentinel puts come next.

    With `concurrency=2, queue_size=2`, items 0 and 1 are in flight and item 2
    is queued when the cancellation lands, so one sentinel fits and the second
    one waits on consumers that are already cancelled.
    """
    yield 0
    yield 1
    yield 2
    try:
        await asyncio.sleep(10)
    except asyncio.CancelledError:
        return
    yield 3


async def _two_then_idle() -> AsyncIterator[int]:
    """Nothing is queued when item 0 fails, so a looping consumer has nothing to take."""
    yield 0
    yield 1
    await asyncio.sleep(10)
    yield 2


async def _outcome(task: asyncio.Task[object]) -> str:
    done, _ = await asyncio.wait([task], timeout=_BOUND_S)
    if not done:
        for t in asyncio.all_tasks():
            if t is not asyncio.current_task():
                t.cancel()
        await asyncio.wait([task], timeout=_BOUND_S)
        return "hung"
    if task.cancelled():
        return "cancelled"
    exc = task.exception()
    return "returned" if exc is None else type(exc).__name__


def _leftover() -> list[str]:
    return [
        t.get_coro().__qualname__  # type: ignore[union-attr]
        for t in asyncio.all_tasks()
        if t is not asyncio.current_task() and not t.done()
    ]


Make = Callable[[], Awaitable[object]]

OUTER_CANCEL: list[tuple[str, Make]] = [
    ("process-fn-absorbs", lambda: process(range(100), _absorbing_fn, concurrency=2)),
    (
        "stream-fn-absorbs",
        lambda: stream(_source(), _absorbing_fn, concurrency=2, queue_size=1),
    ),
    (
        "stream-fn-absorbs-return-exceptions",
        lambda: stream(
            _source(), _absorbing_fn, concurrency=2, queue_size=1, return_exceptions=True
        ),
    ),
    (
        "stream-source-absorbs",
        lambda: stream(_absorbing_source(), _fast, concurrency=2, queue_size=1),
    ),
    (
        "stream-source-absorbs-then-ends",
        lambda: stream(_absorbing_source_that_then_ends(), _slow, concurrency=2, queue_size=2),
    ),
]


@pytest.mark.parametrize(("name", "make"), OUTER_CANCEL, ids=[n for n, _ in OUTER_CANCEL])
async def test_outer_cancel_is_honoured(name: str, make: Make) -> None:
    task = asyncio.ensure_future(make())
    await asyncio.sleep(0.05)
    task.cancel()
    assert await _outcome(task) == "cancelled"
    await asyncio.sleep(0)
    assert _leftover() == []


@pytest.mark.parametrize(("name", "make"), OUTER_CANCEL, ids=[n for n, _ in OUTER_CANCEL])
async def test_outer_asyncio_timeout_fires(name: str, make: Make) -> None:
    async def runner() -> object:
        async with asyncio.timeout(0.05):
            return await make()

    assert await _outcome(asyncio.ensure_future(runner())) == "TimeoutError"
    await asyncio.sleep(0)
    assert _leftover() == []


@pytest.mark.parametrize("concurrency", [2, 4])
async def test_fail_fast_with_a_sibling_that_absorbs_the_cancellation(concurrency: int) -> None:
    task = asyncio.ensure_future(
        stream(_two_then_idle(), _fails_first, concurrency=concurrency, queue_size=4)
    )
    assert await _outcome(task) == "ExceptionGroup"
    eg = task.exception()
    assert isinstance(eg, ExceptionGroup)
    assert [type(e) for e in eg.exceptions] == [ValueError]


async def test_an_absorbed_timeout_deadline_is_not_mistaken_for_a_cancellation() -> None:
    # Control: the pipeline's own `timeout=` cancels `fn` too, and here `fn`
    # absorbs that. `asyncio.timeout` uncancels on exit, so the consumer must
    # keep going and return every item, rather than treating it as #194's case.
    async def absorbs_deadline(x: int) -> object:
        try:
            await asyncio.sleep(10 if x % 2 else 0.001)
            return x
        except asyncio.CancelledError:
            return "fallback"

    results = await asyncio.wait_for(
        stream(_source(6), absorbs_deadline, concurrency=2, queue_size=1, timeout=0.02),
        _BOUND_S,
    )
    assert sorted(map(str, results)) == sorted(["0", "2", "4"] + ["fallback"] * 3)


async def test_a_plain_run_is_unchanged() -> None:
    results = await stream(_source(20), _fast, concurrency=3, queue_size=2)
    assert sorted(results) == list(range(20))  # type: ignore[type-var]
