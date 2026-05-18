"""Tests for per-item timeout + cancellation invariants on ``process``/``stream``.

The acceptance bar for issue #5:
- a typed timeout helper exists and is tested
- cancellation does not leak tasks (every started task observes its
  ``finally`` block)
- examples in README

This file covers the runtime invariants. The examples live in README.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator

import pytest

from async_pipelines import (
    PipelineError,
    PipelineTimeoutError,
    process,
    stream,
)


async def _identity(x: int) -> int:
    return x


# --- PipelineTimeoutError shape ------------------------------------------


def test_pipeline_timeout_error_is_pipeline_error_subclass():
    """Callers that catch the umbrella PipelineError must still observe
    timeouts. This is the contract that lets us add the typed subclass
    without breaking existing handlers."""
    exc = PipelineTimeoutError(index=3, timeout_s=0.25)
    assert isinstance(exc, PipelineError)
    assert exc.index == 3
    assert exc.timeout_s == 0.25
    msg = str(exc)
    assert "3" in msg
    assert "0.25" in msg


# --- process: timeout=None preserves prior behavior -----------------------


@pytest.mark.parametrize("timeout", [None])
async def test_process_timeout_none_matches_untimed_shape(timeout):
    """timeout=None must be byte-identical with the original API. Tests
    pass the same path: TaskGroup, return order preserved, no
    wait_for wrapping."""
    out = await process(range(5), _identity, concurrency=3, timeout=timeout)
    assert out == [0, 1, 2, 3, 4]


# --- process: timeout fires, default fail-fast ----------------------------


async def test_process_timeout_raises_pipeline_timeout_error_fail_fast():
    """When fn exceeds the per-item deadline and return_exceptions is
    False, PipelineTimeoutError surfaces (wrapped in ExceptionGroup by
    TaskGroup, per existing fail-fast tests)."""

    async def fn(x: int) -> int:
        if x == 2:
            await asyncio.sleep(10)  # blow the deadline
        return x

    with pytest.raises((PipelineTimeoutError, ExceptionGroup)) as exc_info:
        await process(range(5), fn, concurrency=3, timeout=0.05)

    raised = exc_info.value
    if isinstance(raised, ExceptionGroup):
        flat = list(raised.exceptions)
        # TaskGroup may nest ExceptionGroups; flatten one level for the assert.
        while flat and isinstance(flat[0], ExceptionGroup):
            flat = list(flat[0].exceptions)
        assert any(isinstance(e, PipelineTimeoutError) for e in flat)
        timeout_exc = next(e for e in flat if isinstance(e, PipelineTimeoutError))
    else:
        timeout_exc = raised
    assert timeout_exc.index == 2
    assert timeout_exc.timeout_s == 0.05


# --- process: timeout + return_exceptions lands in list -------------------


async def test_process_timeout_lands_at_index_when_return_exceptions():
    """With return_exceptions=True the timeout lands in the output slot
    of the offending item; faster items still produce real results."""

    async def fn(x: int) -> int:
        if x == 1:
            await asyncio.sleep(10)
        await asyncio.sleep(0.01)
        return x * 100

    out = await process(range(4), fn, concurrency=4, timeout=0.1, return_exceptions=True)
    assert len(out) == 4
    assert out[0] == 0
    assert isinstance(out[1], PipelineTimeoutError)
    assert out[1].index == 1
    assert out[2] == 200
    assert out[3] == 300


# --- process: cancellation regression — no orphaned tasks -----------------


async def test_process_timeout_cancels_in_flight_tasks_cleanly():
    """When the deadline fires, every in-flight task must observe
    CancelledError and run its finally block. We instrument with
    started/finished counters and assert finished == started after the
    raised batch resolves.

    This is the load-bearing 'no orphaned tasks' invariant for #5.
    """
    started = 0
    finished = 0

    async def fn(x: int) -> int:
        nonlocal started, finished
        started += 1
        try:
            # Item 0 is fast; everything else parks until cancelled.
            if x == 0:
                await asyncio.sleep(0.005)
                return x
            await asyncio.sleep(10)
            return x  # unreachable
        finally:
            finished += 1

    with pytest.raises((PipelineTimeoutError, ExceptionGroup)):
        await process(range(8), fn, concurrency=8, timeout=0.05)

    # Every task that started also ran its finally — no orphans.
    assert finished == started
    assert started >= 2  # at least one slow + the fast item


async def test_process_external_cancellation_propagates_to_all_tasks():
    """The existing TaskGroup-propagated cancel test, but tightened:
    counts include the `finally` block so we assert *every* started
    task ran cleanup, not just observed CancelledError."""
    started = 0
    finished = 0

    async def fn(x: int) -> int:
        nonlocal started, finished
        started += 1
        try:
            await asyncio.sleep(10)
            return x  # unreachable
        finally:
            finished += 1

    task = asyncio.create_task(process(range(20), fn, concurrency=5))
    await asyncio.sleep(0.03)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert started >= 1
    assert finished == started


# --- process: invalid timeout rejected ------------------------------------


@pytest.mark.parametrize("bad", [0, -0.5, -1])
async def test_process_rejects_non_positive_timeout(bad):
    with pytest.raises(ValueError, match="timeout must be positive"):
        await process([1], _identity, concurrency=1, timeout=bad)


# --- stream: timeout shape mirrors process --------------------------------


async def _producer(n: int) -> AsyncIterator[int]:
    for i in range(n):
        yield i


async def test_stream_timeout_raises_pipeline_timeout_error():
    """stream's deadline raises PipelineTimeoutError, with an index
    assigned by the consumer counter (stream returns in completion
    order, not input order — D-003 — so the index is consumer-position,
    not producer-position)."""

    async def fn(x: int) -> int:
        if x == 1:
            await asyncio.sleep(10)
        await asyncio.sleep(0.01)
        return x

    with pytest.raises((PipelineTimeoutError, ExceptionGroup)) as exc_info:
        await stream(_producer(4), fn, concurrency=2, queue_size=2, timeout=0.1)

    raised = exc_info.value
    if isinstance(raised, ExceptionGroup):
        flat = list(raised.exceptions)
        while flat and isinstance(flat[0], ExceptionGroup):
            flat = list(flat[0].exceptions)
        assert any(isinstance(e, PipelineTimeoutError) for e in flat)


async def test_stream_timeout_with_return_exceptions_keeps_pipeline_alive():
    """Same as process: return_exceptions=True turns the timeout into
    an in-line result. Surviving items still produce real values."""

    async def fn(x: int) -> int:
        if x == 2:
            await asyncio.sleep(10)
        await asyncio.sleep(0.005)
        return x * 10

    out = await stream(
        _producer(4),
        fn,
        concurrency=4,
        queue_size=4,
        timeout=0.1,
        return_exceptions=True,
    )

    # 4 producer items in, 4 outcomes out (order is completion-based).
    assert len(out) == 4
    timeouts = [v for v in out if isinstance(v, PipelineTimeoutError)]
    successes = [v for v in out if isinstance(v, int)]
    assert len(timeouts) == 1
    assert set(successes) == {0, 10, 30}


async def test_stream_rejects_non_positive_timeout():
    async def fn(x: int) -> int:
        return x

    with pytest.raises(ValueError, match="timeout must be positive"):
        await stream(_producer(1), fn, concurrency=1, queue_size=1, timeout=0)
