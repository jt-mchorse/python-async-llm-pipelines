"""Tests for ``process`` — order, concurrency cap, failures, cancellation."""

from __future__ import annotations

import asyncio
import time
from collections.abc import AsyncIterator

import pytest

from async_pipelines import process


async def _identity(x: int) -> int:
    return x


async def _slow_identity(x: int, *, delay: float = 0.05) -> int:
    await asyncio.sleep(delay)
    return x


async def test_empty_input_returns_empty_list():
    out = await process([], _identity, concurrency=4)
    assert out == []


async def test_results_preserve_input_order():
    # Mix of fast and slow items — the slow ones finish later but must
    # land at their original index.
    async def fn(x: int) -> int:
        await asyncio.sleep(0.01 * (5 - x))
        return x

    out = await process(range(5), fn, concurrency=3)
    assert out == [0, 1, 2, 3, 4]


async def test_concurrency_ceiling_is_enforced():
    """No more than ``concurrency`` tasks may be in ``fn`` at once."""
    in_flight = 0
    peak = 0
    lock = asyncio.Lock()

    async def fn(x: int) -> int:
        nonlocal in_flight, peak
        async with lock:
            in_flight += 1
            peak = max(peak, in_flight)
        await asyncio.sleep(0.02)
        async with lock:
            in_flight -= 1
        return x

    await process(range(20), fn, concurrency=4)
    assert peak <= 4
    assert peak >= 1


async def test_concurrency_one_serializes():
    started: list[int] = []
    finished: list[int] = []

    async def fn(x: int) -> int:
        started.append(x)
        await asyncio.sleep(0.005)
        finished.append(x)
        return x

    await process(range(5), fn, concurrency=1)
    # With concurrency=1 the next start can't happen before the previous finish.
    for i in range(1, 5):
        assert started.index(i) > finished.index(i - 1)


async def test_default_fail_fast_propagates_first_exception():
    async def fn(x: int) -> int:
        if x == 3:
            raise ValueError("bad item 3")
        await asyncio.sleep(0.01)
        return x

    with pytest.raises((ValueError, ExceptionGroup)) as exc_info:
        await process(range(10), fn, concurrency=4)
    # asyncio.TaskGroup wraps in ExceptionGroup; either form is acceptable
    # as long as the message survives.
    raised = exc_info.value
    if isinstance(raised, ExceptionGroup):
        assert any("bad item 3" in str(e) for e in raised.exceptions)
    else:
        assert "bad item 3" in str(raised)


async def test_return_exceptions_keeps_batch_alive():
    async def fn(x: int) -> int:
        if x % 3 == 0:
            raise ValueError(f"item {x} failed")
        return x * 10

    out = await process(range(6), fn, concurrency=3, return_exceptions=True)
    assert len(out) == 6
    # Index 0, 3 are failures, the rest are 10*x.
    assert isinstance(out[0], ValueError)
    assert out[1] == 10
    assert out[2] == 20
    assert isinstance(out[3], ValueError)
    assert out[4] == 40
    assert out[5] == 50


async def test_invalid_concurrency_rejected():
    with pytest.raises(ValueError, match="positive"):
        await process([1], _identity, concurrency=0)
    with pytest.raises(ValueError, match="positive"):
        await process([1], _identity, concurrency=-3)


async def test_cancellation_cancels_in_flight_work():
    """Cancelling the outer task must cancel every running fn cleanly."""
    started = 0
    cancelled = 0

    async def fn(x: int) -> int:
        nonlocal started, cancelled
        started += 1
        try:
            await asyncio.sleep(10)
        except asyncio.CancelledError:
            cancelled += 1
            raise
        return x  # unreachable

    task = asyncio.create_task(process(range(20), fn, concurrency=5))
    # Let some tasks start.
    await asyncio.sleep(0.05)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    # All started tasks were cancelled.
    assert started >= 1
    assert cancelled == started


async def test_concurrency_speedup_is_real():
    """A simple latency check: 10 sleeps of 0.05s at concurrency=10
    must finish well under 0.5s (the serial time)."""

    async def slow(x: int) -> int:
        await asyncio.sleep(0.05)
        return x

    t0 = time.perf_counter()
    await process(range(10), slow, concurrency=10)
    elapsed = time.perf_counter() - t0
    # Serial would take ~0.5s; bounded-concurrent should be ~0.05s + overhead.
    assert elapsed < 0.25, f"elapsed={elapsed:.3f}s, expected well under 0.25s"


# --- async iterable input -------------------------------------------------


async def _async_range(n: int) -> AsyncIterator[int]:
    """`process` materializes its input — but list() works on a sync
    iterable, not async. So this case is intentionally not supported;
    the test documents the limitation."""
    for i in range(n):
        yield i


async def test_process_does_not_accept_async_iterable():
    """`process` is for finite, materializable inputs. Streams use
    `stream()` instead (separate test file)."""
    with pytest.raises(TypeError):
        await process(_async_range(5), _identity, concurrency=2)  # type: ignore[arg-type]


# Issue #32: process() validates concurrency as isinstance(int) and timeout
# as math.isfinite. Pre-#32 sign-only `<= 0` accepted NaN, fractional, bool,
# +/-Infinity — propagating into asyncio.Semaphore / asyncio.wait_for as
# cryptic deep TypeErrors or silent never-firing behavior.
async def _noop(x):
    return x


@pytest.mark.parametrize(
    "bad",
    [1.5, float("nan"), float("inf"), True, "5"],
)
async def test_process_rejects_non_int_concurrency(bad):
    with pytest.raises(ValueError, match="concurrency must be a positive int"):
        await process([1, 2, 3], _noop, concurrency=bad)


@pytest.mark.parametrize(
    "bad",
    [float("nan"), float("inf"), float("-inf"), 0, -1],
)
async def test_process_rejects_non_finite_or_non_positive_timeout(bad):
    with pytest.raises(ValueError, match="timeout must be a finite positive number"):
        await process([1, 2, 3], _noop, concurrency=1, timeout=bad)


async def test_process_accepts_undefined_timeout():
    result = await process([1, 2, 3], _noop, concurrency=1)
    assert result == [1, 2, 3]
