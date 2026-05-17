"""Tests for ``stream`` — bounded queue, backpressure, fan-out."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator

import pytest

from async_pipelines import StreamMetrics, stream


async def _producer(n: int, delay: float = 0.0) -> AsyncIterator[int]:
    for i in range(n):
        if delay:
            await asyncio.sleep(delay)
        yield i


async def _doubler(x: int) -> int:
    return x * 2


async def test_stream_consumes_all_items():
    out = await stream(_producer(10), _doubler, concurrency=3, queue_size=4)
    assert sorted(out) == [x * 2 for x in range(10)]


async def test_stream_with_empty_producer_returns_empty():
    async def empty() -> AsyncIterator[int]:
        if False:
            yield 0  # pragma: no cover - unreachable

    out = await stream(empty(), _doubler, concurrency=3, queue_size=4)
    assert out == []


async def test_stream_backpressure_blocks_producer_when_queue_full():
    """If the queue is bounded to 2 and the consumer is slow, the
    producer's put must block — the test inspects how many items the
    producer managed to emit before the consumers caught up.
    """
    produced: list[int] = []

    async def slow_producer() -> AsyncIterator[int]:
        for i in range(20):
            produced.append(i)
            yield i

    consumer_lock = asyncio.Event()

    async def gated(x: int) -> int:
        await consumer_lock.wait()
        return x * 2

    async def run():
        return await stream(slow_producer(), gated, concurrency=1, queue_size=2)

    task = asyncio.create_task(run())
    # Give the producer time to fill the queue and block.
    await asyncio.sleep(0.05)
    # Backpressure must hold the producer well below the full input length —
    # it should not have emitted anywhere close to 20. The exact ceiling is
    # queue_size + concurrency + 1 (the producer's pending put), so 4 here.
    assert len(produced) <= 4, f"producer emitted {len(produced)} items, expected ≤4"
    assert len(produced) >= 2, f"producer emitted only {len(produced)} — queue should be full"
    # Now release the consumers and let the pipeline drain.
    consumer_lock.set()
    out = await task
    assert len(out) == 20


async def test_stream_invalid_concurrency_rejected():
    async def empty() -> AsyncIterator[int]:
        if False:
            yield 0  # pragma: no cover

    with pytest.raises(ValueError, match="concurrency"):
        await stream(empty(), _doubler, concurrency=0, queue_size=4)


async def test_stream_invalid_queue_size_rejected():
    async def empty() -> AsyncIterator[int]:
        if False:
            yield 0  # pragma: no cover

    with pytest.raises(ValueError, match="queue_size"):
        await stream(empty(), _doubler, concurrency=2, queue_size=0)


async def test_stream_return_exceptions_collects_failures():
    async def fn(x: int) -> int:
        if x == 3:
            raise ValueError("boom")
        return x * 10

    out = await stream(_producer(5), fn, concurrency=2, queue_size=3, return_exceptions=True)
    assert len(out) == 5
    errs = [o for o in out if isinstance(o, ValueError)]
    successes = sorted(o for o in out if not isinstance(o, BaseException))
    assert len(errs) == 1
    assert successes == [0, 10, 20, 40]


async def test_stream_metrics_records_produced_and_consumed_counts():
    """With no slow consumer, metrics still capture totals."""
    m = StreamMetrics()
    out = await stream(_producer(10), _doubler, concurrency=3, queue_size=4, metrics=m)
    assert len(out) == 10
    assert m.produced == 10
    assert m.consumed == 10
    # Without contention, max depth is bounded by queue_size but may be < it.
    assert 0 <= m.max_queue_depth <= 4


async def test_stream_metrics_records_pauses_under_slow_consumer():
    """With a small queue and a slow consumer, producer must pause —
    that's the backpressure signal we want operators to see.
    """
    m = StreamMetrics()

    async def slow(x: int) -> int:
        await asyncio.sleep(0.01)
        return x * 2

    out = await stream(_producer(30), slow, concurrency=1, queue_size=2, metrics=m)
    assert len(out) == 30
    assert m.produced == 30
    assert m.consumed == 30
    # Producer must have hit a full queue at least once with this shape
    # (fast producer, slow consumer, tiny queue).
    assert m.producer_pauses >= 1
    # Cumulative pause time must reflect the consumer's sleep budget —
    # 28 of the 30 puts can wait on the consumer (first 2 fit unblocked),
    # so we expect substantial pause time, but never negative or NaN.
    assert m.producer_pause_seconds > 0.0
    # High-water mark must equal queue_size when saturated.
    assert m.max_queue_depth == 2


async def test_stream_metrics_no_pauses_when_consumer_fast():
    """If the consumer drains as fast as the producer emits, pauses
    stay zero — the test that gives the metric semantic meaning.
    """
    m = StreamMetrics()

    async def fast(x: int) -> int:
        # Faster than the producer's enumerate loop (effectively zero).
        return x

    out = await stream(_producer(5), fast, concurrency=4, queue_size=10, metrics=m)
    assert len(out) == 5
    assert m.produced == 5
    assert m.consumed == 5
    # Generous queue, plenty of consumers — no backpressure signal expected.
    assert m.producer_pauses == 0
    assert m.producer_pause_seconds == 0.0


async def test_stream_metrics_max_depth_bounded_by_queue_size():
    """No matter how many items flow through, max_queue_depth must
    never exceed queue_size — this is the OOM-safety invariant.
    """
    m = StreamMetrics()

    async def slow(x: int) -> int:
        await asyncio.sleep(0.001)
        return x

    # 1000 items, queue size 8, consumer slower than producer.
    out = await stream(_producer(1000), slow, concurrency=2, queue_size=8, metrics=m)
    assert len(out) == 1000
    assert m.max_queue_depth <= 8, (
        f"max_queue_depth {m.max_queue_depth} exceeded queue_size 8 — backpressure invariant broken"
    )


async def test_stream_metrics_omitted_means_no_overhead_path():
    """metrics=None (the default) leaves the function on the non-metric
    code path. We just assert the behavior is unchanged.
    """
    out = await stream(_producer(20), _doubler, concurrency=4, queue_size=5)
    assert sorted(out) == [x * 2 for x in range(20)]
