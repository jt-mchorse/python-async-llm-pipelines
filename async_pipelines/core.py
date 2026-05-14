"""Bounded-concurrency primitives for LLM-style async workloads.

Both primitives are built on stdlib ``asyncio`` with no third-party deps
and target Python 3.11+ (``asyncio.TaskGroup``).

``process(items, fn, *, concurrency)`` — fan out a finite list through
``fn``, capped at ``concurrency`` simultaneous in-flight calls, results
returned in the input order. By default a failing ``fn`` cancels the
batch (TaskGroup semantics); pass ``return_exceptions=True`` to keep
going and collect exceptions in-line.

``stream(producer, fn, *, concurrency, queue_size)`` — same fan-out
shape but driven off an unbounded async source, with the queue size
providing backpressure to the producer.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterable, Awaitable, Callable, Iterable
from typing import TypeVar

T = TypeVar("T")
R = TypeVar("R")


class PipelineError(RuntimeError):
    """Raised when the pipeline fails-fast and the original exception
    was an ExceptionGroup or otherwise needs a clearer call-site type.

    Currently reserved for future use; the default fail-fast path lets
    the originating exception propagate so the caller's traceback
    pinpoints the failing item.
    """


async def process(
    items: Iterable[T],
    fn: Callable[[T], Awaitable[R]],
    *,
    concurrency: int,
    return_exceptions: bool = False,
) -> list[R | BaseException]:
    """Run ``fn`` on every item with at most ``concurrency`` in flight.

    Args:
        items: any iterable. Materialized into a list internally so order
            can be preserved.
        fn: async callable taking one item, returning one result.
        concurrency: maximum number of concurrent ``fn`` calls. Enforced
            by an ``asyncio.Semaphore`` shared across the batch.
        return_exceptions: when False (default), the first exception
            cancels every other in-flight task and propagates. When
            True, exceptions land in the output list at the matching
            index — useful when one bad document shouldn't lose 999.

    Returns:
        A list with one entry per input, in input order. With
        ``return_exceptions=True``, failing items appear as the
        ``BaseException`` instance.
    """
    if concurrency <= 0:
        raise ValueError(f"concurrency must be positive, got {concurrency}")

    items_list: list[T] = list(items)
    n = len(items_list)
    if n == 0:
        return []

    sem = asyncio.Semaphore(concurrency)
    results: list[R | BaseException] = [None] * n  # type: ignore[list-item]

    async def _run_one(idx: int, item: T) -> None:
        async with sem:
            try:
                results[idx] = await fn(item)
            except BaseException as e:
                if return_exceptions:
                    results[idx] = e
                else:
                    raise

    async with asyncio.TaskGroup() as tg:
        for i, item in enumerate(items_list):
            tg.create_task(_run_one(i, item))

    return results


async def stream(
    producer: AsyncIterable[T],
    fn: Callable[[T], Awaitable[R]],
    *,
    concurrency: int,
    queue_size: int,
    return_exceptions: bool = False,
) -> list[R | BaseException]:
    """Drain an async producer through ``fn`` with bounded concurrency
    and an explicit backpressure queue.

    The producer's ``__anext__`` blocks (via ``queue.put``) when the
    queue is full — that's the backpressure signal. The consumer pool
    drains it as fast as ``concurrency`` allows.

    Results are appended in completion order, not producer order — the
    producer's order isn't necessarily meaningful in a streaming
    context, and forcing index-preservation would defeat backpressure.

    Args:
        producer: async iterable of items.
        fn: async callable taking one item, returning one result.
        concurrency: max consumer fan-out.
        queue_size: bounded queue size; controls backpressure.
        return_exceptions: see ``process``.
    """
    if concurrency <= 0:
        raise ValueError(f"concurrency must be positive, got {concurrency}")
    if queue_size <= 0:
        raise ValueError(f"queue_size must be positive, got {queue_size}")

    queue: asyncio.Queue[T | _Sentinel] = asyncio.Queue(maxsize=queue_size)
    sentinel = _Sentinel()
    results: list[R | BaseException] = []
    results_lock = asyncio.Lock()

    async def _produce() -> None:
        async for item in producer:
            await queue.put(item)
        for _ in range(concurrency):
            await queue.put(sentinel)

    async def _consume() -> None:
        while True:
            item = await queue.get()
            if isinstance(item, _Sentinel):
                queue.task_done()
                return
            try:
                value: R | BaseException = await fn(item)  # type: ignore[assignment]
            except BaseException as e:
                if return_exceptions:
                    value = e
                else:
                    queue.task_done()
                    raise
            async with results_lock:
                results.append(value)
            queue.task_done()

    async with asyncio.TaskGroup() as tg:
        tg.create_task(_produce())
        for _ in range(concurrency):
            tg.create_task(_consume())

    return results


class _Sentinel:
    """Private sentinel for stream() to signal consumers to exit."""

    __slots__ = ()
