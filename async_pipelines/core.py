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
import time
from collections.abc import AsyncIterable, Awaitable, Callable, Iterable
from dataclasses import dataclass, field
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


class PipelineTimeoutError(PipelineError):
    """Raised when a per-item call exceeds the ``timeout`` deadline.

    Carries the item index and the timeout that fired so callers can
    correlate the failure back to its input. Subclass of
    ``PipelineError`` so existing exception handlers that catch
    ``PipelineError`` still observe timeouts.
    """

    def __init__(self, *, index: int, timeout_s: float) -> None:
        super().__init__(f"item at index {index} exceeded timeout of {timeout_s}s")
        self.index = index
        self.timeout_s = timeout_s


@dataclass
class StreamMetrics:
    """In-place metrics surface for ``stream``.

    Pass an instance via ``stream(..., metrics=m)`` and the call writes
    counters and timings into it as the pipeline runs. The instance is
    safe to read after ``stream`` returns; reading during the run gives
    a live (but not atomically consistent across fields) snapshot.

    Fields:
        produced: number of items pulled from the producer.
        consumed: number of items finished by ``fn`` (success or
            exception when ``return_exceptions=True``).
        producer_pauses: count of times ``queue.put`` had to wait for
            space (the backpressure signal — non-zero means the producer
            was slower than the consumer pool).
        max_queue_depth: high-water mark of items sitting in the queue.
        producer_pause_seconds: cumulative wall time the producer spent
            blocked on a full queue.

    All fields default to zero; the dataclass is stdlib-only so this
    stays consistent with D-002 (runtime-dep-free wrapper).
    """

    produced: int = 0
    consumed: int = 0
    producer_pauses: int = 0
    max_queue_depth: int = 0
    producer_pause_seconds: float = 0.0
    # Pre-allocated to keep `__init__` cheap; not part of the public API.
    _started_monotonic: float = field(default=0.0, repr=False, compare=False)


async def process(
    items: Iterable[T],
    fn: Callable[[T], Awaitable[R]],
    *,
    concurrency: int,
    return_exceptions: bool = False,
    timeout: float | None = None,
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
        timeout: optional per-item deadline in seconds. When set, each
            ``fn(item)`` call is wrapped in ``asyncio.wait_for``; if it
            exceeds the deadline, a ``PipelineTimeoutError`` is raised
            (and follows the ``return_exceptions`` policy above). The
            timeout applies per item, not to the batch as a whole. Pass
            ``None`` (the default) to keep the existing untimed shape.

    Returns:
        A list with one entry per input, in input order. With
        ``return_exceptions=True``, failing items appear as the
        ``BaseException`` instance.
    """
    if concurrency <= 0:
        raise ValueError(f"concurrency must be positive, got {concurrency}")
    if timeout is not None and timeout <= 0:
        raise ValueError(f"timeout must be positive when set, got {timeout}")

    items_list: list[T] = list(items)
    n = len(items_list)
    if n == 0:
        return []

    sem = asyncio.Semaphore(concurrency)
    results: list[R | BaseException] = [None] * n  # type: ignore[list-item]

    async def _run_one(idx: int, item: T) -> None:
        async with sem:
            try:
                if timeout is None:
                    results[idx] = await fn(item)
                else:
                    try:
                        results[idx] = await asyncio.wait_for(fn(item), timeout)
                    except TimeoutError as exc:
                        raise PipelineTimeoutError(index=idx, timeout_s=timeout) from exc
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
    metrics: StreamMetrics | None = None,
    timeout: float | None = None,
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
        metrics: optional ``StreamMetrics`` instance written to
            in-place. When provided, ``producer_pauses``,
            ``producer_pause_seconds``, ``max_queue_depth``,
            ``produced``, and ``consumed`` are populated during the
            run. Cost is one ``qsize()`` call and a ``perf_counter``
            pair per produced item, so the overhead is negligible
            relative to any real ``fn`` work.
    """
    if concurrency <= 0:
        raise ValueError(f"concurrency must be positive, got {concurrency}")
    if queue_size <= 0:
        raise ValueError(f"queue_size must be positive, got {queue_size}")
    if timeout is not None and timeout <= 0:
        raise ValueError(f"timeout must be positive when set, got {timeout}")

    queue: asyncio.Queue[T | _Sentinel] = asyncio.Queue(maxsize=queue_size)
    sentinel = _Sentinel()
    results: list[R | BaseException] = []
    results_lock = asyncio.Lock()
    m = metrics  # local alias keeps the hot path readable
    # Index counter shared across consumers so PipelineTimeoutError carries a
    # stable, monotonically-increasing identifier even though stream() drops
    # input order (D-003).
    consumed_index = 0
    index_lock = asyncio.Lock()

    async def _produce() -> None:
        async for item in producer:
            if m is not None:
                # Time the put so we can attribute pause time to backpressure.
                if queue.full():
                    m.producer_pauses += 1
                    start = time.perf_counter()
                    await queue.put(item)
                    m.producer_pause_seconds += time.perf_counter() - start
                else:
                    await queue.put(item)
                m.produced += 1
                depth = queue.qsize()
                if depth > m.max_queue_depth:
                    m.max_queue_depth = depth
            else:
                await queue.put(item)
        for _ in range(concurrency):
            await queue.put(sentinel)

    async def _consume() -> None:
        nonlocal consumed_index
        while True:
            item = await queue.get()
            if isinstance(item, _Sentinel):
                queue.task_done()
                return
            async with index_lock:
                my_idx = consumed_index
                consumed_index += 1
            try:
                if timeout is None:
                    value: R | BaseException = await fn(item)  # type: ignore[assignment]
                else:
                    try:
                        value = await asyncio.wait_for(fn(item), timeout)  # type: ignore[assignment]
                    except TimeoutError as exc:
                        raise PipelineTimeoutError(index=my_idx, timeout_s=timeout) from exc
            except BaseException as e:
                if return_exceptions:
                    value = e
                else:
                    queue.task_done()
                    raise
            async with results_lock:
                results.append(value)
            queue.task_done()
            if m is not None:
                m.consumed += 1

    async with asyncio.TaskGroup() as tg:
        tg.create_task(_produce())
        for _ in range(concurrency):
            tg.create_task(_consume())

    return results


class _Sentinel:
    """Private sentinel for stream() to signal consumers to exit."""

    __slots__ = ()
