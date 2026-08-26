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
import math
import time
from collections.abc import AsyncIterable, Awaitable, Callable, Iterable
from dataclasses import dataclass, field
from typing import Any, TypeVar

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

    def to_dict(self) -> dict[str, Any]:
        # Explicit five-field public contract (#46). Mirrors the
        # observability-parity pattern landed in #45 for `Workload.to_dict`
        # / `RunResult.to_dict`. The `_started_monotonic` private field
        # is intentionally excluded — `asdict(self)` would have included
        # it, which would leak an internal timing checkpoint into JSON
        # consumers (downstream operators reading the bench JSON would
        # see a confusing field with no documented meaning).
        return {
            "produced": self.produced,
            "consumed": self.consumed,
            "producer_pauses": self.producer_pauses,
            "max_queue_depth": self.max_queue_depth,
            "producer_pause_seconds": self.producer_pause_seconds,
        }


def _require_timeout_seconds(timeout: object) -> float | None:
    """Return `timeout` as a positive finite number of seconds, `None`, or raise.

    One definition, imported by every seam that takes a `timeout` -- `process`
    and `stream` here, `dispatch_tool_calls` in `tool_dispatch`. It lived
    inlined at all three, and diverged from the shape `#96` established for the
    two latency seams in `benchmark.py`. That fix's docstring states both the
    rule and the goal:

        Non-numeric types raise `ValueError` here rather than reaching
        `math.isfinite` and coming back as a raw `TypeError`, so this class
        keeps one exception contract.

    The class did not keep one exception contract, because the three `timeout`
    seams -- the package's public API -- were never enumerated. Measured across
    all three, with `stream`'s `queue_size` as the control (it already had the
    safe spelling, three lines away, in the same function):

        value       timeout=            queue_size= (control)
        '5'         TypeError           ValueError
        [1]         TypeError           ValueError
        {'a': 1}    TypeError           ValueError
        True        ACCEPTED            ValueError
        False       ValueError          ValueError
        nan / inf   ValueError          ValueError
        -1 / 0      ValueError          ValueError

    `TypeError: must be real number, not str` is not what a caller catching the
    documented contract catches.

    And `True` was accepted, which is `#96`'s own measured harm reached through
    the other parameter. `asyncio.timeout(True)` is a one-second deadline::

        timeout=True -> ExceptionGroup after 1.003s

    against a 2.0 s task. An operator wiring `timeout` from a config table that
    yields `True` gets a silent one-second deadline on every item and
    `PipelineTimeoutError`s that look like real timeouts.

    Deliberately still accepted: `None` (no deadline) and a plain `int` (a whole
    number of seconds is a legitimate deadline, and the annotation is `float`) --
    the same two exceptions `#96` argued for.

    Deliberately NOT merged with `benchmark._require_duration_seconds`, which
    allows `0.0` because "no simulated latency" is meaningful there. A zero
    timeout is not a deadline, and the existing `<= 0` rule already said so.
    Two rules with one shape, rather than one rule with a flag.
    """
    if timeout is None:
        return None
    if isinstance(timeout, bool) or not isinstance(timeout, (int, float)):
        raise ValueError(f"timeout must be a finite positive number when set, got {timeout!r}")
    if not math.isfinite(timeout) or timeout <= 0:
        raise ValueError(f"timeout must be a finite positive number when set, got {timeout!r}")
    return float(timeout)


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
            cancels every other in-flight task and surfaces inside the
            ``ExceptionGroup`` ``asyncio.TaskGroup`` raises — so catch it
            with ``except* ValueError`` (or whatever ``fn`` raises), not a
            bare ``except ValueError``, which matches nothing. The README's
            timeout example shows the shape. When True, exceptions land in
            the output list at the matching index — useful when one bad
            document shouldn't lose 999.

            ``dispatch_tool_calls`` deliberately differs: it unwraps and
            re-raises ``PipelineError`` instead, so the original type is
            reachable only via ``__cause__`` there (#90).
        timeout: optional per-item deadline in seconds. When set, each
            ``fn(item)`` call is wrapped in ``asyncio.timeout``; if it
            exceeds the deadline, a ``PipelineTimeoutError`` is raised
            (and follows the ``return_exceptions`` policy above). Only
            the deadline's own firing maps to ``PipelineTimeoutError`` —
            ``fn``'s own ``TimeoutError`` propagates unrelabeled (#66). The
            timeout applies per item, not to the batch as a whole. Pass
            ``None`` (the default) to keep the existing untimed shape.

    Returns:
        A list with one entry per input, in input order. With
        ``return_exceptions=True``, failing items appear as the
        ``BaseException`` instance.
    """
    # Integer + finite guards (#32). Pre-#32 NaN/Infinity/fractional passed
    # the sign-only check; asyncio.Semaphore(NaN) raises deep at acquire,
    # Semaphore(1.5) is the same shape, Semaphore(True) silently flattens
    # to Semaphore(1) (bool subclasses int). NaN timeout makes asyncio.
    # timeout behavior implementation-defined; +Infinity silently disables.
    if not isinstance(concurrency, int) or isinstance(concurrency, bool) or concurrency <= 0:
        raise ValueError(f"concurrency must be a positive int, got {concurrency!r}")
    timeout = _require_timeout_seconds(timeout)

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
                    # Use asyncio.timeout()/cm.expired() so only the deadline's
                    # OWN firing maps to PipelineTimeoutError. wait_for() + a bare
                    # `except TimeoutError` couldn't tell a fired deadline from
                    # `fn`'s own TimeoutError (a downstream socket/httpx timeout),
                    # relabeling unrelated failures with a deadline that never
                    # fired — inconsistent with the timeout=None path (#66).
                    try:
                        async with asyncio.timeout(timeout) as cm:
                            results[idx] = await fn(item)
                    except TimeoutError as exc:
                        if cm.expired():
                            raise PipelineTimeoutError(index=idx, timeout_s=timeout) from exc
                        raise
            except BaseException as e:
                # `return_exceptions` collects *fn's* failures so one bad item
                # doesn't lose the batch — those are `Exception`s. A
                # non-`Exception` `BaseException` (CancelledError, KeyboardInterrupt,
                # SystemExit) must still propagate: storing CancelledError as a
                # "result" defeats cooperative cancellation, and swallowing a
                # KeyboardInterrupt hides a Ctrl-C inside the results list (#36).
                # This mirrors `asyncio.gather(return_exceptions=True)`, which
                # only collects `Exception`.
                if return_exceptions and isinstance(e, Exception):
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
        return_exceptions: see ``process`` — including that a fail-fast
            failure arrives inside a ``TaskGroup`` ``ExceptionGroup``, so
            ``except*`` is the shape callers need.
        metrics: optional ``StreamMetrics`` instance written to
            in-place. When provided, ``producer_pauses``,
            ``producer_pause_seconds``, ``max_queue_depth``,
            ``produced``, and ``consumed`` are populated during the
            run. Cost is one ``qsize()`` call and a ``perf_counter``
            pair per produced item, so the overhead is negligible
            relative to any real ``fn`` work.
    """
    # Integer + finite guards (#32) — see `process` for harm rationale; asyncio.Queue
    # additionally raises deep at put/get on a non-int maxsize, NaN included.
    if not isinstance(concurrency, int) or isinstance(concurrency, bool) or concurrency <= 0:
        raise ValueError(f"concurrency must be a positive int, got {concurrency!r}")
    if not isinstance(queue_size, int) or isinstance(queue_size, bool) or queue_size <= 0:
        raise ValueError(f"queue_size must be a positive int, got {queue_size!r}")
    timeout = _require_timeout_seconds(timeout)

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
                    # Attribute only the deadline's own firing to
                    # PipelineTimeoutError; preserve fn's own TimeoutError (#66).
                    try:
                        async with asyncio.timeout(timeout) as cm:
                            value = await fn(item)  # type: ignore[assignment]
                    except TimeoutError as exc:
                        if cm.expired():
                            raise PipelineTimeoutError(index=my_idx, timeout_s=timeout) from exc
                        raise
            except BaseException as e:
                # See `process._run_one` (#36): only `Exception`s are collected
                # under `return_exceptions`. A non-`Exception` `BaseException`
                # (CancelledError / KeyboardInterrupt / SystemExit) propagates so
                # cancellation isn't silently turned into a stored result.
                if return_exceptions and isinstance(e, Exception):
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
