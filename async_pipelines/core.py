"""Bounded-concurrency primitives for LLM-style async workloads.

Both primitives are built on stdlib ``asyncio`` with no third-party deps
and target Python 3.11+ (``asyncio.TaskGroup``).

``process(items, fn, *, concurrency)`` — fan out a finite list through
``fn``, capped at ``concurrency`` simultaneous in-flight calls, results
returned in the input order. By default a failing ``fn`` cancels the
batch (TaskGroup semantics); pass ``return_exceptions=True`` to keep
going and collect exceptions in-line.

``stream(producer, fn, *, concurrency, queue_size)`` — same fan-out
shape but driven off an async source, with the queue size providing
backpressure to the producer. What that bounds is the input *waiting to
be processed* (``queue_size`` queued, at most ``concurrency`` in
flight); the results are returned as one list and are O(n), so the
source must end (#115).
"""

from __future__ import annotations

import asyncio
import math
import sys
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

    def __reduce__(self) -> tuple[Any, ...]:
        # `BaseException.__reduce__` rebuilds as `cls(*self.args)`, and `args` is
        # the one formatted message -- which the keyword-only `__init__` above
        # refuses (#190). `copy.copy`, `copy.deepcopy` and `pickle.loads` all
        # raised TypeError, so a `return_exceptions=True` result holding a
        # timed-out item could not be deep-copied, and returned from a
        # `ProcessPoolExecutor` worker it broke the pool (`BrokenProcessPool`).
        # Rebuild from the two fields instead; `__dict__` carries the rest.
        return (_rebuild_timeout_error, (type(self), self.index, self.timeout_s), self.__dict__)


def _rebuild_timeout_error(
    cls: type[PipelineTimeoutError], index: int, timeout_s: float
) -> PipelineTimeoutError:
    """Unpickle / copy hook for `PipelineTimeoutError.__reduce__` (#190)."""
    return cls(index=index, timeout_s=timeout_s)


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
        producer_pauses: count of times the producer called ``queue.put``
            and had to wait at least one loop step. **Important:** this
            counter increments on every successful handoff to the queue,
            not only when the queue was full — even at very high concurrency
            (where consumers outpace the producer) it counts the normal one-
            step yield that occurs each time a consumer slot frees up, and
            can therefore be near ``n_items`` when there is no consumer
            bottleneck at all (#216). Use ``producer_pause_seconds`` to
            distinguish real backpressure (long total wait, consumer-bound)
            from a fast handoff (near-zero total wait, producer-bound or
            balanced); a high count with near-zero seconds is not a signal
            to grow ``concurrency``.
        max_queue_depth: high-water mark of items sitting in the queue.
        producer_pause_seconds: cumulative wall time the producer spent
            blocked on a full queue. Use this — not ``producer_pauses`` —
            as the primary operator signal for consumer-bound backpressure:
            a large total pause time means the consumer pool could not keep
            up; a near-zero total time (even with a high ``producer_pauses``
            count) means the producer moved faster than the consumers and the
            queue never held the producer back for long.

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


def _refuse_bare_string(name: str, value: object) -> None:
    """Raise ``ValueError`` if *value* is a bare ``str``/``bytes`` (#126).

    A ``str`` is an ``Iterable[str]``, so ``list(items)`` turns one document
    into one item per character and every result has the right shape: measured,
    ``run_pipeline(SerialPipeline(llm, llm), "summarize this doc")`` reported
    ``n_docs=18`` and made 36 LLM calls. ``bytes`` fans out over ints.

    Only the bare-string shape. Every other iterable is still accepted, and
    ``stream`` needs no call -- a ``str`` is not an ``AsyncIterable``, so it
    already fails loud.
    """
    if isinstance(value, (str, bytes, bytearray)):
        fix = f"pass [{value!r}]" if isinstance(value, str) else "decode it to str first"
        raise ValueError(
            f"{name} must be a collection of items, not a bare {type(value).__name__}: "
            f"{value!r} would be split into its characters -- {fix}"
        )


class _CallerCancelProbe:
    """Has the task that called the pipeline been cancelled since it called? (#196)

    #163/#169/#194 asked the *child* task: ``asyncio.current_task().cancelling()``
    inside `fn`'s or the source's task. On Python 3.11 and 3.12 that count is not
    a cancellation signal. An ``asyncio.TaskGroup`` *inside* `fn` whose child
    fails, and which `fn` then handles (``except* ValueError``), cancels the
    enclosing task to wake its ``__aexit__`` and never uncancels it -- fixed in
    3.13. The count stays at 1 with nobody cancelling anything. Measured on
    3.11.14 and 3.12.12, `fn` handling a nested TaskGroup failure and returning:

        stream(5 items, concurrency=1)                 -> hung (#194's guard fired)
        process(..., return_exceptions=True) + a stray
          CancelledError from fn at item 1             -> [0, None, None]
                                                          (#163 undone)

    and ``[0, 1, 2, 3, 4]`` / ``[0, PipelineError, 2]`` on 3.14.

    The pipeline's own children are cancelled only when the pipeline is
    stopping, and it stops for two reasons: a child's failure propagated (the
    `failed` latch each entry point already keeps, set before the raise), or the
    caller cancelled it. In the second case -- and in the first too, because a
    ``TaskGroup`` cancels its parent to wake ``__aexit__`` when a child fails --
    the *caller's* count rises above where it stood on entry. A TaskGroup inside
    `fn` raises only the child's count, so it no longer reads as a cancellation.
    """

    __slots__ = ("_baseline", "_task")

    def __init__(self) -> None:
        self._task = asyncio.current_task()
        self._baseline = self._task.cancelling() if self._task is not None else 0

    def cancelled(self) -> bool:
        return self._task is not None and self._task.cancelling() > self._baseline


class _TaskGroup(asyncio.TaskGroup):
    """`asyncio.TaskGroup` that leaves the caller's cancel count as 3.13 does (#204).

    #196 stopped the library *reading* the count a TaskGroup leaks on 3.11/3.12.
    This stops the library's own TaskGroups *leaving* it on the caller. A child
    failure cancels the parent task to wake ``__aexit__``. 3.11/3.12 uncancel
    that request at the top of ``__aexit__``, so a request made while
    ``__aexit__`` is already waiting on the children is never undone (fixed in
    3.13 by moving the ``uncancel()`` after the wait, gh-116720). Measured on
    3.11.14 and 3.12.12, after the README's handled fail-fast failure:

        caller's asyncio.timeout fires later   -> CancelledError (3.14: TimeoutError)
        external cancel during the teardown    -> lost, next await completes
                                                  (3.14: CancelledError)

    When the group exits with its errors, this backports both 3.13 steps.
    First, uncancel a request the group made during ``__aexit__``. A request
    made before it was already uncancelled, so it is excluded and the count is
    never undone twice. Second, if a cancellation is still pending one loop
    step later (#210), re-arm it, so a cancel the group swallowed while
    aborting reaches the caller's next await. Restoring a baseline recorded on
    entry would be wrong: it cannot tell the group's own request from a real
    external cancel, and would erase the external one.

    It matches 3.13 for the shapes measured in #204, #206 and #210, not
    everywhere. A caller that is already being cancelled and runs a pipeline as
    cleanup (``except CancelledError: await process(...)``) brings that cancel
    in, so a handled failure there lets the cleanup continue on 3.11/3.12,
    where 3.13+ re-arms the cancel at its next await.
    """

    _entry_cancelling = 0

    async def __aenter__(self) -> _TaskGroup:
        # The caller's count on entry (#206). On 3.11/3.12 it may already be
        # raised by a TaskGroup of the caller's own that handled a failure (the
        # #196 leak), and that is not a pending cancellation: re-arming it below
        # raised a `CancelledError` nobody asked for. 3.13 leaks nothing there.
        task = asyncio.current_task()
        self._entry_cancelling = task.cancelling() if task is not None else 0
        await super().__aenter__()
        return self

    async def __aexit__(self, et: Any, exc: Any, tb: Any) -> None:
        if sys.version_info >= (3, 13):
            await super().__aexit__(et, exc, tb)
            return
        requested_before = self._parent_cancel_requested  # type: ignore[attr-defined]
        parent = self._parent_task  # type: ignore[attr-defined]  # 3.12 clears it on exit
        try:
            await super().__aexit__(et, exc, tb)
        except BaseExceptionGroup:
            if parent is not None:
                if self._parent_cancel_requested and not requested_before:  # type: ignore[attr-defined]
                    parent.uncancel()
                # One loop step later (#210): `parent` is the running task here,
                # so `cancel()` would set `_must_cancel`, and 3.11/3.12's
                # `uncancel()` never clears it. An enclosing timeout or TaskGroup
                # that owned the cancel uncancels it as it unwinds, and the stale
                # flag still fired at the caller's next await.
                parent.get_loop().call_soon(_rearm_pending_cancel, parent, self._entry_cancelling)
            raise


def _rearm_pending_cancel(parent: asyncio.Task[Any], entry_cancelling: int) -> None:
    """Re-deliver a cancel a `_TaskGroup` swallowed while aborting (#204, #210).

    Runs one loop step after the group exits, so an owner that unwound in the
    meantime (an enclosing ``asyncio.timeout`` or TaskGroup) has already
    uncancelled its request and nothing is left to re-arm. A cancel nobody will
    uncancel, such as an external ``task.cancel()``, is still pending and lands
    at the caller's next await. Only a cancel that arrived while the group was
    open counts (#206): a count the caller brought in is not one it swallowed.
    """
    if not parent.done() and parent.cancelling() > entry_cancelling:
        parent.uncancel()
        parent.cancel()


def _stray_cancellation_as_error(
    exc: BaseException,
    *,
    stopping: bool,
    index: int | None = None,
    origin: str | None = None,
) -> BaseException:
    """Return a `PipelineError` for a `CancelledError` that `fn` raised on its own.

    Returns `exc` unchanged in every other case (#163).

    `asyncio.TaskGroup` treats a child that raises `CancelledError` as
    *cancelled*, and it ignores cancelled children. When `fn` itself raises one
    while nobody cancelled the pipeline, for example by awaiting a future or
    task that something else cancelled, the item's failure vanished. Measured
    on `main`, five items with item 1's `fn` awaiting a cancelled future, no
    exception raised either way, `return_exceptions` or not:

        process(..., concurrency=1)          -> [0, None, None, None, None]
        dispatch_tool_calls(5 calls)         -> c1 missing, 4 results
        dispatch_tool_calls(..., concurrency=1) -> 1 result
        stream(..., concurrency=1)           -> hangs forever

    *stopping* tells the two apart: it is true when the pipeline is being torn
    down -- a sibling's failure propagated, or the caller cancelled -- and then
    `CancelledError` must keep propagating (#36). Otherwise the error came out of
    `fn`. Callers pass ``failed or probe.cancelled()`` (`_CallerCancelProbe`),
    not this task's ``cancelling()``, which a TaskGroup inside `fn` leaves raised
    on 3.11/3.12 (#196). The replacement is an `Exception`, so it follows the
    `return_exceptions` policy like any other failure of `fn`.
    """
    if not isinstance(exc, asyncio.CancelledError) or stopping:
        return exc
    if origin is None:
        message = (
            f"item at index {index}: fn raised CancelledError, but the pipeline was not "
            f"cancelled -- treated as the item's failure"
        )
    else:
        # The producer side of the same vanishing (#169): `origin` names what
        # raised it when that is not an item's `fn`.
        message = (
            f"{origin} raised CancelledError, but the pipeline was not cancelled -- "
            f"treated as its failure"
        )
    err = PipelineError(message)
    err.__cause__ = exc
    return err


def _raise_if_cancellation_absorbed(stopping: bool) -> None:
    """Raise `CancelledError` if the pipeline is stopping and something absorbed it.

    For `stream`'s two long-lived loops, which hand control to code they do not
    own (#194). `process` and `dispatch_tool_calls` run one task per item, so a
    `fn` that catches the cancellation and returns just ends its task. `stream`'s
    consumer loops back to `queue.get()` and its producer pulls the next item.
    Either way the loop blocked again on a queue nobody would ever serve, because
    the `TaskGroup` had already cancelled the other side. The task then hung for
    good: `TaskGroup` cancels its children once, so cancelling `stream` again
    never reached the stuck child. Measured on 3.11 and 3.12, with `fn` returning
    a fallback from `except CancelledError`:

        process, fn absorbs                  -> cancelled
        stream, fn absorbs                   -> hung after 5 cancels
        stream, source absorbs               -> hung after 5 cancels
        asyncio.timeout(0.05) around stream  -> hung 2 s past its deadline

    *stopping* is the signal `_stray_cancellation_as_error` takes:
    ``failed or probe.cancelled()``. #194 read this task's own ``cancelling()``,
    which a TaskGroup inside `fn` that handles a child's failure leaves at 1 on
    3.11/3.12, so `stream` hung with nobody cancelling it (#196).
    """
    if stopping:
        raise asyncio.CancelledError


def _deadline_escaped(cm: asyncio.Timeout, *, stopping: bool) -> bool:
    """Is this `CancelledError` the per-item deadline that `asyncio.timeout` let through? (#200)

    `asyncio.timeout` turns its own cancellation into `TimeoutError` only when
    the task's ``cancelling()`` count, after it uncancels once, is back where it
    was on entry. A TaskGroup inside `fn` that handles a child's failure leaves
    the count raised on 3.11/3.12 (#196; fixed in 3.13). So does `fn` cancelling
    its own task and absorbing it, on every interpreter. The deadline's
    cancellation then escapes as a bare `CancelledError`, and
    `_stray_cancellation_as_error` relabelled it "fn raised CancelledError".
    Measured on 3.11.14, `timeout=0.2`, `fn(1)` handling a nested TaskGroup
    failure and then hanging:

        process / stream / dispatch_tool_calls  -> PipelineError at item 1
        raw asyncio.timeout around fn(1)        -> CancelledError, cm.expired()=True

    and `PipelineTimeoutError` / `TimeoutError` on 3.14. `cm.expired()` says the
    deadline fired. *stopping* (the same ``failed or probe.cancelled()`` the
    other cancellation sites take) keeps a real teardown propagating as
    `CancelledError` (#36).
    """
    return cm.expired() and not stopping


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
    # An int too large for a double (`10**400`) makes `math.isfinite` itself
    # raise OverflowError, which is not the ValueError this contract promises
    # (#153). It is not a finite duration either, so it takes the same exit.
    try:
        finite = math.isfinite(timeout)
    except OverflowError:
        finite = False
    if not finite or timeout <= 0:
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
    # First, before any `fn` call can be scheduled (#126).
    _refuse_bare_string("items", items)
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
    # Latched by the first failure that will propagate, BEFORE its slot is
    # released (#146). Releasing the semaphore wakes the next waiting item,
    # which ran ahead of the TaskGroup's cancellation and called `fn` -- one
    # more LLM request per failed item, after the batch had already failed
    # (concurrency=1: fn called for [0, 1]; `stream` already stopped at [0]).
    failed = False
    caller = _CallerCancelProbe()

    async def _run_one(idx: int, item: T) -> None:
        nonlocal failed
        async with sem:
            if failed:
                return
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
                    except asyncio.CancelledError as exc:
                        # The deadline fired but `asyncio.timeout` let its own
                        # cancellation through (#200); see `_deadline_escaped`.
                        if _deadline_escaped(cm, stopping=failed or caller.cancelled()):
                            raise PipelineTimeoutError(index=idx, timeout_s=timeout) from exc
                        raise
            except BaseException as caught:
                # A CancelledError that `fn` raised on its own (nobody cancelled
                # us) becomes a PipelineError here; TaskGroup would otherwise
                # drop it and leave `None` at this index (#163).
                e = _stray_cancellation_as_error(
                    caught, stopping=failed or caller.cancelled(), index=idx
                )
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
                    failed = True
                    if e is caught:
                        raise
                    raise e  # noqa: B904 -- `__cause__` is already `caught`

    async with _TaskGroup() as tg:
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

    **What is bounded, and what is not (#115).** At most ``queue_size``
    items wait in the queue and at most ``concurrency`` are in flight, at
    any ``n`` -- the producer cannot run ahead of the consumers. The
    returned list holds every result, so memory for results is O(n), and
    ``stream`` returns only once the producer is exhausted: on a source
    that never ends it never returns. ``tests/test_stream_result_retention.py``
    pins both halves.

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
        timeout: optional per-item deadline in seconds, as in ``process``:
            each ``fn(item)`` call is wrapped in ``asyncio.timeout``, and
            on expiry a ``PipelineTimeoutError`` is raised that follows the
            ``return_exceptions`` policy. Only the deadline's own firing
            maps to it; ``fn``'s own ``TimeoutError`` propagates
            unrelabeled (#66). Unlike ``process``, the error's ``.index``
            is the item's position in *consumption* order, not its
            position in the producer's output -- ``stream`` does not keep
            input order (D-003). ``None`` (the default) means no deadline
            (#178).
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

    # Consumers are started by the producer, one per item put, until
    # `concurrency` are running (#180). Starting `concurrency` of them up
    # front -- and putting as many sentinels through the bounded queue --
    # made the cap cost O(concurrency) whatever the source held: 25 s and
    # 1.3 GiB for two items at concurrency=10**6, a hang at 10**400.
    # `process` and `dispatch_tool_calls` already make one task per item.
    started = 0

    async def _produce() -> None:
        nonlocal failed, started
        try:
            async for item in producer:
                # The source may have absorbed a cancellation; stop before
                # putting into a queue whose consumers are gone (#194).
                _raise_if_cancellation_absorbed(failed or caller.cancelled())
                # Not after the batch has failed (#172/#174): the TaskGroup is
                # shutting down and the item will only be discarded.
                if started < concurrency and not failed:
                    tg.create_task(_consume())
                    started += 1
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
            # The source absorbed it, then ended (#194).
            _raise_if_cancellation_absorbed(failed or caller.cancelled())
        except BaseException as caught:
            # A stray CancelledError from the source itself (#169), the sibling
            # of #163's `fn` case. The TaskGroup ignores a child that ends
            # cancelled, so no sentinel was queued and every consumer waited on
            # `queue.get()` forever. A real cancellation still propagates.
            e = _stray_cancellation_as_error(
                caught, stopping=failed or caller.cancelled(), origin="the producer"
            )
            # The producer fails the batch too, so it latches like a consumer
            # does (#173). Without this, a consumer finishing in the same tick
            # took the next queued item and called `fn` -- at concurrency=1 --
            # before the TaskGroup's cancellation reached it.
            failed = True
            if e is caught:
                raise
            raise e  # noqa: B904 -- `__cause__` is already `caught`
        for _ in range(started):
            await queue.put(sentinel)

    # #146's fail-fast latch, which `process` and `dispatch_tool_calls` got and
    # this function did not (#171). Set by both kinds of child that can fail
    # the batch: a consumer whose `fn` failure propagates, and the producer
    # (#173). A sibling consumer finishing in the same
    # loop tick as a failure took the next item and called `fn` before the
    # TaskGroup's cancellation reached it: at concurrency 4 with equal
    # latencies, fn ran for [0..6] after item 0 had already failed the batch.
    failed = False
    caller = _CallerCancelProbe()

    async def _consume() -> None:
        nonlocal consumed_index, failed
        while True:
            # `fn` may have absorbed this task's cancellation and returned; do
            # not block on a queue the cancelled producer will never fill (#194).
            _raise_if_cancellation_absorbed(failed or caller.cancelled())
            item = await queue.get()
            if isinstance(item, _Sentinel) or failed:
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
                    except asyncio.CancelledError as exc:
                        # The deadline's own cancellation, let through (#200).
                        if _deadline_escaped(cm, stopping=failed or caller.cancelled()):
                            raise PipelineTimeoutError(index=my_idx, timeout_s=timeout) from exc
                        raise
            except BaseException as caught:
                # Stray CancelledError from `fn` (#163): without this the only
                # consumer at concurrency=1 died silently and the producer
                # blocked on a full queue forever.
                e = _stray_cancellation_as_error(
                    caught, stopping=failed or caller.cancelled(), index=my_idx
                )
                # See `process._run_one` (#36): only `Exception`s are collected
                # under `return_exceptions`. A non-`Exception` `BaseException`
                # (CancelledError / KeyboardInterrupt / SystemExit) propagates so
                # cancellation isn't silently turned into a stored result.
                if return_exceptions and isinstance(e, Exception):
                    value = e
                else:
                    failed = True
                    queue.task_done()
                    if e is caught:
                        raise
                    raise e  # noqa: B904 -- `__cause__` is already `caught`
            async with results_lock:
                results.append(value)
            queue.task_done()
            if m is not None:
                m.consumed += 1

    async with _TaskGroup() as tg:
        tg.create_task(_produce())

    return results


class _Sentinel:
    """Private sentinel for stream() to signal consumers to exit."""

    __slots__ = ()
