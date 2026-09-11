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


class _FatalSignal(BaseException):
    """Non-Exception BaseException stand-in (see test_process.py #36)."""


async def test_stream_return_exceptions_does_not_swallow_base_exception():
    # Regression for #36: a non-Exception BaseException raised by fn must
    # propagate out of the consumer pool instead of being stored as a result.
    async def fn(x: int) -> int:
        if x == 3:
            raise _FatalSignal("fatal mid-stream")
        return x * 10

    with pytest.raises(BaseExceptionGroup) as ei:
        await stream(_producer(6), fn, concurrency=2, queue_size=3, return_exceptions=True)
    assert any(isinstance(e, _FatalSignal) for e in ei.value.exceptions)


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


#: `(n, queue_size)` pairs for the OOM-safety invariant, chosen so the table can
#: separate a real bound from a wrong one (#111).
#:
#: The claim is quantified over `n` -- "no matter how many items flow through" in
#: this test's own docstring, and "regardless of `n`" in `docs/backpressure.md`.
#: Both used to demonstrate it at a SINGLE point: `n=1000, queue_size=8`. A bound
#: accidentally hardcoded to `8` passes that, and so does one keyed to
#: `concurrency` when `concurrency` happens to be below 8.
#:
#: So the rows span the three classes that behave differently:
#:   * `n < queue_size` -- the queue never fills, and the bound is satisfied
#:     TRIVIALLY. Included deliberately: it is the row that proves the others are
#:     doing work, because a broken bound cannot show here.
#:   * `n == queue_size` -- the boundary.
#:   * `n >> queue_size`, at more than one `queue_size` -- the only region where a
#:     bound keyed to the wrong variable is visible at all.
_DEPTH_CASES = [
    pytest.param(1, 8, id="n-far-below-queue"),
    pytest.param(7, 8, id="n-just-below-queue"),
    pytest.param(8, 8, id="n-equals-queue"),
    pytest.param(9, 8, id="n-just-above-queue"),
    pytest.param(200, 8, id="n-well-above-queue-8"),
    pytest.param(200, 1, id="queue-1-the-tightest-bound"),
    pytest.param(200, 2, id="queue-2"),
    pytest.param(200, 64, id="queue-64-above-concurrency"),
    pytest.param(1000, 8, id="the-original-single-point"),
]


@pytest.mark.parametrize(("n", "queue_size"), _DEPTH_CASES)
async def test_stream_metrics_max_depth_bounded_by_queue_size(n: int, queue_size: int):
    """No matter how many items flow through, max_queue_depth must
    never exceed queue_size — this is the OOM-safety invariant.

    Parametrised in #111. The docstring above made a claim about every `n` and the
    body ran one, which is the same gap `docs/backpressure.md` had: it says the
    bound holds "regardless of `n`" and every row of its table has `n=5000`,
    because `scripts/bench_backpressure.py` had no `n` axis to vary.
    """
    m = StreamMetrics()

    async def slow(x: int) -> int:
        await asyncio.sleep(0.001)
        return x

    out = await stream(_producer(n), slow, concurrency=2, queue_size=queue_size, metrics=m)
    assert len(out) == n
    assert m.max_queue_depth <= queue_size, (
        f"max_queue_depth {m.max_queue_depth} exceeded queue_size {queue_size} at "
        f"n={n} — backpressure invariant broken"
    )
    # The bound is on the queue, not on the work: every item must still come out.
    assert m.produced == n
    assert m.consumed == n


async def test_the_depth_table_separates_a_real_bound_from_a_wrong_one():
    """The parametrize must prove its parameters do different things (#111).

    A table whose every row passes against a wrong bound is not evidence. This
    runs the two wrong bounds a reader would plausibly write — a hardcoded
    `queue_size` and one keyed to `concurrency` — over the same rows, and asserts
    each is violated by at least one of them. If this ever goes green, the table
    has stopped separating anything and the rows need widening, not the claim.
    """
    hardcoded_8_violations = 0
    concurrency_violations = 0
    for case in _DEPTH_CASES:
        n, queue_size = case.values
        m = StreamMetrics()

        async def slow(x: int) -> int:
            await asyncio.sleep(0.001)
            return x

        await stream(_producer(n), slow, concurrency=2, queue_size=queue_size, metrics=m)
        if m.max_queue_depth > 8:
            hardcoded_8_violations += 1
        if m.max_queue_depth > 2:  # `concurrency=2`
            concurrency_violations += 1

    assert hardcoded_8_violations > 0, (
        "no row reaches a depth above 8, so a bound hardcoded to 8 would pass every "
        "row — the table does not separate it from the real bound"
    )
    assert concurrency_violations > 0, (
        "no row reaches a depth above `concurrency`, so a bound keyed to concurrency "
        "would pass every row"
    )


def test_the_invariant_test_is_actually_parametrised_over_the_case_table():
    """`_DEPTH_CASES` must be USED, not merely present (#111).

    Measured: reverting the invariant test to its original single point
    (`n=1000, queue_size=8`) while leaving `_DEPTH_CASES` in place turns **nothing**
    red. `test_the_depth_table_separates_a_real_bound_from_a_wrong_one` iterates the
    table directly, so it keeps passing, and the evidence silently shrinks from nine
    rows to one — which is the defect #111 is about, reappearing by deletion.

    A case table with no assertion that it is wired into the test it was written for
    is one the next edit can orphan. Same lesson as the call-site arm in
    mcp-server-cookbook#172.
    """
    import inspect

    src = inspect.getsource(test_stream_metrics_max_depth_bounded_by_queue_size)
    for param in ("n: int", "queue_size: int"):
        assert param in src, (
            f"the invariant test no longer takes `{param.split(':')[0]}` as a "
            "parameter, so it is running a single hardcoded point again"
        )
    module_src = inspect.getsource(inspect.getmodule(_producer))
    assert '@pytest.mark.parametrize(("n", "queue_size"), _DEPTH_CASES)' in module_src, (
        "the invariant test is not parametrised over `_DEPTH_CASES`; the table is "
        "orphaned and the evidence is a single point"
    )
    assert len(_DEPTH_CASES) >= 6, (
        f"_DEPTH_CASES shrank to {len(_DEPTH_CASES)} rows; the three classes "
        "(n < queue_size, n == queue_size, n >> queue_size at several queue_size) "
        "need more than that"
    )
    # The classes must all be present, discovered from the table rather than trusted.
    pairs = [c.values for c in _DEPTH_CASES]
    assert any(n < q for n, q in pairs), "no `n < queue_size` row"
    assert any(n == q for n, q in pairs), "no `n == queue_size` row"
    assert len({q for n, q in pairs if n > q}) >= 3, (
        "the `n >> queue_size` rows span fewer than three queue_size values, so a "
        "bound hardcoded to one of them is not separated"
    )


async def test_a_queue_that_never_fills_is_in_the_table_on_purpose():
    """The trivially-satisfied row earns its place by being named.

    `n < queue_size` cannot catch a broken bound — the queue never fills. It is in
    `_DEPTH_CASES` so the suite records that the invariant is also claimed there,
    and this test states why it proves nothing on its own, so nobody later reads
    the row count as nine independent pieces of evidence.
    """
    m = StreamMetrics()

    async def slow(x: int) -> int:
        await asyncio.sleep(0.001)
        return x

    await stream(_producer(1), slow, concurrency=2, queue_size=8, metrics=m)
    assert m.max_queue_depth <= 1, (
        "with a single item the depth cannot exceed 1; if this fails the producer is "
        "enqueueing more than it was given"
    )


async def test_stream_metrics_omitted_means_no_overhead_path():
    """metrics=None (the default) leaves the function on the non-metric
    code path. We just assert the behavior is unchanged.
    """
    out = await stream(_producer(20), _doubler, concurrency=4, queue_size=5)
    assert sorted(out) == [x * 2 for x in range(20)]


# ----------------------------------------------------------------------
# #46: StreamMetrics.to_dict — explicit field-by-field contract
# (excludes the private _started_monotonic field). Sibling of
# Workload.to_dict / RunResult.to_dict shipped in #44/#45.
# ----------------------------------------------------------------------


def test_stream_metrics_to_dict_field_set_is_pinned():
    m = StreamMetrics()
    d = m.to_dict()
    assert sorted(d.keys()) == [
        "consumed",
        "max_queue_depth",
        "produced",
        "producer_pause_seconds",
        "producer_pauses",
    ]


def test_stream_metrics_to_dict_excludes_started_monotonic():
    # The private `_started_monotonic` field must not leak into JSON
    # consumers. `asdict(m)` would have included it — the to_dict
    # contract is the regression net.
    m = StreamMetrics()
    m._started_monotonic = 1234.5
    d = m.to_dict()
    assert "_started_monotonic" not in d
    # And the public surface still reflects the right values.
    assert d["produced"] == 0
    assert d["consumed"] == 0


def test_stream_metrics_to_dict_values_round_trip():
    m = StreamMetrics(
        produced=10,
        consumed=8,
        producer_pauses=2,
        max_queue_depth=4,
        producer_pause_seconds=0.05,
    )
    assert m.to_dict() == {
        "produced": 10,
        "consumed": 8,
        "producer_pauses": 2,
        "max_queue_depth": 4,
        "producer_pause_seconds": 0.05,
    }
