"""What `stream`'s queue bounds, and what it does not (#115).

#111 proved `max_queue_depth <= queue_size` at every `n`, and that is true. Four
surfaces then called it "peak in-memory items are O(queue_size)" and "safe to
point at an unbounded source". `stream` returns every result in one list
(D-003), so the results are O(n), and the committed `docs/backpressure.md` table
showed it in its own `peak_heap_kb` column: 21.4 -> 202.7 KB as `n` went
500 -> 5000 at `queue_size=8`.

Both halves are pinned here by **live-object count**, not by `tracemalloc`
bytes, so the arms are host-independent in the way `test_stream.py`'s depth
table is.
"""

from __future__ import annotations

import gc
import json
import re
import sys
from pathlib import Path

import pytest

from async_pipelines import stream

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scripts"))

import bench_backpressure  # noqa: E402


class _Counted:
    """Counts its own live instances. CPython frees on the last reference, so
    `live` is exact at every await point."""

    live = 0
    peak = 0

    def __init__(self) -> None:
        type(self).live += 1
        type(self).peak = max(type(self).peak, type(self).live)

    def __del__(self) -> None:
        type(self).live -= 1


class _Item(_Counted):
    live = 0
    peak = 0


class _Result(_Counted):
    live = 0
    peak = 0


def _reset() -> None:
    gc.collect()
    for cls in (_Item, _Result):
        cls.live = 0
        cls.peak = 0


async def _producer(n: int):
    for _ in range(n):
        yield _Item()


async def _consume(item: _Item) -> _Result:
    # Yield to the loop so the producer can fill the queue: without this the
    # consumers keep up and the bound is never exercised.
    import asyncio

    await asyncio.sleep(0)
    return _Result()


_QUEUE, _CONC = 8, 2
# Producer holds one item in hand while blocked on `put`; each consumer holds
# the one it is working on.
_INPUT_BOUND = _QUEUE + _CONC + 1


@pytest.mark.parametrize("n", [200, 2000])
async def test_input_waiting_is_bounded_and_results_are_not(n: int) -> None:
    _reset()
    out = await stream(_producer(n), _consume, concurrency=_CONC, queue_size=_QUEUE)
    assert len(out) == n
    # The half #111 proved, restated as live input objects rather than queue
    # depth: the producer never runs more than a queue's worth ahead.
    assert _Item.peak <= _INPUT_BOUND, (_Item.peak, _INPUT_BOUND)
    # The half the four surfaces claimed and was false: every result is alive
    # at return, so memory for results is O(n).
    assert _Result.live == n
    del out
    gc.collect()
    assert _Result.live == 0


async def test_the_input_bound_is_the_same_at_both_n_and_the_result_count_is_not() -> None:
    """The comparison is the claim: across a 10x `n`, peak live input is flat
    and live results scale 10x."""
    peaks, results = [], []
    for n in (200, 2000):
        _reset()
        out = await stream(_producer(n), _consume, concurrency=_CONC, queue_size=_QUEUE)
        peaks.append(_Item.peak)
        results.append(_Result.live)
        del out
    assert peaks[0] == peaks[1] <= _INPUT_BOUND
    assert results == [200, 2000]


async def test_the_retention_arm_can_tell_a_stream_that_drops_results() -> None:
    """Vacuity check: the arm above must separate the real `stream` from one
    whose results do not accumulate. Wrapping `fn` to discard its value is
    that neighbour, and it retains none."""
    _reset()

    async def discard(item: _Item) -> None:
        await _consume(item)

    out = await stream(_producer(200), discard, concurrency=_CONC, queue_size=_QUEUE)
    assert len(out) == 200
    assert _Result.live == 0  # the real arm asserts 200 here


# ----------------------------------------------------------------------
# The surfaces
# ----------------------------------------------------------------------

_OVERCLAIMS = (
    "peak in-memory items are O(queue_size)",
    "safe to point at an unbounded source",
    "OOM-safety invariant for pointing this",
    "holds peak heap",
    "Peak items in",
)

_SURFACES = (
    "README.md",
    "docs/backpressure.md",
    "scripts/bench_backpressure.py",
    "async_pipelines/core.py",
    "async_pipelines/__init__.py",
)


@pytest.mark.parametrize("path", _SURFACES)
def test_no_surface_states_the_queue_bound_as_a_memory_bound(path: str) -> None:
    text = re.sub(r"\s+", " ", (ROOT / path).read_text(encoding="utf-8"))
    # A surface may quote the old wording to say it was wrong; it must then
    # name #115 in the same sentence-ish window.
    for phrase in _OVERCLAIMS:
        for m in re.finditer(re.escape(phrase), text):
            window = text[max(0, m.start() - 400) : m.end() + 400]
            assert "#115" in window, f"{path}: {phrase!r} stated without the #115 correction"


def test_the_committed_report_is_the_renderer_over_the_committed_json() -> None:
    """The md was re-rendered from the committed JSON, not re-measured: the two
    stay one run, and the new sentence is exactly what the renderer says."""
    payload = json.loads((ROOT / "docs/backpressure.json").read_text(encoding="utf-8"))
    rows = [bench_backpressure.BackpressureResult(**r) for r in payload["results"]]
    rendered = bench_backpressure._render_markdown(rows)
    assert (ROOT / "docs/backpressure.md").read_text(encoding="utf-8") == rendered


def test_the_rendered_heap_sentence_quotes_its_own_rows() -> None:
    """The figures in the sentence are the rows' figures, so the sentence
    cannot contradict the column beside it."""
    rows = [
        bench_backpressure.BackpressureResult(
            n=n,
            queue_size=8,
            consumer_ms=1.0,
            concurrency=2,
            duration_s=0.1,
            peak_heap_kb=heap,
            metrics={},
        )
        for n, heap in ((100, 10.0), (1000, 95.0))
    ]
    sentence = bench_backpressure._results_are_o_n_sentence(rows)
    assert "results are O(n)" in sentence
    assert "10.0 → 95.0 (9.5×)" in sentence
    assert "100 → 1000 (10.0×)" in sentence


def test_a_single_n_table_states_the_fact_without_inventing_a_ratio() -> None:
    rows = [
        bench_backpressure.BackpressureResult(
            n=500,
            queue_size=q,
            consumer_ms=1.0,
            concurrency=2,
            duration_s=0.1,
            peak_heap_kb=20.0,
            metrics={},
        )
        for q in (8, 32)
    ]
    sentence = bench_backpressure._results_are_o_n_sentence(rows)
    assert "results are O(n)" in sentence
    assert "×" not in sentence
