"""`producer_pauses` is documented the way it measures (#176, #216).

`StreamMetrics`'s docstring previously said a non-zero `producer_pauses` count
was the backpressure signal that "the consumer pool was the slower end", but in
practice `queue.put` yields for one loop step on every handoff — including when
consumers are much faster than the producer — so the count can be near n_items
with no consumer bottleneck at all (#216).

The fix (issue #216) corrects both the docstring and the README to:
1. Clarify that `producer_pauses` counts every handoff yield, not only
   queue-full waits.
2. Point operators at `producer_pause_seconds` as the reliable signal.

These tests verify the corrected documentation, not the old wording.
"""

from __future__ import annotations

import asyncio
from pathlib import Path

from async_pipelines import StreamMetrics, stream

REPO_ROOT = Path(__file__).resolve().parents[1]
README = REPO_ROOT / "README.md"

# Phrases that still falsely claim a non-zero COUNT alone signals the consumers
# are the slow end (without the pause_seconds qualification).
_STILL_MISLEADING = (
    "producer was slower",
    "producer is slower",
    "producer slower than",
    "consumer was faster",
    "consumers were faster",
    "more headroom than the consumer",
    # The phrases the issue says to remove — they overstate what the count signals.
    "non-zero means the consumer pool was the slower end",
    "non-zero pause count is the operator's signal",
)


async def _pauses(*, producer_s: float, fn_s: float) -> int:
    async def source():
        for i in range(30):
            await asyncio.sleep(producer_s)
            yield i

    async def fn(x: int) -> int:
        await asyncio.sleep(fn_s)
        return x

    m = StreamMetrics()
    await stream(source(), fn, concurrency=2, queue_size=4, metrics=m)
    return m.producer_pauses


# ---- Behavioural facts (unchanged) ----------------------------------------


async def test_a_producer_slower_than_the_consumers_never_pauses() -> None:
    assert await _pauses(producer_s=0.005, fn_s=0.0) == 0


async def test_consumers_slower_than_the_producer_make_it_pause() -> None:
    assert await _pauses(producer_s=0.0, fn_s=0.005) > 0


# ---- Prose-direction checks (updated for #216) ----------------------------


def _docstring_entry() -> str:
    doc = StreamMetrics.__doc__ or ""
    start = doc.index("producer_pauses:")
    end = doc.index("max_queue_depth:", start)
    return " ".join(doc[start:end].lower().split())


def _readme_paragraph() -> str:
    text = README.read_text(encoding="utf-8")
    start = text.index("`producer_pauses` is the count")
    end = text.index("\n\n", start)
    return " ".join(text[start:end].lower().split())


def _assert_prose_disclaims_count_and_points_to_seconds(surface: str, prose: str) -> None:
    """After #216 the documentation must:
    1. Not state that a non-zero count alone means consumers are slow.
    2. Mention that producer_pause_seconds is the reliable signal.
    """
    still_misleading = [p for p in _STILL_MISLEADING if p in prose]
    assert not still_misleading, (
        f"{surface} still has misleading backpressure claim: {still_misleading}\n{prose}"
    )
    assert "producer_pause_seconds" in prose, (
        f"{surface} does not mention producer_pause_seconds as the reliable signal\n{prose}"
    )


def test_the_docstring_names_the_consumers_as_the_slow_end() -> None:
    _assert_prose_disclaims_count_and_points_to_seconds(
        "StreamMetrics docstring", _docstring_entry()
    )


def test_the_readme_names_the_consumers_as_the_slow_end() -> None:
    _assert_prose_disclaims_count_and_points_to_seconds("README Backpressure", _readme_paragraph())


def test_the_direction_check_rejects_the_shipped_inverted_sentences() -> None:
    """Vacuity check: the old misleading sentences must fail the check above."""
    shipped = {
        "docstring": (
            "producer_pauses: count of times ``queue.put`` had to wait for space "
            "(the backpressure signal — non-zero means the consumer pool was the slower end: "
            "the producer had an item ready and the queue was full). "
            "a producer that cannot keep up with the consumers never fills the queue "
            "and leaves this at 0 (#176)."
        ),
        "readme": (
            "`producer_pauses` is the count of `queue.put`s that had to wait for "
            "room; `producer_pause_seconds` is the cumulative wall-time blocked. a "
            "non-zero pause count is the operator's signal that the consumer pool is "
            "the slower end — the producer had items ready and the queue was full — "
            "so either grow `concurrency` or accept the queue as the rate limiter. a "
            "producer that cannot keep up with the consumers never fills the queue, "
            "and the count stays at 0 (#176)."
        ),
    }
    for name, prose in shipped.items():
        try:
            _assert_prose_disclaims_count_and_points_to_seconds(name, prose.lower())
        except AssertionError:
            continue
        raise AssertionError(f"the direction check accepted the old misleading {name} sentence")
