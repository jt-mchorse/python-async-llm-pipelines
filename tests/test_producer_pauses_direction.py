"""`producer_pauses` is documented the way it measures (#176).

`StreamMetrics`'s docstring said::

    producer_pauses: count of times ``queue.put`` had to wait for space
        (the backpressure signal -- non-zero means the producer was slower
        than the consumer pool).

A `put` waits only on a full queue, and the queue fills only when the producer
is *faster* than the consumers. Measured on `main`, `concurrency=2`,
`queue_size=4`, 50 items:

    producer 5 ms/item, fn 1 ms  (producer slower) -> pauses 0
    producer 0 ms/item, fn 5 ms  (consumers slower) -> pauses 24

The metric exists so an operator "can see *which* end of the pipeline is the
bottleneck" (README, #3), and the docstring named the wrong end. The README's
sentence said a non-zero count means "fan-out has more headroom than the
consumer can use", which describes idle consumers, and idle consumers give 0.

The behaviour was already pinned the right way in `tests/test_stream.py`.
Nothing read the prose. As in `test_real_api_claim_direction.py` (#108), these
tests check the DIRECTION each surface states, not one wording, and the
behaviour arms measure the direction the prose has to match.
"""

from __future__ import annotations

import asyncio
from pathlib import Path

from async_pipelines import StreamMetrics, stream

REPO_ROOT = Path(__file__).resolve().parents[1]
README = REPO_ROOT / "README.md"

# Phrases that put the slow end on the producer side (or the consumers idle).
# Each states the inverted direction, whatever the surrounding wording.
_PRODUCER_IS_SLOW = (
    "producer was slower",
    "producer is slower",
    "producer slower than",
    "consumer was faster",
    "consumers were faster",
    "more headroom than the consumer",
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


async def test_a_producer_slower_than_the_consumers_never_pauses() -> None:
    assert await _pauses(producer_s=0.005, fn_s=0.0) == 0


async def test_consumers_slower_than_the_producer_make_it_pause() -> None:
    assert await _pauses(producer_s=0.0, fn_s=0.005) > 0


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


def _assert_names_the_consumers_as_the_slow_end(surface: str, prose: str) -> None:
    inverted = [p for p in _PRODUCER_IS_SLOW if p in prose]
    assert not inverted, f"{surface} puts the slow end on the producer: {inverted}\n{prose}"
    missing = [w for w in ("consumer", "slower") if w not in prose]
    assert not missing, (
        f"{surface} does not say which end a non-zero pause count points at\n{prose}"
    )
    # The side named slower must be the consumers', not the producer's.
    assert prose.index("consumer") < prose.index("slower"), prose


def test_the_docstring_names_the_consumers_as_the_slow_end() -> None:
    _assert_names_the_consumers_as_the_slow_end("StreamMetrics docstring", _docstring_entry())


def test_the_readme_names_the_consumers_as_the_slow_end() -> None:
    _assert_names_the_consumers_as_the_slow_end("README Backpressure", _readme_paragraph())


def test_the_direction_check_rejects_the_shipped_inverted_sentences() -> None:
    """Vacuity check: both sentences #176 replaced must fail the check above."""
    shipped = {
        "docstring": (
            "producer_pauses: count of times ``queue.put`` had to wait for space "
            "(the backpressure signal — non-zero means the producer was slower than "
            "the consumer pool)."
        ),
        "readme": (
            "a non-zero pause count is the operator's signal that fan-out has more "
            "headroom than the consumer can use — either grow `concurrency` or accept "
            "the queue as the rate limiter."
        ),
    }
    for name, prose in shipped.items():
        try:
            _assert_names_the_consumers_as_the_slow_end(name, prose.lower())
        except AssertionError:
            continue
        raise AssertionError(f"the direction check accepted the inverted {name} sentence")
