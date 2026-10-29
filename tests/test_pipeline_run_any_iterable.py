"""Every pipeline's `.run(docs)` accepts any iterable of docs, read once (#240).

`BatchedAsyncPipeline.run` handed `docs` straight to `_chunk`, which slices it
and takes `len()`. A generator, a `deque` or a dict-keys view raised a raw
`TypeError` there, while the serial and async pipelines -- which iterate `docs`
once -- accepted the same input. #236 added a `list(docs)` read to
`run_pipeline` only, so the exported pipeline's own `.run` still failed.

The rows call `.run()` directly, not through `run_pipeline`: that is the
surface #236 did not cover.
"""

from __future__ import annotations

import asyncio
import collections
from collections.abc import Callable, Iterable
from typing import Any

import pytest

from async_pipelines import (
    AsyncPipeline,
    BatchedAsyncPipeline,
    FakeLLM,
    SerialPipeline,
)

DOCS = ["alpha", "beta", "gamma", "delta", "epsilon"]

# `list` and `tuple` worked on all three before #240; they are the controls.
SHAPES: dict[str, Callable[[], Iterable[str]]] = {
    "generator": lambda: (d for d in DOCS),
    "map": lambda: map(str, DOCS),
    "iter": lambda: iter(DOCS),
    "deque": lambda: collections.deque(DOCS),
    "dict keys": lambda: dict.fromkeys(DOCS).keys(),
    "dict values": lambda: {i: d for i, d in enumerate(DOCS)}.values(),
    "tuple": lambda: tuple(DOCS),
    "list": lambda: list(DOCS),
}


class _CountingBatchCaller:
    """A batch endpoint that records each batch it was sent."""

    def __init__(self, tag: str) -> None:
        self.tag = tag
        self.batches: list[list[str]] = []

    async def __call__(self, items: list[str]) -> list[str]:
        self.batches.append(list(items))
        return [f"{item}:{self.tag}" for item in items]


def _pipelines() -> list[tuple[Any, Callable[[], Any]]]:
    """Each shipped pipeline, plus a reading of what its callers were sent."""
    s1, s2 = FakeLLM(latency_seconds=0), FakeLLM(latency_seconds=0)
    a1, a2 = FakeLLM(latency_seconds=0), FakeLLM(latency_seconds=0)
    b1, b2 = _CountingBatchCaller("one"), _CountingBatchCaller("two")
    return [
        (SerialPipeline(s1, s2), lambda: (s1.call_count, s2.call_count)),
        (AsyncPipeline(a1, a2, concurrency=2), lambda: (a1.call_count, a2.call_count)),
        (
            BatchedAsyncPipeline(b1, b2, concurrency=2, batch_size=2),
            lambda: (b1.batches, b2.batches),
        ),
    ]


_NAMES = ["serial", "async", "async+batched"]


def _run(which: int, docs: Iterable[str]) -> tuple[list[str], Any]:
    pipeline, calls = _pipelines()[which]
    outs = asyncio.run(pipeline.run(docs))
    return outs, calls()


@pytest.mark.parametrize("which", range(3), ids=_NAMES)
@pytest.mark.parametrize("shape", list(SHAPES))
def test_run_on_any_iterable_matches_the_list(which: int, shape: str) -> None:
    # Same outputs in the same order, and the callers were sent the same thing.
    assert _run(which, SHAPES[shape]()) == _run(which, list(DOCS))


@pytest.mark.parametrize("shape", list(SHAPES))
def test_the_three_pipelines_agree_on_how_many_docs_they_processed(shape: str) -> None:
    lengths = [len(_run(which, SHAPES[shape]())[0]) for which in range(3)]
    assert lengths == [len(DOCS)] * 3


def test_batched_run_reads_a_generator_once_and_batches_every_doc() -> None:
    # A partial read (e.g. `len()` of a list built from what is left after a
    # first pass) would batch fewer docs than were passed.
    reads: list[str] = []

    def docs() -> Iterable[str]:
        for d in DOCS:
            reads.append(d)
            yield d

    outs, (first, second) = _run(2, docs())
    assert reads == DOCS
    assert first == [["alpha", "beta"], ["gamma", "delta"], ["epsilon"]]
    assert [item for batch in second for item in batch] == [f"{d}:one" for d in DOCS]
    assert outs == [f"{d}:one:two" for d in DOCS]


def test_batched_run_on_an_empty_generator_returns_no_outputs_and_sends_nothing() -> None:
    outs, (first, second) = _run(2, (d for d in ()))
    assert (outs, first, second) == ([], [], [])
