"""`run_pipeline` times a one-shot iterable like a list on every pipeline (#236).

`n_docs=len(docs)` ran *after* `pipeline.run(docs)`, so a generator had every
LLM call spent on the serial and async pipelines -- paid calls with a real
client -- and then raised `TypeError: object of type 'generator' has no len()`,
while the batched pipeline failed in `_chunk` with no call at all. `docs` is now
read once, before any work.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Iterable
from typing import Any

import pytest

from async_pipelines.benchmark import run_pipeline
from tests.test_bare_string_inputs import _NAMES, _pipelines

DOCS = ["alpha", "beta", "gamma", "delta", "epsilon"]

ONE_SHOT: dict[str, Callable[[], Iterable[str]]] = {
    "generator": lambda: (d for d in DOCS),
    "map": lambda: map(str, DOCS),
    "iter": lambda: iter(DOCS),
    "tuple": lambda: tuple(DOCS),
    "dict keys": lambda: dict.fromkeys(DOCS).keys(),
}


def _counts(which: int, docs: Iterable[str]) -> tuple[int, list[int]]:
    pipeline, llms = _pipelines()[which]
    result: Any = asyncio.run(run_pipeline(pipeline, docs))
    return result.n_docs, [llm.call_count for llm in llms]


@pytest.mark.parametrize("which", range(3), ids=_NAMES)
@pytest.mark.parametrize("shape", list(ONE_SHOT))
def test_a_one_shot_iterable_is_timed_like_the_list(which: int, shape: str) -> None:
    assert _counts(which, ONE_SHOT[shape]()) == _counts(which, list(DOCS))


@pytest.mark.parametrize("which", range(3), ids=_NAMES)
def test_n_docs_counts_the_documents_not_the_leftovers(which: int) -> None:
    n_docs, _ = _counts(which, (d for d in DOCS))
    assert n_docs == len(DOCS)
