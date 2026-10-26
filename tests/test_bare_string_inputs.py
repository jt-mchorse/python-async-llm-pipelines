"""A bare `str` is refused where a collection of documents is expected (#126).

A `str` is an `Iterable[str]`, and `docs` / `items` are coerced with `list(...)`,
so one document became one item per character and every result had the right
shape. Measured on `main` (8a21eca):

    run_pipeline(SerialPipeline(llm, llm), "summarize this doc")
        -> n_docs=18, 36 LLM calls, 18 outputs
    process("abc", fn, ...)  -> fn over 'a', 'b', 'c'
    process(b"ab", fn, ...)  -> fn over 97, 98

The stdlib's wrappers of the same two shapes got through the guard (#225).
Measured on `main` (a7b8190, py3.11):

    process(memoryview(b"hello"), fn, ...)          -> [104, 101, 108, 108, 111]
    process(collections.UserString("hello"), fn, ...) -> ['h', 'e', 'l', 'l', 'o']
    process(array.array("u", "hello"), fn, ...)     -> ['h', 'e', 'l', 'l', 'o']
    run_pipeline(SerialPipeline(llm, llm), UserString(_DOC)) -> n_docs=18, 36 calls

`stream()` is not covered here: a `str` is not an `AsyncIterable`, so it was
already loud.
"""

from __future__ import annotations

import array
import asyncio
import collections
import warnings
from typing import Any

import pytest

from async_pipelines import (
    AsyncPipeline,
    BatchedAsyncPipeline,
    FakeLLM,
    SerialPipeline,
    process,
    run_pipeline,
)

_SPLIT = "would be split into its characters"
_DOC = "summarize this doc"


def _text_array(text: str) -> array.array[str]:
    """`array('u', ...)` warns as deprecated on 3.13+; it still fans out."""
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", DeprecationWarning)
        return array.array("u", text)


# #225: the stdlib's wrappers of the str and bytes shapes.
_WRAPPERS = [
    collections.UserString(_DOC),
    memoryview(_DOC.encode()),
    memoryview(bytearray(b"ab")),
    _text_array(_DOC),
]
_BARE = [_DOC, _DOC.encode(), bytearray(b"ab"), "", *_WRAPPERS]


def _id(value: Any) -> str:
    """A stable, distinct id: a `UserString` reprs like a `str`, and a
    `memoryview` reprs as its address."""
    if isinstance(value, memoryview):
        return f"memoryview({value.tobytes()!r})"
    text = repr(value)
    return text if text.startswith(type(value).__name__) else f"{type(value).__name__}({text})"


class _CountingBatchCaller:
    """A batch caller that counts the documents it is sent.

    `make_batch_caller` never invokes the `FakeLLM` it wraps (it simulates one
    round trip and echoes), so its `call_count` stays 0 whatever happens -- an
    arm asserting zero calls through it would pass vacuously. Per-document
    counts keep the totals comparable with the other two pipelines.
    """

    def __init__(self) -> None:
        self.call_count = 0

    async def __call__(self, items: list[str]) -> list[str]:
        self.call_count += len(items)
        return [f"{item}:batched" for item in items]


def _pipelines() -> list[tuple[Any, list[Any]]]:
    """Each shipped pipeline with the callers it uses, so an arm can count."""
    s1, s2 = FakeLLM(latency_seconds=0), FakeLLM(latency_seconds=0)
    a1, a2 = FakeLLM(latency_seconds=0), FakeLLM(latency_seconds=0)
    b1, b2 = _CountingBatchCaller(), _CountingBatchCaller()
    return [
        (SerialPipeline(s1, s2), [s1, s2]),
        (AsyncPipeline(a1, a2, concurrency=4), [a1, a2]),
        (BatchedAsyncPipeline(b1, b2, concurrency=4, batch_size=4), [b1, b2]),
    ]


_NAMES = ["serial", "async", "async+batched"]


@pytest.mark.parametrize("which", range(3), ids=_NAMES)
@pytest.mark.parametrize("bare", _BARE, ids=_id)
def test_every_pipeline_run_refuses_before_any_llm_call(which: int, bare: Any) -> None:
    pipeline, llms = _pipelines()[which]
    with pytest.raises(ValueError, match=_SPLIT) as exc:
        asyncio.run(pipeline.run(bare))
    assert str(exc.value).startswith("docs must be a collection of items")
    assert [llm.call_count for llm in llms] == [0, 0]


@pytest.mark.parametrize("which", range(3), ids=_NAMES)
def test_run_pipeline_refuses_with_the_message_showing_the_working_spelling(which: int) -> None:
    pipeline, llms = _pipelines()[which]
    with pytest.raises(ValueError, match=_SPLIT) as exc:
        asyncio.run(run_pipeline(pipeline, _DOC))
    assert f"pass [{_DOC!r}]" in str(exc.value)
    assert [llm.call_count for llm in llms] == [0, 0]


class _UncheckedPipeline:
    """A third-party pipeline: `run_pipeline` takes `Any`, and this one does
    not check its input -- it just counts what it was handed."""

    name = "third-party"

    def __init__(self) -> None:
        self.seen: list[str] = []

    async def run(self, docs: list[str]) -> list[str]:
        self.seen = list(docs)
        return self.seen


def test_run_pipeline_refuses_even_when_the_pipeline_would_not() -> None:
    # `n_docs=len(docs)` is computed in `run_pipeline`, so a pipeline that
    # accepts anything would still have published a character count.
    pipeline = _UncheckedPipeline()
    with pytest.raises(ValueError, match=_SPLIT):
        asyncio.run(run_pipeline(pipeline, _DOC))
    assert pipeline.seen == []


@pytest.mark.parametrize(
    "docs",
    [[_DOC, "b"], (_DOC, "b")],
    ids=["list", "tuple"],
)
def test_run_pipeline_collections_are_unchanged(docs: Any) -> None:
    for pipeline, llms in _pipelines():
        result = asyncio.run(run_pipeline(pipeline, docs))
        assert result.n_docs == 2
        assert sum(llm.call_count for llm in llms) == 4


def test_the_working_spelling_is_one_document() -> None:
    for pipeline, llms in _pipelines():
        result = asyncio.run(run_pipeline(pipeline, [_DOC]))
        assert result.n_docs == 1
        assert sum(llm.call_count for llm in llms) == 2


# --- process ------------------------------------------------------------------


@pytest.mark.parametrize(
    "bare",
    [
        "abc",
        b"ab",
        bytearray(b"ab"),
        "",
        collections.UserString("abc"),
        memoryview(b"ab"),
        _text_array("abc"),
    ],
    ids=_id,
)
def test_process_refuses_before_any_fn_call(bare: Any) -> None:
    calls: list[Any] = []

    async def fn(item: Any) -> Any:
        calls.append(item)
        return item

    with pytest.raises(ValueError, match=_SPLIT) as exc:
        asyncio.run(process(bare, fn, concurrency=2))
    assert str(exc.value).startswith("items must be a collection of items")
    assert calls == []


@pytest.mark.parametrize(
    "items",
    [["a", "b", "c"], ("a", "b", "c"), (x for x in ["a", "b", "c"]), iter(["a", "b", "c"])],
    ids=["list", "tuple", "generator", "iterator"],
)
def test_process_collections_are_unchanged(items: Any) -> None:
    async def fn(item: str) -> str:
        return item.upper()

    assert asyncio.run(process(items, fn, concurrency=2)) == ["A", "B", "C"]


def test_process_a_list_of_one_string_is_one_item() -> None:
    async def fn(item: str) -> int:
        return len(item)

    assert asyncio.run(process(["abc"], fn, concurrency=2)) == [3]


# --- #225: the wrappers' hints, and what is still accepted --------------------


@pytest.mark.parametrize(
    ("bare", "hint"),
    [
        (collections.UserString(_DOC), f"pass [{_DOC!r}]"),
        (memoryview(_DOC.encode()), "decode it to str first"),
        (_text_array(_DOC), "convert it with .tounicode() first"),
    ],
    ids=["UserString", "memoryview", "array-u"],
)
def test_run_pipeline_refuses_each_wrapper_with_its_hint(bare: Any, hint: str) -> None:
    for pipeline, llms in _pipelines():
        with pytest.raises(ValueError, match=_SPLIT) as exc:
            asyncio.run(run_pipeline(pipeline, bare))
        assert str(exc.value).endswith(hint)
        assert [llm.call_count for llm in llms] == [0, 0]


def test_a_list_of_userstrings_is_still_accepted() -> None:
    # The guard looks at the container, not its items: a list of UserString
    # documents is a collection, exactly like a list of str.
    for pipeline, llms in _pipelines():
        result = asyncio.run(run_pipeline(pipeline, [collections.UserString(_DOC), "b"]))
        assert result.n_docs == 2
        assert sum(llm.call_count for llm in llms) == 4


@pytest.mark.parametrize(
    "items",
    [
        array.array("i", [1, 2, 3]),
        array.array("d", [1.0, 2.0, 3.0]),
        memoryview(array.array("i", [1, 2, 3])),
        memoryview(b"abc").tolist(),
    ],
    ids=["array-i", "array-d", "memoryview-of-array-i", "memoryview.tolist"],
)
def test_process_numeric_collections_are_still_accepted(items: Any) -> None:
    # A numeric array, or a memoryview over one, is a collection of numbers,
    # not one document. Only the text typecodes and byte formats are refused.
    async def fn(item: Any) -> Any:
        return item * 2

    assert asyncio.run(process(items, fn, concurrency=2)) == [x * 2 for x in items]
