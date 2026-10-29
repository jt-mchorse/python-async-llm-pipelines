"""The stdlib's other per-byte / per-character buffers are refused too (#239).

#126 refused a bare `str` / `bytes` and #225 their stdlib wrappers, but an
`mmap.mmap` is a bytes buffer that iterates one byte at a time and got through.
Measured on `main` (ef71f03), `doc.txt` = `summarize this doc`:

    list(mmap)[:3]                              -> [b's', b'u', b'm']
    run_pipeline(SerialPipeline(l1, l2), mmap)  -> n_docs=18, 36 calls
    process(memoryview(mmap), ...)              -> ValueError (refused)

A walk of the stdlib catalog for classes that export a buffer and iterate found
one more shape. `ctypes.create_string_buffer` / `create_unicode_buffer` fan out
per character:

    process(ctypes.create_string_buffer(b"doc"), ...)  -> [b'd', b'o', b'c', b'\\x00']
    process(ctypes.create_unicode_buffer("doc"), ...)  -> ['d', 'o', 'c', '\\x00']

Numeric buffers are still collections of numbers, as #225 decided for
`array('B')`, so a `c_ubyte` array is accepted too.
"""

from __future__ import annotations

import array
import asyncio
import ctypes
import mmap
from collections.abc import Callable, Iterator
from pathlib import Path
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


@pytest.fixture
def mapped(tmp_path: Path) -> Iterator[mmap.mmap]:
    """A read-only map of a file holding `_DOC`, as in the issue."""
    path = tmp_path / "doc.txt"
    path.write_bytes(_DOC.encode())
    with path.open("rb") as f, mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ) as m:
        yield m


def _anon_map() -> mmap.mmap:
    m = mmap.mmap(-1, len(_DOC))
    m.write(_DOC.encode())
    m.seek(0)
    return m


# (id, factory, hint). Factories, so every arm gets a fresh buffer.
_REFUSED: list[tuple[str, Callable[[], Any], str]] = [
    ("mmap", _anon_map, "decode it to str first"),
    (
        "ctypes-string-buffer",
        lambda: ctypes.create_string_buffer(_DOC.encode()),
        "read its .value first",
    ),
    ("ctypes-unicode-buffer", lambda: ctypes.create_unicode_buffer(_DOC), "read its .value first"),
    ("ctypes-c_char-array", lambda: (ctypes.c_char * 3)(*b"abc"), "read its .value first"),
]


class _CountingBatchCaller:
    """`make_batch_caller` never calls the `FakeLLM` it wraps, so count here."""

    def __init__(self) -> None:
        self.call_count = 0

    async def __call__(self, items: list[str]) -> list[str]:
        self.call_count += len(items)
        return [f"{item}:batched" for item in items]


def _pipelines() -> list[tuple[Any, list[Any]]]:
    s1, s2 = FakeLLM(latency_seconds=0), FakeLLM(latency_seconds=0)
    a1, a2 = FakeLLM(latency_seconds=0), FakeLLM(latency_seconds=0)
    b1, b2 = _CountingBatchCaller(), _CountingBatchCaller()
    return [
        (SerialPipeline(s1, s2), [s1, s2]),
        (AsyncPipeline(a1, a2, concurrency=4), [a1, a2]),
        (BatchedAsyncPipeline(b1, b2, concurrency=4, batch_size=4), [b1, b2]),
    ]


_NAMES = ["serial", "async", "async+batched"]


def test_the_issue_repro_is_refused_before_any_llm_call(mapped: mmap.mmap) -> None:
    for pipeline, llms in _pipelines():
        with pytest.raises(ValueError, match=_SPLIT) as exc:
            asyncio.run(run_pipeline(pipeline, mapped))
        assert str(exc.value).startswith("docs must be a collection of items, not a bare mmap")
        assert str(exc.value).endswith("decode it to str first")
        assert [llm.call_count for llm in llms] == [0, 0]


@pytest.mark.parametrize("which", range(3), ids=_NAMES)
@pytest.mark.parametrize(
    ("make", "hint"), [(m, h) for _, m, h in _REFUSED], ids=[i for i, _, _ in _REFUSED]
)
def test_every_pipeline_run_refuses_with_its_hint(
    which: int, make: Callable[[], Any], hint: str
) -> None:
    pipeline, llms = _pipelines()[which]
    with pytest.raises(ValueError, match=_SPLIT) as exc:
        asyncio.run(pipeline.run(make()))
    assert str(exc.value).startswith("docs must be a collection of items")
    assert str(exc.value).endswith(hint)
    assert [llm.call_count for llm in llms] == [0, 0]


@pytest.mark.parametrize("which", range(3), ids=_NAMES)
@pytest.mark.parametrize("make", [m for _, m, _ in _REFUSED], ids=[i for i, _, _ in _REFUSED])
def test_run_pipeline_refuses(which: int, make: Callable[[], Any]) -> None:
    pipeline, llms = _pipelines()[which]
    with pytest.raises(ValueError, match=_SPLIT):
        asyncio.run(run_pipeline(pipeline, make()))
    assert [llm.call_count for llm in llms] == [0, 0]


@pytest.mark.parametrize(
    ("make", "hint"), [(m, h) for _, m, h in _REFUSED], ids=[i for i, _, _ in _REFUSED]
)
def test_process_refuses_before_any_fn_call(make: Callable[[], Any], hint: str) -> None:
    calls: list[Any] = []

    async def fn(item: Any) -> Any:
        calls.append(item)
        return item

    with pytest.raises(ValueError, match=_SPLIT) as exc:
        asyncio.run(process(make(), fn, concurrency=2))
    assert str(exc.value).startswith("items must be a collection of items")
    assert str(exc.value).endswith(hint)
    assert calls == []


# --- what is still accepted ---------------------------------------------------


@pytest.mark.parametrize(
    "make",
    [
        lambda: array.array("B", b"abc"),
        lambda: (ctypes.c_ubyte * 3)(1, 2, 3),
        lambda: (ctypes.c_int * 3)(1, 2, 3),
    ],
    ids=["array-B", "ctypes-c_ubyte-array", "ctypes-c_int-array"],
)
def test_process_numeric_buffers_are_still_accepted(make: Callable[[], Any]) -> None:
    # #225 accepts `array('B')` as numbers; a numeric ctypes array is the same.
    async def fn(item: int) -> int:
        return item * 2

    items = make()
    assert asyncio.run(process(items, fn, concurrency=2)) == [x * 2 for x in list(items)]


def test_the_working_spellings_are_one_document(mapped: mmap.mmap) -> None:
    # The hints name these: decode the map, read the buffer's value.
    for docs in (
        [mapped[:].decode()],
        [ctypes.create_string_buffer(_DOC.encode()).value.decode()],
        [ctypes.create_unicode_buffer(_DOC).value],
    ):
        for pipeline, llms in _pipelines():
            result = asyncio.run(run_pipeline(pipeline, docs))
            assert result.n_docs == 1
            assert sum(llm.call_count for llm in llms) == 2


def test_a_list_of_buffers_is_a_collection(mapped: mmap.mmap) -> None:
    # The guard looks at the container: a list holding a map is two items.
    async def fn(item: Any) -> int:
        return len(item)

    assert asyncio.run(process([mapped, b"ab"], fn, concurrency=2)) == [len(_DOC), 2]
