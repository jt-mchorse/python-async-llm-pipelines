"""A batch caller must return one output per input (#226).

`BatchedAsyncPipeline.run` never checked it. A short batch dropped docs
silently while `run_pipeline` still reported `n_docs=len(docs)`, so docs/s was
inflated, and `llm2_batch` was handed the shortened `mids`, so every later doc
in that batch was paired with the wrong output. Measured on `main` (a7b8190,
py3.11), 8 docs, `batch_size=4`, `short = lambda batch: batch[:-1]`:

    BatchedAsyncPipeline(short, short).run(docs)  -> 4 outputs, no error
    run_pipeline(that pipeline, docs).n_docs      -> 8

Real batch APIs drop errored or expired items, and D-008 / #181 tell operators
to plug one in here. The pipeline runs batches through fail-fast `process`, so
the `ValueError` arrives inside an `ExceptionGroup`, like any other caller
failure (see `tests/test_exception_contract.py`).
"""

from __future__ import annotations

import array
import asyncio
import collections
import itertools
from collections import UserString
from collections.abc import Awaitable, Callable
from typing import Any

import pytest

from async_pipelines import BatchedAsyncPipeline, run_pipeline

_DOCS = [f"d{i}" for i in range(8)]

BatchCaller = Callable[[list[str]], Awaitable[list[str]]]


class _Caller:
    """A batch caller that records each batch it is sent and reshapes its reply."""

    def __init__(self, reshape: Callable[[list[str]], list[str]] = list) -> None:
        self.reshape = reshape
        self.sent: list[list[str]] = []

    async def __call__(self, batch: list[str]) -> list[str]:
        self.sent.append(list(batch))
        return self.reshape([f"{item}+" for item in batch])


def _short(batch: list[str]) -> list[str]:
    return batch[:-1]


def _long(batch: list[str]) -> list[str]:
    return [*batch, "extra"]


def _only_value_error(exc: BaseException) -> ValueError:
    assert isinstance(exc, ExceptionGroup)
    leaves = exc.exceptions
    assert len(leaves) == 1, leaves
    assert isinstance(leaves[0], ValueError), leaves
    return leaves[0]


def _pipeline(llm1: BatchCaller, llm2: BatchCaller, batch_size: int = 4) -> BatchedAsyncPipeline:
    # concurrency=1 makes "which batch failed first" deterministic.
    return BatchedAsyncPipeline(llm1, llm2, concurrency=1, batch_size=batch_size)


@pytest.mark.parametrize("reshape", [_short, _long], ids=["short", "long"])
def test_llm1_batch_of_the_wrong_length_raises_before_llm2_sees_it(reshape: Any) -> None:
    llm1, llm2 = _Caller(reshape), _Caller()
    with pytest.raises(ExceptionGroup) as exc:
        asyncio.run(_pipeline(llm1, llm2).run(_DOCS))
    err = _only_value_error(exc.value)
    got = 3 if reshape is _short else 5
    assert str(err) == (
        f"llm1_batch returned {got} outputs for batch 0 of 4 items; "
        "expected 4 (one per input, in input order)"
    )
    assert llm2.sent == []


@pytest.mark.parametrize("reshape", [_short, _long], ids=["short", "long"])
def test_llm2_batch_of_the_wrong_length_raises(reshape: Any) -> None:
    llm1, llm2 = _Caller(), _Caller(reshape)
    with pytest.raises(ExceptionGroup) as exc:
        asyncio.run(_pipeline(llm1, llm2).run(_DOCS))
    err = _only_value_error(exc.value)
    assert str(err).startswith("llm2_batch returned ")
    assert "for batch 0 of 4 items; expected 4" in str(err)


@pytest.mark.parametrize(
    ("llm1_reshape", "llm2_reshape"),
    [(_short, list), (list, _short), (_short, _short)],
    ids=["llm1-short", "llm2-short", "both-short"],
)
def test_run_pipeline_produces_no_result_for_a_short_batch(
    llm1_reshape: Any, llm2_reshape: Any
) -> None:
    # The published harm: `n_docs=8` over 4 outputs. No `RunResult` at all now.
    pipeline = _pipeline(_Caller(llm1_reshape), _Caller(llm2_reshape))
    result = None
    with pytest.raises(ExceptionGroup) as exc:
        result = asyncio.run(run_pipeline(pipeline, _DOCS))
    _only_value_error(exc.value)
    assert result is None


def test_the_error_names_the_batch_that_came_back_short() -> None:
    # Only the third batch (index 2) is short; batch_size=3 over 8 docs also
    # makes the last batch ragged (2 items), which must be checked against its
    # own size, not batch_size.
    calls = 0

    def short_on_third(batch: list[str]) -> list[str]:
        nonlocal calls
        calls += 1
        return batch[:-1] if calls == 3 else batch

    with pytest.raises(ExceptionGroup) as exc:
        asyncio.run(_pipeline(_Caller(short_on_third), _Caller(), batch_size=3).run(_DOCS))
    err = _only_value_error(exc.value)
    assert str(err).startswith("llm1_batch returned 1 outputs for batch 2 of 2 items; expected 2")


def test_equal_length_batches_are_unchanged() -> None:
    # Control: one output per input, in order, with a ragged last batch.
    llm1, llm2 = _Caller(), _Caller()
    out = asyncio.run(_pipeline(llm1, llm2, batch_size=3).run(_DOCS))
    assert out == [f"{doc}++" for doc in _DOCS]
    assert [len(b) for b in llm1.sent] == [3, 3, 2]
    result = asyncio.run(run_pipeline(_pipeline(_Caller(), _Caller(), batch_size=3), _DOCS))
    assert result.n_docs == 8


def test_empty_docs_send_no_batch() -> None:
    # Control: zero batches, so nothing to check and nothing to call.
    llm1, llm2 = _Caller(_short), _Caller(_short)
    assert asyncio.run(_pipeline(llm1, llm2).run([])) == []
    assert llm1.sent == llm2.sent == []


# --- #229: a bare string of the right length is one output, not N ---------
#
# #228 checked only `len(got)`. A `str` whose length matched the batch passed,
# and `flat.extend` split it into one "output" per character. Measured on
# `main` (a40b2d1, py3.11.14 and 3.12.12), 8 docs, `batch_size=4`: llm1 or
# llm2 returning "ABAB" -> ['A','B','A','B','A','B','A','B'], `b"ABAB"` ->
# [65, 66, ...], and `batch_size=1` returning "Y" -> ['Y', 'Y'].


def _const(value: Any) -> BatchCaller:
    async def caller(batch: list[str]) -> Any:
        return value

    return caller


# Each has length 4, the batch size, so the #226 length check alone passes it.
_BARE_OUTPUTS = [
    pytest.param("ABAB", "bare str", id="str"),
    pytest.param(b"ABAB", "bare bytes", id="bytes"),
    pytest.param(bytearray(b"ABAB"), "bare bytearray", id="bytearray"),
    pytest.param(UserString("ABAB"), "bare UserString", id="UserString"),
    pytest.param(memoryview(b"ABAB"), "bare memoryview", id="memoryview"),
    pytest.param(array.array("u", "ABAB"), "bare array", id="array-u"),
]


@pytest.mark.parametrize(("bare", "shape"), _BARE_OUTPUTS)
def test_llm1_batch_bare_string_of_matching_length_is_refused(bare: Any, shape: str) -> None:
    llm2 = _Caller()
    with pytest.raises(ExceptionGroup) as exc:
        asyncio.run(_pipeline(_const(bare), llm2).run(_DOCS))
    err = _only_value_error(exc.value)
    assert str(err).startswith(
        f"llm1_batch output for batch 0 must be a collection of items, not a {shape}"
    )
    assert llm2.sent == []


@pytest.mark.parametrize(("bare", "shape"), _BARE_OUTPUTS)
def test_llm2_batch_bare_string_of_matching_length_is_refused(bare: Any, shape: str) -> None:
    with pytest.raises(ExceptionGroup) as exc:
        asyncio.run(_pipeline(_Caller(), _const(bare)).run(_DOCS))
    err = _only_value_error(exc.value)
    assert str(err).startswith(
        f"llm2_batch output for batch 0 must be a collection of items, not a {shape}"
    )


def test_run_pipeline_produces_no_result_for_a_bare_string_batch() -> None:
    result = None
    with pytest.raises(ExceptionGroup) as exc:
        result = asyncio.run(run_pipeline(_pipeline(_const("ABAB"), _Caller()), _DOCS))
    _only_value_error(exc.value)
    assert result is None


def test_a_one_char_string_is_refused_at_batch_size_one() -> None:
    # len("Y") == 1 == batch_size: the shortest matching-length string.
    with pytest.raises(ExceptionGroup) as exc:
        asyncio.run(_pipeline(_Caller(), _const("Y"), batch_size=1).run(_DOCS[:2]))
    err = _only_value_error(exc.value)
    assert "not a bare str: 'Y'" in str(err)


def test_a_mismatched_string_is_not_reported_as_n_outputs() -> None:
    # Was "llm2_batch returned 3 outputs for batch 0 of 1 items": characters
    # counted as outputs. The bare-string refusal runs before the length check.
    with pytest.raises(ExceptionGroup) as exc:
        asyncio.run(_pipeline(_Caller(), _const("YES"), batch_size=1).run(_DOCS[:2]))
    err = _only_value_error(exc.value)
    assert "not a bare str: 'YES'" in str(err)
    assert "outputs" not in str(err)


@pytest.mark.parametrize("container", [list, tuple], ids=["list", "tuple"])
def test_a_collection_of_strings_is_still_accepted(container: Any) -> None:
    # Control: only the bare-string shape is refused; a tuple is one output per
    # input too, so `list` is not required (same rule as `docs`, #126).
    async def caller(batch: list[str]) -> Any:
        return container(f"{item}+" for item in batch)

    out = asyncio.run(_pipeline(caller, caller, batch_size=3).run(_DOCS))
    assert out == [f"{doc}++" for doc in _DOCS]


# --- #231: an iterator is read once; an unordered collection is refused -----
#
# #228's `len(got)` made every non-`Sized` output a raw `TypeError` that named
# neither the caller nor the batch, and a `set` passed it and came back in hash
# order. Measured on `main` (2081b24, py3.11.14 and 3.12.12), 6 docs,
# `batch_size=3`: a generator, `map`, `filter`, `iter(list)` or `reversed`
# from either caller -> `TypeError: object of type 'generator' has no len()`;
# a `set` -> no error, docs out of order; `dict.items()` -> `(i, doc)` tuples.


def _reply(shape: Callable[[list[str]], Any]) -> BatchCaller:
    async def caller(batch: list[str]) -> Any:
        return shape([f"{item}+" for item in batch])

    return caller


# Iterated in hash or key order, so position no longer names the input.
_UNORDERED_OUTPUTS = [
    pytest.param(set, "set", id="set"),
    pytest.param(frozenset, "frozenset", id="frozenset"),
    pytest.param(dict.fromkeys, "dict", id="dict"),
    pytest.param(lambda outs: dict.fromkeys(outs).keys(), "dict_keys", id="dict-keys"),
    pytest.param(lambda outs: dict(enumerate(outs)).items(), "dict_items", id="dict-items"),
]


@pytest.mark.parametrize(("shape", "name"), _UNORDERED_OUTPUTS)
def test_llm1_batch_unordered_output_is_refused(shape: Any, name: str) -> None:
    llm2 = _Caller()
    with pytest.raises(ExceptionGroup) as exc:
        asyncio.run(_pipeline(_reply(shape), llm2).run(_DOCS))
    err = _only_value_error(exc.value)
    assert str(err) == (
        f"llm1_batch output for batch 0 must be an ordered collection, not a {name}: "
        "outputs are matched to inputs by position -- return a list"
    )
    assert llm2.sent == []


@pytest.mark.parametrize(("shape", "name"), _UNORDERED_OUTPUTS)
def test_llm2_batch_unordered_output_is_refused(shape: Any, name: str) -> None:
    with pytest.raises(ExceptionGroup) as exc:
        asyncio.run(_pipeline(_Caller(), _reply(shape)).run(_DOCS))
    err = _only_value_error(exc.value)
    assert str(err).startswith(
        f"llm2_batch output for batch 0 must be an ordered collection, not a {name}"
    )


def test_a_one_item_set_is_refused_too() -> None:
    # The shape is refused, not the outcome: a one-item set cannot reorder,
    # but a caller that returns one would reorder at any larger batch.
    with pytest.raises(ExceptionGroup) as exc:
        asyncio.run(_pipeline(_reply(set), _Caller(), batch_size=1).run(_DOCS[:2]))
    assert "not a set" in str(_only_value_error(exc.value))


def test_run_pipeline_produces_no_result_for_a_set_batch() -> None:
    result = None
    with pytest.raises(ExceptionGroup) as exc:
        result = asyncio.run(run_pipeline(_pipeline(_Caller(), _reply(set)), _DOCS))
    _only_value_error(exc.value)
    assert result is None


_ITERATOR_OUTPUTS = [
    pytest.param(lambda outs: (o for o in outs), id="generator"),
    pytest.param(lambda outs: map(str, outs), id="map"),
    pytest.param(lambda outs: filter(None, outs), id="filter"),
    pytest.param(lambda outs: iter(outs), id="list-iterator"),
    pytest.param(lambda outs: reversed(outs[::-1]), id="reversed"),
]


@pytest.mark.parametrize("shape", _ITERATOR_OUTPUTS)
def test_llm1_batch_iterator_is_read_once_and_passed_on_as_a_list(shape: Any) -> None:
    llm2 = _Caller()
    seen: list[type] = []

    async def recording_llm2(batch: list[str]) -> list[str]:
        seen.append(type(batch))
        return await llm2(batch)

    out = asyncio.run(_pipeline(_reply(shape), recording_llm2, batch_size=3).run(_DOCS))
    assert out == [f"{doc}++" for doc in _DOCS]
    # Read once: a second read of a one-shot iterator would hand llm2 nothing.
    assert llm2.sent == [["d0+", "d1+", "d2+"], ["d3+", "d4+", "d5+"], ["d6+", "d7+"]]
    assert seen == [list, list, list]


@pytest.mark.parametrize("shape", _ITERATOR_OUTPUTS)
def test_llm2_batch_iterator_is_accepted(shape: Any) -> None:
    out = asyncio.run(_pipeline(_Caller(), _reply(shape), batch_size=3).run(_DOCS))
    assert out == [f"{doc}++" for doc in _DOCS]


def test_a_short_generator_is_counted_and_named() -> None:
    with pytest.raises(ExceptionGroup) as exc:
        asyncio.run(_pipeline(_reply(lambda outs: iter(outs[:-1])), _Caller()).run(_DOCS))
    err = _only_value_error(exc.value)
    assert str(err) == (
        "llm1_batch returned 3 outputs for batch 0 of 4 items; "
        "expected 4 (one per input, in input order)"
    )


@pytest.mark.parametrize(
    "shape",
    [
        pytest.param(lambda outs: iter([*outs, "extra"]), id="one-extra"),
        pytest.param(lambda outs: itertools.repeat("x"), id="endless"),
    ],
)
def test_a_long_or_endless_generator_raises_instead_of_hanging(shape: Any) -> None:
    # At most one item past the batch is read, so `repeat` cannot hang the run.
    with pytest.raises(ExceptionGroup) as exc:
        asyncio.run(_pipeline(_Caller(), _reply(shape)).run(_DOCS))
    err = _only_value_error(exc.value)
    assert str(err) == (
        "llm2_batch returned more than 4 outputs for batch 0 of 4 items; "
        "expected 4 (one per input, in input order)"
    )


@pytest.mark.parametrize("value", [None, 4], ids=["None", "int"])
def test_a_non_iterable_output_is_named(value: Any) -> None:
    # A caller that forgot to `return` hands back None: was a raw
    # `TypeError: object of type 'NoneType' has no len()`.
    llm2 = _Caller()
    with pytest.raises(ExceptionGroup) as exc:
        asyncio.run(_pipeline(_const(value), llm2).run(_DOCS))
    err = _only_value_error(exc.value)
    assert str(err) == (
        f"llm1_batch output for batch 0 must be a collection of items, not "
        f"{type(value).__name__}: return a list with one output per input"
    )
    assert llm2.sent == []


@pytest.mark.parametrize(
    "shape",
    [
        pytest.param(collections.deque, id="deque"),
        pytest.param(lambda outs: dict(enumerate(outs)).values(), id="dict-values"),
    ],
)
def test_an_ordered_collection_is_still_accepted(shape: Any) -> None:
    # Controls: ordered and sized, so neither refused nor re-read.
    out = asyncio.run(_pipeline(_reply(shape), _reply(shape), batch_size=3).run(_DOCS))
    assert out == [f"{doc}++" for doc in _DOCS]
