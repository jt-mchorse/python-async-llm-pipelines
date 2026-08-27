"""`ToolCall.arguments` aliased the caller's dict, and `concurrency` decided the results (#102).

`#100` fixed `RunResult.extra`: a `frozen=True` dataclass whose one mutable field
aliased the caller's dict, so a "frozen" object could change after construction.
This package has exactly two frozen dataclasses with a mutable field, and `#100`
fixed one of them::

    benchmark.py     RunResult   frozen=True  __post_init__=yes   extra
    tool_dispatch.py ToolCall    frozen=True  __post_init__=NO    arguments

Ingress, measured before this change -- the same script that returns
`{'k': 'original'}` for `RunResult`::

    src = {"nested": {"k": "original"}}
    call = ToolCall(id="t1", name="search", arguments=src)
    src["nested"]["k"] = "MUTATED"; src["added"] = "AFTER"
    -> call.arguments == {'nested': {'k': 'MUTATED'}, 'added': 'AFTER'}

Egress was worse, because `_run_with_telemetry` handed the live dict to
caller-supplied code. Six `ToolCall`s sharing one dict, a tool doing
`args["n"] += 1`::

    concurrency=1 -> results=[1, 2, 3, 4, 5, 6]
    concurrency=2 -> results=[2, 2, 4, 4, 6, 6]
    concurrency=6 -> results=[6, 6, 6, 6, 6, 6]

`concurrency` is documented as "optional upper bound on simultaneous in-flight
calls" -- a throughput knob. It was deciding the answers.

The concurrency assertions below compare results **across concurrency levels**
rather than against a hardcoded list. The property is "the knob stops
mattering", and a hardcoded expectation would pass just as well if the knob
started mattering differently.
"""

from __future__ import annotations

import asyncio
from dataclasses import fields, is_dataclass
from typing import Any

import pytest

from async_pipelines.tool_dispatch import ToolCall, ToolResult, dispatch_tool_calls


def _values(results: list[ToolResult]) -> list[Any]:
    return [r.value for r in results]


# --- ingress ----------------------------------------------------------------


def test_mutating_the_source_dict_does_not_change_a_constructed_call() -> None:
    src = {"nested": {"k": "original"}}
    call = ToolCall(id="t1", name="search", arguments=src)
    src["nested"]["k"] = "MUTATED"
    src["added"] = "AFTER"
    assert call.arguments == {"nested": {"k": "original"}}


def test_the_copy_is_deep_not_shallow() -> None:
    """`#100` measured that shallow "is exactly the half that failed": a shallow
    copy protects depth 1 and nothing below it."""
    src = {"top": "a", "nested": {"k": "original"}, "items": [1, 2]}
    call = ToolCall(id="t1", name="search", arguments=src)
    src["nested"]["k"] = "MUTATED"
    src["items"].append(3)
    assert call.arguments["nested"]["k"] == "original"
    assert call.arguments["items"] == [1, 2]


def test_two_calls_from_one_dict_do_not_share_it() -> None:
    shared = {"q": "hello"}
    a = ToolCall(id="a", name="t", arguments=shared)
    b = ToolCall(id="b", name="t", arguments=shared)
    assert a.arguments is not b.arguments
    a.arguments["q"] = "changed"
    assert b.arguments["q"] == "hello"


def test_the_default_factory_case_still_works() -> None:
    call = ToolCall(id="t1", name="search")
    assert call.arguments == {}


def test_equality_is_unaffected() -> None:
    """`__post_init__` normalizes a field; it must not change what equality
    means, or every existing assertion comparing `ToolCall`s changes meaning."""
    a = ToolCall(id="t1", name="search", arguments={"q": "x", "n": {"deep": 1}})
    b = ToolCall(id="t1", name="search", arguments={"q": "x", "n": {"deep": 1}})
    assert a == b


# --- egress -----------------------------------------------------------------


def test_a_tool_that_mutates_its_arguments_does_not_change_the_record() -> None:
    """`ToolCall` is the record of a request and must still describe it
    afterwards. `ToolResult` carries `elapsed_ms` / `error_repr` so a run is
    reconstructable; a `ToolCall` recording post-mutation state defeats that."""

    async def norm(args: dict[str, Any]) -> str:
        args.setdefault("limit", 10)  # an ordinary thing for a tool to do
        args["q"] = args["q"].upper()
        return str(args["q"])

    call = ToolCall(id="t1", name="norm", arguments={"q": "hello"})
    results = asyncio.run(dispatch_tool_calls([call], registry={"norm": norm}))
    assert _values(results) == ["HELLO"]
    assert call.arguments == {"q": "hello"}


def test_a_mutating_tool_does_not_leak_into_a_sibling_call() -> None:
    async def bump(args: dict[str, Any]) -> int:
        args["n"] = args["n"] + 1
        return int(args["n"])

    shared = {"n": 0}
    calls = [ToolCall(id=f"t{i}", name="bump", arguments=shared) for i in range(4)]
    results = asyncio.run(dispatch_tool_calls(calls, registry={"bump": bump}))
    assert _values(results) == [1, 1, 1, 1]


CONCURRENCIES = [1, 2, 6, None]


def _run_bump_table(concurrency: int | None, n: int = 6) -> list[Any]:
    shared = {"n": 0}
    calls = [ToolCall(id=f"t{i}", name="bump", arguments=shared) for i in range(n)]

    async def bump(args: dict[str, Any]) -> int:
        args["n"] = args["n"] + 1
        await asyncio.sleep(0.001)  # yields, so the interleaving would matter
        return int(args["n"])

    return _values(
        asyncio.run(dispatch_tool_calls(calls, registry={"bump": bump}, concurrency=concurrency))
    )


def test_concurrency_does_not_change_the_results() -> None:
    """The assertion the fix exists for. Compared across levels rather than to a
    hardcoded list: the property is that the knob stops mattering."""
    observed = {c: _run_bump_table(c) for c in CONCURRENCIES}
    distinct = {tuple(v) for v in observed.values()}
    assert len(distinct) == 1, observed


@pytest.mark.parametrize("concurrency", CONCURRENCIES, ids=lambda c: f"concurrency={c}")
def test_each_call_sees_the_arguments_it_was_built_with(concurrency: int | None) -> None:
    """Not just "the same at every level" -- the *right* answer. Six calls each
    incrementing from 0 must all see 1, whatever the interleaving."""
    assert _run_bump_table(concurrency) == [1] * 6


def test_the_timeout_path_copies_too() -> None:
    """`_run_with_telemetry` has two `await fn(...)` sites -- one bare and one
    inside `asyncio.timeout`. A fix applied to only the first is the shape this
    whole issue is about."""

    async def norm(args: dict[str, Any]) -> str:
        args["q"] = args["q"].upper()
        return str(args["q"])

    call = ToolCall(id="t1", name="norm", arguments={"q": "hello"})
    results = asyncio.run(dispatch_tool_calls([call], registry={"norm": norm}, timeout=5.0))
    assert _values(results) == ["HELLO"]
    assert call.arguments == {"q": "hello"}


def test_the_failing_tool_path_does_not_leave_a_mutated_record() -> None:
    async def boom(args: dict[str, Any]) -> str:
        args["touched"] = True
        raise ValueError("nope")

    call = ToolCall(id="t1", name="boom", arguments={"q": "hello"})
    results = asyncio.run(
        dispatch_tool_calls([call], registry={"boom": boom}, return_exceptions=True)
    )
    assert results[0].ok is False
    assert call.arguments == {"q": "hello"}


# --- structural: the enumeration that #100 did not do -----------------------

MUTABLE_HINTS = ("dict", "list", "set", "Sequence", "Mapping")


def _frozen_dataclasses_with_a_mutable_field() -> list[type]:
    import importlib
    import pkgutil

    import async_pipelines

    found: list[type] = []
    seen: set[int] = set()
    modules = [async_pipelines] + [
        importlib.import_module(m.name)
        for m in pkgutil.walk_packages(async_pipelines.__path__, "async_pipelines.")
    ]
    for mod in modules:
        for obj in vars(mod).values():
            if not isinstance(obj, type) or not is_dataclass(obj) or id(obj) in seen:
                continue
            seen.add(id(obj))
            if not obj.__dataclass_params__.frozen:  # type: ignore[attr-defined]
                continue
            if any(any(h in str(f.type) for h in MUTABLE_HINTS) for f in fields(obj)):
                found.append(obj)
    return found


def test_every_frozen_dataclass_with_a_mutable_field_copies_it_on_ingress() -> None:
    """The enumeration `#100` skipped.

    `#100` called `extra` "its one mutable field" -- true of `RunResult`, and it
    is not the only such dataclass in the package. This is what stops a third one
    shipping without the copy.
    """
    classes = _frozen_dataclasses_with_a_mutable_field()
    assert len(classes) >= 2, f"expected at least RunResult and ToolCall, found {classes}"

    for cls in classes:
        mutable = [f for f in fields(cls) if any(h in str(f.type) for h in MUTABLE_HINTS)]
        for f in mutable:
            src: Any = {"nested": {"k": "original"}} if "dict" in str(f.type) else [["original"]]
            kwargs: dict[str, Any] = {f.name: src}
            for other in fields(cls):
                if other.name == f.name:
                    continue
                kwargs.setdefault(other.name, _placeholder(other))
            try:
                obj = cls(**kwargs)
            except (TypeError, ValueError) as e:  # pragma: no cover - shape guard
                pytest.fail(f"could not construct {cls.__name__} for the copy check: {e}")
            if isinstance(src, dict):
                src["nested"]["k"] = "MUTATED"
                assert getattr(obj, f.name)["nested"]["k"] == "original", (
                    f"{cls.__name__}.{f.name} aliases the caller's object (#102)"
                )
            else:
                src[0][0] = "MUTATED"
                assert getattr(obj, f.name)[0][0] == "original", (
                    f"{cls.__name__}.{f.name} aliases the caller's object (#102)"
                )


def _placeholder(f: Any) -> Any:
    t = str(f.type)
    if "str" in t:
        return "x"
    if "int" in t and "float" not in t:
        return 1
    if "float" in t:
        return 1.0
    if "bool" in t:
        return True
    return None
