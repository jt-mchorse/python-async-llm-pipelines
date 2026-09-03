"""Every frozen dataclass with a mutable field is classified (#100, #102, #106).

`frozen=True` stops attribute rebinding; it does nothing about a `dict` or a
`list` the caller still holds a reference to. This package has hit that twice —
`RunResult.extra` (#100) and `ToolCall.arguments` (#102) — and both were fixed
with a `copy.deepcopy` in `__post_init__`.

`ToolResult.value` is the third, and it does not copy. It stayed invisible
because `ToolCall`'s docstring said `RunResult.extra` was "this package's only
other frozen dataclass with a mutable field" — a scoped, true-sounding count
that reads as a completed enumeration, three lines above the class it missed.

So this file does not hand-list the classes. It **discovers** them, and
requires each to be on one of two explicit lists: it deep-copies, or it is a
documented alias with an issue that owns the question. A fourth class cannot
arrive unclassified.

The behaviour of `ToolResult.value` is *characterized*, not asserted as
correct. #106 is an open decision — copy, document the alias, or best-effort
copy — with a real cost either way in a performance-focused package. Pinning
what it does today means whichever answer lands, it lands as a visible diff to
an assertion rather than a silent shift.
"""

from __future__ import annotations

import ast
import asyncio
import copy
import dataclasses
import pathlib
from typing import Any

import pytest

from async_pipelines.benchmark import RunResult
from async_pipelines.tool_dispatch import ToolCall, ToolResult, dispatch_tool_calls

_PKG = pathlib.Path(__file__).resolve().parent.parent / "async_pipelines"

#: Classes that deep-copy their mutable field in `__post_init__`.
COPIES = {"RunResult": "#100", "ToolCall": "#102"}

#: Classes that deliberately alias, each naming the issue that owns the
#: question. An entry here is a statement that the aliasing is *known*, not
#: that it is right.
DOCUMENTED_ALIASES = {"ToolResult": "#106"}

#: Annotations that can hold a mutable object. `Any` is included because that is
#: exactly what `ToolResult.value` is, and excluding it is how a scan can be
#: technically correct and still miss the case it exists for.
_MUTABLE_HINTS = ("dict", "list", "set", "Any", "Mapping", "MutableMapping", "Sequence")


def _frozen_dataclasses_with_mutable_fields() -> dict[str, list[str]]:
    """`{class_name: [field names]}`, read from the source rather than imported.

    AST rather than `dataclasses.fields` at runtime so a class that is never
    imported by this test still counts — the population is "declared in the
    package", not "reachable from here".
    """
    found: dict[str, list[str]] = {}
    for path in sorted(_PKG.glob("*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if not isinstance(node, ast.ClassDef):
                continue
            decorators = [ast.unparse(d) for d in node.decorator_list]
            if not any("dataclass" in d for d in decorators):
                continue
            if not any("frozen=True" in d for d in decorators):
                continue
            fields = [
                ast.unparse(stmt.target)
                for stmt in node.body
                if isinstance(stmt, ast.AnnAssign)
                and stmt.annotation is not None
                and any(h in ast.unparse(stmt.annotation) for h in _MUTABLE_HINTS)
            ]
            if fields:
                found[node.name] = fields
    return found


# --- the lock itself --------------------------------------------------------


def test_the_discovery_is_not_vacuous() -> None:
    """A scan that finds nothing satisfies the classification test below while
    checking nothing, which is strictly worse than the hand-list it replaces."""
    found = _frozen_dataclasses_with_mutable_fields()
    assert len(found) >= 3, found
    assert {"RunResult", "ToolCall", "ToolResult"} <= set(found), found


def test_every_frozen_dataclass_with_a_mutable_field_is_classified() -> None:
    """The guard that would have caught `ToolResult` when it was written."""
    found = _frozen_dataclasses_with_mutable_fields()
    classified = set(COPIES) | set(DOCUMENTED_ALIASES)
    unclassified = sorted(set(found) - classified)
    assert not unclassified, (
        "these frozen dataclasses hold a mutable field and are neither on COPIES "
        "nor on DOCUMENTED_ALIASES:\n"
        + "\n".join(f"  - {name}: {found[name]}" for name in unclassified)
        + "\n(either deep-copy it in __post_init__ like #100/#102, or add it to "
        "DOCUMENTED_ALIASES naming the issue that owns the question)"
    )


def test_the_two_lists_are_disjoint_and_live() -> None:
    """A class on both lists would make the classification meaningless, and a
    name on either list that no longer exists is a stale exemption."""
    assert not (set(COPIES) & set(DOCUMENTED_ALIASES))
    found = set(_frozen_dataclasses_with_mutable_fields())
    stale = sorted((set(COPIES) | set(DOCUMENTED_ALIASES)) - found)
    assert not stale, f"listed but no longer discovered (drop them): {stale}"


@pytest.mark.parametrize("name", sorted(COPIES), ids=sorted(COPIES))
def test_the_copying_classes_really_copy(name: str) -> None:
    """Positively stated: membership of `COPIES` is a claim about behaviour, and
    a class that stopped copying would otherwise stay on the list silently."""
    src: dict[str, Any] = {"nested": {"k": "original"}}
    if name == "RunResult":
        obj = RunResult(
            pipeline_name="r",
            n_docs=1,
            duration_seconds=1.0,
            docs_per_second=1.0,
            speedup_vs_serial=None,
            extra=src,
        )
        held = obj.extra
    else:
        obj = ToolCall(id="t", name="n", arguments=src)
        held = obj.arguments
    src["nested"]["k"] = "MUTATED"
    src["added"] = "AFTER"
    assert held == {"nested": {"k": "original"}}, f"{name} stopped isolating its field"


# --- characterization of the undecided one (#106) ---------------------------


def test_toolresult_value_aliases_the_caller_object() -> None:
    """Characterization, not approval. See #106."""
    src = {"a": [1]}
    result = ToolResult(tool_call_id="t3", name="search", ok=True, value=src)
    assert result.value is src
    src["a"].append(2)
    src["b"] = "new"
    assert result.value == {"a": [1, 2], "b": "new"}


def test_toolcall_is_the_control_for_that() -> None:
    """The same input through the class that *was* fixed, so the assertion above
    is read as an asymmetry between two siblings rather than as normal."""
    src = {"a": [1]}
    call = ToolCall(id="t", name="n", arguments=src)
    assert call.arguments is not src
    src["a"].append(2)
    assert call.arguments == {"a": [1]}


def test_a_frozen_toolresult_changes_after_construction() -> None:
    """The exact shape #102 measured for `ToolCall.arguments`, one class over:
    a **new top-level key** on a frozen object after it was built."""
    state: dict[str, Any] = {"hits": {"n": 1}}

    async def search(_args: dict[str, Any]) -> Any:
        return state

    async def run() -> ToolResult:
        calls = [ToolCall(id="t1", name="search", arguments={"q": "x"})]
        return (await dispatch_tool_calls(calls, registry={"search": search}))[0]

    result = asyncio.run(run())
    assert result.value == {"hits": {"n": 1}}
    state["hits"]["n"] = 999
    state["added"] = "AFTER"
    assert result.value == {"hits": {"n": 999}, "added": "AFTER"}


def test_results_from_one_tool_share_one_object() -> None:
    """The concurrency-shaped consequence, and the one #102's own summary names
    for its sibling: mutating any single record's `value` mutates the others."""
    state: dict[str, Any] = {"n": 0}

    async def search(_args: dict[str, Any]) -> Any:
        return state

    async def run() -> list[ToolResult]:
        calls = [ToolCall(id=f"t{i}", name="search", arguments={}) for i in range(3)]
        return await dispatch_tool_calls(calls, registry={"search": search})

    results = asyncio.run(run())
    assert results[0].value is results[1].value
    assert results[1].value is results[2].value
    results[0].value["n"] = 42
    assert results[2].value["n"] == 42


def test_a_non_deepcopyable_return_survives_today() -> None:
    """Why #106 is a decision and not an obvious fix.

    `value` is `Any`, so a tool may return something `deepcopy` refuses. Today
    that reaches the record intact; a blanket copy would turn a *successful*
    call into a crash at the record boundary. Pinned so that consequence is
    visible to whoever decides, and so a copy landing without a fallback fails
    here rather than in someone's pipeline.
    """

    class NotCopyable:
        def __deepcopy__(self, memo: dict[int, Any]) -> Any:
            raise TypeError("cannot deepcopy a live connection")

    sentinel = NotCopyable()
    with pytest.raises(TypeError):
        copy.deepcopy(sentinel)

    async def connect(_args: dict[str, Any]) -> Any:
        return sentinel

    async def run() -> ToolResult:
        calls = [ToolCall(id="t1", name="connect", arguments={})]
        return (await dispatch_tool_calls(calls, registry={"connect": connect}))[0]

    result = asyncio.run(run())
    assert result.ok is True
    assert result.value is sentinel


def test_the_field_sets_match_what_dataclasses_reports() -> None:
    """Keeps the AST scan honest against the runtime view for the three classes
    that are importable here — a source scan that drifted from reality would
    classify the wrong things."""
    found = _frozen_dataclasses_with_mutable_fields()
    for cls in (RunResult, ToolCall, ToolResult):
        runtime_fields = {f.name for f in dataclasses.fields(cls)}
        assert set(found[cls.__name__]) <= runtime_fields, cls.__name__
