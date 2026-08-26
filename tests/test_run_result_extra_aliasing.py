"""`RunResult.extra` aliased on ingress and at depth on egress (#100).

`RunResult` is `@dataclass(frozen=True)` with one mutable field. `to_dict`'s
docstring made the claim this file is about:

    `extra` is shallow-copied so callers cannot accidentally mutate the frozen
    `RunResult`'s default through the returned dict.

True at depth 1. False at depth 2. Measured on `main`::

    extra = {"top": "v", "nested": {"k": "original"}, "items": ["a"]}
    d = r.to_dict()

    d["extra"]["top"] = "MUTATED"          -> r.extra["top"]         == "v"    (isolated)
    d["extra"]["nested"]["k"] = "MUTATED"  -> r.extra["nested"]["k"] == "MUTATED"
    d["extra"]["items"].append("MUTATED")  -> r.extra["items"]       == ["a", "MUTATED"]

And the ingress side was not copied at all, so a *new top-level key* could
appear on a frozen result after construction::

    src = {"nested": {"k": "original"}}
    r = RunResult(..., extra=src)
    src["nested"]["k"] = "MUTATED"; src["added"] = "AFTER"
    -> r.extra == {'nested': {'k': 'MUTATED'}, 'added': 'AFTER'}

`attach_speedup` passed `extra=r.extra` straight through, so its outputs shared
one dict with its inputs while its docstring said "Returns a new list, one entry
per input".

**The end of the chain is the published artifact.** A caller mutating a nested
value in the dict `to_dict()` handed back changed the number
`dump_benchmark_json` wrote to `docs/benchmarks.json` — which is the file
handoff §10's "do not invent benchmark numbers" is about.

Why the existing test passed: `test_run_result_to_dict_shallow_copies_extra`
mutates at depth 1, the one depth that was isolated. It was correct about what it
checked and silent about the two depths that leaked.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from async_pipelines.benchmark import (
    RunResult,
    Workload,
    attach_speedup,
    dump_benchmark_json,
)


def _result(extra: dict[str, Any], name: str = "async", duration: float = 1.0) -> RunResult:
    return RunResult(
        pipeline_name=name,
        n_docs=10,
        duration_seconds=duration,
        docs_per_second=10.0 / duration,
        extra=extra,
    )


def _nested() -> dict[str, Any]:
    return {"top": "v", "nested": {"k": "original"}, "items": ["a"]}


# ----------------------------------------------------------------------
# Egress: the dict to_dict() hands back
# ----------------------------------------------------------------------


@pytest.mark.parametrize(
    ("label", "mutate"),
    [
        ("top-level key", lambda e: e.__setitem__("top", "MUTATED")),
        ("nested dict value", lambda e: e["nested"].__setitem__("k", "MUTATED")),
        ("nested list", lambda e: e["items"].append("MUTATED")),
        ("new top-level key", lambda e: e.__setitem__("added", "MUTATED")),
        ("deleting a key", lambda e: e.pop("top")),
    ],
)
def test_mutating_the_returned_dict_cannot_reach_the_result(label: str, mutate: Any) -> None:
    r = _result(_nested())
    mutate(r.to_dict()["extra"])
    assert r.extra == _nested(), label


def test_two_calls_to_to_dict_do_not_share_state() -> None:
    r = _result(_nested())
    first = r.to_dict()["extra"]
    first["nested"]["k"] = "MUTATED"
    assert r.to_dict()["extra"] == _nested()


# ----------------------------------------------------------------------
# Ingress: the dict the caller passed in
# ----------------------------------------------------------------------


@pytest.mark.parametrize(
    ("label", "mutate"),
    [
        ("top-level key", lambda e: e.__setitem__("top", "MUTATED")),
        ("nested dict value", lambda e: e["nested"].__setitem__("k", "MUTATED")),
        ("nested list", lambda e: e["items"].append("MUTATED")),
        ("adding a key to a FROZEN result", lambda e: e.__setitem__("added", "AFTER")),
    ],
)
def test_mutating_the_source_dict_after_construction_cannot_reach_the_result(
    label: str, mutate: Any
) -> None:
    src = _nested()
    r = _result(src)
    mutate(src)
    assert r.extra == _nested(), label


def test_frozen_means_frozen_for_its_one_mutable_field() -> None:
    """The dataclass advertises immutability; `extra` was the exception."""
    src = _nested()
    r = _result(src)
    assert r.extra is not src
    assert r.extra["nested"] is not src["nested"]
    assert r.extra["items"] is not src["items"]


# ----------------------------------------------------------------------
# attach_speedup: "Returns a new list, one entry per input"
# ----------------------------------------------------------------------


def test_attach_speedup_outputs_do_not_share_extra_with_their_inputs() -> None:
    serial = _result({"p": {"b": 1}}, name="serial", duration=2.0)
    fast = _result({"p": {"b": 8}}, name="async", duration=1.0)
    out = attach_speedup([serial, fast])

    assert out[1] is not fast
    assert out[1].extra is not fast.extra
    out[1].extra["p"]["b"] = 999
    assert fast.extra["p"]["b"] == 8


def test_attach_speedup_still_computes_the_speedups_it_is_for() -> None:
    """Control: the copy must not change what the function is actually doing."""
    serial = _result({}, name="serial", duration=2.0)
    fast = _result({}, name="async", duration=1.0)
    out = attach_speedup([serial, fast])
    assert [r.pipeline_name for r in out] == ["serial", "async"]
    assert out[0].speedup_vs_serial == pytest.approx(1.0)
    assert out[1].speedup_vs_serial == pytest.approx(2.0)


def test_attach_speedup_preserves_extra_by_value() -> None:
    fast = _result({"p": {"b": 8}}, name="async", duration=1.0)
    out = attach_speedup([_result({}, name="serial", duration=2.0), fast])
    assert out[1].extra == {"p": {"b": 8}}


# ----------------------------------------------------------------------
# The end of the chain: the published artifact
# ----------------------------------------------------------------------


def test_a_post_to_dict_mutation_cannot_change_what_is_written(tmp_path: Path) -> None:
    """The road that makes this worth fixing.

    Before #100, mutating a nested value in the dict `to_dict()` handed back
    changed the number `dump_benchmark_json` wrote — measured, `batch_size`
    went from 8 to 1 in the output file.
    """
    fast = _result({"params": {"batch_size": 8}}, name="async", duration=1.0)
    results = attach_speedup([_result({}, name="serial", duration=2.0), fast])

    handed_back = results[1].to_dict()
    handed_back["extra"]["params"]["batch_size"] = 1

    out = tmp_path / "benchmarks.json"
    dump_benchmark_json(
        out,
        workload=Workload(n_docs=10, llm_call_seconds=0.1, concurrency=4, batch_size=8),
        results=results,
    )
    written = json.loads(out.read_text(encoding="utf-8"))
    assert written["results"][1]["extra"]["params"]["batch_size"] == 8


def test_the_written_artifact_still_round_trips(tmp_path: Path) -> None:
    """Control: deep-copying must not change the JSON contract."""
    results = [_result({"params": {"batch_size": 8}, "note": "x"}, name="async")]
    out = tmp_path / "benchmarks.json"
    dump_benchmark_json(
        out,
        workload=Workload(n_docs=10, llm_call_seconds=0.1, concurrency=4, batch_size=8),
        results=results,
    )
    written = json.loads(out.read_text(encoding="utf-8"))
    assert written["results"][0]["extra"] == {"params": {"batch_size": 8}, "note": "x"}
    assert set(written) == {"workload", "results"}


# ----------------------------------------------------------------------
# Scope + anti-vacuous
# ----------------------------------------------------------------------


def test_workload_to_dict_has_nothing_to_alias() -> None:
    """Checked, not assumed — so the next enumeration of these seams doesn't
    re-derive it. All four fields are scalars."""
    w = Workload(n_docs=10, llm_call_seconds=0.1, concurrency=4, batch_size=8)
    d = w.to_dict()
    assert all(isinstance(v, (int, float, str)) and not isinstance(v, dict) for v in d.values())
    d["n_docs"] = 999
    assert w.n_docs == 10


def test_an_empty_extra_is_still_an_empty_dict() -> None:
    r = RunResult(pipeline_name="p", n_docs=1, duration_seconds=1.0, docs_per_second=1.0)
    assert r.extra == {}
    assert r.to_dict()["extra"] == {}


def test_the_fixture_is_actually_nested() -> None:
    """Anti-vacuous: a flat `_nested()` would make every depth-2 case above pass
    while testing only the depth that already worked."""
    fixture = _nested()
    assert any(isinstance(v, dict) for v in fixture.values())
    assert any(isinstance(v, list) for v in fixture.values())
    assert any(not isinstance(v, (dict, list)) for v in fixture.values())
