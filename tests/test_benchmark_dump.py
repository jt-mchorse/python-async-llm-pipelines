"""Tests for ``Workload.to_dict``, ``RunResult.to_dict``, and
``dump_benchmark_json`` (#44).

Coverage matrix:

- ``Workload.to_dict`` returns all four fields under stable keys; round-
  trips through JSON.
- ``RunResult.to_dict`` returns all six fields under stable keys;
  preserves ``speedup_vs_serial=None`` for the serial baseline; deep-
  copies ``extra`` so callers can't mutate the frozen dataclass via the
  returned dict.
- ``dump_benchmark_json`` writes the byte-identical pre-#44 shape:
  top-level ``{"workload": ..., "results": [...]}`` with ``sort_keys=
  True`` and ``indent=2`` so the existing downstream parsers don't
  break.
- ``dump_benchmark_json`` uses ``atomic_write_text`` so a crashed write
  leaves no partial file at ``path`` (monkey-patch ``os.replace`` to
  raise; the test asserts ``path`` is absent after).
- The ``Workload`` / ``RunResult`` / ``dump_benchmark_json`` names are
  re-exported on ``async_pipelines`` so the public surface lock keeps
  them.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

import async_pipelines
from async_pipelines.benchmark import (
    RunResult,
    Workload,
    dump_benchmark_json,
)

# ---------------------------------------------------------------------------
# Workload.to_dict
# ---------------------------------------------------------------------------


def test_workload_to_dict_returns_all_four_fields() -> None:
    w = Workload(n_docs=1000, llm_call_seconds=0.020, concurrency=32, batch_size=8)
    d = w.to_dict()
    assert set(d.keys()) == {"n_docs", "llm_call_seconds", "concurrency", "batch_size"}
    assert d == {
        "n_docs": 1000,
        "llm_call_seconds": 0.020,
        "concurrency": 32,
        "batch_size": 8,
    }


def test_workload_to_dict_round_trips_through_json() -> None:
    w = Workload(n_docs=500, llm_call_seconds=0.05, concurrency=16, batch_size=4)
    assert json.loads(json.dumps(w.to_dict())) == w.to_dict()


# ---------------------------------------------------------------------------
# RunResult.to_dict
# ---------------------------------------------------------------------------


def test_run_result_to_dict_returns_all_six_fields() -> None:
    r = RunResult(
        pipeline_name="async",
        n_docs=1000,
        duration_seconds=2.5,
        docs_per_second=400.0,
        speedup_vs_serial=16.0,
        extra={"batch_size": 8},
    )
    d = r.to_dict()
    assert set(d.keys()) == {
        "pipeline_name",
        "n_docs",
        "duration_seconds",
        "docs_per_second",
        "speedup_vs_serial",
        "extra",
    }
    assert d["pipeline_name"] == "async"
    assert d["n_docs"] == 1000
    assert d["duration_seconds"] == 2.5
    assert d["docs_per_second"] == 400.0
    assert d["speedup_vs_serial"] == 16.0
    assert d["extra"] == {"batch_size": 8}


def test_run_result_to_dict_preserves_none_speedup_for_serial_baseline() -> None:
    """The serial row has ``speedup_vs_serial=None``; JSON consumers
    route on ``None`` to skip the column for that row.
    """
    r = RunResult(pipeline_name="serial", n_docs=1000, duration_seconds=40.0, docs_per_second=25.0)
    d = r.to_dict()
    assert d["speedup_vs_serial"] is None
    # ``None`` round-trips through JSON as ``null`` → ``None``.
    assert json.loads(json.dumps(d))["speedup_vs_serial"] is None


def test_run_result_to_dict_shallow_copies_extra() -> None:
    """Mutating the returned dict's ``extra`` must not touch the frozen
    `RunResult.extra` default.
    """
    original_extra: dict[str, Any] = {"k": 1}
    r = RunResult(
        pipeline_name="async",
        n_docs=10,
        duration_seconds=1.0,
        docs_per_second=10.0,
        extra=original_extra,
    )
    d = r.to_dict()
    d["extra"]["k"] = 999
    assert r.extra == {"k": 1}
    # Caller's original dict is also untouched.
    assert original_extra == {"k": 1}


def test_run_result_to_dict_round_trips_through_json() -> None:
    r = RunResult(
        pipeline_name="batched",
        n_docs=42,
        duration_seconds=0.5,
        docs_per_second=84.0,
        speedup_vs_serial=4.2,
        extra={"strategy": "batched"},
    )
    assert json.loads(json.dumps(r.to_dict())) == r.to_dict()


# ---------------------------------------------------------------------------
# dump_benchmark_json
# ---------------------------------------------------------------------------


def test_dump_benchmark_json_writes_combined_shape(tmp_path: Path) -> None:
    w = Workload(n_docs=100, llm_call_seconds=0.01, concurrency=8, batch_size=4)
    rs = [
        RunResult(pipeline_name="serial", n_docs=100, duration_seconds=2.0, docs_per_second=50.0),
        RunResult(
            pipeline_name="async",
            n_docs=100,
            duration_seconds=0.2,
            docs_per_second=500.0,
            speedup_vs_serial=10.0,
        ),
    ]
    out = tmp_path / "bench.json"
    dump_benchmark_json(out, workload=w, results=rs)
    loaded = json.loads(out.read_text(encoding="utf-8"))
    assert set(loaded.keys()) == {"workload", "results"}
    assert loaded["workload"] == w.to_dict()
    assert loaded["results"] == [r.to_dict() for r in rs]


def test_dump_benchmark_json_is_byte_identical_to_pre_44_inline_shape(tmp_path: Path) -> None:
    """The pre-#44 ``bench_1000_doc.py`` JSON output was

        json.dumps({"workload": asdict(workload), "results": [asdict(r) for r in results]},
                   indent=2, sort_keys=True)

    The new helper has to produce the same string (modulo a trailing
    newline policy) so existing consumers don't break.
    """
    from dataclasses import asdict

    w = Workload(n_docs=12, llm_call_seconds=0.01, concurrency=4, batch_size=2)
    rs = [
        RunResult(pipeline_name="serial", n_docs=12, duration_seconds=1.0, docs_per_second=12.0),
        RunResult(
            pipeline_name="async",
            n_docs=12,
            duration_seconds=0.1,
            docs_per_second=120.0,
            speedup_vs_serial=10.0,
        ),
    ]
    out_new = tmp_path / "new.json"
    dump_benchmark_json(out_new, workload=w, results=rs)
    expected_old = json.dumps(
        {"workload": asdict(w), "results": [asdict(r) for r in rs]},
        indent=2,
        sort_keys=True,
    )
    assert out_new.read_text(encoding="utf-8") == expected_old


def test_dump_benchmark_json_uses_atomic_write(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Force ``os.replace`` to raise mid-write; the destination file
    must not exist afterward (the tempfile from `atomic_write_text` is
    the holder, ``os.replace`` is the publishing step).
    """
    import os as os_mod

    w = Workload(n_docs=5, llm_call_seconds=0.001, concurrency=2, batch_size=1)
    rs = [
        RunResult(pipeline_name="serial", n_docs=5, duration_seconds=0.1, docs_per_second=50.0),
    ]
    out = tmp_path / "crashed.json"

    def crashed_replace(*_args: Any, **_kwargs: Any) -> None:
        raise OSError("simulated crash before publish")

    monkeypatch.setattr(os_mod, "replace", crashed_replace)
    with pytest.raises(OSError, match="simulated crash"):
        dump_benchmark_json(out, workload=w, results=rs)
    assert not out.exists(), "atomic_write_text should not leave a partial file at the target path"


def test_dump_benchmark_json_accepts_str_path(tmp_path: Path) -> None:
    """``str | Path`` typing; ``str`` input works."""
    w = Workload(n_docs=2, llm_call_seconds=0.001, concurrency=1, batch_size=1)
    rs = [RunResult(pipeline_name="serial", n_docs=2, duration_seconds=0.1, docs_per_second=20.0)]
    out_str = str(tmp_path / "from_str.json")
    dump_benchmark_json(out_str, workload=w, results=rs)
    assert Path(out_str).exists()


# ---------------------------------------------------------------------------
# Public-surface re-export
# ---------------------------------------------------------------------------


def test_dump_benchmark_json_is_re_exported_at_package_level() -> None:
    """Public surface lock: callers should be able to do
    ``from async_pipelines import dump_benchmark_json`` without
    needing to know the submodule path.
    """
    assert hasattr(async_pipelines, "dump_benchmark_json")
    assert async_pipelines.dump_benchmark_json is dump_benchmark_json
    assert "dump_benchmark_json" in async_pipelines.__all__
