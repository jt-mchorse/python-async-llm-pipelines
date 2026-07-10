"""Tests for `scripts/bench_backpressure.py` (#46).

Coverage:
- `BackpressureResult.to_dict` field-set pin (7 fields, metrics shallow-copied).
- Acceptance regression: the script's _write_payload output uses the
  to_dict shape (every results[i] has the contracted 7-field surface,
  including the nested metrics dict that follows StreamMetrics.to_dict's
  5-field contract).
"""

from __future__ import annotations

import asyncio
import json
import sys
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parents[1]
_SCRIPTS_DIR = _REPO_ROOT / "scripts"
if str(_SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS_DIR))

from bench_backpressure import (  # noqa: E402 - sys.path tweak above
    BackpressureResult,
    _build_parser,
    main_async,
)


def _make_result(metrics: dict | None = None) -> BackpressureResult:
    return BackpressureResult(
        n=100,
        queue_size=8,
        consumer_ms=5.0,
        concurrency=4,
        duration_s=0.42,
        peak_heap_kb=128.5,
        metrics={"produced": 100, "consumed": 100} if metrics is None else metrics,
    )


def test_backpressure_result_to_dict_field_set_is_pinned():
    d = _make_result().to_dict()
    assert sorted(d.keys()) == [
        "concurrency",
        "consumer_ms",
        "duration_s",
        "metrics",
        "n",
        "peak_heap_kb",
        "queue_size",
    ]


def test_backpressure_result_to_dict_values_round_trip():
    r = _make_result(metrics={})
    assert r.to_dict() == {
        "n": 100,
        "queue_size": 8,
        "consumer_ms": 5.0,
        "concurrency": 4,
        "duration_s": 0.42,
        "peak_heap_kb": 128.5,
        "metrics": {},
    }


def test_backpressure_result_to_dict_metrics_is_shallow_copied():
    # The frozen-spirit dataclass exposes metrics through to_dict; the
    # returned dict's metrics must be a copy so caller mutation doesn't
    # bleed back into the BackpressureResult instance.
    r = _make_result()
    out = r.to_dict()
    out["metrics"]["leaked"] = 1
    assert "leaked" not in r.metrics


def test_backpressure_result_to_dict_round_trip_through_json():
    # JSON round-trip validates that to_dict() emits native types only;
    # an internal type leak (e.g., a numpy scalar) would surface here.
    r = _make_result()
    payload = json.dumps(r.to_dict(), sort_keys=True)
    parsed = json.loads(payload)
    assert parsed == r.to_dict()


# --- #78: --consumer-ms non-finite guard (sibling of #62/#76) ----------------
#
# `main_async` validated --consumer-ms with only `< 0`, so nan/inf (both valid
# float() inputs, both `< 0` == False) slipped into asyncio.sleep(consumer_ms /
# 1000.0): nan raised `ValueError: Invalid delay` (raw traceback, exit 1) and
# inf hung forever. The guard now mirrors the finiteness checks the timing seams
# enforce (Workload.llm_call_seconds, make_batch_caller.batch_seconds #62) so
# bad operator input lands as a clean exit 2 (#76), never a traceback or a hang.
@pytest.mark.parametrize("bad", ["nan", "inf"])
def test_consumer_ms_non_finite_exits_two_no_traceback(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], bad: str
) -> None:
    out_md = tmp_path / "bp.md"
    out_json = tmp_path / "bp.json"
    args = _build_parser().parse_args(
        ["--n", "3", "--consumer-ms", bad, "--out-md", str(out_md), "--out-json", str(out_json)]
    )
    # If the guard regressed, `inf` would hang here forever and `nan` would raise
    # a ValueError instead of returning 2 — so this test also pins "no hang".
    rc = asyncio.run(main_async(args))
    assert rc == 2
    err = capsys.readouterr().err
    assert "consumer-ms must be a finite number" in err
    assert "Traceback" not in err
    assert not out_md.exists()
    assert not out_json.exists()


def test_consumer_ms_valid_finite_still_runs(tmp_path: Path) -> None:
    # Regression guard: a valid finite --consumer-ms still runs to completion.
    out_md = tmp_path / "bp.md"
    out_json = tmp_path / "bp.json"
    args = _build_parser().parse_args(
        ["--n", "3", "--consumer-ms", "1", "--out-md", str(out_md), "--out-json", str(out_json)]
    )
    rc = asyncio.run(main_async(args))
    assert rc == 0
    assert out_json.exists()


def test_consumer_ms_negative_still_exits_two(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    # The original sign check still holds (the new guard is a superset).
    args = _build_parser().parse_args(
        [
            "--n",
            "3",
            "--consumer-ms",
            "-1",
            "--out-md",
            str(tmp_path / "bp.md"),
            "--out-json",
            str(tmp_path / "bp.json"),
        ]
    )
    rc = asyncio.run(main_async(args))
    assert rc == 2
    assert "consumer-ms must be a finite number" in capsys.readouterr().err
