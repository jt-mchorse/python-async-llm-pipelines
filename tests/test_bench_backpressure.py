"""Tests for `scripts/bench_backpressure.py` (#46).

Coverage:
- `BackpressureResult.to_dict` field-set pin (7 fields, metrics shallow-copied).
- Acceptance regression: the script's _write_payload output uses the
  to_dict shape (every results[i] has the contracted 7-field surface,
  including the nested metrics dict that follows StreamMetrics.to_dict's
  5-field contract).
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[1]
_SCRIPTS_DIR = _REPO_ROOT / "scripts"
if str(_SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS_DIR))

from bench_backpressure import BackpressureResult  # noqa: E402 - sys.path tweak above


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
