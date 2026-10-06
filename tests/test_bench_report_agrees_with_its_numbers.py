"""The bench report does not contradict its own numbers under non-default args (#129).

Two sentences in `render_markdown` were written for the default workload:

* the latency bullet used `:.0f` ms, so `--latency 0.0004` rendered `0 ms`
  directly above `` `await asyncio.sleep(0.0004)` `` (and 0.0025 -> `2 ms`);
* the Real-API paragraph said real speedups "land in the 5-20x spec range"
  right under a table whose ceiling, at `--concurrency 2`, is about 2x -- a real
  floor above the measured ceiling.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from async_pipelines.benchmark import RunResult, Workload  # noqa: E402
from scripts.bench_1000_doc import render_markdown  # noqa: E402

_SPEC = "real-API speedups land in the 5-20x spec range"


def _results(async_speedup: float) -> list[RunResult]:
    return [
        RunResult("serial", 40, 1.0, 40.0, 1.0),
        RunResult("async", 40, 1.0 / async_speedup, 40.0 * async_speedup, async_speedup),
        RunResult("async+batched", 40, 1.0 / async_speedup, 40.0 * async_speedup, async_speedup),
    ]


def _workload(latency: float, concurrency: int = 32) -> Workload:
    return Workload(n_docs=40, llm_call_seconds=latency, concurrency=concurrency, batch_size=1)


@pytest.mark.parametrize(
    ("latency", "shown"), [(0.0004, "0.4 ms"), (0.0025, "2.5 ms"), (0.02, "20 ms")]
)
def test_the_latency_bullet_shows_the_configured_value(latency: float, shown: str) -> None:
    md = render_markdown(_workload(latency), _results(30.0))
    bullet = next(line for line in md.splitlines() if line.startswith("- **Workload.**"))
    assert f"{shown} simulated per call" in bullet, bullet
    assert f"asyncio.sleep({latency})" in md  # the line beneath it agrees


def test_a_low_ceiling_run_does_not_claim_real_apis_beat_it() -> None:
    md = render_markdown(_workload(0.01, concurrency=2), _results(2.02))
    assert _SPEC not in md
    assert "land below the 2.02× measured above" in md
    # The cause is named among the flags this run changed (#135); the sentence
    # used to name `--concurrency 32` whatever the run had changed.
    assert "this run used " in md
    assert "`--concurrency 2`" in md


@pytest.mark.parametrize("ceiling", [5.0, 19.99])
def test_a_ceiling_inside_the_spec_range_still_gets_the_relative_sentence(ceiling: float) -> None:
    # 5-20x under a 12x ceiling would still claim real APIs may reach 20x.
    md = render_markdown(_workload(0.01, concurrency=16), _results(ceiling))
    assert _SPEC not in md
    assert f"land below the {ceiling:.2f}× measured above" in md


def test_the_default_workload_sentence_is_unchanged() -> None:
    md = render_markdown(_workload(0.02), _results(30.0))
    assert _SPEC in md


def test_the_committed_snapshot_still_matches_the_template() -> None:
    # The committed run's ceiling clears 20x, so its paragraph and its latency
    # bullet are byte-identical under the new template: nothing to regenerate.
    committed = (ROOT / "docs" / "benchmarks.md").read_text(encoding="utf-8")
    assert _SPEC in committed
    assert re.search(r"· 20 ms simulated per call ·", committed)
