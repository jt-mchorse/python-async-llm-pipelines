"""The real-API sentence agrees with the speedup the table prints (#198).

`render_markdown` chose the sentence on the full-precision speedup and printed
it `:.2f`. Measured on main: a 19.996x async row printed `20.00×` in the table,
then "so real-API speedups land below the 20.00× measured above".
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parents[1]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from async_pipelines.benchmark import RunResult, attach_speedup  # noqa: E402
from scripts.bench_1000_doc import DEFAULT_WORKLOAD, render_markdown  # noqa: E402

IN_RANGE = "so real-API speedups land in the 5-20x spec range."
BELOW = "so real-API speedups land below the"


def _report(speedup: float) -> tuple[str, str]:
    serial_s = 40.0
    results = attach_speedup(
        [
            RunResult("serial", 1000, serial_s, 1000 / serial_s),
            RunResult("async", 1000, serial_s / speedup, 1000 * speedup / serial_s),
            RunResult("async+batched", 1000, 0.2, 5000.0),
        ]
    )
    md = render_markdown(DEFAULT_WORKLOAD, results)
    cell = next(line for line in md.splitlines() if line.startswith("| async |")).split("|")[-2]
    return cell.strip(), md


@pytest.mark.parametrize("speedup", [19.995, 19.9951, 19.996, 19.999, 20.0, 20.004])
def test_a_speedup_printed_as_20_00_gets_the_in_range_sentence(speedup: float) -> None:
    cell, md = _report(speedup)
    assert cell == "20.00×"  # precondition: the table cell reads 20.00×
    assert IN_RANGE in md
    assert BELOW not in md


@pytest.mark.parametrize("speedup", [19.99, 19.994])
def test_a_speedup_printed_below_20_gets_the_below_sentence(speedup: float) -> None:
    cell, md = _report(speedup)
    assert cell != "20.00×"
    assert f"land below the {cell} measured above" in md


def test_the_cell_and_the_sentence_agree_across_the_boundary() -> None:
    disagreements = []
    for i in range(-200, 201):
        speedup = 20.0 + i * 0.00005
        cell, md = _report(speedup)
        in_range = IN_RANGE in md
        printed_in_range = float(cell.rstrip("×")) >= 20
        if in_range != printed_in_range:
            disagreements.append((speedup, cell, in_range))
    assert disagreements == []
