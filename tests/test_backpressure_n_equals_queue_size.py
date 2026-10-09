"""A row with `n == queue_size` is not evidence for the queue bound (#161).

Its `max_queue_depth` reaches `queue_size`, but that depth is just `n`: the
producer put every item without waiting, and an unbounded queue gives the
identical row. #134's `saturated()` counted it anyway. Measured on `main` with
`--n 80 --queue-size 8 --consumer-ms 1 --concurrency 2 --compare-n`:

    | 80 | 8 | ... | 36 | 8 | ...
    | 8 | 8 | ... | 0 | 8 | ...
    The rows span more than one `n` ([8, 80]) at a fixed `queue_size`, which is
    what makes this table evidence for the `n`-independence of the bound ...

The script's own advice says `n // 10` must EXCEED the `queue_size`; 8 does not
exceed 8.
"""

from __future__ import annotations

import sys
from pathlib import Path

_SCRIPTS_DIR = Path(__file__).resolve().parents[1] / "scripts"
if str(_SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS_DIR))

from bench_backpressure import BackpressureResult, _render_markdown  # noqa: E402


def _row(n: int, queue_size: int, *, depth: int, pauses: int) -> BackpressureResult:
    return BackpressureResult(
        n=n,
        queue_size=queue_size,
        consumer_ms=1.0,
        concurrency=2,
        duration_s=0.05,
        peak_heap_kb=12.0,
        metrics={
            "produced": n,
            "consumed": n,
            "producer_pauses": pauses,
            "max_queue_depth": depth,
            "producer_pause_seconds": 0.0,
        },
    )


_EVIDENCE = "evidence for the `n`-independence"
#: The measured `--n 80 --queue-size 8 --compare-n` rows.
_COMPARE_N_80 = [_row(80, 8, depth=8, pauses=36), _row(8, 8, depth=8, pauses=0)]


def test_an_n_equals_queue_size_row_is_not_evidence_for_n_independence() -> None:
    md = _render_markdown(_COMPARE_N_80)
    assert _EVIDENCE not in md
    assert "The rows span more than one `n`" not in md
    assert "The rows that filled the queue share one `n` ([80])" in md


def test_the_n_equals_queue_size_row_is_named_as_one_the_bound_never_applied_to() -> None:
    md = _render_markdown(_COMPARE_N_80)
    assert "Rows at (`n`, `queue_size`) [(8, 8)] never filled the queue" in md
    # Its depth EQUALS queue_size, so no sentence may say it stayed below it.
    assert "below `queue_size`" not in md


def test_a_single_n_equals_queue_size_row_does_not_present_the_bound_as_shown() -> None:
    # `--n 8 --queue-size 8`: one row, depth 8, no pauses.
    md = _render_markdown([_row(8, 8, depth=8, pauses=0)])
    assert "a fast producer cannot pile items up" not in md
    assert "The rows that filled the queue" not in md
    assert "No row filled the queue" in md
    assert "stayed below" not in md


def test_control_one_more_item_than_the_queue_still_counts() -> None:
    # n = queue_size + 1 is the smallest row the bound can apply to; the fix
    # must not move the line past it.
    md = _render_markdown([_row(90, 8, depth=8, pauses=40), _row(9, 8, depth=8, pauses=1)])
    assert _EVIDENCE in md
    assert "never filled" not in md
