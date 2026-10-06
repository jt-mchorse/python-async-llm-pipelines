"""Only rows where the bound applied count as evidence for it (#134).

`--n 50 --compare-n` adds an n=5 row at queue_size 8: max_queue_depth 5, no
producer pauses -- the queue never filled, so the bound never applied, and the
report still called the rows "evidence for the `n`-independence of the bound".
And with one `n` the report said "Re-run with `--compare-n`" even when it had
been passed: at `--n 1` the extra row is max(1, 1 // 10) = 1 again.
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.bench_backpressure import BackpressureResult, _render_markdown  # noqa: E402


def _row(n: int, queue_size: int, depth: int, pauses: int) -> BackpressureResult:
    return BackpressureResult(
        n=n,
        queue_size=queue_size,
        consumer_ms=1.0,
        concurrency=2,
        duration_s=0.1,
        peak_heap_kb=10.0 + n / 100,
        metrics={
            "producer_pauses": pauses,
            "max_queue_depth": depth,
            "producer_pause_seconds": 0.01 * pauses,
            "items_in": n,
            "items_out": n,
        },
    )


_EVIDENCE = "evidence for the `n`-independence"


def test_an_unfilled_compare_n_row_is_not_evidence() -> None:
    md = _render_markdown([_row(50, 8, 8, 20), _row(5, 8, 5, 0)])
    assert _EVIDENCE not in md
    assert "Rows at `n` [5] never filled the queue" in md


def test_rows_that_all_filled_the_queue_keep_the_committed_sentence() -> None:
    md = _render_markdown([_row(5000, 8, 8, 2558), _row(500, 8, 8, 248)])
    assert "The rows span more than one `n` ([500, 5000])" in md
    assert _EVIDENCE in md
    assert "never filled" not in md


def test_filled_rows_spanning_n_beside_an_unfilled_one_name_only_the_filled() -> None:
    md = _render_markdown([_row(5000, 8, 8, 9), _row(500, 8, 8, 3), _row(5, 8, 5, 0)])
    assert "The rows that filled the queue span more than one `n` ([500, 5000])" in md
    assert "Rows at `n` [5] never filled the queue" in md


def test_one_n_states_the_condition_rather_than_naming_a_flag_already_used() -> None:
    md = _render_markdown([_row(1, 8, 1, 0)])
    assert "Re-run with `--compare-n` for a varied-`n` row" not in md
    assert "`n // 10` exceeds the `queue_size` (8)" in md


# --- #144: no row filled the queue -------------------------------------------


def test_no_filled_row_names_no_filled_rows() -> None:
    # The measured `--n 100 --queue-size 200 --compare-n` shape.
    md = _render_markdown([_row(100, 200, 100, 0), _row(10, 200, 10, 0)])
    assert "The rows that filled the queue" not in md
    assert "share one `n` ([10, 100])" not in md
    assert "No row filled the queue" in md
    assert "`n // 10` exceeds the `queue_size` (200)" in md
    # Said once, not twice.
    assert "never filled the queue" not in md


def test_no_filled_row_does_not_present_the_bound_as_shown() -> None:
    md = _render_markdown([_row(100, 200, 100, 0), _row(10, 200, 10, 0)])
    assert "a fast producer cannot pile items up" not in md
    assert "but no row reached it" in md


def test_one_filled_row_keeps_the_bound_sentence() -> None:
    md = _render_markdown([_row(5000, 8, 8, 2558)])
    assert "a fast producer cannot pile items up" in md
    assert "The rows that filled the queue share one `n` ([5000])" in md
    assert "No row filled the queue" not in md
