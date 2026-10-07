"""The OOM-safety section identifies a row by (n, queue_size), not by n (#149).

`--compare` adds a same-`n` row at 4x the queue, so one `n` can be both a row
that filled the queue and one that did not. Grouping by `n` alone made the
report contradict itself. Measured on `main` with
`--n 20 --queue-size 8 --consumer-ms 1 --compare` (rows: q=8 filled, 6 pauses,
depth 8; q=32 depth 20, no pauses):

    The rows that filled the queue share one `n` ([20]) ... exceeds the
    `queue_size` (32) ...
    Rows at `n` [20] never filled the queue ...

The advice also quoted the largest queue in the table (the 4x row) instead of
the queue a `--compare-n` row runs at, which is the base row's.
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
        duration_s=0.013,
        peak_heap_kb=15.0,
        metrics={
            "produced": n,
            "consumed": n,
            "producer_pauses": pauses,
            "max_queue_depth": depth,
            "producer_pause_seconds": 0.0,
        },
    )


def _oom_section(rows: list[BackpressureResult]) -> str:
    md = _render_markdown(rows)
    return md[md.index("## OOM-safety claim") :]


COMPARE_ROWS = [_row(20, 8, depth=8, pauses=6), _row(20, 32, depth=20, pauses=0)]


def test_a_same_n_compare_pair_is_not_described_as_one_n_both_ways() -> None:
    text = _oom_section(COMPARE_ROWS)
    assert "Rows at `n` [20] never filled the queue" not in text
    assert "Rows at (`n`, `queue_size`) [(20, 32)] never filled the queue" in text


def test_the_advice_quotes_the_queue_the_rows_that_filled_ran_at() -> None:
    text = _oom_section(COMPARE_ROWS)
    assert "exceeds the `queue_size` (8)" in text
    assert "exceeds the `queue_size` (32)" not in text


def test_a_compare_n_row_that_did_not_fill_is_named_with_its_queue() -> None:
    # The #134 shape: --compare-n's n // 10 row below the queue.
    text = _oom_section([_row(50, 8, depth=8, pauses=10), _row(5, 8, depth=5, pauses=0)])
    assert "Rows at (`n`, `queue_size`) [(5, 8)] never filled the queue" in text


def test_every_row_filled_keeps_its_sentence_and_names_no_unfilled_rows() -> None:
    text = _oom_section([_row(100, 8, depth=8, pauses=40), _row(10, 8, depth=8, pauses=2)])
    assert "never filled the queue" not in text
    assert "The rows span more than one `n` ([10, 100])" in text
