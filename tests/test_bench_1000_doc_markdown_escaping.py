"""A free-form `pipeline_name` must not add a column to the rendered table (#92).

`render_markdown`'s output is written to `docs/benchmarks.md`, which is
committed. A `|` in the name added a column the header and separator lacked, so
GitHub drew a mangled grid rather than a wrong number — the failure mode that
survives review.

`pipeline_name` reaches the cell via `run_pipeline(pipeline: Any, docs)` →
`pipeline.name`, so any caller-supplied object with `.name` and `.run` gets
there. That is the same BYO-object path embedding-model-shootout#79 accepted as
the reason to escape its own free-form cell.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from async_pipelines.benchmark import RunResult, Workload  # noqa: E402
from scripts.bench_1000_doc import render_markdown  # noqa: E402

WORKLOAD = Workload(n_docs=10, llm_call_seconds=0.01, concurrency=4, batch_size=2)


def _result(name: str, speedup: float | None = None) -> RunResult:
    return RunResult(
        pipeline_name=name,
        n_docs=10,
        duration_seconds=1.0,
        docs_per_second=10.0,
        speedup_vs_serial=speedup,
    )


def _table_rows(name: str) -> tuple[str, str, str]:
    """(header, separator, first data row) of the pipeline table."""
    rows = [
        line
        for line in render_markdown(WORKLOAD, [_result(name)]).splitlines()
        if line.startswith("|")
    ]
    header = next(line for line in rows if "pipeline |" in line)
    idx = rows.index(header)
    return header, rows[idx + 1], rows[idx + 2]


def _columns(row: str) -> int:
    """GFM column count: cells between the outer pipes, `\\|` not counting."""
    return len(row.strip().strip("|").replace("\\|", "").split("|"))


# ---------------------------------------------------------------------------
# The defect
# ---------------------------------------------------------------------------


def test_pipe_in_pipeline_name_keeps_the_row_aligned():
    header, separator, data = _table_rows("my|pipeline")

    assert _columns(data) == _columns(header) == _columns(separator)


@pytest.mark.parametrize("name", ["|lead", "trail|", "a|b|c", "||"])
def test_every_pipe_shape_stays_aligned(name):
    header, separator, data = _table_rows(name)

    assert _columns(data) == _columns(header) == _columns(separator)


def test_the_pipe_is_escaped_not_stripped():
    """GitHub renders `\\|` as a literal pipe, so the real name still shows."""
    _, _, data = _table_rows("my|pipeline")

    assert "my\\|pipeline" in data


# ---------------------------------------------------------------------------
# Locks
# ---------------------------------------------------------------------------


def test_clean_name_output_is_byte_identical():
    """`docs/benchmarks.md` is committed; regenerating it must not move."""
    _, _, data = _table_rows("serial")

    assert data == "| serial | 1.000 | 10.0 | — |"


def test_a_backtick_is_inert_here():
    """This cell is not an inline-code span, so no backtick handling is needed.

    Pinned so the deliberate omission reads as checked rather than missed — the
    backtick variant is real only where a cell *is* a code span
    (rag-production-kit#130).
    """
    header, separator, data = _table_rows("my`pipeline")

    assert _columns(data) == _columns(header) == _columns(separator)
    assert "my`pipeline" in data


def test_bench_backpressure_has_no_free_form_cell_to_escape():
    """The sibling renderer needs no equivalent fix — every cell is a number.

    Asserted rather than assumed, so the asymmetry between the two benchmark
    scripts is recorded as a checked difference.
    """
    source = (
        Path(__file__).resolve().parent.parent / "scripts" / "bench_backpressure.py"
    ).read_text()
    row_block = source.split('f"| {r.n} | {r.queue_size}')[1].split("lines.append")[0]

    # Every interpolation in the row is a numeric attribute or a formatted
    # float — no bare string field.
    assert "pipeline_name" not in row_block
    assert ":.3f}" in row_block or ":.1f}" in row_block
