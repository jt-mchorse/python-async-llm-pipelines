"""Snapshot test for the README's 1000-doc benchmark table.

The README's "1000-doc benchmark" section quotes three rows
(`serial`, `async`, `async+batched`) with specific cells for `duration
(s)`, `docs/s`, and `speedup vs serial`. The committed
``docs/benchmarks.json`` is the source of truth — the README and
``docs/benchmarks.md`` are both *renderings* of that JSON.

Three sources of truth for the same numbers, with nothing previously
asserting they agree. This test locks **README ↔ JSON** and
**benchmarks.md ↔ JSON** consistency. The wall-clock values are
non-deterministic across machines (so we don't re-run the benchmark
here — that would be a CI flake), but the *rendering* is fully
deterministic given the JSON.

Format rules, observed today:

- README's table uses `:.3f` for `duration`, `:.1f` for `docs/s`, and
  conditional speedup precision: `:.2f×` for ratios < 100, `:.0f×` for
  ratios ≥ 100 (the batched ratio is the only one that triggers the
  integer form). Async and batched cells are bolded; serial is not.
- ``docs/benchmarks.md`` uses `:.3f` / `:.1f` / `:.2f×` uniformly,
  without bolding.

Same hygiene pattern as the sister snapshot tests across the portfolio.

When this snapshot fails, regenerate with::

    python scripts/bench_1000_doc.py --n 1000 --concurrency 32 \
        --batch-size 8 --out docs/benchmarks.md

…then update the README's table cells and inspect with
``git diff README.md docs/benchmarks.*`` before committing.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
README = REPO_ROOT / "README.md"
BENCH_JSON = REPO_ROOT / "docs" / "benchmarks.json"
BENCH_MD = REPO_ROOT / "docs" / "benchmarks.md"

EXPECTED_PIPELINES = {"serial", "async", "async+batched"}

REGEN_HINT = (
    "Regenerate the benchmark:\n"
    "  python scripts/bench_1000_doc.py --n 1000 --concurrency 32 "
    "--batch-size 8 --out docs/benchmarks.md\n"
    "Then update the README table cells from docs/benchmarks.json and "
    "inspect with `git diff README.md docs/benchmarks.*` before committing."
)


@pytest.fixture(scope="module")
def bench_results() -> dict[str, dict]:
    payload = json.loads(BENCH_JSON.read_text(encoding="utf-8"))
    return {r["pipeline_name"]: r for r in payload["results"]}


def _readme_speedup(ratio: float) -> str:
    """Mirror the README's conditional speedup format: :.2f below 100, :.0f at/above."""
    if ratio < 100.0:
        return f"{ratio:.2f}×"
    return f"{ratio:.0f}×"


def _bench_md_speedup(ratio: float) -> str:
    """benchmarks.md uses uniform :.2f× regardless of magnitude."""
    return f"{ratio:.2f}×"


def test_bench_json_has_expected_pipelines(bench_results: dict[str, dict]) -> None:
    """Guard against silently adding or dropping a pipeline run."""
    assert set(bench_results) == EXPECTED_PIPELINES, (
        f"docs/benchmarks.json pipeline-name set {sorted(bench_results)} "
        f"differs from expected {sorted(EXPECTED_PIPELINES)}.\n{REGEN_HINT}"
    )


@pytest.mark.parametrize("pipeline", sorted(EXPECTED_PIPELINES))
def test_readme_row_matches_bench_json(pipeline: str, bench_results: dict[str, dict]) -> None:
    """The README's table row for `pipeline` must quote the JSON cells exactly."""
    r = bench_results[pipeline]
    duration_cell = f"{r['duration_seconds']:.3f}"
    dps_cell = f"{r['docs_per_second']:.1f}"
    speedup_cell = _readme_speedup(r["speedup_vs_serial"])

    readme = README.read_text(encoding="utf-8")
    # Each row is on one line; check the three cells appear in a row whose
    # first cell mentions the pipeline name. Doing a single substring search
    # over the entire README would false-positive on coincidental numbers
    # elsewhere; bounding to one line keeps it strict.
    rows = [
        line
        for line in readme.splitlines()
        if line.strip().startswith("|") and f" {pipeline} " in line
    ]
    assert rows, (
        f"README does not contain a row for pipeline `{pipeline}` in "
        f"the 1000-doc benchmark table.\n{REGEN_HINT}"
    )
    row_text = " ".join(rows)  # Joining tolerates multiple matching rows (won't happen today).

    for cell, label in (
        (duration_cell, "duration"),
        (dps_cell, "docs/s"),
        (speedup_cell, "speedup"),
    ):
        assert cell in row_text, (
            f"README `{pipeline}` row is missing the {label} cell `{cell}` "
            f"(live JSON value: {r}).\n{REGEN_HINT}"
        )


@pytest.mark.parametrize("pipeline", sorted(EXPECTED_PIPELINES))
def test_benchmarks_md_row_matches_bench_json(
    pipeline: str, bench_results: dict[str, dict]
) -> None:
    """The docs/benchmarks.md table row must quote the JSON cells exactly."""
    r = bench_results[pipeline]
    duration_cell = f"{r['duration_seconds']:.3f}"
    dps_cell = f"{r['docs_per_second']:.1f}"
    speedup_cell = _bench_md_speedup(r["speedup_vs_serial"])

    text = BENCH_MD.read_text(encoding="utf-8")
    rows = [
        line
        for line in text.splitlines()
        if line.strip().startswith("|") and f" {pipeline} " in line
    ]
    assert rows, (
        f"docs/benchmarks.md does not contain a row for pipeline "
        f"`{pipeline}` in the 1000-doc benchmark table.\n{REGEN_HINT}"
    )
    row_text = " ".join(rows)

    for cell, label in (
        (duration_cell, "duration"),
        (dps_cell, "docs/s"),
        (speedup_cell, "speedup"),
    ):
        assert cell in row_text, (
            f"docs/benchmarks.md `{pipeline}` row is missing the {label} "
            f"cell `{cell}` (live JSON value: {r}).\n{REGEN_HINT}"
        )
