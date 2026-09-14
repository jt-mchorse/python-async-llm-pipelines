"""The three surfaces of one backpressure measurement agree where they can (#111).

One run is published three times:

* ``docs/backpressure.json`` — what the generator writes.
* ``docs/backpressure.md`` — rendered from the same in-memory results.
* ``README.md``'s table — a hand-maintained excerpt.

Nothing pinned any pair, and the README's table turned out to be from a **different
run** than the report it links to as "full report"::

    queue_size   README duration   json duration   README pauses   json pauses
             8             3.051           3.371            2707          2523
            32             3.080           3.362            2672          2510

``max_queue_depth`` agreed in both (8 and 32), which is exactly the point: the
invariant column is the stable one and the timing columns are measurements of a
machine.

So the pinning is split deliberately, and the split is the substance of this module:

* ``n``, ``queue_size`` and ``max_queue_depth`` are host-independent properties of
  the algorithm. Pinned across **all three** surfaces.
* ``duration_s``, ``peak_heap_kb``, ``producer_pauses`` are measurements. Pinned
  only for the ``.md``-against-``.json`` pair, which is rendered from one run and is
  therefore deterministic. Pinning them against the README would be an assertion
  about this machine, which is how a suite becomes flaky and then gets deleted.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parents[1]
BACKPRESSURE_JSON = _REPO_ROOT / "docs" / "backpressure.json"
BACKPRESSURE_MD = _REPO_ROOT / "docs" / "backpressure.md"
README = _REPO_ROOT / "README.md"

#: Columns that describe the algorithm, not the machine it ran on.
HOST_INDEPENDENT = ("n", "queue_size", "max_queue_depth")

#: Columns that describe the machine. Deterministic within one run, not across runs.
HOST_DEPENDENT = ("duration_s", "peak_heap_kb", "producer_pauses")


@pytest.fixture(scope="module")
def results() -> list[dict]:
    payload = json.loads(BACKPRESSURE_JSON.read_text(encoding="utf-8"))
    rows = payload["results"]
    assert rows, "docs/backpressure.json has no results"
    return rows


def _md_rows() -> list[list[str]]:
    """The `## Measured` table of `docs/backpressure.md`, as cell lists."""
    text = BACKPRESSURE_MD.read_text(encoding="utf-8")
    body = text.split("## Measured", 1)[1].split("##", 1)[0]
    out = []
    for line in body.splitlines():
        line = line.strip()
        if not line.startswith("|") or set(line) <= set("|-: "):
            continue
        cells = [c.strip() for c in line.strip("|").split("|")]
        if cells and cells[0] == "n":
            continue  # header
        out.append(cells)
    return out


def _readme_rows() -> list[list[str]]:
    """The backpressure table in `README.md`, as cell lists."""
    text = README.read_text(encoding="utf-8")
    start = text.index("| n | queue_size | duration_s |")
    body = text[start:].split("\n\n", 1)[0]
    out = []
    for line in body.splitlines():
        cells = [c.strip().strip("*") for c in line.strip().strip("|").split("|")]
        if not cells or cells[0] in ("n", "---:") or set(line) <= set("|-: "):
            continue
        out.append(cells)
    return out


# --- the host-independent columns, across all three surfaces ----------------


def test_the_md_table_matches_the_json_in_full(results: list[dict]) -> None:
    """Same run, so every column is deterministic and all of them are pinned."""
    rows = _md_rows()
    assert len(rows) == len(results), (
        f"docs/backpressure.md has {len(rows)} rows and the JSON has {len(results)}"
    )
    for cells, r in zip(rows, results, strict=True):
        m = r["metrics"]
        expected = [
            str(r["n"]),
            str(r["queue_size"]),
            str(r["consumer_ms"]),
            str(r["concurrency"]),
            f"{r['duration_s']:.3f}",
            f"{r['peak_heap_kb']:.1f}",
            str(m["producer_pauses"]),
            str(m["max_queue_depth"]),
            f"{m['producer_pause_seconds']:.3f}",
        ]
        assert cells == expected, (
            "docs/backpressure.md disagrees with docs/backpressure.json; regenerate:\n"
            "  python scripts/bench_backpressure.py --n 5000 --queue-size 8 "
            "--consumer-ms 1 --concurrency 2 --compare --compare-n"
        )


def test_the_readme_table_matches_the_json_on_the_invariant_columns(
    results: list[dict],
) -> None:
    """`n`, `queue_size`, `max_queue_depth` — the columns the claim rests on.

    This is the arm the pre-#111 README fails: its rows carried a different run's
    timings, and nothing compared any of its columns to the artifact it links to.
    """
    rows = _readme_rows()
    assert len(rows) == len(results), (
        f"README's backpressure table has {len(rows)} rows and the JSON has "
        f"{len(results)}; rebuild it from docs/backpressure.json"
    )
    for cells, r in zip(rows, results, strict=True):
        # README column order: n | queue_size | duration_s | peak_heap_kb |
        #                      producer_pauses | max_queue_depth
        assert cells[0] == str(r["n"]), f"README n {cells[0]} vs JSON {r['n']}"
        assert cells[1] == str(r["queue_size"]), (
            f"README queue_size {cells[1]} vs JSON {r['queue_size']}"
        )
        assert cells[5] == str(r["metrics"]["max_queue_depth"]), (
            f"README max_queue_depth {cells[5]} vs JSON {r['metrics']['max_queue_depth']}"
        )


def test_the_timing_columns_are_deliberately_not_pinned_to_the_readme() -> None:
    """The exclusion, stated as a test so it is a decision and not an oversight.

    `duration_s`, `peak_heap_kb` and `producer_pauses` differ between runs and
    between machines. Pinning them README-against-artifact would be an assertion
    about this laptop. This asserts the *split* exists rather than the values: the
    two column sets are disjoint and together cover what the tables publish.
    """
    assert set(HOST_INDEPENDENT).isdisjoint(HOST_DEPENDENT)
    assert "max_queue_depth" in HOST_INDEPENDENT, (
        "the invariant column must be on the pinned side or this module pins nothing that matters"
    )
    assert "duration_s" in HOST_DEPENDENT


# --- the claim must name evidence that exists -------------------------------


def test_the_oom_claim_does_not_assert_n_independence_without_varied_n(
    results: list[dict],
) -> None:
    """The defect #111 opened with, in its general form.

    The document used to say the bound holds "regardless of `n`" while every row
    shared one `n`. The renderer now computes the sentence from the rows, so this
    asserts the two cannot disagree again in either direction: varied `n` must be
    claimed, and a single `n` must not be.
    """
    text = BACKPRESSURE_MD.read_text(encoding="utf-8")
    n_values = sorted({r["n"] for r in results})
    if len(n_values) > 1:
        assert "span more than one `n`" in text, (
            "the rows vary `n` and the claim does not say so — the evidence is "
            "present and unclaimed"
        )
    else:
        assert "share one `n`" in text, (
            "every row has the same `n` and the document does not say so — this is "
            "the #111 over-claim returning"
        )
    assert "regardless of `n`" not in text, (
        "the unqualified 'regardless of `n`' phrasing is back; the claim has to name "
        "the rows it actually has"
    )


def test_the_committed_rows_do_vary_n(results: list[dict]) -> None:
    """Anti-vacuous for the arm above: the committed artifact takes the `n`-varying
    branch, so the `else` is not what is being exercised in CI.

    Also the regression on the generator: before #111 `bench_backpressure.py` had no
    `n` axis at all, so this was unreachable rather than merely false.
    """
    n_values = sorted({r["n"] for r in results})
    assert len(n_values) > 1, (
        f"the committed run has a single n ({n_values}); regenerate with --compare-n "
        "so the published table can exhibit the bound across n"
    )
    queue_values = sorted({r["queue_size"] for r in results})
    assert len(queue_values) > 1, f"the committed run has a single queue_size ({queue_values})"
    # And there must be a pair sharing queue_size while differing in n, or the
    # table varies both at once and isolates neither.
    by_queue: dict[int, set[int]] = {}
    for r in results:
        by_queue.setdefault(r["queue_size"], set()).add(r["n"])
    assert any(len(ns) > 1 for ns in by_queue.values()), (
        f"no queue_size is measured at more than one n, so the table varies both axes "
        f"together and isolates neither: {by_queue}"
    )


def test_every_row_satisfies_the_bound_the_document_claims(results: list[dict]) -> None:
    """The published artifact must actually exhibit its own invariant."""
    for r in results:
        assert r["metrics"]["max_queue_depth"] <= r["queue_size"], (
            f"the committed artifact violates its own OOM-safety claim at "
            f"n={r['n']} queue_size={r['queue_size']}: "
            f"max_queue_depth={r['metrics']['max_queue_depth']}"
        )


def test_the_readme_documents_the_command_that_produces_the_committed_rows() -> None:
    """The documented command must include the flag the committed table needs.

    Without `--compare-n` the command produces a single-`n` table, and the README
    would be showing rows its own instructions cannot reproduce — the shape this
    session found in three other repos.
    """
    text = README.read_text(encoding="utf-8")
    block = re.search(r"```bash\n(python scripts/bench_backpressure\.py.*?)```", text, re.S)
    assert block, "the README no longer documents the bench_backpressure command"
    command = block.group(1)
    assert "--compare-n" in command, (
        "the documented command omits --compare-n, so following it produces a "
        "single-`n` table and the README's rows become unreproducible"
    )
    assert "--compare" in command
