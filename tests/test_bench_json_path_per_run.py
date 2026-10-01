"""Every `bench_1000_doc` run's raw JSON is its own (#118).

The JSON path was `out_path.with_suffix(".json")` plus one guard for
`--out foo.json`, whose comment gave the remedy -- "Append instead of replace so
both artifacts survive" -- for the one collision it considered. `with_suffix`
replaces whatever follows the last dot, so `--out run.a` and `--out run.b` both
wrote `run.json`, and the second run silently overwrote the first run's raw
results. Sibling of `llm-cost-optimizer#231`.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))

from bench_1000_doc import _json_path_for, main  # noqa: E402

_ARGS = ["--latency", "0.001", "--concurrency", "4", "--batch-size", "4"]


@pytest.mark.parametrize(
    ("out", "json_name"),
    [
        ("benchmarks.md", "benchmarks.json"),  # the documented invocation, unchanged
        ("foo.json", "foo.json.json"),  # the pre-existing self-collision guard's answer
        ("run.a", "run.a.json"),
        ("run.2026-09-30", "run.2026-09-30.json"),
        ("report", "report.json"),
        ("report.v1.md", "report.v1.json"),
    ],
)
def test_the_json_path_belongs_to_its_report(out: str, json_name: str) -> None:
    assert _json_path_for(Path("d") / out) == Path("d") / json_name


def test_the_json_never_lands_on_its_own_report() -> None:
    for out in ("benchmarks.md", "foo.json", "run.a", "x.json.md"):
        assert _json_path_for(Path(out)) != Path(out)


def test_two_dotted_runs_keep_their_own_raw_results(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """The issue's repro, through `main`, on file contents."""
    assert main(["--n", "8", *_ARGS, "--out", str(tmp_path / "run.a")]) == 0
    assert main(["--n", "12", *_ARGS, "--out", str(tmp_path / "run.b")]) == 0
    _ = capsys.readouterr()
    assert sorted(p.name for p in tmp_path.iterdir()) == [
        "run.a",
        "run.a.json",
        "run.b",
        "run.b.json",
    ]
    assert json.loads((tmp_path / "run.a.json").read_text())["workload"]["n_docs"] == 8
    assert json.loads((tmp_path / "run.b.json").read_text())["workload"]["n_docs"] == 12
