"""`bench_backpressure --out-md` and `--out-json` naming one file exit 2 (#188).

The script writes the markdown, then the JSON, to two independent paths. When
both named the same file the JSON replaced the report, both "wrote" lines
printed, and the run exited 0 -- the loss `bench_1000_doc` has guarded since
#118 (`--out foo.json` -> `foo.json.json`). Measured on `main`: the identical
path, a `./` spelling and a symlink alias each exited 0 with JSON only.

Through a subprocess, because the contract is the exit code and stderr an
operator sees.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

BENCH_PY = Path(__file__).resolve().parents[1] / "scripts" / "bench_backpressure.py"


def _run(md: str, js: str, cwd: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, str(BENCH_PY), "--n", "5", "--consumer-ms", "0"]
        + ["--out-md", md, "--out-json", js],
        cwd=cwd,
        capture_output=True,
        text=True,
        check=False,
        timeout=60,
    )


@pytest.mark.parametrize("spelling", ["identical", "dot-slash", "dotdot", "symlink"])
def test_one_file_named_twice_is_refused_before_the_run(spelling: str, tmp_path: Path) -> None:
    (tmp_path / "sub").mkdir()
    md, js = (
        "report",
        {
            "identical": "report",
            "dot-slash": "./report",
            "dotdot": "sub/../report",
            "symlink": "alias",
        }[spelling],
    )
    if spelling == "symlink":
        (tmp_path / "alias").symlink_to("report")
    before = sorted(p.name for p in tmp_path.iterdir())
    proc = _run(md, js, tmp_path)
    assert proc.returncode == 2, proc.stderr
    assert "Traceback" not in proc.stderr, proc.stderr
    lines = proc.stderr.strip().splitlines()
    assert len(lines) == 1, proc.stderr
    assert lines[0].startswith("could not write report: "), lines[0]
    assert "same file" in lines[0], lines[0]
    # Refused before the run: no row printed, nothing written.
    assert proc.stdout == "", proc.stdout
    assert sorted(p.name for p in tmp_path.iterdir()) == before


def test_an_existing_hard_link_is_the_same_file(tmp_path: Path) -> None:
    (tmp_path / "report").write_text("keep\n", encoding="utf-8")
    os.link(tmp_path / "report", tmp_path / "other")
    proc = _run("report", "other", tmp_path)
    assert proc.returncode == 2, proc.stderr
    assert "same file" in proc.stderr, proc.stderr
    assert (tmp_path / "report").read_text(encoding="utf-8") == "keep\n"


def test_distinct_paths_still_write_both_reports(tmp_path: Path) -> None:
    # Control: two files with one stem, the shape the defaults use.
    proc = _run("r.md", "r.json", tmp_path)
    assert proc.returncode == 0, proc.stderr
    assert (tmp_path / "r.md").read_text(encoding="utf-8").startswith("#")
    assert json.loads((tmp_path / "r.json").read_text(encoding="utf-8"))["results"]


def test_an_empty_flag_still_skips_that_report(tmp_path: Path) -> None:
    # Control: `--out-md ''` skips the markdown, so there is nothing to collide.
    proc = _run("", "r.json", tmp_path)
    assert proc.returncode == 0, proc.stderr
    assert sorted(p.name for p in tmp_path.iterdir()) == ["r.json"]
