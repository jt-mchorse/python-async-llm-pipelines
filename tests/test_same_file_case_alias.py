"""Two output names that alias on the filesystem are one file, existing or not (#234).

The bench scripts' guard (#188, #220) ran `samefile` only when both files
existed. On a fresh directory `Report.md` and `report.md` compared as different,
and on a case-insensitive filesystem (macOS's default) both writes landed in one
file: the JSON replaced the markdown report at exit 0. `same_file` now asks the
filesystem about two absent names by creating one and looking for the other.

The case-alias arms need a case-insensitive filesystem. Whether `tmp_path` is
one is detected, not assumed, so they run on macOS and skip on Linux CI with
the reason; the remaining arms run everywhere.
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest

from async_pipelines.io_utils import same_file

SCRIPTS = Path(__file__).resolve().parents[1] / "scripts"


def _case_insensitive(directory: Path) -> bool:
    probe = directory / "case-probe"
    probe.touch()
    try:
        return (directory / "CASE-PROBE").exists()
    finally:
        probe.unlink()


@pytest.fixture
def ci_dir(tmp_path: Path) -> Path:
    if not _case_insensitive(tmp_path):
        pytest.skip("needs a case-insensitive filesystem (macOS default); this one is not")
    return tmp_path


# --- same_file, everywhere --------------------------------------------------


def test_same_path_spellings_are_one_file(tmp_path: Path) -> None:
    (tmp_path / "sub").mkdir()
    a = str(tmp_path / "r.md")
    assert same_file(a, a)
    assert same_file(a, str(tmp_path / "sub" / ".." / "r.md"))


def test_a_dangling_and_an_existing_symlink_are_their_target(tmp_path: Path) -> None:
    (tmp_path / "link").symlink_to("r.md")
    assert same_file(tmp_path / "r.md", tmp_path / "link")
    (tmp_path / "r.md").write_text("x")
    assert same_file(tmp_path / "r.md", tmp_path / "link")


def test_a_hard_link_is_one_file(tmp_path: Path) -> None:
    (tmp_path / "a").write_text("x")
    os.link(tmp_path / "a", tmp_path / "b")
    assert same_file(tmp_path / "a", tmp_path / "b")


def test_distinct_names_are_two_files_and_the_probe_leaves_nothing(tmp_path: Path) -> None:
    assert not same_file(tmp_path / "report.md", tmp_path / "report.json")
    assert list(tmp_path.iterdir()) == []


def test_one_existing_name_is_not_an_absent_one(tmp_path: Path) -> None:
    (tmp_path / "a.md").write_text("keep")
    assert not same_file(tmp_path / "a.md", tmp_path / "b.md")
    assert not same_file(tmp_path / "b.md", tmp_path / "a.md")
    assert sorted(p.name for p in tmp_path.iterdir()) == ["a.md"]
    assert (tmp_path / "a.md").read_text() == "keep"


# --- case aliases, on a case-insensitive filesystem -------------------------


def test_a_case_alias_of_an_absent_file_is_one_file(ci_dir: Path) -> None:
    assert same_file(ci_dir / "Report.md", ci_dir / "report.md")
    assert list(ci_dir.iterdir()) == []


def test_a_case_alias_of_an_existing_file_is_one_file(ci_dir: Path) -> None:
    (ci_dir / "Report.md").write_text("x")
    assert same_file(ci_dir / "Report.md", ci_dir / "report.md")


def _refused(proc: subprocess.CompletedProcess[str]) -> None:
    assert proc.returncode == 2, proc.stdout + proc.stderr
    assert "name the same file" in proc.stderr
    assert "Traceback" not in proc.stderr


def test_bench_backpressure_refuses_a_case_alias_pair(ci_dir: Path) -> None:
    proc = subprocess.run(
        [sys.executable, str(SCRIPTS / "bench_backpressure.py"), "--n", "5", "--consumer-ms", "0",
         "--out-md", "Report.md", "--out-json", "report.md"],
        cwd=ci_dir, capture_output=True, text=True, check=False, timeout=60,
    )  # fmt: skip
    _refused(proc)
    assert list(ci_dir.iterdir()) == []


def test_bench_1000_doc_refuses_a_dangling_case_alias_symlink(ci_dir: Path) -> None:
    (ci_dir / "bench.json").symlink_to("BENCH.md")
    proc = subprocess.run(
        [sys.executable, str(SCRIPTS / "bench_1000_doc.py"), "--n", "4", "--latency", "0.001",
         "--out", "bench.md"],
        cwd=ci_dir, capture_output=True, text=True, check=False, timeout=60,
    )  # fmt: skip
    _refused(proc)
    assert sorted(p.name for p in ci_dir.iterdir()) == ["bench.json"]
