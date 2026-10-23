"""`bench_1000_doc --out` and its derived JSON path naming one file exit 2 (#220).

`_json_path_for` keeps the two *names* apart (#118), but `atomic_write_text`
follows a symlink (#157), so a symlink between the pair let the JSON replace
the markdown report. Measured on `main` (8f0d57c): with `bench.json ->
bench.md`, `--out bench.md` printed both "wrote" lines, exited 0, and left
`bench.md` starting with `{ "results": [`. `bench_backpressure` has refused
the same setup on its two flags since #188.

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

BENCH_PY = Path(__file__).resolve().parents[1] / "scripts" / "bench_1000_doc.py"


def _run(out: str, cwd: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, str(BENCH_PY), "--n", "4", "--latency", "0", "--out", out],
        cwd=cwd,
        capture_output=True,
        text=True,
        check=False,
        timeout=60,
    )


# (out, link, target, the target exists first). Each link makes the derived
# pair resolve to one file.
_ALIASES = {
    "json-to-existing-md": ("bench.md", "bench.json", "bench.md", True),
    "json-to-dangling-md": ("bench.md", "bench.json", "bench.md", False),
    "md-to-json": ("bench.md", "bench.md", "bench.json", False),
    "non-md-suffix": ("run.txt", "run.txt.json", "run.txt", True),
}


@pytest.mark.parametrize("alias", sorted(_ALIASES))
def test_a_symlinked_pair_is_refused_before_the_run(alias: str, tmp_path: Path) -> None:
    out, link, target, exists = _ALIASES[alias]
    if exists:
        (tmp_path / target).write_text("keep\n", encoding="utf-8")
    (tmp_path / link).symlink_to(target)
    before = sorted(p.name for p in tmp_path.iterdir())
    proc = _run(out, tmp_path)
    assert proc.returncode == 2, proc.stderr
    assert "Traceback" not in proc.stderr, proc.stderr
    lines = proc.stderr.strip().splitlines()
    assert len(lines) == 1, proc.stderr
    assert lines[0].startswith("could not write report: "), lines[0]
    assert "same file" in lines[0], lines[0]
    # Refused before the run: no report printed, nothing written.
    assert proc.stdout == "", proc.stdout
    assert sorted(p.name for p in tmp_path.iterdir()) == before
    if exists:
        assert (tmp_path / target).read_text(encoding="utf-8") == "keep\n"


def test_an_existing_hard_link_is_the_same_file(tmp_path: Path) -> None:
    # Parity with `bench_backpressure` (#188), which refuses a hard-linked pair
    # too. Not a harm on its own -- `os.replace` breaks the link -- so this arm
    # pins the shared check, not the bug.
    (tmp_path / "bench.md").write_text("keep\n", encoding="utf-8")
    os.link(tmp_path / "bench.md", tmp_path / "bench.json")
    proc = _run("bench.md", tmp_path)
    assert proc.returncode == 2, proc.stderr
    assert "same file" in proc.stderr, proc.stderr
    assert (tmp_path / "bench.md").read_text(encoding="utf-8") == "keep\n"


def test_a_plain_out_still_writes_both(tmp_path: Path) -> None:
    # Control: the documented shape.
    proc = _run("bench.md", tmp_path)
    assert proc.returncode == 0, proc.stderr
    assert (tmp_path / "bench.md").read_text(encoding="utf-8").startswith("#")
    assert json.loads((tmp_path / "bench.json").read_text(encoding="utf-8"))["results"]


def test_a_json_symlink_to_another_file_still_writes_both(tmp_path: Path) -> None:
    # Control: a symlinked JSON path is fine when it points somewhere else, so
    # the check compares identity, not "is a symlink".
    (tmp_path / "elsewhere.json").write_text("old\n", encoding="utf-8")
    (tmp_path / "bench.json").symlink_to("elsewhere.json")
    proc = _run("bench.md", tmp_path)
    assert proc.returncode == 0, proc.stderr
    assert (tmp_path / "bench.md").read_text(encoding="utf-8").startswith("#")
    assert json.loads((tmp_path / "elsewhere.json").read_text(encoding="utf-8"))["results"]
