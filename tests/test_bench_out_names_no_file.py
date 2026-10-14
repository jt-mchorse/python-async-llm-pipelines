"""`bench_1000_doc --out` with no final path component exits 2, not 1 (#183).

`_json_path_for` derives the raw-results path with `Path.with_name`, which
raises ValueError when the name is empty. `''` (a `--out "$OUT"` with `OUT`
unset), `.` and `/` are all such paths. The pre-run check (#167) built that path
inside a `try` that caught only OSError. Measured on `main`, each one exited 1
with a traceback, while `..` and a named directory exited 2 with
`could not write report: ...`.

Through a subprocess, because the contract is the exit code and stderr an
operator sees.
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest

BENCH_PY = Path(__file__).resolve().parents[1] / "scripts" / "bench_1000_doc.py"


def _run(out: str, cwd: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, str(BENCH_PY), "--n", "1", "--latency", "0", "--out", out],
        cwd=cwd,
        capture_output=True,
        text=True,
        check=False,
        timeout=60,
    )


@pytest.mark.parametrize("out", ["", ".", "/"], ids=["empty", "dot", "root"])
def test_an_out_with_no_name_is_refused_cleanly(out: str, tmp_path: Path) -> None:
    proc = _run(out, tmp_path)
    assert proc.returncode == 2, proc.stderr
    assert "Traceback" not in proc.stderr, proc.stderr
    lines = proc.stderr.strip().splitlines()
    assert len(lines) == 1, proc.stderr
    assert lines[0].startswith("could not write report: "), lines[0]
    # Refused before the run, so nothing was written into the directory.
    assert list(tmp_path.iterdir()) == []


@pytest.mark.parametrize("kind", ["dotdot", "named-dir"])
def test_other_directory_outs_keep_the_same_contract(kind: str, tmp_path: Path) -> None:
    # Control: these already took the clean exit 2 on `main`.
    sub = tmp_path / "sub"
    sub.mkdir()
    proc = _run(".." if kind == "dotdot" else str(sub), sub)
    assert proc.returncode == 2, proc.stderr
    assert "Traceback" not in proc.stderr, proc.stderr
    assert proc.stderr.startswith("could not write report: "), proc.stderr
