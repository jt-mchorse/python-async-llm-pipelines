"""Smoke test for `scripts/capture_demo.sh` (issue #14).

The capture script is the deterministic driver for the 60-second README
demo. JT records the GIF/video while it runs; CI runs it with
`CAPTURE_PACE_SECONDS=0` so the demo can't bitrot the same way
`tests/test_bench_table_snapshot.py` already protects the committed
benchmark numbers in isolation.

Contract this test pins:

1. The script exits 0 on a fresh clone with no API key.
2. Each of the two surfaces actually runs (the surface header + the
   surface's distinctive output both appear).
3. The pytest step prints a passing summary line (no failures).
4. The bench step prints the rendered markdown header and one data row
   per pipeline name (`serial`, `async`, `async+batched`) — proving
   all three pipelines still wire end-to-end.
"""

from __future__ import annotations

import os
import re
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
SCRIPT = REPO_ROOT / "scripts" / "capture_demo.sh"

PIPELINES = ("serial", "async", "async+batched")


@pytest.fixture(scope="module")
def capture_output() -> str:
    """Run the capture script once and reuse its stdout across assertions.

    `CAPTURE_PACE_SECONDS=0` removes the recording pauses so the test
    isn't gated on sleep durations.
    """
    if not SCRIPT.exists():
        pytest.fail(f"missing {SCRIPT}")
    if shutil.which("bash") is None:
        pytest.skip("bash not available")

    env = dict(os.environ)
    env["CAPTURE_PACE_SECONDS"] = "0"
    # Ensure `python` and `pytest` resolve via the venv pytest is
    # running under — capture_demo.sh shells out to both.
    venv_bin = Path(sys.executable).parent
    env["PATH"] = f"{venv_bin}:{env.get('PATH', '')}"

    result = subprocess.run(
        ["bash", str(SCRIPT)],
        cwd=REPO_ROOT,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, (
        f"capture_demo.sh exited {result.returncode}\n"
        f"--- stdout ---\n{result.stdout}\n--- stderr ---\n{result.stderr}"
    )
    return result.stdout


def test_surface_1_pytest_passes(capture_output: str) -> None:
    assert "1/2 · pytest" in capture_output
    # pytest -q prints "N passed in Xs" on success; the regex tolerates
    # "passed" with optional skipped/warnings counts in between.
    assert re.search(r"\d+ passed", capture_output), (
        "pytest summary line missing — capture should show a passing run"
    )
    # No failed tests in the embedded pytest run.
    assert " failed" not in capture_output, "embedded pytest run had failures"


def test_surface_2_bench_renders_table_with_every_pipeline(capture_output: str) -> None:
    assert "2/2 · bench_1000_doc.py" in capture_output
    # Rendered markdown table header (same shape as docs/benchmarks.md;
    # locked by test_bench_table_snapshot.py elsewhere).
    expected_header = "| pipeline | duration (s) | docs/s | speedup vs serial |"
    assert expected_header in capture_output, (
        "bench markdown header drifted; test_bench_table_snapshot.py and this test must agree"
    )
    # One data row per pipeline — proves serial/async/async+batched
    # all wired through the bench script end-to-end.
    for pipeline in PIPELINES:
        # Markdown table cell anchors: leading "| <name> |".
        assert f"| {pipeline} |" in capture_output, (
            f"missing {pipeline!r} data row in rendered bench table"
        )


def test_capture_demo_script_exists_and_is_executable() -> None:
    assert SCRIPT.exists(), f"missing {SCRIPT}"
    assert os.access(SCRIPT, os.X_OK), f"{SCRIPT} should be executable"


# --- #142: the interpreter stage 1 runs on, and the closing narration ---------
# These live in this file because the capture's own pytest run ignores it; a
# test that runs the capture from any other file would recurse.


@pytest.mark.skipif(shutil.which("bash") is None, reason="bash not available")
def test_stage_1_never_runs_a_pytest_from_path(tmp_path: Path) -> None:
    """Stage 1 ran bare `pytest`: Homebrew's here, not the venv's (#142)."""
    marker = "STALE-PYTEST-FROM-PATH"
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    fake = fake_bin / "pytest"
    fake.write_text(f"#!/bin/sh\necho {marker}\nexit 0\n")
    fake.chmod(0o755)
    env = dict(os.environ)
    env["CAPTURE_PACE_SECONDS"] = "0"
    # The fake first; this test's interpreter next, so `python` resolves
    # without a .venv (CI).
    env["PATH"] = os.pathsep.join([str(fake_bin), str(Path(sys.executable).parent), env["PATH"]])
    r = subprocess.run(  # noqa: S603
        ["bash", str(SCRIPT)],
        cwd=REPO_ROOT,
        env=env,
        capture_output=True,
        text=True,
        timeout=600,
        check=False,
    )
    assert r.returncode == 0, r.stderr[-1500:]
    assert marker not in r.stdout + r.stderr, "stage 1 ran the pytest on PATH"
    stage1 = r.stdout.split("1/2 ·", 1)[1].split("2/2 ·", 1)[0]
    assert re.search(r"\d+ passed", stage1), "stage 1 printed no pytest summary"


def test_the_closing_narration_quotes_no_measured_ratio() -> None:
    """It said "30x is the upper bound" beneath a 190x batched row (#142).

    The one number allowed is the spec's own "5-20x" range, which
    `test_real_api_claim_direction.py` requires the passage to name.
    """
    narration = "\n".join(
        line
        for line in SCRIPT.read_text(encoding="utf-8").splitlines()
        if line.startswith("printf")
    )
    tail = narration[narration.index("expect LOWER") :]
    without_spec = re.sub(r"5\s*[-–]\s*20\s*[x×]", "", tail)
    assert not re.search(r"\d+(\.\d+)?\s*[x×]", without_spec), tail
