"""A "wrote <path>" line cannot end the run, whatever the locale (#186).

#104 let a non-UTF-8 `--out` name reach the write. On a filesystem that
accepts the byte (ext4), the write succeeded and the next line printed the
path. Whether that print can encode the lone surrogate depends on
`sys.stdout`'s error handler: `surrogateescape` under the C / C.UTF-8 locale CI
runs in, **strict** under `en_US.UTF-8` and every other UTF-8 locale. Under
strict, both scripts exited 1 with a traceback after the benchmark had run,
with the markdown written and no JSON -- the half pair #167 exists to prevent.

The children run with `PYTHONIOENCODING=utf-8:strict`, so the host's locale no
longer picks the verdict. APFS refuses the byte (EILSEQ) before anything is
written, so on a Mac the subprocess arms hold trivially; ext4 (CI) runs them for
real. The `display_path` arms hold everywhere.
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest

from async_pipelines.io_utils import display_path

_REPO_ROOT = Path(__file__).resolve().parents[1]

# What `surrogateescape` makes of the raw byte 0xFF; built from its codepoint.
SURROGATE = chr(0xDCFF)


def _run_strict(*args: str) -> tuple[int, str]:
    env = {**os.environ, "PYTHONIOENCODING": "utf-8:strict"}
    proc = subprocess.run(
        [sys.executable, *args],
        cwd=str(_REPO_ROOT),
        capture_output=True,
        check=False,
        env=env,
    )
    return proc.returncode, proc.stderr.decode("utf-8", errors="replace")


def _assert_whole_pair_or_nothing(rc: int, stderr: str, md: Path, js: Path) -> None:
    assert "Traceback" not in stderr, stderr
    assert md.exists() == js.exists(), (md.exists(), js.exists(), stderr)
    assert rc == (0 if md.exists() else 2), (rc, stderr)


def test_bench_1000_doc_strict_stdout_unencodable_out(tmp_path: Path) -> None:
    md = tmp_path / ("bench" + SURROGATE + ".md")
    rc, stderr = _run_strict(
        "scripts/bench_1000_doc.py",
        *("--n", "4", "--latency", "0.001", "--concurrency", "2", "--batch-size", "2"),
        *("--out", str(md)),
    )
    _assert_whole_pair_or_nothing(rc, stderr, md, md.with_suffix(".json"))


def test_bench_backpressure_strict_stdout_unencodable_out(tmp_path: Path) -> None:
    md = tmp_path / ("bp" + SURROGATE + ".md")
    js = tmp_path / ("bp" + SURROGATE + ".json")
    rc, stderr = _run_strict(
        "scripts/bench_backpressure.py",
        *("--n", "20", "--queue-size", "2", "--consumer-ms", "0"),
        *("--out-md", str(md), "--out-json", str(js)),
    )
    _assert_whole_pair_or_nothing(rc, stderr, md, js)


def test_bench_1000_doc_strict_stdout_ordinary_out_still_prints_the_path(tmp_path: Path) -> None:
    # Control: the strict child is a working harness, and a valid UTF-8 name
    # (multibyte included) is printed exactly as given.
    md = tmp_path / "bench-é.md"
    proc = subprocess.run(
        [
            sys.executable,
            "scripts/bench_1000_doc.py",
            *("--n", "4", "--latency", "0.001", "--concurrency", "2", "--batch-size", "2"),
            *("--out", str(md)),
        ],
        cwd=str(_REPO_ROOT),
        capture_output=True,
        check=False,
        env={**os.environ, "PYTHONIOENCODING": "utf-8:strict"},
    )
    assert proc.returncode == 0, proc.stderr
    out = proc.stdout.decode("utf-8")
    assert f"benchmarks wrote {md}" in out
    assert f"raw results wrote {md.with_suffix('.json')}" in out


@pytest.mark.parametrize(
    "path",
    ["docs/benchmarks.md", "bench-é.md", "/tmp/日本.json", Path("a/b.md")],
)
def test_display_path_is_unchanged_for_utf8_names(path: str | Path) -> None:
    assert display_path(path) == str(path)


@pytest.mark.parametrize("path", ["bench" + SURROGATE + ".md", Path("d") / ("x" + SURROGATE)])
def test_display_path_of_an_undecodable_byte_encodes_strictly(path: str | Path) -> None:
    shown = display_path(path)
    shown.encode("utf-8")  # strict: what a non-C UTF-8 locale's stdout does
    assert "\\xff" in shown
