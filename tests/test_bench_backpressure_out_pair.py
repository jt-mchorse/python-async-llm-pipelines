"""`bench_backpressure` resolves `--out-md`/`--out-json` as one pair (#238).

The two flags defaulted separately to the committed `docs/backpressure.{md,json}`,
so overriding only one still rewrote the other committed artifact. Measured on
`main` ef71f03, from a scratch dir holding a copy of `docs/`: `--n 50
--queue-size 4 --out-md <tmp>/explore.md` wrote `docs/backpressure.json` (one
row, n=50) beside the untouched three-row `docs/backpressure.md`, exit 0; with
only `--out-json`, `docs/backpressure.md` was rewritten. The committed pair then
came from two runs, which #111 and the README's "comes from one run" rule out.

Now a lone flag places the other beside it (by `bench_1000_doc._json_path_for`'s
suffix rule), and a lone flag that names no file is refused before the run.

The end-to-end arms run the script as a subprocess from a scratch cwd holding a
copy of `docs/`, so the relative defaults land there and never in the checkout.
"""

from __future__ import annotations

import json
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
BENCH_PY = ROOT / "scripts" / "bench_backpressure.py"
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts import bench_backpressure  # noqa: E402


def _scratch(tmp_path: Path) -> tuple[Path, dict[str, bytes]]:
    cwd = tmp_path / "cwd"
    cwd.mkdir()
    shutil.copytree(ROOT / "docs", cwd / "docs")
    committed = {
        name: (cwd / "docs" / name).read_bytes()
        for name in ("backpressure.md", "backpressure.json")
    }
    return cwd, committed


def _run(cwd: Path, *flags: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, str(BENCH_PY), "--n", "12", "--queue-size", "4", "--consumer-ms", "0"]
        + list(flags),
        cwd=cwd,
        capture_output=True,
        text=True,
        check=False,
        timeout=60,
    )


def _changed(cwd: Path, committed: dict[str, bytes]) -> list[str]:
    return sorted(n for n, data in committed.items() if (cwd / "docs" / n).read_bytes() != data)


@pytest.mark.parametrize(
    ("flag", "given", "derived"),
    [
        ("--out-md", "explore.md", "explore.json"),
        ("--out-json", "explore.json", "explore.md"),
    ],
)
def test_one_flag_writes_its_pair_beside_it_and_not_the_committed_one(
    flag: str, given: str, derived: str, tmp_path: Path
) -> None:
    cwd, committed = _scratch(tmp_path)
    out = tmp_path / "out"
    out.mkdir()
    proc = _run(cwd, flag, str(out / given))
    assert proc.returncode == 0, proc.stderr
    assert _changed(cwd, committed) == [], (
        f"only {flag} was given, and the committed pair was rewritten:\n{proc.stdout}"
    )
    assert sorted(p.name for p in out.iterdir()) == sorted([given, derived])
    # The two halves are one run: the markdown table is this JSON's rows.
    md_path = out / ("explore.md" if given.endswith(".md") else derived)
    json_path = out / ("explore.json" if given.endswith(".json") else derived)
    rows = json.loads(json_path.read_text(encoding="utf-8"))["results"]
    assert [(r["n"], r["queue_size"]) for r in rows] == [(12, 4)]
    assert "| 12 | 4 |" in md_path.read_text(encoding="utf-8")


@pytest.mark.parametrize(
    ("md", "js", "expected"),
    [
        # Neither given: the committed pair (#132).
        (None, None, ("docs/backpressure.md", "docs/backpressure.json")),
        # Both given: as given, including an empty string that skips a half.
        ("a.md", "b.json", ("a.md", "b.json")),
        ("", "b.json", ("", "b.json")),
        # One given: the other beside it, suffix swapped when it is the flag's own...
        ("/tmp/x/bp.md", None, ("/tmp/x/bp.md", "/tmp/x/bp.json")),
        (None, "/tmp/x/bp.json", ("/tmp/x/bp.md", "/tmp/x/bp.json")),
        # ...appended otherwise, so `run.a` and `run.b` do not share one (#118),
        # and a `.json` --out-md does not land on itself.
        ("run.a", None, ("run.a", "run.a.json")),
        ("bp.json", None, ("bp.json", "bp.json.json")),
        (None, "bp.md", ("bp.md.md", "bp.md")),
        (None, "report", ("report.md", "report")),
        # A flag spelled as the committed default is still a given flag.
        ("docs/backpressure.md", None, ("docs/backpressure.md", "docs/backpressure.json")),
    ],
)
def test_the_pair_resolution_table(
    md: str | None, js: str | None, expected: tuple[str, str]
) -> None:
    got = bench_backpressure._resolve_out_paths(md, js)
    assert tuple(str(Path(p)) if p else p for p in got) == tuple(
        str(Path(p)) if p else p for p in expected
    )


@pytest.mark.parametrize("flag", ["--out-md", "--out-json"])
@pytest.mark.parametrize("value", ["", "."])
def test_a_lone_flag_that_names_no_file_is_refused_before_the_run(
    flag: str, value: str, tmp_path: Path
) -> None:
    # `--out-md ''` alone used to skip the markdown and still rewrite the
    # committed JSON. There is no "beside" a path with no name.
    cwd, committed = _scratch(tmp_path)
    proc = _run(cwd, flag, value)
    assert proc.returncode == 2, proc.stderr
    assert "Traceback" not in proc.stderr, proc.stderr
    missing = "--out-json" if flag == "--out-md" else "--out-md"
    lines = proc.stderr.strip().splitlines()
    assert len(lines) == 1, proc.stderr
    assert lines[0].startswith("could not write report: "), lines[0]
    assert flag in lines[0], lines[0]
    assert missing in lines[0], lines[0]
    assert "n=" not in proc.stdout, "refused after the run, not before it"
    assert _changed(cwd, committed) == []


def test_control_neither_flag_still_regenerates_the_committed_pair(tmp_path: Path) -> None:
    # The README's provenance command relies on this default (#132).
    cwd, committed = _scratch(tmp_path)
    proc = _run(cwd)
    assert proc.returncode == 0, proc.stderr
    assert _changed(cwd, committed) == ["backpressure.json", "backpressure.md"]


def test_control_both_flags_write_exactly_where_given(tmp_path: Path) -> None:
    cwd, committed = _scratch(tmp_path)
    proc = _run(cwd, "--out-md", str(tmp_path / "a.md"), "--out-json", str(tmp_path / "b.json"))
    assert proc.returncode == 0, proc.stderr
    assert (tmp_path / "a.md").is_file()
    assert (tmp_path / "b.json").is_file()
    assert _changed(cwd, committed) == []
