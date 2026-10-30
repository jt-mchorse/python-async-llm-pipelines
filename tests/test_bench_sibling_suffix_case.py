"""A lone output flag's case-variant suffix follows the filesystem (#244, #248).

#241's lone-flag rule (`bench_backpressure._sibling`) and `bench_1000_doc`'s
`_json_path_for` swapped the suffix only on an exact `== ".md"` / `== ".json"`,
and appended otherwise. Measured on `main` 13298fa, macOS, from a scratch dir
holding a copy of `docs/`: `bench_backpressure.py --out-md docs/backpressure.MD`
wrote `backpressure.MD` and `backpressure.MD.json`, exit 0. On a
case-insensitive filesystem `backpressure.MD` *is* the committed
`backpressure.md`, so the report was rewritten and the committed JSON kept the
previous run's rows; `bench_1000_doc.py --out docs/benchmarks.MD` did the same
to `benchmarks.md` (#244).

#245 fixed that by lowering the suffix everywhere, and so broke the other
filesystem (#248). Measured on `main` a5392ff, a case-sensitive APFS volume:
`--out-md docs/backpressure.MD` rewrote the committed `backpressure.json`,
`--out-json docs/backpressure.JSON` the committed `backpressure.md`, and
`bench_1000_doc --out docs/benchmarks.MD` the committed `benchmarks.json`, each
at exit 0, because there `.MD` is a new file and the swap pointed its sibling
at the committed one. `io_utils.sibling_path` now asks the filesystem whether
the case-variant name aliases its own-suffix spelling, and swaps only if so.

So the right answer depends on the filesystem, and no arm hard-codes it: each
probes the directory it works in. CI (Linux tmp) exercises the case-sensitive
answer; macOS's default exercises the other. Running the file with
`--basetemp` on a case-sensitive volume exercises the first on a Mac.
"""

from __future__ import annotations

import shutil
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
SCRIPTS = ROOT / "scripts"
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

from bench_1000_doc import _json_path_for  # noqa: E402

from async_pipelines.io_utils import sibling_path  # noqa: E402
from scripts import bench_backpressure  # noqa: E402


def _case_insensitive(directory: Path) -> bool:
    probe = directory / "case-probe"
    probe.touch()
    try:
        return (directory / "CASE-PROBE").exists()
    finally:
        probe.unlink()


def _expect(d: Path, name: str, own: str, other: str) -> Path:
    """What a case-variant *name* in *d* should derive: swap where it aliases, else append."""
    p = d / name
    if _case_insensitive(d):
        return p.with_suffix(other)
    return d / (name + other)


# --- the derivation -----------------------------------------------------------


@pytest.mark.parametrize(
    ("name", "own", "other", "want"),
    [
        # The exact suffix swaps on every filesystem.
        ("bp.md", ".md", ".json", "bp.json"),
        ("bp.json", ".json", ".md", "bp.md"),
        # Not the flag's own suffix in any case: append, as #118 requires.
        ("run.a", ".md", ".json", "run.a.json"),
        ("bp.mdx", ".md", ".json", "bp.mdx.json"),
        ("bp.JSON", ".md", ".json", "bp.JSON.json"),
        ("report", ".md", ".json", "report.json"),
    ],
)
def test_sibling_path_rows_that_do_not_depend_on_the_filesystem(
    tmp_path: Path, name: str, own: str, other: str, want: str
) -> None:
    assert sibling_path(tmp_path / name, own, other) == tmp_path / want


_VARIANTS = [
    ("bp.MD", ".md", ".json"),
    ("bp.Md", ".md", ".json"),
    ("bp.mD", ".md", ".json"),
    ("bp.JSON", ".json", ".md"),
    ("bp.Json", ".json", ".md"),
]


@pytest.mark.parametrize(("name", "own", "other"), _VARIANTS)
def test_a_case_variant_suffix_swaps_only_where_it_aliases_in_an_empty_dir(
    tmp_path: Path, name: str, own: str, other: str
) -> None:
    assert sibling_path(tmp_path / name, own, other) == _expect(tmp_path, name, own, other)
    assert list(tmp_path.iterdir()) == [], "the alias probe left a file behind"


@pytest.mark.parametrize(("name", "own", "other"), _VARIANTS)
def test_a_case_variant_suffix_beside_the_committed_pair(
    tmp_path: Path, name: str, own: str, other: str
) -> None:
    """The issue's shape: the own-suffix file and its sibling already exist."""
    (tmp_path / ("bp" + own)).write_text("committed")
    (tmp_path / ("bp" + other)).write_text("committed")
    got = sibling_path(tmp_path / name, own, other)
    assert got == _expect(tmp_path, name, own, other)
    if not _case_insensitive(tmp_path):
        assert got.name != "bp" + other, "a new file's sibling must not be the committed one"


def test_a_case_variant_with_both_spellings_present_appends(tmp_path: Path) -> None:
    """Two real files (case-sensitive only): `bp.MD` is not `bp.md`, so it appends."""
    (tmp_path / "bp.md").write_text("committed")
    if _case_insensitive(tmp_path):
        pytest.skip("two spellings cannot coexist on a case-insensitive filesystem")
    (tmp_path / "bp.MD").write_text("mine")
    assert sibling_path(tmp_path / "bp.MD", ".md", ".json") == tmp_path / "bp.MD.json"


@pytest.mark.parametrize(("name", "own", "other"), _VARIANTS)
def test_a_case_variant_under_a_missing_dir_appends(
    tmp_path: Path, name: str, own: str, other: str
) -> None:
    """Nothing can be committed in a directory that does not exist; none is created."""
    d = tmp_path / "new"
    assert sibling_path(d / name, own, other) == d / (name + other)
    assert not d.exists()


@pytest.mark.parametrize("out", ["benchmarks.MD", "report.v1.Md"])
def test_bench_1000_doc_json_path_follows_the_filesystem(tmp_path: Path, out: str) -> None:
    assert _json_path_for(tmp_path / out) == _expect(tmp_path, out, ".md", ".json")


def test_bench_1000_doc_json_path_never_swaps_a_json_report(tmp_path: Path) -> None:
    assert _json_path_for(tmp_path / "foo.JSON") == tmp_path / "foo.JSON.json"


@pytest.mark.parametrize(
    ("flag", "name"),
    [
        ("--out-md", "backpressure.MD"),
        ("--out-md", "backpressure.Md"),
        ("--out-json", "backpressure.JSON"),
        ("--out-json", "backpressure.Json"),
    ],
)
def test_bench_backpressure_lone_flag_follows_the_filesystem(
    tmp_path: Path, flag: str, name: str
) -> None:
    given = str(tmp_path / name)
    if flag == "--out-md":
        want = (given, str(_expect(tmp_path, name, ".md", ".json")))
        assert bench_backpressure._resolve_out_paths(given, None) == want
    else:
        want = (str(_expect(tmp_path, name, ".json", ".md")), given)
        assert bench_backpressure._resolve_out_paths(None, given) == want


# --- end to end, on whichever filesystem the tmp dir is ---------------------


@pytest.fixture
def scratch(tmp_path: Path) -> Path:
    cwd = tmp_path / "cwd"
    cwd.mkdir()
    shutil.copytree(ROOT / "docs", cwd / "docs")
    return cwd


def _run(cwd: Path, script: str, *flags: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, str(SCRIPTS / script), *flags],
        cwd=cwd,
        capture_output=True,
        text=True,
        check=False,
        timeout=60,
    )


def _snapshot(docs: Path, names: tuple[str, str]) -> dict[str, bytes]:
    return {n: (docs / n).read_bytes() for n in names}


def _assert_pair_consistent(docs: Path, before: dict[str, bytes], given: str) -> None:
    changed = sorted(n for n, data in before.items() if (docs / n).read_bytes() != data)
    names = sorted(p.name for p in docs.iterdir())
    if _case_insensitive(docs):
        # #244: the name *is* the committed file, so the whole pair is this run's.
        assert changed == sorted(before), (
            f"only {changed} of the committed pair changed; the pair now comes from two runs"
        )
        stray = [n for n in names if n.lower().count(".") > 1]
        assert stray == [], f"a lone flag derived a second, uncommitted file: {stray}"
    else:
        # #248: the name is a new file, so nothing committed may change, and its
        # sibling is appended beside it.
        assert changed == [], f"a lone flag to a new file rewrote committed {changed}"
        other = ".json" if Path(given).suffix.lower() == ".md" else ".md"
        assert Path(given).name in names
        assert Path(given).name + other in names


_BP = ("backpressure.md", "backpressure.json")
_BP_ARGS = ("--n", "12", "--queue-size", "4", "--consumer-ms", "0")


@pytest.mark.parametrize(
    ("flag", "given"),
    [
        ("--out-md", "docs/backpressure.MD"),
        ("--out-md", "docs/backpressure.Md"),
        ("--out-json", "docs/backpressure.JSON"),
    ],
)
def test_bench_backpressure_case_variant_keeps_the_committed_pair_consistent(
    scratch: Path, flag: str, given: str
) -> None:
    before = _snapshot(scratch / "docs", _BP)
    proc = _run(scratch, "bench_backpressure.py", *_BP_ARGS, flag, given)
    assert proc.returncode == 0, proc.stderr
    _assert_pair_consistent(scratch / "docs", before, given)


@pytest.mark.parametrize("given", ["docs/benchmarks.MD", "docs/benchmarks.Md"])
def test_bench_1000_doc_case_variant_keeps_the_committed_pair_consistent(
    scratch: Path, given: str
) -> None:
    before = _snapshot(scratch / "docs", ("benchmarks.md", "benchmarks.json"))
    proc = _run(scratch, "bench_1000_doc.py", "--n", "8", "--latency", "0.001", "--out", given)
    assert proc.returncode == 0, proc.stderr
    _assert_pair_consistent(scratch / "docs", before, given)
