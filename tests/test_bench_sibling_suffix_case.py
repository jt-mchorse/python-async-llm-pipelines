"""A lone output flag's sibling matches the suffix ignoring case (#244).

#241's lone-flag rule (`bench_backpressure._sibling`) and `bench_1000_doc`'s
`_json_path_for` swapped the suffix only on an exact `== ".md"` / `== ".json"`,
and appended otherwise. Measured on `main` 13298fa, macOS, from a scratch dir
holding a copy of `docs/`: `bench_backpressure.py --out-md docs/backpressure.MD`
wrote `backpressure.MD` and `backpressure.MD.json`, exit 0. On a
case-insensitive filesystem `backpressure.MD` *is* the committed
`backpressure.md`, so the report was rewritten and the committed JSON kept the
previous run's rows; `bench_1000_doc.py --out docs/benchmarks.MD` did the same
to `benchmarks.md`. `same_file` cannot catch it, because the two outputs really
are different files. Both now go through `io_utils.sibling_path`, which lowers
the suffix before comparing.

The derivation arms run everywhere. The end-to-end arms need a filesystem that
ignores case; whether the scratch dir is one is probed, not assumed, so they
run on macOS and skip on Linux CI with the reason.
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

# --- the derivation, everywhere ---------------------------------------------


@pytest.mark.parametrize(
    ("path", "own", "other", "want"),
    [
        ("d/bp.md", ".md", ".json", "d/bp.json"),
        ("d/bp.MD", ".md", ".json", "d/bp.json"),
        ("d/bp.Md", ".md", ".json", "d/bp.json"),
        ("d/bp.mD", ".md", ".json", "d/bp.json"),
        ("d/bp.json", ".json", ".md", "d/bp.md"),
        ("d/bp.JSON", ".json", ".md", "d/bp.md"),
        ("d/bp.Json", ".json", ".md", "d/bp.md"),
        # Not the flag's own suffix in any case: append, as #118 requires.
        ("d/run.a", ".md", ".json", "d/run.a.json"),
        ("d/bp.mdx", ".md", ".json", "d/bp.mdx.json"),
        ("d/bp.JSON", ".md", ".json", "d/bp.JSON.json"),
        ("d/report", ".md", ".json", "d/report.json"),
    ],
)
def test_sibling_path_swaps_its_own_suffix_in_any_case(
    path: str, own: str, other: str, want: str
) -> None:
    assert sibling_path(path, own, other) == Path(want)


@pytest.mark.parametrize(
    ("out", "json_name"),
    [
        ("benchmarks.MD", "benchmarks.json"),
        ("benchmarks.Md", "benchmarks.json"),
        ("report.v1.MD", "report.v1.json"),
        ("foo.JSON", "foo.JSON.json"),  # never its own report, in any case
    ],
)
def test_bench_1000_doc_json_path_ignores_the_suffix_case(out: str, json_name: str) -> None:
    assert _json_path_for(Path("d") / out) == Path("d") / json_name


@pytest.mark.parametrize(
    ("md", "js", "want"),
    [
        ("docs/backpressure.MD", None, ("docs/backpressure.MD", "docs/backpressure.json")),
        ("docs/backpressure.Md", None, ("docs/backpressure.Md", "docs/backpressure.json")),
        (None, "docs/backpressure.JSON", ("docs/backpressure.md", "docs/backpressure.JSON")),
        (None, "docs/backpressure.Json", ("docs/backpressure.md", "docs/backpressure.Json")),
    ],
)
def test_bench_backpressure_lone_flag_ignores_the_suffix_case(
    md: str | None, js: str | None, want: tuple[str, str]
) -> None:
    assert bench_backpressure._resolve_out_paths(md, js) == want


# --- the real alias, where the filesystem ignores case ----------------------


def _case_insensitive(directory: Path) -> bool:
    probe = directory / "case-probe"
    probe.touch()
    try:
        return (directory / "CASE-PROBE").exists()
    finally:
        probe.unlink()


@pytest.fixture
def scratch(tmp_path: Path) -> Path:
    if not _case_insensitive(tmp_path):
        pytest.skip("needs a case-insensitive filesystem (macOS default); this one is not")
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


def _assert_whole_pair_rewritten(docs: Path, before: dict[str, bytes]) -> None:
    changed = sorted(n for n, data in before.items() if (docs / n).read_bytes() != data)
    assert changed == sorted(before), (
        f"only {changed} of the committed pair changed; the pair now comes from two runs"
    )
    stray = sorted(p.name for p in docs.iterdir() if p.name.lower().count(".") > 1)
    assert stray == [], f"a lone flag derived a second, uncommitted file: {stray}"


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
def test_bench_backpressure_case_alias_keeps_the_committed_pair_together(
    scratch: Path, flag: str, given: str
) -> None:
    before = _snapshot(scratch / "docs", _BP)
    proc = _run(scratch, "bench_backpressure.py", *_BP_ARGS, flag, given)
    assert proc.returncode == 0, proc.stderr
    _assert_whole_pair_rewritten(scratch / "docs", before)


@pytest.mark.parametrize("given", ["docs/benchmarks.MD", "docs/benchmarks.Md"])
def test_bench_1000_doc_case_alias_keeps_the_committed_pair_together(
    scratch: Path, given: str
) -> None:
    before = _snapshot(scratch / "docs", ("benchmarks.md", "benchmarks.json"))
    proc = _run(scratch, "bench_1000_doc.py", "--n", "8", "--latency", "0.001", "--out", given)
    assert proc.returncode == 0, proc.stderr
    _assert_whole_pair_rewritten(scratch / "docs", before)
