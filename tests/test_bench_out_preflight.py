"""The bench scripts check every output path before they run (#167).

Both translated an unwritable path to exit 2, but only once the benchmark had
run, and `bench_1000_doc` replaced its markdown before it found the JSON path
unwritable. Measured on main: `--n 200 --out <file>/benchmarks.md` ran 9 s
before refusing; with `b.json` a directory, `--n 1 --out b.md` printed
`benchmarks wrote .../b.md` and exited 2 with no raw results.
"""

from __future__ import annotations

import asyncio
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts import bench_1000_doc, bench_backpressure  # noqa: E402

RUNS: list[str] = []


@pytest.fixture(autouse=True)
def _count_runs(monkeypatch: pytest.MonkeyPatch) -> None:
    RUNS.clear()
    real_all, real_one = bench_1000_doc._run_all, bench_backpressure.run_one

    async def run_all(workload):  # type: ignore[no-untyped-def]
        RUNS.append("1000_doc")
        return await real_all(workload)

    async def run_one(**kw):  # type: ignore[no-untyped-def]
        RUNS.append("backpressure")
        return await real_one(**kw)

    monkeypatch.setattr(bench_1000_doc, "_run_all", run_all)
    monkeypatch.setattr(bench_backpressure, "run_one", run_one)


def _doc(out: Path) -> int:
    args = bench_1000_doc.build_arg_parser().parse_args(
        ["--n", "1", "--latency", "0", "--out", str(out)]
    )
    return asyncio.run(bench_1000_doc.amain(args))


def _bp(*flags: str) -> int:
    args = bench_backpressure._build_parser().parse_args(["--n", "4", "--consumer-ms", "0", *flags])
    return asyncio.run(bench_backpressure.main_async(args))


def _blocker(tmp_path: Path) -> Path:
    f = tmp_path / "afile"
    f.write_text("x", encoding="utf-8")
    return f


def test_bench_1000_doc_refuses_a_bad_md_path_before_running(tmp_path: Path) -> None:
    assert _doc(_blocker(tmp_path) / "benchmarks.md") == 2
    assert RUNS == []


def test_bench_1000_doc_refuses_a_bad_json_path_before_writing_the_md(tmp_path: Path) -> None:
    (tmp_path / "b.json").mkdir()
    assert _doc(tmp_path / "b.md") == 2
    assert RUNS == []
    assert not (tmp_path / "b.md").exists()


def test_bench_1000_doc_still_writes_both(tmp_path: Path) -> None:
    assert _doc(tmp_path / "out" / "b.md") == 0
    assert RUNS == ["1000_doc"]
    assert (tmp_path / "out" / "b.md").is_file()
    assert (tmp_path / "out" / "b.json").is_file()


@pytest.mark.parametrize("bad", ["--out-md", "--out-json"])
def test_bench_backpressure_refuses_a_bad_path_before_running(tmp_path: Path, bad: str) -> None:
    # Both flags always point into tmp_path: their defaults are the committed
    # docs/backpressure.{md,json}, which a run on the unfixed tree would rewrite.
    paths = {"--out-md": str(tmp_path / "bp.md"), "--out-json": str(tmp_path / "bp.json")}
    paths[bad] = str(_blocker(tmp_path) / "x")
    assert _bp(*[t for kv in paths.items() for t in kv]) == 2
    assert RUNS == []
    assert not (tmp_path / "bp.md").exists()
    assert not (tmp_path / "bp.json").exists()


def test_bench_backpressure_still_writes(tmp_path: Path) -> None:
    assert _bp("--out-md", str(tmp_path / "bp.md"), "--out-json", str(tmp_path / "bp.json")) == 0
    assert RUNS
    assert (tmp_path / "bp.md").is_file()
    assert (tmp_path / "bp.json").is_file()


def test_check_writable_leaves_nothing_behind(tmp_path: Path) -> None:
    from async_pipelines.io_utils import check_writable

    check_writable(tmp_path / "sub" / "r.md")
    assert list((tmp_path / "sub").iterdir()) == []
    with pytest.raises(IsADirectoryError):
        check_writable(tmp_path)
