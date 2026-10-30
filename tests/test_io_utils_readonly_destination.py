"""`atomic_write_text` and `check_writable` refuse a read-only existing output (#246).

`os.replace` swaps the directory entry, so the target's own write bit was never
consulted and a `chmod 444` file was silently rewritten, mode still 0444, while
`Path.write_text`, the call the helper replaced, raises `PermissionError`. The
bench scripts' write seams name a read-only path among the unwritable outputs
they turn into exit 2, and `check_writable` approved it, so the run went ahead
and then overwrote the file. Helper cases run both writers on the same layout,
so the lock is parity with `Path.write_text`.

Root bypasses permission bits (`Path.write_text` succeeds too), so these skip
as root. GitHub's ubuntu runners run as the non-root `runner` user, so CI
exercises them.
"""

from __future__ import annotations

import asyncio
import errno
import os
import stat
import sys
from collections.abc import Iterator
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from async_pipelines.io_utils import atomic_write_text, check_writable  # noqa: E402
from scripts import bench_1000_doc, bench_backpressure  # noqa: E402

pytestmark = [
    pytest.mark.skipif(sys.platform == "win32", reason="POSIX permission bits"),
    pytest.mark.skipif(
        hasattr(os, "geteuid") and os.geteuid() == 0,
        reason="root bypasses file permission bits, so a 0444 file stays writable",
    ),
]


def _write(writer: str, path: Path, text: str) -> None:
    if writer == "atomic":
        atomic_write_text(path, text)
    else:
        path.write_text(text)


def _mode(path: Path) -> int:
    return stat.S_IMODE(os.stat(path).st_mode)


@pytest.fixture
def frozen(tmp_path: Path) -> Iterator[Path]:
    """A 0444 file holding `old`; made writable again for tmp_path cleanup."""
    path = tmp_path / "r.md"
    path.write_text("old\n")
    path.chmod(0o444)
    yield path
    path.chmod(0o644)


@pytest.mark.parametrize("writer", ["atomic", "write_text"])
def test_readonly_file_is_refused(frozen: Path, writer: str) -> None:
    with pytest.raises(PermissionError) as excinfo:
        _write(writer, frozen, "new\n")
    assert excinfo.value.errno == errno.EACCES
    assert frozen.read_text() == "old\n"
    assert _mode(frozen) == 0o444


def test_refusal_leaves_no_temp_file(frozen: Path) -> None:
    with pytest.raises(PermissionError):
        atomic_write_text(frozen, "new\n")
    assert sorted(p.name for p in frozen.parent.iterdir()) == ["r.md"]


@pytest.mark.parametrize("writer", ["atomic", "write_text"])
def test_readonly_file_behind_a_symlink_is_refused(tmp_path: Path, writer: str) -> None:
    # The check runs on the RESOLVED file (#157): a link name over a read-only
    # file must not get through.
    real = tmp_path / "real.md"
    real.write_text("old\n")
    real.chmod(0o444)
    link = tmp_path / "link.md"
    link.symlink_to(real)
    try:
        with pytest.raises(PermissionError):
            _write(writer, link, "new\n")
        assert real.read_text() == "old\n"
        assert link.is_symlink()
    finally:
        real.chmod(0o644)


@pytest.mark.parametrize("writer", ["atomic", "write_text"])
def test_writable_and_missing_targets_still_write(tmp_path: Path, writer: str) -> None:
    # Controls: the refusal is scoped to an existing unwritable file.
    owner_only = tmp_path / "owner.md"
    owner_only.write_text("old\n")
    owner_only.chmod(0o600)
    _write(writer, owner_only, "new\n")
    assert owner_only.read_text() == "new\n"
    assert _mode(owner_only) == 0o600

    missing = tmp_path / "new.md"
    _write(writer, missing, "fresh\n")
    assert missing.read_text() == "fresh\n"


def test_check_writable_refuses_what_the_writer_refuses(frozen: Path) -> None:
    with pytest.raises(PermissionError) as excinfo:
        check_writable(frozen)
    assert excinfo.value.errno == errno.EACCES
    assert sorted(p.name for p in frozen.parent.iterdir()) == ["r.md"]


def test_check_writable_still_passes_writable_and_missing(tmp_path: Path) -> None:
    # Controls: the pre-flight still approves what the writer accepts.
    owner_only = tmp_path / "owner.md"
    owner_only.write_text("old\n")
    owner_only.chmod(0o600)
    check_writable(owner_only)
    check_writable(tmp_path / "new.md")
    assert sorted(p.name for p in tmp_path.iterdir()) == ["owner.md"]


@pytest.mark.parametrize("frozen_name", ["b.md", "b.json"])
def test_bench_1000_doc_refuses_a_readonly_out_before_running(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    frozen_name: str,
) -> None:
    # `b.json` is the derived raw-results path: a read-only one must stop the
    # run before the markdown is written, not after (vector-search-at-scale#301).
    ran: list[str] = []

    async def run_all(workload):  # type: ignore[no-untyped-def]
        ran.append("run")
        return []

    monkeypatch.setattr(bench_1000_doc, "_run_all", run_all)
    ro = tmp_path / frozen_name
    ro.write_text("old\n")
    ro.chmod(0o444)
    try:
        args = bench_1000_doc.build_arg_parser().parse_args(
            ["--n", "1", "--latency", "0", "--out", str(tmp_path / "b.md")]
        )
        assert asyncio.run(bench_1000_doc.amain(args)) == 2
        assert ran == []
        assert ro.read_text() == "old\n"
        assert sorted(p.name for p in tmp_path.iterdir()) == [frozen_name]
        assert "could not write report" in capsys.readouterr().err
    finally:
        ro.chmod(0o644)


@pytest.mark.parametrize("frozen_flag", ["--out-md", "--out-json"])
def test_bench_backpressure_refuses_a_readonly_out_before_running(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, frozen_flag: str
) -> None:
    ran: list[str] = []

    async def run_one(**kw):  # type: ignore[no-untyped-def]
        ran.append("run")
        raise AssertionError("the benchmark ran")

    monkeypatch.setattr(bench_backpressure, "run_one", run_one)
    # Both flags always point into tmp_path: their defaults are the committed
    # docs/backpressure.{md,json}.
    paths = {"--out-md": tmp_path / "bp.md", "--out-json": tmp_path / "bp.json"}
    ro = paths[frozen_flag]
    ro.write_text("old\n")
    ro.chmod(0o444)
    try:
        flags = [t for k, v in paths.items() for t in (k, str(v))]
        args = bench_backpressure._build_parser().parse_args(
            ["--n", "4", "--consumer-ms", "0", *flags]
        )
        assert asyncio.run(bench_backpressure.main_async(args)) == 2
        assert ran == []
        assert ro.read_text() == "old\n"
        assert sorted(p.name for p in tmp_path.iterdir()) == [ro.name]
    finally:
        ro.chmod(0o644)
