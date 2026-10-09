"""`atomic_write_text` writes THROUGH a symlinked destination (#157).

`os.replace` renames onto the link itself, so a symlinked destination used to
become a regular file while the file it pointed at kept its old contents.
`Path.write_text`, which this helper replaced and whose behaviour #124 restored
for file mode, writes through the link. Each case below is compared with
`Path.write_text` on an identical layout rather than with a hand-written
expectation, so the lock is parity and not a guess at it.
"""

from __future__ import annotations

import importlib.util
import os
import stat
import sys
from pathlib import Path

import pytest

from async_pipelines.io_utils import atomic_write_text

pytestmark = pytest.mark.skipif(
    sys.platform == "win32", reason="symlink creation needs privileges on Windows"
)


def _layout(root: Path, *, absolute: bool) -> tuple[Path, Path]:
    real_dir = root / "real"
    real_dir.mkdir(parents=True)
    real = real_dir / "bench.md"
    real.write_text("old\n")
    real.chmod(0o640)
    link = root / "link.md"
    link.symlink_to(real if absolute else Path("real") / "bench.md")
    return link, real


@pytest.mark.parametrize("absolute", [False, True], ids=["relative-link", "absolute-link"])
@pytest.mark.parametrize("writer", ["atomic", "write_text"])
def test_write_goes_through_the_link(tmp_path: Path, absolute: bool, writer: str) -> None:
    link, real = _layout(tmp_path, absolute=absolute)
    if writer == "atomic":
        atomic_write_text(link, "new\n")
    else:
        link.write_text("new\n")

    assert link.is_symlink(), "the link was replaced by a regular file"
    assert real.read_text() == "new\n", "the linked file kept its old contents"
    assert link.read_text() == "new\n"
    assert stat.S_IMODE(os.stat(real).st_mode) == 0o640
    # No temp file left behind in either directory.
    assert sorted(p.name for p in tmp_path.iterdir()) == ["link.md", "real"]
    assert sorted(p.name for p in real.parent.iterdir()) == ["bench.md"]


@pytest.mark.parametrize("writer", ["atomic", "write_text"])
def test_dangling_link_creates_its_target(tmp_path: Path, writer: str) -> None:
    (tmp_path / "real").mkdir()
    real = tmp_path / "real" / "new.md"
    link = tmp_path / "link.md"
    link.symlink_to(Path("real") / "new.md")
    if writer == "atomic":
        atomic_write_text(link, "fresh\n")
    else:
        link.write_text("fresh\n")

    assert link.is_symlink()
    assert real.read_text() == "fresh\n"


def test_plain_destination_is_unchanged_behaviour(tmp_path: Path) -> None:
    dest = tmp_path / "plain.md"
    dest.write_text("old\n")
    atomic_write_text(dest, "new\n")
    assert not dest.is_symlink()
    assert dest.read_text() == "new\n"


def test_bench_out_through_a_link_updates_the_linked_report(tmp_path: Path) -> None:
    """End to end: `bench_1000_doc --out link.md` updates the file the link names."""
    script = Path(__file__).resolve().parents[1] / "scripts" / "bench_1000_doc.py"
    spec = importlib.util.spec_from_file_location("_bench_1000_doc_157", script)
    assert spec is not None
    assert spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)

    link, real = _layout(tmp_path, absolute=False)
    rc = mod.main(["--n", "4", "--latency", "0.0001", "--out", str(link)])
    assert rc == 0
    assert link.is_symlink()
    assert real.read_text().startswith("# Async-pipeline benchmarks")
