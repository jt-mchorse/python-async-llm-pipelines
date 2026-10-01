"""Suite-wide guard: no test may rewrite a committed artifact (#115).

`test_bench_backpressure_unencodable_out_md_has_no_traceback` ran the bench with
`--out-md` in `tmp_path` and no `--out-json`, so the JSON half defaulted to the
committed `docs/backpressure.json`. On ext4, which accepts the test's surrogate
byte in a filename, the run completed, and every CI run overwrote that artifact
with a fresh one-row measurement. On APFS the name is refused and the script
exits first, so it never reproduced on a Mac. Nothing noticed until an arm read
the JSON after that test had run.

The class is "a test wrote outside `tmp_path`", and a per-test fix only closes
the instance that was found. This snapshots every git-tracked file under `docs/`
before the session and fails the session if any of them changed.
"""

from __future__ import annotations

import subprocess
from collections.abc import Iterator
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parent.parent


def _tracked_docs() -> dict[str, bytes]:
    try:
        listed = subprocess.run(
            ["git", "ls-files", "docs"],
            cwd=_ROOT,
            capture_output=True,
            text=True,
            check=True,
        ).stdout.split()
    except (OSError, subprocess.CalledProcessError):
        return {}  # not a git checkout (an sdist); nothing to guard
    return {name: (_ROOT / name).read_bytes() for name in listed if (_ROOT / name).is_file()}


@pytest.fixture(scope="session", autouse=True)
def _committed_docs_are_untouched() -> Iterator[None]:
    before = _tracked_docs()
    yield
    after = _tracked_docs()
    changed = sorted(name for name, data in before.items() if after.get(name) != data)
    assert not changed, (
        f"the test session rewrote committed artifacts: {changed}. A test must write "
        f"only under tmp_path (#115)."
    )
