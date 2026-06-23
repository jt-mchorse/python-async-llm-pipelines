"""Atomicity contract for `async_pipelines.io_utils.atomic_write_text` (issue #36).

Until this PR, four production write sites in this repo used
`Path.write_text` without atomicity guarantees: the markdown and JSON
artifacts written by `scripts/bench_1000_doc.py` (rendered into the
README's "Benchmark Results" section and consumed by downstream
plotting tooling) and the analogous pair in `scripts/bench_backpressure.py`.

A signal between the implicit `open(..., "w")` truncate and `close()`
flush leaves the destination zero-length or partial. README front-page
rendering and downstream plotting both fail loudly on partial JSON
(`JSONDecodeError`) but silently on partial markdown (the table just
ends mid-row).

This PR routes all four sites through a new public helper at
`async_pipelines.io_utils.atomic_write_text`, matching the portfolio
standard from the 2026-05-26 atomic-write arc.

What this file pins:

1. **Helper unit contract** (6 tests): happy path, parent-dir
   creation, overwrite, three load-bearing failure invariants —
   destination-absent on rename failure for new files, no leftover
   `.tmp` siblings, pre-existing-file-unchanged on overwrite failure.
2. **Per-script integration** (2 tests): `bench_1000_doc.main()` and
   `bench_backpressure._build_parser`+`amain` route through the
   helper, proven by monkey-patching `io_utils.os.replace` to raise
   and asserting the destination is untouched.
"""

from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path

import pytest

from async_pipelines import io_utils as io_utils_mod
from async_pipelines.io_utils import atomic_write_text

# ---------------------------------------------------------------------------
# Unit tests on the helper.
# ---------------------------------------------------------------------------


def test_atomic_write_text_happy_path(tmp_path: Path) -> None:
    out = tmp_path / "out.txt"
    atomic_write_text(out, "hello\nworld\n")
    assert out.read_text(encoding="utf-8") == "hello\nworld\n"


def test_atomic_write_text_creates_parent_dirs(tmp_path: Path) -> None:
    out = tmp_path / "deep" / "nested" / "x.json"
    assert not out.parent.exists()
    atomic_write_text(out, "{}")
    assert out.read_text(encoding="utf-8") == "{}"


def test_atomic_write_text_overwrites_existing_file(tmp_path: Path) -> None:
    out = tmp_path / "out.txt"
    out.write_text("STALE-CONTENT-MUST-NOT-SURVIVE", encoding="utf-8")
    atomic_write_text(out, "fresh")
    body = out.read_text(encoding="utf-8")
    assert body == "fresh"
    assert "STALE" not in body


def test_atomic_write_text_replace_failure_leaves_destination_absent(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    out = tmp_path / "result.json"

    def boom(*_args, **_kwargs):
        raise OSError("simulated mid-rename failure")

    monkeypatch.setattr(io_utils_mod.os, "replace", boom)
    with pytest.raises(OSError, match="simulated mid-rename failure"):
        atomic_write_text(out, '{"k": "v"}')

    assert not out.exists()


def test_atomic_write_text_replace_failure_cleans_up_tmp_files(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    out = tmp_path / "artifacts" / "delta.json"
    out.parent.mkdir(parents=True, exist_ok=True)

    def boom(*_args, **_kwargs):
        raise OSError("simulated mid-rename failure")

    monkeypatch.setattr(io_utils_mod.os, "replace", boom)
    with pytest.raises(OSError, match="simulated mid-rename failure"):
        atomic_write_text(out, '{"k": "v"}')

    siblings = list(out.parent.iterdir())
    assert siblings == [], f"expected no temp leftovers in {out.parent}, got {siblings}"


def test_atomic_write_text_destination_unchanged_when_overwriting_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The property `Path.write_text` could never offer."""
    out = tmp_path / "existing.json"
    out.write_text('{"keep": true}', encoding="utf-8")

    def boom(*_args, **_kwargs):
        raise OSError("simulated")

    monkeypatch.setattr(io_utils_mod.os, "replace", boom)
    with pytest.raises(OSError, match="simulated"):
        atomic_write_text(out, '{"overwrite": true}')

    assert out.read_text(encoding="utf-8") == '{"keep": true}'


# ---------------------------------------------------------------------------
# Integration: each bench script's main() routes through atomic_write_text.
# Both scripts insert their parent dir into sys.path at import time; loading
# them as modules via importlib.util preserves that side effect.
# ---------------------------------------------------------------------------


def _load_script(name: str):
    """Load `scripts/<name>` as a module so its public functions are callable.

    Both bench scripts begin with a `sys.path.insert(0, repo_root)` and
    expect `async_pipelines` to be importable; the test process already has
    `async_pipelines` on sys.path via the editable install, so this no-ops.

    The module must be registered in `sys.modules` BEFORE `exec_module`
    runs so that dataclasses defined in the script can find their own
    `__module__` during class creation (otherwise dataclasses raises
    AttributeError when walking sys.modules[cls.__module__].__dict__).
    """
    script_path = Path(__file__).resolve().parent.parent / "scripts" / name
    module_name = f"_bench_under_test_{name.replace('.', '_')}"
    spec = importlib.util.spec_from_file_location(module_name, script_path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    try:
        spec.loader.exec_module(module)
    except Exception:
        sys.modules.pop(module_name, None)
        raise
    return module


def test_bench_1000_doc_routes_through_atomic_helper(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """`scripts/bench_1000_doc.py`'s artifact writes (md + json) must route
    through atomic_write_text. The script uses ``--n 1`` and a near-zero
    fake-latency to keep the test under a second.
    """
    bench = _load_script("bench_1000_doc.py")

    out_md = tmp_path / "bench.md"

    # First fail mode: rename raises. Both writes should fail; destination
    # must remain absent. The script runs serial → async → batched, so we
    # know `out_md` is the first write target. Use `--n 1 --latency 0.0`
    # so the bench finishes near-instantly regardless of pipeline shape.
    def boom(*_args, **_kwargs):
        raise OSError("simulated rename failure")

    monkeypatch.setattr(io_utils_mod.os, "replace", boom)
    with pytest.raises(OSError, match="simulated rename failure"):
        bench.main(
            [
                "--n",
                "1",
                "--concurrency",
                "1",
                "--batch-size",
                "1",
                "--latency",
                "0.0",
                "--out",
                str(out_md),
            ]
        )
    assert not out_md.exists(), "bench --out md must not write destination on replace failure"


def test_bench_1000_doc_json_does_not_clobber_markdown_when_out_ends_in_json(
    tmp_path: Path,
) -> None:
    """`--out foo.json` must not let the raw-JSON dump overwrite the markdown
    report. `Path.with_suffix(".json")` is a no-op on a `.json` path, so the
    JSON sibling used to collide with `--out` and win (markdown silently lost).
    """
    bench = _load_script("bench_1000_doc.py")

    out = tmp_path / "report.json"
    rc = bench.main(
        [
            "--n",
            "5",
            "--latency",
            "0.0",
            "--concurrency",
            "2",
            "--batch-size",
            "2",
            "--out",
            str(out),
        ]
    )
    assert rc == 0

    # The markdown report at --out must survive as markdown, not be replaced
    # by the JSON dump.
    assert out.exists()
    text = out.read_text(encoding="utf-8")
    with pytest.raises(json.JSONDecodeError):
        json.loads(text)

    # The raw JSON lands at a distinct, non-colliding path.
    json_path = out.with_name(out.name + ".json")
    assert json_path.exists()
    assert json.loads(json_path.read_text(encoding="utf-8"))["workload"]["n_docs"] == 5


def test_bench_backpressure_routes_through_atomic_helper(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """`scripts/bench_backpressure.py`'s artifact writes (md + json) must
    route through atomic_write_text.

    Invokes ``main_async(args)`` directly (the synchronous ``main()``
    parses sys.argv, which would compete with pytest's argv).
    """
    import asyncio

    bench = _load_script("bench_backpressure.py")

    out_md = tmp_path / "bp.md"

    def boom(*_args, **_kwargs):
        raise OSError("simulated rename failure")

    monkeypatch.setattr(io_utils_mod.os, "replace", boom)

    args = bench._build_parser().parse_args(
        [
            "--n",
            "8",
            "--queue-size",
            "4",
            "--concurrency",
            "2",
            "--out-md",
            str(out_md),
        ]
    )
    with pytest.raises(OSError, match="simulated rename failure"):
        asyncio.run(bench.main_async(args))
    assert not out_md.exists(), (
        "bench_backpressure --out-md must not write destination on replace failure"
    )


# Make linters happy — sys imported above is used by the script-loading
# pattern's implicit dependency on `sys.path`.
_ = sys
