"""The temp-name byte budget is measured in the bytes the filesystem sees (#104).

`_cap_base_for_temp` exists so a destination basename near NAME_MAX does not
overflow the limit once the temp affixes are prepended. Its comment says the
budget is in bytes because NAME_MAX is a byte limit — true — and the old
implementation counted `str.encode("utf-8")` under the strict error handler,
which is a different set of bytes from the ones the kernel is handed.

The gap is reachable, not pedantic, because POSIX path bytes and `sys.argv`
both decode through `surrogateescape`: a byte that is not valid UTF-8 becomes a
lone surrogate in `U+DC80..U+DCFF`, which strict UTF-8 encoding refuses. So
`--out $'bench\\xff.md'` made the cap raise `UnicodeEncodeError` before it ever
reached the length question.

`UnicodeEncodeError` is a `ValueError`, so neither bench script's
`except OSError` guard caught it — and both guards exist to prevent exactly what
it produced. `bench_1000_doc.py` says it: "Without this guard it escaped `amain`
as a raw traceback at exit 1 — the 'success' range — *after* the benchmark
already ran."

These tests are written so the *host* never decides the verdict. The
`_cap_base_for_temp` cases are pure-function and hold everywhere. The seam cases
assert properties true on both a byte-transparent filesystem (ext4, where the
write succeeds) and a UTF-8-validating one (APFS, which returns `EILSEQ`).
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest

from async_pipelines import io_utils as io_mod
from async_pipelines.io_utils import (
    _MAX_TEMP_BASE_BYTES,
    _cap_base_for_temp,
    atomic_write_text,
)

_REPO_ROOT = Path(__file__).resolve().parents[1]

# A lone low surrogate is what `surrogateescape` produces for the raw byte
# 0xFF. Built from its codepoint rather than written literally so the character
# cannot be mangled by an editor or a copy-paste round trip.
SURROGATE = chr(0xDCFF)


def _fs_len(text: str) -> int:
    """The byte length the kernel sees. Never raises; that is the whole point."""
    return len(os.fsencode(text))


# ---------------------------------------------------------------------------
# The variant table. Axes: length (fits / overflows) x encoding class
# (pure ASCII / multibyte UTF-8 / surrogate-bearing / mixed).
# ---------------------------------------------------------------------------

NAME_VARIANTS = [
    ("ascii-short", "bench.md"),
    ("ascii-at-budget", "a" * _MAX_TEMP_BASE_BYTES),
    ("ascii-long", "a" * 250),
    # "é" is 2 bytes in UTF-8, so 150 of them is 300 bytes: over budget in
    # bytes while well under it in characters.
    ("multibyte-short", "bénch.md"),
    ("multibyte-long", "é" * 150),
    # Each surrogate is exactly one byte under `os.fsencode` — the byte the
    # name actually came from.
    ("surrogate-short", "bench" + SURROGATE + ".md"),
    ("surrogate-long", SURROGATE * 250),
    ("mixed-long", "é" * 50 + SURROGATE * 150),
    ("surrogate-only", SURROGATE),
    ("mixed-at-boundary", "a" * (_MAX_TEMP_BASE_BYTES - 1) + SURROGATE),
]


@pytest.mark.parametrize(("label", "base"), NAME_VARIANTS, ids=[v[0] for v in NAME_VARIANTS])
def test_cap_base_for_temp_never_raises_and_stays_within_budget(label: str, base: str) -> None:
    """Every name a `Path` can hold gets a capped answer, not an exception.

    Strict-UTF-8 measurement raised `UnicodeEncodeError` for the surrogate-
    bearing rows before it could answer the length question at all.
    """
    capped = _cap_base_for_temp(base)

    assert _fs_len(capped) <= _MAX_TEMP_BASE_BYTES, f"{label}: over budget"
    assert capped == base[: len(capped)], (
        f"{label}: the capped name must be a character-boundary prefix of the "
        "original — trimming happens by character so no codepoint is split"
    )
    if _fs_len(base) <= _MAX_TEMP_BASE_BYTES:
        assert capped == base, f"{label}: a name within budget must be returned unchanged"
    else:
        # Maximality: one more character would have gone over. Without this the
        # test would also pass for a cap that returns "" for everything.
        assert len(capped) < len(base)
        assert _fs_len(base[: len(capped) + 1]) > _MAX_TEMP_BASE_BYTES, (
            f"{label}: the cap trimmed further than the budget required"
        )


def test_cap_base_for_temp_agrees_with_the_old_measurement_on_encodable_names() -> None:
    """Switching the measurement must not move the budget for names that worked.

    `os.fsencode` and `str.encode("utf-8")` return the same bytes for every
    string that is valid UTF-8, so every previously-passing name is unaffected;
    the change is confined to the names the old call refused outright.
    """
    for _label, base in NAME_VARIANTS:
        try:
            strict = len(base.encode("utf-8"))
        except UnicodeEncodeError:
            continue  # the population the old measurement could not count at all
        assert _fs_len(base) == strict


def test_name_bytes_never_raises_on_a_surrogate() -> None:
    """The measurement helper itself is total over `str`.

    `os.fsencode` uses `surrogateescape` on POSIX and `surrogatepass` on
    Windows, so it round-trips every string a `Path` can carry.
    """
    assert io_mod._name_bytes("bench" + SURROGATE + ".md") == len(b"bench\xff.md")


# ---------------------------------------------------------------------------
# The seams. The exception *class* is the contract every caller is written
# against, so that is what gets asserted.
# ---------------------------------------------------------------------------


def test_atomic_write_text_unencodable_target_name_fails_as_oserror_if_at_all(
    tmp_path: Path,
) -> None:
    """A destination name the filesystem cannot represent is an OS-level
    problem, and must surface as one.

    Deliberately not asserted as "succeeds" or as "raises": ext4 accepts any
    non-NUL byte in a name and the write goes through, while APFS validates
    UTF-8 and returns `EILSEQ`. Both are correct, and both are `OSError` or
    nothing — which is what a plain `Path.write_text` of the same target does,
    and the only class either bench guard catches.
    """
    target = tmp_path / ("bench" + SURROGATE + ".md")

    try:
        atomic_write_text(target, "# report\n")
    except UnicodeEncodeError as e:  # pragma: no cover - the bug this closes
        pytest.fail(
            "atomic_write_text raised UnicodeEncodeError for an unencodable "
            f"destination *name*: {e!r}. The content was pure ASCII."
        )
    except OSError:
        # The filesystem refused the name. Nothing was left behind.
        assert list(tmp_path.iterdir()) == []
        return

    assert target.read_text(encoding="utf-8") == "# report\n"
    assert [p.name for p in tmp_path.iterdir()] == [target.name]


def test_atomic_write_text_long_unencodable_target_name_is_capped_not_refused(
    tmp_path: Path,
) -> None:
    """The long-name and the unencodable-name axes compose.

    This is the row that needs both halves of the fix: the fast-path check has
    to survive the surrogate to discover the name is over budget, and the trim
    loop has to survive it on every iteration.
    """
    target = tmp_path / (SURROGATE * 250)

    try:
        atomic_write_text(target, "x")
    except UnicodeEncodeError as e:  # pragma: no cover - the bug this closes
        pytest.fail(f"cap raised on a long unencodable name: {e!r}")
    except OSError:
        assert list(tmp_path.iterdir()) == []


def _run(*args: str) -> tuple[int, str]:
    """Drive a bench script in a real subprocess.

    Subprocess rather than in-process for two reasons: it exercises the actual
    argv road — `subprocess` fsencodes the argument and the child decodes it
    back with `surrogateescape`, which is what a shell `$'bench\\xff.md'` does —
    and it gets the real `sys.stderr`, whose `backslashreplace` handler differs
    from the strict-encoding buffer a capture fixture substitutes.

    Returns `(returncode, stderr)` with the streams captured as **bytes** and
    decoded with `errors="replace"`, not via `text=True`. On a filesystem that
    accepts the name (ext4, i.e. CI) the write succeeds, the child prints the
    path, and `sys.stdout`'s `surrogateescape` handler puts the original raw
    byte on the stream. `text=True` decodes that strictly *in the parent* and
    raises `UnicodeDecodeError` inside `subprocess` — a failure of the harness,
    not of the code under test.
    """
    proc = subprocess.run(
        [sys.executable, *args],
        cwd=str(_REPO_ROOT),
        capture_output=True,
        check=False,
    )
    return proc.returncode, proc.stderr.decode("utf-8", errors="replace")


def test_bench_1000_doc_unencodable_out_has_no_traceback(tmp_path: Path) -> None:
    """`--out` is operator input, and the guard around it names its own failure.

    "Without this guard it escaped `amain` as a raw traceback at exit 1 — the
    'success' range — *after* the benchmark already ran." An unencodable name
    reproduced that verbatim, because `UnicodeEncodeError` is a `ValueError`.

    Asserted as "no traceback, and if nothing was written the code is 2" rather
    than a fixed code, because ext4 accepts the name and writes the file while
    APFS refuses it; both are correct.
    """
    out = tmp_path / ("bench" + SURROGATE + ".md")

    rc, stderr = _run("scripts/bench_1000_doc.py", "--n", "20", "--out", str(out))

    assert "Traceback" not in stderr, stderr
    assert "UnicodeEncodeError" not in stderr, stderr
    if not out.exists():
        assert rc == 2
        assert "could not write report" in stderr


def test_bench_backpressure_unencodable_out_md_has_no_traceback(tmp_path: Path) -> None:
    """The sibling script, with the mirrored guard on `--out-md` / `--out-json`.

    Both scripts had to be checked: a guard is per-script, and this one is the
    second member of the population the shared helper feeds.
    """
    out = tmp_path / ("bp" + SURROGATE + ".md")

    rc, stderr = _run("scripts/bench_backpressure.py", "--out-md", str(out))

    assert "Traceback" not in stderr, stderr
    assert "UnicodeEncodeError" not in stderr, stderr
    if not out.exists():
        assert rc == 2
