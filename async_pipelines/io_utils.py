"""Atomic on-disk write helper.

The benchmark scripts in `scripts/` write artifacts (markdown rendered
into the README's "Benchmark Results" section, plus companion JSON
consumed by downstream plotting/aggregation tooling). `Path.write_text`
is not atomic: SIGINT/SIGTERM/disk-full/OOM between the implicit
`open(..., "w")` truncate and `close()` flush leaves the destination
zero-length or partial.

`atomic_write_text` writes to a sibling temp file in the destination's
parent directory, `fsync`s, then `os.replace`s. Same-directory
placement is load-bearing: it guarantees the rename is same-filesystem
so the POSIX rename can't fall back to a copy.

Pattern mirrors the portfolio siblings:
- `rag_kit/io_utils.atomic_write_text` (rag-production-kit#44/#45)
- `eval_harness/io_utils.atomic_write_text` (llm-eval-harness#51, D-015)
- `emb_shootout/io_utils.atomic_write_text` (embedding-model-shootout#37, D-009)
- `prompt_regression/io.atomic_write_text` (prompt-regression-suite#40)
"""

from __future__ import annotations

import contextlib
import os
import secrets
import stat
from pathlib import Path

# Cap the target basename's contribution to the temp filename. The temp name is
# `.<base>.<random>.tmp`; the affixes add ~13-20 bytes, so prepending a full
# basename that is itself near NAME_MAX (255 on ext4/APFS) overflows the limit
# and the write fails with `OSError: [Errno 63] File name too long` — even though
# a plain `Path.write_text` of that same target succeeds (sibling of
# rag-production-kit#128 and mcp-server-cookbook#96). The base in the temp name
# is cosmetic (`ls`-ability); uniqueness comes from `_create_temp`'s random
# component, so truncating it is safe. Budget is in BYTES (NAME_MAX is a byte
# limit) and we trim on a char boundary so multibyte names are never split
# mid-codepoint.
_MAX_TEMP_BASE_BYTES = 200


def _name_bytes(base: str) -> int:
    """Length of *base* in the bytes the filesystem actually sees.

    `os.fsencode`, not `base.encode("utf-8")` (#104). Both halves of the
    comment above are true and the old implementation still counted the wrong
    bytes: NAME_MAX limits the bytes handed to the kernel, which is
    `os.fsencode` — `sys.getfilesystemencoding()` together with
    `sys.getfilesystemencodeerrors()`, i.e. `surrogateescape` on POSIX.

    That handler is why the distinction bites rather than being pedantry. A
    path byte that is not valid UTF-8 arrives in Python as a lone surrogate in
    `U+DC80..U+DCFF`, and strict `str.encode("utf-8")` refuses to encode it —
    so `_cap_base_for_temp` used to raise `UnicodeEncodeError` on a destination
    the OS can name, *before* reaching the length question. `sys.argv` decodes
    with the same handler, so `--out $'bench\\xff.md'` is enough.

    `UnicodeEncodeError` is a `ValueError`, so neither bench script's
    `except OSError` guard caught it — and both guards exist to prevent exactly
    what it produced. `bench_1000_doc.py`: "Without this guard it escaped
    `amain` as a raw traceback at exit 1 — the 'success' range — *after* the
    benchmark already ran". `bench_backpressure.py` carries the mirrored
    comment for `--out-md` / `--out-json`. Measured, both exited 1 with a
    traceback on a name carrying one non-UTF-8 byte.
    `benchmark.dump_benchmark_json` is library-public and calls
    `atomic_write_text` bare, so an embedding caller written against the
    `OSError` a plain `Path.write_text` of that target raises was exposed too.

    `os.fsencode` never raises: `surrogateescape` on POSIX, `surrogatepass` on
    Windows, so every `str` a `Path` can hold round-trips. For a name that is
    valid UTF-8 it returns exactly the old number, so the budget is unchanged
    for every name that worked before.
    """
    return len(os.fsencode(base))


def _cap_base_for_temp(base: str) -> str:
    if _name_bytes(base) <= _MAX_TEMP_BASE_BYTES:
        return base
    out = base
    while out and _name_bytes(out) > _MAX_TEMP_BASE_BYTES:
        out = out[:-1]
    return out


def atomic_write_text(path: str | Path, text: str, encoding: str = "utf-8") -> None:
    """Write *text* to *path* atomically.

    On success the destination contains exactly *text*. On any failure
    path (signal, disk-full, OOM during flush), the destination is
    either unchanged (overwrite case) or absent (new-file case) —
    never partial.

    Parent directories are created with `mkdir(parents=True,
    exist_ok=True)`.
    """
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp_path: Path | None = None
    try:
        fd, tmp_path = _create_temp(target)
        # `os.fdopen` owns *fd* from here: if it raises (unknown codec), `io.open`
        # has already closed it, and the `finally` below removes the temp file.
        with os.fdopen(fd, "w", encoding=encoding) as tmp:
            tmp.write(text)
            tmp.flush()
            os.fsync(tmp.fileno())
        _copy_existing_mode(target, tmp_path)
        os.replace(tmp_path, target)
        tmp_path = None
    finally:
        if tmp_path is not None:
            with contextlib.suppress(FileNotFoundError):
                tmp_path.unlink()


# File mode (#124, portfolio-ops#81). This helper used to create its temp file
# with `tempfile.NamedTemporaryFile`, which always opens 0600 regardless of the
# umask, and `os.replace` carries the temp file's mode onto the target. So every
# file it wrote was owner-only, and an overwrite of an existing 0644 file
# demoted it to 0600 — neither of which the `Path.write_text` it replaced did.
# The temp file is now opened with mode 0o666 so the KERNEL applies the umask
# (reading the umask via `os.umask(0); os.umask(old)` would briefly set a
# process-wide umask of 0 for every other thread), and an existing target's
# mode is copied onto the temp file before the rename.
_TEMP_ATTEMPTS = 100


def _create_temp(target: Path) -> tuple[int, Path]:
    """Exclusively create `.<capped-base>.<random>.tmp` beside *target*.

    Same name shape and random-component length (8 chars) as the
    `NamedTemporaryFile` it replaces, so `_MAX_TEMP_BASE_BYTES` budgets it
    unchanged. `O_EXCL` gives the same no-clobber guarantee; a collision just
    draws another name.
    """
    prefix = f".{_cap_base_for_temp(target.name)}."
    for _ in range(_TEMP_ATTEMPTS):
        candidate = target.parent / f"{prefix}{secrets.token_hex(4)}.tmp"
        try:
            fd = os.open(candidate, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o666)
        except FileExistsError:
            continue
        return fd, candidate
    raise FileExistsError(f"could not create a unique temp file beside {target}")


def _copy_existing_mode(target: Path, tmp_path: Path) -> None:
    """Give *tmp_path* the permission bits of *target*, if *target* exists."""
    try:
        mode = stat.S_IMODE(os.stat(target).st_mode)
    except FileNotFoundError:
        return
    os.chmod(tmp_path, mode)
