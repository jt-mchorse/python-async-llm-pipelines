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
import errno
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
    target = _resolve_symlinked_target(Path(path))
    # A read-only existing target is refused, as `Path.write_text` refuses it,
    # instead of being renamed over (#246). See `_refuse_unwritable_target`.
    _refuse_unwritable_target(target)
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


def check_writable(path: str | Path) -> None:
    """Raise the `OSError` `atomic_write_text(path, ...)` would, without writing (#167).

    The bench scripts translated an unwritable output path into a clean exit 2,
    but only once the benchmark had run -- ~45 s for `bench_1000_doc` at its
    defaults -- and `bench_1000_doc` replaced its markdown before it found the
    JSON path unwritable. This does what the writer does -- the same symlink
    resolution (#157), the same parent `mkdir`, the same exclusively-created
    temp file beside the target -- and removes the temp file, so a path passes
    exactly when the real write would get that far. An existing directory is
    refused too: the final `os.replace` onto it fails, and so is a read-only
    existing file, which the writer refuses before its temp file (#246). The
    preflight llm-eval-harness#287 and vector-search-at-scale#207 added.
    """
    target = _resolve_symlinked_target(Path(path))
    if target.is_dir():
        raise IsADirectoryError(21, "Is a directory", str(target))
    # The writer's own check, so a 0444 output fails here and not after the run.
    # Its `os.stat` is what `_copy_existing_mode` stats: a link loop raises here.
    _refuse_unwritable_target(target)
    target.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_path = _create_temp(target)
    os.close(fd)
    tmp_path.unlink()


def same_file(a: str | Path, b: str | Path) -> bool:
    """Whether writing *a* and writing *b* would write one file (#188, #220, #234).

    `realpath` covers `./`, `..` and symlinks, existing or dangling, and
    `samefile` covers an existing alias it cannot see (a hard link, or a name
    that differs only in case or Unicode normalization on a filesystem that
    ignores the difference, which is macOS's default). The bench scripts ran
    `samefile` only when *both* files existed, so on a fresh output directory
    `Report.md` and `report.md` compared as different, both writes landed in one
    file, and the JSON replaced the report at exit 0 (#234). Whether two absent
    names alias is the filesystem's rule, not a string comparison, so it is
    asked: *a* is created exclusively, *b* is checked, and *a* is removed. If
    only one of them exists they cannot be one file. Call after `check_writable`
    on both, which creates the parent directories.
    """
    ra, rb = os.path.realpath(a), os.path.realpath(b)
    if ra == rb:
        return True
    a_exists, b_exists = os.path.exists(ra), os.path.exists(rb)
    if a_exists and b_exists:
        return os.path.samefile(ra, rb)
    if a_exists or b_exists:
        return False
    fd = os.open(ra, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    os.close(fd)
    try:
        return os.path.exists(rb) and os.path.samefile(ra, rb)
    finally:
        os.unlink(ra)


def sibling_path(path: str | Path, own: str, other: str) -> Path:
    """The `other`-suffixed file that belongs beside *path* (#118, #238, #244, #248).

    Swap the suffix when it is *path*'s own (`bp.md` -> `bp.json`), append
    otherwise (`run.a` -> `run.a.json`), so two runs with different suffixes do
    not share a sibling (#118). Appending to the name rather than calling
    `with_name` keeps a path like `..` from raising; `check_writable` refuses it
    as a directory.

    A suffix that is *own* in another case (`.MD`) is the filesystem's call,
    not a string comparison's. On macOS's default filesystem
    `docs/backpressure.MD` *is* the committed `backpressure.md`, so appending
    (`backpressure.MD.json`) rewrote the report and left the committed JSON
    from another run, at exit 0 (#244). On a case-sensitive one it is a new
    file, so swapping -- #245 lowered the suffix everywhere -- pointed the
    derived JSON at the committed `backpressure.json` and rewrote that instead
    (#248). So the suffix is swapped only when `same_file` says *path* aliases
    `path.with_suffix(own)`, which keeps the pair together where it is one
    pair, and appended otherwise, which touches nothing committed. With no
    parent directory yet nothing there can be committed, so it appends; a probe
    that fails appends too, and `check_writable` reports the error.
    """
    p = Path(path)
    if p.suffix == own:
        return p.with_suffix(other)
    if p.suffix.lower() == own.lower() and _aliases(p, p.with_suffix(own)):
        return p.with_suffix(other)
    return p.parent / (p.name + other)


def _aliases(a: Path, b: Path) -> bool:
    """`same_file(a, b)` when *a*'s directory exists; `False` otherwise or on error."""
    if not a.parent.is_dir():
        return False
    try:
        return same_file(a, b)
    except OSError:
        return False


def display_path(path: str | Path) -> str:
    """*path* as text any UTF-8 stream can print, e.g. a "wrote <path>" line (#186).

    A name byte that is not valid UTF-8 reaches Python as a lone surrogate
    (`surrogateescape`, see `_name_bytes`), and printing it raises
    `UnicodeEncodeError` unless `sys.stdout`'s error handler happens to be
    `surrogateescape` -- true for the C / C.UTF-8 locale CI runs in, false for
    `en_US.UTF-8` and every other UTF-8 locale. The bench scripts printed the
    path between their two writes, so a name the write had just accepted ended
    the run at exit 1 with the markdown written and no JSON.

    The undecodable bytes are shown as `\\xff`, the shell's own `$'...'`
    spelling; a name that is valid UTF-8 comes back unchanged.
    """
    return os.fsencode(path).decode("utf-8", "backslashreplace")


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


def _resolve_symlinked_target(target: Path) -> Path:
    """The file a write to *target* lands in: through a symlink, as `Path.write_text` does (#157).

    `os.replace` renames onto the LINK, not the file it points at. Writing to a
    symlinked destination therefore turned the link into a regular file and left
    the linked file holding its old contents, while `Path.write_text` -- the
    call this helper replaced, and whose behaviour #124 restored for file mode --
    writes through it. `_copy_existing_mode` already followed the link (`os.stat`),
    so the helper copied the linked file's mode onto a file that then replaced
    the link instead.

    Resolving here also puts the temp file beside the RESOLVED file, which keeps
    the rename on one filesystem when the link points to another one. A dangling
    link resolves to the path it names, which the write then creates, as
    `Path.write_text` would. A plain path is returned unchanged.
    """
    if not target.is_symlink():
        return target
    return Path(os.path.realpath(target))


def _refuse_unwritable_target(target: Path) -> None:
    """Raise `PermissionError` if *target* exists and cannot be written (#246).

    `os.replace` swaps the directory ENTRY, so only the directory's write bit is
    checked and the target's own is never consulted: a `chmod 444` output was
    silently rewritten, mode still 0444. `Path.write_text`, the call this helper
    replaced, opens the file for writing and fails with `EACCES`, and the bench
    scripts' write seams name "a read-only filesystem" among the unwritable
    outputs they turn into exit 2. Raising the same error here, before the temp
    file exists, keeps that contract and leaves nothing behind.

    `os.access` asks the kernel the same question the open would, so root (which
    `Path.write_text` lets through) still writes. The `os.stat` first keeps a
    missing target -- the create case -- out of the check, and lets a link loop
    raise its own `ELOOP` rather than a misleading `EACCES`. Like
    `Path.write_text`, this is a check at open time, not a lock: a `chmod`
    racing the write is not covered. Port of prompt-regression-suite#318.
    """
    try:
        os.stat(target)
    except FileNotFoundError:
        return
    if not os.access(target, os.W_OK):
        raise PermissionError(errno.EACCES, os.strerror(errno.EACCES), str(target))


def _copy_existing_mode(target: Path, tmp_path: Path) -> None:
    """Give *tmp_path* the permission bits of *target*, if *target* exists."""
    try:
        mode = stat.S_IMODE(os.stat(target).st_mode)
    except FileNotFoundError:
        return
    os.chmod(tmp_path, mode)
