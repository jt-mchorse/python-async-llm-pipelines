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
import tempfile
from pathlib import Path


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
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding=encoding,
            dir=target.parent,
            prefix=f".{target.name}.",
            suffix=".tmp",
            delete=False,
        ) as tmp:
            tmp_path = Path(tmp.name)
            tmp.write(text)
            tmp.flush()
            os.fsync(tmp.fileno())
        os.replace(tmp_path, target)
        tmp_path = None
    finally:
        if tmp_path is not None:
            with contextlib.suppress(FileNotFoundError):
                tmp_path.unlink()
