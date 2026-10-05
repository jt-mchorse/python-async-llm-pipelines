"""`bench_backpressure.py`'s docstring documents only safe or labelled commands (#132).

The docstring said "Numbers are real and reproducible" over a command with no
`--out-md`/`--out-json` (both default to the committed `docs/backpressure.*`)
and no `--compare --compare-n` (which the committed 3-row artifact needs), so
running it as written replaced the artifact with a 1-row table and turned the
doc-surface tests red. #128 fixed the same shape for `bench_1000_doc.py`.
"""

from __future__ import annotations

import ast
import re
import shlex
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SCRIPT = ROOT / "scripts" / "bench_backpressure.py"
REGEN = "# regenerates the committed snapshot"


def _commands(text: str) -> list[tuple[list[str], bool]]:
    joined = re.sub(r"\\\s*\n\s*", " ", text)
    out = []
    for line in joined.splitlines():
        stripped = line.strip()
        if stripped.startswith("python scripts/bench_backpressure.py"):
            command, _, comment = stripped.partition("#")
            out.append((shlex.split(command), REGEN in "#" + comment))
    return out


def _docstring() -> str:
    return ast.get_docstring(ast.parse(SCRIPT.read_text(encoding="utf-8"))) or ""


def test_the_docstring_documents_commands() -> None:
    assert len(_commands(_docstring())) >= 2


def test_every_unlabelled_docstring_command_writes_outside_docs() -> None:
    for argv, is_regen in _commands(_docstring()):
        if is_regen:
            continue
        for flag in ("--out-md", "--out-json"):
            assert flag in argv, f"no {flag} (defaults to docs/): {' '.join(argv)}"
            assert not argv[argv.index(flag) + 1].startswith("docs/"), " ".join(argv)


def test_the_labelled_regeneration_is_the_readmes_provenance_command() -> None:
    regen = [argv for argv, is_regen in _commands(_docstring()) if is_regen]
    readme = _commands((ROOT / "README.md").read_text(encoding="utf-8"))
    assert len(regen) == 1
    assert readme, "the README no longer documents the provenance command"
    assert regen[0] == readme[0][0]
