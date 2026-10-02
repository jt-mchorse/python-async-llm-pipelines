"""Every README `bench_1000_doc.py` command writes outside `docs/` (#120).

`--out` defaults to the committed `docs/benchmarks.md`. The `## 1000-doc
benchmark (#4)` copy of the command omitted it, so running the README
overwrote the committed snapshot with a new machine's timings and turned
`test_bench_table_snapshot.py` red. The `## Demo` copy already passed
`--out /tmp/bench.md` with a warning saying exactly this.
"""

from __future__ import annotations

import re
import shlex
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
_README = (_ROOT / "README.md").read_text(encoding="utf-8")
_REGEN = "# regenerates the committed snapshot"


def _script_docstring() -> str:
    # The docstring is also `--help` (`build_arg_parser` uses `description=__doc__`),
    # and it was the copy #120 did not reach (#128).
    import ast

    tree = ast.parse((_ROOT / "scripts" / "bench_1000_doc.py").read_text(encoding="utf-8"))
    return ast.get_docstring(tree) or ""


def _commands(text: str) -> list[tuple[list[str], bool]]:
    """Each documented command, and whether its line is labelled a regeneration."""
    joined = re.sub(r"\\\s*\n\s*", " ", text)
    out = []
    for line in joined.splitlines():
        stripped = line.strip()
        if stripped.startswith("python scripts/bench_1000_doc.py"):
            command, _, comment = stripped.partition("#")
            out.append((shlex.split(command), _REGEN in "#" + comment))
    return out


SOURCES = {"README.md": lambda: _README, "scripts/bench_1000_doc.py docstring": _script_docstring}


def test_the_commands_are_found_in_every_source() -> None:
    assert len(_commands(_README)) >= 2  # the #4 section and the Demo section
    assert len(_commands(_script_docstring())) >= 2  # the Usage block (#128)


def test_every_documented_bench_command_writes_outside_docs() -> None:
    for source, text in SOURCES.items():
        for argv, is_regen in _commands(text()):
            shown = f"{source}: {' '.join(argv)}"
            assert "--out" in argv, f"no --out (defaults to docs/benchmarks.md): {shown}"
            out = argv[argv.index("--out") + 1]
            if out.startswith("docs/"):
                assert is_regen, f"writes into docs/ without saying so ({_REGEN!r}): {shown}"


def test_the_help_text_is_the_docstring() -> None:
    # Why the docstring is a publication surface at all.
    import importlib.util

    spec = importlib.util.spec_from_file_location(
        "bench_1000_doc", _ROOT / "scripts" / "bench_1000_doc.py"
    )
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    help_text = module.build_arg_parser().format_help()
    assert "--out /tmp/bench.md" in help_text
