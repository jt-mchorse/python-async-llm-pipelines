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

_README = (Path(__file__).resolve().parent.parent / "README.md").read_text(encoding="utf-8")


def _commands() -> list[list[str]]:
    joined = re.sub(r"\\\s*\n\s*", " ", _README)
    return [
        shlex.split(line.strip())
        for line in joined.splitlines()
        if line.strip().startswith("python scripts/bench_1000_doc.py")
    ]


def test_the_commands_are_found() -> None:
    assert len(_commands()) >= 2  # the #4 section and the Demo section


def test_every_readme_bench_command_writes_outside_docs() -> None:
    for argv in _commands():
        assert "--out" in argv, f"no --out (defaults to docs/benchmarks.md): {' '.join(argv)}"
        out = argv[argv.index("--out") + 1]
        assert not out.startswith("docs/"), f"writes into docs/: {' '.join(argv)}"
