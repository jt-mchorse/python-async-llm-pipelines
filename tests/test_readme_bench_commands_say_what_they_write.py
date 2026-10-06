"""Every README bench command either writes outside docs/ or says it rewrites the snapshot (#138).

`bench_1000_doc`'s README commands got `--out /tmp/...` and a comment in #120,
because the default `--out` overwrites the committed `docs/benchmarks.md`. The
`bench_backpressure` command beside them got neither: run as documented it
rewrote `docs/backpressure.{md,json}` with nothing saying so.
"""

from __future__ import annotations

import re
from pathlib import Path

README = Path(__file__).resolve().parents[1] / "README.md"
_BLOCK = re.compile(r"```bash\n(.*?)```", re.S)
_OUT_FLAG = re.compile(r"--out(?:-md|-json)?\s+(\S+)")


def _bench_blocks(text: str) -> list[tuple[str, str]]:
    """(preceding prose, block) for every bash block that runs a bench script."""
    out = []
    for m in _BLOCK.finditer(text):
        block = m.group(1)
        if re.search(r"scripts/bench_\w+\.py", block):
            prose = text[max(0, m.start() - 600) : m.start()]
            out.append((prose, block))
    return out


def test_every_bench_command_says_what_it_writes() -> None:
    blocks = _bench_blocks(README.read_text(encoding="utf-8"))
    assert len(blocks) >= 3  # bench_1000_doc twice and bench_backpressure, at least
    for prose, block in blocks:
        for command in re.split(r"\n(?=python )", block.strip()):
            if "scripts/bench_" not in command:
                continue
            outs = _OUT_FLAG.findall(command)
            redirected = outs and all(not o.startswith("docs/") for o in outs)
            labelled = re.search(r"rewrites? `?docs/", prose + command)
            assert redirected or labelled, (
                f"README runs a bench script that writes the committed docs/ "
                f"artifacts without saying so:\n{command}"
            )
