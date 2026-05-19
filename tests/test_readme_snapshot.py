"""README snapshot: lock the surface bullet list and demo invariants.

Sister to the portfolio-wide drift-lock pattern landed 2026-05-18+.
The README's "What this is" enumerates five primitives and pins each
to a closed issue (#1 process+stream, #2 tool_dispatch, #3 backpressure
metrics, #4 1000-doc bench, #5 per-item timeouts). The Demo section
must describe today's runnable surface and name a follow-up issue.

The test:
- Asserts every shipped-issue ref appears in `## What this is`.
- Asserts the regression string `This PR ships` does not appear in the
  README (defense against re-introducing the pre-2026-05-19 framing).
- Asserts every relative file path the README references resolves on
  disk.
- Asserts the Demo section names at least one follow-up issue, mentions
  the bench script, and is not the bare `*60-second demo pending*` line.
"""

from __future__ import annotations

import re
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
README = REPO_ROOT / "README.md"


def _readme() -> str:
    return README.read_text(encoding="utf-8")


def test_what_this_is_names_every_shipped_issue() -> None:
    body = _readme()
    start = body.index("## What this is")
    end = body.index("##", start + 1)
    section = body[start:end]
    expected = ["(#1)", "(#2)", "(#3)", "(#4)", "(#5)"]
    missing = [ref for ref in expected if ref not in section]
    assert not missing, (
        f"`## What this is` is missing issue references: {missing}. "
        f"Every closed issue with shipped surface must be cited."
    )


def test_readme_does_not_carry_this_pr_ships_framing() -> None:
    body = _readme()
    assert "This PR ships" not in body, (
        "README contains the stale 'This PR ships' framing; everything ships now. "
        "Rewrite the paragraph past-tense."
    )


def test_referenced_files_exist() -> None:
    body = _readme()
    pattern = re.compile(r"\(([^)\s]+\.(?:md|jsonl|py|html|json|yml|yaml|png|svg))\)")
    refs = {r for r in pattern.findall(body) if not r.startswith(("http://", "https://"))}
    missing = sorted(r for r in refs if not (REPO_ROOT / r).exists())
    assert not missing, (
        f"README references files that don't exist: {missing}. "
        "Either fix the link or commit the file."
    )


def test_demo_section_names_followup_and_describes_today() -> None:
    body = _readme()
    start = body.index("## Demo")
    end = body.index("##", start + 1)
    demo = body[start:end]
    assert re.search(r"#\d+", demo), (
        "Demo section must name at least one follow-up issue (the captured-asset owner)."
    )
    assert "bench_1000_doc" in demo, (
        "Demo section must reference the 1000-doc bench script — it's the demonstrable speedup table."
    )
    stripped = [line for line in demo.strip().splitlines() if line and not line.startswith("#")]
    assert not (len(stripped) == 1 and stripped[0].lower().startswith("*60-second demo pending")), (
        "Demo section is just the bare '60-second demo pending' line. "
        "Replace with a description of today's two-command surface plus the captured-asset follow-up."
    )
    assert "depends on issue [#4]" not in demo, (
        "Demo section still references the gating phrase 'depends on issue [#4]' — #4 is closed."
    )
