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


# ---------------------------------------------------------------------------
# Package directory-tree completeness lock (#82).
#
# The README's Architecture section opens with a fenced `async_pipelines/`
# directory tree. Its bare `foo.py` entries aren't markdown links (so
# `test_referenced_files_exist` skips them) and nothing asserted the tree
# matches the package — that is how `benchmark.py` (#4) and `io_utils.py`
# stayed out of the tree even though `benchmark.py` is a documented shipped
# primitive. Parse the tree block and assert its `*.py` entries equal the
# package's non-dunder module set. Same directory-tree-completeness class as
# nextjs #83, llm-eval-harness #171, prompt-regression-suite #123, ems #99.
_PKG_DIR = REPO_ROOT / "async_pipelines"


def _tree_py_modules(readme: str) -> set[str]:
    """Basenames of the `*.py` entries in the fenced tree that opens with the
    `async_pipelines/` header line (scan stops at the closing fence)."""
    modules: set[str] = set()
    in_tree = False
    for line in readme.splitlines():
        if line.strip() == "async_pipelines/":
            in_tree = True
            continue
        if in_tree:
            if line.strip().startswith("```"):
                break
            m = re.search(r"([A-Za-z_][A-Za-z0-9_]*\.py)\b", line)
            if m:
                modules.add(m.group(1))
    return modules


def test_readme_tree_lists_every_package_module() -> None:
    """The fenced `async_pipelines/` tree names exactly the package's non-dunder
    `*.py` modules — no omission (the #4 benchmark/io_utils drift) and no stale
    leftover (#82)."""
    tree = _tree_py_modules(_readme())
    assert tree, "expected an `async_pipelines/` directory tree with *.py entries in the README"
    disk = {p.name for p in _PKG_DIR.glob("*.py") if p.name != "__init__.py"}
    missing = sorted(disk - tree)
    extra = sorted(tree - disk)
    drift = [
        *(f"missing from tree: {m}" for m in missing),
        *(f"in tree but not on disk: {e}" for e in extra),
    ]
    assert not drift, (
        "README async_pipelines/ directory tree is out of sync with the package:\n"
        + "\n".join(f"  {d}" for d in drift)
        + "\n(update the tree so it depicts the current package layout)"
    )


def test_readme_tree_parser_and_diff_catch_drift() -> None:
    """Inverse safety net: exercise the real parser + set-diff on synthetic
    trees so a vacuous parse can't let drift through."""
    good = "async_pipelines/\n├── a.py\n└── b.py\n```"
    assert _tree_py_modules(good) == {"a.py", "b.py"}
    assert sorted({"a.py", "b.py", "c.py"} - _tree_py_modules(good)) == ["c.py"]
    stale = "async_pipelines/\n├── a.py\n└── gone.py\n```"
    assert sorted(_tree_py_modules(stale) - {"a.py"}) == ["gone.py"]
