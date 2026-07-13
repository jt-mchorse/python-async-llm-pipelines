"""README decision-range upper-bound lock.

Sister to ``chunking-strategies-lab`` ``test_readme_snapshot.py``
``test_decision_range_cites_latest_active`` (pattern leader), plus
propagations in ``llm-eval-harness``, ``llm-cost-optimizer``,
``prompt-regression-suite``, ``rag-production-kit``,
``embedding-model-shootout``, and ``vector-search-at-scale``.

The README's architecture-section summary cites a range like
``D-002…D-NNN``; the upper bound must equal the highest active
(non-superseded) ``D-NNN`` in ``MEMORY/core_decisions_ai.md``. A new
decision landing without the README being updated fails this test
loud — exactly the shape that surfaced when D-011 landed in #36
without the README's D-002…D-010 bound being bumped.
"""

from __future__ import annotations

import re
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
README = REPO_ROOT / "README.md"
DECISIONS = REPO_ROOT / "MEMORY" / "core_decisions_ai.md"


def _max_active_decision_id() -> int:
    """Highest non-superseded ``D-NNN`` in ``MEMORY/core_decisions_ai.md``."""
    text = DECISIONS.read_text(encoding="utf-8")
    blocks = re.split(r"\n(?=- id:)", text)
    best = 0
    for block in blocks:
        id_match = re.search(r"- id:\s*D-(\d+)", block)
        if not id_match:
            continue
        sup_match = re.search(r"superseded_by:\s*(\S+)", block)
        is_active = (sup_match is None) or (sup_match.group(1).strip().lower() == "null")
        if is_active:
            n = int(id_match.group(1))
            if n > best:
                best = n
    return best


def test_decision_range_cites_latest_active() -> None:
    body = README.read_text(encoding="utf-8")
    pattern = re.compile(r"D-0*2\s*(?:…|\.\.\.)\s*D-0*(\d+)")
    matches = pattern.findall(body)
    assert matches, (
        "README.md must cite the active-decision range as "
        "`D-002…D-NNN` somewhere (the architecture-section summary "
        "paragraph by convention). Not found."
    )
    latest = _max_active_decision_id()
    # Every `D-002…D-NNN` citation must cite the latest active id, not just the
    # max of them. A prior `max()`-only check let a stale *lower* citation sail
    # through green when a correct citation elsewhere dominated the max (the
    # README carried both `D-002…D-010` and `D-002…D-011`). Reject any stale one.
    cited = sorted({int(m) for m in matches})
    stale = [c for c in cited if c != latest]
    assert not stale, (
        f"README.md cites decision range(s) up to "
        f"{', '.join(f'D-{c:03d}' for c in stale)} that don't match the highest "
        f"active D-NNN in MEMORY/core_decisions_ai.md (D-{latest:03d}). Every "
        f"`D-002…D-NNN` citation must read D-002…D-{latest:03d}; update the stale one(s)."
    )
