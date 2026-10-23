"""Lock: a "doesn't poison the batch" claim must name its mode (#221).

The README feature list said ``timeout`` raises ``PipelineTimeoutError``
"at the per-item level without poisoning the rest of the batch". That
holds only with ``return_exceptions=True``: the default is fail-fast,
so one timeout cancels the batch and the siblings' completed results
are discarded. The README's own timeouts section and
``docs/architecture.md`` §5 already said so; the feature bullet was the
one surface that dropped the qualifier.

The lock is paragraph-scoped: every paragraph (blank-line-separated
block or top-level list item, code blocks included) in README.md or docs/*.md that says the
batch is not poisoned must mention ``return_exceptions=True`` in the
same paragraph. The behavioural tests below pin the premise in both
directions, so the rule stays tied to what ``process`` actually does.
"""

from __future__ import annotations

import asyncio
import re
from pathlib import Path

import pytest

from async_pipelines import PipelineTimeoutError, process

REPO_ROOT = Path(__file__).resolve().parent.parent
DOC_FILES = (REPO_ROOT / "README.md", *sorted((REPO_ROOT / "docs").glob("*.md")))

# "without poisoning", "don't poison", "doesn't poison", "never poisons".
POISON_CLAIM = re.compile(r"poison", re.IGNORECASE)
QUALIFIER = "return_exceptions=True"


def _paragraphs(path: Path) -> list[tuple[int, str]]:
    """Split on blank lines and top-level list items; return (1-based
    start line, block text). A feature list is one blank-line block, so
    without the bullet split one bullet's qualifier would cover another's
    claim."""
    out: list[tuple[int, str]] = []
    start, buf = 0, []
    for lineno, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        if buf and line.startswith(("- ", "* ")):
            out.append((start, "\n".join(buf)))
            buf = []
        if line.strip():
            if not buf:
                start = lineno
            buf.append(line)
        elif buf:
            out.append((start, "\n".join(buf)))
            buf = []
    if buf:
        out.append((start, "\n".join(buf)))
    return out


def _poison_claims() -> list[tuple[str, int, str]]:
    return [
        (path.relative_to(REPO_ROOT).as_posix(), start, block)
        for path in DOC_FILES
        for start, block in _paragraphs(path)
        if POISON_CLAIM.search(block)
    ]


def test_poison_claims_are_found() -> None:
    # Anti-vacuity: the README has (at least) the feature bullet and the
    # tool-dispatch example comment. If the scan finds nothing, the regex
    # or the paragraph split broke and the lock below is silently green.
    assert len(_poison_claims()) >= 2


def test_feature_bullet_poison_claim_names_return_exceptions() -> None:
    # The exact surface #221 was filed against.
    readme = REPO_ROOT / "README.md"
    bullets = [
        block for _, block in _paragraphs(readme) if block.startswith("- **Per-item timeouts")
    ]
    assert len(bullets) == 1, bullets
    (bullet,) = bullets
    assert POISON_CLAIM.search(bullet), bullet
    assert QUALIFIER in bullet, (
        "README 'Per-item timeouts' bullet claims the batch isn't poisoned "
        f"but doesn't say that holds only with {QUALIFIER} (#221)."
    )
    assert "fail-fast" in bullet, (
        "README 'Per-item timeouts' bullet should name fail-fast as the default (#221)."
    )


@pytest.mark.parametrize(
    ("where", "start", "block"),
    [pytest.param(w, s, b, id=f"{w}:{s}") for w, s, b in _poison_claims()],
)
def test_every_poison_claim_names_return_exceptions(where: str, start: int, block: str) -> None:
    assert QUALIFIER in block, (
        f"{where}:{start} claims a batch is not poisoned without naming "
        f"{QUALIFIER}; the default is fail-fast (#221).\n{block}"
    )


# --- behavioural premise -------------------------------------------------


async def _fn(i: int) -> int:
    # Items 1 and 2 finish at once; 0, 3 and 4 outlive the timeout.
    if i in (1, 2):
        return i
    await asyncio.sleep(10)
    return i


async def test_default_mode_poisons_the_batch() -> None:
    # Fail-fast: the timeouts surface as an ExceptionGroup and no result
    # list comes back, so items 1 and 2's completed values are lost.
    with pytest.raises(ExceptionGroup) as info:
        await process(range(5), _fn, concurrency=10, timeout=0.05)
    assert all(isinstance(e, PipelineTimeoutError) for e in info.value.exceptions)
    assert {e.index for e in info.value.exceptions} == {0, 3, 4}


async def test_return_exceptions_keeps_the_batch() -> None:
    results = await process(range(5), _fn, concurrency=10, timeout=0.05, return_exceptions=True)
    assert results[1:3] == [1, 2]
    assert all(isinstance(results[i], PipelineTimeoutError) for i in (0, 3, 4))
