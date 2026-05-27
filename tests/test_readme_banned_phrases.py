"""README banned-phrase lock — sibling to test_architecture_doc.py's
BANNED_PHRASES guard, applied to `README.md` instead of
`docs/architecture.md`.

The portfolio-wide pattern (first authored in prompt-regression-suite#43) catches pre-shipping framing leaking (see `tests/test_architecture_doc.py` line 63).
The README had the same drift class for two section headers
("Tool dispatch (#2 · this PR)", "1000-doc benchmark (#4 · this
PR)") — both surfaces are shipped (the benchmark even has a real
numbers table in docs/benchmarks.md). This file locks the README
against that exact drift returning.

Issue #40.
"""

from __future__ import annotations

from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
README = REPO_ROOT / "README.md"

# Substrings, lowercase. Substring match (case-insensitive). Pinned in a
# tuple so a future loose edit can't silently drop one.
#
# This repo's README contains the legitimate prose "Bounded queue applies
# backpressure to this producer." — the substring "this pr" matches that
# innocuous line, so the canonical bare "this pr" pattern would false-
# positive here. We tighten to "· this pr" (U+00B7 middle dot + space +
# "this pr"): the exact shape the section-header drift takes
# ("## Foo (#N · this PR)"). The middle dot doesn't appear elsewhere in
# the README's prose; this stays loud against the real drift class
# without flagging "this producer", "this practice", "this print", etc.
BANNED_PHRASES = ("· this pr",)


@pytest.fixture(scope="module")
def readme_text_lower() -> str:
    return README.read_text(encoding="utf-8").lower()


def test_readme_exists() -> None:
    assert README.is_file(), f"README.md missing at {README}"


@pytest.mark.parametrize("phrase", BANNED_PHRASES)
def test_banned_phrase_absent(readme_text_lower: str, phrase: str) -> None:
    assert phrase not in readme_text_lower, (
        f"README contains banned phrase {phrase!r}. "
        "This is pre-shipping framing for surface that has already shipped; "
        "rewrite the section to its steady-state form."
    )


def test_banned_phrases_tuple_locked() -> None:
    # Hard-pin so a future loose edit of this test can't silently drop
    # one of the guards. Same shape as test_architecture_doc.py's
    # `test_banned_phrases_locked`.
    assert BANNED_PHRASES == ("· this pr",)
