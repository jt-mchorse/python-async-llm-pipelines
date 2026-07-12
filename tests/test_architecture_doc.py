"""Architecture-doc lock: catch drift between `docs/architecture.md` and
the actual shipped surface of the repo.

Sister to the architecture-doc locks shipped this same week in
``embedding-model-shootout`` (PR #20), ``vector-search-at-scale``
(PR #22), ``llm-eval-harness`` (PR #30), ``prompt-regression-suite``
(PR #25), ``llm-cost-optimizer`` (PR #28), ``rag-production-kit``
(PR #30), and ``chunking-strategies-lab`` (PR #22), plus the JS
variants.

This doc uses BOTH ``(#NN)`` issue references AND ``D-NNN`` core-decision
references, so coverage is dual-axis (compare:
``chunking-strategies-lab`` PR #22 D-NNN-only;
``embedding-model-shootout`` PR #20 #NN-only).

Five invariants pinned (plus the #70 ``timeout`` kwarg-name lock in
``test_architecture_doc_uses_real_timeout_kwarg_name``):

1. **Path-token reachability.** Every backtick-quoted token starting with
   one of ``RESOLVABLE_PREFIXES`` resolves on disk. Operator-supplied
   future artifacts allow-listed in ``OPERATOR_SUPPLIED_PATHS``.
   Placeholder shapes ``<...>``, ``{...}``, glob ``*`` skipped.

2. **Closed-feature-issue coverage.** Every issue in
   ``KNOWN_SHIPPED_ISSUES`` referenced at least once.

3. **Active-decision coverage.** Every non-superseded ``D-NNN >= 2`` in
   ``MEMORY/core_decisions_ai.md`` referenced at least once.

4. **Banned-phrase absence.**

5. **Symbol-reference resolution** (portfolio-ops #55). Every symbol the
   doc *names* — a ``<submodule>.<symbol>`` attribute ref or a multi-word
   CamelCase public type — resolves to a real attribute of the
   ``async_pipelines`` package, one of its submodules, or the Python
   ``builtins``. The #70 kwarg lock guards one kwarg *name*; this guards
   every named *type*. Catches the drift class #55 catalogued portfolio-wide
   (a doc naming a nonexistent type such as llm-cost-optimizer's
   ``BatchAPIBackend`` stays CI-green). Propagates the
   embedding-model-shootout #71 / llm-eval-harness #140 /
   chunking-strategies-lab #104 / prompt-regression-suite #103 /
   vector-search-at-scale #74 precedents.

Hard-pin tests lock each constant.
"""

from __future__ import annotations

import inspect
import re
from pathlib import Path

import pytest

from async_pipelines import process, stream

REPO_ROOT = Path(__file__).resolve().parent.parent
DOC = REPO_ROOT / "docs" / "architecture.md"
DECISIONS = REPO_ROOT / "MEMORY" / "core_decisions_ai.md"


BANNED_PHRASES = (
    "this pr",
    "pending downstream",
    "(unfiled)",
    "to-be-filed",
)


RESOLVABLE_PREFIXES = (
    "async_pipelines/",
    "scripts/",
    "docs/",
    "tests/",
    ".github/",
)


OPERATOR_SUPPLIED_PATHS: tuple[str, ...] = ()


KNOWN_SHIPPED_ISSUES = (1, 2, 3, 4, 5)


MIN_ACTIVE_DECISION_ID = 2


# Symbol-resolution lock (portfolio-ops #55). `async_pipelines` is a flat
# package (no subpackages today); `_SUBPACKAGES` is kept as an explicit,
# hard-pinned empty tuple so adding one later is a deliberate widening.
_PKG = "async_pipelines"
_PKG_DIR = REPO_ROOT / _PKG
_SUBPACKAGES: tuple[str, ...] = ()

# File-suffix tokens that look like a `<name>.<attr>` symbol reference but are
# really filenames (`core.py`, `benchmark.py`). Excluded from the dotted-symbol
# resolution check so a filename isn't mistaken for a submodule attribute.
# Hard-pinned by `test_symbol_skip_extensions_hard_pin_set`.
SYMBOL_SKIP_EXTENSIONS = ("py", "sqlite", "json", "md", "txt", "yaml", "yml", "sh", "toml")


@pytest.fixture(scope="module")
def doc_text() -> str:
    return DOC.read_text(encoding="utf-8")


@pytest.fixture(scope="module")
def active_decisions() -> tuple[int, ...]:
    text = DECISIONS.read_text(encoding="utf-8")
    blocks = re.split(r"\n(?=- id:)", text)
    active: list[int] = []
    for block in blocks:
        id_match = re.search(r"- id:\s*D-(\d+)", block)
        if not id_match:
            continue
        sup_match = re.search(r"superseded_by:\s*(\S+)", block)
        is_active = (sup_match is None) or (sup_match.group(1).strip().lower() == "null")
        if is_active:
            n = int(id_match.group(1))
            if n >= MIN_ACTIVE_DECISION_ID:
                active.append(n)
    return tuple(sorted(active))


def _extract_backtick_paths(text: str) -> set[str]:
    found: set[str] = set()
    for match in re.finditer(r"`([^`\n]+)`", text):
        token = match.group(1).strip()
        for prefix in RESOLVABLE_PREFIXES:
            if token.startswith(prefix):
                while token and token[-1] in ".,;:":
                    token = token[:-1]
                token = re.sub(r"\(\)$", "", token)
                if "<" in token or "{" in token or "*" in token:
                    break
                if token:
                    found.add(token)
                break
    return found


def _resolves_on_disk(token: str) -> bool:
    return (REPO_ROOT / token).exists()


def _package_symbol_resolves(name: str) -> bool:
    """True if `name` is an attribute of the `async_pipelines` package, any of
    its `*.py` submodules, a listed subpackage, or the Python `builtins`.

    Submodule coverage catches symbols not re-exported at package level.
    Builtins are included so a doc that legitimately names ``KeyboardInterrupt``
    resolves without a hand-maintained allow-list that rots.
    """
    import builtins
    import importlib

    if hasattr(builtins, name):
        return True
    pkg = importlib.import_module(_PKG)
    if hasattr(pkg, name):
        return True
    module_names = [f"{_PKG}.{p.stem}" for p in _PKG_DIR.glob("*.py") if p.stem != "__init__"]
    module_names += [f"{_PKG}.{sub}" for sub in _SUBPACKAGES]
    for module_name in module_names:
        try:
            module = importlib.import_module(module_name)
        except ModuleNotFoundError:
            continue
        if hasattr(module, name):
            return True
    return False


def _extract_symbol_refs(text: str) -> tuple[set[str], set[str]]:
    """Split backtick-quoted tokens into the two symbol-citation styles the doc
    uses, so the resolver only checks genuine symbol claims. Returns
    ``(dotted, camel)``.

    - ``dotted``: ``<submodule>.<symbol>`` where ``<submodule>`` is a real
      ``async_pipelines/*.py`` module stem and the token is not a filename
      (dropped via ``SYMBOL_SKIP_EXTENSIONS``). Package-qualified refs
      (``async_pipelines.core``), stdlib refs (``asyncio.Queue``), and
      cross-repo refs (``eval_harness.io_utils``) are skipped: their prefix is
      not a submodule stem.
    - ``camel``: a *multi-word* CamelCase identifier (an internal
      lowercase->uppercase boundary, e.g. ``RunResult``, ``ToolRegistry``,
      ``FakeLLM``). Single-word / no-boundary tokens (``None``) are excluded:
      they collide with prose. Bare snake_case is not locked.
    """
    submodules = {p.stem for p in _PKG_DIR.glob("*.py") if p.stem != "__init__"}
    dotted: set[str] = set()
    camel: set[str] = set()
    for match in re.finditer(r"`([^`\n]+)`", text):
        token = match.group(1).strip()
        token = re.sub(r"\(\)$", "", token)
        while token and token[-1] in ".,;:":
            token = token[:-1]
        dotted_match = re.fullmatch(r"([a-z_]+)\.([A-Za-z_][A-Za-z0-9_]*)", token)
        if dotted_match:
            module, attr = dotted_match.group(1), dotted_match.group(2)
            if module in submodules and attr not in SYMBOL_SKIP_EXTENSIONS:
                dotted.add(token)
            continue
        if re.fullmatch(r"[A-Z][A-Za-z0-9]*[a-z][A-Za-z0-9]*", token) and re.search(
            r"[a-z][A-Z]", token
        ):
            camel.add(token)
    return dotted, camel


def test_doc_exists() -> None:
    assert DOC.exists(), f"missing {DOC}"


def test_decisions_file_exists() -> None:
    assert DECISIONS.exists(), f"missing {DECISIONS}"


def test_backtick_paths_resolve_on_disk(doc_text: str) -> None:
    tokens = _extract_backtick_paths(doc_text)
    operator_set = set(OPERATOR_SUPPLIED_PATHS)
    unresolved = sorted(t for t in tokens if not _resolves_on_disk(t) and t not in operator_set)
    assert not unresolved, (
        "docs/architecture.md quotes paths that don't exist on disk:\n"
        + "\n".join(f"  - `{t}`" for t in unresolved)
        + "\n(regenerate the doc to match the current layout, fix the typo, "
        "or — if this is an operator-supplied future artifact — add it to "
        "OPERATOR_SUPPLIED_PATHS in tests/test_architecture_doc.py)"
    )


def test_doc_symbol_refs_resolve(doc_text: str) -> None:
    """Every symbol the doc names resolves to a real attribute (portfolio-ops #55).

    ``test_backtick_paths_resolve_on_disk`` validates slash-path tokens only and
    the #70 lock guards one kwarg *name*; a *symbol* reference — a
    ``<submodule>.<symbol>`` attribute or a multi-word CamelCase public type —
    was unguarded. That is exactly the drift class #55 catalogued (a doc naming
    a nonexistent ``BatchAPIBackend`` / ``compute_frontier`` stays CI-green).
    Inverse-verified by ``test_symbol_resolver_flags_injected_drift``.
    """
    import importlib

    dotted, camel = _extract_symbol_refs(doc_text)
    assert dotted or camel, (
        "expected at least one symbol reference (`<submodule>.<symbol>` or a "
        "multi-word CamelCase type) in docs/architecture.md — the resolver "
        "would otherwise be vacuously green"
    )

    unresolved: list[str] = []
    for token in sorted(dotted):
        module_name, _, symbol = token.rpartition(".")
        try:
            module = importlib.import_module(f"{_PKG}.{module_name}")
        except ModuleNotFoundError:
            unresolved.append(f"{token} (module {_PKG}.{module_name} not importable)")
            continue
        if not hasattr(module, symbol):
            unresolved.append(token)
    for token in sorted(camel):
        if not _package_symbol_resolves(token):
            unresolved.append(f"{token} (not an async_pipelines symbol or a builtin)")

    assert not unresolved, (
        "docs/architecture.md names symbols that don't exist in the package:\n"
        + "\n".join(f"  - {u}" for u in unresolved)
        + "\n(fix the doc to match the shipped symbol, or update the rename that "
        "orphaned it)"
    )


def test_symbol_resolver_flags_injected_drift() -> None:
    """Inverse safety net: a nonexistent CamelCase type in doc text is flagged.

    Guards against a vacuously-green resolver — if a refactor ever neutered
    extraction or resolution, this fails. Mirrors the #55 drift shape while a
    real symbol in the same string still resolves.
    """
    fake = "The `NonexistentToolRegistry` yields a `RunResult`."
    dotted, camel = _extract_symbol_refs(fake)
    assert "NonexistentToolRegistry" in camel
    assert "RunResult" in camel
    unresolved = sorted(t for t in camel if not _package_symbol_resolves(t))
    assert unresolved == ["NonexistentToolRegistry"]


def test_symbol_skip_extensions_hard_pin_set() -> None:
    assert SYMBOL_SKIP_EXTENSIONS == (
        "py",
        "sqlite",
        "json",
        "md",
        "txt",
        "yaml",
        "yml",
        "sh",
        "toml",
    )


def test_symbol_subpackages_hard_pin_set() -> None:
    assert _SUBPACKAGES == ()


def test_operator_supplied_paths_actually_absent() -> None:
    landed = [p for p in OPERATOR_SUPPLIED_PATHS if (REPO_ROOT / p).exists()]
    assert not landed, (
        "These paths are listed as operator-supplied in "
        "tests/test_architecture_doc.py but exist on disk:\n"
        + "\n".join(f"  - `{p}`" for p in landed)
        + "\n(drop them from OPERATOR_SUPPLIED_PATHS so the resolvability "
        "check covers them as literal paths)"
    )


def test_every_shipped_issue_referenced(doc_text: str) -> None:
    referenced = {int(m.group(1)) for m in re.finditer(r"#(\d+)\b", doc_text)}
    missing = sorted(set(KNOWN_SHIPPED_ISSUES) - referenced)
    assert not missing, (
        "docs/architecture.md doesn't reference these closed-feature-issues "
        "even once:\n"
        + "\n".join(f"  - #{n}" for n in missing)
        + "\n(every shipped layer should have its origin issue annotated "
        "in the doc; add a `(#NN)` to the relevant component bullet or "
        "diagram node)"
    )


def test_every_active_decision_referenced(doc_text: str, active_decisions: tuple[int, ...]) -> None:
    referenced = {int(m.group(1)) for m in re.finditer(r"\bD-0*(\d+)\b", doc_text)}
    missing = sorted(set(active_decisions) - referenced)
    assert not missing, (
        "docs/architecture.md doesn't reference these active "
        "(non-superseded) core decisions even once:\n"
        + "\n".join(f"  - D-{n:03d}" for n in missing)
        + "\n(every shipped layer / posture in MEMORY/core_decisions_ai.md "
        "should be annotated in the doc where the relevant code lives; "
        "add a `D-NNN` reference to the relevant bullet)"
    )


def test_no_banned_phrases(doc_text: str) -> None:
    lowered = doc_text.lower()
    hits = [p for p in BANNED_PHRASES if p in lowered]
    assert not hits, (
        "docs/architecture.md contains drift phrases:\n"
        + "\n".join(f"  - {p!r}" for p in hits)
        + "\n(these phrases describe a pre-shipping state; the doc is a "
        "steady-state reference, not a PR description)"
    )


def test_banned_phrases_hard_pin_set() -> None:
    assert BANNED_PHRASES == (
        "this pr",
        "pending downstream",
        "(unfiled)",
        "to-be-filed",
    )


def test_resolvable_prefixes_hard_pin_set() -> None:
    assert RESOLVABLE_PREFIXES == (
        "async_pipelines/",
        "scripts/",
        "docs/",
        "tests/",
        ".github/",
    )


def test_known_shipped_issues_hard_pin_set() -> None:
    assert KNOWN_SHIPPED_ISSUES == (1, 2, 3, 4, 5)


def test_min_active_decision_id_hard_pin() -> None:
    assert MIN_ACTIVE_DECISION_ID == 2


def test_operator_supplied_paths_hard_pin_set() -> None:
    assert OPERATOR_SUPPLIED_PATHS == ()


def test_architecture_doc_uses_real_timeout_kwarg_name(doc_text: str) -> None:
    """The per-item timeout kwarg is named ``timeout`` on both ``process`` and
    ``stream``; architecture.md must not resurrect the pre-#21 ``per_item_timeout``
    name (#70).

    This mirrors ``tests/test_readme_kwarg_consistency.py`` (which locks the
    same kwarg in the README under #21) — but that test only reads README.md,
    so the identical drift survived in docs/architecture.md uncaught. Derive the
    real name from the live signatures so the lock can't itself go stale.
    """
    process_params = set(inspect.signature(process).parameters)
    stream_params = set(inspect.signature(stream).parameters)
    # The real kwarg is `timeout`; `per_item_timeout` is not a parameter.
    assert "timeout" in process_params
    assert "timeout" in stream_params
    assert "per_item_timeout" not in (process_params | stream_params)
    # So the doc must use `timeout`, not the nonexistent `per_item_timeout` —
    # a reader copying `process(..., per_item_timeout=5.0)` hits a TypeError.
    assert "per_item_timeout" not in doc_text, (
        "docs/architecture.md references a `per_item_timeout` kwarg that doesn't "
        "exist on process()/stream(); the real keyword-only arg is `timeout` "
        "(renamed in #21 for the README; same drift recurred here)."
    )


def test_architecture_doc_streammetrics_fields_are_real(doc_text: str) -> None:
    """The §3 mermaid node enumerates ``StreamMetrics`` fields; they must match
    the real dataclass. Pre-#80 the node listed ``queue_depth_samples`` /
    ``producer_blocked_seconds`` / ``consumer_idle_seconds`` — none of which
    exist on the dataclass (a reader grepping for them finds nothing).

    Derive the real field set from ``dataclasses.fields`` so the lock can't go
    stale: every public field must be named in the doc, and no non-existent
    field name may appear.
    """
    import dataclasses

    from async_pipelines import StreamMetrics

    real_fields = {f.name for f in dataclasses.fields(StreamMetrics) if not f.name.startswith("_")}
    # Every real public field is named in the architecture doc's mermaid node,
    # so the diagram can't silently drop or rename one vs the dataclass.
    for name in real_fields:
        assert name in doc_text, (
            f"docs/architecture.md StreamMetrics node is missing the real field {name!r}"
        )
    # The pre-#80 invented names must never reappear.
    for invented in ("queue_depth_samples", "producer_blocked_seconds", "consumer_idle_seconds"):
        assert invented not in doc_text, (
            f"docs/architecture.md references a non-existent StreamMetrics field {invented!r}"
        )


def test_architecture_doc_names_the_real_timeout_exception(doc_text: str) -> None:
    """The §5 timeout diagram must name the exception the caller actually sees.
    `stream`/`process` relabel an expired `asyncio.timeout` as
    `PipelineTimeoutError` (core.py) — that's what is captured at the item index
    and what the README tells users to catch. Pre-#80 the diagram labeled the
    captured node `asyncio.TimeoutError`, a type the caller never receives.
    """
    from async_pipelines import PipelineTimeoutError

    assert PipelineTimeoutError.__name__ in doc_text, (
        "docs/architecture.md timeout diagram must name PipelineTimeoutError "
        "(the relabeled exception the caller catches), not the raw asyncio one"
    )
