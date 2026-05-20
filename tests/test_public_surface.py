"""Public-surface tests for ``async_pipelines/__init__.py``.

``async_pipelines`` re-exports 18 names from three submodules
(``benchmark``, ``core``, ``tool_dispatch``) and declares them in
``__all__`` + ``__version__``. Every other test in this suite imports
submodules directly (``from async_pipelines.core import process``), so
silent renames or accidental ``__all__`` drops in ``__init__.py`` don't
fail any test — but they break the README's FIVE quoted ``from
async_pipelines import …`` snippets and its quoted dotted-path
reference ``async_pipelines.tool_dispatch.dispatch_tool_calls``.

These five standalone + 3 parametrized tests lock the surface across
six orthogonal axes:

1. ``__version__`` is set to a semver-ish string.
2. Every name in ``__all__`` is bound on the package and non-None.
3. ``__all__`` agrees with the actual top-level relative ``from .X
   import …`` names (filter on ``level >= 1``).
4. Union of 7 names quoted across the README's five ``from
   async_pipelines import …`` snippets resolves at the top level.
5. README's quoted dotted-path
   ``async_pipelines.tool_dispatch.dispatch_tool_calls`` resolves to
   a callable — guards against ``tool_dispatch`` being renamed or
   ``dispatch_tool_calls`` being moved without updating the README.
6. One anchor per re-exported submodule (3 anchors).

Seventh strike of the portfolio-wide public-surface hygiene pattern.
Orthogonal to ``tests/test_bench_table_snapshot.py`` which locks the
README's numeric benchmark table; this test locks the Python public
surface the README's prose depends on.
"""

from __future__ import annotations

import ast
import importlib
import re
from pathlib import Path

import pytest

import async_pipelines

_INIT_PATH = Path(async_pipelines.__file__)
_SEMVER_PATTERN = re.compile(r"^\d+\.\d+\.\d+(?:[-+].+)?$")

# Union of names quoted across the README's five `from async_pipelines
# import …` snippets (lines 88, 107, 129, 188, 231 in README.md).
README_QUICKSTART_NAMES = (
    "process",
    "stream",
    "StreamMetrics",
    "PipelineTimeoutError",
    "ToolCall",
    "ToolRegistry",
    "dispatch_tool_calls",
)

# README quotes one Python dotted path by name (line 29):
#   `async_pipelines.tool_dispatch.dispatch_tool_calls`
README_DOTTED_PATHS = (("async_pipelines.tool_dispatch", "dispatch_tool_calls"),)

# Anchor names that prove each re-exported submodule survived.
SUBMODULE_ANCHORS = {
    "benchmark": "run_pipeline",
    "core": "process",
    "tool_dispatch": "dispatch_tool_calls",
}


def _parse_init_relative_imports() -> set[str]:
    """Return the set of names imported into ``__init__.py`` via
    top-level relative ``from .X import (...)`` blocks."""
    tree = ast.parse(_INIT_PATH.read_text(encoding="utf-8"))
    names: set[str] = set()
    for node in tree.body:
        if isinstance(node, ast.ImportFrom) and node.level >= 1:
            for alias in node.names:
                names.add(alias.asname or alias.name)
    return names


def test_version_is_set_to_semver_ish_string() -> None:
    """``__version__`` is published; downstream importers and PyPI
    builds rely on it."""
    assert hasattr(async_pipelines, "__version__"), (
        "async_pipelines.__version__ is missing — packaging tools and "
        "downstream `async_pipelines.__version__` lookups will break."
    )
    version = async_pipelines.__version__
    assert isinstance(version, str), (
        f"async_pipelines.__version__ should be a string, got "
        f"{type(version).__name__}: {version!r}."
    )
    assert version, "async_pipelines.__version__ is an empty string."
    assert _SEMVER_PATTERN.match(version), (
        f"async_pipelines.__version__ = {version!r} doesn't look like "
        f"semver (expected MAJOR.MINOR.PATCH[-prerelease][+build])."
    )


def test_all_names_are_bound_and_non_none() -> None:
    """Every name in ``__all__`` must be importable and non-None."""
    missing: list[str] = []
    none_valued: list[str] = []
    for name in async_pipelines.__all__:
        if not hasattr(async_pipelines, name):
            missing.append(name)
            continue
        if getattr(async_pipelines, name) is None:
            none_valued.append(name)
    assert not missing, (
        f"async_pipelines.__all__ advertises names that are not bound "
        f"on the package: {missing}. The most likely cause is a "
        f"re-import line was deleted from __init__.py but __all__ "
        f"wasn't updated."
    )
    assert not none_valued, (
        f"async_pipelines.__all__ entries bound to None: {none_valued}. "
        f"A re-import probably resolved to a missing submodule attribute."
    )


def test_all_matches_actual_top_level_imports() -> None:
    """``__all__`` should equal the set of top-level relative re-exports."""
    advertised = set(async_pipelines.__all__)
    imported = _parse_init_relative_imports()
    only_imported = imported - advertised
    only_advertised = advertised - imported
    assert not only_imported, (
        f"Names imported into async_pipelines/__init__.py but missing "
        f"from __all__: {sorted(only_imported)}. Add them to __all__ "
        f"or stop importing them at the top level."
    )
    assert not only_advertised, (
        f"Names in async_pipelines.__all__ but not imported at the top "
        f"of __init__.py: {sorted(only_advertised)}. Add the import or "
        f"remove the __all__ entry."
    )


def test_readme_quickstart_imports_resolve() -> None:
    """README's five quickstart import snippets must keep working.

    The README literally quotes (lines 88, 107, 129, 188, 231)::

        from async_pipelines import process
        from async_pipelines import stream
        from async_pipelines import StreamMetrics, stream
        from async_pipelines import process, PipelineTimeoutError
        from async_pipelines import ToolCall, ToolRegistry, dispatch_tool_calls

    If any of those seven unique names disappears from the top-level
    surface, every reader who copy-pastes a snippet hits an ImportError.
    """
    missing = [n for n in README_QUICKSTART_NAMES if not hasattr(async_pipelines, n)]
    assert not missing, (
        f"async_pipelines is missing README-quoted names: {missing}. "
        f"The README's quickstart snippets import them directly — "
        f"either restore the exports or update the README."
    )


@pytest.mark.parametrize(
    ("module_path", "attr"),
    README_DOTTED_PATHS,
    ids=[f"{m}.{a}" for m, a in README_DOTTED_PATHS],
)
def test_readme_dotted_path_resolves(module_path: str, attr: str) -> None:
    """README's quoted ``async_pipelines.tool_dispatch.dispatch_tool_calls``
    must keep resolving to a callable.

    The README literally quotes (line 29)::

        async_pipelines.tool_dispatch.dispatch_tool_calls(...) (#2)

    If ``tool_dispatch.py`` is renamed or ``dispatch_tool_calls`` is
    moved, that bullet silently lies. Locking the lookup here keeps
    prose ↔ code in sync.
    """
    module = importlib.import_module(module_path)
    assert hasattr(module, attr), (
        f"`{module_path}.{attr}` no longer resolves. The README quotes "
        f"it by name (around line 29) — either restore the export or "
        f"update the README."
    )
    obj = getattr(module, attr)
    assert callable(obj), (
        f"`{module_path}.{attr}` is no longer callable (got "
        f"{type(obj).__name__}). The README describes it as a function "
        f"call; the lookup must return a callable."
    )


@pytest.mark.parametrize(
    ("submodule", "anchor"),
    sorted(SUBMODULE_ANCHORS.items()),
    ids=sorted(SUBMODULE_ANCHORS.keys()),
)
def test_submodule_anchor_re_exported(submodule: str, anchor: str) -> None:
    """One anchor per re-exported submodule survives at the top level."""
    assert hasattr(async_pipelines, anchor), (
        f"`{anchor}` from `async_pipelines.{submodule}` is no longer "
        f"re-exported at the top level. Did `{submodule}` move or get "
        f"renamed? Update `async_pipelines/__init__.py` to re-export "
        f"from the new path."
    )
