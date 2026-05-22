"""README ↔ code kwarg-name consistency lock (#21).

Two failure modes the snapshot guards against:

1. The architecture block at the top of the README documents each
   primitive's signature as a one-line code fence. If a kwarg there
   doesn't actually exist on `inspect.signature(process)` or
   `inspect.signature(stream)`, a reader copies a wrong invocation and
   hits `TypeError: process() got an unexpected keyword argument …`.
2. The prose examples lower in the README call `process(...)` and
   `stream(...)` with real-looking kwargs. Same drift potential — and
   silent until somebody pastes the snippet into a REPL.

The fix this test guards (#21) renamed the architecture-block kwarg from
`per_item_timeout` to `timeout` to match the code. Without this test,
the same drift would recur the next time someone touches either side.
"""

from __future__ import annotations

import inspect
import re
from pathlib import Path

import pytest

from async_pipelines import process, stream

REPO_ROOT = Path(__file__).resolve().parent.parent
README = REPO_ROOT / "README.md"

_PROCESS_PARAMS = set(inspect.signature(process).parameters)
_STREAM_PARAMS = set(inspect.signature(stream).parameters)


@pytest.fixture(scope="module")
def readme_text() -> str:
    return README.read_text(encoding="utf-8")


# Architecture-block lines look like:
#   │   ├── process(items, fn, *, concurrency, return_exceptions=False, timeout=None) -> list
#   │   └── stream(producer, fn, *, concurrency, queue_size, return_exceptions=False, timeout=None, metrics=None) -> list
_ARCH_PROCESS_RE = re.compile(r"process\(([^)]+)\)\s*->\s*list", re.MULTILINE)
_ARCH_STREAM_RE = re.compile(r"stream\(([^)]+)\)\s*->\s*list", re.MULTILINE)


def _kwargs_from_args_str(args_str: str) -> set[str]:
    """Pull keyword names out of an argument-list string.

    Tolerates positional names (`items`, `fn`, `producer`) — those map to
    real parameters too — and the keyword-only marker (`*`). Returns the
    set of all named identifiers that occur in the signature, dropping
    `*` and `**`-prefixed entries.
    """
    out: set[str] = set()
    for raw in args_str.split(","):
        token = raw.strip()
        if not token or token == "*":
            continue
        # Drop default value if present.
        name = token.split("=", 1)[0].strip()
        # Drop the kwarg-only marker character itself.
        if name == "*":
            continue
        # Drop *args / **kwargs (we don't use them in this surface).
        if name.startswith("*"):
            continue
        out.add(name)
    return out


def test_arch_block_process_signature_matches_code(readme_text: str) -> None:
    m = _ARCH_PROCESS_RE.search(readme_text)
    assert m is not None, "README architecture block must contain a `process(...) -> list` line"
    arch_names = _kwargs_from_args_str(m.group(1))
    unknown = arch_names - _PROCESS_PARAMS
    assert not unknown, (
        f"README architecture block names parameters that don't exist on `process`: "
        f"{sorted(unknown)}. Real `process` params: {sorted(_PROCESS_PARAMS)}. "
        f"Fix the README to match the function signature (or, if the function "
        f"signature legitimately changed, update this snapshot)."
    )


def test_arch_block_stream_signature_matches_code(readme_text: str) -> None:
    m = _ARCH_STREAM_RE.search(readme_text)
    assert m is not None, "README architecture block must contain a `stream(...) -> list` line"
    arch_names = _kwargs_from_args_str(m.group(1))
    unknown = arch_names - _STREAM_PARAMS
    assert not unknown, (
        f"README architecture block names parameters that don't exist on `stream`: "
        f"{sorted(unknown)}. Real `stream` params: {sorted(_STREAM_PARAMS)}. "
        f"Fix the README to match the function signature (or, if the function "
        f"signature legitimately changed, update this snapshot)."
    )


# Call-site kwargs in ```python``` fences: e.g.
#   await process(docs, call_llm, concurrency=10, timeout=5.0)
# We match the function name + opening paren, then balance-parse to the
# matching close, then extract keyword names from the slice.
_FENCE_RE = re.compile(r"```python\n(.*?)\n```", re.DOTALL)


def _extract_kwargs_at_callsites(code: str, fn_name: str) -> set[str]:
    """For every `fn_name(...)` call in `code`, return the union of
    keyword argument names used at those call sites.
    """
    names: set[str] = set()
    needle = f"{fn_name}("
    i = 0
    while True:
        j = code.find(needle, i)
        if j < 0:
            return names
        # Find matching close paren via depth counter.
        depth = 0
        start = j + len(needle)
        k = start
        while k < len(code):
            ch = code[k]
            if ch == "(":
                depth += 1
            elif ch == ")":
                if depth == 0:
                    break
                depth -= 1
            k += 1
        if k >= len(code):
            return names
        args_str = code[start:k]
        # Pull out each comma-separated arg's `name=` prefix at depth 0.
        depth = 0
        token = ""
        tokens: list[str] = []
        for ch in args_str + ",":
            if ch in "([{":
                depth += 1
                token += ch
            elif ch in ")]}":
                depth -= 1
                token += ch
            elif ch == "," and depth == 0:
                if token.strip():
                    tokens.append(token.strip())
                token = ""
            else:
                token += ch
        for t in tokens:
            if "=" in t:
                name = t.split("=", 1)[0].strip()
                # Comprehension or default with `==` would slip in here;
                # filter to a bare identifier.
                if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", name):
                    names.add(name)
        i = k + 1


def test_readme_python_call_sites_use_real_process_kwargs(readme_text: str) -> None:
    for fence in _FENCE_RE.findall(readme_text):
        used = _extract_kwargs_at_callsites(fence, "process")
        unknown = used - _PROCESS_PARAMS
        assert not unknown, (
            f"README ```python``` fence uses `process({', '.join(sorted(unknown))}=...)`, "
            f"which is not a real parameter on `process`. Real params: "
            f"{sorted(_PROCESS_PARAMS)}. Either fix the call site or update "
            f"`async_pipelines.core.process`."
        )


def test_readme_python_call_sites_use_real_stream_kwargs(readme_text: str) -> None:
    for fence in _FENCE_RE.findall(readme_text):
        used = _extract_kwargs_at_callsites(fence, "stream")
        unknown = used - _STREAM_PARAMS
        assert not unknown, (
            f"README ```python``` fence uses `stream({', '.join(sorted(unknown))}=...)`, "
            f"which is not a real parameter on `stream`. Real params: "
            f"{sorted(_STREAM_PARAMS)}. Either fix the call site or update "
            f"`async_pipelines.core.stream`."
        )
