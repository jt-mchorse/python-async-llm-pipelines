"""A README snippet's call to a function the README defines must bind (#159).

#115 gave the README's `stream` producer a required `max_messages` argument and
updated the call beneath it to `items_from_kafka(10_000)`. The StreamMetrics
example further down calls the same producer, and kept `items_from_kafka()`, a
`TypeError` before `stream` ever ran. `test_readme_kwarg_consistency.py` checks
the kwargs handed to `process`/`stream`, not calls to the helpers the README
writes for itself, so nothing noticed.

This lock collects every function a ```python block defines (any block, since
the snippets build on each other) and binds every call to one of them against
that definition's signature. Undefined placeholders such as `consumer` or
`docs` are deliberately out of scope: they are pseudo-code, not a mismatch.
"""

from __future__ import annotations

import ast
import inspect
import re
from pathlib import Path

README = Path(__file__).resolve().parent.parent / "README.md"
_PY_BLOCK = re.compile(r"```python\n(.*?)```", re.DOTALL)


def _blocks() -> list[ast.Module]:
    return [ast.parse(src) for src in _PY_BLOCK.findall(README.read_text(encoding="utf-8"))]


def _signature(node: ast.FunctionDef | ast.AsyncFunctionDef) -> inspect.Signature:
    ns: dict[str, object] = {}
    exec(f"def {node.name}({ast.unparse(node.args)}): pass", ns)  # noqa: S102 - README's own signature
    return inspect.signature(ns[node.name])  # type: ignore[arg-type]


def _calls_to_readme_functions() -> list[tuple[str, inspect.Signature, ast.Call]]:
    blocks = _blocks()
    defs = {
        node.name: node
        for block in blocks
        for node in ast.walk(block)
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
    }
    calls = []
    for block in blocks:
        for node in ast.walk(block):
            if (
                isinstance(node, ast.Call)
                and isinstance(node.func, ast.Name)
                and node.func.id in defs
            ):
                calls.append((ast.unparse(node), _signature(defs[node.func.id]), node))
    return calls


def test_the_readme_defines_and_calls_its_own_helpers() -> None:
    """Anti-vacuity: the lock below has calls to check, including the #159 one."""
    rendered = [text for text, _sig, _node in _calls_to_readme_functions()]
    assert sum(text.startswith("items_from_kafka(") for text in rendered) >= 2, rendered


def test_every_call_to_a_readme_function_binds() -> None:
    bad = []
    for text, sig, call in _calls_to_readme_functions():
        if any(isinstance(a, ast.Starred) for a in call.args) or any(
            k.arg is None for k in call.keywords
        ):
            continue
        try:
            sig.bind(*[None] * len(call.args), **{k.arg: None for k in call.keywords})
        except TypeError as exc:
            bad.append(f"{text}: {exc}")
    assert not bad, bad
