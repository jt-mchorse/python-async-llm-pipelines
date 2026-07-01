"""Docstring lock: the timed path names ``asyncio.timeout``, not ``wait_for``.

Issue #72. This is a "reference async patterns" repo — the per-item /
per-tool ``timeout`` docstring is the contract a reader copies. Post-#66
the implementation uses ``asyncio.timeout()`` + ``cm.expired()`` (not
``asyncio.wait_for``) so that only the deadline's *own* firing maps to
``PipelineTimeoutError`` and ``fn``'s own inner ``TimeoutError`` (a
downstream socket/httpx timeout) is not relabeled. The ``process`` and
``dispatch_tool_calls`` docstrings had drifted, still naming the pre-#66
``asyncio.wait_for``.

``inspect.getdoc`` returns only the function's own docstring — the two
intentional ``wait_for()`` *contrast* comments (``core.py`` and
``tool_dispatch.py``, explaining why #66 moved away) live in inline code
comments, not the docstring, so this lock does not fight them.
"""

from __future__ import annotations

import inspect

import pytest

from async_pipelines.core import process
from async_pipelines.tool_dispatch import dispatch_tool_calls


@pytest.mark.parametrize(
    "fn", [process, dispatch_tool_calls], ids=["process", "dispatch_tool_calls"]
)
def test_timeout_docstring_names_asyncio_timeout(fn) -> None:
    doc = inspect.getdoc(fn) or ""
    assert "asyncio.timeout" in doc, (
        f"{fn.__name__} docstring must document the timed path as "
        f"`asyncio.timeout` (the post-#66 implementation)."
    )


@pytest.mark.parametrize(
    "fn", [process, dispatch_tool_calls], ids=["process", "dispatch_tool_calls"]
)
def test_timeout_docstring_does_not_name_wait_for(fn) -> None:
    doc = inspect.getdoc(fn) or ""
    assert "wait_for" not in doc, (
        f"{fn.__name__} docstring names `asyncio.wait_for` as the timed-path "
        f"contract, but the implementation uses `asyncio.timeout` (#66/#72). "
        f"The contrast explanation belongs in an inline comment, not the docstring."
    )
