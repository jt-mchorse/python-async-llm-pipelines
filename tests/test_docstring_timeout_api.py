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

from async_pipelines.core import process, stream
from async_pipelines.tool_dispatch import dispatch_tool_calls

# Every seam that takes a `timeout` (#178). This list was `[process,
# dispatch_tool_calls]`, and `stream` -- the third seam, named as one by
# `_require_timeout_seconds` -- had no `timeout` entry in its docstring at all.
_TIMEOUT_SEAMS = [process, stream, dispatch_tool_calls]
_IDS = [f.__name__ for f in _TIMEOUT_SEAMS]


@pytest.mark.parametrize("fn", _TIMEOUT_SEAMS, ids=_IDS)
def test_timeout_docstring_names_asyncio_timeout(fn) -> None:
    doc = inspect.getdoc(fn) or ""
    assert "asyncio.timeout" in doc, (
        f"{fn.__name__} docstring must document the timed path as "
        f"`asyncio.timeout` (the post-#66 implementation)."
    )


@pytest.mark.parametrize("fn", _TIMEOUT_SEAMS, ids=_IDS)
def test_timeout_docstring_does_not_name_wait_for(fn) -> None:
    doc = inspect.getdoc(fn) or ""
    assert "wait_for" not in doc, (
        f"{fn.__name__} docstring names `asyncio.wait_for` as the timed-path "
        f"contract, but the implementation uses `asyncio.timeout` (#66/#72). "
        f"The contrast explanation belongs in an inline comment, not the docstring."
    )


@pytest.mark.parametrize("fn", _TIMEOUT_SEAMS, ids=_IDS)
def test_every_parameter_has_an_args_entry(fn) -> None:
    """The shape that let `stream`'s `timeout` go undocumented (#178): the
    other five parameters had entries, so the section read as complete."""
    doc = inspect.getdoc(fn) or ""
    args = doc[doc.index("Args:") :]
    missing = [p for p in inspect.signature(fn).parameters if f"{p}:" not in args]
    assert not missing, f"{fn.__name__} docstring `Args:` has no entry for {missing}"


@pytest.mark.parametrize("fn", _TIMEOUT_SEAMS, ids=_IDS)
def test_timeout_docstring_names_the_error_it_raises(fn) -> None:
    doc = inspect.getdoc(fn) or ""
    assert "PipelineTimeoutError" in doc, fn.__name__


def test_stream_timeout_index_is_documented_as_consumption_order() -> None:
    """`process`'s `.index` is the input position; `stream`'s is not (D-003).
    A reader carrying the `process` meaning over looks up the wrong item."""
    doc = inspect.getdoc(stream) or ""
    entry = " ".join(doc[doc.index("timeout:") :].replace("*", "").split())
    assert "consumption order" in entry, entry
