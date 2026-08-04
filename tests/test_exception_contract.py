"""Lock tests for #90: what each entry point raises on a fail-fast failure.

The three public entry points disagree, `dispatch_tool_calls` has a third shape
its own docstring ruled out, and the comment above its re-raise claimed "parity
with async_pipelines.process()" while achieving the opposite.

Measured on `main` (341cecd), same `fn` raising `ValueError("kaboom")`:

===================================  ========================================
entry point                          caller catches
===================================  ========================================
``process``                          ``ExceptionGroup`` wrapping the original
``stream``                           ``ExceptionGroup`` wrapping the original
``dispatch_tool_calls`` (tool)       ``PipelineError``; type only in ``__cause__``
``dispatch_tool_calls`` (bad name)   ``ToolNotFoundError`` — not a ``PipelineError``
===================================  ========================================

None of the four is changed here. The last row looks like an oversight and I
built the fix — wrap it in `PipelineError` like the branch nine lines below —
before finding `test_dispatch_unknown_tool_fail_fast_raises_immediately`, which
names and pins `ToolNotFoundError` deliberately. So the code is right and the
*docstring* was wrong: it claimed all fail-fast failures arrive wrapped. The
docs now describe all three shapes, these tests pin them, and whether they
should be unified went to the maintainer as #90.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from async_pipelines import process, stream
from async_pipelines.core import PipelineError
from async_pipelines.tool_dispatch import (
    ToolCall,
    ToolNotFoundError,
    ToolRegistry,
    dispatch_tool_calls,
)


async def _boom(_):  # noqa: ANN001, ANN202
    raise ValueError("kaboom")


async def _agen(items):  # noqa: ANN001, ANN202
    for item in items:
        yield item


def _registry_with_boom() -> ToolRegistry:
    reg = ToolRegistry()
    reg.register("ok", _boom)
    return reg


# --- rows 1 and 2: the primitives let the ExceptionGroup through -------------


@pytest.mark.asyncio
async def test_process_surfaces_the_original_type_through_except_star() -> None:
    caught: list[BaseException] = []
    try:
        await process([1, 2], _boom, concurrency=2)
    except* ValueError as eg:
        caught.extend(eg.exceptions)
    assert caught, "`except* ValueError` must match a failing fn in process()"
    assert all(isinstance(e, ValueError) for e in caught)


@pytest.mark.asyncio
async def test_stream_surfaces_the_original_type_through_except_star() -> None:
    caught: list[BaseException] = []
    try:
        await stream(_agen([1, 2]), _boom, concurrency=2, queue_size=2)
    except* ValueError as eg:
        caught.extend(eg.exceptions)
    assert caught, "`except* ValueError` must match a failing fn in stream()"


@pytest.mark.asyncio
async def test_a_bare_except_on_the_original_type_does_not_match() -> None:
    # The docstring used to say the exception "propagates", which reads as a
    # bare `except ValueError`. It doesn't. Pinning the negative so the
    # corrected wording can't quietly regress.
    with pytest.raises(BaseException) as exc:  # noqa: PT011 - the point is the shape
        await process([1], _boom, concurrency=1)
    assert isinstance(exc.value, ExceptionGroup)
    assert not isinstance(exc.value, ValueError)


# --- row 3: the dispatcher wraps (current behaviour, deliberately pinned) ----


@pytest.mark.asyncio
async def test_dispatch_wraps_a_failing_tool_in_pipeline_error() -> None:
    with pytest.raises(PipelineError) as exc:
        await dispatch_tool_calls([ToolCall(id="1", name="ok")], registry=_registry_with_boom())
    # The original type survives only here — that asymmetry with process() is
    # the open question in #90, not something these tests take a side on.
    assert isinstance(exc.value.__cause__, ValueError)


# --- row 4: the third shape, pinned as-is ------------------------------------


@pytest.mark.asyncio
async def test_unknown_tool_name_raises_bare_tool_not_found_error() -> None:
    """The shape the docstring used to rule out.

    `registry.get` runs in the pre-resolve loop, outside the TaskGroup, so
    nothing wraps it. Deliberate — `test_dispatch_unknown_tool_fail_fast_
    raises_immediately` pins it by name — but the docstring said "the first
    exception propagates wrapped in PipelineError", so a caller who followed
    it and wrote `except PipelineError` was uncovered on the case most likely
    to happen, since tool names come from a model (#90).
    """
    with pytest.raises(ToolNotFoundError) as exc:
        await dispatch_tool_calls([ToolCall(id="1", name="nope")], registry=_registry_with_boom())
    assert not isinstance(exc.value, PipelineError)
    assert isinstance(exc.value, KeyError)  # so an upstack `except KeyError` swallows it


@pytest.mark.asyncio
async def test_unknown_tool_name_under_return_exceptions_is_unchanged() -> None:
    results = await dispatch_tool_calls(
        [ToolCall(id="1", name="nope")],
        registry=_registry_with_boom(),
        return_exceptions=True,
    )
    assert len(results) == 1
    assert results[0].ok is False
    assert results[0].tool_call_id == "1"
    assert "ToolNotFoundError" in (results[0].error_repr or "")


@pytest.mark.asyncio
async def test_a_registered_tool_alongside_an_unknown_one_fails_before_running_either() -> None:
    # The pre-resolve loop's whole purpose: a bad name is caught up-front, so
    # the registered tool never runs. Pinned because it is the reason the raise
    # sits outside the TaskGroup, which is why it is unwrapped.
    ran = False

    async def _fine(_):  # noqa: ANN001, ANN202
        nonlocal ran
        ran = True
        return "value"

    reg = ToolRegistry()
    reg.register("fine", _fine)
    with pytest.raises(ToolNotFoundError):
        await dispatch_tool_calls(
            [ToolCall(id="1", name="fine"), ToolCall(id="2", name="nope")],
            registry=reg,
        )
    assert ran is False


# --- the docs may not re-assert the parity that doesn't hold -----------------


def test_docs_name_every_shape_they_describe() -> None:
    """The comment saying "parity with async_pipelines.process()" is why this
    went unnoticed — it named the goal while the code did the opposite.

    Rather than banning the word, require every place that describes the
    dispatcher's failure mode to name all three shapes, so a future editor
    restating the parity claim has to confront what differs.
    """
    root = Path(__file__).resolve().parents[1]
    for rel in ("async_pipelines/tool_dispatch.py", "docs/architecture.md"):
        text = (root / rel).read_text(encoding="utf-8")
        for shape in ("PipelineError", "ExceptionGroup", "ToolNotFoundError"):
            assert shape in text, (
                f"{rel} describes the dispatcher's fail-fast mode without naming "
                f"{shape}. All three shapes are reachable; saying only "
                "'same as process()' is what hid #90."
            )
