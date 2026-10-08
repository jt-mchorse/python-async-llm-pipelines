"""A tool's own PipelineError is the tool's exception, not the dispatcher's (#155).

`dispatch_tool_calls` re-raised any `PipelineError` from its TaskGroup as its
own, so a tool that runs a NESTED dispatch and lets that inner call's deadline
escape was reported as the outer call's timeout. Measured on `main` (a hunt
agent, re-run here): an outer dispatch of 2 calls with `timeout=5`, one tool
running an inner dispatch of 4 sub-calls with `timeout=0.05`, raised
`PipelineTimeoutError: item at index ... exceeded timeout of 0.05s` with an
`ExceptionGroup` as `__cause__` -- a deadline the caller never set, at an index
that may not exist in its batch. The docstring's contract for a tool's
exception is a wrapping `PipelineError` with the original as `__cause__`; #66
fixed the mirror image (a tool's own TimeoutError relabelled as the deadline).
"""

from __future__ import annotations

import asyncio

import pytest

from async_pipelines.core import PipelineError, PipelineTimeoutError
from async_pipelines.tool_dispatch import ToolCall, ToolRegistry, dispatch_tool_calls


def _registries() -> tuple[ToolRegistry, ToolRegistry]:
    inner = ToolRegistry()

    @inner.tool("slow")
    async def slow(_a: dict) -> None:
        await asyncio.sleep(1)

    outer = ToolRegistry()

    @outer.tool("fanout")
    async def fanout(_a: dict) -> object:
        calls = [ToolCall(id=f"i{i}", name="slow", arguments={}) for i in range(4)]
        return await dispatch_tool_calls(calls, registry=inner, timeout=0.05)

    @outer.tool("ok")
    async def ok(_a: dict) -> int:
        return 1

    @outer.tool("raises_pipeline_error")
    async def raises(_a: dict) -> None:
        raise PipelineError("from inside the tool")

    return inner, outer


def test_a_nested_dispatchs_timeout_is_wrapped_as_the_tools_exception() -> None:
    _inner, outer = _registries()
    calls = [
        ToolCall(id="a", name="ok", arguments={}),
        ToolCall(id="b", name="fanout", arguments={}),
    ]
    with pytest.raises(PipelineError) as exc:
        asyncio.run(dispatch_tool_calls(calls, registry=outer, timeout=5))
    assert type(exc.value) is PipelineError  # not the outer call's own timeout
    assert isinstance(exc.value.__cause__, PipelineTimeoutError)
    assert exc.value.__cause__.timeout_s == 0.05


def test_a_tool_raising_a_pipeline_error_is_wrapped_too() -> None:
    _inner, outer = _registries()
    with pytest.raises(PipelineError) as exc:
        asyncio.run(
            dispatch_tool_calls(
                [ToolCall(id="x", name="raises_pipeline_error", arguments={})], registry=outer
            )
        )
    assert isinstance(exc.value.__cause__, PipelineError)
    assert str(exc.value.__cause__) == "from inside the tool"


def test_the_dispatchers_own_deadline_still_raises_as_itself() -> None:
    inner, _outer = _registries()
    with pytest.raises(PipelineTimeoutError) as exc:
        asyncio.run(
            dispatch_tool_calls(
                [ToolCall(id="s", name="slow", arguments={})], registry=inner, timeout=0.05
            )
        )
    assert exc.value.timeout_s == 0.05
    assert exc.value.index == 0
