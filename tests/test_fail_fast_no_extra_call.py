"""A failed batch starts no new call after the failure (#146).

`process` and `dispatch_tool_calls` released the failed item's semaphore slot
before the TaskGroup's cancellation reached the waiters, so the next waiter
acquired it and called `fn` (or its tool) -- one more LLM request per failed
item, after the batch had already failed. At concurrency=1 `process` called
`fn` for [0, 1]; `stream` already stopped at [0].
"""

from __future__ import annotations

import asyncio

import pytest

from async_pipelines import PipelineError, process
from async_pipelines.tool_dispatch import ToolCall, ToolRegistry, dispatch_tool_calls


def _calls_made(concurrency: int, *, return_exceptions: bool = False) -> list[int]:
    calls: list[int] = []

    async def fn(x: int) -> int:
        calls.append(x)
        await asyncio.sleep(0.01)
        if x == 0:
            raise ValueError("item 0 fails")
        return x

    async def run() -> None:
        try:
            await process(
                range(10), fn, concurrency=concurrency, return_exceptions=return_exceptions
            )
        except* ValueError:
            pass

    asyncio.run(run())
    return calls


@pytest.mark.parametrize("concurrency", [1, 2, 4])
def test_process_calls_fn_only_for_the_items_already_in_flight(concurrency: int) -> None:
    assert _calls_made(concurrency) == list(range(concurrency))


def test_return_exceptions_still_runs_every_item() -> None:
    assert sorted(_calls_made(1, return_exceptions=True)) == list(range(10))


def test_dispatch_tool_calls_invokes_no_tool_after_the_failure() -> None:
    invoked: list[str] = []
    registry = ToolRegistry()

    async def tool(args: dict) -> str:
        invoked.append(args["id"])
        await asyncio.sleep(0.01)
        if args["id"] == "c0":
            raise ValueError("c0 fails")
        return args["id"]

    registry.register("t", tool)
    calls = [ToolCall(id=f"c{i}", name="t", arguments={"id": f"c{i}"}) for i in range(5)]

    async def run() -> None:
        with pytest.raises(PipelineError):
            await dispatch_tool_calls(calls, registry=registry, concurrency=1)

    asyncio.run(run())
    assert invoked == ["c0"]
