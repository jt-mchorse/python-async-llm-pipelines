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

from async_pipelines import PipelineError, process, stream
from async_pipelines.tool_dispatch import ToolCall, ToolRegistry, ToolResult, dispatch_tool_calls


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


# --- the unbounded default and a tool that raises before awaiting (#212) -------

_N = 5
_DEFAULT = object()  # omit `concurrency` entirely: the signature's own default
_CONCURRENCIES = [pytest.param(_DEFAULT, id="default"), None, 1, _N, _N + 3]


def _dispatch_invoked(
    concurrency: object,
    *,
    raise_before_await: bool,
    return_exceptions: bool = False,
    timeout: float | None = None,
) -> tuple[list[int], list[ToolResult] | None]:
    invoked: list[int] = []
    registry = ToolRegistry()

    async def tool(args: dict) -> int:
        invoked.append(args["i"])
        if raise_before_await and args["i"] == 0:
            raise KeyError("missing argument")  # the common model-tool-call shape
        await asyncio.sleep(0.01)
        if args["i"] == 0:
            raise ValueError("c0 fails")
        return args["i"]

    registry.register("t", tool)
    calls = [ToolCall(id=f"c{i}", name="t", arguments={"i": i}) for i in range(_N)]
    kwargs: dict = {"return_exceptions": return_exceptions, "timeout": timeout}
    if concurrency is not _DEFAULT:
        kwargs["concurrency"] = concurrency
    out: list[list[ToolResult] | None] = [None]

    async def run() -> None:
        if return_exceptions:
            out[0] = await dispatch_tool_calls(calls, registry=registry, **kwargs)
        else:
            with pytest.raises(PipelineError):
                await dispatch_tool_calls(calls, registry=registry, **kwargs)

    asyncio.run(run())
    return invoked, out[0]


@pytest.mark.parametrize("timeout", [None, 5.0])
@pytest.mark.parametrize("concurrency", _CONCURRENCIES)
def test_dispatch_invokes_no_tool_after_a_failure_before_the_first_await(
    concurrency: object, timeout: float | None
) -> None:
    # Unfixed: unbounded (the default) invoked [0, 1, 2, 3, 4]; every bound
    # gave [0]. The latch lived only on the semaphore path.
    invoked, _ = _dispatch_invoked(concurrency, raise_before_await=True, timeout=timeout)
    assert invoked == [0]


@pytest.mark.parametrize("concurrency", _CONCURRENCIES)
def test_dispatch_return_exceptions_still_invokes_every_tool(concurrency: object) -> None:
    # Control: a collected failure does not latch, bounded or not.
    invoked, results = _dispatch_invoked(
        concurrency, raise_before_await=True, return_exceptions=True
    )
    assert sorted(invoked) == list(range(_N))
    assert results is not None
    assert [r.ok for r in results] == [False] + [True] * (_N - 1)


@pytest.mark.parametrize("concurrency", _CONCURRENCIES)
def test_dispatch_failure_after_an_await_keeps_the_calls_already_in_flight(
    concurrency: object,
) -> None:
    # Control: every call that started before the failure still started --
    # the latch stops NEW calls, it does not un-start running ones.
    invoked, _ = _dispatch_invoked(concurrency, raise_before_await=False)
    expected = 1 if concurrency == 1 else _N
    assert sorted(invoked) == list(range(expected))


@pytest.mark.parametrize("concurrency", [1, _N, _N + 3])
def test_process_calls_no_fn_after_a_failure_before_the_first_await(concurrency: int) -> None:
    # `process` already latched on every bound (it has no unbounded mode:
    # `concurrency=None` is a ValueError there); pinned so the two stay in parity.
    calls: list[int] = []

    async def fn(x: int) -> int:
        calls.append(x)
        if x == 0:
            raise KeyError("missing argument")
        await asyncio.sleep(0.01)
        return x

    async def run() -> None:
        with pytest.raises(ExceptionGroup):
            await process(range(_N), fn, concurrency=concurrency)

    asyncio.run(run())
    assert calls == [0]


# --- stream (#171) -------------------------------------------------------------


def _stream_calls_made(concurrency: int, *, return_exceptions: bool = False) -> list[int]:
    # Equal latencies, as the repo's own FakeLLM benchmark runs: a sibling
    # consumer finishes in the same loop tick as item 0's failure.
    calls: list[int] = []

    async def fn(x: int) -> int:
        calls.append(x)
        await asyncio.sleep(0.01)
        if x == 0:
            raise ValueError("item 0 fails")
        return x

    async def source():
        for i in range(10):
            yield i

    async def run() -> None:
        try:
            await stream(
                source(),
                fn,
                concurrency=concurrency,
                queue_size=10,
                return_exceptions=return_exceptions,
            )
        except* ValueError:
            pass

    asyncio.run(run())
    return calls


@pytest.mark.parametrize("concurrency", [1, 2, 4])
def test_stream_calls_fn_only_for_the_items_already_in_flight(concurrency: int) -> None:
    # #146 left `stream` out on the strength of concurrency=1 alone; at 4 it
    # called fn for [0..6] after the batch had failed.
    assert sorted(_stream_calls_made(concurrency)) == list(range(concurrency))


def test_stream_return_exceptions_still_runs_every_item() -> None:
    # Collected failures do not latch: every item is still processed.
    assert sorted(_stream_calls_made(4, return_exceptions=True)) == list(range(10))


# --- stream: the producer fails the batch (#173) -------------------------------


def _stream_calls_after_source_failure(concurrency: int, kind: str) -> tuple[list[int], list[int]]:
    # Deterministic tie: `fn(0)` wakes the source and yields once, so the source
    # fails first and this consumer resumes in the same tick, with queued items.
    calls: list[int] = []
    after: list[int] = []
    source_failed = False

    async def run() -> None:
        ev = asyncio.Event()
        stray = asyncio.get_running_loop().create_future()
        stray.cancel()

        async def fn(x: int) -> int:
            calls.append(x)
            if source_failed:
                after.append(x)
            if x == 0:
                ev.set()
                await asyncio.sleep(0)
            else:
                await asyncio.sleep(0.05)
            return x

        async def source():
            nonlocal source_failed
            for i in range(concurrency + 2):
                yield i
            await ev.wait()
            source_failed = True
            if kind == "raise":
                raise ConnectionError("source dropped")
            await stray  # #169: the source's own CancelledError

        try:
            await stream(source(), fn, concurrency=concurrency, queue_size=8)
        except* (ConnectionError, PipelineError):
            pass

    asyncio.run(run())
    return calls, after


@pytest.mark.parametrize("kind", ["raise", "stray_cancel"])
@pytest.mark.parametrize("concurrency", [1, 2, 4])
def test_stream_calls_no_fn_after_the_producer_failed(concurrency: int, kind: str) -> None:
    # Unfixed: the consumer resuming in the failure's tick called fn for one
    # more queued item at every concurrency (after == [concurrency]).
    calls, after = _stream_calls_after_source_failure(concurrency, kind)
    assert after == []
    assert sorted(calls) == list(range(concurrency))
