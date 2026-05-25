"""Tests for concurrent tool-call dispatch (issue #2)."""

from __future__ import annotations

import asyncio
import time

import pytest

from async_pipelines import (
    PipelineError,
    ToolCall,
    ToolNotFoundError,
    ToolRegistry,
    ToolResult,
    dispatch_tool_calls,
)

# ----------------------------------------------------------------------
# ToolRegistry
# ----------------------------------------------------------------------


def test_registry_register_and_get():
    r = ToolRegistry()

    async def web_fetch(args: dict) -> str:
        return "fetched"

    r.register("web_fetch", web_fetch)
    assert "web_fetch" in r
    assert r.get("web_fetch") is web_fetch
    assert r.names() == ["web_fetch"]
    assert len(r) == 1


def test_registry_decorator_form():
    r = ToolRegistry()

    @r.tool("ping")
    async def ping(_: dict) -> str:
        return "pong"

    assert "ping" in r
    assert r.get("ping") is ping


def test_registry_rejects_duplicate():
    r = ToolRegistry()

    async def f(_: dict) -> None:
        return None

    r.register("x", f)
    with pytest.raises(ValueError, match="already registered"):
        r.register("x", f)


def test_registry_rejects_empty_name():
    r = ToolRegistry()
    with pytest.raises(ValueError, match="non-empty"):
        r.register("", lambda _a: asyncio.sleep(0))  # type: ignore[arg-type]


def test_registry_get_unknown_raises():
    r = ToolRegistry()
    with pytest.raises(ToolNotFoundError, match="not registered"):
        r.get("nope")


# ----------------------------------------------------------------------
# dispatch_tool_calls — happy path
# ----------------------------------------------------------------------


@pytest.mark.asyncio
async def test_dispatch_handles_single_call():
    r = ToolRegistry()

    @r.tool("echo")
    async def echo(args: dict) -> dict:
        return {"echoed": args}

    out = await dispatch_tool_calls(
        [ToolCall(id="t1", name="echo", arguments={"k": 1})], registry=r
    )
    assert len(out) == 1
    assert isinstance(out[0], ToolResult)
    assert out[0].ok is True
    assert out[0].tool_call_id == "t1"
    assert out[0].name == "echo"
    assert out[0].value == {"echoed": {"k": 1}}


@pytest.mark.asyncio
async def test_dispatch_handles_k_calls_results_match_input_ids():
    r = ToolRegistry()

    @r.tool("identity")
    async def identity(args: dict) -> dict:
        return args

    calls = [ToolCall(id=f"id-{i}", name="identity", arguments={"i": i}) for i in range(7)]
    out = await dispatch_tool_calls(calls, registry=r)
    assert len(out) == 7
    by_id = {r.tool_call_id: r for r in out}
    for call in calls:
        assert by_id[call.id].value == call.arguments


@pytest.mark.asyncio
async def test_dispatch_empty_input_returns_empty():
    r = ToolRegistry()
    assert await dispatch_tool_calls([], registry=r) == []


# ----------------------------------------------------------------------
# Concurrency
# ----------------------------------------------------------------------


@pytest.mark.asyncio
async def test_dispatch_runs_concurrently_by_default():
    r = ToolRegistry()
    sleep_ms = 60

    @r.tool("slow")
    async def slow(args: dict) -> int:
        await asyncio.sleep(sleep_ms / 1000.0)
        return args.get("i", 0)

    calls = [ToolCall(id=f"id-{i}", name="slow", arguments={"i": i}) for i in range(5)]

    t0 = time.perf_counter()
    out = await dispatch_tool_calls(calls, registry=r)
    elapsed_ms = (time.perf_counter() - t0) * 1000.0

    assert len(out) == 5
    # 5 × 60ms = 300ms serial; concurrent should be well under 200ms.
    assert elapsed_ms < 200, f"expected concurrent dispatch to be <200ms; got {elapsed_ms:.1f}ms"


@pytest.mark.asyncio
async def test_dispatch_respects_concurrency_cap():
    r = ToolRegistry()
    in_flight = 0
    peak = 0
    lock = asyncio.Lock()

    @r.tool("counted")
    async def counted(args: dict) -> int:
        nonlocal in_flight, peak
        async with lock:
            in_flight += 1
            peak = max(peak, in_flight)
        await asyncio.sleep(0.02)
        async with lock:
            in_flight -= 1
        return 0

    calls = [ToolCall(id=f"id-{i}", name="counted", arguments={}) for i in range(10)]
    await dispatch_tool_calls(calls, registry=r, concurrency=3)
    assert peak <= 3


@pytest.mark.asyncio
async def test_dispatch_rejects_zero_concurrency():
    r = ToolRegistry()

    @r.tool("noop")
    async def noop(_: dict) -> None:
        return None

    with pytest.raises(ValueError, match="positive"):
        await dispatch_tool_calls([ToolCall(id="x", name="noop")], registry=r, concurrency=0)


# ----------------------------------------------------------------------
# Failure modes
# ----------------------------------------------------------------------


@pytest.mark.asyncio
async def test_dispatch_default_fail_fast_wraps_in_pipeline_error():
    r = ToolRegistry()

    @r.tool("bad")
    async def bad(_: dict) -> None:
        raise RuntimeError("boom")

    with pytest.raises(PipelineError, match="RuntimeError"):
        await dispatch_tool_calls([ToolCall(id="x", name="bad")], registry=r)


@pytest.mark.asyncio
async def test_dispatch_return_exceptions_keeps_batch_alive():
    r = ToolRegistry()

    @r.tool("ok")
    async def ok(_: dict) -> str:
        return "ok"

    @r.tool("bad")
    async def bad(_: dict) -> None:
        raise ValueError("oops")

    calls = [
        ToolCall(id="a", name="ok"),
        ToolCall(id="b", name="bad"),
        ToolCall(id="c", name="ok"),
    ]
    out = await dispatch_tool_calls(calls, registry=r, return_exceptions=True)
    by_id = {r.tool_call_id: r for r in out}
    assert by_id["a"].ok is True
    assert by_id["a"].value == "ok"
    assert by_id["b"].ok is False
    assert "ValueError" in (by_id["b"].error_repr or "")
    assert by_id["c"].ok is True


@pytest.mark.asyncio
async def test_dispatch_unknown_tool_fail_fast_raises_immediately():
    r = ToolRegistry()
    with pytest.raises(ToolNotFoundError):
        await dispatch_tool_calls([ToolCall(id="x", name="missing")], registry=r)


@pytest.mark.asyncio
async def test_dispatch_unknown_tool_in_return_exceptions_mode_records_on_result():
    r = ToolRegistry()
    out = await dispatch_tool_calls(
        [ToolCall(id="x", name="missing")], registry=r, return_exceptions=True
    )
    assert out[0].ok is False
    assert "ToolNotFoundError" in (out[0].error_repr or "")
    assert out[0].name == "missing"


# ----------------------------------------------------------------------
# Telemetry shape
# ----------------------------------------------------------------------


@pytest.mark.asyncio
async def test_tool_result_has_elapsed_ms():
    r = ToolRegistry()

    @r.tool("sleep")
    async def sleep(args: dict) -> int:
        await asyncio.sleep(args.get("s", 0.05))
        return 0

    out = await dispatch_tool_calls(
        [ToolCall(id="x", name="sleep", arguments={"s": 0.05})], registry=r
    )
    assert out[0].elapsed_ms >= 40  # slept ~50ms; allow slack


# ----------------------------------------------------------------------
# Bench: 5 tools serial vs concurrent (acceptance criterion 3)
# ----------------------------------------------------------------------


@pytest.mark.asyncio
async def test_bench_5_tools_serial_vs_concurrent():
    """Acceptance criterion: bench 5 tools serial vs concurrent latency."""
    r = ToolRegistry()
    sleep_ms = 30

    async def fake_tool(args: dict) -> int:
        await asyncio.sleep(sleep_ms / 1000.0)
        return args.get("i", 0)

    for i in range(5):
        r.register(f"tool_{i}", fake_tool)

    calls = [ToolCall(id=f"id-{i}", name=f"tool_{i}", arguments={"i": i}) for i in range(5)]

    # Serial baseline: dispatch with concurrency=1.
    t0 = time.perf_counter()
    serial_out = await dispatch_tool_calls(calls, registry=r, concurrency=1)
    serial_ms = (time.perf_counter() - t0) * 1000.0

    # Concurrent: unbounded.
    t0 = time.perf_counter()
    concurrent_out = await dispatch_tool_calls(calls, registry=r)
    concurrent_ms = (time.perf_counter() - t0) * 1000.0

    assert len(serial_out) == 5
    assert len(concurrent_out) == 5

    # Serial ~5×sleep_ms = 150ms; concurrent ~sleep_ms = 30ms.
    # Assert the speedup is real (3× minimum) — the test stays useful even
    # under noisy CI scheduling.
    assert serial_ms > concurrent_ms * 2, (
        f"expected concurrent dispatch to be at least 2× faster than serial; "
        f"serial={serial_ms:.1f}ms concurrent={concurrent_ms:.1f}ms"
    )

    # Print so the operator capturing benchmark numbers can collect them.
    print(
        f"\n[bench] 5 tools × {sleep_ms}ms sleep: serial={serial_ms:.1f}ms concurrent={concurrent_ms:.1f}ms speedup={serial_ms / concurrent_ms:.2f}×"
    )


# ----------------------------------------------------------------------
# Per-tool timeout (issue #26 — parity with process()/stream())
# ----------------------------------------------------------------------


@pytest.mark.asyncio
async def test_dispatch_timeout_raises_pipeline_timeout_when_return_exceptions_false():
    """A slow tool past the deadline propagates PipelineTimeoutError, wrapped
    in PipelineError by the existing TaskGroup error funnel."""
    from async_pipelines import PipelineTimeoutError

    r = ToolRegistry()

    @r.tool("slow")
    async def slow(_: dict) -> str:
        await asyncio.sleep(0.5)
        return "should not reach"

    calls = [ToolCall(id="x", name="slow", arguments={})]
    with pytest.raises((PipelineTimeoutError, PipelineError)) as exc:
        await dispatch_tool_calls(calls, registry=r, timeout=0.05)
    # When the funnel re-wraps, the original PipelineTimeoutError is the cause.
    # Either way, the surfaced exception's chain should point at PipelineTimeoutError.
    chain = exc.value if isinstance(exc.value, PipelineTimeoutError) else exc.value.__cause__
    assert isinstance(chain, PipelineTimeoutError)
    assert chain.timeout_s == 0.05
    assert chain.index == 0


@pytest.mark.asyncio
async def test_dispatch_timeout_with_return_exceptions_attaches_to_result():
    """With return_exceptions=True, the slow tool's ToolResult.error_repr
    surfaces PipelineTimeoutError and fast tools' results are populated."""
    r = ToolRegistry()

    @r.tool("fast")
    async def fast(args: dict) -> str:
        return f"ok-{args['i']}"

    @r.tool("slow")
    async def slow(_: dict) -> str:
        await asyncio.sleep(0.5)
        return "should not reach"

    calls = [
        ToolCall(id="a", name="fast", arguments={"i": 0}),
        ToolCall(id="b", name="slow", arguments={}),
        ToolCall(id="c", name="fast", arguments={"i": 2}),
    ]
    results = await dispatch_tool_calls(calls, registry=r, return_exceptions=True, timeout=0.05)
    assert len(results) == 3
    assert results[0].ok is True
    assert results[0].value == "ok-0"
    assert results[1].ok is False
    assert "PipelineTimeoutError" in (results[1].error_repr or "")
    assert results[2].ok is True
    assert results[2].value == "ok-2"


@pytest.mark.asyncio
async def test_dispatch_timeout_zero_raises_value_error():
    r = ToolRegistry()

    @r.tool("noop")
    async def noop(_: dict) -> None:
        return None

    with pytest.raises(ValueError, match="timeout must be a finite positive number"):
        await dispatch_tool_calls(
            [ToolCall(id="x", name="noop", arguments={})],
            registry=r,
            timeout=0,
        )


@pytest.mark.asyncio
async def test_dispatch_timeout_negative_raises_value_error():
    r = ToolRegistry()

    @r.tool("noop")
    async def noop(_: dict) -> None:
        return None

    with pytest.raises(ValueError, match="timeout must be a finite positive number"):
        await dispatch_tool_calls(
            [ToolCall(id="x", name="noop", arguments={})],
            registry=r,
            timeout=-0.1,
        )


@pytest.mark.asyncio
async def test_dispatch_timeout_none_is_unchanged_behavior():
    """Regression guard: timeout=None (default) must not change anything —
    a tool that takes longer than any imagined timeout still completes."""
    r = ToolRegistry()

    @r.tool("slow_but_unbounded")
    async def slow(_: dict) -> str:
        await asyncio.sleep(0.05)
        return "completed"

    calls = [ToolCall(id="x", name="slow_but_unbounded", arguments={})]
    results = await dispatch_tool_calls(calls, registry=r)  # no timeout kwarg
    assert len(results) == 1
    assert results[0].ok is True
    assert results[0].value == "completed"
