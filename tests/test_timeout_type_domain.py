"""Every `timeout` seam must reject a bad value the same way (#98).

`#96` fixed this class at the two latency seams in `benchmark.py`, and its
docstring states both the rule and the goal:

    Non-numeric types raise `ValueError` here rather than reaching
    `math.isfinite` and coming back as a raw `TypeError`, so this class keeps
    one exception contract.

The class did not keep one exception contract. The three `timeout` seams — the
package's public API — were never enumerated, and each still read
``if timeout is not None and (not math.isfinite(timeout) or timeout <= 0)``.
Measured across all three:

    value       timeout=            queue_size= (control)
    '5'         TypeError           ValueError
    [1]         TypeError           ValueError
    {'a': 1}    TypeError           ValueError
    True        ACCEPTED            ValueError
    False       ValueError          ValueError
    nan / inf   ValueError          ValueError
    -1 / 0      ValueError          ValueError

`stream`'s `queue_size` is the control because it already had the safe spelling
**three lines away in the same function** — the defect was the minority spelling,
not a missing idea.

`timeout=True` being accepted is `#96`'s own measured harm reached through the
other parameter: `asyncio.timeout(True)` is a one-second deadline, pinned below
by `test_true_was_a_silent_one_second_deadline`.
"""

from __future__ import annotations

import asyncio
import time
from typing import Any

import pytest

from async_pipelines.core import process, stream
from async_pipelines.tool_dispatch import ToolCall, ToolRegistry, dispatch_tool_calls


async def _identity(x: Any) -> Any:
    return x


async def _agen(items: list[Any]):
    for item in items:
        yield item


def _registry() -> ToolRegistry:
    reg = ToolRegistry()

    async def echo(*_args: Any, **_kwargs: Any) -> str:
        return "ok"

    reg.register("echo", echo)
    return reg


async def _call_process(timeout: Any) -> Any:
    return await process([1, 2], _identity, concurrency=2, timeout=timeout)


async def _call_stream(timeout: Any) -> Any:
    return await stream(_agen([1, 2]), _identity, concurrency=2, queue_size=2, timeout=timeout)


async def _call_dispatch(timeout: Any) -> Any:
    return await dispatch_tool_calls(
        [ToolCall(id="1", name="echo", arguments={})],
        registry=_registry(),
        timeout=timeout,
    )


# The control: a sibling argument of `stream` that already used the safe
# spelling. If a change made every argument reject everything, these stay the
# only assertions that would notice.
async def _call_stream_queue_size(queue_size: Any) -> Any:
    return await stream(_agen([1]), _identity, concurrency=2, queue_size=queue_size, timeout=1.0)


SEAMS = [
    ("process", _call_process),
    ("stream", _call_stream),
    ("dispatch_tool_calls", _call_dispatch),
]

# Values that must raise `ValueError` at every seam. The first three used to
# raise a raw `TypeError` from `math.isfinite`; `True` was accepted outright.
REJECTED = ["5", [1], {"a": 1}, True, False, float("nan"), float("inf"), -1, 0, 0.0]

# Values that must still work. `None` means "no deadline"; a plain `int` is a
# legitimate whole number of seconds — the two exceptions #96 argued for.
ACCEPTED = [None, 1, 1.0, 0.5]


def test_the_table_covers_both_verdicts_and_every_seam() -> None:
    assert len(SEAMS) == 3
    assert len(REJECTED) >= 8
    assert len(ACCEPTED) >= 3
    # The three former-TypeError shapes must stay in the table; they are the
    # reason the guard needs an `isinstance` and not just a finiteness check.
    for shape in ("5", [1], {"a": 1}):
        assert shape in REJECTED
    # `True` specifically: it was ACCEPTED, not merely mis-typed.
    assert any(v is True for v in REJECTED)


@pytest.mark.parametrize(("seam", "call"), SEAMS, ids=[s[0] for s in SEAMS])
@pytest.mark.parametrize("value", REJECTED, ids=[repr(v) for v in REJECTED])
def test_every_seam_rejects_with_valueerror(seam: str, call: Any, value: Any) -> None:
    with pytest.raises(ValueError, match="timeout must be a finite positive number"):
        asyncio.run(call(value))


@pytest.mark.parametrize(("seam", "call"), SEAMS, ids=[s[0] for s in SEAMS])
@pytest.mark.parametrize("value", ACCEPTED, ids=[repr(v) for v in ACCEPTED])
def test_every_seam_still_accepts_a_valid_timeout(seam: str, call: Any, value: Any) -> None:
    """Without this half, a guard that rejected everything would pass every case
    above."""
    result = asyncio.run(call(value))
    assert result is not None


@pytest.mark.parametrize("value", ["5", [1], True, -1])
def test_the_control_argument_was_already_correct(value: Any) -> None:
    """`stream`'s `queue_size` had the safe spelling three lines from `timeout`.
    It is asserted here so the file demonstrates the *divergence* that was fixed,
    not merely that a guard exists."""
    with pytest.raises(ValueError, match="queue_size must be a positive int"):
        asyncio.run(_call_stream_queue_size(value))


def test_true_was_a_silent_one_second_deadline() -> None:
    """The harm, not just the type error.

    `asyncio.timeout(True)` arms a one-second deadline. Before this guard,
    `process(..., timeout=True)` against a 2.0 s task raised an `ExceptionGroup`
    after ~1.003 s — indistinguishable from a real timeout. That is `#96`'s
    measured `asyncio.sleep(True) -> 1.002 s` reached through the other
    parameter.

    Asserted as "rejects before doing any work" rather than by timing the old
    behaviour: a wall-clock assertion is a property of the host, not of this
    package.
    """
    started = False

    async def slow(x: Any) -> Any:
        nonlocal started
        started = True
        await asyncio.sleep(2.0)
        return x

    t0 = time.perf_counter()
    with pytest.raises(ValueError, match="timeout must be a finite positive number"):
        asyncio.run(process([1], slow, concurrency=1, timeout=True))
    assert not started, "the guard must fire before any item is dispatched"
    # Not a timing assertion on the *old* behaviour — just that the new one does
    # not arm a deadline at all. Generous bound; the guard is pure Python.
    assert time.perf_counter() - t0 < 1.0


def test_the_rule_lives_in_one_place() -> None:
    """The divergence this issue is about is what happens when a rule is
    inlined at four sites. Assert that the three seams share one implementation
    rather than three copies that can drift again."""
    import inspect

    from async_pipelines import core, tool_dispatch

    for module in (core, tool_dispatch):
        src = inspect.getsource(module)
        # Comments and the helper's own docstring quote the old shape, so strip
        # the helper before grepping — a lock must not trip on the prose
        # explaining its own fix.
        helper_src = inspect.getsource(core._require_timeout_seconds)
        body = src.replace(helper_src, "")
        assert "math.isfinite(timeout)" not in body, (
            f"{module.__name__} still inlines the timeout finiteness check; "
            "call _require_timeout_seconds instead"
        )
