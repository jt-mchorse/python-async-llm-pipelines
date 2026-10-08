"""A huge integer duration is a ValueError, not an OverflowError (#153).

Both duration validators promise one exception contract (`ValueError`), and
both called `math.isfinite`, which RAISES `OverflowError` for an int too large
for a double instead of returning False. Measured on `main`:

    Workload(n_docs=1, llm_call_seconds=10**400) -> OverflowError: int too large to convert to float
    process(..., timeout=10**400)                -> OverflowError

`OverflowError` is not a `ValueError` subclass, so a caller catching the
documented type misses it.
"""

from __future__ import annotations

import asyncio

import pytest

from async_pipelines.benchmark import FakeLLM, Workload, make_batch_caller
from async_pipelines.core import _require_timeout_seconds, process

HUGE = 10**400


def test_workload_latency() -> None:
    with pytest.raises(ValueError, match="llm_call_seconds"):
        Workload(n_docs=1, llm_call_seconds=HUGE)


def test_batch_seconds() -> None:
    with pytest.raises(ValueError, match="batch_seconds"):
        make_batch_caller(FakeLLM(), batch_seconds=HUGE)


def test_the_timeout_validator() -> None:
    with pytest.raises(ValueError, match="timeout"):
        _require_timeout_seconds(HUGE)


def test_process_refuses_it_before_running_anything() -> None:
    async def fn(x: int) -> int:
        return x

    with pytest.raises(ValueError, match="timeout"):
        asyncio.run(process([1], fn, concurrency=1, timeout=HUGE))


def test_a_large_but_representable_int_is_still_accepted() -> None:
    assert _require_timeout_seconds(10**6) == 1e6
    assert Workload(n_docs=1, llm_call_seconds=10**6).llm_call_seconds == 10**6
