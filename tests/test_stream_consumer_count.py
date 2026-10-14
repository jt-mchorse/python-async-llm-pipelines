"""``stream`` starts at most ``min(concurrency, items produced)`` consumers (#180).

``concurrency`` is a cap ("max consumer fan-out"), and ``process`` and
``dispatch_tool_calls`` treat it as one: a task per item, so a large cap is
free. ``stream`` started ``concurrency`` consumer tasks up front and put as
many sentinels through its bounded queue, whatever the source held: two
items at ``concurrency=10**6`` took 25 s and 1.3 GiB, and ``10**400`` (a
positive int the guard rightly accepts) never returned. (No test passes ``10**400`` itself: against the unfixed
code it does not fail, it hangs the run while allocating tasks.)

The count is read inside the source once it is exhausted. Consumers are only
ever started while the source is being drained, so that is the most there
will be.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator

import pytest

from async_pipelines import stream


def _consumer_tasks() -> int:
    return sum(1 for t in asyncio.all_tasks() if t.get_coro().__qualname__.endswith("._consume"))


async def _run(n: int, concurrency: int) -> tuple[list[int], list[int]]:
    seen: list[int] = []

    async def source() -> AsyncIterator[int]:
        for i in range(n):
            yield i
        seen.append(_consumer_tasks())

    async def fn(x: int) -> int:
        return x * 2

    out = await stream(source(), fn, concurrency=concurrency, queue_size=2)
    return out, seen  # type: ignore[return-value]


@pytest.mark.parametrize("concurrency", [1, 3, 100_000])
@pytest.mark.parametrize("n", [0, 1, 2, 7])
async def test_consumer_tasks_are_min_of_concurrency_and_items(n: int, concurrency: int) -> None:
    out, seen = await _run(n, concurrency)
    assert sorted(out) == [2 * i for i in range(n)]
    assert seen == [min(concurrency, n)]


async def test_in_flight_still_reaches_the_cap_on_a_long_source() -> None:
    # Starting consumers lazily must not lower the fan-out: with more items
    # than the cap, `concurrency` calls still overlap.
    in_flight = peak = 0
    release = asyncio.Event()

    async def source() -> AsyncIterator[int]:
        for i in range(20):
            yield i

    async def fn(x: int) -> int:
        nonlocal in_flight, peak
        in_flight += 1
        peak = max(peak, in_flight)
        if peak == 4:
            release.set()
        await release.wait()
        in_flight -= 1
        return x

    out = await asyncio.wait_for(stream(source(), fn, concurrency=4, queue_size=2), 5)
    assert sorted(out) == list(range(20))
    assert peak == 4
