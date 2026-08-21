"""The two fields derived from `duration_seconds` must agree it is undefined.

`RunResult` carries `docs_per_second` and `speedup_vs_serial`, both computed
from the same `duration_seconds`. `attach_speedup` has always returned `None`
for a zero duration, and its comment says why: a run that took no measurable
time is *infinitely fast*, so `0.0` "reads as the slowest-possible result and
would mis-rank it on a dashboard/plot" (#60).

`run_pipeline` returned `0.0` for the same condition until #94 — the exact
misranking that comment forbids, on the `docs/s` column a reader scans first.

The assertions here are on the *pair*. Pinning either field alone would let them
drift apart again, which is the defect this file exists to prevent.
"""

from __future__ import annotations

import asyncio
import time

import pytest

from async_pipelines.benchmark import RunResult, attach_speedup, run_pipeline


class _Instant:
    """A pipeline that does no awaiting at all."""

    def __init__(self) -> None:
        self.name = "instant"

    async def run(self, docs: list[str]) -> list[str]:
        return []


class _Slow:
    name = "serial"

    async def run(self, docs: list[str]) -> list[str]:
        await asyncio.sleep(0.01)
        return []


def _zero_duration_result(monkeypatch, name: str = "instant") -> RunResult:
    """A zero-duration `RunResult` produced BY `run_pipeline`, not handed to it.

    Constructing one with `docs_per_second=None` directly would assert the very
    value under test, and would pass against the pre-fix source. The clock is
    frozen so the zero delta is forced rather than raced for.
    """
    ticks = iter([100.0, 100.0])
    monkeypatch.setattr(time, "perf_counter", lambda: next(ticks))
    pipeline = _Instant()
    pipeline.name = name  # type: ignore[misc]
    return asyncio.run(run_pipeline(pipeline, ["a", "b", "c"]))


# ----------------------------------------------------------------------
# The premise: a zero delta is what this clock actually does
# ----------------------------------------------------------------------


def test_perf_counter_can_return_identical_consecutive_values() -> None:
    """Reachability, measured rather than assumed.

    240 of 2000 back-to-back reads were identical when this was written. The
    assertion is deliberately weak (at least one in a large sample) so it does
    not become flaky on a machine with a finer clock — the point is that this is
    a property of the clock, not a hypothetical.
    """
    identical = sum(1 for _ in range(20_000) if time.perf_counter() - time.perf_counter() == 0.0)
    assert identical > 0, "expected at least one zero delta in 20k back-to-back reads"


# ----------------------------------------------------------------------
# The invariant
# ----------------------------------------------------------------------


def test_zero_duration_makes_both_derived_fields_none(monkeypatch) -> None:
    zero = _zero_duration_result(monkeypatch)
    assert zero.duration_seconds == 0.0

    serial = RunResult(pipeline_name="serial", n_docs=3, duration_seconds=1.0, docs_per_second=3.0)
    attached = attach_speedup([serial, zero])
    instant = next(r for r in attached if r.pipeline_name == "instant")

    assert instant.speedup_vs_serial is None
    assert instant.docs_per_second is None, (
        "docs_per_second must not report 0.0 for a zero-duration run — that is "
        "the slowest-possible value on the column, for the fastest-possible run"
    )


def test_run_pipeline_returns_none_not_zero_for_a_zero_duration(monkeypatch) -> None:
    """Force the zero delta rather than racing the clock for it."""
    ticks = iter([100.0, 100.0])
    monkeypatch.setattr(time, "perf_counter", lambda: next(ticks))

    result = asyncio.run(run_pipeline(_Instant(), ["a", "b", "c"]))

    assert result.duration_seconds == 0.0
    assert result.docs_per_second is None


def test_a_real_run_still_reports_a_number() -> None:
    result = asyncio.run(run_pipeline(_Slow(), ["a", "b", "c"]))
    assert result.duration_seconds > 0.0
    assert isinstance(result.docs_per_second, float)
    assert result.docs_per_second == pytest.approx(3 / result.duration_seconds)


def test_empty_docs_with_real_elapsed_still_reports_zero() -> None:
    """Deliberately NOT swept into the fix.

    Zero documents in a measurable time really is 0.0 docs/s — a truthful zero,
    not an undefined one. Only a zero *duration* is undefined.
    """
    result = asyncio.run(run_pipeline(_Slow(), []))
    assert result.duration_seconds > 0.0
    assert result.docs_per_second == 0.0


# ----------------------------------------------------------------------
# Serialization and rendering follow the sibling field's convention
# ----------------------------------------------------------------------


def test_to_dict_emits_null_like_the_sibling_field(monkeypatch) -> None:
    payload = _zero_duration_result(monkeypatch).to_dict()
    assert payload["docs_per_second"] is None
    assert payload["speedup_vs_serial"] is None


def test_markdown_renders_an_em_dash_for_both_columns(monkeypatch) -> None:
    from async_pipelines.benchmark import Workload
    from scripts.bench_1000_doc import render_markdown

    zero = _zero_duration_result(monkeypatch)
    workload = Workload(n_docs=3, concurrency=2, batch_size=2, llm_call_seconds=0.01)
    rendered = render_markdown(workload, [zero])

    row = next(line for line in rendered.splitlines() if line.startswith("| instant"))
    cells = [c.strip() for c in row.split("|")[1:-1]]
    assert cells[2] == "—", cells
    assert cells[3] == "—", cells


def test_markdown_still_renders_numbers_for_a_normal_row() -> None:
    from async_pipelines.benchmark import Workload
    from scripts.bench_1000_doc import render_markdown

    workload = Workload(n_docs=10, concurrency=2, batch_size=2, llm_call_seconds=0.01)
    results = attach_speedup(
        [
            RunResult("serial", 10, 1.0, 10.0),
            RunResult("async", 10, 0.25, 40.0),
        ]
    )
    rendered = render_markdown(workload, results)

    row = next(line for line in rendered.splitlines() if line.startswith("| async "))
    cells = [c.strip() for c in row.split("|")[1:-1]]
    assert cells[2] == "40.0"
    assert cells[3] == "4.00×"
