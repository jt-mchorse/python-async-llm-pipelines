"""Tests for the 1000-doc benchmark (issue #4).

Two surfaces:

1. The three `Pipeline` classes return outputs in input order, count
   the right number of FakeLLM calls, and exhibit the documented
   speedup relationship under the synthetic latency model.
2. `attach_speedup` math is correct against a known baseline.
3. The `bench_1000_doc.py` script writes the expected markdown + JSON
   shape to the configured paths.
"""

from __future__ import annotations

import asyncio
import json
import sys
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(_REPO_ROOT / "scripts") not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT / "scripts"))

from async_pipelines.benchmark import (  # noqa: E402
    AsyncPipeline,
    BatchedAsyncPipeline,
    FakeLLM,
    RunResult,
    SerialPipeline,
    Workload,
    attach_speedup,
    make_batch_caller,
    run_pipeline,
)


def _docs(n: int) -> list[str]:
    return [f"doc-{i:04d}" for i in range(n)]


# ---------------------------------------------------------------------
# SerialPipeline
# ---------------------------------------------------------------------


async def _run_serial(n: int = 10, latency: float = 0.001) -> tuple[list[str], FakeLLM, FakeLLM]:
    llm1 = FakeLLM(latency_seconds=latency, call_id="L1")
    llm2 = FakeLLM(latency_seconds=latency, call_id="L2")
    pipe = SerialPipeline(llm1, llm2)
    out = await pipe.run(_docs(n))
    return out, llm1, llm2


def test_serial_preserves_input_order():
    out, _, _ = asyncio.run(_run_serial(n=8))
    assert [o.split(":")[0] for o in out] == [f"doc-{i:04d}" for i in range(8)]


def test_serial_calls_both_llms_once_per_doc():
    out, llm1, llm2 = asyncio.run(_run_serial(n=5))
    assert len(out) == 5
    assert llm1.call_count == 5
    assert llm2.call_count == 5


# ---------------------------------------------------------------------
# AsyncPipeline
# ---------------------------------------------------------------------


async def _run_async(
    n: int = 50, concurrency: int = 16, latency: float = 0.001
) -> tuple[list[str], FakeLLM, FakeLLM]:
    llm1 = FakeLLM(latency_seconds=latency, call_id="L1")
    llm2 = FakeLLM(latency_seconds=latency, call_id="L2")
    pipe = AsyncPipeline(llm1, llm2, concurrency=concurrency)
    out = await pipe.run(_docs(n))
    return out, llm1, llm2


def test_async_preserves_input_order():
    out, _, _ = asyncio.run(_run_async(n=20))
    assert [o.split(":")[0] for o in out] == [f"doc-{i:04d}" for i in range(20)]


def test_async_calls_both_llms_once_per_doc():
    out, llm1, llm2 = asyncio.run(_run_async(n=20))
    assert len(out) == 20
    assert llm1.call_count == 20
    assert llm2.call_count == 20


# ---------------------------------------------------------------------
# Speedup: async beats serial
# ---------------------------------------------------------------------


def test_async_beats_serial_under_synthetic_latency():
    # 50 docs, 20 ms per call: serial = 50 * 2 * 0.02 = 2s. Async with
    # concurrency 16 should be much faster — ceil(50/16) * 2 * 0.02 = 0.16s.
    workload = Workload(n_docs=50, llm_call_seconds=0.020, concurrency=16, batch_size=8)

    async def go() -> dict[str, RunResult]:
        serial = SerialPipeline(
            FakeLLM(latency_seconds=workload.llm_call_seconds, call_id="L1s"),
            FakeLLM(latency_seconds=workload.llm_call_seconds, call_id="L2s"),
        )
        async_pipe = AsyncPipeline(
            FakeLLM(latency_seconds=workload.llm_call_seconds, call_id="L1a"),
            FakeLLM(latency_seconds=workload.llm_call_seconds, call_id="L2a"),
            concurrency=workload.concurrency,
        )
        docs = _docs(workload.n_docs)
        serial_result = await run_pipeline(serial, docs)
        async_result = await run_pipeline(async_pipe, docs)
        return {"serial": serial_result, "async": async_result}

    results = asyncio.run(go())
    speedup = results["serial"].duration_seconds / results["async"].duration_seconds
    # We expect ~5-15× under this configuration; allow a wide band so
    # the test isn't flaky on slow CI runners.
    assert speedup >= 3.0, f"expected speedup >= 3.0, got {speedup:.2f}×"


# ---------------------------------------------------------------------
# BatchedAsyncPipeline
# ---------------------------------------------------------------------


def test_batched_pipeline_preserves_input_order():
    workload = Workload(n_docs=24, llm_call_seconds=0.001, concurrency=4, batch_size=8)

    async def go() -> list[str]:
        batched = BatchedAsyncPipeline(
            make_batch_caller(
                FakeLLM(latency_seconds=workload.llm_call_seconds, call_id="L1b"),
                batch_seconds=workload.llm_call_seconds,
            ),
            make_batch_caller(
                FakeLLM(latency_seconds=workload.llm_call_seconds, call_id="L2b"),
                batch_seconds=workload.llm_call_seconds,
            ),
            concurrency=workload.concurrency,
            batch_size=workload.batch_size,
        )
        return await batched.run(_docs(workload.n_docs))

    out = asyncio.run(go())
    assert len(out) == 24
    # Each output carries its source doc prefix; ordering is preserved.
    prefixes = [o.split(":")[0] for o in out]
    assert prefixes == [f"doc-{i:04d}" for i in range(24)]


def test_batched_pipeline_rejects_zero_batch_size():
    with pytest.raises(ValueError, match="batch_size"):
        BatchedAsyncPipeline(
            make_batch_caller(FakeLLM()),
            make_batch_caller(FakeLLM()),
            concurrency=1,
            batch_size=0,
        )


# ---------------------------------------------------------------------
# Constructor-time concurrency validation parity (issue #28)
# ---------------------------------------------------------------------


def test_async_pipeline_rejects_zero_concurrency():
    """`AsyncPipeline(concurrency=0)` raises eagerly.

    `process()` itself enforces `concurrency > 0` at call-time, but a
    misconfigured workload should surface at construction not at the
    first `run()`. Parity with `BatchedAsyncPipeline`'s existing
    `batch_size` guard.
    """
    with pytest.raises(ValueError, match="concurrency must be >= 1"):
        AsyncPipeline(FakeLLM(), FakeLLM(), concurrency=0)


def test_async_pipeline_rejects_negative_concurrency():
    """Negative values are also rejected (a `< 1` not a `!= 0` check)."""
    with pytest.raises(ValueError, match="concurrency must be >= 1"):
        AsyncPipeline(FakeLLM(), FakeLLM(), concurrency=-3)


def test_batched_pipeline_rejects_zero_concurrency():
    """Sibling guard on `BatchedAsyncPipeline` so the two pipelines have
    matching constructor-time validation.
    """
    with pytest.raises(ValueError, match="concurrency must be >= 1"):
        BatchedAsyncPipeline(
            make_batch_caller(FakeLLM()),
            make_batch_caller(FakeLLM()),
            concurrency=0,
            batch_size=8,
        )


def test_async_pipeline_constructs_at_minimum_concurrency_one():
    """`concurrency=1` is the minimum valid value — the boundary holds."""
    pipe = AsyncPipeline(FakeLLM(), FakeLLM(), concurrency=1)
    assert pipe.concurrency == 1


def test_batched_pipeline_constructs_at_minimum_values():
    """`concurrency=1, batch_size=1` constructs cleanly."""
    pipe = BatchedAsyncPipeline(
        make_batch_caller(FakeLLM()),
        make_batch_caller(FakeLLM()),
        concurrency=1,
        batch_size=1,
    )
    assert pipe.concurrency == 1
    assert pipe.batch_size == 1


# Issue #34: completes the #32 sweep. Previously `AsyncPipeline.__init__`
# and `BatchedAsyncPipeline.__init__` used sign-only `< 1` checks; bool /
# float / NaN slipped through and surfaced deep in `process()` at the
# wrong site (which broke the eager-validation contract documented in
# the AsyncPipeline source).
_BAD_INT = [1.5, float("nan"), float("inf"), True, "4"]


@pytest.mark.parametrize("bad", _BAD_INT)
def test_async_pipeline_concurrency_must_be_int(bad):
    with pytest.raises(ValueError, match="concurrency must be an int"):
        AsyncPipeline(FakeLLM(), FakeLLM(), concurrency=bad)


@pytest.mark.parametrize("bad", _BAD_INT)
def test_batched_pipeline_concurrency_must_be_int(bad):
    with pytest.raises(ValueError, match="concurrency must be an int"):
        BatchedAsyncPipeline(
            make_batch_caller(FakeLLM()),
            make_batch_caller(FakeLLM()),
            concurrency=bad,
            batch_size=8,
        )


@pytest.mark.parametrize("bad", _BAD_INT)
def test_batched_pipeline_batch_size_must_be_int(bad):
    with pytest.raises(ValueError, match="batch_size must be an int"):
        BatchedAsyncPipeline(
            make_batch_caller(FakeLLM()),
            make_batch_caller(FakeLLM()),
            concurrency=4,
            batch_size=bad,
        )


@pytest.mark.parametrize("good", [1, 2, 4, 8, 32])
def test_async_pipeline_accepts_valid_int_concurrency(good):
    pipe = AsyncPipeline(FakeLLM(), FakeLLM(), concurrency=good)
    assert pipe.concurrency == good


@pytest.mark.parametrize("good", [1, 2, 4, 8, 32])
def test_batched_pipeline_accepts_valid_int_values(good):
    pipe = BatchedAsyncPipeline(
        make_batch_caller(FakeLLM()),
        make_batch_caller(FakeLLM()),
        concurrency=good,
        batch_size=good * 2,
    )
    assert pipe.concurrency == good
    assert pipe.batch_size == good * 2


# Issue #30: Workload validates fields at construction. Surfaces misconfig
# at the operator-visible API site, not at an inner pipeline factory call.
# n_docs=0 silently produces empty results and zero-division speedup math.
@pytest.mark.parametrize("bad_n", [0, -1, -100])
def test_workload_rejects_non_positive_n_docs(bad_n: int):
    with pytest.raises(ValueError, match=r"n_docs must be >= 1"):
        Workload(n_docs=bad_n)


@pytest.mark.parametrize("bad_latency", [-0.001, -1.0])
def test_workload_rejects_negative_llm_call_seconds(bad_latency: float):
    # Message tightened in #32 to "must be a finite number >= 0.0".
    with pytest.raises(ValueError, match=r"llm_call_seconds must be a finite number >= 0\.0"):
        Workload(n_docs=10, llm_call_seconds=bad_latency)


# Issue #32: extend sign-only Workload checks to isinstance(int) + finiteness.
# NaN llm_call_seconds skews benchmark throughput numbers; fractional n_docs /
# concurrency / batch_size silently truncates via asyncio primitives; bool
# flattens operator intent (Python bool subclasses int).
@pytest.mark.parametrize(
    "bad",
    [1.5, float("nan"), float("inf"), True, "5"],
)
def test_workload_rejects_non_int_n_docs(bad):
    with pytest.raises(ValueError, match="n_docs must be an int"):
        Workload(n_docs=bad)


@pytest.mark.parametrize(
    "bad",
    [float("nan"), float("inf"), float("-inf")],
)
def test_workload_rejects_non_finite_llm_call_seconds(bad: float):
    with pytest.raises(ValueError, match="llm_call_seconds must be a finite number"):
        Workload(n_docs=10, llm_call_seconds=bad)


@pytest.mark.parametrize(
    "bad",
    [1.5, float("nan"), True, "5"],
)
def test_workload_rejects_non_int_concurrency(bad):
    with pytest.raises(ValueError, match="concurrency must be an int"):
        Workload(n_docs=10, concurrency=bad)


@pytest.mark.parametrize(
    "bad",
    [1.5, float("nan"), True, "5"],
)
def test_workload_rejects_non_int_batch_size(bad):
    with pytest.raises(ValueError, match="batch_size must be an int"):
        Workload(n_docs=10, batch_size=bad)


@pytest.mark.parametrize("bad_concurrency", [0, -1])
def test_workload_rejects_non_positive_concurrency(bad_concurrency: int):
    # Defense-in-depth: AsyncPipeline.__init__ catches this too (PR #29), but
    # the Workload-level guard surfaces the failure at the operator's misconfig
    # site instead of inside an inner factory call.
    with pytest.raises(ValueError, match=r"concurrency must be >= 1"):
        Workload(n_docs=10, concurrency=bad_concurrency)


@pytest.mark.parametrize("bad_batch", [0, -1])
def test_workload_rejects_non_positive_batch_size(bad_batch: int):
    with pytest.raises(ValueError, match=r"batch_size must be >= 1"):
        Workload(n_docs=10, batch_size=bad_batch)


def test_workload_accepts_zero_llm_call_seconds():
    # Zero latency is meaningful: instant-LLM smoke test without sleep.
    w = Workload(n_docs=10, llm_call_seconds=0.0)
    assert w.llm_call_seconds == 0.0


def test_workload_accepts_minimum_valid_values():
    # All-ones is the boundary: pin it constructs cleanly so a future
    # contract tightening (e.g. n_docs >= 10) doesn't slip in silently.
    w = Workload(n_docs=1, llm_call_seconds=0.0, concurrency=1, batch_size=1)
    assert w.n_docs == 1
    assert w.concurrency == 1
    assert w.batch_size == 1


# ---------------------------------------------------------------------
# attach_speedup
# ---------------------------------------------------------------------


def test_attach_speedup_computes_ratio_against_serial():
    raw = [
        RunResult(pipeline_name="serial", n_docs=10, duration_seconds=1.0, docs_per_second=10.0),
        RunResult(pipeline_name="async", n_docs=10, duration_seconds=0.25, docs_per_second=40.0),
        RunResult(
            pipeline_name="async+batched", n_docs=10, duration_seconds=0.05, docs_per_second=200.0
        ),
    ]
    with_speedup = attach_speedup(raw)
    by_name = {r.pipeline_name: r for r in with_speedup}
    assert by_name["serial"].speedup_vs_serial == pytest.approx(1.0)
    assert by_name["async"].speedup_vs_serial == pytest.approx(4.0)
    assert by_name["async+batched"].speedup_vs_serial == pytest.approx(20.0)


def test_attach_speedup_handles_zero_serial_duration():
    # Pathological: serial somehow ran in zero time. Speedup is undefined
    # (we return None to signal that rather than divide by zero).
    raw = [
        RunResult(pipeline_name="serial", n_docs=10, duration_seconds=0.0, docs_per_second=0.0),
        RunResult(pipeline_name="async", n_docs=10, duration_seconds=0.1, docs_per_second=100.0),
    ]
    with_speedup = attach_speedup(raw)
    by_name = {r.pipeline_name: r for r in with_speedup}
    assert by_name["serial"].speedup_vs_serial is None
    assert by_name["async"].speedup_vs_serial is None


def test_attach_speedup_preserves_duplicate_named_measurements():
    # Running a pipeline more than once for stable timings is normal
    # benchmarking practice. The prior dict-keyed implementation collapsed
    # same-named results to one (3 in -> 2 out), silently dropping a
    # measurement. Every input must produce exactly one output.
    raw = [
        RunResult(pipeline_name="serial", n_docs=10, duration_seconds=1.0, docs_per_second=10.0),
        RunResult(pipeline_name="async", n_docs=10, duration_seconds=0.25, docs_per_second=40.0),
        RunResult(pipeline_name="async", n_docs=10, duration_seconds=0.20, docs_per_second=50.0),
    ]
    with_speedup = attach_speedup(raw)
    assert len(with_speedup) == len(raw)
    assert [r.pipeline_name for r in with_speedup] == ["serial", "async", "async"]
    # Both async measurements keep their own duration and get their own
    # speedup against the shared serial baseline (1.0 / 0.25, 1.0 / 0.20).
    assert with_speedup[1].speedup_vs_serial == pytest.approx(4.0)
    assert with_speedup[2].speedup_vs_serial == pytest.approx(5.0)


def test_attach_speedup_preserves_input_order_when_serial_not_first():
    # Output order must follow the input sequence, independent of where the
    # serial baseline appears (the prior impl's order was an accident of
    # dict-insertion order of first-seen names).
    raw = [
        RunResult(pipeline_name="async", n_docs=10, duration_seconds=0.25, docs_per_second=40.0),
        RunResult(
            pipeline_name="async+batched", n_docs=10, duration_seconds=0.05, docs_per_second=200.0
        ),
        RunResult(pipeline_name="serial", n_docs=10, duration_seconds=1.0, docs_per_second=10.0),
    ]
    with_speedup = attach_speedup(raw)
    assert [r.pipeline_name for r in with_speedup] == ["async", "async+batched", "serial"]
    assert with_speedup[0].speedup_vs_serial == pytest.approx(4.0)
    assert with_speedup[2].speedup_vs_serial == pytest.approx(1.0)


def test_attach_speedup_accepts_a_one_shot_generator():
    # `results` is typed `Iterable[RunResult]`; a generator is consumed
    # exactly once. Materializing internally must not drop entries.
    def gen():
        yield RunResult(
            pipeline_name="serial", n_docs=10, duration_seconds=1.0, docs_per_second=10.0
        )
        yield RunResult(
            pipeline_name="async", n_docs=10, duration_seconds=0.5, docs_per_second=20.0
        )

    with_speedup = attach_speedup(gen())
    assert [r.pipeline_name for r in with_speedup] == ["serial", "async"]
    assert with_speedup[1].speedup_vs_serial == pytest.approx(2.0)


# ---------------------------------------------------------------------
# bench_1000_doc.py script
# ---------------------------------------------------------------------


def test_bench_script_writes_markdown_and_json(tmp_path: Path):
    from bench_1000_doc import main

    out_md = tmp_path / "bench.md"
    rc = main(
        [
            "--n",
            "20",
            "--latency",
            "0.001",
            "--concurrency",
            "4",
            "--batch-size",
            "4",
            "--out",
            str(out_md),
        ]
    )
    assert rc == 0
    assert out_md.exists()
    md = out_md.read_text(encoding="utf-8")
    # Markdown carries the workload disclosure and a row per pipeline.
    assert "Synthetic LLM disclosure" in md
    for name in ("serial", "async", "async+batched"):
        assert f"| {name} " in md

    out_json = out_md.with_suffix(".json")
    assert out_json.exists()
    payload = json.loads(out_json.read_text())
    assert payload["workload"]["n_docs"] == 20
    pipeline_names = {r["pipeline_name"] for r in payload["results"]}
    assert pipeline_names == {"serial", "async", "async+batched"}
