"""1000-doc benchmark: serial vs async vs async+batched (issue #4).

A `Workload` defines the task: `n_docs` documents through a 2-LLM-call
pipeline, where each call takes `llm_call_seconds` to complete. The
benchmark times three implementations:

- `SerialPipeline` — naive sequential loop. The baseline.
- `AsyncPipeline` — bounded fan-out across items, using the existing
  `process(items, fn, concurrency)` primitive from #1.
- `BatchedAsyncPipeline` — same fan-out, plus batched LLM calls (one
  consolidated request per batch instead of one per doc). Mirrors the
  shape of Anthropic's Batch API for nightly evals etc.

`FakeLLM` is the synthetic in-process LLM used in CI: deterministic
latency via `asyncio.sleep`. The benchmark's *speedup ratios* are
load-bearing and meaningful with the fake; absolute wall-clock is per
the simulated latency. Real-Anthropic-API mode is a documented one-line
swap: anything matching the `LLMClient` Protocol works (D-008).
"""

from __future__ import annotations

import asyncio
import json
import math
import time
from collections.abc import Awaitable, Callable, Iterable, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Protocol

from .core import process
from .io_utils import atomic_write_text


@dataclass(frozen=True)
class Workload:
    """The task definition.

    `n_docs` documents pass through `llm1` and then `llm2`. The total
    LLM work per doc is `2 * llm_call_seconds` in serial; the fan-out
    and batching pipelines compress that based on concurrency / batch
    structure.
    """

    n_docs: int
    llm_call_seconds: float = 0.020
    """Per-call simulated latency. 20 ms approximates a fast cheap-model
    completion against a warmed-up service."""
    concurrency: int = 32
    """Bounded fan-out width for async / batched pipelines."""
    batch_size: int = 8
    """Documents per batch in the batched pipeline; 1 means no
    batching (and the batched pipeline degenerates to AsyncPipeline)."""

    def __post_init__(self) -> None:
        # `n_docs=0` produces an empty docs list and a near-zero duration —
        # speedup math then divides by zero or yields `inf`. The other three
        # fields are also validated at pipeline-constructor time (#28), but
        # surfacing the failure at the Workload construction site points the
        # operator at the misconfigured input instead of an inner factory call.
        # Integer + finite guards (#32). Pre-#32 NaN/Infinity/fractional/bool
        # slipped past sign-only checks: NaN propagates into asyncio.Semaphore
        # / Queue at runtime as cryptic deep TypeErrors; fractional silently
        # truncates downstream; bool subclasses int and flattens operator
        # intent. NaN llm_call_seconds skews the published throughput numbers
        # because the simulated-latency sleep becomes platform-dependent.
        if not isinstance(self.n_docs, int) or isinstance(self.n_docs, bool):
            raise ValueError(f"n_docs must be an int; got {self.n_docs!r}")
        if self.n_docs < 1:
            raise ValueError(f"n_docs must be >= 1; got {self.n_docs}")
        if not math.isfinite(self.llm_call_seconds) or self.llm_call_seconds < 0.0:
            raise ValueError(
                f"llm_call_seconds must be a finite number >= 0.0; got {self.llm_call_seconds!r}"
            )
        if not isinstance(self.concurrency, int) or isinstance(self.concurrency, bool):
            raise ValueError(f"concurrency must be an int; got {self.concurrency!r}")
        if self.concurrency < 1:
            raise ValueError(f"concurrency must be >= 1; got {self.concurrency}")
        if not isinstance(self.batch_size, int) or isinstance(self.batch_size, bool):
            raise ValueError(f"batch_size must be an int; got {self.batch_size!r}")
        if self.batch_size < 1:
            raise ValueError(f"batch_size must be >= 1; got {self.batch_size}")

    def to_dict(self) -> dict[str, Any]:
        """JSON-stable dict of the four fields. Pinning the surface here so
        downstream JSON consumers (notebook, CI parser, dashboard) aren't
        coupled to `dataclasses.asdict`'s greedy behavior — a future
        internal-only field on `Workload` shouldn't ship in the JSON
        contract. Mirrors the observability-parity pattern landed in
        rag-production-kit#51 / llm-cost-optimizer#51/#53 (2026-06-01).
        """
        return {
            "n_docs": self.n_docs,
            "llm_call_seconds": self.llm_call_seconds,
            "concurrency": self.concurrency,
            "batch_size": self.batch_size,
        }


@dataclass(frozen=True)
class RunResult:
    """Output of one pipeline's `run`."""

    pipeline_name: str
    n_docs: int
    duration_seconds: float
    docs_per_second: float
    speedup_vs_serial: float | None = None
    extra: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """JSON-stable dict of the six fields; same rationale as
        `Workload.to_dict`. `extra` is shallow-copied so callers cannot
        accidentally mutate the frozen `RunResult`'s default through the
        returned dict. `speedup_vs_serial` is preserved as `None` when
        unattached (the serial baseline) — JSON consumers route on
        `None` to skip the speedup column for that row.
        """
        return {
            "pipeline_name": self.pipeline_name,
            "n_docs": self.n_docs,
            "duration_seconds": self.duration_seconds,
            "docs_per_second": self.docs_per_second,
            "speedup_vs_serial": self.speedup_vs_serial,
            "extra": dict(self.extra),
        }


class LLMClient(Protocol):
    """Single-method seam for the LLM call.

    Real-API adapters (Anthropic SDK wrapper) conform by implementing
    `async __call__(prompt: str) -> str`. `FakeLLM` is the synthetic
    reference; everything else is BYO.
    """

    async def __call__(self, prompt: str) -> str: ...


@dataclass
class FakeLLM:
    """Synthetic LLM: returns a deterministic echo after a sleep.

    The sleep is the load-bearing detail — it simulates I/O wait so
    bounded fan-out exhibits the expected ~5–20× speedup. The output
    string is just `"<prompt>:<call_id>"` so a downstream consumer can
    distinguish calls.
    """

    latency_seconds: float = 0.020
    call_id: str = "fake"
    call_count: int = field(default=0, init=False)

    async def __call__(self, prompt: str) -> str:
        self.call_count += 1
        await asyncio.sleep(self.latency_seconds)
        return f"{prompt}:{self.call_id}"


# ---------------------------------------------------------------------
# Pipelines
# ---------------------------------------------------------------------


class SerialPipeline:
    """Naive `for doc in docs: await llm1; await llm2`. Baseline."""

    name = "serial"

    def __init__(self, llm1: LLMClient, llm2: LLMClient) -> None:
        self.llm1 = llm1
        self.llm2 = llm2

    async def run(self, docs: list[str]) -> list[str]:
        out: list[str] = []
        for doc in docs:
            mid = await self.llm1(doc)
            final = await self.llm2(mid)
            out.append(final)
        return out


class AsyncPipeline:
    """Bounded fan-out over items via `process(items, fn, concurrency)`.

    Each item runs both LLM calls in sequence inside `fn` — the
    parallelism is across items, not within a single item's two calls.
    Per-item ordering is preserved by `process`.
    """

    name = "async"

    def __init__(
        self,
        llm1: LLMClient,
        llm2: LLMClient,
        *,
        concurrency: int,
    ) -> None:
        # Eager validation: `process()` itself enforces `concurrency > 0`
        # at call-time, but a misconfigured workload spec should surface
        # at construction not at the first `run()` — parity with
        # BatchedAsyncPipeline's batch_size guard below. The isinstance
        # check (#34) matches `Workload.__post_init__` and `process()` so
        # `True` / `1.5` / `NaN` fail loud here, not deep in `process()`
        # which would point at the wrong site.
        if not isinstance(concurrency, int) or isinstance(concurrency, bool):
            raise ValueError(f"concurrency must be an int; got {concurrency!r}")
        if concurrency < 1:
            raise ValueError(f"concurrency must be >= 1; got {concurrency}")
        self.llm1 = llm1
        self.llm2 = llm2
        self.concurrency = concurrency

    async def run(self, docs: list[str]) -> list[str]:
        async def process_one(doc: str) -> str:
            mid = await self.llm1(doc)
            return await self.llm2(mid)

        return await process(docs, process_one, concurrency=self.concurrency)


class BatchedAsyncPipeline:
    """Same fan-out, plus one consolidated LLM call per batch of docs.

    The Batch-API-style flow: collect `batch_size` docs into one call,
    pay one round trip per batch instead of one per doc. In the
    synthetic model that's `O(1)` latency per batch regardless of size;
    in production it's "one HTTP request per batch" with the model
    processing them on its side.

    Per-doc ordering inside a batch is preserved by index.
    """

    name = "async+batched"

    def __init__(
        self,
        llm1_batch: Callable[[list[str]], Awaitable[list[str]]],
        llm2_batch: Callable[[list[str]], Awaitable[list[str]]],
        *,
        concurrency: int,
        batch_size: int,
    ) -> None:
        # Eager validation parity with `AsyncPipeline.__init__` (#34) — fail
        # at construction with the misconfig'd field named, not deep in
        # `process()`.
        if not isinstance(concurrency, int) or isinstance(concurrency, bool):
            raise ValueError(f"concurrency must be an int; got {concurrency!r}")
        if concurrency < 1:
            raise ValueError(f"concurrency must be >= 1; got {concurrency}")
        if not isinstance(batch_size, int) or isinstance(batch_size, bool):
            raise ValueError(f"batch_size must be an int; got {batch_size!r}")
        if batch_size < 1:
            raise ValueError(f"batch_size must be >= 1; got {batch_size}")
        self.llm1_batch = llm1_batch
        self.llm2_batch = llm2_batch
        self.concurrency = concurrency
        self.batch_size = batch_size

    async def run(self, docs: list[str]) -> list[str]:
        batches = _chunk(docs, self.batch_size)

        async def process_batch(batch: list[str]) -> list[str]:
            mids = await self.llm1_batch(batch)
            return await self.llm2_batch(mids)

        outs = await process(batches, process_batch, concurrency=self.concurrency)
        # Flatten preserving order; each batch result is already in the
        # batch's input order, so concatenation reconstructs the original
        # doc sequence.
        flat: list[str] = []
        for batch_out in outs:
            flat.extend(batch_out)
        return flat


def _chunk(items: list[str], size: int) -> list[list[str]]:
    return [items[i : i + size] for i in range(0, len(items), size)]


# ---------------------------------------------------------------------
# Adapters: make a single FakeLLM look like a batch endpoint too
# ---------------------------------------------------------------------


def make_batch_caller(
    llm: LLMClient, *, batch_seconds: float | None = None
) -> Callable[[list[str]], Awaitable[list[str]]]:
    """Wrap a single-call LLM into a batch caller.

    In the synthetic-LLM case, the batch latency defaults to one call's
    latency (the `O(1)` per batch assumption). For a real LLM the
    wrapper would do an actual batched HTTP request; here we just
    `asyncio.gather` the per-item calls with a single sleep upfront,
    simulating a single round trip.
    """

    async def call_batch(items: list[str]) -> list[str]:
        # One simulated "round trip" for the batch, regardless of size.
        per_call = (
            batch_seconds if batch_seconds is not None else getattr(llm, "latency_seconds", 0.0)
        )
        await asyncio.sleep(per_call)
        # Produce a deterministic output for each item; we don't sleep
        # per item — the batch latency above is the only sleep.
        return [f"{item}:{getattr(llm, 'call_id', 'fake')}-batched" for item in items]

    return call_batch


# ---------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------


async def run_pipeline(pipeline: Any, docs: list[str]) -> RunResult:
    """Time a pipeline's `.run(docs)` call. Returns a `RunResult`."""
    t0 = time.perf_counter()
    await pipeline.run(docs)
    elapsed = time.perf_counter() - t0
    return RunResult(
        pipeline_name=pipeline.name,
        n_docs=len(docs),
        duration_seconds=elapsed,
        docs_per_second=(len(docs) / elapsed) if elapsed > 0 else 0.0,
    )


def dump_benchmark_json(
    path: str | Path,
    *,
    workload: Workload,
    results: Sequence[RunResult],
) -> None:
    """Write ``{"workload": ..., "results": [...]}`` atomically to ``path``.

    Centralizes the previously-inlined `asdict(workload)` / `[asdict(r)
    for r in results]` payload assembly that lived in `scripts/
    bench_1000_doc.py` (lines 142-155 before #44) and `scripts/
    bench_backpressure.py` (lines 162-170 before #44). The JSON shape
    is byte-identical to the pre-#44 `bench_1000_doc` output: same
    top-level keys, same `sort_keys=True`, same `indent=2`, same
    per-result field set.

    Uses ``async_pipelines.io_utils.atomic_write_text`` (D-011) so a
    crashed write doesn't leave a partial file at ``path``.

    Mirrors the observability surface landed in rag-production-kit#51
    (``TelemetryStore.dump_aggregate_json``) and llm-cost-optimizer
    #51/#53 (``PromptCacheWrapper.dump_aggregate_json`` /
    ``SemanticCache.dump_stats_json``). Module-level here because
    benchmark output isn't tied to a holding class — `Workload` and
    `list[RunResult]` are the natural carriers.
    """
    payload = {
        "workload": workload.to_dict(),
        "results": [r.to_dict() for r in results],
    }
    atomic_write_text(path, json.dumps(payload, indent=2, sort_keys=True))


def attach_speedup(results: Iterable[RunResult]) -> list[RunResult]:
    """Set each result's `speedup_vs_serial` field. Returns a new list,
    one entry per input, in input order.

    Output is mapped over the materialized input list — not a
    ``{name: result}`` dict — so repeated measurements of the same
    pipeline (a normal benchmarking practice: run each pipeline N times
    for stable timings) all survive and input order is preserved. The
    name lookup is used only to find the ``serial`` baseline; when more
    than one ``serial`` row is present the first is taken as the baseline.
    """
    # Materialize once: `results` may be a one-shot generator, and we both
    # scan it for the serial baseline and map over every entry. Routing the
    # output through a name-keyed dict (the prior shape) silently dropped
    # duplicate-named measurements and coupled output identity to the dict
    # instead of the input sequence.
    materialized = list(results)
    serial = next((r for r in materialized if r.pipeline_name == "serial"), None)
    serial_duration = serial.duration_seconds if serial else None
    out: list[RunResult] = []
    for r in materialized:
        if serial_duration and serial_duration > 0:
            # A candidate that ran in zero time is *infinitely fast* — the ratio
            # is undefined, so signal it with None (the same convention used when
            # the serial baseline itself is zero), not 0.0 (which reads as the
            # slowest-possible result and would mis-rank it on a dashboard/plot).
            speedup = serial_duration / r.duration_seconds if r.duration_seconds > 0 else None
        else:
            speedup = None
        out.append(
            RunResult(
                pipeline_name=r.pipeline_name,
                n_docs=r.n_docs,
                duration_seconds=r.duration_seconds,
                docs_per_second=r.docs_per_second,
                speedup_vs_serial=speedup,
                extra=r.extra,
            )
        )
    return out
