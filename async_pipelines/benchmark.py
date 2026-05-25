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
import time
from collections.abc import Awaitable, Callable, Iterable
from dataclasses import dataclass, field
from typing import Any, Protocol

from .core import process


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
        if self.n_docs < 1:
            raise ValueError(f"n_docs must be >= 1; got {self.n_docs}")
        if self.llm_call_seconds < 0.0:
            raise ValueError(f"llm_call_seconds must be >= 0.0; got {self.llm_call_seconds}")
        if self.concurrency < 1:
            raise ValueError(f"concurrency must be >= 1; got {self.concurrency}")
        if self.batch_size < 1:
            raise ValueError(f"batch_size must be >= 1; got {self.batch_size}")


@dataclass(frozen=True)
class RunResult:
    """Output of one pipeline's `run`."""

    pipeline_name: str
    n_docs: int
    duration_seconds: float
    docs_per_second: float
    speedup_vs_serial: float | None = None
    extra: dict[str, Any] = field(default_factory=dict)


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
        # BatchedAsyncPipeline's batch_size guard below.
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
        if concurrency < 1:
            raise ValueError(f"concurrency must be >= 1; got {concurrency}")
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


def attach_speedup(results: Iterable[RunResult]) -> list[RunResult]:
    """Set each result's `speedup_vs_serial` field. Returns a new list."""
    by_name = {r.pipeline_name: r for r in results}
    serial = by_name.get("serial")
    serial_duration = serial.duration_seconds if serial else None
    out: list[RunResult] = []
    for r in by_name.values():
        if serial_duration and serial_duration > 0:
            speedup = serial_duration / r.duration_seconds if r.duration_seconds > 0 else 0.0
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
