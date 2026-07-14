"""Single-script benchmark: serial vs async vs async+batched on N docs.

By default runs against `FakeLLM` (deterministic, dep-free,
CI-reproducible). The *speedup ratios* are load-bearing and meaningful
under the synthetic LLM; the *absolute latency* is per the simulated
per-call cost. Real-Anthropic numbers are an operator-side swap — see
the bottom of `docs/benchmarks.md`.

Usage:
    python scripts/bench_1000_doc.py
    python scripts/bench_1000_doc.py --n 200 --concurrency 16 --batch-size 8
    python scripts/bench_1000_doc.py --out docs/benchmarks.md
"""

from __future__ import annotations

import argparse
import asyncio
import platform
import sys
import time
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[1]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from async_pipelines.benchmark import (  # noqa: E402
    AsyncPipeline,
    BatchedAsyncPipeline,
    FakeLLM,
    RunResult,
    SerialPipeline,
    Workload,
    attach_speedup,
    dump_benchmark_json,
    make_batch_caller,
    run_pipeline,
)
from async_pipelines.io_utils import atomic_write_text  # noqa: E402


async def _run_all(workload: Workload) -> list[RunResult]:
    docs = [f"doc-{i:04d}" for i in range(workload.n_docs)]

    serial = SerialPipeline(
        FakeLLM(latency_seconds=workload.llm_call_seconds, call_id="llm1-serial"),
        FakeLLM(latency_seconds=workload.llm_call_seconds, call_id="llm2-serial"),
    )
    async_pipe = AsyncPipeline(
        FakeLLM(latency_seconds=workload.llm_call_seconds, call_id="llm1-async"),
        FakeLLM(latency_seconds=workload.llm_call_seconds, call_id="llm2-async"),
        concurrency=workload.concurrency,
    )
    batched_pipe = BatchedAsyncPipeline(
        make_batch_caller(
            FakeLLM(latency_seconds=workload.llm_call_seconds, call_id="llm1-batched"),
            batch_seconds=workload.llm_call_seconds,
        ),
        make_batch_caller(
            FakeLLM(latency_seconds=workload.llm_call_seconds, call_id="llm2-batched"),
            batch_seconds=workload.llm_call_seconds,
        ),
        concurrency=workload.concurrency,
        batch_size=workload.batch_size,
    )

    results: list[RunResult] = []
    for pipeline in (serial, async_pipe, batched_pipe):
        result = await run_pipeline(pipeline, docs)
        results.append(result)
    return attach_speedup(results)


def render_markdown(workload: Workload, results: list[RunResult]) -> str:
    lines: list[str] = []
    lines.append("# Async-pipeline benchmarks (issue #4)")
    lines.append("")
    lines.append(
        f"- **Workload.** {workload.n_docs} docs · 2 LLM calls per doc · "
        f"{workload.llm_call_seconds * 1000:.0f} ms simulated per call · "
        f"concurrency {workload.concurrency} · batch size {workload.batch_size}"
    )
    lines.append(
        f"- **Synthetic LLM disclosure.** Each call is a deterministic "
        f"`await asyncio.sleep({workload.llm_call_seconds})`. The speedup "
        "ratios are load-bearing under this model; the absolute latency "
        "is per the simulated cost. Real-API numbers are a `FakeLLM` → "
        "`AnthropicLLM` swap; the `LLMClient` Protocol is the seam."
    )
    lines.append(
        f"- **Host.** {platform.python_implementation()} {platform.python_version()} "
        f"on {platform.system()} {platform.machine()}, "
        f"run on {time.strftime('%Y-%m-%d')}."
    )
    lines.append("")
    lines.append("| pipeline | duration (s) | docs/s | speedup vs serial |")
    lines.append("| -------- | -----------: | -----: | ----------------: |")
    for r in results:
        speedup = "—" if r.speedup_vs_serial is None else f"{r.speedup_vs_serial:.2f}×"
        lines.append(
            f"| {r.pipeline_name} | {r.duration_seconds:.3f} | "
            f"{r.docs_per_second:.1f} | {speedup} |"
        )
    lines.append("")
    lines.append("## Reproduce")
    lines.append("")
    lines.append("```bash")
    lines.append(
        f"python scripts/bench_1000_doc.py --n {workload.n_docs} "
        f"--concurrency {workload.concurrency} --batch-size {workload.batch_size}"
    )
    lines.append("```")
    lines.append("")
    lines.append("## Real-API mode (operator action)")
    lines.append("")
    lines.append(
        "Swap `FakeLLM` for an Anthropic adapter that conforms to the "
        "`LLMClient` Protocol (`async __call__(prompt: str) -> str`) and "
        "re-run. The same script writes the same table; the speedup "
        "ratios will widen because real API I/O has more headroom for "
        "fan-out than the synthetic 20 ms sleep does."
    )
    lines.append("")
    return "\n".join(lines)


async def amain(args: argparse.Namespace) -> int:
    # Translate a bad operator input (n_docs/concurrency/batch_size < 1,
    # non-finite/negative latency) to a clean stderr line + exit 2 instead of
    # letting `Workload.__post_init__`'s ValueError escape as a raw traceback
    # at exit 1. This mirrors the exit-2 input-validation contract the sibling
    # `bench_backpressure.py:main_async` already honors (#76); the field-named
    # message from `__post_init__` is preserved so the operator still learns
    # which flag was wrong.
    try:
        workload = Workload(
            n_docs=args.n,
            llm_call_seconds=args.latency,
            concurrency=args.concurrency,
            batch_size=args.batch_size,
        )
    except ValueError as e:
        print(f"invalid workload: {e}", file=sys.stderr)
        return 2
    results = await _run_all(workload)
    md = render_markdown(workload, results)
    out_path = Path(args.out)
    # The output path is operator input too: an unwritable `--out` (a read-only
    # filesystem, a permission-denied dir, or a path component that is a file)
    # makes `atomic_write_text` raise OSError. Without this guard it escaped
    # `amain` as a raw traceback at exit 1 — the "success" range — *after* the
    # benchmark already ran, breaking the same exit-2 operator-input contract the
    # `Workload(...)` guard above honors. Translate it to a clean stderr line +
    # exit 2 (write-seam sibling of llm-eval-harness #158/#159).
    try:
        atomic_write_text(out_path, md)
        print(md)
        print(f"\nbenchmarks wrote {out_path}")
        # Stash raw results next to the markdown for further analysis.
        json_path = out_path.with_suffix(".json")
        if json_path == out_path:
            # `--out foo.json`: with_suffix(".json") is a no-op when the suffix is
            # already .json, so the JSON dump would clobber the markdown report we
            # just wrote. Append instead of replace so both artifacts survive.
            json_path = out_path.with_name(out_path.name + ".json")
        dump_benchmark_json(json_path, workload=workload, results=results)
        print(f"raw results wrote {json_path}")
    except OSError as e:
        print(f"could not write report: {e}", file=sys.stderr)
        return 2
    return 0


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--n", type=int, default=1000, help="Number of docs in the workload.")
    p.add_argument(
        "--latency",
        type=float,
        default=0.020,
        help="Simulated per-call LLM latency in seconds.",
    )
    p.add_argument("--concurrency", type=int, default=32, help="Fan-out width.")
    p.add_argument("--batch-size", type=int, default=8, help="Batch size for the batched pipeline.")
    p.add_argument(
        "--out",
        default="docs/benchmarks.md",
        help="Where to write the markdown report (and `.json` raw beside it).",
    )
    args = p.parse_args(argv)
    return asyncio.run(amain(args))


if __name__ == "__main__":
    sys.exit(main())
