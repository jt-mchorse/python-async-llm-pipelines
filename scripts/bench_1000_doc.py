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
        # `pipeline_name` is the one free-form cell here — every other is a
        # formatted number. It arrives via `run_pipeline(pipeline: Any, docs)`
        # → `pipeline.name`, so any caller-supplied object with `.name` and
        # `.run` reaches it. GFM splits table cells on unescaped pipes, so a
        # `|` in the name added a column the header and separator lacked (data
        # row 5 vs header 4) and GitHub drew a mangled grid — in
        # `docs/benchmarks.md`, which is committed (#92). Escape `|` -> `\|`,
        # which GitHub renders as a literal pipe contributing zero delimiters,
        # so a genuinely pipe-bearing name still displays. Same fix as
        # `load._escape_cell` (vector-search-at-scale #125),
        # `aggregate_markdown` (embedding-model-shootout #79) and
        # `comment._row_to_md` (rag-production-kit #130).
        #
        # Backticks are deliberately not neutralized: this cell is not an
        # inline-code span, so a backtick is inert (verified — the table stays
        # aligned). That variant is real only where the cell *is* a code span,
        # which is what rag-production-kit #130 handles.
        # Same em-dash convention as `speedup` above: a zero-duration run has an
        # undefined rate, and printing `0.0` would rank the fastest row last in
        # the column a reader scans first (#94).
        dps = "—" if r.docs_per_second is None else f"{r.docs_per_second:.1f}"
        pipeline_name = r.pipeline_name.replace("|", "\\|")
        lines.append(f"| {pipeline_name} | {r.duration_seconds:.3f} | {dps} | {speedup} |")
    lines.append("")
    lines.append("## Reproduce")
    lines.append("")
    lines.append("```bash")
    # Every workload flag, `--latency` included, and an `--out` outside `docs/`
    # (#122). The block dropped both: following it re-measured at the default
    # 20 ms whatever latency the report was run at, and -- the default `--out`
    # being `docs/benchmarks.md` -- overwrote the committed artifacts, the #120
    # harm on the one spelling #121 did not reach. `repr` of a float round-trips,
    # so the latency reads back as exactly the value measured.
    lines.append(
        f"python scripts/bench_1000_doc.py --n {workload.n_docs} "
        f"--latency {workload.llm_call_seconds!r} "
        f"--concurrency {workload.concurrency} --batch-size {workload.batch_size} "
        "--out /tmp/bench.md"
    )
    lines.append("```")
    lines.append("")
    lines.append("## Real-API mode (operator action)")
    lines.append("")
    # The direction of this claim is load-bearing and it used to be backwards
    # (#108). It said the ratios "will widen because real API I/O has more
    # headroom for fan-out than the synthetic 20 ms sleep does", which inverts
    # the README's own honest-framing paragraph: a pure `await
    # asyncio.sleep(0.02)` has zero per-request CPU, socket, TLS and JSON cost,
    # which is exactly why it fans out perfectly, so the synthetic ratio is the
    # CEILING. Real I/O adds all of that plus rate limits and connection-pool
    # limits, and fans out worse. `scripts/capture_demo.sh` carried the same
    # inverted sentence and is corrected with it;
    # `tests/test_real_api_claim_direction.py` locks the direction of both,
    # rather than either phrasing.
    lines.append(
        "Swap `FakeLLM` for an Anthropic adapter that conforms to the "
        "`LLMClient` Protocol (`async __call__(prompt: str) -> str`) and "
        "re-run. The same script writes the same table. Expect the speedup "
        "ratios to be **lower** than the synthetic ones above, not higher: "
        "`FakeLLM`'s pure-wait `asyncio.sleep` has no per-request CPU, socket, "
        "TLS or JSON cost, so it fans out perfectly and the ratios here are "
        "the theoretical upper bound. Real API I/O adds that overhead and is "
        "additionally bounded by rate limits and connection-pool limits, so "
        "real-API speedups land in the 5-20x spec range. Batch API workloads "
        "are the documented exception and can exceed it."
    )
    lines.append("")
    return "\n".join(lines)


def workload_from_args(args: argparse.Namespace) -> Workload:
    """The flag-to-workload mapping, in one place.

    Named and separated so the provenance lock (#113) resolves a documented
    command through *this* mapping rather than re-deriving it. A lock that
    re-implements the mapping can agree with a stale copy of it; this one
    cannot drift from the script by construction.
    """
    return Workload(
        n_docs=args.n,
        llm_call_seconds=args.latency,
        concurrency=args.concurrency,
        batch_size=args.batch_size,
    )


async def amain(args: argparse.Namespace) -> int:
    # Translate a bad operator input (n_docs/concurrency/batch_size < 1,
    # non-finite/negative latency) to a clean stderr line + exit 2 instead of
    # letting `Workload.__post_init__`'s ValueError escape as a raw traceback
    # at exit 1. This mirrors the exit-2 input-validation contract the sibling
    # `bench_backpressure.py:main_async` already honors (#76); the field-named
    # message from `__post_init__` is preserved so the operator still learns
    # which flag was wrong.
    try:
        workload = workload_from_args(args)
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
        json_path = _json_path_for(out_path)
        dump_benchmark_json(json_path, workload=workload, results=results)
        print(f"raw results wrote {json_path}")
    except OSError as e:
        print(f"could not write report: {e}", file=sys.stderr)
        return 2
    return 0


def _json_path_for(out_path: Path) -> Path:
    """The raw-results path that belongs to the report at `out_path` (#118).

    Replace a `.md` suffix, append `.json` to anything else. This used to be
    `with_suffix(".json")` plus one guard for `--out foo.json`, whose comment
    gave the remedy -- "Append instead of replace so both artifacts survive" --
    for the one collision it considered, the JSON landing on its own report.
    `with_suffix` replaces whatever follows the last dot, so `--out run.a` and
    `--out run.b` both wrote `run.json`, and the second run silently overwrote
    the first run's raw results. Applying the guard's remedy to every non-`.md`
    suffix covers both: `foo.json` still gets `foo.json.json`,
    `docs/benchmarks.md` still gets `docs/benchmarks.json`.
    """
    if out_path.suffix == ".md":
        return out_path.with_suffix(".json")
    return out_path.with_name(out_path.name + ".json")


def build_arg_parser() -> argparse.ArgumentParser:
    """The CLI surface, separated from running it.

    Factored out of `main` so the provenance lock can resolve a *documented*
    command against this parser without executing a ~45s benchmark (#113).
    Comparing the resolved namespace rather than the command string is what
    catches a flag nobody writes: no documented spelling passes `--latency`, so
    the committed `llm_call_seconds` comes from the default below, and a
    string-matching lock would stay green if this default moved.
    """
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
    return p


def main(argv: list[str] | None = None) -> int:
    args = build_arg_parser().parse_args(argv)
    return asyncio.run(amain(args))


if __name__ == "__main__":
    sys.exit(main())
