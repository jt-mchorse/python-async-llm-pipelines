"""Backpressure demo (#3): fast producer, slow consumer, bounded queue.

Shows the backpressure bound: when the producer emits items faster than
the consumer can drain them, ``stream``'s bounded queue holds the number
of items *waiting to be processed* to ``queue_size`` regardless of how
many items the producer would emit. Without backpressure, a naive
`asyncio.Queue` with no ``maxsize`` (or a plain list) grows linearly
with producer rate.

It does not bound peak heap, and this docstring used to say it did
(#115). ``stream`` returns every result in one list, so the
``peak_heap_kb`` column grows with ``n`` -- the table this script writes
shows it, and the claim sentence it renders now says so from the rows.

Numbers are real and reproducible. ``--out-md``/``--out-json`` default to the
COMMITTED ``docs/backpressure.{md,json}``, so a run that omits them rewrites
the artifact the doc-surface tests pin (#132). To explore:

    python scripts/bench_backpressure.py --n 5000 --queue-size 8 \\
        --consumer-ms 1 --concurrency 2 \\
        --out-md /tmp/backpressure.md --out-json /tmp/backpressure.json

To regenerate the committed artifact (the README's provenance command):

    python scripts/bench_backpressure.py --n 5000 --queue-size 8 \\
        --consumer-ms 1 --concurrency 2 --compare --compare-n  # regenerates the committed snapshot

That writes ``docs/backpressure.md`` and ``docs/backpressure.json`` with the
measured peak-heap and pause-time numbers. The bench is dep-free
(``tracemalloc`` and ``asyncio`` are stdlib) so D-002 holds.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import math
import sys
import time
import tracemalloc
from collections.abc import AsyncIterator
from dataclasses import dataclass
from pathlib import Path
from typing import Any

# Allow ``python scripts/bench_backpressure.py`` from a fresh checkout.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from async_pipelines import StreamMetrics, stream  # noqa: E402
from async_pipelines.io_utils import atomic_write_text  # noqa: E402


@dataclass
class BackpressureResult:
    """One run's measured numbers."""

    n: int
    queue_size: int
    consumer_ms: float
    concurrency: int
    duration_s: float
    peak_heap_kb: float
    metrics: dict[str, float | int]

    def to_dict(self) -> dict[str, Any]:
        # Seven-field contract (#46). `metrics` is shallow-copied so
        # caller mutation of the returned dict's metrics doesn't bleed
        # back into the BackpressureResult instance.
        return {
            "n": self.n,
            "queue_size": self.queue_size,
            "consumer_ms": self.consumer_ms,
            "concurrency": self.concurrency,
            "duration_s": self.duration_s,
            "peak_heap_kb": self.peak_heap_kb,
            "metrics": dict(self.metrics),
        }


async def _fast_producer(n: int) -> AsyncIterator[int]:
    """A producer with zero per-item latency — the worst case for a
    slow consumer because every item is ready immediately.
    """
    for i in range(n):
        yield i


def _make_slow_consumer(consumer_ms: float):
    async def consume(x: int) -> int:
        await asyncio.sleep(consumer_ms / 1000.0)
        return x

    return consume


async def run_one(
    *, n: int, queue_size: int, consumer_ms: float, concurrency: int
) -> BackpressureResult:
    """Run a single (n, queue_size, consumer_ms, concurrency) cell and
    return the measured numbers.
    """
    m = StreamMetrics()
    consume = _make_slow_consumer(consumer_ms)

    tracemalloc.start()
    start = time.perf_counter()
    await stream(
        _fast_producer(n),
        consume,
        concurrency=concurrency,
        queue_size=queue_size,
        metrics=m,
    )
    duration_s = time.perf_counter() - start
    _current, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()

    return BackpressureResult(
        n=n,
        queue_size=queue_size,
        consumer_ms=consumer_ms,
        concurrency=concurrency,
        duration_s=duration_s,
        peak_heap_kb=peak / 1024.0,
        metrics=m.to_dict(),
    )


def _results_are_o_n_sentence(results: list[BackpressureResult]) -> str:
    """The half of the claim the queue bound does not cover (#115).

    Quotes the heap growth from the rows when they span `n` at one
    `queue_size`; otherwise states the fact without a number rather than
    inventing one.
    """
    head = (
        "That is a bound on the input side, not on the process: `stream` returns "
        "every result in one list, so results are O(n) and `stream` is not safe "
        "on a source that never ends -- it never returns"
    )
    by_queue: dict[int, list[BackpressureResult]] = {}
    for r in results:
        by_queue.setdefault(r.queue_size, []).append(r)
    for queue_size in sorted(by_queue):
        rows = sorted(by_queue[queue_size], key=lambda r: r.n)
        lo, hi = rows[0], rows[-1]
        if lo.n != hi.n and lo.peak_heap_kb > 0:
            return (
                f"{head}. In this table, at `queue_size` {queue_size}, "
                f"`peak_heap_kb` goes {lo.peak_heap_kb:.1f} → {hi.peak_heap_kb:.1f} "
                f"({hi.peak_heap_kb / lo.peak_heap_kb:.1f}×) as `n` goes "
                f"{lo.n} → {hi.n} ({hi.n / lo.n:.1f}×)."
            )
    return f"{head}."


def _render_markdown(results: list[BackpressureResult]) -> str:
    lines: list[str] = []
    lines.append("# Backpressure demo (#3)\n")
    lines.append(
        "Generated by `scripts/bench_backpressure.py`. The producer emits "
        "items with zero per-item latency; the consumer sleeps "
        "`--consumer-ms` per item. `stream`'s bounded `asyncio.Queue` "
        "applies backpressure when the consumer can't keep up.\n"
    )
    lines.append("## Measured\n")
    lines.append(
        "| n | queue_size | consumer_ms | concurrency | duration_s | "
        "peak_heap_kb | producer_pauses | max_queue_depth | pause_seconds |\n"
        "|---:|---:|---:|---:|---:|---:|---:|---:|---:|"
    )
    for r in results:
        m = r.metrics
        lines.append(
            f"| {r.n} | {r.queue_size} | {r.consumer_ms} | {r.concurrency} | "
            f"{r.duration_s:.3f} | {r.peak_heap_kb:.1f} | "
            f"{m['producer_pauses']} | {m['max_queue_depth']} | "
            f"{m['producer_pause_seconds']:.3f} |"
        )
    lines.append("")
    lines.append("## OOM-safety claim\n")
    # The claim names the evidence it actually has (#111). It used to say the bound
    # holds "regardless of `n`" while every row shared one `n` -- there was no `n`
    # axis in this script to vary -- so the document asserted something its own
    # table could not show. `n_values` below is computed from the rows rather than
    # assumed, so the sentence degrades honestly when the script is run without
    # `--compare-n` instead of over-claiming again.
    n_values = sorted({r.n for r in results})
    queue_values = sorted({r.queue_size for r in results})
    lines.append(
        f"`max_queue_depth` is bounded by `queue_size` in every row above "
        f"({len(results)} rows, queue_size {queue_values}, n {n_values}). That "
        f"bounds the input *waiting to be processed*: a fast producer cannot pile "
        f"items up ahead of a slow consumer, at any `n`."
    )
    lines.append("")
    # And what it does not bound (#115). This sentence used to end "peak
    # in-memory items are O(queue_size)", printed under a `peak_heap_kb` column
    # that grew ~10x with `n` in the same table. `stream` returns every result in
    # one list, so results are O(n). The heap figures are quoted from the rows
    # themselves, so the sentence cannot contradict the column beside it.
    lines.append(_results_are_o_n_sentence(results))
    lines.append("")

    # Evidence for the bound is a row where the bound APPLIED (#134). A
    # `--compare-n` row at n // 10 can be smaller than the queue: n=50 gave an
    # n=5 row with queue_size 8, max_queue_depth 5 and no producer pauses -- the
    # queue never filled, its depth followed `n`, and the paragraph still called
    # the rows "evidence for the `n`-independence of the bound".
    def saturated(r: BackpressureResult) -> bool:
        return r.metrics["max_queue_depth"] >= r.queue_size or r.metrics["producer_pauses"] > 0

    saturated_n = sorted({r.n for r in results if saturated(r)})
    unfilled_n = sorted({r.n for r in results if not saturated(r)})
    proof = (
        "The general claim is proved where it belongs, over a parameterised table "
        "in `tests/test_stream.py` -- "
        "`test_stream_metrics_max_depth_bounded_by_queue_size` -- which is "
        "host-independent; this table is the measured illustration."
    )
    if len(saturated_n) > 1 and not unfilled_n:
        # Every row filled the queue: the sentence the committed snapshot carries.
        lines.append(
            f"The rows span more than one `n` ({n_values}) at a fixed `queue_size`, "
            f"which is what makes this table evidence for the `n`-independence of "
            f"the bound rather than only for its value at one workload size. {proof}"
        )
    elif len(saturated_n) > 1:
        lines.append(
            f"The rows that filled the queue span more than one `n` ({saturated_n}) "
            f"at a fixed `queue_size`, which is what makes them evidence for the "
            f"`n`-independence of the bound rather than only for its value at one "
            f"workload size. {proof}"
        )
    else:
        # With one `n` the advice used to be "Re-run with `--compare-n`" even
        # when `--compare-n` had been passed: at `--n 1` its extra row is
        # max(1, 1 // 10) = 1 again (#134). State the condition instead.
        q = max(queue_values)
        lines.append(
            f"The rows that filled the queue share one `n` ({saturated_n or n_values}), "
            f"so they show the bound at a single workload size and not its "
            f"independence from `n`. A varied-`n` row is evidence only if it also "
            f"fills the queue: run `--compare-n` with an `--n` large enough that "
            f"`n // {_COMPARE_N_DIVISOR}` exceeds the `queue_size` ({q}). {proof}"
        )
    if unfilled_n:
        lines.append("")
        lines.append(
            f"Rows at `n` {unfilled_n} never filled the queue (`max_queue_depth` "
            f"below `queue_size`, no producer pauses), so the bound never applied "
            f"to them: their depth followed `n`."
        )
    lines.append("")
    return "\n".join(lines)


async def main_async(args: argparse.Namespace) -> int:
    if args.n <= 0 or args.queue_size <= 0 or args.concurrency <= 0:
        print("n, queue-size, and concurrency must all be positive", file=sys.stderr)
        return 2
    if not math.isfinite(args.consumer_ms) or args.consumer_ms < 0:
        # `< 0` alone lets nan/inf through (both comparisons are False), and both
        # are valid float() inputs argparse accepts. A non-finite value flows into
        # asyncio.sleep(consumer_ms / 1000.0): nan raises `ValueError: Invalid
        # delay` (raw traceback, exit 1) and inf hangs forever. Mirror the
        # finiteness guard the timing seams enforce (Workload.llm_call_seconds,
        # make_batch_caller.batch_seconds #62) so bad operator input lands as a
        # clean exit 2 (#76), never a traceback or a silent hang.
        print("consumer-ms must be a finite number >= 0", file=sys.stderr)
        return 2

    cells: list[tuple[int, int, float, int]] = [
        (args.n, args.queue_size, args.consumer_ms, args.concurrency),
    ]
    # Add a same-n / 4x-queue cell to make the bound visible in the table.
    if args.compare:
        cells.append((args.n, args.queue_size * 4, args.consumer_ms, args.concurrency))
    # And a same-queue / smaller-n cell, so the table can exhibit the claim
    # `docs/backpressure.md` makes about it (#111). That claim is
    # "`max_queue_depth` is bounded by `queue_size` in every row above REGARDLESS
    # OF `n`" -- and before this there was no `n` axis at all, so every row shared
    # one `n` and the document asserted something its own table could not show.
    # The comment on `--compare` above even said so: "same-n".
    #
    # A fraction of `args.n` rather than a second flag, because the point is that
    # the operator gets the varied-`n` evidence from the documented command rather
    # than from remembering to ask for it. `max(1, ...)` so a small `--n` still
    # produces a legal second row instead of a zero-item run.
    if args.compare_n:
        cells.append(
            (
                max(1, args.n // _COMPARE_N_DIVISOR),
                args.queue_size,
                args.consumer_ms,
                args.concurrency,
            )
        )

    results: list[BackpressureResult] = []
    for n, qs, cms, c in cells:
        r = await run_one(n=n, queue_size=qs, consumer_ms=cms, concurrency=c)
        results.append(r)
        print(
            f"n={r.n} qs={r.queue_size} cms={r.consumer_ms} c={r.concurrency} "
            f"→ duration={r.duration_s:.3f}s peak={r.peak_heap_kb:.1f}kb "
            f"pauses={r.metrics['producer_pauses']} "
            f"max_depth={r.metrics['max_queue_depth']}"
        )

    # The output paths are operator input too: an unwritable `--out-md`/`--out-json`
    # (a read-only filesystem, a permission-denied dir, or a path component that is
    # a file) makes `atomic_write_text` raise OSError, which without this guard
    # escaped `main_async` as a raw traceback at exit 1 — after the benchmark ran —
    # breaking the same exit-2 operator-input contract the `--n`/`--consumer-ms`
    # guards above honor (write-seam sibling of llm-eval-harness #158/#159).
    try:
        if args.out_md:
            atomic_write_text(args.out_md, _render_markdown(results))
            print(f"wrote {args.out_md}")
        if args.out_json:
            payload = {
                "results": [r.to_dict() for r in results],
                "host": sys.platform,
                "python": sys.version.split()[0],
            }
            atomic_write_text(args.out_json, json.dumps(payload, indent=2))
            print(f"wrote {args.out_json}")
    except OSError as e:
        print(f"could not write report: {e}", file=sys.stderr)
        return 2

    return 0


#: `--compare-n` runs a second cell at `n // this`, same `queue_size`. Ten is
#: enough to make the row visibly a different `n` while keeping the extra runtime
#: proportional to a tenth of the main cell (#111).
_COMPARE_N_DIVISOR = 10


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Bounded-queue backpressure demo (#3).")
    p.add_argument("--n", type=int, default=5000, help="items to push through the pipeline")
    p.add_argument("--queue-size", type=int, default=8, help="bounded queue size")
    p.add_argument("--consumer-ms", type=float, default=1.0, help="per-item consumer sleep")
    p.add_argument("--concurrency", type=int, default=2, help="consumer fan-out")
    p.add_argument(
        "--compare",
        action="store_true",
        # Not "the heap bound" (#122): #115 measured that peak heap does not move
        # with queue_size -- 4x the queue gave 83.1 kb vs 80.0 kb -- and the
        # module docstring retracted the claim; this help string kept it.
        help="also run with 4x queue_size to show max_queue_depth moves with it",
    )
    p.add_argument(
        "--compare-n",
        action="store_true",
        help=(
            f"also run with n//{_COMPARE_N_DIVISOR} at the same queue_size, so the "
            "table exhibits the bound across n rather than only across queue_size"
        ),
    )
    p.add_argument("--out-md", default="docs/backpressure.md", help="markdown report")
    p.add_argument("--out-json", default="docs/backpressure.json", help="json report")
    return p


def main() -> int:
    args = _build_parser().parse_args()
    return asyncio.run(main_async(args))


if __name__ == "__main__":
    raise SystemExit(main())
