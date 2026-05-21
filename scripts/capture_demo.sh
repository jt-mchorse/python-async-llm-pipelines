#!/usr/bin/env bash
# Deterministic driver for the 60-second README demo (issue #14).
#
# Runs the two demo surfaces from the README's Demo section in sequence
# on a fresh clone with no API key:
#
#   1. pytest -q              — the full primitive surface (process,
#                               stream, tool_dispatch, backpressure
#                               metrics, per-item timeouts). The
#                               passing summary is the contract that
#                               every primitive still works.
#
#   2. bench_1000_doc.py      — serial vs async vs async+batched on a
#                               200-doc workload (smaller than the
#                               committed 1000-doc headline so the
#                               whole demo fits 60s — the *ratios*
#                               are the load-bearing claim and the
#                               bench discloses that explicitly). Then
#                               cat the rendered markdown so the
#                               speedup table is on camera.
#
# The output is the recording — when JT records the GIF/video, this
# script's stdout is what gets captured. Hermetic: no API key, no
# network, no committed artifacts touched (everything writes under a
# per-run tempdir).
#
# Why --n 200 instead of --n 1000: the full 1000-doc bench at the
# committed --latency 0.02 takes ~45s, which doesn't fit 60s with
# pytest + banners. The bench output's "Synthetic LLM disclosure"
# block already says the *speedup ratios* are load-bearing under the
# synthetic model; the *absolute durations* are per the simulated cost.
# 200 docs preserves the ratio story while staying within tempo. The
# committed docs/benchmarks.md keeps the headline 1000-doc numbers
# (locked by test_bench_table_snapshot.py).
#
# Variables:
#   CAPTURE_PACE_SECONDS  pause between sections (default 2 for
#                         recording; tests/test_capture_demo_smoke.py
#                         sets this to 0).
#   CAPTURE_DEMO_N        bench workload size (default 200).
#
# Exit: 0 on full success; non-zero on any sub-step failure.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PACE="${CAPTURE_PACE_SECONDS:-2}"
N_DOCS="${CAPTURE_DEMO_N:-200}"

banner() {
  printf '\n'
  printf '═══ %s\n' "$1"
  printf '\n'
}

pace() {
  if [ "$PACE" != "0" ]; then
    sleep "$PACE"
  fi
}

cd "$REPO_ROOT"

# Per-run scratch so concurrent recordings (and the smoke test) don't
# collide AND so the bench's default --out (docs/benchmarks.md) can't
# accidentally mutate the committed snapshot. Cleaned up on exit
# including error paths.
TMPDIR_DEMO="$(mktemp -d -t async-pipelines-capture-XXXXXX)"
cleanup() {
  rm -rf "$TMPDIR_DEMO"
}
trap cleanup EXIT INT TERM

# Resolve the Python interpreter from the active venv if one is present.
if [ -x "$REPO_ROOT/.venv/bin/python" ]; then
  PYTHON_BIN="$REPO_ROOT/.venv/bin/python"
elif command -v python >/dev/null 2>&1; then
  PYTHON_BIN="python"
else
  PYTHON_BIN="python3"
fi

BENCH_MD="$TMPDIR_DEMO/benchmarks.md"

banner "python-async-llm-pipelines · 60-second demo"
printf 'two surfaces · FakeLLM (dep-free) · no API key, no network\n'
printf 'full 1000-doc headline numbers live in docs/benchmarks.md (locked by snapshot test).\n'
pace

banner "1/2 · pytest · primitive surface (process · stream · tool_dispatch · backpressure · timeouts)"
printf 'pytest --ignore=tests/test_capture_demo_smoke.py\n'
printf '  every primitive covered by hermetic unit tests; passing summary is the contract.\n'
printf '  (the smoke test is excluded here so the outer pytest run that invokes\n'
printf '   this script does not recursively re-enter it. pyproject.toml addopts\n'
printf '   already includes -q; an extra -q here would silence the summary line.)\n\n'
pytest --ignore=tests/test_capture_demo_smoke.py
pace

banner "2/2 · bench_1000_doc.py · serial vs async vs async+batched (n=$N_DOCS for tempo)"
printf 'python scripts/bench_1000_doc.py --n %s --concurrency 32 --batch-size 8 \\\n' "$N_DOCS"
printf '  --out <tmp>.md\n'
printf '  ratios are the load-bearing claim under the synthetic LLM (bench output discloses this).\n\n'
"$PYTHON_BIN" scripts/bench_1000_doc.py \
  --n "$N_DOCS" \
  --concurrency 32 \
  --batch-size 8 \
  --out "$BENCH_MD"
printf '\n─── rendered bench table ────────────────────────────────────────────\n\n'
cat "$BENCH_MD"
pace

banner "done · five primitives + serial/async/batched speedup table wired end-to-end"
printf 'next stop for real-API numbers:\n'
printf '  swap FakeLLM for an AnthropicLLM adapter implementing LLMClient.\n'
printf '  ANTHROPIC_API_KEY=... python scripts/bench_1000_doc.py --n 200 --concurrency 16 \\\n'
printf '    --batch-size 4 --out /tmp/real.md\n'
printf '  ratios will widen because real network I/O has more fan-out headroom\n'
printf '  than the synthetic 20 ms sleep.\n'
