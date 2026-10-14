"""The real-API swap reaches the serial and async rows, not the batched one (#181).

Every swap passage said that replacing `FakeLLM` with an `LLMClient` adapter
and re-running gives real numbers for "the same table". The async+batched row
is built from `make_batch_caller`, which never calls the client it wraps. It
sleeps once per batch and returns synthetic strings. `make_batch_caller`'s own
docstring said it would "`asyncio.gather` the per-item calls". Measured on
`main` with a counting client, 16 docs: serial 16 + 16 calls, async 16 + 16,
batched 0 + 0, and the batched row reported 37.64x against the real serial
baseline.

The behaviour arms pin the fact the prose now states. If `make_batch_caller`
ever starts calling `llm` (a maintainer call, since it moves the committed
figures), they go red and point at every passage below to revisit.
"""

from __future__ import annotations

import asyncio
import importlib.util
import re
import sys
from pathlib import Path

import pytest

from async_pipelines.benchmark import Workload, make_batch_caller

REPO_ROOT = Path(__file__).resolve().parents[1]
README = REPO_ROOT / "README.md"
BENCH_MD = REPO_ROOT / "docs" / "benchmarks.md"
BENCH_PY = REPO_ROOT / "scripts" / "bench_1000_doc.py"
DEMO_SH = REPO_ROOT / "scripts" / "capture_demo.sh"


def _load_bench_module():
    spec = importlib.util.spec_from_file_location("_bench_1000_doc_181", BENCH_PY)
    assert spec is not None
    assert spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return mod


class _CountingClient:
    """A minimal `LLMClient`: `async __call__(prompt) -> str` and nothing else
    `FakeLLM` has, apart from the two constructor keywords `_run_all` passes."""

    calls: dict[str, int] = {}

    def __init__(self, latency_seconds: float = 0.0, call_id: str = "x") -> None:
        self.call_id = call_id
        _CountingClient.calls[call_id] = 0

    async def __call__(self, prompt: str) -> str:
        _CountingClient.calls[self.call_id] += 1
        await asyncio.sleep(0)
        return f"{prompt}:{self.call_id}"


# --- behaviour: what the prose describes ------------------------------------


def test_make_batch_caller_never_calls_the_client_it_wraps() -> None:
    _CountingClient.calls = {}
    client = _CountingClient(call_id="direct")
    out = asyncio.run(make_batch_caller(client)(["a", "b", "c"]))
    assert out == ["a:direct-batched", "b:direct-batched", "c:direct-batched"]
    assert _CountingClient.calls == {"direct": 0}


def test_the_documented_swap_reaches_serial_and_async_but_not_batched(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    bench = _load_bench_module()
    _CountingClient.calls = {}
    monkeypatch.setattr(bench, "FakeLLM", _CountingClient)
    w = Workload(n_docs=16, llm_call_seconds=0.0, concurrency=4, batch_size=8)
    results = asyncio.run(bench._run_all(w))
    assert [r.pipeline_name for r in results] == ["serial", "async", "async+batched"]
    by_pipeline = {
        p: _CountingClient.calls[f"llm1-{p}"] + _CountingClient.calls[f"llm2-{p}"]
        for p in ("serial", "async", "batched")
    }
    assert by_pipeline == {"serial": 32, "async": 32, "batched": 0}


# --- prose: every swap passage says so --------------------------------------


def _swap_passage(text: str) -> str:
    """From the swap instruction to the end of its paragraph (or 900 chars)."""
    lowered = text.lower()
    idx = lowered.find("swap")
    while idx != -1 and "fakellm" not in lowered[idx : idx + 60]:
        idx = lowered.find("swap", idx + 1)
    assert idx != -1, "no FakeLLM swap passage found"
    end = text.find("\n\n", idx)
    return text[idx : (end if end != -1 else idx + 900)]


def _names_the_batched_exception(passage: str) -> bool:
    flat = re.sub(r"\s+", " ", passage).lower()
    return "make_batch_caller" in flat and "batch endpoint" in flat


def _rendered_report() -> str:
    from async_pipelines.benchmark import RunResult

    bench = _load_bench_module()
    rows = [
        RunResult("serial", 10, 1.0, 10.0, 1.0),
        RunResult("async", 10, 0.1, 100.0, 10.0),
        RunResult("async+batched", 10, 0.01, 1000.0, 100.0),
    ]
    return bench.render_markdown(bench.DEFAULT_WORKLOAD, rows)


@pytest.mark.parametrize(
    "source",
    ["rendered-report", "benchmarks-md", "readme-honest-framing", "demo-narration"],
)
def test_each_swap_passage_names_the_batched_exception(source: str) -> None:
    if source == "rendered-report":
        text = _rendered_report()
    elif source == "benchmarks-md":
        text = BENCH_MD.read_text(encoding="utf-8")
    elif source == "readme-honest-framing":
        text = README.read_text(encoding="utf-8")
        text = text[text.index("**Honest framing on the numbers.**") :]
    else:
        # The narration is `printf` lines; join their payloads.
        text = " ".join(
            m.group(1) for m in re.finditer(r"^printf '(.*)'", DEMO_SH.read_text(), re.M)
        )
    assert _names_the_batched_exception(_swap_passage(text)), (
        f"{source}: the FakeLLM swap passage does not say the async+batched row "
        "stays synthetic (make_batch_caller never calls the client; a real "
        "batched row needs a real batch endpoint)"
    )


def test_make_batch_caller_docstring_claims_no_call_to_llm() -> None:
    doc = re.sub(r"\s+", " ", make_batch_caller.__doc__ or "")
    assert "never calls `llm`" in doc
    # The shipped claim, quoted only as history (in a sentence that says it was
    # wrong), never as a description.
    assert "here we just" not in doc


def test_the_shipped_swap_sentence_fails_the_prose_check() -> None:
    """Anti-vacuity: the passage that shipped is rejected by the predicate."""
    shipped = (
        "Swap `FakeLLM` for an Anthropic adapter that conforms to the `LLMClient` "
        "Protocol (`async __call__(prompt: str) -> str`) and re-run. The same "
        "script writes the same table. Expect the speedup ratios to be **lower** "
        "than the synthetic ones above. Batch API workloads are the documented "
        "exception and can exceed it."
    )
    assert not _names_the_batched_exception(_swap_passage(shipped))
