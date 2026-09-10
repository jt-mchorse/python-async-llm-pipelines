"""The real-API speedup claim points the right way, in every place it is made (#108).

`README.md`'s "Honest framing on the numbers" paragraph is explicit and
correct::

    The spec's range of "5-20x win" assumes real-API I/O, which has
    per-request overhead (TCP, TLS, JSON parsing) that BOUNDS fan-out. The
    synthetic FakeLLM's `asyncio.sleep` is pure-wait, so the speedup ratio is
    THE THEORETICAL UPPER BOUND -- 30x is what `process(...)` can do when each
    "call" is literally just a sleep. Real production speedups land in the
    spec's range, sometimes higher (Batch API workloads).

`README.md:41` says it a second time. And two generated documents said the
opposite:

    scripts/bench_1000_doc.py:140  ->  docs/benchmarks.md
      "the speedup ratios WILL WIDEN because real API I/O has MORE HEADROOM
       for fan-out than the synthetic 20 ms sleep does"

    scripts/capture_demo.sh:117    ->  the 60-second demo narration
      "ratios will widen because real network I/O has more fan-out headroom"

Those cannot both be true. The README says the synthetic 30x is the ceiling and
real lands below it, in 5-20x; the artifact said real has more headroom and the
ratios go up -- above 30x, outside the range the README quotes as the answer.

The README is right, and the reasoning is in its own paragraph: a pure
``await asyncio.sleep(0.02)`` has *zero* per-request CPU, socket, TLS and JSON
cost, which is exactly why it fans out perfectly. Real I/O adds all of it, plus
rate limits and connection-pool limits, so a real client fans out *worse*. A
pure-sleep workload is the upper bound for fan-out, not the floor.

`grep -rn "widen|headroom" tests/` found nothing before this file. A sentence
generated in two places with no test is how it drifted from the README.

**These tests assert the DIRECTION, not a phrasing.** A substring match on one
wording would go red on any rewrite and would not have caught this -- the
inverted sentence was perfectly well-formed prose. The same lesson
`vector-search-at-scale` #139 learned by pinning a guard's spelling instead of
its domain.
"""

from __future__ import annotations

import re
import subprocess
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
README = REPO_ROOT / "README.md"
BENCH_MD = REPO_ROOT / "docs" / "benchmarks.md"
BENCH_PY = REPO_ROOT / "scripts" / "bench_1000_doc.py"
DEMO_SH = REPO_ROOT / "scripts" / "capture_demo.sh"

# Words that assert real-API ratios go UP relative to the synthetic run. Each
# is the claim's direction rather than its wording, so a rewrite that keeps the
# meaning keeps passing and a rewrite that flips it goes red.
_UPWARD = (
    "will widen",
    "widen because",
    "more headroom for fan-out",
    "more fan-out headroom",
    "higher than the synthetic",
    "exceed the synthetic",
)

# And the direction the README establishes.
_DOWNWARD = ("lower", "upper bound", "ceiling", "bounds fan-out", "bound")


def _real_api_prose(text: str) -> str:
    """The part of *text* that talks about swapping in a real API client.

    Located by the swap instruction rather than by a heading, so this works on
    both the markdown and the shell script.
    """
    lowered = text.lower()
    for marker in ("swap `fakellm`", "swap fakellm", "real-api", "real api"):
        idx = lowered.find(marker)
        if idx != -1:
            return text[idx : idx + 1400]
    pytest.fail("no real-API passage found")


@pytest.mark.parametrize(
    ("label", "path"),
    [
        ("the generated benchmark doc", BENCH_MD),
        ("the doc's generator", BENCH_PY),
        ("the 60-second demo narration", DEMO_SH),
        ("the README", README),
    ],
    ids=["benchmarks-md", "bench-generator", "demo-script", "readme"],
)
def test_no_place_claims_real_api_ratios_go_up(label: str, path: Path) -> None:
    """The direction assertion. Red against the sentence that shipped.

    Applied to the *generator* as well as its output, because regenerating the
    doc is an operator action and a corrected artifact with an uncorrected
    generator is one `python scripts/bench_1000_doc.py` away from regressing.
    """
    text = path.read_text(encoding="utf-8")
    # Exclude this repo's own explanations of the fix, which necessarily quote
    # the old wording. Only prose OUTSIDE a comment counts for the .py/.sh.
    if path.suffix in {".py", ".sh"}:
        text = "\n".join(ln for ln in text.splitlines() if not ln.lstrip().startswith(("#", "//")))
    lowered = text.lower()
    offenders = [phrase for phrase in _UPWARD if phrase in lowered]
    assert not offenders, (
        f"{label} ({path.name}) claims real-API speedups go UP relative to the "
        f"synthetic run: {offenders}. The README's honest-framing paragraph says "
        "the synthetic pure-sleep ratio is the theoretical upper bound, because "
        "asyncio.sleep has no per-request overhead to pay."
    )


@pytest.mark.parametrize(
    ("label", "path"),
    [
        ("the generated benchmark doc", BENCH_MD),
        ("the 60-second demo narration", DEMO_SH),
    ],
    ids=["benchmarks-md", "demo-script"],
)
def test_each_place_states_the_direction_positively(label: str, path: Path) -> None:
    """Absence of the wrong claim is not presence of the right one.

    Without this arm, deleting the paragraph entirely would satisfy the test
    above -- and a reader swapping in a real client would then be told nothing
    at all, which is how the disagreement became possible in the first place.
    """
    passage = _real_api_prose(path.read_text(encoding="utf-8")).lower()
    assert any(word in passage for word in _DOWNWARD), (
        f"{label} ({path.name}) no longer states which way real-API ratios go. "
        f"Expected one of {_DOWNWARD} in the real-API passage."
    )
    # And it names the spec range, which is the actual answer to "what should I
    # expect", rather than leaving the reader with only a direction.
    assert re.search(r"5\s*[-–]\s*20\s*[x×]", passage), (
        f"{label} does not name the 5-20x spec range"
    )


def test_the_readme_is_the_source_of_truth_being_aligned_to() -> None:
    """Anti-vacuous: the README really does make the claim the others now echo.

    If the README's honest-framing paragraph is ever softened, these tests are
    aligning the generated docs to a statement that no longer exists.
    """
    text = README.read_text(encoding="utf-8")
    assert "theoretical upper bound" in text
    assert "pure-wait" in text
    assert re.search(r"5[-–]20\s*[x×]", text), "the README no longer names the spec range"
    assert "bounds fan-out" in text, (
        "the README's stated MECHANISM (real I/O bounds fan-out) is gone; the "
        "direction these tests enforce rests on it"
    )


def test_the_two_generators_agree_with_each_other() -> None:
    """A third copy, or a one-sided edit, cannot recreate the disagreement.

    The sentence lives in two files because one writes a doc and one narrates a
    demo. That duplication is the mechanism #108 went through, so it is asserted
    rather than trusted.
    """
    for path in (BENCH_PY, DEMO_SH):
        body = "\n".join(
            ln
            for ln in path.read_text(encoding="utf-8").splitlines()
            if not ln.lstrip().startswith("#")
        ).lower()
        assert any(w in body for w in _DOWNWARD), f"{path.name} states no direction"
        assert not any(p in body for p in _UPWARD), f"{path.name} still claims the wrong direction"


def test_the_upward_phrase_list_is_not_vacuous() -> None:
    """Every assertion above is `not any(...)` over `_UPWARD`, which an empty or
    typo'd list would also satisfy. Prove the list matches the sentence that
    actually shipped."""
    shipped = (
        "The same script writes the same table; the speedup ratios will widen "
        "because real API I/O has more headroom for fan-out than the synthetic "
        "20 ms sleep does."
    ).lower()
    hits = [p for p in _UPWARD if p in shipped]
    assert len(hits) >= 2, f"_UPWARD does not match the sentence #108 fixed: {hits}"


# --- the numbers, re-derived as a passing control -------------------------


def test_the_committed_numbers_are_still_consistent_with_the_declared_workload() -> None:
    """#108 changed prose, not figures. This says so, and keeps saying so.

    Each measured duration must sit at or above the theoretical floor implied by
    the workload the artifact itself declares -- 1000 docs x 2 calls x 20 ms,
    concurrency 32, batch 8. A prose-only change cannot move these, and if a
    future edit does move them this test says which direction.
    """
    import json

    raw = json.loads((REPO_ROOT / "docs" / "benchmarks.json").read_text(encoding="utf-8"))
    w = raw["workload"]
    call_s = w["llm_call_seconds"]
    calls = w["n_docs"] * 2
    floors = {
        "serial": calls * call_s,
        "async": (calls / w["concurrency"]) * call_s,
        "async+batched": ((calls / w["batch_size"]) / w["concurrency"]) * call_s,
    }
    by_name = {r["pipeline_name"]: r for r in raw["results"]}
    assert set(by_name) == set(floors), sorted(by_name)
    for name, floor in floors.items():
        measured = by_name[name]["duration_seconds"]
        assert measured >= floor, (
            f"{name} measured {measured:.4f}s, below its theoretical floor "
            f"{floor:.4f}s -- the numbers no longer match the declared workload"
        )
        # And not absurdly above it, which would mean the workload block and the
        # results have drifted apart.
        assert measured < floor * 2, f"{name} measured {measured:.4f}s vs floor {floor:.4f}s"


def test_the_benchmark_doc_still_declares_the_run_that_produced_it() -> None:
    """Provenance, which a re-render silently rewrites.

    `render_markdown` stamps `platform.python_version()` and today's date -- of
    the process doing the RENDERING, not the run that MEASURED. Regenerating the
    doc to fix prose (which #108 is) therefore re-attributes the numbers to
    whoever ran the regeneration. The committed line is restored by hand for
    that reason, and this pins that the doc names a host and a date rather than
    silently claiming the last editor's.
    """
    text = BENCH_MD.read_text(encoding="utf-8")
    host_lines = [ln for ln in text.splitlines() if ln.startswith("- **Host.**")]
    assert len(host_lines) == 1, host_lines
    assert re.search(r"run on \d{4}-\d{2}-\d{2}", host_lines[0]), host_lines[0]
    assert "CPython" in host_lines[0]


def test_the_demo_script_is_syntactically_valid() -> None:
    """The narration edit is inside a shell script with no test of its own."""
    if not DEMO_SH.exists():  # pragma: no cover
        pytest.skip("capture_demo.sh absent")
    proc = subprocess.run(["bash", "-n", str(DEMO_SH)], capture_output=True, text=True, check=False)
    assert proc.returncode == 0, f"bash -n failed: {proc.stderr}"
    assert sys.version_info >= (3, 11)
