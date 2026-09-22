"""The 1000-doc bench's algorithm axis, pinned across every surface that spells it (#113).

#111 gave `scripts/bench_backpressure.py`'s artifact a provenance lock —
`test_backpressure_doc_surfaces.py::test_the_readme_documents_the_command_that_produces_the_committed_rows`.
The sibling benchmark got the other half of that pair and not this one.

`test_bench_table_snapshot.py` locks README ↔ JSON and `benchmarks.md` ↔ JSON,
deliberately and correctly over the *rendering*: it does not re-run the
benchmark, because `duration` and `docs_per_second` describe a machine and
re-running in CI would be a flake at ~45s wall-clock. That is the right call,
and it leaves the **algorithm** axis — `n_docs`, `concurrency`, `batch_size`,
`llm_call_seconds` — pinned nowhere. The README could document `--n 200` while
the committed JSON came from `--n 1000`, and all three rendering locks would
stay green, because they compare renderings of whatever JSON is committed.

That is #111's split: `n` / `queue_size` / `max_queue_depth` describe the
algorithm and are pinnable across every surface; `duration` / `peak_heap`
describe a machine and are only pinnable md-against-json from one run.

Two things this module does that a string-matching version would not:

- **It parses.** Each documented command is resolved through the script's own
  `build_arg_parser()` and `workload_from_args()`. No documented spelling
  passes `--latency` at all, so the committed `llm_call_seconds: 0.02` comes
  from an argparse **default** — a string comparison cannot see that, and would
  stay green if the default moved to `0.05` while every documented command
  stayed byte-identical. Parsing also makes `--out` fall out for free (it does
  not determine the workload) and survives flag reordering.
- **It discovers the surfaces.** The command is spelled *four* times, not
  three: the README's `## 1000-doc benchmark` block, the README's `## Demo`
  block, `test_bench_table_snapshot.py`'s module docstring, and that file's
  `REGEN_HINT` constant — the one a failure actually prints. Treating "the
  regen hint" as a single surface would reproduce the drift this lock exists
  to catch, one string over. So the surfaces are found by scanning, and the
  count is asserted, so a fifth spelling fails loudly instead of being
  silently skipped.
"""

from __future__ import annotations

import json
import re
import shlex
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.bench_1000_doc import build_arg_parser, workload_from_args  # noqa: E402

README = REPO_ROOT / "README.md"
SNAPSHOT_TEST = REPO_ROOT / "tests" / "test_bench_table_snapshot.py"
BENCH_JSON = REPO_ROOT / "docs" / "benchmarks.json"

#: Every file that may spell the command. Scanned, not hand-indexed into.
SURFACE_FILES = (README, SNAPSHOT_TEST)

#: What the committed artifact says it was produced with.
COMMITTED_WORKLOAD = json.loads(BENCH_JSON.read_text(encoding="utf-8"))["workload"]

_COMMAND_RE = re.compile(r'python\s+scripts/bench_1000_doc\.py([^\n"]*)')


def _fold(text: str) -> str:
    r"""Put every spelling of the command onto one line before extracting.

    Two different continuations are in play and both were found the hard way:

    - **Shell line-continuation.** The README's `## Demo` block and the
      snapshot test's docstring both wrap with a trailing `\` and re-indent.
    - **Python implicit string concatenation.** `REGEN_HINT` is built from two
      adjacent string literals, so the raw file has `--concurrency 32 "` at
      the end of one line and `"--batch-size 8 ...` at the start of the next.
      An extractor that stops at the newline silently reads a *shorter*
      command than the one a failure prints — which is the lock reproducing
      the drift it exists to catch.
    """
    text = re.sub(r"\\\s*\n\s*", " ", text)  # shell continuation
    text = re.sub(r'"\s*\n\s*"', "", text)  # adjacent string literals
    return text


def _documented_commands(text: str) -> list[str]:
    """Every `python scripts/bench_1000_doc.py ...` invocation in `text`.

    The tail is cut at a literal two-character ``\n`` escape as well as at a
    real newline. Folding `REGEN_HINT`'s adjacent literals joins *all five* of
    them, so without this the command would run on into "Then update the
    README table cells..." and argparse would reject the prose.
    """
    out = []
    for match in _COMMAND_RE.finditer(_fold(text)):
        out.append(match.group(1).split("\\n", 1)[0].strip())
    return out


def _all_documented_commands() -> list[tuple[str, str]]:
    out: list[tuple[str, str]] = []
    for path in SURFACE_FILES:
        for tail in _documented_commands(path.read_text(encoding="utf-8")):
            out.append((path.name, tail))
    return out


def _resolved_workload(tail: str) -> dict[str, object]:
    args = build_arg_parser().parse_args(shlex.split(tail))
    return workload_from_args(args).to_dict()


# ----------------------------------------------------------------------
# The population
# ----------------------------------------------------------------------


def test_every_documented_spelling_is_discovered() -> None:
    """Four surfaces, found by scanning rather than listed.

    If this count grows, a new place documents the command and the arm below
    is already covering it — update the number and read the new spelling. If it
    *shrinks*, a surface was deleted or reworded past recognition, which is
    equally worth knowing: a silently-unmatched command is a lock that has
    stopped locking.
    """
    found = _all_documented_commands()
    assert len(found) == 4, (
        "expected 4 documented spellings of the bench command "
        "(README ## 1000-doc benchmark, README ## Demo, the snapshot test's "
        f"module docstring, and its REGEN_HINT); found {len(found)}: {found}"
    )
    # Both files contribute; a regex that only matched one would still hit 4
    # if one file happened to spell it four times.
    assert {name for name, _ in found} == {README.name, SNAPSHOT_TEST.name}


# ----------------------------------------------------------------------
# The lock
# ----------------------------------------------------------------------


@pytest.mark.parametrize(("surface", "tail"), _all_documented_commands(), ids=lambda v: str(v)[:40])
def test_each_documented_command_resolves_to_the_committed_workload(
    surface: str, tail: str
) -> None:
    """The algorithm axis, pinned. Resolved through the script's own parser.

    `--out` differs between the spellings (the `## Demo` one writes to
    `/tmp/bench.md` so a reader cannot clobber the committed artifact) and is
    correctly invisible here: it does not determine the workload. What is
    compared is what `Workload.to_dict()` reports, which is exactly the block
    `docs/benchmarks.json` records.
    """
    assert _resolved_workload(tail) == COMMITTED_WORKLOAD, (
        f"{surface} documents a command that resolves to a different workload "
        f"than docs/benchmarks.json records.\n"
        f"  documented: python scripts/bench_1000_doc.py {tail}\n"
        f"  resolves to: {_resolved_workload(tail)}\n"
        f"  committed:   {COMMITTED_WORKLOAD}"
    )


def test_the_latency_default_is_what_puts_llm_call_seconds_in_the_artifact() -> None:
    """The flag nobody writes, which is why this lock parses instead of matching.

    No documented spelling passes `--latency`, so `llm_call_seconds` reaches
    the committed artifact from the parser's default. A string-comparison lock
    over the commands is structurally blind to it: the default could move and
    every documented command would stay byte-identical.
    """
    for _surface, tail in _all_documented_commands():
        assert "--latency" not in tail
    default = build_arg_parser().parse_args([]).latency
    assert default == COMMITTED_WORKLOAD["llm_call_seconds"]


# ----------------------------------------------------------------------
# The same claims, in English
# ----------------------------------------------------------------------


def test_the_readme_prose_states_the_same_workload_as_the_json() -> None:
    """The fifth surface: the sentence above the table.

    "Real measured numbers on Apple Silicon, CPython 3.14, concurrency 32,
    batch size 8" and "simulating 20 ms per call" are the same workload claims
    written out. A prose assertion is a test case.
    """
    text = README.read_text(encoding="utf-8")
    concurrency = COMMITTED_WORKLOAD["concurrency"]
    batch_size = COMMITTED_WORKLOAD["batch_size"]
    latency_ms = round(COMMITTED_WORKLOAD["llm_call_seconds"] * 1000)

    assert f"concurrency {concurrency}, batch size {batch_size}" in text, (
        "the README's table preamble no longer states the committed workload"
    )
    assert f"simulating {latency_ms} ms per call" in text, (
        "the README's prose latency no longer matches the committed llm_call_seconds"
    )


def test_the_json_workload_block_has_exactly_the_four_algorithm_fields() -> None:
    """Pins the axis itself, so a new workload knob cannot arrive unlocked.

    If `Workload` grows a fifth field, it reaches `docs/benchmarks.json` and
    this arm reddens — at which point the parametrised arm above needs to cover
    it too, because a documented command that sets it would otherwise be
    unpinned exactly the way `llm_call_seconds` nearly was.
    """
    assert set(COMMITTED_WORKLOAD) == {
        "n_docs",
        "concurrency",
        "batch_size",
        "llm_call_seconds",
    }


# ----------------------------------------------------------------------
# Anti-vacuity: the arm must redden on drift
# ----------------------------------------------------------------------


@pytest.mark.parametrize(
    ("original", "drifted"),
    [
        ("--n 1000", "--n 200"),
        ("--concurrency 32", "--concurrency 8"),
        ("--batch-size 8", "--batch-size 4"),
    ],
)
def test_a_drifted_command_would_be_caught(original: str, drifted: str) -> None:
    """Run the lock's own comparison against a perturbed command.

    Checked as a transformation of a real documented spelling rather than a
    hand-written string, so it cannot pass by testing something the README does
    not actually say.
    """
    tails = [tail for _s, tail in _all_documented_commands() if original in tail]
    assert tails, f"no documented command contains {original!r} to perturb"
    perturbed = tails[0].replace(original, drifted)
    assert _resolved_workload(perturbed) != COMMITTED_WORKLOAD, (
        f"perturbing {original!r} -> {drifted!r} did not change the resolved "
        "workload, so the lock would not have caught this drift"
    )


def test_a_drifted_prose_number_would_be_caught() -> None:
    """The same falsification for the English surface, on a copy of the README."""
    text = README.read_text(encoding="utf-8")
    concurrency = COMMITTED_WORKLOAD["concurrency"]
    batch_size = COMMITTED_WORKLOAD["batch_size"]
    drifted = text.replace(
        f"concurrency {concurrency}, batch size {batch_size}",
        f"concurrency {concurrency * 2}, batch size {batch_size}",
    )
    assert drifted != text
    assert f"concurrency {concurrency}, batch size {batch_size}" not in drifted


def test_every_documented_flag_currently_passes_the_parsers_own_default() -> None:
    """The honest bound on what this lock catches, stated rather than assumed.

    Every value the documented commands pass is *already* the parser's
    default: a bare `python scripts/bench_1000_doc.py` resolves to the same
    workload as the fully-flagged spelling. So **dropping** a documented flag
    is a no-op today and the parametrised arm above cannot be written against
    a dropped flag -- I tried, and it passed against correct code.

    What the lock does catch: a documented flag carrying a *wrong value*
    (`--n 200`), and a *moved default* reaching the artifact through a flag
    nobody writes (the `--latency` arm). Those are the two ways the algorithm
    axis can drift while every rendering lock stays green.

    If this arm ever reddens, the documented commands have stopped being
    redundant with the defaults -- at which point a dropped flag *does* change
    the workload, and the perturbation set above should grow one.
    """
    bare = workload_from_args(build_arg_parser().parse_args([])).to_dict()
    assert bare == COMMITTED_WORKLOAD
