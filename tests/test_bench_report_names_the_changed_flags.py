"""The low-ceiling sentence names what this run changed, not a fixed flag (#135).

#129's parenthetical said "the 5-20x spec range assumes the default
`--concurrency 32` workload" whatever the run changed. Measured on main
(aacd981), `--n 10` -- concurrency left at 32 -- printed::

    ... land below the 10.02× measured above (the 5-20x spec range assumes the
    default `--concurrency 32` workload).

pointing the reader at a flag they had not touched; ten docs cap fan-out.
"""

from __future__ import annotations

import sys
from dataclasses import replace
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from async_pipelines.benchmark import RunResult  # noqa: E402
from scripts.bench_1000_doc import (  # noqa: E402
    DEFAULT_WORKLOAD,
    build_arg_parser,
    main,
    render_markdown,
    workload_from_args,
)

_LOW = [
    RunResult("serial", 10, 1.0, 10.0, 1.0),
    RunResult("async", 10, 0.1, 100.0, 10.0),
]


def _sentence(md: str) -> str:
    start = md.index("land below the")
    return md[start : md.index("). ", start) + 1]


@pytest.mark.parametrize(
    ("change", "flag"),
    [
        ({"n_docs": 10}, "`--n 10`"),
        ({"concurrency": 2}, "`--concurrency 2`"),
        ({"batch_size": 1}, "`--batch-size 1`"),
        ({"llm_call_seconds": 0.0004}, "`--latency 0.0004`"),
    ],
)
def test_each_changed_flag_is_named_and_no_other(change: dict, flag: str) -> None:
    sentence = _sentence(render_markdown(replace(DEFAULT_WORKLOAD, **change), _LOW))
    assert f"this run used {flag})" in sentence, sentence


def test_the_default_workload_names_no_changed_flag() -> None:
    sentence = _sentence(render_markdown(DEFAULT_WORKLOAD, _LOW))
    assert "this run used" not in sentence
    assert "1000 docs, 20 ms per call, `--concurrency 32`, `--batch-size 8`" in sentence


def test_the_argparse_defaults_are_the_default_workload() -> None:
    assert workload_from_args(build_arg_parser().parse_args([])) == DEFAULT_WORKLOAD


def test_the_issue_command_blames_n_not_concurrency(tmp_path: Path) -> None:
    out = tmp_path / "b10.md"
    assert main(["--n", "10", "--out", str(out)]) == 0
    sentence = _sentence(out.read_text(encoding="utf-8"))
    assert "this run used `--n 10`)" in sentence, sentence
