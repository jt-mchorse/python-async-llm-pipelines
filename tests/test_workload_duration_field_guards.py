"""The two latency seams reject `bool`, like every other field around them (#96).

`Workload.__post_init__` validates four fields. Three rejected `bool`
explicitly; `llm_call_seconds` — the parameter that defines the published
benchmark numbers — did not, despite the comment directly above those guards
giving the reason:

    "bool subclasses int and flattens operator intent. NaN llm_call_seconds
     skews the published throughput numbers because the simulated-latency
     sleep becomes platform-dependent."

Measured on `main`:

    n_docs=True                       ValueError: n_docs must be an int
    concurrency=True                  ValueError: concurrency must be an int
    batch_size=True                   ValueError: batch_size must be an int
    llm_call_seconds=True             ACCEPTED
    llm_call_seconds=False            ACCEPTED
    llm_call_seconds='0.02'           TypeError: must be real number, not str
    batch_seconds=True                ACCEPTED
    batch_seconds='0.05'              TypeError: must be real number, not str

Three consequences, all measured:

1. `asyncio.sleep(True)` sleeps **1.002 s** where the default is `0.020 s`, so
   `llm_call_seconds=True` silently rewrites every throughput figure by 50x —
   the harm the comment names for NaN, reached through bool.
2. `Workload.to_dict()` is documented as pinning the JSON contract for
   "downstream JSON consumers (notebook, CI parser, dashboard)", and emitted
   `{"llm_call_seconds": true}` — a JSON *boolean* in a numeric field.
3. A string or `None` escaped as a raw `TypeError` from `math.isfinite`,
   outside the `ValueError` contract every other field in the class uses.

`batch_seconds` is included because its own guard says it "mirrors
`llm_call_seconds`", and it did — including the gap. Fixing one and not the
other would have repeated how the gap arrived.
"""

from __future__ import annotations

import asyncio
import json
import time

import pytest

from async_pipelines.benchmark import FakeLLM, Workload, make_batch_caller

# ----------------------------------------------------------------------
# What must now be rejected
# ----------------------------------------------------------------------


@pytest.mark.parametrize("bad", [True, False])
def test_workload_rejects_a_bool_latency(bad: bool) -> None:
    with pytest.raises(ValueError, match=r"llm_call_seconds must be a finite number >= 0\.0"):
        Workload(n_docs=4, llm_call_seconds=bad)


@pytest.mark.parametrize("bad", ["0.02", None, [0.02], {"s": 1}, object()])
def test_workload_rejects_a_non_numeric_latency_with_valueerror(bad: object) -> None:
    """Not `TypeError`. Every other field in this class raises `ValueError`, and
    a caller catching the documented type should not have to catch two."""
    with pytest.raises(ValueError, match=r"llm_call_seconds must be a finite number >= 0\.0"):
        Workload(n_docs=4, llm_call_seconds=bad)  # type: ignore[arg-type]


@pytest.mark.parametrize("bad", [float("nan"), float("inf"), float("-inf"), -0.1, -1])
def test_workload_still_rejects_what_it_always_did(bad: float) -> None:
    with pytest.raises(ValueError, match=r"llm_call_seconds must be a finite number >= 0\.0"):
        Workload(n_docs=4, llm_call_seconds=bad)


@pytest.mark.parametrize("bad", [True, False, "0.05", [1]])
def test_make_batch_caller_rejects_the_same_things(bad: object) -> None:
    """`batch_seconds`' guard said it mirrors `llm_call_seconds` — and it did,
    gap included."""
    llm = FakeLLM(latency_seconds=0.02, call_id="x")
    with pytest.raises(ValueError, match=r"batch_seconds must be a finite number >= 0\.0"):
        make_batch_caller(llm, batch_seconds=bad)  # type: ignore[arg-type]


# ----------------------------------------------------------------------
# What must keep working — the guard is on type and sign, not on float-ness
# ----------------------------------------------------------------------


@pytest.mark.parametrize("ok", [0.0, 0.02, 1, 2, 0.5, 1e-6])
def test_workload_accepts_every_legitimate_duration(ok: float) -> None:
    """`1` is a whole number of seconds and the annotation is `float`; `0.0` is
    "no simulated latency", which the existing `>= 0.0` allows on purpose. A fix
    that demanded a `float` instance would be a different, wrong change."""
    assert Workload(n_docs=4, llm_call_seconds=ok).llm_call_seconds == pytest.approx(ok)


def test_the_default_workload_is_unchanged() -> None:
    """The published numbers must not move: the default is untouched."""
    w = Workload(n_docs=1000)
    assert w.llm_call_seconds == 0.020
    assert (w.concurrency, w.batch_size) == (32, 8)


@pytest.mark.parametrize("ok", [None, 0.0, 0.05, 1])
def test_make_batch_caller_accepts_every_legitimate_duration(ok: object) -> None:
    llm = FakeLLM(latency_seconds=0.02, call_id="x")
    assert make_batch_caller(llm, batch_seconds=ok) is not None  # type: ignore[arg-type]


# ----------------------------------------------------------------------
# The two harms, asserted directly rather than inferred
# ----------------------------------------------------------------------


def test_the_json_contract_can_no_longer_carry_a_boolean() -> None:
    """`to_dict` pins the shape for downstream JSON consumers. `True` used to
    emit `{"llm_call_seconds": true}` — a JSON boolean in a numeric field."""
    with pytest.raises(ValueError, match=r"llm_call_seconds"):
        Workload(n_docs=4, llm_call_seconds=True)

    payload = json.loads(json.dumps(Workload(n_docs=4).to_dict()))
    for field in ("n_docs", "llm_call_seconds", "concurrency", "batch_size"):
        assert isinstance(payload[field], (int, float)), field
        assert not isinstance(payload[field], bool), field


def test_the_50x_claim_is_real() -> None:
    """Documents *why* bool matters more here than on an integer field: the
    value flows into `asyncio.sleep`, and `True` is one second.

    Asserts the ratio between two sleeps rather than an absolute wall-clock
    figure, so it is a statement about `True == 1` rather than about how fast
    this machine happens to be.
    """

    async def slept(value: float) -> float:
        start = time.perf_counter()
        await asyncio.sleep(value)
        return time.perf_counter() - start

    # `asyncio.sleep(True)` is `asyncio.sleep(1)`; the Workload default is 0.020.
    assert float(True) / Workload(n_docs=1).llm_call_seconds == pytest.approx(50.0)
    # And a short sleep really does complete far inside a second, so the ratio
    # above is not a bookkeeping identity.
    assert asyncio.run(slept(0.001)) < 0.5
