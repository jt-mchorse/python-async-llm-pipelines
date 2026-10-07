"""FakeLLM.latency_seconds is validated, and so is the batch caller's fallback (#151).

#96 guarded `Workload.llm_call_seconds` and `make_batch_caller(batch_seconds=)`,
and the comment beside the batch guard says a `None` `batch_seconds` "falls
through to the validated llm-latency fallback". That fallback is
`getattr(llm, "latency_seconds", 0.0)`, and `FakeLLM` never checked its field.
Measured on `main` (a hunt agent, re-run here), 32 docs through
`BatchedAsyncPipeline` with `make_batch_caller(FakeLLM(latency_seconds=X))`:

    X = 0.02  -> 0.044 s   724 docs/s
    X = True  -> 2.005 s    16 docs/s    (asyncio.sleep(True) is one second)
    X = -5.0  -> 0.0 s  263,737 docs/s   (a fabricated throughput)
"""

from __future__ import annotations

import pytest

from async_pipelines.benchmark import FakeLLM, make_batch_caller


@pytest.mark.parametrize("bad", [True, False, -5.0, -1, float("nan"), float("inf"), "0.02", None])
def test_fakellm_refuses_a_latency_that_is_not_a_duration(bad: object) -> None:
    with pytest.raises(ValueError, match="latency_seconds must be a finite number >= 0.0"):
        FakeLLM(latency_seconds=bad)  # type: ignore[arg-type]


@pytest.mark.parametrize("good", [0, 0.0, 0.02, 1])
def test_fakellm_accepts_a_duration(good: float) -> None:
    assert FakeLLM(latency_seconds=good).latency_seconds == good


def test_the_batch_callers_fallback_is_checked_for_any_llm_object() -> None:
    class ByoLLM:  # not a FakeLLM, so nothing upstream validated it
        latency_seconds = -1.0

        async def __call__(self, prompt: str) -> str:
            return prompt

    with pytest.raises(ValueError, match=r"llm\.latency_seconds"):
        make_batch_caller(ByoLLM())


def test_an_llm_without_the_attribute_still_falls_back_to_zero() -> None:
    class Bare:
        async def __call__(self, prompt: str) -> str:
            return prompt

    assert callable(make_batch_caller(Bare()))
