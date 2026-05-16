# Async-pipeline benchmarks (issue #4)

- **Workload.** 1000 docs · 2 LLM calls per doc · 20 ms simulated per call · concurrency 32 · batch size 8
- **Synthetic LLM disclosure.** Each call is a deterministic `await asyncio.sleep(0.02)`. The speedup ratios are load-bearing under this model; the absolute latency is per the simulated cost. Real-API numbers are a `FakeLLM` → `AnthropicLLM` swap; the `LLMClient` Protocol is the seam.
- **Host.** CPython 3.14.0 on Darwin arm64, run on 2026-05-15.

| pipeline | duration (s) | docs/s | speedup vs serial |
| -------- | -----------: | -----: | ----------------: |
| serial | 43.311 | 23.1 | 1.00× |
| async | 1.427 | 700.5 | 30.34× |
| async+batched | 0.172 | 5800.1 | 251.21× |

## Reproduce

```bash
python scripts/bench_1000_doc.py --n 1000 --concurrency 32 --batch-size 8
```

## Real-API mode (operator action)

Swap `FakeLLM` for an Anthropic adapter that conforms to the `LLMClient` Protocol (`async __call__(prompt: str) -> str`) and re-run. The same script writes the same table; the speedup ratios will widen because real API I/O has more headroom for fan-out than the synthetic 20 ms sleep does.
