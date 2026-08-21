# Async-pipeline benchmarks (issue #4)

- **Workload.** 1000 docs · 2 LLM calls per doc · 20 ms simulated per call · concurrency 32 · batch size 8
- **Synthetic LLM disclosure.** Each call is a deterministic `await asyncio.sleep(0.02)`. The speedup ratios are load-bearing under this model; the absolute latency is per the simulated cost. Real-API numbers are a `FakeLLM` → `AnthropicLLM` swap; the `LLMClient` Protocol is the seam.
- **Host.** CPython 3.14.0 on Darwin arm64, run on 2026-05-21.

| pipeline | duration (s) | docs/s | speedup vs serial |
| -------- | -----------: | -----: | ----------------: |
| serial | 43.333 | 23.1 | 1.00× |
| async | 1.421 | 703.8 | 30.50× |
| async+batched | 0.174 | 5749.9 | 249.16× |

## Reproduce

```bash
python scripts/bench_1000_doc.py --n 1000 --concurrency 32 --batch-size 8
```

## Real-API mode (operator action)

Swap `FakeLLM` for an Anthropic adapter that conforms to the `LLMClient` Protocol (`async __call__(prompt: str) -> str`) and re-run. The same script writes the same table; the speedup ratios will widen because real API I/O has more headroom for fan-out than the synthetic 20 ms sleep does.
