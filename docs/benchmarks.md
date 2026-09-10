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

Swap `FakeLLM` for an Anthropic adapter that conforms to the `LLMClient` Protocol (`async __call__(prompt: str) -> str`) and re-run. The same script writes the same table. Expect the speedup ratios to be **lower** than the synthetic ones above, not higher: `FakeLLM`'s pure-wait `asyncio.sleep` has no per-request CPU, socket, TLS or JSON cost, so it fans out perfectly and the ratios here are the theoretical upper bound. Real API I/O adds that overhead and is additionally bounded by rate limits and connection-pool limits, so real-API speedups land in the 5-20x spec range. Batch API workloads are the documented exception and can exceed it.
