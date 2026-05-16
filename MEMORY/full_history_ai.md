# Session History (AI-readable, append-only)

Schema: see .skills/portfolio-memory/SKILL.md

---
session: 2026-05-14T15:50:00Z
duration_min: 45
issue: 1
focus: async_batching_with_bounded_concurrency
delta:
  files_added: 8
  files_changed: 4
  tests_added: 16
  coverage_pct: 97
context_for_next_session:
  - process_items_fn_concurrency_returns_results_in_input_order_taskgroup_plus_semaphore
  - stream_producer_fn_queue_returns_results_in_completion_order_not_producer_order_per_d003
  - default_fail_fast_via_taskgroup_return_exceptions_keeps_batch_alive
  - cancellation_propagates_cleanly_through_taskgroup_to_all_in_flight_fn_calls
  - wrapper_is_runtime_dep_free_per_d002_anthropic_openai_httpx_not_required
  - real_5_20x_win_benchmark_deferred_to_4_needs_real_llm_call_workload
decisions_made: [D-002, D-003]
followups: []
---

---
session: 2026-05-15T19:23Z
duration_min: 60
issue: 2
focus: concurrent_tool_call_dispatch
delta:
  files_added: 2
  files_changed: 2
  tests_added: 17
  test_pass_rate: "33/33"
context_for_next_session:
  - tool_dispatch_layer_shipped_taskgroup_plus_optional_semaphore_per_call_telemetry
  - toolregistry_thin_dict_wrapper_d004_decorator_form_for_natural_registration
  - toolresult_carries_tool_call_id_d005_round_trip_to_anthropic_tool_use_id
  - default_fail_fast_return_exceptions_opt_in_d006_parity_with_process
  - 5_tools_serial_vs_concurrent_bench_in_test_suite_real_5_to_20x_workload_still_4
  - missing_tool_pre_resolved_up_front_in_fail_fast_mode_clean_error_surface
decisions_made: [D-004, D-005, D-006]
followups: []
---

---
session: 2026-05-16T04:37Z
duration_min: 35
issue: 4
focus: 1000_doc_benchmark_serial_vs_async_vs_async_batched
delta:
  files_added: 4
  files_changed: 1
  tests_added: 10
  test_pass_rate: "43/43"
  benchmarks:
    serial_duration_s: 43.311
    async_duration_s: 1.427
    batched_duration_s: 0.172
    async_speedup_vs_serial: 30.34
    batched_speedup_vs_serial: 251.21
    n_docs: 1000
    host: "apple_silicon_arm64_python_3_14_0"
context_for_next_session:
  - benchmark_module_in_async_pipelines_benchmark_py_workload_runresult_3_pipeline_classes
  - fakellm_simulates_per_call_latency_via_asyncio_sleep_d_007_real_api_is_protocol_swap
  - batched_pipeline_uses_make_batch_caller_one_round_trip_per_batch_d_008
  - serial_async_batched_run_pipeline_function_attach_speedup_helper
  - script_bench_1000_doc_py_writes_docs_benchmarks_md_plus_docs_benchmarks_json
  - real_measured_numbers_committed_to_docs_benchmarks_md_30x_async_251x_batched
  - 10_new_tests_43_total_lint_format_clean_no_pytest_timeout_dep
  - issue_4_acceptance_single_script_runs_all_three_modes_done_numbers_in_docs_benchmarks_md_done_5_to_20x_documented_with_honest_disclosure_done
decisions_made: [D-007, D-008]
followups: []
---
