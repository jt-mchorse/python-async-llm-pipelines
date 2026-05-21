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

---
session: 2026-05-17T19:08Z
duration_min: 40
issue: 3
focus: backpressure_metrics_and_oom_safety_demo
delta:
  files_added: 2  # scripts/bench_backpressure.py, docs/backpressure.{md,json}
  files_changed: 3  # core.py adds StreamMetrics + metrics= kwarg, __init__.py exports, tests/test_stream.py
  tests_added: 5
  test_pass_rate: "48/48"
  benchmarks:
    bench_n: 5000
    consumer_ms: 1.0
    concurrency: 2
    queue_size_8: { duration_s: 3.051, peak_heap_kb: 201.7, producer_pauses: 2707, max_queue_depth: 8 }
    queue_size_32: { duration_s: 3.080, peak_heap_kb: 198.2, producer_pauses: 2672, max_queue_depth: 32 }
context_for_next_session:
  - stream_metrics_dataclass_lives_in_core_py_d_009_stdlib_only_dep_free_per_d_002
  - metrics_param_keyword_only_default_none_zero_overhead_when_omitted
  - metrics_record_produced_consumed_producer_pauses_max_queue_depth_producer_pause_seconds
  - producer_pauses_incremented_when_queue_full_at_put_time_pause_duration_via_perf_counter
  - max_queue_depth_high_water_mark_sampled_after_each_put_bounded_by_queue_size_by_construction
  - scripts_bench_backpressure_py_fast_producer_slow_consumer_tracemalloc_peak_heap_writes_docs_backpressure_md_and_json
  - issue_3_acceptance_bounded_queue_implemented_already_in_v0_pr_producer_pauses_now_observable_via_metrics_oom_safety_demo_in_docs_backpressure_md
  - five_new_tests_metrics_present_counts_pauses_no_pauses_when_fast_max_depth_bounded_default_path_unchanged
  - readme_backpressure_subsection_includes_real_numbers_5000_items_qs_8_vs_qs_32_max_depth_matches_queue_size
decisions_made: [D-009]
followups: []
---

---
session: 2026-05-18T03:30Z
duration_min: 30
issue: 5
focus: per_item_timeout_and_cancellation_regression
delta:
  files_changed: 4
  tests_added: 12
context_for_next_session:
  - timeout_kwarg_added_to_process_and_stream_d_010
  - pipelinetimeouterror_subclass_of_pipelineerror_carries_index_and_timeout_s
  - no_orphaned_tasks_invariant_now_has_two_regression_tests_internal_deadline_and_external_cancel
  - timeout_none_default_byte_identical_pre_5_shape
decisions_made: [D-010]
followups: []
---

---
session: 2026-05-18T15:44Z
duration_min: 20
issue: 11
focus: architecture_doc_covers_five_shipped_primitives
delta:
  files_changed: 2  # README.md, docs/architecture.md
  files_added: 0
  tests_added: 0
  test_pass_rate: "60/60"
context_for_next_session:
  - docs_architecture_md_rewritten_one_integrated_pipeline_lifecycle_mermaid_plus_five_layer_sections
  - five_layer_sections_process_stream_tool_dispatch_backpressure_benchmark_timeouts
  - readme_architecture_block_signatures_refreshed_per_item_timeout_metrics_kwargs_visible
  - pending_section_removed_every_primitive_in_section_2_has_shipped
  - mermaid_labels_with_parens_fully_double_quoted_same_lint_as_other_repos_this_session
  - no_new_d_entry_references_d_002_through_d_010
decisions_made: []
followups: []
---

---
session: 2026-05-19T05:45Z
duration_min: 25
issue: 13
focus: drop_this_pr_ships_framing_plus_readme_snapshot
delta:
  files_changed: 1   # README.md
  files_added: 1     # tests/test_readme_snapshot.py
  tests_added: 4
  test_pass_rate: "64/64"
context_for_next_session:
  - readme_what_this_is_rewritten_to_five_bullet_present_tense_layer_picture
  - demo_section_replaces_pending_depends_on_4_with_two_command_hermetic_demo_path
  - snapshot_test_locks_issue_refs_plus_this_pr_ships_absence_plus_demo_section_invariant
  - capture_followup_filed_as_issue_14
  - tamper_verified_reinjecting_this_pr_ships_fires_snapshot
decisions_made: []
followups: ["#14"]
---

---
session: 2026-05-19T15:31Z
duration_min: 25
issue: 16
focus: snapshot_test_locks_readme_and_benchmarks_md_to_docs_benchmarks_json
delta:
  files_added: 1   # tests/test_bench_table_snapshot.py
  files_changed: 0
  tests_added: 7   # 1 pipeline-set guard + 3 readme rows + 3 benchmarks_md rows
  test_pass_rate: "71/71"
context_for_next_session:
  - second_snapshot_test_in_this_repo_first_locks_structural_invariants_second_locks_bench_table_cells
  - three_sources_of_truth_collapsed_docs_benchmarks_json_committed_readme_table_benchmarks_md_table
  - test_renders_json_into_expected_cells_does_not_rerun_bench_wall_clock_non_deterministic
  - readme_uses_conditional_speedup_format_2f_below_100x_0f_at_or_above_only_batched_251x_triggers_integer
  - benchmarks_md_uses_uniform_2f_for_speedup
  - pipeline_name_set_guard_serial_async_async_batched_silent_drops_fail_loudly
  - parametrized_over_three_pipelines_for_each_renderer_seven_tests_total
  - tamper_verified_readme_serial_43_311_to_43_999_fires_with_regen_hint
  - no_new_d_entry_d_004_governs_fakellm_baseline_json_is_pure_render_source
  - remaining_gap_docs_backpressure_json_md_pair_same_pattern_would_apply_potential_followup
decisions_made: []
followups: []
---

---
session: 2026-05-20T03:26Z
duration_min: 20
issue: 18
focus: public_surface_snapshot_test_locks_async_pipelines_top_level_init
delta:
  files_added: 1   # tests/test_public_surface.py
  files_changed: 1   # async_pipelines/__init__.py (+__version__)
  tests_added: 8   # 5 standalone + 3 parametrized submodule anchors
  test_pass_rate: "79/79"
context_for_next_session:
  - async_pipelines_now_publishes_dunder_version_str_0_0_1
  - first_variant_combining_readme_quickstart_imports_axis_and_readme_dotted_path_axis_in_same_suite
  - readme_quotes_five_import_snippets_lines_88_107_129_188_231_union_seven_names
  - readme_quotes_one_dotted_path_line_29_async_pipelines_tool_dispatch_dispatch_tool_calls
  - tamper_verified_four_axes_bad_version_drop_streammetrics_monkey_delete_dispatch_alias_rename_process
  - portable_pattern_seventh_strike_remaining_only_mcp_python_example
decisions_made: []
followups: []
---

---
session: 2026-05-21T19:29Z
duration_min: 28
issue: 14
focus: scripts_capture_demo_sh_two_surface_60s_driver_plus_smoke_test_plus_readme_out_flag_fix
delta:
  files_added: 2   # scripts/capture_demo.sh, tests/test_capture_demo_smoke.py
  files_changed: 1 # README.md (Demo bench invocation now includes --out)
  tests_added: 3
  test_pass_rate: "82/82"
context_for_next_session:
  - eighth_repo_to_land_capture_demo_sh_pattern_this_week
  - two_surfaces_pytest_then_bench_n_200_chosen_so_full_demo_fits_60s_with_ratios_preserved_per_bench_synthetic_llm_disclosure
  - capture_script_must_pass_ignore_tests_test_capture_demo_smoke_py_to_inner_pytest_so_outer_pytest_invocation_doesnt_recurse
  - addopts_ra_q_plus_explicit_q_becomes_qq_silencing_summary_line_script_omits_second_q
  - bench_1000_doc_py_default_out_is_docs_benchmarks_md_passing_out_tmp_keeps_capture_from_mutating_committed_snapshot
  - readme_demo_block_bench_invocation_also_missed_out_flag_fixed_inline_same_kind_of_accuracy_fix_as_vector_search_at_scale
  - smoke_test_pins_n_passed_summary_substring_no_failed_substring_bench_header_signature_and_every_pipeline_data_row
  - no_new_d_entry_pure_glue_plus_documentation_accuracy
decisions_made: []
followups: []
---
