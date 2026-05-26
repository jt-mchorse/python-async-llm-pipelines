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

---
session: 2026-05-22T03:35Z
duration_min: 20
issue: 21
focus: fix_readme_per_item_timeout_drift_to_match_code_timeout_kwarg
delta:
  files_changed: 1   # README.md (two locations: L45 bullet, L67-68 arch block)
  files_added: 1     # tests/test_readme_kwarg_consistency.py
  tests_added: 4
  test_pass_rate: "86/86"
decisions_made: []
context_for_next_session:
  - readme_bullet_l45_and_arch_block_l67_l68_named_the_per_item_timeout_kwarg_real_kwarg_is_timeout_since_5_landed
  - prose_section_at_l196_l203_was_correct_only_the_bullet_and_arch_block_drifted
  - test_readme_kwarg_consistency_parses_arch_block_signature_lines_and_python_code_fence_callsites_compares_against_inspect_signature_process_parameters_and_inspect_signature_stream_parameters_unknown_kwargs_raise
  - call_site_parser_balance_parses_parens_at_depth_zero_so_nested_calls_dont_false_positive_a_kwarg_from_an_outer_call
  - chose_arch_block_param_order_to_match_actual_function_signature_concurrency_first_return_exceptions_then_timeout_metrics_last_on_stream
  - fourth_post_v0_1_honesty_fix_today_after_emb_shootout_chunking_lab_vector_search_at_scale
followups: []
---

---
session: 2026-05-23T03:43Z
duration_min: 15
issue: 24
focus: architecture_doc_drift_lock_dual_axis_test_only
decisions_made: []
delta:
  files_added: 1     # tests/test_architecture_doc.py
  tests_added: 12
  test_pass_rate: "98/98"
context_for_next_session:
  - test_only_lock_zero_doc_changes_doc_already_in_steady_state
  - dual_axis_hash_nn_plus_d_nnn_like_rag_production_kit_pr_30
  - known_shipped_issues_1_through_5_per_handoff_section_2_core_deliverables
  - tamper_verified_four_axes
  - fourth_of_five_sister_issues_in_this_night_sweep_one_remaining_agent_orchestration_platform
followups: []
---

---
session: 2026-05-24T04:00Z
duration_min: 20
issue: 26
focus: dispatch_tool_calls_timeout_kwarg_parity_with_process_stream
delta:
  files_changed: 3   # async_pipelines/tool_dispatch.py, tests/test_tool_dispatch.py, README.md
  tests_added: 5
  test_pass_rate: "103/103"
decisions_made: []
context_for_next_session:
  - process_and_stream_already_had_timeout_kwarg_but_dispatch_tool_calls_did_not_real_safety_gap_misbehaving_tool_could_hang_batch_indefinitely
  - implementation_wraps_fn_call_in_asyncio_wait_for_only_when_timeout_is_set_no_op_overhead_when_timeout_none
  - pipeline_timeout_error_carries_index_into_tool_calls_list_consistent_with_process_per_item_index
  - validation_at_dispatch_entry_timeout_le_0_raises_value_error_parity_with_process
  - return_exceptions_path_works_because_pipeline_timeout_error_is_exception_subclass_caught_by_existing_except_exception_in_run_with_telemetry
  - readme_signature_line_updated_to_show_new_kwarg_and_added_one_paragraph_under_dispatch_example_describing_the_kwarg_plus_pipeline_timeout_error_shape
  - eighth_in_night_session_loop_first_safety_gap_fix_rather_than_cli_parity
followups: []
---

---
session: 2026-05-24T15:45Z
duration_min: 12
issue: 28
focus: async_pipelines_constructor_time_concurrency_validation_plus_stale_init_docstring
delta:
  files_changed: 2   # async_pipelines/benchmark.py, async_pipelines/__init__.py
  files_added: 0
  tests_added: 5
  test_pass_rate: "108/108"
decisions_made: []
context_for_next_session:
  - batched_async_pipeline_already_validated_batch_size_ge_1_at_construction_but_neither_async_pipeline_validated_concurrency_misconfigured_workload_only_blew_up_on_first_run
  - process_itself_validates_concurrency_gt_0_at_call_time_constructor_validation_just_surfaces_one_stack_frame_earlier_no_semantic_change
  - serial_pipeline_untouched_no_concurrency_param
  - __init___py_docstring_listed_pre_26_dispatch_tool_calls_signature_timeout_kwarg_was_added_by_26_but_docstring_missed_it_backfilled_now
  - portfolio_pattern_fifth_in_day_session_loop_after_eval_harness_37_prompt_regression_32_mcp_cookbook_31_emb_shootout_26_first_constructor_validation_polish_rather_than_cli_naming
followups: []
---

---
session: 2026-05-25T01:35Z
duration_min: 15
issue: 30
focus: workload_post_init_validates_fields_at_construction_proximate_failure_for_operator_misconfig
delta:
  files_changed: 1   # async_pipelines/benchmark.py
  files_added: 0
  tests_added: 11  # 3 n_docs param + 2 latency param + 2 concurrency param + 2 batch_size param + 1 zero-latency + 1 minimum-valid
  test_pass_rate: "119_passed"
decisions_made: []
context_for_next_session:
  - workload_frozen_dataclass_at_benchmark_py_32_post_init_validates_n_docs_ge_one_llm_call_seconds_ge_zero_concurrency_ge_one_batch_size_ge_one
  - harm_was_n_docs_zero_or_negative_silently_produced_empty_docs_list_near_zero_duration_speedup_math_divides_by_zero_or_yields_inf
  - concurrency_and_batch_size_already_caught_at_async_pipeline_init_28_29_defense_in_depth_at_workload_surfaces_failure_at_operator_visible_api_not_inner_factory
  - llm_call_seconds_negative_caught_indirectly_by_asyncio_sleep_but_trace_points_at_inner_llm_call_not_at_workload_misconfig_site
  - parametrized_decorator_pattern_per_field_with_two_or_three_bad_values_each_plus_zero_latency_pin_plus_all_ones_minimum_valid_pin
  - sister_to_rag_production_kit_34_retriever_construct_time_k_rrf_validation_same_principle_failure_proximate_to_misconfiguration_site
  - seventh_phase_bc_target_in_180_min_day_session_after_phase_a_5_pr_merge_plus_six_prior_phase_bc_targets
followups: []
---

---
session: 2026-05-25T07:50Z
duration_min: 25
issue: 32
focus: core_benchmark_tool_dispatch_isinstance_int_plus_math_isfinite_guards
delta:
  files_changed: 7   # async_pipelines/core.py + benchmark.py + tool_dispatch.py + tests/test_process.py + test_benchmark.py + test_timeouts.py + test_tool_dispatch.py
  files_added: 0
  tests_added: 30   # approx (parametrized matrices across multiple sites)
  test_pass_rate: "146_passed"
decisions_made: []
context_for_next_session:
  - second_pr_in_python_async_tonight_first_via_phase_a_fixup_merge_of_31_workload_sign_only_post_init
  - three_public_entry_points_process_stream_dispatch_tool_calls_sign_only_on_concurrency_queue_size_timeout_propagated_nan_into_asyncio_semaphore_queue_wait_for_as_cryptic_typeerrors_or_silent_never_firing
  - workload_post_init_sign_only_on_n_docs_concurrency_batch_size_llm_call_seconds_nan_latency_skews_benchmark_throughput_numbers_bool_silently_flattens_count_intent
  - tightened_each_entry_to_isinstance_int_with_explicit_bool_exclusion_for_count_fields_math_isfinite_for_timeout_llm_call_seconds
  - error_messages_tightened_must_be_positive_to_must_be_a_positive_int_or_must_be_a_finite_positive_number_six_pre_existing_tests_pinning_old_strings_bulk_updated_via_sed
  - twelfth_phase_bc_target_in_360_min_night_session_all_originally_unvisited_tonight_repos_plus_all_already_touched_repos_have_two_phase_bc_iterations_now
  - portfolio_finiteness_integer_extension_sweep_complete_twelve_phase_bc_prs_plus_seven_phase_a_fixup_merges_nineteen_substantive_items_tonight_pattern_now_at_full_portfolio_coverage_across_python_and_typescript_repos
followups: []
---

---
session: 2026-05-26T03:45Z
duration_min: 20
issue: 34
focus: async_pipeline_and_batched_async_pipeline_init_isinstance_int_bool_reject_completes_32_sweep
delta:
  files_changed: 2   # async_pipelines/benchmark.py, tests/test_benchmark.py
  files_added: 0
  tests_added: 25   # 3 x 5 reject + 2 x 5 acceptance
  test_pass_rate: "171_passed"
decisions_made: []
context_for_next_session:
  - issue_34_filed_and_closed_in_same_session_two_un_tightened_constructor_sites_in_async_pipelines_benchmark_py
  - async_pipeline_init_and_batched_async_pipeline_init_were_the_two_remaining_sign_only_lt_1_construction_sites_in_repo_after_32_sweep_workload_post_init_process_stream_dispatch_tool_calls
  - serial_pipeline_init_reviewed_and_skipped_takes_no_numeric_parameters
  - silent_failure_mode_one_concurrency_true_was_lt_1_false_silently_bound_self_concurrency_true_then_process_caught_via_32_tightened_validator_but_error_pointed_at_process_not_construction_site_broke_eager_validation_contract_documented_in_source
  - silent_failure_mode_two_concurrency_1_5_silently_bound_surfaced_from_process_with_misleading_site
  - silent_failure_mode_three_concurrency_nan_silently_bound_surfaced_from_process
  - fix_mirrors_workload_post_init_lines_73_80_in_same_file_isinstance_int_bool_reject_above_existing_lt_1_sign_check
  - error_message_shape_concurrency_must_be_an_int_got_repr_then_existing_concurrency_must_be_ge_1_got_value_preserved_existing_test_matchers_carry_over
  - test_strategy_five_parametrize_blocks_three_reject_async_concurrency_batched_concurrency_batched_batch_size_plus_two_acceptance_async_concurrency_batched_both_fields
  - existing_rejects_zero_negative_concurrency_tests_continue_to_pass_unchanged_preservation_pin_implicit
  - test_count_python_async_now_171_was_146_after_32_added_25_new_collected_cases
  - fifth_phase_bc_target_in_360_min_night_session_after_prompt_regression_37_chunking_lab_31_vector_search_31
  - portfolio_validation_sweep_now_extends_into_five_repos_this_night_phase_bc_run_plus_four_phase_a_rescue_merges_total_nine_substantive_items
followups: []
---
