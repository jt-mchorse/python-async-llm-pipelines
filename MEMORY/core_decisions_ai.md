# Core Decisions (AI-readable, YAML, append-only)
# Schema: see .skills/portfolio-memory/SKILL.md

- id: D-001
  date: 2026-05-10
  decision: scope_per_portfolio_handoff_section_2
  rationale: locked_scope_prevents_drift
  alternatives_rejected: []
  reversibility: expensive
  related_issues: []
  superseded_by: null

- id: D-002
  date: 2026-05-14
  decision: wrapper_runtime_dep_free_anthropic_openai_httpx_are_not_required
  rationale: keep_import_cost_low_concurrency_primitive_should_be_usable_with_any_provider_or_no_provider
  alternatives_rejected: [bundle_anthropic_sdk_as_required, bundle_httpx_as_required]
  reversibility: cheap
  related_issues: [1, 2, 4]
  superseded_by: null

- id: D-003
  date: 2026-05-14
  decision: process_returns_results_in_input_order_stream_returns_in_completion_order
  rationale: process_callers_need_to_correlate_outputs_back_to_inputs_without_maintaining_their_own_map_stream_callers_dont_have_meaningful_producer_order_and_forcing_it_would_defeat_backpressure
  alternatives_rejected: [as_completed_iterator_for_both, input_order_for_stream_too]
  reversibility: cheap
  related_issues: [1, 4]
  superseded_by: null

- id: D-004
  date: 2026-05-15
  decision: tool_registry_is_thin_dict_wrapper_async_callables_named_lookup
  rationale: tools_are_just_async_functions_no_inheritance_or_class_hierarchy_decorator_form_for_natural_registration
  alternatives_rejected: [abstract_tool_base_class, dependency_injection_container, openapi_schema_first_registry]
  reversibility: cheap
  related_issues: [2]
  superseded_by: null

- id: D-005
  date: 2026-05-15
  decision: toolresult_carries_tool_call_id_for_round_trip_to_anthropic_tool_use_id
  rationale: anthropic_tool_use_response_requires_matching_id_back_callers_should_not_have_to_maintain_their_own_correlation_map
  alternatives_rejected: [results_in_input_order_no_id, results_keyed_by_position]
  reversibility: cheap
  related_issues: [2]
  superseded_by: null

- id: D-006
  date: 2026-05-15
  decision: dispatch_default_fail_fast_return_exceptions_opt_in_for_partial_tolerance
  rationale: parity_with_async_pipelines_process_d003_acceptance_criterion_partial_failures_dont_poison_satisfied_by_opt_in
  alternatives_rejected: [default_partial_tolerance_silent_failures, separate_function_for_each_mode]
  reversibility: cheap
  related_issues: [2]
  superseded_by: null

- id: D-007
  date: 2026-05-16
  decision: benchmark_ships_with_fakellm_for_ci_real_api_is_two_line_operator_swap
  rationale: speedup_ratio_is_load_bearing_claim_synthetic_sleep_proves_it_real_api_numbers_only_validate_at_operator_cost
  alternatives_rejected: [require_anthropic_api_key_in_ci_burns_budget, no_benchmark_at_all_misses_the_load_bearing_v0_1_claim, ship_fabricated_numbers_violates_handoff_section_10]
  reversibility: cheap
  related_issues: [4]
  superseded_by: null

- id: D-008
  date: 2026-05-16
  decision: batched_pipeline_uses_one_round_trip_per_batch_via_make_batch_caller_seam
  rationale: anthropic_batch_api_shape_is_one_request_per_n_inputs_synthetic_emulates_with_one_sleep_per_batch_real_swaps_in_actual_batch_endpoint
  alternatives_rejected: [batched_just_means_chunked_async_loses_the_batch_api_win, message_create_per_item_inside_batch_redundant_with_async_pipeline]
  reversibility: cheap
  related_issues: [4]
  superseded_by: null

- id: D-009
  date: 2026-05-17
  decision: stream_metrics_is_in_place_dataclass_passed_via_keyword_only_arg_default_none
  rationale: operators_need_to_see_backpressure_engaging_producer_pauses_and_max_queue_depth_in_place_avoids_returning_a_tuple_from_stream_keeps_d002_dep_free_dataclass_is_stdlib
  alternatives_rejected: [return_tuple_of_results_and_metrics_breaks_back_compat_for_two_existing_callsites, separate_metrics_module_splits_30_line_dataclass_from_only_function_that_writes_it, expose_callback_per_event_overkill_for_demo_repo_and_harder_to_reason_about]
  reversibility: cheap
  related_issues: [3]
  superseded_by: null

- id: D-010
  date: 2026-05-18
  decision: per_item_timeout_is_kwarg_on_process_and_stream_not_separate_decorator
  rationale: composes_with_existing_fail_fast_and_return_exceptions_policies_one_api_surface_mirrors_asyncio_wait_for_shape_and_d_006_opt_in_partial_tolerance_idiom
  alternatives_rejected: [separate_timed_decorator_doubles_api_surface, batch_level_deadline_loses_one_slow_item_shouldnt_fail_batch_use_case, throw_timeouterror_directly_no_carried_index_breaks_correlation]
  reversibility: cheap
  related_issues: [5]
  superseded_by: null
