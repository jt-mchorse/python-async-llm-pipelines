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
