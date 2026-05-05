# Table Card Audit Report

## Summary
- total_cards: 37
- parse_success_count: 37
- parse_failed_count: 0
- ready_for_agent_count: 37
- manual_draft_count: 0
- too_long_count: 2
- missing_policy_count: 23

## Cards Needing Attention
| table_name | file | status | ready_for_agent | size_kb | issue_summary |
| --- | --- | --- | --- | --- | --- |
| dim_channel | knowledge_manual/table_cards/dim_channel.yaml | validated | True | 10.45 | missing_required_fields: agent_usage_policy; ready_for_agent_true_but_missing_agent_usage_policy; known_traps_maybe_too_few |
| dim_department | knowledge_manual/table_cards/dim_department.yaml | validated | True | 16.4 | missing_required_fields: agent_usage_policy; ready_for_agent_true_but_missing_agent_usage_policy; size_review_size |
| dim_feature | knowledge_manual/table_cards/dim_feature.yaml | validated | True | 10.64 | missing_required_fields: agent_usage_policy; ready_for_agent_true_but_missing_agent_usage_policy |
| dim_org_structure | knowledge_manual/table_cards/dim_org_structure.yaml | validated | True | 9.67 | missing_required_fields: agent_usage_policy; ready_for_agent_true_but_missing_agent_usage_policy; known_traps_maybe_too_few; sql_patterns_maybe_too_few |
| dim_plan | knowledge_manual/table_cards/dim_plan.yaml | validated | True | 11.64 | missing_required_fields: agent_usage_policy; ready_for_agent_true_but_missing_agent_usage_policy; known_traps_maybe_too_few; sql_patterns_maybe_too_few |
| dim_tenant | knowledge_manual/table_cards/dim_tenant.yaml | validated | True | 13.58 | missing_required_fields: agent_usage_policy; ready_for_agent_true_but_missing_agent_usage_policy; sql_patterns_maybe_too_few |
| dim_tenant_plan | knowledge_manual/table_cards/dim_tenant_plan.yaml | validated | True | 12.03 | missing_required_fields: agent_usage_policy; ready_for_agent_true_but_missing_agent_usage_policy; sql_patterns_maybe_too_few |
| dim_ticket_category | knowledge_manual/table_cards/dim_ticket_category.yaml | validated | True | 9.12 | missing_required_fields: agent_usage_policy; ready_for_agent_true_but_missing_agent_usage_policy; known_traps_maybe_too_few; sql_patterns_maybe_too_few |
| dim_user | knowledge_manual/table_cards/dim_user.yaml | validated | True | 13.93 | missing_required_fields: agent_usage_policy; ready_for_agent_true_but_missing_agent_usage_policy; known_traps_maybe_too_few; sql_patterns_maybe_too_few |
| dim_user_id_mapping | knowledge_manual/table_cards/dim_user_id_mapping.yaml | validated | True | 8.18 | missing_required_fields: agent_usage_policy; ready_for_agent_true_but_missing_agent_usage_policy; known_traps_maybe_too_few; sql_patterns_maybe_too_few |
| dim_user_role_history | knowledge_manual/table_cards/dim_user_role_history.yaml | validated | True | 9.91 | missing_required_fields: agent_usage_policy; ready_for_agent_true_but_missing_agent_usage_policy; known_traps_maybe_too_few; sql_patterns_maybe_too_few |
| fact_actual_revenue | knowledge_manual/table_cards/fact_actual_revenue.yaml | validated_with_warnings | True | 32.96 | size_too_long; known_traps_maybe_too_many |
| fact_ai_usage_log | knowledge_manual/table_cards/fact_ai_usage_log.yaml | validated | True | 27.33 | size_too_long |
| fact_campaign | knowledge_manual/table_cards/fact_campaign.yaml | validated | True | 10.51 | missing_required_fields: agent_usage_policy; ready_for_agent_true_but_missing_agent_usage_policy; known_traps_maybe_too_few; sql_patterns_maybe_too_few |
| fact_campaign_attribution | knowledge_manual/table_cards/fact_campaign_attribution.yaml | validated | True | 17.08 | size_review_size; sql_patterns_maybe_too_many |
| fact_credit_usage | knowledge_manual/table_cards/fact_credit_usage.yaml | validated | True | 15.21 | size_review_size |
| fact_daily_usage | knowledge_manual/table_cards/fact_daily_usage.yaml | validated | True | 18.48 | size_review_size; sql_patterns_maybe_too_many |
| fact_doc_collaboration | knowledge_manual/table_cards/fact_doc_collaboration.yaml | validated_with_warnings | True | 16.87 | size_review_size; sql_patterns_maybe_too_many |
| fact_document | knowledge_manual/table_cards/fact_document.yaml | validated | True | 11.9 | missing_required_fields: agent_usage_policy; ready_for_agent_true_but_missing_agent_usage_policy; known_traps_maybe_too_few |
| fact_event_log | knowledge_manual/table_cards/fact_event_log.yaml | validated | True | 11.66 | missing_required_fields: agent_usage_policy; ready_for_agent_true_but_missing_agent_usage_policy; known_traps_maybe_too_few; sql_patterns_maybe_too_few |
| fact_experiment | knowledge_manual/table_cards/fact_experiment.yaml | validated | True | 10.4 | missing_required_fields: agent_usage_policy; ready_for_agent_true_but_missing_agent_usage_policy; known_traps_maybe_too_few; sql_patterns_maybe_too_few |
| fact_experiment_assignment | knowledge_manual/table_cards/fact_experiment_assignment.yaml | validated | True | 10.21 | missing_required_fields: agent_usage_policy; ready_for_agent_true_but_missing_agent_usage_policy; known_traps_maybe_too_few; sql_patterns_maybe_too_few |
| fact_experiment_metric | knowledge_manual/table_cards/fact_experiment_metric.yaml | validated | True | 12.73 | missing_required_fields: agent_usage_policy; ready_for_agent_true_but_missing_agent_usage_policy; known_traps_maybe_too_few |
| fact_feature_usage | knowledge_manual/table_cards/fact_feature_usage.yaml | validated | True | 15.81 | missing_required_fields: agent_usage_policy; ready_for_agent_true_but_missing_agent_usage_policy; size_review_size |
| fact_invoice | knowledge_manual/table_cards/fact_invoice.yaml | validated | True | 13.12 | missing_required_fields: agent_usage_policy; ready_for_agent_true_but_missing_agent_usage_policy |
| fact_message | knowledge_manual/table_cards/fact_message.yaml | validated | True | 11.84 | missing_required_fields: agent_usage_policy; ready_for_agent_true_but_missing_agent_usage_policy; known_traps_maybe_too_few |
| fact_nps_survey | knowledge_manual/table_cards/fact_nps_survey.yaml | validated_with_warnings | True | 21.91 | size_review_size |
| fact_page_view | knowledge_manual/table_cards/fact_page_view.yaml | validated | True | 12.25 | missing_required_fields: agent_usage_policy; ready_for_agent_true_but_missing_agent_usage_policy; known_traps_maybe_too_few; sql_patterns_maybe_too_few |
| fact_payment | knowledge_manual/table_cards/fact_payment.yaml | validated | True | 12.04 | missing_required_fields: agent_usage_policy; ready_for_agent_true_but_missing_agent_usage_policy |
| fact_subscription | knowledge_manual/table_cards/fact_subscription.yaml | validated | True | 14.34 | missing_required_fields: agent_usage_policy; ready_for_agent_true_but_missing_agent_usage_policy; sql_patterns_maybe_too_few |
| fact_tenant_metrics_snapshot | knowledge_manual/table_cards/fact_tenant_metrics_snapshot.yaml | validated | True | 20.78 | size_review_size |
| fact_ticket | knowledge_manual/table_cards/fact_ticket.yaml | validated | True | 17.97 | size_review_size; sql_patterns_maybe_too_many |

## All Cards
| table_name | file | authoring_status | grain_validated | dq_validated | ready_for_agent | size_kb | known_traps_count | sql_patterns_count |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| dim_channel | knowledge_manual/table_cards/dim_channel.yaml | validated | True | True | True | 10.45 | 2 | 4 |
| dim_department | knowledge_manual/table_cards/dim_department.yaml | validated | True | True | True | 16.4 | 4 | 6 |
| dim_feature | knowledge_manual/table_cards/dim_feature.yaml | validated | True | True | True | 10.64 | 3 | 4 |
| dim_org_structure | knowledge_manual/table_cards/dim_org_structure.yaml | validated | True | True | True | 9.67 | 2 | 3 |
| dim_plan | knowledge_manual/table_cards/dim_plan.yaml | validated | True | True | True | 11.64 | 2 | 3 |
| dim_tenant | knowledge_manual/table_cards/dim_tenant.yaml | validated | True | True | True | 13.58 | 3 | 3 |
| dim_tenant_config | knowledge_manual/table_cards/dim_tenant_config.yaml | validated_with_warnings | True | partial | True | 10.96 | 5 | 5 |
| dim_tenant_plan | knowledge_manual/table_cards/dim_tenant_plan.yaml | validated | True | True | True | 12.03 | 3 | 3 |
| dim_ticket_category | knowledge_manual/table_cards/dim_ticket_category.yaml | validated | True | True | True | 9.12 | 2 | 2 |
| dim_user | knowledge_manual/table_cards/dim_user.yaml | validated | True | True | True | 13.93 | 2 | 3 |
| dim_user_id_mapping | knowledge_manual/table_cards/dim_user_id_mapping.yaml | validated | True | True | True | 8.18 | 2 | 3 |
| dim_user_profile | knowledge_manual/table_cards/dim_user_profile.yaml | validated_with_warnings | partial | True | True | 12.28 | 5 | 8 |
| dim_user_role_history | knowledge_manual/table_cards/dim_user_role_history.yaml | validated | True | True | True | 9.91 | 2 | 3 |
| fact_actual_revenue | knowledge_manual/table_cards/fact_actual_revenue.yaml | validated_with_warnings | partial | partial | True | 32.96 | 8 | 5 |
| fact_ai_usage_log | knowledge_manual/table_cards/fact_ai_usage_log.yaml | validated | True | True | True | 27.33 | 7 | 8 |
| fact_campaign | knowledge_manual/table_cards/fact_campaign.yaml | validated | True | True | True | 10.51 | 2 | 3 |
| fact_campaign_attribution | knowledge_manual/table_cards/fact_campaign_attribution.yaml | validated | True | True | True | 17.08 | 5 | 9 |
| fact_credit_usage | knowledge_manual/table_cards/fact_credit_usage.yaml | validated | True | True | True | 15.21 | 5 | 8 |
| fact_daily_usage | knowledge_manual/table_cards/fact_daily_usage.yaml | validated | True | True | True | 18.48 | 6 | 9 |
| fact_doc_collaboration | knowledge_manual/table_cards/fact_doc_collaboration.yaml | validated_with_warnings | partial | True | True | 16.87 | 6 | 9 |
| fact_document | knowledge_manual/table_cards/fact_document.yaml | validated | True | True | True | 11.9 | 2 | 4 |
| fact_event_log | knowledge_manual/table_cards/fact_event_log.yaml | validated | True | True | True | 11.66 | 2 | 3 |
| fact_experiment | knowledge_manual/table_cards/fact_experiment.yaml | validated | True | True | True | 10.4 | 2 | 3 |
| fact_experiment_assignment | knowledge_manual/table_cards/fact_experiment_assignment.yaml | validated | True | True | True | 10.21 | 1 | 3 |
| fact_experiment_metric | knowledge_manual/table_cards/fact_experiment_metric.yaml | validated | True | True | True | 12.73 | 1 | 4 |
| fact_feature_usage | knowledge_manual/table_cards/fact_feature_usage.yaml | validated | True | True | True | 15.81 | 4 | 5 |
| fact_invoice | knowledge_manual/table_cards/fact_invoice.yaml | validated | True | True | True | 13.12 | 3 | 4 |
| fact_message | knowledge_manual/table_cards/fact_message.yaml | validated | True | True | True | 11.84 | 2 | 4 |
| fact_nps_survey | knowledge_manual/table_cards/fact_nps_survey.yaml | validated_with_warnings | True | partial | True | 21.91 | 6 | 7 |
| fact_page_view | knowledge_manual/table_cards/fact_page_view.yaml | validated | True | True | True | 12.25 | 2 | 3 |
| fact_payment | knowledge_manual/table_cards/fact_payment.yaml | validated | True | True | True | 12.04 | 3 | 4 |
| fact_session | knowledge_manual/table_cards/fact_session.yaml | validated | True | True | True | 14.7 | 5 | 8 |
| fact_subscription | knowledge_manual/table_cards/fact_subscription.yaml | validated | True | True | True | 14.34 | 3 | 3 |
| fact_tenant_metrics_snapshot | knowledge_manual/table_cards/fact_tenant_metrics_snapshot.yaml | validated | True | True | True | 20.78 | 6 | 8 |
| fact_ticket | knowledge_manual/table_cards/fact_ticket.yaml | validated | True | True | True | 17.97 | 5 | 10 |
| fact_ticket_reply | knowledge_manual/table_cards/fact_ticket_reply.yaml | validated | True | True | True | 14.77 | 5 | 7 |
| fact_user_activation | knowledge_manual/table_cards/fact_user_activation.yaml | validated | True | True | True | 11.57 | 4 | 6 |
