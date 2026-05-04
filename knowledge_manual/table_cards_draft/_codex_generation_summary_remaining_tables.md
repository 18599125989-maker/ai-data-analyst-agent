# Remaining Tables Draft Generation Summary

## 识别到的所有表名

- dim_channel
- dim_department
- dim_feature
- dim_org_structure
- dim_plan
- dim_tenant
- dim_tenant_config
- dim_tenant_plan
- dim_ticket_category
- dim_user
- dim_user_id_mapping
- dim_user_profile
- dim_user_role_history
- fact_actual_revenue
- fact_ai_usage_log
- fact_campaign
- fact_campaign_attribution
- fact_credit_usage
- fact_daily_usage
- fact_doc_collaboration
- fact_document
- fact_event_log
- fact_experiment
- fact_experiment_assignment
- fact_experiment_metric
- fact_feature_usage
- fact_invoice
- fact_message
- fact_nps_survey
- fact_page_view
- fact_payment
- fact_session
- fact_subscription
- fact_tenant_metrics_snapshot
- fact_ticket
- fact_ticket_reply
- fact_user_activation

## 跳过的正式表

- dim_department
- fact_actual_revenue
- fact_ai_usage_log

## 已保留的既有 Draft

- dim_tenant
- dim_user
- fact_subscription
- dim_plan
- fact_daily_usage

## 本次生成的 Draft YAML

- knowledge_manual/table_cards_draft/dim_channel.yaml
- knowledge_manual/table_cards_draft/dim_feature.yaml
- knowledge_manual/table_cards_draft/dim_org_structure.yaml
- knowledge_manual/table_cards_draft/dim_tenant_config.yaml
- knowledge_manual/table_cards_draft/dim_tenant_plan.yaml
- knowledge_manual/table_cards_draft/dim_ticket_category.yaml
- knowledge_manual/table_cards_draft/dim_user_id_mapping.yaml
- knowledge_manual/table_cards_draft/dim_user_profile.yaml
- knowledge_manual/table_cards_draft/dim_user_role_history.yaml
- knowledge_manual/table_cards_draft/fact_campaign.yaml
- knowledge_manual/table_cards_draft/fact_campaign_attribution.yaml
- knowledge_manual/table_cards_draft/fact_credit_usage.yaml
- knowledge_manual/table_cards_draft/fact_doc_collaboration.yaml
- knowledge_manual/table_cards_draft/fact_document.yaml
- knowledge_manual/table_cards_draft/fact_event_log.yaml
- knowledge_manual/table_cards_draft/fact_experiment.yaml
- knowledge_manual/table_cards_draft/fact_experiment_assignment.yaml
- knowledge_manual/table_cards_draft/fact_experiment_metric.yaml
- knowledge_manual/table_cards_draft/fact_feature_usage.yaml
- knowledge_manual/table_cards_draft/fact_invoice.yaml
- knowledge_manual/table_cards_draft/fact_message.yaml
- knowledge_manual/table_cards_draft/fact_nps_survey.yaml
- knowledge_manual/table_cards_draft/fact_page_view.yaml
- knowledge_manual/table_cards_draft/fact_payment.yaml
- knowledge_manual/table_cards_draft/fact_session.yaml
- knowledge_manual/table_cards_draft/fact_tenant_metrics_snapshot.yaml
- knowledge_manual/table_cards_draft/fact_ticket.yaml
- knowledge_manual/table_cards_draft/fact_ticket_reply.yaml
- knowledge_manual/table_cards_draft/fact_user_activation.yaml

## 本次生成的 Validate 脚本

- src/03_2_validate_dim_channel_card.py
- src/03_2_validate_dim_feature_card.py
- src/03_2_validate_dim_org_structure_card.py
- src/03_2_validate_dim_tenant_config_card.py
- src/03_2_validate_dim_tenant_plan_card.py
- src/03_2_validate_dim_ticket_category_card.py
- src/03_2_validate_dim_user_id_mapping_card.py
- src/03_2_validate_dim_user_profile_card.py
- src/03_2_validate_dim_user_role_history_card.py
- src/03_2_validate_fact_campaign_card.py
- src/03_2_validate_fact_campaign_attribution_card.py
- src/03_2_validate_fact_credit_usage_card.py
- src/03_2_validate_fact_doc_collaboration_card.py
- src/03_2_validate_fact_document_card.py
- src/03_2_validate_fact_event_log_card.py
- src/03_2_validate_fact_experiment_card.py
- src/03_2_validate_fact_experiment_assignment_card.py
- src/03_2_validate_fact_experiment_metric_card.py
- src/03_2_validate_fact_feature_usage_card.py
- src/03_2_validate_fact_invoice_card.py
- src/03_2_validate_fact_message_card.py
- src/03_2_validate_fact_nps_survey_card.py
- src/03_2_validate_fact_page_view_card.py
- src/03_2_validate_fact_payment_card.py
- src/03_2_validate_fact_session_card.py
- src/03_2_validate_fact_tenant_metrics_snapshot_card.py
- src/03_2_validate_fact_ticket_card.py
- src/03_2_validate_fact_ticket_reply_card.py
- src/03_2_validate_fact_user_activation_card.py

## 需要人工复查的表或字段

- dim_channel: 存在时间字段，需要复核时间口径：created_at；存在可聚合数值字段，需要复核聚合口径：member_count
- dim_feature: 存在时间字段，需要复核时间口径：launched_at
- dim_org_structure: 存在可聚合数值字段，需要复核聚合口径：level
- dim_tenant_config: 存在候选自然键，需要确认唯一性和业务稳定性；存在 JSON/text 字段：config_json；存在可聚合数值字段，需要复核聚合口径：enabled
- dim_tenant_plan: 存在候选自然键，需要确认唯一性和业务稳定性；存在时间字段，需要复核时间口径：effective_from, effective_to
- dim_ticket_category: 存在可聚合数值字段，需要复核聚合口径：sla_hours
- dim_user_profile: 数据字典未明确稳定主键或当前主键仅为候选
- dim_user_role_history: 数据字典未明确稳定主键或当前主键仅为候选；存在时间字段，需要复核时间口径：changed_at
- fact_campaign: 存在时间字段，需要复核时间口径：start_date, end_date；存在可聚合数值字段，需要复核聚合口径：budget
- fact_campaign_attribution: 数据字典未明确稳定主键或当前主键仅为候选；存在候选自然键，需要确认唯一性和业务稳定性；存在时间字段，需要复核时间口径：touch_at；存在可聚合数值字段，需要复核聚合口径：is_converted
- fact_credit_usage: 存在时间字段，需要复核时间口径：dt；存在可聚合数值字段，需要复核聚合口径：quantity, unit_cost
- fact_doc_collaboration: 数据字典未明确稳定主键或当前主键仅为候选；存在时间字段，需要复核时间口径：dt；存在可聚合数值字段，需要复核聚合口径：edit_duration_sec, comment_count
- fact_document: 存在时间字段，需要复核时间口径：created_at, last_edit_at；存在可聚合数值字段，需要复核聚合口径：edit_count
- fact_event_log: 存在 JSON/text 字段：properties_json；存在时间字段，需要复核时间口径：event_time
- fact_experiment: 存在时间字段，需要复核时间口径：start_date, end_date
- fact_experiment_assignment: 数据字典未明确稳定主键或当前主键仅为候选；存在候选自然键，需要确认唯一性和业务稳定性；存在时间字段，需要复核时间口径：assigned_at
- fact_experiment_metric: 数据字典未明确稳定主键或当前主键仅为候选；存在候选自然键，需要确认唯一性和业务稳定性；存在时间字段，需要复核时间口径：dt；存在可聚合数值字段，需要复核聚合口径：sample_size, metric_value, ci_lower, ci_upper
- fact_feature_usage: 存在候选自然键，需要确认唯一性和业务稳定性；存在时间字段，需要复核时间口径：dt；存在可聚合数值字段，需要复核聚合口径：action_count, duration_sec
- fact_invoice: 存在时间字段，需要复核时间口径：issued_at, paid_at；存在可聚合数值字段，需要复核聚合口径：amount
- fact_message: 存在时间字段，需要复核时间口径：sent_at；存在可聚合数值字段，需要复核聚合口径：word_count
- fact_nps_survey: 存在时间字段，需要复核时间口径：survey_date；存在可聚合数值字段，需要复核聚合口径：role, is_completed
- fact_page_view: 存在可聚合数值字段，需要复核聚合口径：view_duration_ms
- fact_payment: 存在时间字段，需要复核时间口径：paid_at；存在可聚合数值字段，需要复核聚合口径：amount
- fact_session: 存在时间字段，需要复核时间口径：start_time, end_time
- fact_tenant_metrics_snapshot: 存在候选自然键，需要确认唯一性和业务稳定性；存在时间字段，需要复核时间口径：snapshot_date；存在可聚合数值字段，需要复核聚合口径：total_users, total_docs, total_storage_mb, total_revenue
- fact_ticket: 存在时间字段，需要复核时间口径：created_at, resolved_at
- fact_ticket_reply: 存在时间字段，需要复核时间口径：replied_at；存在可聚合数值字段，需要复核聚合口径：content_length
- fact_user_activation: 数据字典未明确稳定主键或当前主键仅为候选；存在候选自然键，需要确认唯一性和业务稳定性；存在时间字段，需要复核时间口径：reached_at

## 验证脚本运行通过且无 Summary Warning

- dim_channel
- dim_feature
- dim_org_structure
- dim_tenant_plan
- dim_ticket_category
- dim_user_id_mapping
- dim_user_role_history
- fact_campaign
- fact_campaign_attribution
- fact_credit_usage
- fact_document
- fact_event_log
- fact_experiment
- fact_experiment_assignment
- fact_experiment_metric
- fact_feature_usage
- fact_invoice
- fact_message
- fact_page_view
- fact_payment
- fact_session
- fact_tenant_metrics_snapshot
- fact_ticket
- fact_ticket_reply
- fact_user_activation

## 验证脚本运行通过但存在 Summary Warning

- dim_tenant_config: config_json 存在 5000.0 条无法解析为 JSON 的记录。
- dim_user_profile: 主键候选存在 2500 条重复。
- fact_doc_collaboration: 主键候选存在 1418 条重复。
- fact_nps_survey: plan_tier 存在 8000.0 条无法关联的记录。

## 验证脚本存在 Failed Check

- 无

## 建议后续人工验证顺序

### 第一优先级

- dim_tenant
- dim_user
- fact_subscription
- dim_plan
- fact_daily_usage
- fact_feature_usage
- fact_invoice
- fact_payment
- fact_ticket
- fact_nps_survey
- fact_experiment
- fact_experiment_assignment
- fact_experiment_metric
- dim_user_id_mapping

### 第二优先级

- fact_session
- fact_page_view
- fact_event_log
- fact_message
- fact_document
- fact_doc_collaboration
- fact_user_activation
- fact_campaign
- fact_campaign_attribution
- fact_credit_usage
- fact_tenant_metrics_snapshot
- fact_ticket_reply
- dim_user_profile
- dim_user_role_history
- dim_tenant_config
- dim_tenant_plan

### 第三优先级

- dim_channel
- dim_feature
- dim_org_structure
- dim_ticket_category