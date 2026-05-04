# 5 张核心表 Draft 生成摘要

## 已生成的 YAML

- `knowledge_manual/table_cards_draft/dim_tenant.yaml`
- `knowledge_manual/table_cards_draft/dim_user.yaml`
- `knowledge_manual/table_cards_draft/fact_subscription.yaml`
- `knowledge_manual/table_cards_draft/dim_plan.yaml`
- `knowledge_manual/table_cards_draft/fact_daily_usage.yaml`

## 已生成的验证脚本

- `src/03_2_validate_dim_tenant_card.py`
- `src/03_2_validate_dim_user_card.py`
- `src/03_2_validate_fact_subscription_card.py`
- `src/03_2_validate_dim_plan_card.py`
- `src/03_2_validate_fact_daily_usage_card.py`

## 需要人工复查的重点

- `dim_tenant.name` 当前看起来唯一，但不应直接视为稳定自然键。
- `dim_user.dept_id` 在数据字典中声明“可为空”，但当前 profile 显示无空值，需要核对是否存在空字符串或导入转换差异。
- `fact_subscription.tenant_id` 当前样本唯一，这更像数据快照现象，不应直接上升为长期业务规则。
- `fact_subscription.end_date` 是否允许为空、`trial` 状态如何与时间窗共同解释，需要人工确认。
- `dim_plan.seat_limit` 混合了数字字符串与 `unlimited`，需要确认 Agent 是否允许做结构化解析。
- `dim_plan.features` 当前仅按文本/逗号分隔草稿处理，未声明为已验证结构化字段。
- `fact_daily_usage.feature_usage_json` 需要确认 JSON 键集合、value 语义以及与 `fact_feature_usage` 的口径关系。
- `fact_daily_usage` 只覆盖部分用户，不应被误读为全量用户日活表。

## 建议验证顺序

1. `dim_tenant`
2. `dim_user`
3. `dim_plan`
4. `fact_subscription`
5. `fact_daily_usage`

## 顺序说明

- 先验证 `dim_tenant` 和 `dim_user`，因为它们是后续多张表的核心维度与 JOIN 基础。
- 再验证 `dim_plan`，确认套餐 tier 和价格字段口径，为订阅解释做准备。
- 然后验证 `fact_subscription`，重点核对 MRR、时间窗和套餐关联。
- 最后验证 `fact_daily_usage`，因为它依赖用户维度，并且还涉及 JSON 字段与用户覆盖率判断。
