# Manual Table Cards - Compact Agent Knowledge

本文件由 knowledge_manual/table_cards/*.yaml 自动编译生成。
它是给 Agent 检索和放入 prompt 的短版知识，不是人工源文件。

## Build Overview

- table_card_count: 3
- ready_for_agent_count: 3

## dim_department

- domain: 租户与组织
- status: validated
- ready_for_agent: True
- grain_validated: True
- dq_validated: True

### Prompt Summary
dim_department 是部门维度表，一行代表一个部门。dept_id 是主键； tenant_id 可关联 dim_tenant.tenant_id；dept_id 可关联 dim_user.dept_id； parent_dept_id 是自关联上级部门，空值表示顶级部门。 dept_name 是部门类型枚举，不是唯一键。 JOIN dim_user 后统计部门数应使用 COUNT(DISTINCT dim_department.dept_id)。 当前验证显示 dept_id 唯一，tenant_id 外键覆盖率 100%，dim_user.dept_id 覆盖率 100%。

### Grain
一行 = 一个部门。 每个部门属于一个 tenant。 一个 tenant 可以有多个部门。 parent_dept_id 表示上级部门，空值表示顶级部门。

### Primary Key
- dept_id

### Columns
- dept_id [primary_key]: 部门 ID，唯一标识一个部门。 通常用于统计部门数量，或作为 dim_user.dept_id 的关联键。
- tenant_id [foreign_key] -> dim_tenant.tenant_id: 部门所属企业 ID。 用于把部门关联到企业客户维度 dim_tenant。
- dept_name [dimension]: 部门名称或部门类型，例如 HR、Support、Sales、Design、Product、 Marketing、Finance、Engineering、Legal、Operations。 可用于按部门类型统计组织结构。
- parent_dept_id [self_reference_key] -> dim_department.dept_id: 上级部门 ID，用于表示部门层级关系。 为空表示该部门是顶级部门。

### Field Groups
- id_columns: dept_id, tenant_id, parent_dept_id
- time_columns: []
- metric_columns: []
- dimension_columns: tenant_id, dept_name, parent_dept_id
- enum_columns: dept_name
- json_columns: []
- text_columns: []

### Join Keys
- tenant_id -> dim_tenant.tenant_id; condition: dim_department.tenant_id = dim_tenant.tenant_id; risk: low; status: passed; note: 一个企业可以有多个部门。 本次验证中 unmatched_department_count = 0。 如果 JOIN 后统计企业数，需要使用 COUNT(DISTINCT dim_tenant.tenant_id)。
- dept_id -> dim_user.dept_id; condition: dim_department.dept_id = dim_user.dept_id; risk: low; status: passed; note: 一个部门可以有多个用户。 本次验证中 dim_user.dept_id 无空值，且全部可以关联到 dim_department.dept_id。 为兼容未来数据版本，用户部门分析仍建议从 dim_user LEFT JOIN dim_department。
- parent_dept_id -> dim_department.dept_id; condition: child_department.parent_dept_id = parent_department.dept_id; risk: medium; status: passed; note: parent_dept_id 为空表示顶级部门。 本次验证中 parent_dept_id 空值率为 0.7613， 非空 parent_dept_id 均可关联到有效 dept_id。 自 JOIN 时建议使用别名，例如 child_department 和 parent_department。

### Preferred Metrics
- department_count: COUNT(DISTINCT dept_id) | 部门数量 | 统计部门实体数量时使用。
- tenant_count: COUNT(DISTINCT tenant_id) | 企业数量 | 统计拥有相关部门的企业数量时使用。
- top_level_department_count: SUM(
  CASE
    WHEN parent_dept_id IS NULL OR CAST(parent_dept_id AS VARCHAR) = ''
    THEN 1 ELSE 0
  END
)
 | 顶级部门数量 | parent_dept_id 为空代表顶级部门。

### Known Traps
- Trap: dept_name 不是唯一部门 ID。  Consequence: 如果用 dept_name 统计唯一部门，会把不同企业下同名部门合并。  Prevention: 统计部门实体时使用 dept_id。
- Trap: parent_dept_id 空值率较高。  Consequence: 容易被误判为数据质量问题。  Prevention: 将 parent_dept_id 为空解释为顶级部门。 本次验证中空值率为 0.7613，且非空 parent_dept_id 均可自关联到有效 dept_id。
- Trap: dim_department JOIN dim_user 后直接 COUNT(*)。  Consequence: COUNT(*) 通常统计的是用户记录数，不是部门数。  Prevention: 统计部门数使用 COUNT(DISTINCT dim_department.dept_id)。
- Trap: 使用 INNER JOIN 分析用户部门时可能降低未来数据版本的鲁棒性。  Consequence: 当前验证中 dim_user.dept_id 无空值且全部可关联 dim_department。 但未来数据版本若出现空值或无法匹配，INNER JOIN 会丢失用户。  Prevention: 默认从 dim_user LEFT JOIN dim_department， 并在报告中说明当前数据版本 join coverage 为 100%。

### SQL Pattern Names
- department_count_by_tenant
- department_name_distribution
- top_level_vs_child_departments
- user_count_by_department_name
- department_count_by_tenant_profile
- department_hierarchy_self_join

---

## fact_actual_revenue

- domain: 计费与订阅
- status: validated_with_warnings
- ready_for_agent: True
- grain_validated: partial
- dq_validated: partial

### Prompt Summary
fact_actual_revenue 是月度实收收入事实表。revenue_id 是唯一物理主键。 actual_revenue 是实际入账收入；list_revenue 是标价收入； revenue_gap 可用 list_revenue - actual_revenue 计算。 如果用户问实收、实际收入、入账收入，应优先使用 actual_revenue。 如果按套餐分析，需要通过 sub_id JOIN fact_subscription 获取 plan_tier。 不要将 actual_revenue 与 MRR、发票金额或付款金额混用。 当前验证发现 tenant_id + sub_id + month 有 15 组重复，但这些不是脏重复； 它们是同一订阅同月下不同 discount_rate / actual_revenue 的收入明细。 因此 Agent 不能删除这些记录，也不能只取一条； 订阅月份级和月度收入分析必须 SUM(actual_revenue)。 当前验证还发现 actual_revenue 公式有极小相对偏差， 因此不要重新计算 actual_revenue 替代表字段。

### Grain
物理粒度：一行 = 一条 revenue_id 标识的收入记录。 业务分析时可以按 month、tenant_id + month、plan_tier + month、 tenant_id + sub_id + month 等维度聚合。 当前验证和人工复查显示，tenant_id + sub_id + month 存在 15 组重复。 这些重复组不是完全重复行，而是同一租户、同一订阅、同一月份下， discount_rate 和 actual_revenue 不同，少数组 coupon_amount 也不同的多条收入明细。 因此订阅月份级收入必须先 GROUP BY tenant_id, sub_id, month 后 SUM(actual_revenue)， 不能只取单条记录。

### Primary Key
- revenue_id

### Natural Key
- tenant_id
- sub_id
- month

- natural_key_status: {"is_unique": false, "tested_key": ["tenant_id", "sub_id", "month"], "duplicated_group_count": 15, "exact_duplicate_row_count": 0, "duplicate_interpretation": "tenant_id + sub_id + month 存在 15 组重复。 人工复查显示这些重复组不是完全重复行。 重复组中 seats、list_price_per_seat、list_revenue 通常相同， 但 discount_rate 和 actual_revenue 不同，少数组 coupon_amount 也不同。 因此这些记录更像同一订阅同月下的多条折扣/优惠收入明细，不应删除。\n", "required_agent_behavior": "当问题要求订阅月份级、租户月份级、套餐月份级或总收入级分析时， 必须 SUM(actual_revenue) 聚合 revenue_id 级记录。\n"}

### Columns
- revenue_id [primary_key]: 实收收入记录 ID，唯一标识一条月度收入记录。 当前验证显示 revenue_id 唯一，可作为该表物理主键。
- tenant_id [foreign_key] -> dim_tenant.tenant_id: 企业客户 ID，表示该收入记录所属的租户。 可用于按企业、国家、行业、规模等维度分析收入。
- sub_id [foreign_key] -> fact_subscription.sub_id: 订阅 ID，表示该收入记录对应的订阅周期。 可用于关联 fact_subscription 获取 plan_tier、订阅状态、订阅起止日期等信息。
- month [time_period]: 收入所属月份，格式为 YYYY-MM。 例如 2025-10 表示 2025 年 10 月的收入。
- seats [metric]: 当月席位数，表示该收入记录对应的席位数量。
- list_price_per_seat [metric] -> dim_plan.monthly_price: 标准单席位价格，单位为 USD / seat。 表示未考虑折扣和优惠券前的标准单价。
- list_revenue [metric]: 标价收入，通常表示折扣和优惠券扣减前的收入。
- discount_rate [metric]: 折扣率，为小数比例。 例如 0.15 表示 15% 折扣。
- coupon_amount [metric]: 优惠券扣减金额，单位为 USD。 表示除比例折扣外的固定金额扣减。
- actual_revenue [metric]: 实际入账收入，是商业实收分析时最优先使用的收入字段。 当前人工复查显示，actual_revenue 与 list_revenue * (1 - discount_rate) - coupon_amount 的平均绝对偏差为 0.3144，最大绝对偏差为 4.3867，平均相对偏差为 0.00003。 偏差相对极小，通常可解释为四舍五入、模拟数据噪声或精度处理差异。
- currency [dimension]: 币种字段。 当前验证显示本表 currency 仅包含 USD。

### Field Groups
- id_columns: revenue_id, tenant_id, sub_id
- time_columns: month
- metric_columns: seats, list_price_per_seat, list_revenue, discount_rate, coupon_amount, actual_revenue
- dimension_columns: tenant_id, sub_id, month, currency
- enum_columns: currency
- json_columns: []
- text_columns: []

### Join Keys
- tenant_id -> dim_tenant.tenant_id; condition: fact_actual_revenue.tenant_id = dim_tenant.tenant_id; risk: low; status: passed; note: 用于按国家、行业、规模、企业等维度分析实收收入。 当前验证中 unmatched_revenue_tenant_count = 0。 由于 fact_actual_revenue 是收入记录粒度，JOIN dim_tenant 后聚合收入通常是安全的。
- sub_id -> fact_subscription.sub_id; condition: fact_actual_revenue.sub_id = fact_subscription.sub_id; risk: medium; status: passed_with_grain_warning; note: 当前验证中 unmatched_revenue_sub_count = 0。 JOIN fact_subscription 后按套餐聚合收入是常见用法。 但 sub_id + month 存在 15 组重复，按订阅月份分析时应显式聚合。 如果再 JOIN invoice 或 payment，需要额外注意一对多导致重复计算。
- sub_id -> fact_invoice.sub_id; condition: fact_actual_revenue.sub_id = fact_invoice.sub_id; risk: high; status: not_recommended_by_default; note: 通常不建议直接用 fact_actual_revenue JOIN fact_invoice 后聚合金额。 fact_invoice 是发票粒度，fact_actual_revenue 是月度收入记录粒度， 直接 JOIN 可能造成金额重复或账期错位。 除非问题明确要求对比实收和发票，否则不要默认关联 invoice。

### Preferred Metrics
- actual_revenue: SUM(actual_revenue) | 实际入账收入 | 用户问实收、实际收入、入账收入时优先使用。
- list_revenue: SUM(list_revenue) | 标价收入 | 用于和 actual_revenue 对比，衡量折扣和优惠券影响。
- revenue_gap: SUM(list_revenue - actual_revenue) | 标价收入与实际收入差额 | 可解释为折扣和优惠券导致的收入让利金额。 应直接使用 list_revenue - actual_revenue 的字段差额， 不要重新计算 actual_revenue 后再求差。
- coupon_amount: SUM(coupon_amount) | 优惠券扣减金额 | 用于分析固定优惠券金额对收入的影响。
- avg_discount_rate: AVG(discount_rate) | 平均折扣率 | 简单平均折扣率不一定等于收入加权折扣率。 如需严谨分析，可使用 list_revenue 加权折扣率。
- weighted_discount_rate: SUM(list_revenue * discount_rate) / NULLIF(SUM(list_revenue), 0) | 收入加权折扣率 | 比 AVG(discount_rate) 更适合表达收入加权折扣水平。
- total_seats: SUM(seats) | 总席位数 | 用于分析席位规模与收入之间的关系。 如果重复 sub_id + month 表示拆分收入记录，SUM(seats) 可能需要结合业务语义谨慎解释。

### Agent Usage Policy
- can_use_for_revenue_analysis: True
- ready_for_agent: True
- use_revenue_id_as_physical_key: True
- tenant_sub_month_requires_aggregation: True
- monthly_revenue_should_sum_records: True
- subscription_month_revenue_should_sum_records: True
- actual_revenue_should_be_summed_not_selected_single_row: True
- must_use_actual_revenue_field_for_actual_income: True
- must_not_recalculate_actual_revenue_as_replacement: True
- formula_is_explanatory_only: True
- must_not_drop_duplicate_tenant_sub_month_records: True
- avoid_direct_join_to_invoice_or_payment_by_default: True

### Known Traps
- Trap: 将 actual_revenue 与 MRR 混用。  Consequence: MRR 是 fact_subscription.mrr 的确认收入口径， actual_revenue 是扣除折扣和优惠券后的实际入账收入。 二者不能直接互相替代。  Prevention: 问 MRR 时使用 fact_subscription.mrr； 问实收、实际入账、折扣后收入时使用 fact_actual_revenue.actual_revenue。
- Trap: 将 actual_revenue 与 invoice amount 混用。  Consequence: fact_invoice.amount 是发票金额，可能存在未支付、作废、逾期等状态。 actual_revenue 是月度实收收入口径。  Prevention: 问开票金额时使用 fact_invoice.amount； 问实际收入时使用 fact_actual_revenue.actual_revenue。
- Trap: 将 tenant_id + sub_id + month 当作严格唯一键。  Consequence: 当前验证发现 tenant_id + sub_id + month 存在 15 组重复。 如果 Agent 只取其中一条记录，会低估该订阅月份的收入。  Prevention: 使用 revenue_id 作为物理主键。 如果需要订阅月份级收入，应 GROUP BY tenant_id, sub_id, month， 并 SUM(actual_revenue)、SUM(list_revenue)、SUM(list_revenue - actual_revenue)。
- Trap: 把重复的 tenant_id + sub_id + month 当作脏数据删除。  Consequence: 人工复查显示这些记录不是完全重复行。 它们通常 discount_rate 和 actual_revenue 不同，少数组 coupon_amount 也不同。 删除会丢失真实收入明细。  Prevention: 不删除这些记录。 收入分析时聚合 revenue_id 级收入记录。
- Trap: 强假设 actual_revenue = list_revenue * (1 - discount_rate) - coupon_amount 完全精确。  Consequence: 当前验证发现 954 条记录公式偏差超过 0.05。 但人工复查显示平均相对偏差约为 0.00003，属于极小偏差。 若 Agent 重新计算 actual_revenue，可能和表中实际字段不一致。  Prevention: 分析实际收入时直接使用 actual_revenue 字段。 公式只作为近似解释，不作为强校验或替代字段。
- Trap: 直接 JOIN fact_invoice 或 fact_payment 后聚合收入。  Consequence: invoice 和 payment 粒度与 actual_revenue 不同， 可能造成重复计算或账期错位。  Prevention: 除非问题明确要求发票或付款对比，否则收入分析优先只使用 fact_actual_revenue， 或先对 invoice/payment 预聚合后再 JOIN。
- Trap: 忽略 month 是字符串。  Consequence: 如果使用日期函数时未正确转换，可能导致 SQL 报错。  Prevention: 月度趋势可直接 GROUP BY month； 若需要日期运算，再考虑将 month || '-01' 转换为 DATE。
- Trap: 按套餐分析时忘记 JOIN fact_subscription。  Consequence: fact_actual_revenue 本身没有 plan_tier 字段，无法直接按套餐聚合。  Prevention: 通过 fact_actual_revenue.sub_id = fact_subscription.sub_id 获取 plan_tier。

### SQL Pattern Names
- monthly_actual_revenue_trend
- actual_revenue_by_plan_month
- discount_impact_by_plan_month
- actual_revenue_by_tenant_profile
- subscription_month_level_revenue

---

## fact_ai_usage_log

- domain: 产品使用
- status: validated
- ready_for_agent: True
- grain_validated: True
- dq_validated: True

### Prompt Summary
fact_ai_usage_log 是 AI 调用日志表，一行代表一次 AI credits 操作。 log_id 是主键。user_id_hash 是哈希用户 ID，不能直接 JOIN dim_user.user_id； 必须先通过 dim_user_id_mapping.user_id_hash 映射到明文 user_id。 created_at 是 Unix epoch 秒字符串，需要用 TO_TIMESTAMP(CAST(created_at AS BIGINT)) 转换。 分析 AI 使用/消耗时只统计 operation_type = 'deduct'，并 SUM(credits_amount)。 分析 credits 流水时，deduct 记为负，earn/refund 记为正。 model_name 可用于模型使用分布分析。 2026-04 是截断月份，完整月度趋势优先使用 2025-04 到 2026-03。 remark 可解析，但默认不作为主分析字段；只有用户明确询问 token、cost、session_id 时再解析。

### Grain
一行 = 一次 AI credits 操作或 AI 使用日志。 log_id 唯一标识一条 AI 使用日志。 operation_type 表示本次 credits 是扣减、赠送还是退款。 当前验证显示 log_id 唯一，因此“一行 = 一次 AI 操作”的粒度成立。

### Primary Key
- log_id

### Columns
- log_id [primary_key]: AI 使用日志 ID，唯一标识一次 AI credits 操作。
- user_id_hash [hashed_foreign_key] -> dim_user_id_mapping.user_id_hash: 哈希形式的用户 ID。 该字段不是 dim_user.user_id，不能直接与 dim_user 关联。 必须先通过 dim_user_id_mapping.user_id_hash 映射回明文 user_id。
- operation_type [dimension]: AI credits 操作类型。 当前验证显示仅有 deduct、earn、refund 三种取值。 deduct 表示 credits 扣减，对应 AI 使用/消耗； earn 表示获得 credits； refund 表示 credits 退还。
- credits_amount [metric]: credits 数量，表示本次 AI 操作涉及的积分数量。 对 deduct 操作而言，代表消耗的 credits。 对 earn 或 refund 操作而言，代表获得或退还的 credits。
- model_name [dimension]: AI 模型名称。 当前验证显示共有五种模型：gpt-4o、claude-3.5、moonshot-v1、glm-4、doubao-pro。 可用于分析不同模型的调用次数和 credits 消耗。
- created_at [unix_epoch_seconds]: AI 操作发生时间。 该字段以 Unix epoch 秒形式存储为字符串，例如 "1758470400"。 它不是普通的 YYYY-MM-DD HH:MM:SS 时间戳。
- remark [json_text]: AI 操作备注字段，当前验证显示所有 remark 均非空且可以解析为 JSON。 典型结构包含 action、model、cause、session_id。 cause 中可能包含 input token、output token、cost 等文本信息。

### Field Groups
- id_columns: log_id, user_id_hash
- time_columns: created_at
- metric_columns: credits_amount
- dimension_columns: user_id_hash, operation_type, model_name
- enum_columns: operation_type, model_name
- json_columns: remark
- text_columns: remark

### Join Keys
- user_id_hash -> dim_user_id_mapping.user_id_hash; condition: fact_ai_usage_log.user_id_hash = dim_user_id_mapping.user_id_hash; risk: low; status: passed; note: fact_ai_usage_log 不包含明文 user_id。 如需关联 dim_user、dim_tenant 或其他用户行为表，必须先 JOIN dim_user_id_mapping。 当前验证显示 user_id_hash 映射覆盖率为 100%。
- user_id_hash -> dim_user.user_id; condition: fact_ai_usage_log.user_id_hash = dim_user.user_id; risk: high; status: invalid_by_design; note: 这是错误 JOIN 路径。 user_id_hash 不是明文 user_id，不能直接关联 dim_user.user_id。 正确路径是先关联 dim_user_id_mapping。

### Derived Join Paths
- ai_log_to_user: fact_ai_usage_log.user_id_hash = dim_user_id_mapping.user_id_hash -> dim_user_id_mapping.user_id = dim_user.user_id; risk: low; purpose: 将 AI 使用日志关联到用户维度。
- ai_log_to_tenant: fact_ai_usage_log.user_id_hash = dim_user_id_mapping.user_id_hash -> dim_user_id_mapping.user_id = dim_user.user_id -> dim_user.tenant_id = dim_tenant.tenant_id; risk: low; purpose: 将 AI 使用日志关联到企业租户维度。
- ai_log_to_daily_usage: fact_ai_usage_log.user_id_hash = dim_user_id_mapping.user_id_hash -> dim_user_id_mapping.user_id = fact_daily_usage.user_id; risk: high; purpose: 对比 AI 使用用户与日活、使用时长等行为指标。

### Preferred Metrics
- ai_operation_count: COUNT(DISTINCT log_id) | AI 操作次数 | 所有 operation_type 都可计入操作次数。 如果只分析真实 AI 使用调用，建议过滤 operation_type = 'deduct'。
- ai_deduct_count: COUNT(DISTINCT CASE WHEN operation_type = 'deduct' THEN log_id END) | AI 扣减操作次数 | 更接近真实 AI 使用调用次数。
- credits_consumed: SUM(CASE WHEN operation_type = 'deduct' THEN credits_amount ELSE 0 END) | AI credits 消耗量 | 分析 AI 使用/消耗时优先使用该口径。
- credits_earned: SUM(CASE WHEN operation_type = 'earn' THEN credits_amount ELSE 0 END) | AI credits 获得量 | 用于分析赠送或获得的 credits。
- credits_refunded: SUM(CASE WHEN operation_type = 'refund' THEN credits_amount ELSE 0 END) | AI credits 退还量 | 用于分析退还 credits。
- net_credits_change: SUM(
  CASE
    WHEN operation_type IN ('earn', 'refund') THEN credits_amount
    WHEN operation_type = 'deduct' THEN -credits_amount
    ELSE 0
  END
)
 | credits 净变化 | 该指标用于 credits 流水变化，不等同于 AI 消耗量。 人工确认口径：deduct 记为负，earn/refund 记为正。

### Agent Usage Policy
- can_use_for_ai_usage_analysis: True
- ready_for_agent: True
- use_log_id_as_physical_key: True
- use_deduct_for_ai_consumption: True
- net_credits_change_rule: deduct 记为负，earn/refund 记为正。

- must_use_mapping_table_before_joining_user: True
- must_not_direct_join_user_id_hash_to_dim_user: True
- convert_created_at_from_unix_seconds: True
- treat_2026_04_as_partial_month: True
- use_complete_month_range_for_trends: 2025-04 to 2026-03
- do_not_parse_remark_by_default: True
- parse_remark_only_when_user_asks_token_cost_or_session: True

### Known Traps
- Trap: 直接将 user_id_hash 与 dim_user.user_id JOIN。  Consequence: user_id_hash 是哈希后的用户 ID，不等于明文 user_id。 直接 JOIN 会导致匹配失败或错误结果。  Prevention: 必须先 JOIN dim_user_id_mapping，再关联 dim_user。
- Trap: 忘记过滤 operation_type。  Consequence: deduct、earn、refund 混在一起会导致 credits_amount 含义不清。  Prevention: 分析 AI 使用/消耗时过滤 operation_type = 'deduct'。 分析 credits 流水时用 CASE 表达不同操作的正负方向。
- Trap: 把 deduct、earn、refund 都当作消耗。  Consequence: 会高估 AI 使用消耗。  Prevention: 只有 deduct 代表 AI credits 消耗。 earn/refund 是 credits 增加或返还，不应计入消耗。
- Trap: 把 created_at 当作普通时间戳。  Consequence: created_at 是 Unix epoch 秒字符串，直接 DATE_TRUNC 可能报错或结果错误。  Prevention: 使用 TO_TIMESTAMP(CAST(created_at AS BIGINT)) 转换后再做日期或月份分析。
- Trap: 将 2026-04 当作完整月份参与趋势比较。  Consequence: 当前数据中 2026-04 只有少量边界数据，直接与完整月份比较会低估该月 AI 使用。  Prevention: 完整月度趋势优先使用 2025-04 到 2026-03。 如果展示 2026-04，必须注明它是截断月份。
- Trap: 和 fact_daily_usage 直接按 user_id JOIN，不加日期条件。  Consequence: 会产生用户级多对多膨胀，导致 AI 使用与日活指标重复计算。  Prevention: 先将 AI 日志 created_at 转为日期，再与 fact_daily_usage.dt 对齐。
- Trap: 默认解析 remark 中的 cost 或 token。  Consequence: remark 虽然是 JSON，但 cost/token 信息嵌在 cause 文本中。 默认解析可能增加复杂度并引入不稳定规则。  Prevention: 第一版默认使用 credits_amount。 只有用户明确询问 token、cost、session_id 时，再解析 remark。

### SQL Pattern Names
- ai_credits_by_model
- daily_ai_credits_trend
- monthly_ai_credits_trend_complete_months
- monthly_ai_credits_trend_all_months
- credits_flow_by_operation_type
- ai_credits_by_tenant
- ai_credits_by_tenant_profile
- ai_user_activity_comparison_daily

---
