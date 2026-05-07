# Data Quality Notes · 数据质量与易错点

本文件用于提醒 Agent 生成 SQL 时规避常见错误。

## 1. 粒度不匹配

典型风险：
- fact_invoice 一张发票可能对应多条 fact_payment
- fact_subscription 一个订阅可能对应多个月 fact_actual_revenue
- dim_user_profile 一个 user_id 可能出现多条记录
- fact_experiment_metric 已经是聚合指标，不应和用户级 assignment 随意 JOIN 后重复聚合

规避方式：
- JOIN 前先确认两边表粒度
- 聚合前必要时先按主键预聚合
- 统计用户、企业、订阅时必要时使用 COUNT(DISTINCT ...)

## 2. 空字符串与 NULL

CSV 中空字符串 "" 代表 NULL。
部分字段中空字符串有业务含义：
- end_date 为空表示当前仍有效
- effective_to 为空表示当前仍有效

DuckDB 推荐：
- 使用 NULLIF(CAST(field AS VARCHAR), '') 将空字符串转 NULL
- 使用 COALESCE 处理当前有效期

## 3. 时间字段格式

大部分日期为 YYYY-MM-DD。
大部分时间戳为 YYYY-MM-DD HH:MM:SS。

特殊字段：
- fact_ai_usage_log.created_at 是 unix epoch 秒字符串

DuckDB 可使用：
- TO_TIMESTAMP(CAST(created_at AS BIGINT))

## 4. 金额口径不一致

不同金额字段含义不同：
- fact_subscription.mrr：MRR，确认收入口径
- dim_plan.monthly_price：标价，USD / seat
- fact_invoice.amount：发票金额，可能多币种
- fact_payment.amount：付款尝试金额
- fact_actual_revenue.actual_revenue：实际入账收入

原则：
- 问 MRR：用 fact_subscription.mrr
- 问实收/入账/折扣后收入：用 fact_actual_revenue.actual_revenue
- 问开票：用 fact_invoice.amount
- 问付款：用 fact_payment.amount，并过滤 success

## 5. 枚举值和类型不一致

plan_tier：
- dim_plan.plan_tier 是小写
- fact_subscription.plan_tier 是小写
- fact_nps_survey.plan_tier 是首字母大写

role：
- dim_user.role 是字符串
- fact_nps_survey.role 是 INT 编码

score：
- fact_nps_survey.score 是 STRING
- 未完成调研为 "N/A"

## 6. 累计口径与当日口径

fact_tenant_metrics_snapshot 中：
- total_users 是累计
- total_docs 是累计
- total_storage_mb 是累计
- total_revenue 是生命周期累计
- total_messages 是累计
- active_users_today 是当日口径

不要把 total_revenue 当作当日收入。
如需当日新增，需要对累计字段做差分。

## 7. 当前状态不是历史状态

dim_user.status 是当前状态。
不能直接作为历史留存或注册当月状态解释。

真正留存应结合：
- dim_user.register_at
- fact_daily_usage.dt
- fact_session.start_time
- fact_event_log.event_time

## 8. AI 使用日志 ID 映射

fact_ai_usage_log 没有明文 user_id，只有 user_id_hash。

正确 JOIN：
- fact_ai_usage_log.user_id_hash = dim_user_id_mapping.user_id_hash
- dim_user_id_mapping.user_id = dim_user.user_id

## 9. SQL 生成检查清单

Agent 生成 SQL 后应检查：
1. 是否选对主事实表？
2. 是否确认表粒度？
3. 是否存在一对多 JOIN 后重复聚合？
4. 是否需要 COUNT(DISTINCT ...)？
5. 时间字段是否正确 CAST？
6. 空字符串是否处理？
7. 金额口径是否符合问题？
8. 枚举大小写是否一致？
9. 查询结果为空时是否过滤条件过严？
10. 是否需要限制时间范围？
