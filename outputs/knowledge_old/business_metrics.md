# Business Metrics · 业务指标口径

本文件用于指导 Agent 在生成 SQL 前先判断指标口径。

## 1. 企业画像

推荐表：dim_tenant

常见指标：
- 企业数：COUNT(*)
- 按国家企业数：GROUP BY country
- 按行业企业数：GROUP BY industry
- 按规模企业数：GROUP BY size_tier

注意：
dim_tenant 粒度是一行一个企业，单表分析时可以直接 COUNT(*)。
如果 JOIN 到用户或事实表后统计企业数，需要 COUNT(DISTINCT tenant_id)。

## 2. 用户注册

推荐表：dim_user

常见指标：
- 新注册用户数：COUNT(*)
- 月度新注册用户：按 register_at 截取月份聚合
- 当前状态分布：按 status 聚合

注意：
dim_user.status 是当前状态，不是注册当月状态。
不能把当前 active 占比直接解释为注册当月留存率。

## 3. DAU / 活跃

推荐表：fact_daily_usage

常见口径：
- 有日度使用记录的 user_id 可视作当日活跃用户
- 更严格口径可使用 active_duration_sec > 0

示例逻辑：
按 dt 聚合 COUNT(DISTINCT user_id)。

## 4. 功能使用

推荐表：
- fact_feature_usage
- dim_feature

JOIN：
fact_feature_usage.feature_key = dim_feature.feature_key

常见指标：
- action_count 总和
- duration_sec 总和
- 使用某功能的用户数 COUNT(DISTINCT user_id)

注意：
fact_feature_usage.action_count 与 fact_daily_usage.feature_usage_json 的口径不完全相同。

## 5. MRR

推荐表：fact_subscription

字段：
- mrr

注意：
MRR 是月度经常性收入，不等同于发票金额、付款金额或实际入账收入。

## 6. 实收收入

推荐表：fact_actual_revenue

字段：
- list_revenue
- discount_rate
- coupon_amount
- actual_revenue

注意：
如果用户问“实收”“入账”“折扣后收入”“实际收入”，优先使用 fact_actual_revenue.actual_revenue。

## 7. 发票与付款

推荐表：
- fact_invoice
- fact_payment

注意：
fact_invoice.amount 是发票金额，可能多币种。
fact_payment.amount 是付款尝试金额。
一张发票可能对应多条付款记录，JOIN 后聚合要避免重复计算。
统计成功付款时需要 fact_payment.status = 'success'。

## 8. 客服工单

推荐表：
- fact_ticket
- dim_ticket_category
- fact_ticket_reply

常见指标：
- 工单数
- 各优先级工单数
- 各分类工单数
- 解决时长
- 回复次数

注意：
open / in_progress 的工单 resolved_at 可能为空。
计算解决时长时应过滤 resolved / closed。

## 9. 实验分析

推荐表：
- fact_experiment
- fact_experiment_metric

注意：
fact_experiment_metric 已经是聚合指标表。
不要随意和 fact_experiment_assignment JOIN 后重复聚合 metric_value。

## 10. NPS

推荐表：fact_nps_survey

注意：
score 是 STRING，未完成时为 "N/A"。
计算分数前需要过滤 is_completed = 1 且 score != 'N/A'。
role 是 INT 编码，不是 dim_user.role 的字符串。
plan_tier 是首字母大写，和 dim_plan.plan_tier 小写不一致。
