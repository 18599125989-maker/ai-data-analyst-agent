# Query Recipes · 查询经验库

本文件保存常见分析问题的推荐表、JOIN 路径、SQL 思路和注意事项。
Agent 在生成 SQL 前应优先检查这里是否有相似问题。

---

## Recipe 1：企业画像分布

适用问题：
- 各国家企业数量分布如何？
- 不同规模企业占比如何？
- 国家 × 行业 的企业结构如何？

推荐表：
- dim_tenant

表粒度：
- 一行 = 一个企业

是否需要 JOIN：
- 不需要

SQL 思路：
1. 按 country GROUP BY，统计 COUNT(*)
2. 按 size_tier GROUP BY，统计 COUNT(*) 和占比
3. 按 country, industry GROUP BY，生成二维分布

注意事项：
- 单表统计企业数可以 COUNT(*)
- 如果 JOIN 到用户表后统计企业数，要 COUNT(DISTINCT tenant_id)

---

## Recipe 2：月度新注册用户

适用问题：
- 每月新增用户数是多少？
- 用户注册趋势如何？
- 每月注册用户当前状态分布如何？

推荐表：
- dim_user

表粒度：
- 一行 = 一个用户

时间字段：
- register_at

SQL 思路：
1. 将 register_at 转为 TIMESTAMP
2. 使用 DATE_TRUNC('month', register_at) 截取月份
3. 按月份聚合 COUNT(*)
4. 使用 CASE WHEN 统计 active / inactive / suspended

注意事项：
- dim_user.status 是当前状态，不是注册当月状态
- 当前 active 占比不能直接解释为当月留存率
- 真正留存分析应结合 fact_daily_usage / fact_session / fact_event_log

---

## Recipe 3：各套餐月收入对比，MRR 口径

适用问题：
- 某个月各套餐 MRR 表现如何？
- 各套餐活跃订阅数是多少？
- 各套餐 MRR 和标价收入有什么差异？

推荐表：
- fact_subscription
- dim_plan

JOIN：
- fact_subscription.plan_tier = dim_plan.plan_tier

表粒度：
- fact_subscription：一行 = 一个订阅周期
- dim_plan：一行 = 一个套餐等级

活跃订阅过滤逻辑：
- status = 'active'
- start_date <= 月末
- end_date >= 月初，或 end_date 为空

注意事项：
- end_date 空字符串表示当前仍有效
- monthly_price = 0 的 free 套餐要避免除零
- MRR 不等于实收收入
- 如果用户问实收，应改用 fact_actual_revenue

---

## Recipe 4：各套餐月度实收收入

适用问题：
- 某个月各套餐实际入账收入是多少？
- 折扣和优惠券影响多少？
- list_revenue 和 actual_revenue 差多少？

推荐表：
- fact_actual_revenue
- fact_subscription
- dim_plan 可选

JOIN：
- fact_actual_revenue.sub_id = fact_subscription.sub_id
- fact_subscription.plan_tier = dim_plan.plan_tier

表粒度：
- fact_actual_revenue：一行 = 一个订阅一个月的实收

推荐指标：
- SUM(list_revenue)
- SUM(actual_revenue)
- SUM(list_revenue - actual_revenue)
- AVG(discount_rate)
- SUM(coupon_amount)

注意事项：
- 商业实收分析优先使用 actual_revenue
- 不要把 MRR、发票金额、付款金额和实际入账收入混用

---

## Recipe 5：AI 使用分析

适用问题：
- 哪些用户使用 AI 最多？
- 哪些租户 AI credits 消耗最高？
- 不同模型使用量如何？

推荐表：
- fact_ai_usage_log
- dim_user_id_mapping
- dim_user
- dim_tenant

JOIN：
- fact_ai_usage_log.user_id_hash = dim_user_id_mapping.user_id_hash
- dim_user_id_mapping.user_id = dim_user.user_id
- dim_user.tenant_id = dim_tenant.tenant_id

时间字段：
- fact_ai_usage_log.created_at 是 unix epoch 秒字符串

注意事项：
- 分析消耗时通常过滤 operation_type = 'deduct'
- 如果需要按租户分析，必须通过 mapping 表关联回 dim_user
- remark 中有更细的 token 和 cost 信息，但需要文本解析

---

## Recipe 6：实验效果分析

适用问题：
- 某个 AB 实验 treatment 是否优于 control？
- 不同实验分组的 retention / conversion / nps 是否有差异？

推荐表：
- fact_experiment
- fact_experiment_metric

JOIN：
- fact_experiment.exp_id = fact_experiment_metric.exp_id

推荐指标：
- metric_value
- sample_size
- ci_lower
- ci_upper

注意事项：
- fact_experiment_metric 已经是聚合指标表
- 不要随意 JOIN fact_experiment_assignment 后重复聚合 metric_value
- 判断实验效果时应关注样本量、均值差异和置信区间

---

## Recipe 7：客服工单分析

适用问题：
- 哪类工单最多？
- 高优先级工单解决情况如何？
- 不同租户工单量如何？

推荐表：
- fact_ticket
- dim_ticket_category
- fact_ticket_reply

JOIN：
- fact_ticket.category = dim_ticket_category.category_id
- fact_ticket.ticket_id = fact_ticket_reply.ticket_id

注意事项：
- 工单回复表是一对多，JOIN 后统计工单数要 COUNT(DISTINCT ticket_id)
- 解决时长需要过滤 resolved_at 不为空
