# Question 3 Insight：各套餐月收入对比

## 使用表
- fact_subscription
- dim_plan

## 使用字段
- fact_subscription.sub_id
- fact_subscription.plan_tier
- fact_subscription.start_date
- fact_subscription.end_date
- fact_subscription.status
- fact_subscription.mrr
- dim_plan.plan_tier
- dim_plan.monthly_price

## 主要结论
- 2025-10 当月 MRR 最高的套餐是 business，总 MRR 为 299296.0。
- 已按套餐统计活跃订阅数量、总 MRR、估算 seats 和 nominal revenue。

## 有效订阅口径
本题将满足以下条件的订阅视为 2025-10 当月有效订阅：
1. status = active
2. start_date <= 2025-10-31
3. end_date >= 2025-10-01；如果 end_date 为空，则视为仍然有效。

## 重要口径说明
- fact_subscription.mrr 是月度经常性收入，即确认收入口径。
- estimated_seats 是通过 mrr / monthly_price 反推得到，不是真实 seats 字段。
- nominal_revenue = estimated_seats * monthly_price，所以它通常会非常接近 total_mrr。
- 如果正式分析要求真实商业收入，应优先考虑 fact_actual_revenue.actual_revenue。
- 如果使用发票或付款表，还需要考虑折扣、优惠券、未付款、退款、多币种和账期错位等问题。
