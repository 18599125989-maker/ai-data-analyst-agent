# Question 1 Insight：企业画像分布

## 使用表
- dim_tenant

## 使用字段
- tenant_id
- country
- industry
- size_tier

## 主要结论
- 企业数量最多的国家是 US，共有 63 家企业。
- 占比最高的规模段是 large，占比 21.6%。
- 已生成国家 × 行业二维分布表，用于观察不同国家的行业结构差异。

## 口径说明
- dim_tenant 的粒度是一行一个企业。
- 因此本题可以直接使用 COUNT(*) 统计企业数量。
