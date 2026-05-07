# Agent Schema Context · CloudWork 数据知识库

本文件用于给 AI Data Analyst Agent 提供生成 SQL 前的核心上下文。
内容包括：表语义、表粒度、主键、字段 profile、常见用途、JOIN 摘要和数据质量提醒。

## 1. 数据集概览

- 数据集：CloudWork
- 场景：模拟 SaaS 协作办公产品数据仓库
- 目标：支持自然语言数据分析、Text-to-SQL、多表 JOIN、错误修复和业务洞察生成
- 当前 profile 中发现表数量：37

## 2. 业务域与表清单

### 产品使用
- `dim_feature`：功能维度表，描述功能 key、功能名、模块、类别和上线时间。
- `fact_ai_usage_log`：AI 调用日志表，记录 AI credits 的扣减、赠送、退款等操作。
- `fact_daily_usage`：用户日度聚合使用表，包含会话数、活跃时长、功能使用 JSON。
- `fact_event_log`：埋点事件流水表，记录用户行为事件。
- `fact_feature_usage`：功能使用明细表，记录用户每天对某功能的动作次数和使用时长。
- `fact_page_view`：页面浏览表，记录一次页面访问。
- `fact_session`：会话表，记录用户一次登录到退出的会话。

### 协作与内容
- `dim_channel`：频道/群聊维度表。
- `fact_doc_collaboration`：文档协作明细表，记录用户每天对某文档的编辑和评论。
- `fact_document`：文档表，记录文档创建者、租户、类型、创建和编辑信息。
- `fact_message`：消息表，记录用户在频道中发送的一条消息。

### 增长与实验
- `fact_campaign`：营销活动表，记录渠道、预算、目标人群和起止日期。
- `fact_campaign_attribution`：营销归因触点表，记录用户对营销活动的一次触达。
- `fact_experiment`：AB 实验表，记录实验名称、关联功能、起止日期和状态。
- `fact_experiment_assignment`：实验分流表，记录用户在实验中的分组。
- `fact_experiment_metric`：实验指标日度表，记录实验分组在某天的指标、样本量和置信区间。
- `fact_nps_survey`：NPS 调研表，记录用户评分、角色、套餐和反馈文本。

### 客服工单
- `dim_ticket_category`：工单分类维度表，包含两级分类和 SLA 小时数。
- `fact_ticket`：工单表，记录租户提交的客服问题、优先级、状态、创建和解决时间。
- `fact_ticket_reply`：工单回复表，记录客服、客户或机器人的回复。

### 用户
- `dim_user`：用户维度表，描述用户所属租户、部门、角色、状态、注册和活跃时间。
- `dim_user_id_mapping`：明文 user_id 与哈希 user_id 的映射表，用于关联 AI 使用日志。
- `dim_user_profile`：用户画像与客户端信息表，包含语言、时区、设备、版本。
- `dim_user_role_history`：用户角色变更流水表。
- `fact_user_activation`：用户激活里程碑表，记录首次登录、首次创建文档、首次发消息等行为。

### 租户与组织
- `dim_department`：部门维度表，描述租户下的部门及上下级关系。
- `dim_org_structure`：组织节点表，描述部门、团队、虚拟组等组织结构。
- `dim_tenant`：企业客户维度表，描述企业行业、规模、国家和创建时间。
- `dim_tenant_config`：租户功能开关与配置表。
- `dim_tenant_plan`：租户套餐历史表，记录企业在不同时间区间使用的套餐。
- `fact_tenant_metrics_snapshot`：租户指标快照表，包含用户、文档、收入、消息等累计或当日指标。

### 计费与订阅
- `dim_plan`：套餐维度表，描述套餐等级、标价、席位上限和功能。
- `fact_actual_revenue`：月度实收收入表，记录订阅每月席位、标价收入、折扣、优惠券和实际入账收入。
- `fact_credit_usage`：资源消耗流水表，记录租户每天对某资源的消耗。
- `fact_invoice`：发票表，记录发票金额、币种、开票时间、付款时间和状态。
- `fact_payment`：付款流水表，记录一次付款尝试。
- `fact_subscription`：订阅表，记录租户订阅周期、套餐、MRR 和状态。

## 3. 逐表 Schema 与 Profile

### `dim_channel`

- 业务域：协作与内容
- 业务含义：频道/群聊维度表。
- 数据粒度：一行 = 一个频道
- 主键 / 推荐唯一键：channel_id
- 行数：5591
- 字段数：5

| 字段 | 类型 | 空值率 | 唯一值数 | 唯一率 | 可能主键 | 样例值 |
|---|---|---:|---:|---:|---|---|
| `channel_id` | VARCHAR | 0.0 | 5591 | 1.0 | True | CH00179_02, CH00179_08, CH00335_02 |
| `tenant_id` | VARCHAR | 0.0 | 500 | 0.0894 | False | T00361, T00292, T00166 |
| `channel_type` | VARCHAR | 0.0 | 4 | 0.0007 | False | topic, group, announcement |
| `member_count` | BIGINT | 0.0 | 199 | 0.0356 | False | 150, 159, 89 |
| `created_at` | TIMESTAMP | 0.0 | 5591 | 1.0 | True | 2025-12-28 17:27:55, 2024-12-06 00:37:31, 2025-09-23 05:28:23 |

### `dim_department`

- 业务域：租户与组织
- 业务含义：部门维度表，描述租户下的部门及上下级关系。
- 数据粒度：一行 = 一个部门
- 主键 / 推荐唯一键：dept_id
- 行数：2937
- 字段数：4

| 字段 | 类型 | 空值率 | 唯一值数 | 唯一率 | 可能主键 | 样例值 |
|---|---|---:|---:|---:|---|---|
| `dept_id` | VARCHAR | 0.0 | 2937 | 1.0 | True | D00003_01, D00007_01, D00009_01 |
| `tenant_id` | VARCHAR | 0.0 | 500 | 0.1702 | False | T00018, T00036, T00041 |
| `dept_name` | VARCHAR | 0.0 | 10 | 0.0034 | False | Product, Operations, Sales |
| `parent_dept_id` | VARCHAR | 0.7613 | 599 | 0.2039 | False | D00002_03, D00009_07, D00023_01 |

### `dim_feature`

- 业务域：产品使用
- 业务含义：功能维度表，描述功能 key、功能名、模块、类别和上线时间。
- 数据粒度：一行 = 一个功能
- 主键 / 推荐唯一键：feature_key
- 行数：12
- 字段数：5

| 字段 | 类型 | 空值率 | 唯一值数 | 唯一率 | 可能主键 | 样例值 |
|---|---|---:|---:|---:|---|---|
| `feature_key` | VARCHAR | 0.0 | 12 | 1.0 | True | video_conf, docs, calendar |
| `feature_name` | VARCHAR | 0.0 | 12 | 1.0 | True | Instant Messaging, Approval, Spreadsheets |
| `module` | VARCHAR | 0.0 | 4 | 0.3333 | False | Communication, Workflow, Productivity |
| `category` | VARCHAR | 0.0 | 3 | 0.25 | False | advanced, core, premium |
| `launched_at` | DATE | 0.0 | 12 | 1.0 | True | 2025-01-05, 2025-01-26, 2024-02-04 |

### `dim_org_structure`

- 业务域：租户与组织
- 业务含义：组织节点表，描述部门、团队、虚拟组等组织结构。
- 数据粒度：一行 = 一个组织节点
- 主键 / 推荐唯一键：node_id
- 行数：1901
- 字段数：5

| 字段 | 类型 | 空值率 | 唯一值数 | 唯一率 | 可能主键 | 样例值 |
|---|---|---:|---:|---:|---|---|
| `node_id` | VARCHAR | 0.0 | 1901 | 1.0 | True | N00001_02, N00001_07, N00004_08 |
| `parent_node_id` | VARCHAR | 0.1052 | 953 | 0.5013 | False | N00002_05, N00003_02, N00004_06 |
| `tenant_id` | VARCHAR | 0.0 | 200 | 0.1052 | False | T00002, T00031, T00033 |
| `node_type` | VARCHAR | 0.0 | 3 | 0.0016 | False | virtual_group, department, team |
| `level` | BIGINT | 0.0 | 6 | 0.0032 | False | 5, 1, 2 |

### `dim_plan`

- 业务域：计费与订阅
- 业务含义：套餐维度表，描述套餐等级、标价、席位上限和功能。
- 数据粒度：一行 = 一个套餐等级
- 主键 / 推荐唯一键：plan_tier
- 行数：5
- 字段数：6

| 字段 | 类型 | 空值率 | 唯一值数 | 唯一率 | 可能主键 | 样例值 |
|---|---|---:|---:|---:|---|---|
| `plan_name` | VARCHAR | 0.0 | 5 | 1.0 | True | Professional, Enterprise, Starter |
| `plan_tier` | VARCHAR | 0.0 | 5 | 1.0 | True | business, free, enterprise |
| `monthly_price` | BIGINT | 0.0 | 5 | 1.0 | True | 79, 29, 199 |
| `annual_price` | BIGINT | 0.0 | 5 | 1.0 | True | 0, 4990, 290 |
| `seat_limit` | VARCHAR | 0.0 | 5 | 1.0 | True | 20, 5, 100 |
| `features` | VARCHAR | 0.0 | 5 | 1.0 | True | im,docs,sheets,calendar,video_conf,task, im,docs, im,docs,sheets,calendar,video_conf,task,base,wiki,approval |

### `dim_tenant`

- 业务域：租户与组织
- 业务含义：企业客户维度表，描述企业行业、规模、国家和创建时间。
- 数据粒度：一行 = 一个企业客户
- 主键 / 推荐唯一键：tenant_id
- 行数：500
- 字段数：6

| 字段 | 类型 | 空值率 | 唯一值数 | 唯一率 | 可能主键 | 样例值 |
|---|---|---:|---:|---:|---|---|
| `tenant_id` | VARCHAR | 0.0 | 500 | 1.0 | True | T00001, T00010, T00013 |
| `name` | VARCHAR | 0.0 | 500 | 1.0 | True | Company_3, Company_9, Company_15 |
| `industry` | VARCHAR | 0.0 | 15 | 0.03 | False | Nonprofit, Media, Legal |
| `size_tier` | VARCHAR | 0.0 | 5 | 0.01 | False | enterprise, small, startup |
| `country` | VARCHAR | 0.0 | 10 | 0.02 | False | AU, JP, US |
| `created_at` | TIMESTAMP | 0.0 | 500 | 1.0 | True | 2024-05-13 04:47:04, 2025-01-03 03:42:38, 2025-05-14 12:13:33 |

### `dim_tenant_config`

- 业务域：租户与组织
- 业务含义：租户功能开关与配置表。
- 数据粒度：一行 = 一个租户对一个功能的配置
- 主键 / 推荐唯一键：tenant_id, feature_key
- 行数：6000
- 字段数：4

| 字段 | 类型 | 空值率 | 唯一值数 | 唯一率 | 可能主键 | 样例值 |
|---|---|---:|---:|---:|---|---|
| `tenant_id` | VARCHAR | 0.0 | 500 | 0.0833 | False | T00018, T00036, T00041 |
| `feature_key` | VARCHAR | 0.0 | 12 | 0.002 | False | video_conf, email, wiki |
| `enabled` | BIGINT | 0.0 | 2 | 0.0003 | False | 0, 1 |
| `config_json` | VARCHAR | 0.8333 | 7 | 0.0012 | False | {"max_participants": 500}, {"max_participants": 100}, {"max_participants": 50} |

### `dim_tenant_plan`

- 业务域：租户与组织
- 业务含义：租户套餐历史表，记录企业在不同时间区间使用的套餐。
- 数据粒度：一行 = 一个租户在一个时间区间的套餐记录
- 主键 / 推荐唯一键：tenant_id, effective_from
- 行数：2320
- 字段数：5

| 字段 | 类型 | 空值率 | 唯一值数 | 唯一率 | 可能主键 | 样例值 |
|---|---|---:|---:|---:|---|---|
| `tenant_id` | VARCHAR | 0.0 | 500 | 0.2155 | False | T00002, T00031, T00033 |
| `plan_name` | VARCHAR | 0.0 | 5 | 0.0022 | False | Enterprise, Starter, Business |
| `plan_tier` | VARCHAR | 0.0 | 5 | 0.0022 | False | free, starter, pro |
| `effective_from` | DATE | 0.0 | 661 | 0.2849 | False | 2025-10-26, 2026-03-28, 2024-12-23 |
| `effective_to` | DATE | 0.2134 | 545 | 0.2349 | False | 2026-01-24, 2025-06-29, 2024-12-21 |

### `dim_ticket_category`

- 业务域：客服工单
- 业务含义：工单分类维度表，包含两级分类和 SLA 小时数。
- 数据粒度：一行 = 一个工单分类
- 主键 / 推荐唯一键：category_id
- 行数：12
- 字段数：4

| 字段 | 类型 | 空值率 | 唯一值数 | 唯一率 | 可能主键 | 样例值 |
|---|---|---:|---:|---:|---|---|
| `category_id` | VARCHAR | 0.0 | 12 | 1.0 | True | bug, security, bug_crash |
| `category_name` | VARCHAR | 0.0 | 12 | 1.0 | True | Security Concerns, Permission Issues, Invoice Questions |
| `parent_category_id` | VARCHAR | 0.5 | 3 | 0.25 | False | billing, bug, account |
| `sla_hours` | BIGINT | 0.0 | 6 | 0.5 | False | 4, 48, 8 |

### `dim_user`

- 业务域：用户
- 业务含义：用户维度表，描述用户所属租户、部门、角色、状态、注册和活跃时间。
- 数据粒度：一行 = 一个用户
- 主键 / 推荐唯一键：user_id
- 行数：50000
- 字段数：7

| 字段 | 类型 | 空值率 | 唯一值数 | 唯一率 | 可能主键 | 样例值 |
|---|---|---:|---:|---:|---|---|
| `user_id` | VARCHAR | 0.0 | 50000 | 1.0 | True | U000004, U000007, U000013 |
| `tenant_id` | VARCHAR | 0.0 | 500 | 0.01 | False | T00002, T00031, T00033 |
| `dept_id` | VARCHAR | 0.0 | 2801 | 0.056 | False | D00003_01, D00007_01, D00009_01 |
| `role` | VARCHAR | 0.0 | 4 | 0.0001 | False | admin, viewer, guest |
| `status` | VARCHAR | 0.0 | 3 | 0.0001 | False | suspended, active, inactive |
| `register_at` | TIMESTAMP | 0.0 | 49981 | 0.9996 | False | 2025-06-04 04:45:21, 2025-10-22 22:25:24, 2026-01-19 03:04:20 |
| `last_active_at` | TIMESTAMP | 0.1484 | 42537 | 0.8507 | False | 2025-11-21 02:10:45, 2026-01-30 22:55:31, 2026-02-27 15:51:30 |

### `dim_user_id_mapping`

- 业务域：用户
- 业务含义：明文 user_id 与哈希 user_id 的映射表，用于关联 AI 使用日志。
- 数据粒度：一行 = 一个 user_id 到 user_id_hash 的映射
- 主键 / 推荐唯一键：user_id
- 行数：40016
- 字段数：2

| 字段 | 类型 | 空值率 | 唯一值数 | 唯一率 | 可能主键 | 样例值 |
|---|---|---:|---:|---:|---|---|
| `user_id` | VARCHAR | 0.0 | 40016 | 1.0 | True | U000014, U000041, U000049 |
| `user_id_hash` | VARCHAR | 0.0 | 40016 | 1.0 | True | eaaa7d62cf431a709d2d7f7d18ce7b340318e1c3, 186e2848ce4261032fde9d4fb20dc7cfa51f6945, d410306fff7b64eb2bad02630cd6c319b0386ba9 |

### `dim_user_profile`

- 业务域：用户
- 业务含义：用户画像与客户端信息表，包含语言、时区、设备、版本。
- 数据粒度：一行 = 一个用户的一条画像记录；user_id 可能不唯一
- 主键 / 推荐唯一键：未明确或无单一主键
- 行数：52500
- 字段数：5

| 字段 | 类型 | 空值率 | 唯一值数 | 唯一率 | 可能主键 | 样例值 |
|---|---|---:|---:|---:|---|---|
| `user_id` | VARCHAR | 0.0 | 50000 | 0.9524 | False | U017529, U023491, U033195 |
| `language` | VARCHAR | 0.0 | 5 | 0.0001 | False | de-DE, en-US, zh-CN |
| `timezone` | VARCHAR | 0.0 | 10 | 0.0002 | False | Asia/Seoul, Asia/Tokyo, America/Sao_Paulo |
| `device_os` | VARCHAR | 0.0 | 5 | 0.0001 | False | Windows, macOS, iOS |
| `app_version` | VARCHAR | 0.0 | 930 | 0.0177 | False | 7.13.9, 6.17.0, 8.10.8 |

### `dim_user_role_history`

- 业务域：用户
- 业务含义：用户角色变更流水表。
- 数据粒度：一行 = 一次角色变更
- 主键 / 推荐唯一键：未明确或无单一主键
- 行数：15070
- 字段数：5

| 字段 | 类型 | 空值率 | 唯一值数 | 唯一率 | 可能主键 | 样例值 |
|---|---|---:|---:|---:|---|---|
| `user_id` | VARCHAR | 0.0 | 7516 | 0.4987 | False | U000121, U000163, U000194 |
| `old_role` | VARCHAR | 0.0 | 4 | 0.0003 | False | viewer, admin, guest |
| `new_role` | VARCHAR | 0.0 | 4 | 0.0003 | False | admin, member, guest |
| `changed_at` | TIMESTAMP | 0.0 | 15067 | 0.9998 | False | 2026-03-08 13:38:26, 2026-01-09 10:57:10, 2026-02-23 18:34:46 |
| `changed_by` | VARCHAR | 0.0 | 4066 | 0.2698 | False | admin, U000103, U000115 |

### `fact_actual_revenue`

- 业务域：计费与订阅
- 业务含义：月度实收收入表，记录订阅每月席位、标价收入、折扣、优惠券和实际入账收入。
- 数据粒度：一行 = 一个订阅一个月的实收
- 主键 / 推荐唯一键：revenue_id
- 行数：1366
- 字段数：11

| 字段 | 类型 | 空值率 | 唯一值数 | 唯一率 | 可能主键 | 样例值 |
|---|---|---:|---:|---:|---|---|
| `revenue_id` | VARCHAR | 0.0 | 1366 | 1.0 | True | REV36a6e819c5de, REVf496639e74ab, REVae643ed0df6e |
| `tenant_id` | VARCHAR | 0.0 | 343 | 0.2511 | False | T00018, T00036, T00041 |
| `sub_id` | VARCHAR | 0.0 | 343 | 0.2511 | False | SUB824397d44e81, SUBe7098789a5b6, SUBdc9a1fc6f9a2 |
| `month` | VARCHAR | 0.0 | 12 | 0.0088 | False | 2025-08, 2025-06, 2025-12 |
| `seats` | BIGINT | 0.0 | 163 | 0.1193 | False | 168, 27, 97 |
| `list_price_per_seat` | DOUBLE | 0.0 | 4 | 0.0029 | False | 499.0, 79.0, 29.0 |
| `list_revenue` | DOUBLE | 0.0 | 265 | 0.194 | False | 5372.0, 20497.0, 55888.0 |
| `discount_rate` | DOUBLE | 0.0 | 899 | 0.6581 | False | 0.1877, 0.1299, 0.1224 |
| `coupon_amount` | DOUBLE | 0.0 | 208 | 0.1523 | False | 66.64, 2062.8, 945.21 |
| `actual_revenue` | DOUBLE | 0.0 | 1365 | 0.9993 | False | 1440.05, 1230.38, 4423.34 |
| `currency` | VARCHAR | 0.0 | 1 | 0.0007 | False | USD |

### `fact_ai_usage_log`

- 业务域：产品使用
- 业务含义：AI 调用日志表，记录 AI credits 的扣减、赠送、退款等操作。
- 数据粒度：一行 = 一次 AI 操作
- 主键 / 推荐唯一键：log_id
- 行数：248249
- 字段数：7

| 字段 | 类型 | 空值率 | 唯一值数 | 唯一率 | 可能主键 | 样例值 |
|---|---|---:|---:|---:|---|---|
| `log_id` | VARCHAR | 0.0 | 248249 | 1.0 | True | AIL61509372b9f9, AIL38b76fa540a0, AILa2853734cbe6 |
| `user_id_hash` | VARCHAR | 0.0 | 15000 | 0.0604 | False | 0bb81ddb63178133bfae80b6c3833eedabf519e4, 6c93cc22097be12cce829ae3a891222d9fea696c, fa27d43e23a5f4007c434661564f18652cced54c |
| `operation_type` | VARCHAR | 0.0 | 3 | 0.0 | False | earn, refund, deduct |
| `credits_amount` | BIGINT | 0.0 | 500 | 0.002 | False | 265, 497, 36 |
| `model_name` | VARCHAR | 0.0 | 5 | 0.0 | False | claude-3.5, moonshot-v1, gpt-4o |
| `created_at` | BIGINT | 0.0 | 366 | 0.0015 | False | 1757347200, 1769702400, 1754409600 |
| `remark` | VARCHAR | 0.0 | 248249 | 1.0 | True | {"action": "ai_query", "model": "doubao-pro", "cause": "model: doubao-pro, input: 201, output: 2834, cost: 0.086775 USD (credits)", "session_id": "S05c098af953c"}, {"action": "ai_query", "model": "glm-4", "cause": "model: glm-4, input: 5316, output: 387, cost: 0.059328 USD (credits)", "session_id": "S67bc92c1efc7"}, {"action": "ai_query", "model": "moonshot-v1", "cause": "model: moonshot-v1, input: 1288, output: 2592, cost: 0.102521 USD (credits)", "session_id": "S90bc29a58d8c"} |

### `fact_campaign`

- 业务域：增长与实验
- 业务含义：营销活动表，记录渠道、预算、目标人群和起止日期。
- 数据粒度：一行 = 一个营销活动
- 主键 / 推荐唯一键：campaign_id
- 行数：30
- 字段数：7

| 字段 | 类型 | 空值率 | 唯一值数 | 唯一率 | 可能主键 | 样例值 |
|---|---|---:|---:|---:|---|---|
| `campaign_id` | VARCHAR | 0.0 | 30 | 1.0 | True | CMP005, CMP009, CMP012 |
| `name` | VARCHAR | 0.0 | 30 | 1.0 | True | Campaign_Launch_4, Campaign_Winter_15, Campaign_Spring_24 |
| `channel` | VARCHAR | 0.0 | 6 | 0.2 | False | webinar, email_drip, referral |
| `start_date` | DATE | 0.0 | 30 | 1.0 | True | 2025-04-16, 2025-04-25, 2026-02-04 |
| `end_date` | DATE | 0.0 | 27 | 0.9 | False | 2025-06-29, 2025-06-08, 2026-02-06 |
| `budget` | DOUBLE | 0.0 | 30 | 1.0 | True | 9760.67, 22514.8, 31589.41 |
| `target_segment` | VARCHAR | 0.0 | 5 | 0.1667 | False | dormant, trial, all |

### `fact_campaign_attribution`

- 业务域：增长与实验
- 业务含义：营销归因触点表，记录用户对营销活动的一次触达。
- 数据粒度：一行 = 一个用户对一次活动的一次触达
- 主键 / 推荐唯一键：未明确或无单一主键
- 行数：78414
- 字段数：5

| 字段 | 类型 | 空值率 | 唯一值数 | 唯一率 | 可能主键 | 样例值 |
|---|---|---:|---:|---:|---|---|
| `user_id` | VARCHAR | 0.0 | 34447 | 0.4393 | False | U040181, U031957, U018710 |
| `campaign_id` | VARCHAR | 0.0 | 30 | 0.0004 | False | CMP005, CMP009, CMP012 |
| `touch_type` | VARCHAR | 0.0 | 4 | 0.0001 | False | click, demo_request, signup |
| `touch_at` | TIMESTAMP | 0.0 | 78257 | 0.998 | False | 2025-12-31 12:58:11, 2026-01-22 15:30:25, 2026-01-09 06:00:45 |
| `is_converted` | BIGINT | 0.0 | 2 | 0.0 | False | 0, 1 |

### `fact_credit_usage`

- 业务域：计费与订阅
- 业务含义：资源消耗流水表，记录租户每天对某资源的消耗。
- 数据粒度：一行 = 一个租户一天对某资源的消耗
- 主键 / 推荐唯一键：usage_id
- 行数：64456
- 字段数：6

| 字段 | 类型 | 空值率 | 唯一值数 | 唯一率 | 可能主键 | 样例值 |
|---|---|---:|---:|---:|---|---|
| `usage_id` | VARCHAR | 0.0 | 64456 | 1.0 | True | CUe46a9c4d6f5e, CU7823a0239edb, CU1a82f083daf5 |
| `tenant_id` | VARCHAR | 0.0 | 249 | 0.0039 | False | T00033, T00043, T00053 |
| `resource_type` | VARCHAR | 0.0 | 4 | 0.0001 | False | storage_gb, api_calls, ai_token |
| `quantity` | DOUBLE | 0.0 | 47494 | 0.7368 | False | 261.81, 454.13, 427.67 |
| `unit_cost` | DOUBLE | 0.0 | 4991 | 0.0774 | False | 0.4904, 0.1268, 0.3199 |
| `dt` | DATE | 0.0 | 365 | 0.0057 | False | 2025-04-07, 2025-04-25, 2025-05-19 |

### `fact_daily_usage`

- 业务域：产品使用
- 业务含义：用户日度聚合使用表，包含会话数、活跃时长、功能使用 JSON。
- 数据粒度：一行 = 一个用户一天的聚合行为
- 主键 / 推荐唯一键：user_id, dt
- 行数：1915210
- 字段数：5

| 字段 | 类型 | 空值率 | 唯一值数 | 唯一率 | 可能主键 | 样例值 |
|---|---|---:|---:|---:|---|---|
| `user_id` | VARCHAR | 0.0 | 20000 | 0.0104 | False | U048775, U003114, U027016 |
| `dt` | DATE | 0.0 | 365 | 0.0002 | False | 2025-11-25, 2025-12-19, 2026-01-11 |
| `session_count` | BIGINT | 0.0 | 15 | 0.0 | False | 6, 10, 15 |
| `active_duration_sec` | BIGINT | 0.0 | 7141 | 0.0037 | False | 1223, 4377, 6409 |
| `feature_usage_json` | VARCHAR | 0.0 | 1380148 | 0.7206 | False | {"wiki": 42, "approval": 5, "video_conf": 6, "minutes": 50, "email": 16}, {"im": 2, "docs": 31, "video_conf": 37, "approval": 6}, {"video_conf": 1, "base": 30, "approval": 18} |

### `fact_doc_collaboration`

- 业务域：协作与内容
- 业务含义：文档协作明细表，记录用户每天对某文档的编辑和评论。
- 数据粒度：一行 = 一个用户一天对一个文档的协作
- 主键 / 推荐唯一键：未明确或无单一主键
- 行数：89676
- 字段数：5

| 字段 | 类型 | 空值率 | 唯一值数 | 唯一率 | 可能主键 | 样例值 |
|---|---|---:|---:|---:|---|---|
| `doc_id` | VARCHAR | 0.0 | 30000 | 0.3345 | False | DOCa73b95afcb81, DOCee7cc37140dd, DOC2feb8b24a8e0 |
| `user_id` | VARCHAR | 0.0 | 35558 | 0.3965 | False | U001587, U032917, U017283 |
| `dt` | DATE | 0.0 | 363 | 0.004 | False | 2026-01-06, 2025-11-30, 2026-01-29 |
| `edit_duration_sec` | BIGINT | 0.0 | 3601 | 0.0402 | False | 2382, 598, 1260 |
| `comment_count` | BIGINT | 0.0 | 21 | 0.0002 | False | 9, 14, 5 |

### `fact_document`

- 业务域：协作与内容
- 业务含义：文档表，记录文档创建者、租户、类型、创建和编辑信息。
- 数据粒度：一行 = 一个文档
- 主键 / 推荐唯一键：doc_id
- 行数：80000
- 字段数：7

| 字段 | 类型 | 空值率 | 唯一值数 | 唯一率 | 可能主键 | 样例值 |
|---|---|---:|---:|---:|---|---|
| `doc_id` | VARCHAR | 0.0 | 80000 | 1.0 | True | DOC7fff62dae279, DOCa46f677b6e4e, DOCfca265954902 |
| `creator_id` | VARCHAR | 0.0 | 34575 | 0.4322 | False | U014897, U036016, U002561 |
| `tenant_id` | VARCHAR | 0.0 | 500 | 0.0063 | False | T00334, T00053, T00300 |
| `doc_type` | VARCHAR | 0.0 | 5 | 0.0001 | False | sheet, mindnote, slides |
| `created_at` | TIMESTAMP | 0.0 | 79890 | 0.9986 | False | 2025-11-14 20:31:04, 2025-11-17 10:34:47, 2025-09-17 19:48:17 |
| `last_edit_at` | TIMESTAMP | 0.0 | 79856 | 0.9982 | False | 2025-06-03 09:50:33, 2025-11-14 20:31:04, 2025-05-06 13:10:23 |
| `edit_count` | BIGINT | 0.0 | 200 | 0.0025 | False | 111, 92, 123 |

### `fact_event_log`

- 业务域：产品使用
- 业务含义：埋点事件流水表，记录用户行为事件。
- 数据粒度：一行 = 一次用户行为事件
- 主键 / 推荐唯一键：event_id
- 行数：2000000
- 字段数：6

| 字段 | 类型 | 空值率 | 唯一值数 | 唯一率 | 可能主键 | 样例值 |
|---|---|---:|---:|---:|---|---|
| `event_id` | VARCHAR | 0.0 | 2000000 | 1.0 | True | Edf8a169cd698, Ec8f1013451a1, E388005024c83 |
| `user_id` | VARCHAR | 0.0 | 40016 | 0.02 | False | U028424, U042744, U009354 |
| `event_name` | VARCHAR | 0.0 | 20 | 0.0 | False | task_complete, approval_submit, reaction |
| `event_time` | TIMESTAMP | 0.0 | 1937818 | 0.9689 | False | 2025-12-22 20:38:34, 2025-11-25 05:14:54, 2025-05-06 21:56:13 |
| `page` | VARCHAR | 0.0 | 16 | 0.0 | False | /profile, /home, /task/list |
| `properties_json` | VARCHAR | 0.0 | 459 | 0.0002 | False | {"msg_type": "text"}, {"experiment_id": "EXP012"}, {"doc_type": "sheet"} |

### `fact_experiment`

- 业务域：增长与实验
- 业务含义：AB 实验表，记录实验名称、关联功能、起止日期和状态。
- 数据粒度：一行 = 一个实验
- 主键 / 推荐唯一键：exp_id
- 行数：50
- 字段数：6

| 字段 | 类型 | 空值率 | 唯一值数 | 唯一率 | 可能主键 | 样例值 |
|---|---|---:|---:|---:|---|---|
| `exp_id` | VARCHAR | 0.0 | 50 | 1.0 | True | EXP006, EXP014, EXP018 |
| `exp_name` | VARCHAR | 0.0 | 25 | 0.5 | False | Experiment_wiki_v3, Experiment_minutes_v1, Experiment_video_conf_v1 |
| `feature_key` | VARCHAR | 0.0 | 11 | 0.22 | False | calendar, whiteboard, docs |
| `start_date` | DATE | 0.0 | 46 | 0.92 | False | 2025-06-01, 2025-04-07, 2025-08-16 |
| `end_date` | DATE | 0.0 | 45 | 0.9 | False | 2025-09-02, 2025-12-27, 2026-01-01 |
| `status` | VARCHAR | 0.0 | 3 | 0.06 | False | completed, stopped, running |

### `fact_experiment_assignment`

- 业务域：增长与实验
- 业务含义：实验分流表，记录用户在实验中的分组。
- 数据粒度：一行 = 一个用户在一个实验中的分组
- 主键 / 推荐唯一键：未明确或无单一主键
- 行数：133748
- 字段数：4

| 字段 | 类型 | 空值率 | 唯一值数 | 唯一率 | 可能主键 | 样例值 |
|---|---|---:|---:|---:|---|---|
| `user_id` | VARCHAR | 0.0 | 38824 | 0.2903 | False | U031766, U001161, U040640 |
| `exp_id` | VARCHAR | 0.0 | 50 | 0.0004 | False | EXP006, EXP014, EXP018 |
| `variant` | VARCHAR | 0.0 | 3 | 0.0 | False | treatment_a, control, treatment_b |
| `assigned_at` | TIMESTAMP | 0.0 | 133300 | 0.9967 | False | 2025-08-01 01:51:16, 2025-07-16 02:58:30, 2025-07-19 13:39:08 |

### `fact_experiment_metric`

- 业务域：增长与实验
- 业务含义：实验指标日度表，记录实验分组在某天的指标、样本量和置信区间。
- 数据粒度：一行 = 一个实验的一个分组一个指标在某天的值
- 主键 / 推荐唯一键：未明确或无单一主键
- 行数：8071
- 字段数：8

| 字段 | 类型 | 空值率 | 唯一值数 | 唯一率 | 可能主键 | 样例值 |
|---|---|---:|---:|---:|---|---|
| `exp_id` | VARCHAR | 0.0 | 50 | 0.0062 | False | EXP001, EXP007, EXP008 |
| `variant` | VARCHAR | 0.0 | 3 | 0.0004 | False | treatment_b, control, treatment_a |
| `metric_name` | VARCHAR | 0.0 | 5 | 0.0006 | False | feature_adoption, retention_d7, conversion_rate |
| `dt` | DATE | 0.0 | 340 | 0.0421 | False | 2025-10-03, 2025-09-28, 2025-12-27 |
| `sample_size` | BIGINT | 0.0 | 3966 | 0.4914 | False | 719, 3899, 4872 |
| `metric_value` | DOUBLE | 0.0 | 8041 | 0.9963 | False | 77.5612, 73.292, 17.5241 |
| `ci_lower` | DOUBLE | 0.0 | 8033 | 0.9953 | False | 52.9192, 54.6702, 13.4294 |
| `ci_upper` | DOUBLE | 0.0 | 8043 | 0.9965 | False | 99.4629, 54.0308, 86.2358 |

### `fact_feature_usage`

- 业务域：产品使用
- 业务含义：功能使用明细表，记录用户每天对某功能的动作次数和使用时长。
- 数据粒度：一行 = 一个用户一个功能一天的使用
- 主键 / 推荐唯一键：user_id, feature_key, dt
- 行数：2700092
- 字段数：5

| 字段 | 类型 | 空值率 | 唯一值数 | 唯一率 | 可能主键 | 样例值 |
|---|---|---:|---:|---:|---|---|
| `user_id` | VARCHAR | 0.0 | 15000 | 0.0056 | False | U009473, U029341, U039811 |
| `feature_key` | VARCHAR | 0.0 | 12 | 0.0 | False | base, task, im |
| `dt` | DATE | 0.0 | 365 | 0.0001 | False | 2025-08-14, 2025-08-25, 2025-09-16 |
| `action_count` | BIGINT | 0.0 | 65 | 0.0 | False | 1, 23, 32 |
| `duration_sec` | BIGINT | 0.0 | 3591 | 0.0013 | False | 3000, 489, 1240 |

### `fact_invoice`

- 业务域：计费与订阅
- 业务含义：发票表，记录发票金额、币种、开票时间、付款时间和状态。
- 数据粒度：一行 = 一张发票
- 主键 / 推荐唯一键：invoice_id
- 行数：1248
- 字段数：8

| 字段 | 类型 | 空值率 | 唯一值数 | 唯一率 | 可能主键 | 样例值 |
|---|---|---:|---:|---:|---|---|
| `invoice_id` | VARCHAR | 0.0 | 1248 | 1.0 | True | INV84fe7fb73c1a, INV32379ed68c44, INVa5d580a5a2b7 |
| `tenant_id` | VARCHAR | 0.0 | 343 | 0.2748 | False | T00002, T00033, T00053 |
| `sub_id` | VARCHAR | 0.0 | 343 | 0.2748 | False | SUB824397d44e81, SUBe7098789a5b6, SUBdc9a1fc6f9a2 |
| `amount` | DOUBLE | 0.0 | 1248 | 1.0 | True | 1572.8, 1525.61, 18049.94 |
| `currency` | VARCHAR | 0.0 | 4 | 0.0032 | False | JPY, EUR, CNY |
| `issued_at` | TIMESTAMP | 0.0 | 1248 | 1.0 | True | 2026-01-31 09:17:50, 2025-05-27 12:37:17, 2026-03-19 22:19:14 |
| `paid_at` | TIMESTAMP | 0.2003 | 997 | 0.7989 | False | 2026-03-07 03:17:28, 2025-06-20 05:24:34, 2025-10-25 19:56:53 |
| `status` | VARCHAR | 0.0 | 4 | 0.0032 | False | paid, pending, void |

### `fact_message`

- 业务域：协作与内容
- 业务含义：消息表，记录用户在频道中发送的一条消息。
- 数据粒度：一行 = 一条消息
- 主键 / 推荐唯一键：msg_id
- 行数：314827
- 字段数：6

| 字段 | 类型 | 空值率 | 唯一值数 | 唯一率 | 可能主键 | 样例值 |
|---|---|---:|---:|---:|---|---|
| `msg_id` | VARCHAR | 0.0 | 314827 | 1.0 | True | M479f83016e08, Mcade211a3a9d, Mbe23eed48ac7 |
| `sender_id` | VARCHAR | 0.0 | 26815 | 0.0852 | False | U015085, U015083, U041062 |
| `channel_id` | VARCHAR | 0.0 | 3000 | 0.0095 | False | CH00110_06, CH00064_11, CH00338_09 |
| `msg_type` | VARCHAR | 0.0 | 5 | 0.0 | False | file, sticker, image |
| `sent_at` | TIMESTAMP | 0.0 | 313197 | 0.9948 | False | 2026-03-05 14:03:43, 2025-07-16 06:35:04, 2025-10-29 21:14:06 |
| `word_count` | BIGINT | 0.0 | 500 | 0.0016 | False | 111, 388, 84 |

### `fact_nps_survey`

- 业务域：增长与实验
- 业务含义：NPS 调研表，记录用户评分、角色、套餐和反馈文本。
- 数据粒度：一行 = 一次 NPS 调研
- 主键 / 推荐唯一键：survey_id
- 行数：8000
- 字段数：9

| 字段 | 类型 | 空值率 | 唯一值数 | 唯一率 | 可能主键 | 样例值 |
|---|---|---:|---:|---:|---|---|
| `survey_id` | VARCHAR | 0.0 | 8000 | 1.0 | True | NPS798a65bbfe48, NPS07f337f1e679, NPS8e136e2fa365 |
| `user_id` | VARCHAR | 0.0 | 8000 | 1.0 | True | U021302, U032993, U001102 |
| `tenant_id` | VARCHAR | 0.0 | 489 | 0.0611 | False | T00002, T00300, T00053 |
| `survey_date` | DATE | 0.0 | 366 | 0.0457 | False | 2026-02-07, 2025-07-18, 2025-06-08 |
| `role` | BIGINT | 0.0 | 3 | 0.0004 | False | 1, 3, 2 |
| `score` | VARCHAR | 0.0 | 12 | 0.0015 | False | 8, 10, 2 |
| `plan_tier` | VARCHAR | 0.0 | 5 | 0.0006 | False | Enterprise, Pro, Business |
| `feedback_text` | VARCHAR | 0.1109 | 8 | 0.001 | False | Pricing too high, Great product!, Too slow |
| `is_completed` | BIGINT | 0.0 | 2 | 0.0003 | False | 0, 1 |

### `fact_page_view`

- 业务域：产品使用
- 业务含义：页面浏览表，记录一次页面访问。
- 数据粒度：一行 = 一次页面访问
- 主键 / 推荐唯一键：pv_id
- 行数：1801438
- 字段数：6

| 字段 | 类型 | 空值率 | 唯一值数 | 唯一率 | 可能主键 | 样例值 |
|---|---|---:|---:|---:|---|---|
| `pv_id` | VARCHAR | 0.0 | 1801438 | 1.0 | True | PVed0898776a3a, PVa729b7fa6133, PV5b36425084af |
| `session_id` | VARCHAR | 0.0 | 400000 | 0.222 | False | S31ffce470a83, Se6d913b2fa5f, Sb8832681571a |
| `user_id` | VARCHAR | 0.0 | 29809 | 0.0165 | False | U024143, U036790, U032036 |
| `page_path` | VARCHAR | 0.0 | 16 | 0.0 | False | /task/list, /profile, /home |
| `referrer` | VARCHAR | 0.0589 | 16 | 0.0 | False | /task/list, /profile, /home |
| `view_duration_ms` | BIGINT | 0.0 | 59501 | 0.033 | False | 31771, 16415, 8772 |

### `fact_payment`

- 业务域：计费与订阅
- 业务含义：付款流水表，记录一次付款尝试。
- 数据粒度：一行 = 一次付款尝试；一张发票可能有多条付款记录
- 主键 / 推荐唯一键：payment_id
- 行数：1624
- 字段数：6

| 字段 | 类型 | 空值率 | 唯一值数 | 唯一率 | 可能主键 | 样例值 |
|---|---|---:|---:|---:|---|---|
| `payment_id` | VARCHAR | 0.0 | 1624 | 1.0 | True | PAY04f9322563b5, PAYfb18da005ee8, PAY1073c05f52da |
| `invoice_id` | VARCHAR | 0.0 | 1248 | 0.7685 | False | INV84fe7fb73c1a, INV32379ed68c44, INVa5d580a5a2b7 |
| `method` | VARCHAR | 0.0 | 4 | 0.0025 | False | bank_transfer, credit_card, wechat_pay |
| `amount` | DOUBLE | 0.0 | 1624 | 1.0 | True | 1572.8, 1525.61, 11632.36 |
| `status` | VARCHAR | 0.0 | 3 | 0.0018 | False | failed, pending, success |
| `paid_at` | TIMESTAMP | 0.258 | 1205 | 0.742 | False | 2026-03-10 08:34:40, 2025-09-20 22:31:16, 2025-05-25 08:24:34 |

### `fact_session`

- 业务域：产品使用
- 业务含义：会话表，记录用户一次登录到退出的会话。
- 数据粒度：一行 = 一次会话
- 主键 / 推荐唯一键：session_id
- 行数：1269417
- 字段数：6

| 字段 | 类型 | 空值率 | 唯一值数 | 唯一率 | 可能主键 | 样例值 |
|---|---|---:|---:|---:|---|---|
| `session_id` | VARCHAR | 0.0 | 1269417 | 1.0 | True | S95303fdd05c4, S17927e56cd3b, S6dc4d77c4eea |
| `user_id` | VARCHAR | 0.0 | 30000 | 0.0236 | False | U001745, U033494, U020437 |
| `start_time` | TIMESTAMP | 0.0 | 1234424 | 0.9724 | False | 2026-02-05 01:59:27, 2025-12-30 11:16:52, 2025-08-16 13:56:11 |
| `end_time` | TIMESTAMP | 0.0 | 1234450 | 0.9725 | False | 2026-01-14 01:03:37, 2025-05-15 01:07:50, 2025-12-15 05:36:09 |
| `device` | VARCHAR | 0.0 | 5 | 0.0 | False | Windows, iOS, Linux |
| `ip_country` | VARCHAR | 0.0 | 10 | 0.0 | False | AU, JP, US |

### `fact_subscription`

- 业务域：计费与订阅
- 业务含义：订阅表，记录租户订阅周期、套餐、MRR 和状态。
- 数据粒度：一行 = 一个订阅周期
- 主键 / 推荐唯一键：sub_id
- 行数：343
- 字段数：7

| 字段 | 类型 | 空值率 | 唯一值数 | 唯一率 | 可能主键 | 样例值 |
|---|---|---:|---:|---:|---|---|
| `sub_id` | VARCHAR | 0.0 | 343 | 1.0 | True | SUB824397d44e81, SUBe7098789a5b6, SUBdc9a1fc6f9a2 |
| `tenant_id` | VARCHAR | 0.0 | 343 | 1.0 | True | T00018, T00036, T00041 |
| `plan_tier` | VARCHAR | 0.0 | 4 | 0.0117 | False | business, enterprise, pro |
| `start_date` | DATE | 0.0 | 207 | 0.6035 | False | 2025-05-19, 2025-07-31, 2025-06-05 |
| `end_date` | DATE | 0.0 | 254 | 0.7405 | False | 2026-01-24, 2025-09-05, 2025-08-16 |
| `mrr` | BIGINT | 0.0 | 262 | 0.7638 | False | 14062, 3190, 10349 |
| `status` | VARCHAR | 0.0 | 3 | 0.0087 | False | churned, active, trial |

### `fact_tenant_metrics_snapshot`

- 业务域：租户与组织
- 业务含义：租户指标快照表，包含用户、文档、收入、消息等累计或当日指标。
- 数据粒度：一行 = 一个租户在某一天的指标快照
- 主键 / 推荐唯一键：tenant_id, snapshot_date
- 行数：141684
- 字段数：8

| 字段 | 类型 | 空值率 | 唯一值数 | 唯一率 | 可能主键 | 样例值 |
|---|---|---:|---:|---:|---|---|
| `tenant_id` | VARCHAR | 0.0 | 500 | 0.0035 | False | T00479, T00482, T00498 |
| `snapshot_date` | DATE | 0.0 | 366 | 0.0026 | False | 2025-08-16, 2025-09-02, 2025-09-05 |
| `total_users` | BIGINT | 0.0 | 1140 | 0.008 | False | 464, 472, 484 |
| `total_docs` | BIGINT | 0.0 | 4262 | 0.0301 | False | 1616, 1662, 1667 |
| `total_storage_mb` | BIGINT | 0.0 | 82947 | 0.5854 | False | 60551, 61404, 62296 |
| `total_revenue` | DOUBLE | 0.0 | 140798 | 0.9937 | False | 72429.58, 78021.16, 78551.35 |
| `total_messages` | BIGINT | 0.0 | 106224 | 0.7497 | False | 91977, 96137, 110305 |
| `active_users_today` | BIGINT | 0.0 | 101 | 0.0007 | False | 16, 15, 67 |

### `fact_ticket`

- 业务域：客服工单
- 业务含义：工单表，记录租户提交的客服问题、优先级、状态、创建和解决时间。
- 数据粒度：一行 = 一个工单
- 主键 / 推荐唯一键：ticket_id
- 行数：3922
- 字段数：8

| 字段 | 类型 | 空值率 | 唯一值数 | 唯一率 | 可能主键 | 样例值 |
|---|---|---:|---:|---:|---|---|
| `ticket_id` | VARCHAR | 0.0 | 3922 | 1.0 | True | TK65cbf00be7cd, TKc71f18ec6f67, TK1f799aecfd6b |
| `tenant_id` | VARCHAR | 0.0 | 500 | 0.1275 | False | T00002, T00031, T00033 |
| `reporter_id` | VARCHAR | 0.0 | 3328 | 0.8485 | False | U000168, U000185, U000403 |
| `category` | VARCHAR | 0.0 | 12 | 0.0031 | False | security, bug, account_permission |
| `priority` | VARCHAR | 0.0 | 4 | 0.001 | False | low, medium, critical |
| `status` | VARCHAR | 0.0 | 4 | 0.001 | False | open, resolved, in_progress |
| `created_at` | TIMESTAMP | 0.0 | 3922 | 1.0 | True | 2025-12-18 01:00:05, 2026-03-03 13:50:36, 2025-09-16 00:16:00 |
| `resolved_at` | TIMESTAMP | 0.3195 | 2669 | 0.6805 | False | 2026-03-09 22:50:36, 2025-04-23 23:25:49, 2025-07-19 14:18:36 |

### `fact_ticket_reply`

- 业务域：客服工单
- 业务含义：工单回复表，记录客服、客户或机器人的回复。
- 数据粒度：一行 = 一条工单回复
- 主键 / 推荐唯一键：reply_id
- 行数：21715
- 字段数：5

| 字段 | 类型 | 空值率 | 唯一值数 | 唯一率 | 可能主键 | 样例值 |
|---|---|---:|---:|---:|---|---|
| `reply_id` | VARCHAR | 0.0 | 21715 | 1.0 | True | TR39e25be19d8c, TR5b3e9d698537, TR02c61f5bc1fe |
| `ticket_id` | VARCHAR | 0.0 | 3922 | 0.1806 | False | TKf238e45efd5a, TK1c33e39d3ecb, TK64e19fa5fa5b |
| `author_type` | VARCHAR | 0.0 | 3 | 0.0001 | False | bot, agent, customer |
| `content_length` | BIGINT | 0.0 | 1991 | 0.0917 | False | 1796, 1172, 1344 |
| `replied_at` | TIMESTAMP | 0.0 | 21217 | 0.9771 | False | 2025-04-17 12:25:49, 2025-07-08 12:18:36, 2025-07-09 12:18:36 |

### `fact_user_activation`

- 业务域：用户
- 业务含义：用户激活里程碑表，记录首次登录、首次创建文档、首次发消息等行为。
- 数据粒度：一行 = 一个用户达成一个激活里程碑
- 主键 / 推荐唯一键：未明确或无单一主键
- 行数：180072
- 字段数：3

| 字段 | 类型 | 空值率 | 唯一值数 | 唯一率 | 可能主键 | 样例值 |
|---|---|---:|---:|---:|---|---|
| `user_id` | VARCHAR | 0.0 | 49773 | 0.2764 | False | U034060, U034061, U034062 |
| `milestone_type` | VARCHAR | 0.0 | 6 | 0.0 | False | first_doc_create, first_meeting_join, first_msg_send |
| `reached_at` | TIMESTAMP | 0.0 | 179745 | 0.9982 | False | 2025-12-03 09:21:45, 2025-07-17 15:06:43, 2025-07-07 02:58:08 |

## 4. 主要 JOIN 规则摘要

| 左表 | 右表 | JOIN 条件 | 关系 | 注意事项 |
|---|---|---|---|---|
| `dim_tenant` | `dim_user` | `dim_tenant.tenant_id = dim_user.tenant_id` | one_to_many | 一个企业对应多个用户。统计企业数时 JOIN 后要 COUNT(DISTINCT tenant_id)。 |
| `dim_tenant` | `dim_department` | `dim_tenant.tenant_id = dim_department.tenant_id` | one_to_many | 一个企业对应多个部门。 |
| `dim_department` | `dim_user` | `dim_department.dept_id = dim_user.dept_id` | one_to_many | 一个部门对应多个用户；dim_user.dept_id 可能为空。 |
| `dim_tenant` | `fact_subscription` | `dim_tenant.tenant_id = fact_subscription.tenant_id` | one_to_many | 一个企业可能有多个订阅周期。 |
| `fact_subscription` | `dim_plan` | `fact_subscription.plan_tier = dim_plan.plan_tier` | many_to_one | 订阅表通过套餐等级关联套餐维度表。 |
| `fact_subscription` | `fact_actual_revenue` | `fact_subscription.sub_id = fact_actual_revenue.sub_id` | one_to_many | 一个订阅周期可能对应多个月实收收入。 |
| `fact_subscription` | `fact_invoice` | `fact_subscription.sub_id = fact_invoice.sub_id` | one_to_many | 一个订阅可能对应多张发票。 |
| `fact_invoice` | `fact_payment` | `fact_invoice.invoice_id = fact_payment.invoice_id` | one_to_many | 一张发票可能对应多次付款尝试，聚合金额时要小心重复计算。 |
| `dim_user` | `fact_daily_usage` | `dim_user.user_id = fact_daily_usage.user_id` | one_to_many | 一个用户对应多天使用记录。 |
| `dim_user` | `fact_feature_usage` | `dim_user.user_id = fact_feature_usage.user_id` | one_to_many | 一个用户对应多个功能、多天使用记录。 |
| `fact_feature_usage` | `dim_feature` | `fact_feature_usage.feature_key = dim_feature.feature_key` | many_to_one | 功能使用明细通过 feature_key 关联功能维度。 |
| `dim_user` | `dim_user_id_mapping` | `dim_user.user_id = dim_user_id_mapping.user_id` | one_to_one_or_partial | 用户明文 ID 与哈希 ID 映射，可能不是所有用户都有映射。 |
| `dim_user_id_mapping` | `fact_ai_usage_log` | `dim_user_id_mapping.user_id_hash = fact_ai_usage_log.user_id_hash` | one_to_many | AI 使用日志只有哈希 user_id，需要通过 mapping 表关联。 |
| `dim_user` | `fact_session` | `dim_user.user_id = fact_session.user_id` | one_to_many | 一个用户对应多次会话。 |
| `fact_session` | `fact_page_view` | `fact_session.session_id = fact_page_view.session_id` | one_to_many | 一次会话可以包含多次页面浏览。 |
| `dim_user` | `fact_event_log` | `dim_user.user_id = fact_event_log.user_id` | one_to_many | 一个用户对应多条埋点事件。 |
| `dim_tenant` | `fact_document` | `dim_tenant.tenant_id = fact_document.tenant_id` | one_to_many | 一个企业对应多个文档。 |
| `dim_user` | `fact_document` | `dim_user.user_id = fact_document.creator_id` | one_to_many | 用户作为文档创建者关联文档表。 |
| `fact_document` | `fact_doc_collaboration` | `fact_document.doc_id = fact_doc_collaboration.doc_id` | one_to_many | 一个文档可能有多个用户、多天协作记录。 |
| `dim_channel` | `fact_message` | `dim_channel.channel_id = fact_message.channel_id` | one_to_many | 一个频道对应多条消息。 |
| `dim_user` | `fact_message` | `dim_user.user_id = fact_message.sender_id` | one_to_many | 用户作为消息发送者关联消息表。 |
| `dim_tenant` | `fact_ticket` | `dim_tenant.tenant_id = fact_ticket.tenant_id` | one_to_many | 一个企业对应多个客服工单。 |
| `fact_ticket` | `dim_ticket_category` | `fact_ticket.category = dim_ticket_category.category_id` | many_to_one | 工单通过 category 关联工单分类。 |
| `fact_ticket` | `fact_ticket_reply` | `fact_ticket.ticket_id = fact_ticket_reply.ticket_id` | one_to_many | 一个工单对应多条回复。 |
| `fact_campaign` | `fact_campaign_attribution` | `fact_campaign.campaign_id = fact_campaign_attribution.campaign_id` | one_to_many | 一个营销活动对应多个触点。 |
| `dim_user` | `fact_campaign_attribution` | `dim_user.user_id = fact_campaign_attribution.user_id` | one_to_many | 一个用户可能有多个营销触点。 |
| `fact_experiment` | `fact_experiment_assignment` | `fact_experiment.exp_id = fact_experiment_assignment.exp_id` | one_to_many | 一个实验对应多个用户分流记录。 |
| `fact_experiment` | `fact_experiment_metric` | `fact_experiment.exp_id = fact_experiment_metric.exp_id` | one_to_many | 一个实验对应多个分组、指标和日期的聚合指标。 |
| `dim_user` | `fact_nps_survey` | `dim_user.user_id = fact_nps_survey.user_id` | one_to_many | 一个用户可能对应多条 NPS 调研。 |
| `dim_tenant` | `fact_nps_survey` | `dim_tenant.tenant_id = fact_nps_survey.tenant_id` | one_to_many | 一个企业对应多条 NPS 调研。 |

## 5. Agent 生成 SQL 前的原则

1. 先判断用户问题属于哪个业务域。
2. 再选择主事实表和必要维度表。
3. 必须确认每张表的粒度，避免一对多 JOIN 后重复计算。
4. 金额问题必须区分 MRR、发票金额、付款金额、实收收入。
5. 时间字段需要检查格式，尤其是 fact_ai_usage_log.created_at。
6. 如果涉及 AI 使用日志，必须通过 dim_user_id_mapping 做 ID 映射。
7. 如果涉及 NPS，要处理 score='N/A'、role 编码、plan_tier 大小写。
8. 如果 SQL 执行失败，应结合错误信息和 data_quality_notes.md 修复。

## 6. 官方数据字典原文摘录

以下内容来自项目中的 DATA_DICTIONARY.md，可作为 Agent 的补充上下文。

# CloudWork Dataset · 数据字典

> 面向参赛者的数据集 Schema 说明。请在分析前通读一遍，并留意各表粒度、字段语义、JOIN 关系。

## 元信息

| 项 | 值 |
|---|---|
| 数据集名 | CloudWork |
| 场景 | 合成的 SaaS 协作办公产品数据仓库 |
| 覆盖时间 | 2025-04-01 ~ 2026-03-31（366 天） |
| 表数量 | 37 |
| 总行数 | ~1124 万 |
| 总体积 | ~726 MB |
| 数据真实性 | **完全合成**（`generate_cloudwork.py` + `patch_traps.py`，random seed=42），与任何真实产品/用户无关联 |
| 文件格式 | CSV，UTF-8，逗号分隔，第一行为表头 |

---

## 业务域总览

| 业务域 | 中文 | 表数 | 主题 |
|---|---|---|---|
| A | 租户与组织 | 6 | 企业客户、部门、组织节点、套餐归属、租户日度快照 |
| B | 用户 | 5 | 用户身份、画像、角色变更、激活里程碑、ID 映射 |
| C | 产品使用 | 7 | 会话、PV、事件、日度聚合、功能使用、AI 调用 |
| D | 协作与内容 | 4 | 群聊消息、文档、文档协作 |
| E | 计费与订阅 | 6 | 套餐、订阅、发票、付款、资源消耗、实收收入 |
| F | 客服工单 | 3 | 工单分类、工单、工单回复 |
| G | 增长与实验 | 6 | 营销活动、归因、AB 实验、实验指标、NPS |

---

## 全局约定

- **主键命名**：维度表主键形如 `{实体}_id`（`tenant_id` / `user_id` 等），事实表主键多为 UUID 截断形式
- **日期字段**：`dt` / `*_date` 为 `YYYY-MM-DD` 字符串
- **时间戳字段**：默认 `YYYY-MM-DD HH:MM:SS`（UTC 为主），个别表使用 unix epoch（见字段说明）
- **空值表示**：CSV 中空字符串 `""` 代表 NULL（如 `end_date=""` 表示"当前仍有效"）
- **金额字段**：默认单位为**美元**（USD），`fact_invoice` 包含多币种，见 `currency` 字段
- **user_id 格式**：主格式 `U000001` ~ `U050000`；`fact_ai_usage_log` 表中为哈希字符串，见 `dim_user_id_mapping`
- **tenant_id 格式**：`T00001` ~ `T00500`

---

## A. 租户与组织

### A1. `dim_tenant` — 企业客户维度
- 主键：`tenant_id`
- 粒度：一行 = 一个企业
- 行数：500

| 字段 | 类型 | 说明 |
|---|---|---|
| tenant_id | STRING | 企业 ID（`T00001` 起） |
| name | STRING | 企业名称（形如 `Company_17`） |
| industry | STRING | 行业。枚举：Technology / Finance / Healthcare / Education / Retail / Manufacturing / Media / Consulting / Legal / Real Estate / Government / Nonprofit / Logistics / Hospitality / Energy |
| size_tier | STRING | 规模。枚举：startup / small / medium / large / enterprise |
| country | STRING | 国家代码。枚举：CN / US / JP / DE / GB / SG / IN / BR / KR / AU |
| created_at | TIMESTAMP | 企业创建时间（UTC） |

### A2. `dim_tenant_plan` — 租户套餐历史（SCD2）
- 主键：`tenant_id` + `effective_from`
- 粒度：一行 = 一个租户在某个时间区间使用的套餐
- 行数：~2.3k

| 字段 | 类型 | 说明 |
|---|---|---|
| tenant_id | STRING | 关联 `dim_tenant.tenant_id` |
| plan_name | STRING | 套餐展示名：Free / Starter / Professional / Business / Enterprise |
| plan_tier | STRING | 套餐等级（小写短语）：free / starter / pro / business / enterprise |
| effective_from | DATE | 该套餐生效开始日 |
| effective_to | DATE | 该套餐失效日；**空字符串表示当前仍有效** |

### A3. `dim_department` — 部门
- 主键：`dept_id`
- 粒度：一行 = 一个部门
- 行数：~2.9k

| 字段 | 类型 | 说明 |
|---|---|---|
| dept_id | STRING | 部门 ID，形如 `D00001_03` |
| tenant_id | STRING | 所属企业 |
| dept_name | STRING | 部门名。枚举：Engineering / Product / Design / Sales / Marketing / Support / HR / Finance / Legal / Operations |
| parent_dept_id | STRING | 上级部门 ID（可为空，表示顶级） |

### A4. `dim_org_structure` — 组织节点（团队/虚拟组）
- 主键：`node_id`
- 粒度：一行 = 一个组织节点；只覆盖前 200 个 tenant
- 行数：~1.9k

| 字段 | 类型 | 说明 |
|---|---|---|
| node_id | STRING | 节点 ID，形如 `N00001_03` |
| parent_node_id | STRING | 上级节点 ID（可为空） |
| tenant_id | STRING | 所属企业 |
| node_type | STRING | 节点类型。枚举：department / team / virtual_group |
| level | INT | 层级深度（0~5） |

### A5. `dim_tenant_config` — 租户功能开关与配置
- 主键：`tenant_id` + `feature_key`
- 粒度：一行 = 一个租户对某个功能的开关与配置
- 行数：~6k

| 字段 | 类型 | 说明 |
|---|---|---|
| tenant_id | STRING | 企业 |
| feature_key | STRING | 功能 key，见 `dim_feature.feature_key` |
| enabled | INT | 是否开启，1=开 / 0=关 |
| config_json | STRING | 功能配置 JSON（可能为空字符串）。示例：`{"max_participants": 300}` / `{"max_tables": 50}` |

### A6. `fact_tenant_metrics_snapshot` — 租户日度指标快照
- 主键：`tenant_id` + `snapshot_date`
- 粒度：一行 = 一个租户在某一天的指标快照（采样频率不固定，多数为日度，部分为周度）
- 行数：~14 万

| 字段 | 类型 | 说明 |
|---|---|---|
| tenant_id | STRING | 企业 |
| snapshot_date | DATE | 快照日期 |
| total_users | INT | 截至快照日的用户总数（**累计口径**） |
| total_docs | INT | 截至快照日的文档总数（累计口径） |
| total_storage_mb | INT | 截至快照日的存储总量，单位 MB（累计口径） |
| total_revenue | DOUBLE | 截至快照日的总收入（**生命周期累计**，不是当日增量） |
| total_messages | INT | 截至快照日的消息总数（累计口径） |
| active_users_today | INT | 该日活跃用户数（**当日口径**，不是累计） |

> 注：`total_*` 字段均为累计值，如需"当日新增"或"周环比"需自行做差分。

---

## B. 用户

### B1. `dim_user` — 用户维度
- 主键：`user_id`
- 粒度：一行 = 一个用户
- 行数：50,000

| 字段 | 类型 | 说明 |
|---|---|---|
| user_id | STRING | 用户 ID（`U000001` 起） |
| tenant_id | STRING | 所属企业 |
| dept_id | STRING | 所属部门（可为空） |
| role | STRING | 角色。枚举：admin / member / viewer / guest |
| status | STRING | 账户状态。枚举：active / inactive / suspended |
| register_at | TIMESTAMP | 注册时间 |
| last_active_at | TIMESTAMP | 最近活跃时间（可为空，表示从未活跃） |

### B2. `dim_user_profile` — 用户画像
- 主键：**非唯一**（一个 user_id 可能出现多条，需业务处理）
- 粒度：用户偏好/客户端信息
- 行数：~5.25 万

| 字段 | 类型 | 说明 |
|---|---|---|
| user_id | STRING | 用户 |
| language | STRING | 界面语言。枚举：zh-CN / en-US / ja-JP / de-DE / pt-BR |
| timezone | STRING | 时区字符串（IANA，如 `Asia/Shanghai`） |
| device_os | STRING | 设备系统。枚举：Windows / macOS / iOS / Android / Linux |
| app_version | STRING | 客户端版本号，形如 `7.12.3` |

### B3. `dim_user_role_history` — 角色变更流水
- 主键：无业务主键
- 粒度：一行 = 一次角色变更
- 行数：~1.5 万

| 字段 | 类型 | 说明 |
|---|---|---|
| user_id | STRING | 被变更用户 |
| old_role | STRING | 变更前角色 |
| new_role | STRING | 变更后角色 |
| changed_at | TIMESTAMP | 变更时间 |
| changed_by | STRING | 操作者：`system` / `admin` / 或具体 user_id |

### B4. `fact_user_activation` — 用户激活里程碑
- 粒度：一行 = 一个用户达成一个激活里程碑
- 行数：~18 万

| 字段 | 类型 | 说明 |
|---|---|---|
| user_id | STRING | 用户 |
| milestone_type | STRING | 里程碑类型。枚举：first_login / first_doc_create / first_msg_send / first_meeting_join / invited_first_user / created_first_task |
| reached_at | TIMESTAMP | 达成时间 |

### B5. `dim_user_id_mapping` — 用户 ID 哈希映射
- 主键：`user_id`
- 粒度：一行 = `user_id` 到其哈希值的映射
- 行数：~4 万

| 字段 | 类型 | 说明 |
|---|---|---|
| user_id | STRING | 明文用户 ID（对应 `dim_user.user_id`） |
| user_id_hash | STRING | 哈希后的用户 ID（SHA1 衍生，对应 `fact_ai_usage_log.user_id_hash`） |

> 用途：`fact_ai_usage_log` 出于隐私考虑存储哈希 ID，如需与其他事实表关联必须通过本表做桥接。

---

## C. 产品使用

### C1. `dim_feature` — 功能维度
- 主键：`feature_key`
- 行数：12

| 字段 | 类型 | 说明 |
|---|---|---|
| feature_key | STRING | 功能 key。枚举：im / docs / sheets / calendar / video_conf / email / approval / base / wiki / minutes / task / whiteboard |
| feature_name | STRING | 功能展示名 |
| module | STRING | 所属模块。枚举：Communication / Productivity / Workflow / Knowledge |
| category | STRING | 定位。枚举：core / advanced / premium |
| launched_at | DATE | 上线日期 |

### C2. `fact_session` — 会话
- 主键：`session_id`
- 粒度：一行 = 一次会话（登录到退出）
- 行数：~127 万

| 字段 | 类型 | 说明 |
|---|---|---|
| session_id | STRING | 会话 ID（`S` 前缀） |
| user_id | STRING | 用户 |
| start_time | TIMESTAMP | 会话开始 |
| end_time | TIMESTAMP | 会话结束 |
| device | STRING | 设备。枚举：Windows / macOS / iOS / Android / Linux |
| ip_country | STRING | IP 归属国家代码 |

### C3. `fact_page_view` — 页面浏览
- 主键：`pv_id`
- 粒度：一行 = 一次页面访问
- 行数：~180 万

| 字段 | 类型 | 说明 |
|---|---|---|
| pv_id | STRING | PV ID（`PV` 前缀） |
| session_id | STRING | 关联会话 |
| user_id | STRING | 用户 |
| page_path | STRING | 页面路径，如 `/docs/editor` |
| referrer | STRING | 上一页路径（可为空） |
| view_duration_ms | INT | 停留时长（毫秒） |

### C4. `fact_event_log` — 事件日志（埋点流水）
- 主键：`event_id`
- 粒度：一行 = 一次用户行为事件
- 行数：~200 万（本数据集体量最大）

| 字段 | 类型 | 说明 |
|---|---|---|
| event_id | STRING | 事件 ID（`E` 前缀） |
| user_id | STRING | 用户 |
| event_name | STRING | 事件名。枚举：page_view / button_click / doc_create / doc_edit / msg_send / msg_read / file_upload / file_download / search / share / comment / reaction / calendar_create / calendar_rsvp / video_join / video_leave / task_create / task_complete / approval_submit / approval_approve |
| event_time | TIMESTAMP | 事件发生时间 |
| page | STRING | 发生页面 |
| properties_json | STRING | 事件属性 JSON，不同事件有不同字段。常见：`doc_type`（docx/sheet/bitable/slides）、`msg_type`（text/image/file/sticker）、`experiment_id`（EXP001~EXP050） |

### C5. `fact_daily_usage` — 用户日度聚合
- 主键：`user_id` + `dt`
- 粒度：一行 = 一个用户一天的聚合行为
- 行数：~192 万

| 字段 | 类型 | 说明 |
|---|---|---|
| user_id | STRING | 用户 |
| dt | DATE | 日期 |
| session_count | INT | 当日会话数 |
| active_duration_sec | INT | 当日活跃时长（秒） |
| feature_usage_json | STRING | 当日功能使用 JSON，形如 `{"im": 12, "docs": 5}`，value = 顶层动作次数 |

### C6. `fact_feature_usage` — 功能使用明细
- 主键：`user_id` + `feature_key` + `dt`
- 粒度：一行 = 一个用户一个功能一天的使用
- 行数：~270 万

| 字段 | 类型 | 说明 |
|---|---|---|
| user_id | STRING | 用户 |
| feature_key | STRING | 功能 key |
| dt | DATE | 日期 |
| action_count | INT | 当日动作次数（包含子动作；与 `fact_daily_usage.feature_usage_json` 的同名维度口径不同，请按业务目标选择） |
| duration_sec | INT | 当日使用时长（秒） |

### C7. `fact_ai_usage_log` — AI 调用日志
- 主键：`log_id`
- 粒度：一行 = 一次 AI 操作（如 ai_query）
- 行数：~24.8 万

| 字段 | 类型 | 说明 |
|---|---|---|
| log_id | STRING | 日志 ID |
| user_id_hash | STRING | **哈希形式的 user_id**，需通过 `dim_user_id_mapping` 映射回明文 |
| operation_type | STRING | 操作类型。枚举：deduct（扣减）/ earn（赠送）/ refund（退还） |
| credits_amount | INT | 积分数量 |
| model_name | STRING | 模型名。枚举：gpt-4o / claude-3.5 / doubao-pro / moonshot-v1 / glm-4 |
| created_at | STRING | **unix epoch 秒**（字符串存储，如 `"1747449600"`），**不是** `YYYY-MM-DD HH:MM:SS` |
| remark | STRING | 备注 JSON。典型结构：`{"action": "ai_query", "model": "...", "cause": "model: X, input: N, output: M, cost: Y USD (credits)", "session_id": "..."}`。真实成本（input/output token、cost）嵌在 `cause` 文本中 |

---

## D. 协作与内容

### D1. `dim_channel` — 群聊/频道维度
- 主键：`channel_id`
- 粒度：一行 = 一个频道
- 行数：~5.6k

| 字段 | 类型 | 说明 |
|---|---|---|
| channel_id | STRING | 频道 ID，形如 `CH00001_03` |
| tenant_id | STRING | 所属企业 |
| channel_type | STRING | 枚举：group / topic / announcement / private |
| member_count | INT | 成员数 |
| created_at | TIMESTAMP | 创建时间 |

### D2. `fact_message` — 消息
- 主键：`msg_id`
- 粒度：一行 = 一条消息
- 行数：~31.5 万

| 字段 | 类型 | 说明 |
|---|---|---|
| msg_id | STRING | 消息 ID（`M` 前缀） |
| sender_id | STRING | 发送者 user_id |
| channel_id | STRING | 所在频道 |
| msg_type | STRING | 枚举：text / image / file / rich_text / sticker |
| sent_at | TIMESTAMP | 发送时间 |
| word_count | INT | 字数（仅文本类有意义） |

### D3. `fact_document` — 文档
- 主键：`doc_id`
- 粒度：一行 = 一个文档
- 行数：80,000

| 字段 | 类型 | 说明 |
|---|---|---|
| doc_id | STRING | 文档 ID（`DOC` 前缀） |
| creator_id | STRING | 创建者 user_id |
| tenant_id | STRING | 所属企业 |
| doc_type | STRING | 枚举：docx / sheet / bitable / slides / mindnote |
| created_at | TIMESTAMP | 创建时间 |
| last_edit_at | TIMESTAMP | 最近编辑时间 |
| edit_count | INT | 编辑次数 |

### D4. `fact_doc_collaboration` — 文档协作明细
- 粒度：一行 = 一个用户一天对一个文档的协作
- 行数：~9 万

| 字段 | 类型 | 说明 |
|---|---|---|
| doc_id | STRING | 文档 |
| user_id | STRING | 协作者 |
| dt | DATE | 日期 |
| edit_duration_sec | INT | 当日编辑时长（秒） |
| comment_count | INT | 当日评论数 |

---

## E. 计费与订阅

### E1. `dim_plan` — 套餐维度
- 主键：`plan_tier`
- 行数：5

| 字段 | 类型 | 说明 |
|---|---|---|
| plan_name | STRING | 展示名（Free / Starter / Professional / Business / Enterprise） |
| plan_tier | STRING | 等级 key（free / starter / pro / business / enterprise） |
| monthly_price | DOUBLE | 月度标价（USD / seat） |
| annual_price | DOUBLE | 年度标价（USD / seat） |
| seat_limit | STRING | 席位上限，数字或字符串 `"unlimited"` |
| features | STRING | 包含功能 key 列表（逗号分隔，或 `"all"`） |

### E2. `fact_subscription` — 订阅
- 主键：`sub_id`
- 粒度：一行 = 一个订阅周期
- 行数：~340

| 字段 | 类型 | 说明 |
|---|---|---|
| sub_id | STRING | 订阅 ID（`SUB` 前缀） |
| tenant_id | STRING | 企业 |
| plan_tier | STRING | 套餐等级 |
| start_date | DATE | 订阅开始 |
| end_date | DATE | 订阅结束 |
| mrr | DOUBLE | 月度经常性收入（Monthly Recurring Revenue），**确认收入口径**，与发票金额不一定相等 |
| status | STRING | 枚举：active / churned / trial |

### E3. `fact_invoice` — 发票
- 主键：`invoice_id`
- 粒度：一行 = 一张发票
- 行数：~1.2k

| 字段 | 类型 | 说明 |
|---|---|---|
| invoice_id | STRING | 发票 ID（`INV` 前缀） |
| tenant_id | STRING | 企业 |
| sub_id | STRING | 关联订阅 |
| amount | DOUBLE | 发票金额 |
| currency | STRING | 币种。枚举：USD / CNY / EUR / JPY |
| issued_at | TIMESTAMP | 开票时间 |
| paid_at | TIMESTAMP | 付款时间（仅 `status=paid` 有值） |
| status | STRING | 枚举：paid / pending / overdue / void |

### E4. `fact_payment` — 付款流水
- 主键：`payment_id`
- 粒度：一行 = 一次付款尝试。**一张发票可能对应多条付款记录**（尝试、部分支付、重试）
- 行数：~1.6k

| 字段 | 类型 | 说明 |
|---|---|---|
| payment_id | STRING | 付款 ID（`PAY` 前缀） |
| invoice_id | STRING | 关联发票 |
| method | STRING | 支付方式。枚举：credit_card / bank_transfer / alipay / wechat_pay |
| amount | DOUBLE | 本次付款金额（可能小于发票金额） |
| status | STRING | 枚举：success / failed / pending |
| paid_at | TIMESTAMP | 付款完成时间（失败/待处理为空） |

### E5. `fact_credit_usage` — 资源消耗流水
- 粒度：一行 = 一个租户一天对某资源的消耗
- 行数：~6.4 万

| 字段 | 类型 | 说

……后续内容因长度限制已截断。完整内容请查阅原始 DATA_DICTIONARY.md。
