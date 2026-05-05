#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
构建 recipe knowledge，并只写入通过验证的 recipe。

安全边界：
1. 只写入：
   - outputs/knowledge/retrieval_v2/recipes.json
   - outputs/knowledge/retrieval_v2/recipe_index.json
2. 对每条 candidate recipe 依次执行：
   - schema 验证
   - table / field 验证
   - SQL EXPLAIN 验证
3. 验证失败的 recipe 不写入最终文件，只打印 rejected reason

运行：
python src/03_8_build_recipe_knowledge.py
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, Iterable, List, Sequence, Tuple


ROOT = Path(__file__).resolve().parents[1]
DB_PATH = ROOT / "cloudwork.duckdb"
KNOWLEDGE_DIR = ROOT / "outputs" / "knowledge"
RETRIEVAL_DIR = KNOWLEDGE_DIR / "retrieval_v2"

TABLE_INDEX_PATH = RETRIEVAL_DIR / "table_index.json"
FIELD_INDEX_PATH = RETRIEVAL_DIR / "field_index.json"
JOIN_INDEX_PATH = RETRIEVAL_DIR / "join_index.json"
TABLE_CARDS_PATH = KNOWLEDGE_DIR / "table_cards.json"
COLUMN_CARDS_PATH = KNOWLEDGE_DIR / "column_cards.json"

RECIPES_OUTPUT_PATH = RETRIEVAL_DIR / "recipes.json"
RECIPE_INDEX_OUTPUT_PATH = RETRIEVAL_DIR / "recipe_index.json"

REQUIRED_RECIPE_FIELDS = [
    "recipe_id",
    "name",
    "intent",
    "canonical_question",
    "typical_questions",
    "description",
    "required_tables",
    "optional_tables",
    "required_fields",
    "optional_fields",
    "metrics",
    "dimensions",
    "join_paths",
    "grain",
    "sql_skeleton",
    "risks",
    "visualization",
    "confidence",
    "keywords",
]


def load_json(path: Path, default):
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default


def unique_list(items: Iterable[Any]) -> List[Any]:
    result = []
    seen = set()
    for item in items:
        marker = json.dumps(item, ensure_ascii=False, sort_keys=True) if isinstance(item, (dict, list)) else str(item)
        if marker in seen:
            continue
        seen.add(marker)
        result.append(item)
    return result


def flatten_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    if isinstance(value, (int, float, bool)):
        return str(value)
    if isinstance(value, dict):
        parts = []
        for k, v in value.items():
            parts.append(flatten_text(k))
            parts.append(flatten_text(v))
        return " ".join(x for x in parts if x)
    if isinstance(value, (list, tuple, set)):
        return " ".join(flatten_text(x) for x in value if flatten_text(x))
    return str(value)


def build_candidate_recipes() -> List[Dict[str, Any]]:
    return [
        {
            "recipe_id": "tenant_count_by_country",
            "name": "国家客户数量分布",
            "intent": "按国家统计客户数量并做分布对比",
            "canonical_question": "不同国家的客户数量是多少？请生成柱状图。",
            "typical_questions": [
                "不同国家的客户数量是多少？请生成柱状图。",
                "按国家看客户数分布是怎样的？",
                "请统计各国家客户数量，并画一个柱状图。",
            ],
            "description": "基于 dim_tenant 按 country 统计去重客户数，用于观察客户地域分布。",
            "required_tables": ["dim_tenant"],
            "optional_tables": [],
            "required_fields": ["dim_tenant.tenant_id", "dim_tenant.country"],
            "optional_fields": ["dim_tenant.name", "dim_tenant.industry", "dim_tenant.size_tier"],
            "metrics": [
                {
                    "name": "tenant_count",
                    "expression": "COUNT(DISTINCT tenant_id)",
                    "meaning": "去重客户数量",
                }
            ],
            "dimensions": ["country"],
            "join_paths": [],
            "grain": "按国家聚合；实体口径为 tenant 去重数。",
            "sql_skeleton": (
                "SELECT\n"
                "    country,\n"
                "    COUNT(DISTINCT tenant_id) AS tenant_count\n"
                "FROM dim_tenant\n"
                "GROUP BY country\n"
                "ORDER BY tenant_count DESC\n"
                "LIMIT 30;"
            ),
            "risks": [
                "统计客户数量时应使用 COUNT(DISTINCT tenant_id)，不要直接用 COUNT(*) 替代实体数。",
                "country 为空或标准不一致时，可能影响分布解读。",
            ],
            "visualization": {
                "chart_type": "bar",
                "x": "country",
                "y": "tenant_count",
                "reason": "适合做国家间类别对比。",
            },
            "confidence": "high",
            "keywords": ["国家", "客户数量", "tenant", "country", "柱状图", "分布"],
        },
        {
            "recipe_id": "monthly_new_user_trend",
            "name": "月度新注册用户趋势",
            "intent": "按月统计新注册用户趋势",
            "canonical_question": "每月新注册用户趋势如何？请生成折线图。",
            "typical_questions": [
                "每月新注册用户趋势如何？请生成折线图。",
                "新注册用户按月变化趋势怎么样？",
                "请按月统计新增注册用户，并画折线图。",
            ],
            "description": "基于 dim_user.register_at 统计每月新注册用户数量，用于观察新增趋势。",
            "required_tables": ["dim_user"],
            "optional_tables": [],
            "required_fields": ["dim_user.user_id", "dim_user.register_at"],
            "optional_fields": ["dim_user.status", "dim_user.tenant_id"],
            "metrics": [
                {
                    "name": "new_user_count",
                    "expression": "COUNT(DISTINCT user_id)",
                    "meaning": "每月新增注册用户数",
                }
            ],
            "dimensions": ["register_month"],
            "join_paths": [],
            "grain": "按月份聚合；实体口径为 user 去重数。",
            "sql_skeleton": (
                "SELECT\n"
                "    DATE_TRUNC('month', register_at) AS register_month,\n"
                "    COUNT(DISTINCT user_id) AS new_user_count\n"
                "FROM dim_user\n"
                "GROUP BY register_month\n"
                "ORDER BY register_month;"
            ),
            "risks": [
                "register_at 是注册时间，不代表激活时间或活跃时间。",
                "如需自然月口径，应注意时区与时间字段截断方式。",
            ],
            "visualization": {
                "chart_type": "line",
                "x": "register_month",
                "y": "new_user_count",
                "reason": "适合展示时间趋势。",
            },
            "confidence": "high",
            "keywords": ["月度", "新增用户", "注册", "趋势", "折线图"],
        },
        {
            "recipe_id": "mrr_by_plan_tier",
            "name": "套餐 MRR 对比",
            "intent": "按套餐统计活跃订阅 MRR",
            "canonical_question": "不同套餐的月度 MRR 对比如何？",
            "typical_questions": [
                "不同套餐的月度 MRR 对比如何？",
                "各套餐的 MRR 分布是怎样的？",
                "请统计活跃订阅在不同套餐下的 MRR 对比。",
            ],
            "description": "基于 fact_subscription 统计 active 订阅的订阅数与 MRR。",
            "required_tables": ["fact_subscription"],
            "optional_tables": ["dim_plan"],
            "required_fields": [
                "fact_subscription.sub_id",
                "fact_subscription.plan_tier",
                "fact_subscription.status",
                "fact_subscription.mrr",
            ],
            "optional_fields": ["dim_plan.plan_tier", "dim_plan.monthly_price"],
            "metrics": [
                {
                    "name": "active_subscription_count",
                    "expression": "COUNT(DISTINCT sub_id)",
                    "meaning": "活跃订阅数",
                },
                {
                    "name": "total_mrr",
                    "expression": "SUM(mrr)",
                    "meaning": "月度经常性收入",
                },
            ],
            "dimensions": ["plan_tier"],
            "join_paths": [],
            "grain": "按套餐聚合；订阅实体口径为 sub_id 去重数。",
            "sql_skeleton": (
                "SELECT\n"
                "    plan_tier,\n"
                "    COUNT(DISTINCT sub_id) AS active_subscription_count,\n"
                "    SUM(mrr) AS total_mrr\n"
                "FROM fact_subscription\n"
                "WHERE status = 'active'\n"
                "GROUP BY plan_tier\n"
                "ORDER BY total_mrr DESC;"
            ),
            "risks": [
                "MRR 是订阅口径，不等于实际回款或实收收入。",
                "只筛选 status='active' 时，要注意是否符合当前业务定义的有效订阅。",
                "不要把 COUNT(*) 误解为活跃订阅实体数，应使用 COUNT(DISTINCT sub_id)。",
            ],
            "visualization": {
                "chart_type": "bar",
                "x": "plan_tier",
                "y": "total_mrr",
                "reason": "适合做套餐间收入对比。",
            },
            "confidence": "high",
            "keywords": ["MRR", "套餐", "订阅", "plan_tier", "收入对比"],
        },
        {
            "recipe_id": "top_tenants_by_revenue",
            "name": "高收入客户 Top10",
            "intent": "按客户汇总实际收入并做排名",
            "canonical_question": "收入最高的前 10 个客户是谁？",
            "typical_questions": [
                "收入最高的前 10 个客户是谁？",
                "请找出总收入最高的 10 个客户。",
                "按实际收入排名，Top10 客户有哪些？",
            ],
            "description": "基于 fact_actual_revenue 按 tenant_id 聚合 actual_revenue 并排序。",
            "required_tables": ["fact_actual_revenue"],
            "optional_tables": ["dim_tenant"],
            "required_fields": ["fact_actual_revenue.tenant_id", "fact_actual_revenue.actual_revenue"],
            "optional_fields": ["dim_tenant.tenant_id", "dim_tenant.name", "fact_actual_revenue.currency"],
            "metrics": [
                {
                    "name": "total_revenue",
                    "expression": "SUM(actual_revenue)",
                    "meaning": "客户累计实际收入",
                }
            ],
            "dimensions": ["tenant_id"],
            "join_paths": [],
            "grain": "按 tenant_id 聚合收入。",
            "sql_skeleton": (
                "SELECT\n"
                "    tenant_id,\n"
                "    SUM(actual_revenue) AS total_revenue\n"
                "FROM fact_actual_revenue\n"
                "GROUP BY tenant_id\n"
                "ORDER BY total_revenue DESC\n"
                "LIMIT 10;"
            ),
            "risks": [
                "actual_revenue 与 MRR、发票金额、付款金额不是同一口径。",
                "如存在多币种，应确认 currency 是否需要统一换算。",
                "按 tenant_id 排名不等于按客户名称去重，名称展示需要额外确认字段可用性。",
            ],
            "visualization": {
                "chart_type": "bar",
                "x": "tenant_id",
                "y": "total_revenue",
                "reason": "适合做 TopN 排名对比。",
            },
            "confidence": "high",
            "keywords": ["收入", "客户", "top10", "revenue", "tenant_id", "排名"],
        },
        {
            "recipe_id": "daily_active_user_trend",
            "name": "每日活跃用户趋势",
            "intent": "按日统计活跃用户趋势",
            "canonical_question": "最近一段时间每日活跃用户数趋势如何？",
            "typical_questions": [
                "最近一段时间每日活跃用户数趋势如何？",
                "请看一下 DAU 的每日变化趋势。",
                "按天统计活跃用户数，并画趋势图。",
            ],
            "description": "基于 fact_daily_usage 按 dt 统计每日活跃用户数。",
            "required_tables": ["fact_daily_usage"],
            "optional_tables": [],
            "required_fields": ["fact_daily_usage.dt", "fact_daily_usage.user_id"],
            "optional_fields": ["fact_daily_usage.session_count", "fact_daily_usage.active_duration_sec"],
            "metrics": [
                {
                    "name": "dau",
                    "expression": "COUNT(DISTINCT user_id)",
                    "meaning": "每日活跃用户数",
                }
            ],
            "dimensions": ["dt"],
            "join_paths": [],
            "grain": "按 dt 聚合；实体口径为 user_id 去重数。",
            "sql_skeleton": (
                "SELECT\n"
                "    dt,\n"
                "    COUNT(DISTINCT user_id) AS dau\n"
                "FROM fact_daily_usage\n"
                "GROUP BY dt\n"
                "ORDER BY dt;"
            ),
            "risks": [
                "fact_daily_usage 是日汇总行为表，活跃口径依赖其上游定义。",
                "如果要限定最近一段时间，应在使用时补充时间过滤条件。",
            ],
            "visualization": {
                "chart_type": "line",
                "x": "dt",
                "y": "dau",
                "reason": "适合展示按天趋势。",
            },
            "confidence": "high",
            "keywords": ["DAU", "每日活跃", "趋势", "dt", "折线图"],
        },
        {
            "recipe_id": "ai_credit_operation_distribution",
            "name": "AI Credits 操作类型分布",
            "intent": "分析 AI credits 不同操作类型的事件数与额度分布",
            "canonical_question": "AI credits 的 Deduct、Earn 和 Refund 分布如何？",
            "typical_questions": [
                "AI credits 的 Deduct、Earn 和 Refund 分布如何？",
                "不同 operation_type 下的 AI credits 事件分布是怎样的？",
                "请统计 AI credits 操作类型分布，并做柱状图对比。",
            ],
            "description": "基于 fact_ai_usage_log 按 operation_type 统计事件数与额度总量。",
            "required_tables": ["fact_ai_usage_log"],
            "optional_tables": [],
            "required_fields": ["fact_ai_usage_log.operation_type", "fact_ai_usage_log.credits_amount"],
            "optional_fields": ["fact_ai_usage_log.created_at", "fact_ai_usage_log.model_name"],
            "metrics": [
                {
                    "name": "event_count",
                    "expression": "COUNT(*)",
                    "meaning": "事件条数",
                },
                {
                    "name": "total_credits",
                    "expression": "SUM(credits_amount)",
                    "meaning": "credits 额度总和",
                },
            ],
            "dimensions": ["operation_type"],
            "join_paths": [],
            "grain": "按操作类型聚合；记录口径为日志事件。",
            "sql_skeleton": (
                "SELECT\n"
                "    operation_type,\n"
                "    COUNT(*) AS event_count,\n"
                "    SUM(credits_amount) AS total_credits\n"
                "FROM fact_ai_usage_log\n"
                "GROUP BY operation_type\n"
                "ORDER BY event_count DESC;"
            ),
            "risks": [
                "Deduct、Earn、Refund 不能简单混为净消耗，必须结合 operation_type 单独解释。",
                "COUNT(*) 统计的是日志事件数，不等于用户数或客户数。",
                "credits_amount 的正负方向需结合业务定义解释。",
            ],
            "visualization": {
                "chart_type": "bar",
                "x": "operation_type",
                "y": "event_count",
                "reason": "适合做操作类型分布对比。",
            },
            "confidence": "high",
            "keywords": ["AI credits", "Deduct", "Earn", "Refund", "operation_type", "分布"],
        },
        {
            "recipe_id": "feature_usage_ranking",
            "name": "功能使用排行",
            "intent": "按功能统计使用量并做 TopN 排名",
            "canonical_question": "使用最多的功能有哪些？",
            "typical_questions": [
                "使用最多的功能有哪些？",
                "功能使用量排名是怎样的？",
                "请统计 feature usage 排行，并展示 Top30。",
            ],
            "description": "基于 fact_feature_usage 按 feature_key 统计使用事件量。",
            "required_tables": ["fact_feature_usage"],
            "optional_tables": ["dim_feature"],
            "required_fields": ["fact_feature_usage.feature_key"],
            "optional_fields": ["fact_feature_usage.action_count", "fact_feature_usage.duration_sec", "dim_feature.feature_key"],
            "metrics": [
                {
                    "name": "usage_count",
                    "expression": "COUNT(*)",
                    "meaning": "功能使用记录数",
                }
            ],
            "dimensions": ["feature_key"],
            "join_paths": [],
            "grain": "按 feature_key 聚合；记录口径为功能使用事件。",
            "sql_skeleton": (
                "SELECT\n"
                "    feature_key,\n"
                "    COUNT(*) AS usage_count\n"
                "FROM fact_feature_usage\n"
                "GROUP BY feature_key\n"
                "ORDER BY usage_count DESC\n"
                "LIMIT 30;"
            ),
            "risks": [
                "COUNT(*) 统计的是使用事件，不一定等于使用用户数。",
                "如果不同功能事件上报频率不同，直接比较次数可能有偏差。",
            ],
            "visualization": {
                "chart_type": "bar",
                "x": "feature_key",
                "y": "usage_count",
                "reason": "适合做功能 TopN 排名。",
            },
            "confidence": "high",
            "keywords": ["功能", "使用量", "feature_key", "ranking", "Top30"],
        },
        {
            "recipe_id": "active_subscription_by_plan",
            "name": "套餐活跃订阅数",
            "intent": "按套餐统计活跃订阅数量",
            "canonical_question": "不同套餐的活跃订阅数量是多少？",
            "typical_questions": [
                "不同套餐的活跃订阅数量是多少？",
                "各套餐当前 active subscription 数量如何？",
                "请统计不同 plan_tier 的活跃订阅数。",
            ],
            "description": "基于 fact_subscription 按 plan_tier 统计 active 订阅数。",
            "required_tables": ["fact_subscription"],
            "optional_tables": ["dim_plan"],
            "required_fields": ["fact_subscription.sub_id", "fact_subscription.plan_tier", "fact_subscription.status"],
            "optional_fields": ["dim_plan.plan_tier"],
            "metrics": [
                {
                    "name": "active_subscription_count",
                    "expression": "COUNT(DISTINCT sub_id)",
                    "meaning": "活跃订阅数量",
                }
            ],
            "dimensions": ["plan_tier"],
            "join_paths": [],
            "grain": "按套餐聚合；订阅实体口径为 sub_id 去重数。",
            "sql_skeleton": (
                "SELECT\n"
                "    plan_tier,\n"
                "    COUNT(DISTINCT sub_id) AS active_subscription_count\n"
                "FROM fact_subscription\n"
                "WHERE status = 'active'\n"
                "GROUP BY plan_tier\n"
                "ORDER BY active_subscription_count DESC;"
            ),
            "risks": [
                "COUNT(DISTINCT sub_id) 才是订阅实体口径，不能用 COUNT(*) 代替。",
                "status='active' 是否等同业务上的有效订阅，需要结合业务定义确认。",
            ],
            "visualization": {
                "chart_type": "bar",
                "x": "plan_tier",
                "y": "active_subscription_count",
                "reason": "适合做套餐间订阅数对比。",
            },
            "confidence": "high",
            "keywords": ["活跃订阅", "套餐", "plan_tier", "subscription", "数量"],
        },
        {
            "recipe_id": "ai_credit_usage_by_country",
            "name": "国家维度 AI Credits 使用量",
            "intent": "按国家统计 AI credits 使用量",
            "canonical_question": "不同国家的 AI credits 使用量是多少？",
            "typical_questions": [
                "不同国家的 AI credits 使用量是多少？",
                "按国家看 AI credits 消耗分布如何？",
                "请统计各国家 AI credits 使用量，并生成柱状图。",
            ],
            "description": "尝试将 AI credits 使用日志关联到国家维度并汇总使用量。",
            "required_tables": ["fact_ai_usage_log", "dim_tenant"],
            "optional_tables": [],
            "required_fields": [
                "fact_ai_usage_log.tenant_id",
                "fact_ai_usage_log.credits",
                "dim_tenant.tenant_id",
                "dim_tenant.country",
            ],
            "optional_fields": [],
            "metrics": [
                {
                    "name": "total_credits",
                    "expression": "SUM(a.credits)",
                    "meaning": "国家维度 AI credits 使用量",
                }
            ],
            "dimensions": ["country"],
            "join_paths": ["fact_ai_usage_log.tenant_id = dim_tenant.tenant_id"],
            "grain": "按国家聚合 AI credits 使用量。",
            "sql_skeleton": (
                "SELECT\n"
                "    t.country,\n"
                "    SUM(a.credits) AS total_credits\n"
                "FROM fact_ai_usage_log a\n"
                "JOIN dim_tenant t\n"
                "    ON a.tenant_id = t.tenant_id\n"
                "GROUP BY t.country\n"
                "ORDER BY total_credits DESC\n"
                "LIMIT 30;"
            ),
            "risks": [
                "JOIN 路径如果字段不存在或粒度不明确，应直接拒绝该 recipe。",
                "Deduct / Earn / Refund 混合汇总可能不能直接代表净消耗。",
                "按国家统计前必须确认 credits 与 tenant 的映射关系可信。",
            ],
            "visualization": {
                "chart_type": "bar",
                "x": "country",
                "y": "total_credits",
                "reason": "适合做国家维度对比。",
            },
            "confidence": "low",
            "keywords": ["AI credits", "国家", "country", "使用量", "分布"],
        },
        {
            "recipe_id": "ai_usage_outlier_tenants",
            "name": "AI Credits 异常高使用客户",
            "intent": "识别 AI credits 使用量显著高于均值的客户",
            "canonical_question": "哪些客户的 AI credits 使用量异常偏高？",
            "typical_questions": [
                "哪些客户的 AI credits 使用量异常偏高？",
                "请找出 AI credits 消耗显著异常的客户。",
                "哪些 tenant 的 AI credits 使用量明显高于平均水平？",
            ],
            "description": "尝试基于均值与标准差识别 AI credits 使用异常高的客户。",
            "required_tables": ["fact_ai_usage_log"],
            "optional_tables": [],
            "required_fields": ["fact_ai_usage_log.tenant_id", "fact_ai_usage_log.credits"],
            "optional_fields": [],
            "metrics": [
                {
                    "name": "total_credits",
                    "expression": "SUM(credits)",
                    "meaning": "客户累计 AI credits 使用量",
                }
            ],
            "dimensions": ["tenant_id"],
            "join_paths": [],
            "grain": "先按 tenant 聚合，再做异常值识别。",
            "sql_skeleton": (
                "WITH tenant_usage AS (\n"
                "    SELECT\n"
                "        tenant_id,\n"
                "        SUM(credits) AS total_credits\n"
                "    FROM fact_ai_usage_log\n"
                "    GROUP BY tenant_id\n"
                "),\n"
                "stats AS (\n"
                "    SELECT\n"
                "        AVG(total_credits) AS avg_credits,\n"
                "        STDDEV_SAMP(total_credits) AS std_credits\n"
                "    FROM tenant_usage\n"
                ")\n"
                "SELECT\n"
                "    u.tenant_id,\n"
                "    u.total_credits,\n"
                "    s.avg_credits,\n"
                "    s.std_credits\n"
                "FROM tenant_usage u\n"
                "CROSS JOIN stats s\n"
                "WHERE u.total_credits > s.avg_credits + 2 * s.std_credits\n"
                "ORDER BY u.total_credits DESC;"
            ),
            "risks": [
                "异常值只是统计异常，不代表一定是业务风险或作弊行为。",
                "如果 tenant 字段不存在或映射关系不确定，应拒绝该 recipe。",
                "credits 的定义如果包含 Earn / Refund，异常解释会失真。",
            ],
            "visualization": {
                "chart_type": "table_only",
                "x": "",
                "y": "",
                "reason": "异常客户列表更适合表格呈现。",
            },
            "confidence": "low",
            "keywords": ["异常值", "AI credits", "tenant", "outlier", "均值", "标准差"],
        },
        {
            "recipe_id": "ai_credit_usage_by_country_via_user_mapping",
            "name": "通过用户映射的国家维度 AI Credits 使用量",
            "intent": "通过 user_id_hash 映射链路，将 AI usage log 归属到国家维度后统计 credits 使用量",
            "canonical_question": "不同国家的 AI credits 使用量是多少？",
            "typical_questions": [
                "不同国家的 AI credits 使用量是多少？",
                "如果通过用户映射到客户国家，各国家的 AI credits 使用量如何？",
                "请基于 user mapping 统计各国家 AI credits 使用量，并做柱状图。",
            ],
            "description": "使用 fact_ai_usage_log -> dim_user_id_mapping -> dim_user -> dim_tenant 的映射路径，将日志归属到国家后汇总 AI credits 使用量。",
            "required_tables": ["fact_ai_usage_log", "dim_user_id_mapping", "dim_user", "dim_tenant"],
            "optional_tables": [],
            "required_fields": [
                "fact_ai_usage_log.user_id_hash",
                "fact_ai_usage_log.operation_type",
                "fact_ai_usage_log.credits_amount",
                "dim_user_id_mapping.user_id_hash",
                "dim_user_id_mapping.user_id",
                "dim_user.user_id",
                "dim_user.tenant_id",
                "dim_tenant.tenant_id",
                "dim_tenant.country",
            ],
            "optional_fields": ["fact_ai_usage_log.created_at", "dim_tenant.name"],
            "metrics": [
                {
                    "name": "total_credit_amount",
                    "expression": "SUM(a.credits_amount)",
                    "meaning": "国家维度 AI credits 总量",
                }
            ],
            "dimensions": ["country"],
            "join_paths": [
                "fact_ai_usage_log.user_id_hash = dim_user_id_mapping.user_id_hash",
                "dim_user_id_mapping.user_id = dim_user.user_id",
                "dim_user.tenant_id = dim_tenant.tenant_id",
            ],
            "grain": "先通过 user_id_hash 找到 user_id，再找到 tenant_id，最后按国家聚合 AI credits 使用量。",
            "sql_skeleton": (
                "SELECT\n"
                "    t.country,\n"
                "    SUM(a.credits_amount) AS total_credit_amount\n"
                "FROM fact_ai_usage_log a\n"
                "JOIN dim_user_id_mapping m\n"
                "    ON a.user_id_hash = m.user_id_hash\n"
                "JOIN dim_user u\n"
                "    ON m.user_id = u.user_id\n"
                "JOIN dim_tenant t\n"
                "    ON u.tenant_id = t.tenant_id\n"
                "GROUP BY t.country\n"
                "ORDER BY total_credit_amount DESC\n"
                "LIMIT 30;"
            ),
            "risks": [
                "fact_ai_usage_log 没有 tenant_id，不能直接按 tenant 或 country 聚合。",
                "必须通过 dim_user_id_mapping 和 dim_user 建立 usage log 到 tenant 的映射。",
                "映射表可能有缺失，导致部分日志无法归属到客户或国家。",
                "credits_amount 需要结合 operation_type 解释，Deduct / Earn / Refund 不能简单混为净消耗。",
            ],
            "visualization": {
                "chart_type": "bar",
                "x": "country",
                "y": "total_credit_amount",
                "reason": "适合做国家维度使用量对比。",
            },
            "confidence": "medium",
            "keywords": ["AI credits", "国家", "country", "user mapping", "credits_amount", "使用量"],
        },
        {
            "recipe_id": "ai_usage_outlier_tenants_via_user_mapping",
            "name": "通过用户映射的 AI Credits 异常客户识别",
            "intent": "通过 user_id_hash 映射到 tenant 后识别 AI credits 使用量异常高的客户",
            "canonical_question": "哪些客户的 AI credits 使用量异常偏高？",
            "typical_questions": [
                "哪些客户的 AI credits 使用量异常偏高？",
                "如果通过 user mapping 归属到客户，哪些 tenant 的 AI credits 使用异常高？",
                "请基于用户映射识别 AI credits 使用量明显高于平均水平的客户。",
            ],
            "description": "先通过 user mapping 找到 tenant_id，再按 tenant 聚合 AI credits 总量，并使用均值 + 2 倍标准差识别异常高使用客户。",
            "required_tables": ["fact_ai_usage_log", "dim_user_id_mapping", "dim_user"],
            "optional_tables": ["dim_tenant"],
            "required_fields": [
                "fact_ai_usage_log.user_id_hash",
                "fact_ai_usage_log.operation_type",
                "fact_ai_usage_log.credits_amount",
                "dim_user_id_mapping.user_id_hash",
                "dim_user_id_mapping.user_id",
                "dim_user.user_id",
                "dim_user.tenant_id",
            ],
            "optional_fields": ["dim_tenant.tenant_id", "dim_tenant.country", "fact_ai_usage_log.created_at"],
            "metrics": [
                {
                    "name": "total_credit_amount",
                    "expression": "SUM(a.credits_amount)",
                    "meaning": "客户累计 AI credits 使用量",
                },
                {
                    "name": "avg_credit_amount",
                    "expression": "AVG(total_credit_amount)",
                    "meaning": "客户级平均 AI credits 使用量",
                },
            ],
            "dimensions": ["tenant_id"],
            "join_paths": [
                "fact_ai_usage_log.user_id_hash = dim_user_id_mapping.user_id_hash",
                "dim_user_id_mapping.user_id = dim_user.user_id",
            ],
            "grain": "先按 tenant_id 聚合 credits 总量，再基于整体分布识别异常高使用客户。",
            "sql_skeleton": (
                "WITH tenant_usage AS (\n"
                "    SELECT\n"
                "        u.tenant_id,\n"
                "        SUM(a.credits_amount) AS total_credit_amount\n"
                "    FROM fact_ai_usage_log a\n"
                "    JOIN dim_user_id_mapping m\n"
                "        ON a.user_id_hash = m.user_id_hash\n"
                "    JOIN dim_user u\n"
                "        ON m.user_id = u.user_id\n"
                "    GROUP BY u.tenant_id\n"
                "),\n"
                "stats AS (\n"
                "    SELECT\n"
                "        AVG(total_credit_amount) AS avg_credit_amount,\n"
                "        STDDEV_SAMP(total_credit_amount) AS std_credit_amount\n"
                "    FROM tenant_usage\n"
                ")\n"
                "SELECT\n"
                "    u.tenant_id,\n"
                "    u.total_credit_amount,\n"
                "    s.avg_credit_amount,\n"
                "    s.std_credit_amount\n"
                "FROM tenant_usage u\n"
                "CROSS JOIN stats s\n"
                "WHERE u.total_credit_amount > s.avg_credit_amount + 2 * s.std_credit_amount\n"
                "ORDER BY u.total_credit_amount DESC\n"
                "LIMIT 30;"
            ),
            "risks": [
                "fact_ai_usage_log 没有 tenant_id，不能直接按 tenant 或 country 聚合。",
                "必须通过 dim_user_id_mapping 和 dim_user 建立 usage log 到 tenant 的映射。",
                "映射表可能有缺失，导致部分日志无法归属到客户或国家。",
                "credits_amount 需要结合 operation_type 解释，Deduct / Earn / Refund 不能简单混为净消耗。",
                "outlier 只是统计异常，不代表真实业务风险，需要结合时间窗口、客户规模和操作类型进一步判断。",
            ],
            "visualization": {
                "chart_type": "table_only",
                "x": "",
                "y": "",
                "reason": "异常客户列表更适合表格和阈值说明一起展示。",
            },
            "confidence": "medium",
            "keywords": ["AI credits", "outlier", "tenant", "user mapping", "异常客户", "credits_amount"],
        },
    ]


def _extract_table_name(item: Dict[str, Any]) -> str:
    for key in ("table_name", "table", "name"):
        value = item.get(key)
        if isinstance(value, str) and value:
            return value
    return ""


def _extract_field_name(item: Dict[str, Any]) -> str:
    for key in ("field_name", "column_name", "name"):
        value = item.get(key)
        if isinstance(value, str) and value:
            return value
    return ""


def collect_existing_tables_and_fields(field_index, table_index, column_cards):
    existing_tables: set[str] = set()
    existing_fields: set[Tuple[str, str]] = set()

    if isinstance(table_index, list):
        for item in table_index:
            if not isinstance(item, dict):
                continue
            table_name = _extract_table_name(item)
            if table_name:
                existing_tables.add(table_name)

    if isinstance(field_index, list):
        for item in field_index:
            if not isinstance(item, dict):
                continue
            table_name = _extract_table_name(item)
            field_name = _extract_field_name(item)
            if table_name:
                existing_tables.add(table_name)
            if table_name and field_name:
                existing_fields.add((table_name, field_name))

    if isinstance(column_cards, list):
        for item in column_cards:
            if not isinstance(item, dict):
                continue
            table_name = _extract_table_name(item)
            field_name = _extract_field_name(item)
            if table_name:
                existing_tables.add(table_name)
            if table_name and field_name:
                existing_fields.add((table_name, field_name))

    if isinstance(column_cards, dict):
        for table_name, value in column_cards.items():
            if isinstance(table_name, str) and table_name:
                existing_tables.add(table_name)

            columns = []
            if isinstance(value, list):
                columns = value
            elif isinstance(value, dict):
                columns = value.get("columns") or value.get("fields") or []

            for col in columns:
                if isinstance(col, dict):
                    field_name = _extract_field_name(col)
                    if table_name and field_name:
                        existing_fields.add((table_name, field_name))
                elif isinstance(col, str):
                    existing_fields.add((table_name, col))

    return existing_tables, existing_fields


def validate_recipe_schema(recipe):
    for field in REQUIRED_RECIPE_FIELDS:
        if field not in recipe:
            return False, f"缺少必填字段: {field}"

    if not isinstance(recipe.get("canonical_question"), str) or not recipe["canonical_question"].strip():
        return False, "canonical_question 必须是非空字符串"

    typical_questions = recipe.get("typical_questions")
    if not isinstance(typical_questions, list) or len(typical_questions) < 3:
        return False, "typical_questions 必须是至少包含 3 个问题的非空 list"
    if not all(isinstance(x, str) and x.strip() for x in typical_questions):
        return False, "typical_questions 中所有元素必须是非空字符串"

    if not isinstance(recipe.get("sql_skeleton"), str) or not recipe["sql_skeleton"].strip():
        return False, "sql_skeleton 必须是非空字符串"

    return True, "ok"


def validate_recipe_fields(recipe, existing_tables, existing_fields):
    for table_name in recipe.get("required_tables", []):
        if table_name not in existing_tables:
            return False, f"required_table 不存在: {table_name}"

    for table_name in recipe.get("optional_tables", []):
        if table_name not in existing_tables:
            print(f"[warning] recipe_id={recipe['recipe_id']} optional_table 不存在: {table_name}")

    required_tables = set(recipe.get("required_tables", []))

    for field in recipe.get("required_fields", []):
        if "." in field:
            table_name, field_name = field.split(".", 1)
            if (table_name, field_name) not in existing_fields:
                return False, f"required_field 不存在: {field}"
        else:
            found = any((table_name, field) in existing_fields for table_name in required_tables)
            if not found:
                return False, f"required_field 在 required_tables 中不存在: {field}"

    return True, "ok"


def validate_recipe_sql(recipe, db_path: Path):
    if not db_path.exists():
        print(f"[warning] 数据库不存在，跳过 SQL EXPLAIN 验证: {db_path}")
        return True, "db_missing_skip"

    try:
        import duckdb  # type: ignore
    except Exception as exc:
        return False, f"duckdb 不可用，无法验证 SQL: {exc}"

    sql = recipe.get("sql_skeleton", "").strip()
    if not sql:
        return False, "sql_skeleton 为空"

    conn = None
    try:
        conn = duckdb.connect(str(db_path), read_only=True)
        conn.execute(f"EXPLAIN {sql}")
        return True, "ok"
    except Exception as exc:
        return False, f"SQL EXPLAIN 失败: {exc}"
    finally:
        if conn is not None:
            conn.close()


def filter_valid_recipes(candidates, existing_tables, existing_fields, db_path):
    valid_recipes = []
    rejected_recipes = []

    for recipe in candidates:
        ok, reason = validate_recipe_schema(recipe)
        if not ok:
            rejected_recipes.append({"recipe_id": recipe.get("recipe_id", ""), "reason": reason})
            continue

        ok, reason = validate_recipe_fields(recipe, existing_tables, existing_fields)
        if not ok:
            rejected_recipes.append({"recipe_id": recipe.get("recipe_id", ""), "reason": reason})
            continue

        ok, reason = validate_recipe_sql(recipe, db_path)
        if not ok:
            rejected_recipes.append({"recipe_id": recipe.get("recipe_id", ""), "reason": reason})
            continue

        valid_recipes.append(recipe)

    return valid_recipes, rejected_recipes


def _extract_metric_tokens(metrics: Sequence[Any]) -> List[str]:
    tokens: List[str] = []
    for metric in metrics:
        if isinstance(metric, dict):
            for key in ("name", "expression", "meaning"):
                value = metric.get(key)
                if value:
                    tokens.append(str(value))
        elif metric:
            tokens.append(str(metric))
    return tokens


def build_recipe_index(valid_recipes):
    index = []
    for recipe in valid_recipes:
        tables = unique_list(list(recipe.get("required_tables", [])) + list(recipe.get("optional_tables", [])))
        fields = unique_list(list(recipe.get("required_fields", [])) + list(recipe.get("optional_fields", [])))
        metrics = _extract_metric_tokens(recipe.get("metrics", []))
        searchable_parts = [
            recipe.get("name", ""),
            recipe.get("intent", ""),
            recipe.get("canonical_question", ""),
            flatten_text(recipe.get("typical_questions", [])),
            recipe.get("description", ""),
            flatten_text(tables),
            flatten_text(fields),
            flatten_text(metrics),
            flatten_text(recipe.get("dimensions", [])),
            flatten_text(recipe.get("keywords", [])),
            flatten_text(recipe.get("risks", [])),
        ]
        searchable_text = " ".join(part for part in searchable_parts if part).strip()

        index.append(
            {
                "recipe_id": recipe["recipe_id"],
                "name": recipe["name"],
                "intent": recipe["intent"],
                "canonical_question": recipe["canonical_question"],
                "tables": tables,
                "fields": fields,
                "metrics": metrics,
                "keywords": unique_list(recipe.get("keywords", [])),
                "searchable_text": searchable_text,
                "confidence": recipe["confidence"],
            }
        )

    return index


def write_json(path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(data, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )


def main():
    table_index = load_json(TABLE_INDEX_PATH, [])
    field_index = load_json(FIELD_INDEX_PATH, [])
    _ = load_json(JOIN_INDEX_PATH, [])
    _ = load_json(TABLE_CARDS_PATH, [])
    column_cards = load_json(COLUMN_CARDS_PATH, [])

    existing_tables, existing_fields = collect_existing_tables_and_fields(
        field_index=field_index,
        table_index=table_index,
        column_cards=column_cards,
    )

    candidates = build_candidate_recipes()
    valid_recipes, rejected_recipes = filter_valid_recipes(
        candidates=candidates,
        existing_tables=existing_tables,
        existing_fields=existing_fields,
        db_path=DB_PATH,
    )
    recipe_index = build_recipe_index(valid_recipes)

    write_json(RECIPES_OUTPUT_PATH, valid_recipes)
    write_json(RECIPE_INDEX_OUTPUT_PATH, recipe_index)

    print(f"candidate_count={len(candidates)}")
    print(f"valid_count={len(valid_recipes)}")
    print(f"rejected_count={len(rejected_recipes)}")
    for item in rejected_recipes:
        print(f"rejected recipe_id={item['recipe_id']} reason={item['reason']}")
    print(f"recipes_path={RECIPES_OUTPUT_PATH}")
    print(f"recipe_index_path={RECIPE_INDEX_OUTPUT_PATH}")


if __name__ == "__main__":
    main()
