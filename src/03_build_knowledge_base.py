#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
功能：
构建 CloudWork 离线知识库第一版。

本脚本读取：
1. outputs/profiles/table_profiles.json
2. outputs/profiles/column_profiles.json
3. outputs/sample/question_1/query.sql + insight.md
4. outputs/sample/question_2/query.sql + insight.md
5. outputs/sample/question_3/query.sql + insight.md
6. for_contestants/DATA_DICTIONARY.md

生成：
1. outputs/knowledge/schema_raw.json
2. outputs/knowledge/table_cards.json
3. outputs/knowledge/column_cards.json
4. outputs/knowledge/dq_rules.json
5. outputs/knowledge/grain_rules.json
6. outputs/knowledge/recipes.json
7. outputs/knowledge/kb_summary.json

运行前请先执行：
python src/00_load_data.py
python src/01_profile_tables.py
python src/02_sample_questions.py

运行方式：
python src/03_build_knowledge_base.py
"""

import json
from pathlib import Path
from typing import Any, Dict, List, Optional


# =========================
# 路径配置
# =========================

PROJECT_ROOT = Path(__file__).resolve().parents[1]

PROFILE_DIR = PROJECT_ROOT / "outputs" / "profiles"
SAMPLE_DIR = PROJECT_ROOT / "outputs" / "sample"
KNOWLEDGE_DIR = PROJECT_ROOT / "outputs" / "knowledge"
FOR_CONTESTANTS_DIR = PROJECT_ROOT / "for_contestants"

TABLE_PROFILE_PATH = PROFILE_DIR / "table_profiles.json"
COLUMN_PROFILE_PATH = PROFILE_DIR / "column_profiles.json"
DATA_DICTIONARY_PATH = FOR_CONTESTANTS_DIR / "DATA_DICTIONARY.md"

SCHEMA_RAW_PATH = KNOWLEDGE_DIR / "schema_raw.json"
TABLE_CARDS_PATH = KNOWLEDGE_DIR / "table_cards.json"
COLUMN_CARDS_PATH = KNOWLEDGE_DIR / "column_cards.json"
DQ_RULES_PATH = KNOWLEDGE_DIR / "dq_rules.json"
GRAIN_RULES_PATH = KNOWLEDGE_DIR / "grain_rules.json"
RECIPES_PATH = KNOWLEDGE_DIR / "recipes.json"
KB_SUMMARY_PATH = KNOWLEDGE_DIR / "kb_summary.json"


# =========================
# 通用工具函数
# =========================

def ensure_dir(path: Path) -> None:
    """确保目录存在。"""
    path.mkdir(parents=True, exist_ok=True)


def read_json(path: Path) -> Any:
    """读取 JSON 文件。"""
    if not path.exists():
        raise FileNotFoundError(f"文件不存在：{path}")

    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def write_json(data: Any, path: Path) -> None:
    """写入 JSON 文件。"""
    ensure_dir(path.parent)

    with path.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    print(f"已生成：{path}")


def read_text_if_exists(path: Path) -> str:
    """如果文件存在，则读取文本；否则返回空字符串。"""
    if not path.exists():
        return ""

    return path.read_text(encoding="utf-8")


def get_columns_for_table(
    column_profiles: List[Dict[str, Any]],
    table_name: str,
) -> List[Dict[str, Any]]:
    """从字段级 profile 中筛选某张表的字段。"""
    return [
        item for item in column_profiles
        if item.get("table_name") == table_name
    ]


def get_possible_primary_keys(
    table_column_profiles: List[Dict[str, Any]],
) -> List[str]:
    """根据字段级 profile 获取可能的主键字段。"""
    return [
        item["column_name"]
        for item in table_column_profiles
        if item.get("is_possible_primary_key") is True
    ]


def get_high_null_columns(
    table_column_profiles: List[Dict[str, Any]],
    threshold: float = 0.2,
) -> List[Dict[str, Any]]:
    """获取高空值字段。"""
    result = []

    for item in table_column_profiles:
        null_rate = item.get("null_rate", 0)
        if null_rate is not None and null_rate >= threshold:
            result.append(
                {
                    "column_name": item.get("column_name"),
                    "null_rate": null_rate,
                    "null_count": item.get("null_count"),
                }
            )

    return result


def get_enum_like_columns(
    table_column_profiles: List[Dict[str, Any]],
    max_distinct_count: int = 20,
) -> List[Dict[str, Any]]:
    """
    获取疑似枚举字段。
    注意：这是候选判断，不代表业务上一定是枚举。
    """
    result = []

    for item in table_column_profiles:
        distinct_count = item.get("distinct_count", 0)
        row_count = item.get("row_count", 0)
        column_name = item.get("column_name")
        sample_values = item.get("sample_values", [])

        if row_count > 0 and 1 < distinct_count <= max_distinct_count:
            result.append(
                {
                    "column_name": column_name,
                    "distinct_count": distinct_count,
                    "sample_values": sample_values,
                }
            )

    return result


def guess_date_columns(
    table_column_profiles: List[Dict[str, Any]],
) -> List[str]:
    """根据字段类型和字段名猜测日期/时间字段。"""
    result = []

    for item in table_column_profiles:
        column_name = item.get("column_name", "")
        data_type = item.get("data_type", "")

        lower_name = column_name.lower()
        upper_type = data_type.upper()

        if (
            "DATE" in upper_type
            or "TIMESTAMP" in upper_type
            or lower_name in {"dt", "month"}
            or lower_name.endswith("_at")
            or lower_name.endswith("_date")
            or lower_name.endswith("_time")
        ):
            result.append(column_name)

    return result


def guess_measure_columns(
    table_column_profiles: List[Dict[str, Any]],
) -> List[str]:
    """根据字段类型猜测数值指标字段。"""
    numeric_types = {"BIGINT", "INTEGER", "INT", "DOUBLE", "FLOAT", "DECIMAL", "HUGEINT"}

    result = []

    for item in table_column_profiles:
        column_name = item.get("column_name", "")
        data_type = item.get("data_type", "").upper()

        if any(t in data_type for t in numeric_types):
            # 排除明显 ID / 编码字段
            lower_name = column_name.lower()
            if lower_name.endswith("_id") or lower_name in {"id", "role"}:
                continue
            result.append(column_name)

    return result


def guess_dimension_columns(
    table_column_profiles: List[Dict[str, Any]],
) -> List[str]:
    """根据字段类型和 distinct 情况猜测维度字段。"""
    result = []

    for item in table_column_profiles:
        column_name = item.get("column_name", "")
        data_type = item.get("data_type", "").upper()
        distinct_count = item.get("distinct_count", 0)

        if "VARCHAR" in data_type and distinct_count <= 1000:
            result.append(column_name)

    return result


# =========================
# 1. schema_raw 构建
# =========================

def build_schema_raw(table_profiles: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    从 table_profiles.json 中抽取纯 schema 信息。
    只保留表名、字段名、字段类型，不加入业务解释。
    """
    schema_raw = []

    for table in table_profiles:
        schema_raw.append(
            {
                "table_name": table.get("table_name"),
                "columns": table.get("columns", []),
            }
        )

    return schema_raw


# =========================
# 2. column_cards 构建
# =========================

def build_column_cards(
    column_profiles: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """
    字段卡片第一版。
    基本等同于 column_profiles，但加上 card_type，便于后续检索。
    """
    column_cards = []

    for item in column_profiles:
        column_cards.append(
            {
                "card_type": "column_card",
                "table_name": item.get("table_name"),
                "column_name": item.get("column_name"),
                "data_type": item.get("data_type"),
                "row_count": item.get("row_count"),
                "null_count": item.get("null_count"),
                "null_rate": item.get("null_rate"),
                "distinct_count": item.get("distinct_count"),
                "distinct_rate": item.get("distinct_rate"),
                "min": item.get("min"),
                "max": item.get("max"),
                "sample_values": item.get("sample_values", []),
                "is_possible_primary_key": item.get("is_possible_primary_key"),
            }
        )

    return column_cards


# =========================
# 3. table_cards 构建
# =========================

def build_table_cards(
    table_profiles: List[Dict[str, Any]],
    column_profiles: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """
    表卡片第一版。
    包含 schema、profile 摘要、候选主键、高空值字段、疑似枚举字段。
    """
    table_cards = []

    for table in table_profiles:
        table_name = table.get("table_name")
        table_column_profiles = get_columns_for_table(column_profiles, table_name)

        possible_primary_keys = get_possible_primary_keys(table_column_profiles)
        high_null_columns = get_high_null_columns(table_column_profiles)
        enum_like_columns = get_enum_like_columns(table_column_profiles)
        date_columns = guess_date_columns(table_column_profiles)
        measure_columns = guess_measure_columns(table_column_profiles)
        dimension_columns = guess_dimension_columns(table_column_profiles)

        table_cards.append(
            {
                "card_type": "table_card",
                "table_name": table_name,
                "row_count": table.get("row_count"),
                "column_count": table.get("column_count"),
                "columns": table.get("columns", []),
                "possible_primary_keys": possible_primary_keys,
                "high_null_columns": high_null_columns,
                "enum_like_columns": enum_like_columns,
                "date_columns": date_columns,
                "measure_columns": measure_columns,
                "dimension_columns": dimension_columns,
                "notes": [
                    "This is an automatically generated table card based on schema and data profile.",
                    "possible_primary_keys are candidates only and must be verified against DATA_DICTIONARY.md.",
                ],
            }
        )

    return table_cards


# =========================
# 4. DQ Scanner 初版
# =========================

def build_dq_rules(
    column_profiles: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """
    数据质量规则初版。
    先做自动规则：
    1. 高空值字段
    2. 疑似枚举字段
    3. 疑似类型/业务陷阱字段

    注意：这里的规则是候选规则，后续需要结合 DATA_DICTIONARY.md 做业务解释。
    """
    dq_rules = []

    for item in column_profiles:
        table_name = item.get("table_name")
        column_name = item.get("column_name")
        null_rate = item.get("null_rate", 0)
        distinct_count = item.get("distinct_count", 0)
        sample_values = item.get("sample_values", [])
        data_type = item.get("data_type", "")

        if null_rate is not None and null_rate >= 0.2:
            dq_rules.append(
                {
                    "rule_type": "high_null_rate",
                    "severity": "medium",
                    "table_name": table_name,
                    "column_name": column_name,
                    "null_rate": null_rate,
                    "message": (
                        f"{table_name}.{column_name} has high null rate "
                        f"({null_rate}). Need business interpretation before using it."
                    ),
                }
            )

        if 1 < distinct_count <= 20:
            dq_rules.append(
                {
                    "rule_type": "enum_like_column",
                    "severity": "low",
                    "table_name": table_name,
                    "column_name": column_name,
                    "distinct_count": distinct_count,
                    "sample_values": sample_values,
                    "message": (
                        f"{table_name}.{column_name} looks like an enum-like column. "
                        "Check allowed values before filtering or grouping."
                    ),
                }
            )

        # 针对当前 CloudWork 数据集的几个明显字段陷阱
        if table_name == "fact_ai_usage_log" and column_name == "created_at":
            dq_rules.append(
                {
                    "rule_type": "special_time_format",
                    "severity": "high",
                    "table_name": table_name,
                    "column_name": column_name,
                    "data_type": data_type,
                    "message": (
                        "fact_ai_usage_log.created_at is unix epoch seconds, "
                        "not a normal timestamp string. Convert it before date filtering."
                    ),
                }
            )

        if table_name == "fact_ai_usage_log" and column_name == "user_id_hash":
            dq_rules.append(
                {
                    "rule_type": "hashed_user_id",
                    "severity": "high",
                    "table_name": table_name,
                    "column_name": column_name,
                    "message": (
                        "fact_ai_usage_log uses hashed user IDs. Join with "
                        "dim_user_id_mapping before joining to dim_user."
                    ),
                }
            )

        if table_name == "fact_nps_survey" and column_name == "score":
            dq_rules.append(
                {
                    "rule_type": "mixed_numeric_string",
                    "severity": "high",
                    "table_name": table_name,
                    "column_name": column_name,
                    "sample_values": sample_values,
                    "message": (
                        "fact_nps_survey.score is stored as VARCHAR and may contain N/A. "
                        "Use TRY_CAST and filter invalid values before numeric analysis."
                    ),
                }
            )

        if table_name == "fact_nps_survey" and column_name == "plan_tier":
            dq_rules.append(
                {
                    "rule_type": "case_mismatch",
                    "severity": "medium",
                    "table_name": table_name,
                    "column_name": column_name,
                    "sample_values": sample_values,
                    "message": (
                        "fact_nps_survey.plan_tier uses capitalized values, while "
                        "dim_plan.plan_tier uses lowercase values. Normalize case before joining."
                    ),
                }
            )

        if table_name == "fact_invoice" and column_name == "currency":
            dq_rules.append(
                {
                    "rule_type": "multi_currency",
                    "severity": "high",
                    "table_name": table_name,
                    "column_name": column_name,
                    "sample_values": sample_values,
                    "message": (
                        "fact_invoice contains multiple currencies. Do not sum amount directly "
                        "across currencies without conversion."
                    ),
                }
            )

    return dq_rules


# =========================
# 5. Grain Detector 初版
# =========================

def infer_grain_rule(table_name: str, columns: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    根据表名和字段名粗略推断表粒度。
    第一版采用规则判断，后续可结合 DATA_DICTIONARY.md 增强。
    """
    column_names = [col.get("name") for col in columns]

    grain: str = "unknown"
    risk_notes: List[str] = []

    # 常见维表
    if table_name.startswith("dim_"):
        grain = "dimension_table"
        risk_notes.append("Usually safe to join from fact tables if using the declared key.")

    # 常见事实表规则
    if table_name == "fact_feature_usage":
        grain = "one row per user_id + feature_key + dt"
        risk_notes.append("Do not directly join with another daily fact table before aggregation.")

    elif table_name == "fact_daily_usage":
        grain = "one row per user_id + dt"
        risk_notes.append("feature_usage_json and fact_feature_usage.action_count have different metric definitions.")

    elif table_name == "fact_session":
        grain = "one row per session"
        risk_notes.append("Joining session to page_view may expand rows because one session has many page views.")

    elif table_name == "fact_page_view":
        grain = "one row per page view"
        risk_notes.append("Page view is lower grain than session. Aggregate carefully.")

    elif table_name == "fact_event_log":
        grain = "one row per user event"
        risk_notes.append("Event log is detailed behavior data. COUNT(*) means event count, not user count.")

    elif table_name == "fact_ai_usage_log":
        grain = "one row per AI usage operation"
        risk_notes.append("Uses user_id_hash, not user_id. Must bridge through dim_user_id_mapping.")

    elif table_name == "fact_subscription":
        grain = "one row per subscription period"
        risk_notes.append("Use start_date and end_date to determine whether a subscription is active in a period.")

    elif table_name == "fact_invoice":
        grain = "one row per invoice"
        risk_notes.append("Invoice amount may be in multiple currencies.")

    elif table_name == "fact_payment":
        grain = "one row per payment attempt"
        risk_notes.append("One invoice may have multiple payments. Joining to invoice may expand rows.")

    elif table_name == "fact_actual_revenue":
        grain = "one row per subscription per month"
        risk_notes.append("Use actual_revenue for realized commercial revenue analysis.")

    elif table_name == "fact_tenant_metrics_snapshot":
        grain = "one row per tenant per snapshot_date"
        risk_notes.append("total_* fields are cumulative; do not treat them as daily increments.")

    elif table_name == "fact_ticket_reply":
        grain = "one row per ticket reply"
        risk_notes.append("One ticket may have multiple replies. Joining to fact_ticket may expand rows.")

    elif table_name == "fact_doc_collaboration":
        grain = "one row per doc_id + user_id + dt"
        risk_notes.append("Document collaboration is daily user-doc grain.")

    elif table_name == "fact_experiment_metric":
        grain = "one row per exp_id + variant + metric_name + dt"
        risk_notes.append("Metric table is already aggregated by experiment, variant, metric, and date.")

    elif table_name == "fact_campaign_attribution":
        grain = "one row per user campaign touch"
        risk_notes.append("A user can have multiple touches. Count distinct users for user-level conversion analysis.")

    elif table_name == "fact_user_activation":
        grain = "one row per user activation milestone"
        risk_notes.append("A user can have multiple milestones. Count distinct users if measuring activated users.")

    return {
        "table_name": table_name,
        "columns": column_names,
        "inferred_grain": grain,
        "risk_notes": risk_notes,
        "source": "rule_based_first_version",
    }


def build_grain_rules(
    table_profiles: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """构建表粒度规则。"""
    grain_rules = []

    for table in table_profiles:
        table_name = table.get("table_name")
        columns = table.get("columns", [])
        grain_rules.append(infer_grain_rule(table_name, columns))

    return grain_rules


# =========================
# 6. Recipe Builder 初版
# =========================

def build_recipes() -> List[Dict[str, Any]]:
    """
    从 outputs/sample/question_x 中读取 query.sql 和 insight.md。
    构建第一版 recipes。
    """
    recipes = []

    question_meta = [
        {
            "question_id": "question_1",
            "title": "企业画像分布",
            "analysis_type": "single_table_distribution",
            "main_tables": ["dim_tenant"],
        },
        {
            "question_id": "question_2",
            "title": "月度新注册用户",
            "analysis_type": "time_trend",
            "main_tables": ["dim_user"],
        },
        {
            "question_id": "question_3",
            "title": "各套餐月收入对比",
            "analysis_type": "join_and_revenue_aggregation",
            "main_tables": ["fact_subscription", "dim_plan"],
        },
    ]

    for meta in question_meta:
        qid = meta["question_id"]
        q_dir = SAMPLE_DIR / qid

        query_sql = read_text_if_exists(q_dir / "query.sql")
        insight = read_text_if_exists(q_dir / "insight.md")

        output_files = []
        if q_dir.exists():
            output_files = sorted([p.name for p in q_dir.iterdir() if p.is_file()])

        recipes.append(
            {
                "recipe_id": qid,
                "title": meta["title"],
                "analysis_type": meta["analysis_type"],
                "main_tables": meta["main_tables"],
                "query_sql": query_sql,
                "insight": insight,
                "output_files": output_files,
                "source_dir": str(q_dir.relative_to(PROJECT_ROOT)) if q_dir.exists() else "",
            }
        )

    return recipes


# =========================
# 7. Summary 构建
# =========================

def build_kb_summary(
    table_profiles: List[Dict[str, Any]],
    column_profiles: List[Dict[str, Any]],
    table_cards: List[Dict[str, Any]],
    column_cards: List[Dict[str, Any]],
    dq_rules: List[Dict[str, Any]],
    grain_rules: List[Dict[str, Any]],
    recipes: List[Dict[str, Any]],
    data_dictionary_text: str,
) -> Dict[str, Any]:
    """生成知识库摘要。"""
    total_rows = sum(int(table.get("row_count", 0)) for table in table_profiles)

    return {
        "knowledge_base_name": "CloudWork Knowledge Base",
        "version": "v1_rule_based",
        "description": "First offline knowledge base built from schema, data profiles, sample questions, and data dictionary.",
        "input_files": {
            "table_profiles": str(TABLE_PROFILE_PATH.relative_to(PROJECT_ROOT)),
            "column_profiles": str(COLUMN_PROFILE_PATH.relative_to(PROJECT_ROOT)),
            "data_dictionary": str(DATA_DICTIONARY_PATH.relative_to(PROJECT_ROOT)),
            "sample_questions_output": str(SAMPLE_DIR.relative_to(PROJECT_ROOT)),
        },
        "output_files": {
            "schema_raw": str(SCHEMA_RAW_PATH.relative_to(PROJECT_ROOT)),
            "table_cards": str(TABLE_CARDS_PATH.relative_to(PROJECT_ROOT)),
            "column_cards": str(COLUMN_CARDS_PATH.relative_to(PROJECT_ROOT)),
            "dq_rules": str(DQ_RULES_PATH.relative_to(PROJECT_ROOT)),
            "grain_rules": str(GRAIN_RULES_PATH.relative_to(PROJECT_ROOT)),
            "recipes": str(RECIPES_PATH.relative_to(PROJECT_ROOT)),
        },
        "statistics": {
            "table_count": len(table_profiles),
            "column_count": len(column_profiles),
            "total_rows": total_rows,
            "table_card_count": len(table_cards),
            "column_card_count": len(column_cards),
            "dq_rule_count": len(dq_rules),
            "grain_rule_count": len(grain_rules),
            "recipe_count": len(recipes),
            "data_dictionary_char_count": len(data_dictionary_text),
        },
        "notes": [
            "This version does not build vector embeddings yet.",
            "possible_primary_keys are generated from data profile and must be verified.",
            "dq_rules and grain_rules are first-version rule-based outputs.",
            "Next step is to build a retriever and compact context for SQL generation.",
        ],
    }


# =========================
# 主函数
# =========================

def main() -> None:
    """主流程：构建离线知识库第一版。"""

    ensure_dir(KNOWLEDGE_DIR)

    print("=" * 70)
    print("开始构建 CloudWork Knowledge Base 第一版")
    print("=" * 70)

    table_profiles = read_json(TABLE_PROFILE_PATH)
    column_profiles = read_json(COLUMN_PROFILE_PATH)
    data_dictionary_text = read_text_if_exists(DATA_DICTIONARY_PATH)

    print(f"读取表级 profile：{len(table_profiles)} 张表")
    print(f"读取字段级 profile：{len(column_profiles)} 个字段")
    print(f"读取数据字典字符数：{len(data_dictionary_text)}")

    schema_raw = build_schema_raw(table_profiles)
    column_cards = build_column_cards(column_profiles)
    table_cards = build_table_cards(table_profiles, column_profiles)
    dq_rules = build_dq_rules(column_profiles)
    grain_rules = build_grain_rules(table_profiles)
    recipes = build_recipes()

    kb_summary = build_kb_summary(
        table_profiles=table_profiles,
        column_profiles=column_profiles,
        table_cards=table_cards,
        column_cards=column_cards,
        dq_rules=dq_rules,
        grain_rules=grain_rules,
        recipes=recipes,
        data_dictionary_text=data_dictionary_text,
    )

    write_json(schema_raw, SCHEMA_RAW_PATH)
    write_json(table_cards, TABLE_CARDS_PATH)
    write_json(column_cards, COLUMN_CARDS_PATH)
    write_json(dq_rules, DQ_RULES_PATH)
    write_json(grain_rules, GRAIN_RULES_PATH)
    write_json(recipes, RECIPES_PATH)
    write_json(kb_summary, KB_SUMMARY_PATH)

    print("\n" + "=" * 70)
    print("CloudWork Knowledge Base 第一版构建完成")
    print("=" * 70)
    print(f"表数量：{len(table_profiles)}")
    print(f"字段数量：{len(column_profiles)}")
    print(f"DQ 规则数量：{len(dq_rules)}")
    print(f"Grain 规则数量：{len(grain_rules)}")
    print(f"Recipe 数量：{len(recipes)}")
    print(f"输出目录：{KNOWLEDGE_DIR}")
    print("=" * 70)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"运行失败：{e}")
        raise