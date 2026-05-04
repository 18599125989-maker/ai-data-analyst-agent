#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
功能：
验证 knowledge_manual/table_cards_draft/dim_plan.yaml 中的核心假设。

验证内容：
1. 表行数与字段数
2. plan_tier 是否唯一
3. plan_name 候选自然键是否唯一
4. 与 fact_subscription 的 JOIN 覆盖率
5. seat_limit 分布与可解析性
6. monthly_price / annual_price 数值范围与关系
7. 主要 SQL patterns 是否可执行

输出：
outputs/knowledge/validation/
├── dim_plan_validation.json
└── dim_plan_validation.md
"""

import json
from pathlib import Path

import duckdb


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DB_PATH = PROJECT_ROOT / "cloudwork.duckdb"

OUTPUT_DIR = PROJECT_ROOT / "outputs" / "knowledge" / "validation"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

JSON_OUTPUT = OUTPUT_DIR / "dim_plan_validation.json"
MD_OUTPUT = OUTPUT_DIR / "dim_plan_validation.md"


def make_json_safe(value):
    if value is None:
        return None
    try:
        import pandas as pd
        if pd.isna(value):
            return None
    except Exception:
        pass
    try:
        import numpy as np
        if isinstance(value, np.integer):
            return int(value)
        if isinstance(value, np.floating):
            return float(value)
        if isinstance(value, np.bool_):
            return bool(value)
    except Exception:
        pass
    if hasattr(value, "isoformat"):
        try:
            return value.isoformat()
        except Exception:
            pass
    if isinstance(value, (str, int, float, bool)):
        return value
    return str(value)


def rows_to_json_safe(rows):
    return [{key: make_json_safe(value) for key, value in row.items()} for row in rows]


def run_query(conn, name, sql):
    print(f"正在执行：{name}")
    try:
        df = conn.execute(sql).df()
        return {
            "check_name": name,
            "status": "success",
            "sql": sql.strip(),
            "rows": rows_to_json_safe(df.to_dict(orient="records")),
            "error": None,
        }
    except Exception as e:
        return {
            "check_name": name,
            "status": "failed",
            "sql": sql.strip(),
            "rows": [],
            "error": str(e),
        }


def get_first_row(checks, check_name):
    for item in checks:
        if item["check_name"] == check_name and item["rows"]:
            return item["rows"][0]
    return {}


def build_summary(checks):
    summary = {
        "profile_filled": False,
        "grain_validated": False,
        "dq_validated": False,
        "ready_for_agent_recommendation": False,
        "key_findings": [],
        "warnings": [],
    }

    basic = get_first_row(checks, "basic_table_profile")
    col = get_first_row(checks, "column_count")
    pk = get_first_row(checks, "plan_tier_uniqueness")
    natural_key = get_first_row(checks, "plan_name_uniqueness")
    sub_fk = get_first_row(checks, "subscription_join_coverage")
    seat_parse = get_first_row(checks, "seat_limit_parse_check")
    price = get_first_row(checks, "price_range_check")
    annual_ratio = get_first_row(checks, "annual_vs_monthly_price_check")

    if basic and col:
        summary["profile_filled"] = True
        summary["key_findings"].append(
            f"dim_plan 行数为 {basic.get('row_count')}，字段数为 {col.get('column_count')}。"
        )

    if pk and (pk.get("duplicate_plan_tier_count") or 0) == 0:
        summary["grain_validated"] = True
        summary["key_findings"].append("plan_tier 当前唯一，可支持“一行一个套餐”的粒度判断。")
    elif pk:
        summary["warnings"].append(f"plan_tier 存在 {pk.get('duplicate_plan_tier_count')} 条重复。")

    dq_pass = True

    if natural_key:
        summary["key_findings"].append(
            f"plan_name 重复数为 {natural_key.get('duplicate_plan_name_count')}。"
        )

    if sub_fk:
        unmatched = sub_fk.get("unmatched_subscription_plan_count") or 0
        if unmatched == 0:
            summary["key_findings"].append("fact_subscription.plan_tier 全部可以关联到 dim_plan.plan_tier。")
        else:
            dq_pass = False
            summary["warnings"].append(f"存在 {unmatched} 条 fact_subscription 记录无法关联到 dim_plan。")

    if seat_parse:
        summary["key_findings"].append(
            f"seat_limit 中可直接解析为整数的行数为 {seat_parse.get('numeric_seat_limit_count')}，unlimited 行数为 {seat_parse.get('unlimited_seat_limit_count')}。"
        )

    if price:
        if (price.get("negative_price_count") or 0) > 0:
            dq_pass = False
            summary["warnings"].append(f"存在 {price.get('negative_price_count')} 条负价格记录。")
        else:
            summary["key_findings"].append(
                f"monthly_price 范围为 {price.get('min_monthly_price')} 到 {price.get('max_monthly_price')}。"
            )

    if annual_ratio:
        summary["key_findings"].append(
            f"annual_price 与 monthly_price * 10 不一致的行数为 {annual_ratio.get('non_ten_x_price_count')}。"
        )

    failed_patterns = [
        item["check_name"]
        for item in checks
        if item["check_name"].startswith("sql_pattern_") and item["status"] != "success"
    ]
    if failed_patterns:
        dq_pass = False
        summary["warnings"].append(f"以下 SQL patterns 执行失败：{failed_patterns}")
    else:
        summary["key_findings"].append("主要 SQL patterns 均可执行。")

    summary["dq_validated"] = dq_pass
    summary["ready_for_agent_recommendation"] = (
        summary["profile_filled"] and summary["grain_validated"] and summary["dq_validated"]
    )
    return summary


def write_markdown_report(output):
    summary = output["summary"]
    checks = output["checks"]
    lines = [
        "# dim_plan Table Card Validation Report",
        "",
        "## Summary",
        "",
        f"- profile_filled: `{summary['profile_filled']}`",
        f"- grain_validated: `{summary['grain_validated']}`",
        f"- dq_validated: `{summary['dq_validated']}`",
        f"- ready_for_agent_recommendation: `{summary['ready_for_agent_recommendation']}`",
        "",
        "## Key Findings",
        "",
    ]
    for item in summary["key_findings"]:
        lines.append(f"- {item}")
    if summary["warnings"]:
        lines.extend(["", "## Warnings", ""])
        for item in summary["warnings"]:
            lines.append(f"- {item}")
    lines.extend(["", "## Detailed Check Results", ""])
    for check in checks:
        lines.append(f"### {check['check_name']}")
        lines.append("")
        lines.append(f"- status: `{check['status']}`")
        if check["error"]:
            lines.append(f"- error: `{check['error']}`")
            lines.append("")
            continue
        rows = check["rows"]
        if not rows:
            lines.extend(["", "No rows returned.", ""])
            continue
        headers = list(rows[0].keys())
        lines.extend([
            "",
            "| " + " | ".join(headers) + " |",
            "| " + " | ".join(["---"] * len(headers)) + " |",
        ])
        for row in rows[:30]:
            values = [str(row.get(h, "")).replace("|", "/") for h in headers]
            lines.append("| " + " | ".join(values) + " |")
        if len(rows) > 30:
            lines.extend(["", f"Only first 30 rows shown. Total rows: {len(rows)}"])
        lines.append("")
    MD_OUTPUT.write_text("\n".join(lines), encoding="utf-8")


def main():
    if not DB_PATH.exists():
        raise FileNotFoundError(f"找不到数据库文件：{DB_PATH}")
    conn = duckdb.connect(str(DB_PATH), read_only=True)
    checks = []

    checks.append(run_query(conn, "basic_table_profile", """
        SELECT COUNT(*) AS row_count
        FROM dim_plan;
    """))
    checks.append(run_query(conn, "column_count", """
        SELECT COUNT(*) AS column_count
        FROM information_schema.columns
        WHERE table_name = 'dim_plan';
    """))
    checks.append(run_query(conn, "plan_tier_uniqueness", """
        SELECT
            COUNT(*) AS row_count,
            COUNT(DISTINCT plan_tier) AS distinct_plan_tier_count,
            COUNT(*) - COUNT(DISTINCT plan_tier) AS duplicate_plan_tier_count
        FROM dim_plan;
    """))
    checks.append(run_query(conn, "duplicated_plan_tier_examples", """
        SELECT
            plan_tier,
            COUNT(*) AS duplicate_count
        FROM dim_plan
        GROUP BY plan_tier
        HAVING COUNT(*) > 1
        ORDER BY duplicate_count DESC, plan_tier
        LIMIT 20;
    """))
    checks.append(run_query(conn, "plan_name_uniqueness", """
        SELECT
            COUNT(*) AS row_count,
            COUNT(DISTINCT plan_name) AS distinct_plan_name_count,
            COUNT(*) - COUNT(DISTINCT plan_name) AS duplicate_plan_name_count
        FROM dim_plan;
    """))
    checks.append(run_query(conn, "duplicated_plan_name_examples", """
        SELECT
            plan_name,
            COUNT(*) AS duplicate_count
        FROM dim_plan
        GROUP BY plan_name
        HAVING COUNT(*) > 1
        ORDER BY duplicate_count DESC, plan_name
        LIMIT 20;
    """))
    checks.append(run_query(conn, "subscription_join_coverage", """
        SELECT
            COUNT(*) AS subscription_row_count,
            SUM(CASE WHEN p.plan_tier IS NULL THEN 1 ELSE 0 END) AS unmatched_subscription_plan_count
        FROM fact_subscription s
        LEFT JOIN dim_plan p
            ON s.plan_tier = p.plan_tier;
    """))
    checks.append(run_query(conn, "subscription_unmatched_examples", """
        SELECT
            s.sub_id,
            s.plan_tier
        FROM fact_subscription s
        LEFT JOIN dim_plan p
            ON s.plan_tier = p.plan_tier
        WHERE p.plan_tier IS NULL
        LIMIT 20;
    """))
    checks.append(run_query(conn, "seat_limit_distribution", """
        SELECT
            seat_limit,
            COUNT(*) AS plan_count
        FROM dim_plan
        GROUP BY seat_limit
        ORDER BY plan_count DESC, seat_limit;
    """))
    checks.append(run_query(conn, "seat_limit_parse_check", """
        SELECT
            SUM(CASE WHEN seat_limit = 'unlimited' THEN 1 ELSE 0 END) AS unlimited_seat_limit_count,
            SUM(CASE WHEN seat_limit != 'unlimited' AND TRY_CAST(seat_limit AS BIGINT) IS NOT NULL THEN 1 ELSE 0 END) AS numeric_seat_limit_count,
            SUM(CASE WHEN seat_limit != 'unlimited' AND TRY_CAST(seat_limit AS BIGINT) IS NULL THEN 1 ELSE 0 END) AS invalid_seat_limit_count
        FROM dim_plan;
    """))
    checks.append(run_query(conn, "price_range_check", """
        SELECT
            MIN(monthly_price) AS min_monthly_price,
            MAX(monthly_price) AS max_monthly_price,
            MIN(annual_price) AS min_annual_price,
            MAX(annual_price) AS max_annual_price,
            SUM(CASE WHEN monthly_price < 0 OR annual_price < 0 THEN 1 ELSE 0 END) AS negative_price_count
        FROM dim_plan;
    """))
    checks.append(run_query(conn, "annual_vs_monthly_price_check", """
        SELECT
            COUNT(*) AS row_count,
            SUM(CASE WHEN annual_price != monthly_price * 10 THEN 1 ELSE 0 END) AS non_ten_x_price_count
        FROM dim_plan;
    """))
    checks.append(run_query(conn, "sql_pattern_plan_price_table", """
        SELECT
            plan_tier,
            plan_name,
            monthly_price,
            annual_price,
            seat_limit
        FROM dim_plan
        ORDER BY monthly_price, plan_tier;
    """))
    checks.append(run_query(conn, "sql_pattern_subscription_count_by_plan", """
        SELECT
            p.plan_tier,
            p.plan_name,
            COUNT(DISTINCT s.sub_id) AS subscription_count
        FROM dim_plan p
        LEFT JOIN fact_subscription s
            ON p.plan_tier = s.plan_tier
        GROUP BY p.plan_tier, p.plan_name
        ORDER BY subscription_count DESC, p.plan_tier;
    """))
    checks.append(run_query(conn, "sql_pattern_mrr_by_plan", """
        SELECT
            p.plan_tier,
            p.plan_name,
            SUM(s.mrr) AS total_mrr
        FROM dim_plan p
        LEFT JOIN fact_subscription s
            ON p.plan_tier = s.plan_tier
        GROUP BY p.plan_tier, p.plan_name
        ORDER BY total_mrr DESC NULLS LAST, p.plan_tier;
    """))

    output = {
        "table_name": "dim_plan",
        "summary": build_summary(checks),
        "checks": checks,
    }

    JSON_OUTPUT.write_text(json.dumps(output, ensure_ascii=False, indent=2), encoding="utf-8")
    write_markdown_report(output)
    print(f"已输出：{JSON_OUTPUT}")
    print(f"已输出：{MD_OUTPUT}")


if __name__ == "__main__":
    main()
