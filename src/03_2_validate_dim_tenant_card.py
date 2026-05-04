#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
功能：
验证 knowledge_manual/table_cards_draft/dim_tenant.yaml 中的核心假设。

验证内容：
1. 表行数与字段数
2. tenant_id 是否唯一
3. name 候选自然键是否唯一
4. country / industry / size_tier 枚举分布
5. created_at 时间范围
6. 与 dim_user、fact_subscription 的主要 JOIN 覆盖率
7. 主要 SQL patterns 是否可执行

输出：
outputs/knowledge/validation/
├── dim_tenant_validation.json
└── dim_tenant_validation.md
"""

import json
from pathlib import Path

import duckdb


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DB_PATH = PROJECT_ROOT / "cloudwork.duckdb"

OUTPUT_DIR = PROJECT_ROOT / "outputs" / "knowledge" / "validation"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

JSON_OUTPUT = OUTPUT_DIR / "dim_tenant_validation.json"
MD_OUTPUT = OUTPUT_DIR / "dim_tenant_validation.md"


def make_json_safe(value):
    """把 Pandas / DuckDB 返回的特殊对象转成 JSON 可保存的普通对象"""
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
    safe_rows = []
    for row in rows:
        safe_row = {}
        for key, value in row.items():
            safe_row[key] = make_json_safe(value)
        safe_rows.append(safe_row)
    return safe_rows


def run_query(conn, name, sql):
    print(f"正在执行：{name}")

    try:
        df = conn.execute(sql).df()
        rows = df.to_dict(orient="records")
        return {
            "check_name": name,
            "status": "success",
            "sql": sql.strip(),
            "rows": rows_to_json_safe(rows),
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
    column_count = get_first_row(checks, "column_count")
    pk = get_first_row(checks, "tenant_id_uniqueness")
    natural_key = get_first_row(checks, "name_uniqueness")
    created = get_first_row(checks, "created_at_range_check")
    user_fk = get_first_row(checks, "dim_user_join_coverage")
    subscription_fk = get_first_row(checks, "fact_subscription_join_coverage")

    if basic and column_count:
        summary["profile_filled"] = True
        summary["key_findings"].append(
            f"dim_tenant 行数为 {basic.get('row_count')}，字段数为 {column_count.get('column_count')}。"
        )

    if pk:
        duplicate_count = pk.get("duplicate_tenant_id_count")
        if duplicate_count == 0:
            summary["grain_validated"] = True
            summary["key_findings"].append("tenant_id 当前唯一，可支持“预期一行一个 tenant”的粒度判断。")
        else:
            summary["warnings"].append(f"tenant_id 存在 {duplicate_count} 条重复。")

    dq_pass = True

    if natural_key:
        duplicate_name_count = natural_key.get("duplicate_name_count")
        if duplicate_name_count == 0:
            summary["key_findings"].append("name 在当前样本中也唯一，但仍只建议视为候选自然键。")
        else:
            summary["warnings"].append(f"name 存在 {duplicate_name_count} 条重复，不能视为唯一候选键。")

    if created:
        summary["key_findings"].append(
            f"created_at 时间范围为 {created.get('min_created_at')} 到 {created.get('max_created_at')}。"
        )
        if (created.get("null_created_at_count") or 0) > 0:
            dq_pass = False
            summary["warnings"].append(f"created_at 存在 {created.get('null_created_at_count')} 条空值。")

    if user_fk:
        unmatched = user_fk.get("unmatched_user_tenant_count")
        if unmatched == 0:
            summary["key_findings"].append("dim_user.tenant_id 全部可以关联到 dim_tenant.tenant_id。")
        else:
            dq_pass = False
            summary["warnings"].append(f"存在 {unmatched} 条 dim_user 记录无法关联到 dim_tenant。")

    if subscription_fk:
        unmatched = subscription_fk.get("unmatched_subscription_tenant_count")
        if unmatched == 0:
            summary["key_findings"].append("fact_subscription.tenant_id 全部可以关联到 dim_tenant.tenant_id。")
        else:
            dq_pass = False
            summary["warnings"].append(f"存在 {unmatched} 条 fact_subscription 记录无法关联到 dim_tenant。")

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
        summary["profile_filled"]
        and summary["grain_validated"]
        and summary["dq_validated"]
    )
    return summary


def write_markdown_report(output):
    summary = output["summary"]
    checks = output["checks"]

    lines = []
    lines.append("# dim_tenant Table Card Validation Report")
    lines.append("")
    lines.append("## Summary")
    lines.append("")
    lines.append(f"- profile_filled: `{summary['profile_filled']}`")
    lines.append(f"- grain_validated: `{summary['grain_validated']}`")
    lines.append(f"- dq_validated: `{summary['dq_validated']}`")
    lines.append(f"- ready_for_agent_recommendation: `{summary['ready_for_agent_recommendation']}`")
    lines.append("")
    lines.append("## Key Findings")
    lines.append("")
    for item in summary["key_findings"]:
        lines.append(f"- {item}")
    if summary["warnings"]:
        lines.append("")
        lines.append("## Warnings")
        lines.append("")
        for item in summary["warnings"]:
            lines.append(f"- {item}")
    lines.append("")
    lines.append("## Detailed Check Results")
    lines.append("")

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
            lines.append("")
            lines.append("No rows returned.")
            lines.append("")
            continue
        headers = list(rows[0].keys())
        lines.append("")
        lines.append("| " + " | ".join(headers) + " |")
        lines.append("| " + " | ".join(["---"] * len(headers)) + " |")
        for row in rows[:30]:
            values = [str(row.get(h, "")) for h in headers]
            values = [v.replace("|", "/") for v in values]
            lines.append("| " + " | ".join(values) + " |")
        if len(rows) > 30:
            lines.append("")
            lines.append(f"Only first 30 rows shown. Total rows: {len(rows)}")
        lines.append("")

    MD_OUTPUT.write_text("\n".join(lines), encoding="utf-8")


def main():
    if not DB_PATH.exists():
        raise FileNotFoundError(f"找不到数据库文件：{DB_PATH}")

    conn = duckdb.connect(str(DB_PATH), read_only=True)
    checks = []

    checks.append(run_query(conn, "basic_table_profile", """
        SELECT COUNT(*) AS row_count
        FROM dim_tenant;
    """))

    checks.append(run_query(conn, "column_count", """
        SELECT COUNT(*) AS column_count
        FROM information_schema.columns
        WHERE table_name = 'dim_tenant';
    """))

    checks.append(run_query(conn, "tenant_id_uniqueness", """
        SELECT
            COUNT(*) AS row_count,
            COUNT(DISTINCT tenant_id) AS distinct_tenant_id_count,
            COUNT(*) - COUNT(DISTINCT tenant_id) AS duplicate_tenant_id_count
        FROM dim_tenant;
    """))

    checks.append(run_query(conn, "duplicated_tenant_id_examples", """
        SELECT
            tenant_id,
            COUNT(*) AS duplicate_count
        FROM dim_tenant
        GROUP BY tenant_id
        HAVING COUNT(*) > 1
        ORDER BY duplicate_count DESC, tenant_id
        LIMIT 20;
    """))

    checks.append(run_query(conn, "name_uniqueness", """
        SELECT
            COUNT(*) AS row_count,
            COUNT(DISTINCT name) AS distinct_name_count,
            COUNT(*) - COUNT(DISTINCT name) AS duplicate_name_count
        FROM dim_tenant;
    """))

    checks.append(run_query(conn, "duplicated_name_examples", """
        SELECT
            name,
            COUNT(*) AS duplicate_count
        FROM dim_tenant
        GROUP BY name
        HAVING COUNT(*) > 1
        ORDER BY duplicate_count DESC, name
        LIMIT 20;
    """))

    checks.append(run_query(conn, "country_distribution", """
        SELECT
            country,
            COUNT(DISTINCT tenant_id) AS tenant_count
        FROM dim_tenant
        GROUP BY country
        ORDER BY tenant_count DESC, country;
    """))

    checks.append(run_query(conn, "industry_distribution", """
        SELECT
            industry,
            COUNT(DISTINCT tenant_id) AS tenant_count
        FROM dim_tenant
        GROUP BY industry
        ORDER BY tenant_count DESC, industry;
    """))

    checks.append(run_query(conn, "size_tier_distribution", """
        SELECT
            size_tier,
            COUNT(DISTINCT tenant_id) AS tenant_count
        FROM dim_tenant
        GROUP BY size_tier
        ORDER BY tenant_count DESC, size_tier;
    """))

    checks.append(run_query(conn, "created_at_range_check", """
        SELECT
            COUNT(*) AS row_count,
            SUM(CASE WHEN created_at IS NULL THEN 1 ELSE 0 END) AS null_created_at_count,
            MIN(created_at) AS min_created_at,
            MAX(created_at) AS max_created_at
        FROM dim_tenant;
    """))

    checks.append(run_query(conn, "dim_user_join_coverage", """
        SELECT
            COUNT(*) AS user_row_count,
            SUM(CASE WHEN t.tenant_id IS NULL THEN 1 ELSE 0 END) AS unmatched_user_tenant_count
        FROM dim_user u
        LEFT JOIN dim_tenant t
            ON u.tenant_id = t.tenant_id;
    """))

    checks.append(run_query(conn, "fact_subscription_join_coverage", """
        SELECT
            COUNT(*) AS subscription_row_count,
            SUM(CASE WHEN t.tenant_id IS NULL THEN 1 ELSE 0 END) AS unmatched_subscription_tenant_count
        FROM fact_subscription s
        LEFT JOIN dim_tenant t
            ON s.tenant_id = t.tenant_id;
    """))

    checks.append(run_query(conn, "sql_pattern_tenant_count_by_country", """
        SELECT
            country,
            COUNT(DISTINCT tenant_id) AS tenant_count
        FROM dim_tenant
        GROUP BY country
        ORDER BY tenant_count DESC, country;
    """))

    checks.append(run_query(conn, "sql_pattern_tenant_count_by_industry", """
        SELECT
            industry,
            COUNT(DISTINCT tenant_id) AS tenant_count
        FROM dim_tenant
        GROUP BY industry
        ORDER BY tenant_count DESC, industry;
    """))

    checks.append(run_query(conn, "sql_pattern_tenant_created_by_month", """
        SELECT
            STRFTIME(DATE_TRUNC('month', created_at), '%Y-%m') AS created_month,
            COUNT(DISTINCT tenant_id) AS tenant_count
        FROM dim_tenant
        GROUP BY created_month
        ORDER BY created_month;
    """))

    output = {
        "table_name": "dim_tenant",
        "summary": build_summary(checks),
        "checks": checks,
    }

    JSON_OUTPUT.write_text(
        json.dumps(output, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    write_markdown_report(output)
    print(f"已输出：{JSON_OUTPUT}")
    print(f"已输出：{MD_OUTPUT}")


if __name__ == "__main__":
    main()
