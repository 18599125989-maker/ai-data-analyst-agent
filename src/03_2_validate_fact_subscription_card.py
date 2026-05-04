#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
功能：
验证 knowledge_manual/table_cards_draft/fact_subscription.yaml 中的核心假设。

验证内容：
1. 表行数与字段数
2. sub_id 是否唯一
3. tenant_id + plan_tier + start_date 候选自然键是否唯一
4. tenant_id / plan_tier / sub_id 的主要外键覆盖率
5. status / plan_tier 枚举分布
6. start_date / end_date 时间范围与时间窗异常
7. mrr 数值范围
8. 主要 SQL patterns 是否可执行

输出：
outputs/knowledge/validation/
├── fact_subscription_validation.json
└── fact_subscription_validation.md
"""

import json
from pathlib import Path

import duckdb


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DB_PATH = PROJECT_ROOT / "cloudwork.duckdb"

OUTPUT_DIR = PROJECT_ROOT / "outputs" / "knowledge" / "validation"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

JSON_OUTPUT = OUTPUT_DIR / "fact_subscription_validation.json"
MD_OUTPUT = OUTPUT_DIR / "fact_subscription_validation.md"


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
    pk = get_first_row(checks, "sub_id_uniqueness")
    natural_key = get_first_row(checks, "tenant_plan_start_natural_key_uniqueness")
    tenant_fk = get_first_row(checks, "tenant_id_fk_coverage")
    plan_fk = get_first_row(checks, "plan_tier_fk_coverage")
    revenue_fk = get_first_row(checks, "fact_actual_revenue_sub_id_coverage")
    invalid_window = get_first_row(checks, "invalid_date_window_check")
    mrr = get_first_row(checks, "mrr_range_check")

    if basic and col:
        summary["profile_filled"] = True
        summary["key_findings"].append(
            f"fact_subscription 行数为 {basic.get('row_count')}，字段数为 {col.get('column_count')}。"
        )

    if pk and (pk.get("duplicate_sub_id_count") or 0) == 0:
        summary["grain_validated"] = True
        summary["key_findings"].append("sub_id 当前唯一，可支持“一行一条订阅记录”的粒度判断。")
    elif pk:
        summary["warnings"].append(f"sub_id 存在 {pk.get('duplicate_sub_id_count')} 条重复。")

    dq_pass = True

    if natural_key:
        summary["key_findings"].append(
            f"tenant_id + plan_tier + start_date 的重复组数为 {natural_key.get('duplicate_group_count')}。"
        )

    if tenant_fk and (tenant_fk.get("unmatched_subscription_tenant_count") or 0) != 0:
        dq_pass = False
        summary["warnings"].append(f"存在 {tenant_fk.get('unmatched_subscription_tenant_count')} 条订阅记录无法关联到 dim_tenant。")
    elif tenant_fk:
        summary["key_findings"].append("tenant_id 全部可以关联到 dim_tenant。")

    if plan_fk and (plan_fk.get("unmatched_subscription_plan_count") or 0) != 0:
        dq_pass = False
        summary["warnings"].append(f"存在 {plan_fk.get('unmatched_subscription_plan_count')} 条订阅记录无法关联到 dim_plan。")
    elif plan_fk:
        summary["key_findings"].append("plan_tier 全部可以关联到 dim_plan。")

    if revenue_fk:
        summary["key_findings"].append(
            f"有 {revenue_fk.get('subscription_with_revenue_count')} 个订阅可关联到 fact_actual_revenue。"
        )

    if invalid_window and (invalid_window.get("invalid_date_window_count") or 0) > 0:
        dq_pass = False
        summary["warnings"].append(f"存在 {invalid_window.get('invalid_date_window_count')} 条 end_date < start_date 的记录。")
    elif invalid_window:
        summary["key_findings"].append("未发现 end_date < start_date 的订阅记录。")

    if mrr:
        if (mrr.get("negative_mrr_count") or 0) > 0:
            dq_pass = False
            summary["warnings"].append(f"存在 {mrr.get('negative_mrr_count')} 条负 MRR。")
        else:
            summary["key_findings"].append(
                f"MRR 范围为 {mrr.get('min_mrr')} 到 {mrr.get('max_mrr')}，未发现负值。"
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
        "# fact_subscription Table Card Validation Report",
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
        FROM fact_subscription;
    """))
    checks.append(run_query(conn, "column_count", """
        SELECT COUNT(*) AS column_count
        FROM information_schema.columns
        WHERE table_name = 'fact_subscription';
    """))
    checks.append(run_query(conn, "sub_id_uniqueness", """
        SELECT
            COUNT(*) AS row_count,
            COUNT(DISTINCT sub_id) AS distinct_sub_id_count,
            COUNT(*) - COUNT(DISTINCT sub_id) AS duplicate_sub_id_count
        FROM fact_subscription;
    """))
    checks.append(run_query(conn, "duplicated_sub_id_examples", """
        SELECT
            sub_id,
            COUNT(*) AS duplicate_count
        FROM fact_subscription
        GROUP BY sub_id
        HAVING COUNT(*) > 1
        ORDER BY duplicate_count DESC, sub_id
        LIMIT 20;
    """))
    checks.append(run_query(conn, "tenant_plan_start_natural_key_uniqueness", """
        SELECT
            COUNT(*) AS duplicate_group_count
        FROM (
            SELECT
                tenant_id,
                plan_tier,
                start_date,
                COUNT(*) AS duplicate_count
            FROM fact_subscription
            GROUP BY tenant_id, plan_tier, start_date
            HAVING COUNT(*) > 1
        );
    """))
    checks.append(run_query(conn, "duplicated_tenant_plan_start_examples", """
        SELECT
            tenant_id,
            plan_tier,
            start_date,
            COUNT(*) AS duplicate_count
        FROM fact_subscription
        GROUP BY tenant_id, plan_tier, start_date
        HAVING COUNT(*) > 1
        ORDER BY duplicate_count DESC, tenant_id, plan_tier, start_date
        LIMIT 20;
    """))
    checks.append(run_query(conn, "tenant_id_fk_coverage", """
        SELECT
            COUNT(*) AS row_count,
            SUM(CASE WHEN t.tenant_id IS NULL THEN 1 ELSE 0 END) AS unmatched_subscription_tenant_count
        FROM fact_subscription s
        LEFT JOIN dim_tenant t
            ON s.tenant_id = t.tenant_id;
    """))
    checks.append(run_query(conn, "plan_tier_fk_coverage", """
        SELECT
            COUNT(*) AS row_count,
            SUM(CASE WHEN p.plan_tier IS NULL THEN 1 ELSE 0 END) AS unmatched_subscription_plan_count
        FROM fact_subscription s
        LEFT JOIN dim_plan p
            ON s.plan_tier = p.plan_tier;
    """))
    checks.append(run_query(conn, "fact_actual_revenue_sub_id_coverage", """
        SELECT
            COUNT(DISTINCT s.sub_id) AS total_subscription_count,
            COUNT(DISTINCT r.sub_id) AS subscription_with_revenue_count,
            COUNT(DISTINCT s.sub_id) - COUNT(DISTINCT r.sub_id) AS subscription_without_revenue_count
        FROM fact_subscription s
        LEFT JOIN fact_actual_revenue r
            ON s.sub_id = r.sub_id;
    """))
    checks.append(run_query(conn, "status_distribution", """
        SELECT
            status,
            COUNT(DISTINCT sub_id) AS subscription_count
        FROM fact_subscription
        GROUP BY status
        ORDER BY subscription_count DESC, status;
    """))
    checks.append(run_query(conn, "plan_tier_distribution", """
        SELECT
            plan_tier,
            COUNT(DISTINCT sub_id) AS subscription_count
        FROM fact_subscription
        GROUP BY plan_tier
        ORDER BY subscription_count DESC, plan_tier;
    """))
    checks.append(run_query(conn, "date_range_check", """
        SELECT
            MIN(start_date) AS min_start_date,
            MAX(start_date) AS max_start_date,
            MIN(end_date) AS min_end_date,
            MAX(end_date) AS max_end_date
        FROM fact_subscription;
    """))
    checks.append(run_query(conn, "invalid_date_window_check", """
        SELECT
            COUNT(*) AS invalid_date_window_count
        FROM fact_subscription
        WHERE end_date < start_date;
    """))
    checks.append(run_query(conn, "mrr_range_check", """
        SELECT
            MIN(mrr) AS min_mrr,
            MAX(mrr) AS max_mrr,
            SUM(CASE WHEN mrr < 0 THEN 1 ELSE 0 END) AS negative_mrr_count,
            SUM(CASE WHEN mrr = 0 THEN 1 ELSE 0 END) AS zero_mrr_count
        FROM fact_subscription;
    """))
    checks.append(run_query(conn, "sql_pattern_active_subscriptions_for_month", """
        SELECT
            COUNT(DISTINCT sub_id) AS active_subscription_count
        FROM fact_subscription
        WHERE start_date <= DATE '2025-10-31'
          AND end_date >= DATE '2025-10-01'
          AND status = 'active';
    """))
    checks.append(run_query(conn, "sql_pattern_mrr_by_plan_tier", """
        SELECT
            plan_tier,
            COUNT(DISTINCT sub_id) AS subscription_count,
            SUM(mrr) AS total_mrr
        FROM fact_subscription
        GROUP BY plan_tier
        ORDER BY total_mrr DESC, plan_tier;
    """))
    checks.append(run_query(conn, "sql_pattern_subscription_counts_by_status", """
        SELECT
            status,
            COUNT(DISTINCT sub_id) AS subscription_count
        FROM fact_subscription
        GROUP BY status
        ORDER BY subscription_count DESC, status;
    """))

    output = {
        "table_name": "fact_subscription",
        "summary": build_summary(checks),
        "checks": checks,
    }

    JSON_OUTPUT.write_text(json.dumps(output, ensure_ascii=False, indent=2), encoding="utf-8")
    write_markdown_report(output)
    print(f"已输出：{JSON_OUTPUT}")
    print(f"已输出：{MD_OUTPUT}")


if __name__ == "__main__":
    main()
