#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
功能：
验证 knowledge_manual/table_cards_draft/dim_user.yaml 中的核心假设。

验证内容：
1. 表行数与字段数
2. user_id 是否唯一
3. tenant_id / dept_id 外键覆盖率
4. role / status 枚举分布
5. register_at / last_active_at 时间范围与空值
6. 与 fact_daily_usage、dim_user_id_mapping 的主要覆盖率
7. 主要 SQL patterns 是否可执行

输出：
outputs/knowledge/validation/
├── dim_user_validation.json
└── dim_user_validation.md
"""

import json
from pathlib import Path

import duckdb


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DB_PATH = PROJECT_ROOT / "cloudwork.duckdb"

OUTPUT_DIR = PROJECT_ROOT / "outputs" / "knowledge" / "validation"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

JSON_OUTPUT = OUTPUT_DIR / "dim_user_validation.json"
MD_OUTPUT = OUTPUT_DIR / "dim_user_validation.md"


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
    pk = get_first_row(checks, "user_id_uniqueness")
    tenant_fk = get_first_row(checks, "tenant_id_fk_coverage")
    dept_fk = get_first_row(checks, "dept_id_fk_coverage")
    last_active = get_first_row(checks, "last_active_at_null_and_range_check")
    usage_cov = get_first_row(checks, "fact_daily_usage_join_coverage")
    mapping_cov = get_first_row(checks, "user_id_mapping_coverage")

    if basic and col:
        summary["profile_filled"] = True
        summary["key_findings"].append(
            f"dim_user 行数为 {basic.get('row_count')}，字段数为 {col.get('column_count')}。"
        )

    if pk and (pk.get("duplicate_user_id_count") or 0) == 0:
        summary["grain_validated"] = True
        summary["key_findings"].append("user_id 当前唯一，可支持“一行一个用户”的粒度判断。")
    elif pk:
        summary["warnings"].append(f"user_id 存在 {pk.get('duplicate_user_id_count')} 条重复。")

    dq_pass = True

    if tenant_fk:
        if (tenant_fk.get("unmatched_user_tenant_count") or 0) == 0:
            summary["key_findings"].append("tenant_id 全部可以关联到 dim_tenant。")
        else:
            dq_pass = False
            summary["warnings"].append(f"存在 {tenant_fk.get('unmatched_user_tenant_count')} 条用户记录无法关联到 dim_tenant。")

    if dept_fk:
        unmatched = dept_fk.get("unmatched_user_dept_count") or 0
        blank = dept_fk.get("blank_or_null_dept_id_count") or 0
        if unmatched == 0:
            summary["key_findings"].append("非空 dept_id 全部可以关联到 dim_department。")
        else:
            dq_pass = False
            summary["warnings"].append(f"存在 {unmatched} 条非空 dept_id 无法关联到 dim_department。")
        summary["key_findings"].append(f"dept_id 空值或空字符串数量为 {blank}。")

    if last_active:
        summary["key_findings"].append(
            f"last_active_at 空值数为 {last_active.get('null_last_active_at_count')}，时间范围为 {last_active.get('min_last_active_at')} 到 {last_active.get('max_last_active_at')}。"
        )

    if usage_cov:
        summary["key_findings"].append(
            f"fact_daily_usage 覆盖了 {usage_cov.get('covered_user_count')} 个用户。"
        )

    if mapping_cov:
        summary["key_findings"].append(
            f"dim_user_id_mapping 覆盖了 {mapping_cov.get('mapped_user_count')} 个用户。"
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
        "# dim_user Table Card Validation Report",
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
        FROM dim_user;
    """))
    checks.append(run_query(conn, "column_count", """
        SELECT COUNT(*) AS column_count
        FROM information_schema.columns
        WHERE table_name = 'dim_user';
    """))
    checks.append(run_query(conn, "user_id_uniqueness", """
        SELECT
            COUNT(*) AS row_count,
            COUNT(DISTINCT user_id) AS distinct_user_id_count,
            COUNT(*) - COUNT(DISTINCT user_id) AS duplicate_user_id_count
        FROM dim_user;
    """))
    checks.append(run_query(conn, "duplicated_user_id_examples", """
        SELECT
            user_id,
            COUNT(*) AS duplicate_count
        FROM dim_user
        GROUP BY user_id
        HAVING COUNT(*) > 1
        ORDER BY duplicate_count DESC, user_id
        LIMIT 20;
    """))
    checks.append(run_query(conn, "tenant_id_fk_coverage", """
        SELECT
            COUNT(*) AS row_count,
            SUM(CASE WHEN t.tenant_id IS NULL THEN 1 ELSE 0 END) AS unmatched_user_tenant_count
        FROM dim_user u
        LEFT JOIN dim_tenant t
            ON u.tenant_id = t.tenant_id;
    """))
    checks.append(run_query(conn, "dept_id_fk_coverage", """
        SELECT
            COUNT(*) AS row_count,
            SUM(CASE WHEN u.dept_id IS NULL OR CAST(u.dept_id AS VARCHAR) = '' THEN 1 ELSE 0 END) AS blank_or_null_dept_id_count,
            SUM(
                CASE
                    WHEN u.dept_id IS NOT NULL
                     AND CAST(u.dept_id AS VARCHAR) != ''
                     AND d.dept_id IS NULL
                    THEN 1 ELSE 0
                END
            ) AS unmatched_user_dept_count
        FROM dim_user u
        LEFT JOIN dim_department d
            ON u.dept_id = d.dept_id;
    """))
    checks.append(run_query(conn, "orphan_dept_examples", """
        SELECT
            u.user_id,
            u.tenant_id,
            u.dept_id
        FROM dim_user u
        LEFT JOIN dim_department d
            ON u.dept_id = d.dept_id
        WHERE u.dept_id IS NOT NULL
          AND CAST(u.dept_id AS VARCHAR) != ''
          AND d.dept_id IS NULL
        LIMIT 20;
    """))
    checks.append(run_query(conn, "role_distribution", """
        SELECT
            role,
            COUNT(DISTINCT user_id) AS user_count
        FROM dim_user
        GROUP BY role
        ORDER BY user_count DESC, role;
    """))
    checks.append(run_query(conn, "status_distribution", """
        SELECT
            status,
            COUNT(DISTINCT user_id) AS user_count
        FROM dim_user
        GROUP BY status
        ORDER BY user_count DESC, status;
    """))
    checks.append(run_query(conn, "register_at_range_check", """
        SELECT
            COUNT(*) AS row_count,
            SUM(CASE WHEN register_at IS NULL THEN 1 ELSE 0 END) AS null_register_at_count,
            MIN(register_at) AS min_register_at,
            MAX(register_at) AS max_register_at
        FROM dim_user;
    """))
    checks.append(run_query(conn, "last_active_at_null_and_range_check", """
        SELECT
            COUNT(*) AS row_count,
            SUM(CASE WHEN last_active_at IS NULL THEN 1 ELSE 0 END) AS null_last_active_at_count,
            MIN(last_active_at) AS min_last_active_at,
            MAX(last_active_at) AS max_last_active_at
        FROM dim_user;
    """))
    checks.append(run_query(conn, "fact_daily_usage_join_coverage", """
        SELECT
            COUNT(DISTINCT u.user_id) AS total_user_count,
            COUNT(DISTINCT f.user_id) AS covered_user_count,
            COUNT(DISTINCT u.user_id) - COUNT(DISTINCT f.user_id) AS uncovered_user_count
        FROM dim_user u
        LEFT JOIN fact_daily_usage f
            ON u.user_id = f.user_id;
    """))
    checks.append(run_query(conn, "user_id_mapping_coverage", """
        SELECT
            COUNT(DISTINCT u.user_id) AS total_user_count,
            COUNT(DISTINCT m.user_id) AS mapped_user_count,
            COUNT(DISTINCT u.user_id) - COUNT(DISTINCT m.user_id) AS unmapped_user_count
        FROM dim_user u
        LEFT JOIN dim_user_id_mapping m
            ON u.user_id = m.user_id;
    """))
    checks.append(run_query(conn, "sql_pattern_registered_users_by_month", """
        SELECT
            STRFTIME(DATE_TRUNC('month', register_at), '%Y-%m') AS register_month,
            COUNT(DISTINCT user_id) AS registered_user_count
        FROM dim_user
        GROUP BY register_month
        ORDER BY register_month;
    """))
    checks.append(run_query(conn, "sql_pattern_users_by_role", """
        SELECT
            role,
            COUNT(DISTINCT user_id) AS user_count
        FROM dim_user
        GROUP BY role
        ORDER BY user_count DESC, role;
    """))
    checks.append(run_query(conn, "sql_pattern_users_by_tenant_country", """
        SELECT
            t.country,
            COUNT(DISTINCT u.user_id) AS user_count
        FROM dim_user u
        LEFT JOIN dim_tenant t
            ON u.tenant_id = t.tenant_id
        GROUP BY t.country
        ORDER BY user_count DESC, t.country;
    """))

    output = {
        "table_name": "dim_user",
        "summary": build_summary(checks),
        "checks": checks,
    }

    JSON_OUTPUT.write_text(json.dumps(output, ensure_ascii=False, indent=2), encoding="utf-8")
    write_markdown_report(output)
    print(f"已输出：{JSON_OUTPUT}")
    print(f"已输出：{MD_OUTPUT}")


if __name__ == "__main__":
    main()
