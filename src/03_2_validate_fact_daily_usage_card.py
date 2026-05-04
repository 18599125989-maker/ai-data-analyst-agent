#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
功能：
验证 knowledge_manual/table_cards_draft/fact_daily_usage.yaml 中的核心假设。

验证内容：
1. 表行数与字段数
2. user_id + dt 是否唯一
3. 与 dim_user 的外键覆盖率
4. dt 时间范围
5. session_count / active_duration_sec 数值范围
6. feature_usage_json 的空值与 JSON 兼容性
7. 主要 SQL patterns 是否可执行

输出：
outputs/knowledge/validation/
├── fact_daily_usage_validation.json
└── fact_daily_usage_validation.md
"""

import json
from pathlib import Path

import duckdb


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DB_PATH = PROJECT_ROOT / "cloudwork.duckdb"

OUTPUT_DIR = PROJECT_ROOT / "outputs" / "knowledge" / "validation"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

JSON_OUTPUT = OUTPUT_DIR / "fact_daily_usage_validation.json"
MD_OUTPUT = OUTPUT_DIR / "fact_daily_usage_validation.md"


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
    grain = get_first_row(checks, "user_dt_uniqueness")
    fk = get_first_row(checks, "user_id_fk_coverage")
    dt_range = get_first_row(checks, "dt_range_check")
    session_range = get_first_row(checks, "session_count_range_check")
    duration_range = get_first_row(checks, "active_duration_range_check")
    json_null = get_first_row(checks, "feature_usage_json_null_check")
    json_parse = get_first_row(checks, "feature_usage_json_parse_check")

    if basic and col:
        summary["profile_filled"] = True
        summary["key_findings"].append(
            f"fact_daily_usage 行数为 {basic.get('row_count')}，字段数为 {col.get('column_count')}。"
        )

    if grain and (grain.get("duplicate_user_dt_count") or 0) == 0:
        summary["grain_validated"] = True
        summary["key_findings"].append("user_id + dt 当前唯一，可支持“一行一个用户一天”的粒度判断。")
    elif grain:
        summary["warnings"].append(f"user_id + dt 存在 {grain.get('duplicate_user_dt_count')} 条重复。")

    dq_pass = True

    if fk and (fk.get("unmatched_daily_usage_user_count") or 0) != 0:
        dq_pass = False
        summary["warnings"].append(f"存在 {fk.get('unmatched_daily_usage_user_count')} 条日使用记录无法关联到 dim_user。")
    elif fk:
        summary["key_findings"].append("user_id 全部可以关联到 dim_user.user_id。")

    if dt_range:
        summary["key_findings"].append(
            f"dt 时间范围为 {dt_range.get('min_dt')} 到 {dt_range.get('max_dt')}。"
        )

    if session_range:
        if (session_range.get("negative_session_count") or 0) > 0:
            dq_pass = False
            summary["warnings"].append(f"存在 {session_range.get('negative_session_count')} 条负 session_count。")
        else:
            summary["key_findings"].append(
                f"session_count 范围为 {session_range.get('min_session_count')} 到 {session_range.get('max_session_count')}。"
            )

    if duration_range:
        if (duration_range.get("negative_active_duration_count") or 0) > 0:
            dq_pass = False
            summary["warnings"].append(f"存在 {duration_range.get('negative_active_duration_count')} 条负 active_duration_sec。")
        else:
            summary["key_findings"].append(
                f"active_duration_sec 范围为 {duration_range.get('min_active_duration_sec')} 到 {duration_range.get('max_active_duration_sec')}。"
            )

    if json_null:
        summary["key_findings"].append(
            f"feature_usage_json 空值或空字符串数量为 {json_null.get('blank_feature_usage_json_count')}。"
        )

    if json_parse:
        invalid_json = json_parse.get("invalid_feature_usage_json_count") or 0
        if invalid_json > 0:
            dq_pass = False
            summary["warnings"].append(f"存在 {invalid_json} 条 feature_usage_json 无法作为 JSON 解析。")
        else:
            summary["key_findings"].append("feature_usage_json 当前全部可以作为 JSON 解析。")

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
        "# fact_daily_usage Table Card Validation Report",
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
        FROM fact_daily_usage;
    """))
    checks.append(run_query(conn, "column_count", """
        SELECT COUNT(*) AS column_count
        FROM information_schema.columns
        WHERE table_name = 'fact_daily_usage';
    """))
    checks.append(run_query(conn, "user_dt_uniqueness", """
        SELECT
            COUNT(*) AS row_count,
            COUNT(DISTINCT CAST(user_id AS VARCHAR) || '|' || CAST(dt AS VARCHAR)) AS distinct_user_dt_count,
            COUNT(*) - COUNT(DISTINCT CAST(user_id AS VARCHAR) || '|' || CAST(dt AS VARCHAR)) AS duplicate_user_dt_count
        FROM fact_daily_usage;
    """))
    checks.append(run_query(conn, "duplicated_user_dt_examples", """
        SELECT
            user_id,
            dt,
            COUNT(*) AS duplicate_count
        FROM fact_daily_usage
        GROUP BY user_id, dt
        HAVING COUNT(*) > 1
        ORDER BY duplicate_count DESC, user_id, dt
        LIMIT 20;
    """))
    checks.append(run_query(conn, "user_id_fk_coverage", """
        SELECT
            COUNT(*) AS row_count,
            SUM(CASE WHEN u.user_id IS NULL THEN 1 ELSE 0 END) AS unmatched_daily_usage_user_count
        FROM fact_daily_usage f
        LEFT JOIN dim_user u
            ON f.user_id = u.user_id;
    """))
    checks.append(run_query(conn, "unmatched_user_examples", """
        SELECT
            f.user_id,
            f.dt
        FROM fact_daily_usage f
        LEFT JOIN dim_user u
            ON f.user_id = u.user_id
        WHERE u.user_id IS NULL
        LIMIT 20;
    """))
    checks.append(run_query(conn, "dt_range_check", """
        SELECT
            MIN(dt) AS min_dt,
            MAX(dt) AS max_dt,
            COUNT(DISTINCT dt) AS distinct_dt_count
        FROM fact_daily_usage;
    """))
    checks.append(run_query(conn, "session_count_range_check", """
        SELECT
            MIN(session_count) AS min_session_count,
            MAX(session_count) AS max_session_count,
            SUM(CASE WHEN session_count < 0 THEN 1 ELSE 0 END) AS negative_session_count,
            SUM(CASE WHEN session_count = 0 THEN 1 ELSE 0 END) AS zero_session_count
        FROM fact_daily_usage;
    """))
    checks.append(run_query(conn, "active_duration_range_check", """
        SELECT
            MIN(active_duration_sec) AS min_active_duration_sec,
            MAX(active_duration_sec) AS max_active_duration_sec,
            SUM(CASE WHEN active_duration_sec < 0 THEN 1 ELSE 0 END) AS negative_active_duration_count,
            SUM(CASE WHEN active_duration_sec = 0 THEN 1 ELSE 0 END) AS zero_active_duration_count
        FROM fact_daily_usage;
    """))
    checks.append(run_query(conn, "feature_usage_json_null_check", """
        SELECT
            COUNT(*) AS row_count,
            SUM(
                CASE
                    WHEN feature_usage_json IS NULL OR CAST(feature_usage_json AS VARCHAR) = ''
                    THEN 1 ELSE 0
                END
            ) AS blank_feature_usage_json_count
        FROM fact_daily_usage;
    """))
    checks.append(run_query(conn, "feature_usage_json_parse_check", """
        SELECT
            COUNT(*) AS row_count,
            SUM(
                CASE
                    WHEN TRY_CAST(feature_usage_json AS JSON) IS NULL
                    THEN 1 ELSE 0
                END
            ) AS invalid_feature_usage_json_count
        FROM fact_daily_usage;
    """))
    checks.append(run_query(conn, "sql_pattern_daily_active_users", """
        SELECT
            dt,
            COUNT(DISTINCT user_id) AS active_user_count
        FROM fact_daily_usage
        GROUP BY dt
        ORDER BY dt;
    """))
    checks.append(run_query(conn, "sql_pattern_session_and_duration_by_day", """
        SELECT
            dt,
            SUM(session_count) AS total_session_count,
            SUM(active_duration_sec) AS total_active_duration_sec
        FROM fact_daily_usage
        GROUP BY dt
        ORDER BY dt;
    """))
    checks.append(run_query(conn, "sql_pattern_tenant_level_daily_usage", """
        SELECT
            t.country,
            COUNT(DISTINCT f.user_id) AS active_user_count,
            SUM(f.session_count) AS total_session_count,
            SUM(f.active_duration_sec) AS total_active_duration_sec
        FROM fact_daily_usage f
        LEFT JOIN dim_user u
            ON f.user_id = u.user_id
        LEFT JOIN dim_tenant t
            ON u.tenant_id = t.tenant_id
        GROUP BY t.country
        ORDER BY active_user_count DESC, t.country;
    """))

    output = {
        "table_name": "fact_daily_usage",
        "summary": build_summary(checks),
        "checks": checks,
    }

    JSON_OUTPUT.write_text(json.dumps(output, ensure_ascii=False, indent=2), encoding="utf-8")
    write_markdown_report(output)
    print(f"已输出：{JSON_OUTPUT}")
    print(f"已输出：{MD_OUTPUT}")


if __name__ == "__main__":
    main()
