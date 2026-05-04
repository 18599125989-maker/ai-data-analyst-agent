#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
功能：
验证 knowledge_manual/table_cards/fact_ai_usage_log.yaml 中的核心假设。

验证内容：
1. 表行数与字段数
2. log_id 是否唯一
3. user_id_hash 是否能映射到 dim_user_id_mapping
4. 映射后的 user_id 是否能关联 dim_user
5. operation_type 枚举分布
6. model_name 枚举分布
7. created_at 是否能转换为 Unix timestamp
8. credits_amount 是否存在负值、零值或异常范围
9. remark 是否为空、是否可解析为 JSON
10. 主要 SQL patterns 是否可执行

输出：
outputs/knowledge/validation/
├── fact_ai_usage_log_validation.json
└── fact_ai_usage_log_validation.md
"""

import json
from pathlib import Path

import duckdb


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DB_PATH = PROJECT_ROOT / "cloudwork.duckdb"

OUTPUT_DIR = PROJECT_ROOT / "outputs" / "knowledge" / "validation"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

JSON_OUTPUT = OUTPUT_DIR / "fact_ai_usage_log_validation.json"
MD_OUTPUT = OUTPUT_DIR / "fact_ai_usage_log_validation.md"


def make_json_safe(value):
    """把 Pandas / DuckDB 返回的特殊对象转成 JSON 可保存的普通对象"""
    if value is None:
        return None

    # pandas / numpy 的缺失值
    try:
        import pandas as pd
        if pd.isna(value):
            return None
    except Exception:
        pass

    # numpy 标量
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

    # datetime / timestamp / date
    if hasattr(value, "isoformat"):
        try:
            return value.isoformat()
        except Exception:
            pass

    # 其他无法 JSON 序列化的对象统一转字符串
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
    col_count = get_first_row(checks, "column_count")
    pk = get_first_row(checks, "log_id_uniqueness")
    mapping = get_first_row(checks, "user_id_hash_mapping_coverage")
    mapped_user = get_first_row(checks, "mapped_user_coverage")
    timestamp_check = get_first_row(checks, "created_at_timestamp_parse_check")
    credits_check = get_first_row(checks, "credits_amount_range_check")
    remark_check = get_first_row(checks, "remark_null_and_json_check")
    op_summary = get_first_row(checks, "operation_type_distinct_summary")
    model_summary = get_first_row(checks, "model_name_distinct_summary")

    if basic and col_count:
        summary["profile_filled"] = True
        summary["key_findings"].append(
            f"fact_ai_usage_log 行数为 {basic.get('row_count')}，字段数为 {col_count.get('column_count')}。"
        )

    if pk:
        row_count = pk.get("row_count")
        distinct_count = pk.get("distinct_log_id_count")
        duplicate_count = pk.get("duplicate_log_id_count")

        if row_count == distinct_count and duplicate_count == 0:
            summary["grain_validated"] = True
            summary["key_findings"].append("log_id 唯一，可以验证“一行 = 一次 AI 操作”的 grain。")
        else:
            summary["warnings"].append(
                f"log_id 不唯一：row_count={row_count}, distinct={distinct_count}, duplicate={duplicate_count}。"
            )

    dq_pass = True

    if mapping:
        unmatched = mapping.get("unmatched_ai_user_hash_count")
        if unmatched == 0:
            summary["key_findings"].append("user_id_hash 全部可以映射到 dim_user_id_mapping.user_id_hash。")
        else:
            dq_pass = False
            summary["warnings"].append(
                f"存在 {unmatched} 条 AI 日志无法通过 user_id_hash 映射到 dim_user_id_mapping。"
            )

    if mapped_user:
        unmatched_user = mapped_user.get("unmatched_mapped_user_count")
        if unmatched_user == 0:
            summary["key_findings"].append("映射后的 user_id 全部可以关联到 dim_user.user_id。")
        else:
            dq_pass = False
            summary["warnings"].append(
                f"存在 {unmatched_user} 条映射后的 user_id 无法关联到 dim_user。"
            )

    if op_summary:
        op_count = op_summary.get("operation_type_count")
        op_values = op_summary.get("operation_type_values")
        summary["key_findings"].append(
            f"operation_type 共 {op_count} 种取值：{op_values}。"
        )

        expected_ops = {"deduct", "earn", "refund"}
        actual_ops = set(str(op_values).split(", ")) if op_values else set()
        unexpected_ops = actual_ops - expected_ops

        if unexpected_ops:
            dq_pass = False
            summary["warnings"].append(
                f"operation_type 存在非预期取值：{sorted(unexpected_ops)}。"
            )

    if model_summary:
        summary["key_findings"].append(
            f"model_name 共 {model_summary.get('model_name_count')} 种取值：{model_summary.get('model_name_values')}。"
        )

    if timestamp_check:
        invalid_ts = timestamp_check.get("invalid_created_at_count")
        min_time = timestamp_check.get("min_created_time")
        max_time = timestamp_check.get("max_created_time")

        if invalid_ts == 0:
            summary["key_findings"].append(
                f"created_at 全部可转换为 Unix timestamp，时间范围为 {min_time} 到 {max_time}。"
            )
        else:
            dq_pass = False
            summary["warnings"].append(
                f"存在 {invalid_ts} 条 created_at 无法转换为 BIGINT timestamp。"
            )

    if credits_check:
        negative_count = credits_check.get("negative_credits_count")
        zero_count = credits_check.get("zero_credits_count")
        min_credit = credits_check.get("min_credits_amount")
        max_credit = credits_check.get("max_credits_amount")

        if negative_count == 0:
            summary["key_findings"].append(
                f"credits_amount 范围为 {min_credit} 到 {max_credit}，未发现负值；零值数量为 {zero_count}。"
            )
        else:
            dq_pass = False
            summary["warnings"].append(
                f"credits_amount 存在 {negative_count} 条负值。"
            )

    if remark_check:
        empty_remark = remark_check.get("empty_remark_count")
        invalid_json = remark_check.get("invalid_json_remark_count")

        if empty_remark == 0:
            summary["key_findings"].append("remark 没有空值。")
        else:
            summary["warnings"].append(f"remark 存在 {empty_remark} 条空值。")

        if invalid_json == 0:
            summary["key_findings"].append("remark 全部可以解析为 JSON。")
        else:
            dq_pass = False
            summary["warnings"].append(
                f"remark 存在 {invalid_json} 条无法解析为 JSON。"
            )

    # SQL pattern 是否有失败
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
    lines.append("# fact_ai_usage_log Table Card Validation Report")
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

    with open(MD_OUTPUT, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))


def main():
    if not DB_PATH.exists():
        raise FileNotFoundError(f"找不到数据库文件：{DB_PATH}")

    conn = duckdb.connect(str(DB_PATH), read_only=True)

    checks = []

    # 1. 表基础信息
    checks.append(run_query(
        conn,
        "basic_table_profile",
        """
        SELECT
            COUNT(*) AS row_count
        FROM fact_ai_usage_log;
        """
    ))

    # 2. 字段数
    checks.append(run_query(
        conn,
        "column_count",
        """
        SELECT
            COUNT(*) AS column_count
        FROM information_schema.columns
        WHERE table_name = 'fact_ai_usage_log';
        """
    ))

    # 3. log_id 主键唯一性
    checks.append(run_query(
        conn,
        "log_id_uniqueness",
        """
        SELECT
            COUNT(*) AS row_count,
            COUNT(DISTINCT log_id) AS distinct_log_id_count,
            COUNT(*) - COUNT(DISTINCT log_id) AS duplicate_log_id_count
        FROM fact_ai_usage_log;
        """
    ))

    # 4. 重复 log_id 示例
    checks.append(run_query(
        conn,
        "duplicated_log_id_examples",
        """
        SELECT
            log_id,
            COUNT(*) AS duplicate_count
        FROM fact_ai_usage_log
        GROUP BY log_id
        HAVING COUNT(*) > 1
        ORDER BY duplicate_count DESC
        LIMIT 20;
        """
    ))

    # 5. user_id_hash 映射覆盖率
    checks.append(run_query(
        conn,
        "user_id_hash_mapping_coverage",
        """
        SELECT
            COUNT(*) AS unmatched_ai_user_hash_count
        FROM fact_ai_usage_log a
        LEFT JOIN dim_user_id_mapping m
            ON a.user_id_hash = m.user_id_hash
        WHERE m.user_id_hash IS NULL;
        """
    ))

    # 6. 映射后的 user_id 是否能关联 dim_user
    checks.append(run_query(
        conn,
        "mapped_user_coverage",
        """
        SELECT
            COUNT(*) AS unmatched_mapped_user_count
        FROM fact_ai_usage_log a
        LEFT JOIN dim_user_id_mapping m
            ON a.user_id_hash = m.user_id_hash
        LEFT JOIN dim_user u
            ON m.user_id = u.user_id
        WHERE m.user_id IS NOT NULL
          AND u.user_id IS NULL;
        """
    ))

    # 7. 映射覆盖率详情
    checks.append(run_query(
        conn,
        "mapping_coverage_summary",
        """
        SELECT
            COUNT(*) AS ai_log_count,
            COUNT(DISTINCT a.user_id_hash) AS distinct_ai_user_hash_count,
            COUNT(DISTINCT m.user_id_hash) AS mapped_user_hash_count,
            COUNT(DISTINCT m.user_id) AS mapped_user_count,
            COUNT(DISTINCT u.user_id) AS matched_dim_user_count
        FROM fact_ai_usage_log a
        LEFT JOIN dim_user_id_mapping m
            ON a.user_id_hash = m.user_id_hash
        LEFT JOIN dim_user u
            ON m.user_id = u.user_id;
        """
    ))

    # 8. operation_type 分布
    checks.append(run_query(
        conn,
        "operation_type_distribution",
        """
        SELECT
            operation_type,
            COUNT(*) AS row_count,
            SUM(credits_amount) AS credits_amount
        FROM fact_ai_usage_log
        GROUP BY operation_type
        ORDER BY row_count DESC;
        """
    ))

    # 9. operation_type summary
    checks.append(run_query(
        conn,
        "operation_type_distinct_summary",
        """
        SELECT
            COUNT(DISTINCT operation_type) AS operation_type_count,
            STRING_AGG(DISTINCT operation_type, ', ') AS operation_type_values
        FROM fact_ai_usage_log;
        """
    ))

    # 10. model_name 分布
    checks.append(run_query(
        conn,
        "model_name_distribution",
        """
        SELECT
            model_name,
            COUNT(*) AS row_count,
            SUM(CASE WHEN operation_type = 'deduct' THEN credits_amount ELSE 0 END) AS credits_consumed
        FROM fact_ai_usage_log
        GROUP BY model_name
        ORDER BY row_count DESC;
        """
    ))

    # 11. model_name summary
    checks.append(run_query(
        conn,
        "model_name_distinct_summary",
        """
        SELECT
            COUNT(DISTINCT model_name) AS model_name_count,
            STRING_AGG(DISTINCT model_name, ', ') AS model_name_values
        FROM fact_ai_usage_log;
        """
    ))

    # 12. created_at timestamp 转换检查
    checks.append(run_query(
        conn,
        "created_at_timestamp_parse_check",
        """
        SELECT
            COUNT(*) AS row_count,
            SUM(
                CASE
                    WHEN TRY_CAST(created_at AS BIGINT) IS NULL THEN 1 ELSE 0
                END
            ) AS invalid_created_at_count,
            MIN(TO_TIMESTAMP(TRY_CAST(created_at AS BIGINT))) AS min_created_time,
            MAX(TO_TIMESTAMP(TRY_CAST(created_at AS BIGINT))) AS max_created_time
        FROM fact_ai_usage_log;
        """
    ))

    # 13. created_at 按月分布
    checks.append(run_query(
        conn,
        "created_at_month_distribution",
        """
        SELECT
            STRFTIME(TO_TIMESTAMP(CAST(created_at AS BIGINT)), '%Y-%m') AS month,
            COUNT(*) AS row_count,
            SUM(CASE WHEN operation_type = 'deduct' THEN credits_amount ELSE 0 END) AS credits_consumed
        FROM fact_ai_usage_log
        GROUP BY month
        ORDER BY month;
        """
    ))

    # 14. credits amount 范围检查
    checks.append(run_query(
        conn,
        "credits_amount_range_check",
        """
        SELECT
            MIN(credits_amount) AS min_credits_amount,
            MAX(credits_amount) AS max_credits_amount,
            SUM(CASE WHEN credits_amount < 0 THEN 1 ELSE 0 END) AS negative_credits_count,
            SUM(CASE WHEN credits_amount = 0 THEN 1 ELSE 0 END) AS zero_credits_count,
            ROUND(AVG(credits_amount), 2) AS avg_credits_amount
        FROM fact_ai_usage_log;
        """
    ))

    # 15. credits amount by operation_type
    checks.append(run_query(
        conn,
        "credits_amount_by_operation_type",
        """
        SELECT
            operation_type,
            MIN(credits_amount) AS min_credits_amount,
            MAX(credits_amount) AS max_credits_amount,
            ROUND(AVG(credits_amount), 2) AS avg_credits_amount,
            SUM(credits_amount) AS total_credits_amount
        FROM fact_ai_usage_log
        GROUP BY operation_type
        ORDER BY operation_type;
        """
    ))

    # 16. remark 空值和 JSON 检查
    checks.append(run_query(
        conn,
        "remark_null_and_json_check",
        """
        SELECT
            COUNT(*) AS row_count,
            SUM(CASE WHEN remark IS NULL OR CAST(remark AS VARCHAR) = '' THEN 1 ELSE 0 END) AS empty_remark_count,
            SUM(CASE WHEN TRY_CAST(remark AS JSON) IS NULL THEN 1 ELSE 0 END) AS invalid_json_remark_count
        FROM fact_ai_usage_log;
        """
    ))

    # 17. remark JSON 字段样例
    checks.append(run_query(
        conn,
        "remark_json_key_examples",
        """
        SELECT
            remark
        FROM fact_ai_usage_log
        WHERE remark IS NOT NULL
          AND CAST(remark AS VARCHAR) != ''
        LIMIT 5;
        """
    ))

    # 18. SQL pattern: ai credits by model
    checks.append(run_query(
        conn,
        "sql_pattern_ai_credits_by_model",
        """
        SELECT
            model_name,
            COUNT(DISTINCT log_id) AS operation_count,
            SUM(CASE WHEN operation_type = 'deduct' THEN credits_amount ELSE 0 END) AS credits_consumed
        FROM fact_ai_usage_log
        GROUP BY model_name
        ORDER BY credits_consumed DESC;
        """
    ))

    # 19. SQL pattern: daily AI credits trend
    checks.append(run_query(
        conn,
        "sql_pattern_daily_ai_credits_trend",
        """
        SELECT
            CAST(TO_TIMESTAMP(CAST(created_at AS BIGINT)) AS DATE) AS dt,
            COUNT(DISTINCT log_id) AS operation_count,
            SUM(CASE WHEN operation_type = 'deduct' THEN credits_amount ELSE 0 END) AS credits_consumed
        FROM fact_ai_usage_log
        GROUP BY dt
        ORDER BY dt
        LIMIT 30;
        """
    ))

    # 20. SQL pattern: monthly AI credits trend
    checks.append(run_query(
        conn,
        "sql_pattern_monthly_ai_credits_trend",
        """
        SELECT
            STRFTIME(TO_TIMESTAMP(CAST(created_at AS BIGINT)), '%Y-%m') AS month,
            COUNT(DISTINCT log_id) AS operation_count,
            SUM(CASE WHEN operation_type = 'deduct' THEN credits_amount ELSE 0 END) AS credits_consumed
        FROM fact_ai_usage_log
        GROUP BY month
        ORDER BY month;
        """
    ))

    # 21. SQL pattern: AI credits by tenant
    checks.append(run_query(
        conn,
        "sql_pattern_ai_credits_by_tenant",
        """
        SELECT
            u.tenant_id,
            COUNT(DISTINCT a.log_id) AS ai_operation_count,
            SUM(CASE WHEN a.operation_type = 'deduct' THEN a.credits_amount ELSE 0 END) AS credits_consumed
        FROM fact_ai_usage_log a
        LEFT JOIN dim_user_id_mapping m
            ON a.user_id_hash = m.user_id_hash
        LEFT JOIN dim_user u
            ON m.user_id = u.user_id
        GROUP BY u.tenant_id
        ORDER BY credits_consumed DESC
        LIMIT 30;
        """
    ))

    # 22. SQL pattern: AI credits by tenant profile
    checks.append(run_query(
        conn,
        "sql_pattern_ai_credits_by_tenant_profile",
        """
        SELECT
            t.country,
            t.industry,
            t.size_tier,
            COUNT(DISTINCT a.log_id) AS ai_operation_count,
            SUM(CASE WHEN a.operation_type = 'deduct' THEN a.credits_amount ELSE 0 END) AS credits_consumed
        FROM fact_ai_usage_log a
        LEFT JOIN dim_user_id_mapping m
            ON a.user_id_hash = m.user_id_hash
        LEFT JOIN dim_user u
            ON m.user_id = u.user_id
        LEFT JOIN dim_tenant t
            ON u.tenant_id = t.tenant_id
        GROUP BY t.country, t.industry, t.size_tier
        ORDER BY credits_consumed DESC
        LIMIT 30;
        """
    ))

    # 23. SQL pattern: AI user activity comparison daily
    checks.append(run_query(
        conn,
        "sql_pattern_ai_user_activity_comparison_daily",
        """
        WITH ai_daily AS (
            SELECT
                m.user_id,
                CAST(TO_TIMESTAMP(CAST(a.created_at AS BIGINT)) AS DATE) AS dt,
                COUNT(DISTINCT a.log_id) AS ai_operation_count,
                SUM(CASE WHEN a.operation_type = 'deduct' THEN a.credits_amount ELSE 0 END) AS credits_consumed
            FROM fact_ai_usage_log a
            LEFT JOIN dim_user_id_mapping m
                ON a.user_id_hash = m.user_id_hash
            GROUP BY m.user_id, CAST(TO_TIMESTAMP(CAST(a.created_at AS BIGINT)) AS DATE)
        )
        SELECT
            d.dt,
            CASE WHEN ai.user_id IS NULL THEN 'non_ai_user_day' ELSE 'ai_user_day' END AS ai_usage_group,
            COUNT(DISTINCT d.user_id) AS user_count,
            AVG(d.active_duration_sec) AS avg_active_duration_sec,
            AVG(d.session_count) AS avg_session_count
        FROM fact_daily_usage d
        LEFT JOIN ai_daily ai
            ON d.user_id = ai.user_id
           AND d.dt = ai.dt
        GROUP BY d.dt, ai_usage_group
        ORDER BY d.dt, ai_usage_group
        LIMIT 30;
        """
    ))

    conn.close()

    summary = build_summary(checks)

    output = {
        "table_name": "fact_ai_usage_log",
        "validation_target": "knowledge_manual/table_cards/fact_ai_usage_log.yaml",
        "summary": summary,
        "checks": checks,
    }

    with open(JSON_OUTPUT, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    write_markdown_report(output)

    print("")
    print("验证完成。")
    print(f"JSON 输出：{JSON_OUTPUT}")
    print(f"Markdown 输出：{MD_OUTPUT}")
    print("")
    print("你可以把 Markdown 结果发给我，我会帮你修改 fact_ai_usage_log.yaml。")


if __name__ == "__main__":
    main()