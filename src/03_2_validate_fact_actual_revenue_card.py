#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
功能：
验证 knowledge_manual/table_cards/fact_actual_revenue.yaml 中的核心假设。

验证内容：
1. 表行数与字段数
2. revenue_id 是否唯一
3. sub_id + month 是否唯一，验证“一行 = 一个订阅一个月”
4. tenant_id 是否都能关联 dim_tenant
5. sub_id 是否都能关联 fact_subscription
6. currency 分布
7. actual_revenue 公式是否基本成立
8. 是否存在负收入、异常折扣率、异常席位数
9. month 分布与收入趋势
10. 主要 SQL patterns 是否可执行

输出：
outputs/knowledge/validation/
├── fact_actual_revenue_validation.json
└── fact_actual_revenue_validation.md
"""

import json
from pathlib import Path

import duckdb


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DB_PATH = PROJECT_ROOT / "cloudwork.duckdb"

OUTPUT_DIR = PROJECT_ROOT / "outputs" / "knowledge" / "validation"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

JSON_OUTPUT = OUTPUT_DIR / "fact_actual_revenue_validation.json"
MD_OUTPUT = OUTPUT_DIR / "fact_actual_revenue_validation.md"


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
    col_count = get_first_row(checks, "column_count")
    pk = get_first_row(checks, "revenue_id_uniqueness")
    natural_key = get_first_row(checks, "sub_id_month_duplicate_summary")
    tenant_sub_natural_key = get_first_row(checks, "tenant_sub_id_month_duplicate_summary")
    tenant_fk = get_first_row(checks, "tenant_id_fk_coverage")
    sub_fk = get_first_row(checks, "sub_id_fk_coverage")
    formula = get_first_row(checks, "revenue_formula_check")
    negative = get_first_row(checks, "negative_and_abnormal_value_check")
    currency = get_first_row(checks, "currency_distinct_summary")
    exact_duplicate = get_first_row(checks, "exact_duplicate_row_check")

    if basic and col_count:
        summary["profile_filled"] = True
        summary["key_findings"].append(
            f"fact_actual_revenue 行数为 {basic.get('row_count')}，字段数为 {col_count.get('column_count')}。"
        )

    if pk:
        row_count = pk.get("row_count")
        distinct_count = pk.get("distinct_revenue_id_count")
        duplicate_count = pk.get("duplicate_revenue_id_count")

        if row_count == distinct_count and duplicate_count == 0:
            summary["key_findings"].append("revenue_id 唯一，可以作为主键。")
        else:
            summary["warnings"].append(
                f"revenue_id 不唯一：row_count={row_count}, distinct={distinct_count}, duplicate={duplicate_count}。"
            )

    summary["grain_validated"] = "partial"
    if natural_key:
        duplicated_groups = natural_key.get("duplicated_sub_id_month_groups")
        if duplicated_groups == 0:
            summary["key_findings"].append("sub_id + month 当前唯一。")
        else:
            summary["warnings"].append(
                f"存在 {duplicated_groups} 组重复的 sub_id + month，订阅月份级分析不能只取单条记录。"
            )

    dq_pass = True

    if tenant_sub_natural_key:
        duplicated_groups = tenant_sub_natural_key.get("duplicated_tenant_sub_month_groups")
        if duplicated_groups == 0:
            summary["key_findings"].append("tenant_id + sub_id + month 当前唯一。")
        else:
            summary["warnings"].append(
                f"tenant_id + sub_id + month 存在 {duplicated_groups} 组重复，订阅月份级分析需要聚合 revenue_id 级记录。"
            )

    if exact_duplicate:
        exact_duplicate_row_count = exact_duplicate.get("exact_duplicate_row_count")
        if exact_duplicate_row_count == 0:
            summary["key_findings"].append("重复组不是完全重复行，不建议删除。")
        else:
            summary["warnings"].append(
                f"存在 {exact_duplicate_row_count} 组完全重复行，需要进一步人工复核。"
            )

    if tenant_fk:
        unmatched = tenant_fk.get("unmatched_revenue_tenant_count")
        if unmatched == 0:
            summary["key_findings"].append("tenant_id 全部可以关联到 dim_tenant.tenant_id。")
        else:
            dq_pass = False
            summary["warnings"].append(f"存在 {unmatched} 条收入记录无法关联到 dim_tenant。")

    if sub_fk:
        unmatched = sub_fk.get("unmatched_revenue_sub_count")
        if unmatched == 0:
            summary["key_findings"].append("sub_id 全部可以关联到 fact_subscription.sub_id。")
        else:
            dq_pass = False
            summary["warnings"].append(f"存在 {unmatched} 条收入记录无法关联到 fact_subscription。")

    if currency:
        currency_count = currency.get("currency_count")
        currency_values = currency.get("currency_values")

        if currency_count == 1 and currency_values == "USD":
            summary["key_findings"].append("currency 仅包含 USD。")
        else:
            dq_pass = False
            summary["warnings"].append(
                f"currency 存在多个取值：currency_count={currency_count}, values={currency_values}。"
            )

    if formula:
        mismatch = formula.get("formula_mismatch_count")
        max_abs_diff = formula.get("max_abs_formula_diff")
        avg_relative_diff = formula.get("avg_relative_formula_diff")

        if mismatch == 0:
            summary["key_findings"].append("actual_revenue 公式校验通过。")
        else:
            summary["key_findings"].append(
                f"actual_revenue 公式存在 {mismatch} 条小额偏差记录，最大差异为 {max_abs_diff}，平均相对偏差为 {avg_relative_diff}。"
            )

    if negative:
        bad_counts = {
            "negative_actual_revenue_count": negative.get("negative_actual_revenue_count"),
            "negative_list_revenue_count": negative.get("negative_list_revenue_count"),
            "negative_seats_count": negative.get("negative_seats_count"),
            "abnormal_discount_rate_count": negative.get("abnormal_discount_rate_count"),
            "negative_coupon_amount_count": negative.get("negative_coupon_amount_count"),
        }

        bad_total = sum(v or 0 for v in bad_counts.values())

        if bad_total == 0:
            summary["key_findings"].append("未发现负收入、负席位、负优惠券或异常折扣率。")
        else:
            dq_pass = False
            summary["warnings"].append(f"发现异常数值：{bad_counts}")

    summary["dq_validated"] = True if dq_pass else "partial"
    summary["ready_for_agent_recommendation"] = True

    return summary


def write_markdown_report(output):
    summary = output["summary"]
    checks = output["checks"]

    lines = []
    lines.append("# fact_actual_revenue Table Card Validation Report")
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
        FROM fact_actual_revenue;
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
        WHERE table_name = 'fact_actual_revenue';
        """
    ))

    # 3. revenue_id 主键唯一性
    checks.append(run_query(
        conn,
        "revenue_id_uniqueness",
        """
        SELECT
            COUNT(*) AS row_count,
            COUNT(DISTINCT revenue_id) AS distinct_revenue_id_count,
            COUNT(*) - COUNT(DISTINCT revenue_id) AS duplicate_revenue_id_count
        FROM fact_actual_revenue;
        """
    ))

    # 4. 重复 revenue_id 示例
    checks.append(run_query(
        conn,
        "duplicated_revenue_id_examples",
        """
        SELECT
            revenue_id,
            COUNT(*) AS duplicate_count
        FROM fact_actual_revenue
        GROUP BY revenue_id
        HAVING COUNT(*) > 1
        ORDER BY duplicate_count DESC
        LIMIT 20;
        """
    ))

    # 5. sub_id + month 自然键重复情况
    checks.append(run_query(
        conn,
        "sub_id_month_duplicate_summary",
        """
        SELECT
            COUNT(*) AS duplicated_sub_id_month_groups
        FROM (
            SELECT
                sub_id,
                month,
                COUNT(*) AS duplicate_count
            FROM fact_actual_revenue
            GROUP BY sub_id, month
            HAVING COUNT(*) > 1
        );
        """
    ))

    # 6. 重复 sub_id + month 示例
    checks.append(run_query(
        conn,
        "duplicated_sub_id_month_examples",
        """
        SELECT
            sub_id,
            month,
            COUNT(*) AS duplicate_count
        FROM fact_actual_revenue
        GROUP BY sub_id, month
        HAVING COUNT(*) > 1
        ORDER BY duplicate_count DESC
        LIMIT 20;
        """
    ))

    # 7. tenant_id + sub_id + month 重复情况
    checks.append(run_query(
        conn,
        "tenant_sub_id_month_duplicate_summary",
        """
        SELECT
            COUNT(*) AS duplicated_tenant_sub_month_groups
        FROM (
            SELECT
                tenant_id,
                sub_id,
                month,
                COUNT(*) AS duplicate_count
            FROM fact_actual_revenue
            GROUP BY tenant_id, sub_id, month
            HAVING COUNT(*) > 1
        );
        """
    ))

    # 8. tenant_id + sub_id + month 重复组详情
    checks.append(run_query(
        conn,
        "tenant_sub_id_month_duplicate_detail",
        """
        SELECT
            tenant_id,
            sub_id,
            month,
            COUNT(*) AS row_count,
            COUNT(DISTINCT revenue_id) AS revenue_record_count,
            COUNT(DISTINCT seats) AS distinct_seats_count,
            COUNT(DISTINCT list_price_per_seat) AS distinct_price_count,
            COUNT(DISTINCT list_revenue) AS distinct_list_revenue_count,
            COUNT(DISTINCT discount_rate) AS distinct_discount_rate_count,
            COUNT(DISTINCT coupon_amount) AS distinct_coupon_amount_count,
            COUNT(DISTINCT actual_revenue) AS distinct_actual_revenue_count,
            ROUND(SUM(actual_revenue), 2) AS total_actual_revenue,
            ROUND(MIN(actual_revenue), 2) AS min_actual_revenue,
            ROUND(MAX(actual_revenue), 2) AS max_actual_revenue
        FROM fact_actual_revenue
        GROUP BY tenant_id, sub_id, month
        HAVING COUNT(*) > 1
        ORDER BY row_count DESC, month, tenant_id, sub_id;
        """
    ))

    # 9. 完全重复行检查
    checks.append(run_query(
        conn,
        "exact_duplicate_row_check",
        """
        SELECT
            COUNT(*) AS exact_duplicate_row_count
        FROM (
            SELECT
                tenant_id,
                sub_id,
                month,
                seats,
                list_price_per_seat,
                list_revenue,
                discount_rate,
                coupon_amount,
                actual_revenue,
                currency,
                COUNT(*) AS duplicate_count
            FROM fact_actual_revenue
            GROUP BY
                tenant_id,
                sub_id,
                month,
                seats,
                list_price_per_seat,
                list_revenue,
                discount_rate,
                coupon_amount,
                actual_revenue,
                currency
            HAVING COUNT(*) > 1
        );
        """
    ))

    # 10. tenant_id 外键覆盖率
    checks.append(run_query(
        conn,
        "tenant_id_fk_coverage",
        """
        SELECT
            COUNT(*) AS unmatched_revenue_tenant_count
        FROM fact_actual_revenue r
        LEFT JOIN dim_tenant t
            ON r.tenant_id = t.tenant_id
        WHERE t.tenant_id IS NULL;
        """
    ))

    # 11. sub_id 外键覆盖率
    checks.append(run_query(
        conn,
        "sub_id_fk_coverage",
        """
        SELECT
            COUNT(*) AS unmatched_revenue_sub_count
        FROM fact_actual_revenue r
        LEFT JOIN fact_subscription s
            ON r.sub_id = s.sub_id
        WHERE s.sub_id IS NULL;
        """
    ))

    # 12. currency 取值分布
    checks.append(run_query(
        conn,
        "currency_distribution",
        """
        SELECT
            currency,
            COUNT(*) AS row_count
        FROM fact_actual_revenue
        GROUP BY currency
        ORDER BY row_count DESC;
        """
    ))

    # 13. currency summary
    checks.append(run_query(
        conn,
        "currency_distinct_summary",
        """
        SELECT
            COUNT(DISTINCT currency) AS currency_count,
            STRING_AGG(DISTINCT currency, ', ') AS currency_values
        FROM fact_actual_revenue;
        """
    ))

    # 14. 收入公式检查
    checks.append(run_query(
        conn,
        "revenue_formula_check",
        """
        SELECT
            COUNT(*) AS row_count,
            SUM(
                CASE
                    WHEN ABS(
                        actual_revenue - (list_revenue * (1 - discount_rate) - coupon_amount)
                    ) > 0.05
                    THEN 1 ELSE 0
                END
            ) AS formula_mismatch_count,
            ROUND(
                MAX(
                    ABS(
                        actual_revenue - (list_revenue * (1 - discount_rate) - coupon_amount)
                    )
                ),
                6
            ) AS max_abs_formula_diff,
            ROUND(
                AVG(
                    ABS(
                        actual_revenue - (list_revenue * (1 - discount_rate) - coupon_amount)
                    ) / NULLIF(actual_revenue, 0)
                ),
                6
            ) AS avg_relative_formula_diff
        FROM fact_actual_revenue;
        """
    ))

    # 15. 收入公式不一致示例
    checks.append(run_query(
        conn,
        "revenue_formula_mismatch_examples",
        """
        SELECT
            revenue_id,
            sub_id,
            month,
            list_revenue,
            discount_rate,
            coupon_amount,
            actual_revenue,
            ROUND(
                list_revenue * (1 - discount_rate) - coupon_amount,
                2
            ) AS calculated_actual_revenue,
            ROUND(
                actual_revenue - (list_revenue * (1 - discount_rate) - coupon_amount),
                6
            ) AS diff
        FROM fact_actual_revenue
        WHERE ABS(
            actual_revenue - (list_revenue * (1 - discount_rate) - coupon_amount)
        ) > 0.05
        ORDER BY ABS(
            actual_revenue - (list_revenue * (1 - discount_rate) - coupon_amount)
        ) DESC
        LIMIT 20;
        """
    ))

    # 16. 负值和异常值检查
    checks.append(run_query(
        conn,
        "negative_and_abnormal_value_check",
        """
        SELECT
            SUM(CASE WHEN actual_revenue < 0 THEN 1 ELSE 0 END) AS negative_actual_revenue_count,
            SUM(CASE WHEN list_revenue < 0 THEN 1 ELSE 0 END) AS negative_list_revenue_count,
            SUM(CASE WHEN seats < 0 THEN 1 ELSE 0 END) AS negative_seats_count,
            SUM(CASE WHEN coupon_amount < 0 THEN 1 ELSE 0 END) AS negative_coupon_amount_count,
            SUM(CASE WHEN discount_rate < 0 OR discount_rate > 1 THEN 1 ELSE 0 END) AS abnormal_discount_rate_count
        FROM fact_actual_revenue;
        """
    ))

    # 17. 基础数值范围
    checks.append(run_query(
        conn,
        "numeric_range_summary",
        """
        SELECT
            MIN(seats) AS min_seats,
            MAX(seats) AS max_seats,
            MIN(list_price_per_seat) AS min_list_price_per_seat,
            MAX(list_price_per_seat) AS max_list_price_per_seat,
            MIN(list_revenue) AS min_list_revenue,
            MAX(list_revenue) AS max_list_revenue,
            MIN(discount_rate) AS min_discount_rate,
            MAX(discount_rate) AS max_discount_rate,
            MIN(coupon_amount) AS min_coupon_amount,
            MAX(coupon_amount) AS max_coupon_amount,
            MIN(actual_revenue) AS min_actual_revenue,
            MAX(actual_revenue) AS max_actual_revenue
        FROM fact_actual_revenue;
        """
    ))

    # 18. month 分布和收入趋势
    checks.append(run_query(
        conn,
        "month_distribution",
        """
        SELECT
            month,
            COUNT(*) AS row_count,
            COUNT(DISTINCT sub_id) AS subscription_count,
            ROUND(SUM(list_revenue), 2) AS list_revenue,
            ROUND(SUM(actual_revenue), 2) AS actual_revenue,
            ROUND(SUM(list_revenue - actual_revenue), 2) AS revenue_gap
        FROM fact_actual_revenue
        GROUP BY month
        ORDER BY month;
        """
    ))

    # 19. month distinct summary
    checks.append(run_query(
        conn,
        "month_distinct_summary",
        """
        SELECT
            COUNT(DISTINCT month) AS month_count,
            MIN(month) AS min_month,
            MAX(month) AS max_month
        FROM fact_actual_revenue;
        """
    ))

    # 20. SQL pattern: monthly actual revenue trend
    checks.append(run_query(
        conn,
        "sql_pattern_monthly_actual_revenue_trend",
        """
        SELECT
            month,
            ROUND(SUM(list_revenue), 2) AS list_revenue,
            ROUND(SUM(actual_revenue), 2) AS actual_revenue,
            ROUND(SUM(list_revenue - actual_revenue), 2) AS revenue_gap
        FROM fact_actual_revenue
        GROUP BY month
        ORDER BY month;
        """
    ))

    # 21. SQL pattern: actual revenue by plan month
    checks.append(run_query(
        conn,
        "sql_pattern_actual_revenue_by_plan_month",
        """
        SELECT
            s.plan_tier,
            r.month,
            COUNT(DISTINCT r.sub_id) AS subscription_count,
            SUM(r.seats) AS total_seats,
            ROUND(SUM(r.list_revenue), 2) AS list_revenue,
            ROUND(SUM(r.actual_revenue), 2) AS actual_revenue,
            ROUND(SUM(r.list_revenue - r.actual_revenue), 2) AS revenue_gap
        FROM fact_actual_revenue r
        LEFT JOIN fact_subscription s
            ON r.sub_id = s.sub_id
        WHERE r.month = '2025-10'
        GROUP BY s.plan_tier, r.month
        ORDER BY actual_revenue DESC;
        """
    ))

    # 22. SQL pattern: discount impact by plan month
    checks.append(run_query(
        conn,
        "sql_pattern_discount_impact_by_plan_month",
        """
        SELECT
            s.plan_tier,
            r.month,
            ROUND(SUM(r.list_revenue), 2) AS list_revenue,
            ROUND(SUM(r.coupon_amount), 2) AS coupon_amount,
            ROUND(SUM(r.list_revenue * r.discount_rate), 2) AS estimated_discount_amount,
            ROUND(SUM(r.list_revenue - r.actual_revenue), 2) AS total_revenue_gap,
            ROUND(
                SUM(r.list_revenue - r.actual_revenue) / NULLIF(SUM(r.list_revenue), 0),
                4
            ) AS revenue_gap_rate
        FROM fact_actual_revenue r
        LEFT JOIN fact_subscription s
            ON r.sub_id = s.sub_id
        WHERE r.month = '2025-10'
        GROUP BY s.plan_tier, r.month
        ORDER BY total_revenue_gap DESC;
        """
    ))

    # 23. SQL pattern: actual revenue by tenant profile
    checks.append(run_query(
        conn,
        "sql_pattern_actual_revenue_by_tenant_profile",
        """
        SELECT
            t.country,
            t.industry,
            t.size_tier,
            r.month,
            COUNT(DISTINCT r.tenant_id) AS tenant_count,
            ROUND(SUM(r.actual_revenue), 2) AS actual_revenue
        FROM fact_actual_revenue r
        LEFT JOIN dim_tenant t
            ON r.tenant_id = t.tenant_id
        WHERE r.month = '2025-10'
        GROUP BY t.country, t.industry, t.size_tier, r.month
        ORDER BY actual_revenue DESC
        LIMIT 30;
        """
    ))

    # 24. SQL pattern: subscription month level revenue
    checks.append(run_query(
        conn,
        "sql_pattern_subscription_month_level_revenue",
        """
        SELECT
            tenant_id,
            sub_id,
            month,
            COUNT(DISTINCT revenue_id) AS revenue_record_count,
            SUM(seats) AS total_seats,
            ROUND(SUM(list_revenue), 2) AS list_revenue,
            ROUND(SUM(actual_revenue), 2) AS actual_revenue,
            ROUND(SUM(list_revenue - actual_revenue), 2) AS revenue_gap
        FROM fact_actual_revenue
        GROUP BY tenant_id, sub_id, month
        ORDER BY month, tenant_id, sub_id;
        """
    ))

    conn.close()

    summary = build_summary(checks)

    output = {
        "table_name": "fact_actual_revenue",
        "validation_target": "knowledge_manual/table_cards/fact_actual_revenue.yaml",
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
    print("你可以把 Markdown 结果发给我，我会帮你修改 fact_actual_revenue.yaml。")


if __name__ == "__main__":
    main()
