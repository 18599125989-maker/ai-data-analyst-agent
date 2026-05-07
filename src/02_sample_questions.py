#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
功能：
完成 SAMPLE_QUESTIONS.md 中的 3 道参考题，并保存 SQL、结果、图表和简要结论。

运行前请先执行：
1. python src/00_load_data.py
2. python src/01_profile_tables.py

运行方式：
python src/02_sample_questions.py

输出目录：
outputs/sample/question_1/
outputs/sample/question_2/
outputs/sample/question_3/
"""

from pathlib import Path

import duckdb
import matplotlib.pyplot as plt
import pandas as pd

try:
    from config_paths import DB_PATH, PROJECT_ROOT
except ModuleNotFoundError:
    from src.config_paths import DB_PATH, PROJECT_ROOT


# =========================
# 路径配置
# =========================

OUTPUT_DIR = PROJECT_ROOT / "outputs" / "sample"


# =========================
# 通用工具函数
# =========================

def ensure_dir(path: Path) -> None:
    """确保输出目录存在。"""
    path.mkdir(parents=True, exist_ok=True)


def run_sql(conn: duckdb.DuckDBPyConnection, sql: str) -> pd.DataFrame:
    """执行 SQL 并返回 DataFrame。"""
    return conn.execute(sql).df()


def save_sql(sql: str, output_dir: Path) -> None:
    """保存 SQL 文件，方便后续构建 Recipe。"""
    ensure_dir(output_dir)
    path = output_dir / "query.sql"
    path.write_text(sql.strip() + "\n", encoding="utf-8")
    print(f"  已保存 SQL: {path}")


def save_csv(df: pd.DataFrame, output_dir: Path, filename: str) -> None:
    """保存 DataFrame 为 CSV。"""
    ensure_dir(output_dir)
    path = output_dir / filename
    df.to_csv(path, index=False, encoding="utf-8-sig")
    print(f"  已保存 CSV: {path}")


def save_markdown(text: str, output_dir: Path, filename: str = "insight.md") -> None:
    """保存 Markdown 文本结论。"""
    ensure_dir(output_dir)
    path = output_dir / filename
    path.write_text(text.strip() + "\n", encoding="utf-8")
    print(f"  已保存 Insight: {path}")


def save_current_plot(output_dir: Path, filename: str) -> None:
    """保存当前 matplotlib 图像。"""
    ensure_dir(output_dir)
    path = output_dir / filename
    plt.tight_layout()
    plt.savefig(path, dpi=150)
    plt.close()
    print(f"  已保存图表: {path}")


# =========================
# 参考题 1：企业画像分布
# =========================

def question_1(conn: duckdb.DuckDBPyConnection) -> None:
    """
    参考题 1：企业画像分布

    目标：
    1. 按国家统计企业数量
    2. 按企业规模统计数量和占比
    3. 生成国家 × 行业二维分布表
    """

    print("\n========== 参考题 1：企业画像分布 ==========")

    output_dir = OUTPUT_DIR / "question_1"
    ensure_dir(output_dir)

    country_sql = """
SELECT
    country,
    COUNT(*) AS tenant_count
FROM dim_tenant
GROUP BY country
ORDER BY tenant_count DESC, country;
"""

    size_sql = """
SELECT
    size_tier,
    COUNT(*) AS tenant_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS percentage
FROM dim_tenant
GROUP BY size_tier
ORDER BY tenant_count DESC, size_tier;
"""

    country_industry_sql = """
SELECT
    country,
    industry,
    COUNT(*) AS tenant_count
FROM dim_tenant
GROUP BY country, industry
ORDER BY country, industry;
"""

    query_sql = (
        "-- Question 1: 企业画像分布\n\n"
        "-- 1. 按国家统计企业数\n"
        + country_sql.strip()
        + "\n\n"
        "-- 2. 按规模统计企业数和占比\n"
        + size_sql.strip()
        + "\n\n"
        "-- 3. 国家 × 行业二维分布\n"
        + country_industry_sql.strip()
        + "\n"
    )

    save_sql(query_sql, output_dir)

    country_df = run_sql(conn, country_sql)
    size_df = run_sql(conn, size_sql)
    country_industry_df = run_sql(conn, country_industry_sql)

    save_csv(country_df, output_dir, "country_distribution.csv")
    save_csv(size_df, output_dir, "size_distribution.csv")
    save_csv(country_industry_df, output_dir, "country_industry_distribution.csv")

    pivot_df = country_industry_df.pivot(
        index="country",
        columns="industry",
        values="tenant_count",
    ).fillna(0).astype(int)

    pivot_path = output_dir / "country_industry_pivot.csv"
    pivot_df.to_csv(pivot_path, encoding="utf-8-sig")
    print(f"  已保存 CSV: {pivot_path}")

    plt.figure(figsize=(10, 5))
    plt.bar(country_df["country"], country_df["tenant_count"])
    plt.title("Tenant Count by Country")
    plt.xlabel("Country")
    plt.ylabel("Tenant Count")
    save_current_plot(output_dir, "country_distribution.png")

    plt.figure(figsize=(10, 5))
    plt.bar(size_df["size_tier"], size_df["tenant_count"])
    plt.title("Tenant Count by Size Tier")
    plt.xlabel("Size Tier")
    plt.ylabel("Tenant Count")
    save_current_plot(output_dir, "size_distribution.png")

    top_country = country_df.iloc[0]["country"]
    top_country_count = int(country_df.iloc[0]["tenant_count"])
    top_size = size_df.iloc[0]["size_tier"]
    top_size_pct = float(size_df.iloc[0]["percentage"])

    insight = f"""
# Question 1 Insight：企业画像分布

## 使用表
- dim_tenant

## 使用字段
- tenant_id
- country
- industry
- size_tier

## 主要结论
- 企业数量最多的国家是 {top_country}，共有 {top_country_count} 家企业。
- 占比最高的规模段是 {top_size}，占比 {top_size_pct}%。
- 已生成国家 × 行业二维分布表，用于观察不同国家的行业结构差异。

## 口径说明
- dim_tenant 的粒度是一行一个企业。
- 因此本题可以直接使用 COUNT(*) 统计企业数量。
"""

    save_markdown(insight, output_dir)


# =========================
# 参考题 2：月度新注册用户
# =========================

def question_2(conn: duckdb.DuckDBPyConnection) -> None:
    """
    参考题 2：月度新注册用户

    目标：
    1. 统计 2025-04 到 2026-03 每月新注册用户数
    2. 统计这些用户当前状态分布
    3. 找到注册量最高和最低的月份
    """

    print("\n========== 参考题 2：月度新注册用户 ==========")

    output_dir = OUTPUT_DIR / "question_2"
    ensure_dir(output_dir)

    monthly_sql = """
SELECT
    STRFTIME(DATE_TRUNC('month', register_at), '%Y-%m') AS register_month,
    COUNT(*) AS new_users,
    SUM(CASE WHEN status = 'active' THEN 1 ELSE 0 END) AS active_users,
    SUM(CASE WHEN status = 'inactive' THEN 1 ELSE 0 END) AS inactive_users,
    SUM(CASE WHEN status = 'suspended' THEN 1 ELSE 0 END) AS suspended_users
FROM dim_user
WHERE register_at >= TIMESTAMP '2025-04-01'
  AND register_at <  TIMESTAMP '2026-04-01'
GROUP BY register_month
ORDER BY register_month;
"""

    save_sql(monthly_sql, output_dir)

    monthly_df = run_sql(conn, monthly_sql)

    monthly_df["active_rate"] = (
        monthly_df["active_users"] / monthly_df["new_users"]
    ).round(4)
    monthly_df["inactive_rate"] = (
        monthly_df["inactive_users"] / monthly_df["new_users"]
    ).round(4)
    monthly_df["suspended_rate"] = (
        monthly_df["suspended_users"] / monthly_df["new_users"]
    ).round(4)

    save_csv(monthly_df, output_dir, "monthly_new_users.csv")

    fig, ax1 = plt.subplots(figsize=(12, 5))

    ax1.bar(monthly_df["register_month"], monthly_df["new_users"])
    ax1.set_xlabel("Register Month")
    ax1.set_ylabel("New Users")
    ax1.tick_params(axis="x", rotation=45)

    ax2 = ax1.twinx()
    ax2.plot(
        monthly_df["register_month"],
        monthly_df["active_rate"],
        marker="o",
        label="active_rate",
    )
    ax2.plot(
        monthly_df["register_month"],
        monthly_df["inactive_rate"],
        marker="o",
        label="inactive_rate",
    )
    ax2.plot(
        monthly_df["register_month"],
        monthly_df["suspended_rate"],
        marker="o",
        label="suspended_rate",
    )
    ax2.set_ylabel("Current Status Rate")
    ax2.legend(loc="upper right")

    plt.title("Monthly New Users and Current Status Rate")
    save_current_plot(output_dir, "monthly_new_users.png")

    max_row = monthly_df.loc[monthly_df["new_users"].idxmax()]
    min_row = monthly_df.loc[monthly_df["new_users"].idxmin()]

    insight = f"""
# Question 2 Insight：月度新注册用户

## 使用表
- dim_user

## 使用字段
- user_id
- register_at
- status

## 主要结论
- 注册量最高的月份是 {max_row["register_month"]}，新增用户 {int(max_row["new_users"])} 人。
- 注册量最低的月份是 {min_row["register_month"]}，新增用户 {int(min_row["new_users"])} 人。
- 已计算每个月注册用户在当前时点下的 active / inactive / suspended 状态占比。

## 重要口径说明
- dim_user.status 是用户当前状态，不是注册当月状态。
- 因此，状态占比只能说明这些注册用户现在的账户状态，不能直接解释为注册当月留存率。
- 如果要分析真实留存，需要使用 fact_daily_usage、fact_session 或其他行为表重新定义活跃口径。

## 可能业务解释
- 注册高峰可能与营销活动、产品发布或季节性采购周期有关。
- 注册低谷可能与假期、预算周期或推广减少有关。
"""

    save_markdown(insight, output_dir)


# =========================
# 参考题 3：各套餐月收入对比
# =========================

def question_3(conn: duckdb.DuckDBPyConnection) -> None:
    """
    参考题 3：各套餐月收入对比

    目标：
    1. 找出 2025-10 当月有效订阅
    2. 按套餐统计活跃订阅数
    3. 按套餐汇总 MRR
    4. JOIN dim_plan 获取套餐价格，并估算 seats
    """

    print("\n========== 参考题 3：各套餐月收入对比 ==========")

    output_dir = OUTPUT_DIR / "question_3"
    ensure_dir(output_dir)

    revenue_sql = """
WITH active_subs AS (
    SELECT
        sub_id,
        tenant_id,
        plan_tier,
        start_date,
        end_date,
        mrr,
        status
    FROM fact_subscription
    WHERE status = 'active'
      AND start_date <= DATE '2025-10-31'
      AND COALESCE(end_date, DATE '9999-12-31') >= DATE '2025-10-01'
)

SELECT
    a.plan_tier,
    COUNT(*) AS active_subscription_count,
    ROUND(SUM(a.mrr), 2) AS total_mrr,
    ROUND(SUM(a.mrr / NULLIF(p.monthly_price, 0)), 2) AS estimated_seats,
    ROUND(SUM((a.mrr / NULLIF(p.monthly_price, 0)) * p.monthly_price), 2) AS nominal_revenue
FROM active_subs a
LEFT JOIN dim_plan p
    ON a.plan_tier = p.plan_tier
GROUP BY a.plan_tier
ORDER BY total_mrr DESC;
"""

    save_sql(revenue_sql, output_dir)

    result_df = run_sql(conn, revenue_sql)

    result_df["mrr_minus_nominal"] = (
        result_df["total_mrr"] - result_df["nominal_revenue"]
    ).round(2)

    save_csv(result_df, output_dir, "plan_revenue_comparison.csv")

    plt.figure(figsize=(10, 5))
    plt.bar(result_df["plan_tier"], result_df["active_subscription_count"])
    plt.title("Active Subscription Count by Plan Tier - 2025-10")
    plt.xlabel("Plan Tier")
    plt.ylabel("Active Subscription Count")
    save_current_plot(output_dir, "active_subscription_count.png")

    plt.figure(figsize=(10, 5))
    plt.bar(result_df["plan_tier"], result_df["total_mrr"])
    plt.title("Total MRR by Plan Tier - 2025-10")
    plt.xlabel("Plan Tier")
    plt.ylabel("Total MRR")
    save_current_plot(output_dir, "total_mrr.png")

    top_plan = result_df.iloc[0]["plan_tier"]
    top_mrr = float(result_df.iloc[0]["total_mrr"])

    insight = f"""
# Question 3 Insight：各套餐月收入对比

## 使用表
- fact_subscription
- dim_plan

## 使用字段
- fact_subscription.sub_id
- fact_subscription.plan_tier
- fact_subscription.start_date
- fact_subscription.end_date
- fact_subscription.status
- fact_subscription.mrr
- dim_plan.plan_tier
- dim_plan.monthly_price

## 主要结论
- 2025-10 当月 MRR 最高的套餐是 {top_plan}，总 MRR 为 {top_mrr}。
- 已按套餐统计活跃订阅数量、总 MRR、估算 seats 和 nominal revenue。

## 有效订阅口径
本题将满足以下条件的订阅视为 2025-10 当月有效订阅：
1. status = active
2. start_date <= 2025-10-31
3. end_date >= 2025-10-01；如果 end_date 为空，则视为仍然有效。

## 重要口径说明
- fact_subscription.mrr 是月度经常性收入，即确认收入口径。
- estimated_seats 是通过 mrr / monthly_price 反推得到，不是真实 seats 字段。
- nominal_revenue = estimated_seats * monthly_price，所以它通常会非常接近 total_mrr。
- 如果正式分析要求真实商业收入，应优先考虑 fact_actual_revenue.actual_revenue。
- 如果使用发票或付款表，还需要考虑折扣、优惠券、未付款、退款、多币种和账期错位等问题。
"""

    save_markdown(insight, output_dir)


# =========================
# 主函数
# =========================

def main() -> None:
    """主函数：依次执行 3 道参考题。"""

    if not DB_PATH.exists():
        raise FileNotFoundError(
            f"数据库不存在：{DB_PATH}\n请先运行：python src/00_load_data.py"
        )

    ensure_dir(OUTPUT_DIR)

    print("=" * 70)
    print("开始执行 SAMPLE_QUESTIONS.md 中的 3 道参考题")
    print(f"DuckDB 数据库路径：{DB_PATH}")
    print(f"输出目录：{OUTPUT_DIR}")
    print("=" * 70)

    conn = duckdb.connect(str(DB_PATH))

    try:
        question_1(conn)
        question_2(conn)
        question_3(conn)

    finally:
        conn.close()

    print("\n" + "=" * 70)
    print("全部参考题执行完成。")
    print(f"结果已保存到：{OUTPUT_DIR}")
    print("=" * 70)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"运行失败：{e}")
        raise
