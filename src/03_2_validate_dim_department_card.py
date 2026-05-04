#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
功能：
验证 knowledge_manual/table_cards/dim_department.yaml 中的核心假设。

验证内容：
1. 表行数与字段数
2. dept_id 是否唯一，是否能作为主键
3. tenant_id 是否都能关联 dim_tenant
4. parent_dept_id 空值比例
5. parent_dept_id 非空值是否都能自关联到 dim_department.dept_id
6. dept_name 枚举分布
7. dim_user.dept_id 是否能关联 dim_department.dept_id
8. table card 中常用 SQL patterns 是否可执行

输出：
outputs/knowledge/validation/
├── dim_department_validation.json
└── dim_department_validation.md
"""

import json
from pathlib import Path

import duckdb


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DB_PATH = PROJECT_ROOT / "cloudwork.duckdb"

OUTPUT_DIR = PROJECT_ROOT / "outputs" / "knowledge" / "validation"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

JSON_OUTPUT = OUTPUT_DIR / "dim_department_validation.json"
MD_OUTPUT = OUTPUT_DIR / "dim_department_validation.md"


def run_query(conn, name, sql):
    """执行 SQL，并返回结构化结果"""
    print(f"正在执行：{name}")

    try:
        df = conn.execute(sql).df()
        return {
            "check_name": name,
            "status": "success",
            "sql": sql.strip(),
            "rows": df.to_dict(orient="records"),
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
        FROM dim_department;
        """
    ))

    # 2. 字段数检查
    checks.append(run_query(
        conn,
        "column_count",
        """
        SELECT
            COUNT(*) AS column_count
        FROM information_schema.columns
        WHERE table_name = 'dim_department';
        """
    ))

    # 3. dept_id 主键唯一性
    checks.append(run_query(
        conn,
        "dept_id_uniqueness",
        """
        SELECT
            COUNT(*) AS row_count,
            COUNT(DISTINCT dept_id) AS distinct_dept_id_count,
            COUNT(*) - COUNT(DISTINCT dept_id) AS duplicate_dept_id_count
        FROM dim_department;
        """
    ))

    # 4. 找出重复 dept_id，如果有
    checks.append(run_query(
        conn,
        "duplicated_dept_id_examples",
        """
        SELECT
            dept_id,
            COUNT(*) AS duplicate_count
        FROM dim_department
        GROUP BY dept_id
        HAVING COUNT(*) > 1
        ORDER BY duplicate_count DESC
        LIMIT 20;
        """
    ))

    # 5. tenant_id 外键覆盖率
    checks.append(run_query(
        conn,
        "tenant_id_fk_coverage",
        """
        SELECT
            COUNT(*) AS unmatched_department_count
        FROM dim_department d
        LEFT JOIN dim_tenant t
            ON d.tenant_id = t.tenant_id
        WHERE t.tenant_id IS NULL;
        """
    ))

    # 6. tenant_id 分布
    checks.append(run_query(
        conn,
        "department_count_by_tenant_summary",
        """
        WITH dept_count AS (
            SELECT
                tenant_id,
                COUNT(DISTINCT dept_id) AS department_count
            FROM dim_department
            GROUP BY tenant_id
        )
        SELECT
            COUNT(*) AS tenant_count_with_departments,
            MIN(department_count) AS min_department_count,
            MAX(department_count) AS max_department_count,
            ROUND(AVG(department_count), 2) AS avg_department_count
        FROM dept_count;
        """
    ))

    # 7. parent_dept_id 空值比例
    checks.append(run_query(
        conn,
        "parent_dept_id_null_rate",
        """
        SELECT
            COUNT(*) AS row_count,
            SUM(
                CASE
                    WHEN parent_dept_id IS NULL OR CAST(parent_dept_id AS VARCHAR) = ''
                    THEN 1 ELSE 0
                END
            ) AS null_parent_dept_count,
            ROUND(
                SUM(
                    CASE
                        WHEN parent_dept_id IS NULL OR CAST(parent_dept_id AS VARCHAR) = ''
                        THEN 1 ELSE 0
                    END
                ) * 1.0 / COUNT(*),
                4
            ) AS null_parent_dept_rate
        FROM dim_department;
        """
    ))

    # 8. parent_dept_id 自关联覆盖率
    checks.append(run_query(
        conn,
        "orphan_parent_dept_id",
        """
        SELECT
            COUNT(*) AS orphan_parent_dept_count
        FROM dim_department child
        LEFT JOIN dim_department parent
            ON child.parent_dept_id = parent.dept_id
        WHERE child.parent_dept_id IS NOT NULL
          AND CAST(child.parent_dept_id AS VARCHAR) != ''
          AND parent.dept_id IS NULL;
        """
    ))

    # 9. orphan parent examples
    checks.append(run_query(
        conn,
        "orphan_parent_dept_id_examples",
        """
        SELECT
            child.dept_id,
            child.tenant_id,
            child.dept_name,
            child.parent_dept_id
        FROM dim_department child
        LEFT JOIN dim_department parent
            ON child.parent_dept_id = parent.dept_id
        WHERE child.parent_dept_id IS NOT NULL
          AND CAST(child.parent_dept_id AS VARCHAR) != ''
          AND parent.dept_id IS NULL
        LIMIT 20;
        """
    ))

    # 10. dept_name 枚举分布
    checks.append(run_query(
        conn,
        "dept_name_distribution",
        """
        SELECT
            dept_name,
            COUNT(DISTINCT dept_id) AS department_count
        FROM dim_department
        GROUP BY dept_name
        ORDER BY department_count DESC;
        """
    ))

    # 11. dept_name 是否存在 NULL / 空字符串
    checks.append(run_query(
        conn,
        "dept_name_null_check",
        """
        SELECT
            COUNT(*) AS bad_dept_name_count
        FROM dim_department
        WHERE dept_name IS NULL
           OR CAST(dept_name AS VARCHAR) = '';
        """
    ))

    # 12. dim_user.dept_id 关联情况
    checks.append(run_query(
        conn,
        "dim_user_dept_id_join_coverage",
        """
        SELECT
            COUNT(*) AS user_count,
            SUM(
                CASE
                    WHEN u.dept_id IS NULL OR CAST(u.dept_id AS VARCHAR) = ''
                    THEN 1 ELSE 0
                END
            ) AS user_without_dept_count,
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
        """
    ))

    # 13. SQL pattern: department_count_by_tenant
    checks.append(run_query(
        conn,
        "sql_pattern_department_count_by_tenant",
        """
        SELECT
            tenant_id,
            COUNT(DISTINCT dept_id) AS department_count
        FROM dim_department
        GROUP BY tenant_id
        ORDER BY department_count DESC
        LIMIT 20;
        """
    ))

    # 14. SQL pattern: top_level_vs_child_departments
    checks.append(run_query(
        conn,
        "sql_pattern_top_level_vs_child_departments",
        """
        SELECT
            CASE
                WHEN parent_dept_id IS NULL OR CAST(parent_dept_id AS VARCHAR) = '' THEN 'top_level'
                ELSE 'child_department'
            END AS department_level_type,
            COUNT(DISTINCT dept_id) AS department_count
        FROM dim_department
        GROUP BY department_level_type
        ORDER BY department_count DESC;
        """
    ))

    # 15. SQL pattern: user_count_by_department_name
    checks.append(run_query(
        conn,
        "sql_pattern_user_count_by_department_name",
        """
        SELECT
            COALESCE(d.dept_name, 'unknown_or_unassigned') AS dept_name,
            COUNT(DISTINCT u.user_id) AS user_count
        FROM dim_user u
        LEFT JOIN dim_department d
            ON u.dept_id = d.dept_id
        GROUP BY COALESCE(d.dept_name, 'unknown_or_unassigned')
        ORDER BY user_count DESC;
        """
    ))

    conn.close()

    # 自动判断结果
    summary = build_summary(checks)

    output = {
        "table_name": "dim_department",
        "validation_target": "knowledge_manual/table_cards/dim_department.yaml",
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
    print("你可以把 Markdown 结果发给我，我会帮你修改 dim_department.yaml。")


def get_first_row(checks, check_name):
    for item in checks:
        if item["check_name"] == check_name:
            if item["rows"]:
                return item["rows"][0]
    return {}


def build_summary(checks):
    """根据检查结果生成自动摘要"""

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
    pk = get_first_row(checks, "dept_id_uniqueness")
    tenant_fk = get_first_row(checks, "tenant_id_fk_coverage")
    parent_orphan = get_first_row(checks, "orphan_parent_dept_id")
    user_join = get_first_row(checks, "dim_user_dept_id_join_coverage")
    dept_name_bad = get_first_row(checks, "dept_name_null_check")
    parent_null = get_first_row(checks, "parent_dept_id_null_rate")

    if basic and col_count:
        summary["profile_filled"] = True
        summary["key_findings"].append(
            f"dim_department 行数为 {basic.get('row_count')}，字段数为 {col_count.get('column_count')}。"
        )

    if pk:
        row_count = pk.get("row_count")
        distinct_count = pk.get("distinct_dept_id_count")
        duplicate_count = pk.get("duplicate_dept_id_count")

        if row_count == distinct_count and duplicate_count == 0:
            summary["grain_validated"] = True
            summary["key_findings"].append("dept_id 唯一，可以验证“一行 = 一个部门”的 grain。")
        else:
            summary["warnings"].append(
                f"dept_id 不唯一：row_count={row_count}, distinct={distinct_count}, duplicate={duplicate_count}。"
            )

    dq_pass = True

    if tenant_fk:
        unmatched = tenant_fk.get("unmatched_department_count")
        if unmatched == 0:
            summary["key_findings"].append("dim_department.tenant_id 可以全部关联到 dim_tenant.tenant_id。")
        else:
            dq_pass = False
            summary["warnings"].append(f"存在 {unmatched} 条部门记录无法关联到 dim_tenant。")

    if parent_orphan:
        orphan = parent_orphan.get("orphan_parent_dept_count")
        if orphan == 0:
            summary["key_findings"].append("非空 parent_dept_id 均可在 dim_department.dept_id 中找到对应上级部门。")
        else:
            dq_pass = False
            summary["warnings"].append(f"存在 {orphan} 条 parent_dept_id 找不到对应上级部门。")

    if dept_name_bad:
        bad_count = dept_name_bad.get("bad_dept_name_count")
        if bad_count == 0:
            summary["key_findings"].append("dept_name 没有 NULL 或空字符串。")
        else:
            dq_pass = False
            summary["warnings"].append(f"dept_name 存在 {bad_count} 条 NULL 或空字符串。")

    if user_join:
        user_without_dept = user_join.get("user_without_dept_count")
        unmatched_user_dept = user_join.get("unmatched_user_dept_count")

        summary["key_findings"].append(
            f"dim_user 中 dept_id 为空的用户数为 {user_without_dept}。"
        )

        if unmatched_user_dept == 0:
            summary["key_findings"].append("dim_user.dept_id 的非空值均可关联到 dim_department.dept_id。")
        else:
            dq_pass = False
            summary["warnings"].append(
                f"dim_user 中存在 {unmatched_user_dept} 条 dept_id 无法关联到 dim_department。"
            )

    if parent_null:
        summary["key_findings"].append(
            f"parent_dept_id 空值率为 {parent_null.get('null_parent_dept_rate')}，通常可解释为顶级部门比例。"
        )

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
    lines.append("# dim_department Table Card Validation Report")
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

        # 简单 markdown table
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


if __name__ == "__main__":
    main()
