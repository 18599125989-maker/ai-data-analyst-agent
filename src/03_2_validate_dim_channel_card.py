#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
功能：
验证 knowledge_manual/table_cards_draft/dim_channel.yaml 中的核心假设。

输出：
outputs/knowledge/validation/
├── dim_channel_validation.json
└── dim_channel_validation.md
"""

import json
from pathlib import Path

import duckdb


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DB_PATH = PROJECT_ROOT / "cloudwork.duckdb"

OUTPUT_DIR = PROJECT_ROOT / "outputs" / "knowledge" / "validation"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

JSON_OUTPUT = OUTPUT_DIR / "dim_channel_validation.json"
MD_OUTPUT = OUTPUT_DIR / "dim_channel_validation.md"

TABLE_NAME = 'dim_channel'
PRIMARY_KEY = ['channel_id']
NATURAL_KEY = []
FOREIGN_KEYS = [{'column': 'tenant_id', 'ref_table': 'dim_tenant', 'ref_col': 'tenant_id'}]
ENUM_COLUMNS = ['channel_type']
TIME_COLUMNS = ['created_at']
NUMERIC_COLUMNS = ['member_count']
JSON_COLUMNS = []
SQL_PATTERNS = {'time_distribution': 'SELECT\n    created_at AS time_key,\n    COUNT(*) AS row_count\nFROM dim_channel\nGROUP BY created_at\nORDER BY created_at;', 'enum_distribution': 'SELECT\n    channel_type,\n    COUNT(DISTINCT channel_id) AS entity_count\nFROM dim_channel\nGROUP BY channel_type\nORDER BY entity_count DESC, channel_type;', 'metric_trend': 'SELECT\n    created_at AS time_key,\n    ROUND(SUM(member_count), 2) AS total_member_count\nFROM dim_channel\nGROUP BY created_at\nORDER BY created_at;', 'join_coverage_sample': 'SELECT\n    COUNT(*) AS row_count\nFROM dim_channel base\nLEFT JOIN dim_tenant ref\n    ON base.tenant_id = ref.tenant_id;'}


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


def build_key_expr(columns):
    if len(columns) == 1:
        return columns[0]
    return " || '|' || ".join([
        f"COALESCE(CAST({col} AS VARCHAR), '__NULL__')" for col in columns
    ])


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
    pk = get_first_row(checks, "primary_key_uniqueness")

    if basic and col:
        summary["profile_filled"] = True
        summary["key_findings"].append(
            f"{TABLE_NAME} 行数为 {basic.get('row_count')}，字段数为 {col.get('column_count')}。"
        )

    if pk:
        dup = pk.get("duplicate_primary_key_count")
        if dup == 0:
            summary["grain_validated"] = True
            summary["key_findings"].append("主键候选当前唯一。")
        else:
            summary["grain_validated"] = False
            summary["warnings"].append(f"主键候选存在 {dup} 条重复。")

    if NATURAL_KEY:
        nk = get_first_row(checks, "candidate_natural_key_uniqueness")
        if nk:
            dup = nk.get("duplicate_natural_key_count")
            if dup == 0 and summary["grain_validated"] is True:
                summary["key_findings"].append("候选自然键当前唯一。")
            elif dup and dup > 0:
                if summary["grain_validated"] is True:
                    summary["grain_validated"] = "partial"
                summary["warnings"].append(f"候选自然键存在 {dup} 条重复。")

    dq_level = "true"
    for fk in FOREIGN_KEYS:
        check_name = f"{fk['column']}_fk_coverage"
        row = get_first_row(checks, check_name)
        if not row:
            continue
        unmatched = row.get("unmatched_count")
        if unmatched == 0:
            summary["key_findings"].append(f"{fk['column']} 当前全部可关联到 {fk['ref_table']}.{fk['ref_col']}。")
        else:
            summary["warnings"].append(f"{fk['column']} 存在 {unmatched} 条无法关联的记录。")
            dq_level = "partial"

    for col_name in JSON_COLUMNS:
        row = get_first_row(checks, f"{col_name}_json_parse_check")
        if not row:
            continue
        invalid = row.get("invalid_json_count")
        if invalid and invalid > 0:
            summary["warnings"].append(f"{col_name} 存在 {invalid} 条无法解析为 JSON 的记录。")
            dq_level = "partial"

    for col_name in NUMERIC_COLUMNS:
        row = get_first_row(checks, f"{col_name}_numeric_range_check")
        if not row:
            continue
        negative = row.get("negative_count")
        if negative and negative > 0:
            summary["warnings"].append(f"{col_name} 存在 {negative} 条负值记录。")
            dq_level = "partial"

    failed_checks = [item["check_name"] for item in checks if item["status"] != "success"]
    if failed_checks:
        summary["warnings"].append(f"存在执行失败的 checks：{failed_checks}")
        dq_level = "partial"

    failed_patterns = [
        item["check_name"]
        for item in checks
        if item["check_name"].startswith("sql_pattern_") and item["status"] != "success"
    ]
    if failed_patterns:
        summary["warnings"].append(f"以下 SQL patterns 执行失败：{failed_patterns}")
        dq_level = "partial"
    else:
        summary["key_findings"].append("主要 SQL patterns 已执行。")

    summary["dq_validated"] = True if dq_level == "true" else "partial"
    summary["ready_for_agent_recommendation"] = bool(summary["profile_filled"] and summary["grain_validated"] in [True, "partial"] and summary["dq_validated"] in [True, "partial"])
    return summary


def write_markdown_report(output):
    summary = output["summary"]
    checks = output["checks"]
    lines = []
    lines.append(f"# {TABLE_NAME} Table Card Validation Report")
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
            values = [str(row.get(h, "")).replace("|", "/") for h in headers]
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

    checks.append(run_query(conn, "basic_table_profile", f"""
        SELECT COUNT(*) AS row_count
        FROM {TABLE_NAME};
    """))

    checks.append(run_query(conn, "column_count", f"""
        SELECT COUNT(*) AS column_count
        FROM information_schema.columns
        WHERE table_name = '{TABLE_NAME}';
    """))

    pk_expr = build_key_expr(PRIMARY_KEY)
    pk_group = ", ".join(PRIMARY_KEY)
    checks.append(run_query(conn, "primary_key_uniqueness", f"""
        SELECT
            COUNT(*) AS row_count,
            COUNT(DISTINCT {pk_expr}) AS distinct_primary_key_count,
            COUNT(*) - COUNT(DISTINCT {pk_expr}) AS duplicate_primary_key_count
        FROM {TABLE_NAME};
    """))

    checks.append(run_query(conn, "duplicated_primary_key_examples", f"""
        SELECT
            {pk_group},
            COUNT(*) AS duplicate_count
        FROM {TABLE_NAME}
        GROUP BY {pk_group}
        HAVING COUNT(*) > 1
        ORDER BY duplicate_count DESC
        LIMIT 20;
    """))

    if NATURAL_KEY:
        nk_expr = build_key_expr(NATURAL_KEY)
        nk_group = ", ".join(NATURAL_KEY)
        checks.append(run_query(conn, "candidate_natural_key_uniqueness", f"""
            SELECT
                COUNT(*) AS row_count,
                COUNT(DISTINCT {nk_expr}) AS distinct_natural_key_count,
                COUNT(*) - COUNT(DISTINCT {nk_expr}) AS duplicate_natural_key_count
            FROM {TABLE_NAME};
        """))
        checks.append(run_query(conn, "duplicated_natural_key_examples", f"""
            SELECT
                {nk_group},
                COUNT(*) AS duplicate_count
            FROM {TABLE_NAME}
            GROUP BY {nk_group}
            HAVING COUNT(*) > 1
            ORDER BY duplicate_count DESC
            LIMIT 20;
        """))

    for fk in FOREIGN_KEYS:
        checks.append(run_query(conn, f"{fk['column']}_fk_coverage", f"""
            SELECT
                COUNT(*) AS row_count,
                SUM(
                    CASE
                        WHEN base.{fk['column']} IS NOT NULL
                         AND CAST(base.{fk['column']} AS VARCHAR) != ''
                         AND ref.{fk['ref_col']} IS NULL
                        THEN 1 ELSE 0
                    END
                ) AS unmatched_count
            FROM {TABLE_NAME} base
            LEFT JOIN {fk['ref_table']} ref
                ON base.{fk['column']} = ref.{fk['ref_col']};
        """))
        checks.append(run_query(conn, f"{fk['column']}_fk_unmatched_examples", f"""
            SELECT
                base.{fk['column']}
            FROM {TABLE_NAME} base
            LEFT JOIN {fk['ref_table']} ref
                ON base.{fk['column']} = ref.{fk['ref_col']}
            WHERE base.{fk['column']} IS NOT NULL
              AND CAST(base.{fk['column']} AS VARCHAR) != ''
              AND ref.{fk['ref_col']} IS NULL
            LIMIT 20;
        """))

    for col in ENUM_COLUMNS:
        checks.append(run_query(conn, f"{col}_distribution", f"""
            SELECT
                {col},
                COUNT(*) AS row_count
            FROM {TABLE_NAME}
            GROUP BY {col}
            ORDER BY row_count DESC, {col}
            LIMIT 50;
        """))
        checks.append(run_query(conn, f"{col}_null_check", f"""
            SELECT
                COUNT(*) AS blank_or_null_count
            FROM {TABLE_NAME}
            WHERE {col} IS NULL
               OR CAST({col} AS VARCHAR) = '';
        """))

    for col in TIME_COLUMNS:
        checks.append(run_query(conn, f"{col}_time_range_check", f"""
            SELECT
                COUNT(*) AS row_count,
                SUM(CASE WHEN {col} IS NULL THEN 1 ELSE 0 END) AS null_count,
                MIN({col}) AS min_value,
                MAX({col}) AS max_value
            FROM {TABLE_NAME};
        """))

    for col in NUMERIC_COLUMNS:
        checks.append(run_query(conn, f"{col}_numeric_range_check", f"""
            SELECT
                MIN({col}) AS min_value,
                MAX({col}) AS max_value,
                SUM(CASE WHEN {col} < 0 THEN 1 ELSE 0 END) AS negative_count,
                SUM(CASE WHEN {col} = 0 THEN 1 ELSE 0 END) AS zero_count
            FROM {TABLE_NAME};
        """))

    for col in JSON_COLUMNS:
        checks.append(run_query(conn, f"{col}_json_null_check", f"""
            SELECT
                COUNT(*) AS row_count,
                SUM(CASE WHEN {col} IS NULL OR CAST({col} AS VARCHAR) = '' THEN 1 ELSE 0 END) AS blank_count
            FROM {TABLE_NAME};
        """))
        checks.append(run_query(conn, f"{col}_json_parse_check", f"""
            SELECT
                COUNT(*) AS row_count,
                SUM(CASE WHEN TRY_CAST({col} AS JSON) IS NULL THEN 1 ELSE 0 END) AS invalid_json_count
            FROM {TABLE_NAME};
        """))

    for name, sql in SQL_PATTERNS.items():
        checks.append(run_query(conn, f"sql_pattern_{name}", sql))

    conn.close()

    output = {
        "table_name": TABLE_NAME,
        "validation_target": f"knowledge_manual/table_cards_draft/{TABLE_NAME}.yaml",
        "summary": build_summary(checks),
        "checks": checks,
    }

    JSON_OUTPUT.write_text(json.dumps(output, ensure_ascii=False, indent=2), encoding="utf-8")
    write_markdown_report(output)
    print(f"已输出：{JSON_OUTPUT}")
    print(f"已输出：{MD_OUTPUT}")


if __name__ == "__main__":
    main()
