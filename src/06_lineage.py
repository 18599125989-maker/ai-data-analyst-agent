#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import re
from typing import Any, Dict, Iterable, List


def _safe_list(value: Any) -> List[Any]:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    if isinstance(value, tuple):
        return list(value)
    return [value]


def _safe_str(value: Any) -> str:
    if value is None:
        return ""
    return str(value)


def _unique_keep_order(items: Iterable[Any]) -> List[Any]:
    result = []
    seen = set()
    for item in items:
        key = _safe_str(item)
        if key in seen or not key:
            continue
        seen.add(key)
        result.append(item)
    return result


def _extract_tables_from_sql(sql: str) -> List[str]:
    if not sql:
        return []
    patterns = re.findall(r"\b(?:from|join)\s+([a-zA-Z_][a-zA-Z0-9_]*)", sql, flags=re.IGNORECASE)
    return _unique_keep_order(patterns)


def _extract_used_columns_from_sql(sql: str, candidate_fields: List[Dict[str, Any]]) -> List[str]:
    if not sql:
        return []
    sql_lower = sql.lower()
    found = []
    bare_found = set()

    for field in candidate_fields:
        table_name = _safe_str(field.get("table_name"))
        field_name = _safe_str(field.get("field_name") or field.get("column_name") or field.get("name"))
        if not field_name:
            continue

        full_name = f"{table_name}.{field_name}" if table_name else field_name
        if table_name and f"{table_name.lower()}.{field_name.lower()}" in sql_lower:
            found.append(full_name)
            bare_found.add(field_name.lower())
            continue

        pattern = rf"\b{re.escape(field_name.lower())}\b"
        if re.search(pattern, sql_lower):
            if table_name:
                found.append(full_name)
            else:
                found.append(field_name)
            bare_found.add(field_name.lower())

    return _unique_keep_order(found)


def _extract_metrics_from_sql(sql: str) -> List[Dict[str, Any]]:
    if not sql:
        return []

    metrics: List[Dict[str, Any]] = []
    pattern = re.compile(
        r"((?:count|sum|avg|min|max|stddev_samp)\s*\([^)]*\))\s*(?:as\s+([a-zA-Z_][a-zA-Z0-9_]*))?",
        flags=re.IGNORECASE,
    )
    for match in pattern.finditer(sql):
        expression = match.group(1).strip()
        alias = _safe_str(match.group(2)).strip()
        func_name = re.match(r"([a-zA-Z_]+)\s*\(", expression)
        default_name = func_name.group(1).lower() if func_name else "metric"
        metrics.append(
            {
                "name": alias or default_name,
                "expression": expression,
                "source_hint": "derived_from_sql_or_result_column",
            }
        )

    return metrics


def _build_result_schema(df) -> List[Dict[str, str]]:
    if df is None:
        return []
    try:
        return [
            {"column": _safe_str(col), "dtype": _safe_str(dtype)}
            for col, dtype in zip(df.columns, df.dtypes)
        ]
    except Exception:
        return []


def _build_sql_path(executed_sql: str) -> Dict[str, Any]:
    sql_lower = _safe_str(executed_sql).lower()
    stripped = sql_lower.lstrip()
    if stripped.startswith("with"):
        sql_type = "WITH"
    elif stripped.startswith("select"):
        sql_type = "SELECT"
    else:
        sql_type = "UNKNOWN"

    return {
        "executed_sql": executed_sql or "",
        "sql_type": sql_type,
        "has_join": bool(re.search(r"\bjoin\b", sql_lower, flags=re.IGNORECASE)),
        "has_group_by": bool(re.search(r"\bgroup\s+by\b", sql_lower, flags=re.IGNORECASE)),
        "has_order_by": bool(re.search(r"\border\s+by\b", sql_lower, flags=re.IGNORECASE)),
        "has_limit": bool(re.search(r"\blimit\b", sql_lower, flags=re.IGNORECASE)),
    }


def _build_lineage_summary(
    used_tables: List[str],
    sql_path: Dict[str, Any],
    result_schema: List[Dict[str, str]],
    visualization: Dict[str, Any],
) -> List[str]:
    summary = []
    if used_tables:
        summary.append(f"本次分析使用 {', '.join(used_tables)} 表。")
    if sql_path.get("has_join"):
        summary.append("SQL 包含 JOIN。")
    else:
        summary.append("SQL 未使用 JOIN。")
    if sql_path.get("has_group_by"):
        summary.append("SQL 包含 GROUP BY 聚合。")
    if result_schema:
        cols = ", ".join(item.get("column", "") for item in result_schema if item.get("column"))
        if cols:
            summary.append(f"结果列包括 {cols}。")
    if visualization:
        x = _safe_str(visualization.get("x"))
        y = _safe_str(visualization.get("y"))
        chart_type = _safe_str(visualization.get("chart_type"))
        if chart_type and (x or y):
            summary.append(f"图表映射为 {chart_type}，x={x or '空'}，y={y or '空'}。")
    return summary[:6]


def build_lineage(
    question: str,
    result_json: dict,
    retrieval_context: dict,
    executed_sql: str,
    df,
    visualization_spec: dict | None = None,
) -> dict:
    try:
        result_json = result_json or {}
        retrieval_context = retrieval_context or {}
        visualization_spec = visualization_spec or {}

        used_tables = _safe_list(result_json.get("used_tables"))
        if not used_tables:
            used_tables = _safe_list(retrieval_context.get("selected_table_names"))
        if not used_tables:
            used_tables = _extract_tables_from_sql(executed_sql)
        used_tables = _unique_keep_order([_safe_str(x) for x in used_tables if _safe_str(x)])

        used_columns = _safe_list(result_json.get("used_columns"))
        if not used_columns:
            used_columns = _extract_used_columns_from_sql(
                executed_sql,
                _safe_list(retrieval_context.get("candidate_fields")),
            )
        used_columns = _unique_keep_order([_safe_str(x) for x in used_columns if _safe_str(x)])

        candidate_tables = _unique_keep_order(
            [_safe_str(x) for x in _safe_list(retrieval_context.get("selected_table_names")) if _safe_str(x)]
        )

        candidate_joins = []
        for item in _safe_list(retrieval_context.get("candidate_joins"))[:10]:
            if not isinstance(item, dict):
                continue
            candidate_joins.append(
                {
                    "join_condition": _safe_str(item.get("join_condition")),
                    "source_table": _safe_str(item.get("source_table")),
                    "source_field": _safe_str(item.get("source_field")),
                    "target_table": _safe_str(item.get("target_table")),
                    "target_field": _safe_str(item.get("target_field")),
                    "relationship": _safe_str(item.get("relationship")),
                    "risk_level": _safe_str(item.get("risk_level")),
                    "note": _safe_str(item.get("note")),
                }
            )

        metrics = _extract_metrics_from_sql(executed_sql)
        sql_path = _build_sql_path(executed_sql)
        result_schema = _build_result_schema(df)

        visualization = {
            "chart_type": _safe_str(visualization_spec.get("chart_type")),
            "x": _safe_str(visualization_spec.get("x")),
            "y": _safe_str(visualization_spec.get("y")),
            "series": _safe_str(visualization_spec.get("series")),
            "title": _safe_str(visualization_spec.get("title")),
        }

        lineage_summary = _build_lineage_summary(
            used_tables=used_tables,
            sql_path=sql_path,
            result_schema=result_schema,
            visualization=visualization,
        )

        return {
            "question": question or "",
            "used_tables": used_tables,
            "used_columns": used_columns,
            "candidate_tables": candidate_tables,
            "candidate_joins": candidate_joins,
            "metrics": metrics,
            "sql_path": sql_path,
            "result_schema": result_schema,
            "visualization": visualization,
            "lineage_summary": lineage_summary,
        }
    except Exception:
        return {}
