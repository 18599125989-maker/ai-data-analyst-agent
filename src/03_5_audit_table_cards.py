#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
批量审计 knowledge_manual/table_cards/ 下所有 YAML table card 的质量。

输出：
- outputs/knowledge/table_card_audit.json
- outputs/knowledge/table_card_audit.md
"""

import json
from pathlib import Path
from typing import Any, Dict, List

import yaml


PROJECT_ROOT = Path(__file__).resolve().parents[1]
CARD_DIR = PROJECT_ROOT / "knowledge_manual" / "table_cards"
OUTPUT_DIR = PROJECT_ROOT / "outputs" / "knowledge"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

JSON_OUTPUT = OUTPUT_DIR / "table_card_audit.json"
MD_OUTPUT = OUTPUT_DIR / "table_card_audit.md"

REQUIRED_FIELDS = [
    "table_name",
    "business_meaning",
    "grain",
    "columns",
    "field_groups",
    "preferred_metrics",
    "known_traps",
    "status",
    "validation_summary",
    "agent_usage_policy",
]


def safe_bool(value: Any) -> Any:
    if isinstance(value, bool):
        return value
    return value


def size_label(size_kb: float) -> str:
    if size_kb > 25:
        return "too_long"
    if size_kb >= 15:
        return "review_size"
    return "ok"


def count_label(count: int, ok_min: int, ok_max: int) -> str:
    if count == 0:
        return "missing"
    if count < ok_min:
        return "maybe_too_few"
    if count <= ok_max:
        return "ok"
    return "maybe_too_many"


def as_list(value: Any) -> List[Any]:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    return [value]


def load_yaml(path: Path) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f)
    if not isinstance(data, dict):
        raise ValueError("YAML 顶层结构不是 dict")
    return data


def inspect_card(path: Path) -> Dict[str, Any]:
    result: Dict[str, Any] = {
        "file": str(path.relative_to(PROJECT_ROOT)),
        "parse_success": False,
        "parse_error": None,
        "table_name": path.stem,
        "size_kb": round(path.stat().st_size / 1024.0, 2),
        "size_status": None,
        "missing_required_fields": [],
        "status": {},
        "issues": [],
        "key_check": {},
        "known_traps_count": 0,
        "known_traps_status": "missing",
        "sql_patterns_count": 0,
        "sql_patterns_status": "missing",
    }
    result["size_status"] = size_label(result["size_kb"])

    try:
        data = load_yaml(path)
    except Exception as e:
        result["parse_error"] = str(e)
        result["issues"].append(f"yaml_parse_failed: {e}")
        return result

    result["parse_success"] = True
    result["table_name"] = data.get("table_name", path.stem)

    for field in REQUIRED_FIELDS:
        if field not in data:
            result["missing_required_fields"].append(field)
    if result["missing_required_fields"]:
        result["issues"].append(
            "missing_required_fields: " + ", ".join(result["missing_required_fields"])
        )

    status = data.get("status") or {}
    result["status"] = {
        "authoring_status": status.get("authoring_status"),
        "profile_filled": safe_bool(status.get("profile_filled")),
        "grain_validated": status.get("grain_validated"),
        "dq_validated": status.get("dq_validated"),
        "ready_for_agent": safe_bool(status.get("ready_for_agent")),
    }

    authoring_status = status.get("authoring_status")
    ready_for_agent = status.get("ready_for_agent")
    dq_validated = status.get("dq_validated")

    if ready_for_agent is False:
        result["issues"].append("ready_for_agent_false")
    if authoring_status == "manual_draft":
        result["issues"].append("authoring_status_manual_draft")

    known_traps = as_list(data.get("known_traps"))
    result["known_traps_count"] = len(known_traps)
    result["known_traps_status"] = count_label(len(known_traps), 3, 7)

    if authoring_status == "validated_with_warnings" and len(known_traps) == 0:
        result["issues"].append("validated_with_warnings_but_missing_known_traps")

    if dq_validated is False and ready_for_agent is True:
        result["issues"].append("dq_validated_false_but_ready_for_agent_true")

    agent_usage_policy = data.get("agent_usage_policy")
    if ready_for_agent is True and not isinstance(agent_usage_policy, dict):
        result["issues"].append("ready_for_agent_true_but_missing_agent_usage_policy")

    primary_key = as_list(data.get("primary_key"))
    natural_key_candidate = as_list(data.get("natural_key_candidate"))
    natural_key_status = data.get("natural_key_status")

    key_issues: List[str] = []
    if len(primary_key) == 0:
        key_issues.append("primary_key_empty")
    if natural_key_candidate:
        key_issues.append("natural_key_candidate_present")
    if isinstance(natural_key_status, dict):
        key_issues.append("natural_key_status_present")
    if len(primary_key) > 1:
        key_issues.append(
            "primary_key_has_multiple_fields_maybe_composite_business_key"
        )
    if (
        isinstance(natural_key_status, dict)
        and natural_key_status.get("validation_status") == "failed"
        and ready_for_agent is True
    ):
        if len(known_traps) == 0:
            key_issues.append(
                "natural_key_failed_ready_for_agent_without_duplicate_handling_traps"
            )
        else:
            key_issues.append(
                "natural_key_failed_ready_for_agent_check_duplicate_handling_rules"
            )

    result["key_check"] = {
        "primary_key": primary_key,
        "primary_key_empty": len(primary_key) == 0,
        "natural_key_candidate": natural_key_candidate,
        "natural_key_status_exists": isinstance(natural_key_status, dict),
        "issues": key_issues,
    }

    sql_patterns = data.get("sql_patterns")
    sql_patterns_count = len(sql_patterns) if isinstance(sql_patterns, dict) else 0
    result["sql_patterns_count"] = sql_patterns_count
    result["sql_patterns_status"] = count_label(sql_patterns_count, 4, 8)

    if result["size_status"] != "ok":
        result["issues"].append(f"size_{result['size_status']}")
    if result["known_traps_status"] != "ok":
        result["issues"].append(f"known_traps_{result['known_traps_status']}")
    if result["sql_patterns_status"] != "ok":
        result["issues"].append(f"sql_patterns_{result['sql_patterns_status']}")

    return result


def issue_summary(card_result: Dict[str, Any]) -> str:
    issues = card_result.get("issues", [])
    if not issues:
        return "ok"
    return "; ".join(issues)


def build_summary(results: List[Dict[str, Any]]) -> Dict[str, Any]:
    parse_success_count = sum(1 for x in results if x["parse_success"])
    parse_failed_count = len(results) - parse_success_count
    ready_for_agent_count = sum(
        1
        for x in results
        if isinstance(x.get("status"), dict) and x["status"].get("ready_for_agent") is True
    )
    manual_draft_count = sum(
        1
        for x in results
        if isinstance(x.get("status"), dict)
        and x["status"].get("authoring_status") == "manual_draft"
    )
    too_long_count = sum(1 for x in results if x["size_status"] == "too_long")
    missing_policy_count = sum(
        1
        for x in results
        if "ready_for_agent_true_but_missing_agent_usage_policy" in x.get("issues", [])
    )
    return {
        "total_cards": len(results),
        "parse_success_count": parse_success_count,
        "parse_failed_count": parse_failed_count,
        "ready_for_agent_count": ready_for_agent_count,
        "manual_draft_count": manual_draft_count,
        "too_long_count": too_long_count,
        "missing_policy_count": missing_policy_count,
    }


def build_attention_list(results: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    attention = []
    for item in results:
        if item["issues"]:
            attention.append(
                {
                    "table_name": item["table_name"],
                    "file": item["file"],
                    "status": (item.get("status") or {}).get("authoring_status"),
                    "ready_for_agent": (item.get("status") or {}).get("ready_for_agent"),
                    "size_kb": item["size_kb"],
                    "issue_summary": issue_summary(item),
                }
            )
    return attention


def to_markdown_table(rows: List[Dict[str, Any]], columns: List[str]) -> str:
    if not rows:
        return "_None_\n"
    header = "| " + " | ".join(columns) + " |"
    sep = "| " + " | ".join(["---"] * len(columns)) + " |"
    lines = [header, sep]
    for row in rows:
        values = []
        for col in columns:
            value = row.get(col, "")
            values.append(str(value).replace("\n", " "))
        lines.append("| " + " | ".join(values) + " |")
    return "\n".join(lines) + "\n"


def write_markdown_report(
    results: List[Dict[str, Any]],
    summary: Dict[str, Any],
    attention: List[Dict[str, Any]],
) -> None:
    all_rows = []
    for item in results:
        status = item.get("status") or {}
        all_rows.append(
            {
                "table_name": item["table_name"],
                "file": item["file"],
                "authoring_status": status.get("authoring_status"),
                "grain_validated": status.get("grain_validated"),
                "dq_validated": status.get("dq_validated"),
                "ready_for_agent": status.get("ready_for_agent"),
                "size_kb": item["size_kb"],
                "known_traps_count": item["known_traps_count"],
                "sql_patterns_count": item["sql_patterns_count"],
            }
        )

    lines = [
        "# Table Card Audit Report",
        "",
        "## Summary",
        f"- total_cards: {summary['total_cards']}",
        f"- parse_success_count: {summary['parse_success_count']}",
        f"- parse_failed_count: {summary['parse_failed_count']}",
        f"- ready_for_agent_count: {summary['ready_for_agent_count']}",
        f"- manual_draft_count: {summary['manual_draft_count']}",
        f"- too_long_count: {summary['too_long_count']}",
        f"- missing_policy_count: {summary['missing_policy_count']}",
        "",
        "## Cards Needing Attention",
        to_markdown_table(
            attention,
            [
                "table_name",
                "file",
                "status",
                "ready_for_agent",
                "size_kb",
                "issue_summary",
            ],
        ),
        "## All Cards",
        to_markdown_table(
            all_rows,
            [
                "table_name",
                "file",
                "authoring_status",
                "grain_validated",
                "dq_validated",
                "ready_for_agent",
                "size_kb",
                "known_traps_count",
                "sql_patterns_count",
            ],
        ),
    ]
    MD_OUTPUT.write_text("\n".join(lines), encoding="utf-8")


def main() -> None:
    card_paths = sorted(CARD_DIR.glob("*.yaml"))
    results = [inspect_card(path) for path in card_paths]
    summary = build_summary(results)
    attention = build_attention_list(results)

    payload = {
        "summary": summary,
        "attention_cards": attention,
        "cards": results,
    }
    JSON_OUTPUT.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    write_markdown_report(results, summary, attention)

    print(f"Audit completed: {len(results)} cards")
    print(f"JSON: {JSON_OUTPUT}")
    print(f"MD: {MD_OUTPUT}")


if __name__ == "__main__":
    main()
