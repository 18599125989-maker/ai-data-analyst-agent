#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
功能：
把 knowledge_manual/table_cards/*.yaml 编译成 Agent 可直接检索和使用的知识文件。

输入：
knowledge_manual/table_cards/
├── dim_department.yaml
├── fact_actual_revenue.yaml
└── fact_ai_usage_log.yaml

输出：
outputs/knowledge/manual_cards/
├── table_cards_manual.json
├── compact_table_cards.md
├── table_card_index.json
└── build_summary.json

说明：
1. table_cards_manual.json：
   保存完整 YAML 结构，供程序精确读取。

2. compact_table_cards.md：
   保存短版上下文，适合放入 LLM prompt。

3. table_card_index.json：
   保存检索索引，包括表名、domain、字段名、join key、use cases、traps、summary 等。

4. build_summary.json：
   保存本次编译摘要。
"""

import json
from pathlib import Path
from typing import Any, Dict, List

import yaml


PROJECT_ROOT = Path(__file__).resolve().parents[1]

CARD_DIR = PROJECT_ROOT / "knowledge_manual" / "table_cards"

OUTPUT_DIR = PROJECT_ROOT / "outputs" / "knowledge" / "manual_cards"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

FULL_JSON_OUTPUT = OUTPUT_DIR / "table_cards_manual.json"
COMPACT_MD_OUTPUT = OUTPUT_DIR / "compact_table_cards.md"
INDEX_JSON_OUTPUT = OUTPUT_DIR / "table_card_index.json"
SUMMARY_JSON_OUTPUT = OUTPUT_DIR / "build_summary.json"


def load_yaml_file(path: Path) -> Dict[str, Any]:
    """读取单个 YAML table card。"""
    with open(path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f)

    if not isinstance(data, dict):
        raise ValueError(f"YAML 内容不是 dict：{path}")

    if "table_name" not in data:
        raise ValueError(f"缺少 table_name：{path}")

    return data


def safe_get(data: Dict[str, Any], key: str, default=None):
    value = data.get(key, default)
    return value if value is not None else default


def to_list(value: Any) -> List[Any]:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    return [value]


def get_domain_name(card: Dict[str, Any]) -> str:
    domain = card.get("domain", {})
    if isinstance(domain, dict):
        return domain.get("name", "")
    return str(domain)


def get_domain_id(card: Dict[str, Any]) -> str:
    domain = card.get("domain", {})
    if isinstance(domain, dict):
        return domain.get("id", "")
    return ""


def get_status(card: Dict[str, Any]) -> Dict[str, Any]:
    status = card.get("status", {})
    return status if isinstance(status, dict) else {}


def get_column_names(card: Dict[str, Any]) -> List[str]:
    columns = card.get("columns", {})
    if isinstance(columns, dict):
        return list(columns.keys())
    return []


def extract_join_targets(card: Dict[str, Any]) -> List[str]:
    targets = []
    for item in to_list(card.get("join_keys")):
        if isinstance(item, dict):
            ref = item.get("references")
            if ref:
                targets.append(str(ref))
    return targets


def extract_field_groups(card: Dict[str, Any]) -> Dict[str, List[str]]:
    field_groups = card.get("field_groups", {})
    if not isinstance(field_groups, dict):
        return {}

    result = {}
    for key, value in field_groups.items():
        result[key] = [str(x) for x in to_list(value)]
    return result


def extract_preferred_metrics(card: Dict[str, Any]) -> List[Dict[str, str]]:
    metrics = card.get("preferred_metrics", {})
    result = []

    if isinstance(metrics, dict):
        for name, info in metrics.items():
            if isinstance(info, dict):
                result.append({
                    "name": str(name),
                    "expression": str(info.get("expression", "")),
                    "meaning": str(info.get("meaning", "")),
                    "note": str(info.get("note", "")),
                })
            else:
                result.append({
                    "name": str(name),
                    "expression": "",
                    "meaning": str(info),
                    "note": "",
                })

    return result


def extract_known_traps(card: Dict[str, Any]) -> List[str]:
    traps = []
    for item in to_list(card.get("known_traps")):
        if isinstance(item, dict):
            trap = item.get("trap", "")
            consequence = item.get("consequence", "")
            prevention = item.get("prevention", "")
            text = f"Trap: {trap} Consequence: {consequence} Prevention: {prevention}".strip()
            traps.append(text)
        else:
            traps.append(str(item))
    return traps


def extract_sql_pattern_names(card: Dict[str, Any]) -> List[str]:
    sql_patterns = card.get("sql_patterns", {})
    if isinstance(sql_patterns, dict):
        return list(sql_patterns.keys())
    return []


def build_index_record(card: Dict[str, Any], source_path: Path) -> Dict[str, Any]:
    """构建检索索引记录。"""
    table_name = card.get("table_name")
    status = get_status(card)

    field_groups = extract_field_groups(card)

    record = {
        "table_name": table_name,
        "source_file": str(source_path.relative_to(PROJECT_ROOT)),
        "card_version": card.get("card_version"),
        "card_type": card.get("card_type"),
        "domain_id": get_domain_id(card),
        "domain_name": get_domain_name(card),
        "ready_for_agent": bool(status.get("ready_for_agent", False)),
        "authoring_status": status.get("authoring_status"),
        "profile_filled": status.get("profile_filled"),
        "grain_validated": status.get("grain_validated"),
        "dq_validated": status.get("dq_validated"),
        "business_meaning": card.get("business_meaning", ""),
        "grain": card.get("grain", ""),
        "primary_key": to_list(card.get("primary_key")),
        "natural_key_candidate": to_list(card.get("natural_key_candidate")),
        "columns": get_column_names(card),
        "field_groups": field_groups,
        "join_targets": extract_join_targets(card),
        "recommended_use_cases": [str(x) for x in to_list(card.get("recommended_use_cases"))],
        "preferred_metrics": extract_preferred_metrics(card),
        "known_traps": extract_known_traps(card),
        "sql_pattern_names": extract_sql_pattern_names(card),
        "prompt_summary": card.get("prompt_summary", ""),
    }

    # 方便简单关键词检索：拼一个 search_text
    search_parts = [
        record["table_name"],
        record["domain_name"],
        record["business_meaning"],
        record["grain"],
        record["prompt_summary"],
        " ".join(record["columns"]),
        " ".join(record["recommended_use_cases"]),
        " ".join(record["join_targets"]),
        " ".join(record["known_traps"]),
        " ".join([m["name"] + " " + m["meaning"] + " " + m["expression"] for m in record["preferred_metrics"]]),
    ]

    record["search_text"] = "\n".join([str(x) for x in search_parts if x])

    return record


def format_list(items: List[Any], indent: str = "- ") -> str:
    if not items:
        return "- 无"
    return "\n".join(f"{indent}{item}" for item in items)


def build_compact_card_md(card: Dict[str, Any]) -> str:
    """生成单张表的 compact markdown，上下文较短，适合喂给 Agent。"""
    table_name = card.get("table_name")
    status = get_status(card)
    field_groups = extract_field_groups(card)

    lines = []
    lines.append(f"## {table_name}")
    lines.append("")
    lines.append(f"- domain: {get_domain_name(card)}")
    lines.append(f"- status: {status.get('authoring_status')}")
    lines.append(f"- ready_for_agent: {status.get('ready_for_agent')}")
    lines.append(f"- grain_validated: {status.get('grain_validated')}")
    lines.append(f"- dq_validated: {status.get('dq_validated')}")
    lines.append("")

    if card.get("prompt_summary"):
        lines.append("### Prompt Summary")
        lines.append(card.get("prompt_summary", "").strip())
        lines.append("")

    lines.append("### Grain")
    lines.append(str(card.get("grain", "")).strip())
    lines.append("")

    lines.append("### Primary Key")
    lines.append(format_list([str(x) for x in to_list(card.get("primary_key"))]))
    lines.append("")

    if card.get("natural_key_candidate") or card.get("natural_key_status"):
        lines.append("### Natural Key")
        lines.append(format_list([str(x) for x in to_list(card.get("natural_key_candidate"))]))
        natural_status = card.get("natural_key_status")
        if natural_status:
            lines.append("")
            lines.append(f"- natural_key_status: {json.dumps(natural_status, ensure_ascii=False)}")
        lines.append("")

    lines.append("### Columns")
    columns = card.get("columns", {})
    if isinstance(columns, dict):
        for col_name, col_info in columns.items():
            if isinstance(col_info, dict):
                semantic_type = col_info.get("semantic_type", "")
                meaning = str(col_info.get("business_meaning", "")).strip().replace("\n", " ")
                ref = col_info.get("references")
                short = f"- {col_name}"
                if semantic_type:
                    short += f" [{semantic_type}]"
                if ref:
                    short += f" -> {ref}"
                if meaning:
                    short += f": {meaning}"
                lines.append(short)
            else:
                lines.append(f"- {col_name}")
    else:
        lines.append("- 无")
    lines.append("")

    if field_groups:
        lines.append("### Field Groups")
        for group_name, values in field_groups.items():
            lines.append(f"- {group_name}: {', '.join(values) if values else '[]'}")
        lines.append("")

    join_keys = to_list(card.get("join_keys"))
    if join_keys:
        lines.append("### Join Keys")
        for item in join_keys:
            if isinstance(item, dict):
                col = item.get("column", "")
                ref = item.get("references", "")
                condition = item.get("join_condition", "")
                risk = item.get("risk_level", "")
                status_text = item.get("validation_status", "")
                note = str(item.get("note", "")).strip().replace("\n", " ")
                lines.append(f"- {col} -> {ref}; condition: {condition}; risk: {risk}; status: {status_text}; note: {note}")
        lines.append("")

    derived_paths = card.get("derived_join_paths", {})
    if isinstance(derived_paths, dict) and derived_paths:
        lines.append("### Derived Join Paths")
        for path_name, path_info in derived_paths.items():
            if isinstance(path_info, dict):
                path = path_info.get("path", [])
                purpose = str(path_info.get("purpose", "")).strip().replace("\n", " ")
                risk = path_info.get("risk_level", "")
                lines.append(f"- {path_name}: {' -> '.join([str(x) for x in to_list(path)])}; risk: {risk}; purpose: {purpose}")
        lines.append("")

    metrics = extract_preferred_metrics(card)
    if metrics:
        lines.append("### Preferred Metrics")
        for metric in metrics:
            lines.append(
                f"- {metric['name']}: {metric['expression']} | {metric['meaning']} | {metric['note'].strip().replace(chr(10), ' ')}"
            )
        lines.append("")

    agent_policy = card.get("agent_usage_policy")
    if isinstance(agent_policy, dict) and agent_policy:
        lines.append("### Agent Usage Policy")
        for key, value in agent_policy.items():
            lines.append(f"- {key}: {value}")
        lines.append("")

    traps = extract_known_traps(card)
    if traps:
        lines.append("### Known Traps")
        for trap in traps:
            lines.append(f"- {trap.strip().replace(chr(10), ' ')}")
        lines.append("")

    sql_patterns = card.get("sql_patterns", {})
    if isinstance(sql_patterns, dict) and sql_patterns:
        lines.append("### SQL Pattern Names")
        for name in sql_patterns.keys():
            lines.append(f"- {name}")
        lines.append("")

    return "\n".join(lines).strip() + "\n"


def build_all_compact_md(cards: List[Dict[str, Any]]) -> str:
    lines = []
    lines.append("# Manual Table Cards - Compact Agent Knowledge")
    lines.append("")
    lines.append("本文件由 knowledge_manual/table_cards/*.yaml 自动编译生成。")
    lines.append("它是给 Agent 检索和放入 prompt 的短版知识，不是人工源文件。")
    lines.append("")

    ready_count = sum(1 for c in cards if get_status(c).get("ready_for_agent") is True)
    lines.append("## Build Overview")
    lines.append("")
    lines.append(f"- table_card_count: {len(cards)}")
    lines.append(f"- ready_for_agent_count: {ready_count}")
    lines.append("")

    for card in cards:
        lines.append(build_compact_card_md(card))
        lines.append("---")
        lines.append("")

    return "\n".join(lines)


def main():
    if not CARD_DIR.exists():
        raise FileNotFoundError(f"找不到 table card 目录：{CARD_DIR}")

    yaml_files = sorted(CARD_DIR.glob("*.yaml"))

    if not yaml_files:
        raise FileNotFoundError(f"没有找到 YAML 文件：{CARD_DIR}")

    cards = []
    index_records = []
    errors = []

    print(f"发现 {len(yaml_files)} 个 table card YAML。")

    for path in yaml_files:
        print(f"读取：{path}")
        try:
            card = load_yaml_file(path)
            cards.append(card)
            index_records.append(build_index_record(card, path))
        except Exception as e:
            errors.append({
                "file": str(path),
                "error": str(e),
            })

    # 按 table_name 排序，保证输出稳定
    cards = sorted(cards, key=lambda x: x.get("table_name", ""))
    index_records = sorted(index_records, key=lambda x: x.get("table_name", ""))

    with open(FULL_JSON_OUTPUT, "w", encoding="utf-8") as f:
        json.dump(cards, f, ensure_ascii=False, indent=2)

    compact_md = build_all_compact_md(cards)
    with open(COMPACT_MD_OUTPUT, "w", encoding="utf-8") as f:
        f.write(compact_md)

    with open(INDEX_JSON_OUTPUT, "w", encoding="utf-8") as f:
        json.dump(index_records, f, ensure_ascii=False, indent=2)

    ready_count = sum(1 for r in index_records if r.get("ready_for_agent") is True)
    summary = {
        "source_dir": str(CARD_DIR.relative_to(PROJECT_ROOT)),
        "output_dir": str(OUTPUT_DIR.relative_to(PROJECT_ROOT)),
        "card_count": len(cards),
        "ready_for_agent_count": ready_count,
        "not_ready_count": len(cards) - ready_count,
        "tables": [
            {
                "table_name": r["table_name"],
                "ready_for_agent": r["ready_for_agent"],
                "authoring_status": r["authoring_status"],
                "grain_validated": r["grain_validated"],
                "dq_validated": r["dq_validated"],
            }
            for r in index_records
        ],
        "errors": errors,
        "outputs": {
            "full_json": str(FULL_JSON_OUTPUT.relative_to(PROJECT_ROOT)),
            "compact_md": str(COMPACT_MD_OUTPUT.relative_to(PROJECT_ROOT)),
            "index_json": str(INDEX_JSON_OUTPUT.relative_to(PROJECT_ROOT)),
            "summary_json": str(SUMMARY_JSON_OUTPUT.relative_to(PROJECT_ROOT)),
        },
    }

    with open(SUMMARY_JSON_OUTPUT, "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)

    print("")
    print("=" * 70)
    print("Agent knowledge 编译完成。")
    print(f"读取 table cards：{len(cards)}")
    print(f"ready_for_agent：{ready_count}")
    print("")
    print(f"完整结构化知识：{FULL_JSON_OUTPUT}")
    print(f"短版 Agent 上下文：{COMPACT_MD_OUTPUT}")
    print(f"检索索引：{INDEX_JSON_OUTPUT}")
    print(f"编译摘要：{SUMMARY_JSON_OUTPUT}")
    print("=" * 70)

    if errors:
        print("")
        print("注意：部分 YAML 读取失败：")
        for err in errors:
            print(f"- {err['file']}: {err['error']}")


if __name__ == "__main__":
    main()