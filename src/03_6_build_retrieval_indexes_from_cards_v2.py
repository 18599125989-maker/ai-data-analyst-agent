#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
03_6_build_retrieval_indexes_from_cards_v2.py

面向当前 knowledge_manual/table_cards/ 正式卡结构的 retrieval 编译脚本。

相比 v1 的设计重点：
1. 按当前正式 table card 的真实结构做稳定解析
2. 更明确地提取 references / source_field / target_table / target_field
3. 将 known_traps 与 agent_usage_policy 分离成不同索引
4. 更适合作为 data analyst agent 的检索底座

输入：
- knowledge_manual/table_cards/*.yaml
- outputs/knowledge/recipes.json        可选
- outputs/knowledge/recipes.md          可选

输出：
- outputs/knowledge/retrieval_v2/table_index.json
- outputs/knowledge/retrieval_v2/field_index.json
- outputs/knowledge/retrieval_v2/metric_index.json
- outputs/knowledge/retrieval_v2/join_index.json
- outputs/knowledge/retrieval_v2/trap_index.json
- outputs/knowledge/retrieval_v2/policy_index.json
- outputs/knowledge/retrieval_v2/recipe_index.json
- outputs/knowledge/retrieval_v2/compact_table_cards.md
- outputs/knowledge/retrieval_v2/build_summary.json

运行：
python src/03_6_build_retrieval_indexes_from_cards_v2.py
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import yaml


ROOT = Path(__file__).resolve().parents[1]
CARD_DIR = ROOT / "knowledge_manual" / "table_cards"
OUTPUT_DIR = ROOT / "outputs" / "knowledge" / "retrieval_v2"
RECIPES_JSON = ROOT / "outputs" / "knowledge" / "recipes.json"
RECIPES_MD = ROOT / "outputs" / "knowledge" / "recipes.md"


def ensure_output_dir() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def safe_str(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    if isinstance(value, (int, float, bool)):
        return str(value)
    try:
        return json.dumps(value, ensure_ascii=False)
    except Exception:
        return str(value)


def as_list(value: Any) -> List[Any]:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    if isinstance(value, tuple):
        return list(value)
    return [value]


def get_nested(data: Dict[str, Any], path: List[str], default: Any = None) -> Any:
    cur: Any = data
    for key in path:
        if not isinstance(cur, dict):
            return default
        cur = cur.get(key)
        if cur is None:
            return default
    return cur


def normalize_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in {"true", "yes", "1", "y"}
    if isinstance(value, (int, float)):
        return value != 0
    return False


def compact_text(value: Any, max_len: int = 500) -> str:
    text = safe_str(value).replace("\n", " ").strip()
    text = re.sub(r"\s+", " ", text)
    if len(text) > max_len:
        return text[: max_len - 3] + "..."
    return text


def dump_json(path: Path, data: Any) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def tokenize_text(text: str) -> List[str]:
    text = safe_str(text)
    tokens = set()

    for tok in re.findall(r"[A-Za-z_][A-Za-z0-9_]*", text):
        tokens.add(tok.lower())

    chinese_keywords = [
        "收入", "实收", "发票", "付款", "退款", "订阅", "套餐",
        "用户", "企业", "租户", "活跃", "日活", "月活", "时长",
        "工单", "客服", "回复", "解决", "优先级",
        "文档", "协作", "评论", "编辑",
        "活动", "营销", "转化", "归因",
        "nps", "满意度", "评分",
        "ai", "模型", "token", "credit", "credits", "消耗",
        "功能", "使用", "设备", "国家", "地区",
        "快照", "趋势", "排名", "分布", "对比",
        "join", "grain", "distinct", "group by",
    ]
    lower_text = text.lower()
    for kw in chinese_keywords:
        if kw.lower() in lower_text:
            tokens.add(kw.lower())

    return sorted(tokens)


def make_keywords(*parts: Any) -> List[str]:
    return tokenize_text(" ".join(safe_str(p) for p in parts if p is not None))


def slugify(text: str) -> str:
    text = safe_str(text).strip().lower()
    text = re.sub(r"[^\w\u4e00-\u9fff]+", "_", text)
    text = re.sub(r"_+", "_", text)
    return text.strip("_") or "item"


def parse_reference(reference: Any) -> Tuple[str, str]:
    text = safe_str(reference).strip()
    if "." in text:
        table, field = text.split(".", 1)
        return table.strip(), field.strip()
    return text, ""


def card_table_name(card: Dict[str, Any]) -> str:
    return safe_str(card.get("table_name") or Path(card.get("_source_file", "")).stem)


def is_ready_for_agent(card: Dict[str, Any]) -> bool:
    return normalize_bool(get_nested(card, ["status", "ready_for_agent"], False))


def get_profile(card: Dict[str, Any]) -> Dict[str, Any]:
    value = card.get("profile")
    return value if isinstance(value, dict) else {}


def get_validation_summary(card: Dict[str, Any]) -> Dict[str, Any]:
    value = card.get("validation_summary")
    return value if isinstance(value, dict) else {}


def get_columns(card: Dict[str, Any]) -> List[Dict[str, Any]]:
    columns = card.get("columns", {})
    result: List[Dict[str, Any]] = []
    if isinstance(columns, dict):
        for name, meta in columns.items():
            item = {"name": name}
            if isinstance(meta, dict):
                item.update(meta)
            else:
                item["description"] = safe_str(meta)
            result.append(item)
    elif isinstance(columns, list):
        for col in columns:
            if isinstance(col, dict):
                result.append(col)
            elif isinstance(col, str):
                result.append({"name": col})
    return result


def get_field_groups(card: Dict[str, Any]) -> Dict[str, List[str]]:
    raw = card.get("field_groups", {})
    if not isinstance(raw, dict):
        return {}
    return {k: [safe_str(x) for x in as_list(v)] for k, v in raw.items()}


def get_primary_key(card: Dict[str, Any]) -> List[str]:
    return [safe_str(x) for x in as_list(card.get("primary_key")) if safe_str(x)]


def get_natural_key_candidate(card: Dict[str, Any]) -> List[str]:
    return [safe_str(x) for x in as_list(card.get("natural_key_candidate")) if safe_str(x)]


def get_business_meaning(card: Dict[str, Any]) -> str:
    return safe_str(card.get("business_meaning"))


def get_grain_summary(card: Dict[str, Any]) -> str:
    return compact_text(card.get("grain"), 1000)


def get_row_count(card: Dict[str, Any]) -> Optional[int]:
    for source in [get_profile(card), get_validation_summary(card), get_nested(card, ["validation_summary", "key_results"], {})]:
        if isinstance(source, dict):
            for key in ["row_count", "rows"]:
                if key in source:
                    try:
                        return int(source[key])
                    except Exception:
                        pass
    return None


def get_column_count(card: Dict[str, Any]) -> Optional[int]:
    for source in [get_profile(card), get_validation_summary(card), get_nested(card, ["validation_summary", "key_results"], {})]:
        if isinstance(source, dict):
            for key in ["column_count", "columns_count"]:
                if key in source:
                    try:
                        return int(source[key])
                    except Exception:
                        pass
    cols = get_columns(card)
    return len(cols) if cols else None


def warning_level(card: Dict[str, Any]) -> str:
    status = card.get("status", {})
    if not isinstance(status, dict):
        return "high"
    authoring_status = safe_str(status.get("authoring_status")).lower()
    grain_validated = safe_str(status.get("grain_validated")).lower()
    dq_validated = safe_str(status.get("dq_validated")).lower()
    if grain_validated in {"false", "partial"}:
        return "high"
    if authoring_status == "validated_with_warnings":
        return "medium"
    if dq_validated == "partial":
        return "medium"
    return "low"


def load_yaml_cards() -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    cards: List[Dict[str, Any]] = []
    errors: List[Dict[str, Any]] = []
    if not CARD_DIR.exists():
        raise FileNotFoundError(f"Card directory not found: {CARD_DIR}")

    for path in sorted(CARD_DIR.glob("*.yaml")):
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = yaml.safe_load(f)
            if not isinstance(data, dict):
                errors.append({"file": str(path), "error": "YAML root is not dict"})
                continue
            data["_source_file"] = str(path)
            data["_file_size_kb"] = round(path.stat().st_size / 1024.0, 2)
            cards.append(data)
        except Exception as e:
            errors.append({"file": str(path), "error": repr(e)})
    return cards, errors


def extract_metrics(card: Dict[str, Any]) -> List[Dict[str, Any]]:
    metrics = []
    raw = card.get("preferred_metrics", {})
    table_name = card_table_name(card)
    if isinstance(raw, dict):
        for metric_name, meta in raw.items():
            if isinstance(meta, dict):
                metrics.append(
                    {
                        "table_name": table_name,
                        "metric_name": safe_str(metric_name),
                        "expression": safe_str(meta.get("expression")),
                        "meaning": safe_str(meta.get("meaning")),
                        "note": safe_str(meta.get("note")),
                        "applicable_grain": safe_str(meta.get("applicable_grain")),
                        "required_filters": safe_str(meta.get("required_filters") or meta.get("filter")),
                    }
                )
            else:
                metrics.append(
                    {
                        "table_name": table_name,
                        "metric_name": safe_str(metric_name),
                        "expression": safe_str(meta),
                        "meaning": "",
                        "note": "",
                        "applicable_grain": "",
                        "required_filters": "",
                    }
                )
    return metrics


def extract_join_keys(card: Dict[str, Any]) -> List[Dict[str, Any]]:
    joins = []
    table_name = card_table_name(card)
    for item in as_list(card.get("join_keys")):
        if not isinstance(item, dict):
            continue
        source_field = safe_str(item.get("column"))
        target_table, target_field = parse_reference(item.get("references"))
        joins.append(
            {
                "source": "join_keys",
                "source_table": table_name,
                "source_field": source_field,
                "target_table": target_table,
                "target_field": target_field,
                "references": safe_str(item.get("references")),
                "relationship": safe_str(item.get("relationship")),
                "join_condition": safe_str(item.get("join_condition")),
                "purpose": safe_str(item.get("purpose")),
                "risk_level": safe_str(item.get("risk_level")),
                "validation_status": safe_str(item.get("validation_status")),
                "note": safe_str(item.get("note")),
            }
        )
    return joins


def infer_target_from_path(path_steps: List[str]) -> Tuple[str, str]:
    last_table = ""
    last_field = ""
    for step in path_steps:
        matches = re.findall(r"\b(?:dim|fact)_[A-Za-z0-9_]+\.[A-Za-z0-9_]+\b", step)
        if matches:
            table_field = matches[-1]
            last_table, last_field = table_field.split(".", 1)
    return last_table, last_field


def extract_derived_joins(card: Dict[str, Any]) -> List[Dict[str, Any]]:
    joins = []
    table_name = card_table_name(card)
    raw = card.get("derived_join_paths", {})
    if not isinstance(raw, dict):
        return joins
    for path_name, meta in raw.items():
        if not isinstance(meta, dict):
            continue
        path_steps = [safe_str(x) for x in as_list(meta.get("path"))]
        target_table, target_field = infer_target_from_path(path_steps)
        joins.append(
            {
                "source": "derived_join_paths",
                "path_name": safe_str(path_name),
                "source_table": table_name,
                "source_field": "",
                "target_table": target_table,
                "target_field": target_field,
                "references": f"{target_table}.{target_field}" if target_table and target_field else "",
                "relationship": safe_str(meta.get("relationship")),
                "join_condition": "",
                "path": path_steps,
                "purpose": safe_str(meta.get("purpose")),
                "risk_level": safe_str(meta.get("risk_level")),
                "validation_status": safe_str(meta.get("validation_status")),
                "note": safe_str(meta.get("note")),
            }
        )
    return joins


def extract_joins(card: Dict[str, Any]) -> List[Dict[str, Any]]:
    return extract_join_keys(card) + extract_derived_joins(card)


def extract_traps(card: Dict[str, Any]) -> List[Dict[str, Any]]:
    result = []
    table_name = card_table_name(card)
    for item in as_list(card.get("known_traps")):
        if isinstance(item, dict):
            trap = safe_str(item.get("trap"))
            consequence = safe_str(item.get("consequence"))
            prevention = safe_str(item.get("prevention"))
            related_fields = [safe_str(x) for x in as_list(item.get("related_fields"))]
        else:
            trap = safe_str(item)
            consequence = ""
            prevention = ""
            related_fields = []
        text = " ".join([trap, consequence, prevention])
        severity = "high" if any(
            token in text.lower()
            for token in ["count(*)", "distinct", "join", "grain", "unique", "唯一", "重复", "收入", "nps", "快照"]
        ) else "medium"
        result.append(
            {
                "table_name": table_name,
                "trap": trap,
                "consequence": consequence,
                "prevention": prevention,
                "related_fields": related_fields,
                "severity": severity,
            }
        )
    return result


def extract_policy_flags(card: Dict[str, Any]) -> List[Dict[str, Any]]:
    result = []
    table_name = card_table_name(card)
    raw = card.get("agent_usage_policy", {})
    if not isinstance(raw, dict):
        return result
    for flag, value in raw.items():
        if value is None or value == "" or value is False:
            continue
        result.append(
            {
                "table_name": table_name,
                "policy_flag": safe_str(flag),
                "policy_value": value,
                "policy_value_text": safe_str(value),
                "keywords": make_keywords(table_name, flag, value),
            }
        )
    return result


def build_table_index(cards: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    items = []
    for card in cards:
        table_name = card_table_name(card)
        status = card.get("status", {}) if isinstance(card.get("status"), dict) else {}
        fg = get_field_groups(card)
        joins = extract_joins(card)
        metrics = extract_metrics(card)
        natural_key_status = card.get("natural_key_status", {})
        item = {
            "table_name": table_name,
            "source_file": card.get("_source_file"),
            "file_size_kb": card.get("_file_size_kb"),
            "business_meaning": get_business_meaning(card),
            "grain_summary": get_grain_summary(card),
            "row_count": get_row_count(card),
            "column_count": get_column_count(card),
            "authoring_status": status.get("authoring_status"),
            "profile_filled": status.get("profile_filled"),
            "grain_validated": status.get("grain_validated"),
            "dq_validated": status.get("dq_validated"),
            "ready_for_agent": status.get("ready_for_agent"),
            "warning_level": warning_level(card),
            "primary_key": get_primary_key(card),
            "natural_key_candidate": get_natural_key_candidate(card),
            "natural_key_status": natural_key_status if isinstance(natural_key_status, dict) else {},
            "field_groups": fg,
            "metric_names": [m["metric_name"] for m in metrics],
            "join_targets": sorted(
                {
                    j["target_table"]
                    for j in joins
                    if j.get("target_table")
                }
            ),
            "keywords": make_keywords(
                table_name,
                get_business_meaning(card),
                get_grain_summary(card),
                " ".join([c.get("name", "") for c in get_columns(card)]),
                " ".join([m["metric_name"] for m in metrics]),
                safe_str(card.get("known_traps")),
                safe_str(card.get("agent_usage_policy")),
            ),
        }
        items.append(item)
    return items


def build_field_index(cards: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    items = []
    for card in cards:
        table_name = card_table_name(card)
        fg = get_field_groups(card)
        for col in get_columns(card):
            field_name = safe_str(col.get("name"))
            target_table, target_field = parse_reference(col.get("references"))
            items.append(
                {
                    "table_name": table_name,
                    "field_name": field_name,
                    "semantic_type": safe_str(col.get("semantic_type")),
                    "business_meaning": safe_str(col.get("business_meaning")),
                    "references": safe_str(col.get("references")),
                    "target_table": target_table,
                    "target_field": target_field,
                    "validation_status": safe_str(col.get("validation_status")),
                    "sample_values": col.get("sample_values") or get_nested(col, ["profile", "sample_values"], []),
                    "notes": col.get("notes", []),
                    "is_metric": field_name in fg.get("metric_columns", []),
                    "is_dimension": field_name in fg.get("dimension_columns", []),
                    "is_time": field_name in fg.get("time_columns", []),
                    "is_id": field_name in fg.get("id_columns", []),
                    "is_enum": field_name in fg.get("enum_columns", []),
                    "is_json": field_name in fg.get("json_columns", []),
                    "is_text": field_name in fg.get("text_columns", []),
                    "keywords": make_keywords(
                        table_name,
                        field_name,
                        col.get("semantic_type"),
                        col.get("business_meaning"),
                        col.get("references"),
                        col.get("notes"),
                        col.get("sample_values"),
                    ),
                }
            )
    return items


def build_metric_index(cards: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    items = []
    for card in cards:
        for metric in extract_metrics(card):
            metric["keywords"] = make_keywords(
                metric.get("table_name"),
                metric.get("metric_name"),
                metric.get("expression"),
                metric.get("meaning"),
                metric.get("note"),
                metric.get("required_filters"),
            )
            items.append(metric)
    return items


def build_join_index(cards: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    items = []
    for card in cards:
        for join in extract_joins(card):
            join["keywords"] = make_keywords(
                join.get("source_table"),
                join.get("source_field"),
                join.get("target_table"),
                join.get("target_field"),
                join.get("references"),
                join.get("join_condition"),
                join.get("path"),
                join.get("purpose"),
                join.get("risk_level"),
                join.get("validation_status"),
                join.get("note"),
            )
            items.append(join)
    return items


def build_trap_index(cards: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    items = []
    for card in cards:
        for trap in extract_traps(card):
            trap["keywords"] = make_keywords(
                trap.get("table_name"),
                trap.get("trap"),
                trap.get("consequence"),
                trap.get("prevention"),
                trap.get("related_fields"),
            )
            items.append(trap)
    return items


def build_policy_index(cards: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    items = []
    for card in cards:
        items.extend(extract_policy_flags(card))
    return items


def recipe_from_sql_patterns(card: Dict[str, Any]) -> List[Dict[str, Any]]:
    table_name = card_table_name(card)
    raw = card.get("sql_patterns", {})
    recipes = []
    if not isinstance(raw, dict):
        return recipes
    for name, sql in raw.items():
        recipes.append(
            {
                "recipe_id": f"{table_name}__{slugify(name)}",
                "name": safe_str(name),
                "description": "",
                "required_tables": [table_name],
                "required_fields": [],
                "metrics": [],
                "dimensions": [],
                "join_paths": [],
                "grain_rules": [get_grain_summary(card)],
                "dq_rules": [t["trap"] for t in extract_traps(card)[:3]],
                "sql_skeleton": safe_str(sql),
                "source": "table_card_sql_pattern",
                "confidence": "medium_or_low",
            }
        )
    return recipes


def load_recipes_json() -> List[Dict[str, Any]]:
    if not RECIPES_JSON.exists():
        return []
    try:
        with open(RECIPES_JSON, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, list):
            return [x for x in data if isinstance(x, dict)]
        if isinstance(data, dict):
            raw = data.get("recipes") or data.get("items") or []
            return [x for x in as_list(raw) if isinstance(x, dict)]
    except Exception:
        return []
    return []


def load_recipes_md() -> List[Dict[str, Any]]:
    if not RECIPES_MD.exists():
        return []
    try:
        text = RECIPES_MD.read_text(encoding="utf-8")
    except Exception:
        return []
    recipes = []
    sections = re.split(r"\n(?=##\s+)", text)
    for sec in sections:
        sec = sec.strip()
        if not sec:
            continue
        m = re.search(r"^##\s+(.+)$", sec, flags=re.MULTILINE)
        if not m:
            continue
        title = m.group(1).strip()
        recipes.append(
            {
                "recipe_id": slugify(title),
                "name": title,
                "description": compact_text(sec, 1200),
                "required_tables": [],
                "required_fields": [],
                "metrics": [],
                "dimensions": [],
                "join_paths": [],
                "grain_rules": [],
                "dq_rules": [],
                "sql_skeleton": "",
                "source": "sample_question_recipe",
                "confidence": "medium",
            }
        )
    return recipes


def build_recipe_index(cards: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    raw = load_recipes_json()
    if not raw:
        raw = load_recipes_md()
    if not raw:
        for card in cards:
            raw.extend(recipe_from_sql_patterns(card))

    result = []
    seen = {}
    for item in raw:
        rid = safe_str(item.get("recipe_id") or slugify(item.get("name")))
        seen[rid] = seen.get(rid, 0) + 1
        if seen[rid] > 1:
            rid = f"{rid}_{seen[rid]}"
        normalized = {
            "recipe_id": rid,
            "name": safe_str(item.get("name")),
            "description": safe_str(item.get("description")),
            "required_tables": [safe_str(x) for x in as_list(item.get("required_tables"))],
            "required_fields": [safe_str(x) for x in as_list(item.get("required_fields"))],
            "metrics": [safe_str(x) for x in as_list(item.get("metrics"))],
            "dimensions": [safe_str(x) for x in as_list(item.get("dimensions"))],
            "join_paths": as_list(item.get("join_paths")),
            "grain_rules": [safe_str(x) for x in as_list(item.get("grain_rules"))],
            "dq_rules": [safe_str(x) for x in as_list(item.get("dq_rules"))],
            "sql_skeleton": safe_str(item.get("sql_skeleton") or item.get("sql")),
            "source": safe_str(item.get("source")),
            "confidence": safe_str(item.get("confidence")),
        }
        normalized["keywords"] = make_keywords(
            normalized["recipe_id"],
            normalized["name"],
            normalized["description"],
            normalized["required_tables"],
            normalized["required_fields"],
            normalized["metrics"],
            normalized["dimensions"],
            normalized["join_paths"],
            normalized["grain_rules"],
            normalized["dq_rules"],
            normalized["sql_skeleton"],
        )
        result.append(normalized)
    return result


def build_compact_markdown(cards: List[Dict[str, Any]]) -> str:
    parts = ["# Compact Table Cards v2\n", "\n", "> Auto-generated for retrieval_v2.\n"]
    for card in sorted(cards, key=card_table_name):
        table_name = card_table_name(card)
        status = card.get("status", {}) if isinstance(card.get("status"), dict) else {}
        parts.append(f"\n## {table_name}\n")
        parts.append(f"- authoring_status: `{status.get('authoring_status')}`\n")
        parts.append(f"- grain_validated: `{status.get('grain_validated')}`\n")
        parts.append(f"- dq_validated: `{status.get('dq_validated')}`\n")
        parts.append(f"- warning_level: `{warning_level(card)}`\n")
        parts.append(f"- business_meaning: {compact_text(get_business_meaning(card), 320)}\n")
        parts.append(f"- grain: {compact_text(get_grain_summary(card), 420)}\n")
        pk = get_primary_key(card)
        nk = get_natural_key_candidate(card)
        parts.append(f"- primary_key: `{pk}`\n")
        parts.append(f"- natural_key_candidate: `{nk}`\n")
        metrics = extract_metrics(card)[:6]
        if metrics:
            parts.append("- metrics:\n")
            for metric in metrics:
                parts.append(
                    f"  - {metric['metric_name']}: `{compact_text(metric['expression'], 120)}`\n"
                )
        joins = extract_joins(card)[:5]
        if joins:
            parts.append("- join_rules:\n")
            for join in joins:
                label = join.get("references") or join.get("target_table") or ""
                extra = join.get("join_condition") or safe_str(join.get("path"))
                parts.append(f"  - {compact_text(label, 120)} | {compact_text(extra, 180)}\n")
        traps = extract_traps(card)[:4]
        if traps:
            parts.append("- known_traps:\n")
            for trap in traps:
                parts.append(f"  - {compact_text(trap['trap'], 180)}\n")
        policies = extract_policy_flags(card)[:8]
        if policies:
            parts.append("- policy_flags:\n")
            for policy in policies:
                value = policy["policy_value_text"]
                parts.append(f"  - {policy['policy_flag']}: {compact_text(value, 120)}\n")
    return "".join(parts)


def build_summary(
    all_cards: List[Dict[str, Any]],
    ready_cards: List[Dict[str, Any]],
    parse_errors: List[Dict[str, Any]],
    table_index: List[Dict[str, Any]],
    field_index: List[Dict[str, Any]],
    metric_index: List[Dict[str, Any]],
    join_index: List[Dict[str, Any]],
    trap_index: List[Dict[str, Any]],
    policy_index: List[Dict[str, Any]],
    recipe_index: List[Dict[str, Any]],
) -> Dict[str, Any]:
    skipped_cards = []
    for card in all_cards:
        if not is_ready_for_agent(card):
            skipped_cards.append(
                {
                    "table_name": card_table_name(card),
                    "source_file": card.get("_source_file"),
                    "authoring_status": get_nested(card, ["status", "authoring_status"]),
                    "ready_for_agent": get_nested(card, ["status", "ready_for_agent"]),
                }
            )
    return {
        "total_yaml_files": len(list(CARD_DIR.glob("*.yaml"))) if CARD_DIR.exists() else 0,
        "parsed_count": len(all_cards),
        "parse_failed_count": len(parse_errors),
        "ready_for_agent_count": len(ready_cards),
        "skipped_count": len(skipped_cards),
        "table_index_count": len(table_index),
        "field_index_count": len(field_index),
        "metric_index_count": len(metric_index),
        "join_index_count": len(join_index),
        "trap_index_count": len(trap_index),
        "policy_index_count": len(policy_index),
        "recipe_index_count": len(recipe_index),
        "skipped_cards": skipped_cards,
        "errors": parse_errors,
        "output_files": {
            "table_index": str(OUTPUT_DIR / "table_index.json"),
            "field_index": str(OUTPUT_DIR / "field_index.json"),
            "metric_index": str(OUTPUT_DIR / "metric_index.json"),
            "join_index": str(OUTPUT_DIR / "join_index.json"),
            "trap_index": str(OUTPUT_DIR / "trap_index.json"),
            "policy_index": str(OUTPUT_DIR / "policy_index.json"),
            "recipe_index": str(OUTPUT_DIR / "recipe_index.json"),
            "compact_table_cards": str(OUTPUT_DIR / "compact_table_cards.md"),
            "build_summary": str(OUTPUT_DIR / "build_summary.json"),
        },
    }


def main() -> None:
    ensure_output_dir()
    all_cards, parse_errors = load_yaml_cards()
    ready_cards = [card for card in all_cards if is_ready_for_agent(card)]

    table_index = build_table_index(ready_cards)
    field_index = build_field_index(ready_cards)
    metric_index = build_metric_index(ready_cards)
    join_index = build_join_index(ready_cards)
    trap_index = build_trap_index(ready_cards)
    policy_index = build_policy_index(ready_cards)
    recipe_index = build_recipe_index(ready_cards)
    compact_md = build_compact_markdown(ready_cards)

    dump_json(OUTPUT_DIR / "table_index.json", table_index)
    dump_json(OUTPUT_DIR / "field_index.json", field_index)
    dump_json(OUTPUT_DIR / "metric_index.json", metric_index)
    dump_json(OUTPUT_DIR / "join_index.json", join_index)
    dump_json(OUTPUT_DIR / "trap_index.json", trap_index)
    dump_json(OUTPUT_DIR / "policy_index.json", policy_index)
    dump_json(OUTPUT_DIR / "recipe_index.json", recipe_index)
    (OUTPUT_DIR / "compact_table_cards.md").write_text(compact_md, encoding="utf-8")

    summary = build_summary(
        all_cards=all_cards,
        ready_cards=ready_cards,
        parse_errors=parse_errors,
        table_index=table_index,
        field_index=field_index,
        metric_index=metric_index,
        join_index=join_index,
        trap_index=trap_index,
        policy_index=policy_index,
        recipe_index=recipe_index,
    )
    dump_json(OUTPUT_DIR / "build_summary.json", summary)

    print("=" * 72)
    print("Retrieval index build v2 completed.")
    print("=" * 72)
    print(f"Total YAML files:       {summary['total_yaml_files']}")
    print(f"Parsed cards:           {summary['parsed_count']}")
    print(f"Parse failed:           {summary['parse_failed_count']}")
    print(f"Ready for agent cards:  {summary['ready_for_agent_count']}")
    print(f"Skipped cards:          {summary['skipped_count']}")
    print("-" * 72)
    print(f"table_index_count:      {summary['table_index_count']}")
    print(f"field_index_count:      {summary['field_index_count']}")
    print(f"metric_index_count:     {summary['metric_index_count']}")
    print(f"join_index_count:       {summary['join_index_count']}")
    print(f"trap_index_count:       {summary['trap_index_count']}")
    print(f"policy_index_count:     {summary['policy_index_count']}")
    print(f"recipe_index_count:     {summary['recipe_index_count']}")
    print("-" * 72)
    print(f"Output dir:             {OUTPUT_DIR}")
    print(f"Compact cards:          {OUTPUT_DIR / 'compact_table_cards.md'}")
    print(f"Build summary:          {OUTPUT_DIR / 'build_summary.json'}")
    print("=" * 72)
    if parse_errors:
        print("WARNING: Some YAML files failed to parse. See build_summary.json.")


if __name__ == "__main__":
    main()
