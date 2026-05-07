#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
CloudWork AI Data Analyst Agent CLI v2

目标流程：
1. Agent1: Task Understanding
2. Agent2: Knowledge Retriever
3. Agent3: Grain & DQ Guard
4. Agent4: SQL Planner + Generator
5. Agent5: SQL Executor + Repair
6. Agent6: Answer + Visualization

运行前建议先执行：
python3 src/00_load_data.py
python3 src/01_profile_tables.py
python3 src/02_sample_questions.py
python3 src/03_build_knowledge_base.py
python3 src/03_6_build_retrieval_indexes_from_cards_v2.py
python3 src/03_7_build_visualization_knowledge.py
"""

from __future__ import annotations

import importlib.util
import json
import math
import os
import re
import threading
import time
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, List, Tuple
from urllib import error, request

import duckdb
import matplotlib.pyplot as plt
import pandas as pd

try:
    import numpy as np
except Exception:  # pragma: no cover - numpy may be unavailable in some environments
    np = None

try:
    from config_paths import DB_PATH, ENV_PATH, PROJECT_ROOT
except ModuleNotFoundError:
    from src.config_paths import DB_PATH, ENV_PATH, PROJECT_ROOT

KNOWLEDGE_DIR = PROJECT_ROOT / "outputs" / "knowledge"
RETRIEVAL_V2_DIR = KNOWLEDGE_DIR / "retrieval_v2"
LOG_DIR = PROJECT_ROOT / "outputs" / "logs" / "query_logs"
ASSET_DIR = PROJECT_ROOT / "outputs" / "logs" / "query_assets"
MEMORY_DIR = PROJECT_ROOT / "outputs" / "memory"
ANSWER_MEMORY_PATH = MEMORY_DIR / "answer_memory.jsonl"

TABLE_CARDS_PATH = KNOWLEDGE_DIR / "table_cards.json"
COLUMN_CARDS_PATH = KNOWLEDGE_DIR / "column_cards.json"
DQ_RULES_PATH = KNOWLEDGE_DIR / "dq_rules.json"
GRAIN_RULES_PATH = KNOWLEDGE_DIR / "grain_rules.json"
VISUALIZATION_RULES_PATH = KNOWLEDGE_DIR / "visualization_rules.json"

TABLE_INDEX_PATH = RETRIEVAL_V2_DIR / "table_index.json"
FIELD_INDEX_PATH = RETRIEVAL_V2_DIR / "field_index.json"
METRIC_INDEX_PATH = RETRIEVAL_V2_DIR / "metric_index.json"
JOIN_INDEX_PATH = RETRIEVAL_V2_DIR / "join_index.json"
TRAP_INDEX_PATH = RETRIEVAL_V2_DIR / "trap_index.json"
POLICY_INDEX_PATH = RETRIEVAL_V2_DIR / "policy_index.json"
RECIPE_INDEX_PATH = RETRIEVAL_V2_DIR / "recipe_index.json"
FULL_RECIPES_PATH = RETRIEVAL_V2_DIR / "recipes.json"
TABLE_EMBEDDING_INDEX_PATH = RETRIEVAL_V2_DIR / "table_embedding_index.json"
FIELD_EMBEDDING_INDEX_PATH = RETRIEVAL_V2_DIR / "field_embedding_index.json"
METRIC_EMBEDDING_INDEX_PATH = RETRIEVAL_V2_DIR / "metric_embedding_index.json"
RECIPE_EMBEDDING_INDEX_PATH = RETRIEVAL_V2_DIR / "recipe_embedding_index.json"
ANSWER_MEMORY_INDEX_PATH = RETRIEVAL_V2_DIR / "answer_memory_index.json"
ANSWER_MEMORY_EMBEDDING_INDEX_PATH = RETRIEVAL_V2_DIR / "answer_memory_embedding_index.json"

DEFAULT_SILICONFLOW_API_URL = "https://api.siliconflow.cn/v1/chat/completions"
DEFAULT_MODEL = "Qwen/Qwen2.5-72B-Instruct"
DEFAULT_SILICONFLOW_EMBEDDING_API_URL = "https://api.siliconflow.cn/v1/embeddings"
DEFAULT_SILICONFLOW_EMBEDDING_MODEL = "BAAI/bge-m3"

MAX_CONTEXT_TABLES = 8
MAX_CONTEXT_FIELDS = 40
MAX_CONTEXT_METRICS = 18
MAX_CONTEXT_JOINS = 20
MAX_CONTEXT_TRAPS = 16
MAX_CONTEXT_POLICIES = 16
MAX_CONTEXT_RECIPES = 5
MAX_CONTEXT_ANSWER_MEMORIES = 1
MAX_RESULT_ROWS = 30
MAX_REPAIR_ATTEMPTS = 1
STRUCTURAL_BOOST_SCORE = 6.0


def load_lineage_builder():
    lineage_path = PROJECT_ROOT / "src" / "06_lineage.py"
    spec = importlib.util.spec_from_file_location("lineage_module", lineage_path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module.build_lineage


def get_lineage_builder():
    try:
        return load_lineage_builder()
    except Exception:
        return lambda *args, **kwargs: {}


def load_answer_memory_index_builder():
    builder_path = PROJECT_ROOT / "src" / "08_build_answer_memory_index.py"
    spec = importlib.util.spec_from_file_location("answer_memory_index_module", builder_path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module.build_answer_memory_indexes


def get_answer_memory_index_builder():
    try:
        return load_answer_memory_index_builder()
    except Exception:
        return None


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def load_dotenv(env_path: Path) -> None:
    if not env_path.exists():
        return

    for line in env_path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = value


def read_json(path: Path) -> Any:
    if not path.exists():
        raise FileNotFoundError(f"找不到文件：{path}")
    return json.loads(path.read_text(encoding="utf-8"))


def read_optional_json(path: Path, default: Any) -> Any:
    if path.exists():
        return read_json(path)
    return default


def make_json_serializable(obj: Any) -> Any:
    if obj is None:
        return None

    if type(obj).__name__ == "NaTType":
        return None

    if isinstance(obj, (pd.Timestamp, datetime, date)):
        return obj.isoformat()

    if obj is pd.NaT:
        return None

    try:
        if pd.isna(obj):
            return None
    except Exception:
        pass

    if np is not None:
        if isinstance(obj, np.integer):
            return int(obj)
        if isinstance(obj, np.floating):
            return float(obj)
        if isinstance(obj, np.bool_):
            return bool(obj)
        if isinstance(obj, np.ndarray):
            return [make_json_serializable(item) for item in obj.tolist()]

    if isinstance(obj, dict):
        return {
            make_json_serializable(key): make_json_serializable(value)
            for key, value in obj.items()
        }

    if isinstance(obj, (list, tuple, set)):
        return [make_json_serializable(item) for item in obj]

    if isinstance(obj, (str, int, float, bool)):
        return obj

    try:
        json.dumps(obj, ensure_ascii=False)
        return obj
    except TypeError:
        return str(obj)


def write_json(path: Path, data: Any) -> None:
    ensure_dir(path.parent)
    safe_data = make_json_serializable(data)
    path.write_text(json.dumps(safe_data, ensure_ascii=False, indent=2), encoding="utf-8")


def append_jsonl(path: Path, data: Any) -> None:
    ensure_dir(path.parent)
    with path.open("a", encoding="utf-8") as f:
        safe_data = make_json_serializable(data)
        f.write(json.dumps(safe_data, ensure_ascii=False) + "\n")


def normalize_text(text: str) -> str:
    return re.sub(r"\s+", " ", str(text).lower()).strip()


def tokenize(text: str) -> List[str]:
    normalized = normalize_text(text)
    latin_tokens = re.findall(r"[a-zA-Z0-9_]+", normalized)
    chinese_chunks = re.findall(r"[\u4e00-\u9fff]{2,}", normalized)
    ordered = []
    seen = set()
    for token in latin_tokens + chinese_chunks:
        if token not in seen:
            seen.add(token)
            ordered.append(token)
    return ordered


def safe_to_string(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    if isinstance(value, (list, dict)):
        try:
            return json.dumps(make_json_serializable(value), ensure_ascii=False)
        except TypeError:
            return str(value)
    return str(value)


def unique_keep_order(items: List[str]) -> List[str]:
    return list(dict.fromkeys(item for item in items if item))


def extract_table_field_refs(text: str) -> List[str]:
    if not text:
        return []
    return unique_keep_order(
        re.findall(r"\b((?:dim|fact)_[A-Za-z0-9_]+\.[A-Za-z0-9_]+)\b", safe_to_string(text))
    )


def extract_table_names(text: str) -> List[str]:
    refs = extract_table_field_refs(text)
    tables = [ref.split(".", 1)[0] for ref in refs if "." in ref]
    tables.extend(re.findall(r"\b((?:dim|fact)_[A-Za-z0-9_]+)\b", safe_to_string(text)))
    return unique_keep_order(tables)


def extract_join_item_tables(item: Dict[str, Any]) -> List[str]:
    tables = [
        safe_to_string(item.get("source_table")),
        safe_to_string(item.get("target_table")),
    ]
    for step in item.get("path", []) or []:
        tables.extend(extract_table_names(step))
    return unique_keep_order(tables)


def extract_join_item_fields(item: Dict[str, Any]) -> List[str]:
    fields = []
    source_table = safe_to_string(item.get("source_table"))
    source_field = safe_to_string(item.get("source_field"))
    target_table = safe_to_string(item.get("target_table"))
    target_field = safe_to_string(item.get("target_field"))
    if source_table and source_field:
        fields.append(f"{source_table}.{source_field}")
    if target_table and target_field:
        fields.append(f"{target_table}.{target_field}")
    for step in item.get("path", []) or []:
        fields.extend(extract_table_field_refs(step))
    return unique_keep_order(fields)


def extract_recipe_tables(item: Dict[str, Any]) -> List[str]:
    return unique_keep_order(
        [safe_to_string(x) for x in (item.get("required_tables", []) or [])]
        + [safe_to_string(x) for x in (item.get("optional_tables", []) or [])]
        + [safe_to_string(x) for x in (item.get("tables", []) or [])]
    )


def extract_recipe_fields(item: Dict[str, Any]) -> List[str]:
    return unique_keep_order(
        [safe_to_string(x) for x in (item.get("required_fields", []) or [])]
        + [safe_to_string(x) for x in (item.get("optional_fields", []) or [])]
        + [safe_to_string(x) for x in (item.get("fields", []) or [])]
    )


def augment_items_by_names(
    current_items: List[Dict[str, Any]],
    source_items: List[Dict[str, Any]],
    desired_names: List[str],
    item_key: str,
    retrieval_mode: str,
) -> List[Dict[str, Any]]:
    existing_names = {
        safe_to_string(item.get(item_key))
        for item in current_items
        if safe_to_string(item.get(item_key))
    }
    lookup = {
        safe_to_string(item.get(item_key)): item
        for item in source_items
        if safe_to_string(item.get(item_key))
    }
    augmented = list(current_items)
    for name in desired_names:
        if not name or name in existing_names or name not in lookup:
            continue
        copied = dict(lookup[name])
        copied["_score"] = copied.get("_score", STRUCTURAL_BOOST_SCORE - 1)
        copied["_keyword_score"] = copied.get("_keyword_score", 0)
        copied["_embedding_score"] = copied.get("_embedding_score", 0.0)
        copied["_retrieval_mode"] = retrieval_mode
        augmented.append(copied)
        existing_names.add(name)
    return augmented


def augment_fields_by_refs(
    current_items: List[Dict[str, Any]],
    source_items: List[Dict[str, Any]],
    desired_refs: List[str],
    retrieval_mode: str,
) -> List[Dict[str, Any]]:
    existing_refs = {
        f"{safe_to_string(item.get('table_name'))}.{safe_to_string(item.get('field_name') or item.get('column_name'))}"
        for item in current_items
        if safe_to_string(item.get("table_name")) and safe_to_string(item.get("field_name") or item.get("column_name"))
    }
    lookup = {}
    for item in source_items:
        table_name = safe_to_string(item.get("table_name"))
        field_name = safe_to_string(item.get("field_name") or item.get("column_name"))
        if table_name and field_name:
            lookup[f"{table_name}.{field_name}"] = item

    augmented = list(current_items)
    for ref in desired_refs:
        if not ref or ref in existing_refs or ref not in lookup:
            continue
        copied = dict(lookup[ref])
        copied["_score"] = copied.get("_score", STRUCTURAL_BOOST_SCORE - 1)
        copied["_keyword_score"] = copied.get("_keyword_score", 0)
        copied["_embedding_score"] = copied.get("_embedding_score", 0.0)
        copied["_retrieval_mode"] = retrieval_mode
        augmented.append(copied)
        existing_refs.add(ref)
    return augmented


def extract_sql_metadata(sql: str) -> Tuple[List[str], List[str]]:
    sql_text = safe_to_string(sql)
    alias_map: Dict[str, str] = {}
    table_names: List[str] = []

    for table_name, alias in re.findall(
        r"\b(?:from|join)\s+((?:dim|fact)_[A-Za-z0-9_]+)(?:\s+(?:as\s+)?([A-Za-z_][A-Za-z0-9_]*))?",
        sql_text,
        flags=re.IGNORECASE,
    ):
        table_names.append(table_name)
        alias_name = safe_to_string(alias)
        if alias_name:
            alias_map[alias_name] = table_name

    field_refs = extract_table_field_refs(sql_text)
    for alias_name, field_name in re.findall(
        r"\b([A-Za-z_][A-Za-z0-9_]*)\.([A-Za-z_][A-Za-z0-9_]*)\b",
        sql_text,
    ):
        mapped_table = alias_map.get(alias_name)
        if mapped_table:
            field_refs.append(f"{mapped_table}.{field_name}")

    return unique_keep_order(table_names), unique_keep_order(field_refs)


def sync_result_metadata_with_sql(result_json: Dict[str, Any], sql: str) -> Dict[str, Any]:
    synced = dict(result_json or {})
    sql_tables, sql_fields = extract_sql_metadata(sql)
    if sql_tables:
        synced["used_tables"] = sql_tables
    if sql_fields:
        synced["used_columns"] = sql_fields
    return synced


def limit_text(text: str, max_chars: int) -> str:
    text = safe_to_string(text).strip()
    if len(text) <= max_chars:
        return text
    return text[:max_chars] + " ..."


def df_preview_records(df: pd.DataFrame, limit: int = 10) -> List[Dict[str, Any]]:
    if df is None or df.empty:
        return []
    try:
        return df.head(limit).to_dict(orient="records")
    except Exception:
        return []


def to_ascii_label(text: str) -> str:
    """
    图表标题尽量使用 ASCII，避免无中文字体环境下 matplotlib 发出大量 glyph warning。
    """
    cleaned = safe_to_string(text).strip()
    if not cleaned:
        return ""
    ascii_text = cleaned.encode("ascii", errors="ignore").decode("ascii").strip()
    return ascii_text or "Chart"


def score_text(query: str, text: str) -> int:
    query_norm = normalize_text(query)
    text_norm = normalize_text(text)
    score = 0

    for token in tokenize(query):
        if token and token in text_norm:
            score += 3

    if query_norm and query_norm in text_norm:
        score += 10

    return score


def cosine_similarity(vec1: List[float], vec2: List[float]) -> float:
    if not vec1 or not vec2 or len(vec1) != len(vec2):
        return 0.0

    dot_product = 0.0
    norm_1 = 0.0
    norm_2 = 0.0
    for value_1, value_2 in zip(vec1, vec2):
        dot_product += value_1 * value_2
        norm_1 += value_1 * value_1
        norm_2 += value_2 * value_2

    if norm_1 <= 0 or norm_2 <= 0:
        return 0.0

    return dot_product / math.sqrt(norm_1 * norm_2)


def call_siliconflow_embedding(text: str) -> List[float] | None:
    api_key = os.environ.get("SILICONFLOW_API_KEY", "").strip()
    api_url = os.environ.get(
        "SILICONFLOW_EMBEDDING_API_URL",
        DEFAULT_SILICONFLOW_EMBEDDING_API_URL,
    ).strip()
    embedding_model = os.environ.get(
        "SILICONFLOW_EMBEDDING_MODEL",
        DEFAULT_SILICONFLOW_EMBEDDING_MODEL,
    ).strip()

    if not api_key:
        print("warning: embedding retrieval fallback to keyword-only, reason: missing SILICONFLOW_API_KEY")
        return None

    payload = {
        "model": embedding_model,
        "input": text,
    }
    req = request.Request(
        api_url,
        data=json.dumps(payload).encode("utf-8"),
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
        method="POST",
    )

    try:
        with request.urlopen(req, timeout=120) as resp:
            raw = resp.read().decode("utf-8")
    except error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="ignore")
        print(f"warning: embedding retrieval fallback to keyword-only, reason: HTTPError {exc.code}: {body}")
        return None
    except error.URLError as exc:
        print(f"warning: embedding retrieval fallback to keyword-only, reason: URLError: {exc}")
        return None

    try:
        result = json.loads(raw)
    except json.JSONDecodeError as exc:
        print(f"warning: embedding retrieval fallback to keyword-only, reason: JSON decode failed: {raw[:500]}")
        return None

    data = result.get("data")
    if not isinstance(data, list):
        print(f"warning: embedding retrieval fallback to keyword-only, reason: invalid response data: {result}")
        return None

    if not data:
        print("warning: embedding retrieval fallback to keyword-only, reason: empty embedding response")
        return None

    first_item = data[0]
    embedding = first_item.get("embedding") if isinstance(first_item, dict) else None
    if not isinstance(embedding, list):
        print(f"warning: embedding retrieval fallback to keyword-only, reason: invalid embedding item: {first_item}")
        return None

    return embedding


def strip_embedding_fields(item: Dict[str, Any]) -> Dict[str, Any]:
    cleaned = dict(item)
    cleaned.pop("_embedding", None)
    return cleaned


def load_visualization_rules() -> Dict[str, Any]:
    if VISUALIZATION_RULES_PATH.exists():
        return read_json(VISUALIZATION_RULES_PATH)

    return {
        "version": "fallback",
        "chart_rules": [
            {
                "rule_id": "time_trend_line",
                "intent_keywords": ["趋势", "月", "周", "按月", "月度", "每日", "每周"],
                "recommended_chart": "line",
                "x_role": "time",
                "y_role": "metric",
            },
            {
                "rule_id": "distribution_bar",
                "intent_keywords": ["分布", "排名", "对比", "占比", "头部", "拆解"],
                "recommended_chart": "bar",
                "x_role": "dimension",
                "y_role": "metric",
            },
        ],
    }


def load_knowledge_base() -> Dict[str, Any]:
    recipes = read_json(FULL_RECIPES_PATH)
    recipe_lookup = {}
    for item in recipes:
        if not isinstance(item, dict):
            continue
        recipe_id = safe_to_string(item.get("recipe_id"))
        if recipe_id:
            recipe_lookup[recipe_id] = item

    return {
        "legacy": {
            "table_cards": read_json(TABLE_CARDS_PATH),
            "column_cards": read_json(COLUMN_CARDS_PATH),
            "dq_rules": read_json(DQ_RULES_PATH),
            "grain_rules": read_json(GRAIN_RULES_PATH),
            "recipes": recipes,
            "recipe_lookup": recipe_lookup,
        },
        "retrieval_v2": {
            "table_index": read_json(TABLE_INDEX_PATH),
            "field_index": read_json(FIELD_INDEX_PATH),
            "metric_index": read_json(METRIC_INDEX_PATH),
            "join_index": read_json(JOIN_INDEX_PATH),
            "trap_index": read_json(TRAP_INDEX_PATH),
            "policy_index": read_json(POLICY_INDEX_PATH),
            "recipe_index": read_json(RECIPE_INDEX_PATH),
            "table_embedding_index": read_optional_json(TABLE_EMBEDDING_INDEX_PATH, []),
            "field_embedding_index": read_optional_json(FIELD_EMBEDDING_INDEX_PATH, []),
            "metric_embedding_index": read_optional_json(METRIC_EMBEDDING_INDEX_PATH, []),
            "recipe_embedding_index": read_optional_json(RECIPE_EMBEDDING_INDEX_PATH, []),
            "answer_memory_index": read_optional_json(ANSWER_MEMORY_INDEX_PATH, []),
            "answer_memory_embedding_index": read_optional_json(
                ANSWER_MEMORY_EMBEDDING_INDEX_PATH, []
            ),
        },
        "visualization": load_visualization_rules(),
    }


def check_required_files() -> None:
    if not DB_PATH.exists():
        raise FileNotFoundError(
            f"找不到数据库：{DB_PATH}\n请先运行：python3 src/00_load_data.py"
        )

    required_files = [
        TABLE_CARDS_PATH,
        COLUMN_CARDS_PATH,
        DQ_RULES_PATH,
        GRAIN_RULES_PATH,
        TABLE_INDEX_PATH,
        FIELD_INDEX_PATH,
        METRIC_INDEX_PATH,
        JOIN_INDEX_PATH,
        TRAP_INDEX_PATH,
        POLICY_INDEX_PATH,
        RECIPE_INDEX_PATH,
        FULL_RECIPES_PATH,
    ]

    for file_path in required_files:
        if not file_path.exists():
            raise FileNotFoundError(
                f"找不到知识库文件：{file_path}\n"
                "请先运行：python3 src/03_build_knowledge_base.py\n"
                "以及：python3 src/03_6_build_retrieval_indexes_from_cards_v2.py"
            )


def infer_needs_visualization(question: str) -> bool:
    hints = [
        "趋势", "分布", "对比", "变化", "走势", "占比",
        "排名", "头部", "图表", "可视化", "拆解",
    ]
    lower = normalize_text(question)
    return any(hint in lower for hint in hints)


def infer_result_shape(question: str) -> str:
    lower = normalize_text(question)
    if any(x in lower for x in ["趋势", "每月", "月度", "每日", "每周"]):
        return "time_series"
    if any(x in lower for x in ["分布", "占比", "排名", "对比", "头部", "拆解"]):
        return "categorical_comparison"
    return "tabular_answer"


def agent1_task_understanding(question: str) -> Dict[str, Any]:
    return {
        "raw_question": question,
        "normalized_question": normalize_text(question),
        "tokens": tokenize(question),
        "needs_visualization": infer_needs_visualization(question),
        "result_shape": infer_result_shape(question),
        "time_hints": re.findall(r"\b20\d{2}(?:[-/]\d{2})?(?:[-/]\d{2})?\b", question),
    }


def format_recipe_for_search(recipe: Dict[str, Any]) -> str:
    return " ".join(
        [
            safe_to_string(recipe.get("recipe_id")),
            safe_to_string(recipe.get("name")),
            safe_to_string(recipe.get("title")),
            safe_to_string(recipe.get("intent")),
            safe_to_string(recipe.get("canonical_question")),
            " ".join(recipe.get("typical_questions", [])),
            safe_to_string(recipe.get("description")),
            safe_to_string(recipe.get("analysis_type")),
            " ".join(recipe.get("main_tables", [])),
            " ".join(recipe.get("required_tables", [])),
            " ".join(recipe.get("optional_tables", [])),
            " ".join(recipe.get("required_fields", [])),
            " ".join(recipe.get("optional_fields", [])),
            " ".join(recipe.get("metrics", [])),
            " ".join(recipe.get("dimensions", [])),
            " ".join(recipe.get("join_paths", [])),
            safe_to_string(recipe.get("grain")),
            " ".join(recipe.get("risks", [])),
            safe_to_string(recipe.get("source")),
            " ".join(recipe.get("keywords", [])),
        ]
    )


def format_answer_memory_for_search(item: Dict[str, Any]) -> str:
    return " ".join(
        [
            safe_to_string(item.get("source_question")),
            safe_to_string(item.get("answerable_description")),
            " ".join(item.get("question_patterns", []) or []),
            " ".join(item.get("used_tables", []) or []),
            " ".join(item.get("used_columns", []) or []),
            " ".join(item.get("result_columns", []) or []),
            safe_to_string(item.get("result_structure")),
            safe_to_string(item.get("visualization_hint")),
            " ".join(item.get("limitations", []) or []),
            " ".join(item.get("keywords", []) or []),
        ]
    )


def enrich_recipe_hits(
    recipe_hits: List[Dict[str, Any]],
    recipe_lookup: Dict[str, Dict[str, Any]],
) -> List[Dict[str, Any]]:
    enriched = []
    for hit in recipe_hits:
        recipe_id = safe_to_string(hit.get("recipe_id"))
        full_recipe = recipe_lookup.get(recipe_id, {})
        merged = dict(full_recipe) if isinstance(full_recipe, dict) else {}
        merged.update(
            {
                "recipe_id": recipe_id,
                "recipe_index_hit": hit,
                "_score": hit.get("_score", 0),
            }
        )
        if "name" not in merged or not merged.get("name"):
            merged["name"] = safe_to_string(hit.get("name") or merged.get("title"))
        if "description" not in merged or not merged.get("description"):
            merged["description"] = safe_to_string(
                hit.get("description") or merged.get("insight")
            )
        if "required_tables" not in merged or not merged.get("required_tables"):
            merged["required_tables"] = hit.get("required_tables", [])
        enriched.append(merged)
    return enriched


def retrieve_ranked_items(
    question: str,
    items: List[Dict[str, Any]],
    text_builder,
    limit: int,
    boost_tables: set[str] | None = None,
    table_field: str = "table_name",
) -> List[Dict[str, Any]]:
    scored = []
    boost_tables = boost_tables or set()

    for item in items:
        score = score_text(question, text_builder(item))
        table_name = safe_to_string(item.get(table_field))
        if table_name and table_name in boost_tables:
            score += 6
        if score > 0:
            enriched = dict(item)
            enriched["_score"] = score
            scored.append(enriched)

    scored.sort(key=lambda x: x["_score"], reverse=True)
    return scored[:limit]


def retrieve_hybrid_ranked_items(
    question: str,
    items: List[Dict[str, Any]],
    text_builder,
    limit: int,
    question_embedding: List[float] | None = None,
    boost_tables: set[str] | None = None,
    table_field: str = "table_name",
    keyword_weight: float = 1.0,
    embedding_weight: float = 20.0,
    min_embedding_score: float = 0.20,
) -> List[Dict[str, Any]]:
    scored = []
    boost_tables = boost_tables or set()

    for item in items:
        keyword_score = float(score_text(question, text_builder(item)))
        embedding_score = 0.0

        raw_embedding = item.get("_embedding")
        if question_embedding and isinstance(raw_embedding, list):
            embedding_score = cosine_similarity(question_embedding, raw_embedding)

        if not (keyword_score > 0 or embedding_score >= min_embedding_score):
            continue

        final_score = keyword_score * keyword_weight + embedding_score * embedding_weight
        table_name = safe_to_string(item.get(table_field))
        if table_name and table_name in boost_tables:
            final_score += STRUCTURAL_BOOST_SCORE

        enriched = strip_embedding_fields(item)
        enriched["_score"] = round(final_score, 6)
        enriched["_keyword_score"] = int(keyword_score)
        enriched["_embedding_score"] = round(embedding_score, 6)
        enriched["_retrieval_mode"] = (
            "hybrid"
            if question_embedding is not None and isinstance(raw_embedding, list)
            else "keyword_fallback"
        )
        scored.append(enriched)

    scored.sort(
        key=lambda item: (
            item.get("_score", 0),
            item.get("_keyword_score", 0),
        ),
        reverse=True,
    )
    return scored[:limit]


def build_default_table_candidates(table_index: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    defaults = {
        "dim_tenant",
        "dim_user",
        "dim_plan",
        "fact_subscription",
        "fact_actual_revenue",
        "fact_daily_usage",
        "fact_feature_usage",
        "fact_ai_usage_log",
    }
    result = []
    for item in table_index:
        if item.get("table_name") in defaults:
            copied = dict(item)
            copied["_score"] = 1
            copied["_keyword_score"] = 1
            copied["_embedding_score"] = 0.0
            copied["_retrieval_mode"] = "keyword_fallback"
            result.append(copied)
    return result[:MAX_CONTEXT_TABLES]


def choose_retrieval_items(
    base_items: List[Dict[str, Any]],
    embedding_items: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    if embedding_items and len(embedding_items) == len(base_items):
        return embedding_items
    return base_items


def agent2_knowledge_retriever(question: str, kb: Dict[str, Any], task: Dict[str, Any]) -> Dict[str, Any]:
    retrieval = kb["retrieval_v2"]
    legacy = kb["legacy"]
    table_items = choose_retrieval_items(retrieval["table_index"], retrieval["table_embedding_index"])
    field_items = choose_retrieval_items(retrieval["field_index"], retrieval["field_embedding_index"])
    metric_items = choose_retrieval_items(retrieval["metric_index"], retrieval["metric_embedding_index"])
    recipe_items = choose_retrieval_items(retrieval["recipe_index"], retrieval["recipe_embedding_index"])
    answer_memory_items = (
        retrieval["answer_memory_embedding_index"] or retrieval["answer_memory_index"]
    )

    embedding_available = any([
        retrieval.get("table_embedding_index"),
        retrieval.get("field_embedding_index"),
        retrieval.get("metric_embedding_index"),
        retrieval.get("recipe_embedding_index"),
        retrieval.get("answer_memory_embedding_index"),
    ])

    question_embedding = None
    if embedding_available:
        question_embedding = call_siliconflow_embedding(question)

    tables = retrieve_hybrid_ranked_items(
        question=question,
        items=table_items,
        text_builder=lambda x: " ".join(
            [
                safe_to_string(x.get("table_name")),
                safe_to_string(x.get("business_meaning")),
                safe_to_string(x.get("grain_summary")),
                " ".join(x.get("metric_names", [])),
                " ".join(x.get("join_targets", [])),
                " ".join(x.get("keywords", [])),
            ]
        ),
        limit=MAX_CONTEXT_TABLES,
        question_embedding=question_embedding,
        keyword_weight=1.0,
        embedding_weight=20.0,
        min_embedding_score=0.20,
    )
    if not tables:
        tables = build_default_table_candidates(retrieval["table_index"])

    selected_table_names = {safe_to_string(item.get("table_name")) for item in tables}

    fields = retrieve_hybrid_ranked_items(
        question=question,
        items=field_items,
        text_builder=lambda x: " ".join(
            [
                safe_to_string(x.get("table_name")),
                safe_to_string(x.get("field_name") or x.get("column_name")),
                safe_to_string(x.get("semantic_type")),
                safe_to_string(x.get("business_meaning")),
                safe_to_string(x.get("references")),
                " ".join(x.get("keywords", [])),
            ]
        ),
        limit=MAX_CONTEXT_FIELDS,
        question_embedding=question_embedding,
        boost_tables=selected_table_names,
        table_field="table_name",
        keyword_weight=1.0,
        embedding_weight=20.0,
        min_embedding_score=0.20,
    )

    metrics = retrieve_hybrid_ranked_items(
        question=question,
        items=metric_items,
        text_builder=lambda x: " ".join(
            [
                safe_to_string(x.get("table_name")),
                safe_to_string(x.get("metric_name")),
                safe_to_string(x.get("expression")),
                safe_to_string(x.get("meaning")),
                safe_to_string(x.get("note")),
                " ".join(x.get("keywords", [])),
            ]
        ),
        limit=MAX_CONTEXT_METRICS,
        question_embedding=question_embedding,
        boost_tables=selected_table_names,
        table_field="table_name",
        keyword_weight=1.0,
        embedding_weight=20.0,
        min_embedding_score=0.20,
    )

    joins = retrieve_ranked_items(
        question=question,
        items=retrieval["join_index"],
        text_builder=lambda x: " ".join(
            [
                safe_to_string(x.get("source_table")),
                safe_to_string(x.get("source_field")),
                safe_to_string(x.get("target_table")),
                safe_to_string(x.get("target_field")),
                safe_to_string(x.get("join_condition")),
                safe_to_string(x.get("note")),
                " ".join(x.get("keywords", [])),
            ]
        ),
        limit=MAX_CONTEXT_JOINS,
        boost_tables=selected_table_names,
        table_field="source_table",
    )

    traps = retrieve_ranked_items(
        question=question,
        items=retrieval["trap_index"],
        text_builder=lambda x: " ".join(
            [
                safe_to_string(x.get("table_name")),
                safe_to_string(x.get("trap")),
                safe_to_string(x.get("consequence")),
                safe_to_string(x.get("prevention")),
                " ".join(x.get("keywords", [])),
            ]
        ),
        limit=MAX_CONTEXT_TRAPS,
        boost_tables=selected_table_names,
    )

    policies = retrieve_ranked_items(
        question=question,
        items=retrieval["policy_index"],
        text_builder=lambda x: " ".join(
            [
                safe_to_string(x.get("table_name")),
                safe_to_string(x.get("policy_flag")),
                safe_to_string(x.get("policy_value_text")),
                " ".join(x.get("keywords", [])),
            ]
        ),
        limit=MAX_CONTEXT_POLICIES,
        boost_tables=selected_table_names,
    )

    recipe_index_hits = retrieve_hybrid_ranked_items(
        question=question,
        items=recipe_items,
        text_builder=format_recipe_for_search,
        limit=MAX_CONTEXT_RECIPES,
        question_embedding=question_embedding,
        keyword_weight=1.0,
        embedding_weight=20.0,
        min_embedding_score=0.20,
    )
    full_recipes = enrich_recipe_hits(recipe_index_hits, legacy["recipe_lookup"])

    structural_table_names = set(selected_table_names)
    structural_field_refs = set()
    for item in joins:
        structural_table_names.update(extract_join_item_tables(item))
        structural_field_refs.update(extract_join_item_fields(item))
    for item in recipe_index_hits:
        structural_table_names.update(extract_recipe_tables(item))
        structural_field_refs.update(extract_recipe_fields(item))
    for item in full_recipes:
        structural_table_names.update(extract_recipe_tables(item))
        structural_field_refs.update(extract_recipe_fields(item))

    tables = augment_items_by_names(
        current_items=tables,
        source_items=retrieval["table_index"],
        desired_names=sorted(structural_table_names),
        item_key="table_name",
        retrieval_mode="structural_context",
    )
    selected_table_names = {safe_to_string(item.get("table_name")) for item in tables}
    fields = augment_fields_by_refs(
        current_items=fields,
        source_items=retrieval["field_index"],
        desired_refs=sorted(structural_field_refs),
        retrieval_mode="structural_context",
    )

    candidate_answer_memories: List[Dict[str, Any]] = []
    try:
        candidate_answer_memories = retrieve_hybrid_ranked_items(
            question=question,
            items=answer_memory_items,
            text_builder=format_answer_memory_for_search,
            limit=MAX_CONTEXT_ANSWER_MEMORIES,
            question_embedding=question_embedding,
            keyword_weight=1.0,
            embedding_weight=20.0,
            min_embedding_score=0.20,
        )
    except Exception as exc:
        print(f"warning: answer memory retrieval skipped, reason: {exc}")
        candidate_answer_memories = []

    return {
        "task": task,
        "candidate_tables": tables,
        "candidate_fields": fields,
        "candidate_metrics": metrics,
        "candidate_joins": joins,
        "candidate_traps": traps,
        "candidate_policies": policies,
        "candidate_recipe_hits": recipe_index_hits,
        "candidate_recipes": full_recipes,
        "candidate_answer_memories": candidate_answer_memories[:MAX_CONTEXT_ANSWER_MEMORIES],
        "answer_memory_retrieved_count": len(candidate_answer_memories[:MAX_CONTEXT_ANSWER_MEMORIES]),
        "selected_table_names": sorted(selected_table_names),
        "embedding_available": embedding_available,
        "embedding_used": question_embedding is not None,
    }


def agent3_grain_dq_guard(retrieval_context: Dict[str, Any]) -> Dict[str, Any]:
    tables = retrieval_context["candidate_tables"]
    traps = retrieval_context["candidate_traps"]
    joins = retrieval_context["candidate_joins"]
    policies = retrieval_context["candidate_policies"]

    guardrails = []
    warnings = []

    for table in tables:
        if safe_to_string(table.get("warning_level")) in {"high", "medium"}:
            warnings.append(
                f"{table.get('table_name')} warning_level={table.get('warning_level')}"
            )
        if table.get("grain_summary"):
            guardrails.append(
                f"{table.get('table_name')} grain: {safe_to_string(table.get('grain_summary'))}"
            )

    for trap in traps[:8]:
        warnings.append(
            f"{trap.get('table_name')}: {trap.get('trap')} -> {trap.get('prevention')}"
        )

    for join in joins[:10]:
        join_text = (
            f"{join.get('source_table')}.{join.get('source_field')} = "
            f"{join.get('target_table')}.{join.get('target_field')}"
        )
        guardrails.append(
            f"推荐 JOIN: {join_text}; relation={join.get('relationship')}; risk={join.get('risk_level')}"
        )

    for policy in policies[:8]:
        guardrails.append(
            f"policy {policy.get('table_name')}.{policy.get('policy_flag')}={policy.get('policy_value_text')}"
        )

    guardrails.append("统计 tenant/user/subscription 等实体数时，先确认 grain，再决定 COUNT(*) 还是 COUNT(DISTINCT ...)")
    guardrails.append("如跨事实表 JOIN，优先先聚合再 JOIN，避免乘法膨胀")

    return {
        "guardrails": guardrails[:20],
        "warnings": warnings[:20],
    }


def compact_table_context(items: List[Dict[str, Any]]) -> str:
    lines = []
    for item in items:
        lines.append(
            f"- {item.get('table_name')}: grain={safe_to_string(item.get('grain_summary'))}; "
            f"meaning={safe_to_string(item.get('business_meaning'))}; "
            f"metrics={', '.join(item.get('metric_names', [])[:6])}; "
            f"join_targets={', '.join(item.get('join_targets', [])[:6])}; "
            f"warning_level={safe_to_string(item.get('warning_level'))}"
        )
    return "\n".join(lines)


def compact_field_context(items: List[Dict[str, Any]]) -> str:
    lines = []
    for item in items:
        lines.append(
            f"- {item.get('table_name')}.{item.get('field_name')}: "
            f"type={item.get('semantic_type')}; "
            f"meaning={safe_to_string(item.get('business_meaning'))}; "
            f"ref={safe_to_string(item.get('references'))}"
        )
    return "\n".join(lines)


def compact_metric_context(items: List[Dict[str, Any]]) -> str:
    lines = []
    for item in items:
        lines.append(
            f"- {item.get('table_name')}.{item.get('metric_name')}: "
            f"{safe_to_string(item.get('expression'))}; "
            f"meaning={safe_to_string(item.get('meaning'))}; "
            f"note={safe_to_string(item.get('note'))}"
        )
    return "\n".join(lines)


def compact_join_context(items: List[Dict[str, Any]]) -> str:
    lines = []
    for item in items:
        path_steps = [safe_to_string(step) for step in (item.get("path", []) or []) if safe_to_string(step)]
        join_text = safe_to_string(item.get("join_condition"))
        if not join_text and path_steps:
            join_text = " -> ".join(path_steps)
        tables_text = ", ".join(extract_join_item_tables(item))
        lines.append(
            f"- {join_text} "
            f"(relationship={item.get('relationship')}, risk={item.get('risk_level')}, tables={tables_text}) "
            f"note={safe_to_string(item.get('note'))}"
        )
    return "\n".join(lines)


def compact_trap_context(items: List[Dict[str, Any]]) -> str:
    lines = []
    for item in items:
        lines.append(
            f"- {item.get('table_name')}: trap={safe_to_string(item.get('trap'))}; "
            f"prevention={safe_to_string(item.get('prevention'))}"
        )
    return "\n".join(lines)


def compact_policy_context(items: List[Dict[str, Any]]) -> str:
    lines = []
    for item in items:
        lines.append(
            f"- {item.get('table_name')}.{item.get('policy_flag')}={item.get('policy_value_text')}"
        )
    return "\n".join(lines)


def compact_recipe_context(items: List[Dict[str, Any]]) -> str:
    lines = []
    for item in items:
        sql = safe_to_string(item.get("sql_skeleton") or item.get("query_sql"))
        insight = safe_to_string(item.get("description") or item.get("insight"))
        if len(sql) > 800:
            sql = sql[:800] + "\n... truncated ..."
        if len(insight) > 400:
            insight = insight[:400] + "\n... truncated ..."
        lines.append(
            f"- recipe_id={item.get('recipe_id')}; "
            f"name={safe_to_string(item.get('name') or item.get('title'))}; "
            f"tables={', '.join(item.get('required_tables', []) or item.get('main_tables', []))}\n"
            f"  sql={sql}\n"
            f"  notes={insight}"
        )
    return "\n".join(lines)


def compact_answer_memory_context(items: List[Dict[str, Any]]) -> str:
    if not items:
        return "无"

    lines = []
    for item in items[:MAX_CONTEXT_ANSWER_MEMORIES]:
        limitations = "；".join(
            limit_text(x, 120) for x in (item.get("limitations", []) or [])[:3]
        )
        lines.append(
            "\n".join(
                [
                    f"- memory_id={safe_to_string(item.get('memory_id'))}",
                    f"  历史问题：{limit_text(item.get('source_question'), 120)}",
                    f"  该历史 SQL 可展示：{limit_text(item.get('answerable_description'), 260)}",
                    f"  使用表：{', '.join((item.get('used_tables', []) or [])[:8])}",
                    f"  使用字段：{', '.join((item.get('used_columns', []) or [])[:10])}",
                    f"  结果列：{', '.join((item.get('result_columns', []) or [])[:8])}",
                    f"  可视化提示：{limit_text(item.get('visualization_hint'), 120)}",
                    f"  限制：{limit_text(limitations, 200)}",
                ]
            )
        )
    return "\n".join(lines)


def build_analysis_context(result: Dict[str, Any]) -> Dict[str, Any]:
    result = result or {}
    result_json = result.get("result_json", {}) or {}
    df = result.get("dataframe")

    previous_result_columns: List[str] = []
    previous_result_preview: List[Dict[str, Any]] = []
    if df is not None:
        try:
            previous_result_columns = [str(col) for col in df.columns]
            previous_result_preview = df.head(10).to_dict(orient="records")
        except Exception:
            previous_result_columns = []
            previous_result_preview = []

    return {
        "previous_question": safe_to_string(result.get("question")),
        "previous_analysis_plan": safe_to_string(result_json.get("analysis_plan")),
        "previous_sql": safe_to_string(result.get("executed_sql")),
        "previous_used_tables": list(result_json.get("used_tables", []) or []),
        "previous_used_columns": list(result_json.get("used_columns", []) or []),
        "previous_result_columns": previous_result_columns,
        "previous_result_preview": previous_result_preview,
        "previous_visualization_spec": result.get("visualization_spec", {}) or {},
        "previous_lineage": result.get("lineage", {}) or {},
        "previous_log_path": safe_to_string(result.get("log_path")),
    }


def compact_previous_context(previous_context: Dict[str, Any] | None) -> str:
    if not previous_context:
        return ""

    preview_rows = previous_context.get("previous_result_preview", []) or []
    preview_rows = preview_rows[:10]
    lineage = previous_context.get("previous_lineage", {}) or {}
    lineage_summary = lineage.get("lineage_summary", []) if isinstance(lineage, dict) else []

    sections = [
        f"上一轮问题：{safe_to_string(previous_context.get('previous_question'))}",
        f"上一轮分析计划：{safe_to_string(previous_context.get('previous_analysis_plan'))}",
        f"上一轮 SQL：\n{safe_to_string(previous_context.get('previous_sql'))}",
        f"上一轮使用表：{', '.join(previous_context.get('previous_used_tables', []) or [])}",
        f"上一轮使用字段：{', '.join(previous_context.get('previous_used_columns', []) or [])}",
        f"上一轮结果列：{', '.join(previous_context.get('previous_result_columns', []) or [])}",
        f"上一轮结果预览（最多10行）：\n{json.dumps(make_json_serializable(preview_rows), ensure_ascii=False, indent=2)}",
        f"上一轮图表配置：\n{json.dumps(make_json_serializable(previous_context.get('previous_visualization_spec', {}) or {}), ensure_ascii=False, indent=2)}",
    ]
    if lineage_summary:
        sections.append("上一轮 lineage 摘要：\n" + "\n".join(f"- {x}" for x in lineage_summary[:6]))
    return "\n\n".join(sections).strip()


def infer_followup_edit_type(question: str) -> List[str]:
    question_norm = normalize_text(question)
    edit_types: List[str] = []

    filter_keywords = ["只看", "过滤", "限定", "仅看", "保留", "排除"]
    topn_keywords = ["前 ", "前10", "前20", "top", "top ", "limit"]
    sort_keywords = ["升序", "降序", "从高到低", "从低到高", "排序"]
    chart_keywords = ["折线图", "柱状图", "表格", "换成图", "换成折线", "换成柱状", "图表"]
    dimension_keywords = ["按", "拆", "拆分", "细分", "分组", "再按"]
    time_keywords = ["月份", "月", "日期", "时间", "最近", "天", "周", "2025", "2026", "这个月"]

    if any(x in question_norm for x in filter_keywords):
        edit_types.append("filter_condition")
    if any(x in question_norm for x in topn_keywords):
        edit_types.append("topn_change")
    if any(x in question_norm for x in sort_keywords):
        edit_types.append("sort_change")
    if any(x in question_norm for x in chart_keywords):
        edit_types.append("chart_change")
    if any(x in question_norm for x in dimension_keywords):
        edit_types.append("dimension_split")
    if any(x in question_norm for x in time_keywords):
        edit_types.append("time_filter_or_time_dimension")

    return unique_keep_order(edit_types)


def build_sql_generation_prompt(
    question: str,
    task: Dict[str, Any],
    retrieval_context: Dict[str, Any],
    guard: Dict[str, Any],
    previous_context: Dict[str, Any] | None = None,
) -> str:
    previous_context_block = ""
    if previous_context:
        previous_context_block = f"""
上一轮分析上下文（仅供参考）：
{compact_previous_context(previous_context)}

当前问题如果是在上一轮基础上的追问，请优先复用上一轮已验证的表、字段、SQL 口径与图表配置，再按本轮需求做最小修改。
识别到的可能修改类型：
{", ".join(infer_followup_edit_type(question)) or "未识别到明确修改类型"}

追问处理规则：
1. `filter_condition`：优先增加或调整 WHERE 条件。
2. `topn_change`：优先调整 LIMIT。
3. `sort_change`：优先调整 ORDER BY。
4. `chart_change`：SQL 可以不变，优先调整 visualization_recommendation。
5. `dimension_split`：增加或调整 GROUP BY 维度，并确认字段存在。
6. `time_filter_or_time_dimension`：增加时间过滤或时间分组，并确认时间字段存在。

如果当前问题其实是新问题，或上一轮上下文与当前问题冲突，请忽略上一轮上下文，按当前问题重新分析。
不要因为上一轮上下文而编造表、字段、JOIN 或指标。
""".strip()

    return f"""
你是 Agent4：SQL 规划与生成专家，服务于一个中文数据分析项目。

上游流程：
- Agent1 已完成用户问题理解。
- Agent2 已基于 retrieval_v2 检索出表、字段、指标、JOIN、recipe 等知识。
- Agent3 已给出 grain 与数据质量防护约束。

你的任务：
为用户问题生成一条 DuckDB SQL。

硬性规则：
1. 只能使用 DuckDB SQL。
2. 只能使用上下文里明确给出的表、字段、指标、JOIN 路径和 recipe。
3. 不允许编造表名或字段名。
4. 选择 COUNT(*)、COUNT(DISTINCT ...)、SUM(...) 前必须先遵守 grain 和 trap 约束。
5. 如果一对多 JOIN 可能带来重复，优先先聚合再 JOIN，或谨慎使用 DISTINCT。
6. 如果使用 fact_ai_usage_log.created_at，要记住它是 unix epoch seconds。
7. 如果使用 fact_nps_survey.score，必要时使用 TRY_CAST，并安全处理 N/A。
8. `analysis_plan`、`warnings`、`visualization_recommendation.title`、`visualization_recommendation.reason` 必须全部使用中文。
9. 只返回 JSON，不要返回 markdown。
10. 当 fact_daily_usage 需要关联 dim_tenant 或使用 dim_tenant.industry / country / size_tier / name 时，必须先 JOIN dim_user，再 JOIN dim_tenant；禁止使用 fact_daily_usage.user_id = dim_tenant.user_id。

历史相似案例使用规则：
1. 历史相似案例仅供弱参考。
2. 不得直接复制历史 SQL。
3. 不得因为历史案例而使用当前 candidate_tables / candidate_fields / candidate_metrics / candidate_joins 中没有出现的表字段。
4. 如果历史案例与当前 join/trap/policy/guardrail 冲突，必须以当前正式 guardrail 为准。
5. 当前 SQL 必须优先遵守 candidate_tables、candidate_fields、candidate_metrics、candidate_joins、traps、policies 和 Agent3 guardrails。
6. 历史案例只用于帮助理解可能相关的分析表达、常见结果结构和相似问题处理方式。
7. answer_memory 不等于 validated recipe，不能作为唯一依据。
8. 第一版只允许使用 top1 历史案例，避免多个自动 memory 对 SQL Planner 产生过强影响。

返回 JSON 格式：
{{
  "analysis_plan": "中文分析计划",
  "sql": "DuckDB SQL query",
  "used_tables": ["table1", "table2"],
  "used_columns": ["table.column"],
  "warnings": ["中文风险提示"],
  "visualization_recommendation": {{
    "needed": true,
    "chart_type": "line|bar|table_only",
    "x": "column name or empty",
    "y": "column name or empty",
    "series": "column name or empty",
    "title": "中文图表标题",
    "reason": "中文原因说明"
  }}
}}

Agent1 task understanding:
- normalized_question: {task.get("normalized_question")}
- tokens: {task.get("tokens")}
- needs_visualization: {task.get("needs_visualization")}
- result_shape: {task.get("result_shape")}
- time_hints: {task.get("time_hints")}
- previous_context_available: {task.get("previous_context_available")}
- followup_edit_types: {task.get("followup_edit_types")}

用户问题：
{question}

{previous_context_block}

候选表：
{compact_table_context(retrieval_context["candidate_tables"])}

候选字段：
{compact_field_context(retrieval_context["candidate_fields"])}

候选指标：
{compact_metric_context(retrieval_context["candidate_metrics"])}

推荐 JOIN：
{compact_join_context(retrieval_context["candidate_joins"])}

已知陷阱：
{compact_trap_context(retrieval_context["candidate_traps"])}

策略标记：
{compact_policy_context(retrieval_context["candidate_policies"])}

相似 recipes：
{compact_recipe_context(retrieval_context["candidate_recipes"])}

历史相似案例（仅供弱参考，不可覆盖正式知识库）：
{compact_answer_memory_context(retrieval_context.get("candidate_answer_memories", []))}

Agent3 守卫规则：
{chr(10).join(f"- {x}" for x in guard["guardrails"])}

Agent3 风险提示：
{chr(10).join(f"- {x}" for x in guard["warnings"])}
""".strip()


def build_sql_repair_prompt(
    question: str,
    task: Dict[str, Any],
    retrieval_context: Dict[str, Any],
    guard: Dict[str, Any],
    bad_sql: str,
    error_message: str,
    previous_context: Dict[str, Any] | None = None,
) -> str:
    base_prompt = build_sql_generation_prompt(
        question,
        task,
        retrieval_context,
        guard,
        previous_context=previous_context,
    )
    return f"""
{base_prompt}

上一条 SQL 执行失败，或执行结果未通过校验。

失败 SQL：
{bad_sql}

报错信息：
{error_message}

额外修复提醒：
- dim_tenant 没有 user_id。用户行为事实表关联租户维度时必须通过 dim_user：fact_xxx.user_id = dim_user.user_id，再 dim_user.tenant_id = dim_tenant.tenant_id。

请修复 SQL，并保持相同 JSON 格式返回。
""".strip()


def call_siliconflow(prompt: str) -> str:
    api_key = os.environ.get("SILICONFLOW_API_KEY", "").strip()
    api_url = os.environ.get("SILICONFLOW_API_URL", DEFAULT_SILICONFLOW_API_URL).strip()
    model = os.environ.get("SILICONFLOW_MODEL", DEFAULT_MODEL).strip()

    if not api_key:
        raise RuntimeError("未找到 SILICONFLOW_API_KEY，请在 .env 中配置。")

    payload = {
        "model": model,
        "messages": [
            {
                "role": "system",
                "content": (
                    "你是一个严谨的 DuckDB SQL 生成助手。"
                    "你必须只返回合法 JSON，且所有自然语言内容必须使用中文。"
                ),
            },
            {
                "role": "user",
                "content": prompt,
            },
        ],
        "temperature": 0.1,
        "max_tokens": 2600,
    }

    req = request.Request(
        api_url,
        data=json.dumps(payload).encode("utf-8"),
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
        method="POST",
    )

    try:
        with request.urlopen(req, timeout=120) as resp:
            raw = resp.read().decode("utf-8")
    except error.HTTPError as e:
        body = e.read().decode("utf-8", errors="ignore")
        raise RuntimeError(f"SiliconFlow HTTPError {e.code}: {body}") from e
    except error.URLError as e:
        raise RuntimeError(f"SiliconFlow URLError: {e}") from e

    result = json.loads(raw)
    return result["choices"][0]["message"]["content"]


def extract_json_from_llm_output(text: str) -> Dict[str, Any]:
    text = text.strip()
    if text.startswith("```"):
        text = re.sub(r"^```[a-zA-Z]*\s*", "", text)
        text = re.sub(r"\s*```$", "", text)
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        match = re.search(r"\{.*\}", text, flags=re.DOTALL)
        if not match:
            raise ValueError(f"模型输出不是 JSON：{text}")
        return json.loads(match.group(0))


def clean_generated_sql(sql: str) -> str:
    sql = sql.strip()
    sql = re.sub(r"^```sql\s*", "", sql, flags=re.IGNORECASE)
    sql = re.sub(r"^```\s*", "", sql)
    sql = re.sub(r"\s*```$", "", sql)
    return sql.strip()


def is_safe_select_sql(sql: str) -> bool:
    cleaned = sql.strip().lower()
    if not (cleaned.startswith("select") or cleaned.startswith("with")):
        return False
    forbidden_keywords = [
        " drop ", " delete ", " update ", " insert ", " alter ",
        " create ", " truncate ", " attach ", " copy ", " vacuum ",
    ]
    padded = " " + cleaned + " "
    return not any(keyword in padded for keyword in forbidden_keywords)


def add_limit_if_needed(sql: str, limit: int = MAX_RESULT_ROWS) -> str:
    sql_clean = sql.rstrip().rstrip(";")
    if re.search(r"\blimit\s+\d+\b", sql_clean, flags=re.IGNORECASE):
        return sql_clean + ";"
    return sql_clean + f"\nLIMIT {limit};"


def execute_sql(conn: duckdb.DuckDBPyConnection, sql: str) -> Tuple[bool, pd.DataFrame, str, str]:
    try:
        cleaned_sql = clean_generated_sql(sql)
        if not is_safe_select_sql(cleaned_sql):
            return False, pd.DataFrame(), "只允许执行 SELECT 或 WITH 查询。", cleaned_sql
        executed_sql = add_limit_if_needed(cleaned_sql)
        df = conn.execute(executed_sql).df()
        return True, df, "", executed_sql
    except Exception as e:
        return False, pd.DataFrame(), str(e), clean_generated_sql(sql)


def get_table_schema_lookup(
    conn: duckdb.DuckDBPyConnection,
    table_names: List[str],
) -> Dict[str, set[str]]:
    schema_lookup: Dict[str, set[str]] = {}
    for table_name in unique_keep_order([safe_to_string(x) for x in table_names]):
        if not table_name:
            continue
        try:
            columns = conn.execute(f'DESCRIBE "{table_name}"').fetchall()
        except Exception:
            continue
        schema_lookup[table_name] = {safe_to_string(row[0]) for row in columns if row}
    return schema_lookup


def merge_warning_lists(*warning_groups: List[str]) -> List[str]:
    merged: List[str] = []
    seen = set()
    for group in warning_groups:
        for item in group:
            text = safe_to_string(item).strip()
            if text and text not in seen:
                seen.add(text)
                merged.append(text)
    return merged


def validate_sql_references(
    conn: duckdb.DuckDBPyConnection,
    result_json: Dict[str, Any],
    retrieval_context: Dict[str, Any],
) -> Tuple[bool, List[str], str]:
    warnings: List[str] = []
    used_tables = [
        safe_to_string(item)
        for item in (result_json.get("used_tables", []) or [])
        if safe_to_string(item)
    ]
    used_columns = [
        safe_to_string(item)
        for item in (result_json.get("used_columns", []) or [])
        if safe_to_string(item)
    ]

    if not used_tables:
        warnings.append("模型未明确给出 used_tables，结果可解释性较弱。")
        return True, warnings, ""

    schema_lookup = get_table_schema_lookup(conn, used_tables)
    candidate_tables = set(retrieval_context.get("selected_table_names", []) or [])
    candidate_fields = {
        f"{safe_to_string(item.get('table_name'))}.{safe_to_string(item.get('field_name') or item.get('column_name'))}"
        for item in retrieval_context.get("candidate_fields", [])
        if safe_to_string(item.get("table_name")) and safe_to_string(item.get("field_name") or item.get("column_name"))
    }
    join_allowed_tables = set()
    join_allowed_fields = set()
    for item in retrieval_context.get("candidate_joins", []):
        join_allowed_tables.update(extract_join_item_tables(item))
        join_allowed_fields.update(extract_join_item_fields(item))

    recipe_allowed_tables = set()
    recipe_allowed_fields = set()
    for item in retrieval_context.get("candidate_recipe_hits", []):
        for table_name in (item.get("required_tables", []) or []):
            recipe_allowed_tables.add(safe_to_string(table_name))
        for table_name in (item.get("optional_tables", []) or []):
            recipe_allowed_tables.add(safe_to_string(table_name))
        for field_name in (item.get("required_fields", []) or []):
            recipe_allowed_fields.add(safe_to_string(field_name))
        for field_name in (item.get("optional_fields", []) or []):
            recipe_allowed_fields.add(safe_to_string(field_name))
    for item in retrieval_context.get("candidate_recipes", []):
        for table_name in (item.get("required_tables", []) or item.get("main_tables", []) or []):
            recipe_allowed_tables.add(safe_to_string(table_name))
        for table_name in (item.get("optional_tables", []) or []):
            recipe_allowed_tables.add(safe_to_string(table_name))
        for field_name in (item.get("required_fields", []) or []):
            recipe_allowed_fields.add(safe_to_string(field_name))
        for field_name in (item.get("optional_fields", []) or []):
            recipe_allowed_fields.add(safe_to_string(field_name))

    fatal_issues: List[str] = []

    dim_tenant_user_join_fields = {
        "fact_daily_usage.user_id",
        "fact_feature_usage.user_id",
        "fact_session.user_id",
        "fact_page_view.user_id",
    }
    if "dim_tenant.user_id" in used_columns:
        fatal_issues.append(
            "dim_tenant 没有 user_id。用户行为事实表关联租户维度时必须通过 dim_user：fact_xxx.user_id = dim_user.user_id，再 dim_user.tenant_id = dim_tenant.tenant_id。"
        )
    if "dim_tenant.user_id" in used_columns and any(field in used_columns for field in dim_tenant_user_join_fields):
        warnings.append(
            "dim_tenant 没有 user_id。用户行为事实表关联租户维度时必须通过 dim_user：fact_xxx.user_id = dim_user.user_id，再 dim_user.tenant_id = dim_tenant.tenant_id。"
        )

    for table_name in used_tables:
        if table_name not in schema_lookup:
            fatal_issues.append(f"used_table 不存在或不可访问：{table_name}")
        elif (
            candidate_tables
            and table_name not in candidate_tables
            and table_name not in join_allowed_tables
            and table_name not in recipe_allowed_tables
        ):
            warnings.append(f"使用表 {table_name} 不在 Agent2 候选表中，请确认是否为幻觉表。")

    for column_ref in used_columns:
        if "." not in column_ref:
            warnings.append(f"used_column 未包含表前缀：{column_ref}")
            continue

        table_name, column_name = column_ref.split(".", 1)
        table_name = safe_to_string(table_name)
        column_name = safe_to_string(column_name)

        if table_name not in schema_lookup:
            fatal_issues.append(f"used_column 引用了不存在的表：{column_ref}")
            continue
        if column_name not in schema_lookup[table_name]:
            fatal_issues.append(f"字段归属不正确或字段不存在：{column_ref}")
            continue
        if (
            candidate_fields
            and column_ref not in candidate_fields
            and column_ref not in join_allowed_fields
            and column_ref not in recipe_allowed_fields
        ):
            warnings.append(f"使用字段 {column_ref} 不在 Agent2 候选字段中，请确认字段归属与业务语义。")

    if fatal_issues:
        repair_reason = "SQL 引用校验失败：\n" + "\n".join(f"- {item}" for item in fatal_issues[:12])
        return False, merge_warning_lists(warnings, fatal_issues), repair_reason

    return True, warnings, ""


def validate_query_result(
    task: Dict[str, Any],
    result_json: Dict[str, Any],
    df: pd.DataFrame,
    executed_sql: str,
    retrieval_context: Dict[str, Any],
    guard: Dict[str, Any],
) -> Tuple[bool, List[str], str]:
    warnings: List[str] = []
    fatal_issues: List[str] = []

    if df.empty:
        fatal_issues.append("查询结果为空，当前 SQL 很可能没有正确回答问题。")

    columns = [safe_to_string(col) for col in df.columns]
    numeric_columns = [col for col in df.columns if pd.api.types.is_numeric_dtype(df[col])]

    result_shape = safe_to_string(task.get("result_shape"))
    if result_shape == "time_series":
        if len(df.columns) < 2:
            fatal_issues.append("趋势类问题至少应返回时间列和数值列。")
        if not numeric_columns:
            fatal_issues.append("趋势类问题结果中未识别到数值列。")
    elif result_shape == "categorical_comparison":
        if len(df.columns) < 2:
            fatal_issues.append("对比类问题至少应返回维度列和数值列。")
        if not numeric_columns:
            fatal_issues.append("对比类问题结果中未识别到数值列。")

    if numeric_columns:
        all_zero_numeric = True
        for col in numeric_columns:
            series = df[col].fillna(0)
            if not series.empty and float(series.abs().sum()) > 0:
                all_zero_numeric = False
                break
        if all_zero_numeric and task.get("needs_visualization"):
            warnings.append("结果数值列全部为 0，请确认过滤条件或指标口径是否合理。")
    else:
        warnings.append("结果中未识别到数值列，可视化和指标解释能力会较弱。")

    visualization_spec = result_json.get("visualization_recommendation") or {}
    if isinstance(visualization_spec, dict) and visualization_spec.get("needed"):
        for axis_key in ["x", "y", "series"]:
            axis_value = safe_to_string(visualization_spec.get(axis_key))
            if axis_value and axis_value not in columns:
                fatal_issues.append(
                    f"visualization_recommendation.{axis_key}={axis_value} 不在结果列中。"
                )

    question_norm = normalize_text(safe_to_string(task.get("raw_question")))
    if any(token in question_norm for token in ["最高", "最多", "top", "排名"]):
        if " order by " not in (" " + executed_sql.lower() + " "):
            warnings.append("问题带有排序语义，但 SQL 中未显式包含 ORDER BY。")

    used_tables = set(result_json.get("used_tables", []) or [])
    if used_tables:
        recommended_join_texts = {
            safe_to_string(item.get("join_condition"))
            for item in retrieval_context.get("candidate_joins", [])
            if safe_to_string(item.get("join_condition"))
        }
        if len(used_tables) >= 2 and not recommended_join_texts:
            warnings.append("当前 SQL 使用了多张表，但 Agent2 未检索到推荐 JOIN，请重点复核。")

    if fatal_issues:
        repair_reason = "结果校验失败：\n" + "\n".join(f"- {item}" for item in fatal_issues[:12])
        if guard.get("warnings"):
            repair_reason += "\n请同时复核以下风险提示：\n" + "\n".join(
                f"- {item}" for item in guard["warnings"][:8]
            )
        return False, merge_warning_lists(warnings, fatal_issues), repair_reason

    return True, warnings, ""


def build_answer_memory_prompt(
    question: str,
    result_json: Dict[str, Any],
    executed_sql: str,
    df: pd.DataFrame,
    visualization_spec: Dict[str, Any],
    validation: Dict[str, Any] | None = None,
) -> str:
    preview_rows = df_preview_records(df, limit=10)
    validation = validation or {}
    validator_findings = validation.get("validator_findings", []) or []
    return f"""
你要根据用户问题、已执行 SQL、结果列、结果预览、使用表字段、warnings，生成一条“SQL 能力描述 memory”。

要求：
1. 只描述这条 SQL 实际可以回答什么。
2. 不要声称该 SQL 是业务上唯一正确口径。
3. 不要生成 SQL 中没有体现的业务结论。
4. 不要扩大适用范围。
5. 如果 SQL 只返回 tenant_id，不要说它返回企业名称。
6. 如果 SQL 使用 actual_revenue，不要说它代表 MRR、发票金额或付款金额。
7. 如果 SQL 使用 mrr，不要说它代表实际回款。
8. 如果 SQL 没有时间过滤，不要说它代表“最近”。
9. 如果 SQL 没有关联 dim_tenant，不要说它能展示国家、行业、规模。
10. 如果 SQL 没有关联 dim_plan，不要说它能展示套餐名称或套餐价格。
11. 如果有 warnings，必须写入 limitations。
12. 输出合法 JSON，不要输出 markdown。

返回 JSON 格式：
{{
  "answerable_description": "这条 SQL 可以展示什么内容",
  "question_patterns": [
    "这条 SQL 适合回答的相似问题表达 1",
    "这条 SQL 适合回答的相似问题表达 2",
    "这条 SQL 适合回答的相似问题表达 3"
  ],
  "used_tables_summary": [],
  "used_columns_summary": [],
  "result_structure": "返回哪些列，每列大概代表什么",
  "visualization_hint": "适合表格/柱状图/折线图/不适合可视化",
  "limitations": []
}}

用户问题：
{question}

已执行 SQL：
{executed_sql}

使用表：
{json.dumps(make_json_serializable(result_json.get("used_tables", []) or []), ensure_ascii=False, indent=2)}

使用字段：
{json.dumps(make_json_serializable(result_json.get("used_columns", []) or []), ensure_ascii=False, indent=2)}

结果列：
{json.dumps(make_json_serializable([safe_to_string(col) for col in df.columns]), ensure_ascii=False, indent=2)}

结果预览（最多 10 行）：
{json.dumps(make_json_serializable(preview_rows), ensure_ascii=False, indent=2)}

Warnings：
{json.dumps(make_json_serializable(result_json.get("warnings", []) or []), ensure_ascii=False, indent=2)}

Validator Findings：
{json.dumps(make_json_serializable(validator_findings), ensure_ascii=False, indent=2)}

Visualization Spec：
{json.dumps(make_json_serializable(visualization_spec or {}), ensure_ascii=False, indent=2)}
""".strip()


def maybe_write_answer_memory(
    question: str,
    result_json: Dict[str, Any],
    executed_sql: str,
    df: pd.DataFrame,
    retrieval_context: Dict[str, Any],
    guard: Dict[str, Any],
    visualization_spec: Dict[str, Any],
    lineage: Dict[str, Any] | None = None,
    validation: Dict[str, Any] | None = None,
) -> Dict[str, str | bool]:
    validation = validation or {}
    if df is None or df.empty:
        return {
            "answer_memory_written": False,
            "answer_memory_id": "",
            "answer_memory_path": "",
        }

    if not is_safe_select_sql(executed_sql):
        return {
            "answer_memory_written": False,
            "answer_memory_id": "",
            "answer_memory_path": "",
        }

    used_tables = list(result_json.get("used_tables", []) or [])
    if not used_tables:
        return {
            "answer_memory_written": False,
            "answer_memory_id": "",
            "answer_memory_path": "",
        }

    if validation.get("has_high_risk", False):
        return {
            "answer_memory_written": False,
            "answer_memory_id": "",
            "answer_memory_path": "",
        }

    try:
        prompt = build_answer_memory_prompt(
            question=question,
            result_json=result_json,
            executed_sql=executed_sql,
            df=df,
            visualization_spec=visualization_spec,
            validation=validation,
        )
        memory_output = extract_json_from_llm_output(call_siliconflow(prompt))
    except Exception as exc:
        print(f"warning: answer memory generation skipped, reason: {exc}")
        return {
            "answer_memory_written": False,
            "answer_memory_id": "",
            "answer_memory_path": "",
        }

    memory_id = (
        "ansmem_"
        + time.strftime("%Y%m%d_%H%M%S")
        + f"_{int(time.time() * 1000) % 1000000:06d}"
    )
    lineage = lineage or {}
    used_columns = list(result_json.get("used_columns", []) or [])
    validator_findings = list(validation.get("validator_findings", []) or [])

    memory_record = {
        "memory_id": memory_id,
        "memory_type": "answer_memory",
        "source_question": question,
        "answerable_description": safe_to_string(memory_output.get("answerable_description")),
        "question_patterns": list(memory_output.get("question_patterns", []) or []),
        "executed_sql": executed_sql,
        "used_tables": used_tables,
        "used_columns": used_columns,
        "used_tables_summary": list(memory_output.get("used_tables_summary", []) or []),
        "used_columns_summary": list(memory_output.get("used_columns_summary", []) or []),
        "result_columns": [safe_to_string(col) for col in df.columns],
        "result_structure": safe_to_string(memory_output.get("result_structure")),
        "result_preview": df_preview_records(df, limit=10),
        "visualization_spec": visualization_spec or {},
        "visualization_hint": safe_to_string(memory_output.get("visualization_hint")),
        "limitations": merge_warning_lists(
            list(memory_output.get("limitations", []) or []),
            list(result_json.get("warnings", []) or []),
        ),
        "warnings": list(result_json.get("warnings", []) or []),
        "validator_findings": validator_findings,
        "lineage_summary": list(lineage.get("lineage_summary", []) or []),
        "created_at": time.strftime("%Y-%m-%d %H:%M:%S"),
        "confidence": "auto_descriptive",
    }

    if not memory_record["answerable_description"]:
        print("warning: answer memory generation skipped, reason: empty answerable_description")
        return {
            "answer_memory_written": False,
            "answer_memory_id": "",
            "answer_memory_path": "",
        }

    try:
        append_jsonl(ANSWER_MEMORY_PATH, memory_record)
    except Exception as exc:
        print(f"warning: answer memory write skipped, reason: {exc}")
        return {
            "answer_memory_written": False,
            "answer_memory_id": "",
            "answer_memory_path": "",
        }

    return {
        "answer_memory_written": True,
        "answer_memory_id": memory_id,
        "answer_memory_path": str(ANSWER_MEMORY_PATH),
    }


def schedule_answer_memory_write(
    question: str,
    result_json: Dict[str, Any],
    executed_sql: str,
    df: pd.DataFrame,
    retrieval_context: Dict[str, Any],
    guard: Dict[str, Any],
    visualization_spec: Dict[str, Any],
    lineage: Dict[str, Any] | None = None,
    validation: Dict[str, Any] | None = None,
) -> Dict[str, str | bool]:
    validation = validation or {}

    if df is None or df.empty or not is_safe_select_sql(executed_sql):
        return {
            "answer_memory_written": False,
            "answer_memory_id": "",
            "answer_memory_path": "",
            "answer_memory_scheduled": False,
        }

    if not list(result_json.get("used_tables", []) or []):
        return {
            "answer_memory_written": False,
            "answer_memory_id": "",
            "answer_memory_path": "",
            "answer_memory_scheduled": False,
        }

    if validation.get("has_high_risk", False):
        return {
            "answer_memory_written": False,
            "answer_memory_id": "",
            "answer_memory_path": "",
            "answer_memory_scheduled": False,
        }

    result_json_copy = json.loads(json.dumps(make_json_serializable(result_json), ensure_ascii=False))
    retrieval_context_copy = json.loads(json.dumps(make_json_serializable(retrieval_context), ensure_ascii=False))
    guard_copy = json.loads(json.dumps(make_json_serializable(guard), ensure_ascii=False))
    visualization_spec_copy = json.loads(json.dumps(make_json_serializable(visualization_spec or {}), ensure_ascii=False))
    lineage_copy = json.loads(json.dumps(make_json_serializable(lineage or {}), ensure_ascii=False))
    validation_copy = json.loads(json.dumps(make_json_serializable(validation), ensure_ascii=False))
    df_copy = df.copy(deep=True)
    answer_memory_index_builder = get_answer_memory_index_builder()

    def background_worker() -> None:
        status = maybe_write_answer_memory(
            question=question,
            result_json=result_json_copy,
            executed_sql=executed_sql,
            df=df_copy,
            retrieval_context=retrieval_context_copy,
            guard=guard_copy,
            visualization_spec=visualization_spec_copy,
            lineage=lineage_copy,
            validation=validation_copy,
        )
        if status.get("answer_memory_written") and answer_memory_index_builder is not None:
            try:
                answer_memory_index_builder()
            except Exception as exc:
                print(f"warning: answer memory index rebuild skipped, reason: {exc}")

    worker = threading.Thread(
        target=background_worker,
        daemon=True,
    )
    worker.start()

    return {
        "answer_memory_written": False,
        "answer_memory_id": "",
        "answer_memory_path": str(ANSWER_MEMORY_PATH),
        "answer_memory_scheduled": True,
    }


def save_query_log(log_data: Dict[str, Any]) -> Path:
    ensure_dir(LOG_DIR)
    timestamp = time.strftime("%Y%m%d_%H%M%S")
    path = LOG_DIR / f"query_{timestamp}.json"
    write_json(path, log_data)
    return path


def build_query_log_payload(
    question: str,
    task: Dict[str, Any],
    retrieval_context: Dict[str, Any],
    guard: Dict[str, Any],
    success: bool,
    result_json: Dict[str, Any] | None = None,
    repair_output: Dict[str, Any] | None = None,
    error_message: str = "",
    sql: str = "",
    executed_sql: str = "",
    visualization_spec: Dict[str, Any] | None = None,
    visualization_path: str = "",
    lineage: Dict[str, Any] | None = None,
    df: pd.DataFrame | None = None,
    previous_context: Dict[str, Any] | None = None,
    followup_edit_types: List[str] | None = None,
    answer_memory_written: bool = False,
    answer_memory_id: str = "",
    answer_memory_path: str = "",
    answer_memory_scheduled: bool = False,
) -> Dict[str, Any]:
    previous_context = previous_context or {}
    previous_context_summary = {
        "previous_question": safe_to_string(previous_context.get("previous_question")),
        "previous_used_tables": list(previous_context.get("previous_used_tables", []) or []),
        "previous_result_columns": list(previous_context.get("previous_result_columns", []) or []),
        "previous_result_preview": list(previous_context.get("previous_result_preview", []) or [])[:10],
    }
    payload = {
        "question": question,
        "task_understanding": task,
        "retrieval_context_summary": {
            "candidate_tables": retrieval_context["selected_table_names"],
            "embedding_available": retrieval_context.get("embedding_available", False),
            "embedding_used": retrieval_context.get("embedding_used", False),
            "field_count": len(retrieval_context["candidate_fields"]),
            "metric_count": len(retrieval_context["candidate_metrics"]),
            "join_count": len(retrieval_context["candidate_joins"]),
            "trap_count": len(retrieval_context["candidate_traps"]),
            "policy_count": len(retrieval_context["candidate_policies"]),
            "table_retrieval_modes": [
                {
                    "table_name": item.get("table_name"),
                    "score": item.get("_score"),
                    "keyword_score": item.get("_keyword_score"),
                    "embedding_score": item.get("_embedding_score"),
                    "mode": item.get("_retrieval_mode"),
                }
                for item in retrieval_context.get("candidate_tables", [])
            ],
            "answer_memory_retrieved_count": retrieval_context.get(
                "answer_memory_retrieved_count", 0
            ),
            "candidate_answer_memory_ids": [
                item.get("memory_id")
                for item in retrieval_context.get("candidate_answer_memories", [])
            ],
            "recipe_ids": [
                item.get("recipe_id")
                for item in retrieval_context["candidate_recipes"]
            ],
        },
        "guard": guard,
        "success": success,
        "previous_context_used": bool(previous_context),
        "followup_edit_types": list(followup_edit_types or []),
        "previous_context_summary": previous_context_summary,
        "answer_memory_written": answer_memory_written,
        "answer_memory_id": answer_memory_id,
        "answer_memory_path": answer_memory_path,
        "answer_memory_scheduled": answer_memory_scheduled,
    }

    if success:
        payload.update(
            {
                "analysis_plan": (result_json or {}).get("analysis_plan", ""),
                "sql": sql,
                "executed_sql": executed_sql,
                "used_tables": (result_json or {}).get("used_tables", []),
                "used_columns": (result_json or {}).get("used_columns", []),
                "warnings": (result_json or {}).get("warnings", []),
                "visualization_spec": visualization_spec or {},
                "visualization_path": visualization_path,
                "lineage": lineage or {},
                "result_row_count": len(df) if df is not None else 0,
                "result_preview": (
                    df.head(MAX_RESULT_ROWS).to_dict(orient="records")
                    if df is not None else []
                ),
            }
        )
    else:
        payload.update(
            {
                "llm_output": result_json or {},
                "repair_output": repair_output,
                "error": error_message,
            }
        )

    return payload


def choose_fallback_chart(task: Dict[str, Any], df: pd.DataFrame) -> Dict[str, Any]:
    if df.empty or len(df.columns) < 2:
        return {
            "needed": False,
            "chart_type": "table_only",
            "x": "",
            "y": "",
            "series": "",
            "title": "",
            "reason": "结果为空或列数不足",
        }

    x_col = df.columns[0]
    numeric_cols = [col for col in df.columns[1:] if pd.api.types.is_numeric_dtype(df[col])]
    y_col = numeric_cols[0] if numeric_cols else ""

    if not y_col:
        return {
            "needed": False,
            "chart_type": "table_only",
            "x": "",
            "y": "",
            "series": "",
            "title": "",
            "reason": "未识别出可视化数值列",
        }

    chart_type = "line" if task.get("result_shape") == "time_series" else "bar"
    return {
        "needed": task.get("needs_visualization", True),
        "chart_type": chart_type,
        "x": x_col,
        "y": y_col,
        "series": "",
        "title": safe_to_string(task.get("raw_question"))[:80],
        "reason": "基于结果列结构自动推断",
    }


def normalize_visualization_spec(
    spec: Dict[str, Any] | None,
    task: Dict[str, Any],
    df: pd.DataFrame,
) -> Dict[str, Any]:
    if not isinstance(spec, dict):
        return choose_fallback_chart(task, df)

    normalized = {
        "needed": bool(spec.get("needed")),
        "chart_type": safe_to_string(spec.get("chart_type") or "table_only").lower(),
        "x": safe_to_string(spec.get("x")),
        "y": safe_to_string(spec.get("y")),
        "series": safe_to_string(spec.get("series")),
        "title": safe_to_string(spec.get("title")),
        "reason": safe_to_string(spec.get("reason")),
    }

    if normalized["chart_type"] not in {"line", "bar", "table_only"}:
        normalized["chart_type"] = "table_only"

    if normalized["chart_type"] != "table_only":
        if normalized["x"] not in df.columns or normalized["y"] not in df.columns:
            return choose_fallback_chart(task, df)

    if not normalized["title"]:
        normalized["title"] = safe_to_string(task.get("raw_question"))[:80]

    normalized["title"] = to_ascii_label(normalized["title"])

    return normalized


def render_visualization(df: pd.DataFrame, spec: Dict[str, Any]) -> str:
    if not spec.get("needed") or spec.get("chart_type") == "table_only":
        return ""

    x_col = spec.get("x")
    y_col = spec.get("y")
    series_col = spec.get("series")
    chart_type = spec.get("chart_type")

    if x_col not in df.columns or y_col not in df.columns:
        return ""

    ensure_dir(ASSET_DIR)
    timestamp = time.strftime("%Y%m%d_%H%M%S")
    output_path = ASSET_DIR / f"viz_{timestamp}.png"

    plt.figure(figsize=(12, 6))

    if series_col and series_col in df.columns:
        pivot_df = df.pivot_table(
            index=x_col,
            columns=series_col,
            values=y_col,
            aggfunc="sum",
        ).fillna(0)
        if chart_type == "line":
            pivot_df.plot(kind="line", ax=plt.gca(), marker="o")
        else:
            pivot_df.plot(kind="bar", ax=plt.gca())
    else:
        plot_df = df[[x_col, y_col]].copy()
        if chart_type == "line":
            plt.plot(plot_df[x_col], plot_df[y_col], marker="o")
        else:
            plt.bar(plot_df[x_col].astype(str), plot_df[y_col])

    plt.title(spec.get("title") or "")
    plt.xlabel(x_col)
    plt.ylabel(y_col)
    plt.xticks(rotation=30, ha="right")
    plt.tight_layout()
    plt.savefig(output_path, dpi=150)
    plt.close()
    return str(output_path)


def print_retrieval_summary(retrieval_context: Dict[str, Any], guard: Dict[str, Any]) -> None:
    print("\n[Agent2: Knowledge Retriever]")
    print(
        "Embedding: "
        f"available={retrieval_context.get('embedding_available', False)}, "
        f"used={retrieval_context.get('embedding_used', False)}"
    )

    tables = retrieval_context.get("candidate_tables", [])
    if tables:
        print("候选表：")
        for item in tables:
            table_name = safe_to_string(item.get("table_name")) or "unknown_table"
            score = item.get("_score")
            mode = safe_to_string(item.get("_retrieval_mode"))
            if score is not None and mode:
                print(f"- {table_name} score={float(score):.2f} mode={mode}")
            else:
                print(f"- {table_name}")
    else:
        print("候选表：无")

    recipes = retrieval_context["candidate_recipes"]
    if recipes:
        print("参考 recipe：")
        for item in recipes:
            recipe_name = safe_to_string(item.get("name") or item.get("title") or item.get("recipe_id"))
            print(f"- {recipe_name}")

    answer_memories = retrieval_context.get("candidate_answer_memories", []) or []
    if answer_memories:
        print("\n[Answer Memory]")
        print("历史相似案例：最多 1 条")
        for item in answer_memories[:MAX_CONTEXT_ANSWER_MEMORIES]:
            summary = safe_to_string(
                item.get("source_question") or item.get("answerable_description")
            )
            print(f"- {limit_text(summary, 120)}")

    print("\n[Agent3: Grain & DQ Guard]")
    for item in guard["warnings"][:6]:
        print(f"- {item}")


def print_answer(
    result_json: Dict[str, Any],
    df: pd.DataFrame,
    executed_sql: str,
    visualization_spec: Dict[str, Any],
    visualization_path: str,
    lineage: Dict[str, Any] | None = None,
) -> None:
    print("\n[Agent4: Analysis Plan]")
    print(result_json.get("analysis_plan", ""))

    print("\n[Agent5: Generated SQL]")
    print(executed_sql)

    print("\n[Used Tables]")
    for table in result_json.get("used_tables", []) or ["未提供"]:
        print(f"- {table}")

    print("\n[Warnings]")
    warnings = result_json.get("warnings", [])
    if warnings:
        for item in warnings[:3]:
            print(f"- {item}")
        if len(warnings) > 3:
            print("- 仅展示前 3 条风险提示，完整风险已记录在查询日志中。")
    else:
        print("- 无明显风险提示")

    print("\n[结果预览]")
    if df.empty:
        print("结果为空。")
    else:
        print(df.to_string(index=False))

    print("\n[Agent6: 可视化]")
    print(json.dumps(make_json_serializable(visualization_spec), ensure_ascii=False, indent=2))
    if visualization_path:
        print(f"图表文件: {visualization_path}")

    lineage = lineage or {}
    if lineage:
        sql_path = lineage.get("sql_path", {})
        result_columns = [
            item.get("column")
            for item in lineage.get("result_schema", [])
            if isinstance(item, dict) and item.get("column")
        ]
        print("\n[Lineage: 数据血缘]")
        print("使用表：", ", ".join(lineage.get("used_tables", [])) or "无")
        print("结果列：", ", ".join(result_columns) if result_columns else "无")
        features = []
        if sql_path.get("has_join"):
            features.append("JOIN")
        if sql_path.get("has_group_by"):
            features.append("GROUP BY")
        if sql_path.get("has_order_by"):
            features.append("ORDER BY")
        if sql_path.get("has_limit"):
            features.append("LIMIT")
        print("SQL 特征：", ", ".join(features) if features else "基础 SELECT")


def run_question_pipeline(
    question: str,
    kb: Dict[str, Any],
    conn: duckdb.DuckDBPyConnection,
    previous_context: Dict[str, Any] | None = None,
) -> Dict[str, Any]:
    build_lineage = get_lineage_builder()
    task = agent1_task_understanding(question)
    task["previous_context_available"] = previous_context is not None
    task["followup_edit_types"] = infer_followup_edit_type(question)
    retrieval_context = agent2_knowledge_retriever(question, kb, task)
    guard = agent3_grain_dq_guard(retrieval_context)

    prompt = build_sql_generation_prompt(
        question,
        task,
        retrieval_context,
        guard,
        previous_context=previous_context,
    )
    raw_output = call_siliconflow(prompt)
    result_json = extract_json_from_llm_output(raw_output)
    result_json["warnings"] = list(result_json.get("warnings", []) or [])
    validation_summary = {
        "has_high_risk": False,
        "validator_findings": [],
    }

    sql = clean_generated_sql(result_json.get("sql", ""))
    result_json = sync_result_metadata_with_sql(result_json, sql)
    success, df, err, executed_sql = execute_sql(conn, sql)

    validator_warnings: List[str] = []
    if success:
        ref_valid, ref_warnings, ref_reason = validate_sql_references(
            conn=conn,
            result_json=result_json,
            retrieval_context=retrieval_context,
        )
        result_valid, result_warnings, result_reason = validate_query_result(
            task=task,
            result_json=result_json,
            df=df,
            executed_sql=executed_sql,
            retrieval_context=retrieval_context,
            guard=guard,
        )
        validator_warnings = merge_warning_lists(ref_warnings, result_warnings)
        result_json["warnings"] = merge_warning_lists(
            result_json.get("warnings", []),
            validator_warnings,
        )
        validation_summary["validator_findings"] = validator_warnings
        if not ref_valid:
            success = False
            err = ref_reason
            validation_summary["has_high_risk"] = True
        elif not result_valid:
            success = False
            err = result_reason
            validation_summary["has_high_risk"] = True

    repair_output = None
    if not success and MAX_REPAIR_ATTEMPTS > 0:
        print("\n[Agent5: SQL 执行失败]")
        print(err)
        print("\n尝试修复 SQL...")

        repair_prompt = build_sql_repair_prompt(
            question=question,
            task=task,
            retrieval_context=retrieval_context,
            guard=guard,
            bad_sql=sql,
            error_message=err,
            previous_context=previous_context,
        )
        raw_repair_output = call_siliconflow(repair_prompt)
        repair_output = extract_json_from_llm_output(raw_repair_output)
        repair_output["warnings"] = list(repair_output.get("warnings", []) or [])
        repaired_sql = clean_generated_sql(repair_output.get("sql", ""))
        repair_output = sync_result_metadata_with_sql(repair_output, repaired_sql)
        success, df, err, executed_sql = execute_sql(conn, repaired_sql)

        if success:
            ref_valid, ref_warnings, ref_reason = validate_sql_references(
                conn=conn,
                result_json=repair_output,
                retrieval_context=retrieval_context,
            )
            result_valid, result_warnings, result_reason = validate_query_result(
                task=task,
                result_json=repair_output,
                df=df,
                executed_sql=executed_sql,
                retrieval_context=retrieval_context,
                guard=guard,
            )
            validator_warnings = merge_warning_lists(ref_warnings, result_warnings)
            repair_output["warnings"] = merge_warning_lists(
                repair_output.get("warnings", []),
                validator_warnings,
            )
            validation_summary["validator_findings"] = validator_warnings
            if not ref_valid:
                success = False
                err = ref_reason
                validation_summary["has_high_risk"] = True
            elif not result_valid:
                success = False
                err = result_reason
                validation_summary["has_high_risk"] = True

        if success:
            result_json = repair_output
            sql = repaired_sql

    if not success:
        log_payload = build_query_log_payload(
            question=question,
            task=task,
            retrieval_context=retrieval_context,
            guard=guard,
            success=False,
            result_json=result_json,
            repair_output=repair_output,
            error_message=err,
            previous_context=previous_context,
            followup_edit_types=task.get("followup_edit_types", []),
            answer_memory_written=False,
            answer_memory_id="",
            answer_memory_path="",
            answer_memory_scheduled=False,
        )
        log_path = save_query_log(log_payload)
        return {
            "success": False,
            "question": question,
            "task": task,
            "retrieval_context": retrieval_context,
            "guard": guard,
            "result_json": result_json,
            "repair_output": repair_output,
            "error": err,
            "previous_context_used": previous_context is not None,
            "previous_context": previous_context or {},
            "log_path": str(log_path),
            "answer_memory_written": False,
            "answer_memory_id": "",
            "answer_memory_path": "",
            "answer_memory_scheduled": False,
        }

    visualization_spec = normalize_visualization_spec(
        result_json.get("visualization_recommendation"),
        task,
        df,
    )
    visualization_path = render_visualization(df, visualization_spec)
    lineage = build_lineage(
        question=question,
        result_json=result_json,
        retrieval_context=retrieval_context,
        executed_sql=executed_sql,
        df=df,
        visualization_spec=visualization_spec,
    )

    answer_memory_status = schedule_answer_memory_write(
        question=question,
        result_json=result_json,
        executed_sql=executed_sql,
        df=df,
        retrieval_context=retrieval_context,
        guard=guard,
        visualization_spec=visualization_spec,
        lineage=lineage,
        validation=validation_summary,
    )

    log_payload = build_query_log_payload(
        question=question,
        task=task,
        retrieval_context=retrieval_context,
        guard=guard,
        success=True,
        result_json=result_json,
        sql=sql,
        executed_sql=executed_sql,
        visualization_spec=visualization_spec,
        visualization_path=visualization_path,
        lineage=lineage,
        df=df,
        previous_context=previous_context,
        followup_edit_types=task.get("followup_edit_types", []),
        answer_memory_written=bool(answer_memory_status.get("answer_memory_written", False)),
        answer_memory_id=safe_to_string(answer_memory_status.get("answer_memory_id")),
        answer_memory_path=safe_to_string(answer_memory_status.get("answer_memory_path")),
        answer_memory_scheduled=bool(answer_memory_status.get("answer_memory_scheduled", False)),
    )
    log_path = save_query_log(log_payload)

    return {
        "success": True,
        "question": question,
        "task": task,
        "retrieval_context": retrieval_context,
        "guard": guard,
        "result_json": result_json,
        "sql": sql,
        "executed_sql": executed_sql,
        "dataframe": df,
        "visualization_spec": visualization_spec,
        "visualization_path": visualization_path,
        "lineage": lineage,
        "previous_context_used": previous_context is not None,
        "previous_context": previous_context or {},
        "log_path": str(log_path),
        "answer_memory_written": bool(answer_memory_status.get("answer_memory_written", False)),
        "answer_memory_id": safe_to_string(answer_memory_status.get("answer_memory_id")),
        "answer_memory_path": safe_to_string(answer_memory_status.get("answer_memory_path")),
        "answer_memory_scheduled": bool(answer_memory_status.get("answer_memory_scheduled", False)),
    }


def answer_question(question: str, kb: Dict[str, Any], conn: duckdb.DuckDBPyConnection) -> None:
    result = run_question_pipeline(question, kb, conn)
    print_retrieval_summary(result["retrieval_context"], result["guard"])

    if not result["success"]:
        print("\n[Agent5: SQL 修复后仍失败]")
        print(result["error"])
        print(f"\n查询日志已保存：{result['log_path']}")
        return

    print_answer(
        result["result_json"],
        result["dataframe"],
        result["executed_sql"],
        result["visualization_spec"],
        result["visualization_path"],
        result.get("lineage", {}),
    )
    print(f"\n查询日志已保存：{result['log_path']}")


def main() -> None:
    load_dotenv(ENV_PATH)
    check_required_files()

    model = os.environ.get("SILICONFLOW_MODEL", DEFAULT_MODEL).strip()
    api_url = os.environ.get("SILICONFLOW_API_URL", DEFAULT_SILICONFLOW_API_URL).strip()

    kb = load_knowledge_base()
    conn = duckdb.connect(str(DB_PATH))
    embedding_available = any(
        kb["retrieval_v2"].get(name)
        for name in [
            "table_embedding_index",
            "field_embedding_index",
            "metric_embedding_index",
            "recipe_embedding_index",
        ]
    )

    print("=" * 72)
    print("CloudWork AI 数据分析 Agent CLI v2")
    print("=" * 72)
    print(f"模型: {model}")
    print(f"API 地址: {api_url}")
    print("流程:")
    print("Agent1 问题理解 -> Agent2 知识检索")
    print("-> Agent3 Grain&DQ 守卫 -> Agent4 SQL 规划与生成")
    print("-> Agent5 SQL 执行与修复 -> Agent6 答案与可视化")
    print("")
    print("知识库使用方式:")
    print("- Agent2 / Agent3 / Agent4 默认使用 outputs/knowledge/retrieval_v2")
    print("- Recipe 运行时使用 outputs/knowledge/retrieval_v2/recipes.json")
    print("- Visualization 优先读取 outputs/knowledge/visualization_rules.json")
    if embedding_available:
        print("- Embedding 检索: hybrid retrieval available")
    else:
        print("- Embedding 检索: keyword fallback mode")
    print("")
    print("输入 exit / quit / q 退出。")
    print("=" * 72)

    try:
        while True:
            question = input("\n请输入问题: ").strip()
            if question.lower() in {"exit", "quit", "q"}:
                print("已退出。")
                break
            if not question:
                continue
            try:
                answer_question(question, kb, conn)
            except KeyboardInterrupt:
                print("\n已中断当前问题。")
                continue
            except Exception as e:
                print(f"\n运行失败：{e}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
