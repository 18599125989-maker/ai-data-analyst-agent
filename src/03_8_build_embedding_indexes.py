#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
基于 retrieval_v2 已生成的结构化索引，补充 embedding 索引。

输入：
- outputs/knowledge/retrieval_v2/table_index.json
- outputs/knowledge/retrieval_v2/field_index.json
- outputs/knowledge/retrieval_v2/metric_index.json
- outputs/knowledge/retrieval_v2/recipe_index.json

输出：
- outputs/knowledge/retrieval_v2/table_embedding_index.json
- outputs/knowledge/retrieval_v2/field_embedding_index.json
- outputs/knowledge/retrieval_v2/metric_embedding_index.json
- outputs/knowledge/retrieval_v2/recipe_embedding_index.json
"""

from __future__ import annotations

import json
import math
import os
import re
from pathlib import Path
from typing import Any, Callable, Dict, List
from urllib import error, request

try:
    from config_paths import ENV_PATH, PROJECT_ROOT
except ModuleNotFoundError:
    from src.config_paths import ENV_PATH, PROJECT_ROOT


DEFAULT_SILICONFLOW_EMBEDDING_API_URL = "https://api.siliconflow.cn/v1/embeddings"
DEFAULT_SILICONFLOW_EMBEDDING_MODEL = "BAAI/bge-m3"
DEFAULT_BATCH_SIZE = 32
FORCE_REBUILD_EMBEDDINGS = False

RETRIEVAL_V2_DIR = PROJECT_ROOT / "outputs" / "knowledge" / "retrieval_v2"

TABLE_INDEX_PATH = RETRIEVAL_V2_DIR / "table_index.json"
FIELD_INDEX_PATH = RETRIEVAL_V2_DIR / "field_index.json"
METRIC_INDEX_PATH = RETRIEVAL_V2_DIR / "metric_index.json"
RECIPE_INDEX_PATH = RETRIEVAL_V2_DIR / "recipe_index.json"

TABLE_EMBEDDING_INDEX_PATH = RETRIEVAL_V2_DIR / "table_embedding_index.json"
FIELD_EMBEDDING_INDEX_PATH = RETRIEVAL_V2_DIR / "field_embedding_index.json"
METRIC_EMBEDDING_INDEX_PATH = RETRIEVAL_V2_DIR / "metric_embedding_index.json"
RECIPE_EMBEDDING_INDEX_PATH = RETRIEVAL_V2_DIR / "recipe_embedding_index.json"


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


def write_json_atomic(path: Path, data: Any) -> None:
    ensure_dir(path.parent)
    temp_path = path.with_suffix(path.suffix + ".tmp")
    temp_path.write_text(
        json.dumps(data, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    temp_path.replace(path)


def normalize_whitespace(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip()


def safe_to_string(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    if isinstance(value, (list, dict)):
        try:
            return json.dumps(value, ensure_ascii=False)
        except TypeError:
            return str(value)
    return str(value)


def build_table_embedding_text(item: Dict[str, Any]) -> str:
    return " ".join(
        [
            safe_to_string(item.get("table_name")),
            safe_to_string(item.get("business_meaning")),
            safe_to_string(item.get("grain_summary")),
            " ".join(item.get("metric_names", []) or []),
            " ".join(item.get("join_targets", []) or []),
            " ".join(item.get("keywords", []) or []),
        ]
    )


def build_field_embedding_text(item: Dict[str, Any]) -> str:
    return " ".join(
        [
            safe_to_string(item.get("table_name")),
            safe_to_string(item.get("field_name") or item.get("column_name")),
            safe_to_string(item.get("semantic_type")),
            safe_to_string(item.get("business_meaning")),
            safe_to_string(item.get("references")),
            " ".join(item.get("keywords", []) or []),
        ]
    )


def build_metric_embedding_text(item: Dict[str, Any]) -> str:
    return " ".join(
        [
            safe_to_string(item.get("table_name")),
            safe_to_string(item.get("metric_name")),
            safe_to_string(item.get("expression")),
            safe_to_string(item.get("meaning")),
            safe_to_string(item.get("note")),
            " ".join(item.get("keywords", []) or []),
        ]
    )


def build_recipe_embedding_text(item: Dict[str, Any]) -> str:
    metrics = item.get("metrics", []) or []
    metric_texts = []
    for metric in metrics:
        if isinstance(metric, dict):
            metric_texts.append(
                " ".join(
                    [
                        safe_to_string(metric.get("name")),
                        safe_to_string(metric.get("expression")),
                        safe_to_string(metric.get("meaning")),
                    ]
                )
            )
        else:
            metric_texts.append(safe_to_string(metric))

    return " ".join(
        [
            safe_to_string(item.get("recipe_id")),
            safe_to_string(item.get("name")),
            safe_to_string(item.get("title")),
            safe_to_string(item.get("intent")),
            safe_to_string(item.get("canonical_question")),
            " ".join(item.get("typical_questions", []) or []),
            safe_to_string(item.get("description")),
            " ".join(item.get("required_tables", []) or []),
            " ".join(item.get("optional_tables", []) or []),
            " ".join(item.get("required_fields", []) or []),
            " ".join(item.get("optional_fields", []) or []),
            " ".join(metric_texts),
            " ".join(item.get("dimensions", []) or []),
            " ".join(item.get("join_paths", []) or []),
            safe_to_string(item.get("grain")),
            safe_to_string(item.get("sql_skeleton")),
            " ".join(item.get("risks", []) or []),
            " ".join(item.get("keywords", []) or []),
        ]
    )


def fetch_embeddings(
    texts: List[str],
    api_key: str,
    api_url: str,
    embedding_model: str,
) -> List[List[float]]:
    payload = {
        "model": embedding_model,
        "input": texts,
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
        raise RuntimeError(f"Embedding HTTPError {exc.code}: {body}") from exc
    except error.URLError as exc:
        raise RuntimeError(f"Embedding URLError: {exc}") from exc

    try:
        result = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"Embedding 响应 JSON 解析失败: {raw[:500]}") from exc

    data = result.get("data")
    if not isinstance(data, list):
        raise RuntimeError(f"Embedding 响应缺少 data 列表: {result}")

    ordered = sorted(
        data,
        key=lambda item: item.get("index", math.inf) if isinstance(item, dict) else math.inf,
    )
    embeddings: List[List[float]] = []
    for item in ordered:
        if not isinstance(item, dict) or not isinstance(item.get("embedding"), list):
            raise RuntimeError(f"Embedding 响应项格式不合法: {item}")
        embeddings.append(item["embedding"])

    if len(embeddings) != len(texts):
        raise RuntimeError(
            f"Embedding 返回数量不匹配，输入 {len(texts)} 条，返回 {len(embeddings)} 条。"
        )

    return embeddings


def build_embedding_index(
    source_path: Path,
    output_path: Path,
    text_builder: Callable[[Dict[str, Any]], str],
    api_key: str,
    api_url: str,
    embedding_model: str,
    batch_size: int = DEFAULT_BATCH_SIZE,
) -> None:
    source_items = read_json(source_path)
    if not isinstance(source_items, list):
        raise TypeError(f"索引文件必须是 list: {source_path}")

    status = "rebuilt"
    if output_path.exists() and not FORCE_REBUILD_EMBEDDINGS:
        existing_items = read_json(output_path)
        if isinstance(existing_items, list) and len(existing_items) == len(source_items):
            status = "skipped"
            print(f"source path: {source_path}")
            print(f"output path: {output_path}")
            print(f"item count: {len(source_items)}")
            print(f"status: {status}")
            print("-" * 70)
            return

    valid_items: List[Dict[str, Any]] = []
    valid_texts: List[str] = []

    for idx, item in enumerate(source_items):
        if not isinstance(item, dict):
            print(f"warning: {source_path.name} 第 {idx} 条不是对象，已跳过。")
            continue
        embedding_text = normalize_whitespace(text_builder(item))
        if not embedding_text:
            print(f"warning: {source_path.name} 第 {idx} 条 embedding_text 为空，已跳过。")
            continue
        copied = dict(item)
        copied["_embedding_text"] = embedding_text
        valid_items.append(copied)
        valid_texts.append(embedding_text)

    output_items: List[Dict[str, Any]] = []
    for start in range(0, len(valid_texts), batch_size):
        end = start + batch_size
        batch_texts = valid_texts[start:end]
        batch_embeddings = fetch_embeddings(
            batch_texts,
            api_key=api_key,
            api_url=api_url,
            embedding_model=embedding_model,
        )
        for item, embedding in zip(valid_items[start:end], batch_embeddings):
            enriched = dict(item)
            enriched["_embedding"] = embedding
            output_items.append(enriched)

    write_json_atomic(output_path, output_items)

    print(f"source path: {source_path}")
    print(f"output path: {output_path}")
    print(f"item count: {len(source_items)}")
    print(f"status: {status}")
    print("-" * 70)


def main() -> None:
    load_dotenv(ENV_PATH)

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
        raise RuntimeError("未找到 SILICONFLOW_API_KEY，请在 .env 或环境变量中配置。")

    build_embedding_index(
        source_path=TABLE_INDEX_PATH,
        output_path=TABLE_EMBEDDING_INDEX_PATH,
        text_builder=build_table_embedding_text,
        api_key=api_key,
        api_url=api_url,
        embedding_model=embedding_model,
    )
    build_embedding_index(
        source_path=FIELD_INDEX_PATH,
        output_path=FIELD_EMBEDDING_INDEX_PATH,
        text_builder=build_field_embedding_text,
        api_key=api_key,
        api_url=api_url,
        embedding_model=embedding_model,
    )
    build_embedding_index(
        source_path=METRIC_INDEX_PATH,
        output_path=METRIC_EMBEDDING_INDEX_PATH,
        text_builder=build_metric_embedding_text,
        api_key=api_key,
        api_url=api_url,
        embedding_model=embedding_model,
    )
    build_embedding_index(
        source_path=RECIPE_INDEX_PATH,
        output_path=RECIPE_EMBEDDING_INDEX_PATH,
        text_builder=build_recipe_embedding_text,
        api_key=api_key,
        api_url=api_url,
        embedding_model=embedding_model,
    )


if __name__ == "__main__":
    main()
