#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import json
import math
import os
import re
from pathlib import Path
from typing import Any, Dict, List
from urllib import error, request

try:
    from config_paths import ENV_PATH, PROJECT_ROOT
except ModuleNotFoundError:
    from src.config_paths import ENV_PATH, PROJECT_ROOT


DEFAULT_SILICONFLOW_EMBEDDING_API_URL = "https://api.siliconflow.cn/v1/embeddings"
DEFAULT_SILICONFLOW_EMBEDDING_MODEL = "BAAI/bge-m3"
DEFAULT_BATCH_SIZE = 32

MEMORY_PATH = PROJECT_ROOT / "outputs" / "memory" / "answer_memory.jsonl"
RETRIEVAL_V2_DIR = PROJECT_ROOT / "outputs" / "knowledge" / "retrieval_v2"
ANSWER_MEMORY_INDEX_PATH = RETRIEVAL_V2_DIR / "answer_memory_index.json"
ANSWER_MEMORY_EMBEDDING_INDEX_PATH = RETRIEVAL_V2_DIR / "answer_memory_embedding_index.json"


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


def write_json(path: Path, data: Any) -> None:
    ensure_dir(path.parent)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def normalize_text(text: str) -> str:
    return re.sub(r"\s+", " ", str(text).lower()).strip()


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


def tokenize(text: str) -> List[str]:
    normalized = normalize_text(text)
    latin_tokens = re.findall(r"[a-zA-Z0-9_]+", normalized)
    chinese_chunks = re.findall(r"[\u4e00-\u9fff]{2,}", normalized)
    ordered = []
    seen = set()
    for token in latin_tokens + chinese_chunks:
        if token and token not in seen:
            seen.add(token)
            ordered.append(token)
    return ordered


def read_jsonl(path: Path) -> List[Dict[str, Any]]:
    items: List[Dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        payload = json.loads(line)
        if isinstance(payload, dict):
            items.append(payload)
    return items


def build_answer_memory_embedding_text(item: Dict[str, Any]) -> str:
    return normalize_text(
        " ".join(
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
                " ".join(item.get("warnings", []) or []),
            ]
        )
    )


def build_answer_memory_keywords(item: Dict[str, Any]) -> List[str]:
    texts = [
        safe_to_string(item.get("source_question")),
        safe_to_string(item.get("answerable_description")),
        " ".join(item.get("used_tables", []) or []),
        " ".join(item.get("used_columns", []) or []),
        " ".join(item.get("result_columns", []) or []),
        " ".join(item.get("question_patterns", []) or []),
    ]
    tokens: List[str] = []
    for text in texts:
        tokens.extend(tokenize(text))
    return list(dict.fromkeys(tokens))


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


def build_answer_memory_indexes() -> Dict[str, Any]:
    load_dotenv(ENV_PATH)

    if not MEMORY_PATH.exists():
        print(f"未找到 answer memory 文件：{MEMORY_PATH}")
        print("安全退出，不生成索引。")
        return {
            "index_written": False,
            "embedding_index_written": False,
            "memory_count": 0,
        }

    memory_items = read_jsonl(MEMORY_PATH)
    if not memory_items:
        print(f"answer memory 文件为空：{MEMORY_PATH}")
        print("安全退出，不生成索引。")
        return {
            "index_written": False,
            "embedding_index_written": False,
            "memory_count": 0,
        }

    index_items: List[Dict[str, Any]] = []
    for item in memory_items:
        index_item = {
            "memory_id": safe_to_string(item.get("memory_id")),
            "memory_type": safe_to_string(item.get("memory_type")),
            "source_question": safe_to_string(item.get("source_question")),
            "answerable_description": safe_to_string(item.get("answerable_description")),
            "question_patterns": list(item.get("question_patterns", []) or []),
            "used_tables": list(item.get("used_tables", []) or []),
            "used_columns": list(item.get("used_columns", []) or []),
            "result_columns": list(item.get("result_columns", []) or []),
            "result_structure": safe_to_string(item.get("result_structure")),
            "visualization_hint": safe_to_string(item.get("visualization_hint")),
            "limitations": list(item.get("limitations", []) or []),
            "warnings": list(item.get("warnings", []) or []),
            "confidence": safe_to_string(item.get("confidence")),
            "created_at": safe_to_string(item.get("created_at")),
        }
        index_item["keywords"] = build_answer_memory_keywords(index_item)
        index_item["_embedding_text"] = build_answer_memory_embedding_text(index_item)
        index_items.append(index_item)

    write_json(ANSWER_MEMORY_INDEX_PATH, index_items)
    print(f"已生成：{ANSWER_MEMORY_INDEX_PATH}")
    print(f"memory count: {len(index_items)}")
    result = {
        "index_written": True,
        "embedding_index_written": False,
        "memory_count": len(index_items),
    }

    api_key = os.environ.get("SILICONFLOW_API_KEY", "").strip()
    if not api_key:
        print("未找到 SILICONFLOW_API_KEY，只生成 answer_memory_index.json。")
        return result

    api_url = os.environ.get(
        "SILICONFLOW_EMBEDDING_API_URL",
        DEFAULT_SILICONFLOW_EMBEDDING_API_URL,
    ).strip()
    embedding_model = os.environ.get(
        "SILICONFLOW_EMBEDDING_MODEL",
        DEFAULT_SILICONFLOW_EMBEDDING_MODEL,
    ).strip()

    try:
        embedding_items = [dict(item) for item in index_items]
        texts = [safe_to_string(item.get("_embedding_text")) for item in embedding_items]
        for start in range(0, len(texts), DEFAULT_BATCH_SIZE):
            end = start + DEFAULT_BATCH_SIZE
            batch_embeddings = fetch_embeddings(
                texts[start:end],
                api_key=api_key,
                api_url=api_url,
                embedding_model=embedding_model,
            )
            for item, embedding in zip(embedding_items[start:end], batch_embeddings):
                item["_embedding"] = embedding
        write_json(ANSWER_MEMORY_EMBEDDING_INDEX_PATH, embedding_items)
        print(f"已生成：{ANSWER_MEMORY_EMBEDDING_INDEX_PATH}")
        result["embedding_index_written"] = True
    except Exception as exc:
        print(f"warning: answer memory embedding index skipped, reason: {exc}")

    return result


def main() -> None:
    build_answer_memory_indexes()


if __name__ == "__main__":
    main()
