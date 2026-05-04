#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
功能：
CloudWork AI Data Analyst Agent CLI 第一版。

主流程：
1. 用户输入自然语言问题
2. 读取 outputs/knowledge/ 下的结构化知识库
3. 简单检索相关表、字段、DQ 规则、Grain 规则、Recipe
4. 调用 SiliconFlow LLM 生成 DuckDB SQL
5. 执行 SQL
6. 如果 SQL 执行失败，尝试修复一次
7. 输出分析计划、SQL、结果预览、使用表和风险提示
8. 保存 query log

运行前请先执行：
python src/00_load_data.py
python src/01_profile_tables.py
python src/02_sample_questions.py
python src/03_build_knowledge_base.py

.env 示例：
SILICONFLOW_API_KEY=你的APIKEY
SILICONFLOW_API_URL=https://api.siliconflow.cn/v1/chat/completions
SILICONFLOW_MODEL=Qwen/Qwen2.5-72B-Instruct

运行方式：
python src/04_agent_cli.py
"""

import json
import os
import re
import time
from pathlib import Path
from typing import Any, Dict, List, Tuple
from urllib import request, error

import duckdb
import pandas as pd


# =========================
# 路径配置
# =========================

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DB_PATH = PROJECT_ROOT / "cloudwork.duckdb"
ENV_PATH = PROJECT_ROOT / ".env"

KNOWLEDGE_DIR = PROJECT_ROOT / "outputs" / "knowledge"
LOG_DIR = PROJECT_ROOT / "outputs" / "logs" / "query_logs"

TABLE_CARDS_PATH = KNOWLEDGE_DIR / "table_cards.json"
COLUMN_CARDS_PATH = KNOWLEDGE_DIR / "column_cards.json"
DQ_RULES_PATH = KNOWLEDGE_DIR / "dq_rules.json"
GRAIN_RULES_PATH = KNOWLEDGE_DIR / "grain_rules.json"
RECIPES_PATH = KNOWLEDGE_DIR / "recipes.json"

DEFAULT_SILICONFLOW_API_URL = "https://api.siliconflow.cn/v1/chat/completions"
DEFAULT_MODEL = "Qwen/Qwen2.5-72B-Instruct"

MAX_CONTEXT_TABLES = 6
MAX_CONTEXT_COLUMNS = 50
MAX_CONTEXT_RULES = 12
MAX_CONTEXT_RECIPES = 3
MAX_RESULT_ROWS = 30
MAX_REPAIR_ATTEMPTS = 1


# =========================
# 基础工具函数
# =========================

def ensure_dir(path: Path) -> None:
    """确保目录存在。"""
    path.mkdir(parents=True, exist_ok=True)


def load_dotenv(env_path: Path) -> None:
    """
    简易 .env 加载器。
    不依赖 python-dotenv。
    """
    if not env_path.exists():
        return

    for line in env_path.read_text(encoding="utf-8").splitlines():
        line = line.strip()

        if not line or line.startswith("#") or "=" not in line:
            continue

        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")

        if key:
            os.environ[key] = value


def read_json(path: Path) -> Any:
    """读取 JSON 文件。"""
    if not path.exists():
        raise FileNotFoundError(f"找不到文件：{path}")
    return json.loads(path.read_text(encoding="utf-8"))


def write_json(path: Path, data: Any) -> None:
    """写入 JSON 文件。"""
    ensure_dir(path.parent)
    path.write_text(
        json.dumps(data, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def normalize_text(text: str) -> str:
    """文本归一化，用于简单关键词检索。"""
    return re.sub(r"\s+", " ", str(text).lower()).strip()


def tokenize(text: str) -> List[str]:
    """
    简单分词：
    主要服务英文/字段名/表名/指标名。
    """
    text = normalize_text(text)
    tokens = re.findall(r"[a-zA-Z0-9_]+", text)
    return list(dict.fromkeys(tokens))


def safe_to_string(value: Any) -> str:
    """安全转字符串。"""
    if value is None:
        return ""
    return str(value)


# =========================
# 知识库加载
# =========================

def load_knowledge_base() -> Dict[str, Any]:
    """加载 03 生成的结构化知识库。"""
    return {
        "table_cards": read_json(TABLE_CARDS_PATH),
        "column_cards": read_json(COLUMN_CARDS_PATH),
        "dq_rules": read_json(DQ_RULES_PATH),
        "grain_rules": read_json(GRAIN_RULES_PATH),
        "recipes": read_json(RECIPES_PATH),
    }


def check_required_files() -> None:
    """检查运行所需文件是否存在。"""
    if not DB_PATH.exists():
        raise FileNotFoundError(
            f"找不到数据库：{DB_PATH}\n请先运行：python src/00_load_data.py"
        )

    required_files = [
        TABLE_CARDS_PATH,
        COLUMN_CARDS_PATH,
        DQ_RULES_PATH,
        GRAIN_RULES_PATH,
        RECIPES_PATH,
    ]

    for file_path in required_files:
        if not file_path.exists():
            raise FileNotFoundError(
                f"找不到知识库文件：{file_path}\n请先运行：python src/03_build_knowledge_base.py"
            )


# =========================
# 简单 Retriever
# =========================

def score_text(query: str, text: str) -> int:
    """
    简单关键词打分。
    第一版不做 embedding，先保证可运行、可解释。
    """
    query_norm = normalize_text(query)
    text_norm = normalize_text(text)
    tokens = tokenize(query)

    score = 0

    for token in tokens:
        if token in text_norm:
            score += 3

    if query_norm and query_norm in text_norm:
        score += 10

    return score


def table_card_to_search_text(card: Dict[str, Any]) -> str:
    """把 table_card 转成可检索文本。"""
    parts = [
        card.get("table_name", ""),
        " ".join([c.get("name", "") for c in card.get("columns", [])]),
        " ".join(card.get("possible_primary_keys", [])),
        " ".join(card.get("date_columns", [])),
        " ".join(card.get("measure_columns", [])),
        " ".join(card.get("dimension_columns", [])),
    ]

    for item in card.get("enum_like_columns", []):
        parts.append(item.get("column_name", ""))
        parts.extend([safe_to_string(v) for v in item.get("sample_values", [])])

    for item in card.get("high_null_columns", []):
        parts.append(item.get("column_name", ""))

    return " ".join(parts)


def column_card_to_search_text(card: Dict[str, Any]) -> str:
    """把 column_card 转成可检索文本。"""
    return " ".join(
        [
            card.get("table_name", ""),
            card.get("column_name", ""),
            card.get("data_type", ""),
            safe_to_string(card.get("min")),
            safe_to_string(card.get("max")),
            " ".join([safe_to_string(v) for v in card.get("sample_values", [])]),
        ]
    )


def rule_to_search_text(rule: Dict[str, Any]) -> str:
    """把 dq/grain rule 转成可检索文本。"""
    return " ".join(
        [
            safe_to_string(rule.get("table_name")),
            safe_to_string(rule.get("column_name")),
            safe_to_string(rule.get("rule_type")),
            safe_to_string(rule.get("message")),
            safe_to_string(rule.get("inferred_grain")),
            " ".join([safe_to_string(x) for x in rule.get("risk_notes", [])]),
        ]
    )


def recipe_to_search_text(recipe: Dict[str, Any]) -> str:
    """把 recipe 转成可检索文本。"""
    return " ".join(
        [
            recipe.get("recipe_id", ""),
            recipe.get("title", ""),
            recipe.get("analysis_type", ""),
            " ".join(recipe.get("main_tables", [])),
            recipe.get("query_sql", ""),
            recipe.get("insight", ""),
        ]
    )


def retrieve_context(question: str, kb: Dict[str, Any]) -> Dict[str, Any]:
    """
    从知识库中检索与问题相关的表、字段、规则和 recipe。
    """
    table_cards = kb["table_cards"]
    column_cards = kb["column_cards"]
    dq_rules = kb["dq_rules"]
    grain_rules = kb["grain_rules"]
    recipes = kb["recipes"]

    scored_tables = []
    for card in table_cards:
        score = score_text(question, table_card_to_search_text(card))
        if score > 0:
            scored_tables.append((score, card))

    scored_tables.sort(key=lambda x: x[0], reverse=True)

    if not scored_tables:
        default_table_names = {
            "dim_tenant",
            "dim_user",
            "fact_subscription",
            "dim_plan",
            "fact_feature_usage",
            "fact_daily_usage",
            "fact_actual_revenue",
            "fact_invoice",
            "fact_payment",
        }
        scored_tables = [
            (1, card)
            for card in table_cards
            if card.get("table_name") in default_table_names
        ]

    selected_tables = [card for _, card in scored_tables[:MAX_CONTEXT_TABLES]]
    selected_table_names = {card.get("table_name") for card in selected_tables}

    scored_columns = []
    for card in column_cards:
        table_name = card.get("table_name")
        base_score = 5 if table_name in selected_table_names else 0
        score = base_score + score_text(question, column_card_to_search_text(card))
        if score > 0:
            scored_columns.append((score, card))

    scored_columns.sort(key=lambda x: x[0], reverse=True)
    selected_columns = [card for _, card in scored_columns[:MAX_CONTEXT_COLUMNS]]

    scored_dq = []
    for rule in dq_rules:
        table_name = rule.get("table_name")
        base_score = 5 if table_name in selected_table_names else 0
        score = base_score + score_text(question, rule_to_search_text(rule))
        if score > 0:
            scored_dq.append((score, rule))

    scored_dq.sort(key=lambda x: x[0], reverse=True)
    selected_dq = [rule for _, rule in scored_dq[:MAX_CONTEXT_RULES]]

    scored_grain = []
    for rule in grain_rules:
        table_name = rule.get("table_name")
        base_score = 5 if table_name in selected_table_names else 0
        score = base_score + score_text(question, rule_to_search_text(rule))
        if score > 0:
            scored_grain.append((score, rule))

    scored_grain.sort(key=lambda x: x[0], reverse=True)
    selected_grain = [rule for _, rule in scored_grain[:MAX_CONTEXT_RULES]]

    scored_recipes = []
    for recipe in recipes:
        score = score_text(question, recipe_to_search_text(recipe))
        if score > 0:
            scored_recipes.append((score, recipe))

    scored_recipes.sort(key=lambda x: x[0], reverse=True)
    selected_recipes = [recipe for _, recipe in scored_recipes[:MAX_CONTEXT_RECIPES]]

    return {
        "candidate_tables": selected_tables,
        "candidate_columns": selected_columns,
        "dq_rules": selected_dq,
        "grain_rules": selected_grain,
        "recipes": selected_recipes,
    }


# =========================
# Prompt 构建
# =========================

def compact_table_context(tables: List[Dict[str, Any]]) -> str:
    """压缩表上下文。"""
    lines = []

    for table in tables:
        lines.append(f"Table: {table.get('table_name')}")
        lines.append(f"Rows: {table.get('row_count')}")
        lines.append("Columns:")

        for col in table.get("columns", []):
            lines.append(f"- {col.get('name')} ({col.get('type')})")

        if table.get("possible_primary_keys"):
            lines.append("Possible primary keys: " + ", ".join(table.get("possible_primary_keys", [])))

        if table.get("date_columns"):
            lines.append("Date columns: " + ", ".join(table.get("date_columns", [])))

        if table.get("measure_columns"):
            lines.append("Measure columns: " + ", ".join(table.get("measure_columns", [])))

        if table.get("dimension_columns"):
            lines.append("Dimension columns: " + ", ".join(table.get("dimension_columns", [])))

        lines.append("")

    return "\n".join(lines)


def compact_column_context(columns: List[Dict[str, Any]]) -> str:
    """压缩字段上下文。"""
    lines = []

    for col in columns:
        sample_values = col.get("sample_values", [])
        sample_text = ", ".join([safe_to_string(v) for v in sample_values[:5]])

        lines.append(
            f"- {col.get('table_name')}.{col.get('column_name')} "
            f"type={col.get('data_type')}, "
            f"null_rate={col.get('null_rate')}, "
            f"distinct_count={col.get('distinct_count')}, "
            f"min={col.get('min')}, max={col.get('max')}, "
            f"samples=[{sample_text}]"
        )

    return "\n".join(lines)


def compact_rules_context(rules: List[Dict[str, Any]]) -> str:
    """压缩 DQ / Grain 规则上下文。"""
    lines = []

    for rule in rules:
        message = rule.get("message")

        if message:
            table_name = rule.get("table_name")
            column_name = rule.get("column_name")
            if column_name:
                lines.append(f"- {table_name}.{column_name}: {message}")
            else:
                lines.append(f"- {table_name}: {message}")
        else:
            risk_notes = rule.get("risk_notes", [])
            lines.append(
                f"- {rule.get('table_name')}: "
                f"grain={rule.get('inferred_grain')}; "
                f"risk_notes={' | '.join(risk_notes)}"
            )

    return "\n".join(lines)


def compact_recipe_context(recipes: List[Dict[str, Any]]) -> str:
    """压缩 recipe 上下文。"""
    lines = []

    for recipe in recipes:
        query_sql = recipe.get("query_sql", "")
        insight = recipe.get("insight", "")

        if len(query_sql) > 1600:
            query_sql = query_sql[:1600] + "\n... SQL truncated ..."

        if len(insight) > 1000:
            insight = insight[:1000] + "\n... insight truncated ..."

        lines.append(f"Recipe: {recipe.get('title')}")
        lines.append(f"Analysis type: {recipe.get('analysis_type')}")
        lines.append(f"Main tables: {', '.join(recipe.get('main_tables', []))}")
        lines.append("SQL example:")
        lines.append(query_sql)
        lines.append("Notes:")
        lines.append(insight)
        lines.append("")

    return "\n".join(lines)


def build_sql_generation_prompt(question: str, context: Dict[str, Any]) -> str:
    """构建 SQL 生成 Prompt。"""

    table_context = compact_table_context(context["candidate_tables"])
    column_context = compact_column_context(context["candidate_columns"])
    dq_context = compact_rules_context(context["dq_rules"])
    grain_context = compact_rules_context(context["grain_rules"])
    recipe_context = compact_recipe_context(context["recipes"])

    prompt = f"""
You are a careful AI Data Analyst Agent.

Your task:
Generate ONE DuckDB SQL query to answer the user's question.

Hard rules:
1. Use DuckDB SQL only.
2. Use only tables and columns provided in the context.
3. Do not invent table names.
4. Do not invent column names.
5. Prefer simple, verifiable SQL.
6. If joining one-to-many tables, avoid duplicated aggregation.
7. If counting users or tenants after joins, prefer COUNT(DISTINCT ...).
8. If using fact_ai_usage_log.created_at, remember it is unix epoch seconds.
9. If using fact_nps_survey.score, filter N/A and use TRY_CAST.
10. Return JSON only. No markdown.

Required JSON format:
{{
  "analysis_plan": "brief plan in Chinese",
  "sql": "DuckDB SQL query",
  "used_tables": ["table1", "table2"],
  "used_columns": ["table.column"],
  "warnings": ["important warning"]
}}

User question:
{question}

Candidate tables:
{table_context}

Candidate columns:
{column_context}

Grain rules:
{grain_context}

Data quality rules:
{dq_context}

Similar recipes:
{recipe_context}
"""

    return prompt.strip()


def build_sql_repair_prompt(
    question: str,
    context: Dict[str, Any],
    bad_sql: str,
    error_message: str,
) -> str:
    """构建 SQL 修复 Prompt。"""

    base_prompt = build_sql_generation_prompt(question, context)

    repair_prompt = f"""
{base_prompt}

The previous SQL failed.

Failed SQL:
{bad_sql}

Error message:
{error_message}

Please repair the SQL.
Return JSON only, using the same required JSON format.
"""

    return repair_prompt.strip()


# =========================
# SiliconFlow 调用
# =========================

def call_siliconflow(prompt: str) -> str:
    """调用 SiliconFlow Chat Completions API。"""

    api_key = os.environ.get("SILICONFLOW_API_KEY", "").strip()
    api_url = os.environ.get("SILICONFLOW_API_URL", DEFAULT_SILICONFLOW_API_URL).strip()
    model = os.environ.get("SILICONFLOW_MODEL", DEFAULT_MODEL).strip()

    if not api_key:
        raise RuntimeError(
            "未找到 SILICONFLOW_API_KEY。请在 .env 中设置：SILICONFLOW_API_KEY=你的apikey"
        )

    payload = {
        "model": model,
        "messages": [
            {
                "role": "system",
                "content": (
                    "You are a precise SQL generation assistant. "
                    "You must return valid JSON only."
                ),
            },
            {
                "role": "user",
                "content": prompt,
            },
        ],
        "temperature": 0.1,
        "max_tokens": 2048,
    }

    data = json.dumps(payload).encode("utf-8")

    req = request.Request(
        api_url,
        data=data,
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
    """
    从模型输出中提取 JSON。
    兼容模型偶尔包裹 markdown 的情况。
    """
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


# =========================
# SQL 执行
# =========================

def clean_generated_sql(sql: str) -> str:
    """清理 SQL 文本。"""
    sql = sql.strip()
    sql = re.sub(r"^```sql\s*", "", sql, flags=re.IGNORECASE)
    sql = re.sub(r"^```\s*", "", sql)
    sql = re.sub(r"\s*```$", "", sql)
    return sql.strip()


def is_safe_select_sql(sql: str) -> bool:
    """
    安全检查：第一版只允许 SELECT / WITH 查询。
    """
    cleaned = sql.strip().lower()

    if not (cleaned.startswith("select") or cleaned.startswith("with")):
        return False

    forbidden_keywords = [
        " drop ",
        " delete ",
        " update ",
        " insert ",
        " alter ",
        " create ",
        " truncate ",
        " attach ",
        " copy ",
        " vacuum ",
    ]

    padded = " " + cleaned + " "
    return not any(keyword in padded for keyword in forbidden_keywords)


def add_limit_if_needed(sql: str, limit: int = MAX_RESULT_ROWS) -> str:
    """
    如果 SQL 没有 LIMIT，则追加 LIMIT。
    注意：对于聚合查询，LIMIT 不影响聚合结果，只限制返回行数。
    """
    sql_clean = sql.rstrip().rstrip(";")

    if re.search(r"\blimit\s+\d+\b", sql_clean, flags=re.IGNORECASE):
        return sql_clean + ";"

    return sql_clean + f"\nLIMIT {limit};"


def execute_sql(
    conn: duckdb.DuckDBPyConnection,
    sql: str,
) -> Tuple[bool, pd.DataFrame, str, str]:
    """
    执行 SQL。
    返回：
    success, dataframe, error_message, executed_sql
    """
    try:
        cleaned_sql = clean_generated_sql(sql)

        if not is_safe_select_sql(cleaned_sql):
            return False, pd.DataFrame(), "Only SELECT or WITH queries are allowed.", cleaned_sql

        executed_sql = add_limit_if_needed(cleaned_sql)
        df = conn.execute(executed_sql).df()

        return True, df, "", executed_sql

    except Exception as e:
        return False, pd.DataFrame(), str(e), clean_generated_sql(sql)


# =========================
# 日志
# =========================

def save_query_log(log_data: Dict[str, Any]) -> Path:
    """保存单次查询日志。"""
    ensure_dir(LOG_DIR)

    timestamp = time.strftime("%Y%m%d_%H%M%S")
    path = LOG_DIR / f"query_{timestamp}.json"

    write_json(path, log_data)
    return path


# =========================
# 输出展示
# =========================

def print_context_summary(context: Dict[str, Any]) -> None:
    """打印检索摘要。"""
    table_names = [t.get("table_name") for t in context["candidate_tables"]]

    print("\n[Context Retriever]")
    print("候选表：", ", ".join(table_names) if table_names else "无")

    if context["recipes"]:
        recipe_titles = [r.get("title") for r in context["recipes"]]
        print("相似参考题：", ", ".join(recipe_titles))


def print_answer(result_json: Dict[str, Any], df: pd.DataFrame, executed_sql: str) -> None:
    """打印最终结果。"""
    print("\n[Analysis Plan]")
    print(result_json.get("analysis_plan", ""))

    print("\n[Generated SQL]")
    print(executed_sql)

    print("\n[Used Tables]")
    used_tables = result_json.get("used_tables", [])
    if used_tables:
        for table in used_tables:
            print(f"- {table}")
    else:
        print("- 未提供")

    print("\n[Warnings]")
    warnings = result_json.get("warnings", [])
    if warnings:
        for item in warnings:
            print(f"- {item}")
    else:
        print("- 无明显风险提示")

    print("\n[Result Preview]")
    if df.empty:
        print("结果为空。")
    else:
        print(df.to_string(index=False))


# =========================
# Agent 主流程
# =========================

def answer_question(
    question: str,
    kb: Dict[str, Any],
    conn: duckdb.DuckDBPyConnection,
) -> None:
    """回答单个用户问题。"""

    context = retrieve_context(question, kb)
    print_context_summary(context)

    prompt = build_sql_generation_prompt(question, context)

    raw_output = call_siliconflow(prompt)
    result_json = extract_json_from_llm_output(raw_output)

    sql = clean_generated_sql(result_json.get("sql", ""))

    success, df, err, executed_sql = execute_sql(conn, sql)

    repair_output = None

    if not success and MAX_REPAIR_ATTEMPTS > 0:
        print("\n[SQL Execution Failed]")
        print(err)
        print("\n尝试修复 SQL...")

        repair_prompt = build_sql_repair_prompt(
            question=question,
            context=context,
            bad_sql=sql,
            error_message=err,
        )

        raw_repair_output = call_siliconflow(repair_prompt)
        repair_output = extract_json_from_llm_output(raw_repair_output)

        repaired_sql = clean_generated_sql(repair_output.get("sql", ""))
        success, df, err, executed_sql = execute_sql(conn, repaired_sql)

        if success:
            result_json = repair_output
            sql = repaired_sql

    if not success:
        print("\n[SQL Execution Failed After Repair]")
        print(err)

        log_path = save_query_log(
            {
                "question": question,
                "context_summary": {
                    "candidate_tables": [
                        t.get("table_name") for t in context["candidate_tables"]
                    ],
                    "recipe_titles": [
                        r.get("title") for r in context["recipes"]
                    ],
                },
                "llm_output": result_json,
                "repair_output": repair_output,
                "success": False,
                "error": err,
            }
        )

        print(f"\n查询日志已保存：{log_path}")
        return

    print_answer(result_json, df, executed_sql)

    log_path = save_query_log(
        {
            "question": question,
            "context_summary": {
                "candidate_tables": [
                    t.get("table_name") for t in context["candidate_tables"]
                ],
                "recipe_titles": [
                    r.get("title") for r in context["recipes"]
                ],
            },
            "analysis_plan": result_json.get("analysis_plan", ""),
            "sql": sql,
            "executed_sql": executed_sql,
            "used_tables": result_json.get("used_tables", []),
            "used_columns": result_json.get("used_columns", []),
            "warnings": result_json.get("warnings", []),
            "success": True,
            "result_row_count": len(df),
            "result_preview": df.head(MAX_RESULT_ROWS).to_dict(orient="records"),
        }
    )

    print(f"\n查询日志已保存：{log_path}")


# =========================
# CLI 主程序
# =========================

def main() -> None:
    """CLI 主入口。"""

    load_dotenv(ENV_PATH)
    check_required_files()

    model = os.environ.get("SILICONFLOW_MODEL", DEFAULT_MODEL).strip()
    api_url = os.environ.get("SILICONFLOW_API_URL", DEFAULT_SILICONFLOW_API_URL).strip()

    kb = load_knowledge_base()
    conn = duckdb.connect(str(DB_PATH))

    print("=" * 70)
    print("CloudWork AI Data Analyst Agent CLI")
    print("=" * 70)
    print(f"Model: {model}")
    print(f"API URL: {api_url}")
    print("输入自然语言问题，Agent 会生成 DuckDB SQL 并执行。")
    print("输入 exit / quit / q 退出。")
    print("")
    print("示例问题：")
    print("1. How many tenants are there by country?")
    print("2. How many new users registered each month from 2025-04 to 2026-03?")
    print("3. What is the total MRR by plan tier in October 2025?")
    print("=" * 70)

    try:
        while True:
            question = input("\nAsk a question: ").strip()

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