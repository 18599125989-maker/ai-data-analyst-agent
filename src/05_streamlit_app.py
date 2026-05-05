#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import importlib.util
from pathlib import Path

import streamlit as st


PROJECT_ROOT = Path(__file__).resolve().parents[1]
AGENT_CLI_PATH = PROJECT_ROOT / "src" / "04_agent_cli.py"


def load_agent_module():
    spec = importlib.util.spec_from_file_location("agent_cli_module", AGENT_CLI_PATH)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


@st.cache_resource
def init_runtime():
    module = load_agent_module()
    module.load_dotenv(module.ENV_PATH)
    module.check_required_files()
    kb = module.load_knowledge_base()
    conn = module.duckdb.connect(str(module.DB_PATH))
    return module, kb, conn


def render_sidebar(module) -> None:
    st.sidebar.title("CloudWork AI 数据分析")
    st.sidebar.markdown("**当前前端可输出内容**")
    st.sidebar.markdown(
        "\n".join(
            [
                "- Agent1 问题理解结果",
                "- Agent2 候选表 / 候选 recipe",
                "- Agent3 grain 与 DQ 风险提示",
                "- Agent4 分析计划",
                "- Agent5 生成 SQL 与执行结果",
                "- Agent6 图表建议与已生成图表",
                "- 查询日志文件路径",
            ]
        )
    )
    st.sidebar.markdown("**知识底座**")
    st.sidebar.markdown(
        "\n".join(
            [
                "- `outputs/knowledge/retrieval_v2`",
                "- `outputs/knowledge/recipes.json`",
                "- `outputs/knowledge/visualization_rules.json`",
            ]
        )
    )
    st.sidebar.caption(f"数据库: `{module.DB_PATH.name}`")


def render_success(result: dict) -> None:
    result_json = result["result_json"]
    retrieval_context = result["retrieval_context"]
    guard = result["guard"]
    df = result["dataframe"]

    st.subheader("分析结果")
    st.markdown(f"**分析计划**：{result_json.get('analysis_plan', '')}")

    st.subheader("结果数据")
    st.dataframe(df, use_container_width=True, hide_index=True)

    if result.get("visualization_path"):
        st.subheader("图表")
        st.image(result["visualization_path"], use_container_width=True)

    st.subheader("SQL")
    st.code(result["executed_sql"], language="sql")

    col1, col2 = st.columns(2)
    with col1:
        st.subheader("使用表")
        st.write(result_json.get("used_tables", []))
    with col2:
        st.subheader("风险提示")
        warnings = result_json.get("warnings", [])
        if warnings:
            for item in warnings:
                st.warning(item)
        else:
            st.info("无明显风险提示")

    with st.expander("Agent1 问题理解", expanded=False):
        st.json(result["task"], expanded=False)

    with st.expander("Agent2 候选知识", expanded=False):
        st.markdown("**候选表**")
        st.write(retrieval_context["selected_table_names"])
        st.markdown("**参考 recipes**")
        st.write(
            [
                item.get("name") or item.get("title") or item.get("recipe_id")
                for item in retrieval_context["candidate_recipes"]
            ]
        )

    with st.expander("Agent3 守卫规则", expanded=False):
        for item in guard["guardrails"]:
            st.write(f"- {item}")
        if guard["warnings"]:
            st.markdown("**风险**")
            for item in guard["warnings"]:
                st.warning(item)

    with st.expander("Agent6 可视化配置", expanded=False):
        st.json(result["visualization_spec"], expanded=False)

    st.caption(f"查询日志：{result['log_path']}")


def render_failure(result: dict) -> None:
    st.error("SQL 生成或执行失败")
    st.write(result.get("error", "未知错误"))

    with st.expander("Agent1 问题理解", expanded=False):
        st.json(result["task"], expanded=False)

    with st.expander("Agent2 / Agent3 上下文", expanded=False):
        st.write(result["retrieval_context"]["selected_table_names"])
        for item in result["guard"]["warnings"]:
            st.warning(item)

    with st.expander("模型输出", expanded=False):
        st.json(result.get("result_json", {}), expanded=False)

    st.caption(f"查询日志：{result['log_path']}")


def main() -> None:
    st.set_page_config(
        page_title="CloudWork AI 数据分析",
        page_icon="📊",
        layout="wide",
    )
    st.title("CloudWork AI 数据分析前端")
    st.caption("中文问题输入 -> 多 Agent 编排 -> SQL -> 结果 -> 可视化")

    module, kb, conn = init_runtime()
    render_sidebar(module)

    default_question = "统计 2025-10 各套餐的总 MRR 对比，并给一个合适的图表"
    question = st.text_area("请输入问题", value=default_question, height=120)

    if st.button("开始分析", type="primary", use_container_width=True):
        if not question.strip():
            st.warning("请输入问题")
            return

        with st.spinner("Agent 正在分析问题、生成 SQL 并执行..."):
            result = module.run_question_pipeline(question.strip(), kb, conn)

        if result["success"]:
            render_success(result)
        else:
            render_failure(result)


if __name__ == "__main__":
    main()
