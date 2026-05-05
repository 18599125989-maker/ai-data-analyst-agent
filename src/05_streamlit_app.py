#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import importlib.util
import json
from pathlib import Path

import streamlit as st
import streamlit.components.v1 as components


PROJECT_ROOT = Path(__file__).resolve().parents[1]
AGENT_CLI_PATH = PROJECT_ROOT / "src" / "04_agent_cli.py"
GRAPH_HTML_PATH = PROJECT_ROOT / "outputs" / "knowledge" / "knowledge_graph.html"
GRAPH_JSON_PATH = PROJECT_ROOT / "outputs" / "knowledge" / "knowledge_graph.json"


def load_agent_module():
    spec = importlib.util.spec_from_file_location("agent_cli_module", AGENT_CLI_PATH)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


@st.cache_resource
def init_runtime(agent_cli_mtime_ns: int):
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
                "- `outputs/knowledge/retrieval_v2/recipes.json`",
                "- `outputs/knowledge/visualization_rules.json`",
            ]
        )
    )
    st.sidebar.caption(f"数据库: `{module.DB_PATH.name}`")


def render_global_er_graph() -> None:
    if GRAPH_HTML_PATH.exists():
        html = GRAPH_HTML_PATH.read_text(encoding="utf-8")
        components.html(html, height=700, scrolling=True)
        return

    if GRAPH_JSON_PATH.exists():
        payload = json.loads(GRAPH_JSON_PATH.read_text(encoding="utf-8"))
        summary = payload.get("summary", {})
        st.markdown("**图谱摘要**")
        st.write(summary)
        st.markdown("**节点数量 / 边数量**")
        st.write(
            {
                "nodes": len(payload.get("nodes", [])),
                "edges": len(payload.get("edges", [])),
            }
        )
        st.markdown("**JSON Fallback**")
        st.json(payload, expanded=False)
        return

    st.info("尚未生成知识图谱，请先运行：python src/08_build_knowledge_graph.py")


def render_success(result: dict) -> None:
    result_json = result["result_json"]
    retrieval_context = result["retrieval_context"]
    guard = result["guard"]
    df = result["dataframe"]

    st.subheader("分析结果")
    if result.get("previous_context_used"):
        st.info("本次分析已使用上一轮上下文。")
    followup_edit_types = result.get("task", {}).get("followup_edit_types", []) or []
    if followup_edit_types:
        st.caption(f"识别到的追问修改类型：{', '.join(followup_edit_types)}")
    st.markdown(f"**分析计划**：{result_json.get('analysis_plan', '')}")

    st.subheader("结果数据")
    st.dataframe(df, use_container_width=True, hide_index=True)

    if result.get("visualization_path"):
        st.subheader("图表")
        st.image(result["visualization_path"], use_container_width=True)

    st.subheader("SQL")
    st.code(result["executed_sql"], language="sql")

    with st.expander("数据血缘 / SQL 执行路径", expanded=False):
        lineage = result.get("lineage", {})
        if not lineage:
            st.info("当前结果未生成 lineage 信息")
        else:
            st.markdown("**使用表**")
            st.write(lineage.get("used_tables", []))

            st.markdown("**使用字段**")
            st.write(lineage.get("used_columns", []))

            st.markdown("**指标口径**")
            st.write(lineage.get("metrics", []))

            st.markdown("**SQL 特征**")
            st.json(lineage.get("sql_path", {}), expanded=False)

            st.markdown("**结果列 Schema**")
            st.write(lineage.get("result_schema", []))

            st.markdown("**可视化映射**")
            st.json(lineage.get("visualization", {}), expanded=False)

            st.markdown("**血缘摘要**")
            for item in lineage.get("lineage_summary", []):
                st.write(f"- {item}")

            st.markdown("**完整 Lineage JSON**")
            st.json(lineage, expanded=False)

    with st.expander("上一轮上下文 / Follow-up Context", expanded=False):
        previous_context = result.get("previous_context", {}) or {}
        if not result.get("previous_context_used") or not previous_context:
            st.info("本次未使用上一轮上下文")
        else:
            st.markdown("**上一轮问题**")
            st.write(previous_context.get("previous_question", ""))
            st.markdown("**上一轮 SQL**")
            st.code(previous_context.get("previous_sql", ""), language="sql")
            st.markdown("**上一轮结果列**")
            st.write(previous_context.get("previous_result_columns", []))
            st.markdown("**上一轮图表配置**")
            st.json(previous_context.get("previous_visualization_spec", {}), expanded=False)

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

    if "last_analysis_context" not in st.session_state:
        st.session_state["last_analysis_context"] = None

    agent_cli_mtime_ns = AGENT_CLI_PATH.stat().st_mtime_ns if AGENT_CLI_PATH.exists() else 0
    module, kb, conn = init_runtime(agent_cli_mtime_ns)
    render_sidebar(module)
    show_er_graph = st.sidebar.checkbox("显示全局 ER 图 / 知识图谱", value=False)

    if show_er_graph:
        st.subheader("全局 ER 图 / 知识图谱")
        render_global_er_graph()
        st.divider()

    default_question = "统计 2025-10 各套餐的总 MRR 对比，并给一个合适的图表"
    question = st.text_area("请输入问题", value=default_question, height=120)
    last_context = st.session_state.get("last_analysis_context")

    st.markdown("**追问模式**")
    if last_context:
        st.success("已保存上一轮分析上下文")
        st.caption(f"上一轮问题：{last_context.get('previous_question', '')}")
        st.caption(
            "上一轮使用表："
            + (", ".join(last_context.get("previous_used_tables", [])) or "无")
        )
        st.caption(
            "上一轮结果列："
            + (", ".join(last_context.get("previous_result_columns", [])) or "无")
        )
    else:
        st.info("当前还没有可用于追问的上一轮成功分析结果。")

    use_previous_context = st.checkbox(
        "将本次问题作为上一轮结果的追问处理",
        value=False,
        disabled=not bool(last_context),
        help="适合用于：只看某条件、改 Top N、改排序、换图表、按新维度拆分、增加时间条件。",
    )

    if st.button("清除上一轮上下文", use_container_width=True):
        st.session_state["last_analysis_context"] = None
        st.rerun()

    if st.button("开始分析", type="primary", use_container_width=True):
        if not question.strip():
            st.warning("请输入问题")
            return

        previous_context = last_context if use_previous_context else None

        with st.spinner("Agent 正在分析问题、生成 SQL 并执行..."):
            result = module.run_question_pipeline(
                question.strip(),
                kb,
                conn,
                previous_context=previous_context,
            )

        if result["success"]:
            st.session_state["last_analysis_context"] = module.build_analysis_context(result)
            render_success(result)
        else:
            render_failure(result)


if __name__ == "__main__":
    main()
