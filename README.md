# CloudWork AI 数据分析 Agent

一个面向中文业务问题的 Text-to-SQL 数据分析项目。  
项目目标不是“直接让大模型猜 SQL”，而是先构建可检索、可约束、可解释的业务知识库，再通过多 Agent 流程完成：

`用户问题 -> 任务理解 -> 知识检索 -> Grain / DQ 守卫 -> SQL 生成 -> SQL 执行 / 修复 -> 结果回答 / 可视化`

本项目同时提供：
- 命令行版本：[src/04_agent_cli.py](/Users/zhang/Desktop/飞书比赛/src/04_agent_cli.py)
- Streamlit 前端：[src/05_streamlit_app.py](/Users/zhang/Desktop/飞书比赛/src/05_streamlit_app.py)

---

## 1. 项目简介

在真实 BI / 数据分析场景中，Text-to-SQL 的主要难点通常不是 SQL 语法本身，而是：
- 业务口径不清
- 表粒度（grain）容易混淆
- 一对多 JOIN 容易导致重复统计
- 指标与事实表混用，导致结论错误
- 纯 schema 不足以支撑中文业务问答

因此，本项目采用“知识增强的多 Agent 数据分析架构”：
- 用离线 profiling + table card + validation 先把数据库知识结构化
- 用 retrieval_v2 把表、字段、指标、JOIN、陷阱、策略、recipe 编译成可检索索引
- 在 SQL 生成前先做 grain / DQ 防护
- 最终输出不仅有 SQL，还有分析计划、风险提示、结果预览和图表建议

项目定位更接近“可解释的数据分析 Agent”，而不是单纯的“SQL 生成器”。

---

## 2. 核心功能

- 支持中文自然语言提问，自动生成 DuckDB SQL
- 基于知识库检索相关表、字段、指标、JOIN 路径和参考 recipe
- 在 SQL 生成前自动加入 grain / DQ 约束，降低错误聚合风险
- 支持 SQL 执行失败后的自动修复
- 输出分析计划、SQL、结果表、风险提示和图表建议
- 支持将查询结果保存为日志，便于复盘和评审展示
- 支持 Streamlit 前端，便于比赛演示

---

## 3. 整体架构

### 3.1 Query -> Agent -> Output

当前主流程如下：

1. `Agent1: Task Understanding`
   - 识别用户问题中的时间范围、结果形态、是否需要可视化
   - 输出结构化任务理解结果

2. `Agent2: Knowledge Retriever`
   - 从 `outputs/knowledge/retrieval_v2/` 中检索相关表、字段、指标、JOIN、recipe
   - 为后续 SQL 生成提供业务上下文，而不是只给 schema

3. `Agent3: Grain & DQ Guard`
   - 根据 table card、validation、trap、policy 生成约束
   - 明确提醒哪些统计必须 `COUNT(DISTINCT ...)`
   - 明确哪些 JOIN 路径存在放大风险

4. `Agent4: SQL Planner + Generator`
   - 基于中文问题和检索上下文生成 DuckDB SQL
   - 同时产出中文分析计划、风险提示、可视化建议

5. `Agent5: SQL Executor + Repair`
   - 执行 SQL
   - 若失败，则携带错误信息触发一次修复

6. `Agent6: Answer + Visualization`
   - 输出结果表
   - 根据结果和规则生成图表
   - 保存 query log 便于展示与复盘

### 3.2 当前可输出内容

无论在 CLI 还是 Streamlit 前端，当前系统都可以输出：
- 用户问题的结构化理解结果
- 候选表 / 候选 recipe
- Grain 与数据质量风险提示
- 中文分析计划
- 生成后的 SQL
- SQL 执行结果预览
- 图表建议与已生成图表
- 查询日志路径

---

## 4. 知识库设计

### 4.1 为什么要做知识库

如果只把数据库 schema 给大模型，模型很容易出现以下问题：
- 只看字段名猜业务含义
- 把记录数当实体数
- 把 MRR、实收、发票、付款等不同口径混在一起
- 不知道哪些字段可 JOIN、哪些 JOIN 会放大

所以本项目将“数据库知识”拆成多层：
- 原始 schema 层
- 统计 profiling 层
- 业务 table card 层
- validation / audit 层
- retrieval index 层
- visualization rule 层

### 4.2 知识库构建流程

整体流程如下：

1. 载入原始 CSV，写入 DuckDB
2. 对表和字段做 profiling
3. 生成 sample question / answer，沉淀基础 recipe
4. 生成初版 knowledge base
5. 编写 / 校验 table card
6. 对 table card 做审计与 validation
7. 编译 retrieval_v2 检索索引
8. 编译 visualization 规则

### 4.3 知识库的组成

核心知识文件位于 `outputs/knowledge/`：

- `schema_raw.json`
  - 纯 schema 信息

- `table_cards.json`
  - 表级画像与基础业务描述

- `column_cards.json`
  - 字段级统计信息与样例值

- `dq_rules.json`
  - 数据质量规则

- `grain_rules.json`
  - 粒度与聚合风险规则

- `recipes.json`
  - 参考问答 / SQL 样例，用于 few-shot 与检索

- `retrieval_v2/`
  - 面向 Agent 的核心检索底座
  - 包含：
    - `table_index.json`
    - `field_index.json`
    - `metric_index.json`
    - `join_index.json`
    - `trap_index.json`
    - `policy_index.json`
    - `recipe_index.json`

- `visualization_rules.json`
  - 可视化推荐规则

### 4.4 retrieval_v2 的作用

`retrieval_v2` 是当前项目最关键的一层知识编译结果。  
它不是简单把 table card 原文拼接起来，而是拆成多个面向 Agent 的检索视角：

- `table_index`：表的业务含义、grain、指标、目标 JOIN 表
- `field_index`：字段语义、引用关系、是否时间/指标/维度
- `metric_index`：推荐指标表达式与使用说明
- `join_index`：推荐 JOIN 路径与风险等级
- `trap_index`：常见误用方式与规避建议
- `policy_index`：可转化为 prompt 约束的策略标记
- `recipe_index`：可复用的分析问题模式

这使得 Agent 在回答中文业务问题时，不再是“猜”，而是“先检索、再约束、后生成”。

---

## 5. 关键文件说明

为了方便评委阅读，这里只列关键文件，不展开所有中间开发脚本。

### 5.1 数据准备与基础知识构建

- [src/00_load_data.py](/Users/zhang/Desktop/飞书比赛/src/00_load_data.py)
  - 将原始数据导入 DuckDB

- [src/01_profile_tables.py](/Users/zhang/Desktop/飞书比赛/src/01_profile_tables.py)
  - 生成表级 / 字段级 profiling

- [src/02_sample_questions.py](/Users/zhang/Desktop/飞书比赛/src/02_sample_questions.py)
  - 生成参考问题、SQL、结果和图表

- [src/03_build_knowledge_base.py](/Users/zhang/Desktop/飞书比赛/src/03_build_knowledge_base.py)
  - 构建第一版结构化知识库

### 5.2 Table Card 与知识增强

- `knowledge_manual/table_cards/*.yaml`
  - 每张表的人工增强 card
  - 包含业务含义、grain、JOIN、指标、陷阱等信息

- `src/03_2_validate_*_card.py`
  - 一批表级 validation 脚本
  - 用于校验 card 中的关键业务假设是否与数据一致

- [src/03_5_audit_table_cards.py](/Users/zhang/Desktop/飞书比赛/src/03_5_audit_table_cards.py)
  - 对 table card 做统一审计

- [src/03_6_build_retrieval_indexes_from_cards_v2.py](/Users/zhang/Desktop/飞书比赛/src/03_6_build_retrieval_indexes_from_cards_v2.py)
  - 从正式 table card 编译出 `retrieval_v2`

- [src/03_7_build_visualization_knowledge.py](/Users/zhang/Desktop/飞书比赛/src/03_7_build_visualization_knowledge.py)
  - 生成可视化规则文件

### 5.3 Agent 与前端

- [src/04_agent_cli.py](/Users/zhang/Desktop/飞书比赛/src/04_agent_cli.py)
  - 主 Agent 流程
  - 包含多 Agent 编排、SQL 执行、修复、日志保存

- [src/05_streamlit_app.py](/Users/zhang/Desktop/飞书比赛/src/05_streamlit_app.py)
  - Streamlit 演示前端
  - 用于中文问题输入、结果展示和图表展示

---

## 6. 文件结构概览

```text
.
├── src/
│   ├── 00_load_data.py
│   ├── 01_profile_tables.py
│   ├── 02_sample_questions.py
│   ├── 03_build_knowledge_base.py
│   ├── 03_2_validate_*_card.py
│   ├── 03_5_audit_table_cards.py
│   ├── 03_6_build_retrieval_indexes_from_cards_v2.py
│   ├── 03_7_build_visualization_knowledge.py
│   ├── 04_agent_cli.py
│   └── 05_streamlit_app.py
├── knowledge_manual/
│   └── table_cards/
├── outputs/
│   ├── knowledge/
│   │   ├── retrieval_v2/
│   │   ├── validation/
│   │   ├── recipes.json
│   │   └── visualization_rules.json
│   ├── sample/
│   └── logs/
├── cloudwork.duckdb
├── requirements.txt
└── README.md
```

---

## 7. 环境配置与启动方式

### 7.1 Python 依赖

本项目依赖见 [requirements.txt](/Users/zhang/Desktop/飞书比赛/requirements.txt)。

建议使用虚拟环境：

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 7.2 API Key 配置

当前 LLM 调用默认接入 SiliconFlow 兼容接口。  
请在项目根目录创建 `.env` 文件，例如：

```bash
SILICONFLOW_API_KEY=你的APIKey
SILICONFLOW_API_URL=https://api.siliconflow.cn/v1/chat/completions
SILICONFLOW_MODEL=Qwen/Qwen2.5-72B-Instruct
```

### 7.3 从零开始运行

建议按以下顺序执行：

```bash
python3 src/00_load_data.py
python3 src/01_profile_tables.py
python3 src/02_sample_questions.py
python3 src/03_build_knowledge_base.py
python3 src/03_6_build_retrieval_indexes_from_cards_v2.py
python3 src/03_7_build_visualization_knowledge.py
```

### 7.4 启动 CLI

```bash
python3 src/04_agent_cli.py
```

### 7.5 启动 Streamlit 前端

```bash
streamlit run src/05_streamlit_app.py
```

---

## 8. 项目亮点总结

- 面向中文业务分析场景设计
- 采用多 Agent 架构，职责清晰
- 强调 grain / DQ / JOIN 风险控制
- 通过 table card + validation + retrieval_v2 形成可复用知识底座
- 支持 SQL 修复、结果可视化和前端演示

如果只用一句话概括本项目：

**这是一个以知识库为核心、面向中文业务分析的可解释 Text-to-SQL Agent 系统。**
