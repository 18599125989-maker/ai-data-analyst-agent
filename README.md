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
- 在结构化检索上叠加 embedding，形成 hybrid retrieval
- 在 SQL 生成前先做 grain / DQ 防护
- 在 SQL 执行后补充引用校验、结果校验与单轮 repair
- 用 answer memory 记录成功案例，作为后续相似问题的弱参考
- 最终输出不仅有 SQL，还有分析计划、风险提示、结果预览、图表建议与 lineage

项目定位更接近“可解释的数据分析 Agent”，而不是单纯的“SQL 生成器”。

---

## 2. 核心功能

- 支持中文自然语言提问，自动生成 DuckDB SQL
- 基于知识库检索相关表、字段、指标、JOIN 路径和参考 recipe
- 支持 hybrid retrieval：保留关键词检索，并用 embedding 增强 table / field / metric / recipe 的语义召回
- 在 SQL 生成前自动加入 grain / DQ 约束，降低错误聚合风险
- 支持 SQL 执行失败后的自动修复
- 支持 SQL 引用校验与结果校验，减少“能执行但结果明显不合理”的情况
- 支持 answer memory，将成功分析沉淀为弱参考历史案例
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
   - 对 table / field / metric / recipe 使用 hybrid retrieval
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
   - 若 SQL 引用校验或结果校验未通过，也会进入 repair

6. `Agent6: Answer + Visualization`
   - 输出结果表
   - 根据结果和规则生成图表
   - 生成 lineage，保存 query log
   - 异步写入 answer memory，不阻塞主结果返回

### 3.2 当前可输出内容

无论在 CLI 还是 Streamlit 前端，当前系统都可以输出：
- 用户问题的结构化理解结果
- 候选表 / 候选 recipe
- 候选历史相似案例（若 answer memory 可用）
- Grain 与数据质量风险提示
- 中文分析计划
- 生成后的 SQL
- SQL 执行结果预览
- 图表建议与已生成图表
- lineage 摘要
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
- embedding retrieval 层
- answer memory 层
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
8. 生成 embedding index
9. 编译 visualization 规则
10. 按需构建 answer memory index

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
  - 上游 recipe 构建产物

- `retrieval_v2/recipes.json`
  - Agent 运行时使用的完整 recipe 集合
  - 与 `recipe_index.json` 配合使用：先检索索引，再按 `recipe_id` 回填完整 recipe

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
    - `recipes.json`
    - `table_embedding_index.json`
    - `field_embedding_index.json`
    - `metric_embedding_index.json`
    - `recipe_embedding_index.json`
    - `answer_memory_index.json`
    - `answer_memory_embedding_index.json`

- `memory/answer_memory.jsonl`
  - 成功查询后异步追加的历史案例记忆
  - 用于后续相似问题的弱参考检索

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

当前实现中：
- `table / field / metric / recipe` 支持 hybrid retrieval
- `join / trap / policy` 继续保持结构化关键词检索，避免语义相关但结构错误的 JOIN 进入 SQL

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

- [src/03_8_build_embedding_indexes.py](/Users/zhang/Desktop/飞书比赛/src/03_8_build_embedding_indexes.py)
  - 为 table / field / metric / recipe 构建 embedding index

- [src/03_8_build_recipe_knowledge.py](/Users/zhang/Desktop/飞书比赛/src/03_8_build_recipe_knowledge.py)
  - 构建结构化 recipe 知识

- [src/03_7_build_visualization_knowledge.py](/Users/zhang/Desktop/飞书比赛/src/03_7_build_visualization_knowledge.py)
  - 生成可视化规则文件

### 5.3 Agent 与前端

- [src/04_agent_cli.py](/Users/zhang/Desktop/飞书比赛/src/04_agent_cli.py)
  - 主 Agent 流程
  - 包含多 Agent 编排、hybrid retrieval、SQL 执行、repair、validator、lineage、answer memory 与日志保存

- [src/05_streamlit_app.py](/Users/zhang/Desktop/飞书比赛/src/05_streamlit_app.py)
  - Streamlit 演示前端
  - 用于中文问题输入、结果展示和图表展示

- [src/06_lineage.py](/Users/zhang/Desktop/飞书比赛/src/06_lineage.py)
  - 查询血缘构建

- [src/08_build_answer_memory_index.py](/Users/zhang/Desktop/飞书比赛/src/08_build_answer_memory_index.py)
  - 将 answer memory 编译成检索索引

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
│   ├── memory/
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

### 7.2 环境变量配置

当前 LLM 调用默认接入 SiliconFlow 兼容接口。  
请在项目根目录创建 `.env` 文件。除了 API Key，数据目录和 DuckDB 路径也支持通过环境变量覆盖：

```bash
SILICONFLOW_API_KEY=你的APIKey
SILICONFLOW_API_URL=https://api.siliconflow.cn/v1/chat/completions
SILICONFLOW_MODEL=Qwen/Qwen2.5-72B-Instruct
SILICONFLOW_EMBEDDING_API_URL=https://api.siliconflow.cn/v1/embeddings
SILICONFLOW_EMBEDDING_MODEL=BAAI/bge-m3
CSV_DIR=for_contestants/csv
DUCKDB_PATH=cloudwork.duckdb
```

说明：

- `SILICONFLOW_API_KEY`：必填，用于调用 LLM 接口
- `SILICONFLOW_API_URL`、`SILICONFLOW_MODEL`：可选，不填时使用代码中的默认值
- `SILICONFLOW_EMBEDDING_API_URL`、`SILICONFLOW_EMBEDDING_MODEL`：可选，用于 hybrid retrieval 的 embedding 构建与查询
- `CSV_DIR`：可选，CSV 数据目录；默认值为项目根目录下的 `for_contestants/csv`
- `DUCKDB_PATH`：可选，DuckDB 数据库文件路径；默认值为项目根目录下的 `cloudwork.duckdb`
- `CSV_DIR` 和 `DUCKDB_PATH` 支持相对路径或绝对路径；相对路径会按项目根目录解析

### 7.3 快速启动

如果仓库中的数据库和知识库产物已经存在，可直接启动：

```bash
python3 src/04_agent_cli.py
```

或启动 Streamlit 前端：

```bash
streamlit run src/05_streamlit_app.py
```

### 7.4 完整复现

如果需要从原始 CSV 全量重建，请按以下顺序执行：

```bash
python3 src/00_load_data.py
python3 src/01_profile_tables.py
python3 src/02_sample_questions.py
python3 src/03_build_knowledge_base.py
python3 src/03_6_build_retrieval_indexes_from_cards_v2.py
python3 src/03_8_build_recipe_knowledge.py
python3 src/03_8_build_embedding_indexes.py
python3 src/03_7_build_visualization_knowledge.py
```

然后再启动：

```bash
streamlit run src/05_streamlit_app.py
```

### 7.5 可选步骤

如果需要提前构建 answer memory 检索索引，可额外执行：

```bash
python3 src/08_build_answer_memory_index.py
```

---

## 8. 项目亮点总结

- 面向中文业务分析场景设计
- 采用多 Agent 架构，职责清晰
- 采用 hybrid retrieval，在结构化检索基础上增强语义召回
- 强调 grain / DQ / JOIN 风险控制
- 通过 table card + validation + retrieval_v2 形成可复用知识底座
- 支持 SQL repair、结果校验、lineage、answer memory 和前端演示

如果只用一句话概括本项目：

**这是一个以知识库为核心、面向中文业务分析的可解释 Text-to-SQL Agent 系统。**

---

## Good-to-have Extensions

### 1. 数据血缘溯源 Lineage Tracking

当前项目已实现基础版数据血缘溯源能力，可在每次分析后：
- 展示本次分析使用的表和字段
- 展示 SQL 执行路径
- 展示 JOIN / GROUP BY / LIMIT 等 SQL 特征
- 展示结果列 schema
- 展示图表 x / y 映射
- 将 lineage 写入 query log，便于复盘和审计

后续可继续扩展：
- 更精确的 SQL parser
- 字段级 lineage
- 指标级来源追踪
- 可视化 DAG / ER 图
- 点击结论查看来源 SQL 和字段

### 2. 全局 ER 图 / 知识图谱可视化

系统支持基于知识库自动生成全局 ER / Knowledge Graph。
- 表作为节点，JOIN 关系作为边。
- 节点展示表类型、grain、字段数量、关键字段和风险级别。
- 边展示 join_condition、relationship 和 risk_level。
- 该图不是写死的，而是由 `table_index.json`、`join_index.json`、`field_index.json`、`table_cards.json`、`column_cards.json` 自动生成。
- 前端提供“显示全局 ER 图 / 知识图谱”入口，点击后可查看整体数据结构。
- 如果 pyvis 可用，展示交互式 HTML 图；否则展示 JSON summary fallback。

运行方式：

```bash
python src/08_build_knowledge_graph.py
```

然后：

```bash
streamlit run src/05_streamlit_app.py
```

在侧边栏勾选：

`显示全局 ER 图 / 知识图谱`

### 3. 多轮追问与上下文记忆 Follow-up Context

系统支持基础版多轮追问能力。
- 在同一 Streamlit session 中，系统会保存上一轮成功分析的问题、SQL、使用表、使用字段、结果列、结果预览、图表配置和 lineage。
- 用户可以手动勾选“将本次问题作为上一轮结果的追问处理”，基于上一轮结果继续提问。
- 当前支持的追问修改类型包括：
  - 过滤条件：只看某国家、某状态、某客户
  - Top N 修改：前 10 改前 20
  - 排序修改：升序、降序
  - 图表类型修改：柱状图、折线图、表格
  - 维度拆分：按行业、套餐、国家等维度重新分组
  - 时间条件 / 时间维度：只看某个月、按月 / 按天、最近 30 天
- 第一版只保存上一轮上下文，避免长期历史污染 SQL 生成。

### 4. Answer Memory 持续学习

系统支持轻量的 answer memory：
- 成功查询后，会异步生成一条保守的历史案例描述，写入 `outputs/memory/answer_memory.jsonl`
- 后续可编译成 `answer_memory_index.json` 与 `answer_memory_embedding_index.json`
- Agent2 最多只检索 1 条历史相似案例，作为 SQL Planner 的弱参考
- answer memory 不会覆盖 table / field / metric / join / trap / policy，也不会覆盖 Agent3 guardrails
