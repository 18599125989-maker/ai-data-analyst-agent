#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
构建 visualization knowledge。

输出：
- outputs/knowledge/visualization_rules.json

作用：
1. 为 Agent6 提供可扩展的图表推荐规则
2. 把 visualization 逻辑从 CLI 硬编码中抽离出来
3. 便于后续随着 sample question / answer 增加而持续扩充
"""

import json
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
OUTPUT_PATH = PROJECT_ROOT / "outputs" / "knowledge" / "visualization_rules.json"


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def build_visualization_rules() -> dict:
    return {
        "version": "v1",
        "description": "CloudWork AI 数据分析 Agent 的可视化推荐规则。",
        "chart_types": {
            "line": {
                "best_for": ["时间趋势", "连续时间变化", "按天/周/月趋势"],
                "required_roles": ["x=time", "y=metric"],
            },
            "bar": {
                "best_for": ["类别对比", "排名", "分布对比"],
                "required_roles": ["x=dimension", "y=metric"],
            },
            "table_only": {
                "best_for": ["明细行", "宽表结果", "没有数值指标"],
                "required_roles": [],
            },
        },
        "chart_rules": [
            {
                "rule_id": "time_series_default",
                "priority": 100,
                "question_keywords": ["趋势", "走势", "变化", "按月", "月度", "每日", "每周"],
                "result_shape": "time_series",
                "recommended_chart": "line",
                "reason_template": "问题目标是观察时间序列变化，折线图更适合展示趋势。",
            },
            {
                "rule_id": "distribution_default",
                "priority": 90,
                "question_keywords": ["分布", "占比", "排名", "对比", "头部", "拆解"],
                "result_shape": "categorical_comparison",
                "recommended_chart": "bar",
                "reason_template": "问题目标是比较类别间差异，柱状图更适合展示排序和对比。",
            },
            {
                "rule_id": "fallback_table",
                "priority": 10,
                "question_keywords": [],
                "result_shape": "tabular_answer",
                "recommended_chart": "table_only",
                "reason_template": "当前结果更适合表格展示。",
            },
        ],
        "render_constraints": {
            "max_categories_for_bar": 20,
            "max_series_for_grouped_chart": 8,
            "max_points_for_line": 200,
        },
    }


def main() -> None:
    ensure_dir(OUTPUT_PATH.parent)
    data = build_visualization_rules()
    OUTPUT_PATH.write_text(
        json.dumps(data, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    print(f"已生成：{OUTPUT_PATH}")


if __name__ == "__main__":
    main()
