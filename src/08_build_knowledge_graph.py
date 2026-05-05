#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, Iterable, List


ROOT = Path(__file__).resolve().parents[1]
KNOWLEDGE_DIR = ROOT / "outputs" / "knowledge"
RETRIEVAL_DIR = KNOWLEDGE_DIR / "retrieval_v2"

TABLE_INDEX_PATH = RETRIEVAL_DIR / "table_index.json"
JOIN_INDEX_PATH = RETRIEVAL_DIR / "join_index.json"
FIELD_INDEX_PATH = RETRIEVAL_DIR / "field_index.json"
TABLE_CARDS_PATH = KNOWLEDGE_DIR / "table_cards.json"
COLUMN_CARDS_PATH = KNOWLEDGE_DIR / "column_cards.json"

GRAPH_JSON_PATH = KNOWLEDGE_DIR / "knowledge_graph.json"
GRAPH_HTML_PATH = KNOWLEDGE_DIR / "knowledge_graph.html"


def load_json(path: Path, default):
    if not path.exists():
        print(f"warning: file not found: {path}")
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        print(f"warning: failed to parse json: {path} error={exc}")
        return default


def infer_table_type(table_name: str, table_item: dict) -> str:
    name = str(table_name or "").lower()
    if name.startswith("dim_"):
        return "dimension"
    if name.startswith("fact_"):
        return "fact"
    if any(token in name for token in ["mapping", "map", "bridge"]):
        return "mapping"
    return "table"


def _safe_str(value: Any) -> str:
    if value is None:
        return ""
    return str(value)


def _safe_list(value: Any) -> List[Any]:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    if isinstance(value, tuple):
        return list(value)
    return [value]


def _unique_keep_order(items: Iterable[str]) -> List[str]:
    result = []
    seen = set()
    for item in items:
        key = _safe_str(item)
        if not key or key in seen:
            continue
        seen.add(key)
        result.append(key)
    return result


def _collect_table_card_lookup(table_cards: Any) -> Dict[str, Dict[str, Any]]:
    lookup: Dict[str, Dict[str, Any]] = {}
    if isinstance(table_cards, list):
        for item in table_cards:
            if isinstance(item, dict) and item.get("table_name"):
                lookup[str(item["table_name"])] = item
    elif isinstance(table_cards, dict):
        for key, value in table_cards.items():
            if isinstance(value, dict):
                lookup[str(key)] = value
    return lookup


def _collect_column_card_fields(column_cards: Any) -> Dict[str, List[str]]:
    fields_by_table: Dict[str, List[str]] = {}
    if isinstance(column_cards, list):
        for item in column_cards:
            if not isinstance(item, dict):
                continue
            table_name = _safe_str(item.get("table_name"))
            field_name = _safe_str(item.get("column_name") or item.get("field_name") or item.get("name"))
            if table_name and field_name:
                fields_by_table.setdefault(table_name, []).append(field_name)
    elif isinstance(column_cards, dict):
        for table_name, value in column_cards.items():
            fields = []
            if isinstance(value, dict):
                fields = _safe_list(value.get("columns") or value.get("fields"))
            elif isinstance(value, list):
                fields = value
            for field in fields:
                if isinstance(field, dict):
                    field_name = _safe_str(field.get("column_name") or field.get("field_name") or field.get("name"))
                else:
                    field_name = _safe_str(field)
                if table_name and field_name:
                    fields_by_table.setdefault(str(table_name), []).append(field_name)
    return {k: _unique_keep_order(v) for k, v in fields_by_table.items()}


def build_nodes(table_index, field_index, table_cards, column_cards) -> list[dict]:
    table_card_lookup = _collect_table_card_lookup(table_cards)
    column_card_fields = _collect_column_card_fields(column_cards)
    field_items = [x for x in _safe_list(field_index) if isinstance(x, dict)]

    by_table: Dict[str, Dict[str, Any]] = {}

    for item in _safe_list(table_index):
        if not isinstance(item, dict):
            continue
        table_name = _safe_str(item.get("table_name") or item.get("name"))
        if not table_name:
            continue
        by_table[table_name] = {
            "table_item": item,
            "fields": [],
        }

    for item in field_items:
        table_name = _safe_str(item.get("table_name") or item.get("table") or item.get("name"))
        field_name = _safe_str(item.get("field_name") or item.get("column_name") or item.get("name"))
        if not table_name:
            continue
        by_table.setdefault(table_name, {"table_item": {}, "fields": []})
        if field_name:
            by_table[table_name]["fields"].append(field_name)

    for table_name, fields in column_card_fields.items():
        by_table.setdefault(table_name, {"table_item": {}, "fields": []})
        by_table[table_name]["fields"].extend(fields)

    key_keywords = ["id", "key", "date", "dt", "month", "country", "status", "type", "tier", "amount", "revenue", "mrr", "credit"]
    nodes = []

    for table_name, bundle in sorted(by_table.items()):
        table_item = bundle.get("table_item", {}) or {}
        card = table_card_lookup.get(table_name, {})
        fields = _unique_keep_order(bundle.get("fields", []))
        if not fields and isinstance(card, dict):
            for col in _safe_list(card.get("columns")):
                if isinstance(col, dict):
                    name = _safe_str(col.get("name") or col.get("column_name"))
                    if name:
                        fields.append(name)
        field_count = len(fields)
        key_fields = [
            field for field in fields
            if any(token in field.lower() for token in key_keywords)
        ][:8]
        grain = _safe_str(table_item.get("grain_summary") or card.get("grain"))
        business_meaning = _safe_str(table_item.get("business_meaning") or card.get("business_meaning") or card.get("notes"))
        warning_level = _safe_str(table_item.get("warning_level") or card.get("warning_level") or "unknown") or "unknown"
        node_type = infer_table_type(table_name, table_item)
        title = "\n".join(
            [
                f"table: {table_name}",
                f"type: {node_type}",
                f"grain: {grain or 'unknown'}",
                f"field_count: {field_count}",
                f"key_fields: {', '.join(key_fields) if key_fields else 'none'}",
                f"warning_level: {warning_level}",
                f"business_meaning: {business_meaning or 'unknown'}",
            ]
        )
        nodes.append(
            {
                "id": table_name,
                "label": table_name,
                "type": node_type,
                "grain": grain,
                "business_meaning": business_meaning,
                "field_count": field_count,
                "key_fields": key_fields,
                "warning_level": warning_level,
                "title": title,
            }
        )

    return nodes


def build_edges(join_index) -> list[dict]:
    edges = []
    seen = set()

    for item in _safe_list(join_index):
        if not isinstance(item, dict):
            continue

        source = _safe_str(item.get("source_table") or item.get("from_table") or item.get("left_table"))
        target = _safe_str(item.get("target_table") or item.get("to_table") or item.get("right_table"))
        source_field = _safe_str(item.get("source_field") or item.get("from_field") or item.get("left_field"))
        target_field = _safe_str(item.get("target_field") or item.get("to_field") or item.get("right_field"))
        join_condition = _safe_str(item.get("join_condition"))
        if not join_condition and source and target and source_field and target_field:
            join_condition = f"{source}.{source_field} = {target}.{target_field}"

        if not source or not target:
            continue

        dedup_key = (source, target, source_field, target_field)
        if dedup_key in seen:
            continue
        seen.add(dedup_key)

        label = f"{source_field} = {target_field}" if source_field and target_field else join_condition
        edges.append(
            {
                "source": source,
                "target": target,
                "label": label,
                "join_condition": join_condition,
                "source_field": source_field,
                "target_field": target_field,
                "relationship": _safe_str(item.get("relationship") or "unknown"),
                "risk_level": _safe_str(item.get("risk_level") or "unknown"),
                "note": _safe_str(item.get("note")),
            }
        )

    return edges


def build_graph_payload(nodes, edges) -> dict:
    table_type_counts = {"dimension": 0, "fact": 0, "mapping": 0, "table": 0}
    for node in nodes:
        node_type = _safe_str(node.get("type") or "table")
        table_type_counts[node_type] = table_type_counts.get(node_type, 0) + 1

    risk_level_counts = {"low": 0, "medium": 0, "high": 0, "unknown": 0}
    for edge in edges:
        risk = _safe_str(edge.get("risk_level") or "unknown")
        risk_level_counts[risk] = risk_level_counts.get(risk, 0) + 1

    return {
        "nodes": nodes,
        "edges": edges,
        "summary": {
            "node_count": len(nodes),
            "edge_count": len(edges),
            "table_type_counts": table_type_counts,
            "risk_level_counts": risk_level_counts,
        },
    }


def write_json(path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def render_pyvis_graph(payload, html_path):
    try:
        from pyvis.network import Network  # type: ignore
    except ImportError:
        print("pyvis is not installed; only knowledge_graph.json was generated.")
        return False
    try:
        net = Network(height="820px", width="100%", directed=True, bgcolor="#ffffff", font_color="#111111")
        color_map = {
            "dimension": "#6baed6",
            "fact": "#fd8d3c",
            "mapping": "#74c476",
            "table": "#bdbdbd",
        }
        groups = {
            "dimension": [],
            "mapping": [],
            "fact": [],
            "table": [],
        }
        for node in payload.get("nodes", []):
            node_type = _safe_str(node.get("type") or "table")
            groups.setdefault(node_type, []).append(node)

        base_x = {
            "dimension": -500,
            "mapping": 0,
            "fact": 500,
            "table": 0,
        }

        for node_type in ["dimension", "mapping", "fact", "table"]:
            nodes = groups.get(node_type, [])
            count = len(nodes)
            for index, node in enumerate(nodes):
                y = int((index - count / 2) * 90)
                x = base_x.get(node_type, 0)
                size = 18 + min(int(node.get("field_count") or 0), 12)
                net.add_node(
                    node.get("id"),
                    label=node.get("label"),
                    title=node.get("title"),
                    group=node_type,
                    color=color_map.get(node_type, color_map["table"]),
                    size=size,
                    x=x,
                    y=y,
                )

        edge_color_map = {
            "low": "#bdbdbd",
            "medium": "#f4a261",
            "high": "#d62728",
            "unknown": "#9e9e9e",
        }

        for edge in payload.get("edges", []):
            risk_level = _safe_str(edge.get("risk_level") or "unknown").lower()
            edge_color = edge_color_map.get(risk_level, edge_color_map["unknown"])
            edge_width = 3 if risk_level == "high" else 2 if risk_level == "medium" else 1
            edge_label = "HIGH" if risk_level == "high" else ""
            edge_title = "\n".join(
                [
                    f"join_condition: {_safe_str(edge.get('join_condition'))}",
                    f"source_field: {_safe_str(edge.get('source_field'))}",
                    f"target_field: {_safe_str(edge.get('target_field'))}",
                    f"relationship: {_safe_str(edge.get('relationship'))}",
                    f"risk_level: {_safe_str(edge.get('risk_level'))}",
                    f"note: {_safe_str(edge.get('note'))}",
                ]
            )
            net.add_edge(
                edge.get("source"),
                edge.get("target"),
                label=edge_label,
                title=edge_title,
                color=edge_color,
                width=edge_width,
                arrows="to",
            )

        net.set_options(
            """
            {
              "physics": {
                "enabled": true,
                "barnesHut": {
                  "gravitationalConstant": -10000,
                  "springLength": 220,
                  "springConstant": 0.03,
                  "damping": 0.3
                },
                "stabilization": {
                  "enabled": true,
                  "iterations": 200
                }
              },
              "interaction": {
                "hover": true,
                "navigationButtons": true,
                "keyboard": true,
                "tooltipDelay": 100
              },
              "edges": {
                "arrows": {
                  "to": {
                    "enabled": true,
                    "scaleFactor": 0.6
                  }
                },
                "font": {"size": 8},
                "smooth": {"enabled": true, "type": "dynamic"}
              },
              "nodes": {
                "font": {"size": 11}
              }
            }
            """
        )
        net.write_html(str(html_path), open_browser=False, notebook=False)
        if html_path.exists():
            try:
                html = html_path.read_text(encoding="utf-8")
                legend_html = """
<div style="position: relative; z-index: 9999; margin: 8px 0 12px 0; padding: 12px 14px; border: 1px solid #d9d9d9; border-radius: 8px; background: #fafafa; font-family: Arial, sans-serif; font-size: 13px; line-height: 1.6;">
  <div style="font-weight: 700; margin-bottom: 6px;">ER 图 / 知识图谱说明</div>
  <div><span style="display:inline-block;width:10px;height:10px;background:#6baed6;border-radius:50%;margin-right:6px;"></span>蓝色 = dimension</div>
  <div><span style="display:inline-block;width:10px;height:10px;background:#fd8d3c;border-radius:50%;margin-right:6px;"></span>橙色 = fact</div>
  <div><span style="display:inline-block;width:10px;height:10px;background:#74c476;border-radius:50%;margin-right:6px;"></span>绿色 = mapping</div>
  <div><span style="display:inline-block;width:10px;height:10px;background:#bdbdbd;border-radius:50%;margin-right:6px;"></span>灰色 = table</div>
  <div><span style="color:#d62728;font-weight:700;">红边</span> = high risk JOIN</div>
  <div><span style="color:#f4a261;font-weight:700;">橙边</span> = medium risk JOIN</div>
  <div><span style="color:#9e9e9e;font-weight:700;">灰边</span> = low / unknown JOIN</div>
  <div style="margin-top:6px;">为了避免视觉拥挤，默认隐藏部分边标签，完整 JOIN 信息可通过鼠标悬停查看。</div>
  <div>鼠标悬停节点/边可查看 grain、key_fields、join_condition、risk_level 等详情。</div>
</div>
"""
                if "<body>" in html:
                    html = html.replace("<body>", f"<body>{legend_html}", 1)
                    html_path.write_text(html, encoding="utf-8")
                    print("legend_inserted=true")
                else:
                    print("legend_inserted=false")
            except Exception:
                print("legend_inserted=false")
            return True
        return False
    except Exception as exc:
        print(f"warning: failed to render pyvis graph: {exc}")
        return False


def main():
    table_index = load_json(TABLE_INDEX_PATH, [])
    join_index = load_json(JOIN_INDEX_PATH, [])
    field_index = load_json(FIELD_INDEX_PATH, [])
    table_cards = load_json(TABLE_CARDS_PATH, [])
    column_cards = load_json(COLUMN_CARDS_PATH, [])

    nodes = build_nodes(table_index, field_index, table_cards, column_cards)
    edges = build_edges(join_index)
    payload = build_graph_payload(nodes, edges)

    write_json(GRAPH_JSON_PATH, payload)
    html_ok = render_pyvis_graph(payload, GRAPH_HTML_PATH)

    print(f"node_count={payload['summary']['node_count']}")
    print(f"edge_count={payload['summary']['edge_count']}")
    print(f"graph_json_path={GRAPH_JSON_PATH}")
    if html_ok and GRAPH_HTML_PATH.exists():
        print(f"graph_html_path={GRAPH_HTML_PATH}")
    else:
        print("graph_html_skipped=true")


if __name__ == "__main__":
    main()
