#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
功能：
对 DuckDB 中已加载的数据表做基础数据探查。

输出结果保存在 outputs/profiles/ 目录下：
1. table_profiles.json：表级统计信息
2. column_profiles.json：字段级统计信息

运行前请先执行：
python src/00_load_data.py
"""

import json
from pathlib import Path
import duckdb

try:
    from config_paths import DB_PATH, PROJECT_ROOT
except ModuleNotFoundError:
    from src.config_paths import DB_PATH, PROJECT_ROOT

OUTPUT_DIR = PROJECT_ROOT / "outputs" / "profiles"

# 输出文件路径
TABLE_PROFILE_PATH = OUTPUT_DIR / "table_profiles.json"
COLUMN_PROFILE_PATH = OUTPUT_DIR / "column_profiles.json"


def main() -> None:
    """
    主函数：生成表级和字段级 profile。
    """
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    if not DB_PATH.exists():
        raise FileNotFoundError(
            f"数据库不存在，请先运行 src/00_load_data.py\n路径：{DB_PATH}"
        )

    conn = duckdb.connect(str(DB_PATH))

    try:
        tables = [row[0] for row in conn.execute("SHOW TABLES").fetchall()]
        tables = sorted(tables)

        print(f"共发现 {len(tables)} 张表，开始生成探查报告...\n")

        table_profiles = []
        column_profiles = []

        for idx, table_name in enumerate(tables, 1):
            print(f"[{idx}/{len(tables)}] 处理表：{table_name}")

            row_count = conn.execute(
                f'SELECT COUNT(*) FROM "{table_name}"'
            ).fetchone()[0]

            columns = conn.execute(
                f'DESCRIBE "{table_name}"'
            ).fetchall()

            column_list = [
                {
                    "name": col[0],
                    "type": col[1],
                }
                for col in columns
            ]

            table_profile = {
                "table_name": table_name,
                "row_count": row_count,
                "column_count": len(column_list),
                "columns": column_list,
            }
            table_profiles.append(table_profile)

            print(f"  行数：{row_count:,}，字段数：{len(column_list)}")

            for col_idx, col in enumerate(column_list, 1):
                col_name = col["name"]
                col_type = col["type"]

                print(f"    [{col_idx}/{len(column_list)}] 字段：{col_name}")

                null_count = conn.execute(
                    f'SELECT COUNT(*) FROM "{table_name}" WHERE "{col_name}" IS NULL'
                ).fetchone()[0]

                null_rate = round(null_count / row_count, 4) if row_count > 0 else 0

                distinct_count = conn.execute(
                    f'SELECT COUNT(DISTINCT "{col_name}") FROM "{table_name}"'
                ).fetchone()[0]

                distinct_rate = round(distinct_count / row_count, 4) if row_count > 0 else 0

                try:
                    min_val, max_val = conn.execute(
                        f'SELECT MIN("{col_name}"), MAX("{col_name}") FROM "{table_name}"'
                    ).fetchone()
                except Exception:
                    min_val = None
                    max_val = None

                try:
                    samples = [
                        str(row[0])
                        for row in conn.execute(
                            f'''
                            SELECT DISTINCT "{col_name}"
                            FROM "{table_name}"
                            WHERE "{col_name}" IS NOT NULL
                            ORDER BY "{col_name}"
                            LIMIT 5
                            '''
                        ).fetchall()
                    ]
                except Exception:
                    samples = []

                is_possible_primary_key = (
                    null_count == 0
                    and distinct_count == row_count
                    and row_count > 0
                )

                column_profile = {
                    "table_name": table_name,
                    "column_name": col_name,
                    "data_type": col_type,
                    "row_count": row_count,
                    "null_count": null_count,
                    "null_rate": null_rate,
                    "distinct_count": distinct_count,
                    "distinct_rate": distinct_rate,
                    "min": str(min_val) if min_val is not None else None,
                    "max": str(max_val) if max_val is not None else None,
                    "sample_values": samples,
                    "is_possible_primary_key": is_possible_primary_key,
                }

                column_profiles.append(column_profile)

        with TABLE_PROFILE_PATH.open("w", encoding="utf-8") as f:
            json.dump(table_profiles, f, ensure_ascii=False, indent=2)

        with COLUMN_PROFILE_PATH.open("w", encoding="utf-8") as f:
            json.dump(column_profiles, f, ensure_ascii=False, indent=2)

        print("\n" + "=" * 60)
        print("探查报告生成完成！")
        print(f"表级报告：{TABLE_PROFILE_PATH}")
        print(f"字段级报告：{COLUMN_PROFILE_PATH}")
        print("=" * 60)

    finally:
        conn.close()


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"运行失败：{e}")
        raise
