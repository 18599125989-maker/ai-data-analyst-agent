#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
功能：
1. 将 for_contestants/csv 目录下的所有 CSV 文件加载到 DuckDB 数据库
2. 自动推断字段类型
3. 打印每张表的加载结果
4. 生成 outputs/profiles/data_inventory.csv，记录每张表的行数、字段数和加载状态
"""

from pathlib import Path
from typing import List, Dict, Any
import csv
import duckdb


# 项目根目录：当前文件 src/00_load_data.py 的上一级目录
PROJECT_ROOT = Path(__file__).resolve().parents[1]

# CSV 数据目录
CSV_DIR = PROJECT_ROOT / "for_contestants" / "csv"

# DuckDB 数据库路径
DB_PATH = PROJECT_ROOT / "cloudwork.duckdb"

# 输出目录
PROFILE_DIR = PROJECT_ROOT / "outputs" / "profiles"
INVENTORY_PATH = PROFILE_DIR / "data_inventory.csv"


def csv_file_to_table_name(csv_path: Path) -> str:
    """
    将 CSV 文件名转换为 DuckDB 表名。
    例如：dim_user.csv -> dim_user
    """
    return csv_path.stem


def write_inventory(inventory: List[Dict[str, Any]], output_path: Path) -> None:
    """
    将数据加载清单写入 CSV 文件。
    """
    output_path.parent.mkdir(parents=True, exist_ok=True)

    fieldnames = [
        "table_name",
        "csv_file",
        "row_count",
        "column_count",
        "load_status",
        "error_message",
    ]

    with output_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(inventory)


def load_csvs_to_duckdb(csv_dir: Path, db_path: Path) -> None:
    """
    批量加载 CSV 文件到 DuckDB。
    """
    if not csv_dir.exists():
        raise FileNotFoundError(f"CSV 目录不存在，请检查路径: {csv_dir}")

    csv_files: List[Path] = sorted(csv_dir.glob("*.csv"))

    if not csv_files:
        raise FileNotFoundError(f"在 {csv_dir} 目录下未找到任何 CSV 文件")

    print("=" * 70)
    print(f"CSV 文件目录: {csv_dir}")
    print(f"DuckDB 数据库路径: {db_path}")
    print(f"发现 CSV 文件数量: {len(csv_files)} 个")
    print("=" * 70)

    inventory: List[Dict[str, Any]] = []

    with duckdb.connect(str(db_path)) as conn:
        for csv_path in csv_files:
            table_name = csv_file_to_table_name(csv_path)

            print(f"\n正在加载: {csv_path.name} -> 表名: {table_name}")

            try:
                # 如果已存在同名表，先删除，保证重复运行结果一致
                conn.execute(f'DROP TABLE IF EXISTS "{table_name}"')

                # 读取 CSV 并创建 DuckDB 表
                # sample_size = -1：使用全量数据推断字段类型，更稳
                # 注意：不使用 ignore_errors=true，避免静默跳过坏行
                conn.execute(
                    f"""
                    CREATE TABLE "{table_name}" AS
                    SELECT *
                    FROM read_csv_auto(
                        '{csv_path.as_posix()}',
                        header = true,
                        sample_size = -1
                    )
                    """
                )

                row_count = conn.execute(
                    f'SELECT COUNT(*) FROM "{table_name}"'
                ).fetchone()[0]

                column_info = conn.execute(
                    f'DESCRIBE "{table_name}"'
                ).fetchall()

                col_count = len(column_info)

                print(f"  加载成功，行数: {row_count:,}，字段数: {col_count}")

                inventory.append(
                    {
                        "table_name": table_name,
                        "csv_file": csv_path.name,
                        "row_count": row_count,
                        "column_count": col_count,
                        "load_status": "success",
                        "error_message": "",
                    }
                )

            except Exception as e:
                print(f"  加载失败: {e}")

                inventory.append(
                    {
                        "table_name": table_name,
                        "csv_file": csv_path.name,
                        "row_count": 0,
                        "column_count": 0,
                        "load_status": "failed",
                        "error_message": str(e),
                    }
                )

        # 保存数据清单
        write_inventory(inventory, INVENTORY_PATH)

        print("\n" + "=" * 70)
        print("CSV 加载流程结束")
        print(f"数据清单已保存到: {INVENTORY_PATH}")

        success_count = sum(1 for item in inventory if item["load_status"] == "success")
        failed_count = sum(1 for item in inventory if item["load_status"] == "failed")

        print(f"成功加载: {success_count} 张表")
        print(f"加载失败: {failed_count} 张表")

        print("\n当前数据库中的表:")
        tables = conn.execute("SHOW TABLES").fetchall()
        for idx, table in enumerate(tables, 1):
            print(f"  {idx}. {table[0]}")

        print("=" * 70)

        if failed_count > 0:
            raise RuntimeError("存在 CSV 加载失败，请检查 data_inventory.csv 中的 error_message。")


if __name__ == "__main__":
    try:
        load_csvs_to_duckdb(CSV_DIR, DB_PATH)
    except Exception as e:
        print(f"\n加载流程失败，错误信息: {e}")
        raise