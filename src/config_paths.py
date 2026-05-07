#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import os
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
ENV_PATH = PROJECT_ROOT / ".env"

DEFAULT_CSV_DIR = PROJECT_ROOT / "for_contestants" / "csv"
DEFAULT_DUCKDB_PATH = PROJECT_ROOT / "cloudwork.duckdb"


def load_project_dotenv() -> None:
    if not ENV_PATH.exists():
        return

    for line in ENV_PATH.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue

        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = value


def resolve_config_path(env_name: str, default_path: Path) -> Path:
    raw_value = os.environ.get(env_name, "").strip()
    if not raw_value:
        return default_path

    configured_path = Path(raw_value).expanduser()
    if not configured_path.is_absolute():
        configured_path = PROJECT_ROOT / configured_path
    return configured_path


load_project_dotenv()

CSV_DIR = resolve_config_path("CSV_DIR", DEFAULT_CSV_DIR)
DB_PATH = resolve_config_path("DUCKDB_PATH", DEFAULT_DUCKDB_PATH)
