from __future__ import annotations

import os
from pathlib import Path
from functools import lru_cache
from typing import Any

import yaml

ROOT = Path(__file__).resolve().parents[2]


@lru_cache(maxsize=1)
def load_settings() -> dict[str, Any]:
    path = ROOT / "config" / "settings.yaml"
    with path.open(encoding="utf-8") as f:
        cfg = yaml.safe_load(f)

    # Lightweight env overrides — enough for local vs docker without a full settings framework
    if os.getenv("WAREHOUSE_ENGINE"):
        cfg["warehouse"]["engine"] = os.getenv("WAREHOUSE_ENGINE")
    if os.getenv("KAFKA_BOOTSTRAP"):
        cfg["streaming"]["bootstrap_servers"] = os.getenv("KAFKA_BOOTSTRAP")
    return cfg


def path_from_cfg(key: str) -> Path:
    cfg = load_settings()
    rel = cfg["paths"][key]
    p = ROOT / rel
    p.mkdir(parents=True, exist_ok=True)
    return p
