from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import jsonschema
from jsonschema import Draft202012Validator

from src.utils.config import path_from_cfg
from src.utils.logging_setup import get_logger

log = get_logger(__name__)


def load_contract(name: str) -> dict[str, Any]:
    path = path_from_cfg("contracts") / name
    with path.open(encoding="utf-8") as f:
        return json.load(f)


def validate_records(
    records: list[dict[str, Any]],
    contract_name: str,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Return (valid, rejected) with rejection reasons attached."""
    schema = load_contract(contract_name)
    validator = Draft202012Validator(schema)
    valid: list[dict[str, Any]] = []
    rejected: list[dict[str, Any]] = []

    for rec in records:
        errors = sorted(validator.iter_errors(rec), key=lambda e: e.path)
        if not errors:
            valid.append(rec)
            continue
        reason = "; ".join(f"{'/'.join(map(str, e.path)) or '<root>'}: {e.message}" for e in errors[:3])
        bad = dict(rec)
        bad["_reject_reason"] = reason
        rejected.append(bad)

    log.info(
        "Contract %s: %s valid, %s rejected",
        contract_name,
        len(valid),
        len(rejected),
    )
    return valid, rejected
