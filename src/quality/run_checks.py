"""
Lightweight quality gates.

I deliberately avoid a heavy GE suite for the default path so the repo stays
runnable with `pip install -r requirements.txt` and no extra init steps.
Great Expectations is still listed for teams that want to expand checks.
"""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from src.utils.config import load_settings, path_from_cfg
from src.utils.logging_setup import get_logger

log = get_logger(__name__)


@dataclass
class CheckResult:
    name: str
    passed: bool
    severity: str
    observed: str
    threshold: str
    detail: str = ""


def _projects_checks(df: pd.DataFrame) -> list[CheckResult]:
    results: list[CheckResult] = []

    null_id = df["project_id"].isna().mean()
    results.append(
        CheckResult(
            "project_id_not_null",
            null_id == 0,
            "critical",
            f"{null_id:.2%}",
            "0%",
        )
    )

    bad_budget = (df["budget_azn"] <= 0).mean()
    results.append(
        CheckResult(
            "budget_positive",
            bad_budget < 0.02,
            "critical",
            f"{bad_budget:.2%}",
            "< 2%",
            detail="Zero/negative budgets break cost overrun metrics",
        )
    )

    missing_quality = df["quality_score"].isna().mean()
    results.append(
        CheckResult(
            "quality_score_completeness",
            missing_quality < 0.20,
            "warning",
            f"{missing_quality:.2%}",
            "< 20%",
        )
    )

    # Duplicate project_id across sources is expected occasionally;
    # warn if it exceeds 5% of rows.
    dup_rate = 1 - df["project_id"].nunique() / len(df)
    results.append(
        CheckResult(
            "project_id_uniqueness",
            dup_rate < 0.05,
            "warning",
            f"{dup_rate:.2%}",
            "< 5%",
        )
    )

    future_starts = (pd.to_datetime(df["planned_start"]) > pd.Timestamp("2027-01-01")).mean()
    results.append(
        CheckResult(
            "planned_start_plausible",
            future_starts < 0.03,
            "warning",
            f"{future_starts:.2%}",
            "< 3%",
        )
    )

    return results


def _daily_checks(df: pd.DataFrame) -> list[CheckResult]:
    results: list[CheckResult] = []
    if df.empty:
        return [
            CheckResult("daily_not_empty", False, "critical", "0 rows", "> 0")
        ]

    neg_progress = (df["progress_pct"] < 0).mean()
    results.append(
        CheckResult(
            "progress_non_negative",
            neg_progress == 0,
            "critical",
            f"{neg_progress:.2%}",
            "0%",
        )
    )
    return results


def run() -> bool:
    cfg = load_settings()
    raw = path_from_cfg("raw")
    threshold = cfg["batch"]["quality_pass_threshold"]

    projects = pd.read_csv(raw / "projects.csv")
    daily_path = raw / "project_daily.csv"
    daily = pd.read_csv(daily_path) if daily_path.exists() else pd.DataFrame()

    all_results = _projects_checks(projects) + _daily_checks(daily)

    critical_fail = any(not r.passed and r.severity == "critical" for r in all_results)
    passed_count = sum(1 for r in all_results if r.passed)
    score = passed_count / max(len(all_results), 1)

    for r in all_results:
        mark = "PASS" if r.passed else "FAIL"
        log.info("[%s] %-30s observed=%s threshold=%s (%s)", mark, r.name, r.observed, r.threshold, r.severity)
        if r.detail:
            log.info("         %s", r.detail)

    log.info("Quality score %.2f (threshold %.2f)", score, threshold)

    out = path_from_cfg("processed") / "quality_summary.json"
    import json

    payload = {
        "score": score,
        "critical_fail": critical_fail,
        "checks": [r.__dict__ for r in all_results],
    }
    out.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    if critical_fail:
        log.error("Critical quality failures — pipeline should quarantine or stop")
        return False
    if score < threshold:
        log.warning("Score below threshold; quarantine mode recommended")
        return cfg["batch"]["quarantine_on_fail"]
    return True


def main() -> None:
    ok = run()
    raise SystemExit(0 if ok else 1)


if __name__ == "__main__":
    main()
