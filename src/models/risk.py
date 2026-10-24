"""
Explainable delay-risk scoring.

Site teams reject black-box scores. This module keeps weights in config and
returns the feature breakdown so a project manager can argue with the number.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from src.utils.config import load_settings

MODEL_VERSION = "rules_v1"


@dataclass
class RiskResult:
    score: float
    band: str
    features: dict[str, float]
    model_version: str = MODEL_VERSION


def _clip(x: float, lo: float = 0.0, hi: float = 1.0) -> float:
    return max(lo, min(hi, x))


def score_project_snapshot(
    *,
    progress_pct: float | None,
    planned_elapsed_pct: float | None,
    quality_score: float | None,
    cost_overrun_pct: float | None,
    contractor_delay_rate: float | None = 0.25,
    weather_severity: float | None = 0.0,
    sensor_anomaly: float | None = 0.0,
) -> RiskResult:
    cfg = load_settings()["risk_model"]
    w = cfg["weights"]

    # Progress slip: if planned elapsed is 60% but actual progress is 30% → high slip
    if progress_pct is None or planned_elapsed_pct is None or planned_elapsed_pct <= 0:
        progress_slip = 0.4
    else:
        gap = planned_elapsed_pct - (progress_pct / 100.0)
        progress_slip = _clip(gap / 0.5)  # 50pp gap → 1.0

    quality_trend = 0.5 if quality_score is None else _clip((85 - quality_score) / 35)
    cost_pressure = 0.0 if cost_overrun_pct is None else _clip(cost_overrun_pct / 40.0)
    # fold cost into progress_slip slightly so we do not invent a sixth weight
    progress_slip = _clip(0.7 * progress_slip + 0.3 * cost_pressure)

    contractor_history = _clip(contractor_delay_rate if contractor_delay_rate is not None else 0.25)
    weather = _clip(weather_severity or 0.0)
    anomaly = _clip(sensor_anomaly or 0.0)

    features = {
        "progress_slip": round(progress_slip, 4),
        "quality_trend": round(quality_trend, 4),
        "contractor_history": round(contractor_history, 4),
        "weather_severity": round(weather, 4),
        "sensor_anomaly": round(anomaly, 4),
    }

    score = (
        w["progress_slip"] * features["progress_slip"]
        + w["quality_trend"] * features["quality_trend"]
        + w["contractor_history"] * features["contractor_history"]
        + w["weather_severity"] * features["weather_severity"]
        + w["sensor_anomaly"] * features["sensor_anomaly"]
    )
    score = round(_clip(score), 4)

    if score >= cfg["high_risk_threshold"]:
        band = "high"
    elif score >= cfg["medium_risk_threshold"]:
        band = "medium"
    else:
        band = "low"

    return RiskResult(score=score, band=band, features=features)


def score_event(event: dict[str, Any], project_ctx: dict[str, Any] | None = None) -> RiskResult:
    """Map a single sensor/progress event into a risk contribution."""
    project_ctx = project_ctx or {}
    et = event.get("event_type")
    payload = event.get("payload") or {}

    weather = 0.0
    anomaly = 0.0
    if et == "weather":
        rain = float(payload.get("rain_mm") or 0)
        wind = float(payload.get("wind_ms") or 0)
        weather = _clip(rain / 30.0 * 0.6 + wind / 15.0 * 0.4)
    elif et == "equipment_stop":
        hours = float(payload.get("hours") or 0)
        anomaly = _clip(hours / 8.0)
    elif et == "material_delay":
        anomaly = _clip(float(payload.get("days_impact") or 0) / 7.0)
    elif et == "quality_check":
        q = float(payload.get("score") or 80)
        anomaly = _clip((80 - q) / 30.0) if q < 80 else 0.0

    return score_project_snapshot(
        progress_pct=project_ctx.get("progress_pct"),
        planned_elapsed_pct=project_ctx.get("planned_elapsed_pct", 0.5),
        quality_score=project_ctx.get("quality_score"),
        cost_overrun_pct=project_ctx.get("cost_overrun_pct"),
        contractor_delay_rate=project_ctx.get("contractor_delay_rate", 0.25),
        weather_severity=weather,
        sensor_anomaly=anomaly,
    )
