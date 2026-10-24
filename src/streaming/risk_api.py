"""
Tiny FastAPI service so a dashboard or field app can request a score.
Not meant to be production auth-hardened — just a clear integration surface.
"""
from __future__ import annotations

from typing import Any

from fastapi import FastAPI
from pydantic import BaseModel, Field

from src.models.risk import score_event, score_project_snapshot

app = FastAPI(title="Construction Risk API", version="0.1.0")


class SnapshotRequest(BaseModel):
    progress_pct: float | None = None
    planned_elapsed_pct: float | None = Field(None, description="0–1+ fraction of plan time elapsed")
    quality_score: float | None = None
    cost_overrun_pct: float | None = None
    contractor_delay_rate: float | None = 0.25
    weather_severity: float | None = 0.0
    sensor_anomaly: float | None = 0.0


class EventRequest(BaseModel):
    event_type: str
    payload: dict[str, Any] = Field(default_factory=dict)
    project_ctx: dict[str, Any] = Field(default_factory=dict)


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/score/snapshot")
def score_snapshot(body: SnapshotRequest):
    result = score_project_snapshot(**body.model_dump())
    return {
        "risk_score": result.score,
        "risk_band": result.band,
        "features": result.features,
        "model_version": result.model_version,
    }


@app.post("/score/event")
def score_single_event(body: EventRequest):
    event = {"event_type": body.event_type, "payload": body.payload}
    result = score_event(event, body.project_ctx)
    return {
        "risk_score": result.score,
        "risk_band": result.band,
        "features": result.features,
        "model_version": result.model_version,
    }
