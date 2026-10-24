from src.models.risk import score_event, score_project_snapshot


def test_high_slip_pushes_score_up():
    low = score_project_snapshot(
        progress_pct=90,
        planned_elapsed_pct=0.5,
        quality_score=90,
        cost_overrun_pct=0,
        weather_severity=0,
        sensor_anomaly=0,
        contractor_delay_rate=0.1,
    )
    high = score_project_snapshot(
        progress_pct=20,
        planned_elapsed_pct=0.8,
        quality_score=60,
        cost_overrun_pct=35,
        weather_severity=0.7,
        sensor_anomaly=0.5,
        contractor_delay_rate=0.5,
    )
    assert high.score > low.score
    assert high.band in {"medium", "high"}


def test_event_weather_affects_features():
    r = score_event(
        {"event_type": "weather", "payload": {"rain_mm": 40, "wind_ms": 12}},
        project_ctx={"progress_pct": 50, "planned_elapsed_pct": 0.5},
    )
    assert r.features["weather_severity"] > 0.5


def test_score_bounds():
    r = score_project_snapshot(
        progress_pct=0,
        planned_elapsed_pct=2.0,
        quality_score=0,
        cost_overrun_pct=200,
        weather_severity=1,
        sensor_anomaly=1,
        contractor_delay_rate=1,
    )
    assert 0.0 <= r.score <= 1.0
