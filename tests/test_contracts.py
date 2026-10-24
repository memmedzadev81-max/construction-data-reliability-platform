from src.contracts.validate import validate_records


def test_valid_project_passes():
    rec = {
        "project_id": "RP-1001",
        "contractor_id": "C001",
        "planned_start": "2024-01-01",
        "planned_end": "2024-12-01",
        "budget_azn": 1_000_000,
        "source_system": "pm_export",
        "extracted_at": "2026-10-01T12:00:00+00:00",
    }
    valid, rejected = validate_records([rec], "projects_v1.json")
    assert len(valid) == 1
    assert not rejected


def test_zero_budget_rejected():
    rec = {
        "project_id": "RP-1002",
        "contractor_id": "C001",
        "planned_start": "2024-01-01",
        "planned_end": "2024-12-01",
        "budget_azn": 0,
        "source_system": "pm_export",
        "extracted_at": "2026-10-01T12:00:00+00:00",
    }
    valid, rejected = validate_records([rec], "projects_v1.json")
    assert not valid
    assert len(rejected) == 1
    assert "budget" in rejected[0]["_reject_reason"].lower()
