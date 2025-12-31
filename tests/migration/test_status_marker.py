from __future__ import annotations

from src.migration.status_marker import plan_status_updates


def test_plan_status_updates_truthy_and_idempotent():
    records = [
        {"id": "rec1", "fields": {"Ready For migration": True, "Migration Status": ""}},
        {"id": "rec2", "fields": {"Ready For migration": "yes", "Migration Status": "Prepared"}},
        {"id": "rec3", "fields": {"Ready For migration": "no", "Migration Status": ""}},
        {"id": "rec4", "fields": {"Ready For migration": 1, "Migration Status": "Queued"}},
        {"id": "rec5", "fields": {"Ready For migration": "", "Migration Status": ""}},
    ]
    updates = plan_status_updates(records, only_if_blank=True)
    # rec1: ready True, blank status -> update
    # rec2: ready yes, already Prepared -> skip
    # rec3: not ready -> skip
    # rec4: ready 1, status not blank, only_if_blank -> skip
    # rec5: not ready -> skip
    ids = [u["id"] for u in updates]
    assert ids == ["rec1"]
    assert updates[0]["fields"].get("Migration Status") == "Prepared"


def test_plan_status_updates_override_when_allowed():
    records = [
        {"id": "rec1", "fields": {"Ready For migration": True, "Migration Status": "Queued"}},
    ]
    updates = plan_status_updates(records, only_if_blank=False)
    assert updates and updates[0]["fields"].get("Migration Status") == "Prepared"

