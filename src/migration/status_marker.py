from __future__ import annotations

import time
from typing import Any, Dict, List, Tuple

from src.airtable import AirtableConfig, fetch_records, update_records_by_id


def _truthy(val: Any) -> bool:
    try:
        if isinstance(val, bool):
            return val
        if val is None:
            return False
        if isinstance(val, (int, float)):
            # Airtable checkbox may come as 1/0
            return int(val) != 0
        s = str(val).strip().lower()
        return s in ("true", "yes", "y", "1", "ready") or (s != "" and s != "nan")
    except Exception:
        return False


def plan_status_updates(
    records: List[Dict[str, Any]],
    *,
    ready_col: str = "Ready For migration",
    status_col: str = "Migration Status",
    prepared_value: str = "Prepared",
    prepared_at_field: str | None = "Prepared At",
    only_if_blank: bool = True,
) -> List[Dict[str, Any]]:
    """Compute minimal updates for records with ready_col truthy.

    Returns list of {id, fields} objects suitable for Airtable PATCH.
    Does not perform any network I/O.
    """
    updates: List[Dict[str, Any]] = []
    now_iso = time.strftime("%Y-%m-%dT%H:%M:%S.000Z", time.gmtime())
    for rec in records:
        rid = rec.get("id")
        fields = rec.get("fields", {}) or {}
        if not rid:
            continue
        if not _truthy(fields.get(ready_col)):
            continue
        current_status = str(fields.get(status_col, "")).strip()
        if only_if_blank and current_status:
            # Already has a status; skip idempotently
            continue
        if current_status == prepared_value:
            # Already set as desired
            continue
        upd_fields: Dict[str, Any] = {status_col: prepared_value}
        if prepared_at_field:
            upd_fields[prepared_at_field] = now_iso
        updates.append({"id": rid, "fields": upd_fields})
    return updates


def mark_prepared_from_ready(
    cfg: AirtableConfig,
    *,
    ready_col: str = "Ready For migration",
    status_col: str = "Migration Status",
    prepared_value: str = "Prepared",
    prepared_at_field: str | None = "Prepared At",
    only_if_blank: bool = True,
    dry_run: bool = False,
) -> Tuple[int, int]:
    """Mark records as Prepared based on a Ready flag in Airtable.

    Returns (candidates, updated) counts.
    """
    recs = fetch_records(cfg)
    updates = plan_status_updates(
        recs,
        ready_col=ready_col,
        status_col=status_col,
        prepared_value=prepared_value,
        prepared_at_field=prepared_at_field,
        only_if_blank=only_if_blank,
    )
    if dry_run or not updates:
        return len(updates), 0
    updated = update_records_by_id(cfg, updates, typecast=True)
    return len(updates), updated

