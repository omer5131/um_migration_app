#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
from typing import Optional

from src.config import AIRTABLE as CFG
from src.airtable import AirtableConfig
from src.migration.status_marker import mark_prepared_from_ready


def parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Mark Airtable records as Prepared based on a Ready flag")
    ap.add_argument("--table", help="Airtable table name/id (default: $AIRTABLE_TABLE)")
    ap.add_argument("--view", help="Airtable view to read (default: $AIRTABLE_VIEW)")
    ap.add_argument("--ready-col", default=None, help="Column name for Ready flag (default: config)")
    ap.add_argument("--status-col", default=None, help="Column name for status (default: config)")
    ap.add_argument("--prepared-value", default=None, help="Status value to write (default: config)")
    ap.add_argument("--prepared-at-field", default=None, help="Timestamp field to set (default: config; set to '' to disable)")
    ap.add_argument("--all", dest="only_if_blank", action="store_false", help="Update even if status already set")
    ap.add_argument("--dry-run", action="store_true", help="Do not write; show counts only")
    return ap.parse_args(argv)


def main(argv: Optional[list[str]] = None) -> int:
    args = parse_args(argv)

    api_key = (CFG.get("API_KEY") or "").strip()
    base_id = (CFG.get("BASE_ID") or "").strip()
    table = (args.table or CFG.get("TABLE") or "").strip()
    view = (args.view or CFG.get("VIEW") or None) or None
    if not api_key or not base_id or not table:
        print("ERROR: Missing Airtable config. Set AIRTABLE_API_KEY, AIRTABLE_BASE_ID, AIRTABLE_TABLE.", file=sys.stderr)
        return 2

    cfg = AirtableConfig(api_key=api_key, base_id=base_id, table_id_or_name=table, view=view)
    ready_col = args.ready_col or (CFG.get("READY_COLUMN") or "Ready For migration")
    status_col = args.status_col or (CFG.get("STATUS_COLUMN") or "Migration Status")
    prepared_value = args.prepared_value or (CFG.get("PREPARED_VALUE") or "Prepared")
    prepared_at_field = args.prepared_at_field
    if prepared_at_field == "":
        prepared_at_field = None
    if prepared_at_field is None:
        prepared_at_field = CFG.get("PREPARED_AT_FIELD") or "Prepared At"

    candidates, updated = mark_prepared_from_ready(
        cfg,
        ready_col=ready_col,
        status_col=status_col,
        prepared_value=prepared_value,
        prepared_at_field=prepared_at_field,
        only_if_blank=bool(args.only_if_blank),
        dry_run=bool(args.dry_run),
    )
    mode = "DRY-RUN" if args.dry_run else "APPLIED"
    print(f"{mode}: candidates={candidates}, updated={updated}, table='{table}', ready='{ready_col}', status='{status_col}', value='{prepared_value}'")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

