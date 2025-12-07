#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import sys
from typing import Optional

try:
    from dotenv import load_dotenv  # type: ignore
    load_dotenv()
except Exception:
    pass

import pathlib
sys.path.append(str(pathlib.Path(__file__).resolve().parents[1]))
from src.migration.schema import AirtableAuth, create_table_from_csv
from src.config import AIRTABLE as CFG


def parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Create an Airtable table whose fields match a CSV header exactly")
    ap.add_argument("--csv", required=True, help="Path to CSV file (e.g., data/approvals.csv)")
    ap.add_argument("--table", help="New Airtable table name (default: Approvals)")
    ap.add_argument("--primary-field", help="Primary field name (default: first CSV column)")
    ap.add_argument("--dry-run", action="store_true", help="Print the creation payload and exit")
    return ap.parse_args(argv)


def main(argv: Optional[list[str]] = None) -> int:
    args = parse_args(argv)

    api_key = (os.getenv("AIRTABLE_API_KEY", CFG.get("API_KEY", "")) or "").strip()
    base_id = (os.getenv("AIRTABLE_BASE_ID", CFG.get("BASE_ID", "")) or "").strip()
    if not api_key or not base_id:
        print("ERROR: Missing Airtable config. Use Streamlit secrets [airtable] or env vars.", file=sys.stderr)
        return 2

    table_name = (args.table or "Approvals").strip()
    auth = AirtableAuth(api_key=api_key, base_id=base_id)

    try:
        result = create_table_from_csv(
            args.csv,
            auth,
            table_name=table_name,
            primary_field=args.primary_field,
            multiline_fields=["Comment", "Gained by plan (not currently in project)"],
            dry_run=args.dry_run,
        )
    except Exception as e:
        print(f"ERROR: Failed to create table: {e}", file=sys.stderr)
        return 3

    if args.dry_run:
        print("DRY-RUN payload:")
        import json as _json
        print(_json.dumps(result, indent=2))
        return 0

    # On success, print the new table id and name (payload format from API)
    tbl = result.get("tables") or result  # creation may return a single table or a wrapper
    if isinstance(tbl, dict):
        name = tbl.get("name")
        tid = tbl.get("id")
        print(f"CREATED: table name='{name}' id='{tid}'")
    else:
        print(str(result))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
