#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import sys
from typing import Optional

import pandas as pd

try:
    from dotenv import load_dotenv  # type: ignore
    load_dotenv()  # best-effort
except Exception:
    pass

from src.migration.airtable_sync import (
    AirtableAuth,
    UpsertOptions,
    load_csv,
    upsert_dataframe,
)


def parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Upsert a local CSV into Airtable (safe with --dry-run)")
    ap.add_argument("--csv", required=True, help="Path to input CSV file")
    ap.add_argument("--key-column", required=True, help="CSV column to use as unique key in Airtable")
    ap.add_argument("--table", help="Airtable table name or ID (default: $AIRTABLE_TABLE)")
    ap.add_argument("--dry-run", action="store_true", help="Do not write to Airtable; print counts only")
    ap.add_argument("--limit", type=int, default=0, help="Limit number of CSV rows (for testing)")
    ap.add_argument("--batch-size", type=int, default=10, help="Records per API call (<=10)")
    ap.add_argument("--no-typecast", action="store_true", help="Disable Airtable typecasting on write")
    ap.add_argument("--include-nulls", action="store_true", help="Send null/empty fields as blanks (overwrites)")
    ap.add_argument("--no-filter-unknown", action="store_true", help="Do not filter CSV columns against Airtable schema")
    ap.add_argument(
        "--field-alias",
        action="append",
        default=[],
        help="Map CSV column to Airtable field, format COL=AIRTABLE_FIELD (repeatable)",
    )
    return ap.parse_args(argv)


def main(argv: Optional[list[str]] = None) -> int:
    args = parse_args(argv)

    api_key = os.getenv("AIRTABLE_API_KEY", "").strip()
    base_id = os.getenv("AIRTABLE_BASE_ID", "").strip()
    table = (args.table or os.getenv("AIRTABLE_TABLE", "")).strip()

    if not api_key:
        print("ERROR: AIRTABLE_API_KEY is not set.", file=sys.stderr)
        return 2
    if not base_id:
        print("ERROR: AIRTABLE_BASE_ID is not set.", file=sys.stderr)
        return 2
    if not table:
        print("ERROR: Table not provided. Use --table or set AIRTABLE_TABLE.", file=sys.stderr)
        return 2

    try:
        df: pd.DataFrame = load_csv(args.csv, limit=args.limit if args.limit > 0 else None)
    except Exception as e:
        print(f"ERROR: Failed to load CSV: {e}", file=sys.stderr)
        return 2

    if args.key_column not in df.columns:
        print(f"ERROR: Key column '{args.key_column}' not found in CSV columns: {list(df.columns)}", file=sys.stderr)
        return 2

    auth = AirtableAuth(api_key=api_key, base_id=base_id)
    # Parse field aliases
    aliases = {}
    for pair in args.field_alias:
        if "=" in pair:
            src, dst = pair.split("=", 1)
            aliases[src.strip()] = dst.strip()

    opts = UpsertOptions(
        key_field=args.key_column,
        table=table,
        batch_size=max(1, min(10, int(args.batch_size))),
        typecast=not args.no_typecast,
        skip_nulls=not args.include_nulls,
        filter_unknown_fields=not args.no_filter_unknown,
        field_aliases=aliases or None,
    )

    created, updated = upsert_dataframe(df, auth, opts, dry_run=args.dry_run)
    mode = "DRY-RUN" if args.dry_run else "APPLIED"
    print(f"{mode}: to_create={created}, to_update={updated}, table='{table}', key='{opts.key_field}'")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
