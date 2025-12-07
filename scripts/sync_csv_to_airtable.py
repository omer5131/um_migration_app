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

import json
import pandas as pd
from src.migration.airtable_sync import AirtableAuth, UpsertOptions, upsert_dataframe
from src.config import AIRTABLE as AT_CFG


def _looks_like_series_dump(s: str) -> bool:
    return isinstance(s, str) and ("dtype:" in s) and ("Name:" in s) and ("\n" in s)


def _clean_series_dump_text(s: str) -> str:
    lines = [ln for ln in s.splitlines() if ln.strip()]
    if not lines:
        return ""
    first = lines[0].strip()
    parts = first.split(maxsplit=1)
    if len(parts) == 2:
        val = parts[1].strip()
        return "" if val == "None" else val
    return s


def clean_cell(x):
    if isinstance(x, pd.Series):
        try:
            return clean_cell(x.iloc[0])
        except Exception:
            return ""
    if isinstance(x, dict):
        return json.dumps(x, ensure_ascii=False)
    if isinstance(x, list):
        return ", ".join(map(str, x))
    if isinstance(x, str) and _looks_like_series_dump(x):
        return _clean_series_dump_text(x)
    return x


def parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Sync a local CSV to Airtable (raw values, safe with --dry-run)")
    ap.add_argument("--csv", required=True, help="Path to input CSV file (e.g., data/approvals.csv)")
    ap.add_argument("--key-column", required=True, help="CSV column to use as unique key in Airtable")
    ap.add_argument("--table", required=False, help="Airtable table name or ID (default: $AIRTABLE_TABLE)")
    ap.add_argument("--dry-run", action="store_true", help="Do not write to Airtable; print counts only")
    ap.add_argument("--limit", type=int, default=0, help="Limit number of CSV rows (for testing)")
    ap.add_argument("--batch-size", type=int, default=10, help="Records per API call (<=10)")
    ap.add_argument("--include-nulls", action="store_true", help="Send empty cells as blanks to overwrite fields")
    ap.add_argument("--typecast", action="store_true", help="Enable Airtable typecasting (off by default)")
    ap.add_argument("--show-sample", type=int, default=0, help="Print N cleaned rows for inspection (no write)")
    ap.add_argument("--verify", type=int, default=0, help="After apply, fetch and print N records' fields for verification")
    return ap.parse_args(argv)


def main(argv: Optional[list[str]] = None) -> int:
    args = parse_args(argv)

    # Prefer Streamlit secrets via src.config; it already falls back to env
    api_key = (AT_CFG.get("API_KEY") or "").strip()
    base_id = (AT_CFG.get("BASE_ID") or "").strip()
    # If --table is not provided, use approvals table first, then default table
    table = (args.table or AT_CFG.get("APPROVALS_TABLE") or AT_CFG.get("TABLE") or "").strip()

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
        df = pd.read_csv(args.csv)
        if args.limit and args.limit > 0:
            df = df.head(args.limit)
        df = df.applymap(clean_cell)
        # Strip whitespace/newlines on all string cells to avoid key mismatches (e.g., trailing \n)
        df = df.applymap(lambda v: v.strip() if isinstance(v, str) else v)
        # Normalize NaN/None -> empty string for Airtable
        for col in df.columns:
            df[col] = df[col].apply(lambda v: "" if (v is None or (isinstance(v, float) and pd.isna(v)) or (isinstance(v, str) and v == "nan")) else v)
    except Exception as e:
        print(f"ERROR: Failed to load or clean CSV: {e}", file=sys.stderr)
        return 2

    if args.key_column not in df.columns:
        print(f"ERROR: Key column '{args.key_column}' not found in CSV columns: {list(df.columns)}", file=sys.stderr)
        return 2

    # Optional: show a sample of cleaned rows (local only)
    if args.show_sample and args.show_sample > 0:
        sample = df.head(args.show_sample)
        print("CLEANED SAMPLE (first", args.show_sample, "rows):")
        print(sample.to_dict(orient="records"))
        # If only sampling, we still continue to dry-run/apply per flags

    auth = AirtableAuth(api_key=api_key, base_id=base_id)
    opts = UpsertOptions(
        key_field=args.key_column,
        table=table,
        batch_size=max(1, min(10, int(args.batch_size))),
        typecast=bool(args.typecast),
        include_nulls=bool(args.include_nulls),
    )

    created, updated = upsert_dataframe(df, auth, opts, dry_run=args.dry_run)
    mode = "DRY-RUN" if args.dry_run else "APPLIED"
    print(f"{mode}: to_create={created}, to_update={updated}, table='{table}', key='{opts.key_field}'")
    # Optional verification: fetch a few records by key and print the two fields
    if not args.dry_run and args.verify and args.verify > 0:
        import urllib.parse
        import requests as _req

        def _headers():
            return {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}

        def _encode(seg: str) -> str:
            return urllib.parse.quote(seg, safe="")

        def _fetch_by_key(kval: str) -> dict:
            url = f"https://api.airtable.com/v0/{base_id}/{_encode(table)}"
            fval = kval.replace("'", "\\'")
            params = {"filterByFormula": f"{{{args.key_column}}}='{fval}'", "maxRecords": 1}
            r = _req.get(url, headers=_headers(), params=params, timeout=30)
            r.raise_for_status()
            recs = (r.json() or {}).get("records", [])
            return recs[0] if recs else {}

        check_cols = [
            "Add-ons needed",
            "Gained by plan (not currently in project)",
        ]
        print("VERIFY: Fetching", args.verify, "records by key to inspect fields...")
        for i, (_, row) in enumerate(df.head(args.verify).iterrows()):
            key_val = str(row.get(args.key_column, "")).strip()
            if not key_val:
                continue
            rec = _fetch_by_key(key_val)
            fields = rec.get("fields", {}) if rec else {}
            view = {c: fields.get(c) for c in check_cols}
            print(f"- {args.key_column}='{key_val}':", view)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
