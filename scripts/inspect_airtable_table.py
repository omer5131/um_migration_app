#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import sys
from typing import Dict, Optional, Tuple

import pandas as pd

try:
    from dotenv import load_dotenv  # type: ignore
    load_dotenv()
except Exception:
    pass

from src.migration.airtable_sync import AirtableAuth, get_table_fields
from src.config import AIRTABLE as CFG


def normalize_name(s: str) -> str:
    return "".join(ch.lower() for ch in s if ch.isalnum())


def diff_csv_vs_table(csv_path: str, fields: Dict[str, str]) -> Tuple[str, int]:
    df = pd.read_csv(csv_path, nrows=50)  # sample header and some values
    csv_cols = list(df.columns)
    table_fields = list(fields.keys())

    inv_index = {normalize_name(f): f for f in table_fields}

    lines = []
    missing = 0
    lines.append(f"CSV columns: {len(csv_cols)} | Table fields: {len(table_fields)}")
    lines.append("")
    for col in csv_cols:
        n = normalize_name(col)
        exact = col in fields
        approx = inv_index.get(n)
        if exact:
            lines.append(f"OK   | {col} -> {col} [{fields[col]}]")
        elif approx:
            lines.append(f"CASE | {col} ≈ {approx} [{fields[approx]}] (case/spacing mismatch)")
        else:
            missing += 1
            # show sample type
            sample_val = next((v for v in df[col].dropna().tolist() if str(v).strip() != ""), None)
            stype = type(sample_val).__name__ if sample_val is not None else "empty"
            lines.append(f"MISS | {col} (no matching field in table) sample_type={stype}")

    return "\n".join(lines), missing


def parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Inspect Airtable table schema and diff against a CSV header")
    ap.add_argument("--csv", required=True, help="Path to CSV file (e.g., data/approvals.csv)")
    ap.add_argument("--table", help="Airtable table name or ID (default: $AIRTABLE_TABLE)")
    return ap.parse_args(argv)


def main(argv: Optional[list[str]] = None) -> int:
    args = parse_args(argv)
    # Prefer Streamlit secrets/config, override with env if present
    api_key = (os.getenv("AIRTABLE_API_KEY", CFG.get("API_KEY", "")) or "").strip()
    base_id = (os.getenv("AIRTABLE_BASE_ID", CFG.get("BASE_ID", "")) or "").strip()
    table = (args.table or os.getenv("AIRTABLE_TABLE", CFG.get("TABLE", "")) or "").strip()

    if not api_key or not base_id or not table:
        print("ERROR: Missing Airtable config. Use Streamlit secrets [airtable] or env vars, and provide --table if needed.", file=sys.stderr)
        return 2

    auth = AirtableAuth(api_key=api_key, base_id=base_id)
    fields = get_table_fields(auth, table)
    if not fields:
        print("WARNING: Could not read Airtable metadata; check API key permissions.")
        return 3

    report, missing = diff_csv_vs_table(args.csv, fields)
    print(report)
    if missing:
        print(f"\nMissing in table: {missing}. Consider adding these fields or enabling filtering.")
    else:
        print("\nAll CSV columns match table fields (case-insensitive check).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
