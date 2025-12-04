#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from typing import List, Dict, Any

import pandas as pd

from src.data_loader import load_all_data
from src.utils.ga_features import ga_visibility_for_dataframe


def main() -> None:
    parser = argparse.ArgumentParser(description="List GA feature visibility per customer")
    parser.add_argument("--filter", dest="filter", default="", help="Substring to filter account name")
    parser.add_argument("--json", dest="as_json", action="store_true", help="Output JSON instead of table")
    parser.add_argument("--limit", dest="limit", type=int, default=0, help="Limit number of rows in output")
    args = parser.parse_args()

    data = load_all_data()
    if not data or "mapping" not in data:
        raise SystemExit("No mapping data loaded. Use the app to configure data sources or place cache under data/.")

    df: pd.DataFrame = data["mapping"].copy()
    # Normalize account name column
    if "name" not in df.columns and "SalesForce_Account_NAME" in df.columns:
        df = df.rename(columns={"SalesForce_Account_NAME": "name"})

    if args.filter:
        mask = df["name"].astype(str).str.contains(args.filter, case=False, na=False)
        df = df[mask]

    rows: List[Dict[str, Any]] = ga_visibility_for_dataframe(df)

    if args.limit and args.limit > 0:
        rows = rows[: args.limit]

    if args.as_json:
        print(json.dumps(rows, indent=2))
        return

    # Pretty table
    print(f"Accounts: {len(rows)}\n")
    for r in rows:
        name = r.get("name", "")
        present = r.get("ga_present", [])
        missing = r.get("ga_missing", [])
        total = r.get("ga_total", 0)
        print(f"- {name}")
        print(f"  GA present: {len(present)}/{total}")
        if present:
            print("  + ", ", ".join(present))
        if missing:
            print("  - missing: ", ", ".join(missing))
        print()


if __name__ == "__main__":
    main()

