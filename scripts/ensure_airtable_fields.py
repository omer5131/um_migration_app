from __future__ import annotations

import os
import sys
from typing import List

# Reuse Airtable helpers from the app
import pathlib
sys.path.append(str(pathlib.Path(__file__).resolve().parents[1]))
from src.airtable import AirtableConfig, ensure_field_exists, ensure_field_type, fetch_records
from src.config import AIRTABLE as AT_CFG
from src.utils.airtable_client import get_airtable_cfg

try:
    from dotenv import load_dotenv  # type: ignore
    load_dotenv()
except Exception:
    pass


def main() -> int:
    # Prefer Streamlit secrets via src.config; fallback to env
    cfg = get_airtable_cfg("approvals") or get_airtable_cfg("table")
    if not cfg:
        api_key = os.getenv("AIRTABLE_API_KEY", AT_CFG.get("API_KEY", "")).strip()
        base_id = os.getenv("AIRTABLE_BASE_ID", AT_CFG.get("BASE_ID", "")).strip()
        table_id = (
            os.getenv("AIRTABLE_APPROVALS_TABLE", AT_CFG.get("APPROVALS_TABLE", "")).strip()
            or os.getenv("AIRTABLE_TABLE", AT_CFG.get("TABLE", "")).strip()
        )
        if not api_key or not base_id or not table_id:
            print("ERROR: Missing Airtable config. Set in Streamlit secrets [airtable] or env vars.", file=sys.stderr)
            return 2
        cfg = AirtableConfig(api_key=api_key, base_id=base_id, table_id_or_name=table_id)

    # Fields we expect for full sync with the Recommendations & Agent page naming
    desired_fields: List[str] = [
        "Account",
        "Sub Type",
        "Final Plan",
        "Add-ons needed",
        "Gained by plan (not currently in project)",
        "Approved By",
        "Approved At",
        "Comment",
        "Under trial",
        # Optional analytics columns that may appear in CSV
        "bloat_costly",
        "irrelevantFeatures",
    ]

    created_or_exists = []
    for fname in desired_fields:
        ok = ensure_field_type(cfg, fname, desired_type="multilineText")
        # Fall back to existence check if type conversion is not permitted
        if not ok:
            ok = ensure_field_exists(cfg, fname, field_type="multilineText")
        created_or_exists.append((fname, ok))

    # Print summary of field ensure/convert operations first
    succeeded = [f for f, ok in created_or_exists if ok]
    failed = [f for f, ok in created_or_exists if not ok]
    print(f"Fields set to multilineText (or already correct): {', '.join(succeeded) if succeeded else 'none'}")
    if failed:
        print(f"Fields not ensured/converted: {', '.join(failed)}")

    # Test access: fetch a small sample of records (optional)
    try:
        records = fetch_records(cfg)
        count = len(records)
        print(f"OK: Access verified. Records in table: {count}.")
    except Exception as e:
        print(f"ERROR: Could not fetch records: {e}", file=sys.stderr)
        return 3

    return 0


if __name__ == "__main__":
    sys.exit(main())
