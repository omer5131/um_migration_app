from __future__ import annotations

import os
import time
from typing import Optional

import pandas as pd
import pytest

from src.migration.airtable_sync import AirtableAuth, UpsertOptions, fetch_existing_map, upsert_dataframe


def _env(name: str) -> Optional[str]:
    v = os.getenv(name, "").strip()
    return v or None


def _has_live_creds() -> bool:
    return all([
        _env("AIRTABLE_API_KEY"),
        _env("AIRTABLE_BASE_ID"),
        _env("AIRTABLE_TABLE") or _env("AIRTABLE_TEST_TABLE"),
        os.getenv("PYTEST_AIRTABLE_LIVE") == "1",
    ])


@pytest.mark.skipif(not _has_live_creds(), reason="Live Airtable creds/table not provided or live flag not set")
def test_reach_airtable_and_columns_from_csv():
    api_key = _env("AIRTABLE_API_KEY")
    base_id = _env("AIRTABLE_BASE_ID")
    table = _env("AIRTABLE_TEST_TABLE") or _env("AIRTABLE_TABLE")  # prefer dedicated test table
    assert api_key and base_id and table

    # Load representative CSV to capture its data model
    csv_path = os.path.join("data", "approvals.csv")
    assert os.path.exists(csv_path), "Expected data/approvals.csv to exist for schema"
    df = pd.read_csv(csv_path)
    assert not df.empty, "CSV is empty"

    # Require a key column in CSV; default to 'Account' which is present in approvals.csv
    key_col = "Account"
    assert key_col in df.columns, f"Missing key column '{key_col}' in CSV"

    auth = AirtableAuth(api_key=api_key, base_id=base_id)

    # 1) GET connectivity and mapping fetch
    existing = fetch_existing_map(auth, table, key_col)
    assert isinstance(existing, dict)

    # 2) Dry-run upsert should compute create/update counts without writing
    opts = UpsertOptions(key_field=key_col, table=table, batch_size=10)
    created, updated = upsert_dataframe(df.head(5), auth, opts, dry_run=True)
    assert isinstance(created, int) and isinstance(updated, int)

    # 3) Optional: safe write to a test table only, creating a unique record
    if os.getenv("AIRTABLE_ALLOW_WRITE") == "1" and _env("AIRTABLE_TEST_TABLE"):
        unique_key = f"PYTEST_{int(time.time())}"
        row = df.head(1).copy()
        row.loc[:, key_col] = unique_key
        c, u = upsert_dataframe(row, auth, opts, dry_run=False)
        # Expect creation of a new record
        assert c == 1 and u == 0

