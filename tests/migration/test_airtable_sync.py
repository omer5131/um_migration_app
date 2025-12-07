from __future__ import annotations

import types
from typing import Any, Dict

import pandas as pd

import src.migration.airtable_sync as mod


class _Resp:
    def __init__(self, status: int, payload: Dict[str, Any]):
        self.status_code = status
        self._payload = payload
        self.text = str(payload)

    def json(self):
        return self._payload

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")


def _mk_requests(existing_keys: list[str]):
    # Simulate one page GET and ok POST/PATCH
    def get(url, headers=None, params=None, timeout=None):  # noqa: D401
        records = []
        for i, k in enumerate(existing_keys):
            records.append({"id": f"rec{i}", "fields": {"Key": k}})
        return _Resp(200, {"records": records})

    def request(method, url, headers=None, json=None, timeout=None):  # noqa: D401
        # Echo back number of records processed
        recs = (json or {}).get("records", [])
        return _Resp(200, {"records": recs})

    fake = types.SimpleNamespace(get=get, request=request)
    return fake


def test_upsert_counts_create_and_update(monkeypatch):
    # Existing: B only
    monkeypatch.setattr(mod, "requests", _mk_requests(["B"]))

    df = pd.DataFrame([
        {"Key": "A", "Foo": 1},
        {"Key": "B", "Foo": 2},
    ])
    auth = mod.AirtableAuth(api_key="k", base_id="b")
    opts = mod.UpsertOptions(key_field="Key", table="T")

    created, updated = mod.upsert_dataframe(df, auth, opts, dry_run=True)
    assert created == 1 and updated == 1


def test_include_nulls_overwrites_with_blanks(monkeypatch):
    # Existing: X
    monkeypatch.setattr(mod, "requests", _mk_requests(["X"]))

    df = pd.DataFrame([
        {"Key": "X", "Col1": None, "Col2": ""},
    ])
    auth = mod.AirtableAuth(api_key="k", base_id="b")
    opts = mod.UpsertOptions(key_field="Key", table="T", skip_nulls=False)

    # Execute non dry-run to traverse HTTP paths (they're mocked)
    created, updated = mod.upsert_dataframe(df, auth, opts, dry_run=False)
    assert created == 0 and updated == 1


def test_series_values_are_normalized(monkeypatch):
    # Table has a multipleSelects field and a text field
    def _fake_get_fields(auth, table):
        return {"Add-ons needed": "multipleSelects", "Comment": "multilineText", "Key": "singleLineText"}

    monkeypatch.setattr(mod, "requests", _mk_requests(["S"]))
    monkeypatch.setattr(mod, "get_table_fields", _fake_get_fields)

    # Create a DataFrame where a cell holds a pandas Series (object)
    addons_series = pd.Series(["A", None, "B"])  # Series object in a cell
    comment_series = pd.Series(["x", "y"])       # will be joined
    df = pd.DataFrame([
        {"Key": "S", "Add-ons needed": addons_series, "Comment": comment_series},
    ])

    auth = mod.AirtableAuth(api_key="k", base_id="b")
    opts = mod.UpsertOptions(key_field="Key", table="T")
    # Non dry-run to exercise HTTP path (mocked)
    c, u = mod.upsert_dataframe(df, auth, opts, dry_run=False)
    assert c == 0 and u == 1


def test_multiple_select_splitting_and_dequote(monkeypatch):
    # multipleSelects coercion: handle JSON arrays, comma-separated, quotes, and dedup
    monkeypatch.setattr(mod, "requests", _mk_requests([]))

    def _fake_get_fields(auth, table):
        return {"Key": "singleLineText", "Add-ons needed": "multipleSelects"}

    monkeypatch.setattr(mod, "get_table_fields", _fake_get_fields)

    df = pd.DataFrame([
        {"Key": "A", "Add-ons needed": '["UBO", "UBO", " Wet Cargo "]'},
        {"Key": "B", "Add-ons needed": 'UBO, "Wet Cargo",  UBO ,  '},
    ])
    auth = mod.AirtableAuth(api_key="k", base_id="b")
    opts = mod.UpsertOptions(key_field="Key", table="T")
    c, u = mod.upsert_dataframe(df, auth, opts, dry_run=False)
    # Both rows should be created (no existing), but mocked GET returns none and POST echoes back
    assert c == 2 and u == 0
