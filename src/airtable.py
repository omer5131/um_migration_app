from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

try:
    import requests  # lightweight; used for Airtable REST API
    HAS_REQUESTS = True
except Exception:
    requests = None
    HAS_REQUESTS = False


@dataclass
class AirtableConfig:
    api_key: str
    base_id: str
    table_id_or_name: str
    view: Optional[str] = None
    page_size: int = 100


def _assert_requests():
    if not HAS_REQUESTS:
        raise ImportError("requests is required. Run: pip install requests")


def fetch_records(cfg: AirtableConfig) -> List[Dict[str, Any]]:
    """Fetch all records from Airtable table/view via REST API.

    Returns list of Airtable record objects: {id, fields, createdTime}.
    """
    _assert_requests()

    headers = {
        "Authorization": f"Bearer {cfg.api_key}",
    }
    url = f"https://api.airtable.com/v0/{cfg.base_id}/{_encode_segment(cfg.table_id_or_name)}"

    params: Dict[str, Any] = {"pageSize": cfg.page_size}
    if cfg.view:
        params["view"] = cfg.view

    records: List[Dict[str, Any]] = []
    offset: Optional[str] = None
    while True:
        if offset:
            params["offset"] = offset
        resp = requests.get(url, headers=headers, params=params, timeout=30)
        resp.raise_for_status()
        payload = resp.json()
        records.extend(payload.get("records", []))
        offset = payload.get("offset")
        if not offset:
            break
    return records


def records_to_dataframe(records: List[Dict[str, Any]]) -> pd.DataFrame:
    """Flatten Airtable records -> DataFrame of `fields` with `id` column."""
    if not records:
        return pd.DataFrame()
    rows = []
    for r in records:
        fields = r.get("fields", {}) or {}
        row = {**fields, "_airtable_id": r.get("id")}
        rows.append(row)
    return pd.DataFrame(rows)


def _encode_segment(segment: str) -> str:
    # Airtable allows table names in path; keep simple URL-escape for spaces
    return segment.replace(" ", "%20")


def save_cache(df: pd.DataFrame, path: str) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    payload = {
        "schema": list(map(str, df.columns.tolist() if not df.empty else [])),
        "rows": df.to_dict(orient="records"),
        "saved_at": int(time.time()),
    }
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f)


def load_cache(path: str) -> Tuple[pd.DataFrame, Optional[int]]:
    with open(path, "r", encoding="utf-8") as f:
        payload = json.load(f)
    rows = payload.get("rows", [])
    df = pd.DataFrame(rows)
    return df, payload.get("saved_at")


def load_cached_or_fetch(cfg: AirtableConfig, cache_path: str, ttl_seconds: int | None = None) -> pd.DataFrame:
    """Load cached Airtable data if fresh; otherwise fetch and refresh cache.

    - ttl_seconds=None: always use cache if present; never auto-refresh.
    - ttl_seconds=0: always refresh.
    - ttl_seconds>0: refresh if cache is older than TTL.
    """
    # Try cache first
    if os.path.exists(cache_path):
        try:
            df, saved_at = load_cache(cache_path)
            if ttl_seconds is None:
                return df
            if isinstance(saved_at, int) and (time.time() - saved_at) < ttl_seconds:
                return df
        except Exception:
            # fall through to refetch on cache error
            pass

    # Fetch and refresh cache
    records = fetch_records(cfg)
    df = records_to_dataframe(records)
    save_cache(df, cache_path)
    return df


# ------- Write helpers (Approvals upsert) -------

def _fetch_all_by_key(cfg: AirtableConfig, key_field: str) -> Dict[str, Dict[str, Any]]:
    """Return mapping key -> record dict for existing table."""
    existing = {}
    for r in fetch_records(cfg):
        fields = r.get("fields", {}) or {}
        key = str(fields.get(key_field, "")).strip()
        if key:
            existing[key] = r
    return existing


def _chunk(iterable: List[Any], n: int) -> List[List[Any]]:
    return [iterable[i:i+n] for i in range(0, len(iterable), n)]


def upsert_dataframe(cfg: AirtableConfig, df: pd.DataFrame, key_field: str = "Account", typecast: bool = True) -> Tuple[int, int]:
    """Upsert DataFrame rows into Airtable by key_field.

    Returns (created_count, updated_count).
    """
    _assert_requests()
    if df is None or df.empty:
        return 0, 0

    existing = _fetch_all_by_key(cfg, key_field)

    to_create: List[Dict[str, Any]] = []
    to_update: List[Dict[str, Any]] = []

    for row in df.to_dict(orient="records"):
        key_val = str(row.get(key_field, "")).strip()
        if not key_val:
            continue
        fields = {k: v for k, v in row.items() if v is not None}
        if key_val in existing:
            to_update.append({"id": existing[key_val]["id"], "fields": fields})
        else:
            to_create.append({"fields": fields})

    url = f"https://api.airtable.com/v0/{cfg.base_id}/{_encode_segment(cfg.table_id_or_name)}"
    headers = {"Authorization": f"Bearer {cfg.api_key}", "Content-Type": "application/json"}

    created = 0
    for batch in _chunk(to_create, 10):
        payload = {"records": batch, "typecast": typecast}
        resp = requests.post(url, headers=headers, json=payload, timeout=30)
        resp.raise_for_status()
        created += len(resp.json().get("records", []))

    updated = 0
    for batch in _chunk(to_update, 10):
        payload = {"records": batch, "typecast": typecast}
        resp = requests.patch(url, headers=headers, json=payload, timeout=30)
        resp.raise_for_status()
        updated += len(resp.json().get("records", []))

    return created, updated


def upsert_single(cfg: AirtableConfig, fields: Dict[str, Any], key_field: str = "Account", typecast: bool = True) -> str:
    """Upsert a single record by key_field. Returns record ID."""
    _assert_requests()
    key_val = str(fields.get(key_field, "")).strip()
    if not key_val:
        raise ValueError(f"Missing key field '{key_field}' in fields")
    existing = _fetch_all_by_key(cfg, key_field)

    url = f"https://api.airtable.com/v0/{cfg.base_id}/{_encode_segment(cfg.table_id_or_name)}"
    headers = {"Authorization": f"Bearer {cfg.api_key}", "Content-Type": "application/json"}

    if key_val in existing:
        rec_id = existing[key_val]["id"]
        payload = {"records": [{"id": rec_id, "fields": fields}], "typecast": typecast}
        resp = requests.patch(url, headers=headers, json=payload, timeout=30)
        resp.raise_for_status()
        return rec_id
    else:
        payload = {"records": [{"fields": fields}], "typecast": typecast}
        resp = requests.post(url, headers=headers, json=payload, timeout=30)
        resp.raise_for_status()
        return resp.json()["records"][0]["id"]

