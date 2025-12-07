from __future__ import annotations

import os
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

try:
    import requests  # type: ignore
    _HAS_REQUESTS = True
except Exception:  # pragma: no cover - environment guard
    requests = None  # type: ignore
    _HAS_REQUESTS = False


@dataclass
class AirtableAuth:
    api_key: str
    base_id: str


@dataclass
class UpsertOptions:
    key_field: str
    table: str
    batch_size: int = 10  # Airtable limit is 10
    typecast: bool = False  # write as-is by default
    include_nulls: bool = False  # if True, send blanks for empty values to overwrite
    max_retries: int = 3
    retry_backoff: float = 1.5  # seconds base


def _require_requests() -> None:
    if not _HAS_REQUESTS:
        raise ImportError("requests is required. Run: pip install requests")


def _quote_segment(s: str) -> str:
    return s.replace(" ", "%20")


def load_csv(csv_path: str, *, limit: Optional[int] = None) -> pd.DataFrame:
    """Load a CSV as raw text (no NA parsing)."""
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"CSV not found: {csv_path}")
    df = pd.read_csv(csv_path, dtype=str, keep_default_na=False)
    if limit is not None and limit > 0:
        df = df.head(limit)
    return df


def _http_with_retry(method: str, url: str, headers: Dict[str, str], json_payload: Dict[str, Any], *,
                     max_retries: int, backoff: float) -> Any:
    assert requests is not None
    attempt = 0
    last_exc: Optional[Exception] = None
    while attempt <= max_retries:
        try:
            resp = requests.request(method, url, headers=headers, json=json_payload, timeout=60)
            if resp.status_code >= 400:
                # Retry on 429/5xx
                if resp.status_code in (429,) or 500 <= resp.status_code < 600:
                    raise RuntimeError(f"HTTP {resp.status_code}: {getattr(resp, 'text', '')[:1000]}")
                raise RuntimeError(f"HTTP {resp.status_code}: {getattr(resp, 'text', '')[:1000]}")
            return resp.json()
        except Exception as e:  # pragma: no cover - network branch
            last_exc = e
            if attempt == max_retries:
                break
            time.sleep(backoff ** attempt)
            attempt += 1
    raise RuntimeError(f"Airtable request failed after {max_retries+1} attempts: {last_exc}")


def fetch_existing_map(auth: AirtableAuth, table: str, key_field: str) -> Dict[str, Dict[str, Any]]:
    """Fetch all records and return a map key_value -> record (id, fields)."""
    _require_requests()
    headers = {"Authorization": f"Bearer {auth.api_key}"}
    url = f"https://api.airtable.com/v0/{auth.base_id}/{_quote_segment(table)}"
    params: Dict[str, Any] = {"pageSize": 100}
    out: Dict[str, Dict[str, Any]] = {}
    while True:
        resp = requests.get(url, headers=headers, params=params, timeout=60)  # type: ignore[attr-defined]
        resp.raise_for_status()
        payload = resp.json() or {}
        for r in payload.get("records", []):
            fields = r.get("fields", {}) or {}
            key = str(fields.get(key_field, "")).strip()
            if key:
                out[key] = r
        offset = payload.get("offset")
        if not offset:
            break
        params["offset"] = offset
    return out


def _normalize_row_raw(row: Dict[str, Any], include_nulls: bool) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for k, v in row.items():
        # All values should be strings (due to dtype=str). Ensure str for safety.
        if v is None:
            if include_nulls:
                out[k] = ""
            continue
        sv = str(v)
        if sv == "":
            if include_nulls:
                out[k] = ""
            continue
        out[k] = sv
    return out


def upsert_dataframe(df: pd.DataFrame, auth: AirtableAuth, opts: UpsertOptions, *, dry_run: bool = False) -> Tuple[int, int]:
    """Upsert a DataFrame into Airtable by key column, writing values as-is.

    Returns (created, updated) counts.
    """
    _require_requests()
    if df is None or df.empty:
        return 0, 0

    existing = fetch_existing_map(auth, opts.table, opts.key_field)

    to_create: List[Dict[str, Any]] = []
    to_update: List[Dict[str, Any]] = []

    for row in df.to_dict(orient="records"):
        key_val = str(row.get(opts.key_field, "")).strip()
        if not key_val:
            continue
        fields = _normalize_row_raw(row, include_nulls=opts.include_nulls)
        if key_val in existing:
            rid = existing[key_val].get("id")
            if rid:
                to_update.append({"id": rid, "fields": fields})
        else:
            to_create.append({"fields": fields})

    if dry_run:
        return len(to_create), len(to_update)

    url = f"https://api.airtable.com/v0/{auth.base_id}/{_quote_segment(opts.table)}"
    headers = {"Authorization": f"Bearer {auth.api_key}", "Content-Type": "application/json"}

    created = 0
    for i in range(0, len(to_create), max(1, min(10, opts.batch_size))):
        batch = to_create[i : i + opts.batch_size]
        payload = {"records": batch, "typecast": bool(opts.typecast)}
        resp_json = _http_with_retry("POST", url, headers, payload, max_retries=opts.max_retries, backoff=opts.retry_backoff)
        created += len((resp_json or {}).get("records", []))

    updated = 0
    for i in range(0, len(to_update), max(1, min(10, opts.batch_size))):
        batch = to_update[i : i + opts.batch_size]
        payload = {"records": batch, "typecast": bool(opts.typecast)}
        resp_json = _http_with_retry("PATCH", url, headers, payload, max_retries=opts.max_retries, backoff=opts.retry_backoff)
        updated += len((resp_json or {}).get("records", []))

    return created, updated

