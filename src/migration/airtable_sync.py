from __future__ import annotations

import math
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
    batch_size: int = 10  # Airtable limit
    typecast: bool = True
    max_retries: int = 4
    retry_backoff: float = 1.5  # seconds base for exponential backoff
    skip_nulls: bool = True  # do not send null/NaN/empty fields
    filter_unknown_fields: bool = True  # limit to existing Airtable fields via metadata
    field_aliases: Optional[Dict[str, str]] = None  # map CSV column -> Airtable field name


def _require_requests() -> None:
    if not _HAS_REQUESTS:
        raise ImportError("requests is required. Run: pip install requests")


def _quote_segment(s: str) -> str:
    # Minimal quoting to support spaces in table names
    return s.replace(" ", "%20")


def _is_null(v: Any) -> bool:
    if v is None:
        return True
    try:
        if isinstance(v, float) and math.isnan(v):
            return True
    except Exception:
        pass
    try:
        # pandas NA-like
        if pd.isna(v):
            return True
    except Exception:
        pass
    if isinstance(v, str) and v.strip() == "":
        return True
    return False


def _normalize_value(value: Any, field_type: Optional[str] = None, *, skip_nulls: bool = True) -> Any:
    # Null handling
    if skip_nulls and _is_null(value):
        return None
    # Pandas Series -> list or joined string
    try:
        if isinstance(value, pd.Series):
            vals = [x for x in value.dropna().tolist() if not _is_null(x)]
            if field_type and field_type.lower() == "multipleselects":
                return [str(x) for x in vals]
            return ", ".join([str(x) for x in vals]) if vals else None
    except Exception:
        pass
    return value


def _normalize_fields(row: Dict[str, Any], skip_nulls: bool, known_fields: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for k, v in row.items():
        ftype = (known_fields or {}).get(k)
        nv = _normalize_value(v, ftype, skip_nulls=skip_nulls)
        if skip_nulls and nv is None:
            continue
        out[k] = nv
    return out


def _clean_item(s: str) -> str:
    s = s.strip()
    # Trim surrounding quotes
    if (s.startswith('"') and s.endswith('"')) or (s.startswith("'") and s.endswith("'")):
        s = s[1:-1].strip()
    return s


def _coerce_value_for_type(value: Any, ftype: str) -> Any:
    """Coerce a CSV value to Airtable field type when possible using simple rules.

    - date: ISO date string (YYYY-MM-DD)
    - dateTime: ISO datetime string with Z
    - number / percent / currency: numeric
    - checkbox: boolean
    - multipleSelects: list[str] (split on commas if given a string)
    """
    try:
        t = (ftype or "").lower()
        if t in ("singlelinetext", "multilinetext"):
            if _is_null(value):
                return None
            return str(value)
        if t in ("date",):
            ts = pd.to_datetime(value, utc=True, errors="coerce")
            if ts is not None and not pd.isna(ts):
                return ts.date().isoformat()
            return value
        if t in ("datetime", "dateTime".lower()):
            ts = pd.to_datetime(value, utc=True, errors="coerce")
            if ts is not None and not pd.isna(ts):
                return ts.isoformat().replace("+00:00", "Z")
            return value
        if t in ("number", "percent", "currency"):
            if isinstance(value, str) and value.strip() == "":
                return None
            try:
                return float(value)
            except Exception:
                return value
        if t == "checkbox":
            if isinstance(value, bool):
                return value
            s = str(value).strip().lower()
            return s in ("1", "true", "yes", "y")
        if t == "multipleselects":
            if isinstance(value, list):
                cleaned = [_clean_item(str(x)) for x in value if not _is_null(x)]
                # de-duplicate while preserving order
                dedup = list(dict.fromkeys([c for c in cleaned if c]))
                return dedup
            if isinstance(value, str):
                # accept JSON-like list or comma-separated
                if value.strip().startswith("[") and value.strip().endswith("]"):
                    try:
                        import json as _json
                        arr = _json.loads(value)
                        if isinstance(arr, list):
                            cleaned = [_clean_item(str(x)) for x in arr if not _is_null(x)]
                            return list(dict.fromkeys([c for c in cleaned if c]))
                    except Exception:
                        pass
                # fallback split by comma
                items = [_clean_item(s) for s in value.split(",")]
                return list(dict.fromkeys([s for s in items if s]))
        return value
    except Exception:
        return value


def _http_with_retry(method: str, url: str, headers: Dict[str, str], json_payload: Dict[str, Any], *,
                     max_retries: int, backoff: float) -> Any:
    """Perform HTTP with basic retry on 429/5xx."""
    assert requests is not None  # for type checkers
    attempt = 0
    last_exc: Optional[Exception] = None
    while attempt <= max_retries:
        try:
            resp = requests.request(method, url, headers=headers, json=json_payload, timeout=60)
            # Provide richer context on failure, and retry on 429/5xx
            if resp.status_code >= 400:
                # Retry on rate limit or server errors
                if resp.status_code in (429,) or 500 <= resp.status_code < 600:
                    raise RuntimeError(f"HTTP {resp.status_code}: {resp.text[:1000]}")
                # Non-retryable (4xx) -> raise with response body
                raise RuntimeError(f"HTTP {resp.status_code}: {resp.text[:1000]}")
            # Success
            return resp.json()
        except Exception as e:  # pragma: no cover - network branch
            last_exc = e
            if attempt == max_retries:
                break
            sleep_for = (backoff ** attempt)
            time.sleep(sleep_for)
            attempt += 1
    # Exhausted
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


def get_table_fields(auth: AirtableAuth, table: str) -> Dict[str, str]:
    """Return mapping field_name -> field_type for a table using the Metadata API.

    If metadata access is not allowed, returns an empty dict.
    """
    _require_requests()
    headers = {
        "Authorization": f"Bearer {auth.api_key}",
        "Content-Type": "application/json",
    }
    meta_url = f"https://api.airtable.com/v0/meta/bases/{auth.base_id}/tables"
    try:
        r = requests.get(meta_url, headers=headers, timeout=60)  # type: ignore[attr-defined]
        r.raise_for_status()
        payload = r.json() or {}
        for t in payload.get("tables", []):
            if str(t.get("id")) == table or str(t.get("name")) == table:
                fields = t.get("fields") or []
                return {str(f.get("name")): str(f.get("type")) for f in fields}
        return {}
    except Exception:  # pragma: no cover - network branch
        return {}


def upsert_dataframe(df: pd.DataFrame, auth: AirtableAuth, opts: UpsertOptions, *, dry_run: bool = False) -> Tuple[int, int]:
    """Upsert a DataFrame into Airtable by key column.

    Returns (created, updated) counts.
    """
    _require_requests()
    if df is None or df.empty:
        return 0, 0

    # Build existing map
    existing = fetch_existing_map(auth, opts.table, opts.key_field)

    to_create: List[Dict[str, Any]] = []
    to_update: List[Dict[str, Any]] = []

    # Prepare entries
    known_fields = get_table_fields(auth, opts.table) if opts.filter_unknown_fields else {}
    # Apply header aliases if provided
    if opts.field_aliases:
        df = df.rename(columns=opts.field_aliases)
    records = df.to_dict(orient="records")
    for row in records:
        key_val = str(row.get(opts.key_field, "")).strip()
        if not key_val:
            continue
        fields = _normalize_fields(row, skip_nulls=opts.skip_nulls, known_fields=known_fields)
        if known_fields:
            # Filter to fields that exist in Airtable to avoid 422 errors from unknown columns
            filtered = {}
            for k, v in fields.items():
                if k not in known_fields:
                    continue
                filtered[k] = _coerce_value_for_type(v, known_fields[k])
            fields = filtered
        if key_val in existing:
            rec_id = existing[key_val].get("id")
            if not rec_id:
                continue
            to_update.append({"id": rec_id, "fields": fields})
        else:
            to_create.append({"fields": fields})

    # Short-circuit for dry-run
    if dry_run:
        return len(to_create), len(to_update)

    url = f"https://api.airtable.com/v0/{auth.base_id}/{_quote_segment(opts.table)}"
    headers = {
        "Authorization": f"Bearer {auth.api_key}",
        "Content-Type": "application/json",
    }

    created = 0
    for i in range(0, len(to_create), opts.batch_size):
        batch = to_create[i : i + opts.batch_size]
        payload = {"records": batch, "typecast": opts.typecast}
        resp_json = _http_with_retry("POST", url, headers, payload, max_retries=opts.max_retries, backoff=opts.retry_backoff)
        created += len(resp_json.get("records", []))

    updated = 0
    for i in range(0, len(to_update), opts.batch_size):
        batch = to_update[i : i + opts.batch_size]
        payload = {"records": batch, "typecast": opts.typecast}
        resp_json = _http_with_retry("PATCH", url, headers, payload, max_retries=opts.max_retries, backoff=opts.retry_backoff)
        updated += len(resp_json.get("records", []))

    return created, updated


def load_csv(csv_path: str, *, limit: Optional[int] = None) -> pd.DataFrame:
    """Load a CSV file into a DataFrame with basic NA handling.

    - Preserves column names as-is.
    - Optionally limits rows for testing.
    """
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"CSV not found: {csv_path}")
    df = pd.read_csv(csv_path)
    if limit is not None and limit > 0:
        df = df.head(limit)
    return df
