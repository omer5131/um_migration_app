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

    # Align behavior with CSV sync script: clean cells and trim whitespace
    def _clean_cell(val: Any) -> Any:
        try:
            import pandas as _pd  # local alias
            if isinstance(val, getattr(_pd, 'Series', ())) or str(type(val)).endswith(".Series'>"):
                try:
                    return _clean_cell(val.iloc[0])
                except Exception:
                    return None
        except Exception:
            pass
        if isinstance(val, dict):
            try:
                return json.dumps(val, ensure_ascii=False)
            except Exception:
                return str(val)
        if isinstance(val, list):
            return ", ".join(map(str, val))
        if isinstance(val, str):
            return val.strip()
        try:
            if pd.isna(val):
                return None
        except Exception:
            pass
        return val

    try:
        df = df.applymap(_clean_cell)
    except Exception:
        pass

    existing = _fetch_all_by_key(cfg, key_field)

    to_create: List[Dict[str, Any]] = []
    to_update: List[Dict[str, Any]] = []

    CORE_FIELDS = {
        'Final plan',
        'Add-ons needed',
        'Gained by plan (not currently in project)',
        'Approved By',
        'Approved At',
        'Comment',
        'Under trial',
    }
    # Always update these fields regardless of timestamp guards
    ALWAYS_UPDATE_FIELDS = {
        'Add-ons needed',
        'Gained by plan (not currently in project)',
    }

    def _is_empty(v: Any) -> bool:
        if v is None:
            return True
        if isinstance(v, str):
            return len(v.strip()) == 0
        if isinstance(v, (list, dict)):
            return len(v) == 0
        return False

    def _parse_ts(v: Any) -> Optional[int]:
        if v is None:
            return None
        try:
            if isinstance(v, (int, float)):
                return int(v)
            s = str(v).strip()
            if not s:
                return None
            # Expect ISO; pandas can parse
            ts = pd.to_datetime(s, utc=True, errors='coerce')
            if ts is not None and not pd.isna(ts):
                return int(ts.timestamp())
        except Exception:
            return None
        return None

    def _normalize_value(v: Any) -> Any:
        # None stays None
        if v is None:
            return None
        # Pandas NA/NaN handling for scalars
        try:
            if isinstance(v, float) and pd.isna(v):
                return None
        except Exception:
            pass
        # Series -> join string values; drop NA
        try:
            import pandas as _pd
            if isinstance(v, getattr(_pd, 'Series', ())) or str(type(v)).endswith(".Series'>"):
                try:
                    vals = [str(x).strip() for x in v.dropna().tolist() if str(x).strip()]
                    return ", ".join(vals) if vals else None
                except Exception:
                    return str(v)
        except Exception:
            pass
        # Lists/dicts pass through as-is (Airtable can accept arrays for multiselects if configured)
        if isinstance(v, (list, dict)):
            return v
        # Generic NA-like
        try:
            if pd.isna(v):
                return None
        except Exception:
            pass
        # Fallback stringify
        return v

    for row in df.to_dict(orient="records"):
        key_val = str(row.get(key_field, "")).strip()
        if not key_val:
            continue
        # Filter out None, NaN, and convert numeric NaN to None
        fields = {}
        for k, v in row.items():
            nv = _normalize_value(v)
            if nv is None:
                continue
            fields[k] = nv

        if key_val in existing:
            ex_fields = existing[key_val].get("fields", {}) or {}
            merged: Dict[str, Any] = {}
            our_ts = _parse_ts(fields.get('Approved At'))
            ex_ts = _parse_ts(ex_fields.get('Approved At'))
            for k, v in fields.items():
                if _is_empty(v):
                    continue
                if k in ALWAYS_UPDATE_FIELDS:
                    merged[k] = v
                elif k in CORE_FIELDS:
                    # Only update core fields if our timestamp is newer/equal, else skip to avoid overriding
                    if ex_ts is not None and our_ts is not None and our_ts < ex_ts:
                        continue
                    merged[k] = v
                else:
                    # Non-core: only fill if missing/empty in Airtable to avoid overriding others' values
                    if k in ex_fields and not _is_empty(ex_fields.get(k)):
                        continue
                    merged[k] = v
            if merged:
                to_update.append({"id": existing[key_val]["id"], "fields": merged})
        else:
            to_create.append({"fields": fields})

    url = f"https://api.airtable.com/v0/{cfg.base_id}/{_encode_segment(cfg.table_id_or_name)}"
    headers = {"Authorization": f"Bearer {cfg.api_key}", "Content-Type": "application/json"}

    created = 0
    for batch in _chunk(to_create, 10):
        payload = {"records": batch, "typecast": typecast}
        resp = requests.post(url, headers=headers, json=payload, timeout=30)
        try:
            resp.raise_for_status()
        except Exception as e:
            # Provide actionable context when Airtable rejects the payload (e.g., 422)
            details = resp.text[:1000] if hasattr(resp, "text") else str(e)
            sample_keys = list(batch[0].get("fields", {}).keys()) if batch else []
            raise RuntimeError(
                f"Airtable create failed: HTTP {getattr(resp, 'status_code', 'unknown')} | sample field keys: {sample_keys} | response: {details}"
            ) from e
        created += len(resp.json().get("records", []))

    updated = 0
    for batch in _chunk(to_update, 10):
        payload = {"records": batch, "typecast": typecast}
        resp = requests.patch(url, headers=headers, json=payload, timeout=30)
        try:
            resp.raise_for_status()
        except Exception as e:
            details = resp.text[:1000] if hasattr(resp, "text") else str(e)
            sample_keys = list(batch[0].get("fields", {}).keys()) if batch else []
            raise RuntimeError(
                f"Airtable update failed: HTTP {getattr(resp, 'status_code', 'unknown')} | sample field keys: {sample_keys} | response: {details}"
            ) from e
        updated += len(resp.json().get("records", []))

    return created, updated


def ensure_field_exists(cfg: AirtableConfig, desired_field_name: str, field_type: str = "multilineText") -> bool:
    """Best-effort attempt to ensure a field exists on an Airtable table.

    Requires access to the Airtable Metadata API. If unavailable or the request fails,
    this function safely returns False without interrupting the caller.

    Args:
        cfg: AirtableConfig with base_id and table_id_or_name
        desired_field_name: Field name to ensure exists (case-sensitive)
        field_type: Airtable field type (e.g., 'multilineText', 'singleLineText')

    Returns:
        True if the field exists or was created; False otherwise.
    """
    _assert_requests()
    try:
        headers = {
            "Authorization": f"Bearer {cfg.api_key}",
            "Content-Type": "application/json",
        }
        # List base tables to locate table ID and existing fields
        meta_url = f"https://api.airtable.com/v0/meta/bases/{cfg.base_id}/tables"
        r = requests.get(meta_url, headers=headers, timeout=30)
        r.raise_for_status()
        tables = (r.json() or {}).get("tables", [])
        target = None
        for t in tables:
            if str(t.get("id")) == str(cfg.table_id_or_name) or str(t.get("name")) == str(cfg.table_id_or_name):
                target = t
                break
        if not target:
            return False

        # Already exists?
        existing_fields = {str(f.get("name")) for f in (target.get("fields") or [])}
        if desired_field_name in existing_fields:
            return True

        table_id = target.get("id")
        if not table_id:
            return False

        # Try PATCH table to add a field
        patch_url = f"https://api.airtable.com/v0/meta/bases/{cfg.base_id}/tables/{table_id}"
        payload = {"fields": [{"name": desired_field_name, "type": field_type}]}
        rp = requests.patch(patch_url, headers=headers, json=payload, timeout=30)
        try:
            rp.raise_for_status()
            return True
        except Exception:
            # Fallback: attempt POST to /fields (older schema variants)
            try:
                post_url = f"https://api.airtable.com/v0/meta/bases/{cfg.base_id}/tables/{table_id}/fields"
                rp2 = requests.post(post_url, headers=headers, json={"name": desired_field_name, "type": field_type}, timeout=30)
                rp2.raise_for_status()
                return True
            except Exception:
                return False
    except Exception:
        # If metadata API not available or any error occurs, just return False
        return False


def ensure_field_type(cfg: AirtableConfig, field_name: str, desired_type: str = "multilineText") -> bool:
    """Ensure a field exists and has the desired type, attempting to convert if needed.

    Uses the Airtable Metadata API. Returns True on success; False if not allowed or failed.
    """
    _assert_requests()
    try:
        headers = {
            "Authorization": f"Bearer {cfg.api_key}",
            "Content-Type": "application/json",
        }
        # Locate table and existing field
        meta_url = f"https://api.airtable.com/v0/meta/bases/{cfg.base_id}/tables"
        r = requests.get(meta_url, headers=headers, timeout=30)
        r.raise_for_status()
        tables = (r.json() or {}).get("tables", [])
        target = None
        for t in tables:
            if str(t.get("id")) == str(cfg.table_id_or_name) or str(t.get("name")) == str(cfg.table_id_or_name):
                target = t
                break
        if not target:
            return False

        fields = target.get("fields") or []
        existing = None
        for f in fields:
            if str(f.get("name")) == field_name:
                existing = f
                break

        table_id = target.get("id")
        if not table_id:
            return False

        patch_url = f"https://api.airtable.com/v0/meta/bases/{cfg.base_id}/tables/{table_id}"

        if existing is None:
            # Create new field with the desired type
            payload = {"fields": [{"name": field_name, "type": desired_type}]}
            rp = requests.patch(patch_url, headers=headers, json=payload, timeout=30)
            try:
                rp.raise_for_status()
                return True
            except Exception:
                try:
                    post_url = f"https://api.airtable.com/v0/meta/bases/{cfg.base_id}/tables/{table_id}/fields"
                    rp2 = requests.post(post_url, headers=headers, json={"name": field_name, "type": desired_type}, timeout=30)
                    rp2.raise_for_status()
                    return True
                except Exception:
                    return False

        # If exists and already the desired type, done
        current_type = str(existing.get("type", ""))
        if current_type == desired_type:
            return True

        # Attempt to convert type via PATCH using field id
        field_id = existing.get("id")
        if not field_id:
            return False
        payload = {"fields": [{"id": field_id, "type": desired_type}]}
        rp = requests.patch(patch_url, headers=headers, json=payload, timeout=30)
        try:
            rp.raise_for_status()
            return True
        except Exception:
            return False
    except Exception:
        return False

def upsert_single(cfg: AirtableConfig, fields: Dict[str, Any], key_field: str = "Account", typecast: bool = True) -> str:
    """Upsert a single record by key_field. Returns record ID."""
    _assert_requests()
    key_val = str(fields.get(key_field, "")).strip()
    if not key_val:
        raise ValueError(f"Missing key field '{key_field}' in fields")

    # Clean fields: remove None and NaN values
    def _normalize_value(v: Any) -> Any:
        if v is None:
            return None
        try:
            if isinstance(v, float) and pd.isna(v):
                return None
        except Exception:
            pass
        try:
            import pandas as _pd
            if isinstance(v, getattr(_pd, 'Series', ())) or str(type(v)).endswith(".Series'>"):
                try:
                    vals = [str(x).strip() for x in v.dropna().tolist() if str(x).strip()]
                    return ", ".join(vals) if vals else None
                except Exception:
                    return str(v)
        except Exception:
            pass
        if isinstance(v, (list, dict)):
            return v
        try:
            if pd.isna(v):
                return None
        except Exception:
            pass
        return v

    clean_fields = {}
    for k, v in fields.items():
        nv = _normalize_value(v)
        if nv is None:
            continue
        clean_fields[k] = nv

    existing = _fetch_all_by_key(cfg, key_field)

    url = f"https://api.airtable.com/v0/{cfg.base_id}/{_encode_segment(cfg.table_id_or_name)}"
    headers = {"Authorization": f"Bearer {cfg.api_key}", "Content-Type": "application/json"}

    if key_val in existing:
        rec = existing[key_val]
        ex_fields = rec.get('fields', {}) or {}
        CORE_FIELDS = {
            'Final plan',
            'Add-ons needed',
            'Gained by plan (not currently in project)',
            'Approved By',
            'Approved At',
            'Comment',
            'Under trial',
        }
        def _is_empty(v: Any) -> bool:
            if v is None:
                return True
            if isinstance(v, str):
                return len(v.strip()) == 0
            if isinstance(v, (list, dict)):
                return len(v) == 0
            return False
        def _parse_ts(v: Any) -> Optional[int]:
            if v is None:
                return None
            try:
                if isinstance(v, (int, float)):
                    return int(v)
                s = str(v).strip()
                if not s:
                    return None
                ts = pd.to_datetime(s, utc=True, errors='coerce')
                if ts is not None and not pd.isna(ts):
                    return int(ts.timestamp())
            except Exception:
                return None
            return None
        our_ts = _parse_ts(clean_fields.get('Approved At'))
        ex_ts = _parse_ts(ex_fields.get('Approved At'))
        merged: Dict[str, Any] = {}
        for k, v in clean_fields.items():
            if _is_empty(v):
                continue
            if k in CORE_FIELDS:
                if ex_ts is not None and our_ts is not None and our_ts < ex_ts:
                    continue
                merged[k] = v
            else:
                if k in ex_fields and not _is_empty(ex_fields.get(k)):
                    continue
                merged[k] = v
        if not merged:
            return rec.get('id')
        payload = {"records": [{"id": rec.get('id'), "fields": merged}], "typecast": typecast}
        resp = requests.patch(url, headers=headers, json=payload, timeout=30)
        try:
            resp.raise_for_status()
        except Exception as e:
            details = resp.text[:1000] if hasattr(resp, "text") else str(e)
            raise RuntimeError(
                f"Airtable update failed: HTTP {getattr(resp, 'status_code', 'unknown')} | field keys: {list(clean_fields.keys())} | response: {details}"
            ) from e
        return rec.get('id')
    else:
        payload = {"records": [{"fields": clean_fields}], "typecast": typecast}
        resp = requests.post(url, headers=headers, json=payload, timeout=30)
        try:
            resp.raise_for_status()
        except Exception as e:
            details = resp.text[:1000] if hasattr(resp, "text") else str(e)
            raise RuntimeError(
                f"Airtable create failed: HTTP {getattr(resp, 'status_code', 'unknown')} | field keys: {list(clean_fields.keys())} | response: {details}"
            ) from e
        return resp.json()["records"][0]["id"]
