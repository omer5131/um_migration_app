from __future__ import annotations

import csv
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

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


def _require_requests() -> None:
    if not _HAS_REQUESTS:
        raise ImportError("requests is required. Run: pip install requests")


def list_tables(auth: AirtableAuth) -> List[Dict[str, Any]]:
    """Return Airtable base tables using the Metadata API."""
    _require_requests()
    url = f"https://api.airtable.com/v0/meta/bases/{auth.base_id}/tables"
    headers = {
        "Authorization": f"Bearer {auth.api_key}",
        "Content-Type": "application/json",
    }
    resp = requests.get(url, headers=headers, timeout=60)  # type: ignore[attr-defined]
    resp.raise_for_status()
    payload = resp.json() or {}
    return payload.get("tables", [])


def table_exists(auth: AirtableAuth, name_or_id: str) -> Optional[Dict[str, Any]]:
    """Return table object if it exists by name or id, else None."""
    for t in list_tables(auth):
        if str(t.get("id")) == name_or_id or str(t.get("name")) == name_or_id:
            return t
    return None


def create_table(auth: AirtableAuth, name: str, fields: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Create a new table with given fields using the Metadata API.

    Tries a few payload shapes for broader compatibility.
    """
    _require_requests()
    url = f"https://api.airtable.com/v0/meta/bases/{auth.base_id}/tables"
    headers = {
        "Authorization": f"Bearer {auth.api_key}",
        "Content-Type": "application/json",
    }

    # Attempt 1: {name, fields}
    payload1: Dict[str, Any] = {"name": name, "fields": fields}
    resp1 = requests.post(url, headers=headers, json=payload1, timeout=60)  # type: ignore[attr-defined]
    if resp1.status_code < 300:
        return resp1.json()

    # Attempt 2: {tables: [{name, fields}]}
    payload2: Dict[str, Any] = {"tables": [{"name": name, "fields": fields}]}
    resp2 = requests.post(url, headers=headers, json=payload2, timeout=60)  # type: ignore[attr-defined]
    if resp2.status_code < 300:
        return resp2.json()

    # Attempt 3: create empty {name}, then PATCH fields
    create_payload: Dict[str, Any] = {"name": name}
    resp3 = requests.post(url, headers=headers, json=create_payload, timeout=60)  # type: ignore[attr-defined]
    if resp3.status_code >= 300:
        text = getattr(resp3, "text", "")
        raise RuntimeError(f"Create empty table failed: HTTP {getattr(resp3, 'status_code', '?')}: {text[:1000]}")
    created = resp3.json() or {}
    table_id = created.get("id") or created.get("table", {}).get("id")
    if not table_id:
        t = table_exists(auth, name)
        table_id = t.get("id") if t else None
    if not table_id:
        raise RuntimeError("Could not determine newly created table id")

    patch_url = f"https://api.airtable.com/v0/meta/bases/{auth.base_id}/tables/{table_id}"
    patch_payload = {"fields": fields}
    resp_patch = requests.patch(patch_url, headers=headers, json=patch_payload, timeout=60)  # type: ignore[attr-defined]
    if resp_patch.status_code < 300:
        return resp_patch.json()
    text = getattr(resp_patch, "text", "")
    raise RuntimeError(f"Add fields failed: HTTP {getattr(resp_patch, 'status_code', '?')}: {text[:1000]}")


def read_csv_headers(csv_path: str) -> List[str]:
    """Read the header row (column names) from a CSV file without pandas."""
    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.reader(f)
        try:
            headers = next(reader)
        except StopIteration:
            raise ValueError("CSV appears to be empty; no header row found")
    return [h.strip() for h in headers]


def build_field_defs_from_headers(headers: List[str], *, multiline: Optional[List[str]] = None) -> List[Dict[str, Any]]:
    """Map CSV headers to Airtable field definitions.

    By default, all fields are created as singleLineText to preserve exact CSV text.
    Provide `multiline` with a subset of field names to create them as multilineText.
    """
    ml = set(multiline or [])
    out: List[Dict[str, Any]] = []
    for name in headers:
        ftype = "multilineText" if name in ml else "singleLineText"
        out.append({"name": name, "type": ftype})
    return out


def create_table_from_csv(csv_path: str, auth: AirtableAuth, *, table_name: str, primary_field: Optional[str] = None,
                          multiline_fields: Optional[List[str]] = None, dry_run: bool = False) -> Dict[str, Any]:
    """Create a table where columns mirror the CSV header exactly.

    - primary_field: if provided, it is placed first; otherwise the first CSV column is used.
    - multiline_fields: optional list of column names to create as multilineText.
    - dry_run: if True, returns a payload-like dict without calling the API.
    """
    headers = read_csv_headers(csv_path)
    if not headers:
        raise ValueError("CSV has no columns")

    # Decide primary field
    primary = primary_field or headers[0]
    # Reorder so primary is first
    ordered = [primary] + [h for h in headers if h != primary]
    fields = build_field_defs_from_headers(ordered, multiline=multiline_fields)

    if dry_run:
        return {"name": table_name, "fields": fields}

    # Safety: don't create if a table with same name already exists
    if table_exists(auth, table_name):
        raise RuntimeError(f"A table named '{table_name}' already exists in this base")

    # Create the table (empty -> add fields)
    created = create_table(auth, table_name, fields)

    # If caller asks for a specific primary field name, try to rename the primary field
    try:
        t = table_exists(auth, table_name)
        if t and primary_field:
            primary_id = t.get("primaryFieldId")
            if not primary_id:
                t = table_exists(auth, table_name)
                primary_id = (t or {}).get("primaryFieldId")
            if primary_id and isinstance(primary_id, str):
                headers = {
                    "Authorization": f"Bearer {auth.api_key}",
                    "Content-Type": "application/json",
                }
                patch_url = f"https://api.airtable.com/v0/meta/bases/{auth.base_id}/tables/{t.get('id')}"
                resp = requests.patch(
                    patch_url,
                    headers=headers,
                    json={"fields": [{"id": primary_id, "name": primary}]},  # type: ignore[name-defined]
                    timeout=60,
                )  # type: ignore[attr-defined]
                try:
                    resp.raise_for_status()
                except Exception:
                    pass
    except Exception:
        pass

    return created
