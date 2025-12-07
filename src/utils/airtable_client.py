from __future__ import annotations

from typing import Optional

try:
    import streamlit as st  # type: ignore
except Exception:
    st = None  # type: ignore

from src.airtable import AirtableConfig
from src.config import AIRTABLE as AT_CFG


def get_airtable_cfg(kind: str = "table") -> Optional[AirtableConfig]:
    """Build an AirtableConfig using Streamlit secrets or env via src.config.

    kind:
      - "table": use mapping table
      - "approvals": use approvals table
    """
    api_key = (AT_CFG.get("API_KEY") or "").strip()
    base_id = (AT_CFG.get("BASE_ID") or "").strip()
    view = (AT_CFG.get("VIEW") or "").strip() or None
    table_id = (AT_CFG.get("APPROVALS_TABLE") if kind == "approvals" else AT_CFG.get("TABLE")) or ""
    table_id = table_id.strip()

    if not (api_key and base_id and table_id):
        return None
    return AirtableConfig(api_key=api_key, base_id=base_id, table_id_or_name=table_id, view=view)


def bearer_headers() -> dict:
    """Return default Airtable auth headers using secrets/env."""
    api_key = (AT_CFG.get("API_KEY") or "").strip()
    if not api_key:
        return {}
    return {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}

