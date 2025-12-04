from __future__ import annotations

import json
import os
from typing import List, Dict

import streamlit as st
import pandas as pd

from src.json_reorder import reorder_features_json
from src.config import EXTRA_COST_FEATURES, GA_FEATURES, IRRELEVANT_FEATURES
from src.exporter import build_updated_excel_bytes, save_updated_excel_file
from src.sheets import write_dataframe


_DISPLAY_KEY_MAP = {
    "plan": "Final plan",
    "extras": "Add-ons needed",
    "bloat_features": "Gained by plan (not currently in project)",
    "bloat_costly": "Bloat-costly",
}


def preview_with_display_names(data: Dict) -> Dict:
    ordered = reorder_features_json(data)
    order = [
        "plan",
        "extras",
        "bloat_features",
        "bloat_costly",
        "irrelevantFeatures",
    ]
    out: Dict = {}
    for k in order:
        display = _DISPLAY_KEY_MAP.get(k, k)
        if k == "plan":
            out[display] = ordered.get(k) if ordered.get(k) is not None else data.get(k)
        else:
            out[display] = ordered.get(k, data.get(k, []))
    return out


def enrich_bloat_with_ga(bloat_features: List[str] | set[str], ga_will_appear: List[str] | set[str]) -> List[str]:
    try:
        return sorted(list(set(bloat_features or []) | set(ga_will_appear or [])))
    except Exception:
        return list(bloat_features or [])


def classify_sets(plan_feats: set[str], user_feats: set[str], extras_set: set[str]) -> dict:
    ga_all = set(GA_FEATURES)
    ga_present = {f for f in user_feats if str(f).strip() in ga_all}
    ga_from_bundle = {f for f in (plan_feats | extras_set) if str(f).strip() in ga_all}
    ga_will_appear = ga_from_bundle - ga_present
    ga = ga_present | ga_will_appear
    irr = {f for f in (plan_feats | user_feats | extras_set) if str(f).strip() in set(IRRELEVANT_FEATURES)}
    irr -= ga
    plan_norm = set(plan_feats) - ga - irr
    user_norm = set(user_feats) - ga - irr
    extras_norm = set(extras_set) - ga - irr
    effective_bundle = plan_norm | extras_norm
    bloat_features = sorted(effective_bundle - user_norm)
    cost_set = {x.lower() for x in EXTRA_COST_FEATURES}
    bloat_costly = [b for b in bloat_features if str(b).strip().lower() in cost_set]
    return {
        'ga': sorted(ga),
        'ga_present': sorted(ga_present),
        'ga_will_appear': sorted(ga_will_appear),
        'irrelevant': sorted(irr),
        'plan_norm': plan_norm,
        'user_norm': user_norm,
        'extras_norm': extras_norm,
        'bloat_features': bloat_features,
        'bloat_costly': bloat_costly,
    }


def make_details_payload(plan_name: str, cls: dict, extras_list: List[str]) -> dict:
    return {
        'plan': plan_name,
        'Add-ons needed': [str(x).strip() for x in extras_list],
        'Gained by plan (not currently in project)': enrich_bloat_with_ga(
            cls.get('bloat_features', []), cls.get('ga_will_appear', [])
        ),
        'bloat_costly': list(cls.get('bloat_costly', [])),
    }


def autosave_exports(store) -> None:
    try:
        st.session_state['last_export_excel'] = build_updated_excel_bytes(st.session_state.get('data', {}), store.all())
        save_updated_excel_file("data/updated_migration.xlsx", st.session_state.get('data', {}), store.all())
        st.info("Auto-saved updated Excel to data/updated_migration.xlsx")
        gs = st.session_state.get('gsheets')
        if gs and gs.get('enable_write'):
            write_dataframe(gs['client'], gs['spreadsheet_key'], gs['approvals_ws'], store.all())
    except Exception as e:
        st.warning(f"Excel/Sheets export error: {e}")


def get_airtable_config():
    from src.config import AIRTABLE as AT_CFG

    manual_config = st.session_state.get('airtable_manual', {})
    api_key = manual_config.get('api_key', '').strip() or AT_CFG.get('API_KEY', '').strip()
    base_id = manual_config.get('base_id', '').strip() or AT_CFG.get('BASE_ID', '').strip()
    table_id = manual_config.get('table', '').strip() or AT_CFG.get('TABLE', '').strip()
    approvals_table = manual_config.get('approvals_table', '').strip() or AT_CFG.get('APPROVALS_TABLE', 'tblWWegam2OOTYpv3').strip()

    if api_key and base_id:
        return {
            'api_key': api_key,
            'base_id': base_id,
            'table_id': table_id,
            'approvals_table': approvals_table,
        }
    return None


def sync_approval_to_airtable(store, account: str, subtype: str, plan: str, extras: list, approved_by: str, details: dict | None = None) -> tuple:
    try:
        config = get_airtable_config()
        if config:
            api_key = config.get('api_key', '').strip()
            base_id = config.get('base_id', '').strip()
            table_id = config.get('approvals_table', '').strip()
            if not api_key or not base_id or not table_id:
                store.upsert(account, subtype, plan, extras, approved_by, details=details)
                return True, f"Saved to CSV (Airtable config incomplete: api_key={bool(api_key)}, base_id={bool(base_id)}, table_id={bool(table_id)})"
            airtable_config = {'api_key': api_key, 'base_id': base_id, 'table_id': table_id}
            return store.upsert_and_sync(account, subtype, plan, extras, approved_by, airtable_config, details=details)
        else:
            store.upsert(account, subtype, plan, extras, approved_by, details=details)
            return True, "Saved to CSV (Airtable not configured)"
    except Exception as e:
        store.upsert(account, subtype, plan, extras, approved_by, details=details)
        return True, f"Saved to CSV but sync failed: {str(e)}"

