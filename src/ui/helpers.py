from __future__ import annotations

import json
import os
from typing import List, Dict

import streamlit as st
import pandas as pd

from src.json_reorder import reorder_features_json
from src.config import EXTRA_COST_FEATURES, GA_FEATURES, IRRELEVANT_FEATURES
from src.exporter import build_updated_excel_bytes, save_updated_excel_file


_DISPLAY_KEY_MAP = {
    "plan": "Final plan",
    "addOnPlans": "Applied Add-on Plans",
    "extras": "Add-ons needed",
    "bloat_features": "Gained by plan (not currently in project)",
    "bloat_costly": "Bloat-costly",
}


def preview_with_display_names(data: Dict) -> Dict:
    """Preview JSON with merged add-ons and GA vs Final breakdown."""
    ordered = reorder_features_json(data)

    def _norm_list(val):
        if isinstance(val, list):
            return [str(x).strip() for x in val if str(x).strip()]
        return [str(x).strip() for x in (val or []) if str(x).strip()]

    plan_val = ordered.get("plan") if ordered.get("plan") is not None else data.get("plan")
    extras = _norm_list(ordered.get("extras") or data.get("extras"))
    applied_plans = _norm_list(ordered.get("addOnPlans") or data.get("addOnPlans"))

    seen = set()
    merged_extras = []
    for x in extras + applied_plans:
        key = x.lower()
        if key and key not in seen:
            seen.add(key)
            merged_extras.append(x)

    out: Dict = {}
    out[_DISPLAY_KEY_MAP["plan"]] = plan_val
    out[_DISPLAY_KEY_MAP["extras"]] = merged_extras

    # Build GA vs Final breakdown for "Gained by plan"
    ga_list = ordered.get("ga_will_appear", data.get("ga_will_appear", []))
    bloat_list = ordered.get("bloat_features", data.get("bloat_features", []))
    try:
        ga_clean = sorted([str(x).strip() for x in (ga_list or []) if str(x).strip()])
    except Exception:
        ga_clean = ga_list or []
    try:
        bloat_clean = sorted([str(x).strip() for x in (bloat_list or []) if str(x).strip()])
    except Exception:
        bloat_clean = bloat_list or []
    # Prefer plan-only gain if present; else fallback to bloat list (non-GA gains)
    plan_only = ordered.get("plan_only_gain", data.get("plan_only_gain", None))
    if plan_only is not None:
        try:
            final_clean = sorted([str(x).strip() for x in (plan_only or []) if str(x).strip()])
        except Exception:
            final_clean = plan_only or []
    else:
        final_clean = bloat_clean
    out[_DISPLAY_KEY_MAP["bloat_features"]] = {
        "Granted by GA": ga_clean,
        "Final plan": final_clean,
    }

    out[_DISPLAY_KEY_MAP["bloat_costly"]] = ordered.get("bloat_costly", data.get("bloat_costly", []))
    # Some builds map irrelevantFeatures to same display name; keep if present
    if _DISPLAY_KEY_MAP.get("irrelevantFeatures"):
        out[_DISPLAY_KEY_MAP.get("irrelevantFeatures")] = ordered.get("irrelevantFeatures", data.get("irrelevantFeatures", []))
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


def make_details_payload(
    plan_name: str,
    cls: dict,
    extras_list: List[str],
    comment: str | None = None,
    under_trial: str | None = None,
) -> dict:
    # Normalize extras list defensively (handle pandas Series or comma-separated string)
    try:
        import pandas as _pd  # optional
    except Exception:
        _pd = None
    if _pd is not None and isinstance(extras_list, getattr(_pd, 'Series', ())):
        extras_iter = [x for x in extras_list.dropna().tolist()]
    elif isinstance(extras_list, str):
        extras_iter = [x.strip() for x in extras_list.split(',')]
    else:
        extras_iter = extras_list or []

    ga_will_appear = list(cls.get('ga_will_appear', []))
    try:
        plan_norm = set(cls.get('plan_norm', set()))
        user_norm = set(cls.get('user_norm', set()))
        plan_only_gain = sorted([str(x).strip() for x in (plan_norm - user_norm) if str(x).strip()])
    except Exception:
        plan_only_gain = []

    # Provide a breakdown for Gained by plan: Granted by GA vs Final plan (plan-only gains)
    gained_breakdown = {
        'Granted by GA': sorted([str(x).strip() for x in ga_will_appear if str(x).strip()]),
        'Final plan': plan_only_gain,
    }

    payload = {
        'plan': plan_name,
        'Add-ons needed': [str(x).strip() for x in extras_iter if str(x).strip()],
        'Gained by plan (not currently in project)': gained_breakdown,
        'bloat_costly': list(cls.get('bloat_costly', [])),
    }
    if comment is not None and str(comment).strip():
        payload['Comment'] = str(comment).strip()
    if under_trial is not None and str(under_trial).strip():
        # Keep Airtable/CSV header label exactly as specified
        payload['Under trial'] = str(under_trial).strip()
    return payload


def autosave_exports(store) -> None:
    try:
        st.session_state['last_export_excel'] = build_updated_excel_bytes(st.session_state.get('data', {}), store.all())
        save_updated_excel_file("data/updated_migration.xlsx", st.session_state.get('data', {}), store.all())
        st.info("Auto-saved updated Excel to data/updated_migration.xlsx")
    except Exception as e:
        st.warning(f"Excel export error: {e}")


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
    """Persist locally immediately and defer Airtable sync to navigation time.

    This avoids blocking the UI on REST calls. A pending flag is stored in session state
    and processed when the user navigates between top-level tabs.
    """
    try:
        # Always upsert locally first
        store.upsert(account, subtype, plan, extras, approved_by, details=details)

        config = get_airtable_config()
        api_key = (config or {}).get('api_key', '').strip() if config else ''
        base_id = (config or {}).get('base_id', '').strip() if config else ''
        table_id = (config or {}).get('approvals_table', '').strip() if config else ''

        if api_key and base_id and table_id:
            # Mark sync as pending and stash config for later
            st.session_state['airtable_sync_pending'] = True
            st.session_state['airtable_sync_config'] = {'api_key': api_key, 'base_id': base_id, 'table_id': table_id}
            return True, "Saved to CSV (Airtable sync deferred; will run on next navigation)"
        else:
            if not config:
                return True, "Saved to CSV (Airtable not configured)"
            return True, f"Saved to CSV (Airtable config incomplete: api_key={bool(api_key)}, base_id={bool(base_id)}, table_id={bool(table_id)})"
    except Exception as e:
        return True, f"Saved to CSV; deferred sync setup failed: {str(e)}"

def sync_denial_to_airtable(store, account: str, subtype: str, plan: str, extras: list, denied_by: str, details: dict | None = None) -> tuple:
    """Submit a denial using the same persistence/sync flow as approval.

    Records a 'Decision' field with value 'Denied' in the saved details so downstream
    systems can distinguish it from approvals. Uses the same CSV/Airtable pathways.
    """
    try:
        details = details or {}
        d = dict(details)
        d['Decision'] = 'Denied'
        return sync_approval_to_airtable(store, account, subtype, plan, extras, denied_by, details=d)
    except Exception as e:
        return False, f"Denial save failed: {e}"
