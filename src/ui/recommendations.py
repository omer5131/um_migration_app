from __future__ import annotations

import streamlit as st
import pandas as pd

from src.logic import MigrationLogic
from src.utils import parse_feature_list
from src.plan_definitions import get_flat_plan_json, get_active_plan_json
from src.data_loader import load_all_data
from src.ui.helpers import classify_sets, enrich_bloat_with_ga, preview_with_display_names, make_details_payload, autosave_exports, sync_approval_to_airtable


def _sync_approvals_from_airtable():
    """Sync approvals from Airtable to local store before showing recommendations."""
    from src.ui.helpers import get_airtable_config
    from src.airtable import AirtableConfig, fetch_records, records_to_dataframe

    airtable_config = get_airtable_config()
    if not airtable_config:
        return None

    try:
        cfg = AirtableConfig(
            api_key=airtable_config['api_key'],
            base_id=airtable_config['base_id'],
            table_id_or_name=airtable_config['approvals_table']
        )
        records = fetch_records(cfg)
        df_approvals = records_to_dataframe(records)
        return df_approvals
    except Exception as e:
        st.warning(f"Could not fetch approvals from Airtable: {e}")
        return None


def render(store, openai_key: str, paid_bloat_penalty: int):
    # Sync approvals from Airtable when user opens recommendations tab
    if st.session_state.get('should_sync_airtable_approvals', True):
        with st.spinner("Syncing approvals from Airtable..."):
            airtable_approvals = _sync_approvals_from_airtable()
            if airtable_approvals is not None and not airtable_approvals.empty:
                st.session_state['airtable_approvals'] = airtable_approvals
                st.session_state['last_airtable_sync'] = pd.Timestamp.now()
                st.success(f"✅ Synced {len(airtable_approvals)} approvals from Airtable")
            st.session_state['should_sync_airtable_approvals'] = False

    data = st.session_state.get("data") or load_all_data()
    if not data:
        st.warning("Please load data first in 'Data Sources'.")
        st.stop()

    if 'mapping' not in data or not isinstance(data.get('mapping'), pd.DataFrame):
        try:
            loaded = load_all_data()
            if loaded and isinstance(loaded.get('mapping'), pd.DataFrame):
                if isinstance(data.get('plan_json'), dict):
                    loaded['plan_json'] = data['plan_json']
                data = loaded
                st.session_state['data'] = data
            else:
                st.warning("No mapping table loaded yet. Go to 'Data Sources' to connect Airtable or upload Excel.")
                st.stop()
        except Exception:
            st.warning("Failed to load mapping. Go to 'Data Sources'.")
            st.stop()

    try:
        data['plan_json'] = get_active_plan_json()
    except Exception:
        data['plan_json'] = data.get('plan_json') or get_flat_plan_json()

    logic_engine = MigrationLogic(None, data.get('plan_json'), cost_bloat_weight=paid_bloat_penalty)
    # AI toggle and bulk decision removed; recommendations run purely by logic_engine

    # Show Airtable sync status
    last_sync = st.session_state.get('last_airtable_sync')
    if last_sync:
        st.info(f"Data source: {data.get('_source', 'unknown')} | Last Airtable sync: {last_sync.strftime('%Y-%m-%d %H:%M:%S')}")
    else:
        st.info(f"Data source: {data.get('_source', 'unknown')}")

    mapping = data['mapping']
    df = mapping.copy()
    if 'name' not in df.columns and 'SalesForce_Account_NAME' in df.columns:
        df = df.rename(columns={'SalesForce_Account_NAME': 'name'})

    # Be defensive: DataFrame columns may be non-strings; normalize for matching
    cols_norm = {c: str(c).lower() for c in df.columns}
    csm_col = next((col for col, lc in cols_norm.items() if 'csm' in lc), None)
    subtype_col = (
        'Sub Type' if 'Sub Type' in df.columns else
        ('Subtype' if 'Subtype' in df.columns else
         next((col for col, lc in cols_norm.items() if ('sub' in lc and 'type' in lc)), None))
    )
    segment_col = next((col for col, lc in cols_norm.items() if 'segment' in lc), None)

    # Add manual sync button
    sync_col1, sync_col2 = st.columns([1, 3])
    with sync_col1:
        if st.button("🔄 Sync from Airtable"):
            st.session_state['should_sync_airtable_approvals'] = True
            st.rerun()
    with sync_col2:
        airtable_count = 0
        if 'airtable_approvals' in st.session_state:
            airtable_count = len(st.session_state['airtable_approvals'])
        st.caption(f"Airtable has {airtable_count} approved accounts")

    st.markdown("**Filters (pre-run):**")
    fcols = st.columns(3)
    with fcols[0]:
        if csm_col:
            csm_vals = sorted([x for x in df[csm_col].dropna().unique()])
            selected_csms = st.multiselect("Actual CSM", csm_vals, default=csm_vals)
        else:
            selected_csms = None
    with fcols[1]:
        if subtype_col:
            subtype_vals = sorted([x for x in df[subtype_col].dropna().unique()])
            selected_subtypes = st.multiselect("Sub Type", subtype_vals, default=subtype_vals)
        else:
            selected_subtypes = None
    with fcols[2]:
        if segment_col:
            segment_vals = sorted([x for x in df[segment_col].dropna().unique()])
            selected_segments = st.multiselect("Segment", segment_vals, default=segment_vals)
        else:
            selected_segments = None

    # Ensure index alignment to avoid unalignable boolean indexing errors
    mask = pd.Series([True] * len(df), index=df.index)
    if selected_csms is not None:
        mask &= df[csm_col].isin(selected_csms)
    if selected_subtypes is not None:
        mask &= df[subtype_col].isin(selected_subtypes)
    if segment_col and selected_segments is not None:
        mask &= df[segment_col].isin(selected_segments)
    df_filtered = df[mask].reset_index(drop=True)
    # Enforce: filter out rows with Status == 'Cancels' or null (mapping: Account<>CSM<>Project)
    try:
        status_col = next((c for c in df_filtered.columns if str(c).strip().lower() == 'status' or 'status' in str(c).lower()), None)
        if status_col:
            ser = df_filtered[status_col]
            df_filtered = df_filtered[ser.notna()]
            df_filtered = df_filtered[ser.astype(str).str.strip().str.lower() != 'cancels'].reset_index(drop=True)
    except Exception:
        # If anything goes wrong, keep current filtered set rather than failing
        pass
    st.caption(f"Filtered to {len(df_filtered)} rows from mapping tab.")

    st.info(
        f"Loaded {len(df)} accounts. Matrix contains {len(logic_engine.plan_definitions)} plan definitions."
    )

    if st.button("Run Migration Logic"):
        results = []
        progress = st.progress(0)
        total = len(df_filtered)

        for i, row in df_filtered.iterrows():
            account_name = row.get('name', str(i))

            # Check both local store and Airtable approvals
            approved = store.get(account_name)
            is_denied = False

            # Also check if account is in Airtable approvals
            airtable_approvals = st.session_state.get('airtable_approvals')
            is_in_airtable = False
            if airtable_approvals is not None and not airtable_approvals.empty:
                # Check if account exists in Airtable approvals
                if 'Account' in airtable_approvals.columns:
                    is_in_airtable = account_name in airtable_approvals['Account'].values

            try:
                is_denied = str((approved or {}).get('Decision', '')).strip().lower() == 'denied'
            except Exception:
                is_denied = False
            if approved and not is_denied:
                plan_name = approved['Final Plan']
                plan_features = set(logic_engine.plan_definitions.get(plan_name, set()))
                user_features = set(parse_feature_list(row.get('featureNames', [])))
                chosen_extras = set(approved.get('Add-ons needed', []))
                cls = classify_sets(plan_features, user_features, chosen_extras)
                bloat_features = enrich_bloat_with_ga(cls['bloat_features'], cls.get('ga_will_appear', []))
                bloat_costly = cls['bloat_costly']
                rec = {
                    'recommended_plan': plan_name,
                    'extras': sorted(list(cls['extras_norm'])),
                    'extras_count': len(cls['extras_norm']),
                    'bloat_score': len(bloat_features),
                    'bloat_features': bloat_features,
                    'bloat_costly': bloat_costly,
                    'bloat_costly_count': len(bloat_costly),
                    'irrelevantFeatures': cls['irrelevant'],
                    'status': 'Locked (Human Approved)',
                }
            else:
                rec = logic_engine.recommend(row)

                plan_name = rec.get('recommended_plan')
                plan_features = set(logic_engine.plan_definitions.get(plan_name, set()))
                user_features = set(parse_feature_list(row.get('featureNames', [])))
                extras_set = set(rec.get('extras', []))
                cls = classify_sets(plan_features, user_features, extras_set)
                bloat_features = cls['bloat_features']
                bloat_costly = cls['bloat_costly']
                rec['bloat_score'] = len(bloat_features)
                rec['bloat_features'] = enrich_bloat_with_ga(bloat_features, cls.get('ga_will_appear', []))
                rec['bloat_costly'] = bloat_costly
                rec['bloat_costly_count'] = len(bloat_costly)
                rec['irrelevantFeatures'] = cls['irrelevant']

            # Mark if account is already in Airtable
            already_mapped_status = ""
            if is_in_airtable:
                already_mapped_status = "✅ Already Mapped"

            res_row = {
                "Account": account_name,
                "Already Mapped": already_mapped_status,
                "Sub Type": row.get('Sub Type', row.get('Subtype', 'Unknown')),
                "Recommended Plan": rec['recommended_plan'],
                "Add-ons needed": ", ".join(rec['extras']),
                "Extras Count": rec.get('extras_count', 0),
                "Gained by plan (not currently in project)": ", ".join(rec.get('bloat_features', [])),
                "Costly Bloat Count": rec.get('bloat_costly_count', 0),
                "Bloat Score": rec.get('bloat_score', 0),
                "Irrelevant Features": ", ".join(rec.get('irrelevantFeatures', [])),
                "Status": rec['status'],
                "Raw Rec": rec,
            }
            results.append(res_row)
            progress.progress(min((i + 1) / total, 1.0))

        st.session_state['results'] = pd.DataFrame(results)
        st.session_state['df_filtered'] = df_filtered

    # Post-run summary metrics are displayed in the Review panel to avoid duplication.

    # The rest of the original app shows the review panel, candidate selection, and approval actions.
    # To keep this refactor scoped, we'll leave that detailed UI in app.py for now.
