from __future__ import annotations

import streamlit as st
import pandas as pd

from src.logic import MigrationLogic
from src.agent import ReviewAgent
from src.decision_agent import DecisionAgent
from src.utils import parse_feature_list
from src.plan_definitions import get_flat_plan_json, get_active_plan_json
from src.data_loader import load_all_data
from src.ui.helpers import classify_sets, enrich_bloat_with_ga, preview_with_display_names, make_details_payload, autosave_exports, sync_approval_to_airtable


def render(store, use_ai_bulk: bool, openai_key: str, paid_bloat_penalty: int):
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
    agent = ReviewAgent(openai_key)
    decision_agent = DecisionAgent(openai_key)
    st.session_state.setdefault('ai_decisions', {})

    st.info(f"Data source: {data.get('_source', 'unknown')}")

    mapping = data['mapping']
    df = mapping.copy()
    if 'name' not in df.columns and 'SalesForce_Account_NAME' in df.columns:
        df = df.rename(columns={'SalesForce_Account_NAME': 'name'})

    csm_col = next((c for c in df.columns if 'csm' in c.lower()), None)
    subtype_col = (
        'Sub Type' if 'Sub Type' in df.columns else
        ('Subtype' if 'Subtype' in df.columns else
         next((c for c in df.columns if 'sub' in c.lower() and 'type' in c.lower()), None))
    )
    segment_col = next((c for c in df.columns if 'segment' in c.lower()), None)

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

    mask = pd.Series([True] * len(df))
    if selected_csms is not None:
        mask &= df[csm_col].isin(selected_csms)
    if selected_subtypes is not None:
        mask &= df[subtype_col].isin(selected_subtypes)
    if segment_col and selected_segments is not None:
        mask &= df[segment_col].isin(selected_segments)
    df_filtered = df[mask].reset_index(drop=True)
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
            approved = store.get(account_name)
            if approved:
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
                if use_ai_bulk and openai_key:
                    ai_dec = decision_agent.make_decision(
                        account_name=account_name,
                        subtype=row.get('Sub Type', row.get('Subtype', 'Unknown')),
                        user_features=row.get('featureNames', []),
                        logic_result=rec,
                    )
                    if isinstance(ai_dec, dict) and isinstance(ai_dec.get('parsed'), dict):
                        parsed = ai_dec['parsed']
                        ai_plan = parsed.get('plan') or rec.get('recommended_plan')
                        ai_extras = [str(x).strip() for x in parsed.get('extras', rec.get('extras', []))]
                        rec['recommended_plan'] = ai_plan
                        rec['extras'] = ai_extras
                        rec['extras_count'] = len(ai_extras)
                        st.session_state['ai_decisions'][account_name] = ai_dec

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

            res_row = {
                "Account": account_name,
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
