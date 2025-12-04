from __future__ import annotations

import streamlit as st
import pandas as pd

from src.logic import MigrationLogic
from src.agent import ReviewAgent
from src.decision_agent import DecisionAgent
from src.utils import parse_feature_list
from src.plan_definitions import get_active_plan_json, get_flat_plan_json
from src.exporter import build_updated_excel_bytes, save_updated_excel_file
from src.sheets import write_dataframe
from src.ui.helpers import (
    classify_sets as _classify_sets,
    enrich_bloat_with_ga as _enrich_bloat_with_ga,
    preview_with_display_names as _preview_with_display_names,
    make_details_payload as _make_details_payload,
    autosave_exports as _autosave_exports,
    sync_approval_to_airtable as _sync_approval_to_airtable,
)


def render(store, openai_key: str, approved_by: str, cost_bloat_weight: int = 0) -> None:
    if 'results' not in st.session_state:
        return

    res_df = st.session_state['results']
    st.subheader("Migration Overview")
    col1, col2, col3 = st.columns(3)
    col1.metric("Total Accounts", len(res_df))
    col2.metric("Zero Add-ons", len(res_df[res_df['Extras Count'] == 0]))
    col3.metric("High Bloat (>5)", len(res_df[res_df['Bloat Score'] > 5]))

    # Post-run filter: Recommended Plan
    plans = sorted([p for p in res_df['Recommended Plan'].dropna().unique()])
    selected_plans = st.multiselect("Filter by Recommended Plan", plans, default=plans)
    res_filtered = res_df[res_df['Recommended Plan'].isin(selected_plans)] if selected_plans else res_df

    st.dataframe(res_filtered.drop(columns=['Raw Rec']))

    # Export updated Excel (with approvals merged)
    st.markdown("\n**Export Updated Excel**")
    approvals_df = store.all()
    if st.button("Generate Updated Excel"):
        try:
            bytes_xlsx = build_updated_excel_bytes(st.session_state.get('data', {}), approvals_df)
            st.session_state['last_export_excel'] = bytes_xlsx
            save_updated_excel_file("data/updated_migration.xlsx", st.session_state.get('data', {}), approvals_df)
            st.success("Generated updated Excel and saved to data/updated_migration.xlsx")
        except Exception as e:
            st.error(f"Export error: {e}")
    if st.session_state.get('last_export_excel'):
        st.download_button(
            label="Download updated_migration.xlsx",
            data=st.session_state['last_export_excel'],
            file_name="updated_migration.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )

    # Optional: Sync approvals and updated mapping back to Google Sheets
    gs = st.session_state.get('gsheets')
    if gs and gs.get('enable_write'):
        if st.button("Sync to Google Sheet"):
            try:
                client = gs['client']
                key = gs['spreadsheet_key']
                write_dataframe(client, key, gs['approvals_ws'], approvals_df)
                mapping_df = st.session_state.get('data', {}).get('mapping')
                name_col = 'name' if ('name' in mapping_df.columns) else (
                    'SalesForce_Account_NAME' if 'SalesForce_Account_NAME' in mapping_df.columns else None)
                out_df = mapping_df.copy()
                if name_col:
                    field_name = "Add-ons needed" if "Add-ons needed" in approvals_df.columns else ("Extras" if "Extras" in approvals_df.columns else None)
                    cols = ["Account", "Final Plan"] + ([field_name] if field_name else [])
                    appr = approvals_df[cols].rename(
                        columns={"Account": name_col, "Final Plan": "Final Plan", (field_name or "Extras"): "Final Add-ons needed"}
                    )
                    out_df = out_df.merge(appr, on=name_col, how="left")
                write_dataframe(client, key, gs['updated_map_ws'], out_df)
                st.success("Synced approvals and updated mapping to Google Sheet.")
            except Exception as e:
                st.error(f"Google Sheets sync error: {e}")

    st.divider()
    st.subheader("🕵️ Agent & Human Review")

    selected_acc = st.selectbox("Select Account", res_filtered['Account'].unique())
    prev_acc = st.session_state.get('selected_account_prev')
    if prev_acc != selected_acc:
        st.session_state['ai_decision_open'] = False
    st.session_state['selected_account_prev'] = selected_acc
    row = res_filtered[res_filtered['Account'] == selected_acc].iloc[0]
    df_filtered_state = st.session_state.get('df_filtered')
    if df_filtered_state is None:
        df_filtered_state = st.session_state.get('data', {}).get('mapping')
    raw_rows = df_filtered_state[df_filtered_state['name'] == selected_acc] if 'name' in df_filtered_state.columns else df_filtered_state.iloc[0:0]
    raw_data = raw_rows.iloc[0] if not raw_rows.empty else {}

    current_plan = row['Recommended Plan']
    current_extras = [x.strip() for x in str(row.get('Add-ons needed', '')).split(',') if x.strip()]

    approved = store.get(selected_acc)
    locked = approved is not None
    lock_status = "Locked (Human Approved)" if locked else "Not Locked"
    st.caption(f"Status: {lock_status}")
    if locked:
        if st.button("Unlock (Remove approval)"):
            try:
                ok = store.delete(selected_acc)
                if ok:
                    st.success("Approval removed. This account is now unlocked.")
                    try:
                        st.session_state['last_export_excel'] = build_updated_excel_bytes(st.session_state.get('data', {}), store.all())
                        save_updated_excel_file("data/updated_migration.xlsx", st.session_state.get('data', {}), store.all())
                    except Exception:
                        pass
                    st.rerun()
                else:
                    st.warning("No saved approval to remove.")
            except Exception as e:
                st.error(f"Unlock failed: {e}")

    # Recreate engine with current plan mapping
    try:
        plan_json = st.session_state.get('data', {}).get('plan_json') or get_active_plan_json()
    except Exception:
        plan_json = get_flat_plan_json()
    logic_engine = MigrationLogic(None, plan_json, cost_bloat_weight=cost_bloat_weight)
    agent = ReviewAgent(openai_key)
    decision_agent = DecisionAgent(openai_key)

    agent_col, human_col = st.columns(2)
    with agent_col:
        if st.button("Ask Agent to Review Selection"):
            if not openai_key:
                st.error("Please enter an OpenAI API Key in the sidebar.")
            else:
                with st.spinner("Agent is analyzing..."):
                    review = agent.review_recommendation(
                        account_name=row['Account'],
                        subtype=row['Sub Type'],
                        user_features=raw_data.get('featureNames', []),
                        recommendation=row['Raw Rec']
                    )
                    st.success("Agent Feedback:")
                    st.write(review)

        with st.expander("AI Decision Maker", expanded=st.session_state.get('ai_decision_open', False) or (row['Account'] in st.session_state.get('ai_decisions', {}))):
            st.markdown("**AI Decision Maker**")
            if st.button("Get AI Decision"):
                if not openai_key:
                    st.error("Please enter an OpenAI API Key in the sidebar.")
                else:
                    with st.spinner("DecisionAgent is evaluating all candidates..."):
                        decision = decision_agent.make_decision(
                            account_name=row['Account'],
                            subtype=row['Sub Type'],
                            user_features=raw_data.get('featureNames', []),
                            logic_result=row['Raw Rec'],
                        )
                        st.success("AI Decision:")
                        if isinstance(decision, dict):
                            st.write(decision.get('text', ''))
                            st.session_state.setdefault('ai_decisions', {})
                            st.session_state['ai_decisions'][row['Account']] = decision
                            st.session_state['ai_decision_open'] = True
                        else:
                            st.write(decision)

            ai_decision_saved = (st.session_state.get('ai_decisions', {}) or {}).get(row['Account'])
            if ai_decision_saved:
                st.markdown("Last AI Decision:")
                st.write(ai_decision_saved.get('text', ''))

    # Human override / approval UI
    with human_col:
        st.markdown("**Human Override & Approve**")
        candidates_for_dropdown = []
        if isinstance(row['Raw Rec'], dict):
            candidates_for_dropdown = row['Raw Rec'].get('all_plans') or row['Raw Rec'].get('all_candidates', [])
        plan_options = []
        try:
            plan_options = [c.get('plan') for c in candidates_for_dropdown if c.get('plan')]
            plan_options = list(dict.fromkeys(plan_options))
        except Exception:
            plan_options = []

        ai_decisions = st.session_state.get('ai_decisions', {}) or {}
        ai_for_account = ai_decisions.get(row['Account']) if isinstance(ai_decisions, dict) else None
        ai_plan = None
        if isinstance(ai_for_account, dict):
            parsed = ai_for_account.get('parsed') if isinstance(ai_for_account.get('parsed'), dict) else {}
            ai_plan = parsed.get('plan')

        target_plan = ai_plan or current_plan
        if target_plan and target_plan not in plan_options:
            plan_options = [target_plan] + plan_options

        def _norm(s):
            return str(s or "").strip().lower()

        default_idx = 0
        if plan_options:
            for i, p in enumerate(plan_options):
                if _norm(p) == _norm(target_plan):
                    default_idx = i
                    break
            else:
                for i, p in enumerate(plan_options):
                    pn = _norm(p)
                    tn = _norm(target_plan)
                    if tn and (tn in pn or pn in tn):
                        default_idx = i
                        break

        new_plan = (
            st.selectbox("Final plan", plan_options, index=default_idx if plan_options else 0)
            if plan_options else st.text_input("Final plan", value=current_plan, disabled=False)
        )
        new_extras_str = st.text_area("Final Add-ons needed (comma-separated)", value=", ".join(current_extras), height=80)
        new_extras = [x.strip() for x in new_extras_str.split(',') if x.strip()]

        if st.button("Save & Lock (Human Approved)"):
            if not approved_by.strip():
                st.error("Please enter your name in the sidebar.")
            else:
                try:
                    plan_feats = set(logic_engine.plan_definitions.get(new_plan, set()))
                    user_feats = set(parse_feature_list(raw_data.get('featureNames', [])))
                    extras_set = set(new_extras)
                    cls = _classify_sets(plan_feats, user_feats, extras_set)
                except Exception:
                    cls = { 'ga': [], 'ga_present': [], 'ga_will_appear': [], 'irrelevant': [], 'bloat_features': [], 'bloat_costly': [] }

                details_payload = _make_details_payload(new_plan, cls, new_extras)
                success, msg = _sync_approval_to_airtable(
                    store, selected_acc, row['Sub Type'], new_plan, new_extras, approved_by.strip(), details=details_payload
                )
                if success:
                    st.success(f"Saved and locked! {msg}")
                    st.caption("Re-run logic to see locked status in table.")
                else:
                    st.warning(msg)

                _autosave_exports(store)

        st.markdown("---")
        st.markdown("**Choose from Candidates**")
        candidates = []
        if isinstance(row['Raw Rec'], dict):
            candidates = row['Raw Rec'].get('all_plans') or row['Raw Rec'].get('all_candidates', [])

        option_labels = [
            f"{c.get('plan')} (extras={c.get('extras_count', len(c.get('extras', [])))}, bloat={c.get('bloat_count', len(c.get('bloat_features', [])))}, paid_bloat={c.get('bloat_costly_count', len(c.get('bloat_costly', [])))})"
            for c in candidates
        ]

        ai_decisions = st.session_state.get('ai_decisions', {}) or {}
        ai_for_account = ai_decisions.get(row['Account']) if isinstance(ai_decisions, dict) else None
        ai_plan = None
        if isinstance(ai_for_account, dict):
            parsed = ai_for_account.get('parsed') if isinstance(ai_for_account.get('parsed'), dict) else {}
            ai_plan = parsed.get('plan')

        target_plan = ai_plan or current_plan
        norm = lambda s: str(s or "").strip().lower()
        target_norm = norm(target_plan)
        default_idx = 0
        for i, c in enumerate(candidates):
            try:
                if norm(c.get('plan')) == target_norm:
                    default_idx = i
                    break
            except Exception:
                pass
        else:
            for i, c in enumerate(candidates):
                try:
                    if target_norm and target_norm in norm(c.get('plan')):
                        default_idx = i
                        break
                except Exception:
                    pass
            else:
                for i, c in enumerate(candidates):
                    try:
                        if norm(c.get('plan')) and norm(c.get('plan')) in target_norm:
                            default_idx = i
                            break
                    except Exception:
                        pass

        selected_idx = (
            st.selectbox(
                "Candidate Options",
                list(range(len(candidates))),
                index=default_idx if candidates else 0,
                format_func=lambda i: option_labels[i] if i < len(option_labels) else "",
            )
            if candidates
            else None
        )
        if selected_idx is not None:
            cand = candidates[selected_idx]
            st.caption("Preview of selected candidate")
            try:
                plan_feats = set(logic_engine.plan_definitions.get(cand.get('plan'), set()))
                user_feats = set(parse_feature_list(raw_data.get('featureNames', [])))
                extras_set = set(cand.get('extras', []))
                cls = _classify_sets(plan_feats, user_feats, extras_set)
                enriched = dict(cand)
                enriched['bloat_features'] = _enrich_bloat_with_ga(cand.get('bloat_features', []), cls.get('ga_will_appear', []))
                st.json(_preview_with_display_names(enriched))
            except Exception:
                st.json(_preview_with_display_names(cand))
            if st.button("Approve Selected Option & Lock"):
                if not approved_by.strip():
                    st.error("Please enter your name in the sidebar.")
                else:
                    cand_extras = [str(x).strip() for x in cand.get('extras', [])]
                    details_payload = _make_details_payload(
                        cand.get('plan', current_plan), cls, cand_extras
                    )
                    success, msg = _sync_approval_to_airtable(
                        store, selected_acc, row['Sub Type'], cand.get('plan', current_plan), cand_extras, approved_by.strip(), details=details_payload
                    )
                    if success:
                        st.success(f"Selected candidate approved and locked! {msg}")
                    else:
                        st.warning(msg)
                    _autosave_exports(store)

        st.markdown("---")
        st.markdown("**Apply AI Decision**")
        ai_dec = (st.session_state.get('ai_decisions', {}) or {}).get(row['Account'])
        if ai_dec and isinstance(ai_dec, dict) and isinstance(ai_dec.get('parsed'), dict):
            parsed = ai_dec['parsed']
            plan_name = parsed.get('plan')
            plan_feats = set(logic_engine.plan_definitions.get(plan_name, set()))
            extras_list = [str(x).strip() for x in parsed.get('extras', [])]
            user_feats = set(parse_feature_list(raw_data.get('featureNames', [])))
            cls = _classify_sets(plan_feats, user_feats, set(extras_list))
            bloat_feats = cls['bloat_features']
            irr_feats = cls['irrelevant']
            st.caption(parsed.get('reasoning', ''))
            st.json(
                _preview_with_display_names(
                    {
                        'plan': plan_name,
                        'extras': sorted(list(cls['extras_norm'])),
                        'bloat_features': _enrich_bloat_with_ga(bloat_feats, cls.get('ga_will_appear', [])),
                        'bloat_costly': parsed.get('bloat_costly', []),
                        'irrelevantFeatures': irr_feats,
                    }
                )
            )
            if st.button("Approve AI Decision & Lock"):
                if not approved_by.strip():
                    st.error("Please enter your name in the sidebar.")
                else:
                    ai_extras = [str(x).strip() for x in parsed.get('extras', [])]
                    details_payload = _make_details_payload(
                        parsed.get('plan', current_plan), cls, ai_extras
                    )
                    success, msg = _sync_approval_to_airtable(
                        store, selected_acc, row['Sub Type'], parsed.get('plan', current_plan), ai_extras, approved_by.strip(), details=details_payload
                    )
                    if success:
                        st.success(f"AI decision approved and locked! {msg}")
                    else:
                        st.warning(msg)
                    _autosave_exports(store)

