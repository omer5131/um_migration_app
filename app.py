import streamlit as st
import pandas as pd
from src.data_loader import (
    load_all_data,
    load_from_csv_paths,
    load_from_excel,
    suggest_excel_sheet_mapping,
    _build_plan_json,
    flatten_family_plan_json,
)
from src.logic import MigrationLogic
from src.utils import parse_feature_list
from src.config import EXTRA_COST_FEATURES
from src.agent import ReviewAgent
from src.decision_agent import DecisionAgent
from src.exporter import build_updated_excel_bytes, save_updated_excel_file
from src.persistence import ApprovalsStore
from src.sheets import make_client, load_from_sheets

st.set_page_config(layout="wide", page_title="Migration AI Tool")

def main():
    st.title("Account Migration Engine 🤖")

    # --- Sidebar Config & Tabs ---
    st.sidebar.header("Navigation")
    tab = st.sidebar.radio("Go to", ["Data Sources", "Recommendations & Agent"], index=1)

    st.sidebar.header("Configuration")
    openai_key = st.sidebar.text_input("OpenAI API Key (for Agent)", type="password")
    approved_by = st.sidebar.text_input("Your Name (for approvals)")
    use_ai_bulk = st.sidebar.checkbox("Use AI for recommendations (beta)", value=False)

    store = ApprovalsStore()

    # Shared: data loader state
    if "data" not in st.session_state:
        st.session_state["data"] = None

    if tab == "Data Sources":
        st.subheader("Connect Data Sources")
        source = st.radio("Select data source", ["Excel Workbook", "CSV Files (default)", "Google Sheets"], index=0)

        if source == "Excel Workbook":
            upl = st.file_uploader("Upload Excel file (.xlsx)", type=["xlsx"])
            if upl is not None:
                try:
                    xls = pd.ExcelFile(upl)
                    sheet_names = xls.sheet_names
                    st.write(f"Detected sheets: {', '.join(sheet_names)}")
                    guessed = suggest_excel_sheet_mapping(sheet_names)
                    col2, col3 = st.columns(2)
                    with col2:
                        map_sheet = st.selectbox("Mapping sheet", sheet_names, index=sheet_names.index(guessed['mapping']) if guessed['mapping'] in sheet_names else 0)
                    with col3:
                        plan_sheet = st.selectbox("Plan<>FF sheet", sheet_names, index=sheet_names.index(guessed['plan_matrix']) if guessed['plan_matrix'] in sheet_names else 0)

                    if st.button("Load from Excel"):
                        sheet_map = {
                            'mapping': map_sheet,
                            'plan_matrix': plan_sheet,
                        }
                        data = load_from_excel(upl.getvalue(), sheet_map)
                        if data:
                            st.session_state["data"] = data
                            st.success("Excel data loaded.")
                except Exception as e:
                    st.error(f"Excel error: {e}")

        elif source == "CSV Files (default)":
            st.write("Using default CSV filenames from repo root.")
            if st.button("Load from CSV"):
                data = load_all_data()
                if data:
                    st.session_state["data"] = data
                    st.success("CSV data loaded.")
        else:
            st.write("Provide Google Sheets details and Service Account JSON.")
            col_a, col_b = st.columns(2)
            with col_a:
                acc_key = st.text_input("Accounts Spreadsheet Key")
                acc_ws = st.text_input("Accounts Worksheet Name", value="Accounts")
                map_key = st.text_input("Mapping Spreadsheet Key")
                map_ws = st.text_input("Mapping Worksheet Name", value="Account<>CSM<>Project")
                plan_key = st.text_input("Plan<>FF Spreadsheet Key")
                plan_ws = st.text_input("Plan<>FF Worksheet Name", value="Plan <> FF")
            with col_b:
                creds_json = st.text_area("Service Account JSON", height=200)

            if st.button("Connect & Load Sheets"):
                if not creds_json.strip():
                    st.error("Service Account JSON is required.")
                else:
                    try:
                        client = make_client(creds_json)
                        sheets_cfg = {
                            'accounts': {"spreadsheet_key": acc_key, "worksheet": acc_ws},
                            'mapping': {"spreadsheet_key": map_key, "worksheet": map_ws},
                            'plan_matrix': {"spreadsheet_key": plan_key, "worksheet": plan_ws},
                        }
                        data = load_from_sheets(client, sheets_cfg)
                        st.session_state["data"] = data
                        st.success("Sheets data loaded.")
                    except Exception as e:
                        st.error(f"Sheets error: {e}")

        st.markdown("---")
        st.subheader("Manual Plan JSON (override)")
        st.caption("Paste a nested family → plan → features JSON to override the Plan <> FF mapping.")
        plan_json_text = st.text_area("Plan JSON", value="{}", height=220)
        if st.button("Use This Plan JSON"):
            import json
            try:
                nested = json.loads(plan_json_text)
                flat, extras = flatten_family_plan_json(nested)
                if st.session_state.get("data") is None:
                    st.session_state["data"] = {}
                st.session_state["data"]["plan_json"] = flat
                # keep raw for debug
                st.session_state["data"]["plan_json_raw"] = nested
                st.session_state["data"].pop("plan_matrix", None)
                st.success(f"Loaded manual Plan JSON with {len(flat)} plans. Extras: {len(extras)} items")
            except Exception as e:
                st.error(f"Invalid JSON: {e}")

        if st.session_state["data"] is not None:
            d = st.session_state["data"]
            # Ensure plan_json is present (e.g., for Google Sheets path)
            if 'plan_json' not in d and 'plan_matrix' in d:
                try:
                    d['plan_json'] = _build_plan_json(d['plan_matrix'])
                except Exception:
                    d['plan_json'] = {}

            acc_part = f", accounts={len(d['accounts'])}" if 'accounts' in d else ""
            plan_count = len(d.get('plan_json', {})) if 'plan_json' in d else (len(d['plan_matrix']) if 'plan_matrix' in d else 0)
            st.info(
                f"Loaded mapping={len(d['mapping'])}, plans={plan_count}{acc_part}."
            )

            with st.expander("Plan → Features (JSON)", expanded=True):
                st.json(d.get('plan_json', {}))
                st.caption("Plan Matrix columns and sample (for debugging)")
                try:
                    pm = d.get('plan_matrix')
                    if pm is not None:
                        st.write({i: c for i, c in enumerate(pm.columns.tolist())})
                        st.dataframe(pm.head(10))
                except Exception:
                    pass

        st.subheader("Approved Rows Store")
        st.dataframe(store.all())

    else:  # Recommendations & Agent
        # Load data either from session or CSV fallback
        data = st.session_state.get("data") or load_all_data()
        if not data:
            st.warning("Please load data first in 'Data Sources'.")
            st.stop()

        logic_engine = MigrationLogic(data.get('plan_matrix'), data.get('plan_json'))
        agent = ReviewAgent(openai_key)
        decision_agent = DecisionAgent(openai_key)
        st.session_state.setdefault('ai_decisions', {})

        mapping = data['mapping']
        # Always use mapping (Account<>CSM<>Project) as the source of accounts
        df = mapping.copy()
        if 'name' not in df.columns and 'SalesForce_Account_NAME' in df.columns:
            df = df.rename(columns={'SalesForce_Account_NAME': 'name'})

        # Pre-run filters: Actual CSM and Sub Type
        csm_col = next((c for c in df.columns if 'csm' in c.lower()), None)
        subtype_col = (
            'Sub Type' if 'Sub Type' in df.columns else
            ('Subtype' if 'Subtype' in df.columns else
             next((c for c in df.columns if 'sub' in c.lower() and 'type' in c.lower()), None))
        )

        st.markdown("**Filters (pre-run):**")
        fcols = st.columns(2)
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

        mask = pd.Series([True] * len(df))
        if selected_csms is not None:
            mask &= df[csm_col].isin(selected_csms)
        if selected_subtypes is not None:
            mask &= df[subtype_col].isin(selected_subtypes)
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
                    # Respect human lock; do not re-run logic
                    # Compute effective bloat using (plan + extras) - user_features
                    plan_name = approved['Final Plan']
                    plan_features = set(logic_engine.plan_definitions.get(plan_name, set()))
                    user_features = set(parse_feature_list(row.get('featureNames', [])))
                    chosen_extras = set(approved['Extras'])
                    effective_bundle = plan_features | chosen_extras
                    bloat_features = sorted(effective_bundle - user_features)
                    cost_set = {x.lower() for x in EXTRA_COST_FEATURES}
                    bloat_costly = [b for b in bloat_features if str(b).strip().lower() in cost_set]
                    rec = {
                        'recommended_plan': plan_name,
                        'extras': list(chosen_extras),
                        'extras_count': len(chosen_extras),
                        'bloat_score': len(bloat_features),
                        'bloat_features': bloat_features,
                        'bloat_costly': bloat_costly,
                        'bloat_costly_count': len(bloat_costly),
                        'status': 'Locked (Human Approved)',
                    }
                else:
                    rec = logic_engine.recommend(row)
                    # Optionally apply AI decision to choose the plan/extras
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

                    # Recompute bloat explicitly as (plan + extras) - user_features for clarity
                    plan_name = rec.get('recommended_plan')
                    plan_features = set(logic_engine.plan_definitions.get(plan_name, set()))
                    user_features = set(parse_feature_list(row.get('featureNames', [])))
                    extras_set = set(rec.get('extras', []))
                    effective_bundle = plan_features | extras_set
                    bloat_features = sorted(effective_bundle - user_features)
                    cost_set = {x.lower() for x in EXTRA_COST_FEATURES}
                    bloat_costly = [b for b in bloat_features if str(b).strip().lower() in cost_set]
                    rec['bloat_score'] = len(bloat_features)
                    rec['bloat_features'] = bloat_features
                    rec['bloat_costly'] = bloat_costly
                    rec['bloat_costly_count'] = len(bloat_costly)

                res_row = {
                    "Account": account_name,
                    "Sub Type": row.get('Sub Type', row.get('Subtype', 'Unknown')),
                    "Recommended Plan": rec['recommended_plan'],
                    "Extras (Add-ons)": ", ".join(rec['extras']),
                    "Extras Count": rec.get('extras_count', 0),
                    "Costly Bloat Count": rec.get('bloat_costly_count', 0),
                    "Bloat Score": rec.get('bloat_score', 0),
                    "Status": rec['status'],
                    "Raw Rec": rec,
                }
                results.append(res_row)
                progress.progress(min((i + 1) / total, 1.0))

            st.session_state['results'] = pd.DataFrame(results)
            st.session_state['df_filtered'] = df_filtered

        if 'results' in st.session_state:
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
                    # Also save to workspace for convenience
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

            st.divider()
            st.subheader("🕵️ Agent & Human Review")

            selected_acc = st.selectbox("Select Account", res_filtered['Account'].unique())
            # Reset AI decision panel open state only when account changes
            prev_acc = st.session_state.get('selected_account_prev')
            if prev_acc != selected_acc:
                st.session_state['ai_decision_open'] = False
            st.session_state['selected_account_prev'] = selected_acc
            row = res_filtered[res_filtered['Account'] == selected_acc].iloc[0]
            df_filtered_state = st.session_state.get('df_filtered', df)
            raw_rows = df_filtered_state[df_filtered_state['name'] == selected_acc] if 'name' in df_filtered_state.columns else df_filtered_state.iloc[0:0]
            raw_data = raw_rows.iloc[0] if not raw_rows.empty else {}

            # Current values (from rec or locked)
            current_plan = row['Recommended Plan']
            current_extras = [x.strip() for x in str(row['Extras (Add-ons)']).split(',') if x.strip()]

            approved = store.get(selected_acc)
            locked = approved is not None
            lock_status = "Locked (Human Approved)" if locked else "Not Locked"
            st.caption(f"Status: {lock_status}")

            # Agent review and AI decision maker
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

                    # Always show last AI decision for this account if present
                    ai_decision_saved = (st.session_state.get('ai_decisions', {}) or {}).get(row['Account'])
                    if ai_decision_saved:
                        st.markdown("Last AI Decision:")
                        st.write(ai_decision_saved.get('text', ''))

            # Human override / approval UI
            with human_col:
                st.markdown("**Human Override & Approve**")
                new_plan = st.text_input("Final Plan", value=current_plan, disabled=False)
                new_extras_str = st.text_area("Final Extras (comma-separated)", value=", ".join(current_extras), height=80)
                new_extras = [x.strip() for x in new_extras_str.split(',') if x.strip()]

                if st.button("Save & Lock (Human Approved)"):
                    if not approved_by.strip():
                        st.error("Please enter your name in the sidebar.")
                    else:
                        store.upsert(
                            account=selected_acc,
                            subtype=row['Sub Type'],
                            final_plan=new_plan,
                            extras=new_extras,
                            approved_by=approved_by.strip(),
                        )
                        st.success("Saved and locked. Re-run logic to see locked status in table.")
                        # Auto-save updated Excel snapshot
                        try:
                            st.session_state['last_export_excel'] = build_updated_excel_bytes(st.session_state.get('data', {}), store.all())
                            save_updated_excel_file("data/updated_migration.xlsx", st.session_state.get('data', {}), store.all())
                            st.info("Auto-saved updated Excel to data/updated_migration.xlsx")
                        except Exception:
                            pass

                st.markdown("---")
                st.markdown("**Choose from Candidates**")
                candidates = row['Raw Rec'].get('all_candidates', []) if isinstance(row['Raw Rec'], dict) else []
                option_labels = [
                    f"{c.get('plan')} (bloat={c.get('bloat_count', len(c.get('bloat_features', [])))}, extras={c.get('extras_count', len(c.get('extras', [])))})"
                    for c in candidates
                ]
                selected_idx = st.selectbox("Candidate Options", list(range(len(candidates))), format_func=lambda i: option_labels[i] if i < len(option_labels) else "") if candidates else None
                if selected_idx is not None:
                    cand = candidates[selected_idx]
                    st.caption("Preview of selected candidate")
                    st.json({
                        'plan': cand.get('plan'),
                        'extras': cand.get('extras', []),
                        'bloat_features': cand.get('bloat_features', []),
                        'bloat_costly': cand.get('bloat_costly', []),
                    })
                    if st.button("Approve Selected Option & Lock"):
                        if not approved_by.strip():
                            st.error("Please enter your name in the sidebar.")
                        else:
                            store.upsert(
                                account=selected_acc,
                                subtype=row['Sub Type'],
                                final_plan=cand.get('plan', current_plan),
                                extras=[str(x).strip() for x in cand.get('extras', [])],
                                approved_by=approved_by.strip(),
                            )
                            st.success("Selected candidate approved and locked.")
                            try:
                                st.session_state['last_export_excel'] = build_updated_excel_bytes(st.session_state.get('data', {}), store.all())
                                save_updated_excel_file("data/updated_migration.xlsx", st.session_state.get('data', {}), store.all())
                                st.info("Auto-saved updated Excel to data/updated_migration.xlsx")
                            except Exception:
                                pass

                st.markdown("---")
                st.markdown("**Apply AI Decision**")
                ai_dec = (st.session_state.get('ai_decisions', {}) or {}).get(row['Account'])
                if ai_dec and isinstance(ai_dec, dict) and isinstance(ai_dec.get('parsed'), dict):
                    parsed = ai_dec['parsed']
                    # Compute bloat as (plan + extras) - user_features
                    plan_name = parsed.get('plan')
                    plan_feats = set(logic_engine.plan_definitions.get(plan_name, set()))
                    extras_list = [str(x).strip() for x in parsed.get('extras', [])]
                    user_feats = set(parse_feature_list(raw_data.get('featureNames', [])))
                    effective = plan_feats | set(extras_list)
                    bloat_feats = sorted(effective - user_feats)
                    st.caption(parsed.get('reasoning', ''))
                    st.json({
                        'plan': plan_name,
                        'extras': extras_list,
                        'covered': parsed.get('covered', []),
                        'bloat_features': bloat_feats,
                        'bloat_score': len(bloat_feats),
                    })
                    if st.button("Approve AI Decision & Lock"):
                        if not approved_by.strip():
                            st.error("Please enter your name in the sidebar.")
                        else:
                            store.upsert(
                                account=selected_acc,
                                subtype=row['Sub Type'],
                                final_plan=parsed.get('plan', current_plan),
                                extras=[str(x).strip() for x in parsed.get('extras', [])],
                                approved_by=approved_by.strip(),
                            )
                            st.success("AI decision approved and locked.")
                            try:
                                st.session_state['last_export_excel'] = build_updated_excel_bytes(st.session_state.get('data', {}), store.all())
                                save_updated_excel_file("data/updated_migration.xlsx", st.session_state.get('data', {}), store.all())
                                st.info("Auto-saved updated Excel to data/updated_migration.xlsx")
                            except Exception:
                                pass

if __name__ == "__main__":
    main()
