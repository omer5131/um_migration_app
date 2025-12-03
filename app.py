import streamlit as st
import os
import json
import pandas as pd
from src.data_loader import (
    load_all_data,
    load_from_csv_paths,
    load_from_excel,
    suggest_excel_sheet_mapping,
    flatten_family_plan_json,
)
from src.logic import MigrationLogic
from src.utils import parse_feature_list
from src.config import EXTRA_COST_FEATURES, EXTRA_COST_BLOAT_WEIGHT, GA_FEATURES, IRRELEVANT_FEATURES
from src.agent import ReviewAgent
from src.decision_agent import DecisionAgent
from src.exporter import build_updated_excel_bytes, save_updated_excel_file
from src.persistence import ApprovalsStore
from src.sheets import make_client, load_from_sheets, extract_key_from_url, write_dataframe
from src.config import AIRTABLE as AT_CFG
from src.airtable import AirtableConfig as ATConfig, upsert_single as at_upsert_single, upsert_dataframe as at_upsert_df
from src.plan_definitions import get_flat_plan_json, get_active_plan_json
from src.json_reorder import reorder_features_json

# Display mapping for preview JSON keys
_DISPLAY_KEY_MAP = {
    "extras": "add-ons to compatability",
    "bloat_features": "features on the house",
}


def _preview_with_display_names(data):
    """Order keys per spec, then rename display keys for preview."""
    ordered = reorder_features_json(data)
    order = [
        "plan",
        "extras",
        "bloat_features",
        "bloat_costly",
        "gaFeatures",
        "irrelevantFeatures",
    ]
    out = {}
    for k in order:
        display = _DISPLAY_KEY_MAP.get(k, k)
        out[display] = ordered.get(k, []) if k != "plan" else ordered.get(k)
    return out


def _get_airtable_config():
    """Get Airtable configuration from session state (Data Sources) or config file.

    Returns: dict with api_key, base_id, table_id, approvals_table or None if not configured
    """
    # First check if user manually configured in Data Sources
    manual_config = st.session_state.get('airtable_manual', {})
    api_key = manual_config.get('api_key') or AT_CFG.get('API_KEY')
    base_id = manual_config.get('base_id') or AT_CFG.get('BASE_ID')
    table_id = manual_config.get('table') or AT_CFG.get('TABLE')
    approvals_table = manual_config.get('approvals_table') or AT_CFG.get('APPROVALS_TABLE', 'tblWWegam2OOTYpv3')

    if api_key and base_id:
        return {
            'api_key': api_key,
            'base_id': base_id,
            'table_id': table_id,
            'approvals_table': approvals_table
        }
    return None


def _sync_approval_to_airtable(store, account: str, subtype: str, plan: str, extras: list, approved_by: str) -> tuple:
    """Helper to sync a single approval to Airtable with backup.

    Returns: (success: bool, message: str)
    """
    try:
        config = _get_airtable_config()

        if config:
            airtable_config = {
                'api_key': config['api_key'],
                'base_id': config['base_id'],
                'table_id': config['approvals_table']
            }
            return store.upsert_and_sync(account, subtype, plan, extras, approved_by, airtable_config)
        else:
            # No Airtable config, just save to CSV
            store.upsert(account, subtype, plan, extras, approved_by)
            return True, "Saved to CSV (Airtable not configured)"
    except Exception as e:
        # Fallback to CSV-only if sync fails
        store.upsert(account, subtype, plan, extras, approved_by)
        return True, f"Saved to CSV but sync failed: {str(e)}"


st.set_page_config(layout="wide", page_title="Migration AI Tool")

def main():
    st.title("Account Migration Engine 🤖")

    # Attempt auto-load from Airtable once per session
    if "auto_airtable_attempted" not in st.session_state:
        st.session_state["auto_airtable_attempted"] = True
        st.session_state["auto_airtable_ok"] = False
        st.session_state["auto_airtable_error"] = None

        # Check if Airtable is configured in .env
        if AT_CFG.get('API_KEY') and AT_CFG.get('BASE_ID') and AT_CFG.get('TABLE'):
            try:
                from src.data_loader import load_from_airtable
                # Auto-load from Airtable using .env config, preferring cache
                data = load_from_airtable(refresh=False, ttl_seconds=None)
                if data and isinstance(data, dict) and 'mapping' in data:
                    st.session_state["data"] = data
                    st.session_state["source"] = "airtable"
                    st.session_state["auto_airtable_ok"] = True
                else:
                    st.session_state["auto_airtable_error"] = "Airtable returned invalid data"
            except Exception as e:
                st.session_state["auto_airtable_error"] = str(e)
        else:
            st.session_state["auto_airtable_error"] = "Airtable credentials not configured in .env"

    # --- Sidebar Config & Tabs ---
    st.sidebar.header("Navigation")
    if st.session_state.get("auto_airtable_ok"):
        # Airtable loaded successfully - show main tabs only
        nav_options = ["Recommendations & Agent", "Plan Mapping", "Approved"]
        nav_index = 0
        st.sidebar.success("✅ Airtable connected")
        st.sidebar.caption("Data loaded from Airtable")

        # Show approvals sync status
        if AT_CFG.get('APPROVALS_TABLE'):
            st.sidebar.info(f"💾 Approvals auto-sync to: `{AT_CFG.get('APPROVALS_TABLE')}`")
    else:
        # Airtable not loaded - show Data Sources tab
        nav_options = ["Data Sources", "Recommendations & Agent", "Plan Mapping", "Approved"]
        nav_index = 0
        if st.session_state.get("auto_airtable_error"):
            st.sidebar.warning(f"⚠️ Airtable: {st.session_state['auto_airtable_error'][:50]}")

    tab = st.sidebar.radio("Go to", nav_options, index=nav_index)

    st.sidebar.header("Configuration")
    openai_key_input = st.sidebar.text_input("OpenAI API Key (for Agent)", type="password")
    # Fallback to Streamlit secrets if input is empty
    try:
        from src.config import OPENAI_API_KEY as _OA
    except Exception:
        _OA = ""
    openai_key = openai_key_input or _OA
    approved_by = st.sidebar.text_input("Your Name (for approvals)")
    use_ai_bulk = st.sidebar.checkbox("Use AI for recommendations (beta)", value=False)
    paid_bloat_penalty = st.sidebar.slider(
        "Paid bloat penalty (weight)", min_value=0, max_value=15, value=EXTRA_COST_BLOAT_WEIGHT, step=1,
        help="Higher weight penalizes plans that include paid features the user doesn't have."
    )

    store = ApprovalsStore()

    # Shared: data loader state
    if "data" not in st.session_state:
        st.session_state["data"] = None
    # Auto-load saved Plan JSON (flattened) if present so the app always uses it
    if st.session_state["data"] is None:
        try:
            if os.path.exists("data/plan_json.json"):
                with open("data/plan_json.json", "r") as f:
                    saved_plan_json = json.load(f)
                # If file is nested family -> plan -> features, flatten it
                if isinstance(saved_plan_json, dict) and any(isinstance(v, dict) for v in saved_plan_json.values()):
                    from src.data_loader import flatten_family_plan_json
                    flat, _extras = flatten_family_plan_json(saved_plan_json)
                    saved_plan_json = flat
                st.session_state["data"] = {"plan_json": saved_plan_json}
        except Exception:
            pass

    if tab == "Data Sources":
        st.subheader("Connect Data Sources")
        # Make Airtable the first and default option
        source = st.radio("Select data source", ["Airtable", "Excel Workbook", "CSV Files (default)", "Google Sheets"], index=0)

        if source == "Excel Workbook":
            upl = st.file_uploader("Upload Excel file (.xlsx)", type=["xlsx"])
            if upl is not None:
                try:
                    xls = pd.ExcelFile(upl)
                    sheet_names = xls.sheet_names
                    st.write(f"Detected sheets: {', '.join(sheet_names)}")
                    guessed = suggest_excel_sheet_mapping(sheet_names)
                    map_sheet = st.selectbox(
                        "Mapping sheet",
                        sheet_names,
                        index=sheet_names.index(guessed['mapping']) if guessed['mapping'] in sheet_names else 0,
                    )

                    if st.button("Load from Excel"):
                        sheet_map = {'mapping': map_sheet}
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
        elif source == "SQLite (local)":
            st.info("SQLite option is disabled.")
        elif source == "Google Sheets":
            st.write("Provide Google Sheets URL(s) and Service Account JSON.")
            col_a, col_b = st.columns(2)
            with col_a:
                default_url = "https://docs.google.com/spreadsheets/d/12uSZdBwdR_RrbxTW7xrx0yuf9idXVEHIgjQ2nLm-KCE/edit?gid=1389810451"
                sheet_url = st.text_input("Google Sheet URL (auto-detected)", value=default_url)
                map_ws = st.text_input("Mapping Worksheet Name", value="Account Migration mapping (9)")
                approvals_ws = st.text_input("Approvals Worksheet Name (write-back)", value="Approvals")
                updated_map_ws = st.text_input("Updated Mapping Sheet (write-back)", value="Account<>CSM<>Project (updated)")
                enable_write = st.checkbox("Enable write-back to Google Sheet", value=True,
                                           help="Service Account must have edit access to this spreadsheet.")
            with col_b:
                creds_json = st.text_area("Service Account JSON", height=220)

            if st.button("Connect & Load Sheets"):
                # Allow fallback to Streamlit secrets without exposing the content
                effective_creds = creds_json.strip()
                if not effective_creds:
                    try:
                        from src.config import GOOGLE_SERVICE_ACCOUNT_JSON as _GS
                        effective_creds = _GS.strip()
                    except Exception:
                        effective_creds = ""
                if not effective_creds:
                    st.error("Service Account JSON is required (paste it here or set GOOGLE_SERVICE_ACCOUNT_JSON in secrets).")
                else:
                    try:
                        client = make_client(effective_creds)
                        key = extract_key_from_url(sheet_url)
                        if not key:
                            st.error("Could not extract spreadsheet key from the URL.")
                        else:
                            sheets_cfg = {
                                'mapping': {"spreadsheet_key": key, "worksheet": map_ws},
                            }
                            data = load_from_sheets(client, sheets_cfg)
                            # Keep connection details for later write-back
                            st.session_state["gsheets"] = {
                                'client': client,
                                'spreadsheet_key': key,
                                'mapping_ws': map_ws,
                                'approvals_ws': approvals_ws,
                                'updated_map_ws': updated_map_ws,
                                'enable_write': enable_write,
                            }
                            # Annotate source
                            data['_source'] = f"gsheets:{map_ws}"
                            st.session_state["data"] = data
                            st.success("Sheets data loaded.")
                    except Exception as e:
                        st.error(f"Sheets error: {e}")

        else:  # Airtable
            from src.config import AIRTABLE as AT
            from src.data_loader import load_from_airtable
            from src.airtable import AirtableConfig as _ATCfg, load_cached_or_fetch as _at_load

            # Show current Airtable status
            if AT.get('API_KEY') and AT.get('BASE_ID') and AT.get('TABLE'):
                st.success("✅ Airtable is configured in .env")
                col_stat1, col_stat2 = st.columns(2)
                with col_stat1:
                    st.info(f"**Base ID:** {AT.get('BASE_ID')}")
                    st.info(f"**Mapping Table:** {AT.get('TABLE')}")
                with col_stat2:
                    st.info(f"**Approvals Table:** {AT.get('APPROVALS_TABLE', 'tblWWegam2OOTYpv3')}")
                    st.info(f"**Cache:** {AT.get('CACHE_PATH', 'data/airtable_mapping.json')}")

                if st.button("🔄 Force Refresh from Airtable Now"):
                    try:
                        with st.spinner("Refreshing from Airtable..."):
                            data = load_from_airtable(refresh=True, ttl_seconds=0)
                            if data and isinstance(data, dict) and 'mapping' in data:
                                st.session_state["data"] = data
                                st.session_state["source"] = "airtable"
                                st.session_state["auto_airtable_ok"] = True
                                st.success(f"✅ Refreshed! Loaded {len(data['mapping'])} accounts from Airtable")
                            else:
                                st.error("Failed to load data from Airtable")
                    except Exception as e:
                        st.error(f"Error refreshing: {e}")

                st.markdown("---")

            st.write("**Manual Configuration** (optional - only if you need to change settings):")

            # Pull current environment/secrets and prepare manual override state
            env_api_key = AT.get('API_KEY', '')
            env_base_id = AT.get('BASE_ID', '')
            env_table = AT.get('TABLE', '')
            env_view = AT.get('VIEW', '')
            env_cache = AT.get('CACHE_PATH', 'data/airtable_mapping.json')
            env_approvals = AT.get('APPROVALS_TABLE', 'Approvals')

            st.session_state.setdefault('airtable_manual', {
                'api_key': env_api_key,
                'base_id': env_base_id,
                'table': env_table,
                'view': env_view,
                'cache_path': env_cache,
                'approvals_table': env_approvals,
            })

            colA, colB = st.columns(2)
            with colA:
                api_key = st.text_input("AIRTABLE_API_KEY", value=(st.session_state['airtable_manual']['api_key'] or env_api_key), type="password", disabled=False)
                base_id = st.text_input("AIRTABLE_BASE_ID", value=(st.session_state['airtable_manual']['base_id'] or env_base_id), disabled=False)
                table_name = st.text_input("AIRTABLE_TABLE (name or id)", value=(st.session_state['airtable_manual']['table'] or env_table), disabled=False)
            with colB:
                view = st.text_input("AIRTABLE_VIEW (optional)", value=(st.session_state['airtable_manual']['view'] or env_view), disabled=False)
                cache_path = st.text_input("AIRTABLE_CACHE_PATH", value=(st.session_state['airtable_manual']['cache_path'] or env_cache or 'data/airtable_mapping.json'), disabled=False)
                approvals_table = st.text_input("AIRTABLE_APPROVALS_TABLE", value=(st.session_state['airtable_manual']['approvals_table'] or env_approvals or 'Approvals'), disabled=False)

            # Always update session with current form values
            st.session_state['airtable_manual'] = {
                'api_key': (api_key or '').strip(),
                'base_id': (base_id or '').strip(),
                'table': (table_name or '').strip(),
                'view': (view or '').strip(),
                'cache_path': (cache_path or 'data/airtable_mapping.json').strip(),
                'approvals_table': (approvals_table or 'Approvals').strip(),
            }

            st.caption("Review the values and click Save to write them to .env for next runs (do not commit secrets).")

            def _write_env(env_path: str, updates: dict[str, str]):
                try:
                    existing_lines = []
                    kv = {}
                    if os.path.exists(env_path):
                        with open(env_path, 'r', encoding='utf-8') as f:
                            for line in f.read().splitlines():
                                existing_lines.append(line)
                                if '=' in line and not line.strip().startswith('#'):
                                    k, v = line.split('=', 1)
                                    kv[k.strip()] = v
                    kv.update(updates)
                    seen = set()
                    out = []
                    for line in existing_lines:
                        if '=' in line and not line.strip().startswith('#'):
                            k, _ = line.split('=', 1)
                            k = k.strip()
                            if k in updates and k not in seen:
                                out.append(f"{k}={kv[k]}")
                                seen.add(k)
                            else:
                                out.append(line)
                        else:
                            out.append(line)
                    for k in updates:
                        if k not in seen:
                            out.append(f"{k}={kv[k]}")
                    with open(env_path, 'w', encoding='utf-8') as f:
                        f.write("\n".join(out) + "\n")
                    return True, None
                except Exception as e:
                    return False, str(e)

            # Single action: Save and refresh cache (in the background-like flow)
            if st.button("Save"):
                updates = {
                    'AIRTABLE_API_KEY': (st.session_state['airtable_manual']['api_key'] or env_api_key),
                    'AIRTABLE_BASE_ID': (st.session_state['airtable_manual']['base_id'] or env_base_id),
                    'AIRTABLE_TABLE': (st.session_state['airtable_manual']['table'] or env_table),
                    'AIRTABLE_VIEW': (st.session_state['airtable_manual']['view'] or env_view),
                    'AIRTABLE_CACHE_PATH': (st.session_state['airtable_manual']['cache_path'] or env_cache or 'data/airtable_mapping.json'),
                    'AIRTABLE_APPROVALS_TABLE': (st.session_state['airtable_manual']['approvals_table'] or env_approvals or 'Approvals'),
                }
                ok, err = _write_env('.env', updates)
                if ok:
                    # Build config from current form values and refresh cache file
                    try:
                        ak = updates['AIRTABLE_API_KEY']
                        bid = updates['AIRTABLE_BASE_ID']
                        tbl = updates['AIRTABLE_TABLE']
                        vw = updates['AIRTABLE_VIEW'] or None
                        cp = updates['AIRTABLE_CACHE_PATH'] or 'data/airtable_mapping.json'
                        if not (ak and bid and tbl):
                            st.warning("Saved to .env, but missing required fields to refresh cache (API_KEY/BASE_ID/TABLE).")
                        else:
                            cfg = _ATCfg(api_key=ak, base_id=bid, table_id_or_name=tbl, view=vw)
                            with st.spinner("Saving and refreshing Airtable cache..."):
                                df = _at_load(cfg, cp, ttl_seconds=0)
                                # Update app data immediately so it is used everywhere
                                st.session_state['data'] = {'mapping': df, 'plan_json': get_active_plan_json(), '_source': 'airtable_live'}
                            st.success("Saved and refreshed Airtable cache.")
                            st.caption(f"Cache path: {cp}")
                    except Exception as e:
                        st.warning(f"Saved to .env, but cache refresh failed: {e}")
                else:
                    st.error(f"Failed to write .env: {err}")

        

        if st.session_state["data"] is not None:
            d = st.session_state["data"]
            mapping_len = len(d['mapping']) if isinstance(d.get('mapping'), pd.DataFrame) else 0
            acc_part = f", accounts={len(d['accounts'])}" if isinstance(d.get('accounts'), pd.DataFrame) else ""
            plan_count = len(d.get('plan_json', {})) if isinstance(d.get('plan_json'), dict) else 0
            st.info(
                f"Loaded mapping={mapping_len}, plans={plan_count}{acc_part}."
            )
            # Data source banner
            ds = d.get('_source') or st.session_state.get('source') or 'unknown'
            st.info(f"Data source: {ds}")

            with st.expander("Plan → Features (JSON)", expanded=True):
                st.json(d.get('plan_json', {}))

            st.markdown("---")
            st.subheader("Manual Plan JSON (override)")
            st.caption("Paste a nested family → plan → features JSON to override the hard-coded mapping.")
            plan_json_text = st.text_area("Plan JSON", value="{}", height=220)
            col_m1, col_m2 = st.columns(2)
            if col_m1.button("Use This Plan JSON"):
                try:
                    nested = json.loads(plan_json_text)
                    flat, extras = flatten_family_plan_json(nested)
                    if st.session_state.get("data") is None:
                        st.session_state["data"] = {}
                    st.session_state["data"]["plan_json"] = flat
                    try:
                        os.makedirs("data", exist_ok=True)
                        with open("data/plan_json.json", "w") as f:
                            json.dump(flat, f, indent=2)
                        st.success(f"Saved manual Plan JSON to data/plan_json.json ({len(flat)} plans). Extras captured: {len(extras)} items")
                    except Exception as e:
                        st.warning(f"Loaded Plan JSON, but failed to persist to file: {e}")
                except Exception as e:
                    st.error(f"Invalid JSON: {e}")
            if col_m2.button("Clear Saved Plan JSON"):
                try:
                    if os.path.exists("data/plan_json.json"):
                        os.remove("data/plan_json.json")
                    if st.session_state.get("data"):
                        st.session_state["data"].pop("plan_json", None)
                    st.success("Cleared saved Plan JSON file.")
                except Exception as e:
                    st.error(f"Failed to clear saved Plan JSON: {e}")

        st.subheader("Approved Rows Store")
        st.dataframe(store.all())

    elif tab == "Plan Mapping":
        st.subheader("Plan Search")
        st.caption("View plans and their features. Editing is disabled.")

        # Always use the active plan mapping (file-backed) for display
        try:
            plan_map = get_active_plan_json()
        except Exception:
            plan_map = get_flat_plan_json()

        plans = sorted(plan_map.keys())
        q = st.text_input("Search plans or features", placeholder="Type to search…")

        def matches(plan: str, feats: list[str], query: str) -> bool:
            if not query:
                return True
            s = query.lower().strip()
            if s in plan.lower():
                return True
            return any(s in str(f).lower() for f in feats)

        filtered = [p for p in plans if matches(p, plan_map.get(p, []), q)]
        st.caption(f"{len(filtered)} of {len(plans)} plans match")

        left, right = st.columns([1, 2])
        with left:
            selected_plan = st.selectbox("Plans", filtered, index=0 if filtered else None)
        with right:
            if selected_plan:
                feats = plan_map.get(selected_plan, [])
                st.markdown(f"**Features in {selected_plan} ({len(feats)}):**")
                st.dataframe(pd.DataFrame({"feature": feats}), use_container_width=True)

    elif tab == "Approved":
        st.subheader("Approved Rows Store")

        # Get Airtable config (from Data Sources or .env)
        airtable_config = _get_airtable_config()

        # Control buttons
        col1, col2, col3, col4 = st.columns([1, 1, 1, 2])
        with col1:
            if st.button("🔄 Refresh from Airtable"):
                st.session_state.pop('approvals_df_cached', None)
                st.rerun()
        with col2:
            show_local = st.checkbox("Show local CSV only", value=False)
        with col3:
            if st.button("⬆️ Sync Local to Airtable"):
                if airtable_config:
                    with st.spinner("Syncing to Airtable..."):
                        success, msg, created, updated = store.sync_to_airtable(
                            airtable_config['api_key'],
                            airtable_config['base_id'],
                            airtable_config['approvals_table']
                        )
                        if success:
                            st.success(f"{msg}")
                            st.session_state.pop('approvals_df_cached', None)  # Clear cache
                        else:
                            st.error(msg)
                else:
                    st.error("Airtable not configured. Please configure in Data Sources tab.")

        # Load approvals from Airtable or local CSV
        try:
            if show_local:
                # Show only local CSV
                df_appr = store.all()
                data_source = "Local CSV"
            else:
                # Try to load from Airtable if not already cached
                if 'approvals_df_cached' not in st.session_state:
                    if airtable_config:
                        from src.airtable import AirtableConfig, fetch_records, records_to_dataframe

                        with st.spinner("Loading from Airtable..."):
                            cfg = AirtableConfig(
                                api_key=airtable_config['api_key'],
                                base_id=airtable_config['base_id'],
                                table_id_or_name=airtable_config['approvals_table']
                            )
                            records = fetch_records(cfg)
                            df_appr = records_to_dataframe(records)

                            # Convert Airtable date format back to display format
                            if 'Approved At' in df_appr.columns:
                                try:
                                    df_appr['Approved At'] = pd.to_datetime(df_appr['Approved At'])
                                except Exception:
                                    pass

                            st.session_state['approvals_df_cached'] = df_appr
                            data_source = "Airtable"
                    else:
                        # Fallback to local CSV if Airtable not configured
                        df_appr = store.all()
                        data_source = "Local CSV (Airtable not configured)"
                else:
                    df_appr = st.session_state['approvals_df_cached']
                    data_source = "Airtable (cached)"

        except Exception as e:
            st.warning(f"Could not load from Airtable: {e}")
            df_appr = store.all()
            data_source = "Local CSV (Airtable failed)"

        # Show data source and config status
        col_info1, col_info2 = st.columns(2)
        with col_info1:
            st.caption(f"Data source: **{data_source}**")
        with col_info2:
            if airtable_config:
                st.caption(f"✅ Airtable: `{airtable_config['base_id']}/{airtable_config['approvals_table']}`")
            else:
                st.caption("⚠️ Airtable not configured")

        if df_appr is None or df_appr.empty:
            st.info("No approvals saved yet.")
        else:
            # Add a search box to filter approvals
            q = st.text_input("Search approvals", placeholder="Filter by account, subtype, plan, etc.")
            view = df_appr.copy()

            # Show a human-readable timestamp
            if 'Approved At' in view.columns:
                try:
                    # Handle both Unix timestamp and ISO format
                    if pd.api.types.is_numeric_dtype(view['Approved At']):
                        view["Approved At (UTC)"] = pd.to_datetime(view["Approved At"], unit="s", utc=True)
                    else:
                        view["Approved At (UTC)"] = pd.to_datetime(view["Approved At"], utc=True)
                except Exception:
                    pass

            if q:
                s = q.strip().lower()
                mask = view.astype(str).apply(lambda col: col.str.lower().str.contains(s, na=False))
                view = view[mask.any(axis=1)]

            st.caption(f"{len(view)} approval(s)")
            st.dataframe(view, use_container_width=True)

            # Download CSV of current view
            csv_bytes = view.to_csv(index=False).encode("utf-8")
            st.download_button(
                label="Download approvals CSV",
                data=csv_bytes,
                file_name="approvals.csv",
                mime="text/csv",
            )

    elif tab == "Recommendations & Agent":
        # Load data either from session or fallback
        data = st.session_state.get("data") or load_all_data()
        if not data:
            st.warning("Please load data first in 'Data Sources'.")
            st.stop()

        # Ensure mapping exists; if session only had plan_json, try to backfill
        if 'mapping' not in data or not isinstance(data.get('mapping'), pd.DataFrame):
            try:
                loaded = load_all_data()
                if loaded and isinstance(loaded.get('mapping'), pd.DataFrame):
                    # Preserve any in-session plan_json override
                    if isinstance(data.get('plan_json'), dict):
                        loaded['plan_json'] = data['plan_json']
                    data = loaded
                    st.session_state['data'] = data
                else:
                    st.warning("No mapping table loaded yet. Go to 'Data Sources' to connect Airtable or Sheets.")
                    st.stop()
            except Exception:
                st.warning("Failed to load mapping. Go to 'Data Sources'.")
                st.stop()

        # Always refresh plan mapping from data/plan_json.json if present
        try:
            data['plan_json'] = get_active_plan_json()
        except Exception:
            data['plan_json'] = data.get('plan_json') or get_flat_plan_json()
        logic_engine = MigrationLogic(None, data.get('plan_json'), cost_bloat_weight=paid_bloat_penalty)
        agent = ReviewAgent(openai_key)
        decision_agent = DecisionAgent(openai_key)
        st.session_state.setdefault('ai_decisions', {})

        # Show data source banner
        st.info(f"Data source: {data.get('_source', 'unknown')}")

        mapping = data['mapping']
        # Always use mapping (Account<>CSM<>Project) as the source of accounts
        df = mapping.copy()
        if 'name' not in df.columns and 'SalesForce_Account_NAME' in df.columns:
            df = df.rename(columns={'SalesForce_Account_NAME': 'name'})

        # Pre-run filters: Actual CSM, Sub Type, and Segment
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

        # Quick refresh for Airtable cache from within this tab
        src_label = str(data.get('_source', ''))
        # Airtable cache refresh button removed; use Data Sources → Airtable → Save

        if st.button("Run Migration Logic"):
            results = []
            progress = st.progress(0)
            total = len(df_filtered)

            # Helpers for GA/Irrelevant classification
            def _classify_sets(plan_feats: set[str], user_feats: set[str], extras_set: set[str]) -> dict:
                ga = {f for f in (plan_feats | user_feats | extras_set) if str(f).strip() in set(GA_FEATURES)}
                irr = {f for f in (plan_feats | user_feats | extras_set) if str(f).strip() in set(IRRELEVANT_FEATURES)}
                # Precedence GA over Irrelevant
                irr -= ga
                # Sanitize for comparison
                plan_norm = plan_feats - ga - irr
                user_norm = user_feats - ga - irr
                extras_norm = extras_set - ga - irr
                effective_bundle = plan_norm | extras_norm
                bloat_features = sorted(effective_bundle - user_norm)
                cost_set = {x.lower() for x in EXTRA_COST_FEATURES}
                bloat_costly = [b for b in bloat_features if str(b).strip().lower() in cost_set]
                return {
                    'ga': sorted(ga),
                    'irrelevant': sorted(irr),
                    'plan_norm': plan_norm,
                    'user_norm': user_norm,
                    'extras_norm': extras_norm,
                    'bloat_features': bloat_features,
                    'bloat_costly': bloat_costly,
                }

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
                    cls = _classify_sets(plan_features, user_features, chosen_extras)
                    bloat_features = cls['bloat_features']
                    bloat_costly = cls['bloat_costly']
                    rec = {
                        'recommended_plan': plan_name,
                        'extras': sorted(list(cls['extras_norm'])),
                        'extras_count': len(cls['extras_norm']),
                        'bloat_score': len(bloat_features),
                        'bloat_features': bloat_features,
                        'bloat_costly': bloat_costly,
                        'bloat_costly_count': len(bloat_costly),
                        'gaFeatures': cls['ga'],
                        'irrelevantFeatures': cls['irrelevant'],
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
                    cls = _classify_sets(plan_features, user_features, extras_set)
                    bloat_features = cls['bloat_features']
                    bloat_costly = cls['bloat_costly']
                    rec['bloat_score'] = len(bloat_features)
                    rec['bloat_features'] = bloat_features
                    rec['bloat_costly'] = bloat_costly
                    rec['bloat_costly_count'] = len(bloat_costly)
                    rec['gaFeatures'] = cls['ga']
                    rec['irrelevantFeatures'] = cls['irrelevant']

                res_row = {
                    "Account": account_name,
                    "Sub Type": row.get('Sub Type', row.get('Subtype', 'Unknown')),
                    "Recommended Plan": rec['recommended_plan'],
                    "Extras (Add-ons)": ", ".join(rec['extras']),
                    "Extras Count": rec.get('extras_count', 0),
                    "GA Features": ", ".join(rec.get('gaFeatures', [])),
                    "Irrelevant Features": ", ".join(rec.get('irrelevantFeatures', [])),
                    "Bloat Features": ", ".join(rec.get('bloat_features', [])),
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

            # Optional: Sync approvals and updated mapping back to Google Sheets
            gs = st.session_state.get('gsheets')
            if gs and gs.get('enable_write'):
                if st.button("Sync to Google Sheet"):
                    try:
                        client = gs['client']
                        key = gs['spreadsheet_key']
                        # Write approvals
                        write_dataframe(client, key, gs['approvals_ws'], approvals_df)
                        # Build updated mapping with Final Plan/Final Extras merged
                        # Recreate updated mapping similar to exporter
                        mapping_df = st.session_state.get('data', {}).get('mapping')
                        name_col = 'name' if ('name' in mapping_df.columns) else (
                            'SalesForce_Account_NAME' if 'SalesForce_Account_NAME' in mapping_df.columns else None)
                        out_df = mapping_df.copy()
                        if name_col:
                            appr = approvals_df[["Account", "Final Plan", "Extras", "Approved By", "Approved At"]].rename(
                                columns={"Account": name_col, "Final Plan": "Final Plan", "Extras": "Final Extras"}
                            )
                            out_df = out_df.merge(appr, on=name_col, how="left")
                        write_dataframe(client, key, gs['updated_map_ws'], out_df)
                        st.success("Synced approvals and updated mapping to Google Sheet.")
                    except Exception as e:
                        st.error(f"Google Sheets sync error: {e}")

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
                        # Save to CSV and sync to Airtable with backup
                        success, msg = _sync_approval_to_airtable(
                            store, selected_acc, row['Sub Type'], new_plan, new_extras, approved_by.strip()
                        )
                        if success:
                            st.success(f"Saved and locked! {msg}")
                            st.caption("Re-run logic to see locked status in table.")
                        else:
                            st.warning(msg)

                        # Auto-save updated Excel snapshot
                        try:
                            st.session_state['last_export_excel'] = build_updated_excel_bytes(st.session_state.get('data', {}), store.all())
                            save_updated_excel_file("data/updated_migration.xlsx", st.session_state.get('data', {}), store.all())
                            st.info("Auto-saved updated Excel to data/updated_migration.xlsx")
                            # Optionally sync to Google Sheets
                            gs = st.session_state.get('gsheets')
                            if gs and gs.get('enable_write'):
                                write_dataframe(gs['client'], gs['spreadsheet_key'], gs['approvals_ws'], store.all())
                        except Exception as e:
                            st.warning(f"Excel/Sheets export error: {e}")

                st.markdown("---")
                st.markdown("**Choose from Candidates**")
                # Show all plan applications and their impact, not only filtered finals
                candidates = []
                if isinstance(row['Raw Rec'], dict):
                    candidates = row['Raw Rec'].get('all_plans') or row['Raw Rec'].get('all_candidates', [])
                option_labels = [
                    f"{c.get('plan')} (extras={c.get('extras_count', len(c.get('extras', [])))}, bloat={c.get('bloat_count', len(c.get('bloat_features', [])))}, paid_bloat={c.get('bloat_costly_count', len(c.get('bloat_costly', [])))})"
                    for c in candidates
                ]
                selected_idx = st.selectbox("Candidate Options", list(range(len(candidates))), format_func=lambda i: option_labels[i] if i < len(option_labels) else "") if candidates else None
                if selected_idx is not None:
                    cand = candidates[selected_idx]
                    st.caption("Preview of selected candidate")
                    st.json(_preview_with_display_names(cand))
                    if st.button("Approve Selected Option & Lock"):
                        if not approved_by.strip():
                            st.error("Please enter your name in the sidebar.")
                        else:
                            # Save to CSV and sync to Airtable with backup
                            cand_extras = [str(x).strip() for x in cand.get('extras', [])]
                            success, msg = _sync_approval_to_airtable(
                                store, selected_acc, row['Sub Type'], cand.get('plan', current_plan), cand_extras, approved_by.strip()
                            )
                            if success:
                                st.success(f"Selected candidate approved and locked! {msg}")
                            else:
                                st.warning(msg)

                            try:
                                st.session_state['last_export_excel'] = build_updated_excel_bytes(st.session_state.get('data', {}), store.all())
                                save_updated_excel_file("data/updated_migration.xlsx", st.session_state.get('data', {}), store.all())
                                st.info("Auto-saved updated Excel to data/updated_migration.xlsx")
                                gs = st.session_state.get('gsheets')
                                if gs and gs.get('enable_write'):
                                    write_dataframe(gs['client'], gs['spreadsheet_key'], gs['approvals_ws'], store.all())
                            except Exception as e:
                                st.warning(f"Excel/Sheets export error: {e}")

                st.markdown("---")
                st.markdown("**Apply AI Decision**")
                ai_dec = (st.session_state.get('ai_decisions', {}) or {}).get(row['Account'])
                if ai_dec and isinstance(ai_dec, dict) and isinstance(ai_dec.get('parsed'), dict):
                    parsed = ai_dec['parsed']
                    # Compute GA/Irrelevant/bloat with precedence
                    plan_name = parsed.get('plan')
                    plan_feats = set(logic_engine.plan_definitions.get(plan_name, set()))
                    extras_list = [str(x).strip() for x in parsed.get('extras', [])]
                    user_feats = set(parse_feature_list(raw_data.get('featureNames', [])))
                    cls = _classify_sets(plan_feats, user_feats, set(extras_list))
                    bloat_feats = cls['bloat_features']
                    ga_feats = cls['ga']
                    irr_feats = cls['irrelevant']
                    st.caption(parsed.get('reasoning', ''))
                    st.json(
                        _preview_with_display_names(
                            {
                                'plan': plan_name,
                                'extras': sorted(list(cls['extras_norm'])),
                                'bloat_features': bloat_feats,
                                'bloat_costly': parsed.get('bloat_costly', []),
                                'gaFeatures': ga_feats,
                                'irrelevantFeatures': irr_feats,
                            }
                        )
                    )
                    if st.button("Approve AI Decision & Lock"):
                        if not approved_by.strip():
                            st.error("Please enter your name in the sidebar.")
                        else:
                            # Save to CSV and sync to Airtable with backup
                            ai_extras = [str(x).strip() for x in parsed.get('extras', [])]
                            success, msg = _sync_approval_to_airtable(
                                store, selected_acc, row['Sub Type'], parsed.get('plan', current_plan), ai_extras, approved_by.strip()
                            )
                            if success:
                                st.success(f"AI decision approved and locked! {msg}")
                            else:
                                st.warning(msg)

                            try:
                                st.session_state['last_export_excel'] = build_updated_excel_bytes(st.session_state.get('data', {}), store.all())
                                save_updated_excel_file("data/updated_migration.xlsx", st.session_state.get('data', {}), store.all())
                                st.info("Auto-saved updated Excel to data/updated_migration.xlsx")
                                gs = st.session_state.get('gsheets')
                                if gs and gs.get('enable_write'):
                                    write_dataframe(gs['client'], gs['spreadsheet_key'], gs['approvals_ws'], store.all())
                            except Exception as e:
                                st.warning(f"Excel/Sheets export error: {e}")

if __name__ == "__main__":
    main()
