from __future__ import annotations

import streamlit as st
import pandas as pd

from src.data_loader import load_from_excel, suggest_excel_sheet_mapping
from src.airtable import AirtableConfig, load_cached_or_fetch
from src.plan_definitions import get_active_plan_json
from src.ui.helpers import get_airtable_config
from src.config import AIRTABLE as AT_CFG


def render():
    st.subheader("Connect Data Sources")
    source = st.radio("Select data source", ["Airtable", "Excel Workbook"], index=0)

    if source == "Airtable":
        # Show status and only prompt for key if not provided via secrets/env
        env_api_key = AT_CFG.get('API_KEY', '')
        env_base_id = AT_CFG.get('BASE_ID', '')
        env_table = AT_CFG.get('TABLE', '')
        env_view = AT_CFG.get('VIEW', '')
        env_cache = AT_CFG.get('CACHE_PATH', 'data/airtable_mapping.json')

        st.session_state.setdefault('airtable_manual', {'api_key': ''})
        api_key = env_api_key.strip()

        if not api_key:
            # Only ask if not configured via secrets/env
            manual_val = st.session_state['airtable_manual'].get('api_key', '')
            api_key_input = st.text_input("Airtable API Key", value=manual_val, type="password")
            st.session_state['airtable_manual']['api_key'] = (api_key_input or '').strip()
            api_key = (api_key_input or '').strip()
            st.caption("Base and table are preconfigured; enter API key if not in secrets/env.")
        else:
            st.info("Using Airtable API key from secrets/env.")

        col_a, col_b = st.columns(2)
        with col_a:
            if not env_api_key and st.button("Save API Key"):
                st.success("Saved in session. You can now load from Airtable.")
        with col_b:
            if st.button("Load from Airtable"):
                if not api_key:
                    st.error("Please configure an Airtable API Key.")
                elif not env_base_id or not env_table:
                    st.error("Airtable Base/Table not configured. Set in secrets/env.")
                else:
                    try:
                        with st.spinner("Fetching from Airtable..."):
                            cfg = AirtableConfig(api_key=api_key, base_id=env_base_id, table_id_or_name=env_table, view=env_view or None)
                            df = load_cached_or_fetch(cfg, env_cache, ttl_seconds=0)
                            data = {'mapping': df, 'plan_json': get_active_plan_json(), '_source': 'airtable_live'}
                            st.session_state['data'] = data
                            st.success(f"Loaded {len(df)} records from Airtable.")
                    except Exception as e:
                        st.error(f"Airtable error: {e}")

        # Sidebar indicator
        airtable_cfg = get_airtable_config()
        if airtable_cfg:
            st.sidebar.success("✅ Airtable sync enabled")
        else:
            st.sidebar.warning("⚠️ Airtable sync disabled")
            st.sidebar.caption("Provide API key in Data Sources")

    elif source == "Excel Workbook":
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
