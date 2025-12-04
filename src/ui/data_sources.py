from __future__ import annotations

import json
import streamlit as st
import pandas as pd

from src.data_loader import load_all_data, load_from_excel, suggest_excel_sheet_mapping
from src.sheets import make_client, extract_key_from_url
from src.ui.helpers import get_airtable_config


def render():
    st.subheader("Connect Data Sources")
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

    elif source == "Google Sheets":
        st.write("Provide Google Sheets URL(s) and Service Account JSON.")
        col_a, col_b = st.columns(2)
        with col_a:
            default_url = "https://docs.google.com/spreadsheets/d/12uSZdBwdR_RrbxTW7xrx0yuf9idXVEHIgjQ2nLm-KCE/edit?gid=1389810451"
            sheet_url = st.text_input("Google Sheet URL (auto-detected)", value=default_url)
            map_ws = st.text_input("Mapping Worksheet Name", value="Account Migration mapping (9)")
            approvals_ws = st.text_input("Approvals Worksheet Name (write-back)", value="Approvals")
            updated_map_ws = st.text_input("Updated Mapping Sheet (write-back)", value="Account<>CSM<>Project (updated)")
            enable_write = st.checkbox("Enable write-back to Google Sheet", value=True, help="Service Account must have edit access to this spreadsheet.")
        with col_b:
            creds_json = st.text_area("Service Account JSON", height=220)

        if st.button("Connect & Load Sheets"):
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
                        st.session_state['gsheets'] = {
                            'client': client,
                            'spreadsheet_key': key,
                            'approvals_ws': approvals_ws,
                            'updated_map_ws': updated_map_ws,
                            'enable_write': enable_write,
                        }
                        st.success("Connected to Google Sheets. You can now use write-back features.")
                except Exception as e:
                    st.error(f"Google Sheets error: {e}")

    # Sidebar Airtable status indicator
    airtable_cfg = get_airtable_config()
    if airtable_cfg:
        st.sidebar.success("✅ Airtable sync enabled")
        if st.sidebar.button("🔄 Reset Airtable Config", help="Clear cached config and reload from .env"):
            st.session_state.pop("airtable_manual", None)
            st.session_state.pop("airtable_initialized", None)
            st.rerun()
    else:
        st.sidebar.warning("⚠️ Airtable sync disabled")
        st.sidebar.caption("Configure in Data Sources tab")

