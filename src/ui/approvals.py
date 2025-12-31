from __future__ import annotations

import pandas as pd
import streamlit as st

from src.ui.helpers import get_airtable_config
from src.airtable import AirtableConfig, fetch_records, records_to_dataframe
from src.config import AIRTABLE_SYNC_INTERVAL_SECONDS


def _start_background_airtable_sync(store, cfg: dict, interval_seconds: int) -> None:
    """Start a background daemon thread that periodically syncs approvals to Airtable.

    Runs without Streamlit UI calls to avoid interfering with the user.
    Guarded so it starts only once per session.
    """
    try:
        import threading
        import time

        if interval_seconds is None or interval_seconds <= 0:
            return

        # Ensure single start per session
        key_started = 'airtable_bg_sync_started'
        if getattr(st.session_state, key_started, False):
            return

        def _loop():
            # Snapshot credentials for the thread's lifetime
            api_key = cfg.get('api_key')
            base_id = cfg.get('base_id')
            approvals_table = cfg.get('approvals_table')
            while True:
                try:
                    # Best-effort sync; avoid Streamlit APIs in background thread
                    store.sync_to_airtable(api_key, base_id, approvals_table, backup=True)
                except Exception:
                    # Swallow errors to keep the loop alive
                    pass
                time.sleep(interval_seconds)

        t = threading.Thread(target=_loop, name="airtable-bg-sync", daemon=True)
        t.start()
        st.session_state[key_started] = True
    except Exception:
        # Fail silently if background thread cannot start
        pass


def render(store):
    st.subheader("Approved")

    airtable_config = get_airtable_config()
    # Fire-and-forget background sync to Airtable without blocking UI
    if airtable_config:
        _start_background_airtable_sync(store, airtable_config, AIRTABLE_SYNC_INTERVAL_SECONDS)
    # Default to local view so recent approvals (with merged add-ons) appear immediately
    show_local = st.checkbox("Show only local approvals", value=False)

    # Manual sync to Airtable to avoid waiting on deferred background sync
    if airtable_config:
        if st.button("Sync approvals to Airtable now"):
            try:
                cfg = airtable_config
                with st.spinner("Syncing to Airtable..."):
                    ok, msg, created, updated = store.sync_to_airtable(
                        cfg['api_key'], cfg['base_id'], cfg['approvals_table'], backup=True
                    )
                st.session_state.pop('approvals_df_cached', None)
                if ok:
                    st.success(f"Airtable sync complete: {created} created, {updated} updated")
                else:
                    st.warning(msg)
            except Exception as e:
                st.warning(f"Airtable sync error: {e}")

    try:
        if show_local:
            df_appr = store.all()
            data_source = "Local CSV"
        else:
            if 'approvals_df_cached' not in st.session_state:
                if airtable_config:
                    with st.spinner("Loading from Airtable..."):
                        cfg = AirtableConfig(
                            api_key=airtable_config['api_key'],
                            base_id=airtable_config['base_id'],
                            table_id_or_name=airtable_config['approvals_table']
                        )
                        records = fetch_records(cfg)
                        df_appr = records_to_dataframe(records)
                        if 'Approved At' in df_appr.columns:
                            try:
                                df_appr['Approved At'] = pd.to_datetime(df_appr['Approved At'])
                            except Exception:
                                pass
                        st.session_state['approvals_df_cached'] = df_appr
                        data_source = "Airtable"
                else:
                    df_appr = store.all()
                    data_source = "Local CSV (Airtable not configured)"
            else:
                df_appr = st.session_state['approvals_df_cached']
                data_source = "Airtable (cached)"
    except Exception as e:
        st.warning(f"Could not load from Airtable: {e}")
        df_appr = store.all()
        data_source = "Local CSV (Airtable failed)"

    col_info1, col_info2 = st.columns(2)
    with col_info1:
        st.caption(f"Data source: **{data_source}**")
    with col_info2:
        if airtable_config:
            st.caption("✅ Airtable connected")
        else:
            st.caption("⚠️ Airtable not configured")

    if df_appr is None or df_appr.empty:
        st.info("No approvals saved yet.")
        return

    # Ensure "Add-ons needed" shows merged extras + applied add-on plans for display
    try:
        def _to_listish(val):
            import json as _json
            if val is None:
                return []
            if isinstance(val, list):
                return [str(x).strip() for x in val if str(x).strip()]
            s = str(val)
            if not s or s.lower() == 'none':
                return []
            s = s.strip()
            # Try JSON array
            if (s.startswith('[') and s.endswith(']')) or (s.startswith('{') and s.endswith('}')):
                try:
                    parsed = _json.loads(s)
                    if isinstance(parsed, list):
                        return [str(x).strip() for x in parsed if str(x).strip()]
                except Exception:
                    pass
            # Fallback: comma-separated
            return [x.strip() for x in s.split(',') if x.strip()]

        if 'Applied Add-on Plans' in df_appr.columns:
            left = df_appr.get('Add-ons needed')
            right = df_appr.get('Applied Add-on Plans')
            merged_col = []
            for a, b in zip(left.fillna('') if left is not None else [], right.fillna('') if right is not None else []):
                merged = []
                seen = set()
                for item in _to_listish(a) + _to_listish(b):
                    key = item.lower()
                    if key and key not in seen:
                        seen.add(key)
                        merged.append(item)
                merged_col.append(', '.join(merged))
            if merged_col:
                df_appr['Add-ons needed'] = merged_col
    except Exception:
        pass

    q = st.text_input("Search approvals", placeholder="Filter by account, subtype, plan, etc.")
    view = df_appr.copy()
    # Use user-friendly column name for on-screen display
    if 'Comment' in view.columns and 'Approval Comment' not in view.columns:
        view = view.rename(columns={'Comment': 'Approval Comment'})

    if 'Approved At' in view.columns:
        try:
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
    st.dataframe(view, width='stretch')

    csv_bytes = view.to_csv(index=False).encode("utf-8")
    st.download_button(
        label="Download approvals CSV",
        data=csv_bytes,
        file_name="approvals.csv",
        mime="text/csv",
    )

    st.markdown("---")
    st.markdown("**Undo / Remove Approvals (Local CSV)**")
    try:
        acc_options = sorted([str(x) for x in view['Account'].dropna().unique()]) if 'Account' in view.columns else []
    except Exception:
        acc_options = []
    sel = st.multiselect("Select accounts to remove (unlock)", acc_options)
    if st.button("Delete selected from local approvals"):
        if not sel:
            st.info("No accounts selected.")
        else:
            removed = store.delete_many(sel)
            if removed > 0:
                st.success(f"Removed {removed} record(s) from local approvals.")
                st.session_state.pop('approvals_df_cached', None)
                st.rerun()
            else:
                st.warning("No matching records removed.")
