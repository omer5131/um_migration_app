import os
import pandas as pd
import streamlit as st
from src.config import FILES, AIRTABLE
from src.airtable import AirtableConfig, load_cached_or_fetch
from src.plan_definitions import get_flat_plan_json, get_active_plan_json
import glob


def _build_plan_json(plan_df: pd.DataFrame) -> dict:
    """Transform Plan <> FF dataframe into {plan: [features,...]} dict.

    Preferred parsing (two-column long format):
    - First column: Plan name (appears once, with empty cells below)
    - Second column: Feature name
    - We ffill the plan column and group features per plan.

    Fallbacks:
    - Detect columns containing 'PLAN' and one or more 'FF'/'FEATURE' columns.
    - Or a wide matrix with one 'FF' feature column and plan columns as truthy markers.
    """
    result: dict[str, set] = {}
    if plan_df is None or plan_df.empty:
        return {}

    # Try the simple two-column approach first
    if plan_df.shape[1] >= 2:
        df2 = plan_df.iloc[:, :2].copy()
        plan_col2, feat_col2 = df2.columns[0], df2.columns[1]
        df2[plan_col2] = df2[plan_col2].ffill()
        df2 = df2[~df2[feat_col2].isna()].copy()
        df2[plan_col2] = df2[plan_col2].astype(str).str.strip()
        df2[feat_col2] = df2[feat_col2].astype(str).str.strip()
        plan_json = {}
        for plan, sub in df2.groupby(plan_col2):
            feats = [f for f in sub[feat_col2].tolist() if f and f.lower() != 'nan']
            if feats:
                plan_json[plan] = sorted(list(dict.fromkeys(feats)))
        if plan_json:
            return plan_json

    df = plan_df.copy()
    df.columns = [str(c).upper().strip() for c in df.columns]

    def _truthy(val) -> bool:
        if pd.isna(val):
            return False
        if isinstance(val, (int, float)):
            return val != 0 and not pd.isna(val)
        s = str(val).strip().lower()
        return s not in ("", "0", "nan", "false", "no")

    # Detect columns
    plan_col = next((c for c in df.columns if 'PLAN' in c), None)
    feature_cols = [c for c in df.columns if ('FF' in c or 'FEATURE' in c)]

    # Case A: Long format (PLAN + one or many FF columns containing feature names per row)
    if plan_col and feature_cols:
        for _, row in df.iterrows():
            raw_plan = row.get(plan_col)
            if pd.isna(raw_plan):
                continue
            plan_name = str(raw_plan).strip()
            if not plan_name:
                continue
            for fcol in feature_cols:
                raw_feat = row.get(fcol)
                if pd.isna(raw_feat):
                    continue
                feat_name = str(raw_feat).strip()
                if not feat_name:
                    continue
                result.setdefault(plan_name, set()).add(feat_name)
        # If we collected meaningful data, return
        if any(len(v) > 0 for v in result.values()):
            return {k: sorted(list(v)) for k, v in result.items()}

    # Case B: Wide matrix (one FF column listing features; remaining columns are plan names with truthy markers)
    if feature_cols:
        ff_col = feature_cols[0]
        possible_plans = [c for c in df.columns if c != ff_col and 'NOTE' not in c and 'COMMENT' not in c]
        # Heuristic: treat non-FF columns as plans if at least one truthy marker exists
        plan_like_cols = [c for c in possible_plans if df[c].apply(_truthy).any()]
        if plan_like_cols:
            for _, row in df.iterrows():
                raw_feat = row.get(ff_col)
                if pd.isna(raw_feat):
                    continue
                feat_name = str(raw_feat).strip()
                if not feat_name:
                    continue
                for pcol in plan_like_cols:
                    mark = row.get(pcol)
                    if _truthy(mark):
                        plan_name = str(pcol).strip()
                        if plan_name:
                            result.setdefault(plan_name, set()).add(feat_name)
            if any(len(v) > 0 for v in result.values()):
                return {k: sorted(list(v)) for k, v in result.items()}

    # Fallback: return empty mapping
    return {}


def flatten_family_plan_json(nested: dict) -> tuple[dict, list]:
    """Flatten a nested family->plan->features mapping into {plan: [features]}.

    Returns (plan_json, extras_list)
    - Ignores non-dict/list values gracefully.
    - If an 'EXTRAS' family exists, pulls a subkey 'Extras' (or any) list as extras.
    """
    flat: dict[str, set] = {}
    extras: list[str] = []
    if not isinstance(nested, dict):
        return {}, []

    for family, plans in nested.items():
        if isinstance(plans, dict):
            # Handle EXTRAS family specially
            if str(family).strip().upper() == 'EXTRAS':
                # Take the first list value found as extras
                for _name, vals in plans.items():
                    if isinstance(vals, list):
                        extras = [str(x).strip() for x in vals if str(x).strip()]
                        break
                continue

            for plan_name, features in plans.items():
                if isinstance(features, list):
                    plan = str(plan_name).strip()
                    if not plan:
                        continue
                    for f in features:
                        fs = str(f).strip()
                        if fs:
                            flat.setdefault(plan, set()).add(fs)
        elif isinstance(plans, list):
            # Top-level family maps directly to a list of features -> treat family name as plan
            plan = str(family).strip()
            for f in plans:
                fs = str(f).strip()
                if fs:
                    flat.setdefault(plan, set()).add(fs)

    return ({k: sorted(list(v)) for k, v in flat.items()}, extras)

    # Convert sets to sorted lists
    return {k: sorted(list(v)) for k, v in result.items()}

@st.cache_data
def load_all_data():
    """Load mapping/accounts with preference for Airtable; fallback to Excel.

    Load order:
    1) Airtable cache (or live Airtable if configured)
    2) Local Excel workbook found under data/ (best-effort sheet detection)
    """
    data = {}

    # Prefer Airtable cache if present
    cache_path = AIRTABLE.get("CACHE_PATH")
    used_airtable = False
    if cache_path and os.path.exists(cache_path):
        try:
            import json
            with open(cache_path, "r", encoding="utf-8") as f:
                payload = json.load(f)
            rows = payload.get("rows", [])
            data['mapping'] = pd.DataFrame(rows)
            data['_source'] = f"airtable_cache:{cache_path}"
            used_airtable = True
        except Exception as e:
            st.warning(f"Airtable cache load failed, falling back to CSV: {e}")

    # Try live fetch if allowed and no cache used
    if not used_airtable and all(AIRTABLE.get(k) for k in ("API_KEY", "BASE_ID", "TABLE", "CACHE_PATH")):
        try:
            cfg = AirtableConfig(
                api_key=AIRTABLE['API_KEY'],
                base_id=AIRTABLE['BASE_ID'],
                table_id_or_name=AIRTABLE['TABLE'],
                view=AIRTABLE.get('VIEW') or None,
            )
            df = load_cached_or_fetch(cfg, AIRTABLE['CACHE_PATH'], ttl_seconds=None)
            data['mapping'] = df
            data['_source'] = "airtable_live"
            used_airtable = True
        except Exception as e:
            st.warning(f"Airtable fetch failed, attempting Excel fallback: {e}")

    # Fallback to a local Excel workbook under data/
    if not used_airtable:
        excel_loaded = False
        try:
            # Look for an Excel that contains mapping in its name; otherwise any xlsx in data/
            candidates = sorted(
                glob.glob(os.path.join("data", "*.xlsx"))
            )
            # Prefer files that look like the Account Migration mapping
            def _score(p: str) -> int:
                name = os.path.basename(p).lower()
                score = 0
                for kw in ("account", "migration", "mapping"):
                    if kw in name:
                        score += 1
                return score
            candidates.sort(key=_score, reverse=True)

            if candidates:
                path = candidates[0]
                xls = pd.ExcelFile(path)
                sheets = xls.sheet_names
                # Preferred sheet names
                preferred = [
                    "Account Migration mapping (9)",
                    "Account<>CSM<>Project",
                ]
                sheet = next((s for s in preferred if s in sheets), None)
                if not sheet:
                    # Heuristic using existing helper
                    guessed = suggest_excel_sheet_mapping(sheets)
                    sheet = guessed.get('mapping') or (sheets[0] if sheets else None)
                if sheet:
                    df_map = pd.read_excel(path, sheet_name=sheet, engine='openpyxl')
                    data['mapping'] = df_map
                    data['_source'] = f"excel:{os.path.basename(path)}:{sheet}"
                    excel_loaded = True
        except Exception as e:
            st.warning(f"Local Excel load failed: {e}")

        if excel_loaded:
            # Accounts CSV is optional; if present, load
            try:
                data['accounts'] = pd.read_csv(FILES['accounts'])
            except FileNotFoundError:
                data['accounts'] = pd.DataFrame()
            data['plan_json'] = get_active_plan_json()
            return data
    # If reached here, no data source succeeded
    st.error("No data loaded. Configure Airtable or upload an Excel workbook in Data Sources.")
    return None


# Removed legacy CSV loaders


@st.cache_data
def load_from_airtable(refresh: bool = False, ttl_seconds: int | None = None):
    """Load mapping from Airtable (cached to disk). Plan JSON remains hard-coded.

    - refresh=True forces a fetch.
    - ttl_seconds controls staleness if not forcing refresh.
    """
    req_keys = ("API_KEY", "BASE_ID", "TABLE", "CACHE_PATH")
    if not all(AIRTABLE.get(k) for k in req_keys):
        raise ValueError("Missing Airtable config. Set AIRTABLE_API_KEY, AIRTABLE_BASE_ID, AIRTABLE_TABLE in .env")
    cfg = AirtableConfig(
        api_key=AIRTABLE['API_KEY'],
        base_id=AIRTABLE['BASE_ID'],
        table_id_or_name=AIRTABLE['TABLE'],
        view=AIRTABLE.get('VIEW') or None,
    )
    ttl = 0 if refresh else ttl_seconds
    df = load_cached_or_fetch(cfg, AIRTABLE['CACHE_PATH'], ttl_seconds=ttl)
    src = "airtable_live" if refresh or ttl == 0 else "airtable_cached"
    return {
        'mapping': df,
        'plan_json': get_active_plan_json(),
        '_source': src,
    }


@st.cache_data
def load_accounts_mapping_from_airtable(refresh: bool = False, ttl_seconds: int | None = None):
    """Load the Accounts mapping directly from the Airtable table 'Account<>CSM<>Project'.

    Uses AIRTABLE.API_KEY/BASE_ID and AIRTABLE.ACCOUNTS_TABLE. Caches to
    AIRTABLE.ACCOUNTS_CACHE_PATH.

    - refresh=True forces a fetch.
    - ttl_seconds controls staleness if not forcing refresh.
    """
    req_keys = ("API_KEY", "BASE_ID", "ACCOUNTS_TABLE", "ACCOUNTS_CACHE_PATH")
    if not all(AIRTABLE.get(k) for k in req_keys):
        # Fall back to generic loader if accounts-specific config is missing
        return load_from_airtable(refresh=refresh, ttl_seconds=ttl_seconds)

    cfg = AirtableConfig(
        api_key=AIRTABLE['API_KEY'],
        base_id=AIRTABLE['BASE_ID'],
        table_id_or_name=AIRTABLE['ACCOUNTS_TABLE'],
        view=(AIRTABLE.get('ACCOUNTS_VIEW') or None),
    )
    ttl = 0 if refresh else ttl_seconds
    df = load_cached_or_fetch(cfg, AIRTABLE['ACCOUNTS_CACHE_PATH'], ttl_seconds=ttl)
    src = "airtable_live(accounts)" if refresh or ttl == 0 else "airtable_cached(accounts)"
    return {
        'mapping': df,
        'plan_json': get_active_plan_json(),
        '_source': src,
    }


@st.cache_data
def suggest_excel_sheet_mapping(sheet_names: list[str]):
    """Heuristically suggest the mapping sheet name only."""
    lower = [s.lower() for s in sheet_names]
    def find(*keywords):
        for i, s in enumerate(lower):
            if all(k in s for k in keywords):
                return sheet_names[i]
        return None

    mapping = find('csm') or find('mapping') or find('project') or (sheet_names[0] if sheet_names else None)
    return {'mapping': mapping}


@st.cache_data
def load_from_excel(file_bytes: bytes, sheet_map: dict):
    """Load dataframes from an Excel workbook; mapping only. Plan JSON is hard-coded."""
    try:
        dfs = {}
        sheet = sheet_map.get('mapping')
        if not sheet:
            raise ValueError("Missing sheet selection for 'mapping'")
        dfs['mapping'] = pd.read_excel(io=file_bytes, sheet_name=sheet, engine='openpyxl')
        dfs['plan_json'] = get_active_plan_json()
        dfs['_source'] = f"excel_upload:{sheet}"
        return dfs
    except FileNotFoundError as e:
        st.error(f"Missing File: {e}")
        return None
