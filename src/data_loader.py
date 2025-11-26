import pandas as pd
import streamlit as st
from src.config import FILES


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
    data = {}
    missing = []
    
    # Load specific CSVs
    try:
        data['accounts'] = pd.read_csv(FILES['accounts'])
        data['mapping'] = pd.read_csv(FILES['account_csm_project'])
        # Load the critical Plan <> FF file
        # It has a weird header structure based on snippets, usually Row 0 is header
        data['plan_matrix'] = pd.read_csv(FILES['plan_features'])
        # Build structured JSON-like plan dictionary
        data['plan_json'] = _build_plan_json(data['plan_matrix'])
    except FileNotFoundError as e:
        st.error(f"Missing File: {e}")
        return None
        
    return data


@st.cache_data
def load_from_csv_paths(accounts_path: str, mapping_path: str, plan_matrix_path: str):
    try:
        accounts_df = pd.read_csv(accounts_path)
        mapping_df = pd.read_csv(mapping_path)
        plan_df = pd.read_csv(plan_matrix_path)
        return {
            'accounts': accounts_df,
            'mapping': mapping_df,
            'plan_matrix': plan_df,
            'plan_json': _build_plan_json(plan_df),
        }
    except FileNotFoundError as e:
        st.error(f"Missing File: {e}")
        return None


@st.cache_data
def suggest_excel_sheet_mapping(sheet_names: list[str]):
    """Heuristically suggest sheet names for accounts, mapping, plan_matrix."""
    lower = [s.lower() for s in sheet_names]
    def find(*keywords):
        for i, s in enumerate(lower):
            if all(k in s for k in keywords):
                return sheet_names[i]
        return None

    mapping = find('csm') or find('mapping') or find('project') or (sheet_names[0] if sheet_names else None)
    plan = find('plan') or find('ff') or find('feature') or (sheet_names[1] if len(sheet_names) > 1 else mapping)
    return {
        'mapping': mapping,
        'plan_matrix': plan,
    }


@st.cache_data
def load_from_excel(file_bytes: bytes, sheet_map: dict):
    """Load dataframes from an Excel workbook using provided sheet mapping.

    sheet_map keys expected here: 'mapping', 'plan_matrix'
    """
    try:
        dfs = {}
        for key, sheet in sheet_map.items():
            if not sheet:
                raise ValueError(f"Missing sheet selection for '{key}'")
            dfs[key] = pd.read_excel(io=file_bytes, sheet_name=sheet, engine='openpyxl')
        # Also compute structured plan JSON
        if 'plan_matrix' in dfs:
            dfs['plan_json'] = _build_plan_json(dfs['plan_matrix'])
        return dfs
    except FileNotFoundError as e:
        st.error(f"Missing File: {e}")
        return None
