from __future__ import annotations

from typing import Iterable, List, Dict, Any

from src.config import GA_FEATURES
from src.utils import parse_feature_list


GA_SET = set(GA_FEATURES)


def extract_user_features(row: Dict[str, Any]) -> List[str]:
    """Extract a list of technical feature flags from a mapping row.

    Looks for common columns that hold feature lists and falls back to any
    column containing the word 'feature' or 'ff'.
    """
    preferred_cols = [
        "featureNames",
        "features",
        "Feature Names",
        "Feature Flags",
        "FF",
        "Flags",
        "featureNames_values",
    ]
    for col in preferred_cols:
        if col in row and row[col] is not None:
            feats = parse_feature_list(row[col])
            if feats:
                return feats

    # Fallback: scan any column name containing feature/ff
    for k, v in row.items():
        name = str(k).lower()
        if ("feature" in name or name == "ff") and v is not None:
            feats = parse_feature_list(v)
            if feats:
                return feats
    return []


def ga_visibility_for_row(row: Dict[str, Any]) -> Dict[str, Any]:
    """Compute GA visibility for a single account row.

    Returns a dict with keys:
    - name: account name if present
    - ga_present: sorted list of GA features present in user's current flags
    - ga_missing: sorted list of GA features not present in user's current flags
    - ga_present_count / ga_missing_count / ga_total
    """
    name = row.get("name") or row.get("SalesForce_Account_NAME") or ""
    user_feats = set(map(str, extract_user_features(row)))
    ga_present = sorted(list(GA_SET & user_feats))
    ga_missing = sorted(list(GA_SET - user_feats))
    return {
        "name": str(name).strip(),
        "ga_present": ga_present,
        "ga_missing": ga_missing,
        "ga_present_count": len(ga_present),
        "ga_missing_count": len(ga_missing),
        "ga_total": len(GA_SET),
    }


def ga_visibility_for_dataframe(df) -> List[Dict[str, Any]]:
    """Compute GA visibility for all rows in a pandas DataFrame.

    The DataFrame is expected to mirror the mapping table shape used in the app.
    """
    results: List[Dict[str, Any]] = []
    if df is None:
        return results
    for _, row in df.iterrows():
        results.append(ga_visibility_for_row(row.to_dict()))
    return results

