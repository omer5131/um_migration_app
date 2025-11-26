import pandas as pd
import re
from src.config import SUBTYPE_MAPPING
from src.utils import parse_feature_list, is_subtype_supported, create_feature_map_for_subtype

SHIPOWNER_KEYS = {"shipowner", "ship owner", "operator"}

def get_pricing_sheet_for_subtype(subtype, data_dict):
    """Maps an Account SubType to the correct Pricing DataFrame."""
    if pd.isna(subtype):
        return None, "Unknown"
    
    subtype_lower = str(subtype).lower().strip()
    
    # Check if the subtype string contains any of our known keys
    for key, dataset_key in SUBTYPE_MAPPING.items():
        if key in subtype_lower:
            # Limit to shipowner subset only for initial version
            if key in SHIPOWNER_KEYS and dataset_key == "pricing_shipowners":
                return data_dict.get(dataset_key), dataset_key
            # Otherwise, not supported yet
            return None, "Unsupported"
            
    return None, "Generic"

def extract_package_features(pricing_df):
    """
    Parses a pricing sheet to extract sets of features for Core, Advanced, and Premium.
    """
    packages = {
        "Core": set(),
        "Advanced": set(),
        "Premium": set()
    }
    
    if pricing_df is None:
        return packages

    cols = pricing_df.columns
    
    # Helper to find column index containing the tier name
    def find_col(keyword):
        for c in cols:
            if isinstance(c, str) and keyword.lower() in c.lower():
                return c
        return None

    for pkg in ["Core", "Advanced", "Premium"]:
        col = find_col(pkg)
        if col:
            # Get all values, drop NaNs, convert to string, strip whitespace
            features = pricing_df[col].dropna().astype(str).apply(lambda x: x.strip())
            # Filter out pricing rows (containing $) or headers/items
            features = [f for f in features if "$" not in f and f.lower() != "item" and len(f) > 2]
            packages[pkg] = set(features)
    
    # Hierarchy Logic: Premium includes Advanced, Advanced includes Core
    packages["Advanced"] = packages["Advanced"].union(packages["Core"])
    packages["Premium"] = packages["Premium"].union(packages["Advanced"])
    
    return packages

def _extract_account_features(row):
    """Heuristically extract a list of technical feature flags from a row."""
    # Preferred column
    preferred_cols = [
        "featureNames",
        "features",
        "Feature Names",
        "Feature Flags",
        "FF",
        "Flags",
    ]
    for col in preferred_cols:
        if col in row.index and pd.notna(row[col]):
            return parse_feature_list(row[col])
    # Fallback: any column containing 'feature' or 'ff'
    for col in row.index:
        if re.search(r"(feature|ff)", str(col), flags=re.I):
            val = row[col]
            if pd.notna(val):
                feats = parse_feature_list(val)
                if feats:
                    return feats
    return []

def recommend_package(account_row, data, tech_to_market_map, rosetta_df):
    """
    Core Logic:
    1. Identify Account Vertical.
    2. Get Package Definitions.
    3. Translate Features.
    4. Calculate Gaps & Bloat.
    5. Recommend best fit.
    """
    
    # 1. Identify Vertical & Pricing
    subtype = account_row.get('Sub Type', 'Unknown')
    # Gate 1: only Shipowner-type subtypes for now
    subtype_lower = str(subtype).lower()
    if not any(k in subtype_lower for k in ["shipowner", "ship owner", "operator"]):
        return {
            "status": "Skipped",
            "reason": "Only Shipowner subtypes supported in this version",
        }

    # Gate 2: Plan <> FF must list this subtype
    if not is_subtype_supported(rosetta_df, subtype):
        return {
            "status": "Skipped",
            "reason": "Subtype not supported in Plan <> FF",
        }

    pricing_df, vertical_key = get_pricing_sheet_for_subtype(subtype, data)
    if pricing_df is None:
        return {
            "status": "Skipped",
            "reason": f"No compatible pricing for SubType: {subtype}",
            "recommendation": "N/A",
            "add_ons": [],
            "missing_from_plan": [],
            "extra_in_plan": [],
            "bloat": 0,
        }
    # Build a subtype-specific feature mapping from Plan <> FF
    tm_map = create_feature_map_for_subtype(rosetta_df, subtype)
    if not tm_map:
        return {
            "status": "Skipped",
            "reason": "No compatible plan mapping found in Plan <> FF",
            "recommendation": "N/A",
            "add_ons": [],
            "missing_from_plan": [],
            "extra_in_plan": [],
            "bloat": 0,
        }
        
    # 2. Get Package Definitions
    pkg_defs = extract_package_features(pricing_df)
    
    # 3. Translate Features (Tech Name -> Marketing Name)
    raw_features = _extract_account_features(account_row)
    current_marketing_features = set()
    unmapped_features = []
    
    for f in raw_features:
        if f in tm_map:
            current_marketing_features.add(tm_map[f])
        else:
            # Strict mode: only accept features defined in Plan <> FF mapping
            unmapped_features.append(f)

    # 4. Evaluate Tiers
    candidates = []
    
    # Only compare against features that exist in Plan <> FF mapping
    allowed_marketing = set(tm_map.values())
    
    for tier in ["Core", "Advanced", "Premium"]:
        tier_features = pkg_defs[tier] & allowed_marketing
        
        # Missing features = account has but plan tier doesn't include (needs add-ons)
        missing_from_tier = current_marketing_features - tier_features

        # Extra (bloat) = plan includes but account doesn't have enabled
        bloat_features = tier_features - current_marketing_features
        bloat_score = len(bloat_features)
        
        num_addons = len(missing_from_tier)
        
        candidates.append({
            "tier": tier,
            "add_ons": list(missing_from_tier),
            "num_addons": num_addons,
            "bloat_score": bloat_score,
            "missing_from_plan": list(missing_from_tier),
            "extra_in_plan": list(bloat_features),
        })
        
    # 5. Select Winner
    # Sort primarily by Add-on count (Lowest is better), secondarily by Bloat score (Lowest is better)
    candidates.sort(key=lambda x: (x['num_addons'], x['bloat_score']))
    
    best_candidate = candidates[0]
    
    return {
        "status": "Success",
        "recommendation": best_candidate['tier'],
        "add_ons": best_candidate['add_ons'],
        "missing_from_plan": best_candidate['missing_from_plan'],
        "extra_in_plan": best_candidate['extra_in_plan'],
        "bloat": best_candidate['bloat_score'],
        "vertical": vertical_key.replace("pricing_", "").title(),
        "unmapped": unmapped_features,
        "current_features_marketing": list(current_marketing_features),
        "raw_features": raw_features
    }
