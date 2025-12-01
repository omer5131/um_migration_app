from __future__ import annotations

"""
Plan → features mapping loader.

Single source of truth is data/plan_json.json when present.
We keep only a minimal default (Global.GA) in code, to avoid hardcoding plans.
"""

from typing import Dict, List
import os
import json

from src.config import GA_FEATURES


# Minimal defaults: keep only Global.GA in code; everything else comes from file
DEFAULT_NESTED_PLAN_JSON: Dict[str, dict | list] = {
    "Global": {"GA": GA_FEATURES}
}


def _load_nested_plan_from_file(path: str = "data/plan_json.json") -> dict | None:
    """Load plan JSON from disk and combine with minimal defaults (Global.GA only).

    - Flat file (plan -> [features]) is wrapped under a family named 'UserPlans'.
    - Nested file (family -> plan -> [features] | family -> [features]) is used as-is;
      we only ensure Global.GA exists.
    """
    try:
        if not os.path.exists(path):
            return None
        with open(path, "r", encoding="utf-8") as f:
            obj = json.load(f)
        if not isinstance(obj, dict):
            return None

        # Case A: flat plan mapping → build nested with only Global.GA and one family 'UserPlans'
        if all(isinstance(v, list) for v in obj.values()):
            nested = {
                "Global": {"GA": GA_FEATURES},
                "UserPlans": {str(p).strip(): [str(x).strip() for x in (feats or []) if str(x).strip()] for p, feats in obj.items()},
            }
            return nested

        # Case B: nested mapping → use as-is, but ensure Global.GA exists
        nested = obj.copy()
        if "Global" not in nested or not isinstance(nested["Global"], dict):
            nested["Global"] = {"GA": GA_FEATURES}
        else:
            nested["Global"]["GA"] = GA_FEATURES
        return nested
    except Exception:
        return None


# Single source of truth at runtime: prefer the file; fall back to minimal default
NESTED_PLAN_JSON: Dict[str, dict | list] = _load_nested_plan_from_file() or DEFAULT_NESTED_PLAN_JSON


def get_flat_plan_json() -> Dict[str, List[str]]:
    """Flatten the nested structure into {plan: [features,...]}.

    - Ignores the EXTRAS family for the returned mapping.
    - Deduplicates and trims feature names.
    """
    flat: Dict[str, set] = {}
    for family, plans in NESTED_PLAN_JSON.items():
        if str(family).strip().upper() == "EXTRAS":
            continue
        if isinstance(plans, dict):
            for plan_name, feats in plans.items():
                if not isinstance(feats, list):
                    continue
                plan = str(plan_name).strip()
                for f in feats:
                    fs = str(f).strip()
                    if fs:
                        flat.setdefault(plan, set()).add(fs)
        elif isinstance(plans, list):
            # Treat family name as plan when value is a list
            plan = str(family).strip()
            for f in plans:
                fs = str(f).strip()
                if fs:
                    flat.setdefault(plan, set()).add(fs)
    return {k: sorted(list(v)) for k, v in flat.items()}


def _flatten_if_nested(obj: dict) -> Dict[str, List[str]]:
    """Flatten a nested family->plan->features mapping into {plan: [features]}.

    If the input already looks flat (plan -> list), it is returned as-is.
    """
    # Detect flat mapping: every value is a list
    if isinstance(obj, dict) and all(isinstance(v, list) for v in obj.values()):
        return {str(k).strip(): [str(x).strip() for x in v if str(x).strip()] for k, v in obj.items()}

    flat: Dict[str, set] = {}
    for family, plans in (obj or {}).items():
        if str(family).strip().upper() == "EXTRAS":
            # ignore extras in plan mapping
            continue
        if isinstance(plans, dict):
            for plan_name, feats in plans.items():
                if not isinstance(feats, list):
                    continue
                plan = str(plan_name).strip()
                for f in feats:
                    fs = str(f).strip()
                    if fs:
                        flat.setdefault(plan, set()).add(fs)
        elif isinstance(plans, list):
            plan = str(family).strip()
            for f in plans:
                fs = str(f).strip()
                if fs:
                    flat.setdefault(plan, set()).add(fs)
    return {k: sorted(list(v)) for k, v in flat.items()}


def get_active_plan_json(path: str = "data/plan_json.json") -> Dict[str, List[str]]:
    """Return the plan mapping to use at runtime.

    Prefers `data/plan_json.json` if present (supports nested or flat),
    otherwise falls back to a minimal default with only Global.GA.
    """
    try:
        nested = _load_nested_plan_from_file(path)
        if isinstance(nested, dict):
            return _flatten_if_nested(nested)
    except Exception:
        pass
    return _flatten_if_nested(DEFAULT_NESTED_PLAN_JSON)

