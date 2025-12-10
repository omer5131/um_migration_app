from __future__ import annotations

from typing import Dict, Iterable, List, Set

import pandas as pd

from src.config import (
    SUBTYPE_KEYWORD_MAP,
    EXTRA_COST_FEATURES,
    EXTRA_COST_BLOAT_WEIGHT,
    GA_FEATURES,
    IRRELEVANT_FEATURES,
)
from src.utils import parse_feature_list, clean_feature_name
from src.plan_definitions import get_add_on_plans

# Default soft-matching dictionary (display name -> canonical name)
DEFAULT_SYNONYMS: Dict[str, str] = {
    "Advanced Search": "advancedSearchOwners",
    "Port Control": "portStateControl",
    "Weather Map": "weatherLayer",
}


def canonicalize(name: str, synonyms: Dict[str, str] | None = None) -> str:
    s = clean_feature_name(name)
    if not s:
        return s
    syn = synonyms or {}
    for k, v in syn.items():
        if s.lower() == str(k).strip().lower():
            return str(v).strip()
    return s


def compute_bloat_stats(
    plan_definitions: Dict[str, Set[str]],
    plan_name: str,
    extras: Iterable[str],
    user_features: Iterable[str],
) -> dict:
    """
    Compute bloat metrics as (plan_features ∪ extras) − user_features and identify costly bloat.
    Returns a dict with bloat_features, bloat_costly, bloat_score, bloat_costly_count.
    """
    plan_features = set(plan_definitions.get(plan_name, set()))
    user_set = {clean_feature_name(f) for f in user_features or []}
    extras_set = {clean_feature_name(e) for e in extras or []}
    effective_bundle = plan_features | extras_set
    bloat_features = sorted(effective_bundle - user_set)
    cost_set = {x.lower() for x in EXTRA_COST_FEATURES}
    bloat_costly = [b for b in bloat_features if str(b).strip().lower() in cost_set]
    return {
        "bloat_features": bloat_features,
        "bloat_costly": bloat_costly,
        "bloat_score": len(bloat_features),
        "bloat_costly_count": len(bloat_costly),
    }


class MigrationLogic:
    def __init__(
        self,
        plan_matrix_df: pd.DataFrame | None = None,
        plan_json: dict | None = None,
        cost_bloat_weight: int | None = None,
        synonyms: Dict[str, str] | None = None,
    ):
        self.synonyms = synonyms or DEFAULT_SYNONYMS
        if isinstance(plan_json, dict) and plan_json:
            self.plan_definitions = {k: {canonicalize(x, self.synonyms) for x in (v or [])} for k, v in plan_json.items()}
        else:
            self.plan_definitions = self._build_plan_definitions(plan_matrix_df)
        self.cost_bloat_weight = (
            cost_bloat_weight if cost_bloat_weight is not None else EXTRA_COST_BLOAT_WEIGHT
        )
        # Classification sets (canonicalized)
        self.ga_set: Set[str] = {canonicalize(f, self.synonyms) for f in GA_FEATURES}
        self.irrelevant_set: Set[str] = {canonicalize(f, self.synonyms) for f in IRRELEVANT_FEATURES}
        # Add-on plans that can be aggregated on top of any base plan
        raw_addons = get_add_on_plans()
        self.add_on_plans: Dict[str, Set[str]] = {
            str(name): {canonicalize(feat, self.synonyms) for feat in (feats or [])}
            for name, feats in (raw_addons or {}).items()
        }
        # Build a case-insensitive alias map to known canonical feature names
        self._feature_alias: Dict[str, str] = {}
        def _add_alias(src_set: Iterable[str]):
            for feat in src_set:
                key = str(feat).strip().lower()
                if key and key not in self._feature_alias:
                    self._feature_alias[key] = str(feat)
        for feats in self.plan_definitions.values():
            _add_alias(feats)
        for feats in self.add_on_plans.values():
            _add_alias(feats)
        _add_alias(self.ga_set)
        _add_alias(self.irrelevant_set)

    def _canon_feature(self, name: str) -> str:
        s = clean_feature_name(name)
        if not s:
            return s
        # First, apply synonym mapping by case-insensitive key match
        for k, v in (self.synonyms or {}).items():
            if s.lower() == str(k).strip().lower():
                return str(v).strip()
        # Then, snap to a known canonical feature by case-insensitive match
        return self._feature_alias.get(s.lower(), s)

    def _classify(self, features: Iterable[str]) -> dict:
        """Classify into GA / Irrelevant / Normal. Precedence: GA -> Irrelevant -> Normal."""
        ga, irr, normal = set(), set(), set()
        for f in (features or []):
            cf = canonicalize(f, self.synonyms)
            if cf in self.ga_set:
                ga.add(cf)
            elif cf in self.irrelevant_set:
                irr.add(cf)
            else:
                normal.add(cf)
        return {"ga": ga, "irrelevant": irr, "normal": normal}

    def _build_plan_definitions(self, df: pd.DataFrame | None):
        definitions: Dict[str, Set[str]] = {}
        if df is None or df.empty:
            return definitions

        df = df.copy()
        df.columns = [str(c).upper().strip() for c in df.columns]

        plan_col = next((c for c in df.columns if "PLAN" in c), None)
        ff_col = next((c for c in df.columns if "FF" in c or "FEATURE" in c), None)
        if not plan_col or not ff_col:
            return definitions

        for _, row in df.iterrows():
            plan_name = str(row.get(plan_col, "")).strip()
            feature = str(row.get(ff_col, "")).strip()
            if not plan_name or not feature:
                continue
            if plan_name.lower() == "nan" or feature.lower() == "nan":
                continue

            if plan_name not in definitions:
                definitions[plan_name] = set()
            definitions[plan_name].add(canonicalize(feature, self.synonyms))

        return definitions

    def get_relevant_plans(self, subtype: str) -> List[str]:
        if pd.isna(subtype):
            return []
        s = str(subtype).lower()
        keyword = None
        for k, v in SUBTYPE_KEYWORD_MAP.items():
            if k in s:
                keyword = v
                break
        if not keyword:
            return []
        kw = keyword.lower()
        return [p for p in self.plan_definitions.keys() if kw in p.lower()]

    def _families_for_feature(self, feature: str) -> Set[str]:
        fams: Set[str] = set()
        for plan in self.plan_definitions:
            for _, fam in SUBTYPE_KEYWORD_MAP.items():
                if fam.lower() in plan.lower() and feature in self.plan_definitions.get(plan, set()):
                    fams.add(fam)
        return fams

    def _business_value_score(self, coverage_count: int, user_count: int, extras_count: int, missing_critical: int, subtype_aligned: bool, synonym_used: int) -> float:
        if user_count <= 0:
            return 0.0
        coverage_ratio = coverage_count / max(1, user_count)
        score = 30.0 * coverage_ratio
        score += 10.0 if subtype_aligned else 0.0
        score -= 2.0 * extras_count
        score -= 20.0 * missing_critical
        score -= 5.0 * synonym_used
        return max(-100.0, min(100.0, score))

    def recommend(self, account_row: dict) -> dict:
        subtype = account_row.get("Sub Type", account_row.get("Subtype", "Unknown"))
        raw_user_features = parse_feature_list(account_row.get("featureNames", []))
        user_features = {self._canon_feature(f) for f in raw_user_features}

        candidates = self.get_relevant_plans(subtype)
        if not candidates:
            return {
                "status": "NO_MATCHING_PLANS",
                "recommended_plan": "Manual Review",
                "covered_features": [],
                "extras": [],
                "bloat_features": [],
                "bloat_score": 0,
                "extras_count": 0,
                "reason": f"No plans found for subtype '{subtype}'",
                "all_candidates": [],
                "ambiguous_mapping": False,
                "unrecognized_features": list(user_features),
                "migration_confidence": 0.0,
            }

        analyses: List[dict] = []  # all relevant plans with impact (including paid bloat)
        valid_candidates: List[dict] = []  # filtered list used for auto-recommendation
        all_plan_features: Set[str] = set().union(*self.plan_definitions.values()) if self.plan_definitions else set()
        synonym_hits = {f for f in raw_user_features if canonicalize(f, self.synonyms) != clean_feature_name(f)}
        # Pre-compute add-on coverage: only the subset of add-on features used by the account
        applied_add_ons: List[str] = []
        add_on_cover: Set[str] = set()
        for addon_name, addon_feats in self.add_on_plans.items():
            used = {f for f in addon_feats if f in user_features}
            if used:
                add_on_cover |= used
                applied_add_ons.append(addon_name)

        for plan in candidates:
            # Base plan features
            plan_features_raw = {clean_feature_name(f) for f in self.plan_definitions.get(plan, set())}
            # Combine with applicable add-on features actually used by the account
            combined_plan_features = plan_features_raw | add_on_cover
            # Classify and sanitize
            u = self._classify(user_features)
            p = self._classify(combined_plan_features)
            user_norm = u["normal"]
            plan_norm = p["normal"]

            covered = sorted(user_norm & plan_norm)
            # Feature-level extras missing from the base plan
            feature_extras = sorted(user_norm - plan_norm)
            extras_weighted = len(feature_extras)
            bloat = sorted(plan_norm - user_norm)
            cost_set = {x.lower() for x in EXTRA_COST_FEATURES}
            bloat_costly = [b for b in bloat if str(b).strip().lower() in cost_set]
            bloat_weighted = len(bloat) + self.cost_bloat_weight * len(bloat_costly)

            missing_critical = 0
            subtype_aligned = True
            synonym_used = len(synonym_hits)
            bv_score = self._business_value_score(
                coverage_count=len(covered),
                user_count=len(user_features),
                extras_count=len(feature_extras),
                missing_critical=missing_critical,
                subtype_aligned=subtype_aligned,
                synonym_used=synonym_used,
            )

            ga_combined = sorted(list((u["ga"] | p["ga"])) )
            irr_combined = sorted(list((u["irrelevant"] | p["irrelevant"])) )

            # Display/Sync: Merge applied add-on plan names into Add-ons needed
            # Exclude individual features that are covered by add-on bundles
            feature_extras_not_in_addons = [f for f in feature_extras if f not in add_on_cover]
            merged_extras: List[str] = []
            seen_me = set()
            for x in list(feature_extras_not_in_addons) + list(applied_add_ons):
                key = str(x).strip().lower()
                if key and key not in seen_me:
                    seen_me.add(key)
                    merged_extras.append(str(x).strip())

            row_data = {
                "plan": plan,
                "addOnPlans": list(applied_add_ons),
                "covered_features": covered,
                # extras: merged for display/sync; feature_extras kept for internal calculations
                "extras": merged_extras,
                "feature_extras": feature_extras,
                "bloat_features": bloat,
                "bloat_score": len(bloat),
                "bloat_costly": bloat_costly,
                "bloat_costly_count": len(bloat_costly),
                "bloat_weighted": bloat_weighted,
                # Keep ranking based on feature extras only
                "extras_count": len(feature_extras),
                "extras_weighted": extras_weighted,
                "coverage_count": len(covered),
                "business_value_score": bv_score,
                "gaFeatures": ga_combined,
                "irrelevantFeatures": irr_combined,
                "planFeatures": sorted(list(plan_norm)),
                "accountFeatures": sorted(list(user_norm)),
                "missingFeatures": feature_extras,
            }
            analyses.append(row_data)
            if len(bloat_costly) == 0:
                valid_candidates.append(row_data)

        if not valid_candidates:
            return {
                "status": "No Valid Plans",
                "recommended_plan": "Manual Review",
                "covered_features": [],
                "extras": [],
                "bloat_features": [],
                "bloat_score": 0,
                "bloat_costly": [],
                "bloat_costly_count": 0,
                "bloat_weighted": 0,
                "extras_count": 0,
                "extras_weighted": 0,
                "all_candidates": [],
                "reason": "All candidates rejected due to paid bloat or no subtype matches",
                "ambiguous_mapping": False,
                "unrecognized_features": sorted([f for f in user_features if f not in all_plan_features]),
                "migration_confidence": 0.0,
            }

        valid_candidates.sort(
            key=lambda x: (
                x["extras_count"],
                x["bloat_weighted"],
                -x["coverage_count"],
                -x["business_value_score"],
            )
        )
        winner = valid_candidates[0]

        fams = set()
        for f in user_features:
            fams.update(self._families_for_feature(f))
        ambiguous_mapping = len(fams) > 1
        unrecognized = sorted([f for f in user_features if f not in all_plan_features])
        coverage_ratio = winner["coverage_count"] / max(1, len(user_features))
        conf = 30.0 * coverage_ratio + (10.0 if not ambiguous_mapping else 0.0) - 2.0 * winner["extras_count"] - 5.0 * len(synonym_hits)
        migration_confidence = float(max(0.0, min(100.0, conf)))

        return {
            "status": "Success",
            "recommended_plan": winner["plan"],
            "addOnPlans": list(applied_add_ons),
            "covered_features": winner["covered_features"],
            # Expose merged extras for display/sync and feature_extras for calculations
            "extras": winner["extras"],
            "feature_extras": winner.get("feature_extras", []),
            "bloat_features": winner["bloat_features"],
            "bloat_score": winner["bloat_score"],
            "bloat_costly": winner["bloat_costly"],
            "bloat_costly_count": winner["bloat_costly_count"],
            "extras_count": winner["extras_count"],
            "extras_weighted": winner["extras_weighted"],
            "bloat_weighted": winner["bloat_weighted"],
            "business_value_score": winner["business_value_score"],
            "ambiguous_mapping": ambiguous_mapping,
            "unrecognized_features": unrecognized,
            "migration_confidence": migration_confidence,
            "all_candidates": [
                {
                    "plan": a["plan"],
                    "addOnPlans": a.get("addOnPlans", []),
                    "extras": a["extras"],
                    "feature_extras": a.get("feature_extras", []),
                    "extras_weighted": a["extras_weighted"],
                    "bloat_features": a["bloat_features"],
                    "bloat_count": a["bloat_score"],
                    "bloat_costly": a["bloat_costly"],
                    "bloat_costly_count": a["bloat_costly_count"],
                    "bloat_weighted": a["bloat_weighted"],
                    "coverage_count": a["coverage_count"],
                    "business_value_score": a["business_value_score"],
                    "gaFeatures": a.get("gaFeatures", []),
                    "irrelevantFeatures": a.get("irrelevantFeatures", []),
                    "planFeatures": a.get("planFeatures", []),
                    "accountFeatures": a.get("accountFeatures", []),
                    "missingFeatures": a.get("missingFeatures", []),
                }
                for a in valid_candidates
            ],
            # New: include all relevant plans regardless of paid bloat
            "all_plans": [
                {
                    "plan": a["plan"],
                    "addOnPlans": a.get("addOnPlans", []),
                    "extras": a["extras"],
                    "feature_extras": a.get("feature_extras", []),
                    "extras_weighted": a["extras_weighted"],
                    "bloat_features": a["bloat_features"],
                    "bloat_count": a["bloat_score"],
                    "bloat_costly": a["bloat_costly"],
                    "bloat_costly_count": a["bloat_costly_count"],
                    "bloat_weighted": a["bloat_weighted"],
                    "coverage_count": a["coverage_count"],
                    "business_value_score": a["business_value_score"],
                    "gaFeatures": a.get("gaFeatures", []),
                    "irrelevantFeatures": a.get("irrelevantFeatures", []),
                    "planFeatures": a.get("planFeatures", []),
                    "accountFeatures": a.get("accountFeatures", []),
                    "missingFeatures": a.get("missingFeatures", []),
                }
                for a in analyses
            ],
            # New sections for GA/Irrelevant and normalized views
            "gaFeatures": winner.get("gaFeatures", []),
            "irrelevantFeatures": winner.get("irrelevantFeatures", []),
            "planFeatures": winner.get("planFeatures", []),
            "accountFeatures": winner.get("accountFeatures", []),
            "missingFeatures": winner.get("missingFeatures", []),
            "why": f"GA Features in this plan: {', '.join(winner.get('gaFeatures', [])) or 'None'}",
        }

    def apply_human_override(self, plan_name: str, extras_list: Iterable[str], user_features: Iterable[str]) -> dict:
        """Apply a CSM override while enforcing red lines and recomputing metrics.

        Respects GA and Irrelevant precedence; these do not contribute to extras/bloat.
        """
        extras_canon = [self._canon_feature(e) for e in (extras_list or [])]
        user_canon = [self._canon_feature(u) for u in (user_features or [])]

        # Classify and sanitize
        u = self._classify(user_canon)
        e = self._classify(extras_canon)
        plan_features_raw = set(self.plan_definitions.get(plan_name, set()))
        # Apply add-on coverage for human override context as well
        add_on_cover: Set[str] = set()
        for addon_name, addon_feats in self.add_on_plans.items():
            used = {f for f in addon_feats if f in set(user_canon)}
            if used:
                add_on_cover |= used
        combined_plan_features = list(plan_features_raw | add_on_cover)
        p = self._classify(combined_plan_features)

        user_norm = u["normal"]
        plan_norm = p["normal"]
        extras_norm = e["normal"]

        # Effective bundle and bloat after removing GA/irrelevant
        effective_bundle = plan_norm | set(extras_norm)
        bloat_features = sorted(effective_bundle - user_norm)
        cost_set = {x.lower() for x in EXTRA_COST_FEATURES}
        paid_bloat = [b for b in bloat_features if str(b).strip().lower() in cost_set]
        if paid_bloat:
            return {
                "status": "REJECTED_RED_LINE",
                "reason": "Override introduces paid bloat",
                "paid_bloat": paid_bloat,
            }
        covered = sorted(user_norm & plan_norm)
        extras = sorted(set(extras_norm))
        return {
            "status": "APPROVED_BY_CSM",
            "final_plan": plan_name,
            "extras": extras,
            "covered_features": covered,
            "bloat_features": bloat_features,
            "bloat_costly": paid_bloat,
            "bloat_costly_count": len(paid_bloat),
            "bloat_score": len(bloat_features),
            "gaFeatures": sorted(list((u["ga"] | p["ga"]))),
            "irrelevantFeatures": sorted(list((u["irrelevant"] | p["irrelevant"]))),
        }
