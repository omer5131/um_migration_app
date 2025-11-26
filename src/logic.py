import pandas as pd
from src.config import (
    SUBTYPE_KEYWORD_MAP,
    EXTRA_COST_FEATURES,
    EXTRA_COST_WEIGHT,
    EXTRA_COST_BLOAT_WEIGHT,
)
from src.utils import parse_feature_list, clean_feature_name


class MigrationLogic:
    def __init__(self, plan_matrix_df=None, plan_json: dict | None = None):
        if isinstance(plan_json, dict) and plan_json:
            # Use pre-built JSON mapping of plan -> list(features)
            self.plan_definitions = {k: set(v or []) for k, v in plan_json.items()}
        else:
            self.plan_definitions = self._build_plan_definitions(plan_matrix_df)

    def _build_plan_definitions(self, df):
        """
        Build plan->features mapping from the Plan <> FF matrix.
        Returns: { 'Shipowners Core': {'Feature A', 'Feature B'}, ... }
        """
        definitions = {}
        if df is None or df.empty:
            return definitions

        df = df.copy()
        df.columns = [str(c).upper().strip() for c in df.columns]

        plan_col = next((c for c in df.columns if 'PLAN' in c), None)
        ff_col = next((c for c in df.columns if 'FF' in c or 'FEATURE' in c), None)
        if not plan_col or not ff_col:
            return definitions

        for _, row in df.iterrows():
            plan_name = str(row.get(plan_col, '')).strip()
            feature = str(row.get(ff_col, '')).strip()
            if not plan_name or not feature:
                continue
            if plan_name.lower() == 'nan' or feature.lower() == 'nan':
                continue

            if plan_name not in definitions:
                definitions[plan_name] = set()
            definitions[plan_name].add(clean_feature_name(feature))

        return definitions

    def get_relevant_plans(self, subtype):
        """Filter plans by subtype family (strict)."""
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

    def recommend(self, account_row):
        """
        Algorithm:
        1) Filter by SubType family strictly.
        2) For each candidate plan, compute:
           - covered_features = user ∩ plan
           - extras = user − plan
           - bloat = plan − user
        3) Choose plan by minimizing bloat first, then minimizing extras.
        Returns required detailed fields.
        """
        subtype = account_row.get('Sub Type', account_row.get('Subtype', 'Unknown'))
        user_features = set(
            clean_feature_name(f) for f in parse_feature_list(account_row.get('featureNames', []))
        )

        candidates = self.get_relevant_plans(subtype)
        if not candidates:
            return {
                'status': 'No Matching Plans',
                'recommended_plan': 'Manual Review',
                'covered_features': [],
                'extras': [],
                'bloat_features': [],
                'bloat_score': 0,
                'extras_count': 0,
                'reason': f"No plans found for subtype '{subtype}'",
                'all_candidates': [],
            }

        analyses = []
        for plan in candidates:
            plan_features = set(clean_feature_name(f) for f in self.plan_definitions.get(plan, set()))

            covered = sorted(user_features & plan_features)
            # Extras are gaps the user has that the plan lacks
            extras = sorted(user_features - plan_features)
            # Extras weighting: do NOT penalize costly extras (they're already paid) -> simple count
            costly_extras = []
            extras_weighted = len(extras)
            # Bloat is computed on the effective bundle (plan + extras) minus user features
            effective_bundle = plan_features | set(extras)
            bloat = sorted(effective_bundle - user_features)
            # Identify costly bloat features (we can't give these for free)
            cost_set = {x.lower() for x in EXTRA_COST_FEATURES}
            bloat_lower = [b.lower() for b in bloat]
            bloat_costly = [b for b, bl in zip(bloat, bloat_lower) if bl in cost_set]
            bloat_weighted = len(bloat) + EXTRA_COST_BLOAT_WEIGHT * len(bloat_costly)

            analyses.append({
                'plan': plan,
                'covered_features': covered,
                'extras': extras,
                'extras_costly': costly_extras,
                'extras_costly_count': len(costly_extras),
                'bloat_features': bloat,
                'bloat_score': len(bloat),
                'bloat_costly': bloat_costly,
                'bloat_costly_count': len(bloat_costly),
                'bloat_weighted': bloat_weighted,
                'extras_count': len(extras),
                'extras_weighted': extras_weighted,
                'coverage_count': len(covered),
            })

        # New priority: minimize extras (simple count), then bloat (weighted for costly bloat);
        # tie-break by higher coverage
        analyses.sort(key=lambda x: (x['extras_weighted'], x['bloat_weighted'], -x['coverage_count']))
        winner = analyses[0]

        return {
            'status': 'Success',
            'recommended_plan': winner['plan'],
            'covered_features': winner['covered_features'],
            'extras': winner['extras'],
            'extras_costly': winner['extras_costly'],
            'extras_costly_count': winner['extras_costly_count'],
            'bloat_features': winner['bloat_features'],
            'bloat_score': winner['bloat_score'],
            'bloat_costly': winner['bloat_costly'],
            'bloat_costly_count': winner['bloat_costly_count'],
            'extras_count': winner['extras_count'],
            'extras_weighted': winner['extras_weighted'],
            'bloat_weighted': winner['bloat_weighted'],
            'all_candidates': [
                {
                    'plan': a['plan'],
                    'extras': a['extras'],
                    'extras_costly': a['extras_costly'],
                    'extras_costly_count': a['extras_costly_count'],
                    'extras_weighted': a['extras_weighted'],
                    'bloat_features': a['bloat_features'],
                    'bloat_count': a['bloat_score'],
                    'bloat_costly': a['bloat_costly'],
                    'bloat_costly_count': a['bloat_costly_count'],
                    'bloat_weighted': a['bloat_weighted'],
                }
                for a in analyses
            ],
        }
