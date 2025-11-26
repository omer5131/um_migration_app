import pandas as pd
from src.config import (
    SUBTYPE_KEYWORD_MAP,
    EXTRA_COST_FEATURES,
    EXTRA_COST_WEIGHT,
    EXTRA_COST_BLOAT_WEIGHT,
)
from src.utils import parse_feature_list, clean_feature_name


class MigrationLogic:
    def __init__(self, plan_matrix_df=None, plan_json: dict | None = None, cost_bloat_weight: int | None = None):
        if isinstance(plan_json, dict) and plan_json:
            # Use pre-built JSON mapping of plan -> list(features)
            self.plan_definitions = {k: set(v or []) for k, v in plan_json.items()}
        else:
            self.plan_definitions = self._build_plan_definitions(plan_matrix_df)
        # Allow runtime tuning of paid-bloat penalty
        self.cost_bloat_weight = cost_bloat_weight if cost_bloat_weight is not None else EXTRA_COST_BLOAT_WEIGHT

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
        valid_candidates = []
        for plan in candidates:
            plan_features = set(clean_feature_name(f) for f in self.plan_definitions.get(plan, set()))

            covered = sorted(user_features & plan_features)
            # Extras are gaps the user has that the plan lacks
            extras = sorted(user_features - plan_features)
            # Extras weighting: simple count (paid extras not penalized)
            extras_weighted = len(extras)
            # Bloat = plan features the user doesn't currently use
            bloat = sorted(plan_features - user_features)
            # Identify costly bloat features (we can't give these for free)
            cost_set = {x.lower() for x in EXTRA_COST_FEATURES}
            bloat_costly = [b for b in bloat if str(b).strip().lower() in cost_set]
            # Hard constraint: reject plans with any paid bloat
            if len(bloat_costly) > 0:
                continue
            bloat_weighted = len(bloat) + self.cost_bloat_weight * len(bloat_costly)

            row_data = {
                'plan': plan,
                'covered_features': covered,
                'extras': extras,
                'bloat_features': bloat,
                'bloat_score': len(bloat),
                'bloat_costly': bloat_costly,
                'bloat_costly_count': len(bloat_costly),
                'bloat_weighted': bloat_weighted,
                'extras_count': len(extras),
                'extras_weighted': extras_weighted,
                'coverage_count': len(covered),
            }
            analyses.append(row_data)
            valid_candidates.append(row_data)

        # If none passed the hard constraint, bail out
        if not valid_candidates:
            return {
                'status': 'No Valid Plans',
                'recommended_plan': 'Manual Review',
                'covered_features': [],
                'extras': [],
                'bloat_features': [],
                'bloat_score': 0,
                'bloat_costly': [],
                'bloat_costly_count': 0,
                'bloat_weighted': 0,
                'extras_count': 0,
                'extras_weighted': 0,
                'all_candidates': [],
                'reason': 'All candidates rejected due to paid bloat or no subtype matches',
            }
        # New priority: minimize extras (simple count), then bloat (weighted for costly bloat); tie-break by higher coverage
        valid_candidates.sort(key=lambda x: (x['extras_count'], x['bloat_weighted'], -x['coverage_count']))
        winner = valid_candidates[0]

        return {
            'status': 'Success',
            'recommended_plan': winner['plan'],
            'covered_features': winner['covered_features'],
            'extras': winner['extras'],
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
                    'extras_weighted': a['extras_weighted'],
                    'bloat_features': a['bloat_features'],
                    'bloat_count': a['bloat_score'],
                    'bloat_costly': a['bloat_costly'],
                    'bloat_costly_count': a['bloat_costly_count'],
                    'bloat_weighted': a['bloat_weighted'],
                }
                for a in valid_candidates
            ],
        }
