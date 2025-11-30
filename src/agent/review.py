import ast
from typing import Any, Dict, List

import openai

from src.config import EXTRA_COST_FEATURES, SUBTYPE_KEYWORD_MAP

# Soft-match synonyms shared with recommendation engine
DEFAULT_SYNONYMS: Dict[str, str] = {
    "Advanced Search": "advancedSearchOwners",
    "Port Control": "portStateControl",
    "Weather Map": "weatherLayer",
}


def canonicalize(name: str, synonyms: Dict[str, str] | None = None) -> str:
    s = str(name).strip()
    if not s:
        return s
    syn = synonyms or {}
    for k, v in syn.items():
        if s.lower() == str(k).strip().lower():
            return str(v).strip()
    return s


class ReviewAgent:
    """
    Stateless wrapper around an LLM call that audits a single recommendation.

    This module intentionally has no UI framework imports to remain reusable
    (no Streamlit dependency).
    """

    def __init__(self, api_key: str | None):
        self.api_key = api_key
        self.client = openai.OpenAI(api_key=api_key) if api_key else None
        self.synonyms = DEFAULT_SYNONYMS

    def review_summary(self, account_name: str, subtype: str, user_features: Any, recommendation: Dict[str, Any]) -> Dict[str, Any]:
        """Return a structured review including classification and soft matches."""
        plan = recommendation.get("recommended_plan")
        extras = [str(x).strip() for x in recommendation.get("extras", [])]
        bloat_features = recommendation.get("bloat_features") or recommendation.get("bloat_details", [])
        cost_set = {x.lower() for x in EXTRA_COST_FEATURES}
        paid_bloat = [b for b in bloat_features if str(b).strip().lower() in cost_set]

        # Parse user features into a list
        if isinstance(user_features, str):
            try:
                user_list = ast.literal_eval(user_features)
            except Exception:
                user_list = [x.strip() for x in user_features.split(",") if x.strip()]
        else:
            user_list = user_features or []

        # Soft matches: extras that canonically match a user feature name or vice versa
        extras_canon = {canonicalize(x, self.synonyms) for x in extras}
        user_canon = {canonicalize(x, self.synonyms) for x in user_list}
        soft_matches = sorted(list(extras_canon & user_canon))

        # Ambiguity: subtype keyword not present in plan name
        subtype_keyword = None
        s = str(subtype or "").lower()
        for k, v in SUBTYPE_KEYWORD_MAP.items():
            if k in s:
                subtype_keyword = v
                break
        subtype_aligned = bool(subtype_keyword and subtype_keyword.lower() in str(plan or "").lower())

        # Unrecognized features: those that after canonicalization are still ambiguous
        unrecognized = [x for x in user_canon if not x]

        # Classification
        if paid_bloat:
            classification = "REJECT"
        elif not subtype_aligned or soft_matches or len(extras) > 5:
            classification = "WARNING"
        else:
            classification = "APPROVED"

        return {
            "classification": classification,
            "soft_matches": soft_matches,
            "paid_bloat": paid_bloat,
            "subtype_aligned": subtype_aligned,
            "unrecognized_features": unrecognized,
        }

    def review_recommendation(
        self,
        account_name: str,
        subtype: str,
        user_features: Any,
        recommendation: Dict[str, Any],
    ) -> str:
        if not self.client:
            return "Agent not active (No API Key)"

        plan = recommendation.get("recommended_plan")
        extras = recommendation.get("extras", [])
        bloat_features = recommendation.get("bloat_features") or recommendation.get(
            "bloat_details", []
        )
        paid_bloat = [
            b
            for b in bloat_features
            if str(b).strip().lower() in {x.lower() for x in EXTRA_COST_FEATURES}
        ]

        # Ensure user_features is a clean set of strings
        if isinstance(user_features, str):
            try:
                user_list = ast.literal_eval(user_features)
            except Exception:
                user_list = user_features.split(",")
        else:
            user_list = user_features

        user_set = {str(f).strip() for f in (user_list or [])}
        extras_set = {str(f).strip() for f in (extras or [])}
        covered_features = list(user_set - extras_set)

        user_message = f"""
        **Analyze this Migration Case:**

        1. **Account Details:**
           - Name: {account_name}
           - SubType: {subtype}
           - Total Feature Count: {len(user_set)}

        2. **System Proposal:**
           - **Selected Plan:** {plan}

           - **✅ MATCHED FEATURES (Covered):** {covered_features}
           - **➕ EXTRAS (Add-ons):** {extras}
           - **⚠️ BLOAT (Plan+Extras − Current):** {bloat_features}
             - **🚫 Paid Bloat (cannot give for free):** {paid_bloat}

        **Verdict?**
        """

        system_instruction = f"""
        You are an expert Account Migration Architect.

        1) Check SubType: Does the Plan Name '{plan}' match the Account SubType '{subtype}'? If not, reject.
        2) Analyze Extras: Suspect mapping errors? If a feature effectively exists under a different name, note it.
        3) Trade-off: Prefer fewer Extras even if accepting some harmless bloat; then minimize bloat.
        4) Final Verdict: APPROVED / WARNING / REJECT with a short, professional explanation.
        """

        try:
            response = self.client.chat.completions.create(
                model="gpt-4o",
                messages=[
                    {"role": "system", "content": system_instruction},
                    {"role": "user", "content": user_message},
                ],
                temperature=0.2,
                max_tokens=300,
            )
            return response.choices[0].message.content
        except Exception as e:
            return f"Agent Error: {str(e)}"
