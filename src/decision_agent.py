import openai
import ast
import re
from src.config import EXTRA_COST_FEATURES


class DecisionAgent:
    def __init__(self, api_key):
        self.api_key = api_key
        self.client = None
        if api_key:
            self.client = openai.OpenAI(api_key=api_key)

    def _parse_features(self, features_data):
        """
        Helper to safely parse feature lists from strings or lists.
        """
        if isinstance(features_data, str):
            try:
                # Try parsing as python list string "['a', 'b']"
                parsed = ast.literal_eval(features_data)
                if isinstance(parsed, (list, tuple)):
                    return [str(f).strip() for f in parsed]
            except Exception:
                pass
            # Fallback to comma separation
            return [str(f).strip() for f in features_data.split(',') if f.strip()]
        elif isinstance(features_data, (list, set, tuple)):
            return [str(f).strip() for f in features_data]
        return []

    def _parse_decision_text(self, text: str):
        """Parse the agent's markdown-like decision into a structured dict.

        Expected blocks:
        **DECISION:** <plan>
        **REASONING:** <one line>
        **COVERED:** [list]
        **EXTRAS:** [list]
        """
        out = {"plan": None, "reasoning": "", "covered": [], "extras": []}
        if not isinstance(text, str):
            return out

        def find_block(label: str):
            # Match lines like **LABEL:** value
            m = re.search(r"\*\*" + re.escape(label) + r"\*\*:\s*(.+)", text)
            return m.group(1).strip() if m else None

        decision = find_block("DECISION")
        if decision:
            out["plan"] = decision
        reasoning = find_block("REASONING")
        if reasoning:
            out["reasoning"] = reasoning
        covered_txt = find_block("COVERED")
        extras_txt = find_block("EXTRAS")

        def to_list(s):
            if not s:
                return []
            # Try literal list first
            try:
                v = ast.literal_eval(s)
                if isinstance(v, (list, tuple)):
                    return [str(x).strip() for x in v]
            except Exception:
                pass
            # Fallback: comma-separated
            return [x.strip() for x in re.split(r",|;", s) if x.strip()]

        out["covered"] = to_list(covered_txt)
        out["extras"] = to_list(extras_txt)
        return out

    def make_decision(self, account_name, subtype, user_features, logic_result):
        """
        Analyzes ALL candidates provided by the logic engine and selects the best fit.
        """
        if not self.client:
            return "Agent not active (No API Key)"

        # 1. Parse User Features
        user_list = self._parse_features(user_features)
        user_set = set(user_list)

        # 2. Prepare Candidates Context
        candidates = logic_result.get('all_candidates', [])

        # Fallback to the single recommendation
        if not candidates and 'recommended_plan' in logic_result:
            candidates = [{
                'plan': logic_result.get('recommended_plan'),
                'extras': logic_result.get('extras', []),
                'bloat_features': logic_result.get('bloat_details', []),
                'bloat_count': logic_result.get('bloat_score', 0)
            }]

        if not candidates:
            return "No valid plan candidates found to choose from."

        candidates_context = ""
        cost_set = {x.lower() for x in EXTRA_COST_FEATURES}
        for i, cand in enumerate(candidates):
            plan_name = cand.get('plan', 'Unknown')
            extras = cand.get('extras', [])
            bloat = cand.get('bloat_features', [])
            bloat_score = cand.get('bloat_count', len(bloat))

            # Calculate what this plan actually covers
            extras_set = set(self._parse_features(extras))
            covered = list(user_set - extras_set)
            # Paid bloat (we can't give these for free)
            paid_bloat = [b for b in bloat if str(b).strip().lower() in cost_set]

            candidates_context += f"""
            --- OPTION {i+1}: {plan_name} ---
            - 🔍 COVERED FEATURES ({len(covered)}): {covered}
            - ➕ EXTRAS NEEDED ({len(extras)}): {extras}
            - 🚫 PAID BLOAT ({len(paid_bloat)}): {paid_bloat}
            - ⚠️ BLOAT (Plan+Extras − Current Features) ({bloat_score}): {bloat}
            """

        # 3. Construct Prompt for Decision Making
        system_instruction = """
        You are an expert Account Migration Decision Maker.
        Your goal is to select the BEST plan for a customer based on their current feature usage.

        ### DECISION RULES (Strict):
        1) Strict SubType Match: The plan MUST match the account's SubType (e.g., Bunkering -> Bunkering plan family).
        2) NO Paid Bloat: If a plan includes any expensive features the user does not currently use (paid bloat), REJECT that plan.
        3) Minimize Extras (Primary): Among valid plans, prefer the plan that requires the fewest extras (add-ons), even if there is some harmless bloat.
        4) Minimize Bloat (Secondary): If extras are similar, choose the plan with less bloat overall.
        5) Normalization: Treat feature names case-insensitively; trim whitespace.

        ### OUTPUT FORMAT:
        Produce a decision in this specific format:
        **DECISION:** [Selected Plan Name]
        **REASONING:** [Concise explanation of the trade-off]
        **COVERED:** [List of covered features]
        **EXTRAS:** [List of extra features needed]
        """

        user_message = f"""
        **Account:** {account_name} (SubType: {subtype})
        **Current Features:** {user_list}

        **Available Plan Options:**
        {candidates_context}

        **Task:** Compare the options above. Reject options with any Paid Bloat. From the remainder, select the plan that minimizes Extras first, then minimizes Bloat, while covering the user's needs.
        """

        try:
            response = self.client.chat.completions.create(
                model="gpt-4o",
                messages=[
                    {"role": "system", "content": system_instruction},
                    {"role": "user", "content": user_message},
                ],
                temperature=0.1,
                max_tokens=600,
            )
            text = response.choices[0].message.content
            parsed = self._parse_decision_text(text)
            return {"text": text, "parsed": parsed}
        except Exception as e:
            return f"Agent Error: {str(e)}"
