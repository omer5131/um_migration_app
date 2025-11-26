import openai
import streamlit as st
import ast
from src.config import EXTRA_COST_FEATURES

class ReviewAgent:
    def __init__(self, api_key):
        self.api_key = api_key
        self.client = None
        if api_key:
            self.client = openai.OpenAI(api_key=api_key)

    def review_recommendation(self, account_name, subtype, user_features, recommendation):
        if not self.client:
            return "Agent not active (No API Key)"

        plan = recommendation.get('recommended_plan')
        extras = recommendation.get('extras', [])
        bloat_score = recommendation.get('bloat_score', 0)
        bloat_features = recommendation.get('bloat_features') or recommendation.get('bloat_details', [])
        paid_bloat = [b for b in bloat_features if str(b).strip().lower() in {x.lower() for x in EXTRA_COST_FEATURES}]
        
        # --- 1. Calculate "Covered Features" (The Match) ---
        # We need to see what actually matched to judge the plan quality.
        # Covered = User_Features - Extras
        
        # Ensure user_features is a clean set of strings
        if isinstance(user_features, str):
            try:
                user_list = ast.literal_eval(user_features)
            except:
                user_list = user_features.split(',')
        else:
            user_list = user_features

        user_set = set([str(f).strip() for f in user_list])
        extras_set = set([str(f).strip() for f in extras])
        paid_extras = [e for e in extras_set if str(e).strip().lower() in {x.lower() for x in EXTRA_COST_FEATURES}]
        
        # The intersection of what the user has and what wasn't flagged as missing
        covered_features = list(user_set - extras_set)

        # --- 2. Construct Comprehensive Prompt ---
        user_message = f"""
        **Analyze this Migration Case:**
        
        1. **Account Details:**
           - Name: {account_name}
           - SubType: {subtype}
           - Total Feature Count: {len(user_set)}
           
        2. **System Proposal:**
           - **Selected Plan:** {plan}
           
           - **✅ MATCHED FEATURES (Covered):** {covered_features}
             *(These are features the user has that ARE included in the plan)*
             
           - **➕ EXTRAS (Add-ons):** {extras}
             *(These are features the user has that are NOT in the plan and need to be added)*
           
           - **⚠️ BLOAT (Plan+Extras − Current):** {bloat_features}
             *(Capabilities included in the Plan plus Extras that the user does NOT currently use)*
             - **🚫 Paid Bloat (cannot give for free):** {paid_bloat}
        
        **Verdict?**
        """

        system_instruction = """
        You are an expert Account Migration Architect.
        
        **YOUR AUDIT ALGORITHM:**
        
        1. **Check SubType:** - Does the Plan Name '{plan}' match the Account SubType '{subtype}'? 
           - (e.g. 'Bunkering' subtype must have a 'Bunkering' plan).
           - If NO -> REJECT immediately.
        
        2. **Analyze "Extras" (Mapping vs Real):** - Look at the 'EXTRAS' list. Do any of these look like they *should* be in the plan but might be unmapped? (e.g. 'wetCargoData' vs 'Wet Cargo').
           - **Crucial:** If you suspect a mapping error (the feature effectively exists in the plan under a different name), count it as "Covered" mentally and mention it.
           - If they are true add-ons, are they reasonable?
           
        3. **Analyze Trade-off (New Preference):**
           - **Primary:** Minimize Extras (prefer fewer add-ons), even if that means accepting some bloat.
           - **Secondary:** Minimize Bloat (computed as Plan+Extras − Current Features).
           - If two options have similar extras, choose the one with lower bloat.
           
        4. **Final Verdict:**
           - **APPROVED:** Good fit. Low bloat, reasonable extras.
           - **WARNING:** High bloat (user paying for unused tech) OR suspected mapping errors in Extras.
           - **REJECT:** Wrong subtype family or nonsensical proposal.
        
        Output a short, professional assessment.
        """

        try:
            response = self.client.chat.completions.create(
                model="gpt-4o",
                messages=[
                    {"role": "system", "content": system_instruction},
                    {"role": "user", "content": user_message}
                ],
                temperature=0.2, # Keep it analytical
                max_tokens=300
            )
            return response.choices[0].message.content
        except Exception as e:
            return f"Agent Error: {str(e)}"
