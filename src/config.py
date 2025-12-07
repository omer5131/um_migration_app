import os
try:
    # Load environment from .env if present (local dev)
    from dotenv import load_dotenv  # type: ignore
    load_dotenv()
except Exception:
    pass

try:
    # Streamlit Community Cloud secrets (if running under Streamlit)
    import streamlit as st  # type: ignore
    SECRETS = dict(st.secrets)
except Exception:
    SECRETS = {}

# File Mappings
FILES = {
    "accounts": "Account Migration mapping (4).xlsx - Accounts.csv",
    "account_csm_project": "Account Migration mapping (4).xlsx - Account<>CSM<>Project.csv",
    # Plan mapping is hard-coded in src/plan_definitions.py; no Plan <> FF file needed.
    "pricing_data": "New T&S Pricing - July 2025 (2).xlsx"
}

# Airtable configuration (read from env). Used to sync mapping table to a cached file.
def _from_secrets(name: str, default: str | None = ""):
    """Resolve a value from Streamlit secrets with flexible casing.

    Supports both top-level keys and grouped keys, e.g.:
    - AIRTABLE_API_KEY
    - group/key forms like (AIRTABLE, API_KEY) or (airtable, api_key)
    """
    # Direct lookup first
    if name in SECRETS:
        val = SECRETS.get(name, default)
        return val.strip() if isinstance(val, str) else val

    # Try dot-notation as a fallback (e.g., AIRTABLE.API_KEY)
    grp = key = None
    if "." in name:
        grp, key = name.split(".", 1)
    elif "_" in name:
        # Split once: AIRTABLE_API_KEY -> (AIRTABLE, API_KEY)
        grp, key = name.split("_", 1)

    if grp and key:
        # Consider common casings for group and key
        grp_candidates = [grp, grp.upper(), grp.lower(), grp.capitalize()]
        key_candidates = [key, key.upper(), key.lower(), key.capitalize()]
        for g in grp_candidates:
            group = SECRETS.get(g)
            if isinstance(group, dict):
                for k in key_candidates:
                    if k in group:
                        val = group.get(k, default)
                        return val.strip() if isinstance(val, str) else val
    return default


def _getenv(name: str, default: str = "") -> str:
    # Prefer Streamlit secrets if present, otherwise OS env
    val = _from_secrets(name, None)
    if val is None or val == "":
        val = os.getenv(name, default)
    return val.strip() if isinstance(val, str) else val


# Airtable configuration - loaded from .env file
# Copy .env.example to .env and fill in your Airtable credentials
AIRTABLE = {
    "API_KEY": _getenv("AIRTABLE_API_KEY", ""),
    "BASE_ID": _getenv("AIRTABLE_BASE_ID", "appt1H2lJxpR8NCbC"),
    "TABLE": _getenv("AIRTABLE_TABLE", "tbl7xPdfPcPKzx3Tc"),
    "VIEW": _getenv("AIRTABLE_VIEW", "viwnlPWDCWif3ovUJ"),
    "CACHE_PATH": _getenv("AIRTABLE_CACHE_PATH", os.path.join("data", "airtable_mapping.json")),
    "APPROVALS_TABLE": _getenv("AIRTABLE_APPROVALS_TABLE", "tblWWegam2OOTYpv3"),
}

# Optional: other secrets commonly used in app
OPENAI_API_KEY = _getenv("OPENAI_API_KEY", "")

# Global Availability (GA) features — always available and not counted as extras/bloat
GA_FEATURES = [
    "activitySequences",
    "softAoi",
    "nasAgGridTable",
    "mapMarkers",
    "weatherLayer",
    "nasTransmissionInAreaActivityType",
    "searchDraftBreadthFilters",
    "nasAccidentsActivity",
    "weatherLayer",
    "warRiskArea",
    "newNasRiskDesign",
    "newNasSideBarDesign",
    "darkFleetVOI",
    "trafficLanes",
    "uncertaintyArea",
    "grayFleetVOI",
    "userAlreadyLoggedInWarning",
]

# Irrelevant features — ignored in comparisons and not counted toward extras/bloat/GA/missing
# Note: If a feature appears in both GA and Irrelevant, GA takes precedence (see engine precedence rules).
IRRELEVANT_FEATURES = [
    "activitySequences",
    "advancedSearch",
    "advancedSearchOwners",
    "elasticSearch",
    "meetings",
]

# Mapping SubType to specific keywords found in the 'PLAN' column of Plan <> FF.csv
SUBTYPE_KEYWORD_MAP = {
    "bunkering": "Bunkering",
    "oil": "Oil",  # Matches 'Oil & Energy'
    "energy": "Oil",
    "shipowner": "Shipowners",
    "operator": "Shipowners",
    "insurance": "Insurer",
    "insurer": "Insurer",
    "financial": "Financial",
    "bank": "Financial",
    "commodity": "Commodity",
    "trader": "Commodity",  # Matches 'Commodity Trader'
    "freight": "Maritime",  # Matches 'Maritime Services'
    "maritime": "Maritime",
}

# Features that are considered high-cost (paid) if added as extras
EXTRA_COST_FEATURES = {
    'uboData', 'wetCargoData', 'visualLinkAnalysis', 'nasAgGridTable',
    'maiExpertVesselSummary', 'uboData', 'cddScreening', 'exportCenter'
}



# Weight multiplier applied to high-cost extras in scoring
EXTRA_COST_WEIGHT = 0

# Additional weight applied when high-cost features appear in BLOAT (we can't give these for free)
EXTRA_COST_BLOAT_WEIGHT = 100
