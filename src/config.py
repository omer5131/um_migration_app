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


# Airtable configuration (read from env). Used to sync mapping table to a cached file.
def _from_secrets(name: str, default: str = "") -> str:
    # First check top-level, then nested groups (e.g., AIRTABLE.API_KEY)
    if name in SECRETS:
        val = SECRETS.get(name, default)
        return val.strip() if isinstance(val, str) else val
    # Resolve group and key when using NAME like AIRTABLE_API_KEY
    if "_" in name:
        grp, key = name.split("_", 1)
        group = SECRETS.get(grp) if isinstance(SECRETS.get(grp), dict) else None
        if group and key in group:
            val = group.get(key, default)
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
GOOGLE_SERVICE_ACCOUNT_JSON = _getenv("GOOGLE_SERVICE_ACCOUNT_JSON", "")

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
    "sendHeartbeat",
    "complianceMode",
    "complianceLabel"
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

# Default file locations (can be overridden via env vars)
FILES = {
    # Optional accounts CSV; data_loader falls back gracefully if missing
    "accounts": _getenv("ACCOUNTS_CSV_PATH", os.path.join("data", "accounts.csv")),
    # Approvals CSV handled by persistence; exposed here for convenience/consistency
    "approvals": _getenv("APPROVALS_CSV_PATH", os.path.join("data", "approvals.csv")),
}
