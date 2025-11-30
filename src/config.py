import os
try:
    # Load environment from .env if present
    from dotenv import load_dotenv  # type: ignore
    load_dotenv()
except Exception:
    pass

# File Mappings
FILES = {
    "accounts": "Account Migration mapping (4).xlsx - Accounts.csv",
    "account_csm_project": "Account Migration mapping (4).xlsx - Account<>CSM<>Project.csv",
    # Plan mapping is hard-coded in src/plan_definitions.py; no Plan <> FF file needed.
    "pricing_data": "New T&S Pricing - July 2025 (2).xlsx"
}

# Airtable configuration (read from env). Used to sync mapping table to a cached file.
def _getenv(name: str, default: str = "") -> str:
    val = os.getenv(name, default)
    return val.strip() if isinstance(val, str) else val


AIRTABLE = {
    "API_KEY": _getenv("AIRTABLE_API_KEY", ""),
    "BASE_ID": _getenv("AIRTABLE_BASE_ID", ""),
    "TABLE": _getenv("AIRTABLE_TABLE", ""),  # table name or ID
    "VIEW": _getenv("AIRTABLE_VIEW", ""),
    # Persistent cache path for Airtable pull
    "CACHE_PATH": _getenv("AIRTABLE_CACHE_PATH", os.path.join("data", "airtable_mapping.json")),
    # Table (or ID) to store approvals records
    "APPROVALS_TABLE": _getenv("AIRTABLE_APPROVALS_TABLE", "Approvals"),
}

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
    'uboData', 'wetCargoData', 'vlaMaiExpert', 'nasAgGridTable',
    'maiExpertVesselSummary', 'uboData', 'cddScreening', 'exportCenter'
}



# Weight multiplier applied to high-cost extras in scoring
EXTRA_COST_WEIGHT = 0

# Additional weight applied when high-cost features appear in BLOAT (we can't give these for free)
EXTRA_COST_BLOAT_WEIGHT = 100
