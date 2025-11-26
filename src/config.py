import os

# File Mappings
FILES = {
    "accounts": "Account Migration mapping (4).xlsx - Accounts.csv",
    "account_csm_project": "Account Migration mapping (4).xlsx - Account<>CSM<>Project.csv",
    "plan_features": "Account Migration mapping (4).xlsx - Plan <> FF.csv", 
    # We still keep these for reference if needed, but primary logic now uses Plan <> FF
    "pricing_data": "New T&S Pricing - July 2025 (2).xlsx" 
}

# Mapping SubType to specific keywords found in the 'PLAN' column of Plan <> FF.csv
SUBTYPE_KEYWORD_MAP = {
    "bunkering": "Bunkering",
    "oil": "Oil",
    "energy": "Energy",
    "shipowner": "Shipowners",
    "operator": "Shipowners",
    "insurance": "Insurer",
    "insurer": "Insurer",
    "financial": "Financial",
    "bank": "Financial",
    "commodity": "Commodity",
    "trader": "Trader",
    "freight": "Maritime",
    "maritime": "Maritime"
}

# Features that are considered high-cost (paid) if added as extras
EXTRA_COST_FEATURES = {
    # normalize comparisons to lowercase in code
    "maiexpertvesseladversemedia",
    "maiexpertvesselsummary",
    "ubodata",
    "wetcargodata",
    "nasagridtable",
    "nasaggridtable",
    "vlamaiexpert",
    "visuallinkanalysis",
}



# Weight multiplier applied to high-cost extras in scoring
EXTRA_COST_WEIGHT = 0

# Additional weight applied when high-cost features appear in BLOAT (we can't give these for free)
EXTRA_COST_BLOAT_WEIGHT = 100
