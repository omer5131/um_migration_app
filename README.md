# Account Migration Recommendation Tool

This application automates the mapping of existing customer accounts to a new "Plan + Add-ons" pricing structure.

## Setup & Installation

1.  **Install Dependencies:**
    Ensure you have Python installed, then run:
    ```bash
    pip install -r requirements.txt
    ```

2.  **Data Files / Sources:**
    - CSV/Excel: You can continue to load local files.
    - Google Sheets: Use the UI to provide a URL and service account JSON.
    - Airtable (recommended & default): Configure environment variables and sync to a persistent cache. The app auto-loads from Airtable on startup; the Data Sources tab hides if this succeeds. Other sources only appear if Airtable fails.
      - Copy `config/.env.example` to `.env` (or export env vars in your shell) and set:
        - `AIRTABLE_API_KEY`, `AIRTABLE_BASE_ID`, `AIRTABLE_TABLE`, optional `AIRTABLE_VIEW`, and `AIRTABLE_CACHE_PATH`.
      - To sync immediately: `python scripts/sync_airtable.py --refresh`
      - The Streamlit UI also has an “Airtable” source with buttons to sync or load from cache.
      - Approvals: Human-approved selections can be upserted to a separate Airtable table (set `AIRTABLE_APPROVALS_TABLE`, default `Approvals`). Use the per-approval actions or the bulk "Sync approvals to Airtable" button.
    
    Plan→Features is hard-coded in `src/plan_definitions.py`. You can override it at runtime via the app’s “Manual Plan JSON (override)” section.

3.  **Running the App:**
    Run the application using Streamlit:
    ```bash
    streamlit run app.py
    ```

## Project Structure

* `app.py`: Main application entry point and UI.
* `src/`: Core logic and modules.
    * `config.py`: File paths and configuration constants.
    * `airtable.py`: Minimal Airtable REST client and disk cache helpers.
    * `plan_definitions.py`: Hard-coded plan→features mapping.
    * `data_loader.py`: Loads mapping data and injects plan definitions.
    * `engine.py`: Core recommendation algorithm.
    * `utils.py`: Helper functions for parsing and data cleaning.
* `scripts/sync_airtable.py`: CLI to sync Airtable → `data/airtable_mapping.json`.
* `config/.env.example`: Example Airtable environment configuration.

## Secrets (Streamlit Cloud)

- Do not commit real secrets. `.env` and local secrets files are ignored by git.
- In Streamlit Community Cloud, set secrets via App → Settings → Secrets. Paste TOML like:

```
[AIRTABLE]
API_KEY = "your_airtable_api_key"
BASE_ID = "app_..."
TABLE = "Mapping"  # or table ID
VIEW = ""  # optional
CACHE_PATH = "data/airtable_mapping.json"
APPROVALS_TABLE = "Approvals"

# Optional
OPENAI_API_KEY = "sk-..."
GOOGLE_SERVICE_ACCOUNT_JSON = """{ ...service account json... }"""
```

The app automatically prefers `st.secrets` over OS env vars (see `src/config.py`), so deployment requires no code changes.
