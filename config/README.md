Using secrets safely

- Do not commit real secrets. The repo already ignores `.env`, `.streamlit/secrets.toml`, and `config/secrets.toml`.
- For local development, copy `config/.env.example` to `.env` and fill values, or copy `.streamlit/secrets.toml.example` to `.streamlit/secrets.toml`.

Streamlit Community Cloud

1) Open your deployed app in Streamlit Community Cloud.
2) Go to: App -> Settings -> Secrets.
3) Paste TOML-formatted secrets and Save.

Example secrets (copy/paste into Streamlit Secrets):

```
[AIRTABLE]
API_KEY = "your_airtable_api_key"
BASE_ID = "app_..."
TABLE = "Mapping"  # or table ID
VIEW = ""  # optional
CACHE_PATH = "data/airtable_mapping.json"
APPROVALS_TABLE = "Approvals"

# Optional: used by the Agent
OPENAI_API_KEY = "sk-..."
```

How the app reads secrets

- `src/config.py` first loads `.env` (if present) for local dev via `python-dotenv`.
- If running in Streamlit, it reads `st.secrets` (supports both top-level keys and nested `[AIRTABLE]` group):
  - `OPENAI_API_KEY` at the top level
  - `AIRTABLE.API_KEY`, `AIRTABLE.BASE_ID`, `AIRTABLE.TABLE`, `AIRTABLE.VIEW`, `AIRTABLE.CACHE_PATH`, `AIRTABLE.APPROVALS_TABLE`
- The app code prefers Streamlit secrets over OS env variables automatically, so no code changes are required when deploying.
