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

# Optional: Google Sheets service account JSON (multiline TOML string)
GOOGLE_SERVICE_ACCOUNT_JSON = """
{
  "type": "service_account",
  "project_id": "...",
  "private_key_id": "...",
  "private_key": "-----BEGIN PRIVATE KEY-----\n...\n-----END PRIVATE KEY-----\n",
  "client_email": "...",
  "client_id": "...",
  "auth_uri": "https://accounts.google.com/o/oauth2/auth",
  "token_uri": "https://oauth2.googleapis.com/token",
  "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
  "client_x509_cert_url": "..."
}
"""
```

How the app reads secrets

- `src/config.py` first loads `.env` (if present) for local dev via `python-dotenv`.
- If running in Streamlit, it reads `st.secrets` (supports both top-level keys and nested `[AIRTABLE]` group):
  - `OPENAI_API_KEY` and `GOOGLE_SERVICE_ACCOUNT_JSON` at the top level
  - `AIRTABLE.API_KEY`, `AIRTABLE.BASE_ID`, `AIRTABLE.TABLE`, `AIRTABLE.VIEW`, `AIRTABLE.CACHE_PATH`, `AIRTABLE.APPROVALS_TABLE`
- The app code prefers Streamlit secrets over OS env variables automatically, so no code changes are required when deploying.

