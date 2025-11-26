from __future__ import annotations

import json
from typing import Dict, Optional

import pandas as pd

try:
    import gspread  # optional dependency
    from google.oauth2.service_account import Credentials
    HAS_GSPREAD = True
except Exception:  # ImportError and others
    gspread = None
    Credentials = None
    HAS_GSPREAD = False


def make_client(service_account_json: str):
    if not HAS_GSPREAD:
        raise ImportError(
            "gspread/google-auth not installed. Run: pip install gspread google-auth"
        )
    creds_dict = json.loads(service_account_json)
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets.readonly",
        "https://www.googleapis.com/auth/drive.readonly",
    ]
    credentials = Credentials.from_service_account_info(creds_dict, scopes=scopes)
    return gspread.authorize(credentials)


def read_worksheet_as_df(client, spreadsheet_key: str, worksheet_name: str) -> pd.DataFrame:
    if not HAS_GSPREAD:
        raise ImportError(
            "gspread/google-auth not installed. Run: pip install gspread google-auth"
        )
    sh = client.open_by_key(spreadsheet_key)
    ws = sh.worksheet(worksheet_name)
    data = ws.get_all_records()
    return pd.DataFrame(data)


def load_from_sheets(client, sheets_cfg: Dict[str, Dict[str, str]]) -> Dict[str, pd.DataFrame]:
    if not HAS_GSPREAD:
        raise ImportError(
            "gspread/google-auth not installed. Run: pip install gspread google-auth"
        )
    data: Dict[str, pd.DataFrame] = {}
    for key, cfg in sheets_cfg.items():
        data[key] = read_worksheet_as_df(client, cfg["spreadsheet_key"], cfg["worksheet"])
    return data
