from __future__ import annotations

import json
import re
from typing import Dict

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
        "https://www.googleapis.com/auth/spreadsheets",  # read/write spreadsheets
        "https://www.googleapis.com/auth/drive.readonly",
    ]
    credentials = Credentials.from_service_account_info(creds_dict, scopes=scopes)
    return gspread.authorize(credentials)


def extract_key_from_url(url: str) -> str | None:
    """Extract spreadsheet key from a Google Sheets URL."""
    m = re.search(r"/spreadsheets/d/([a-zA-Z0-9-_]+)", str(url))
    return m.group(1) if m else None


def read_worksheet_as_df(client, spreadsheet_key: str, worksheet_name: str) -> pd.DataFrame:
    if not HAS_GSPREAD:
        raise ImportError(
            "gspread/google-auth not installed. Run: pip install gspread google-auth"
        )
    sh = client.open_by_key(spreadsheet_key)
    ws = sh.worksheet(worksheet_name)
    data = ws.get_all_records()
    return pd.DataFrame(data)


def ensure_worksheet(client, spreadsheet_key: str, worksheet_name: str, rows: int = 1000, cols: int = 50):
    sh = client.open_by_key(spreadsheet_key)
    try:
        return sh.worksheet(worksheet_name)
    except Exception:
        return sh.add_worksheet(title=worksheet_name, rows=str(rows), cols=str(cols))


def write_dataframe(client, spreadsheet_key: str, worksheet_name: str, df: pd.DataFrame):
    if not HAS_GSPREAD:
        raise ImportError(
            "gspread/google-auth not installed. Run: pip install gspread google-auth"
        )
    ws = ensure_worksheet(client, spreadsheet_key, worksheet_name)
    # Clear and write header + values
    ws.clear()
    if df is None or df.empty:
        return
    values = [list(map(str, df.columns.tolist()))]
    values += [list(map(lambda x: "" if pd.isna(x) else str(x), row)) for row in df.to_numpy()]
    ws.update('A1', values)


def load_from_sheets(client, sheets_cfg: Dict[str, Dict[str, str]]) -> Dict[str, pd.DataFrame]:
    if not HAS_GSPREAD:
        raise ImportError(
            "gspread/google-auth not installed. Run: pip install gspread google-auth"
        )
    data: Dict[str, pd.DataFrame] = {}
    for key, cfg in sheets_cfg.items():
        data[key] = read_worksheet_as_df(client, cfg["spreadsheet_key"], cfg["worksheet"])
    return data
