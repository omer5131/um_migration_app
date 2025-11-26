import json
import os
from typing import Dict, Any
from src.config import DATA_DIR

APPROVALS_FILE = os.path.join(DATA_DIR, "approvals.json")

def _ensure_dir():
    os.makedirs(DATA_DIR, exist_ok=True)

def load_approvals() -> Dict[str, Any]:
    _ensure_dir()
    if not os.path.exists(APPROVALS_FILE):
        return {}
    try:
        with open(APPROVALS_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}

def save_approval(account_name: str, plan: str):
    approvals = load_approvals()
    approvals[account_name] = {"plan": plan}
    _ensure_dir()
    with open(APPROVALS_FILE, "w", encoding="utf-8") as f:
        json.dump(approvals, f, indent=2)

def clear_approval(account_name: str):
    approvals = load_approvals()
    if account_name in approvals:
        del approvals[account_name]
        _ensure_dir()
        with open(APPROVALS_FILE, "w", encoding="utf-8") as f:
            json.dump(approvals, f, indent=2)

