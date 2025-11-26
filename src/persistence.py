import os
import json
import time
from typing import Dict, Optional, List

import pandas as pd


DEFAULT_PATH = os.path.join("data", "approvals.csv")


class ApprovalsStore:
    """Simple CSV-backed store for human-approved rows.

    Schema:
    - Account, Sub Type, Final Plan, Extras (comma-separated), Approved By, Approved At (epoch seconds)
    """

    def __init__(self, path: str = DEFAULT_PATH):
        self.path = path
        os.makedirs(os.path.dirname(self.path), exist_ok=True)
        self._df = self._load()

    def _load(self) -> pd.DataFrame:
        if os.path.exists(self.path):
            try:
                return pd.read_csv(self.path)
            except Exception:
                return pd.DataFrame(
                    columns=[
                        "Account",
                        "Sub Type",
                        "Final Plan",
                        "Extras",
                        "Approved By",
                        "Approved At",
                    ]
                )
        return pd.DataFrame(
            columns=[
                "Account",
                "Sub Type",
                "Final Plan",
                "Extras",
                "Approved By",
                "Approved At",
            ]
        )

    def _persist(self) -> None:
        # Atomic-ish write
        tmp_path = self.path + ".tmp"
        self._df.to_csv(tmp_path, index=False)
        os.replace(tmp_path, self.path)

    def get(self, account: str) -> Optional[Dict]:
        if self._df.empty:
            return None
        rows = self._df[self._df["Account"] == account]
        if rows.empty:
            return None
        row = rows.iloc[0].to_dict()
        # Parse extras as list
        extras = [x.strip() for x in str(row.get("Extras", "")).split(",") if x.strip()]
        row["Extras"] = extras
        return row

    def upsert(self, account: str, subtype: str, final_plan: str, extras: List[str], approved_by: str = "") -> None:
        ts = int(time.time())
        extras_str = ", ".join(extras)
        idx = None
        if not self._df.empty:
            matches = self._df.index[self._df["Account"] == account].tolist()
            if matches:
                idx = matches[0]

        row = {
            "Account": account,
            "Sub Type": subtype,
            "Final Plan": final_plan,
            "Extras": extras_str,
            "Approved By": approved_by,
            "Approved At": ts,
        }

        if idx is None:
            self._df = pd.concat([self._df, pd.DataFrame([row])], ignore_index=True)
        else:
            for k, v in row.items():
                self._df.at[idx, k] = v
        self._persist()

    def all(self) -> pd.DataFrame:
        return self._df.copy()

