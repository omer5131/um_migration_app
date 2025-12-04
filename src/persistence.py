import os
import json
import time
from typing import Dict, Optional, List, Tuple
from pathlib import Path

import pandas as pd


# Resolve to repository root/data/approvals.csv regardless of working directory
_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_PATH = str(_ROOT / "data" / "approvals.csv")
BACKUP_PATH = str(_ROOT / "data" / "approvals_backup.csv")


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

    def sync_to_airtable(self, api_key: str, base_id: str, table_id: str, backup: bool = True) -> Tuple[bool, str, int, int]:
        """Sync all approvals to Airtable and create backup.

        Args:
            api_key: Airtable API key
            base_id: Airtable base ID
            table_id: Airtable table ID
            backup: Whether to create a backup CSV file (default True)

        Returns:
            Tuple of (success, message, created_count, updated_count)
        """
        try:
            # Create backup first
            if backup:
                backup_path = BACKUP_PATH
                self._df.to_csv(backup_path, index=False)

            # Import here to avoid circular dependency
            from src.airtable import AirtableConfig, upsert_dataframe
            from datetime import datetime

            # Convert timestamp to ISO 8601 format for Airtable
            df_for_airtable = self._df.copy()

            # Replace NaN values with None (JSON-compatible)
            df_for_airtable = df_for_airtable.replace({pd.NA: None, float('nan'): None})
            df_for_airtable = df_for_airtable.where(pd.notna(df_for_airtable), None)

            if 'Approved At' in df_for_airtable.columns:
                df_for_airtable['Approved At'] = df_for_airtable['Approved At'].apply(
                    lambda x: datetime.fromtimestamp(int(x)).strftime('%Y-%m-%dT%H:%M:%S.000Z') if pd.notna(x) and x is not None else None
                )

            # Sync to Airtable
            cfg = AirtableConfig(api_key=api_key, base_id=base_id, table_id_or_name=table_id)
            created, updated = upsert_dataframe(cfg, df_for_airtable, key_field='Account')

            msg = f"Synced to Airtable: {created} created, {updated} updated"
            if backup:
                msg += f" | Backup saved to {backup_path}"

            return True, msg, created, updated

        except Exception as e:
            return False, f"Airtable sync failed: {str(e)}", 0, 0

    def upsert_and_sync(self, account: str, subtype: str, final_plan: str, extras: List[str],
                       approved_by: str = "", airtable_config: Optional[Dict] = None) -> Tuple[bool, str]:
        """Upsert approval to CSV and optionally sync to Airtable.

        Args:
            account: Account name
            subtype: Sub type
            final_plan: Final plan selection
            extras: List of extras
            approved_by: Name of approver
            airtable_config: Optional dict with keys: api_key, base_id, table_id

        Returns:
            Tuple of (success, message)
        """
        # Save to CSV first
        self.upsert(account, subtype, final_plan, extras, approved_by)

        # Try to sync to Airtable if config provided
        if airtable_config and all(k in airtable_config for k in ['api_key', 'base_id', 'table_id']):
            success, msg, _, _ = self.sync_to_airtable(
                airtable_config['api_key'],
                airtable_config['base_id'],
                airtable_config['table_id']
            )
            if success:
                return True, f"Saved to CSV and {msg}"
            else:
                return True, f"Saved to CSV but Airtable sync failed: {msg}"

        return True, "Saved to CSV (Airtable not configured)"
