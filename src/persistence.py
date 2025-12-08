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

    Schema (CSV):
    - Account, Sub Type, Final Plan, Add-ons needed (comma-separated), Approved By, Approved At (epoch seconds)
    - Plus optional analytics fields saved as columns when provided:
      plan, Add-ons needed, Gained by plan (not currently in project), Bloat-costly
    """

    def __init__(self, path: str = DEFAULT_PATH):
        self.path = path
        os.makedirs(os.path.dirname(self.path), exist_ok=True)
        self._df = self._load()

    def _load(self) -> pd.DataFrame:
        if os.path.exists(self.path):
            try:
                df = pd.read_csv(self.path)
                # Drop legacy extras columns unconditionally; use only 'Add-ons needed'
                legacy_cols = ['Extras', 'add-ons to compatability', 'add-ons to compatibility']
                for legacy_col in legacy_cols:
                    if legacy_col in df.columns:
                        df = df.drop(columns=[legacy_col])
                return df
            except Exception:
                return pd.DataFrame(
                    columns=[
                        "Account",
                        "Sub Type",
                        "Final Plan",
                        "Add-ons needed",
                        "Approved By",
                        "Approved At",
                    ]
                )
        return pd.DataFrame(
            columns=[
                "Account",
                "Sub Type",
                "Final Plan",
                "Add-ons needed",
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
        # Parse add-ons needed as list
        extras = [x.strip() for x in str(row.get("Add-ons needed", "")).split(",") if x.strip()]
        row["Add-ons needed"] = extras
        return row

    def upsert(self, account: str, subtype: str, final_plan: str, extras: List[str], approved_by: str = "",
               details: Optional[Dict] = None) -> None:
        ts = int(time.time())
        # Merge Applied Add-on Plans into Add-ons needed at store-level as a safety net
        def _to_list(val) -> List[str]:
            try:
                import pandas as _pd
            except Exception:
                _pd = None
            if val is None:
                return []
            if isinstance(val, list):
                return [str(x).strip() for x in val if str(x).strip()]
            if _pd is not None and isinstance(val, getattr(_pd, 'Series', ())) :
                return [str(x).strip() for x in val.dropna().tolist() if str(x).strip()]
            if isinstance(val, str):
                s = val.strip()
                if not s:
                    return []
                # Try JSON first, then comma-separated
                try:
                    import json as _json
                    parsed = _json.loads(s)
                    if isinstance(parsed, list):
                        return [str(x).strip() for x in parsed if str(x).strip()]
                except Exception:
                    pass
                return [x.strip() for x in s.split(',') if x.strip()]
            return [str(val).strip()] if str(val).strip() else []

        extras_list = _to_list(extras)
        applied_from_details: List[str] = []
        if isinstance(details, dict):
            for k in ('Applied Add-on Plans', 'addOnPlans'):
                if k in details:
                    applied_from_details = _to_list(details.get(k))
                    break
        merged_extras: List[str] = []
        seen = set()
        for x in list(extras_list) + list(applied_from_details):
            key = str(x).strip().lower()
            if key and key not in seen:
                seen.add(key)
                merged_extras.append(str(x).strip())
        extras_str = ", ".join(merged_extras)
        idx = None
        if not self._df.empty:
            matches = self._df.index[self._df["Account"] == account].tolist()
            if matches:
                idx = matches[0]

        row = {
            "Account": account,
            "Sub Type": subtype,
            "Final Plan": final_plan,
            "Add-ons needed": extras_str,
            "Approved By": approved_by,
            "Approved At": ts,
        }

        # Merge optional analytics fields, converting lists/dicts/Series to safe CSV scalars
        if details and isinstance(details, dict):
            for k, v in details.items():
                # Prefer canonical field from arguments; skip overriding via details
                if str(k).strip() == 'Add-ons needed':
                    continue
                try:
                    import pandas as _pd  # local import to avoid test-time hard dep
                except Exception:
                    _pd = None

                # Normalize pandas objects
                if _pd is not None and isinstance(v, getattr(_pd, 'Series', ())) :
                    try:
                        # Prefer list of strings joined; drop NaNs
                        vals = [str(x).strip() for x in v.dropna().tolist() if str(x).strip()]
                        row[k] = ", ".join(vals) if vals else None
                    except Exception:
                        row[k] = str(v)
                    continue

                # Standard containers
                if isinstance(v, (list, dict)):
                    try:
                        row[k] = json.dumps(v, ensure_ascii=False)
                    except Exception:
                        row[k] = str(v)
                    continue

                # Scalars and everything else
                try:
                    row[k] = v if (v is None or isinstance(v, (str, int, float, bool))) else str(v)
                except Exception:
                    row[k] = None

        if idx is None:
            self._df = pd.concat([self._df, pd.DataFrame([row])], ignore_index=True)
        else:
            for k, v in row.items():
                self._df.at[idx, k] = v
        self._persist()

    def all(self) -> pd.DataFrame:
        return self._df.copy()

    def delete(self, account: str) -> bool:
        """Delete an approval row by Account. Returns True if a row was removed."""
        if self._df.empty:
            return False
        before = len(self._df)
        self._df = self._df[self._df["Account"] != account].reset_index(drop=True)
        if len(self._df) != before:
            self._persist()
            return True
        return False

    def delete_many(self, accounts: List[str]) -> int:
        """Delete multiple approval rows by Account. Returns number of rows removed."""
        if not accounts:
            return 0
        to_remove = set(str(a) for a in accounts)
        before = len(self._df)
        self._df = self._df[~self._df["Account"].astype(str).isin(to_remove)].reset_index(drop=True)
        removed = before - len(self._df)
        if removed > 0:
            self._persist()
        return removed

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
            from src.airtable import AirtableConfig, upsert_dataframe, fetch_records, ensure_field_exists
            from datetime import datetime

            # Convert timestamp to ISO 8601 format for Airtable
            df_for_airtable = self._df.copy()

            # Clean Account field - remove newlines and extra whitespace
            if 'Account' in df_for_airtable.columns:
                def _clean_account(x):
                    """Normalize Account values to clean strings or None without ambiguous truth checks."""
                    if x is None:
                        return None
                    # Strings: strip whitespace/newlines; coerce empty to None
                    if isinstance(x, str):
                        s = x.replace('\n', '').replace('\r', '').strip()
                        return s if s else None
                    # Numeric scalars: handle NaN and convert to string
                    if isinstance(x, (int, float)):
                        try:
                            return None if pd.isna(x) else str(x)
                        except Exception:
                            return str(x)
                    # Fallback: stringify other types (avoid boolean context)
                    try:
                        return str(x)
                    except Exception:
                        return None
                df_for_airtable['Account'] = df_for_airtable['Account'].apply(_clean_account)

            # Normalize column names: drop legacy extras columns and keep only 'Add-ons needed'
            for legacy_col in ['Extras', 'add-ons to compatability', 'add-ons to compatibility']:
                if legacy_col in df_for_airtable.columns:
                    df_for_airtable.drop(columns=[legacy_col], inplace=True)
            # Optional other legacy variants for 'Gained by plan' are left as-is unless present; we do not merge here
            # Normalize plan field casing to match Airtable schema: prefer 'Final Plan'
            if 'Final plan' in df_for_airtable.columns and 'Final Plan' not in df_for_airtable.columns:
                df_for_airtable.rename(columns={'Final plan': 'Final Plan'}, inplace=True)
            if 'Recommended Plan' in df_for_airtable.columns and 'Final Plan' not in df_for_airtable.columns:
                df_for_airtable.rename(columns={'Recommended Plan': 'Final Plan'}, inplace=True)

            # Replace NaN values with None (JSON-compatible)
            df_for_airtable = df_for_airtable.replace({pd.NA: None, float('nan'): None})
            df_for_airtable = df_for_airtable.where(pd.notna(df_for_airtable), None)

            if 'Approved At' in df_for_airtable.columns:
                def _ts_to_iso(x):
                    if x is None:
                        return None
                    try:
                        # Handle NaN for numeric types
                        if pd.isna(x):
                            return None
                    except Exception:
                        # Non-numeric types shouldn't be considered NA here
                        pass
                    try:
                        return datetime.fromtimestamp(int(x)).strftime('%Y-%m-%dT%H:%M:%S.000Z')
                    except Exception:
                        return None
                df_for_airtable['Approved At'] = df_for_airtable['Approved At'].apply(_ts_to_iso)
            # Final alignment with CLI cleaner: clean cells and trim whitespace/newlines on all string cells
            def _clean_cell(v):
                import pandas as _pd
                if isinstance(v, getattr(_pd, 'Series', ())) or str(type(v)).endswith(".Series'>"):
                    try:
                        return _clean_cell(v.iloc[0])
                    except Exception:
                        return None
                if isinstance(v, dict):
                    try:
                        return json.dumps(v, ensure_ascii=False)
                    except Exception:
                        return str(v)
                if isinstance(v, list):
                    return ', '.join(map(str, v))
                if isinstance(v, str):
                    return v.replace('\n', '').replace('\r', '').strip()
                try:
                    if pd.isna(v):
                        return None
                except Exception:
                    pass
                return v

            try:
                df_for_airtable = df_for_airtable.applymap(_clean_cell)
            except Exception:
                pass

            # Basic known fields commonly present; we will actually try to sync all columns,
            # but keep this for context and potential future filtering if needed.
            basic_fields = {
                'Account', 'Sub Type', 'Final Plan', 'Add-ons needed', 'Gained by plan (not currently in project)',
                'Approved By', 'Approved At', 'Comment', 'Under trial'
            }

            # Detect existing field names in Airtable by sampling records
            existing_airtable_fields = set()
            try:
                cfg = AirtableConfig(api_key=api_key, base_id=base_id, table_id_or_name=table_id)
                sample = fetch_records(cfg)
                for r in sample[:50]:
                    fields = r.get('fields') or {}
                    existing_airtable_fields.update([str(k) for k in fields.keys()])
            except Exception:
                pass

            # Ensure all DF columns exist in Airtable (best effort) so we can fully sync
            try:
                cfg = AirtableConfig(api_key=api_key, base_id=base_id, table_id_or_name=table_id)
                desired_fields = [c for c in df_for_airtable.columns if c]
                for fname in desired_fields:
                    if fname not in existing_airtable_fields and fname != 'Account':
                        # Default to long text; special columns can be adjusted manually in Airtable
                        ensure_field_exists(cfg, fname, field_type='multilineText')
                # Refresh existing fields snapshot after attempted creation
                try:
                    sample = fetch_records(cfg)
                    existing_airtable_fields = set()
                    for r in sample[:50]:
                        fields = r.get('fields') or {}
                        existing_airtable_fields.update([str(k) for k in fields.keys()])
                except Exception:
                    pass
            except Exception:
                pass

            # Prefer syncing all DF columns; if Airtable still lacks some, we will fallback below on error
            sync_cols = [c for c in df_for_airtable.columns]
            df_for_airtable = df_for_airtable[sync_cols]

            # Sync to Airtable (attempt with current fields; on schema error, retry without unknowns)
            cfg = AirtableConfig(api_key=api_key, base_id=base_id, table_id_or_name=table_id)
            try:
                created, updated = upsert_dataframe(cfg, df_for_airtable, key_field='Account', typecast=False)
            except Exception as e:
                # On failure, attempt to create all missing fields, then retry full sync
                try:
                    desired_fields = [c for c in df_for_airtable.columns if c]
                    for fname in desired_fields:
                        if fname != 'Account' and fname not in existing_airtable_fields:
                            try:
                                ensure_field_exists(cfg, fname, field_type='multilineText')
                            except Exception:
                                pass
                    # Refresh existing fields snapshot
                    try:
                        sample = fetch_records(cfg)
                        existing_airtable_fields = set()
                        for r in sample[:50]:
                            fields = r.get('fields') or {}
                            existing_airtable_fields.update([str(k) for k in fields.keys()])
                    except Exception:
                        pass
                    # Retry full set; if still fails, propagate detailed error
                    created, updated = upsert_dataframe(cfg, df_for_airtable, key_field='Account', typecast=False)
                except Exception:
                    # As a last resort, propagate the original error to surface schema issues
                    raise

            msg = f"Synced to Airtable: {created} created, {updated} updated"
            if backup:
                msg += f" | Backup saved to {backup_path}"

            return True, msg, created, updated

        except Exception as e:
            return False, f"Airtable sync failed: {str(e)}", 0, 0

    def upsert_and_sync(self, account: str, subtype: str, final_plan: str, extras: List[str],
                       approved_by: str = "", airtable_config: Optional[Dict] = None,
                       details: Optional[Dict] = None) -> Tuple[bool, str]:
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
        # Save to CSV first (with details if provided)
        self.upsert(account, subtype, final_plan, extras, approved_by, details=details)

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
