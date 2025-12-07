from __future__ import annotations

from io import BytesIO
from typing import Dict, Tuple, List
import os
import pandas as pd
from src.recommendation.engine import MigrationLogic
from src.plan_definitions import get_active_plan_json
from src.utils import parse_feature_list


def _flatten_plan_json(plan_json: Dict[str, list]) -> pd.DataFrame:
    rows = []
    for plan, feats in (plan_json or {}).items():
        for f in feats or []:
            rows.append({"Plan": plan, "Feature": f})
    return pd.DataFrame(rows)


def build_updated_excel_bytes(data: Dict, approvals_df: pd.DataFrame) -> bytes:
    mapping_df: pd.DataFrame = data.get("mapping", pd.DataFrame()).copy()
    plan_json: Dict = data.get("plan_json", {}) or get_active_plan_json()

    # Merge approvals into mapping
    if not approvals_df.empty and not mapping_df.empty:
        name_col = "name" if "name" in mapping_df.columns else (
            "SalesForce_Account_NAME" if "SalesForce_Account_NAME" in mapping_df.columns else None
        )
        out_df = mapping_df.copy()
        if name_col:
            # Merge approved plan, add-ons, approver, timestamp, and comment (if provided)
            field_name = "Add-ons needed" if "Add-ons needed" in approvals_df.columns else ("Extras" if "Extras" in approvals_df.columns else None)
            base_cols = ["Account", "Final Plan", "Approved By", "Approved At"]
            opt_cols = []
            if field_name:
                opt_cols.append(field_name)
            if "Comment" in approvals_df.columns:
                opt_cols.append("Comment")
            cols = base_cols + opt_cols
            appr = approvals_df[cols].rename(
                columns={
                    "Account": name_col,
                    "Final Plan": "Final Plan",
                    (field_name or "Extras"): "Final Add-ons needed",
                    "Comment": "Approval Comment",
                }
            )
            # Prefer left merge to keep all mapping rows
            out_df = out_df.merge(appr, on=name_col, how="left")
        else:
            out_df["Final Plan"] = None
            out_df["Final Add-ons needed"] = None
    else:
        out_df = mapping_df.copy()
        if not out_df.empty:
            out_df["Final Plan"] = None
            out_df["Final Add-ons needed"] = None

    plan_df = _flatten_plan_json(plan_json)

    # Build recommendations per account (deterministic engine output, not human overrides)
    rec_df = pd.DataFrame()
    try:
        if not mapping_df.empty and isinstance(plan_json, dict) and plan_json:
            logic = MigrationLogic(None, plan_json)
            df = mapping_df.copy()
            name_col = "name" if "name" in df.columns else (
                "SalesForce_Account_NAME" if "SalesForce_Account_NAME" in df.columns else None
            )
            if name_col is None:
                # Create a fallback name for indexing
                df[name_col := "Account"] = [f"Account {i+1}" for i in range(len(df))]

            subtype_col = (
                "Sub Type" if "Sub Type" in df.columns else (
                    "Subtype" if "Subtype" in df.columns else None
                )
            )
            rows: List[dict] = []
            for _, row in df.iterrows():
                # The engine reads from keys: 'Sub Type'/'Subtype' and 'featureNames'
                account_row = row.to_dict()
                # Ensure featureNames present (engine will parse if str/JSON list)
                account_row.setdefault("featureNames", account_row.get("featureNames", []))
                rec = logic.recommend(account_row)
                # Combine add-on plan names with feature extras for a single actionable column
                try:
                    plan_names = [str(x).strip() for x in rec.get("addOnPlans", []) if str(x).strip()]
                    feature_extras = [str(x).strip() for x in rec.get("extras", []) if str(x).strip()]
                    combined_addons = [x for x in (plan_names + feature_extras) if x]
                except Exception:
                    combined_addons = [str(x) for x in rec.get("extras", [])]

                rows.append({
                    "Account": row.get(name_col),
                    "Sub Type": row.get(subtype_col) if subtype_col else None,
                    "Recommended Plan": rec.get("recommended_plan"),
                    "Add-ons needed": ", ".join(combined_addons),
                    "Applied Add-on Plans": ", ".join([str(x) for x in rec.get("addOnPlans", [])]),
                    "Extras Count": rec.get("extras_count"),
                    "Bloat Count": rec.get("bloat_score"),
                    "Paid Bloat Count": rec.get("bloat_costly_count"),
                    "Migration Confidence": rec.get("migration_confidence"),
                    "Status": rec.get("status"),
                })
            rec_df = pd.DataFrame(rows)
    except Exception:
        # Keep exporter resilient; if recommendations fail, omit the sheet
        rec_df = pd.DataFrame()

    bio = BytesIO()
    with pd.ExcelWriter(bio, engine="openpyxl") as writer:
        out_df.to_excel(writer, sheet_name="Account<>CSM<>Project (updated)", index=False)
        approvals_df.to_excel(writer, sheet_name="Approvals", index=False)
        plan_df.to_excel(writer, sheet_name="Plan <> FF", index=False)
        if not rec_df.empty:
            rec_df.to_excel(writer, sheet_name="Recommendations Plan", index=False)
        meta = pd.DataFrame(
            {
                "Key": ["Rows (mapping)", "Rows (approvals)", "Plans", "Rows (recommendations)"],
                "Value": [len(out_df), len(approvals_df), len(plan_json or {}), len(rec_df)],
            }
        )
        meta.to_excel(writer, sheet_name="Metadata", index=False)
    bio.seek(0)
    return bio.getvalue()


def save_updated_excel_file(path: str, data: Dict, approvals_df: pd.DataFrame) -> str:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    content = build_updated_excel_bytes(data, approvals_df)
    with open(path, "wb") as f:
        f.write(content)
    return path
