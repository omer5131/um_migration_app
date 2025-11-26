from __future__ import annotations

from io import BytesIO
from typing import Dict, Tuple
import os
import pandas as pd


def _flatten_plan_json(plan_json: Dict[str, list]) -> pd.DataFrame:
    rows = []
    for plan, feats in (plan_json or {}).items():
        for f in feats or []:
            rows.append({"Plan": plan, "Feature": f})
    return pd.DataFrame(rows)


def build_updated_excel_bytes(data: Dict, approvals_df: pd.DataFrame) -> bytes:
    mapping_df: pd.DataFrame = data.get("mapping", pd.DataFrame()).copy()
    plan_json: Dict = data.get("plan_json", {})

    # Merge approvals into mapping
    if not approvals_df.empty and not mapping_df.empty:
        name_col = "name" if "name" in mapping_df.columns else (
            "SalesForce_Account_NAME" if "SalesForce_Account_NAME" in mapping_df.columns else None
        )
        out_df = mapping_df.copy()
        if name_col:
            appr = approvals_df[["Account", "Final Plan", "Extras", "Approved By", "Approved At"]].rename(
                columns={
                    "Account": name_col,
                    "Final Plan": "Final Plan",
                    "Extras": "Final Extras",
                }
            )
            # Prefer left merge to keep all mapping rows
            out_df = out_df.merge(appr, on=name_col, how="left")
        else:
            out_df["Final Plan"] = None
            out_df["Final Extras"] = None
    else:
        out_df = mapping_df.copy()
        if not out_df.empty:
            out_df["Final Plan"] = None
            out_df["Final Extras"] = None

    plan_df = _flatten_plan_json(plan_json)

    bio = BytesIO()
    with pd.ExcelWriter(bio, engine="openpyxl") as writer:
        out_df.to_excel(writer, sheet_name="Account<>CSM<>Project (updated)", index=False)
        approvals_df.to_excel(writer, sheet_name="Approvals", index=False)
        plan_df.to_excel(writer, sheet_name="Plan <> FF", index=False)
        meta = pd.DataFrame(
            {
                "Key": ["Rows (mapping)", "Rows (approvals)", "Plans"],
                "Value": [len(out_df), len(approvals_df), len(plan_json or {})],
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

