import os
import sqlite3
from typing import Dict, Tuple

import pandas as pd


def connect_db(path: str) -> sqlite3.Connection:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    return sqlite3.connect(path)


def flatten_plan_json(plan_json: Dict[str, list]) -> pd.DataFrame:
    rows = []
    for plan, feats in (plan_json or {}).items():
        for f in feats or []:
            rows.append({"Plan": str(plan), "Feature": str(f)})
    return pd.DataFrame(rows, columns=["Plan", "Feature"]) if rows else pd.DataFrame(columns=["Plan", "Feature"]) 


def to_sql_replace(conn: sqlite3.Connection, table: str, df: pd.DataFrame) -> None:
    # Replace table contents entirely
    df.to_sql(table, conn, if_exists="replace", index=False)


def init_or_update_db(db_path: str, mapping_df: pd.DataFrame, plan_json: Dict[str, list], approvals_df: pd.DataFrame) -> str:
    """Create or update a SQLite DB with mapping, plan_ff (flattened), approvals."""
    with connect_db(db_path) as conn:
        # mapping
        if mapping_df is not None and not mapping_df.empty:
            to_sql_replace(conn, "mapping", mapping_df)
        else:
            to_sql_replace(conn, "mapping", pd.DataFrame())

        # plan_ff
        plan_df = flatten_plan_json(plan_json or {})
        to_sql_replace(conn, "plan_ff", plan_df)

        # approvals
        if approvals_df is not None and not approvals_df.empty:
            to_sql_replace(conn, "approvals", approvals_df)
        else:
            to_sql_replace(conn, "approvals", pd.DataFrame(columns=["Account","Sub Type","Final Plan","Extras","Approved By","Approved At"]))
    return db_path


def load_from_db(db_path: str) -> Dict[str, pd.DataFrame]:
    with connect_db(db_path) as conn:
        data: Dict[str, pd.DataFrame] = {}
        try:
            data["mapping"] = pd.read_sql("SELECT * FROM mapping", conn)
        except Exception:
            data["mapping"] = pd.DataFrame()
        try:
            plan_ff = pd.read_sql("SELECT * FROM plan_ff", conn)
        except Exception:
            plan_ff = pd.DataFrame(columns=["Plan","Feature"]) 
        # also provide plan_json for engine
        data["plan_matrix"] = plan_ff.rename(columns={"Plan":"PLAN","Feature":"FF"})
        plan_json: Dict[str, list] = {}
        if not plan_ff.empty:
            for plan, sub in plan_ff.groupby("Plan"):
                feats = [str(x).strip() for x in sub["Feature"].tolist() if str(x).strip()]
                plan_json[str(plan)] = sorted(list(dict.fromkeys(feats)))
        data["plan_json"] = plan_json
        return data


def write_approvals_to_db(db_path: str, approvals_df: pd.DataFrame) -> None:
    with connect_db(db_path) as conn:
        to_sql_replace(conn, "approvals", approvals_df)

