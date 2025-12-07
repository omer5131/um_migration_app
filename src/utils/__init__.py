"""Utility helpers package.

Exports common helpers used across the project so callers can
`from src.utils import parse_feature_list, clean_feature_name`.
"""

from __future__ import annotations

import ast
from typing import Any, List

import pandas as pd


def parse_feature_list(feature_str: Any) -> List[str]:
    """Parse a stringified list or CSV into a Python list of strings.

    Accepts values like "['featA', 'featB']", actual lists, NaN, or
    comma-separated strings. Returns an empty list on invalid input.
    """
    try:
        if pd.isna(feature_str):
            return []
        if isinstance(feature_str, list):
            return feature_str
        return ast.literal_eval(str(feature_str))
    except (ValueError, SyntaxError):
        if isinstance(feature_str, str) and "," in feature_str:
            return [x.strip() for x in feature_str.split(",")]
        return []


def clean_feature_name(name: Any) -> str:
    """Normalize a feature name for comparison (strip whitespace)."""
    return str(name).strip()


__all__ = ["parse_feature_list", "clean_feature_name"]
