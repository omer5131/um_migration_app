import ast
import pandas as pd
import re

def parse_feature_list(feature_str):
    """
    Parses "['featA', 'featB']" into a python list.
    """
    try:
        if pd.isna(feature_str):
            return []
        if isinstance(feature_str, list):
            return feature_str
        return ast.literal_eval(str(feature_str))
    except (ValueError, SyntaxError):
        # Fallback for comma separated strings
        if isinstance(feature_str, str) and ',' in feature_str:
            return [x.strip() for x in feature_str.split(',')]
        return []

def clean_feature_name(name):
    """Normalizes feature names for comparison (strips whitespace)."""
    return str(name).strip()
