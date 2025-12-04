from __future__ import annotations

import pandas as pd

from src.utils.ga_features import ga_visibility_for_row, ga_visibility_for_dataframe
from src.config import GA_FEATURES


def test_ga_visibility_for_row_basic():
    row = {
        "name": "Acme Co",
        "featureNames": [GA_FEATURES[0], GA_FEATURES[1], "nonGaFeat"],
    }
    vis = ga_visibility_for_row(row)
    assert vis["name"] == "Acme Co"
    assert GA_FEATURES[0] in vis["ga_present"]
    assert GA_FEATURES[1] in vis["ga_present"]
    assert "nonGaFeat" not in vis["ga_present"]
    assert vis["ga_total"] == len(GA_FEATURES)
    assert vis["ga_present_count"] == 2
    assert vis["ga_missing_count"] == len(GA_FEATURES) - 2


def test_ga_visibility_for_dataframe_multiple():
    df = pd.DataFrame(
        [
            {"name": "A", "featureNames": [GA_FEATURES[0]]},
            {"name": "B", "featureNames": ["x", "y"]},
        ]
    )
    all_vis = ga_visibility_for_dataframe(df)
    assert len(all_vis) == 2
    assert all_vis[0]["name"] == "A"
    assert GA_FEATURES[0] in all_vis[0]["ga_present"]
    assert all_vis[1]["name"] == "B"
    assert all_vis[1]["ga_present_count"] == 0

