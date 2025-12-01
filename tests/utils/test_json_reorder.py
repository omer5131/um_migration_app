from __future__ import annotations

from src.json_reorder import reorder_features_json


def test_reorder_matches_required_order_and_preserves_values():
    input_data = {
        "plan": "Insurer Advanced",
        "extras": [
            "hideHistoricalRiskIndicators",
            "hideMapTimelineOnVesselProfile",
            "savedQueriesNotifications",
            "wetCargoData",
        ],
        "gaFeatures": [
            "darkFleetVOI",
            "grayFleetVOI",
            "nasAccidentsActivity",
            "nasAddVesselsToVoi",
            "newAdvancedSearch",
            "newNasSideBarDesign",
            "warRiskArea",
        ],
        "irrelevantFeatures": [],
        "bloat_features": [
            "hideBorderSecurityRisk",
            "hideRuleEngine",
            "hideSafetyRisk",
            "userAlreadyLoggedInWarning",
        ],
        "bloat_costly": [],
    }

    out = reorder_features_json(input_data)

    assert list(out.keys()) == [
        "plan",
        "extras",
        "bloat_features",
        "bloat_costly",
        "gaFeatures",
        "irrelevantFeatures",
    ]

    # Preserve values exactly
    assert out["plan"] == input_data["plan"]
    assert out["extras"] == input_data["extras"]
    assert out["bloat_features"] == input_data["bloat_features"]
    assert out["bloat_costly"] == input_data["bloat_costly"]
    assert out["gaFeatures"] == input_data["gaFeatures"]
    assert out["irrelevantFeatures"] == input_data["irrelevantFeatures"]


def test_missing_keys_are_added_as_empty_arrays():
    # 'plan' intentionally missing too, per spec
    input_data = {
        "extras": ["a"],
        "gaFeatures": ["b"],
    }

    out = reorder_features_json(input_data)

    assert list(out.keys()) == [
        "plan",
        "extras",
        "bloat_features",
        "bloat_costly",
        "gaFeatures",
        "irrelevantFeatures",
    ]

    # Existing values preserved
    assert out["extras"] == ["a"]
    assert out["gaFeatures"] == ["b"]

    # Missing keys added as empty arrays
    assert out["plan"] == []
    assert out["bloat_features"] == []
    assert out["bloat_costly"] == []
    assert out["irrelevantFeatures"] == []
