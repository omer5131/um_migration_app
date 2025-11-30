import pytest

from src.recommendation import MigrationLogic, compute_bloat_stats


PLAN_JSON = {
    # Families are inferred from plan names (via SUBTYPE_KEYWORD_MAP in config)
    "Shipowners Core": [
        "advancedSearchOwners",  # canonical form
        "weatherLayer",
    ],
    "Bunkering Basic": [
        "portStateControl",  # canonical form
        "simpleFeature",
    ],
    "Bunkering Pro": [
        "portStateControl",
        "proOnlyFeature",
        # Include costly feature to test red line rejections in selection vs override
    ],
}


def test_normalization_soft_matching():
    logic = MigrationLogic(plan_json=PLAN_JSON)
    row = {
        "Sub Type": "Bunkering",
        # 'Port Control' should map to 'portStateControl' and be covered
        "featureNames": ["Port Control", "simpleFeature"],
    }
    rec = logic.recommend(row)
    assert rec["status"] == "Success"
    assert rec["recommended_plan"].startswith("Bunkering")
    # No extras because both map into plan features
    assert rec["extras_count"] == 0


def test_paid_bloat_rejection():
    logic = MigrationLogic(plan_json={
        "Bunkering With Costly": ["portStateControl", "uboData"],  # uboData is costly
        "Bunkering Safe": ["portStateControl"],
    })
    row = {"Sub Type": "Bunkering", "featureNames": ["Port Control"]}
    rec = logic.recommend(row)
    # The plan containing paid bloat not used by user must be rejected
    assert rec["recommended_plan"] == "Bunkering Safe"
    for c in rec["all_candidates"]:
        assert "uboData" not in c.get("bloat_features", [])


def test_ranking_prefers_fewer_extras():
    logic = MigrationLogic(plan_json={
        "Bunkering Plan A": ["portStateControl", "x", "y", "z"],
        "Bunkering Plan B": ["portStateControl"],
    })
    # User uses only portStateControl -> Plan B should win (0 extras vs extras for Plan A)
    row = {"Sub Type": "Bunkering", "featureNames": ["Port Control"]}
    rec = logic.recommend(row)
    assert rec["recommended_plan"] == "Bunkering Plan B"


def test_subtype_filtering():
    logic = MigrationLogic(plan_json=PLAN_JSON)
    # For Shipowners subtype, should not consider Bunkering plans
    row = {"Sub Type": "Shipowner", "featureNames": ["Advanced Search"]}
    rec = logic.recommend(row)
    assert rec["status"] in ("Success", "NO_MATCHING_PLANS")
    if rec["status"] == "Success":
        assert rec["recommended_plan"].startswith("Shipowners")


def test_compute_bloat_stats_and_override_red_line():
    logic = MigrationLogic(plan_json={
        "Bunkering Base": ["portStateControl"],
    })
    user = ["Port Control"]
    # If override adds a paid bloat feature as extra, it should still be fine (paid extras are allowed),
    # but if it causes bloat (plan+extras minus user), and it's paid, it must be rejected.
    # Here extras only include user features -> no bloat.
    stats = compute_bloat_stats(logic.plan_definitions, "Bunkering Base", ["Port Control"], user)
    assert stats["bloat_score"] == 0

    # Now attempt override that triggers paid bloat due to non-user costly feature in effective bundle
    res = logic.apply_human_override("Bunkering Base", ["uboData"], user)
    assert res["status"] == "REJECTED_RED_LINE"
