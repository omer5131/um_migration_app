from __future__ import annotations

from typing import Any, Mapping


REQUIRED_ORDER = [
    "plan",
    "extras",
    "bloat_features",
    "bloat_costly",
    "gaFeatures",
    "irrelevantFeatures",
]


def reorder_features_json(data: Mapping[str, Any]) -> dict:
    """
    Reorganize a JSON-like mapping into the strict, predefined key order.

    Required order:
    1. plan
    2. extras
    3. bloat_features
    4. bloat_costly
    5. gaFeatures
    6. irrelevantFeatures

    Behavior:
    - Preserve all values exactly as-is.
    - Do not rename or modify feature names.
    - If a key is missing, include it with an empty array ([]).
    - Output structure must follow the exact key order above.
    """

    result: dict[str, Any] = {}

    for key in REQUIRED_ORDER:
        if key in data:
            result[key] = data[key]
        else:
            # Per requirement: if a key is missing, include with empty array
            result[key] = []

    return result

