#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from src.json_reorder import reorder_features_json


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Reorder JSON keys into the required fixed order."
    )
    parser.add_argument(
        "input",
        nargs="?",
        help="Path to input JSON file. Reads stdin if omitted.",
    )
    parser.add_argument(
        "-o",
        "--output",
        help="Path to write output JSON. Prints to stdout if omitted.",
    )
    return parser.parse_args()


def read_json(path: str | None) -> dict:
    if path is None:
        raw = sys.stdin.read()
        return json.loads(raw)
    else:
        return json.loads(Path(path).read_text(encoding="utf-8"))


def write_json(obj: dict, path: str | None) -> None:
    # Match sample: space after colon, no spaces after commas
    text = json.dumps(obj, indent=2, separators=(",", ": "))
    if path is None:
        sys.stdout.write(text + "\n")
    else:
        Path(path).write_text(text + "\n", encoding="utf-8")


def main() -> int:
    args = parse_args()
    data = read_json(args.input)
    reordered = reorder_features_json(data)
    write_json(reordered, args.output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
