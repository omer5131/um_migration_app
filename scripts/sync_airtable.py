#!/usr/bin/env python3
"""
Sync Airtable table/view into a persistent JSON cache under data/.

Usage:
  python scripts/sync_airtable.py [--refresh] [--ttl 0]

Environment variables (see src/config.AIRTABLE):
  AIRTABLE_API_KEY, AIRTABLE_BASE_ID, AIRTABLE_TABLE, AIRTABLE_VIEW, AIRTABLE_CACHE_PATH
"""
from __future__ import annotations

import argparse
import sys
from typing import Optional

from src.config import AIRTABLE as CFG
from src.airtable import AirtableConfig, load_cached_or_fetch, save_cache


def main(argv: Optional[list[str]] = None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--refresh", action="store_true", help="Ignore cache and fetch from Airtable")
    ap.add_argument("--ttl", type=int, default=None, nargs="?", help="Refresh if cache older than TTL seconds (default: never)")
    args = ap.parse_args(argv)

    api_key = CFG.get("API_KEY", "")
    base_id = CFG.get("BASE_ID", "")
    table = CFG.get("TABLE", "")
    view = CFG.get("VIEW") or None
    cache_path = CFG.get("CACHE_PATH")

    if not (api_key and base_id and table and cache_path):
        print("Missing Airtable config. Set AIRTABLE_API_KEY, AIRTABLE_BASE_ID, AIRTABLE_TABLE, AIRTABLE_CACHE_PATH", file=sys.stderr)
        return 2

    cfg = AirtableConfig(api_key=api_key, base_id=base_id, table_id_or_name=table, view=view)
    ttl = 0 if args.refresh else args.ttl
    df = load_cached_or_fetch(cfg, cache_path, ttl_seconds=ttl)
    # Ensure cache exists even when ttl=None and it loaded from cache
    save_cache(df, cache_path)
    print(f"Synced {len(df)} records to {cache_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

