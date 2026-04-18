"""Merge per-metro listing JSONs into the canonical outputs.

Inputs:
  data/by-metro/{metro}.json  (one array per metro)

Outputs:
  data/listings.json   (flat array across all metros)
  data/stats.json      (counts by metro x category + timestamp)
"""
from __future__ import annotations

import json
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
BY_METRO_DIR = REPO / "data" / "by-metro"
LISTINGS_OUT = REPO / "data" / "listings.json"
STATS_OUT = REPO / "data" / "stats.json"

METROS = ["atlanta", "tampa", "austin", "nashville", "asheville"]
CATEGORIES = ["veterinarian", "groomer", "boarder", "daycare", "sitter"]


def main() -> None:
    all_listings: list[dict] = []
    by_metro_cat: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
    seen_ids: set[str] = set()

    for metro in METROS:
        path = BY_METRO_DIR / f"{metro}.json"
        if not path.exists():
            print(f"[warn] missing {path}")
            continue
        listings = json.loads(path.read_text())
        for item in listings:
            lid = item.get("id")
            if lid in seen_ids:
                continue
            seen_ids.add(lid)
            all_listings.append(item)
            by_metro_cat[item["metro"]][item["category"]] += 1

    LISTINGS_OUT.write_text(json.dumps(all_listings, indent=2))

    stats = {
        "generatedAt": datetime.now(timezone.utc).isoformat(),
        "totalListings": len(all_listings),
        "byMetro": {
            metro: {
                **{cat: by_metro_cat[metro].get(cat, 0) for cat in CATEGORIES},
                "_total": sum(by_metro_cat[metro].values()),
            }
            for metro in METROS
        },
    }
    STATS_OUT.write_text(json.dumps(stats, indent=2))

    print(f"wrote {LISTINGS_OUT.name}: {len(all_listings)} listings")
    print(f"wrote {STATS_OUT.name}")
    print()
    print(f"{'metro':<12} " + " ".join(f"{c[:4]:>5}" for c in CATEGORIES) + "  total")
    for metro in METROS:
        row = [f"{by_metro_cat[metro].get(c, 0):>5}" for c in CATEGORIES]
        total = sum(by_metro_cat[metro].values())
        print(f"{metro:<12} " + " ".join(row) + f"  {total:>5}")


if __name__ == "__main__":
    main()
