"""Orchestrator: fetch OSM + NC Vet Board for Asheville, normalize, dedupe, write JSON."""
from __future__ import annotations

import json
import os
import sys

# Ensure this directory is on sys.path regardless of how the script is invoked.
_HERE = os.path.dirname(os.path.abspath(__file__))
if _HERE not in sys.path:
    sys.path.insert(0, _HERE)

from fetch import fetch_osm, fetch_nc_vet_board  # noqa: E402
from normalize import normalize_osm, dedupe  # noqa: E402


OUT_PATH = os.path.abspath(
    os.path.join(_HERE, "..", "..", "..", "data", "by-metro", "asheville.json")
)


def _canonicalize(listing: dict) -> dict:
    """Produce a stable field order matching other metro files."""
    ordered_keys = [
        "id", "name", "category", "subcategories",
        "address", "city", "state", "zip", "metro", "lat", "lng",
        "phone", "website", "email",
        "sources", "sourceIds", "lastSeenAt",
        "hours", "description", "tags",
        "claimed", "claimedAt",
    ]
    out = {}
    for k in ordered_keys:
        out[k] = listing.get(k, None)
    return out


def main() -> int:
    print("[run] fetching OSM…")
    raw_osm = fetch_osm()
    listings: list[dict] = []
    for rec in raw_osm:
        norm = normalize_osm(rec)
        if norm:
            listings.append(norm)
    print(f"[run] osm normalized: {len(listings)}")

    print("[run] probing NC vet board…")
    vet_raw = fetch_nc_vet_board()
    # Currently NC VMB is not scraped programmatically; list will be empty.
    for rec in vet_raw:
        # If a future iteration returns parseable records, normalize them here.
        pass
    print(f"[run] vet-board records: {len(vet_raw)}")

    before = len(listings)
    listings = dedupe(listings)
    print(f"[run] deduped: {before} -> {len(listings)}")

    # Stable sort by category, then name
    listings.sort(key=lambda x: (x.get("category", ""), (x.get("name") or "").lower()))

    # Canonicalize field order
    listings = [_canonicalize(x) for x in listings]

    os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)
    with open(OUT_PATH, "w", encoding="utf-8") as f:
        json.dump(listings, f, indent=2, ensure_ascii=False)

    # Print counts by category for the report.
    counts: dict[str, int] = {}
    for x in listings:
        c = x.get("category", "unknown")
        counts[c] = counts.get(c, 0) + 1
    print(f"[run] wrote {len(listings)} listings to {OUT_PATH}")
    print(f"[run] by category: {counts}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
