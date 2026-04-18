"""Entry point: fetch -> normalize -> dedupe -> write atlanta.json."""
from __future__ import annotations

import json
import os
import sys
from collections import Counter

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

from fetch import fetch_osm, fetch_ga_vet_board  # noqa: E402
from normalize import normalize_osm  # noqa: E402
from dedupe import dedupe  # noqa: E402

REPO_ROOT = os.path.abspath(os.path.join(HERE, "..", "..", ".."))
OUT_PATH = os.path.join(REPO_ROOT, "data", "by-metro", "atlanta.json")


def main() -> int:
    listings: list[dict] = []

    # Source 1: OSM
    osm_raw = fetch_osm()
    for r in osm_raw:
        n = normalize_osm(r)
        if n:
            listings.append(n)
    print(f"[normalize] osm listings: {len(listings)}")

    # Source 2: GA Vet Board (best-effort, skipped if stateful/blocked)
    vet_raw = fetch_ga_vet_board()
    # Vet-board normalization would go here; currently always empty
    print(f"[normalize] vet-board listings: {len(vet_raw)}")

    # Dedupe within metro
    deduped = dedupe(listings)
    print(f"[dedupe] {len(listings)} -> {len(deduped)}")

    # Ensure output dir exists
    os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)
    with open(OUT_PATH, "w", encoding="utf-8") as f:
        json.dump(deduped, f, indent=2, ensure_ascii=False)

    counts = Counter(x["category"] for x in deduped)
    print(f"[write] {OUT_PATH} count={len(deduped)} by-cat={dict(counts)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
