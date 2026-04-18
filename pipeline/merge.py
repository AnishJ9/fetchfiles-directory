"""Merge per-metro + enrichment listing JSONs into canonical outputs.

Inputs:
  data/by-metro/{metro}.json         (seed per-metro data)
  data/enrichment/*.json             (chains, osm_wider, shelters_*, etc.)
  data/enrichment/descriptions.json  (special: lookup keyed by listing id,
                                      injected into each listing's description)

Outputs:
  data/listings.json   (flat array, cross-source deduped)
  data/stats.json      (counts by metro x category + timestamp + by-source)

Dedupe rules (per SCHEMA.md):
  Two listings are the same if:
    - same normalized phone (digits only, >= 10 digits), OR
    - fuzzy name ratio >= 0.88 AND within ~100m
  On merge: union sources, prefer earliest lastSeenAt, prefer longer non-null fields.
"""
from __future__ import annotations

import json
import math
import re
from collections import defaultdict
from datetime import datetime, timezone
from difflib import SequenceMatcher
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
BY_METRO_DIR = REPO / "data" / "by-metro"
ENRICHMENT_DIR = REPO / "data" / "enrichment"
LISTINGS_OUT = REPO / "data" / "listings.json"
STATS_OUT = REPO / "data" / "stats.json"

METROS = ["atlanta", "tampa", "austin", "nashville", "asheville"]
CATEGORIES = ["veterinarian", "groomer", "boarder", "daycare", "sitter", "shelter"]

NAME_THRESHOLD = 0.88
GEO_THRESHOLD_M = 100.0


def normalize_phone(p: str | None) -> str:
    if not p:
        return ""
    digits = re.sub(r"\D", "", p)
    if len(digits) == 11 and digits.startswith("1"):
        digits = digits[1:]
    return digits if len(digits) >= 10 else ""


def normalize_name(n: str | None) -> str:
    if not n:
        return ""
    return re.sub(r"[^\w\s]", "", n).lower().strip()


def haversine_m(a: tuple[float, float], b: tuple[float, float]) -> float:
    R = 6371000.0
    lat1, lon1 = map(math.radians, a)
    lat2, lon2 = map(math.radians, b)
    dlat, dlon = lat2 - lat1, lon2 - lon1
    h = math.sin(dlat / 2) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2) ** 2
    return 2 * R * math.asin(math.sqrt(h))


def name_ratio(a: str, b: str) -> float:
    return SequenceMatcher(None, a, b).ratio()


def merge_one(existing: dict, incoming: dict) -> dict:
    out = dict(existing)
    # union sources
    out["sources"] = sorted(set(existing.get("sources", []) + incoming.get("sources", [])))
    # merge sourceIds
    out["sourceIds"] = {**(incoming.get("sourceIds") or {}), **(existing.get("sourceIds") or {})}
    # prefer earlier lastSeenAt
    a, b = existing.get("lastSeenAt"), incoming.get("lastSeenAt")
    if a and b:
        out["lastSeenAt"] = min(a, b)
    # prefer longer non-null scalar fields
    for key in ("phone", "website", "email", "address", "description"):
        if len(str(incoming.get(key) or "")) > len(str(existing.get(key) or "")):
            out[key] = incoming[key]
    return out


def find_dup(incoming: dict, pool: list[dict]) -> int | None:
    iphone = normalize_phone(incoming.get("phone"))
    iname = normalize_name(incoming.get("name"))
    ilatlng = (incoming.get("lat"), incoming.get("lng"))
    imetro = incoming.get("metro")

    for idx, cand in enumerate(pool):
        if cand.get("metro") != imetro:
            continue
        # phone match wins immediately
        cphone = normalize_phone(cand.get("phone"))
        if iphone and cphone and iphone == cphone:
            return idx
        # name+geo match
        cname = normalize_name(cand.get("name"))
        if not iname or not cname:
            continue
        if name_ratio(iname, cname) < NAME_THRESHOLD:
            continue
        try:
            dist = haversine_m(ilatlng, (cand["lat"], cand["lng"]))
        except (TypeError, ValueError):
            continue
        if dist <= GEO_THRESHOLD_M:
            return idx
    return None


DESCRIPTIONS_FILE = "descriptions.json"


def load_source_files() -> list[tuple[str, list[dict]]]:
    sources: list[tuple[str, list[dict]]] = []
    for metro in METROS:
        p = BY_METRO_DIR / f"{metro}.json"
        if p.exists():
            sources.append((f"by-metro/{metro}", json.loads(p.read_text())))
    if ENRICHMENT_DIR.exists():
        for p in sorted(ENRICHMENT_DIR.glob("*.json")):
            if p.name == DESCRIPTIONS_FILE:
                continue  # handled separately, not a listing array
            sources.append((f"enrichment/{p.stem}", json.loads(p.read_text())))
    return sources


def load_descriptions() -> dict[str, str]:
    p = ENRICHMENT_DIR / DESCRIPTIONS_FILE
    if not p.exists():
        return {}
    raw = json.loads(p.read_text())
    return {k: v["description"] for k, v in raw.items() if v.get("description")}


def main() -> None:
    pool: list[dict] = []
    by_source_count: dict[str, dict[str, int]] = defaultdict(lambda: {"raw": 0, "added": 0, "merged": 0})

    for source_label, listings in load_source_files():
        for item in listings:
            by_source_count[source_label]["raw"] += 1
            idx = find_dup(item, pool)
            if idx is None:
                pool.append(item)
                by_source_count[source_label]["added"] += 1
            else:
                pool[idx] = merge_one(pool[idx], item)
                by_source_count[source_label]["merged"] += 1

    # Inject scraped descriptions (keyed by listing id) into each listing.
    descriptions = load_descriptions()
    desc_injected = 0
    for r in pool:
        if not r.get("description") and r["id"] in descriptions:
            r["description"] = descriptions[r["id"]]
            desc_injected += 1

    by_metro_cat: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
    for r in pool:
        by_metro_cat[r["metro"]][r["category"]] += 1

    LISTINGS_OUT.write_text(json.dumps(pool, indent=2))

    stats = {
        "generatedAt": datetime.now(timezone.utc).isoformat(),
        "totalListings": len(pool),
        "byMetro": {
            metro: {
                **{cat: by_metro_cat[metro].get(cat, 0) for cat in CATEGORIES},
                "_total": sum(by_metro_cat[metro].values()),
            }
            for metro in METROS
        },
        "bySource": dict(by_source_count),
    }
    STATS_OUT.write_text(json.dumps(stats, indent=2))

    print(f"wrote {LISTINGS_OUT.name}: {len(pool)} listings")
    print(f"wrote {STATS_OUT.name}")
    print()
    print(f"{'metro':<12} " + " ".join(f"{c[:4]:>5}" for c in CATEGORIES) + "  total")
    for metro in METROS:
        row = [f"{by_metro_cat[metro].get(c, 0):>5}" for c in CATEGORIES]
        total = sum(by_metro_cat[metro].values())
        print(f"{metro:<12} " + " ".join(row) + f"  {total:>5}")
    print()
    print(f"{'source':<30} {'raw':>6} {'added':>7} {'merged':>7}")
    for source_label, counts in by_source_count.items():
        print(f"{source_label:<30} {counts['raw']:>6} {counts['added']:>7} {counts['merged']:>7}")
    print()
    print(f"descriptions injected: {desc_injected}")


if __name__ == "__main__":
    main()
