"""Second-pass OSM Overpass sweep for pet-business listings.

Captures tags the first pass missed (healthcare=veterinary, office=veterinary,
craft=dog_groomer, craft=animal_groomer, amenity=pet_boarding, amenity=pet_care,
and shop=pet with service:* flags).

Outputs:
  data/enrichment/osm_wider.json  — flat array, schema per docs/SCHEMA.md
"""
from __future__ import annotations

import hashlib
import json
import re
import time
from datetime import datetime, timezone
from pathlib import Path

import requests


REPO = Path(__file__).resolve().parent.parent.parent
OUT_PATH = REPO / "data" / "enrichment" / "osm_wider.json"

OVERPASS_MIRRORS = [
    "https://overpass-api.de/api/interpreter",
    "https://overpass.kumi.systems/api/interpreter",
]

OSM_TIMEOUT = 30             # hard HTTP cap
OSM_SERVER_TIMEOUT = 25      # server-side Overpass [timeout:]

# metro name -> (south, west, north, east, default_state)
METROS: dict[str, tuple[float, float, float, float, str]] = {
    "atlanta":   (33.40, -84.85, 34.15, -83.90, "GA"),
    "tampa":     (27.50, -82.90, 28.30, -82.20, "FL"),
    "austin":    (30.00, -98.10, 30.65, -97.40, "TX"),
    "nashville": (35.80, -87.10, 36.45, -86.40, "TN"),
    "asheville": (35.35, -82.85, 35.80, -82.35, "NC"),
}


# ---- Overpass ---------------------------------------------------------------

def _build_query(bbox: tuple[float, float, float, float]) -> str:
    """Union the wider tag set in a single Overpass query."""
    s, w, n, e = bbox
    b = f"({s},{w},{n},{e})"
    # Each selector emitted as node+way+relation.
    selectors = [
        # veterinarian variants
        '["healthcare"="veterinary"]',
        '["office"="veterinary"]',
        '["amenity"="veterinary"]',  # include for dedupe safety
        # groomer variants
        '["craft"="dog_groomer"]',
        '["craft"="animal_groomer"]',
        '["craft"="pet_grooming"]',
        '["shop"="pet_grooming"]',   # include for dedupe safety
        # boarding / daycare / pet care
        '["amenity"="animal_boarding"]',  # include for dedupe safety
        '["amenity"="pet_boarding"]',
        '["amenity"="pet_care"]',
        # shop=pet with any relevant service:* flag
        '["shop"="pet"]["service:dog_boarding"="yes"]',
        '["shop"="pet"]["service:dog_daycare"="yes"]',
        '["shop"="pet"]["service:grooming"="yes"]',
        '["shop"="pet"]["service:pet_boarding"="yes"]',
    ]
    parts = []
    for sel in selectors:
        for kind in ("node", "way", "relation"):
            parts.append(f"  {kind}{sel}{b};")
    body = "\n".join(parts)
    return f"[out:json][timeout:{OSM_SERVER_TIMEOUT}];\n(\n{body}\n);\nout center tags;"


def _post_overpass(query: str) -> dict | None:
    """POST to Overpass with mirror fallback on 429/504 or connection error."""
    last_err = None
    for url in OVERPASS_MIRRORS:
        try:
            r = requests.post(
                url,
                data={"data": query},
                timeout=OSM_TIMEOUT,
                headers={
                    "User-Agent": "fetchfiles-directory/1.0 (osm_wider second pass)",
                },
            )
            if r.status_code == 200:
                return r.json()
            print(f"[osm_wider] HTTP {r.status_code} from {url}")
            last_err = f"HTTP {r.status_code}"
            if r.status_code not in (429, 502, 503, 504):
                # non-retriable: still try next mirror but don't sleep long
                time.sleep(1)
                continue
            time.sleep(2)
        except requests.exceptions.Timeout as e:
            print(f"[osm_wider] timeout {url}: {e}")
            last_err = f"timeout: {e}"
            time.sleep(1)
        except Exception as e:
            print(f"[osm_wider] failed {url}: {e}")
            last_err = str(e)
            time.sleep(1)
    print(f"[osm_wider] all mirrors failed: {last_err}")
    return None


# ---- classification ---------------------------------------------------------

def classify(tags: dict) -> tuple[str | None, str]:
    """Return (category, matched_tag_label) for an OSM element.

    Precedence:
      veterinarian > boarder > daycare > groomer (so that a vet that also
      grooms lands as veterinarian, matching user intent for "best guess").
    """
    # Veterinarian
    if tags.get("amenity") == "veterinary":
        return "veterinarian", "amenity=veterinary"
    if tags.get("healthcare") == "veterinary":
        return "veterinarian", "healthcare=veterinary"
    if tags.get("office") == "veterinary":
        return "veterinarian", "office=veterinary"

    # Boarding
    if tags.get("amenity") == "animal_boarding":
        return "boarder", "amenity=animal_boarding"
    if tags.get("amenity") == "pet_boarding":
        return "boarder", "amenity=pet_boarding"

    # shop=pet with service flags — examine in order daycare > boarding > grooming
    if tags.get("shop") == "pet":
        if tags.get("service:dog_daycare") == "yes":
            return "daycare", "shop=pet+service:dog_daycare"
        if tags.get("service:dog_boarding") == "yes":
            return "boarder", "shop=pet+service:dog_boarding"
        if tags.get("service:pet_boarding") == "yes":
            return "boarder", "shop=pet+service:pet_boarding"
        if tags.get("service:grooming") == "yes":
            return "groomer", "shop=pet+service:grooming"
        # shop=pet alone with no service flag is not a listing for us
        # (still fall through in case another tag matched)

    # amenity=pet_care — ambiguous; treat as daycare (most common real use)
    if tags.get("amenity") == "pet_care":
        return "daycare", "amenity=pet_care"

    # Groomer
    if tags.get("craft") == "dog_groomer":
        return "groomer", "craft=dog_groomer"
    if tags.get("craft") == "animal_groomer":
        return "groomer", "craft=animal_groomer"
    if tags.get("craft") == "pet_grooming":
        return "groomer", "craft=pet_grooming"
    if tags.get("shop") == "pet_grooming":
        return "groomer", "shop=pet_grooming"

    return None, ""


# ---- normalization ----------------------------------------------------------

def _compose_address(tags: dict) -> str:
    housenum = (tags.get("addr:housenumber") or "").strip()
    street = (tags.get("addr:street") or "").strip()
    unit = (tags.get("addr:unit") or "").strip()
    parts: list[str] = []
    if housenum and street:
        parts.append(f"{housenum} {street}")
    elif street:
        parts.append(street)
    if unit:
        parts.append(f"Unit {unit}")
    return ", ".join(parts).strip()


def _normalize_phone(raw: str | None) -> str | None:
    if not raw:
        return None
    digits = re.sub(r"\D+", "", raw)
    if not digits:
        return None
    if len(digits) == 10:
        return f"+1{digits}"
    if len(digits) == 11 and digits.startswith("1"):
        return f"+{digits}"
    return raw.strip() or None


def _phone_from_tags(tags: dict) -> str | None:
    for k in ("phone", "contact:phone"):
        v = tags.get(k)
        if v:
            return _normalize_phone(v)
    return None


def _website(tags: dict) -> str | None:
    for k in ("website", "contact:website", "url"):
        v = tags.get(k)
        if v:
            return v.strip()
    return None


def _email(tags: dict) -> str | None:
    for k in ("email", "contact:email"):
        v = tags.get(k)
        if v:
            return v.strip()
    return None


def _hours(tags: dict) -> dict | None:
    v = tags.get("opening_hours")
    if not v:
        return None
    return {"raw": v}


def _make_id(name: str, address: str) -> str:
    key = f"{name}|{address}".lower()
    return hashlib.sha256(key.encode("utf-8")).hexdigest()[:16]


def _subcategories(tags: dict) -> list[str]:
    subs: list[str] = []
    if tags.get("mobile") == "yes":
        subs.append("mobile")
    if tags.get("pets") == "cats" or tags.get("cat") == "yes":
        subs.append("cat-only")
    return subs


def normalize(el: dict, metro: str, default_state: str) -> dict | None:
    tags = el.get("tags") or {}
    name = (tags.get("name") or "").strip()
    if not name:
        return None

    category, _ = classify(tags)
    if not category:
        return None

    if el.get("type") == "node":
        lat = el.get("lat")
        lng = el.get("lon")
    else:
        c = el.get("center") or {}
        lat = c.get("lat")
        lng = c.get("lon")

    address = _compose_address(tags)
    # must have either address or coords
    if not address and (lat is None or lng is None):
        return None

    city = (tags.get("addr:city") or "").strip()
    zip_ = (tags.get("addr:postcode") or "").strip()
    state = (tags.get("addr:state") or default_state).strip() or default_state

    listing: dict = {
        "id": _make_id(name, address or f"{lat},{lng}"),
        "name": name,
        "category": category,
        "address": address,
        "city": city,
        "state": state,
        "zip": zip_,
        "metro": metro,
        "lat": float(lat) if lat is not None else None,
        "lng": float(lng) if lng is not None else None,
        "sources": ["osm_wider"],
        "sourceIds": {"osm": f"{el.get('type')}/{el.get('id')}"},
        "lastSeenAt": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "claimed": False,
    }

    phone = _phone_from_tags(tags)
    if phone:
        listing["phone"] = phone
    website = _website(tags)
    if website:
        listing["website"] = website
    email = _email(tags)
    if email:
        listing["email"] = email
    hours = _hours(tags)
    if hours:
        listing["hours"] = hours
    subs = _subcategories(tags)
    if subs:
        listing["subcategories"] = subs

    return listing


# ---- driver -----------------------------------------------------------------

def run() -> None:
    all_listings: list[dict] = []
    seen_ids: set[str] = set()

    # counts: metro -> category -> count
    counts: dict[str, dict[str, int]] = {m: {} for m in METROS}
    # per-tag counts (after normalization) across all metros
    tag_hits: dict[str, int] = {}
    tag_queried = [
        "healthcare=veterinary",
        "office=veterinary",
        "amenity=veterinary",
        "craft=dog_groomer",
        "craft=animal_groomer",
        "craft=pet_grooming",
        "shop=pet_grooming",
        "amenity=animal_boarding",
        "amenity=pet_boarding",
        "amenity=pet_care",
        "shop=pet+service:dog_daycare",
        "shop=pet+service:dog_boarding",
        "shop=pet+service:pet_boarding",
        "shop=pet+service:grooming",
    ]
    for t in tag_queried:
        tag_hits[t] = 0

    for metro, info in METROS.items():
        s, w, n, e, default_state = info
        bbox = (s, w, n, e)
        print(f"[osm_wider] {metro} bbox={bbox}")
        q = _build_query(bbox)
        data = _post_overpass(q)
        if not data:
            print(f"[osm_wider] no data for {metro}")
            continue
        elements = data.get("elements", []) or []
        print(f"[osm_wider] {metro} raw elements: {len(elements)}")

        for el in elements:
            tags = el.get("tags") or {}
            _, tag_label = classify(tags)
            listing = normalize(el, metro, default_state)
            if not listing:
                continue
            if listing["id"] in seen_ids:
                continue
            seen_ids.add(listing["id"])
            all_listings.append(listing)
            counts[metro][listing["category"]] = counts[metro].get(listing["category"], 0) + 1
            if tag_label in tag_hits:
                tag_hits[tag_label] += 1

        # polite pause between metros
        time.sleep(2)

    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUT_PATH.write_text(json.dumps(all_listings, indent=2))

    # ---- report -----------------------------------------------------------
    print()
    print(f"wrote {OUT_PATH} -- {len(all_listings)} listings")
    print()
    cats = ["veterinarian", "groomer", "boarder", "daycare", "sitter"]
    header = f"{'metro':<11}" + "".join(f"{c[:5]:>7}" for c in cats) + f"{'total':>8}"
    print(header)
    for metro in METROS:
        row = [counts[metro].get(c, 0) for c in cats]
        total = sum(row)
        print(f"{metro:<11}" + "".join(f"{v:>7}" for v in row) + f"{total:>8}")

    print()
    print("per-tag normalized hits:")
    for t in tag_queried:
        print(f"  {t:<40} {tag_hits[t]}")
    zero = [t for t in tag_queried if tag_hits[t] == 0]
    if zero:
        print(f"tags with zero results: {', '.join(zero)}")


if __name__ == "__main__":
    run()
