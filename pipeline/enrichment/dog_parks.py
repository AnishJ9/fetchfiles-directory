"""OSM Overpass sweep for dog park listings across the 5 launch metros.

Tag queried:
  - leisure=dog_park

Dog parks commonly lack formal names, phone, and website. Unnamed parks
are kept and labeled "Off-leash area".

Outputs:
  data/enrichment/dog_parks.json -- flat array, schema per docs/SCHEMA.md
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
OUT_PATH = REPO / "data" / "enrichment" / "dog_parks.json"

OVERPASS_MIRRORS = [
    "https://overpass-api.de/api/interpreter",
    "https://overpass.kumi.systems/api/interpreter",
]

OSM_TIMEOUT = 30             # hard HTTP cap
OSM_SERVER_TIMEOUT = 25      # server-side Overpass [timeout:]

# metro -> (south, west, north, east, default_state)
METROS: dict[str, tuple[float, float, float, float, str]] = {
    "atlanta":   (33.40, -84.85, 34.15, -83.90, "GA"),
    "tampa":     (27.50, -82.90, 28.30, -82.20, "FL"),
    "austin":    (30.00, -98.10, 30.65, -97.40, "TX"),
    "nashville": (35.80, -87.10, 36.45, -86.40, "TN"),
    "asheville": (35.35, -82.85, 35.80, -82.35, "NC"),
}

SELECTOR = '["leisure"="dog_park"]'
FALLBACK_NAME = "Off-leash area"


# ---- Overpass ---------------------------------------------------------------

def _build_query(bbox: tuple[float, float, float, float]) -> str:
    s, w, n, e = bbox
    b = f"({s},{w},{n},{e})"
    parts: list[str] = []
    for kind in ("node", "way", "relation"):
        parts.append(f"  {kind}{SELECTOR}{b};")
    body = "\n".join(parts)
    return f"[out:json][timeout:{OSM_SERVER_TIMEOUT}];\n(\n{body}\n);\nout center tags;"


def _post_overpass(query: str) -> dict | None:
    """POST to Overpass with mirror fallback on 429/5xx or connection error."""
    last_err = None
    for url in OVERPASS_MIRRORS:
        try:
            r = requests.post(
                url,
                data={"data": query},
                timeout=OSM_TIMEOUT,
                headers={
                    "User-Agent": "fetchfiles-directory/1.0 (dog_parks pass)",
                },
            )
            if r.status_code == 200:
                return r.json()
            print(f"[dog_parks] HTTP {r.status_code} from {url}")
            last_err = f"HTTP {r.status_code}"
            if r.status_code not in (429, 502, 503, 504):
                time.sleep(1)
                continue
            time.sleep(2)
        except requests.exceptions.Timeout as e:
            print(f"[dog_parks] timeout {url}: {e}")
            last_err = f"timeout: {e}"
            time.sleep(1)
        except Exception as e:
            print(f"[dog_parks] failed {url}: {e}")
            last_err = str(e)
            time.sleep(1)
    print(f"[dog_parks] all mirrors failed: {last_err}")
    return None


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


def _make_id(name: str, key_suffix: str) -> str:
    key = f"{name}|{key_suffix}".lower()
    return hashlib.sha256(key.encode("utf-8")).hexdigest()[:16]


def normalize(el: dict, metro: str, default_state: str) -> tuple[dict | None, bool]:
    """Return (listing, had_no_name). had_no_name is True when we substituted
    FALLBACK_NAME for a missing name tag."""
    tags = el.get("tags") or {}
    raw_name = (tags.get("name") or "").strip()

    if el.get("type") == "node":
        lat = el.get("lat")
        lng = el.get("lon")
    else:
        c = el.get("center") or {}
        lat = c.get("lat")
        lng = c.get("lon")

    address = _compose_address(tags)

    # Accept unnamed parks if we have at least coords or an address.
    if not raw_name and not address and (lat is None or lng is None):
        return None, False

    had_no_name = not raw_name
    name = raw_name or FALLBACK_NAME

    # id key: address if present, else coords string
    if address:
        key_suffix = address
    elif lat is not None and lng is not None:
        key_suffix = f"{float(lat):.6f},{float(lng):.6f}"
    else:
        # should not hit given the guard above, but be safe
        key_suffix = f"{el.get('type')}/{el.get('id')}"

    # address fallback: the coord string is used so the listing row isn't blank
    address_out = address
    if not address_out and lat is not None and lng is not None:
        address_out = f"{float(lat):.6f},{float(lng):.6f}"

    city = (tags.get("addr:city") or "").strip()
    zip_ = (tags.get("addr:postcode") or "").strip()
    state = (tags.get("addr:state") or default_state).strip() or default_state

    listing: dict = {
        "id": _make_id(name, key_suffix),
        "name": name,
        "category": "dog_park",
        "address": address_out,
        "city": city,
        "state": state,
        "zip": zip_,
        "metro": metro,
        "lat": float(lat) if lat is not None else None,
        "lng": float(lng) if lng is not None else None,
        "sources": ["osm"],
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

    return listing, had_no_name


# ---- driver -----------------------------------------------------------------

def run() -> None:
    all_listings: list[dict] = []
    seen_ids: set[str] = set()

    counts: dict[str, int] = {m: 0 for m in METROS}
    no_name_counts: dict[str, int] = {m: 0 for m in METROS}

    for metro, info in METROS.items():
        s, w, n, e, default_state = info
        bbox = (s, w, n, e)
        print(f"[dog_parks] {metro} bbox={bbox}")
        q = _build_query(bbox)
        data = _post_overpass(q)
        if not data:
            print(f"[dog_parks] no data for {metro}")
            continue
        elements = data.get("elements", []) or []
        print(f"[dog_parks] {metro} raw elements: {len(elements)}")

        for el in elements:
            listing, had_no_name = normalize(el, metro, default_state)
            if not listing:
                continue
            if listing["id"] in seen_ids:
                continue
            seen_ids.add(listing["id"])
            all_listings.append(listing)
            counts[metro] += 1
            if had_no_name:
                no_name_counts[metro] += 1

        # polite pause between metros
        time.sleep(2)

    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUT_PATH.write_text(json.dumps(all_listings, indent=2))

    # ---- report -----------------------------------------------------------
    print()
    print(f"wrote {OUT_PATH} -- {len(all_listings)} dog park listings")
    print()
    print(f"{'metro':<11}{'parks':>8}{'no_name':>10}")
    for metro in METROS:
        print(f"{metro:<11}{counts[metro]:>8}{no_name_counts[metro]:>10}")


if __name__ == "__main__":
    run()
