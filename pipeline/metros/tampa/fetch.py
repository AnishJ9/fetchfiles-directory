"""Fetch pet services for Tampa, FL metro.

Sources:
  1. OpenStreetMap Overpass API (30s hard timeout)
  2. Florida DBPR Board of Veterinary Medicine (5 min hard cap; best-effort)

Writes: data/by-metro/tampa.json
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import sys
import time
from datetime import datetime, timezone
from math import asin, cos, radians, sin, sqrt
from typing import Any, Iterable

import requests

# rapidfuzz is declared in requirements.txt and preferred. If it's unavailable in
# the execution environment, fall back to a stdlib difflib ratio, which produces
# a 0-100 score closely matching rapidfuzz.fuzz.ratio for short business names.
try:
    from rapidfuzz import fuzz as _rf_fuzz

    def name_ratio(a: str, b: str) -> float:
        return float(_rf_fuzz.ratio(a, b))
except ImportError:  # pragma: no cover
    from difflib import SequenceMatcher

    def name_ratio(a: str, b: str) -> float:
        return SequenceMatcher(None, a, b).ratio() * 100.0

# ---- Config ----
METRO_SLUG = "tampa"
STATE = "FL"
BBOX = (27.50, -82.90, 28.30, -82.20)  # south, west, north, east
OVERPASS_URL = "https://overpass-api.de/api/interpreter"
OVERPASS_MIRRORS = [
    "https://overpass-api.de/api/interpreter",
    "https://overpass.kumi.systems/api/interpreter",
    "https://overpass.openstreetmap.ru/api/interpreter",
]
OVERPASS_HARD_TIMEOUT = 30  # seconds per request
VET_BOARD_HARD_CAP = 300  # seconds total

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
OUTPUT_PATH = os.path.join(REPO_ROOT, "data", "by-metro", "tampa.json")

# Tampa MSA cities (Hillsborough, Pinellas, Pasco, Hernando counties + commonly included)
TAMPA_MSA_CITIES = {
    # Hillsborough
    "tampa", "brandon", "plant city", "temple terrace", "riverview", "valrico",
    "lutz", "apollo beach", "ruskin", "sun city center", "wimauma", "seffner",
    "thonotosassa", "dover", "mango", "gibsonton", "balm", "lithia",
    # Pinellas
    "st. petersburg", "st petersburg", "saint petersburg", "clearwater",
    "largo", "pinellas park", "dunedin", "tarpon springs", "palm harbor",
    "oldsmar", "safety harbor", "seminole", "st. pete beach", "st pete beach",
    "treasure island", "madeira beach", "redington beach", "belleair",
    "belleair beach", "indian rocks beach", "indian shores", "kenneth city",
    "gulfport", "south pasadena", "redington shores", "north redington beach",
    # Pasco
    "new port richey", "port richey", "hudson", "land o' lakes", "land o lakes",
    "zephyrhills", "dade city", "holiday", "wesley chapel", "odessa", "trinity",
    "spring hill", "shady hills", "san antonio", "saint leo", "st. leo",
    # Hernando
    "brooksville", "weeki wachee", "hernando beach",
}


# ---- Helpers ----

def iso_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def sha16(name: str, address: str) -> str:
    digest = hashlib.sha256(f"{name}|{address}".encode("utf-8")).hexdigest()
    return digest[:16]


def haversine_m(lat1: float, lng1: float, lat2: float, lng2: float) -> float:
    R = 6371000.0
    phi1, phi2 = radians(lat1), radians(lat2)
    dphi = radians(lat2 - lat1)
    dlam = radians(lng2 - lng1)
    a = sin(dphi / 2) ** 2 + cos(phi1) * cos(phi2) * sin(dlam / 2) ** 2
    return 2 * R * asin(sqrt(a))


_phone_re = re.compile(r"\D+")


def norm_phone(p: str | None) -> str | None:
    if not p:
        return None
    digits = _phone_re.sub("", p)
    if len(digits) == 11 and digits.startswith("1"):
        digits = digits[1:]
    if len(digits) != 10:
        return None
    return digits


def e164(p: str | None) -> str | None:
    d = norm_phone(p)
    if not d:
        return p or None
    return f"+1{d}"


def norm_name(s: str) -> str:
    s = s.lower()
    s = re.sub(r"[^a-z0-9 ]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def in_tampa_msa(city: str | None) -> bool:
    if not city:
        return False
    c = city.strip().lower()
    return c in TAMPA_MSA_CITIES


# ---- Overpass ----

def build_overpass_query(bbox: tuple[float, float, float, float]) -> str:
    s, w, n, e = bbox
    # Separate result sets per category so we can categorize by query origin
    return f"""
    [out:json][timeout:25];
    (
      node["amenity"="veterinary"]({s},{w},{n},{e});
      way["amenity"="veterinary"]({s},{w},{n},{e});
      relation["amenity"="veterinary"]({s},{w},{n},{e});
      node["shop"="pet_grooming"]({s},{w},{n},{e});
      way["shop"="pet_grooming"]({s},{w},{n},{e});
      node["craft"="pet_grooming"]({s},{w},{n},{e});
      way["craft"="pet_grooming"]({s},{w},{n},{e});
      node["amenity"="animal_boarding"]({s},{w},{n},{e});
      way["amenity"="animal_boarding"]({s},{w},{n},{e});
      relation["amenity"="animal_boarding"]({s},{w},{n},{e});
    );
    out center tags;
    """


def classify_osm(tags: dict[str, str]) -> str | None:
    if tags.get("amenity") == "veterinary":
        return "veterinarian"
    if tags.get("shop") == "pet_grooming" or tags.get("craft") == "pet_grooming":
        return "groomer"
    if tags.get("amenity") == "animal_boarding":
        # animal_boarding covers both boarding and daycare in OSM; inspect tags
        boarding = (tags.get("animal_boarding") or "").lower()
        if "daycare" in boarding or tags.get("dog_daycare") == "yes":
            return "daycare"
        return "boarder"
    return None


def fetch_overpass() -> list[dict[str, Any]]:
    q = build_overpass_query(BBOX)
    data: dict[str, Any] | None = None
    for mirror in OVERPASS_MIRRORS:
        try:
            print(f"[osm] POST {mirror} ...", flush=True)
            r = requests.post(
                mirror,
                data={"data": q},
                timeout=OVERPASS_HARD_TIMEOUT,
                headers={
                    "User-Agent": "fetchfiles-tampa/1.0 (contact: anish.joseph58@gmail.com)"
                },
            )
            if r.status_code == 429:
                print(f"[osm] {mirror} rate-limited (429)", flush=True)
                continue
            r.raise_for_status()
            data = r.json()
            break
        except Exception as exc:
            print(f"[osm] {mirror} FAILED: {exc}", flush=True)
            continue
    if data is None:
        print("[osm] all mirrors failed", flush=True)
        return []

    elements = data.get("elements", [])
    print(f"[osm] got {len(elements)} elements", flush=True)

    listings: list[dict[str, Any]] = []
    for el in elements:
        tags = el.get("tags") or {}
        name = (tags.get("name") or "").strip()
        if not name:
            continue
        cat = classify_osm(tags)
        if not cat:
            continue

        # coords
        if el.get("type") == "node":
            lat = el.get("lat")
            lng = el.get("lon")
        else:
            center = el.get("center") or {}
            lat = center.get("lat")
            lng = center.get("lon")
        if lat is None or lng is None:
            continue

        # address
        housenumber = (tags.get("addr:housenumber") or "").strip()
        street = (tags.get("addr:street") or "").strip()
        city = (tags.get("addr:city") or "").strip()
        zip_ = (tags.get("addr:postcode") or "").strip()
        st = (tags.get("addr:state") or "").strip() or STATE
        address_parts = [p for p in [f"{housenumber} {street}".strip()] if p.strip()]
        address = ", ".join(address_parts).strip(", ").strip()
        if not address and not city:
            continue  # drop listings with no usable address info

        # optional contact
        phone = tags.get("phone") or tags.get("contact:phone")
        website = tags.get("website") or tags.get("contact:website")
        email = tags.get("email") or tags.get("contact:email")

        osm_id = f"{el.get('type')}/{el.get('id')}"
        rec = {
            "id": sha16(name, address),
            "name": name,
            "category": cat,
            "address": address,
            "city": city,
            "state": st if st else STATE,
            "zip": zip_,
            "metro": METRO_SLUG,
            "lat": float(lat),
            "lng": float(lng),
            "sources": ["osm"],
            "sourceIds": {"osm": osm_id},
            "lastSeenAt": iso_now(),
            "claimed": False,
        }
        if phone:
            rec["phone"] = e164(phone) or phone
        if website:
            rec["website"] = website
        if email:
            rec["email"] = email

        listings.append(rec)

    return listings


# ---- FL Vet Board (best-effort) ----

def fetch_fl_vet_board(deadline_ts: float) -> list[dict[str, Any]]:
    """Attempt to pull FL vet license data from DBPR.

    DBPR's license search requires interactive form submission with CAPTCHAs and is
    notoriously hostile to automation. We make a best-effort request; if blocked or
    slow, we skip. Hard cap is enforced by deadline_ts.
    """
    url = "https://www.myfloridalicense.com/wl11.asp"
    headers = {
        "User-Agent": "fetchfiles-tampa/1.0 (contact: anish.joseph58@gmail.com)",
    }
    try:
        remaining = deadline_ts - time.time()
        if remaining <= 5:
            print("[vet] skipping: budget exhausted", flush=True)
            return []
        timeout = min(15.0, remaining - 1)
        print("[vet] GET license search page...", flush=True)
        r = requests.get(url, headers=headers, timeout=timeout)
        if r.status_code != 200:
            print(f"[vet] blocked/non-200: {r.status_code}", flush=True)
            return []
        # DBPR license search is a stateful ASP form with CAPTCHA. Automating it
        # reliably is out of scope for this 5-min budget. We log and skip.
        if "wl11.asp" in r.text.lower() or "license" in r.text.lower():
            print(
                "[vet] landing page reached; bulk scrape requires stateful form "
                "submission + CAPTCHA. Skipping within budget.",
                flush=True,
            )
        return []
    except Exception as exc:
        print(f"[vet] FAILED: {exc}", flush=True)
        return []


# ---- Dedupe ----

def dedupe(listings: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Dedupe by phone OR (rapidfuzz name >= 88 AND within 100m)."""
    kept: list[dict[str, Any]] = []
    phone_index: dict[str, int] = {}
    for rec in listings:
        phone = norm_phone(rec.get("phone"))
        nname = norm_name(rec["name"])
        merged_idx: int | None = None

        if phone and phone in phone_index:
            merged_idx = phone_index[phone]
        else:
            for i, other in enumerate(kept):
                if name_ratio(nname, norm_name(other["name"])) >= 88:
                    dist = haversine_m(rec["lat"], rec["lng"], other["lat"], other["lng"])
                    if dist <= 100:
                        merged_idx = i
                        break

        if merged_idx is None:
            kept.append(rec)
            if phone:
                phone_index[phone] = len(kept) - 1
        else:
            other = kept[merged_idx]
            # keep earliest lastSeenAt
            if rec["lastSeenAt"] < other["lastSeenAt"]:
                other["lastSeenAt"] = rec["lastSeenAt"]
            # union sources
            src_union = list(dict.fromkeys(list(other.get("sources", [])) + list(rec.get("sources", []))))
            other["sources"] = src_union
            # union sourceIds
            sid = dict(other.get("sourceIds") or {})
            sid.update(rec.get("sourceIds") or {})
            other["sourceIds"] = sid
            # prefer longest non-null for optional fields
            for key in ("phone", "website", "email", "address", "city", "zip"):
                a = other.get(key) or ""
                b = rec.get(key) or ""
                if len(b) > len(a):
                    other[key] = b
            # register phone if newly available
            if phone and phone not in phone_index:
                phone_index[phone] = merged_idx
    return kept


# ---- Main ----

def main() -> int:
    start = time.time()
    print(f"[main] fetchfiles tampa start {iso_now()}", flush=True)

    osm_listings = fetch_overpass()
    print(f"[main] osm listings: {len(osm_listings)}", flush=True)

    vet_deadline = start + VET_BOARD_HARD_CAP
    vet_listings = fetch_fl_vet_board(vet_deadline)
    print(f"[main] vet listings: {len(vet_listings)}", flush=True)

    all_raw = osm_listings + vet_listings
    # Filter to MSA cities where city is known; unknown-city rows kept (OSM bbox already scopes them)
    filtered: list[dict[str, Any]] = []
    for rec in all_raw:
        city = rec.get("city")
        if city and not in_tampa_msa(city):
            # drop items clearly outside MSA
            continue
        filtered.append(rec)

    deduped = dedupe(filtered)
    # Sort for stable output
    deduped.sort(key=lambda r: (r["category"], norm_name(r["name"])))

    # Count by category
    counts: dict[str, int] = {}
    for r in deduped:
        counts[r["category"]] = counts.get(r["category"], 0) + 1

    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(deduped, f, ensure_ascii=False, indent=2)

    print(f"[main] wrote {len(deduped)} listings to {OUTPUT_PATH}", flush=True)
    print(f"[main] counts: {counts}", flush=True)
    print(f"[main] elapsed: {time.time() - start:.1f}s", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
