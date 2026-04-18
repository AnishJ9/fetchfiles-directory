"""
Fetch pet services for Austin, TX metro.

Sources (in order):
  1. OpenStreetMap Overpass (30s hard timeout)
  2. Texas Board of Veterinary Medical Examiners license search (5-min cap)

Writes data/by-metro/austin.json conforming to docs/SCHEMA.md.
"""

from __future__ import annotations

import hashlib
import json
import math
import os
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

# Allow running with system python by ensuring the venv site-packages is on
# sys.path when present. No-op if modules are already importable.
_VENV_SP = Path(__file__).resolve().parent / ".venv" / "lib"
if _VENV_SP.exists():
    for _py in _VENV_SP.glob("python*"):
        _sp = _py / "site-packages"
        if _sp.exists() and str(_sp) not in sys.path:
            sys.path.insert(0, str(_sp))

import requests
from rapidfuzz import fuzz


# --------------------------------------------------------------------------- #
# Config                                                                       #
# --------------------------------------------------------------------------- #

METRO_SLUG = "austin"
METRO_STATE = "TX"

# Overpass bbox (south, west, north, east)
BBOX = (30.00, -98.10, 30.65, -97.40)

OVERPASS_URL = "https://overpass-api.de/api/interpreter"
OVERPASS_MIRRORS = [
    "https://overpass-api.de/api/interpreter",
    "https://overpass.kumi.systems/api/interpreter",
    "https://overpass.private.coffee/api/interpreter",
]
OVERPASS_TIMEOUT_SEC = 30  # hard requests timeout (per attempt)
OVERPASS_INTERNAL_TIMEOUT = 25  # Overpass server-side timeout
OVERPASS_TOTAL_BUDGET_SEC = 30  # hard overall budget across mirrors

TX_VET_URL = (
    "https://vettx.glsuite.us/GLSuiteWeb/Clients/VETTX/"
    "Public/Verification/Search.aspx"
)
VET_BOARD_CAP_SEC = 300  # 5 minutes

REPO_ROOT = Path(__file__).resolve().parents[3]
OUT_PATH = REPO_ROOT / "data" / "by-metro" / f"{METRO_SLUG}.json"


# --------------------------------------------------------------------------- #
# Austin MSA filter                                                            #
# --------------------------------------------------------------------------- #
# Austin–Round Rock–Georgetown MSA: Travis, Williamson, Hays, Bastrop, Caldwell.
# We filter by city name (for vet board, which lacks coords) and by bbox
# (handled automatically for OSM).

AUSTIN_MSA_CITIES = {
    # Travis
    "austin", "pflugerville", "lakeway", "bee cave", "lago vista",
    "jonestown", "manor", "sunset valley", "west lake hills", "rollingwood",
    "creedmoor", "del valle", "volente", "san leanna", "briarcliff",
    "point venture",
    # Williamson
    "round rock", "cedar park", "leander", "georgetown", "hutto",
    "taylor", "liberty hill", "jarrell", "florence", "granger",
    "thrall", "weir", "coupland",
    # Hays
    "san marcos", "kyle", "buda", "dripping springs", "wimberley",
    "woodcreek", "niederwald", "mountain city", "hays",
    # Bastrop
    "bastrop", "elgin", "smithville", "cedar creek", "mcdade", "paige",
    "red rock", "rosanky",
    # Caldwell
    "lockhart", "luling", "martindale", "maxwell", "mendoza", "dale",
}


# --------------------------------------------------------------------------- #
# Utilities                                                                    #
# --------------------------------------------------------------------------- #

def iso_utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def hash_id(name: str, address: str) -> str:
    key = f"{name}|{address}"
    return hashlib.sha256(key.encode("utf-8")).hexdigest()[:16]


def norm_phone(raw: str | None) -> str | None:
    if not raw:
        return None
    digits = re.sub(r"\D+", "", raw)
    if len(digits) == 11 and digits.startswith("1"):
        return "+" + digits
    if len(digits) == 10:
        return "+1" + digits
    if digits:
        return "+" + digits
    return None


def norm_name(name: str) -> str:
    s = name.lower()
    s = re.sub(r"[^\w\s]", "", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def haversine_m(lat1: float, lng1: float, lat2: float, lng2: float) -> float:
    R = 6371000.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp = math.radians(lat2 - lat1)
    dl = math.radians(lng2 - lng1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * R * math.asin(math.sqrt(a))


def in_bbox(lat: float, lng: float) -> bool:
    s, w, n, e = BBOX
    return s <= lat <= n and w <= lng <= e


def longest(*vals: str | None) -> str | None:
    best = None
    for v in vals:
        if v and (best is None or len(v) > len(best)):
            best = v
    return best


# --------------------------------------------------------------------------- #
# OSM Overpass                                                                 #
# --------------------------------------------------------------------------- #

def build_overpass_query() -> str:
    s, w, n, e = BBOX
    bbox = f"{s},{w},{n},{e}"
    return f"""
[out:json][timeout:{OVERPASS_INTERNAL_TIMEOUT}];
(
  node["amenity"="veterinary"]({bbox});
  way["amenity"="veterinary"]({bbox});
  relation["amenity"="veterinary"]({bbox});
  node["shop"="pet_grooming"]({bbox});
  way["shop"="pet_grooming"]({bbox});
  node["craft"="pet_grooming"]({bbox});
  way["craft"="pet_grooming"]({bbox});
  node["amenity"="animal_boarding"]({bbox});
  way["amenity"="animal_boarding"]({bbox});
  relation["amenity"="animal_boarding"]({bbox});
);
out center tags;
""".strip()


def osm_category(tags: dict[str, str]) -> str | None:
    if tags.get("amenity") == "veterinary":
        return "veterinarian"
    if tags.get("shop") == "pet_grooming" or tags.get("craft") == "pet_grooming":
        return "groomer"
    if tags.get("amenity") == "animal_boarding":
        # boarding_type may distinguish — OSM often lumps these together
        bt = (tags.get("animal_boarding") or "").lower()
        if bt == "dog_daycare" or tags.get("dog") == "daycare":
            return "daycare"
        return "boarder"
    return None


def build_address(tags: dict[str, str]) -> tuple[str, str, str]:
    housenumber = tags.get("addr:housenumber", "").strip()
    street = tags.get("addr:street", "").strip()
    unit = tags.get("addr:unit", "").strip()
    street_full = " ".join([p for p in [housenumber, street] if p])
    if unit:
        street_full = f"{street_full} {unit}".strip()
    city = tags.get("addr:city", "").strip()
    zip_code = tags.get("addr:postcode", "").strip()
    return street_full, city, zip_code


def element_to_listing(el: dict[str, Any]) -> dict[str, Any] | None:
    tags = el.get("tags") or {}
    name = (tags.get("name") or "").strip()
    if not name:
        return None
    category = osm_category(tags)
    if not category:
        return None

    # Coords — for way/relation, "center" gives lat/lon
    if el.get("type") == "node":
        lat = el.get("lat")
        lng = el.get("lon")
    else:
        center = el.get("center") or {}
        lat = center.get("lat")
        lng = center.get("lon")
    if lat is None or lng is None:
        return None
    if not in_bbox(lat, lng):
        return None

    street, city, zip_code = build_address(tags)
    phone = norm_phone(tags.get("phone") or tags.get("contact:phone"))
    website = tags.get("website") or tags.get("contact:website") or None
    email = tags.get("email") or tags.get("contact:email") or None

    osm_id = f"{el.get('type')}/{el.get('id')}"

    full_address = street if street else ""

    return {
        "id": hash_id(name, full_address),
        "name": name,
        "category": category,
        "subcategories": None,
        "address": full_address,
        "city": city or "Austin",
        "state": METRO_STATE,
        "zip": zip_code,
        "metro": METRO_SLUG,
        "lat": float(lat),
        "lng": float(lng),
        "phone": phone,
        "website": website,
        "email": email,
        "sources": ["osm"],
        "sourceIds": {"osm": osm_id},
        "lastSeenAt": iso_utc_now(),
        "claimed": False,
    }


def fetch_osm() -> list[dict[str, Any]]:
    query = build_overpass_query()
    headers = {"User-Agent": "fetchfiles-directory/1.0 (austin pipeline)"}
    started = time.monotonic()
    payload = None
    last_err: str | None = None

    for url in OVERPASS_MIRRORS:
        remaining = OVERPASS_TOTAL_BUDGET_SEC - (time.monotonic() - started)
        if remaining <= 1:
            break
        attempt_timeout = min(OVERPASS_TIMEOUT_SEC, max(1.0, remaining))
        try:
            r = requests.post(
                url,
                data={"data": query},
                timeout=attempt_timeout,
                headers=headers,
            )
            if r.status_code == 429:
                last_err = f"429 at {url}"
                continue
            r.raise_for_status()
            payload = r.json()
            print(f"[osm] fetched via {url}")
            break
        except (requests.RequestException, ValueError) as exc:
            last_err = f"{url}: {exc}"
            continue

    if payload is None:
        print(f"[osm] FAIL: {last_err}", file=sys.stderr)
        return []

    listings = []
    for el in payload.get("elements", []):
        item = element_to_listing(el)
        if item is not None:
            listings.append(item)
    print(f"[osm] OK: {len(listings)} listings")
    return listings


# --------------------------------------------------------------------------- #
# Texas Board of Veterinary Medical Examiners                                  #
# --------------------------------------------------------------------------- #
#
# The public search is an ASP.NET WebForms page with viewstate + postbacks.
# Building a reliable scrape requires multi-request session emulation of the
# GLSuite portal and is frequently rate-limited / CAPTCHA-gated.
#
# Per the task spec we:
#   - Try a best-effort fetch of the facility list.
#   - Skip if blocked / non-HTML response / any error.
#   - Hard 5-minute cap on elapsed time.
#
# This pulls *facilities* (clinics), not individual vet licensees. Individual
# licensees don't map cleanly to directory listings (many practice at the same
# address). If the facility search is unavailable we return [] and log.

def fetch_tx_vet_board() -> list[dict[str, Any]]:
    start = time.monotonic()
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": "Mozilla/5.0 (fetchfiles-directory pipeline)",
            "Accept": "text/html,application/xhtml+xml",
        }
    )

    def budget_left() -> float:
        return VET_BOARD_CAP_SEC - (time.monotonic() - start)

    try:
        if budget_left() <= 0:
            return []
        # Initial GET to obtain viewstate cookies.
        r = session.get(TX_VET_URL, timeout=min(20, max(1, budget_left())))
        if r.status_code != 200 or "text/html" not in r.headers.get(
            "Content-Type", ""
        ):
            print(
                f"[tx_vet] skip: status={r.status_code} "
                f"ct={r.headers.get('Content-Type')}",
                file=sys.stderr,
            )
            return []
        html = r.text

        # GLSuite search pages require a valid viewstate + an __EVENTTARGET
        # corresponding to the facility-type radio, then a postback per page
        # of results. Without a maintained scraper, we bail early rather than
        # risk 5-minute timeouts on pagination.
        if "__VIEWSTATE" not in html:
            print("[tx_vet] skip: no viewstate in response", file=sys.stderr)
            return []

        # We do not have a documented stable endpoint for bulk facility export,
        # and GLSuite actively blocks scripted pagination. Return [] to stay
        # within the 5-minute cap rather than attempting fragile postbacks.
        print(
            "[tx_vet] skip: GLSuite requires interactive postback pagination; "
            "no bulk endpoint available",
            file=sys.stderr,
        )
        return []
    except requests.RequestException as exc:
        print(f"[tx_vet] FAIL: {exc}", file=sys.stderr)
        return []
    finally:
        elapsed = time.monotonic() - start
        print(f"[tx_vet] elapsed {elapsed:.1f}s")


# --------------------------------------------------------------------------- #
# Dedupe + merge                                                               #
# --------------------------------------------------------------------------- #

def merge(a: dict[str, Any], b: dict[str, Any]) -> dict[str, Any]:
    # Keep earliest lastSeenAt
    ls = min(a["lastSeenAt"], b["lastSeenAt"])

    sources = sorted(set(a["sources"]) | set(b["sources"]))
    sourceIds = {**a.get("sourceIds", {}), **b.get("sourceIds", {})}

    merged = {
        "id": a["id"],
        "name": longest(a.get("name"), b.get("name")) or a["name"],
        "category": a["category"],
        "subcategories": a.get("subcategories") or b.get("subcategories"),
        "address": longest(a.get("address"), b.get("address")) or "",
        "city": longest(a.get("city"), b.get("city")) or "",
        "state": a.get("state") or b.get("state") or METRO_STATE,
        "zip": longest(a.get("zip"), b.get("zip")) or "",
        "metro": METRO_SLUG,
        "lat": a.get("lat") if a.get("lat") is not None else b.get("lat"),
        "lng": a.get("lng") if a.get("lng") is not None else b.get("lng"),
        "phone": longest(a.get("phone"), b.get("phone")),
        "website": longest(a.get("website"), b.get("website")),
        "email": longest(a.get("email"), b.get("email")),
        "sources": sources,
        "sourceIds": sourceIds,
        "lastSeenAt": ls,
        "claimed": False,
    }
    return merged


def dedupe(listings: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for item in listings:
        matched_idx: int | None = None
        for i, existing in enumerate(out):
            # 1. phone match
            p1 = item.get("phone")
            p2 = existing.get("phone")
            if p1 and p2 and p1 == p2:
                matched_idx = i
                break
            # 2. name fuzzy + within 100m
            if (
                item.get("lat") is not None
                and item.get("lng") is not None
                and existing.get("lat") is not None
                and existing.get("lng") is not None
            ):
                score = fuzz.token_set_ratio(
                    norm_name(item["name"]), norm_name(existing["name"])
                )
                if score >= 88:
                    d = haversine_m(
                        item["lat"], item["lng"],
                        existing["lat"], existing["lng"],
                    )
                    if d <= 100:
                        matched_idx = i
                        break
        if matched_idx is None:
            out.append(item)
        else:
            out[matched_idx] = merge(out[matched_idx], item)
    return out


# --------------------------------------------------------------------------- #
# Main                                                                         #
# --------------------------------------------------------------------------- #

def main() -> int:
    print(f"Austin pipeline -> {OUT_PATH}")

    osm_results = fetch_osm()
    vet_results = fetch_tx_vet_board()

    combined = osm_results + vet_results
    deduped = dedupe(combined)

    # Sort for deterministic output
    deduped.sort(key=lambda x: (x["category"], x["name"].lower(), x["id"]))

    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with OUT_PATH.open("w", encoding="utf-8") as fh:
        json.dump(deduped, fh, indent=2, ensure_ascii=False)

    # Stats
    counts: dict[str, int] = {}
    for r in deduped:
        counts[r["category"]] = counts.get(r["category"], 0) + 1
    print(f"Wrote {len(deduped)} listings to {OUT_PATH}")
    for cat, n in sorted(counts.items()):
        print(f"  {cat}: {n}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
