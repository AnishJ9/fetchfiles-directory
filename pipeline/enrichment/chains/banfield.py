"""Banfield Pet Hospital fetcher.

Strategy: enumerate per-state store-page URLs from sitemap.xml, fetch each
hospital's page, parse JSON-LD for name + address + lat/lng.
"""
from __future__ import annotations

import re
import time

from . import common
from ._http import get
from ._parse import extract_ld_json, find_local_business, place_to_parts
from ._states import two_letter

CHAIN = "banfield"
SITEMAP = "https://www.banfield.com/sitemap.xml"
TARGET_STATES_BF = ["ga", "fl", "tx", "tn", "nc"]

# Metro city slugs in store URLs (lower, dashed).
# Source: US Census / metro-area components for each of our 5 metros.
METRO_CITY_SLUGS = {
    "atlanta": {  # GA
        "atlanta", "sandy-springs", "roswell", "alpharetta", "marietta", "decatur",
        "duluth", "smyrna", "dunwoody", "johns-creek", "peachtree-corners",
        "kennesaw", "lawrenceville", "snellville", "brookhaven", "cumming",
        "college-park", "douglasville", "acworth", "powder-springs", "mcdonough",
        "stockbridge", "fayetteville", "morrow", "austell", "tucker",
        "stone-mountain", "union-city", "east-point", "lilburn", "norcross",
        "doraville", "buford", "suwanee", "mableton", "hiram", "canton",
        "woodstock", "milton",
    },
    "tampa": {  # FL
        "tampa", "st-petersburg", "saint-petersburg", "clearwater",
        "brandon", "largo", "riverview", "wesley-chapel", "temple-terrace",
        "town-n-country", "town-n-country", "lutz", "carrollwood", "ruskin",
        "apollo-beach", "land-o-lakes", "plant-city", "valrico", "dover",
        "pinellas-park", "seminole", "palm-harbor", "dunedin", "tarpon-springs",
        "oldsmar", "safety-harbor", "sun-city-center", "gibsonton", "new-tampa",
    },
    "austin": {  # TX
        "austin", "round-rock", "cedar-park", "pflugerville", "georgetown",
        "leander", "kyle", "buda", "bee-cave", "lakeway", "lake-travis",
        "manor", "san-marcos", "dripping-springs",
    },
    "nashville": {  # TN
        "nashville", "franklin", "brentwood", "murfreesboro", "hendersonville",
        "mt-juliet", "mount-juliet", "gallatin", "smyrna", "la-vergne", "lavergne",
        "cool-springs", "antioch", "goodlettsville", "nolensville", "spring-hill",
        "donelson", "green-hills", "bellevue",
    },
    "asheville": {  # NC
        "asheville", "arden", "fletcher", "hendersonville", "black-mountain",
        "weaverville", "candler", "mars-hill", "swannanoa",
    },
}


def _fetch_sitemap() -> list[str]:
    txt = get(SITEMAP, timeout=30)
    if not txt:
        return []
    pat = re.compile(
        r"https://www\.banfield\.com/locations/veterinarians/([a-z]{2})/([a-z0-9-]+)/([a-z0-9-]+)(?:[\"'<])"
    )
    out = set()
    for m in pat.finditer(txt):
        st, city, slug = m.group(1), m.group(2), m.group(3)
        if slug in {"available-services", "service-pricing"}:
            continue
        if st not in TARGET_STATES_BF:
            continue
        # filter by metro city slug
        metro = _metro_for_slug(st, city)
        if not metro:
            continue
        out.add(f"https://www.banfield.com/locations/veterinarians/{st}/{city}/{slug}")
    return sorted(out)


def _metro_for_slug(state: str, city_slug: str) -> str | None:
    state = state.lower()
    if state == "ga":
        if city_slug in METRO_CITY_SLUGS["atlanta"]:
            return "atlanta"
    elif state == "fl":
        if city_slug in METRO_CITY_SLUGS["tampa"]:
            return "tampa"
    elif state == "tx":
        if city_slug in METRO_CITY_SLUGS["austin"]:
            return "austin"
    elif state == "tn":
        if city_slug in METRO_CITY_SLUGS["nashville"]:
            return "nashville"
    elif state == "nc":
        if city_slug in METRO_CITY_SLUGS["asheville"]:
            return "asheville"
    return None


def fetch() -> list[dict]:
    urls = _fetch_sitemap()
    print(f"[{CHAIN}] {len(urls)} candidate store pages in target metros")
    common.cache_write(CHAIN, {"urls": urls})
    listings: list[dict] = []
    seen_ids: set[str] = set()
    for i, url in enumerate(urls):
        html = get(url, timeout=15)
        if not html:
            continue
        objs = extract_ld_json(html)
        biz = find_local_business(objs)
        parts = place_to_parts(biz) if biz else None
        if not parts or parts["lat"] is None or parts["lng"] is None:
            continue
        metro = common.metro_for(parts["lat"], parts["lng"])
        if not metro:
            continue
        # store ID from URL path (last slug)
        store_id = url.rstrip("/").rsplit("/", 1)[-1]
        state = two_letter(parts["state"]) or parts["state"]
        # Normalize name: JSON-LD often just has "City" — prefix "Banfield Pet Hospital"
        bf_name = parts["name"].strip()
        if "banfield" not in bf_name.lower():
            bf_name = f"Banfield Pet Hospital {bf_name}".strip()
        listing = common.build_listing(
            name=bf_name,
            category="veterinarian",
            address=parts["street"],
            city=parts["city"],
            state=state,
            zip_=parts["zip"],
            lat=parts["lat"],
            lng=parts["lng"],
            metro=metro,
            chain=CHAIN,
            store_id=store_id,
            phone=parts["phone"],
            website=parts.get("url") or url,
        )
        if listing["id"] in seen_ids:
            continue
        seen_ids.add(listing["id"])
        listings.append(listing)
        # polite
        if i % 20 == 19:
            time.sleep(0.2)
    print(f"[{CHAIN}] {len(listings)} listings in target metros")
    return listings
