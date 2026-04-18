"""Dogtopia fetcher.

Strategy: sitemap_index.xml lists per-location page-sitemap.xml sub-sitemaps;
location slug is the domain-level path segment. Dogtopia location pages have
JSON-LD with lat/lng (swapped in some pages). Fetch each candidate and parse.
"""
from __future__ import annotations

import re
import time

from . import common
from ._http import get
from ._parse import extract_ld_json, find_local_business, place_to_parts
from ._states import two_letter

CHAIN = "dogtopia"
SITEMAP_INDEX = "https://www.dogtopia.com/sitemap_index.xml"

# Slug keywords that indicate relevance to our target metros or states.
# Slugs examples: austin, atlanta-northbrookhaven, tampa-westchase,
# hickory-plaza-nashville, florida-..., georgia-..., tennessee-...
SLUG_KEYWORDS = [
    "atlanta", "alpharetta", "sandy-springs", "roswell", "marietta", "decatur",
    "duluth", "smyrna", "dunwoody", "johns-creek", "peachtree", "kennesaw",
    "lawrenceville", "brookhaven", "cumming", "woodstock", "canton", "buford",
    "tampa", "st-pete", "saint-pete", "clearwater", "brandon", "largo",
    "riverview", "wesley", "temple-terrace", "carrollwood", "lutz", "westchase",
    "austin", "round-rock", "cedar-park", "pflugerville", "georgetown",
    "leander", "kyle", "buda", "bee-cave", "lakeway", "lake-travis",
    "nashville", "franklin", "brentwood", "hendersonville", "mt-juliet",
    "mount-juliet", "gallatin", "goodlettsville", "nolensville",
    "asheville", "arden", "fletcher", "weaverville", "black-mountain", "candler",
    # state-prefixed slugs
    "florida", "georgia", "texas", "tennessee", "north-carolina",
]


def _candidate_slugs() -> list[str]:
    txt = get(SITEMAP_INDEX, timeout=30)
    if not txt:
        return []
    pat = re.compile(r"https://www\.dogtopia\.com/([a-z0-9-]+)/page-sitemap\.xml")
    slugs = set()
    for m in pat.finditer(txt):
        s = m.group(1)
        slugs.add(s)
    # filter by keywords
    kept = []
    for s in sorted(slugs):
        if any(k in s for k in SLUG_KEYWORDS):
            kept.append(s)
    return kept


def fetch() -> list[dict]:
    slugs = _candidate_slugs()
    print(f"[{CHAIN}] {len(slugs)} candidate location slugs (keyword-matched)")
    common.cache_write(CHAIN, {"slugs": slugs})
    listings: list[dict] = []
    seen_ids: set[str] = set()
    for i, slug in enumerate(slugs):
        url = f"https://www.dogtopia.com/{slug}/"
        html = get(url, timeout=15)
        if not html:
            continue
        objs = extract_ld_json(html)
        biz = find_local_business(objs)
        parts = place_to_parts(biz) if biz else None
        if not parts:
            continue
        lat, lng = parts["lat"], parts["lng"]
        # Dogtopia sometimes swaps latitude/longitude in one of the objects.
        # If lat is out of US range (e.g., negative), swap.
        if lat is not None and lng is not None:
            # Heuristic: US lat is ~24..50, lng is ~-125..-66.
            if not (20 <= lat <= 55) and (20 <= lng <= 55):
                lat, lng = lng, lat
            if not (-130 <= lng <= -60) and (-130 <= lat <= -60):
                lat, lng = lng, lat
        if lat is None or lng is None:
            continue
        metro = common.metro_for(lat, lng)
        if not metro:
            continue
        state2 = two_letter(parts["state"]) or parts["state"]
        store_id = slug
        name = parts["name"] or "Dogtopia"
        listing = common.build_listing(
            name=name,
            category="daycare",
            address=parts["street"],
            city=parts["city"],
            state=state2,
            zip_=parts["zip"],
            lat=lat,
            lng=lng,
            metro=metro,
            chain=CHAIN,
            store_id=store_id,
            phone=parts["phone"],
            website=url,
            subcategories=["boarding"],
        )
        if listing["id"] in seen_ids:
            continue
        seen_ids.add(listing["id"])
        listings.append(listing)
        if i % 15 == 14:
            time.sleep(0.2)
    print(f"[{CHAIN}] {len(listings)} listings in target metros")
    return listings
