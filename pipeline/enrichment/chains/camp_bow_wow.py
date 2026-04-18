"""Camp Bow Wow fetcher.

Strategy: sitemap.xml → enumerate top-level location slugs like
`/albany/`, `/alpharetta/`. Fetch each, parse JSON-LD for address (no lat/lng
in schema). Filter by (city, state) → metro via static centroid table; use
centroid coords as lat/lng.
"""
from __future__ import annotations

import re
import time

from . import common
from ._http import get
from ._parse import extract_ld_json, find_local_business, place_to_parts
from ._metro_cities import metro_for_city_state
from ._states import two_letter

CHAIN = "camp_bow_wow"
SITEMAP = "https://www.campbowwow.com/sitemap.xml"


def _enumerate_locations() -> list[str]:
    txt = get(SITEMAP, timeout=30)
    if not txt:
        return []
    pat = re.compile(r"<loc>(https://www\.campbowwow\.com/([a-z0-9-]+)/)</loc>")
    excluded = {
        "about-us", "accessibility", "blogs", "contact-us", "franchising",
        "giving-back", "locations", "privacy-policy", "services", "site-map",
        "site-search", "terms-of-use-app", "why-camp-bow-wow", "careers",
        "jobs", "employment",
    }
    out: list[str] = []
    seen = set()
    for m in pat.finditer(txt):
        url = m.group(1)
        slug = m.group(2)
        if slug in excluded:
            continue
        if url in seen:
            continue
        seen.add(url)
        out.append(url)
    return out


def fetch() -> list[dict]:
    urls = _enumerate_locations()
    print(f"[{CHAIN}] {len(urls)} location candidate pages")
    common.cache_write(CHAIN, {"urls": urls})
    listings: list[dict] = []
    seen_ids: set[str] = set()
    # Quick pre-filter: only fetch pages whose slug hints at our 5 metro states.
    # Slug examples: alpharetta, austin, austin-north, nashville, franklin-tn
    # Since slug patterns don't include state reliably, we fetch all and filter
    # by JSON-LD state. 240 pages × 0.3s ≈ 72s — acceptable.
    for i, url in enumerate(urls):
        html = get(url, timeout=15)
        if not html:
            continue
        objs = extract_ld_json(html)
        biz = find_local_business(objs)
        parts = place_to_parts(biz) if biz else None
        if not parts:
            continue
        state2 = two_letter(parts["state"]) or parts["state"]
        # CBW JSON-LD rarely has lat/lng → use centroid table lookup
        metro, lat, lng = metro_for_city_state(parts["city"], state2)
        if not metro:
            # If lat/lng were present, validate via bbox
            if parts["lat"] is not None and parts["lng"] is not None:
                metro = common.metro_for(parts["lat"], parts["lng"])
                if metro:
                    lat, lng = parts["lat"], parts["lng"]
                else:
                    continue
            else:
                continue
        store_id = url.rstrip("/").rsplit("/", 1)[-1]
        name = parts["name"] or "Camp Bow Wow"
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
        if i % 20 == 19:
            time.sleep(0.2)
    print(f"[{CHAIN}] {len(listings)} listings in target metros")
    return listings
