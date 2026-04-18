"""MedVet fetcher.

Strategy: location-sitemap.xml (40 URLs), fetch each, parse JSON-LD.
"""
from __future__ import annotations

import re
import time

from . import common
from ._http import get
from ._parse import extract_ld_json, find_local_business, place_to_parts
from ._states import two_letter

CHAIN = "medvet"
SITEMAP = "https://www.medvet.com/location-sitemap.xml"


def _enumerate_urls() -> list[str]:
    txt = get(SITEMAP, timeout=30)
    if not txt:
        return []
    return [m.group(1) for m in re.finditer(r"<loc>(https://www\.medvet\.com/location/[^<]+)</loc>", txt)]


def fetch() -> list[dict]:
    urls = _enumerate_urls()
    print(f"[{CHAIN}] {len(urls)} location pages (national)")
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
        store_id = url.rstrip("/").rsplit("/", 1)[-1]
        state = two_letter(parts["state"]) or parts["state"]
        listing = common.build_listing(
            name=parts["name"],
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
            website=url,
        )
        if listing["id"] in seen_ids:
            continue
        seen_ids.add(listing["id"])
        listings.append(listing)
        if i % 10 == 9:
            time.sleep(0.1)
    print(f"[{CHAIN}] {len(listings)} listings in target metros")
    return listings
