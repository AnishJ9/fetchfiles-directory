"""Thrive Pet Healthcare fetcher.

Strategy: sitemap.xml (gzip-decoded via requests), filter URLs under
/locations/{georgia|florida|texas|tennessee|north-carolina}/..., fetch each,
parse JSON-LD.
"""
from __future__ import annotations

import re
import time

from . import common
from ._http import get
from ._parse import extract_ld_json, find_local_business, place_to_parts
from ._states import two_letter

CHAIN = "thrive"
SITEMAP = "https://www.thrivepetcare.com/sitemap.xml"
TARGET_STATES = ["georgia", "florida", "texas", "tennessee", "north-carolina"]


def _enumerate_urls() -> list[str]:
    txt = get(SITEMAP, timeout=30)
    if not txt:
        return []
    url_re = re.compile(r"<loc>(https://www\.thrivepetcare\.com/locations/([a-z-]+)/[^<]+)</loc>")
    out: list[str] = []
    seen = set()
    for m in url_re.finditer(txt):
        url, state = m.group(1), m.group(2)
        if state not in TARGET_STATES:
            continue
        if url in seen:
            continue
        seen.add(url)
        out.append(url)
    return out


def fetch() -> list[dict]:
    urls = _enumerate_urls()
    print(f"[{CHAIN}] {len(urls)} candidate store pages in target states")
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
        if i % 20 == 19:
            time.sleep(0.2)
    print(f"[{CHAIN}] {len(listings)} listings in target metros")
    return listings
