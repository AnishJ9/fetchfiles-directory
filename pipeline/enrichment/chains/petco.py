"""Petco fetcher.

Strategy: sitemap1.xml + sitemap2.xml enumerate per-store pages. Filter to
URLs matching /{ga|fl|tx|tn|nc}/.../pet-supplies-... (store root pages),
fetch each, parse JSON-LD.
"""
from __future__ import annotations

import re
import time

from . import common
from ._http import get
from ._parse import extract_ld_json, find_local_business, place_to_parts
from ._states import two_letter

CHAIN = "petco"
SITEMAPS = [
    "https://stores.petco.com/sitemap1.xml",
    "https://stores.petco.com/sitemap2.xml",
]
TARGET_STATES = {"ga", "fl", "tx", "tn", "nc"}
URL_RE = re.compile(
    r"https://stores\.petco\.com/([a-z]{2})/[a-z0-9-]+/pet-supplies-[a-z0-9-]+\.html"
)


def _enumerate_urls() -> list[str]:
    urls: list[str] = []
    seen = set()
    for sm in SITEMAPS:
        txt = get(sm, timeout=30)
        if not txt:
            continue
        for m in URL_RE.finditer(txt):
            st = m.group(1)
            if st not in TARGET_STATES:
                continue
            u = m.group(0)
            if u in seen:
                continue
            seen.add(u)
            urls.append(u)
    return urls


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
        # store_id = the trailing numeric from URL filename
        m = re.search(r"-(\d+)\.html$", url)
        store_id = m.group(1) if m else url.rsplit("/", 1)[-1]
        state = two_letter(parts["state"]) or parts["state"]
        name = parts["name"] or "Petco"
        if "petco" in name.lower() and ("pet store" in name.lower() or "pet supplies" in name.lower()):
            name = "Petco"
        listing = common.build_listing(
            name=name,
            category="groomer",
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
            subcategories=["retail"],
        )
        if listing["id"] in seen_ids:
            continue
        seen_ids.add(listing["id"])
        listings.append(listing)
        if i % 20 == 19:
            time.sleep(0.2)
    print(f"[{CHAIN}] {len(listings)} listings in target metros")
    return listings
