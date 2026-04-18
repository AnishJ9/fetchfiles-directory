"""VCA Animal Hospitals fetcher.

Strategy: per-state hospital sitemaps (georgia/florida/texas/tennessee/
north-carolina), pick only top-level hospital pages (path like
/{hospital-slug}, not /.../services or /.../hospital), fetch each,
parse JSON-LD.
"""
from __future__ import annotations

import re
import time

from . import common
from ._http import get
from ._parse import extract_ld_json, find_local_business, place_to_parts
from ._states import two_letter

CHAIN = "vca"
STATE_SITEMAPS = [
    ("georgia", "https://vcahospitals.com/-/sitemap/sitemap-hospitals-georgia.xml"),
    ("florida", "https://vcahospitals.com/-/sitemap/sitemap-hospitals-florida.xml"),
    ("texas", "https://vcahospitals.com/-/sitemap/sitemap-hospitals-texas.xml"),
    ("tennessee", "https://vcahospitals.com/-/sitemap/sitemap-hospitals-tennessee.xml"),
    ("north-carolina", "https://vcahospitals.com/-/sitemap/sitemap-hospitals-north-carolina.xml"),
]

# Only hospital root pages — one slug after domain
HOSP_RE = re.compile(r"<loc>https://vcahospitals\.com/([a-z0-9-]+)</loc>")


def _enumerate_hospitals() -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for state, sm_url in STATE_SITEMAPS:
        txt = get(sm_url, timeout=30)
        if not txt:
            print(f"[{CHAIN}] failed to fetch {sm_url}")
            continue
        for m in HOSP_RE.finditer(txt):
            slug = m.group(1)
            if slug in seen:
                continue
            seen.add(slug)
            out.append(f"https://vcahospitals.com/{slug}")
    return out


def fetch() -> list[dict]:
    urls = _enumerate_hospitals()
    print(f"[{CHAIN}] {len(urls)} hospital pages in target states")
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
