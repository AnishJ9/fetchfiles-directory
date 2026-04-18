"""PetSmart fetcher.

Strategy:
1. Crawl state pages (stores.petsmart.com/us/{ga,fl,tx,tn,nc}) for city URLs.
2. For each metro city, fetch the city page, extract store slugs.
3. For each store slug, fetch `/grooming` (has JSON-LD) and optionally
   `/petshotel-doggie-day-camp`.
4. Produce groomer listings. Also add Banfield-inside-PetSmart as a distinct
   veterinarian listing when the store page mentions Banfield.

PetSmart is a Rio SEO / Location3 directory (no public JSON API in budget).
"""
from __future__ import annotations

import re
import time

from . import common
from ._http import get
from ._parse import extract_ld_json, find_local_business, place_to_parts
from ._states import two_letter
from .banfield import METRO_CITY_SLUGS

CHAIN = "petsmart"

STATE_BASE = "https://stores.petsmart.com/us/{state}"
TARGET_STATES = ["ga", "fl", "tx", "tn", "nc"]

STATE_TO_METRO = {
    "ga": "atlanta",
    "fl": "tampa",
    "tx": "austin",
    "tn": "nashville",
    "nc": "asheville",
}


def _city_urls(state: str) -> list[str]:
    url = STATE_BASE.format(state=state)
    html = get(url, timeout=15)
    if not html:
        return []
    pat = re.compile(r'href="(https://stores\.petsmart\.com/us/' + state + r'/[a-z0-9-]+)"')
    out: list[str] = []
    seen = set()
    for m in pat.finditer(html):
        u = m.group(1).rstrip("/")
        if u in seen:
            continue
        seen.add(u)
        out.append(u)
    return out


def _store_slugs_for_city(state: str, city_slug: str) -> list[str]:
    url = f"https://stores.petsmart.com/us/{state}/{city_slug}"
    html = get(url, timeout=15)
    if not html:
        return []
    # Links are like stores.petsmart.com/{state}/{city}/{slug}/grooming (no /us/)
    pat = re.compile(
        r"stores\.petsmart\.com/" + re.escape(state) + r"/" + re.escape(city_slug) + r"/([a-z0-9-]+)/(grooming|petshotel-doggie-day-camp)"
    )
    slugs = set()
    for m in pat.finditer(html):
        slugs.add(m.group(1))
    return sorted(slugs)


def fetch() -> list[dict]:
    # 1. Gather candidate city URLs per state and filter to metro cities
    candidates: list[tuple[str, str, str]] = []  # (state, city, metro)
    for state in TARGET_STATES:
        metro = STATE_TO_METRO[state]
        cities_in_metro = METRO_CITY_SLUGS[metro]
        cu = _city_urls(state)
        for u in cu:
            city = u.rsplit("/", 1)[-1]
            if city in cities_in_metro:
                candidates.append((state, city, metro))
    print(f"[{CHAIN}] {len(candidates)} metro cities")

    # 2. Expand to store slugs per city
    store_entries: list[tuple[str, str, str, str]] = []  # (state, city, slug, metro)
    for state, city, metro in candidates:
        slugs = _store_slugs_for_city(state, city)
        for s in slugs:
            store_entries.append((state, city, s, metro))

    # Dedupe on (state, city, slug)
    uniq: dict[tuple[str, str, str], str] = {}
    for st, c, sl, m in store_entries:
        uniq[(st, c, sl)] = m
    print(f"[{CHAIN}] {len(uniq)} candidate stores")

    common.cache_write(CHAIN, {"stores": [f"{st}/{c}/{sl}" for (st, c, sl) in uniq.keys()]})

    # 3. Fetch each store's grooming page for JSON-LD
    listings: list[dict] = []
    seen_ids: set[str] = set()
    for (state, city, slug), metro in uniq.items():
        url = f"https://stores.petsmart.com/{state}/{city}/{slug}/grooming"
        html = get(url, timeout=15)
        if not html:
            continue
        objs = extract_ld_json(html)
        biz = find_local_business(objs)
        parts = place_to_parts(biz) if biz else None
        if not parts or parts["lat"] is None or parts["lng"] is None:
            continue
        ml = common.metro_for(parts["lat"], parts["lng"]) or metro
        st2 = two_letter(parts["state"]) or parts["state"] or state.upper()
        store_id = f"{state}/{city}/{slug}"
        name = parts["name"] or "PetSmart"
        # Normalize name: some pages say "PetSmart Grooming" — prefer "PetSmart"
        if "petsmart" in name.lower() and "grooming" in name.lower():
            name = "PetSmart"
        listing = common.build_listing(
            name=name,
            category="groomer",
            address=parts["street"],
            city=parts["city"],
            state=st2,
            zip_=parts["zip"],
            lat=parts["lat"],
            lng=parts["lng"],
            metro=ml,
            chain=CHAIN,
            store_id=store_id,
            phone=parts["phone"],
            website=f"https://stores.petsmart.com/{state}/{city}/{slug}",
            subcategories=["retail"],
        )
        if listing["id"] not in seen_ids:
            seen_ids.add(listing["id"])
            listings.append(listing)

        # If page references Banfield, produce a companion veterinarian listing
        if re.search(r"banfield", html, flags=re.IGNORECASE):
            vet_listing = common.build_listing(
                name="Banfield Pet Hospital",
                category="veterinarian",
                address=parts["street"],
                city=parts["city"],
                state=st2,
                zip_=parts["zip"],
                lat=parts["lat"],
                lng=parts["lng"],
                metro=ml,
                chain="banfield",
                store_id=store_id,
                phone=parts["phone"],
                website=f"https://stores.petsmart.com/{state}/{city}/{slug}",
                subcategories=["inside-petsmart"],
            )
            if vet_listing["id"] not in seen_ids:
                seen_ids.add(vet_listing["id"])
                listings.append(vet_listing)

    print(f"[{CHAIN}] {len(listings)} listings in target metros")
    return listings
