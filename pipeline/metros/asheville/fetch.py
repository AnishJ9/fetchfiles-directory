"""Fetch raw pet-service data for Asheville MSA from OSM + NC Vet Board."""
from __future__ import annotations

import time
import requests

OVERPASS_URLS = [
    "https://overpass-api.de/api/interpreter",
    "https://overpass.kumi.systems/api/interpreter",
    "https://overpass.private.coffee/api/interpreter",
]
BBOX = (35.35, -82.85, 35.80, -82.35)  # south, west, north, east
OSM_TIMEOUT = 30  # hard cap per HTTP request
OSM_QUERY_SERVER_TIMEOUT = 25  # server-side Overpass timeout


def _overpass_query(q: str) -> dict | None:
    """Try each Overpass mirror until one returns 200 JSON.

    Each HTTP request is bounded to OSM_TIMEOUT (30s). We fall through
    mirrors on 429/5xx; no long sleeps so the whole walk stays snappy.
    One very short retry on the primary after throttling."""
    attempts = list(OVERPASS_URLS) + [OVERPASS_URLS[0]]
    for i, url in enumerate(attempts):
        try:
            r = requests.post(
                url,
                data={"data": q},
                timeout=OSM_TIMEOUT,
                headers={"User-Agent": "fetchfiles-directory/1.0 (asheville pet services)"},
            )
            if r.status_code == 200:
                try:
                    return r.json()
                except Exception as e:
                    print(f"[osm] {url}: bad JSON: {e}")
                    continue
            print(f"[osm] {url} -> HTTP {r.status_code}")
            if r.status_code == 429 and i == len(attempts) - 2:
                # small backoff before the final retry on the primary
                time.sleep(5)
        except Exception as e:
            print(f"[osm] {url} failed: {e}")
            continue
    return None


def _build_q(selector: str) -> str:
    s, w, n, e = BBOX
    return f"""
[out:json][timeout:{OSM_QUERY_SERVER_TIMEOUT}];
(
  node{selector}({s},{w},{n},{e});
  way{selector}({s},{w},{n},{e});
  relation{selector}({s},{w},{n},{e});
);
out center tags;
""".strip()


def fetch_osm() -> list[dict]:
    """Return list of {category, osm_type, osm_id, lat, lng, tags}.

    For each category we try up to 3 passes across the mirror set if every
    earlier pass fails. Overpass is flaky, and we don't want one transient
    502/504 to zero out an entire category. Each HTTP request stays bounded
    to OSM_TIMEOUT (30s)."""
    queries = [
        ("veterinarian", '[amenity=veterinary]'),
        ("groomer", '[shop=pet_grooming]'),
        ("groomer", '[craft=pet_grooming]'),
        ("boarder", '[amenity=animal_boarding]'),
    ]
    raw: list[dict] = []
    for category, selector in queries:
        q = _build_q(selector)
        print(f"[osm] {category} selector={selector}")
        data = None
        for pass_num in range(1, 4):
            data = _overpass_query(q)
            if data is not None:
                break
            print(f"[osm] {category}: pass {pass_num} failed, retrying…")
            time.sleep(3)
        if not data:
            print(f"[osm] {category}: giving up after 3 passes")
            continue
        for el in data.get("elements", []):
            tags = el.get("tags", {}) or {}
            if el.get("type") == "node":
                lat = el.get("lat")
                lng = el.get("lon")
            else:
                c = el.get("center") or {}
                lat = c.get("lat")
                lng = c.get("lon")
            if lat is None or lng is None:
                continue
            raw.append(
                {
                    "category": category,
                    "osm_type": el.get("type"),
                    "osm_id": el.get("id"),
                    "lat": lat,
                    "lng": lng,
                    "tags": tags,
                }
            )
        time.sleep(1)  # be polite between queries
    print(f"[osm] total raw: {len(raw)}")
    return raw


# ---------- NC Vet Board ----------

NC_VMB_URL = "https://www.ncvmb.org/"
# Known third-party lookup used by some NC boards.
NC_LICENSEE_SEARCH_HOSTS = [
    "https://www.ncvmb.org/license-verification/",
    "https://www.ncvmb.org/licensees/",
    "https://ncvmb.gov.licensesearch.com/",
]
VET_BOARD_TIMEOUT = 300  # 5-min hard cap total


def fetch_nc_vet_board() -> list[dict]:
    """Best-effort scrape of NC vet board.

    NCVMB doesn't publish an open data feed. Its public licensee lookup is a
    stateful form (often JS-rendered or fronted by a third-party verification
    site). Without a browser context and inside a 5-minute budget, we probe
    the known endpoints, log what we see, and return [] if we can't extract
    structured records. OSM is the primary source for this metro.
    """
    t0 = time.time()
    session = requests.Session()
    session.headers.update(
        {"User-Agent": "fetchfiles-directory/1.0 (asheville pet services research)"}
    )
    try:
        for url in NC_LICENSEE_SEARCH_HOSTS:
            if time.time() - t0 > VET_BOARD_TIMEOUT:
                print("[nc-vet] budget exceeded")
                return []
            try:
                r = session.get(url, timeout=20, allow_redirects=True)
                print(f"[nc-vet] GET {url} -> {r.status_code}")
                low = r.text.lower()
                if r.status_code == 200 and any(
                    k in low for k in ("licen", "search", "veterinar")
                ):
                    # Landing page reached, but extraction requires state
                    # replay. Log and stop rather than spending the budget.
                    print(f"[nc-vet] {url}: landing page reached; stateful search — skipping within budget")
                    return []
            except Exception as e:
                print(f"[nc-vet] {url} failed: {e}")
        print("[nc-vet] no endpoints yielded parseable results")
    except Exception as e:
        print(f"[nc-vet] failed: {e}")
    return []
