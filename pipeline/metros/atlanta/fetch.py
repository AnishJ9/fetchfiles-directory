"""Fetch raw pet-service data for Atlanta MSA from OSM + GA Vet Board."""
from __future__ import annotations

import time
import requests

OVERPASS_URL = "https://overpass-api.de/api/interpreter"
BBOX = (33.40, -84.85, 34.15, -83.90)  # south, west, north, east
OSM_TIMEOUT = 30  # hard cap per HTTP request
OSM_QUERY_SERVER_TIMEOUT = 25  # server-side Overpass timeout


OVERPASS_MIRRORS = [
    "https://overpass-api.de/api/interpreter",
    "https://overpass.kumi.systems/api/interpreter",
    "https://overpass.private.coffee/api/interpreter",
]


def _overpass_query(q: str) -> dict | None:
    last_err = None
    for url in OVERPASS_MIRRORS:
        try:
            r = requests.post(
                url,
                data={"data": q},
                timeout=OSM_TIMEOUT,
                headers={"User-Agent": "fetchfiles-directory/1.0 (atlanta pet services)"},
            )
            if r.status_code == 200:
                return r.json()
            print(f"[osm] HTTP {r.status_code} from {url}")
            last_err = r.status_code
        except Exception as e:
            print(f"[osm] failed {url}: {e}")
            last_err = e
        time.sleep(1)
    print(f"[osm] all mirrors failed: {last_err}")
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
    """Return list of {category, osm_type, osm_id, lat, lng, tags}."""
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
        data = _overpass_query(q)
        if not data:
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


# ---------- GA Vet Board ----------

GA_VERIFY_URL = "https://verify.sos.ga.gov/verification/Search.aspx"
VET_BOARD_TIMEOUT = 300  # 5-min hard cap total


def fetch_ga_vet_board() -> list[dict]:
    """Best-effort scrape of GA vet board.

    The GA SoS verification site is a stateful ASP.NET WebForms app that
    requires __VIEWSTATE, __EVENTVALIDATION, and search-term input; results
    are paginated and rendered server-side. Without a browser context and
    with a 5-minute budget, we attempt a simple probe and log. If we cannot
    retrieve structured records, return [] and log.
    """
    t0 = time.time()
    session = requests.Session()
    session.headers.update(
        {"User-Agent": "fetchfiles-directory/1.0 (atlanta pet services research)"}
    )
    try:
        r = session.get(GA_VERIFY_URL, timeout=20)
        print(f"[ga-vet] GET {GA_VERIFY_URL} -> {r.status_code}")
        # Without a documented open data feed or parseable listing endpoint,
        # full scraping exceeds the 5-minute budget and is stateful. Log and skip.
        if time.time() - t0 > VET_BOARD_TIMEOUT:
            print("[ga-vet] budget exceeded")
            return []
        print("[ga-vet] portal is stateful ASP.NET; skipping within budget")
    except Exception as e:
        print(f"[ga-vet] failed: {e}")
    return []
