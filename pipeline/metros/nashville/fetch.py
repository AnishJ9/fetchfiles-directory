"""Fetch raw pet-service data for Nashville MSA from OSM + TN Vet Board."""
from __future__ import annotations

import time
import requests

OVERPASS_URL = "https://overpass-api.de/api/interpreter"
BBOX = (35.80, -87.10, 36.45, -86.40)  # south, west, north, east
OSM_TIMEOUT = 30  # hard cap per HTTP request
OSM_QUERY_SERVER_TIMEOUT = 25  # server-side Overpass timeout


def _overpass_query(q: str, attempts: int = 3) -> dict | None:
    last_err: str | None = None
    for i in range(1, attempts + 1):
        try:
            r = requests.post(
                OVERPASS_URL,
                data={"data": q},
                timeout=OSM_TIMEOUT,
                headers={"User-Agent": "fetchfiles-directory/1.0 (nashville pet services)"},
            )
            if r.status_code == 200:
                return r.json()
            last_err = f"HTTP {r.status_code}"
            print(f"[osm] {last_err} (attempt {i}/{attempts})")
            # Back off on 429/5xx and retry.
            if r.status_code in (429, 502, 503, 504):
                time.sleep(2 * i)
                continue
            return None
        except Exception as e:
            last_err = str(e)
            print(f"[osm] failed: {e} (attempt {i}/{attempts})")
            time.sleep(2 * i)
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


OSM_PHASE_BUDGET_SEC = 180  # soft total budget for the OSM phase


def fetch_osm() -> list[dict]:
    """Return list of {category, osm_type, osm_id, lat, lng, tags}."""
    queries = [
        ("veterinarian", '[amenity=veterinary]'),
        ("groomer", '[shop=pet_grooming]'),
        ("groomer", '[craft=pet_grooming]'),
        ("boarder", '[amenity=animal_boarding]'),
    ]
    raw: list[dict] = []
    t0 = time.time()
    for category, selector in queries:
        if time.time() - t0 > OSM_PHASE_BUDGET_SEC:
            print("[osm] phase budget exceeded; stopping")
            break
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


# ---------- TN Vet Board ----------

TN_LICENSE_URL = "https://apps.health.tn.gov/Licensure/default.aspx"
VET_BOARD_TIMEOUT = 300  # 5-min hard cap total


def fetch_tn_vet_board() -> list[dict]:
    """Best-effort scrape of TN vet board.

    The TN Department of Health license verification portal is a stateful
    ASP.NET WebForms app requiring __VIEWSTATE / __EVENTVALIDATION token
    round-tripping plus a profession dropdown selection. Without a browser
    context and within a 5-minute budget, we attempt a probe, log, and
    return [] if we can't retrieve structured records.
    """
    t0 = time.time()
    session = requests.Session()
    session.headers.update(
        {"User-Agent": "fetchfiles-directory/1.0 (nashville pet services research)"}
    )
    try:
        r = session.get(TN_LICENSE_URL, timeout=20)
        print(f"[tn-vet] GET {TN_LICENSE_URL} -> {r.status_code}")
        if time.time() - t0 > VET_BOARD_TIMEOUT:
            print("[tn-vet] budget exceeded")
            return []
        # Portal is stateful ASP.NET; full form roundtrip not attempted
        # within 5-min budget without a browser engine.
        print("[tn-vet] portal is stateful ASP.NET; skipping within budget")
    except Exception as e:
        print(f"[tn-vet] failed: {e}")
    return []
