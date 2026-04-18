"""ProPublica Nonprofit Explorer enrichment for animal shelter 501(c)(3)s.

Searches the ProPublica Nonprofit Explorer API for animal-related nonprofits
in five states (GA, FL, TX, TN, NC), pulls full organization details for the
most relevant hits, geocodes addresses (via the API's lat/lng if present, or
ZIP centroid lookup, or finally OpenStreetMap Nominatim), and filters to the
five launch metro bounding boxes.

Outputs:
  data/enrichment/shelters_propublica.json -- flat array, schema per
  docs/SCHEMA.md, category="shelter".

No auth required. Polite rate limits: ~1.2s between ProPublica calls,
~1.1s between Nominatim calls (Nominatim's policy is 1 req/sec max).
"""
from __future__ import annotations

import hashlib
import json
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import requests


REPO = Path(__file__).resolve().parent.parent.parent
OUT_PATH = REPO / "data" / "enrichment" / "shelters_propublica.json"
CACHE_DIR = REPO / "data" / "enrichment" / ".cache" / "propublica"

API_BASE = "https://projects.propublica.org/nonprofits/api/v2"
NOMINATIM_BASE = "https://nominatim.openstreetmap.org/search"

USER_AGENT = "fetchfiles-directory/1.0 (shelters_propublica enrichment; contact: anish.joseph58@gmail.com)"

HTTP_TIMEOUT = 30
PROPUBLICA_DELAY = 1.2     # seconds between ProPublica requests
NOMINATIM_DELAY = 1.1      # seconds between Nominatim requests (their 1req/s rule)

MAX_PAGES_PER_QUERY = 10   # ProPublica returns 25 results/page; 10 pages = 250 hits
MAX_EINS_PER_STATE = 100   # cap on detailed fetches per state, per spec

# state -> metros that fall (mostly) inside it. Used to skip states with no
# matching metro just in case.
STATES = ["GA", "FL", "TX", "TN", "NC"]
SEARCH_TERMS = ["animal shelter", "humane society", "animal rescue"]

# metro -> (south, west, north, east). From docs/SCHEMA.md.
METRO_BBOX: dict[str, tuple[float, float, float, float]] = {
    "atlanta":   (33.40, -84.85, 34.15, -83.90),
    "tampa":     (27.50, -82.90, 28.30, -82.20),
    "austin":    (30.00, -98.10, 30.65, -97.40),
    "nashville": (35.80, -87.10, 36.45, -86.40),
    "asheville": (35.35, -82.85, 35.80, -82.35),
}

# Expected state for each metro (used to bias geocoding).
METRO_STATE: dict[str, str] = {
    "atlanta": "GA",
    "tampa": "FL",
    "austin": "TX",
    "nashville": "TN",
    "asheville": "NC",
}


# ---- HTTP helpers ----------------------------------------------------------

_session = requests.Session()
_session.headers.update({"User-Agent": USER_AGENT, "Accept": "application/json"})

_last_call: dict[str, float] = {"propublica": 0.0, "nominatim": 0.0}


def _polite(kind: str, delay: float) -> None:
    now = time.monotonic()
    wait = delay - (now - _last_call[kind])
    if wait > 0:
        time.sleep(wait)
    _last_call[kind] = time.monotonic()


def _get_json(url: str, params: dict | None, kind: str, delay: float) -> dict | None:
    """GET with simple retry on transient errors. Returns parsed JSON or None."""
    for attempt in range(3):
        _polite(kind, delay)
        try:
            r = _session.get(url, params=params, timeout=HTTP_TIMEOUT)
        except requests.RequestException as e:
            print(f"[propublica] {kind} request error (attempt {attempt + 1}): {e}")
            time.sleep(2 + attempt * 2)
            continue
        if r.status_code == 200:
            try:
                return r.json()
            except ValueError:
                print(f"[propublica] non-JSON response from {url}")
                return None
        if r.status_code == 404:
            return None
        if r.status_code in (429, 502, 503, 504):
            print(f"[propublica] {kind} HTTP {r.status_code} (attempt {attempt + 1}); backing off")
            time.sleep(3 + attempt * 3)
            continue
        print(f"[propublica] {kind} HTTP {r.status_code} from {url} (params={params})")
        return None
    return None


# ---- ProPublica API --------------------------------------------------------

def search_state(state: str, q: str) -> list[dict]:
    """Paginate through search.json for a given state + keyword. Returns
    a list of organization summary dicts (whatever fields ProPublica gives in
    the search payload). Stops on empty page or hard cap."""
    out: list[dict] = []
    for page in range(MAX_PAGES_PER_QUERY):
        params = {"q": q, "state[id]": state, "page": page}
        data = _get_json(f"{API_BASE}/search.json", params, "propublica", PROPUBLICA_DELAY)
        if not data:
            break
        orgs = data.get("organizations") or []
        if not orgs:
            break
        out.extend(orgs)
        # Stop early if past the last page based on ProPublica metadata
        num_pages = data.get("num_pages")
        if num_pages is not None and page + 1 >= num_pages:
            break
        if len(orgs) < 25:
            break
    return out


def fetch_org(ein: str) -> dict | None:
    """Fetch /organizations/<ein>.json. ProPublica strips leading zeros and
    expects digits only."""
    digits = re.sub(r"\D+", "", str(ein))
    if not digits:
        return None
    cache_path = CACHE_DIR / f"{digits}.json"
    if cache_path.exists():
        try:
            return json.loads(cache_path.read_text())
        except Exception:
            pass
    data = _get_json(f"{API_BASE}/organizations/{digits}.json", None, "propublica", PROPUBLICA_DELAY)
    if data:
        try:
            cache_path.parent.mkdir(parents=True, exist_ok=True)
            cache_path.write_text(json.dumps(data))
        except Exception as e:
            print(f"[propublica] cache write failed for ein={digits}: {e}")
    return data


# ---- NTEE / shelter classification ----------------------------------------

# NTEE codes for animal shelters / humane societies / rescues.
# D20 = Animal Protection & Welfare, D40 = Veterinary Services (skip),
# D60 = Animal Services NEC. We accept D20-D60 except D40.
SHELTER_NTEE_PREFIXES = ("D20", "D21", "D22", "D23", "D30", "D31", "D32", "D33",
                          "D34", "D50", "D51", "D52", "D53", "D60", "D61")

# Keywords that strongly indicate shelter/rescue/humane intent in the name.
SHELTER_NAME_RX = re.compile(
    r"\b(humane society|animal shelter|animal rescue|spca|society for the prevention of cruelty"
    r"|animal welfare|animal control|animal services|animal sanctuary|animal haven|pet rescue"
    r"|dog rescue|cat rescue|paws|animal league|adoption|no[- ]kill)\b",
    re.IGNORECASE,
)

# Strong negative signals -- keep us from pulling vets, museums, breed clubs,
# horse-only orgs, wildlife rehab, etc. (Loose -- only used to deprioritize.)
NON_SHELTER_NAME_RX = re.compile(
    r"\b(veterinary clinic|hospital association|breeders|kennel club|equestrian|horsemen|"
    r"audubon|zoological|aquarium|wildlife rehabilitation|wildlife rehab|pony club)\b",
    re.IGNORECASE,
)


def looks_like_shelter(org: dict) -> bool:
    name = (org.get("name") or "").strip()
    ntee = (org.get("ntee_code") or "").strip().upper()
    # Accept on NTEE prefix
    if ntee.startswith(SHELTER_NTEE_PREFIXES):
        return True
    # Accept on name keywords
    if name and SHELTER_NAME_RX.search(name):
        # Exclude obvious non-shelters
        if NON_SHELTER_NAME_RX.search(name):
            return False
        return True
    return False


# ---- geocoding -------------------------------------------------------------

def in_bbox(lat: float, lng: float, bbox: tuple[float, float, float, float]) -> bool:
    s, w, n, e = bbox
    return (s <= lat <= n) and (w <= lng <= e)


def metro_for(lat: float | None, lng: float | None) -> str | None:
    if lat is None or lng is None:
        return None
    for metro, bbox in METRO_BBOX.items():
        if in_bbox(lat, lng, bbox):
            return metro
    return None


def nominatim_geocode(address: str, city: str, state: str, zip_: str) -> tuple[float, float] | None:
    """OSM Nominatim free-form geocode. Returns (lat, lng) or None.
    Respects the 1 req/sec policy via _polite()."""
    parts = [p for p in [address, city, state, zip_] if p]
    if not parts:
        return None
    q = ", ".join(parts) + ", USA"
    params = {"q": q, "format": "json", "limit": 1, "countrycodes": "us"}
    data = _get_json(NOMINATIM_BASE, params, "nominatim", NOMINATIM_DELAY)
    if not data:
        return None
    if isinstance(data, list) and data:
        try:
            return float(data[0]["lat"]), float(data[0]["lon"])
        except (KeyError, TypeError, ValueError):
            return None
    return None


# ---- normalization ---------------------------------------------------------

def _normalize_phone(raw: str | None) -> str | None:
    if not raw:
        return None
    digits = re.sub(r"\D+", "", raw)
    if not digits:
        return None
    if len(digits) == 10:
        return f"+1{digits}"
    if len(digits) == 11 and digits.startswith("1"):
        return f"+{digits}"
    return raw.strip() or None


def _make_id(name: str, address: str) -> str:
    key = f"{name}|{address}".lower()
    return hashlib.sha256(key.encode("utf-8")).hexdigest()[:16]


def _str(v) -> str:
    if v is None:
        return ""
    return str(v).strip()


def _coerce_float(v) -> float | None:
    if v in (None, "", "null"):
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def build_listing(detail: dict, metro: str, lat: float, lng: float,
                   geocoded_via: str) -> dict | None:
    """Build a canonical Listing dict from ProPublica /organizations payload."""
    org = detail.get("organization") or {}
    name = _str(org.get("name") or org.get("sub_name"))
    if not name:
        return None
    address = _str(org.get("address"))
    city = _str(org.get("city"))
    state = _str(org.get("state"))
    zip_ = _str(org.get("zipcode") or org.get("zip"))
    ein = _str(org.get("ein"))

    listing: dict = {
        "id": _make_id(name, address or f"{lat},{lng}"),
        "name": name,
        "category": "shelter",
        "address": address,
        "city": city,
        "state": state,
        "zip": zip_,
        "metro": metro,
        "lat": float(lat),
        "lng": float(lng),
        "sources": ["propublica"],
        "sourceIds": {"ein": ein},
        "lastSeenAt": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "claimed": False,
    }

    phone = _normalize_phone(org.get("phone") or org.get("contact_phone"))
    if phone:
        listing["phone"] = phone
    website = _str(org.get("website_url") or org.get("website"))
    if website:
        listing["website"] = website
    tags: list[str] = []
    ntee = _str(org.get("ntee_code")).upper()
    if ntee:
        tags.append(f"ntee:{ntee}")
    tags.append(f"geocode:{geocoded_via}")
    listing["tags"] = tags
    return listing


# ---- driver ---------------------------------------------------------------

def run() -> None:
    started = time.monotonic()
    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    CACHE_DIR.mkdir(parents=True, exist_ok=True)

    # state -> dict[ein] -> summary org (from search results)
    by_state: dict[str, dict[str, dict]] = {s: {} for s in STATES}
    state_zero_hits: list[str] = []

    for state in STATES:
        all_hits: dict[str, dict] = {}
        for term in SEARCH_TERMS:
            try:
                hits = search_state(state, term)
            except Exception as e:
                print(f"[propublica] search error state={state} q='{term}': {e}")
                hits = []
            print(f"[propublica] search state={state} q='{term}': {len(hits)} raw")
            for o in hits:
                ein = _str(o.get("ein") or o.get("strein"))
                if not ein:
                    continue
                # Keep richest summary we've seen
                if ein not in all_hits or (
                    not (all_hits[ein].get("ntee_code")) and o.get("ntee_code")
                ):
                    all_hits[ein] = o
        # Filter to those that look like shelters
        shelter_hits = {ein: o for ein, o in all_hits.items() if looks_like_shelter(o)}
        print(f"[propublica] state={state} unique={len(all_hits)} shelter-like={len(shelter_hits)}")
        if not shelter_hits:
            state_zero_hits.append(state)
        # Cap to the spec's limit per state
        if len(shelter_hits) > MAX_EINS_PER_STATE:
            # Prefer NTEE-matched first
            ranked = sorted(
                shelter_hits.items(),
                key=lambda kv: (
                    0 if (kv[1].get("ntee_code") or "").upper().startswith(SHELTER_NTEE_PREFIXES)
                    else 1,
                    kv[1].get("name") or "",
                ),
            )
            shelter_hits = dict(ranked[:MAX_EINS_PER_STATE])
            print(f"[propublica] state={state} capped to {MAX_EINS_PER_STATE} EINs")
        by_state[state] = shelter_hits

    # ---- detail fetch + bbox filter ---------------------------------------
    listings: list[dict] = []
    seen_ids: set[str] = set()
    eins_fetched = 0
    geocoded_via_nominatim = 0
    metro_counts: dict[str, int] = {m: 0 for m in METRO_BBOX}
    deadline = started + (24 * 60)  # leave 1 min slack inside the 25-min cap

    for state, shelter_hits in by_state.items():
        expected_metro = None
        for metro, mstate in METRO_STATE.items():
            if mstate == state:
                expected_metro = metro
                break

        for ein, summary in shelter_hits.items():
            if time.monotonic() > deadline:
                print("[propublica] runtime cap approaching; stopping detail fetches")
                break
            detail = fetch_org(ein)
            if not detail:
                continue
            eins_fetched += 1
            org = detail.get("organization") or {}

            address = _str(org.get("address"))
            city = _str(org.get("city"))
            org_state = _str(org.get("state"))
            zip_ = _str(org.get("zipcode") or org.get("zip"))

            # Try API-supplied coords first (ProPublica historically has none, but check)
            lat = _coerce_float(org.get("latitude") or org.get("lat"))
            lng = _coerce_float(org.get("longitude") or org.get("lng") or org.get("lon"))
            geocoded_via = "propublica"

            # Skip if address is empty -- can't geocode confidently
            if (lat is None or lng is None) and not (address or zip_):
                continue

            # Quick geographic precheck: if state mismatch and not within any
            # expected metro state, skip to save Nominatim quota.
            if org_state and expected_metro and org_state.upper() != state:
                # different state, may still be valid (we searched by state filter)
                pass

            if lat is None or lng is None:
                coords = nominatim_geocode(address, city, org_state or state, zip_)
                if coords:
                    lat, lng = coords
                    geocoded_via = "nominatim"
                    geocoded_via_nominatim += 1
                else:
                    continue

            metro = metro_for(lat, lng)
            if not metro:
                continue

            listing = build_listing(detail, metro, lat, lng, geocoded_via)
            if not listing:
                continue
            if listing["id"] in seen_ids:
                continue
            seen_ids.add(listing["id"])
            listings.append(listing)
            metro_counts[metro] += 1

        if time.monotonic() > deadline:
            break

    # ---- write ------------------------------------------------------------
    OUT_PATH.write_text(json.dumps(listings, indent=2))

    # ---- report -----------------------------------------------------------
    elapsed = time.monotonic() - started
    print()
    print(f"wrote {OUT_PATH} -- {len(listings)} listings ({elapsed:.0f}s)")
    print()
    print(f"{'metro':<11}{'count':>7}")
    for metro in METRO_BBOX:
        print(f"{metro:<11}{metro_counts[metro]:>7}")
    print()
    print(f"EINs fetched (detail):     {eins_fetched}")
    print(f"Geocoded via Nominatim:    {geocoded_via_nominatim}")
    if state_zero_hits:
        print(f"States with 0 shelter-like hits: {', '.join(state_zero_hits)}")


if __name__ == "__main__":
    try:
        run()
    except KeyboardInterrupt:
        print("interrupted", file=sys.stderr)
        sys.exit(130)
