"""Pet Hotels enrichment -- OSM + chain locators for 5 launch metros.

Produces data/enrichment/pet_hotels.json (flat array, schema per docs/SCHEMA.md,
category = "pet_hotel").

Data sources:
  1) OSM Overpass: amenity=animal_boarding filtered to names containing
     "hotel/resort/inn/lodge/spa", OR combined with tourism=*.
  2) Chain locators:
       - Wag Hotels     (https://waghotels.com/locations/)        -- JS SPA, no scrape
       - K9 Resorts     (https://www.k9resorts.com/locations/{state}/ + per-location JSON-LD)
       - Hounds Town    (https://houndstownusa.com/locations-sitemap.xml; state pages have addresses)
       - Best Friends   (https://bestfriendspetcare.com/locations/{slug}/ with wpsl-js data)
       - Preferred Pets (https://preferredpetshotel.com) -- DNS doesn't resolve
       - Morris Animal  (https://morrisanimalinn.com/) -- NJ only, out of metro bboxes

Cap per chain: 3 minutes. Overall script runtime target: <25 minutes.
Dedupe within-source by id only; cross-source dedupe handled by merge.py.
"""
from __future__ import annotations

import hashlib
import json
import re
import socket
import time
from datetime import datetime, timezone
from pathlib import Path

import requests

REPO = Path(__file__).resolve().parent.parent.parent
OUT_PATH = REPO / "data" / "enrichment" / "pet_hotels.json"

# ---- metros ---------------------------------------------------------------

METROS: dict[str, tuple[float, float, float, float, str]] = {
    "atlanta":   (33.40, -84.85, 34.15, -83.90, "GA"),
    "tampa":     (27.50, -82.90, 28.30, -82.20, "FL"),
    "austin":    (30.00, -98.10, 30.65, -97.40, "TX"),
    "nashville": (35.80, -87.10, 36.45, -86.40, "TN"),
    "asheville": (35.35, -82.85, 35.80, -82.35, "NC"),
}

TARGET_STATES = {"GA", "FL", "TX", "TN", "NC"}


def metro_for(lat: float | None, lng: float | None) -> str | None:
    if lat is None or lng is None:
        return None
    try:
        lat = float(lat)
        lng = float(lng)
    except (TypeError, ValueError):
        return None
    for name, (s, w, n, e, _st) in METROS.items():
        if s <= lat <= n and w <= lng <= e:
            return name
    return None


# (city_lower, state_2letter) -> (metro, approx_lat, approx_lng)
# Used as a fallback when lat/lng are not in the structured data.
METRO_CITY_CENTROIDS: dict[tuple[str, str], tuple[str, float, float]] = {
    # Atlanta (GA)
    ("atlanta", "GA"):          ("atlanta", 33.7490, -84.3880),
    ("sandy springs", "GA"):    ("atlanta", 33.9304, -84.3733),
    ("roswell", "GA"):          ("atlanta", 34.0232, -84.3616),
    ("alpharetta", "GA"):       ("atlanta", 34.0754, -84.2941),
    ("marietta", "GA"):         ("atlanta", 33.9526, -84.5499),
    ("n.e. marietta", "GA"):    ("atlanta", 33.9526, -84.5499),
    ("ne marietta", "GA"):      ("atlanta", 33.9526, -84.5499),
    ("decatur", "GA"):          ("atlanta", 33.7748, -84.2963),
    ("duluth", "GA"):           ("atlanta", 34.0029, -84.1446),
    ("smyrna", "GA"):           ("atlanta", 33.8839, -84.5144),
    ("dunwoody", "GA"):         ("atlanta", 33.9462, -84.3346),
    ("johns creek", "GA"):      ("atlanta", 34.0289, -84.1986),
    ("peachtree corners", "GA"):("atlanta", 33.9700, -84.2216),
    ("kennesaw", "GA"):         ("atlanta", 34.0234, -84.6155),
    ("lawrenceville", "GA"):    ("atlanta", 33.9562, -83.9880),
    ("brookhaven", "GA"):       ("atlanta", 33.8651, -84.3365),
    ("cumming", "GA"):          ("atlanta", 34.2073, -84.1402),
    ("acworth", "GA"):          ("atlanta", 34.0661, -84.6777),
    ("powder springs", "GA"):   ("atlanta", 33.8595, -84.6838),
    ("austell", "GA"):          ("atlanta", 33.8132, -84.6352),
    ("tucker", "GA"):           ("atlanta", 33.8545, -84.2171),
    ("stone mountain", "GA"):   ("atlanta", 33.8081, -84.1702),
    ("lilburn", "GA"):          ("atlanta", 33.8901, -84.1430),
    ("norcross", "GA"):         ("atlanta", 33.9412, -84.2135),
    ("doraville", "GA"):        ("atlanta", 33.8973, -84.2707),
    ("buford", "GA"):           ("atlanta", 34.1206, -83.9882),
    ("suwanee", "GA"):          ("atlanta", 34.0515, -84.0714),
    ("mableton", "GA"):         ("atlanta", 33.8187, -84.5777),
    ("canton", "GA"):           ("atlanta", 34.2368, -84.4908),
    ("woodstock", "GA"):        ("atlanta", 34.1015, -84.5194),
    ("milton", "GA"):           ("atlanta", 34.1320, -84.3005),
    ("conyers", "GA"):          ("atlanta", 33.6679, -84.0177),
    ("lithia springs", "GA"):   ("atlanta", 33.7998, -84.6532),
    # Tampa (FL)
    ("tampa", "FL"):            ("tampa", 27.9506, -82.4572),
    ("st. petersburg", "FL"):   ("tampa", 27.7676, -82.6403),
    ("st petersburg", "FL"):    ("tampa", 27.7676, -82.6403),
    ("saint petersburg", "FL"): ("tampa", 27.7676, -82.6403),
    ("clearwater", "FL"):       ("tampa", 27.9659, -82.8001),
    ("brandon", "FL"):           ("tampa", 27.9378, -82.2859),
    ("largo", "FL"):             ("tampa", 27.9095, -82.7873),
    ("riverview", "FL"):         ("tampa", 27.8659, -82.3265),
    ("wesley chapel", "FL"):     ("tampa", 28.2397, -82.3275),
    ("temple terrace", "FL"):    ("tampa", 28.0353, -82.3895),
    ("carrollwood", "FL"):       ("tampa", 28.0506, -82.5021),
    ("lutz", "FL"):              ("tampa", 28.1506, -82.4617),
    ("ruskin", "FL"):            ("tampa", 27.7203, -82.4326),
    ("apollo beach", "FL"):      ("tampa", 27.7700, -82.4020),
    ("land o lakes", "FL"):      ("tampa", 28.2189, -82.4615),
    ("valrico", "FL"):           ("tampa", 27.9417, -82.2379),
    ("pinellas park", "FL"):     ("tampa", 27.8428, -82.6995),
    ("seminole", "FL"):          ("tampa", 27.8395, -82.7901),
    ("palm harbor", "FL"):       ("tampa", 28.0780, -82.7637),
    ("dunedin", "FL"):           ("tampa", 28.0198, -82.7873),
    ("oldsmar", "FL"):           ("tampa", 28.0342, -82.6651),
    ("safety harbor", "FL"):     ("tampa", 27.9903, -82.6929),
    ("odessa", "FL"):            ("tampa", 28.1950, -82.5887),
    # Austin (TX)
    ("austin", "TX"):            ("austin", 30.2672, -97.7431),
    ("round rock", "TX"):        ("austin", 30.5083, -97.6789),
    ("cedar park", "TX"):        ("austin", 30.5052, -97.8203),
    ("pflugerville", "TX"):      ("austin", 30.4394, -97.6200),
    ("georgetown", "TX"):        ("austin", 30.6333, -97.6780),
    ("leander", "TX"):           ("austin", 30.5788, -97.8531),
    ("kyle", "TX"):              ("austin", 29.9893, -97.8772),
    ("buda", "TX"):              ("austin", 30.0852, -97.8406),
    ("bee cave", "TX"):          ("austin", 30.3085, -97.9481),
    ("lakeway", "TX"):           ("austin", 30.3641, -97.9753),
    ("manor", "TX"):             ("austin", 30.3404, -97.5569),
    ("dripping springs", "TX"):  ("austin", 30.1902, -98.0867),
    ("westlake hills", "TX"):    ("austin", 30.2918, -97.8050),
    ("west lake hills", "TX"):   ("austin", 30.2918, -97.8050),
    # Nashville (TN)
    ("nashville", "TN"):         ("nashville", 36.1627, -86.7816),
    ("franklin", "TN"):          ("nashville", 35.9251, -86.8689),
    ("brentwood", "TN"):         ("nashville", 35.9973, -86.7828),
    ("hendersonville", "TN"):    ("nashville", 36.3047, -86.6200),
    ("mt juliet", "TN"):         ("nashville", 36.1998, -86.5186),
    ("mount juliet", "TN"):      ("nashville", 36.1998, -86.5186),
    ("mt. juliet", "TN"):        ("nashville", 36.1998, -86.5186),
    ("gallatin", "TN"):          ("nashville", 36.3881, -86.4467),
    ("antioch", "TN"):           ("nashville", 36.0592, -86.6717),
    ("goodlettsville", "TN"):    ("nashville", 36.3231, -86.7133),
    ("nolensville", "TN"):       ("nashville", 35.9534, -86.6691),
    # Asheville (NC)
    ("asheville", "NC"):         ("asheville", 35.5951, -82.5515),
    ("arden", "NC"):             ("asheville", 35.4767, -82.5165),
    ("fletcher", "NC"):          ("asheville", 35.4314, -82.5001),
    ("black mountain", "NC"):    ("asheville", 35.6174, -82.3214),
    ("weaverville", "NC"):       ("asheville", 35.6968, -82.5610),
    ("candler", "NC"):           ("asheville", 35.5383, -82.6959),
    ("swannanoa", "NC"):         ("asheville", 35.6009, -82.4001),
}


def metro_for_city_state(city: str, state: str) -> tuple[str | None, float, float]:
    if not city or not state:
        return None, 0.0, 0.0
    key = (city.strip().lower(), state.strip().upper())
    hit = METRO_CITY_CENTROIDS.get(key)
    if hit and hit[0]:
        return hit
    return None, 0.0, 0.0


# ---- shared utilities -----------------------------------------------------

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/127.0.0.0 Safari/537.36"
)

# global connect-socket timeout safety
socket.setdefaulttimeout(15)

HOTEL_NAME_RE = re.compile(r"\b(hotel|resort|inn|lodge|spa)\b", re.IGNORECASE)


def now_utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def make_id(name: str, address: str) -> str:
    key = f"{(name or '').strip()}|{(address or '').strip()}".lower()
    return hashlib.sha256(key.encode("utf-8")).hexdigest()[:16]


def normalize_phone(raw: str | None) -> str | None:
    if not raw:
        return None
    digits = re.sub(r"\D+", "", str(raw))
    if not digits:
        return None
    if len(digits) == 10:
        return f"+1{digits}"
    if len(digits) == 11 and digits.startswith("1"):
        return f"+{digits}"
    return str(raw).strip() or None


def http_get(url: str, timeout: int = 10, retries: int = 1,
             accept: str = "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"
             ) -> str | None:
    headers = {
        "User-Agent": USER_AGENT,
        "Accept": accept,
        "Accept-Language": "en-US,en;q=0.5",
    }
    last_err = None
    rtimeout = (min(timeout, 6), timeout)
    for attempt in range(retries + 1):
        try:
            r = requests.get(url, headers=headers, timeout=rtimeout, allow_redirects=True)
            if r.status_code == 200:
                return r.text
            last_err = f"HTTP {r.status_code}"
        except Exception as e:
            last_err = str(e)
        if attempt < retries:
            time.sleep(0.4)
    print(f"[http] giveup {url}: {last_err}", flush=True)
    return None


# ---- JSON-LD parsing ------------------------------------------------------

LD_RE = re.compile(
    r'<script[^>]*type=["\']application/ld\+json["\'][^>]*>(.*?)</script>',
    re.DOTALL | re.IGNORECASE,
)


def _sanitize_json(blob: str) -> str:
    out = []
    in_str = False
    esc = False
    for ch in blob:
        if not in_str:
            if ch == '"':
                in_str = True
            out.append(ch)
        else:
            if esc:
                out.append(ch)
                esc = False
            elif ch == "\\":
                out.append(ch)
                esc = True
            elif ch == '"':
                in_str = False
                out.append(ch)
            elif ch in ("\n", "\r"):
                out.append("\\n")
            elif ch == "\t":
                out.append("\\t")
            else:
                out.append(ch)
    return "".join(out)


def extract_ld_json(html: str) -> list:
    out: list = []
    for m in LD_RE.finditer(html or ""):
        blob = m.group(1).strip()
        try:
            obj = json.loads(blob)
        except Exception:
            try:
                obj = json.loads(_sanitize_json(blob))
            except Exception:
                continue
        if isinstance(obj, list):
            out.extend(obj)
        else:
            out.append(obj)
    return out


def _walk(obj, visit):
    if isinstance(obj, dict):
        visit(obj)
        for v in obj.values():
            _walk(v, visit)
    elif isinstance(obj, list):
        for v in obj:
            _walk(v, visit)


def find_local_business(objs: list) -> dict | None:
    """Prefer LocalBusiness / lodging-style with a street address and geo/lat."""
    lb_candidates: list[dict] = []
    addr_candidates: list[dict] = []

    def visit(d: dict) -> None:
        t = d.get("@type")
        ts = [str(x).lower() for x in t] if isinstance(t, list) else [str(t).lower()] if t else []
        addr = d.get("address")
        has_address = isinstance(addr, dict) and bool(addr.get("streetAddress"))
        has_name = bool(d.get("name"))
        if not has_address or not has_name:
            return
        want_lb = any(
            tt in ("localbusiness", "lodgingbusiness", "hotel", "petstore", "store",
                   "animalshelter", "veterinarycare", "pethospital")
            or "business" in tt
            for tt in ts
        )
        if want_lb:
            lb_candidates.append(d)
        else:
            addr_candidates.append(d)

    for o in objs:
        _walk(o, visit)
    return (lb_candidates or addr_candidates)[0] if (lb_candidates or addr_candidates) else None


def _to_float(v):
    if v is None:
        return None
    try:
        s = str(v).strip().rstrip(";")
        if s == "":
            return None
        return float(s)
    except (TypeError, ValueError):
        return None


def place_to_parts(obj: dict) -> dict | None:
    if not obj:
        return None
    name = str(obj.get("name") or "").strip()
    addr = obj.get("address") or {}
    if not isinstance(addr, dict):
        return None
    street = str(addr.get("streetAddress") or "").strip()
    city = str(addr.get("addressLocality") or "").strip()
    state = str(addr.get("addressRegion") or "").strip()
    zip_ = str(addr.get("postalCode") or "").strip()
    geo = obj.get("geo") or {}
    lat = None
    lng = None
    if isinstance(geo, dict):
        lat = geo.get("latitude")
        lng = geo.get("longitude")
    if lat is None:
        lat = obj.get("latitude")
    if lng is None:
        lng = obj.get("longitude")
    lat = _to_float(lat)
    lng = _to_float(lng)
    phone = (
        obj.get("telephone") or obj.get("telePhone") or obj.get("phone") or None
    )
    if isinstance(phone, dict):
        phone = phone.get("digits") or phone.get("text") or None
    url = obj.get("url") or None
    if not name:
        return None
    return {
        "name": name,
        "street": street,
        "city": city,
        "state": state,
        "zip": zip_,
        "lat": lat,
        "lng": lng,
        "phone": phone,
        "url": url,
    }


STATE_TWO = {
    "alabama": "AL", "alaska": "AK", "arizona": "AZ", "arkansas": "AR",
    "california": "CA", "colorado": "CO", "connecticut": "CT", "delaware": "DE",
    "florida": "FL", "georgia": "GA", "hawaii": "HI", "idaho": "ID",
    "illinois": "IL", "indiana": "IN", "iowa": "IA", "kansas": "KS",
    "kentucky": "KY", "louisiana": "LA", "maine": "ME", "maryland": "MD",
    "massachusetts": "MA", "michigan": "MI", "minnesota": "MN", "mississippi": "MS",
    "missouri": "MO", "montana": "MT", "nebraska": "NE", "nevada": "NV",
    "new hampshire": "NH", "new jersey": "NJ", "new mexico": "NM", "new york": "NY",
    "north carolina": "NC", "north dakota": "ND", "ohio": "OH", "oklahoma": "OK",
    "oregon": "OR", "pennsylvania": "PA", "rhode island": "RI",
    "south carolina": "SC", "south dakota": "SD", "tennessee": "TN", "texas": "TX",
    "utah": "UT", "vermont": "VT", "virginia": "VA", "washington": "WA",
    "west virginia": "WV", "wisconsin": "WI", "wyoming": "WY",
}


def two_letter(name_or_code: str) -> str:
    if not name_or_code:
        return ""
    s = name_or_code.strip().lower().replace("-", " ")
    if len(s) == 2:
        return s.upper()
    return STATE_TWO.get(s, name_or_code.strip().upper() if len(name_or_code) == 2 else "")


# ---- listing builder ------------------------------------------------------

def build_listing(
    *,
    name: str,
    address: str,
    city: str,
    state: str,
    zip_: str,
    lat: float | None,
    lng: float | None,
    metro: str,
    source: str,              # "osm" or "chain_locator"
    source_chain: str | None,  # chain key when source=chain_locator
    source_id: str,
    phone: str | None = None,
    website: str | None = None,
    email: str | None = None,
    subcategories: list[str] | None = None,
    hours: dict | None = None,
) -> dict:
    listing = {
        "id": make_id(name, address or (f"{lat},{lng}" if lat is not None else "")),
        "name": name.strip(),
        "category": "pet_hotel",
        "address": (address or "").strip(),
        "city": (city or "").strip(),
        "state": (state or "").strip(),
        "zip": (zip_ or "").strip(),
        "metro": metro,
        "lat": float(lat) if lat is not None else None,
        "lng": float(lng) if lng is not None else None,
        "sources": ["osm"] if source == "osm" else ["chain_locator"],
        "sourceIds": (
            {"osm": source_id}
            if source == "osm"
            else {"chain": source_chain or "", "storeId": source_id}
        ),
        "lastSeenAt": now_utc(),
        "claimed": False,
    }
    phone_n = normalize_phone(phone)
    if phone_n:
        listing["phone"] = phone_n
    if website:
        listing["website"] = website.strip()
    if email:
        listing["email"] = email.strip()
    if subcategories:
        listing["subcategories"] = subcategories
    if hours:
        listing["hours"] = hours
    return listing


# ---- OSM Overpass ---------------------------------------------------------

OVERPASS_MIRRORS = [
    "https://overpass-api.de/api/interpreter",
    "https://overpass.kumi.systems/api/interpreter",
]
OSM_TIMEOUT = 30
OSM_SERVER_TIMEOUT = 25


def _overpass_query(bbox: tuple[float, float, float, float]) -> str:
    s, w, n, e = bbox
    b = f"({s},{w},{n},{e})"
    parts = []
    for kind in ("node", "way", "relation"):
        parts.append(f'  {kind}["amenity"="animal_boarding"]{b};')
    body = "\n".join(parts)
    return f"[out:json][timeout:{OSM_SERVER_TIMEOUT}];\n(\n{body}\n);\nout center tags;"


def _post_overpass(query: str) -> dict | None:
    last_err = None
    for url in OVERPASS_MIRRORS:
        try:
            r = requests.post(
                url,
                data={"data": query},
                timeout=OSM_TIMEOUT,
                headers={"User-Agent": "fetchfiles-directory/1.0 (pet_hotels pass)"},
            )
            if r.status_code == 200:
                return r.json()
            print(f"[osm] HTTP {r.status_code} from {url}", flush=True)
            last_err = f"HTTP {r.status_code}"
            time.sleep(1 if r.status_code not in (429, 502, 503, 504) else 2)
        except requests.exceptions.Timeout as e:
            print(f"[osm] timeout {url}: {e}", flush=True)
            last_err = f"timeout: {e}"
            time.sleep(1)
        except Exception as e:
            print(f"[osm] failed {url}: {e}", flush=True)
            last_err = str(e)
            time.sleep(1)
    print(f"[osm] all mirrors failed: {last_err}", flush=True)
    return None


def _osm_compose_address(tags: dict) -> str:
    housenum = (tags.get("addr:housenumber") or "").strip()
    street = (tags.get("addr:street") or "").strip()
    unit = (tags.get("addr:unit") or "").strip()
    parts: list[str] = []
    if housenum and street:
        parts.append(f"{housenum} {street}")
    elif street:
        parts.append(street)
    if unit:
        parts.append(f"Unit {unit}")
    return ", ".join(parts).strip()


def _osm_phone(tags: dict) -> str | None:
    for k in ("phone", "contact:phone"):
        v = tags.get(k)
        if v:
            return v
    return None


def _osm_website(tags: dict) -> str | None:
    for k in ("website", "contact:website", "url"):
        v = tags.get(k)
        if v:
            return v.strip()
    return None


def _osm_email(tags: dict) -> str | None:
    for k in ("email", "contact:email"):
        v = tags.get(k)
        if v:
            return v.strip()
    return None


def _osm_hours(tags: dict) -> dict | None:
    v = tags.get("opening_hours")
    if not v:
        return None
    return {"raw": v}


def _osm_is_hotel(tags: dict) -> bool:
    """Premium-boarding signal:
       - name contains hotel|resort|inn|lodge|spa, OR
       - has tourism=* alongside amenity=animal_boarding.
    """
    name = (tags.get("name") or "").strip()
    if name and HOTEL_NAME_RE.search(name):
        return True
    if tags.get("tourism"):
        return True
    return False


def fetch_osm() -> list[dict]:
    rows: list[dict] = []
    seen_ids: set[str] = set()
    counts: dict[str, int] = {m: 0 for m in METROS}

    for metro, info in METROS.items():
        s, w, n, e, default_state = info
        bbox = (s, w, n, e)
        print(f"[osm] {metro} bbox={bbox}", flush=True)
        data = _post_overpass(_overpass_query(bbox))
        if not data:
            print(f"[osm] retrying {metro} after 5s", flush=True)
            time.sleep(5)
            data = _post_overpass(_overpass_query(bbox))
        if not data:
            print(f"[osm] no data for {metro}", flush=True)
            continue
        elements = data.get("elements") or []
        print(f"[osm] {metro} raw elements (amenity=animal_boarding): {len(elements)}", flush=True)

        for el in elements:
            tags = el.get("tags") or {}
            name = (tags.get("name") or "").strip()
            if not name:
                continue
            if not _osm_is_hotel(tags):
                continue

            if el.get("type") == "node":
                lat, lng = el.get("lat"), el.get("lon")
            else:
                c = el.get("center") or {}
                lat, lng = c.get("lat"), c.get("lon")

            address = _osm_compose_address(tags)
            if not address and (lat is None or lng is None):
                continue

            city = (tags.get("addr:city") or "").strip()
            zip_ = (tags.get("addr:postcode") or "").strip()
            state = (tags.get("addr:state") or default_state).strip() or default_state

            listing = build_listing(
                name=name,
                address=address,
                city=city,
                state=state,
                zip_=zip_,
                lat=lat,
                lng=lng,
                metro=metro,
                source="osm",
                source_chain=None,
                source_id=f"{el.get('type')}/{el.get('id')}",
                phone=_osm_phone(tags),
                website=_osm_website(tags),
                email=_osm_email(tags),
                hours=_osm_hours(tags),
            )
            if listing["id"] in seen_ids:
                continue
            seen_ids.add(listing["id"])
            rows.append(listing)
            counts[metro] += 1

        time.sleep(1.5)

    print(f"[osm] total kept: {len(rows)} -- per-metro {counts}", flush=True)
    return rows


# ---- Chain: Wag Hotels ----------------------------------------------------
# Wag Hotels is a React SPA; the main bundle has only CA addresses. Skip.
def chain_wag_hotels(deadline: float) -> list[dict]:
    chain = "wag_hotels"
    # Pull JS bundle and scan for target-state city markers as a sanity check.
    html = http_get("https://waghotels.com/", timeout=10, retries=0)
    if html is None:
        print(f"[{chain}] skipped: homepage unreachable", flush=True)
        return []
    # Try to pull bundle; if we see any target city, we'd need a richer scraper.
    js_url = None
    m = re.search(r'src="(/assets/index\.[a-f0-9]+\.js)"', html)
    if m:
        js_url = f"https://waghotels.com{m.group(1)}"
    cities_found = set()
    if js_url and time.time() < deadline:
        js = http_get(js_url, timeout=20, retries=0)
        if js:
            for city in ("Atlanta", "Tampa", "Austin", "Nashville", "Asheville",
                         "Marietta", "Franklin", "Brentwood"):
                if city in js:
                    cities_found.add(city)
    print(f"[{chain}] target-city hits in JS bundle: {sorted(cities_found) or '(none)'}", flush=True)
    # As of this pass, Wag Hotels operates in CA only.
    return []


# ---- Chain: K9 Resorts ----------------------------------------------------

def chain_k9_resorts(deadline: float) -> list[dict]:
    chain = "k9_resorts"
    base = "https://www.k9resorts.com"
    # Limit to the states we actually care about.
    target_state_slugs = ("georgia", "florida", "texas", "tennessee", "north-carolina")
    loc_slugs: set[str] = set()

    for state_slug in target_state_slugs:
        if time.time() > deadline:
            break
        html = http_get(f"{base}/locations/{state_slug}/", timeout=12, retries=1)
        if not html:
            continue
        # Per-location links are anchor tags "/<city-slug>/" inside the cards.
        # Match hrefs that are single-segment slugs (no additional path).
        for m in re.finditer(r'href="/([a-z0-9-]+)/"', html):
            slug = m.group(1)
            # Filter out navigation/content pages.
            if slug in {
                "about-us", "contact-us", "franchise", "locations", "doggie-daycare",
                "why-choose-us", "privacy-policy", "sitemap", "awards", "accessibility",
                "articles", "pamper-package", "summercamp", "customer-testimonial-videos",
                "luxury-boarding", "terms-and-conditions", "comingsoon",
            }:
                continue
            if slug.startswith("location") or slug.startswith("franchise"):
                continue
            loc_slugs.add(slug)

    print(f"[{chain}] {len(loc_slugs)} candidate location slugs", flush=True)

    rows: list[dict] = []
    seen: set[str] = set()
    for slug in sorted(loc_slugs):
        if time.time() > deadline:
            print(f"[{chain}] deadline reached", flush=True)
            break
        url = f"{base}/{slug}/"
        html = http_get(url, timeout=10, retries=1)
        if not html:
            continue
        objs = extract_ld_json(html)
        biz = find_local_business(objs)
        parts = place_to_parts(biz) if biz else None
        if not parts:
            continue
        state2 = two_letter(parts["state"]) or parts["state"]
        if state2 not in TARGET_STATES:
            continue

        lat, lng = parts["lat"], parts["lng"]
        metro = metro_for(lat, lng) if (lat is not None and lng is not None) else None
        if not metro:
            m_hit, clat, clng = metro_for_city_state(parts["city"], state2)
            if m_hit:
                metro = m_hit
                if lat is None or lng is None:
                    lat, lng = clat, clng
        if not metro:
            continue

        name = parts["name"] or "K9 Resorts Luxury Pet Hotel"
        listing = build_listing(
            name=name,
            address=parts["street"],
            city=parts["city"],
            state=state2,
            zip_=parts["zip"],
            lat=lat,
            lng=lng,
            metro=metro,
            source="chain_locator",
            source_chain=chain,
            source_id=slug,
            phone=parts["phone"],
            website=url,
        )
        if listing["id"] in seen:
            continue
        seen.add(listing["id"])
        rows.append(listing)
    print(f"[{chain}] kept {len(rows)} in target metros", flush=True)
    return rows


# ---- Chain: Hounds Town USA ----------------------------------------------

def _ht_parse_state_page(html: str, default_state: str) -> list[dict]:
    """Pull each location card: address, city, state, zip, phone, location URL."""
    results: list[dict] = []
    # The pattern repeats: a phone href + address block.  Use the href of the
    # booking/detail link as the anchor, then grab preceding context.
    # We parse cards by finding each /locations/<slug>/ link and extracting
    # the nearest phone and address up to that point.
    # Split HTML at each "/locations/<slug>/"  -- segments between hold card.
    card_re = re.compile(
        r'href="(https?://houndstownusa\.com/locations/([a-z0-9-]+)/)"[^>]*>(?:[^<]|<(?!/a>)[^<]*)*?</a>',
        re.IGNORECASE,
    )
    # Grab all location slug occurrences + their positions.
    positions = [(m.start(), m.group(1)) for m in re.finditer(
        r'href="https?://houndstownusa\.com/locations/([a-z0-9-]+)/"', html)]
    if not positions:
        return results
    # Split html at those positions into coarse chunks.
    # Better: just find all addresses and phones and pair with nearest slug.
    addr_pat = re.compile(
        r'(\d{2,5}\s+[A-Z][A-Za-z0-9 ./,-]+?(?:Rd|Road|St|Street|Ave|Avenue|Blvd|Drive|Dr|Way|Hwy|Highway|Pkwy|Parkway|Lane|Ln|Pl|Place|Court|Ct|Cir|Circle|Trail|Trl|Terrace|Trace)[A-Za-z0-9 .,-]*?,\s*[A-Z][A-Za-z .]+?,\s*[A-Z]{2}\s*\d{5})',
    )
    phone_pat = re.compile(r'href="tel:([+\d()\s.-]+)"', re.IGNORECASE)
    # Map from slug to nearest address occurrence by position
    slugs_with_pos: list[tuple[int, str]] = list(dict.fromkeys(
        (p, s) for p, s in positions
    ))
    addrs: list[tuple[int, str]] = [(m.start(), m.group(1)) for m in addr_pat.finditer(html)]
    phones: list[tuple[int, str]] = [(m.start(), m.group(1)) for m in phone_pat.finditer(html)]
    if not slugs_with_pos or not addrs:
        return results
    # Dedupe slugs (first occurrence wins)
    seen_slugs = set()
    unique_slugs: list[tuple[int, str]] = []
    for pos, sl in slugs_with_pos:
        if sl in seen_slugs:
            continue
        seen_slugs.add(sl)
        unique_slugs.append((pos, sl))
    # Dedupe addrs (first occurrence wins)
    seen_addrs: set[str] = set()
    unique_addrs: list[tuple[int, str]] = []
    for pos, a in addrs:
        key = a.strip().lower()
        if key in seen_addrs:
            continue
        seen_addrs.add(key)
        unique_addrs.append((pos, a))

    # Greedy bipartite pairing: each address assigned to at most one slug.
    # For each slug (in page order), pick the closest NOT-YET-USED address.
    used_addr_idx: set[int] = set()
    for pos, slug in unique_slugs:
        best_idx = None
        best_dist = 10**9
        for i, (apos, addr) in enumerate(unique_addrs):
            if i in used_addr_idx:
                continue
            d = abs(apos - pos)
            if d < best_dist:
                best_dist = d
                best_idx = i
        if best_idx is None:
            continue
        used_addr_idx.add(best_idx)
        apos, addr_full = unique_addrs[best_idx]
        am = re.match(r'(.+?),\s*([A-Z][A-Za-z .]+?),\s*([A-Z]{2})\s*(\d{5})', addr_full.strip())
        if not am:
            continue
        street = am.group(1).strip().rstrip(",")
        city = am.group(2).strip()
        state = am.group(3).strip().upper()
        zip_ = am.group(4).strip()
        # Nearest phone
        phone = None
        pbest = 10**9
        for ppos, p in phones:
            d = abs(ppos - pos)
            if d < pbest:
                pbest = d
                phone = p
        results.append({
            "slug": slug,
            "street": street,
            "city": city,
            "state": state,
            "zip": zip_,
            "phone": phone,
        })
    return results


def chain_hounds_town(deadline: float) -> list[dict]:
    chain = "hounds_town"
    # Use the state-level location pages for each target state.
    target_state_slugs = ("georgia", "florida", "texas", "tennessee", "north-carolina")
    rows: list[dict] = []
    seen: set[str] = set()

    for state_slug in target_state_slugs:
        if time.time() > deadline:
            break
        url = f"https://houndstownusa.com/location/{state_slug}/"
        html = http_get(url, timeout=12, retries=1)
        if not html:
            continue
        state2 = two_letter(state_slug)
        cards = _ht_parse_state_page(html, state2)
        print(f"[{chain}] {state_slug}: parsed {len(cards)} location cards", flush=True)
        for c in cards:
            metro, lat, lng = metro_for_city_state(c["city"], c["state"])
            if not metro:
                continue
            name = f"Hounds Town {c['city']}"
            loc_url = f"https://houndstownusa.com/locations/{c['slug']}/"
            listing = build_listing(
                name=name,
                address=c["street"],
                city=c["city"],
                state=c["state"],
                zip_=c["zip"],
                lat=lat,
                lng=lng,
                metro=metro,
                source="chain_locator",
                source_chain=chain,
                source_id=c["slug"],
                phone=c["phone"],
                website=loc_url,
            )
            if listing["id"] in seen:
                continue
            seen.add(listing["id"])
            rows.append(listing)
    print(f"[{chain}] kept {len(rows)} in target metros", flush=True)
    return rows


# ---- Chain: Best Friends Pet Care -----------------------------------------

def _bf_wpsl_locations(html: str) -> list[dict]:
    """Parse the wpsl-js `locations` JS object embedded in a location page."""
    # Pattern: "locations":[{...}]
    m = re.search(r'"locations"\s*:\s*(\[[^\]]*\])', html)
    if not m:
        return []
    blob = m.group(1)
    try:
        arr = json.loads(blob)
    except Exception:
        return []
    if not isinstance(arr, list):
        return []
    return [x for x in arr if isinstance(x, dict)]


def chain_best_friends(deadline: float) -> list[dict]:
    chain = "best_friends"
    # Get the full list of location slugs from the directory.
    idx = http_get("https://www.bestfriendspetcare.com/locations", timeout=12, retries=1)
    if not idx:
        # try bare domain
        idx = http_get("https://bestfriendspetcare.com/locations/", timeout=12, retries=1)
    if not idx:
        print(f"[{chain}] no data fetched", flush=True)
        return []

    slugs = sorted(set(re.findall(
        r'https?://(?:www\.)?bestfriendspetcare\.com/locations/([a-z0-9-]+)/?', idx)))
    print(f"[{chain}] {len(slugs)} candidate location slugs", flush=True)

    rows: list[dict] = []
    seen: set[str] = set()
    for slug in slugs:
        if time.time() > deadline:
            print(f"[{chain}] deadline reached", flush=True)
            break
        url = f"https://bestfriendspetcare.com/locations/{slug}/"
        html = http_get(url, timeout=10, retries=1)
        if not html:
            continue
        entries = _bf_wpsl_locations(html)
        if not entries:
            continue
        for e in entries:
            state2 = two_letter(e.get("state") or "")
            if state2 not in TARGET_STATES:
                continue
            lat = _to_float(e.get("lat"))
            lng = _to_float(e.get("lng"))
            metro = metro_for(lat, lng) if lat is not None and lng is not None else None
            if not metro:
                m_hit, clat, clng = metro_for_city_state(e.get("city") or "", state2)
                if m_hit:
                    metro = m_hit
                    if lat is None or lng is None:
                        lat, lng = clat, clng
            if not metro:
                continue
            # Name: prefer page title ("City, ST") or fall back to store name.
            store_name = str(e.get("store") or "").strip() or slug.replace("-", " ").title()
            name = f"Best Friends Pet Hotel {store_name}"
            street = str(e.get("address") or "").strip()
            addr2 = str(e.get("address2") or "").strip()
            full_street = f"{street}, {addr2}".strip(", ") if addr2 else street
            city = str(e.get("city") or "").strip()
            zip_ = str(e.get("zip") or "").strip()
            listing = build_listing(
                name=name,
                address=full_street,
                city=city,
                state=state2,
                zip_=zip_,
                lat=lat,
                lng=lng,
                metro=metro,
                source="chain_locator",
                source_chain=chain,
                source_id=slug,
                website=url,
            )
            if listing["id"] in seen:
                continue
            seen.add(listing["id"])
            rows.append(listing)
    print(f"[{chain}] kept {len(rows)} in target metros", flush=True)
    return rows


# ---- Chain: Preferred Pets Hotel ------------------------------------------

def chain_preferred_pets(deadline: float) -> list[dict]:
    chain = "preferred_pets"
    # Host does not resolve. Try once; if DNS fails, skip.
    try:
        socket.gethostbyname("preferredpetshotel.com")
    except socket.gaierror:
        print(f"[{chain}] skipped: DNS does not resolve preferredpetshotel.com", flush=True)
        return []
    # If resolution worked, try a fetch (keeps behaviour simple)
    html = http_get("https://preferredpetshotel.com/", timeout=10, retries=0)
    if not html:
        print(f"[{chain}] no data fetched", flush=True)
        return []
    # Single-site brand; if JSON-LD has target-state address, accept.
    objs = extract_ld_json(html)
    biz = find_local_business(objs)
    parts = place_to_parts(biz) if biz else None
    if not parts:
        return []
    state2 = two_letter(parts["state"]) or parts["state"]
    if state2 not in TARGET_STATES:
        return []
    lat, lng = parts["lat"], parts["lng"]
    metro = metro_for(lat, lng) if lat is not None and lng is not None else None
    if not metro:
        mh, clat, clng = metro_for_city_state(parts["city"], state2)
        if mh:
            metro = mh
            if lat is None or lng is None:
                lat, lng = clat, clng
    if not metro:
        return []
    listing = build_listing(
        name=parts["name"] or "Preferred Pets Hotel",
        address=parts["street"],
        city=parts["city"],
        state=state2,
        zip_=parts["zip"],
        lat=lat,
        lng=lng,
        metro=metro,
        source="chain_locator",
        source_chain=chain,
        source_id="home",
        phone=parts["phone"],
        website="https://preferredpetshotel.com",
    )
    print(f"[{chain}] kept 1 in target metros", flush=True)
    return [listing]


# ---- Chain: Morris Animal Inn ---------------------------------------------

def chain_morris_animal(deadline: float) -> list[dict]:
    chain = "morris_animal"
    # Morris Animal Inn operates in NJ only (Morristown, Montville, Warren).
    # Still probe for any target-state presence in case the chain expanded.
    html = http_get("https://morrisanimalinn.com/", timeout=10, retries=0)
    if not html:
        print(f"[{chain}] no data fetched", flush=True)
        return []
    objs = extract_ld_json(html)
    rows: list[dict] = []
    seen: set[str] = set()

    def _visit(d):
        t = d.get("@type") if isinstance(d, dict) else None
        ts = [str(x).lower() for x in t] if isinstance(t, list) else [str(t).lower()] if t else []
        if not ts:
            return
        if not isinstance(d.get("address"), dict):
            return
        parts = place_to_parts(d)
        if not parts:
            return
        state2 = two_letter(parts["state"]) or parts["state"]
        if state2 not in TARGET_STATES:
            return
        lat, lng = parts["lat"], parts["lng"]
        metro = metro_for(lat, lng) if lat is not None and lng is not None else None
        if not metro:
            mh, clat, clng = metro_for_city_state(parts["city"], state2)
            if mh:
                metro = mh
                if lat is None or lng is None:
                    lat, lng = clat, clng
        if not metro:
            return
        listing = build_listing(
            name=parts["name"] or "Morris Animal Inn",
            address=parts["street"],
            city=parts["city"],
            state=state2,
            zip_=parts["zip"],
            lat=lat,
            lng=lng,
            metro=metro,
            source="chain_locator",
            source_chain=chain,
            source_id=parts["city"].lower().replace(" ", "-") or "home",
            phone=parts["phone"],
            website="https://morrisanimalinn.com",
        )
        if listing["id"] not in seen:
            seen.add(listing["id"])
            rows.append(listing)

    for o in objs:
        _walk(o, _visit)
    print(f"[{chain}] kept {len(rows)} in target metros", flush=True)
    return rows


# ---- driver ---------------------------------------------------------------

CHAIN_FNS = [
    ("wag_hotels",     chain_wag_hotels),
    ("k9_resorts",     chain_k9_resorts),
    ("hounds_town",    chain_hounds_town),
    ("best_friends",   chain_best_friends),
    ("preferred_pets", chain_preferred_pets),
    ("morris_animal",  chain_morris_animal),
]

CHAIN_CAP_SEC = 180       # 3 min per chain
OVERALL_CAP_SEC = 25 * 60  # 25 min total


def run() -> None:
    t0 = time.time()
    per_source_counts: dict[str, int] = {}
    skipped: list[str] = []

    # 1. OSM
    osm_rows = fetch_osm()
    per_source_counts["osm"] = len(osm_rows)

    # 2. Chain locators (cap 3 min each)
    chain_rows_all: list[dict] = []
    per_chain_counts: dict[str, int] = {}
    for name, fn in CHAIN_FNS:
        if time.time() - t0 >= OVERALL_CAP_SEC:
            print(f"[main] overall budget exceeded; skipping {name}", flush=True)
            skipped.append(name)
            per_chain_counts[name] = 0
            continue
        print(f"[main] --- running chain: {name} ---", flush=True)
        deadline = time.time() + CHAIN_CAP_SEC
        t_chain = time.time()
        try:
            rows = fn(deadline)
        except Exception as e:
            print(f"[main] {name} raised {type(e).__name__}: {e}", flush=True)
            rows = []
            skipped.append(name)
        per_chain_counts[name] = len(rows)
        chain_rows_all.extend(rows)
        print(f"[main] {name} finished in {time.time() - t_chain:.1f}s -> {len(rows)}", flush=True)

    # Dedupe within-source by id
    all_rows: list[dict] = []
    seen_ids: set[str] = set()
    for row in osm_rows + chain_rows_all:
        if row["id"] in seen_ids:
            continue
        seen_ids.add(row["id"])
        all_rows.append(row)

    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUT_PATH.write_text(json.dumps(all_rows, indent=2))

    # Counts by metro
    counts_by_metro: dict[str, int] = {m: 0 for m in METROS}
    for r in all_rows:
        if r["metro"] in counts_by_metro:
            counts_by_metro[r["metro"]] += 1

    # ---- report ----
    print()
    print("=" * 60)
    print(f"wrote {OUT_PATH} -- {len(all_rows)} pet_hotel listings")
    print("=" * 60)
    print(f"{'metro':<12}{'count':>8}")
    for m, n in counts_by_metro.items():
        print(f"{m:<12}{n:>8}")
    print()
    print(f"{'source':<18}{'count':>8}")
    print(f"{'osm':<18}{per_source_counts['osm']:>8}")
    for name, _ in CHAIN_FNS:
        print(f"{name:<18}{per_chain_counts.get(name, 0):>8}")
    if skipped:
        print(f"skipped: {skipped}")
    print(f"elapsed: {time.time() - t0:.1f}s")


if __name__ == "__main__":
    run()
