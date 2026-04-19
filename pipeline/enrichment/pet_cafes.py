"""Pet-cafe / pet-bakery enrichment.

Pulls listings for places that make food/treats FOR pets (pet bakeries,
dog-treat cafes) — not places where humans eat with their pets.

Sources:
  1. Chain locators (primary) — Woof Gang Bakery is the only chain that still
     publishes a clean per-location feed with street addresses. Three Dog
     Bakery and Lazy Dog Cookie Co are now CPG brands without owned retail.
     Bocce's Bakery and Wet Noses are Shopify CPG stores. The Barkery and
     Bone Appetit Bakery are regional single-shops (no feed).
  2. OSM name-heuristic sweep via Overpass — shop=bakery with pet-ish names,
     shop=pet with craft=bakery / cuisine=pet, amenity=cafe with pet-ish names.

Output:
  data/enrichment/pet_cafes.json — flat array, category="pet_cafe".
"""
from __future__ import annotations

import hashlib
import json
import re
import time
from datetime import datetime, timezone
from pathlib import Path

import requests


REPO = Path(__file__).resolve().parent.parent.parent
OUT_PATH = REPO / "data" / "enrichment" / "pet_cafes.json"

OVERPASS_MIRRORS = [
    "https://overpass-api.de/api/interpreter",
    "https://overpass.kumi.systems/api/interpreter",
]

OSM_HTTP_TIMEOUT = 30
OSM_SERVER_TIMEOUT = 25

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/127.0.0.0 Safari/537.36"
)

# metro -> (south, west, north, east, default_state)
METROS: dict[str, tuple[float, float, float, float, str]] = {
    "atlanta":   (33.40, -84.85, 34.15, -83.90, "GA"),
    "tampa":     (27.50, -82.90, 28.30, -82.20, "FL"),
    "austin":    (30.00, -98.10, 30.65, -97.40, "TX"),
    "nashville": (35.80, -87.10, 36.45, -86.40, "TN"),
    "asheville": (35.35, -82.85, 35.80, -82.35, "NC"),
}

TARGET_STATES = {"GA", "FL", "TX", "TN", "NC"}

# Name-heuristic tokens (case-insensitive substrings).
NAME_TOKENS = ("dog", "pet", "bark", "pup", "paw", "canine", "woof", "tail")

# False-positive exclusions (human-food).
NAME_FP_EXCLUDE = ("hot dog", "hotdog", "corndog", "corn dog", "buffalo")

# Strong "pet-focused" positive signals.
NAME_STRONG_POS = ("bakery", "treats", "bark", "pup", "paw")


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


def metro_for(lat: float | None, lng: float | None) -> str | None:
    if lat is None or lng is None:
        return None
    try:
        lat = float(lat); lng = float(lng)
    except (TypeError, ValueError):
        return None
    for name, (s, w, n, e, _st) in METROS.items():
        if s <= lat <= n and w <= lng <= e:
            return name
    return None


# ---- HTTP -------------------------------------------------------------------

def http_get(url: str, timeout: int = 10) -> str | None:
    try:
        r = requests.get(
            url,
            headers={"User-Agent": USER_AGENT, "Accept": "*/*"},
            timeout=(min(timeout, 6), timeout),
            allow_redirects=True,
        )
        if r.status_code == 200:
            return r.text
        return None
    except Exception as e:
        print(f"[pet_cafes] fetch fail {url}: {e}", flush=True)
        return None


# ---- Chain: Woof Gang Bakery ------------------------------------------------

WOOF_GANG_SITEMAPS = [
    "https://woofgangbakery.com/location-sitemap1.xml",
    "https://woofgangbakery.com/location-sitemap2.xml",
]


def _wg_enumerate_location_urls() -> list[str]:
    urls: list[str] = []
    pat = re.compile(r"<loc>(https://woofgangbakery\.com/pages/locations/[^<]+)</loc>")
    for sm in WOOF_GANG_SITEMAPS:
        txt = http_get(sm, timeout=15)
        if not txt:
            continue
        for m in pat.finditer(txt):
            urls.append(m.group(1).strip())
    # dedupe, keep order
    seen = set(); out = []
    for u in urls:
        if u in seen:
            continue
        seen.add(u); out.append(u)
    return out


# Address divs look like: <div class="full-address">1504 66th St. N, St. Petersburg, FL, 33710</div>
WG_ADDR_RE = re.compile(
    r'<div class="full-address">\s*([^<]+?)\s*</div>'
)
WG_PHONE_RE = re.compile(r'<div class="phone">\s*<a href="tel:([^"]+)">')
WG_NAME_RE = re.compile(r'<meta\s+property="og:title"\s+content="([^"]+)"')


def _wg_parse_page(html: str) -> dict | None:
    m_addr = WG_ADDR_RE.search(html)
    if not m_addr:
        return None
    addr = m_addr.group(1).strip()
    # Expected pattern: STREET, CITY, ST, ZIP  (commas)
    parts = [p.strip() for p in addr.split(",")]
    if len(parts) < 4:
        return None
    street = parts[0]
    city = parts[1]
    state = parts[2][:2].upper()
    zip_ = re.sub(r"\D+", "", parts[3])[:5]
    phone = None
    m_ph = WG_PHONE_RE.search(html)
    if m_ph:
        phone = m_ph.group(1).strip()
    title = None
    m_t = WG_NAME_RE.search(html)
    if m_t:
        title = m_t.group(1).strip()
    return {
        "street": street,
        "city": city,
        "state": state,
        "zip": zip_,
        "phone": phone,
        "title": title,
    }


def _wg_city_to_metro(city: str, state: str) -> str | None:
    """Map well-known metro cities to the 5 target metros.

    Only used when we lack lat/lng. List derived from addresses seen on
    the sitemap inspection for our 5 target states.
    """
    c = (city or "").strip().lower()
    s = (state or "").strip().upper()
    if s not in TARGET_STATES:
        return None
    # atlanta (GA)
    ATL = {
        "atlanta", "alpharetta", "roswell", "marietta", "smyrna", "sandy springs",
        "dunwoody", "brookhaven", "decatur", "kennesaw", "woodstock", "milton",
        "johns creek", "cumming", "duluth", "peachtree corners", "peachtree city",
        "lawrenceville", "suwanee", "buford", "canton", "tucker", "douglasville",
        "acworth", "powder springs", "stone mountain", "norcross", "chamblee",
        "east point", "college park", "hapeville", "fairburn", "union city",
        "snellville", "grayson", "mableton", "austell", "fayetteville", "newnan",
    }
    # tampa (FL)
    TPA = {
        "tampa", "st. petersburg", "st petersburg", "saint petersburg",
        "clearwater", "brandon", "riverview", "wesley chapel", "lutz",
        "temple terrace", "plant city", "apollo beach", "valrico", "seminole",
        "largo", "pinellas park", "dunedin", "palm harbor", "safety harbor",
        "odessa", "land o' lakes", "land o lakes", "oldsmar", "tarpon springs",
        "new port richey", "port richey", "hudson", "zephyrhills", "ruskin",
        "sun city center", "gibsonton", "belleair bluffs", "belleair",
        "st. pete beach", "st pete beach", "treasure island", "indian rocks beach",
    }
    # austin (TX)
    ATX = {
        "austin", "round rock", "cedar park", "leander", "pflugerville",
        "georgetown", "buda", "kyle", "dripping springs", "bee cave",
        "lakeway", "west lake hills", "lake travis", "manor", "elgin",
        "bastrop", "hutto", "san marcos", "wimberley",
    }
    # nashville (TN)
    BNA = {
        "nashville", "brentwood", "franklin", "murfreesboro", "hendersonville",
        "mount juliet", "smyrna", "la vergne", "lavergne", "gallatin",
        "goodlettsville", "antioch", "hermitage", "madison", "nolensville",
        "spring hill", "thompson's station", "thompsons station", "fairview",
        "white house", "ashland city",
    }
    # asheville (NC)
    AVL = {
        "asheville", "weaverville", "black mountain", "fairview", "arden",
        "fletcher", "hendersonville", "candler", "leicester", "swannanoa",
        "woodfin", "biltmore forest", "biltmore",
    }
    if s == "GA" and c in ATL:
        return "atlanta"
    if s == "FL" and c in TPA:
        return "tampa"
    if s == "TX" and c in ATX:
        return "austin"
    if s == "TN" and c in BNA:
        return "nashville"
    if s == "NC" and c in AVL:
        return "asheville"
    return None


def fetch_woof_gang() -> list[dict]:
    chain = "woof_gang_bakery"
    started = time.time()
    urls = _wg_enumerate_location_urls()
    print(f"[{chain}] enumerated {len(urls)} location URLs")
    listings: list[dict] = []
    seen_ids: set[str] = set()
    for i, url in enumerate(urls):
        # Overall chain cap: 3 minutes.
        if time.time() - started > 180:
            print(f"[{chain}] 3-min cap hit at {i}/{len(urls)}")
            break
        html = http_get(url, timeout=10)
        if not html:
            continue
        # Quick pre-filter: page HTML must contain one of our target states
        # formatted as ", GA," etc. in the full-address line.
        if not any(f", {s}," in html for s in TARGET_STATES):
            continue
        parts = _wg_parse_page(html)
        if not parts:
            continue
        if parts["state"] not in TARGET_STATES:
            continue
        metro = _wg_city_to_metro(parts["city"], parts["state"])
        if not metro:
            continue
        # Use the slug for a clean brand-prefixed name. Example slug:
        # "st-pete" -> "Woof Gang Bakery - St Pete"; the page title is
        # typically "Dog Grooming in X | Woof Gang Bakery & Grooming"
        # which is SEO-flavored rather than a clean brand name.
        store_id = url.rstrip("/").rsplit("/", 1)[-1]
        slug_pretty = store_id.replace("-", " ").title()
        name = f"Woof Gang Bakery - {slug_pretty}"
        listing = {
            "id": make_id(name, parts["street"]),
            "name": name,
            "category": "pet_cafe",
            "address": parts["street"],
            "city": parts["city"],
            "state": parts["state"],
            "zip": parts["zip"],
            "metro": metro,
            "lat": None,
            "lng": None,
            "sources": ["chain_locator"],
            "sourceIds": {"chain": chain, "storeId": store_id},
            "lastSeenAt": now_utc(),
            "claimed": False,
            "website": url,
        }
        ph = normalize_phone(parts["phone"])
        if ph:
            listing["phone"] = ph
        if listing["id"] in seen_ids:
            continue
        seen_ids.add(listing["id"])
        listings.append(listing)
        # polite pacing
        if i % 20 == 19:
            time.sleep(0.2)
    print(f"[{chain}] {len(listings)} listings in target metros (elapsed {time.time()-started:.1f}s)")
    return listings


# ---- Chain: Three Dog Bakery / Lazy Dog Cookie / Bocce's / Wet Noses --------

def fetch_three_dog() -> list[dict]:
    """Three Dog Bakery is now a CPG brand (threedog.com) — no owned
    retail locations; the site has a /where-to-buy/ page for retailers.
    No clean per-location feed. Return []. (Checked sitemap: treat-sitemap.xml,
    no location-sitemap.)"""
    print("[three_dog_bakery] no clean location feed (CPG brand); skipping")
    return []


def fetch_lazy_dog_cookies() -> list[dict]:
    """Lazy Dog Cookie Co is a Shopify CPG store — products + online orders,
    no physical retail. Store-locator page references retailers of their
    product, not owned locations. Skip."""
    print("[lazy_dog_cookies] Shopify CPG, no owned retail; skipping")
    return []


def fetch_bocces_bakery() -> list[dict]:
    """Bocce's Bakery — Shopify CPG, no owned retail."""
    print("[bocces_bakery] Shopify CPG, no owned retail; skipping")
    return []


def fetch_wet_noses() -> list[dict]:
    """Wet Noses — Shopify CPG / wholesale brand, no owned retail."""
    print("[wet_noses] Shopify CPG, no owned retail; skipping")
    return []


# ---- OSM Overpass name-heuristic sweep -------------------------------------

def _build_overpass_query(bbox: tuple[float, float, float, float]) -> str:
    s, w, n, e = bbox
    b = f"({s},{w},{n},{e})"
    # Use Overpass regex on 'name' (case-insensitive) for the name-heuristic
    # cases (shop=bakery, amenity=cafe). For shop=pet + craft/cuisine, no
    # name filter. Regex alternation for tokens.
    tok = "|".join(NAME_TOKENS)
    selectors: list[str] = []
    # shop=bakery with pet-ish name
    selectors.append(f'["shop"="bakery"]["name"~"{tok}",i]')
    # shop=pet with craft=bakery
    selectors.append('["shop"="pet"]["craft"="bakery"]')
    # shop=pet with cuisine=pet
    selectors.append('["shop"="pet"]["cuisine"="pet"]')
    # amenity=cafe with pet-ish name
    selectors.append(f'["amenity"="cafe"]["name"~"{tok}",i]')
    parts: list[str] = []
    for sel in selectors:
        for kind in ("node", "way", "relation"):
            parts.append(f"  {kind}{sel}{b};")
    body = "\n".join(parts)
    return f"[out:json][timeout:{OSM_SERVER_TIMEOUT}];\n(\n{body}\n);\nout center tags;"


def _post_overpass(query: str) -> dict | None:
    last_err = None
    for url in OVERPASS_MIRRORS:
        try:
            r = requests.post(
                url,
                data={"data": query},
                timeout=OSM_HTTP_TIMEOUT,
                headers={"User-Agent": "fetchfiles-directory/1.0 (pet_cafes)"},
            )
            if r.status_code == 200:
                return r.json()
            print(f"[pet_cafes][overpass] HTTP {r.status_code} from {url}")
            last_err = f"HTTP {r.status_code}"
            time.sleep(1)
        except requests.exceptions.Timeout as e:
            print(f"[pet_cafes][overpass] timeout {url}: {e}")
            last_err = f"timeout: {e}"
            time.sleep(1)
        except Exception as e:
            print(f"[pet_cafes][overpass] failed {url}: {e}")
            last_err = str(e)
            time.sleep(1)
    print(f"[pet_cafes][overpass] all mirrors failed: {last_err}")
    return None


def _name_is_pet_focused(name: str) -> tuple[bool, str]:
    """Return (keep, reason). Implements the false-positive filter:
      - exclude if name contains any NAME_FP_EXCLUDE substring (human-food)
      - must have a strong positive signal OR be shop=pet/craft=bakery
        (caller handles the craft case separately).
    Returns "reason" as a short label for logging.
    """
    if not name:
        return False, "empty"
    ln = name.lower()
    for fp in NAME_FP_EXCLUDE:
        if fp in ln:
            return False, f"fp:{fp}"
    # strong positive
    for s in NAME_STRONG_POS:
        if s in ln:
            return True, f"strong:{s}"
    return False, "weak"


def _compose_osm_address(tags: dict) -> str:
    hn = (tags.get("addr:housenumber") or "").strip()
    st = (tags.get("addr:street") or "").strip()
    unit = (tags.get("addr:unit") or "").strip()
    parts: list[str] = []
    if hn and st:
        parts.append(f"{hn} {st}")
    elif st:
        parts.append(st)
    if unit:
        parts.append(f"Unit {unit}")
    return ", ".join(parts).strip()


def _normalize_osm(el: dict, metro: str, default_state: str) -> tuple[dict | None, str]:
    """Return (listing_or_None, filter_reason). filter_reason is empty when kept."""
    tags = el.get("tags") or {}
    name = (tags.get("name") or "").strip()
    if not name:
        return None, "no-name"

    shop = tags.get("shop")
    craft = tags.get("craft")
    cuisine = tags.get("cuisine")
    amenity = tags.get("amenity")

    # Path A: shop=pet with craft=bakery or cuisine=pet — treat as pet bakery
    # without name filter (tag itself is the signal).
    tag_signal = False
    if shop == "pet" and (craft == "bakery" or cuisine == "pet"):
        tag_signal = True

    if not tag_signal:
        # Path B: name heuristic. Apply positive + false-positive filter.
        keep, reason = _name_is_pet_focused(name)
        if not keep:
            return None, reason

    # Coordinates
    if el.get("type") == "node":
        lat = el.get("lat"); lng = el.get("lon")
    else:
        c = el.get("center") or {}
        lat = c.get("lat"); lng = c.get("lon")
    address = _compose_osm_address(tags)
    if not address and (lat is None or lng is None):
        return None, "no-addr-no-coords"

    city = (tags.get("addr:city") or "").strip()
    state = (tags.get("addr:state") or default_state).strip() or default_state
    zip_ = (tags.get("addr:postcode") or "").strip()

    listing: dict = {
        "id": make_id(name, address or f"{lat},{lng}"),
        "name": name,
        "category": "pet_cafe",
        "address": address,
        "city": city,
        "state": state[:2].upper() if state else default_state,
        "zip": zip_,
        "metro": metro,
        "lat": float(lat) if lat is not None else None,
        "lng": float(lng) if lng is not None else None,
        "sources": ["osm"],
        "sourceIds": {"osm": f"{el.get('type')}/{el.get('id')}"},
        "lastSeenAt": now_utc(),
        "claimed": False,
    }
    ph = None
    for k in ("phone", "contact:phone"):
        if tags.get(k):
            ph = normalize_phone(tags[k]); break
    if ph:
        listing["phone"] = ph
    for k in ("website", "contact:website", "url"):
        if tags.get(k):
            listing["website"] = tags[k].strip(); break
    for k in ("email", "contact:email"):
        if tags.get(k):
            listing["email"] = tags[k].strip(); break
    if tags.get("opening_hours"):
        listing["hours"] = {"raw": tags["opening_hours"]}
    subs: list[str] = []
    if shop == "pet" and craft == "bakery":
        subs.append("pet-bakery")
    if amenity == "cafe":
        subs.append("cafe")
    if subs:
        listing["subcategories"] = subs
    return listing, ""


def fetch_osm() -> tuple[list[dict], list[tuple[str, str, str]]]:
    """Return (listings, filtered). filtered is a list of (metro, name, reason)
    for OSM name-hits that were excluded by the false-positive filter."""
    all_listings: list[dict] = []
    filtered: list[tuple[str, str, str]] = []
    seen_ids: set[str] = set()
    for metro, info in METROS.items():
        s, w, n, e, default_state = info
        bbox = (s, w, n, e)
        print(f"[pet_cafes][osm] {metro} bbox={bbox}")
        q = _build_overpass_query(bbox)
        data = _post_overpass(q)
        if not data:
            print(f"[pet_cafes][osm] no data for {metro}")
            continue
        els = data.get("elements", []) or []
        print(f"[pet_cafes][osm] {metro} raw elements: {len(els)}")
        for el in els:
            tags = el.get("tags") or {}
            name = (tags.get("name") or "").strip()
            listing, reason = _normalize_osm(el, metro, default_state)
            if listing is None:
                # only log name-related exclusions that were actually name hits
                # (not missing addr/coords) for the report.
                if reason and reason not in ("no-name", "no-addr-no-coords"):
                    filtered.append((metro, name, reason))
                continue
            if listing["id"] in seen_ids:
                continue
            seen_ids.add(listing["id"])
            all_listings.append(listing)
        time.sleep(2)
    return all_listings, filtered


# ---- Driver -----------------------------------------------------------------

def run() -> None:
    started = time.time()
    overall_cap = 20 * 60  # 20 minutes

    all_listings: list[dict] = []
    source_counts: dict[str, int] = {
        "osm": 0,
        "woof_gang_bakery": 0,
        "three_dog_bakery": 0,
        "lazy_dog_cookies": 0,
        "bocces_bakery": 0,
        "wet_noses": 0,
    }
    metro_counts: dict[str, int] = {m: 0 for m in METROS}

    # --- Chain fetchers ------------------------------------------------------
    chain_fetchers = [
        ("woof_gang_bakery", fetch_woof_gang),
        ("three_dog_bakery", fetch_three_dog),
        ("lazy_dog_cookies", fetch_lazy_dog_cookies),
        ("bocces_bakery",    fetch_bocces_bakery),
        ("wet_noses",        fetch_wet_noses),
    ]
    for label, fn in chain_fetchers:
        if time.time() - started > overall_cap:
            print(f"[pet_cafes] overall 20-min cap hit before {label}")
            break
        try:
            items = fn()
        except Exception as e:
            print(f"[pet_cafes] chain {label} raised: {e}")
            items = []
        for it in items:
            all_listings.append(it)
            source_counts[label] = source_counts.get(label, 0) + 1
            metro_counts[it["metro"]] = metro_counts.get(it["metro"], 0) + 1

    # --- OSM sweep ----------------------------------------------------------
    filtered: list[tuple[str, str, str]] = []
    if time.time() - started <= overall_cap:
        osm_items, filtered = fetch_osm()
        for it in osm_items:
            all_listings.append(it)
            source_counts["osm"] = source_counts.get("osm", 0) + 1
            metro_counts[it["metro"]] = metro_counts.get(it["metro"], 0) + 1

    # --- Write output ------------------------------------------------------
    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUT_PATH.write_text(json.dumps(all_listings, indent=2))

    # --- Report ------------------------------------------------------------
    print()
    print(f"wrote {OUT_PATH} -- {len(all_listings)} listings")
    print()
    print("per-metro counts:")
    for m in METROS:
        print(f"  {m:<10} {metro_counts.get(m, 0)}")
    print()
    print("per-source counts:")
    for k, v in source_counts.items():
        print(f"  {k:<22} {v}")
    print()
    print(f"OSM name-hits filtered as false-positives: {len(filtered)}")
    for metro, name, reason in filtered[:30]:
        print(f"  [{metro}] {name!r} -> {reason}")
    print()
    print(f"elapsed: {time.time()-started:.1f}s")


if __name__ == "__main__":
    run()
