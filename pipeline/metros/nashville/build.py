"""Build Nashville metro pet-services listings from OSM + TN vet board.

Usage:
    python -m pipeline.metros.nashville.build
or:
    python pipeline/metros/nashville/build.py

Writes data/by-metro/nashville.json.
"""
from __future__ import annotations

import hashlib
import json
import os
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

# Allow running as a script.
_HERE = Path(__file__).resolve().parent
_REPO = _HERE.parents[2]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from pipeline.metros.nashville.fetch import fetch_osm, fetch_tn_vet_board  # noqa: E402

try:
    from rapidfuzz import fuzz  # type: ignore
except ImportError:  # pragma: no cover
    # Lightweight fallback: Levenshtein-based ratio on the 0..100 scale.
    # This matches rapidfuzz.fuzz.ratio() semantics closely enough for the
    # ≥88 dedupe threshold used here. Only engaged if rapidfuzz is absent.
    class _FuzzFallback:
        @staticmethod
        def ratio(a: str, b: str) -> float:
            if not a and not b:
                return 100.0
            if not a or not b:
                return 0.0
            # Classic Levenshtein distance, O(len(a)*len(b)).
            la, lb = len(a), len(b)
            if la < lb:
                a, b = b, a
                la, lb = lb, la
            prev = list(range(lb + 1))
            for i in range(1, la + 1):
                curr = [i] + [0] * lb
                ca = a[i - 1]
                for j in range(1, lb + 1):
                    cost = 0 if ca == b[j - 1] else 1
                    curr[j] = min(
                        curr[j - 1] + 1,
                        prev[j] + 1,
                        prev[j - 1] + cost,
                    )
                prev = curr
            dist = prev[lb]
            return 100.0 * (1.0 - dist / max(la, 1))

    fuzz = _FuzzFallback()  # type: ignore


METRO = "nashville"
STATE = "TN"
OUT_PATH = _REPO / "data" / "by-metro" / "nashville.json"


def _now_iso() -> str:
    # ISO-8601 UTC, seconds resolution.
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _stable_id(name: str, address: str) -> str:
    key = f"{name}|{address}"
    return hashlib.sha256(key.encode("utf-8")).hexdigest()[:16]


def _clean_phone(raw: Optional[str]) -> Optional[str]:
    if not raw:
        return None
    digits = re.sub(r"\D", "", raw)
    if not digits:
        return None
    if len(digits) == 10:
        return f"+1{digits}"
    if len(digits) == 11 and digits.startswith("1"):
        return f"+{digits}"
    return raw.strip() or None


def _norm_phone(p: Optional[str]) -> Optional[str]:
    if not p:
        return None
    d = re.sub(r"\D", "", p)
    if len(d) == 11 and d.startswith("1"):
        d = d[1:]
    return d if len(d) == 10 else d or None


def _norm_name(name: str) -> str:
    return re.sub(r"[^a-z0-9]+", " ", (name or "").lower()).strip()


def _haversine_m(a: tuple[float, float], b: tuple[float, float]) -> float:
    import math
    lat1, lon1 = a
    lat2, lon2 = b
    r = 6371000.0
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dl = math.radians(lon2 - lon1)
    x = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dl / 2) ** 2
    return 2 * r * math.asin(math.sqrt(x))


def _address_from_tags(tags: Dict[str, str]) -> str:
    parts: List[str] = []
    hnum = tags.get("addr:housenumber")
    street = tags.get("addr:street")
    if hnum and street:
        parts.append(f"{hnum} {street}")
    elif street:
        parts.append(street)
    unit = tags.get("addr:unit")
    if unit:
        parts.append(f"Unit {unit}")
    return ", ".join(parts)


def osm_to_listing(raw: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    tags = raw.get("tags") or {}
    name = (tags.get("name") or "").strip()
    if not name:
        return None
    address = _address_from_tags(tags)
    city = (tags.get("addr:city") or "").strip()
    zip_ = (tags.get("addr:postcode") or "").strip()
    phone = _clean_phone(tags.get("phone") or tags.get("contact:phone"))
    website = (tags.get("website") or tags.get("contact:website") or "").strip() or None
    email = (tags.get("email") or tags.get("contact:email") or "").strip() or None

    osm_type = raw.get("osm_type")
    osm_id = raw.get("osm_id")
    osm_ref = f"{osm_type}/{osm_id}" if osm_type and osm_id else ""

    listing = {
        "id": _stable_id(name, address),
        "name": name,
        "category": raw["category"],
        "subcategories": None,
        "address": address,
        "city": city,
        "state": STATE,
        "zip": zip_,
        "metro": METRO,
        "lat": raw["lat"],
        "lng": raw["lng"],
        "phone": phone,
        "website": website,
        "email": email,
        "sources": ["osm"],
        "sourceIds": {"osm": osm_ref} if osm_ref else {},
        "lastSeenAt": _now_iso(),
        "hours": None,
        "description": None,
        "tags": None,
        "claimed": False,
        "claimedAt": None,
    }
    return listing


def dedupe(listings: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Dedupe by normalized phone OR (name fuzzy ≥88 AND within 100m)."""
    kept: List[Dict[str, Any]] = []
    for li in listings:
        phone = _norm_phone(li.get("phone"))
        nname = _norm_name(li.get("name") or "")
        lat, lng = li.get("lat"), li.get("lng")
        dup_idx = -1
        for i, ex in enumerate(kept):
            ephone = _norm_phone(ex.get("phone"))
            if phone and ephone and phone == ephone:
                dup_idx = i
                break
            ename = _norm_name(ex.get("name") or "")
            elat, elng = ex.get("lat"), ex.get("lng")
            if nname and ename and fuzz.ratio(nname, ename) >= 88:
                if (
                    lat is not None and lng is not None
                    and elat is not None and elng is not None
                    and _haversine_m((lat, lng), (elat, elng)) <= 100.0
                ):
                    dup_idx = i
                    break
        if dup_idx < 0:
            kept.append(li)
        else:
            kept[dup_idx] = _merge(kept[dup_idx], li)
    return kept


def _merge(a: Dict[str, Any], b: Dict[str, Any]) -> Dict[str, Any]:
    """Keep earliest lastSeenAt, union sources, prefer longest non-null fields."""
    out = dict(a)
    # Earliest lastSeenAt
    la, lb = a.get("lastSeenAt"), b.get("lastSeenAt")
    if la and lb:
        out["lastSeenAt"] = min(la, lb)
    elif lb and not la:
        out["lastSeenAt"] = lb
    # Union sources
    src = list({*(a.get("sources") or []), *(b.get("sources") or [])})
    out["sources"] = sorted(src)
    # Merge sourceIds
    sid = dict(a.get("sourceIds") or {})
    sid.update(b.get("sourceIds") or {})
    out["sourceIds"] = sid
    # Prefer longest non-null string fields
    for k in ("address", "city", "zip", "phone", "website", "email", "description"):
        av = a.get(k) or ""
        bv = b.get(k) or ""
        out[k] = av if len(av) >= len(bv) else bv
        if out[k] == "":
            out[k] = a.get(k) if a.get(k) is not None else b.get(k)
    return out


def main() -> int:
    print(f"[build] metro={METRO}")
    # 1) OSM
    osm_raw = fetch_osm()
    osm_listings: List[Dict[str, Any]] = []
    for r in osm_raw:
        li = osm_to_listing(r)
        if li:
            osm_listings.append(li)
    osm_ok = True if osm_raw else False
    print(f"[build] osm listings (named): {len(osm_listings)}")

    # 2) TN vet board (best-effort; will likely return [])
    vet_listings: List[Dict[str, Any]] = []
    vet_raw = fetch_tn_vet_board()
    vet_ok = bool(vet_raw)
    for r in vet_raw:
        # Shape is best-effort; our fetcher currently returns [] so this is a no-op.
        vet_listings.append(r)

    # 3) Merge + dedupe
    merged = dedupe(osm_listings + vet_listings)

    # 4) Sort for deterministic output
    merged.sort(key=lambda x: (x["category"], x["name"].lower()))

    # 5) Write
    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(OUT_PATH, "w", encoding="utf-8") as f:
        json.dump(merged, f, indent=2, ensure_ascii=False)
        f.write("\n")

    # 6) Report
    from collections import Counter
    counts = Counter(x["category"] for x in merged)
    print(f"[build] wrote {OUT_PATH} ({len(merged)} listings)")
    print(f"[build] by category: {dict(counts)}")
    print(f"[build] sources: osm={'ok' if osm_ok else 'fail'}, "
          f"tn_vet_board={'ok' if vet_ok else 'skipped'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
