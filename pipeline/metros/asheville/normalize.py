"""Normalize raw OSM/vet-board records into canonical schema for Asheville."""
from __future__ import annotations

import hashlib
import re
from datetime import datetime, timezone


STATE = "NC"
METRO = "asheville"


def _compose_address(tags: dict) -> str:
    parts = []
    housenum = tags.get("addr:housenumber", "").strip()
    street = tags.get("addr:street", "").strip()
    unit = tags.get("addr:unit", "").strip()
    if housenum and street:
        parts.append(f"{housenum} {street}")
    elif street:
        parts.append(street)
    if unit:
        parts.append(f"Unit {unit}")
    return ", ".join(parts).strip()


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


def _phone_digits(phone: str | None) -> str:
    if not phone:
        return ""
    return re.sub(r"\D+", "", phone)


def _website(tags: dict) -> str | None:
    for k in ("website", "contact:website", "url"):
        v = tags.get(k)
        if v:
            return v.strip()
    return None


def _email(tags: dict) -> str | None:
    for k in ("email", "contact:email"):
        v = tags.get(k)
        if v:
            return v.strip()
    return None


def _phone_from_tags(tags: dict) -> str | None:
    for k in ("phone", "contact:phone"):
        v = tags.get(k)
        if v:
            return _normalize_phone(v)
    return None


def _hours(tags: dict) -> dict | None:
    v = tags.get("opening_hours")
    if not v:
        return None
    return {"raw": v}


def _make_id(name: str, address: str) -> str:
    """id = first 16 hex chars of sha256(name + "|" + address)."""
    key = f"{name}|{address}"
    return hashlib.sha256(key.encode("utf-8")).hexdigest()[:16]


def normalize_osm(rec: dict) -> dict | None:
    tags = rec.get("tags", {}) or {}
    name = (tags.get("name") or "").strip()
    if not name:
        return None
    address = _compose_address(tags)
    city = tags.get("addr:city", "").strip()
    zip_ = tags.get("addr:postcode", "").strip()
    state = (tags.get("addr:state") or STATE).strip() or STATE

    listing = {
        "id": _make_id(name, address),
        "name": name,
        "category": rec["category"],
        "address": address,
        "city": city,
        "state": state,
        "zip": zip_,
        "metro": METRO,
        "lat": float(rec["lat"]),
        "lng": float(rec["lng"]),
        "sources": ["osm"],
        "sourceIds": {"osm": f"{rec['osm_type']}/{rec['osm_id']}"},
        "lastSeenAt": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "claimed": False,
    }

    phone = _phone_from_tags(tags)
    if phone:
        listing["phone"] = phone
    website = _website(tags)
    if website:
        listing["website"] = website
    email = _email(tags)
    if email:
        listing["email"] = email
    hours = _hours(tags)
    if hours:
        listing["hours"] = hours

    # subcategories from tags
    subs: list[str] = []
    if tags.get("mobile") == "yes":
        subs.append("mobile")
    if tags.get("pets") == "cats" or tags.get("cat") == "yes":
        subs.append("cat-only")
    if subs:
        listing["subcategories"] = subs

    return listing


# ---------- Dedupe ----------

def _norm_name(name: str) -> str:
    s = (name or "").lower()
    s = re.sub(r"[^a-z0-9]+", " ", s).strip()
    s = re.sub(r"\s+", " ", s)
    return s


def _haversine_m(lat1: float, lng1: float, lat2: float, lng2: float) -> float:
    import math
    R = 6371000.0
    p1 = math.radians(lat1)
    p2 = math.radians(lat2)
    dp = math.radians(lat2 - lat1)
    dl = math.radians(lng2 - lng1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return R * c


def dedupe(listings: list[dict]) -> list[dict]:
    """Dedupe by normalized phone OR (rapidfuzz name ratio >= 88 AND within 100m).

    Merge: keep earliest lastSeenAt, union sources, prefer longest non-null fields.
    """
    try:
        from rapidfuzz import fuzz
    except Exception:
        fuzz = None

    kept: list[dict] = []

    def _merge(a: dict, b: dict) -> dict:
        # keep earliest lastSeenAt
        try:
            ls_a = a.get("lastSeenAt") or ""
            ls_b = b.get("lastSeenAt") or ""
            merged = dict(a) if ls_a <= ls_b or not ls_b else dict(b)
        except Exception:
            merged = dict(a)
        # union sources
        srcs = list(dict.fromkeys((a.get("sources") or []) + (b.get("sources") or [])))
        merged["sources"] = srcs
        # union sourceIds
        src_ids = {}
        src_ids.update(a.get("sourceIds") or {})
        src_ids.update(b.get("sourceIds") or {})
        merged["sourceIds"] = src_ids
        # prefer longest non-null for string fields
        for k in ("name", "address", "city", "state", "zip", "phone", "website", "email"):
            va = a.get(k) or ""
            vb = b.get(k) or ""
            merged[k] = va if len(va) >= len(vb) else vb
        return merged

    for cur in listings:
        match_idx = -1
        cur_phone = _phone_digits(cur.get("phone"))
        cur_name = _norm_name(cur.get("name", ""))
        cur_lat = cur.get("lat")
        cur_lng = cur.get("lng")
        for i, k in enumerate(kept):
            # phone match
            kp = _phone_digits(k.get("phone"))
            if cur_phone and kp and cur_phone == kp:
                match_idx = i
                break
            # name + proximity match
            kn = _norm_name(k.get("name", ""))
            if not cur_name or not kn:
                continue
            if fuzz is not None:
                score = fuzz.ratio(cur_name, kn)
            else:
                score = 100 if cur_name == kn else 0
            if score >= 88:
                if cur_lat is not None and k.get("lat") is not None:
                    d = _haversine_m(cur_lat, cur_lng, k["lat"], k["lng"])
                    if d <= 100:
                        match_idx = i
                        break
        if match_idx >= 0:
            kept[match_idx] = _merge(kept[match_idx], cur)
        else:
            kept.append(cur)
    return kept
