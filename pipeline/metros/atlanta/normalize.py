"""Normalize raw OSM/vet-board records into canonical schema."""
from __future__ import annotations

import hashlib
import re
from datetime import datetime, timezone


STATE = "GA"
METRO = "atlanta"


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
    key = f"{name}|{address}".lower()
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
        "id": _make_id(name, address or f"{rec['lat']},{rec['lng']}"),
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
