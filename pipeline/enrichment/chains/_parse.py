"""Parse structured data (JSON-LD) from fetched HTML pages."""
from __future__ import annotations

import json
import re


LD_RE = re.compile(
    r'<script[^>]*type=["\']application/ld\+json["\'][^>]*>(.*?)</script>',
    re.DOTALL | re.IGNORECASE,
)


def extract_ld_json(html: str) -> list:
    """Return list of parsed JSON-LD objects from HTML."""
    out: list = []
    for m in LD_RE.finditer(html or ""):
        blob = m.group(1).strip()
        # clean trailing commas / stray HTML entities
        try:
            obj = json.loads(blob)
        except Exception:
            # try to salvage: replace single quotes, trim
            try:
                obj = json.loads(blob.strip().rstrip(","))
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
    """Return the first dict that looks like a LocalBusiness/place."""
    wanted_types = {
        "localbusiness", "animalshelter", "veterinarycare", "pethospital",
        "petstore", "store", "place", "dogdaycare", "dayboardingdogs",
        "medicalbusiness", "medicalorganization", "groomer",
    }
    # priority: has address + name
    candidates: list[dict] = []

    def visit(d: dict) -> None:
        t = d.get("@type")
        if isinstance(t, list):
            ts = [str(x).lower() for x in t]
        else:
            ts = [str(t).lower()] if t else []
        has_address = isinstance(d.get("address"), dict)
        has_name = bool(d.get("name"))
        if any(tt in wanted_types or "business" in tt or "medical" in tt or "store" in tt or "hospital" in tt for tt in ts) and has_address and has_name:
            candidates.append(d)
        elif has_address and has_name and ts == []:
            candidates.append(d)

    for o in objs:
        _walk(o, visit)
    return candidates[0] if candidates else None


def place_to_parts(obj: dict) -> dict | None:
    """Extract {name, address, city, state, zip, lat, lng, phone, url}."""
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
    # fallback: obj-level latitude/longitude
    if lat is None:
        lat = obj.get("latitude")
    if lng is None:
        lng = obj.get("longitude")
    # coerce to float if possible; strip stray semicolons
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
    # state to 2-letter if possible
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
