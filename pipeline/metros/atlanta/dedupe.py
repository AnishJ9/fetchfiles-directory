"""Dedupe listings within this metro."""
from __future__ import annotations

import math
import re

try:
    from rapidfuzz import fuzz  # preferred, per requirements.txt
    def _ratio(a: str, b: str) -> float:
        return fuzz.ratio(a, b)
except Exception:  # pragma: no cover - fallback if rapidfuzz unavailable
    from difflib import SequenceMatcher
    def _ratio(a: str, b: str) -> float:
        return 100.0 * SequenceMatcher(None, a, b).ratio()


def _norm_name(s: str) -> str:
    return re.sub(r"[^a-z0-9 ]+", "", (s or "").lower()).strip()


def _phone_digits(s: str | None) -> str:
    if not s:
        return ""
    return re.sub(r"\D+", "", s)


def _haversine_m(a_lat: float, a_lng: float, b_lat: float, b_lng: float) -> float:
    R = 6_371_000.0
    p1 = math.radians(a_lat)
    p2 = math.radians(b_lat)
    dp = math.radians(b_lat - a_lat)
    dl = math.radians(b_lng - a_lng)
    h = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * R * math.asin(math.sqrt(h))


def _prefer_longer(a: str | None, b: str | None) -> str | None:
    if not a:
        return b
    if not b:
        return a
    return a if len(a) >= len(b) else b


def _merge(a: dict, b: dict) -> dict:
    """Merge b into a. Union sources; prefer longer non-null; keep earliest lastSeenAt."""
    out = dict(a)
    # sources union, preserve order from a first
    src = list(dict.fromkeys([*a.get("sources", []), *b.get("sources", [])]))
    out["sources"] = src
    # sourceIds union
    sid = dict(a.get("sourceIds") or {})
    for k, v in (b.get("sourceIds") or {}).items():
        sid.setdefault(k, v)
    out["sourceIds"] = sid

    for field in ("name", "address", "city", "state", "zip", "phone", "website", "email", "description"):
        out[field] = _prefer_longer(a.get(field), b.get(field)) or a.get(field) or b.get(field)

    # earliest lastSeenAt
    la = a.get("lastSeenAt")
    lb = b.get("lastSeenAt")
    if la and lb:
        out["lastSeenAt"] = min(la, lb)
    elif lb and not la:
        out["lastSeenAt"] = lb

    # merge subcategories / tags union
    for field in ("subcategories", "tags"):
        if a.get(field) or b.get(field):
            out[field] = list(dict.fromkeys([*(a.get(field) or []), *(b.get(field) or [])]))

    # hours: keep a's unless missing
    if not a.get("hours") and b.get("hours"):
        out["hours"] = b["hours"]

    # claimed: stays False unless either True
    out["claimed"] = bool(a.get("claimed") or b.get("claimed"))

    return out


def dedupe(listings: list[dict]) -> list[dict]:
    """Dedupe: same normalized phone OR (fuzzy name>=88 AND <100m)."""
    merged: list[dict] = []
    for L in listings:
        target_idx = -1
        lp = _phone_digits(L.get("phone"))
        ln = _norm_name(L.get("name", ""))
        for i, M in enumerate(merged):
            mp = _phone_digits(M.get("phone"))
            if lp and mp and lp == mp:
                target_idx = i
                break
            mn = _norm_name(M.get("name", ""))
            if ln and mn and _ratio(ln, mn) >= 88:
                try:
                    d = _haversine_m(L["lat"], L["lng"], M["lat"], M["lng"])
                except Exception:
                    d = 1e9
                if d <= 100.0:
                    target_idx = i
                    break
        if target_idx < 0:
            merged.append(L)
        else:
            merged[target_idx] = _merge(merged[target_idx], L)
    return merged
