"""Shared utilities for chain locator fetchers."""
from __future__ import annotations

import hashlib
import json
import os
import re
from datetime import datetime, timezone
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
CACHE_DIR = REPO_ROOT / "data" / "enrichment" / ".cache"
OUTPUT_PATH = REPO_ROOT / "data" / "enrichment" / "chains.json"

METROS = {
    "atlanta":   (33.40, -84.85, 34.15, -83.90),  # s, w, n, e
    "tampa":     (27.50, -82.90, 28.30, -82.20),
    "austin":    (30.00, -98.10, 30.65, -97.40),
    "nashville": (35.80, -87.10, 36.45, -86.40),
    "asheville": (35.35, -82.85, 35.80, -82.35),
}

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/127.0.0.0 Safari/537.36"
)


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


def metro_for(lat: float, lng: float) -> str | None:
    if lat is None or lng is None:
        return None
    try:
        lat = float(lat)
        lng = float(lng)
    except (TypeError, ValueError):
        return None
    for name, (s, w, n, e) in METROS.items():
        if s <= lat <= n and w <= lng <= e:
            return name
    return None


def cache_write(chain: str, payload) -> None:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    p = CACHE_DIR / f"{chain}.json"
    with p.open("w", encoding="utf-8") as f:
        if isinstance(payload, (dict, list)):
            json.dump(payload, f)
        else:
            f.write(str(payload))


def cache_read(chain: str):
    p = CACHE_DIR / f"{chain}.json"
    if not p.exists():
        return None
    try:
        with p.open("r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


def build_listing(
    *,
    name: str,
    category: str,
    address: str,
    city: str,
    state: str,
    zip_: str,
    lat: float,
    lng: float,
    metro: str,
    chain: str,
    store_id: str,
    phone: str | None = None,
    website: str | None = None,
    email: str | None = None,
    subcategories: list[str] | None = None,
    hours: dict | None = None,
) -> dict:
    listing = {
        "id": make_id(name, address or f"{lat},{lng}"),
        "name": name.strip(),
        "category": category,
        "address": (address or "").strip(),
        "city": (city or "").strip(),
        "state": (state or "").strip(),
        "zip": (zip_ or "").strip(),
        "metro": metro,
        "lat": float(lat),
        "lng": float(lng),
        "sources": ["chain_locator"],
        "sourceIds": {"chain": chain, "storeId": str(store_id)},
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
