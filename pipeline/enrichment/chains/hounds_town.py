"""Hounds Town USA fetcher.

Hounds Town's site is behind Cloudflare challenge for sitemap/data endpoints.
Skip with a log.
"""
from __future__ import annotations

from . import common

CHAIN = "hounds_town"


def fetch() -> list[dict]:
    print(f"[{CHAIN}] skipped: site returns Cloudflare challenge for sitemap")
    common.cache_write(CHAIN, {"skipped": "cloudflare_challenge"})
    return []
