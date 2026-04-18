"""BluePearl Specialty Pet Hospital fetcher.

BluePearl's sitemap is behind a Cloudflare challenge and there is no public
JSON endpoint readily accessible in-budget. Skip with a log.
"""
from __future__ import annotations

from . import common

CHAIN = "bluepearl"


def fetch() -> list[dict]:
    print(f"[{CHAIN}] skipped: site is Cloudflare-challenged (returns JS challenge page)")
    common.cache_write(CHAIN, {"skipped": "cloudflare_challenge"})
    return []
