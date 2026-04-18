"""Combine all chain fetchers into a single flat array.

Output: data/enrichment/chains.json

Hard cap: 20 minutes total.
"""
from __future__ import annotations

import json
import time
from pathlib import Path

from . import common
from . import banfield, vca, bluepearl, thrive, medvet
from . import petsmart, petco
from . import camp_bow_wow, dogtopia, hounds_town

# Order: fastest first + skipped-fast first
CHAINS = [
    ("bluepearl",    bluepearl.fetch),
    ("hounds_town",  hounds_town.fetch),
    ("medvet",       medvet.fetch),
    ("thrive",       thrive.fetch),
    ("banfield",     banfield.fetch),
    ("vca",          vca.fetch),
    ("camp_bow_wow", camp_bow_wow.fetch),
    ("dogtopia",     dogtopia.fetch),
    ("petco",        petco.fetch),
    ("petsmart",     petsmart.fetch),
]

HARD_CAP_SECONDS = 20 * 60


def main() -> int:
    t0 = time.time()
    all_listings: list[dict] = []
    per_chain_counts: dict[str, int] = {}
    skipped: list[str] = []

    for name, fn in CHAINS:
        elapsed = time.time() - t0
        if elapsed >= HARD_CAP_SECONDS:
            print(f"[main] BUDGET EXCEEDED after {elapsed:.1f}s; skipping {name}")
            skipped.append(name)
            continue
        remaining = HARD_CAP_SECONDS - elapsed
        print(f"[main] running {name} (elapsed {elapsed:.1f}s, remaining {remaining:.1f}s)")
        try:
            rows = fn()
        except Exception as e:
            print(f"[main] {name} raised {type(e).__name__}: {e}")
            rows = []
            skipped.append(name)
        per_chain_counts[name] = len(rows)
        all_listings.extend(rows)

    # Dedupe on id (same name+address)
    seen: set[str] = set()
    deduped: list[dict] = []
    for x in all_listings:
        if x["id"] in seen:
            continue
        seen.add(x["id"])
        deduped.append(x)

    common.OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with common.OUTPUT_PATH.open("w", encoding="utf-8") as f:
        json.dump(deduped, f, indent=2)

    # Print summary
    print("=" * 60)
    print("SUMMARY")
    print("=" * 60)
    for name, _ in CHAINS:
        print(f"  {name:16s}: {per_chain_counts.get(name, 0):4d} listings")
    print(f"  {'TOTAL':16s}: {len(deduped):4d} listings (dedupe of {len(all_listings)})")
    # Per-metro breakdown
    from collections import Counter
    by_metro = Counter(x["metro"] for x in deduped)
    print(f"  by-metro: {dict(by_metro)}")
    print(f"  skipped: {skipped}")
    print(f"  elapsed: {time.time() - t0:.1f}s")
    print(f"  output: {common.OUTPUT_PATH}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
