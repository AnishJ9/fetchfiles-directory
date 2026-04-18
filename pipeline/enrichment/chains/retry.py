"""Retry specific chains and merge into existing chains.json."""
from __future__ import annotations

import json
import time
from pathlib import Path

from . import common
from . import banfield, medvet, thrive, dogtopia, petsmart, vca, camp_bow_wow


RETRY = [
    ("dogtopia",     dogtopia.fetch),
    ("petsmart",     petsmart.fetch),
    ("banfield",     banfield.fetch),
    ("thrive",       thrive.fetch),
    ("medvet",       medvet.fetch),
    ("vca",          vca.fetch),
]

HARD_CAP_SECONDS = 15 * 60


def main() -> int:
    t0 = time.time()
    # Load existing
    existing = []
    if common.OUTPUT_PATH.exists():
        with common.OUTPUT_PATH.open() as f:
            existing = json.load(f)

    # Keep rows that weren't from the chains we're retrying
    retry_chains = {name for name, _ in RETRY}
    kept = [r for r in existing if r["sourceIds"]["chain"] not in retry_chains]
    print(f"[retry] keeping {len(kept)} existing rows from non-retry chains")

    all_new: list[dict] = []
    per_chain_counts: dict[str, int] = {}
    for name, fn in RETRY:
        elapsed = time.time() - t0
        if elapsed >= HARD_CAP_SECONDS:
            print(f"[retry] BUDGET EXCEEDED; skipping {name}")
            continue
        print(f"[retry] running {name}")
        try:
            rows = fn()
        except Exception as e:
            print(f"[retry] {name} raised {type(e).__name__}: {e}")
            rows = []
        per_chain_counts[name] = len(rows)
        all_new.extend(rows)

    combined = kept + all_new
    # Dedupe on id
    seen: set[str] = set()
    deduped: list[dict] = []
    for x in combined:
        if x["id"] in seen:
            continue
        seen.add(x["id"])
        deduped.append(x)

    with common.OUTPUT_PATH.open("w", encoding="utf-8") as f:
        json.dump(deduped, f, indent=2)

    from collections import Counter
    print("=" * 60)
    print("RETRY SUMMARY")
    print("=" * 60)
    for name, _ in RETRY:
        print(f"  {name:16s}: {per_chain_counts.get(name, 0):4d} listings")
    print(f"  {'TOTAL':16s}: {len(deduped):4d} (kept {len(kept)} + new {len(all_new)})")
    by_chain = Counter(x["sourceIds"]["chain"] for x in deduped)
    by_metro = Counter(x["metro"] for x in deduped)
    print(f"  by chain: {dict(by_chain)}")
    print(f"  by metro: {dict(by_metro)}")
    print(f"  elapsed: {time.time() - t0:.1f}s")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
