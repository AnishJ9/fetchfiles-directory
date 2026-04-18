"""Fast retry: targeted chains with unbuffered output and tight timeouts."""
from __future__ import annotations

import json
import sys
import time

# Ensure unbuffered stdout
sys.stdout.reconfigure(line_buffering=True)

from . import common
from . import banfield, petsmart, dogtopia


TARGET = [
    ("banfield", banfield.fetch),
    ("petsmart", petsmart.fetch),
    ("dogtopia", dogtopia.fetch),
]

HARD_CAP_SECONDS = 6 * 60


def main() -> int:
    t0 = time.time()

    # Load existing
    if common.OUTPUT_PATH.exists():
        with common.OUTPUT_PATH.open() as f:
            existing = json.load(f)
    else:
        existing = []

    targets = {name for name, _ in TARGET}
    # Keep rows not in retry set
    kept = [r for r in existing if r["sourceIds"]["chain"] not in targets]
    print(f"[fast-retry] kept {len(kept)} from non-target chains", flush=True)

    # Special: PetSmart's Banfield-companion rows use chain=banfield but store IDs are
    # state/city/slug. We want to keep existing Banfield rows that were produced by
    # PetSmart-crawl if banfield itself fails — so keep rows whose storeId looks like a path.
    # (Skip this for now; simpler to just rerun.)

    all_new: list[dict] = []
    counts: dict[str, int] = {}
    for name, fn in TARGET:
        elapsed = time.time() - t0
        if elapsed >= HARD_CAP_SECONDS:
            print(f"[fast-retry] budget exceeded; skipping {name}", flush=True)
            continue
        print(f"[fast-retry] running {name} (remaining: {HARD_CAP_SECONDS - elapsed:.0f}s)", flush=True)
        try:
            rows = fn()
        except Exception as e:
            print(f"[fast-retry] {name} error {type(e).__name__}: {e}", flush=True)
            rows = []
        counts[name] = len(rows)
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
    print("FAST-RETRY SUMMARY")
    for name, _ in TARGET:
        print(f"  {name:16s}: {counts.get(name, 0):4d}")
    print(f"  TOTAL: {len(deduped)} (kept {len(kept)} + new {len(all_new)})")
    by_chain = Counter(x["sourceIds"]["chain"] for x in deduped)
    by_metro = Counter(x["metro"] for x in deduped)
    print(f"  by chain: {dict(by_chain)}")
    print(f"  by metro: {dict(by_metro)}")
    print(f"  elapsed: {time.time() - t0:.1f}s")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
