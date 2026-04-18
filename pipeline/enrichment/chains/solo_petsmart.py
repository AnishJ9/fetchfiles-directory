"""Solo PetSmart retry."""
from __future__ import annotations

import json
import sys
import time

sys.stdout.reconfigure(line_buffering=True)

from . import common, petsmart


def main() -> int:
    t0 = time.time()
    rows = petsmart.fetch()
    print(f"petsmart fetched: {len(rows)} in {time.time() - t0:.1f}s", flush=True)

    with common.OUTPUT_PATH.open() as f:
        existing = json.load(f)

    # Remove old petsmart + inside-petsmart-banfield companions (those have chain=banfield
    # but storeId with a slash). Keep everything else.
    def is_ps_related(r):
        ch = r["sourceIds"]["chain"]
        sid = r["sourceIds"].get("storeId", "")
        if ch == "petsmart":
            return True
        if ch == "banfield" and "/" in sid:
            return True
        return False

    kept = [r for r in existing if not is_ps_related(r)]
    combined = kept + rows
    seen: set[str] = set()
    deduped: list[dict] = []
    for r in combined:
        if r["id"] in seen:
            continue
        seen.add(r["id"])
        deduped.append(r)

    with common.OUTPUT_PATH.open("w") as f:
        json.dump(deduped, f, indent=2)

    from collections import Counter
    print(f"wrote: {len(deduped)}", flush=True)
    print(f"by chain: {dict(Counter(r['sourceIds']['chain'] for r in deduped))}", flush=True)
    print(f"by metro: {dict(Counter(r['metro'] for r in deduped))}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
