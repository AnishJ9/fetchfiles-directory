"""Flag listings that appear to serve exotic pets.

Heuristic: scan each listing's name + description (and website/tags) for
strong exotic-pet keywords. Write a lookup file that merge.py uses to set
`petTypes` on each listing.

Output:
  data/enrichment/exotics.json  — object { listing_id: { petTypes: ["exotic", ...] } }
"""
from __future__ import annotations

import json
import re
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent.parent
LISTINGS = REPO / "data" / "listings.json"
OUT = REPO / "data" / "enrichment" / "exotics.json"

# Keywords keyed by pet-type tag. Regex with word boundaries to reduce FPs.
KEYWORDS: dict[str, list[str]] = {
    "exotic": [
        r"\bexotic(s)?\b",
        r"\bexotic\s+(pet|animal|mammal)s?\b",
    ],
    "avian": [
        r"\bavian\b",
        r"\bparrot(s)?\b",
        r"\bcockatoo(s)?\b",
        r"\bcockatiel(s)?\b",
        r"\bparakeet(s)?\b",
        r"\bmacaw(s)?\b",
    ],
    "reptile": [
        r"\breptile(s)?\b",
        r"\bherpetolog",
        r"\bsnake(s)?\b",
        r"\blizard(s)?\b",
        r"\biguana(s)?\b",
        r"\bgecko(s)?\b",
        r"\bturtle(s)?\b",
        r"\btortoise(s)?\b",
    ],
    "small_mammal": [
        r"\brabbit(s)?\b",
        r"\bbunny|bunnies\b",
        r"\bguinea\s+pig(s)?\b",
        r"\bchinchilla(s)?\b",
        r"\bferret(s)?\b",
        r"\bhedgehog(s)?\b",
        r"\bhamster(s)?\b",
        r"\bsugar\s+glider",
    ],
    "pocket_pet": [
        r"\bpocket\s+pet(s)?\b",
        r"\bsmall\s+mammal(s)?\b",
    ],
}

# Compile for speed
COMPILED = {tag: [re.compile(p, re.I) for p in pats] for tag, pats in KEYWORDS.items()}

# Heuristic: if ≥2 of these "species" tags match, we also flag "exotic"
SPECIES_TAGS = {"avian", "reptile", "small_mammal", "pocket_pet"}

# Only check categories where exotic-friendliness is meaningful
RELEVANT = {"veterinarian", "groomer", "boarder", "daycare", "sitter", "pet_hotel"}


def tags_for_text(text: str) -> set[str]:
    """Return which pet-type tags match the given text."""
    tags: set[str] = set()
    for tag, patterns in COMPILED.items():
        for p in patterns:
            if p.search(text):
                tags.add(tag)
                break
    return tags


def main() -> None:
    listings = json.loads(LISTINGS.read_text())
    out: dict[str, dict] = {}

    for l in listings:
        if l.get("category") not in RELEVANT:
            continue
        blob = " ".join(
            filter(
                None,
                [
                    l.get("name", ""),
                    l.get("description", ""),
                    l.get("website", ""),
                    " ".join(l.get("tags", []) or []),
                ],
            )
        )
        tags = tags_for_text(blob)
        if not tags:
            continue
        # Any species tag OR an explicit "exotic" → flag as exotic-friendly
        species_matches = tags & SPECIES_TAGS
        if "exotic" in tags or len(species_matches) >= 1:
            pet_types = sorted(tags | ({"exotic"} if species_matches else set()))
            out[l["id"]] = {"petTypes": pet_types}

    OUT.write_text(json.dumps(out, indent=2))

    # Summary
    from collections import Counter
    print(f"scanned: {sum(1 for l in listings if l.get('category') in RELEVANT)} listings")
    print(f"flagged: {len(out)}")
    type_counts: Counter[str] = Counter()
    for rec in out.values():
        for t in rec["petTypes"]:
            type_counts[t] += 1
    for tag, n in type_counts.most_common():
        print(f"  {tag:<14} {n}")


if __name__ == "__main__":
    main()
