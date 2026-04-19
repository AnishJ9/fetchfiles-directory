"""Flag listing attributes based on name + description.

Produces a flag array per listing. Attributes that may apply depend on
category (e.g., "large_breed" only matters for groomers).

Output:
  data/enrichment/attributes.json  — { listing_id: { attributes: [...] } }
"""
from __future__ import annotations

import json
import re
from collections import Counter
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent.parent
LISTINGS = REPO / "data" / "listings.json"
OUT = REPO / "data" / "enrichment" / "attributes.json"

# attribute -> (regex patterns, applicable categories)
ATTR_RULES: dict[str, tuple[list[str], set[str]]] = {
    "emergency": (
        [
            r"\bemergenc(y|ies)\b",
            r"\b24\s*[-/]?\s*(hour|hr|7)",
            r"\b24/7\b",
            r"\bafter[- ]?hours?\b",
            r"\burgent\s+care\b",
            r"\bE\.?R\.?\s+vet\b",
        ],
        {"veterinarian"},
    ),
    "exotic": (
        [
            r"\bexotic(s|\s+(pet|animal|mammal|bird|reptile))?\b",
            r"\bavian\b",
            r"\breptile(s)?\b",
            r"\bparrot(s)?\b",
            r"\biguana\b",
            r"\brabbit(s)?\b",
            r"\bguinea\s+pig\b",
            r"\bchinchilla\b",
            r"\bferret(s)?\b",
            r"\bhedgehog\b",
            r"\bsnake(s)?\b",
            r"\blizard(s)?\b",
            r"\bturtle(s)?\b",
            r"\btortoise(s)?\b",
            r"\bsugar\s+glider\b",
            r"\bpocket\s+pet(s)?\b",
            r"\bsmall\s+mammal(s)?\b",
        ],
        {"veterinarian", "groomer", "boarder", "daycare", "sitter", "pet_hotel"},
    ),
    "cat_friendly": (
        [
            r"\bcat(s)?\s+(only|exclusive|specialty|friendly)\b",
            r"\bfeline\s+(only|exclusive|medicine|specialty)\b",
            r"\bfor\s+cats\b",
            r"\bcat\s+(groom|grooming|boarding|daycare|care|hotel)\b",
        ],
        {"veterinarian", "groomer", "boarder", "daycare", "sitter", "pet_hotel"},
    ),
    "large_breed": (
        [
            r"\blarge\s+(breed|dog|paw)s?\b",
            r"\bbig\s+dog(s)?\b",
            r"\bheavy\s+coat(s|ed)?\b",
            r"\bdouble\s+coat(s|ed)?\b",
            r"\bdoodle(s)?\b",
            r"\bgolden(s)?\s+and\s+poodle",
        ],
        {"groomer"},
    ),
    "small_breed": (
        [
            r"\bsmall\s+(breed|dog|paw)s?\b",
            r"\btoy\s+breed(s)?\b",
            r"\byorkie|maltese|chihuahua",
        ],
        {"groomer"},
    ),
    "overnight": (
        [
            r"\bovernight(s)?\b",
            r"\bstay[-\s]?over\b",
            r"\b24[-\s]?hour\b",
            r"\bsleep[-\s]?over\b",
        ],
        {"sitter", "daycare", "boarder", "pet_hotel"},
    ),
    "house_calls": (
        [
            r"\bhouse\s+call(s)?\b",
            r"\bmobile\s+(vet|groom|grooming)\b",
            r"\bin[- ]home\s+(vet|care|visit)\b",
        ],
        {"veterinarian", "groomer", "sitter"},
    ),
}

COMPILED = {
    attr: ([re.compile(p, re.I) for p in patterns], cats)
    for attr, (patterns, cats) in ATTR_RULES.items()
}


def attributes_for(listing: dict) -> list[str]:
    category = listing.get("category", "")
    blob = " ".join(
        filter(
            None,
            [
                listing.get("name", ""),
                listing.get("description", ""),
                listing.get("website", ""),
                " ".join(listing.get("tags", []) or []),
            ],
        )
    )
    found: list[str] = []
    for attr, (patterns, cats) in COMPILED.items():
        if category not in cats:
            continue
        if any(p.search(blob) for p in patterns):
            found.append(attr)
    return found


def main() -> None:
    listings = json.loads(LISTINGS.read_text())
    out: dict[str, dict] = {}
    attr_counts: Counter[str] = Counter()

    for l in listings:
        attrs = attributes_for(l)
        if not attrs:
            continue
        out[l["id"]] = {"attributes": attrs}
        for a in attrs:
            attr_counts[a] += 1

    OUT.write_text(json.dumps(out, indent=2))

    print(f"scanned: {len(listings)} listings")
    print(f"flagged: {len(out)}")
    for attr, n in attr_counts.most_common():
        print(f"  {attr:<14} {n}")


if __name__ == "__main__":
    main()
