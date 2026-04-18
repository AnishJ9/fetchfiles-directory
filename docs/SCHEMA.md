# Canonical Listing Schema

All pipeline outputs and site inputs conform to this schema. One JSON object per listing.

## Fields

```typescript
interface Listing {
  // Identity
  id: string;                    // stable hash of (name + address)
  name: string;                  // business name, trimmed
  category: Category;            // see below
  subcategories?: string[];      // e.g., ["mobile", "cat-only"]

  // Location
  address: string;               // full street address
  city: string;
  state: string;                 // 2-letter code
  zip: string;
  metro: Metro;                  // one of the 5 launch metros
  lat: number;
  lng: number;

  // Contact (optional, best-effort from sources)
  phone?: string;                // E.164 format if parseable, else raw
  website?: string;              // full URL
  email?: string;

  // Provenance
  sources: Source[];             // which data sources contributed
  sourceIds: Record<string, string>; // e.g., { "osm": "node/12345" }
  lastSeenAt: string;            // ISO 8601 date

  // Enrichment (optional)
  hours?: Record<string, string>; // { "mon": "9am-6pm", ... }
  description?: string;
  tags?: string[];               // free-form labels

  // Directory state
  claimed: boolean;              // has the business claimed this listing?
  claimedAt?: string;
}

type Category =
  | "veterinarian"
  | "groomer"
  | "boarder"
  | "daycare"
  | "sitter"
  | "shelter";

type Metro =
  | "atlanta"
  | "tampa"
  | "austin"
  | "nashville"
  | "asheville";

type Source =
  | "osm"           // OpenStreetMap via Overpass
  | "napps"         // National Association of Professional Pet Sitters
  | "psi"           // Pet Sitters International
  | "ibpsa"         // International Boarding & Pet Services Association
  | "ndgaa"         // National Dog Groomers Association of America
  | "claim";        // submitted via claim form
```

## Metro Bounding Boxes (for Overpass queries)

```
atlanta:    south=33.40, west=-84.85, north=34.15, east=-83.90
tampa:      south=27.50, west=-82.90, north=28.30, east=-82.20
austin:     south=30.00, west=-98.10, north=30.65, east=-97.40
nashville:  south=35.80, west=-87.10, north=36.45, east=-86.40
asheville:  south=35.35, west=-82.85, north=35.80, east=-82.35
```

## Output Files

- `data/listings.json` — flat array of all listings across all metros
- `data/by-metro/{metro}.json` — per-metro slices (same schema, filtered)
- `data/stats.json` — counts by metro × category, last-updated timestamp

## Dedupe Rules

Two listings are duplicates if:
1. Same normalized phone (digits only), OR
2. Same normalized name (lowercase, stripped punctuation) AND within 100m geographically

Merge strategy on dup: keep earliest `lastSeenAt`, union of `sources`, prefer longest non-null fields.
