import fs from "node:fs";
import path from "node:path";
import type { Category, Listing, Metro } from "@/types/listing";

// Cache between calls within a build.
let cache: Listing[] | null = null;

export function getAllListings(): Listing[] {
  if (cache) return cache;
  const filePath = path.join(
    process.cwd(),
    "public",
    "data",
    "listings.json",
  );
  const raw = fs.readFileSync(filePath, "utf-8");
  const parsed = JSON.parse(raw) as Listing[];
  cache = parsed;
  return parsed;
}

export function getListingsByMetro(metro: Metro): Listing[] {
  return getAllListings().filter((l) => l.metro === metro);
}

export function getListingsByMetroCategory(
  metro: Metro,
  category: Category,
): Listing[] {
  return getAllListings().filter(
    (l) => l.metro === metro && l.category === category,
  );
}

export function getListingById(id: string): Listing | undefined {
  return getAllListings().find((l) => l.id === id);
}

export function getRelatedListings(
  listing: Listing,
  count: number = 6,
): Listing[] {
  return getAllListings()
    .filter(
      (l) =>
        l.id !== listing.id &&
        l.metro === listing.metro &&
        l.category === listing.category,
    )
    .slice(0, count);
}

export function getNearbyServices(
  listing: Listing,
  count: number = 6,
): Listing[] {
  return getAllListings()
    .filter(
      (l) =>
        l.id !== listing.id &&
        l.metro === listing.metro &&
        l.category !== listing.category,
    )
    .slice(0, count);
}

export function countByMetroCategory(
  metro: Metro,
): Record<Category, number> {
  const counts: Record<Category, number> = {
    veterinarian: 0,
    groomer: 0,
    boarder: 0,
    daycare: 0,
    sitter: 0,
    shelter: 0,
  };
  for (const l of getListingsByMetro(metro)) {
    counts[l.category]++;
  }
  return counts;
}

export function countByMetro(): Record<Metro, number> {
  const counts: Record<Metro, number> = {
    atlanta: 0,
    tampa: 0,
    austin: 0,
    nashville: 0,
    asheville: 0,
  };
  for (const l of getAllListings()) {
    counts[l.metro]++;
  }
  return counts;
}
