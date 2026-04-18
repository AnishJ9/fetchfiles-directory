import type { MetadataRoute } from "next";
import { getAllListings } from "@/lib/data";
import { METROS, CATEGORIES } from "@/types/listing";

const BASE =
  process.env.NEXT_PUBLIC_SITE_URL ?? "https://directory.fetch-files.com";

export default function sitemap(): MetadataRoute.Sitemap {
  const now = new Date();
  const entries: MetadataRoute.Sitemap = [
    { url: `${BASE}/`, lastModified: now },
    { url: `${BASE}/search`, lastModified: now },
  ];

  for (const metro of METROS) {
    entries.push({ url: `${BASE}/${metro}`, lastModified: now });
    for (const category of CATEGORIES) {
      entries.push({
        url: `${BASE}/${metro}/${category}`,
        lastModified: now,
      });
    }
  }

  for (const l of getAllListings()) {
    entries.push({
      url: `${BASE}/${l.metro}/${l.category}/${l.id}`,
      lastModified: l.lastSeenAt ? new Date(l.lastSeenAt) : now,
    });
  }

  return entries;
}
