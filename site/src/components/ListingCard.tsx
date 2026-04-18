import Link from "next/link";
import type { Listing } from "@/types/listing";
import { CATEGORY_LABELS_SINGULAR } from "@/types/listing";
import { BadgeCheck } from "lucide-react";

export function ListingCard({ listing }: { listing: Listing }) {
  const href = `/${listing.metro}/${listing.category}/${listing.id}`;
  return (
    <Link
      href={href}
      className="group block rounded-lg border border-ink-100 bg-white p-4 hover:border-accent-600 hover:shadow-sm transition"
    >
      <div className="flex items-start justify-between gap-2">
        <div className="font-semibold text-ink-900 group-hover:text-accent-700 leading-snug">
          {listing.name}
        </div>
        {listing.claimed && (
          <BadgeCheck
            className="w-4 h-4 text-accent-600 mt-1 flex-shrink-0"
            aria-label="Claimed"
          />
        )}
      </div>

      <div className="mt-1 text-xs text-ink-500 flex items-center gap-2 flex-wrap">
        <span className="inline-block px-2 py-0.5 rounded-full bg-ink-50 text-ink-700">
          {CATEGORY_LABELS_SINGULAR[listing.category]}
        </span>
        <span>{listing.city}</span>
      </div>

      {listing.tags && listing.tags.length > 0 && (
        <div className="mt-2 text-xs text-ink-500 line-clamp-1">
          {listing.tags.slice(0, 3).join(" \u00b7 ")}
        </div>
      )}

      <div className="mt-3 text-sm text-accent-600 group-hover:text-accent-700">
        View &rarr;
      </div>
    </Link>
  );
}
