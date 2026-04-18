import Link from "next/link";
import { MapPin } from "lucide-react";
import {
  type Listing,
  CATEGORY_LABELS_SINGULAR,
} from "@/types/listing";

export function ListingTile({ listing }: { listing: Listing }) {
  return (
    <Link
      href={`/${listing.metro}/${listing.category}/${listing.id}`}
      className="block rounded-xl border border-ink-100 bg-white p-4 hover:border-accent-600 transition"
    >
      <div className="text-xs text-ink-500 mb-1">
        {CATEGORY_LABELS_SINGULAR[listing.category]}
      </div>
      <div className="font-medium text-ink-900 line-clamp-2">
        {listing.name}
      </div>
      <div className="mt-1 text-xs text-ink-500 flex items-center gap-1">
        <MapPin className="w-3 h-3" />
        {listing.city}
      </div>
    </Link>
  );
}
