import type { Metadata } from "next";
import { Suspense } from "react";
import { getAllListings } from "@/lib/data";
import { SearchClient } from "./SearchClient";

export const metadata: Metadata = {
  title: "Search",
  description: "Search pet services by name, category, or location.",
};

export default function SearchPage() {
  const listings = getAllListings();
  return (
    <div className="mx-auto max-w-6xl px-4 pt-14 pb-16">
      <h1 className="text-3xl sm:text-4xl font-semibold tracking-tight text-ink-900">
        Search
      </h1>
      <p className="mt-2 text-ink-500">
        Search by business name, category, city, or address.
      </p>
      <div className="mt-6">
        <Suspense
          fallback={<div className="text-ink-500 text-sm">Loading...</div>}
        >
          <SearchClient listings={listings} />
        </Suspense>
      </div>
    </div>
  );
}
