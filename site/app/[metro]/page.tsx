import Link from "next/link";
import { notFound } from "next/navigation";
import type { Metadata } from "next";
import {
  METROS,
  METRO_LABELS,
  METRO_STATES,
  CATEGORIES,
  CATEGORY_LABELS,
  type Metro,
} from "@/types/listing";
import {
  countByMetroCategory,
  getListingsByMetro,
} from "@/lib/data";
import { ListingCard } from "@/components/ListingCard";
import { FetchFilesCTA } from "@/components/FetchFilesCTA";

export function generateStaticParams() {
  return METROS.map((metro) => ({ metro }));
}

export function generateMetadata({
  params,
}: {
  params: { metro: string };
}): Metadata {
  const metro = params.metro as Metro;
  if (!METROS.includes(metro)) return {};
  const label = METRO_LABELS[metro];
  return {
    title: `Pet services in ${label}`,
    description: `Find vets, groomers, boarders, daycares, and sitters in ${label}, ${METRO_STATES[metro]}.`,
  };
}

export default function MetroPage({ params }: { params: { metro: string } }) {
  const metro = params.metro as Metro;
  if (!METROS.includes(metro)) notFound();

  const counts = countByMetroCategory(metro);
  const top = getListingsByMetro(metro).slice(0, 6);
  const label = METRO_LABELS[metro];

  return (
    <div>
      <section className="mx-auto max-w-6xl px-4 pt-14 pb-6">
        <div className="text-sm text-ink-500 mb-2">
          <Link href="/" className="hover:text-ink-900">
            Home
          </Link>{" "}
          / {label}
        </div>
        <h1 className="text-3xl sm:text-4xl font-semibold tracking-tight text-ink-900">
          Pet services in {label}
        </h1>
        <p className="mt-2 text-ink-500">
          {METRO_STATES[metro]} &middot; {getListingsByMetro(metro).length}{" "}
          listings
        </p>
      </section>

      <section className="mx-auto max-w-6xl px-4 pb-10">
        <h2 className="text-lg font-semibold text-ink-900 mb-3">Categories</h2>
        <div className="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-5 gap-3">
          {CATEGORIES.map((c) => (
            <Link
              key={c}
              href={`/${metro}/${c}`}
              className="rounded-lg border border-ink-100 bg-white p-4 hover:border-accent-600 transition"
            >
              <div className="font-medium text-ink-900">
                {CATEGORY_LABELS[c]}
              </div>
              <div className="text-xs text-ink-500 mt-1">
                {counts[c]} in {label}
              </div>
            </Link>
          ))}
        </div>
      </section>

      {top.length > 0 && (
        <section className="mx-auto max-w-6xl px-4 pb-10">
          <h2 className="text-lg font-semibold text-ink-900 mb-3">
            Top listings
          </h2>
          <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-3">
            {top.map((l) => (
              <ListingCard key={l.id} listing={l} />
            ))}
          </div>
        </section>
      )}

      <FetchFilesCTA />
    </div>
  );
}
