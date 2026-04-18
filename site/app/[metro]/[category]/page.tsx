import Link from "next/link";
import { notFound } from "next/navigation";
import type { Metadata } from "next";
import {
  METROS,
  METRO_LABELS,
  METRO_STATES,
  CATEGORIES,
  CATEGORY_LABELS,
  type Category,
  type Metro,
} from "@/types/listing";
import { getListingsByMetroCategory } from "@/lib/data";
import { CategoryListings } from "./CategoryListings";
import { FetchFilesCTA } from "@/components/FetchFilesCTA";

export function generateStaticParams() {
  const params: { metro: string; category: string }[] = [];
  for (const metro of METROS) {
    for (const category of CATEGORIES) {
      params.push({ metro, category });
    }
  }
  return params;
}

export function generateMetadata({
  params,
}: {
  params: { metro: string; category: string };
}): Metadata {
  const metro = params.metro as Metro;
  const category = params.category as Category;
  if (!METROS.includes(metro) || !CATEGORIES.includes(category)) return {};
  const mLabel = METRO_LABELS[metro];
  const cLabel = CATEGORY_LABELS[category];
  return {
    title: `${cLabel} in ${mLabel}`,
    description: `Find ${cLabel.toLowerCase()} for pets in ${mLabel}, ${METRO_STATES[metro]}.`,
  };
}

export default function CategoryPage({
  params,
}: {
  params: { metro: string; category: string };
}) {
  const metro = params.metro as Metro;
  const category = params.category as Category;
  if (!METROS.includes(metro) || !CATEGORIES.includes(category)) notFound();

  const listings = getListingsByMetroCategory(metro, category);
  const mLabel = METRO_LABELS[metro];
  const cLabel = CATEGORY_LABELS[category];

  // Pull all unique subcategories for client-side filter UI
  const subSet = new Set<string>();
  for (const l of listings) {
    for (const s of l.subcategories ?? []) subSet.add(s);
  }
  const subs = Array.from(subSet).sort();

  return (
    <div>
      <section className="mx-auto max-w-6xl px-4 pt-14 pb-6">
        <div className="text-sm text-ink-500 mb-2">
          <Link href="/" className="hover:text-ink-900">
            Home
          </Link>{" "}
          /{" "}
          <Link href={`/${metro}`} className="hover:text-ink-900">
            {mLabel}
          </Link>{" "}
          / {cLabel}
        </div>
        <h1 className="text-3xl sm:text-4xl font-semibold tracking-tight text-ink-900">
          {cLabel} in {mLabel}
        </h1>
        <p className="mt-2 text-ink-500">
          {listings.length} listing{listings.length === 1 ? "" : "s"}
        </p>
      </section>

      <section className="mx-auto max-w-6xl px-4 pb-12">
        <CategoryListings listings={listings} subcategories={subs} />
      </section>

      <FetchFilesCTA />
    </div>
  );
}
