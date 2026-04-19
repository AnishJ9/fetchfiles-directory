"use client";

import { useMemo, useState } from "react";
import type { Listing } from "@/types/listing";
import { ListingCard } from "@/components/ListingCard";

const PAGE_SIZE = 12;

const ATTRIBUTE_LABELS: Record<string, string> = {
  emergency: "Emergency / 24-7",
  exotic: "Exotic-friendly",
  cat_friendly: "Cat-friendly",
  large_breed: "Large breed",
  small_breed: "Small breed",
  overnight: "Overnight",
  house_calls: "House calls / mobile",
};

export function CategoryListings({
  listings,
  subcategories,
}: {
  listings: Listing[];
  subcategories: string[];
}) {
  const [sub, setSub] = useState<string>("all");
  const [activeAttrs, setActiveAttrs] = useState<string[]>([]);
  const [page, setPage] = useState<number>(1);

  const availableAttrs = useMemo(() => {
    const set = new Set<string>();
    for (const l of listings) {
      for (const a of l.attributes ?? []) set.add(a);
    }
    return Array.from(set).sort();
  }, [listings]);

  const filtered = useMemo(() => {
    let out = listings;
    if (sub !== "all") {
      out = out.filter((l) => (l.subcategories ?? []).includes(sub));
    }
    if (activeAttrs.length > 0) {
      out = out.filter((l) => {
        const attrs = l.attributes ?? [];
        return activeAttrs.every((a) => attrs.includes(a));
      });
    }
    return out;
  }, [listings, sub, activeAttrs]);

  const totalPages = Math.max(1, Math.ceil(filtered.length / PAGE_SIZE));
  const current = page > totalPages ? 1 : page;
  const start = (current - 1) * PAGE_SIZE;
  const pageItems = filtered.slice(start, start + PAGE_SIZE);

  function toggleAttr(a: string) {
    setPage(1);
    setActiveAttrs((cur) =>
      cur.includes(a) ? cur.filter((x) => x !== a) : [...cur, a],
    );
  }

  return (
    <div>
      {subcategories.length > 0 && (
        <div className="flex flex-wrap gap-2 mb-3">
          <button
            onClick={() => {
              setSub("all");
              setPage(1);
            }}
            className={`px-3 py-1 rounded-full text-sm border ${
              sub === "all"
                ? "bg-accent-600 text-white border-accent-600"
                : "bg-white text-ink-700 border-ink-100 hover:border-accent-600"
            }`}
          >
            All
          </button>
          {subcategories.map((s) => (
            <button
              key={s}
              onClick={() => {
                setSub(s);
                setPage(1);
              }}
              className={`px-3 py-1 rounded-full text-sm border ${
                sub === s
                  ? "bg-accent-600 text-white border-accent-600"
                  : "bg-white text-ink-700 border-ink-100 hover:border-accent-600"
              }`}
            >
              {s}
            </button>
          ))}
        </div>
      )}

      {availableAttrs.length > 0 && (
        <div className="flex flex-wrap items-center gap-2 mb-4">
          <span className="text-xs text-ink-500 uppercase tracking-wide">
            Filter
          </span>
          {availableAttrs.map((a) => {
            const active = activeAttrs.includes(a);
            return (
              <button
                key={a}
                onClick={() => toggleAttr(a)}
                className={`px-3 py-1 rounded-full text-sm border ${
                  active
                    ? "bg-warm-600 text-white border-warm-600"
                    : "bg-white text-ink-700 border-ink-100 hover:border-warm-500"
                }`}
              >
                {ATTRIBUTE_LABELS[a] ?? a}
              </button>
            );
          })}
          {activeAttrs.length > 0 && (
            <button
              onClick={() => setActiveAttrs([])}
              className="text-xs text-ink-500 underline hover:text-ink-900 ml-1"
            >
              clear
            </button>
          )}
        </div>
      )}

      {pageItems.length === 0 ? (
        <div className="text-ink-500 text-sm">No listings match that filter.</div>
      ) : (
        <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-3">
          {pageItems.map((l) => (
            <ListingCard key={l.id} listing={l} />
          ))}
        </div>
      )}

      {totalPages > 1 && (
        <div className="mt-6 flex items-center gap-3 justify-center">
          <button
            onClick={() => setPage((p) => Math.max(1, p - 1))}
            disabled={current === 1}
            className="px-3 py-1.5 text-sm border border-ink-100 rounded-md disabled:opacity-50 bg-white"
          >
            Prev
          </button>
          <span className="text-sm text-ink-500">
            Page {current} of {totalPages}
          </span>
          <button
            onClick={() => setPage((p) => Math.min(totalPages, p + 1))}
            disabled={current === totalPages}
            className="px-3 py-1.5 text-sm border border-ink-100 rounded-md disabled:opacity-50 bg-white"
          >
            Next
          </button>
        </div>
      )}
    </div>
  );
}
