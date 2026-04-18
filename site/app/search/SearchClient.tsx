"use client";

import { useEffect, useMemo, useState } from "react";
import { useSearchParams, useRouter } from "next/navigation";
import type { Listing } from "@/types/listing";
import { ListingCard } from "@/components/ListingCard";
import { Search } from "lucide-react";

function normalize(s: string): string {
  return s.toLowerCase().trim();
}

function matches(l: Listing, q: string): boolean {
  const n = normalize(q);
  if (!n) return false;
  return (
    normalize(l.name).includes(n) ||
    normalize(l.address).includes(n) ||
    normalize(l.city).includes(n) ||
    normalize(l.category).includes(n) ||
    (l.tags ?? []).some((t) => normalize(t).includes(n)) ||
    (l.subcategories ?? []).some((s) => normalize(s).includes(n))
  );
}

export function SearchClient({ listings }: { listings: Listing[] }) {
  const sp = useSearchParams();
  const router = useRouter();
  const initial = sp.get("q") ?? "";
  const [q, setQ] = useState(initial);

  useEffect(() => {
    const current = sp.get("q") ?? "";
    if (current !== q) {
      const url = q ? `/search?q=${encodeURIComponent(q)}` : "/search";
      router.replace(url);
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [q]);

  const results = useMemo(() => {
    if (!q.trim()) return [];
    return listings.filter((l) => matches(l, q)).slice(0, 60);
  }, [listings, q]);

  return (
    <div>
      <div className="relative">
        <Search className="w-4 h-4 absolute left-3 top-1/2 -translate-y-1/2 text-ink-500" />
        <input
          autoFocus
          value={q}
          onChange={(e) => setQ(e.target.value)}
          placeholder="e.g. groomer, Buckhead, Sunshine Vet"
          className="w-full rounded-md border border-ink-100 pl-9 pr-3 py-2.5 text-sm bg-white focus:outline-none focus:border-accent-600"
        />
      </div>

      <div className="mt-6">
        {!q.trim() ? (
          <div className="text-ink-500 text-sm">Type to search.</div>
        ) : results.length === 0 ? (
          <div className="text-ink-500 text-sm">
            No matches for &ldquo;{q}&rdquo;.
          </div>
        ) : (
          <>
            <div className="text-xs text-ink-500 mb-3">
              {results.length} result{results.length === 1 ? "" : "s"}
            </div>
            <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-3">
              {results.map((l) => (
                <ListingCard key={l.id} listing={l} />
              ))}
            </div>
          </>
        )}
      </div>
    </div>
  );
}
