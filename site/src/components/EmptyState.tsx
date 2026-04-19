import Link from "next/link";
import { PawPrint } from "lucide-react";
import type { Category, Metro } from "@/types/listing";
import {
  CATEGORY_LABELS,
  METRO_LABELS,
} from "@/types/listing";

export function EmptyState({
  category,
  metro,
}: {
  category: Category;
  metro: Metro;
}) {
  return (
    <div className="rounded-xl border border-dashed border-ink-100 bg-warm-50 p-10 text-center">
      <div className="mx-auto w-12 h-12 rounded-full bg-warm-100 flex items-center justify-center mb-3">
        <PawPrint className="w-6 h-6 text-warm-700" />
      </div>
      <h3 className="font-semibold text-ink-900">
        No {CATEGORY_LABELS[category].toLowerCase()} in {METRO_LABELS[metro]} yet
      </h3>
      <p className="mt-1 text-sm text-ink-500 max-w-md mx-auto">
        We haven&apos;t indexed any yet. If you run one or know one, add it and
        help other pet parents find it.
      </p>
      <Link
        href="/claim/new"
        className="mt-4 inline-block px-4 py-2 rounded-md bg-accent-600 text-white text-sm font-medium hover:bg-accent-700"
      >
        Add a listing
      </Link>
    </div>
  );
}
