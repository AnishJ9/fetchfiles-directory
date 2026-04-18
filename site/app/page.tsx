import Link from "next/link";
import {
  METROS,
  METRO_LABELS,
  CATEGORIES,
  CATEGORY_LABELS,
} from "@/types/listing";
import { countByMetro } from "@/lib/data";
import { FetchFilesCTA } from "@/components/FetchFilesCTA";
import {
  Stethoscope,
  Scissors,
  Home,
  Sun,
  UserRound,
  MapPin,
} from "lucide-react";

const CATEGORY_ICONS = {
  veterinarian: Stethoscope,
  groomer: Scissors,
  boarder: Home,
  daycare: Sun,
  sitter: UserRound,
} as const;

export default function HomePage() {
  const counts = countByMetro();

  return (
    <div>
      {/* Hero */}
      <section className="mx-auto max-w-6xl px-4 pt-14 pb-10">
        <h1 className="text-3xl sm:text-4xl font-semibold tracking-tight text-ink-900">
          Find pet services in your city.
        </h1>
        <p className="mt-3 text-ink-500 max-w-xl">
          Vets, groomers, boarders, daycares, and sitters across five metros.
          Pick a city to start.
        </p>

        <div className="mt-8 grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-5 gap-3">
          {METROS.map((m) => (
            <Link
              key={m}
              href={`/${m}`}
              className="group rounded-lg border border-ink-100 bg-white p-4 hover:border-accent-600 hover:shadow-sm transition"
            >
              <div className="flex items-center gap-2 text-ink-900 font-medium">
                <MapPin className="w-4 h-4 text-accent-600" />
                {METRO_LABELS[m]}
              </div>
              <div className="text-xs text-ink-500 mt-1">
                {counts[m]} listings
              </div>
            </Link>
          ))}
        </div>
      </section>

      {/* Category preview */}
      <section className="mx-auto max-w-6xl px-4 pb-10">
        <h2 className="text-lg font-semibold text-ink-900 mb-4">
          Browse by service
        </h2>
        <div className="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-5 gap-3">
          {CATEGORIES.map((c) => {
            const Icon = CATEGORY_ICONS[c];
            return (
              <Link
                key={c}
                href={`/atlanta/${c}`}
                className="rounded-lg border border-ink-100 bg-white p-4 hover:border-accent-600 transition"
              >
                <Icon className="w-5 h-5 text-accent-600" />
                <div className="mt-2 font-medium text-ink-900">
                  {CATEGORY_LABELS[c]}
                </div>
                <div className="text-xs text-ink-500 mt-1">
                  Browse in Atlanta &rarr;
                </div>
              </Link>
            );
          })}
        </div>
      </section>

      <FetchFilesCTA />
    </div>
  );
}
