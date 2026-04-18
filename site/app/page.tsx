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
  House,
  Sparkles,
  HeartHandshake,
  MapPin,
  PawPrint,
} from "lucide-react";

const CATEGORY_ICONS = {
  veterinarian: Stethoscope,
  groomer: Scissors,
  boarder: House,
  daycare: Sparkles,
  sitter: HeartHandshake,
} as const;

export default function HomePage() {
  const counts = countByMetro();

  return (
    <div>
      {/* Hero */}
      <section className="relative overflow-hidden bg-warm-50 border-b border-warm-100">
        <div
          className="absolute inset-0 bg-paw-pattern"
          aria-hidden="true"
        />
        <div className="relative mx-auto max-w-6xl px-4 pt-14 pb-12">
          <div className="inline-flex items-center gap-2 text-xs font-medium text-warm-700 bg-warm-100 rounded-full px-3 py-1 mb-4">
            <PawPrint className="w-3.5 h-3.5" />
            A directory for pet parents
          </div>
          <h1 className="text-3xl sm:text-5xl font-semibold tracking-tight text-ink-900 max-w-2xl">
            Find your pet&apos;s people.
          </h1>
          <p className="mt-3 text-ink-700 max-w-xl">
            Vets, groomers, boarders, daycares, and sitters across five metros.
            Pick a city and fetch.
          </p>

          <div className="mt-8 grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-5 gap-3">
            {METROS.map((m) => (
              <Link
                key={m}
                href={`/${m}`}
                className="group rounded-xl border border-warm-100 bg-white p-4 hover:border-warm-500 hover:shadow-sm transition"
              >
                <div className="flex items-center gap-2 text-ink-900 font-medium">
                  <MapPin className="w-4 h-4 text-warm-600" />
                  {METRO_LABELS[m]}
                </div>
                <div className="text-xs text-ink-500 mt-1">
                  {counts[m]} listings
                </div>
              </Link>
            ))}
          </div>
        </div>
      </section>

      {/* Category preview */}
      <section className="mx-auto max-w-6xl px-4 pt-12 pb-10">
        <div className="flex items-baseline justify-between mb-4">
          <h2 className="text-lg font-semibold text-ink-900">
            Browse by service
          </h2>
          <span className="text-xs text-ink-500">
            <PawPrint className="inline w-3.5 h-3.5 mr-1 text-warm-600" />
            every type of care your pet needs
          </span>
        </div>
        <div className="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-5 gap-3">
          {CATEGORIES.map((c) => {
            const Icon = CATEGORY_ICONS[c];
            return (
              <Link
                key={c}
                href={`/atlanta/${c}`}
                className="rounded-xl border border-ink-100 bg-white p-4 hover:border-accent-600 transition"
              >
                <div className="inline-flex items-center justify-center w-10 h-10 rounded-full bg-accent-50">
                  <Icon className="w-5 h-5 text-accent-700" />
                </div>
                <div className="mt-3 font-medium text-ink-900">
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
