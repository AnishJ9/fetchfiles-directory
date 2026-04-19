import Link from "next/link";
import { notFound } from "next/navigation";
import type { Metadata } from "next";
import {
  METROS,
  METRO_LABELS,
  CATEGORIES,
  CATEGORY_LABELS,
  CATEGORY_LABELS_SINGULAR,
  type Category,
  type Metro,
} from "@/types/listing";
import {
  getAllListings,
  getListingById,
  getRelatedListings,
  getNearbyServices,
} from "@/lib/data";
import { FetchFilesCTA } from "@/components/FetchFilesCTA";
import { CategoryGuide } from "@/components/CategoryGuide";
import { ListingTile } from "@/components/ListingTile";
import { Comments } from "@/components/Comments";
import {
  MapPin,
  Phone,
  Globe,
  Mail,
  Clock,
  BadgeCheck,
  Navigation,
} from "lucide-react";

export function generateStaticParams() {
  return getAllListings().map((l) => ({
    metro: l.metro,
    category: l.category,
    id: l.id,
  }));
}

export function generateMetadata({
  params,
}: {
  params: { metro: string; category: string; id: string };
}): Metadata {
  const l = getListingById(params.id);
  if (!l) return {};
  const cLabel = CATEGORY_LABELS_SINGULAR[l.category];
  return {
    title: `${l.name} — ${cLabel} in ${l.city}`,
    description:
      l.description ??
      `${l.name} is a ${cLabel.toLowerCase()} in ${l.city}, ${l.state}. View address, phone, and hours.`,
  };
}

export default function ListingPage({
  params,
}: {
  params: { metro: string; category: string; id: string };
}) {
  const metro = params.metro as Metro;
  const category = params.category as Category;
  if (!METROS.includes(metro) || !CATEGORIES.includes(category)) notFound();

  const l = getListingById(params.id);
  if (!l || l.metro !== metro || l.category !== category) notFound();

  const related = getRelatedListings(l, 6);
  const nearby = getNearbyServices(l, 6);

  const mLabel = METRO_LABELS[metro];
  const cLabel = CATEGORY_LABELS[category];

  // OSM bbox-based static iframe — no API key needed
  const bboxDelta = 0.008;
  const left = l.lng - bboxDelta;
  const right = l.lng + bboxDelta;
  const top = l.lat + bboxDelta;
  const bottom = l.lat - bboxDelta;
  const mapSrc = `https://www.openstreetmap.org/export/embed.html?bbox=${left}%2C${bottom}%2C${right}%2C${top}&layer=mapnik&marker=${l.lat}%2C${l.lng}`;
  const mapLink = `https://www.openstreetmap.org/?mlat=${l.lat}&mlon=${l.lng}#map=16/${l.lat}/${l.lng}`;

  const directionsQuery = encodeURIComponent(
    `${l.name}, ${l.address}, ${l.city}, ${l.state} ${l.zip}`,
  );
  const googleDirections = `https://www.google.com/maps/dir/?api=1&destination=${directionsQuery}`;
  const appleDirections = `https://maps.apple.com/?q=${directionsQuery}`;

  const jsonLd = {
    "@context": "https://schema.org",
    "@type": "LocalBusiness",
    name: l.name,
    address: {
      "@type": "PostalAddress",
      streetAddress: l.address,
      addressLocality: l.city,
      addressRegion: l.state,
      postalCode: l.zip,
      addressCountry: "US",
    },
    geo: {
      "@type": "GeoCoordinates",
      latitude: l.lat,
      longitude: l.lng,
    },
    ...(l.phone ? { telephone: l.phone } : {}),
    ...(l.website ? { url: l.website } : {}),
    ...(l.email ? { email: l.email } : {}),
    ...(l.description ? { description: l.description } : {}),
  };

  const days = [
    ["mon", "Mon"],
    ["tue", "Tue"],
    ["wed", "Wed"],
    ["thu", "Thu"],
    ["fri", "Fri"],
    ["sat", "Sat"],
    ["sun", "Sun"],
  ] as const;

  return (
    <div>
      <script
        type="application/ld+json"
        dangerouslySetInnerHTML={{ __html: JSON.stringify(jsonLd) }}
      />

      <section className="mx-auto max-w-6xl px-4 pt-14 pb-6">
        <div className="text-sm text-ink-500 mb-2">
          <Link href="/" className="hover:text-ink-900">
            Home
          </Link>{" "}
          /{" "}
          <Link href={`/${metro}`} className="hover:text-ink-900">
            {mLabel}
          </Link>{" "}
          /{" "}
          <Link href={`/${metro}/${category}`} className="hover:text-ink-900">
            {cLabel}
          </Link>{" "}
          / {l.name}
        </div>

        <div className="flex items-start justify-between gap-3">
          <div>
            <h1 className="text-3xl sm:text-4xl font-semibold tracking-tight text-ink-900">
              {l.name}
            </h1>
            <div className="mt-2 text-ink-500 flex items-center gap-2 text-sm flex-wrap">
              <span className="inline-block px-2 py-0.5 rounded-full bg-warm-100 text-warm-700">
                {CATEGORY_LABELS_SINGULAR[l.category]}
              </span>
              <span className="text-ink-500">
                in {l.city}, {l.state}
              </span>
              {l.subcategories?.map((s) => (
                <span
                  key={s}
                  className="inline-block px-2 py-0.5 rounded-full bg-ink-50 text-ink-500"
                >
                  {s}
                </span>
              ))}
              {l.claimed && (
                <span className="inline-flex items-center gap-1 text-accent-700">
                  <BadgeCheck className="w-4 h-4" /> Claimed
                </span>
              )}
            </div>
          </div>
        </div>
      </section>

      <section className="mx-auto max-w-6xl px-4 pb-10 grid grid-cols-1 lg:grid-cols-3 gap-6">
        <div className="lg:col-span-2 space-y-6">
          {l.description && (
            <div className="rounded-xl border border-ink-100 bg-white p-5">
              <div className="text-sm text-ink-700 leading-relaxed">
                {l.description}
              </div>
            </div>
          )}

          <div className="rounded-xl border border-ink-100 bg-white p-5">
            <h2 className="font-semibold text-ink-900 mb-3">Contact</h2>
            <dl className="space-y-2 text-sm text-ink-700">
              <div className="flex items-start gap-2">
                <MapPin className="w-4 h-4 text-ink-500 mt-0.5 flex-shrink-0" />
                <div>
                  {l.address}
                  <br />
                  {l.city}, {l.state} {l.zip}
                </div>
              </div>
              {l.phone && (
                <div className="flex items-center gap-2">
                  <Phone className="w-4 h-4 text-ink-500" />
                  <a
                    href={`tel:${l.phone}`}
                    className="hover:text-accent-700"
                  >
                    {l.phone}
                  </a>
                </div>
              )}
              {l.website && (
                <div className="flex items-center gap-2">
                  <Globe className="w-4 h-4 text-ink-500" />
                  <a
                    href={l.website}
                    target="_blank"
                    rel="noreferrer"
                    className="hover:text-accent-700 break-all"
                  >
                    {l.website}
                  </a>
                </div>
              )}
              {l.email && (
                <div className="flex items-center gap-2">
                  <Mail className="w-4 h-4 text-ink-500" />
                  <a
                    href={`mailto:${l.email}`}
                    className="hover:text-accent-700"
                  >
                    {l.email}
                  </a>
                </div>
              )}
            </dl>
            <div className="mt-4 pt-3 border-t border-ink-100 flex flex-wrap gap-2 text-sm">
              <a
                href={googleDirections}
                target="_blank"
                rel="noreferrer"
                className="inline-flex items-center gap-1.5 px-3 py-1.5 rounded-md bg-accent-600 text-white hover:bg-accent-700"
              >
                <Navigation className="w-4 h-4" />
                Get directions
              </a>
              <a
                href={appleDirections}
                target="_blank"
                rel="noreferrer"
                className="inline-flex items-center gap-1.5 px-3 py-1.5 rounded-md border border-ink-100 text-ink-700 hover:bg-ink-50"
              >
                Apple Maps
              </a>
            </div>
          </div>

          {l.hours && Object.keys(l.hours).length > 0 && (
            <div className="rounded-xl border border-ink-100 bg-white p-5">
              <h2 className="font-semibold text-ink-900 mb-3 flex items-center gap-2">
                <Clock className="w-4 h-4 text-ink-500" /> Hours
              </h2>
              <dl className="grid grid-cols-2 sm:grid-cols-3 gap-y-1 gap-x-6 text-sm text-ink-700">
                {days.map(([key, label]) => (
                  <div key={key} className="flex justify-between">
                    <dt className="text-ink-500">{label}</dt>
                    <dd>{l.hours?.[key] ?? "—"}</dd>
                  </div>
                ))}
              </dl>
            </div>
          )}

          <div className="rounded-xl border border-ink-100 bg-white overflow-hidden">
            <iframe
              src={mapSrc}
              className="w-full h-64"
              loading="lazy"
              title={`Map of ${l.name}`}
            />
            <div className="p-3 text-xs text-ink-500 border-t border-ink-100">
              <a
                href={mapLink}
                target="_blank"
                rel="noreferrer"
                className="underline hover:text-ink-900"
              >
                View larger map &rarr;
              </a>
            </div>
          </div>

          <CategoryGuide category={l.category} />

          {related.length > 0 && (
            <div>
              <h2 className="font-semibold text-ink-900 mb-3">
                Other {cLabel.toLowerCase()} in {mLabel}
              </h2>
              <div className="grid grid-cols-1 sm:grid-cols-2 gap-3">
                {related.map((r) => (
                  <ListingTile key={r.id} listing={r} />
                ))}
              </div>
              <Link
                href={`/${metro}/${category}`}
                className="mt-3 inline-block text-sm text-accent-700 font-medium hover:text-accent-800"
              >
                See all {cLabel.toLowerCase()} in {mLabel} &rarr;
              </Link>
            </div>
          )}

          {nearby.length > 0 && (
            <div>
              <h2 className="font-semibold text-ink-900 mb-3">
                Nearby services
              </h2>
              <div className="grid grid-cols-1 sm:grid-cols-2 gap-3">
                {nearby.map((r) => (
                  <ListingTile key={r.id} listing={r} />
                ))}
              </div>
            </div>
          )}

          <Comments term={l.id} />
        </div>

        <aside className="space-y-4">
          <FetchFilesCTA variant="card" />
          <Link
            href={`/claim/${l.id}`}
            className="block rounded-xl border border-ink-100 bg-white p-4 hover:border-accent-600 transition"
          >
            <div className="font-semibold text-ink-900">Claim this listing</div>
            <div className="text-sm text-ink-500 mt-1">
              Own {l.name}? Confirm your info and keep it current.
            </div>
            <div className="text-sm text-accent-700 mt-2 font-medium">
              Start claim &rarr;
            </div>
          </Link>
        </aside>
      </section>
    </div>
  );
}
