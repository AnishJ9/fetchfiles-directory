import type { Metadata } from "next";
import Link from "next/link";
import { getListingById } from "@/lib/data";
import { ClaimForm } from "./ClaimForm";

export const metadata: Metadata = {
  title: "Claim a listing",
  description:
    "Own your business listing on Fetch Directory. Keep hours, contact info, and services current.",
};

// Note: claim/[id] is not statically pre-rendered — the id may reference a
// listing that doesn't exist yet (e.g. "new") or one added after build.
// Rendering is per-request here.
export const dynamic = "force-dynamic";

export default function ClaimPage({ params }: { params: { id: string } }) {
  const listing =
    params.id === "new" ? undefined : getListingById(params.id);

  const prefill = {
    listingId: params.id,
    businessName: listing?.name ?? "",
    city: listing?.city ?? "",
    state: listing?.state ?? "",
  };

  return (
    <div className="mx-auto max-w-2xl px-4 pt-14 pb-16">
      <div className="text-sm text-ink-500 mb-2">
        <Link href="/" className="hover:text-ink-900">
          Home
        </Link>{" "}
        / Claim
      </div>

      <h1 className="text-3xl sm:text-4xl font-semibold tracking-tight text-ink-900">
        {listing ? `Claim ${listing.name}` : "List your business"}
      </h1>
      <p className="mt-2 text-ink-500">
        {listing
          ? "Confirm you own this business and we'll get in touch to verify."
          : "Tell us about your business and we'll add it to the directory."}
      </p>

      {listing && (
        <div className="mt-4 rounded-md border border-ink-100 bg-white p-3 text-sm text-ink-700">
          <div className="font-medium text-ink-900">{listing.name}</div>
          <div className="text-ink-500">
            {listing.address}, {listing.city}, {listing.state} {listing.zip}
          </div>
        </div>
      )}

      <div className="mt-6">
        <ClaimForm prefill={prefill} />
      </div>
    </div>
  );
}
