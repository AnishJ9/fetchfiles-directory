"use client";

import { useState } from "react";

interface Prefill {
  listingId: string;
  businessName: string;
  city: string;
  state: string;
}

const DEST_EMAIL = "anish.joseph58@gmail.com";

export function ClaimForm({ prefill }: { prefill: Prefill }) {
  const [sent, setSent] = useState(false);

  function onSubmit(e: React.FormEvent<HTMLFormElement>) {
    e.preventDefault();
    const form = e.currentTarget;
    const data = Object.fromEntries(
      new FormData(form).entries(),
    ) as Record<string, string>;

    const subject = prefill.businessName
      ? `Claim listing: ${prefill.businessName}`
      : `New listing submission: ${data.businessName ?? "(no name)"}`;

    const lines = [
      `Listing ID: ${prefill.listingId}`,
      `Business: ${data.businessName ?? ""}`,
      `Owner: ${data.ownerName ?? ""}`,
      `Email: ${data.email ?? ""}`,
      `Phone: ${data.phone ?? ""}`,
      `Role: ${data.relationship ?? ""}`,
      `Notes: ${data.notes ?? ""}`,
    ];

    const mailto =
      `mailto:${DEST_EMAIL}` +
      `?subject=${encodeURIComponent(subject)}` +
      `&body=${encodeURIComponent(lines.join("\n"))}`;

    window.location.href = mailto;
    setSent(true);
  }

  if (sent) {
    return (
      <div className="rounded-lg border border-accent-600 bg-accent-50 p-5 text-accent-800">
        <div className="font-semibold">Your email client should have opened.</div>
        <div className="text-sm mt-1">
          Send the draft and we&apos;ll get in touch. If nothing opened, email{" "}
          <a className="underline" href={`mailto:${DEST_EMAIL}`}>
            {DEST_EMAIL}
          </a>{" "}
          directly.
        </div>
      </div>
    );
  }

  return (
    <form onSubmit={onSubmit} className="space-y-4">
      <Field
        name="businessName"
        label="Business name"
        defaultValue={prefill.businessName}
        required
      />
      <Field name="ownerName" label="Your name" required />
      <Field name="email" type="email" label="Email" required />
      <Field name="phone" type="tel" label="Phone" />
      <Field
        name="relationship"
        label="Your role at the business"
        placeholder="Owner, manager, etc."
        required
      />
      <div>
        <label className="block text-sm font-medium text-ink-700 mb-1">
          Notes (optional)
        </label>
        <textarea
          name="notes"
          rows={3}
          className="w-full rounded-md border border-ink-100 px-3 py-2 text-sm focus:outline-none focus:border-accent-600 bg-white"
          placeholder="Anything we should know?"
        />
      </div>

      <button
        type="submit"
        className="px-4 py-2 rounded-md bg-accent-600 text-white text-sm font-medium hover:bg-accent-700"
      >
        Submit claim
      </button>
    </form>
  );
}

function Field({
  name,
  label,
  type = "text",
  defaultValue,
  placeholder,
  required,
}: {
  name: string;
  label: string;
  type?: string;
  defaultValue?: string;
  placeholder?: string;
  required?: boolean;
}) {
  return (
    <div>
      <label className="block text-sm font-medium text-ink-700 mb-1">
        {label}
        {required && <span className="text-red-600"> *</span>}
      </label>
      <input
        name={name}
        type={type}
        defaultValue={defaultValue}
        placeholder={placeholder}
        required={required}
        className="w-full rounded-md border border-ink-100 px-3 py-2 text-sm focus:outline-none focus:border-accent-600 bg-white"
      />
    </div>
  );
}
