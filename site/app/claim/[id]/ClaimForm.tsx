"use client";

import { useState } from "react";

interface Prefill {
  listingId: string;
  businessName: string;
  city: string;
  state: string;
}

export function ClaimForm({ prefill }: { prefill: Prefill }) {
  const [status, setStatus] = useState<
    "idle" | "submitting" | "success" | "error"
  >("idle");
  const [error, setError] = useState<string>("");

  async function onSubmit(e: React.FormEvent<HTMLFormElement>) {
    e.preventDefault();
    setStatus("submitting");
    setError("");

    const form = e.currentTarget;
    const data = Object.fromEntries(new FormData(form).entries());
    data.listingId = prefill.listingId;

    try {
      const res = await fetch("/api/claim", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(data),
      });
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      setStatus("success");
    } catch (err) {
      setError(err instanceof Error ? err.message : "Unknown error");
      setStatus("error");
    }
  }

  if (status === "success") {
    return (
      <div className="rounded-lg border border-accent-600 bg-accent-50 p-5 text-accent-800">
        <div className="font-semibold">Thanks — we got it.</div>
        <div className="text-sm mt-1">
          We&apos;ll review your claim and reach out at the email you gave us.
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

      {status === "error" && (
        <div className="text-sm text-red-700">
          Sorry — something went wrong submitting that. {error}
        </div>
      )}

      <button
        type="submit"
        disabled={status === "submitting"}
        className="px-4 py-2 rounded-md bg-accent-600 text-white text-sm font-medium hover:bg-accent-700 disabled:opacity-60"
      >
        {status === "submitting" ? "Submitting..." : "Submit claim"}
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
