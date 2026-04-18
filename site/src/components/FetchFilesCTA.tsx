import { FileText } from "lucide-react";

export function FetchFilesCTA({ variant = "banner" }: { variant?: "banner" | "card" }) {
  if (variant === "card") {
    return (
      <a
        href="https://www.fetch-files.com/"
        target="_blank"
        rel="noreferrer"
        className="block rounded-lg border border-accent-600 bg-accent-50 p-4 hover:bg-accent-100 transition"
      >
        <div className="flex items-start gap-3">
          <FileText className="w-5 h-5 text-accent-700 mt-0.5 flex-shrink-0" />
          <div>
            <div className="font-semibold text-accent-800">
              Store your pet&apos;s records with FetchFiles
            </div>
            <div className="text-sm text-accent-800/80 mt-1">
              Keep vaccines, visits, and meds in one place. Share with any vet
              or sitter in seconds.
            </div>
            <div className="text-sm text-accent-700 mt-2 font-medium">
              Open FetchFiles &rarr;
            </div>
          </div>
        </div>
      </a>
    );
  }

  return (
    <section className="bg-accent-700 text-white">
      <div className="mx-auto max-w-6xl px-4 py-10 flex flex-col sm:flex-row items-start sm:items-center justify-between gap-4">
        <div>
          <div className="text-xl font-semibold">
            One place for your pet&apos;s records.
          </div>
          <div className="text-sm text-accent-100 mt-1">
            FetchFiles stores vaccines, vet visits, and meds. Share with any
            listing here in seconds.
          </div>
        </div>
        <a
          href="https://www.fetch-files.com/"
          target="_blank"
          rel="noreferrer"
          className="inline-block px-4 py-2 rounded-md bg-white text-accent-700 font-medium hover:bg-ink-50"
        >
          Try FetchFiles
        </a>
      </div>
    </section>
  );
}
