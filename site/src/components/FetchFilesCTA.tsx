import { FileText, PawPrint } from "lucide-react";

export function FetchFilesCTA({
  variant = "banner",
}: {
  variant?: "banner" | "card";
}) {
  if (variant === "card") {
    return (
      <a
        href="https://www.fetch-files.com/"
        target="_blank"
        rel="noreferrer"
        className="block rounded-xl border border-accent-600 bg-accent-50 p-4 hover:bg-accent-100 transition"
      >
        <div className="flex items-start gap-3">
          <FileText className="w-5 h-5 text-accent-700 mt-0.5 flex-shrink-0" />
          <div>
            <div className="font-semibold text-accent-800">
              Keep your pet&apos;s records travel-ready
            </div>
            <div className="text-sm text-accent-800/80 mt-1">
              Vaccines, visits, and meds in one place. Share with any vet or
              sitter in seconds via FetchFiles.
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
    <section className="relative overflow-hidden bg-accent-700 text-white">
      <PawPrint
        className="absolute -right-6 -bottom-8 w-44 h-44 text-white/10 rotate-[-20deg]"
        aria-hidden="true"
      />
      <div className="relative mx-auto max-w-6xl px-4 py-12 flex flex-col sm:flex-row items-start sm:items-center justify-between gap-5">
        <div>
          <div className="text-2xl font-semibold">
            Your pet&apos;s records, always within reach.
          </div>
          <div className="text-sm text-accent-100 mt-2 max-w-xl">
            FetchFiles keeps every vaccine, vet visit, and prescription in one
            place — share with any sitter, groomer, or clinic in seconds.
          </div>
        </div>
        <a
          href="https://www.fetch-files.com/"
          target="_blank"
          rel="noreferrer"
          className="inline-flex items-center gap-2 px-5 py-2.5 rounded-md bg-white text-accent-700 font-medium hover:bg-ink-50 whitespace-nowrap"
        >
          <PawPrint className="w-4 h-4" />
          Try FetchFiles
        </a>
      </div>
    </section>
  );
}
