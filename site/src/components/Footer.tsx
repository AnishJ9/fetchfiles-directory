import Link from "next/link";
import { METROS, METRO_LABELS } from "@/types/listing";

export function Footer() {
  return (
    <footer className="border-t border-ink-100 bg-white mt-16">
      <div className="mx-auto max-w-6xl px-4 py-10 text-sm text-ink-500 flex flex-col sm:flex-row gap-6 sm:items-start sm:justify-between">
        <div className="space-y-2">
          <div className="font-semibold text-ink-900">Fetch Directory</div>
          <div>Pet services you can trust, in the cities you know.</div>
          <div className="text-xs">
            Powered by{" "}
            <a
              href="https://www.fetch-files.com/"
              className="underline hover:text-ink-900"
              target="_blank"
              rel="noreferrer"
            >
              FetchFiles
            </a>
            .
          </div>
        </div>

        <div>
          <div className="font-medium text-ink-700 mb-2">Metros</div>
          <ul className="grid grid-cols-2 gap-x-6 gap-y-1">
            {METROS.map((m) => (
              <li key={m}>
                <Link href={`/${m}`} className="hover:text-ink-900">
                  {METRO_LABELS[m]}
                </Link>
              </li>
            ))}
          </ul>
        </div>

        <div className="text-xs">
          &copy; {new Date().getFullYear()} Fetch Directory
        </div>
      </div>
    </footer>
  );
}
