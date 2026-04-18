import Link from "next/link";
import { Search, Menu, PawPrint } from "lucide-react";
import { METROS, METRO_LABELS } from "@/types/listing";

export function Header() {
  return (
    <header className="border-b border-ink-100 bg-white/80 backdrop-blur sticky top-0 z-20">
      <div className="mx-auto max-w-6xl px-4 flex items-center justify-between h-14">
        <Link
          href="/"
          className="flex items-center gap-2 font-semibold text-ink-900 tracking-tight"
        >
          <span className="inline-flex items-center justify-center w-8 h-8 rounded-full bg-warm-100">
            <PawPrint className="w-4 h-4 text-warm-700" />
          </span>
          Fetch Directory
        </Link>

        <nav className="flex items-center gap-5 text-sm text-ink-700">
          <details className="relative group">
            <summary className="list-none cursor-pointer select-none flex items-center gap-1 hover:text-ink-900">
              <Menu className="w-4 h-4" />
              Metros
            </summary>
            <div className="absolute right-0 mt-2 w-44 bg-white border border-ink-100 rounded-md shadow-sm p-2">
              {METROS.map((m) => (
                <Link
                  key={m}
                  href={`/${m}`}
                  className="block px-3 py-1.5 rounded hover:bg-ink-50 text-ink-700"
                >
                  {METRO_LABELS[m]}
                </Link>
              ))}
            </div>
          </details>

          <Link
            href="/search"
            className="flex items-center gap-1 hover:text-ink-900"
          >
            <Search className="w-4 h-4" /> Search
          </Link>

          <Link
            href="/claim/new"
            className="hidden sm:inline-block px-3 py-1.5 rounded-md bg-accent-600 text-white hover:bg-accent-700 text-sm"
          >
            List your business
          </Link>
        </nav>
      </div>
    </header>
  );
}
