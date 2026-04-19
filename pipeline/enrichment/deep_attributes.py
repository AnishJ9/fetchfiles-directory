"""Deep-crawl each listing's website to detect service attributes beyond homepage.

For every listing in data/listings.json with a website and a category in
{veterinarian, groomer, boarder, daycare, sitter, pet_hotel}, fetch the
homepage, collect same-host candidate URLs (services / about / emergency /
exotic / cats / etc.), fetch up to 6 of them, concat the visible text, and
run it through the existing attributes_for() rules.

Writes a supplement at data/enrichment/deep_attributes.json. Shape matches
data/enrichment/attributes.json:

  { "<listing_id>": { "attributes": ["emergency", "exotic"] }, ... }

Politeness:
  User-Agent: FetchDirectory-Bot/0.1 (anish.joseph58@gmail.com)
  10s per-request hard timeout, <=3 retries, max 3 redirects
  0.5-1s jitter between requests to the same host
  256 KB stream limit per page, HTML-only
  Max 8 worker threads (no total runtime cap — runs to completion)
  Max 6 non-homepage URLs per listing
"""
from __future__ import annotations

import html
import json
import random
import re
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from html.parser import HTMLParser
from pathlib import Path
from urllib.parse import urljoin, urlparse

import requests

from pipeline.enrichment.attributes import attributes_for

REPO = Path(__file__).resolve().parent.parent.parent
LISTINGS_PATH = REPO / "data" / "listings.json"
EXISTING_ATTRS_PATH = REPO / "data" / "enrichment" / "attributes.json"
OUT_PATH = REPO / "data" / "enrichment" / "deep_attributes.json"

USER_AGENT = "FetchDirectory-Bot/0.1 (anish.joseph58@gmail.com)"
REQUEST_TIMEOUT = 10          # seconds, hard per request
MAX_RETRIES = 3
MAX_REDIRECTS = 3
MAX_WORKERS = 8
JITTER_MIN = 0.5
JITTER_MAX = 1.0
MAX_BYTES = 256 * 1024        # 256 KB stream cap per page
MAX_SUBPAGES = 6              # 6 URLs beyond homepage per listing

TARGET_CATEGORIES = {
    "veterinarian",
    "groomer",
    "boarder",
    "daycare",
    "sitter",
    "pet_hotel",
}

# TLDs observed across listings — same set as descriptions.py but include a few
# more common vet TLDs just in case.
ALLOWED_TLDS = {
    "com", "org", "net", "io", "co", "us", "vet", "biz", "info",
    "health", "care", "pet", "pets", "dog", "cat", "club",
}

# Keyword fragments (case-insensitive) that mark a URL / link text as
# worth fetching. Substring match against path + query + link text.
SUBPAGE_KEYWORDS = [
    "services", "specialt", "about", "emergency", "exotic", "avian",
    "reptile", "cats", "cat-only", "cat-exclusive", "feline", "hours",
    "urgent", "24hr", "24-hour", "boarding", "grooming", "breed",
    "mobile", "house-call",
]

# File extensions we won't follow into.
SKIP_EXTENSIONS = {
    ".pdf", ".jpg", ".jpeg", ".png", ".gif", ".webp", ".svg", ".ico",
    ".mp4", ".mov", ".avi", ".mp3", ".wav", ".zip", ".tar", ".gz",
    ".doc", ".docx", ".xls", ".xlsx", ".ppt", ".pptx", ".css", ".js",
}


# ---------- HTML extraction --------------------------------------------------

class _LinkAndTextParser(HTMLParser):
    """Collect <a href> with surrounding text and visible page text."""

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.links: list[tuple[str, list[str]]] = []  # (href, text buffer)
        self.text_parts: list[str] = []
        self._suppress_depth = 0
        self._a_stack: list[tuple[str, list[str]]] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag in ("script", "style", "noscript", "template", "svg"):
            self._suppress_depth += 1
            return
        if tag == "a":
            d = {k.lower(): (v or "") for k, v in attrs}
            href = d.get("href", "").strip()
            buf: list[str] = []
            self._a_stack.append((href, buf))

    def handle_endtag(self, tag: str) -> None:
        if tag in ("script", "style", "noscript", "template", "svg"):
            if self._suppress_depth > 0:
                self._suppress_depth -= 1
            return
        if tag == "a" and self._a_stack:
            href, buf = self._a_stack.pop()
            if href:
                self.links.append((href, buf))

    def handle_startendtag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        # Self-closing <a /> — unusual but handle.
        if tag == "a":
            d = {k.lower(): (v or "") for k, v in attrs}
            href = d.get("href", "").strip()
            if href:
                self.links.append((href, []))

    def handle_data(self, data: str) -> None:
        if self._suppress_depth > 0:
            return
        if self._a_stack:
            # Feed anchor-local buffer so we can score link text.
            self._a_stack[-1][1].append(data)
        self.text_parts.append(data)


def _decode(body: bytes) -> str:
    try:
        return body.decode("utf-8", errors="replace")
    except Exception:
        return body.decode("latin-1", errors="replace")


def parse_page(body: bytes) -> tuple[list[tuple[str, str]], str]:
    """Return (list of (href, link_text), visible_text_blob)."""
    parser = _LinkAndTextParser()
    try:
        parser.feed(_decode(body))
        parser.close()
    except Exception:
        # Even partial is fine.
        pass
    links: list[tuple[str, str]] = []
    for href, buf in parser.links:
        text = " ".join("".join(buf).split()).strip()
        links.append((href, text))
    blob = " ".join("".join(parser.text_parts).split()).strip()
    blob = html.unescape(blob)
    return links, blob


# ---------- URL handling -----------------------------------------------------

def _host_root(host: str) -> str:
    """Drop leading 'www.' but keep full host otherwise (no subdomain guess)."""
    host = (host or "").lower()
    if host.startswith("www."):
        host = host[4:]
    return host


def url_is_safe(url: str) -> tuple[bool, str]:
    try:
        u = url if "://" in url else "http://" + url
        p = urlparse(u)
    except Exception:
        return False, "parse_error"
    if p.scheme not in ("http", "https"):
        return False, "bad_scheme"
    host = (p.hostname or "").lower()
    if not host or host in ("localhost", "127.0.0.1", "::1"):
        return False, "localhost"
    if "." not in host:
        return False, "no_tld"
    tld = host.rsplit(".", 1)[-1]
    if tld not in ALLOWED_TLDS:
        return False, f"tld:{tld}"
    return True, ""


def score_candidate(url: str, link_text: str) -> int:
    """Count keyword hits across lowercase URL path+query and link text."""
    try:
        p = urlparse(url)
    except Exception:
        return 0
    path = (p.path or "").lower()
    query = (p.query or "").lower()
    text = (link_text or "").lower()
    combined = f"{path} {query} {text}"
    return sum(1 for kw in SUBPAGE_KEYWORDS if kw in combined)


def pick_subpages(
    homepage_url: str,
    homepage_host: str,
    links: list[tuple[str, str]],
    limit: int,
) -> list[str]:
    """Return up to `limit` same-host URLs worth visiting, highest score first."""
    seen: set[str] = set()
    candidates: list[tuple[int, str]] = []
    for href, text in links:
        if not href:
            continue
        href = href.strip()
        if href.startswith("#") or href.startswith("javascript:") or href.startswith("mailto:") or href.startswith("tel:"):
            continue
        full = urljoin(homepage_url, href)
        # Drop fragment, normalize.
        try:
            parsed = urlparse(full)
        except Exception:
            continue
        if parsed.scheme not in ("http", "https"):
            continue
        host = _host_root(parsed.hostname or "")
        if host != homepage_host:
            continue
        path = parsed.path or "/"
        # Skip non-HTML extensions.
        lower_path = path.lower()
        for ext in SKIP_EXTENSIONS:
            if lower_path.endswith(ext):
                break
        else:
            # no-break: file extension is OK
            normalized = parsed._replace(fragment="").geturl()
            if normalized in seen:
                continue
            if normalized == homepage_url:
                continue
            score = score_candidate(normalized, text)
            if score == 0:
                continue
            seen.add(normalized)
            candidates.append((score, normalized))
    # Highest score first, stable-ish by URL string
    candidates.sort(key=lambda x: (-x[0], x[1]))
    return [u for _, u in candidates[:limit]]


# ---------- Fetching ---------------------------------------------------------

def make_session() -> requests.Session:
    s = requests.Session()
    s.headers["User-Agent"] = USER_AGENT
    s.headers["Accept"] = "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"
    s.headers["Accept-Language"] = "en-US,en;q=0.9"
    s.max_redirects = MAX_REDIRECTS
    return s


def fetch(session: requests.Session, url: str) -> tuple[bytes | None, str]:
    if "://" not in url:
        url = "http://" + url
    last_err = "error:unknown"
    for attempt in range(MAX_RETRIES):
        try:
            with session.get(
                url,
                timeout=REQUEST_TIMEOUT,
                allow_redirects=True,
                stream=True,
            ) as r:
                if r.status_code in (404, 403, 410):
                    return None, str(r.status_code)
                if r.status_code >= 400:
                    last_err = f"http:{r.status_code}"
                    if r.status_code < 500:
                        return None, last_err
                    time.sleep(0.5 * (attempt + 1))
                    continue
                ctype = (r.headers.get("Content-Type") or "").lower()
                if ctype and "html" not in ctype and "xml" not in ctype:
                    return None, f"ctype:{ctype.split(';')[0]}"
                buf = bytearray()
                for chunk in r.iter_content(chunk_size=16 * 1024):
                    if not chunk:
                        break
                    buf.extend(chunk)
                    if len(buf) >= MAX_BYTES:
                        break
                return bytes(buf), "ok"
        except requests.exceptions.Timeout:
            last_err = "timeout"
        except requests.exceptions.TooManyRedirects:
            return None, "too_many_redirects"
        except requests.exceptions.SSLError:
            return None, "ssl_error"
        except requests.exceptions.ConnectionError:
            last_err = "conn_error"
        except Exception as e:
            last_err = f"error:{type(e).__name__}"
        time.sleep(0.5 * (attempt + 1))
    return None, last_err


# ---------- Worker -----------------------------------------------------------

def process_one(listing: dict) -> tuple[str, list[str], dict]:
    """Return (id, attributes, debug_info).

    debug_info keys: status, pages_fetched, subpages_tried.
    """
    lid = listing["id"]
    info = {"status": "ok", "pages_fetched": 0, "subpages_tried": 0}
    url = (listing.get("website") or "").strip()
    ok, reason = url_is_safe(url)
    if not ok:
        info["status"] = f"skip:{reason}"
        return lid, [], info

    session = make_session()
    homepage_host = _host_root(urlparse(url if "://" in url else "http://" + url).hostname or "")

    # Homepage
    time.sleep(random.uniform(JITTER_MIN, JITTER_MAX))
    body, status = fetch(session, url)
    if status != "ok" or body is None:
        info["status"] = f"homepage:{status}"
        return lid, [], info
    info["pages_fetched"] = 1

    home_links, home_text = parse_page(body)
    text_parts: list[str] = [home_text]

    # Pick candidate subpages.
    homepage_normalized = urlparse(url if "://" in url else "http://" + url)._replace(fragment="").geturl()
    subpages = pick_subpages(homepage_normalized, homepage_host, home_links, MAX_SUBPAGES)
    info["subpages_tried"] = len(subpages)

    for sub_url in subpages:
        time.sleep(random.uniform(JITTER_MIN, JITTER_MAX))
        body, status = fetch(session, sub_url)
        if status != "ok" or body is None:
            continue
        info["pages_fetched"] += 1
        _, sub_text = parse_page(body)
        if sub_text:
            text_parts.append(sub_text)

    combined = " ".join(text_parts)
    synthetic = {
        "category": listing.get("category", ""),
        "name": listing.get("name", ""),
        "website": listing.get("website", ""),
        "description": combined,
        "tags": listing.get("tags", []),
    }
    attrs = attributes_for(synthetic)
    return lid, attrs, info


# ---------- Driver -----------------------------------------------------------

def main() -> None:
    listings = json.loads(LISTINGS_PATH.read_text())
    targets = [
        l for l in listings
        if (l.get("website") or "").strip()
        and l.get("category") in TARGET_CATEGORIES
    ]

    existing: dict = {}
    if EXISTING_ATTRS_PATH.exists():
        try:
            existing = json.loads(EXISTING_ATTRS_PATH.read_text())
        except Exception:
            existing = {}

    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)

    print(f"[deep_attributes] {len(targets)} eligible listings")
    print(f"[deep_attributes] workers: {MAX_WORKERS} (no total runtime cap)")
    print(f"[deep_attributes] existing flagged (homepage pass): {len(existing)}")

    results: dict[str, dict] = {}
    status_counts: Counter[str] = Counter()
    page_counts: list[int] = []
    start = time.monotonic()

    completed = 0
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = {ex.submit(process_one, l): l["id"] for l in targets}
        for fut in as_completed(futures):
            try:
                lid, attrs, info = fut.result()
            except Exception as e:
                lid = futures[fut]
                attrs, info = [], {"status": f"error:{type(e).__name__}", "pages_fetched": 0, "subpages_tried": 0}
            status_counts[info.get("status", "unknown")] += 1
            page_counts.append(int(info.get("pages_fetched", 0)))
            if attrs:
                results[lid] = {"attributes": attrs}
            completed += 1
            if completed % 25 == 0 or completed == len(targets):
                elapsed = int(time.monotonic() - start)
                print(
                    f"[deep_attributes] {completed}/{len(targets)} "
                    f"flagged={len(results)} elapsed={elapsed}s",
                    flush=True,
                )

    # Only write once fully done.
    OUT_PATH.write_text(json.dumps(results, indent=2, sort_keys=True))

    # --- Summary ---
    avg_pages = (sum(page_counts) / len(page_counts)) if page_counts else 0.0
    newly_flagged = [lid for lid in results if lid not in existing]
    attr_new_hits: Counter[str] = Counter()
    for lid in newly_flagged:
        for a in results[lid]["attributes"]:
            attr_new_hits[a] += 1

    print()
    print(f"wrote {OUT_PATH} ({len(results)} entries)")
    print()
    print(f"eligible listings          : {len(targets)}")
    print(f"processed                  : {completed}")
    print(f"listings flagged (deep)    : {len(results)}")
    print(f"newly flagged vs homepage  : {len(newly_flagged)}")
    print(f"avg pages fetched / site   : {avg_pages:.2f}")
    print()
    print("new-hit breakdown by attribute:")
    for attr, n in attr_new_hits.most_common():
        print(f"  {attr:<14} {n}")
    print()
    print("status breakdown:")
    for status, count in status_counts.most_common():
        print(f"  {status:<28} {count}")


if __name__ == "__main__":
    main()
