"""Scrape homepage meta description from each listing's website.

For every listing in data/listings.json with a non-empty `website`, fetch the
homepage and extract the best short description. Writes a single lookup JSON
keyed by listing id to data/enrichment/descriptions.json. The merge step
(pipeline/merge.py) folds the description into each listing later.

Extraction priority (first present, length >= 40 chars):
  1. <meta property="og:description" content="...">
  2. <meta name="description" content="...">
  3. <meta name="twitter:description" content="...">
  4. First <p> tag with >= 40 chars of stripped text.

Politeness:
  User-Agent: FetchDirectory-Bot/0.1 (anish.joseph58@gmail.com)
  10s per-request timeout (hard), <=3 retries, 0.5-1s jitter between requests
  Max 8 worker threads, hard 30 min runtime cap.
  Skip localhost / unusual TLDs / 404 / 403.
  Limit redirects to 3.
"""
from __future__ import annotations

import html
import json
import random
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from html.parser import HTMLParser
from pathlib import Path
from urllib.parse import urlparse

import requests

REPO = Path(__file__).resolve().parent.parent.parent
LISTINGS_PATH = REPO / "data" / "listings.json"
OUT_PATH = REPO / "data" / "enrichment" / "descriptions.json"

USER_AGENT = "FetchDirectory-Bot/0.1 (anish.joseph58@gmail.com)"
REQUEST_TIMEOUT = 10           # seconds, hard
MAX_RETRIES = 3
MAX_REDIRECTS = 3
MAX_WORKERS = 8
RUNTIME_CAP_S = 30 * 60        # 30 min hard cap
JITTER_MIN = 0.5
JITTER_MAX = 1.0
MAX_DESC_LEN = 400
MIN_DESC_LEN = 40
MAX_BYTES = 512 * 1024         # only parse first 512 KB of HTML

# Conservative TLD allowlist (covers all observed TLDs; adjust as needed).
ALLOWED_TLDS = {
    "com", "org", "net", "io", "co", "us", "vet", "biz", "info",
    "health", "care", "pet", "pets", "dog", "cat", "club",
}


# ---------- HTML parsing -----------------------------------------------------

class _MetaAndPParser(HTMLParser):
    """Streaming parser to capture meta descriptions + first usable <p> text.

    We don't use BeautifulSoup to avoid an extra dependency. The parser
    short-circuits as soon as we have all three meta candidates AND a usable
    paragraph; the caller can also stop feeding bytes once enough is collected.
    """

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.og: str | None = None
        self.meta: str | None = None
        self.tw: str | None = None
        self.first_p: str | None = None
        self._in_p = False
        self._p_buf: list[str] = []
        # ignore <p>s nested inside script/style/nav-style stuff
        self._suppress_depth = 0

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag == "meta":
            d = {k.lower(): (v or "") for k, v in attrs}
            content = d.get("content") or ""
            if not content:
                return
            prop = d.get("property", "").lower()
            name = d.get("name", "").lower()
            if prop == "og:description" and self.og is None:
                self.og = content
            elif name == "description" and self.meta is None:
                self.meta = content
            elif name == "twitter:description" and self.tw is None:
                self.tw = content
        elif tag in ("script", "style", "noscript"):
            self._suppress_depth += 1
        elif tag == "p" and self._suppress_depth == 0 and self.first_p is None:
            self._in_p = True
            self._p_buf = []

    def handle_endtag(self, tag: str) -> None:
        if tag in ("script", "style", "noscript"):
            if self._suppress_depth > 0:
                self._suppress_depth -= 1
        elif tag == "p" and self._in_p:
            text = " ".join("".join(self._p_buf).split()).strip()
            if len(text) >= MIN_DESC_LEN:
                self.first_p = text
            self._in_p = False
            self._p_buf = []

    def handle_data(self, data: str) -> None:
        if self._in_p and self._suppress_depth == 0:
            self._p_buf.append(data)

    def done(self) -> bool:
        return bool(self.og and self.meta and self.tw and self.first_p)


def _clean(text: str) -> str:
    text = html.unescape(text)
    # collapse whitespace
    text = re.sub(r"\s+", " ", text).strip()
    if len(text) > MAX_DESC_LEN:
        # cap at MAX_DESC_LEN, try not to mid-word
        cut = text[:MAX_DESC_LEN]
        sp = cut.rfind(" ")
        if sp >= MAX_DESC_LEN - 40:
            cut = cut[:sp]
        text = cut.rstrip(",;:.- ") + "..."
    return text


def extract_description(html_bytes: bytes) -> str | None:
    """Run the prioritized extraction over a chunk of HTML bytes."""
    parser = _MetaAndPParser()
    try:
        # Decode best-effort
        try:
            text = html_bytes.decode("utf-8", errors="replace")
        except Exception:
            text = html_bytes.decode("latin-1", errors="replace")
        parser.feed(text)
        parser.close()
    except Exception:
        # Even partial parse is fine — fall through and use what we have.
        pass

    for cand in (parser.og, parser.meta, parser.tw, parser.first_p):
        if not cand:
            continue
        cleaned = _clean(cand)
        if len(cleaned) >= MIN_DESC_LEN:
            return cleaned
    return None


# ---------- URL filtering ----------------------------------------------------

def url_is_safe(url: str) -> tuple[bool, str]:
    """Return (ok, reason). Reject localhost, unusual TLDs, unsupported schemes."""
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


# ---------- Fetching ---------------------------------------------------------

def fetch(session: requests.Session, url: str) -> tuple[bytes | None, str]:
    """Fetch up to MAX_BYTES of the URL with retries. Return (body, status).

    `status` is "ok", "404", "403", "timeout", "error:<short>", "skip:<tld>".
    """
    if "://" not in url:
        url = "http://" + url
    last_err: str = "error:unknown"
    for attempt in range(MAX_RETRIES):
        try:
            with session.get(
                url,
                timeout=REQUEST_TIMEOUT,
                allow_redirects=True,
                stream=True,
            ) as r:
                # urllib3 default redirect chain is 30; cap explicitly via session below.
                if r.status_code in (404, 403, 410):
                    return None, str(r.status_code)
                if r.status_code >= 400:
                    last_err = f"http:{r.status_code}"
                    # 5xx → retry; 4xx (other) → don't retry
                    if r.status_code < 500:
                        return None, last_err
                    time.sleep(0.5 * (attempt + 1))
                    continue
                ctype = (r.headers.get("Content-Type") or "").lower()
                if "html" not in ctype and "xml" not in ctype and ctype:
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
        except requests.exceptions.ConnectionError as e:
            last_err = "conn_error"
        except Exception as e:
            last_err = f"error:{type(e).__name__}"
        time.sleep(0.5 * (attempt + 1))
    return None, last_err


def make_session() -> requests.Session:
    s = requests.Session()
    s.headers["User-Agent"] = USER_AGENT
    s.headers["Accept"] = "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"
    s.headers["Accept-Language"] = "en-US,en;q=0.9"
    s.max_redirects = MAX_REDIRECTS
    return s


# ---------- Worker -----------------------------------------------------------

def process_one(listing: dict) -> tuple[str, str | None, str]:
    """Return (id, description_or_None, status)."""
    lid = listing["id"]
    url = (listing.get("website") or "").strip()
    ok, reason = url_is_safe(url)
    if not ok:
        return lid, None, f"skip:{reason}"
    # polite per-thread jitter
    time.sleep(random.uniform(JITTER_MIN, JITTER_MAX))
    session = make_session()
    body, status = fetch(session, url)
    if status != "ok" or body is None:
        return lid, None, status
    desc = extract_description(body)
    if not desc:
        return lid, None, "no_description"
    return lid, desc, "ok"


# ---------- Driver -----------------------------------------------------------

def main() -> None:
    listings = json.loads(LISTINGS_PATH.read_text())
    targets = [l for l in listings if (l.get("website") or "").strip()]

    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)

    results: dict[str, dict] = {}
    status_counts: dict[str, int] = {}
    deadline = time.monotonic() + RUNTIME_CAP_S

    print(f"[descriptions] {len(targets)} listings have a website")
    print(f"[descriptions] runtime cap: {RUNTIME_CAP_S // 60} min, workers: {MAX_WORKERS}")

    timed_out = False
    completed = 0
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = {ex.submit(process_one, l): l["id"] for l in targets}
        try:
            for fut in as_completed(futures):
                if time.monotonic() >= deadline:
                    timed_out = True
                    break
                try:
                    lid, desc, status = fut.result()
                except Exception as e:
                    lid = futures[fut]
                    desc, status = None, f"error:{type(e).__name__}"
                status_counts[status] = status_counts.get(status, 0) + 1
                if desc:
                    results[lid] = {"description": desc}
                completed += 1
                if completed % 25 == 0 or completed == len(targets):
                    print(
                        f"[descriptions] {completed}/{len(targets)} "
                        f"got={len(results)} elapsed={int(time.monotonic() - (deadline - RUNTIME_CAP_S))}s"
                    )
        finally:
            # Don't wait for stragglers if we timed out — write what we have.
            if timed_out:
                print("[descriptions] runtime cap hit — cancelling pending work")
                for f in futures:
                    if not f.done():
                        f.cancel()

    OUT_PATH.write_text(json.dumps(results, indent=2, sort_keys=True))

    fetched_ok = status_counts.get("ok", 0)
    extracted = len(results)
    skipped = sum(c for s, c in status_counts.items() if s != "ok")

    print()
    print(f"wrote {OUT_PATH} ({extracted} descriptions)")
    print()
    print(f"listings with website : {len(targets)}")
    print(f"fetched OK            : {fetched_ok}")
    print(f"descriptions extracted: {extracted}")
    print(f"skipped (any reason)  : {skipped}")
    if timed_out:
        print("note: runtime cap hit before all targets were processed")
    print()
    print("status breakdown:")
    for status, count in sorted(status_counts.items(), key=lambda kv: -kv[1]):
        print(f"  {status:<24} {count}")


if __name__ == "__main__":
    main()
