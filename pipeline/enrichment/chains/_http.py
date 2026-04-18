"""HTTP helpers: polite fetch with retries."""
from __future__ import annotations

import time
import requests

from .common import USER_AGENT


def get(url: str, timeout: int = 15, retries: int = 2) -> str | None:
    last_err = None
    headers = {
        "User-Agent": USER_AGENT,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
        "Accept-Encoding": "gzip, deflate",
    }
    for attempt in range(retries + 1):
        try:
            r = requests.get(url, headers=headers, timeout=timeout, allow_redirects=True)
            if r.status_code == 200:
                return r.text
            last_err = f"HTTP {r.status_code}"
        except Exception as e:
            last_err = str(e)
        if attempt < retries:
            time.sleep(0.4)
    print(f"[http] giveup {url}: {last_err}")
    return None


def get_json(url: str, timeout: int = 15, retries: int = 2):
    txt = get(url, timeout=timeout, retries=retries)
    if not txt:
        return None
    try:
        import json
        return json.loads(txt)
    except Exception:
        return None
