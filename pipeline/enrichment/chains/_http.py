"""HTTP helpers: polite fetch with retries."""
from __future__ import annotations

import socket
import time
import requests
from urllib3.util.connection import allowed_gai_family, HAS_IPV6
import urllib3.util.connection as urllib3_cn

from .common import USER_AGENT

# Global default socket timeout as a backstop for hung TCP connections.
socket.setdefaulttimeout(12)

# Force IPv4 to avoid hung IPv6 routes on some networks.
def _allowed_gai_family_ipv4() -> int:
    return socket.AF_INET

urllib3_cn.allowed_gai_family = _allowed_gai_family_ipv4


def get(url: str, timeout: int = 8, retries: int = 1) -> str | None:
    last_err = None
    headers = {
        "User-Agent": USER_AGENT,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
        "Accept-Encoding": "gzip, deflate",
    }
    # requests takes (connect, read) tuple — cap both low
    rtimeout = (min(timeout, 6), timeout)
    for attempt in range(retries + 1):
        try:
            r = requests.get(url, headers=headers, timeout=rtimeout, allow_redirects=True)
            if r.status_code == 200:
                return r.text
            last_err = f"HTTP {r.status_code}"
        except Exception as e:
            last_err = str(e)
        if attempt < retries:
            time.sleep(0.4)
    print(f"[http] giveup {url}: {last_err}", flush=True)
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
