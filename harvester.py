#!/usr/bin/env python3
"""
================================================================================
        RSS INTELLIGENCE HARVESTER (ALL-IN-ONE, LLM-PROCESSED, HARDENED)
================================================================================
1. Fetches all RSS feeds directly (FT, WSJ, NYT, Bloomberg, SCMP, WaPo).
2. Parses XML (RSS 2.0 + Atom), deduplicates, filters to today + yesterday (UTC).
3. Caps prompt size, sends dump + analyst instructions to NVIDIA Nemotron
   (Ultra primary, fallback via env) with output VALIDATION GATE.
4. Writes only validated briefs to disk. Raw API responses archived to
   ./llm_debug/ for forensics.
  No intermediate JSON file required — feed fetching is built in.

  ==============================================================================
  FETCH-STRATEGY NOTES (FT HTTP 429 remediation)
  ==============================================================================
  R1. Polite pause always runs, including on the failure path. Previously the
      sleep sat after a `continue`, so a failed fetch skipped it entirely and
      the script fired ~20 requests in 3.2s once FT started refusing.
  R2. Per-host minimum spacing. FT is paced deliberately slowly (8s base);
      other publishers use a light 0.35s courtesy gap.
  R3. Adaptive backoff. On a 429 a host's spacing triples (capped) and decays
      back toward its base after successes. Recovers on its own.
  R4. Circuit breaker is HALF-OPEN, not permanent. After N consecutive 429s a
      host is skipped only for COOLDOWN seconds, then probed once. If the host
      recovers, harvesting resumes. Strike state is per-host, so a throttle at
      one publisher can never open another publisher's breaker.
  R5. ETag / Last-Modified conditional GETs + TTL disk cache.
  R6. Honest, contactable User-Agent.
  R7. FT feed tiering: must-have feeds always fetched; the rest only if
      must-have coverage falls short of a threshold.

  Verified NOT used: ft-rss.herokuapp.com (Heroku free-tier shutdown, 502) and
  FiveFilters Full-Text RSS (free hosted service discontinued by its operator).
================================================================================
"""
import os
import sys
import re
import json
import gzip
import time
import random
import logging
import datetime
import hashlib
import warnings
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Optional
from dataclasses import dataclass
import urllib.request
import urllib.parse
import urllib.error
from urllib.error import HTTPError
try:
    from dotenv import load_dotenv
except ImportError:
    print("Error: 'python-dotenv' required. Run: pip install python-dotenv")
    sys.exit(1)
warnings.filterwarnings("ignore")
# ==============================================================================
# ENVIRONMENT & CONFIGURATION
# ==============================================================================
# Local: loads from .env if present. CI: values come from GitHub Secrets (env vars).
loaded = load_dotenv()  # picks up .env in cwd if it exists
print(f"load_dotenv returned: {loaded}")
for var in ["NVIDIA_API_KEY", "NEMOTRON_ULTRA_MODEL",
            "NEMOTRON_LIGHTNING_MODEL", "NIM_POWERFUL_MODEL"]:
    v = os.getenv(var)
    print(f"{var}: {'SET (len=%d)' % len(v) if v else 'MISSING'}")
# --- Output config ---
OUTPUT_FILENAME = "news_intelligence_brief.txt"
RAW_DUMP_FILENAME = "news_context_raw_dump.txt"
KEEP_RAW_DUMP = True
DEBUG_DIR = Path("llm_debug")
# --- NVIDIA API CONFIG (override via .env) ---
NVIDIA_API_BASE = "https://integrate.api.nvidia.com/v1"
NVIDIA_API_KEY = os.getenv("NVIDIA_API_KEY")
NEMOTRON_ULTRA_MODEL = os.getenv("NEMOTRON_ULTRA_MODEL")
NEMOTRON_LIGHTNING_MODEL = os.getenv("NEMOTRON_LIGHTNING_MODEL")
NIM_POWERFUL_MODEL = os.getenv("NIM_POWERFUL_MODEL")
MODEL_CHAIN = [
    m for m in [
        {"model": NIM_POWERFUL_MODEL, "base_url": NVIDIA_API_BASE, "api_key": NVIDIA_API_KEY},
        {"model": NEMOTRON_ULTRA_MODEL, "base_url": NVIDIA_API_BASE, "api_key": NVIDIA_API_KEY},
        {"model": NEMOTRON_LIGHTNING_MODEL, "base_url": NVIDIA_API_BASE, "api_key": NVIDIA_API_KEY},
    ]
    if m["model"] and m["api_key"]
]
if not MODEL_CHAIN:
    logging.error("No valid models configured. .env loaded? Vars present?")
    sys.exit(1)
ATTEMPTS_PER_MODEL = 2
MAX_OUTPUT_TOKENS = 8000
LLM_TEMPERATURE = 0.1
LLM_RATE_LIMIT_BACKOFF = 30
LLM_REQUEST_TIMEOUT = 300
# --- INPUT SIZE GUARDS ---
MAX_CHARS_PER_ITEM = 500
INPUT_CHAR_BUDGET = 100_000
# ==============================================================================
# FETCH PACING CONFIG  (R2 / R3 / R4)
# ==============================================================================
# This job runs once per day, so pacing is deliberately conservative and the
# settings are plain constants rather than .env knobs. Edit them in-code.
FEED_TIMEOUT = 15
# Minimum seconds between two requests to the SAME host, keyed by hostname
# suffix. Anything not listed falls through to DEFAULT_HOST_DELAY.
#
# FT is the only host observed to throttle aggressively (26/26 failures while
# every other publisher succeeded), so it gets its own slow pace. 8s base means
# the 12 must-have FT feeds take roughly two minutes — trivial for a daily run
# and comfortably under any sane per-minute allowance.
FEED_HOST_DELAY = {
    "ft.com": 8.0,
}
DEFAULT_HOST_DELAY = 0.35
# Jitter ceiling per host, so the request pattern is not metronomic.
FEED_HOST_JITTER = {
    "ft.com": 1.5,
}
DEFAULT_HOST_JITTER = 0.35
# Short unconditional courtesy pause after every attempt, success or failure.
# _throttle() is the real limiter; this is just belt-and-braces politeness.
FEED_COURTESY_SLEEP = (0.05, 0.25)
# Ceiling for adaptive backoff (R3). FT spacing triples per 429 up to here.
MAX_HOST_DELAY = 45.0
BACKOFF_FACTOR = 3.0
BACKOFF_DECAY = 0.80          # shrink toward base on each success
MIN_BACKOFF_WAIT = 15         # floor when honouring a 429
MAX_BACKOFF_WAIT = 60
# Circuit breaker (R4). Consecutive 429s before a host goes quiet; it is NOT
# skipped for the rest of the run, only for COOLDOWN seconds, after which one
# probe is allowed. A throttled host that recovers will rejoin the harvest.
BREAKER_429_THRESHOLD = 3
BREAKER_COOLDOWN = 120
# Conditional-GET / TTL cache (R5). cwd-relative, matching this script's other
# output paths.
FEED_CACHE_DIR = Path("feed_cache")
FEED_CACHE_TTL_SECONDS = 20 * 60
# Tiering (R7): only pull nice-to-have feeds if must-have coverage is thin.
TIER2_MIN_ARTICLES = 150
# Contact string for the User-Agent. A real address can help you get treated
# more leniently; leave the placeholder if you don't want to publish one.
FEED_CONTACT = "fake_email@proton.me"
# ==============================================================================
# THINKING MODE
# ==============================================================================
NO_THINK_KWARG = True
# ==============================================================================
# OUTPUT VALIDATION GATE
# ==============================================================================
MIN_BRIEF_CHARS = 600
MIN_WORD_RATIO = 0.60
REQUIRED_HEADERS = ["NEWS INTELLIGENCE BRIEF"]
# ==============================================================================
# FEED CONFIGURATION — organized by publication
# ==============================================================================
FEEDS = {
    # ─── Financial Times ───
    "FT: Home (International)": "https://www.ft.com/rss/home",
    "FT: Markets": "https://www.ft.com/markets?format=rss",
    "FT: Global Economy": "https://www.ft.com/global-economy?format=rss",
    "FT: Emerging Markets": "https://www.ft.com/emerging-markets?format=rss",
    "FT: World": "https://www.ft.com/world?format=rss",
    "FT: US": "https://www.ft.com/us?format=rss",
    "FT: Companies": "https://www.ft.com/companies?format=rss",
    "FT: Technology": "https://www.ft.com/technology?format=rss",
    "FT: China": "https://www.ft.com/china?format=rss",
    "FT: Europe": "https://www.ft.com/world/europe?format=rss",
    "FT: Americas": "https://www.ft.com/world/americas?format=rss",
    "FT: Asia-Pacific": "https://www.ft.com/world/asia-pacific?format=rss",
    # --- FT nice-to-have: fetched only if must-have coverage is thin (R7) ---
    "FT: Opinion": "https://www.ft.com/opinion?format=rss",
    "FT: Energy": "https://www.ft.com/energy?format=rss",
    "FT: Climate Capital": "https://www.ft.com/climate-capital?format=rss",
    "FT: Work & Careers": "https://www.ft.com/work-careers?format=rss",
    "FT: Financials": "https://www.ft.com/financials?format=rss",
    "FT: Health": "https://www.ft.com/health?format=rss",
    "FT: Industrials": "https://www.ft.com/industrials?format=rss",
    "FT: Media": "https://www.ft.com/media?format=rss",
    "FT: Professional Services": "https://www.ft.com/professional-services?format=rss",
    "FT: Retail & Consumer": "https://www.ft.com/retail-consumer?format=rss",
    "FT: Telecoms": "https://www.ft.com/telecoms?format=rss",
    "FT: Africa": "https://www.ft.com/africa?format=rss",
    "FT: Middle East & North Africa": "https://www.ft.com/world/middle-east-north-africa?format=rss",
    # ─── Wall Street Journal ───
    "WSJ: World News": "https://feeds.a.dj.com/rss/RSSWorldNews.xml",
    "WSJ: Markets": "https://feeds.a.dj.com/rss/RSSMarketsMain.xml",
    "WSJ: US Business": "https://feeds.a.dj.com/rss/WSJcomUSBusiness.xml",
    # ─── The New York Times ───
    "NYT: Home Page": "https://rss.nytimes.com/services/xml/rss/nyt/HomePage.xml",
    "NYT: World": "https://rss.nytimes.com/services/xml/rss/nyt/World.xml",
    "NYT: US": "https://rss.nytimes.com/services/xml/rss/nyt/US.xml",
    "NYT: Politics": "https://rss.nytimes.com/services/xml/rss/nyt/Politics.xml",
    "NYT: Business": "https://rss.nytimes.com/services/xml/rss/nyt/Business.xml",
    "NYT: Technology": "https://rss.nytimes.com/services/xml/rss/nyt/Technology.xml",
    "NYT: Science": "https://rss.nytimes.com/services/xml/rss/nyt/Science.xml",
    "NYT: Health": "https://rss.nytimes.com/services/xml/rss/nyt/Health.xml",
    "NYT: Sports": "https://rss.nytimes.com/services/xml/rss/nyt/Sports.xml",
    "NYT: Arts": "https://rss.nytimes.com/services/xml/rss/nyt/Arts.xml",
    "NYT: Books": "https://rss.nytimes.com/services/xml/rss/nyt/Books.xml",
    "NYT: Movies": "https://rss.nytimes.com/services/xml/rss/nyt/Movies.xml",
    "NYT: Music": "https://rss.nytimes.com/services/xml/rss/nyt/Music.xml",
    "NYT: Television": "https://rss.nytimes.com/services/xml/rss/nyt/Television.xml",
    "NYT: Theater": "https://rss.nytimes.com/services/xml/rss/nyt/Theater.xml",
    "NYT: Fashion & Style": "https://rss.nytimes.com/services/xml/rss/nyt/FashionandStyle.xml",
    "NYT: Dining & Wine": "https://rss.nytimes.com/services/xml/rss/nyt/DiningandWine.xml",
    "NYT: Travel": "https://rss.nytimes.com/services/xml/rss/nyt/Travel.xml",
    "NYT: Real Estate": "https://rss.nytimes.com/services/xml/rss/nyt/RealEstate.xml",
    "NYT: Automobiles": "https://rss.nytimes.com/services/xml/rss/nyt/Automobiles.xml",
    "NYT: Job Market": "https://rss.nytimes.com/services/xml/rss/nyt/JobMarket.xml",
    "NYT: Education": "https://rss.nytimes.com/services/xml/rss/nyt/Education.xml",
    "NYT: Magazine": "https://rss.nytimes.com/services/xml/rss/nyt/Magazine.xml",
    "NYT: Opinion": "https://rss.nytimes.com/services/xml/rss/nyt/Opinion.xml",
    "NYT: Obituaries": "https://rss.nytimes.com/services/xml/rss/nyt/Obituaries.xml",
    "NYT: Corrections": "https://rss.nytimes.com/services/xml/rss/nyt/Corrections.xml",
    "NYT: Your Money": "https://rss.nytimes.com/services/xml/rss/nyt/YourMoney.xml",
    # ─── Bloomberg ───
    "Bloomberg: Markets": "https://feeds.bloomberg.com/markets/news.rss",
    "Bloomberg: Politics": "https://feeds.bloomberg.com/politics/news.rss",
    "Bloomberg: Business": "https://feeds.bloomberg.com/business/news.rss",
    "Bloomberg: Technology": "https://feeds.bloomberg.com/technology/news.rss",
    "Bloomberg: Wealth": "https://feeds.bloomberg.com/wealth/news.rss",
    "Bloomberg: Economics": "https://feeds.bloomberg.com/economics/news.rss",
    "Bloomberg: Industries": "https://feeds.bloomberg.com/industries/news.rss",
    "Bloomberg: Green": "https://feeds.bloomberg.com/green/news.rss",
    # ─── South China Morning Post ───
    "SCMP: World": "https://www.scmp.com/rss/5/feed",
    "SCMP: This Week in Asia": "https://www.scmp.com/rss/3/feed",
    "SCMP: Hong Kong": "https://www.scmp.com/rss/2/feed",
    "SCMP: China": "https://www.scmp.com/rss/4/feed",
    "SCMP: Property": "https://www.scmp.com/rss/96/feed",
    "SCMP: Sport": "https://www.scmp.com/rss/95/feed",
    "SCMP: People & Culture": "https://www.scmp.com/rss/318202/feed",
    "SCMP: Lifestyle": "https://www.scmp.com/rss/94/feed",
    "SCMP: Tech": "https://www.scmp.com/rss/36/feed",
    "SCMP: Business": "https://www.scmp.com/rss/92/feed",
    # ─── Washington Post ───
    "WaPo: World": "https://feeds.washingtonpost.com/rss/world",
    "WaPo: National": "https://feeds.washingtonpost.com/rss/national",
    "WaPo: Business": "https://feeds.washingtonpost.com/rss/business",
    "WaPo: Technology": "https://feeds.washingtonpost.com/rss/technology",
    "WaPo: Politics": "https://feeds.washingtonpost.com/rss/politics",
}
# ─── Must-have feeds (always fetched) ───
# Your 12 FT sections, plus every non-FT publisher. Only FT is tiered.
MUST_HAVE_FEEDS = {
    "FT: Home (International)", "FT: Markets", "FT: Global Economy",
    "FT: Emerging Markets", "FT: World", "FT: US", "FT: Companies",
    "FT: Technology", "FT: China", "FT: Europe", "FT: Americas",
    "FT: Asia-Pacific",
}
MUST_HAVE_FEEDS.update({k for k in FEEDS if not k.startswith("FT: ")})


def is_must_have(feed_name: str) -> bool:
    return feed_name in MUST_HAVE_FEEDS


# ==============================================================================
# LOGGING
# ==============================================================================
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] - %(message)s')
logger = logging.getLogger("RSSIntelHarvester")
# ==============================================================================
# HOST STATE  (per-host pacing, backoff, breaker)
# ==============================================================================
@dataclass
class HostState:
    """Per-host request budget. One of these per hostname, never global."""
    host: str
    base_delay: float
    jitter: float
    delay: float = 0.0
    strikes: int = 0              # consecutive 429s
    successes: int = 0
    last_request: float = 0.0     # monotonic timestamp
    tripped_at: Optional[float] = None   # monotonic; None = breaker closed

    def __post_init__(self):
        self.delay = self.base_delay

    @property
    def cooling(self) -> bool:
        return self.tripped_at is not None

    def cooldown_remaining(self) -> int:
        if self.tripped_at is None:
            return 0
        return max(0, int(BREAKER_COOLDOWN - (time.monotonic() - self.tripped_at)))

    def note_success(self) -> None:
        self.strikes = 0
        self.successes += 1
        self.tripped_at = None
        if self.delay > self.base_delay:
            self.delay = max(self.base_delay, self.delay * BACKOFF_DECAY)

    def note_throttled(self) -> None:
        self.strikes += 1
        self.delay = min(MAX_HOST_DELAY, max(self.delay * BACKOFF_FACTOR,
                                             self.base_delay * BACKOFF_FACTOR))
        if self.strikes >= BREAKER_429_THRESHOLD:
            self.tripped_at = time.monotonic()

    def note_failure(self) -> None:
        """A non-429 failure does not penalize the host's pacing."""
        pass

    def reset_for_run(self) -> None:
        self.strikes = 0
        self.tripped_at = None
        self.delay = self.base_delay


_HOSTS: dict = {}


def host_of(url: str) -> str:
    try:
        return (urllib.parse.urlparse(url).netloc or "").lower()
    except Exception:
        return ""


def _lookup_suffix(host: str, table: dict) -> Optional[float]:
    for suffix, val in table.items():
        if host == suffix or host.endswith("." + suffix):
            return val
    return None


def get_host_state(url: str) -> HostState:
    """Fetch (or lazily create) the HostState for this URL's host."""
    host = host_of(url)
    if host not in _HOSTS:
        base = _lookup_suffix(host, FEED_HOST_DELAY)
        jit = _lookup_suffix(host, FEED_HOST_JITTER)
        _HOSTS[host] = HostState(
            host=host,
            base_delay=DEFAULT_HOST_DELAY if base is None else base,
            jitter=DEFAULT_HOST_JITTER if jit is None else jit,
        )
    return _HOSTS[host]


def reset_host_states() -> None:
    for st in _HOSTS.values():
        st.reset_for_run()


def _throttle(st: HostState) -> None:
    """Block until this host's current inter-request gap has elapsed."""
    gap = st.delay + random.uniform(0, st.jitter)
    if st.last_request:
        wait = gap - (time.monotonic() - st.last_request)
        if wait > 0:
            time.sleep(wait)
    st.last_request = time.monotonic()


# ==============================================================================
# HTTP
# ==============================================================================
# R6: identify honestly. The spoofed Firefox string was inaccurate and, on
# some hosts, itself a reason to apply stricter anonymous-client limits.
HEADERS = {
    "User-Agent": f"RSSIntelHarvester/2.0 (+{FEED_CONTACT})",
    "Accept": "application/rss+xml, application/atom+xml, application/xml;q=0.9, text/xml;q=0.9, */*;q=0.8",
    "Accept-Encoding": "gzip, deflate",
    "Connection": "close",
}


@dataclass
class FetchResult:
    """Fetch outcome carrying status + headers so 429/Retry-After is visible."""
    status: int
    body: bytes = b""
    etag: str = ""
    last_modified: str = ""
    from_cache: bool = False
    error: str = ""
    retry_after: int = 0

    @property
    def ok(self) -> bool:
        return self.status == 200 and bool(self.body)


def _cache_paths(url: str) -> tuple:
    key = hashlib.sha256(url.encode("utf-8")).hexdigest()[:24]
    return (FEED_CACHE_DIR / f"{key}.xml", FEED_CACHE_DIR / f"{key}.meta.json")


def _cache_read(url: str) -> tuple:
    """Return (body, etag, last_modified) if a fresh-enough entry exists."""
    body_p, meta_p = _cache_paths(url)
    try:
        if not body_p.exists() or not meta_p.exists():
            return None, "", ""
        meta = json.loads(meta_p.read_text(encoding="utf-8"))
        if time.time() - meta.get("ts", 0) > FEED_CACHE_TTL_SECONDS:
            return None, "", ""
        return body_p.read_bytes(), meta.get("etag", ""), meta.get("last_modified", "")
    except Exception:
        return None, "", ""


def _cache_write(url: str, body: bytes, etag: str, last_modified: str) -> None:
    body_p, meta_p = _cache_paths(url)
    try:
        FEED_CACHE_DIR.mkdir(parents=True, exist_ok=True)
        body_p.write_bytes(body)
        meta_p.write_text(json.dumps({
            "ts": time.time(), "url": url,
            "etag": etag, "last_modified": last_modified,
        }), encoding="utf-8")
    except Exception as e:
        logger.debug(f"Cache write failed: {e}")


def http_get(url: str, timeout=30) -> FetchResult:
    """GET with gzip handling, conditional revalidation, real status codes."""
    cached_body, cached_etag, cached_lm = _cache_read(url)

    # Fresh cache hit inside TTL with nothing to revalidate: skip the network.
    if cached_body is not None and not (cached_etag or cached_lm):
        return FetchResult(200, cached_body, cached_etag, cached_lm, from_cache=True)

    headers = dict(HEADERS)
    if cached_etag:
        headers["If-None-Match"] = cached_etag
    if cached_lm:
        headers["If-Modified-Since"] = cached_lm

    try:
        req = urllib.request.Request(url, headers=headers)
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            data = resp.read()
            if resp.info().get('Content-Encoding') == 'gzip' or data.startswith(b'\x1f\x8b'):
                data = gzip.decompress(data)
            etag = resp.headers.get("ETag", "") or ""
            last_mod = resp.headers.get("Last-Modified", "") or ""
            _cache_write(url, data, etag, last_mod)
            return FetchResult(200, data, etag, last_mod)
    except HTTPError as e:
        # 304: our cached copy is still authoritative. Refresh TTL so the next
        # run short-circuits without touching the network.
        if e.code == 304 and cached_body is not None:
            _cache_write(url, cached_body, cached_etag, cached_lm)
            return FetchResult(200, cached_body, cached_etag, cached_lm, from_cache=True)
        retry_after = 0
        try:
            hdr = e.headers.get("Retry-After", "") if e.headers else ""
            if hdr and hdr.strip().isdigit():
                retry_after = int(hdr.strip())
        except Exception:
            retry_after = 0
        return FetchResult(e.code, b"", error=f"HTTP {e.code} {e.reason}",
                           retry_after=retry_after)
    except Exception as e:
        return FetchResult(0, b"", error=str(e))


def fetch_feed(url: str) -> FetchResult:
    """Fetch one feed with host-aware pacing and a single 429 retry."""
    st = get_host_state(url)

    _throttle(st)
    res = http_get(url, timeout=FEED_TIMEOUT)
    st.last_request = time.monotonic()

    if res.ok:
        st.note_success()
        return res

    if res.status != 429:
        st.note_failure()
        return res

    # --- 429: back off, retry this feed exactly once ---
    st.note_throttled()
    wait = res.retry_after or int(min(MAX_BACKOFF_WAIT,
                                      max(MIN_BACKOFF_WAIT, st.delay)))
    wait = min(MAX_BACKOFF_WAIT, wait)
    logger.warning(f"[429] {st.host}: throttled (strike {st.strikes}, "
                   f"next spacing ~{st.delay:.1f}s). Honouring Retry-After="
                   f"{res.retry_after or wait}s.")
    time.sleep(wait)

    _throttle(st)
    res = http_get(url, timeout=FEED_TIMEOUT)
    st.last_request = time.monotonic()

    if res.ok:
        st.note_success()
    elif res.status == 429:
        st.note_throttled()
        logger.warning(f"[429] {st.host}: still throttled after {wait}s wait.")
    else:
        st.note_failure()
    return res


# ==============================================================================
# DATE PARSING
# ==============================================================================
def parse_any_date(date_str: str) -> datetime.datetime:
    """Robustly parse ISO 8601 / RFC 822 / dateutil fallback -> UTC."""
    s = (date_str or "").strip()
    try:
        return datetime.datetime.fromisoformat(s)
    except ValueError:
        pass
    for fmt in ("%a, %d %b %Y %H:%M:%S %Z",
                "%a, %d %b %Y %H:%M:%S %z"):
        try:
            dt = datetime.datetime.strptime(s, fmt)
            return dt.replace(tzinfo=datetime.timezone.utc) if dt.tzinfo is None else dt.astimezone(datetime.timezone.utc)
        except ValueError:
            continue
    try:
        from dateutil import parser
        return parser.parse(s).astimezone(datetime.timezone.utc)
    except Exception:
        return datetime.datetime.now(datetime.timezone.utc)
# ==============================================================================
# RSS / ATOM PARSING
# ==============================================================================
NS = {
    "media": "http://search.yahoo.com/mrss/",
    "dc": "http://purl.org/dc/elements/1.1/",
    "content": "http://purl.org/rss/1.0/modules/content/",
    "atom": "http://www.w3.org/2005/Atom",
}
@dataclass
class Article:
    guid: str
    title: str
    link: str
    pub_date: datetime.datetime
    summary: str
    source_feed: str
    publication: str
def _text(elem: Optional[ET.Element], tag: str, ns: dict = None) -> str:
    if elem is None:
        return ""
    child = elem.find(tag, ns)
    if child is None:
        return ""
    return (child.text or "").strip()
def parse_feed(xml_content: bytes, feed_name: str) -> list[Article]:
    articles = []
    if not xml_content:
        return articles
    try:
        root = ET.fromstring(xml_content)
    except ET.ParseError as e:
        logger.warning(f"  ✗ XML parse error in {feed_name}: {e}")
        return articles
    publication = feed_name.split(":")[0].strip() if ":" in feed_name else "Unknown"
    channel = root.find("channel")
    if channel is not None:
        # RSS 2.0
        for item in channel.findall("item"):
            guid_el = item.find("guid")
            guid = (guid_el.text or "").strip() if guid_el is not None else ""
            link = _text(item, "link")
            if not guid:
                guid = link
            title = _text(item, "title")
            pub_raw = _text(item, "pubDate")
            pub_date = parse_any_date(pub_raw) if pub_raw else datetime.datetime.now(datetime.timezone.utc)
            summary = _text(item, "description")
            ce = item.find("content:encoded", NS)
            if ce is not None and ce.text:
                summary = ce.text.strip()
            if title:
                articles.append(Article(guid, title, link, pub_date, summary, feed_name, publication))
    else:
        # Atom
        for entry in root.findall("{http://www.w3.org/2005/Atom}entry"):
            guid = _text(entry, "atom:id", NS) or ""
            title = _text(entry, "atom:title", NS)
            link_el = entry.find("atom:link", NS)
            link = link_el.get("href") if link_el is not None else ""
            pub_raw = _text(entry, "atom:published", NS) or _text(entry, "atom:updated", NS)
            pub_date = parse_any_date(pub_raw) if pub_raw else datetime.datetime.now(datetime.timezone.utc)
            summary = _text(entry, "atom:summary", NS) or _text(entry, "atom:content", NS)
            if title and (guid or link):
                articles.append(Article(guid or link, title, link, pub_date, summary, feed_name, publication))
    return articles
# ==============================================================================
# HARVEST
# ==============================================================================
def _absorb(feed_name: str, content: bytes, all_articles: dict, allowed: set) -> int:
    articles = parse_feed(content, feed_name)
    kept = 0
    for art in articles:
        if art.pub_date.date() not in allowed:
            continue
        key = art.guid or art.link
        if key not in all_articles:
            all_articles[key] = art
            kept += 1
    print(f"    ✓ {feed_name}: {len(articles)} fetched, {kept} in window")
    return len(articles)


def harvest_feeds() -> list[Article]:
    """Fetch feeds in tiers, dedupe by GUID, filter to today + yesterday (UTC)."""
    reset_host_states()

    now_utc = datetime.datetime.now(datetime.timezone.utc)
    today = now_utc.date()
    yesterday = (now_utc - datetime.timedelta(days=1)).date()
    allowed = {today, yesterday}

    all_articles: dict = {}
    pubs: dict = {}
    for name, url in FEEDS.items():
        pubs.setdefault(name.split(":")[0], []).append((name, url))

    def sweep(select, label):
        total_raw = 0
        for pub, entries in pubs.items():
            todo = [(n, u) for n, u in entries if select(n)]
            if not todo:
                continue
            print(f"  ── {pub} ({len(todo)} feeds{label}) ──")
            for feed_name, url in todo:
                st = get_host_state(url)

                # --- half-open breaker: skip only while cooling down ---
                if st.cooling:
                    left = st.cooldown_remaining()
                    if left > 0:
                        print(f"    ⏸ {feed_name}: {st.host} in backoff "
                              f"({left}s remaining, resumes after cooldown)")
                        continue
                    logger.info(f"[breaker] {st.host}: cooldown elapsed, probing again.")
                    st.tripped_at = None

                try:
                    res = fetch_feed(url)

                    if res.ok:
                        total_raw += _absorb(feed_name, res.body, all_articles, allowed)
                        if res.from_cache:
                            print("       ↺ served from cache (no network request)")
                        if st.delay > st.base_delay * 1.05:
                            print(f"       ⇅ {st.host} spacing relaxing toward "
                                  f"{st.base_delay:.1f}s")
                    elif res.status == 429:
                        if st.cooling:
                            print(f"    ✗ {feed_name}: 429 — {st.host} cooling "
                                  f"down {st.cooldown_remaining()}s")
                        else:
                            print(f"    ✗ {feed_name}: 429 throttled")
                    else:
                        print(f"    ✗ {feed_name}: fetch failed "
                              f"({res.error or res.status})")
                finally:
                    # R1: unconditional courtesy pause, success or failure.
                    time.sleep(random.uniform(*FEED_COURTESY_SLEEP))
        return total_raw

    # ── Pass 1: must-have ──
    total_raw = sweep(is_must_have, "")

    # ── Pass 2: nice-to-have only if coverage is thin (R7) ──
    if len(all_articles) < TIER2_MIN_ARTICLES:
        logger.info(f"Coverage {len(all_articles)} < {TIER2_MIN_ARTICLES}; "
                    f"pulling nice-to-have feeds.")
        total_raw += sweep(lambda n: not is_must_have(n), ", 2nd tier")
    else:
        logger.info(f"Coverage {len(all_articles)} >= {TIER2_MIN_ARTICLES}; "
                    f"skipping nice-to-have feeds.")

    degraded = sorted(h for h, s in _HOSTS.items()
                      if s.strikes > 0 or s.cooling or not s.successes)
    logger.info(f"Harvest: {total_raw} raw items, {len(all_articles)} unique "
                f"in today/yesterday window.")
    logger.info(f"Host pacing report: {len(_HOSTS)} hosts touched.")
    for h in sorted(_HOSTS):
        s = _HOSTS[h]
        if s.strikes or s.cooling or s.delay > s.base_delay * 1.05:
            logger.info(f"  - {h}: {s.successes} ok, {s.strikes} strikes, "
                        f"spacing now {s.delay:.1f}s (base {s.base_delay:.1f}s)"
                        + (f", COOLING {s.cooldown_remaining()}s" if s.cooling else ""))
    if degraded:
        logger.warning(f"Hosts that throttled or failed this run: {degraded}")

    return sorted(all_articles.values(), key=lambda a: a.pub_date, reverse=True)
# ==============================================================================
# SYSTEM PROMPT — NEWS INTELLIGENCE BRIEF
# ==============================================================================
SYSTEM_PROMPT = """
ROLE: Senior Macro Intelligence Analyst.
STYLE: Ultra-concise, dense financial intelligence brief. Zero conversational filler, zero setup sentences, zero meta-commentary.
TASK: Process the provided raw news headlines + summaries from major financial publications (FT, WSJ, NYT, Bloomberg, SCMP, Washington Post) and transform them into a single, highly scannable section.
================================================================================
NEWS INTELLIGENCE BRIEF
================================================================================
HEADER: Must start directly with `### NEWS INTELLIGENCE BRIEF`
DISCLAIMER: Immediately beneath header: `*(All data sourced from public RSS feeds: FT, WSJ, NYT, Bloomberg, SCMP, WaPo)*`
STRUCTURE: Divide into short sub-topical blocks.
SUB-HEADERS: Create your own Level-4 Markdown headings `#### CATEGORY NAME` in ALL CAPS that best fit the actual content. Do NOT use a fixed list. Generate categories dynamically from the feed — e.g., if the data contains central bank moves, create `#### CENTRAL BANKS & RATES`; if it contains semiconductor earnings, create `#### SEMICONDUCTORS & TECH`; if it contains China property, create `#### CHINA PROPERTY & CREDIT`. Every item must appear under some category. No item left uncategorized. Typical macro categories emerge naturally (central banks, bonds, geopolitics, commodities, economic data, corporate earnings, tech/AI, regional), but adapt to what's actually in the window.
BULLETS: Single dash `-`, max 2-3 per sub-header.
CONSTRAINTS:
  1. Every entry = one isolated bullet point. No multi-sentence paragraphs.
  2. Retain every figure, bps, rate, quote, ticker ($AAPL, etc.), percentage.
  3. No source attributions unless citing a specific exclusive/scoop.
  4. DO NOT start bullets with "Per FT", "According to WSJ", "Bloomberg reports...".
  5. Jump straight to the core metric/event/entity (e.g., `- Brent crude topped $91/bbl on Hormuz closure risk...`).
  6. Discard pure noise: generic "markets mixed" wrap-ups, clickbait, non-financial lifestyle fluff.
================================================================================
GENERAL SYNTAX & FORMATTING STRICT RULES
================================================================================
1. First line of output MUST be `### NEWS INTELLIGENCE BRIEF`. No intro.
2. Output ends immediately after last bullet. No summary/conclusion.
3. Minimize passive verbs/filler. Use high-impact verbs (surged, printed, plummeted, eased, beat, missed, guided, launched, acquired).
4. Be economical with words.
"""
# ==============================================================================
# LLM PIPELINE (HARDENED & FIXED)
# ==============================================================================
def _save_debug(tag: str, text: str):
    try:
        DEBUG_DIR.mkdir(parents=True, exist_ok=True)
        ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        safe = re.sub(r'[^A-Za-z0-9_.-]', '_', tag)
        (DEBUG_DIR / f"{ts}__{safe}.txt").write_text(text, encoding="utf-8")
    except Exception as e:
        logger.debug(f"Debug-save failed: {e}")
def _validate_brief(text: str) -> tuple[bool, str]:
    t = (text or "").strip()
    if len(t) < MIN_BRIEF_CHARS:
        return False, f"too short ({len(t)} chars)"
    probe = t[:4000]
    readable = sum(ch.isalnum() or ch.isspace() for ch in probe) / max(len(probe), 1)
    if readable < MIN_WORD_RATIO:
        return False, f"non-textual output ({readable:.0%} readable chars)"
    upper = t.upper()
    for hdr in REQUIRED_HEADERS:
        if hdr not in upper:
            return False, f"missing required header '{hdr}'"
    return True, ""
def _post_chat_completion(model_cfg: dict, user_prompt: str) -> tuple[str, str]:
    base_url = model_cfg["base_url"].rstrip("/")
    api_key  = model_cfg["api_key"]
    model    = model_cfg["model"]
    url      = f"{base_url}/chat/completions"
    def send(payload: dict) -> str:
        body = json.dumps(payload).encode("utf-8")
        hdrs = {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "Authorization": f"Bearer {api_key}",
            "User-Agent": HEADERS["User-Agent"],
        }
        req = urllib.request.Request(url, data=body, headers=hdrs, method="POST")
        with urllib.request.urlopen(req, timeout=LLM_REQUEST_TIMEOUT) as resp:
            return resp.read().decode("utf-8", errors="replace")
    payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user",   "content": user_prompt},
        ],
        "temperature": LLM_TEMPERATURE,
        "top_p": 0.95,
        "max_tokens": MAX_OUTPUT_TOKENS,
        "stream": False,
    }
    if NO_THINK_KWARG and "nemotron" in model.lower() and "nvidia" in base_url:
        payload["chat_template_kwargs"] = {"enable_thinking": False}
    try:
        raw = send(payload)
    except HTTPError as e:
        if NO_THINK_KWARG and e.code in (400, 422) and "chat_template_kwargs" in payload:
            logger.info(f"[LLM] {model}: endpoint rejected chat_template_kwargs; retrying without.")
            payload.pop("chat_template_kwargs", None)
            raw = send(payload)
        else:
            raise
    _save_debug(f"response_{model}_attempt", raw)
    data = json.loads(raw)
    choice = (data.get("choices") or [{}])[0]
    msg = choice.get("message") or {}
    content = msg.get("content")
    if isinstance(content, list):
        content = "".join(p.get("text", "") for p in content if isinstance(p, dict))
    content = (content or "").strip()
    finish = choice.get("finish_reason", "?")
    usage = data.get("usage") or {}
    logger.info(f"[LLM] {model}: finish_reason={finish} | "
                f"tokens {usage.get('prompt_tokens', '?')} in / {usage.get('completion_tokens', '?')} out")
    reasoning = (msg.get("reasoning_content") or "").strip()
    if not content and reasoning:
        logger.warning(f"[LLM] 'content' empty but {len(reasoning)} chars reasoning returned.")
        _save_debug(f"orphan_reasoning_{model}", reasoning)
    return content, finish
def llm_prepare_file(user_prompt: str) -> tuple[str | None, str]:
    valid_chain = [m for m in MODEL_CHAIN if m.get("model") and m.get("base_url") and m.get("api_key")]
    if not valid_chain:
        logger.error("No valid models configured (check .env).")
        return None, ""
    if not NVIDIA_API_KEY and any("nvidia" in m["base_url"] for m in valid_chain):
        logger.error("NVIDIA_API_KEY missing for NVIDIA endpoints.")
        return None, ""
    logger.info(f"[LLM] Payload: {len(user_prompt):,} chars (~{len(user_prompt)//4:,} tokens est.)")
    _save_debug("last_request_user_message", user_prompt)
    for idx, model_cfg in enumerate(valid_chain):
        label = "PRIMARY" if idx == 0 else "FALLBACK"
        model_name = model_cfg["model"]
        for attempt in range(1, ATTEMPTS_PER_MODEL + 1):
            logger.info(f"[LLM] Calling {label}: {model_name} (attempt {attempt}/{ATTEMPTS_PER_MODEL})")
            try:
                text, finish = _post_chat_completion(model_cfg, user_prompt)
                if finish == "length":
                    logger.warning("[LLM] Hit max_tokens — consider raising MAX_OUTPUT_TOKENS.")
                ok, why = _validate_brief(text)
                if ok:
                    logger.info(f"[LLM] Validated OK via {model_name}")
                    return strip_code_fences(text), model_name
                logger.warning(f"[LLM] REJECTED {label} output: {why}")
            except HTTPError as e:
                if e.code == 429:
                    logger.warning(f"[LLM] {label} rate-limited (429). Backing off {LLM_RATE_LIMIT_BACKOFF}s...")
                    time.sleep(LLM_RATE_LIMIT_BACKOFF)
                elif e.code == 404:
                    logger.warning(f"[LLM] {label} model not found (404). Skipping remaining attempts.")
                    break
                else:
                    logger.warning(f"[LLM] {label} failed: HTTP {e.code} {e.reason}")
            except Exception as e:
                logger.warning(f"[LLM] {label} failed: {e}")
            time.sleep(min(10, 3 * attempt))
        if idx < len(valid_chain) - 1:
            logger.info("[LLM] Falling back to next model...")
    return None, ""
def strip_code_fences(text: str) -> str:
    t = text.strip()
    if t.startswith("```"):
        t = re.sub(r'^```[a-zA-Z]*\n', '', t)
        t = re.sub(r'\n```\s*$', '', t)
    return t.strip()
# ==============================================================================
# ORCHESTRATION
# ==============================================================================
def main():
    print("=" * 60)
    print("  RSS INTELLIGENCE HARVESTER (ALL-IN-ONE)")
    print("  Fetch feeds -> filter -> LLM brief via NVIDIA Nemotron")
    print("=" * 60)
    # 1. Harvest feeds directly (fetch + parse + dedupe + date filter)
    articles = harvest_feeds()
    if not articles:
        logger.warning("Zero articles in window. Exiting.")
        return
    # 2. Build prompt with budget
    now_str = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    budget = {"left": INPUT_CHAR_BUDGET}
    stats = {"kept": 0, "dropped": 0}
    lines = []
    kept_articles = []   # FIX: track the items actually kept, for accurate pub counts
    for art in articles:
        pub = art.publication
        title = art.title.strip()
        summary = art.summary.replace("\n", " ").strip()
        time_str = art.pub_date.strftime("%H:%M")
        line = f"[{time_str}] [{pub}] {title}"
        if summary:
            line += f" — {summary}"
        if len(line) > MAX_CHARS_PER_ITEM:
            line = line[:MAX_CHARS_PER_ITEM] + " …[truncated]"
        cost = len(line) + 1
        if cost > budget["left"]:
            stats["dropped"] += 1
            continue
        budget["left"] -= cost
        stats["kept"] += 1
        lines.append(line)
        kept_articles.append(art)
    logger.info(f"[Prompt] Kept {stats['kept']} items ({stats['dropped']} dropped by budget).")
    # FIX: count publications over the kept set only. Previously this sliced
    # `articles[:stats["kept"]]`, which counted the first N sorted articles
    # rather than the ones that actually fit the budget — so dropped items
    # could be counted and kept items omitted.
    pubs = {}
    for art in kept_articles:
        pubs[art.publication] = pubs.get(art.publication, 0) + 1
    pub_summary = ", ".join(f"{k}: {v}" for k, v in sorted(pubs.items()))
    user_prompt = "\n\n".join([
        "=" * 80,
        f"NEWS RAW DATA DUMP | {now_str}",
        f"LOOKBACK: Today + Yesterday (UTC) | ENTRIES INCLUDED: {stats['kept']} ({pub_summary})",
        "=" * 80,
        "",
        "\n".join(lines),
        "",
        "=" * 80,
        "END OF RAW DATA DUMP",
    ])
    if KEEP_RAW_DUMP:
        try:
            Path(RAW_DUMP_FILENAME).write_text(user_prompt, encoding="utf-8")
            logger.info(f"Audit copy: {RAW_DUMP_FILENAME}")
        except Exception as e:
            logger.warning(f"Could not write raw dump: {e}")
    # 3. Call LLM pipeline
    brief, model_used = llm_prepare_file(user_prompt)
    if not brief:
        logger.error("All models failed validation. Raw dump + llm_debug retained.")
        sys.exit(1)
    # 4. Write output
    try:
        Path(OUTPUT_FILENAME).write_text(brief, encoding="utf-8")
        logger.info(f"SUCCESS [{model_used}]: Brief written -> {OUTPUT_FILENAME}")
    except Exception as e:
        logger.error(f"Failed to write file: {e}")
if __name__ == "__main__":
    main()
