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
import subprocess
import hashlib
import warnings
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Optional
from dataclasses import dataclass
import urllib.request
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
OUTPUT_DIR = Path(r"C:/Code/Trading")
OUTPUT_FILENAME = "news_intelligence_brief.txt"
RAW_DUMP_FILENAME = "news_context_raw_dump.txt"
KEEP_RAW_DUMP = True
DEBUG_DIR = OUTPUT_DIR / "llm_debug"

# --- NVIDIA API CONFIG (override via .env) ---
NVIDIA_API_BASE = os.getenv("NVIDIA_API_BASE", "https://integrate.api.nvidia.com/v1")
NVIDIA_API_KEY = os.getenv("NVIDIA_API_KEY")
NEMOTRON_ULTRA_MODEL = os.getenv("NEMOTRON_ULTRA_MODEL")
NEMOTRON_LIGHTNING_MODEL = os.getenv("NEMOTRON_LIGHTNING_MODEL")
NIM_POWERFUL_MODEL = os.getenv("NIM_POWERFUL_MODEL")

MODEL_CHAIN = [
    m for m in [
        {"model": NEMOTRON_ULTRA_MODEL, "base_url": NVIDIA_API_BASE, "api_key": NVIDIA_API_KEY},
        {"model": NIM_POWERFUL_MODEL, "base_url": NVIDIA_API_BASE, "api_key": NVIDIA_API_KEY},
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

# --- FEED FETCH GUARDS ---
FEED_TIMEOUT = 15
FEED_PAUSE_RANGE = (0.3, 0.8)   # polite pause between feed requests

# --- THINKING MODE ---
NO_THINK_KWARG = True

# --- OUTPUT VALIDATION GATE ---
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
    "FT: Opinion": "https://www.ft.com/opinion?format=rss",
    "FT: World": "https://www.ft.com/world?format=rss",
    "FT: UK": "https://www.ft.com/uk?format=rss",
    "FT: US": "https://www.ft.com/us?format=rss",
    "FT: Companies": "https://www.ft.com/companies?format=rss",
    "FT: Energy": "https://www.ft.com/energy?format=rss",
    "FT: Climate Capital": "https://www.ft.com/climate-capital?format=rss",
    "FT: Work & Careers": "https://www.ft.com/work-careers?format=rss",
    "FT: Financials": "https://www.ft.com/financials?format=rss",
    "FT: Technology": "https://www.ft.com/technology?format=rss",
    "FT: Health": "https://www.ft.com/health?format=rss",
    "FT: Industrials": "https://www.ft.com/industrials?format=rss",
    "FT: Media": "https://www.ft.com/media?format=rss",
    "FT: Professional Services": "https://www.ft.com/professional-services?format=rss",
    "FT: Retail & Consumer": "https://www.ft.com/retail-consumer?format=rss",
    "FT: Telecoms": "https://www.ft.com/telecoms?format=rss",
    "FT: China": "https://www.ft.com/china?format=rss",
    "FT: Africa": "https://www.ft.com/africa?format=rss",
    "FT: Europe": "https://www.ft.com/world/europe?format=rss",
    "FT: Americas": "https://www.ft.com/world/americas?format=rss",
    "FT: Asia-Pacific": "https://www.ft.com/world/asia-pacific?format=rss",
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

# ==============================================================================
# LOGGING
# ==============================================================================
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] - %(message)s')
logger = logging.getLogger("RSSIntelHarvester")

# ==============================================================================
# HTTP & UTILS
# ==============================================================================
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:135.0) Gecko/20100101 Firefox/135.0",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Encoding": "gzip, deflate",
    "Connection": "keep-alive"
}

def http_get(url: str, timeout=30) -> bytes:
    """GET with gzip handling; returns raw bytes (needed for XML)."""
    try:
        req = urllib.request.Request(url, headers=HEADERS)
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            data = resp.read()
            if resp.info().get('Content-Encoding') == 'gzip' or data.startswith(b'\x1f\x8b'):
                data = gzip.decompress(data)
            return data
    except Exception as e:
        logger.error(f"HTTP Fail [{url}]: {e}")
        return b""

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
# HARVEST: fetch all feeds -> dedupe -> date filter
# ==============================================================================
def harvest_feeds() -> list[Article]:
    """Fetch all feeds, dedupe by GUID, filter to today + yesterday (UTC)."""
    now_utc = datetime.datetime.now(datetime.timezone.utc)
    today = now_utc.date()
    yesterday = (now_utc - datetime.timedelta(days=1)).date()
    allowed = {today, yesterday}

    all_articles: dict[str, Article] = {}
    pubs = {}
    for name, url in FEEDS.items():
        pubs.setdefault(name.split(":")[0], []).append((name, url))

    total_raw = 0
    for pub, feeds in pubs.items():
        print(f"  ── {pub} ({len(feeds)} feeds) ──")
        for feed_name, url in feeds:
            content = http_get(url, timeout=FEED_TIMEOUT)
            if not content:
                print(f"    ✗ {feed_name}: fetch failed")
                continue
            articles = parse_feed(content, feed_name)
            # date filter + dedupe
            kept = 0
            for art in articles:
                total_raw += 1
                if art.pub_date.date() not in allowed:
                    continue
                key = art.guid or art.link
                if key not in all_articles:
                    all_articles[key] = art
                    kept += 1
            print(f"    ✓ {feed_name}: {len(articles)} fetched, {kept} in window")
            time.sleep(random.uniform(*FEED_PAUSE_RANGE))

    logger.info(f"Harvest: {total_raw} raw items, {len(all_articles)} unique in today/yesterday window.")
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
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    now_str = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    budget = {"left": INPUT_CHAR_BUDGET}
    stats = {"kept": 0, "dropped": 0}

    lines = []
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

    logger.info(f"[Prompt] Kept {stats['kept']} items ({stats['dropped']} dropped by budget).")

    pubs = {}
    for art in articles[:stats["kept"]]:
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
            (OUTPUT_DIR / RAW_DUMP_FILENAME).write_text(user_prompt, encoding="utf-8")
            logger.info(f"Audit copy: {OUTPUT_DIR / RAW_DUMP_FILENAME}")
        except Exception as e:
            logger.warning(f"Could not write raw dump: {e}")

    # 3. Call LLM pipeline
    brief, model_used = llm_prepare_file(user_prompt)
    if not brief:
        logger.error("All models failed validation. Raw dump + llm_debug retained.")
        sys.exit(1)

    # 4. Write output
    out_path = OUTPUT_DIR / OUTPUT_FILENAME
    try:
        out_path.write_text(brief, encoding="utf-8")
        logger.info(f"SUCCESS [{model_used}]: Brief written -> {out_path}")

        if sys.platform.startswith('win'):
            os.startfile(OUTPUT_DIR)
        elif sys.platform.startswith('darwin'):
            subprocess.Popen(['open', OUTPUT_DIR])
        else:
            subprocess.Popen(['xdg-open', OUTPUT_DIR])
    except Exception as e:
        logger.error(f"Failed to write file: {e}")

if __name__ == "__main__":
    main()
