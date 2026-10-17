#!/usr/bin/env python3
"""
================================================================================
        RSS INTELLIGENCE HARVESTER (ALL-IN-ONE, LLM-PROCESSED, HARDENED)
================================================================================
1. Fetches all RSS feeds directly (FT, WSJ, NYT, Bloomberg, SCMP).
2. Parses XML (RSS 2.0 + Atom), deduplicates, filters to today + yesterday (UTC).
3. Caps prompt size, sends dump + analyst instructions to NVIDIA Build models
   (primary, fallback via env) with output VALIDATION GATE.
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

  ==============================================================================
  LLM PIPELINE STRATEGY  (v3, carried forward unchanged through v5)
  ==============================================================================
  L1. PER-MODEL TUNING. `MAX_OUTPUT_TOKENS` / `LLM_TEMPERATURE` are gone.
      Each model gets `(max_output_tokens, temperature, timeout)` from
      `MODEL_TUNING`, matched by substring, with a conservative default for
      anything unrecognised. Step 5 Preview's settings follow its published
      model card (temperature=1.0, top_p=0.95); Nemotron entries do not send
      temperature, which the Nemotron models do not expose.

  L2. ESCALATING RETRY. `ATTEMPTS_PER_MODEL=2` used to re-send the identical
      payload, so a truncation failure was reproduced byte-for-byte 2 minutes
      later. Attempt 2 now multiplies the budget by `RETRY_TOKEN_MULTIPLIER`
      and re-sends — but ONLY when the previous attempt proved truncation
      (see L7).

  L3. REASONING IS RECOVERED, NOT DISCARDED. The old code detected
      `reasoning_content`, wrote it to llm_debug/, and then returned the empty
      `content` string so the validator rejected it as "too short (0 chars)".
      v3 returns the reasoning alongside the content and, when the reasoning
      itself contains the required output header (some models emit the answer
      inside the reasoning channel), validates it as a last resort.

  L4. THE FAILURE MODE IS NAMED. "too short (0 chars)" told you nothing.
      v3 logs the specific cause — truncated by max_tokens vs. thinking-only
      response vs. genuinely bad output — and names the knob to turn.

  L5. ONE CONFIG KNOB PER MODEL, EDITABLE IN CODE. Each entry is a plain
      dict literal at the top of the file. No .env plumbing, no precedence
      puzzles. If a model's budget needs changing, you change one number in
      one place and the startup banner prints what it resolved to.

  ==============================================================================
  v5 — WHAT CHANGED AND WHY  (2026-10-17, after script_ran_but_no_step5.txt)
  ==============================================================================
  The Step 5 failure was NOT a timeout and NOT a token budget. Both attempts
  died at ~273s and ~276s against a 900s ceiling, with
  `RemoteDisconnected` — which is `http.client.RemoteDisconnected`, raised
  when the PEER closes the connection before any HTTP response byte arrives.
  A client timeout raises a different exception from a different layer.

  Cause: `stream: False`. Step 5 is a reasoning model; on a ~25k-token news
  dump it computes for minutes before its first output token. With no bytes
  in flight, the gateway's idle limit closes the socket long before the model
  finishes. Raising request_timeout therefore buys nothing — you were never
  waiting on it.

  L6. SSE STREAMING (`stream: True`) with delta accumulation. Bytes flow from
      the first token onward, so the connection is never idle and the gateway
      limit is never reached. Time-to-first-byte is logged, which is the number
      that confirms or kills this theory on the next run. If an endpoint
      rejects `stream=true` (HTTP 400/422) the call retries non-streamed, and
      if streaming fails at the transport level the retry loop falls back to
      `stream=False`, so a streaming-hostile endpoint degrades instead of
      losing the model.

  CONFIRMED on the 2026-10-17 06:52 run. The primary model returned
  `finish_reason=stop | stream=yes ttfb=1.7s | content=32089 chars,
  reasoning=104925 chars` after a 690s generation, and validated OK. ttfb of
  1.7s against the previous ~273s idle wall is the whole story: the socket
  received bytes almost immediately this time and never went idle.

  L7. ESCALATION IS CONDITIONAL ON TRUNCATION. Attempt 2 doubles the budget
  only when attempt 1 returned `finish_reason=length`. After a transport
  failure it retries with the IDENTICAL budget. The old unconditional
  doubling gave Step 5 more room to think, which gave the gateway more
  time to give up (276.4s at 64000 vs 273.3s at 32000) and burned a whole
  attempt before falling back.

  L8. TRANSPORT FAILURES LOG NUMBERS. The old `except Exception` swallowed the
      failure with no evidence, and because the exception fired inside
      `urlopen()` before the response-dump line, `llm_debug/` held nothing
      from either attempt. Now logged: model, max_tokens, payload chars,
      approx tokens, elapsed seconds, exception class and message.

  FEEDS. WaPo dropped — five feeds returned four 502s and one 404 on three
  consecutive runs; that is a dead endpoint, not flakiness, and the 429-only
  retry path gave a 502 exactly one attempt. NYT: Job Market, NYT:
  Corrections and Bloomberg: Green dropped — 404 on every run. WSJ is kept
  but the run log has now answered the question: see the WSJ block below.

  ==============================================================================
  v6 — WHAT CHANGED AND WHY  (2026-10-17, after successful_run.txt)
  ==============================================================================
  L9. GLM-5.3-FLASH JOINS THE CHAIN AS THE INTERMEDIATE MODEL, and
      `NIM_ULTRA_POWERFUL_MODEL` — a secret that existed in the workflow env
      from the start but was never read by the script — is now used. The
      chain is: VPOWERFUL (Step 5 Preview, primary) -> ULTRA_POWERFUL
      (z-ai/glm-5.3-flash) -> POWERFUL -> ULTRA -> LIGHTNING.

      Placement rationale: GLM-5.3-Flash is a strong model with materially
      worse latency than Step 5. It is a good SECOND call and a poor FIRST
      call — if it were primary, every brief would wait on its latency even on
      the days Step 5 would have answered first. Behind the primary it costs
      nothing when Step 5 succeeds and only determines the run when it
      doesn't. The 34,194-char index.html proves Step 5 still lands first in
      practice, so GLM does not slow the daily job down.

  ==============================================================================
  v7 — WHAT CHANGED AND WHY  (2026-10-17, GLM budget at parity with Step 5)
  ==============================================================================
  v6 sized GLM-5.3-Flash at 24000 max_output_tokens on the theory that a
  non-reasoning model needs no chain-of-thought headroom — the Nemotron
  fallback had produced a validated brief in 1,781 tokens, so 24k looked like
  ample room. That reasoning was sound on paper and wrong in posture: it bets
  the intermediate model's success on the assumption that it stays small. The
  failure modes are asymmetric. Over-budgeting costs nothing, because
  `max_tokens` is a ceiling, not a target — an unused allowance is simply
  unspent. Under-budgeting costs the run: `finish_reason=length`, a rejected
  brief, and a fall through to Nemotron, which is exactly what position 2 was
  added to avoid.

  So GLM now matches Step 5 at 32000, with the doubled 1800s timeout retained
  on top. Worth naming what did NOT change and why: the retry escalation still
  treats the two identically (L7 — doubling to 64000 on `finish_reason=length`
  only), because the mechanism is about the transport, not the model. If GLM's
  ceiling still proves tight, the next move is `finish_reason=length` in the
  log saying so, which is exactly what L4 was built to surface.

  DIAGNOSED, NOT FIXED: WSJ. The three WSJ feeds have kept 0 items on every
  run, and the diagnostic added in v5 has now answered why — parsed dates are
  rooted in January 2025 (World News 2025-01-24..2025-01-27, Markets
  2025-01-25..2025-01-27, US Business 2025-01-24). Every other stale feed in
  the same run parsed correctly to within days of the window. The pubDates
  are genuinely old, which means the fix is a wider lookback window for WSJ
  specifically — NOT a parser change. That is deliberately left for a
  follow-up rather than guessed at here, because a wider window interacts
  with dedupe and with the prompt budget and deserves its own change.

  Also observed in the same run and left alone deliberately: FT: China and
  FT: Americas returned 0 in window with a parsed range ending 2026-10-17
  04:00, i.e. they are fresh but simply carry nothing inside the window
  today. That is normal feed variation, not a defect. Several NYT culture
  feeds (Movies/Music/Television/Theater/Travel/Automobiles) also returned 0
  with ranges that end on or near 2026-10-16 — again fresh items that missed
  the window by hours, not broken parsing.
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
            "NEMOTRON_LIGHTNING_MODEL", "NIM_POWERFUL_MODEL",
            "NIM_ULTRA_POWERFUL_MODEL", "NIM_VPOWERFUL_MODEL"]:
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
NIM_ULTRA_POWERFUL_MODEL = os.getenv("NIM_ULTRA_POWERFUL_MODEL")
NIM_VPOWERFUL_MODEL = os.getenv("NIM_VPOWERFUL_MODEL")
# v6 (L9): the chain is ordered by preference, not by model strength. Step 5
# Preview leads because latency, not quality, is what the daily job actually
# cares about — it answered in 690s when it worked, and the fallback models
# answer faster. GLM-5.3-Flash sits at position 2 because it is the best model
# that is reachable when Step 5 is not: good enough that you want it over
# Nemotron, slow enough that you do not want it first.
#
# Note `NIM_ULTRA_POWERFUL_MODEL` was already present in the workflow env from
# the beginning but was never read by this script. It now is.
MODEL_CHAIN = [
    m for m in [
        {"model": NIM_VPOWERFUL_MODEL, "base_url": NVIDIA_API_BASE, "api_key": NVIDIA_API_KEY},
        {"model": NIM_ULTRA_POWERFUL_MODEL, "base_url": NVIDIA_API_BASE, "api_key": NVIDIA_API_KEY},
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
# CHANGED (v3, L1): was a single global `MAX_OUTPUT_TOKENS = 8000`.
# See MODEL_TUNING below.
LLM_RATE_LIMIT_BACKOFF = 30
# CHANGED (v3, L1): was a single global `LLM_REQUEST_TIMEOUT = 300`.
# Baseline is now per-model via MODEL_TUNING; this is the floor for any model
# without an explicit entry.
DEFAULT_LLM_REQUEST_TIMEOUT = 300
# CHANGED (v3, L2): retry budget multiplier. Attempt 2 asks for this many more
# tokens than attempt 1 rather than repeating the request verbatim.
# CHANGED (v5, L7): applied only when attempt 1 returned finish_reason=length.
RETRY_TOKEN_MULTIPLIER = 2
# ==============================================================================
# PER-MODEL LLM TUNING  (L1)
# ==============================================================================
# Matched by substring against the model id, case-insensitively, in the order
# written — so a more specific key must come before a more general one. First
# match wins; anything unmatched falls through to _DEFAULT_MODEL_TUNING.
#
# `max_output_tokens` — the completion cap. Reasoning tokens count against it,
#   so a reasoning model on a ~25k-token news dump needs far more headroom than
#   a chat model needs.
# `temperature` / `top_p` — per the model's published recommendation. Step 5
#   Preview's card specifies 1.0 / 0.95; the Nemotron family has no sampling
#   dial, so its entries set temperature to the previous global default and it
#   is simply not sent (see `"send_temperature"`).
# `request_timeout` — socket ceiling only. It does NOT bound how long a
#   non-streamed request can wait: the gateway closes idle connections well
#   before this, which is why v5 streams (L6). Kept generous so a slow but
#   streamed response is never cut off client-side.
# `send_temperature` — False omits the key entirely rather than sending a
#   value the endpoint would reject with HTTP 400.
#
# v7: `max_output_tokens` is a CEILING, NOT A TARGET. Every model that has not
# yet been observed in production gets the most generous budget we are willing
# to afford rather than the smallest budget we think it needs. An unused
# allowance is simply unspent; a tight ceiling converts a model that was going
# to succeed into a `finish_reason=length` failure and a fall through to the
# next model. That asymmetry is why GLM-5.3-Flash now matches Step 5 at 32000.
MODEL_TUNING = [
    {
        "match": "step-5-preview",          # VPOWERFUL — Step 5 Preview
        "max_output_tokens": 32000,
        "temperature": 1.0,
        "top_p": 0.95,
        "request_timeout": 900,
        "send_temperature": True,
        "note": "Model card: temperature=1.0, top_p=0.95, up to 1M output.",
    },
    {
        # v6 (L9) added GLM; v7 raised its budget from 24000 to 32000, matching
        # Step 5 Preview exactly. See the v7 docstring block for why: a ceiling
        # is not a target, and over-provisioning a model that has never been
        # observed costs nothing while under-provisioning it costs the run.
        #
        # Non-reasoning, so it has no chain-of-thought to fund — but that is an
        # argument about what it will USE, not about what it should be ALLOWED.
        #
        # The doubled timeout (1800s vs Step 5's 900s) is retained from v6 and
        # is the one knob that matters for a latency-heavy model: with
        # streaming on, a long generation costs wall-clock, not correctness,
        # so the only thing a timeout can do here is cut a good answer off.
        # `temperature: 0.1` is still the house default rather than a GLM
        # model-card value — the first knob to revisit if GLM's output reads
        # flat. See the temperature note in the header block above.
        "match": "glm",
        "max_output_tokens": 32000,
        "temperature": 0.1,
        "top_p": 0.95,
        "request_timeout": 1800,
        "send_temperature": True,
        "note": "GLM-5.3-Flash: 32000 like Step 5; 1800s ceiling for its latency.",
    },
    {
        "match": "nemotron",                # ULTRA / LIGHTNING / POWERFUL(Super)
        "max_output_tokens": 16000,
        "temperature": 0.1,
        "top_p": 0.95,
        "request_timeout": 900,
        "send_temperature": False,
        "note": "No sampling dial; 0.1 retained only as a documented no-op.",
    },
]
_DEFAULT_MODEL_TUNING = {
    "match": "*",
    "max_output_tokens": 32000,
    "temperature": 0.1,
    "top_p": 0.95,
    "request_timeout": 900,
    "send_temperature": True,
    "note": "Unmatched model: generous budget, conservative temperature.",
}
# Field names NIM endpoints have been observed to use for the reasoning channel.
# v1/v2 read only the first entry, so an endpoint using `reasoning` returned
# nothing and the empty content was misread as a truncation.
REASONING_FIELD_NAMES = ["reasoning_content", "reasoning", "reasoning_text"]


def _tuning_for(model: str) -> dict:
    """Resolve the tuning entry for `model`. Unmatched -> conservative default."""
    low = (model or "").lower()
    for entry in MODEL_TUNING:
        if entry["match"].lower() in low:
            return entry
    return _DEFAULT_MODEL_TUNING


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
# --- INPUT SIZE GUARDS ---
MAX_CHARS_PER_ITEM = 500
INPUT_CHAR_BUDGET = 100_000
# --- THINKING MODE ---
NO_THINK_KWARG = True
# ==============================================================================
# THINKING MODE
# ==============================================================================
# The old gate required "nemotron" in the model name, so any non-Nemotron
# model (e.g. stepfun-ai/step-5-preview) silently kept its default thinking
# behaviour — which for a reasoning model on a 25k-token dump meant the
# entire token budget was consumed by CoT before a single answer token.
# The gate is now an explicit per-model set, so non-Nemotron models are handled
# by the same switch rather than falling through it.
#
# v6 (L9): GLM-5.3-Flash is NOT added here. It is a non-reasoning chat model,
# so it has no thinking mode to disable, and sending `chat_template_kwargs`
# to an endpoint that does not implement it is exactly the HTTP 400 that the
# retry path exists to survive. Step 5 Preview is likewise absent and streams
# fine on its default behaviour (confirmed by the 06:52 run).
NO_THINK_MODEL_KEYWORDS = ["nemotron"]
# ==============================================================================
# OUTPUT VALIDATION GATE
# ==============================================================================
MIN_BRIEF_CHARS = 600
MIN_WORD_RATIO = 0.60
REQUIRED_HEADERS = ["NEWS INTELLIGENCE BRIEF"]
# ==============================================================================
# FEED CONFIGURATION — organized by publication
# ==============================================================================
# v5 FEED REMOVALS (all evidenced across 3+ consecutive runs):
#   * All 5 Washington Post feeds — 4x HTTP 502 + 1x HTTP 404 every run.
#     A dead endpoint, not flakiness. Note the 429-only retry path meant a 502
#     got exactly one attempt, so nothing here was ever going to recover.
#   * NYT: Job Market        — HTTP 404 every run.
#   * NYT: Corrections       — HTTP 404 every run.
#   * Bloomberg: Green       — HTTP 404 every run.
#   * WSJ (3 feeds)          — KEPT. Keeps 0 items because their pubDates are
#     genuinely rooted in January 2025 (see the v6 docstring block). The fix
#     is a WSJ-specific wider lookback, deliberately deferred to its own
#     change rather than guessed at here.
FEEDS = {
    # ──── Financial Times ────
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
    # ──── Wall Street Journal ────
    "WSJ: World News": "https://feeds.a.dj.com/rss/RSSWorldNews.xml",
    "WSJ: Markets": "https://feeds.a.dj.com/rss/RSSMarketsMain.xml",
    "WSJ: US Business": "https://feeds.a.dj.com/rss/WSJcomUSBusiness.xml",
    # ──── The New York Times ────
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
    "NYT: Education": "https://rss.nytimes.com/services/xml/rss/nyt/Education.xml",
    "NYT: Magazine": "https://rss.nytimes.com/services/xml/rss/nyt/Magazine.xml",
    "NYT: Opinion": "https://rss.nytimes.com/services/xml/rss/nyt/Opinion.xml",
    "NYT: Obituaries": "https://rss.nytimes.com/services/xml/rss/nyt/Obituaries.xml",
    "NYT: Your Money": "https://rss.nytimes.com/services/xml/rss/nyt/YourMoney.xml",
    # ──── Bloomberg ────
    "Bloomberg: Markets": "https://feeds.bloomberg.com/markets/news.rss",
    "Bloomberg: Politics": "https://feeds.bloomberg.com/politics/news.rss",
    "Bloomberg: Business": "https://feeds.bloomberg.com/business/news.rss",
    "Bloomberg: Technology": "https://feeds.bloomberg.com/technology/news.rss",
    "Bloomberg: Wealth": "https://feeds.bloomberg.com/wealth/news.rss",
    "Bloomberg: Economics": "https://feeds.bloomberg.com/economics/news.rss",
    "Bloomberg: Industries": "https://feeds.bloomberg.com/industries/news.rss",
    # ──── South China Morning Post ────
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
}
# ──── Must-have feeds (always fetched) ────
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
    # v5: a feed can yield items and keep none for two very different reasons —
    # genuinely old pubDates, or items that only missed the window by hours.
    # The diagnostic range tells them apart, which matters because the fixes
    # are opposite: old dates want a wider lookback, fresh-but-outside dates
    # want nothing at all. Left in permanently rather than removed once WSJ was
    # diagnosed, because FT: China, FT: Americas and six NYT culture feeds also
    # trip it and are all benign.
    if articles and kept == 0:
        oldest = min(a.pub_date for a in articles)
        newest = max(a.pub_date for a in articles)
        allowed_str = ", ".join(str(d) for d in sorted(allowed))
        logger.info(f"    · {feed_name}: 0 kept — parsed {oldest:%Y-%m-%d %H:%M}.."
                    f"{newest:%Y-%m-%d %H:%M} UTC | allowed: {allowed_str}")
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
                        # v5: name the status, not just "fetch failed" — 404 and
                        # 502 need different responses and used to look alike.
                        print(f"    ✗ {feed_name}: HTTP {res.status} "
                              f"({res.error or 'no detail'})")
                finally:
                    # R1: unconditional courtesy pause, success or failure.
                    time.sleep(random.uniform(*FEED_COURTESY_SLEEP))
        return total_raw

    # ──── Pass 1: must-have ────
    total_raw = sweep(is_must_have, "")

    # ──── Pass 2: nice-to-have only if coverage is thin (R7) ────
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
# SYSTEM PROMPT — NEWS INTELLIGENCE BRIEF   (v4 policy, retained in v5/v6/v7)
# ==============================================================================
# v4 change: human-interest and lifestyle items were promoted to first-class
# content and romantic/sexual relationship coverage excluded.
#
# v1-v3 closed with "Discard pure noise: generic 'markets mixed' wrap-ups,
# clickbait, non-financial lifestyle fluff." That instruction was doing more
# than removing noise: it removed the Arts, Books, Film, Music, Television,
# Theatre, Fashion, Dining, Travel, Real Estate and Obituaries items that the
# harvester fetches as must-have feeds. The brief was macro-only by
# instruction rather than by data.
#
# v5 edits to this prompt are limited to the two source lists, because WaPo is
# no longer fetched (see the v5 feed removals above). v6 and v7 change nothing
# here — the balance policy and the romance exclusion are model-agnostic and
# were deliberately left untouched by a model-chain change.
SYSTEM_PROMPT = """
ROLE: Senior Intelligence Analyst writing a morning brief that is read by two people over breakfast — one a markets professional who wants macro and financial density, one a general reader who wants people, culture and life.
STYLE: Ultra-concise, dense, highly scannable. Zero conversational filler, zero setup sentences, zero meta-commentary. Every bullet must earn its place.
TASK: Process the provided raw news headlines + summaries from major publications (FT, WSJ, NYT, Bloomberg, SCMP) and transform them into a single, highly scannable section.

================================================================================
BALANCE — THE MOST IMPORTANT INSTRUCTION IN THIS PROMPT
================================================================================
The raw dump is dominated by markets and geopolitics. Do NOT let that dominate the brief.
Treat the following three bands as equals, and make sure all three are represented:

  1. MACRO & MARKETS — central banks, rates, bonds, currencies, commodities,
     geopolitics, trade, corporate earnings, deals, tech and AI, energy.
  2. PEOPLE & CULTURE — human-interest reporting, profiles, obituaries,
     remarkable lives, arts, books, film, television, music, theatre, fashion,
     food, travel, design, architecture, city life.
  3. IDEAS & LIVING — science, health, education, work, sport, personal
     finance, housing, and ordinary life.

A culture or human-interest item is NOT filler and is NOT less newsworthy than a market item. A superb obituary, a remarkable piece of reporting on ordinary life, or a genuinely good review belongs here on the same footing as a bond auction.
Conversely, weak market items are still weak items. Do not pad the culture sections with trivia to make the brief look balanced; select the strongest items in each band.
The test: a reader who skips the markets sections entirely should still find a full, satisfying read; a reader who skips the culture sections should still get the macro picture.

================================================================================
WHAT TO INCLUDE
================================================================================
Human interest and lifestyle (first-class content, not filler):
  - Reporting about people: profiles, obituaries, remarkable lives, human stories.
  - Arts, books, film, television, music, theatre — with the actual critical judgment and the concrete facts, not a bare headline.
  - Food, travel, design, architecture, fashion, and city life.
  - Science and health when they are about human life, not clinical abstracts.
  - Education, work, sport, housing, and personal finance.

================================================================================
WHAT TO EXCLUDE — skip these entirely
================================================================================
Content whose primary subject is interpersonal romantic or sexual relationships:
  1. Dating, courtship, and mating coverage; sex columns and sex-advice pieces.
  2. Romantic and sexual "how-to" — relationship advice, marriage counselling, guides about partners, personal essays about romantic life.
  3. Celebrity romance, breakups, engagements, and romantic gossip.
  4. Anything whose news value is WHO SOMEONE IS INVOLVED WITH rather than WHAT HAPPENED.
The reason is editorial: this brief is read by two people together, and romantic/relationship material tends to start conversations that neither reader wants at breakfast.

Also skip, regardless of band:
  - Generic "markets mixed" wrap-ups and no-news round-ups.
  - Clickbait, listicles, quizzes, and advice columns.
  - Personality pieces with no reported content.
  - Duplicate coverage of the same event already captured above.

SCOPE NOTE — READ THIS BEFORE APPLYING THE EXCLUSION ABOVE:
The exclusion applies ONLY to personal, romantic, and sexual relationships.
It does NOT apply to the ordinary, macro sense of the words "relations" or
"relationship", which must still be included in full:
  - "US–China relations", "EU–UK trade relations", "cross-strait relations"
  - "the Fed's relationship with markets", "the bank's relationship with its clients"
  - "US–Japan security relationship", "Moscow's relations with Beijing"
A geopolitical or business story must NEVER be dropped because it contains one of
those words. The exclusion is about romance, dating and sex — nothing else.

================================================================================
NEWS INTELLIGENCE BRIEF
================================================================================
HEADER: Must start directly with `### NEWS INTELLIGENCE BRIEF`
DISCLAIMER: Immediately beneath header: `*(All data sourced from public RSS feeds: FT, WSJ, NYT, Bloomberg, SCMP)*`
STRUCTURE: Divide into short sub-topical blocks.
SUB-HEADERS: Create your own Level-4 Markdown headings `#### CATEGORY NAME` in ALL CAPS that best fit the actual content. Do NOT use a fixed list. Generate categories dynamically from the feed — e.g., if the data contains central bank moves, create `#### CENTRAL BANKS & RATES`; if it contains semiconductor earnings, create `#### SEMICONDUCTORS & TECH`; if it contains China property, create `#### CHINA PROPERTY & CREDIT`; if it contains a great obituary, create `#### OBITUARIES` or `#### REMARKABLE LIVES`; if it contains film and books, create `#### ARTS & CULTURE`; if it contains food and travel, create `#### FOOD & TRAVEL`. Every item must appear under some category. No item left uncategorized. Typical macro categories emerge naturally (central banks, bonds, geopolitics, commodities, economic data, corporate earnings, tech/AI, regional), but adapt to what's actually in the window.
BULLETS: Single dash `-`, max 2-3 per sub-header.
CONSTRAINTS:
  1. Every entry = one isolated bullet point. No multi-sentence paragraphs.
  2. Retain every figure, bps, rate, quote, ticker ($AAPL, etc.), percentage.
  3. No source attributions unless citing a specific exclusive/scoop.
  4. DO NOT start bullets with "Per FT", "According to WSJ", "Bloomberg reports...".
  5. Jump straight to the core metric/event/entity (e.g., `- Brent crude topped $91/bbl on Hormuz closure risk...`).
  6. For culture items, jump straight to the judgment and the specifics (e.g., `- New Le Carré novel tracks a retired spy...`), not to the headline.
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
def _extract_reasoning(msg: dict) -> str:
    """Pull the reasoning channel out of a chat-completion message.

    v1/v2 read only `reasoning_content`. Some NIM endpoints use `reasoning`
    or `reasoning_text` instead; on those, the script saw an empty content
    field, no reasoning field, and reported a generic "too short (0 chars)".
    """
    for field in REASONING_FIELD_NAMES:
        val = msg.get(field)
        if isinstance(val, str) and val.strip():
            return val.strip()
    return ""


def _post_chat_completion(model_cfg: dict, user_prompt: str,
                          max_tokens: int, temperature: float
                          ) -> tuple[str, str, str, dict]:
    """One chat-completion call.

    Returns (content, finish_reason, reasoning, meta).

    v5 (L6): streams by default. `stream: False` is what produced the Step 5
    failures in script_ran_but_no_step5.txt — a reasoning model on a ~25k-token
    dump spends minutes computing before its first output token, and with no
    bytes in flight the gateway's idle limit closed the socket at ~273s with
    `http.client.RemoteDisconnected`, far inside the 900s socket timeout.
    Streaming keeps bytes moving from the first token, so the idle limit is
    never reached, and logs time-to-first-byte.

    Two fallbacks, both automatic:
      * HTTP 400/422 on the streamed request -> retry non-streamed.
      * a transport-level failure on the streamed request -> the caller retries
        with use_stream=False, so a streaming-hostile endpoint degrades to the
        old behaviour instead of losing the model.
    """
    base_url = model_cfg["base_url"].rstrip("/")
    api_key  = model_cfg["api_key"]
    model    = model_cfg["model"]
    tuning   = _tuning_for(model)
    url      = f"{base_url}/chat/completions"

    def build_payload(stream: bool) -> dict:
        payload = {
            "model": model,
            "messages": [
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user",   "content": user_prompt},
            ],
            "top_p": tuning["top_p"],
            "max_tokens": max_tokens,
            "stream": stream,
        }
        # Only send temperature when the model actually exposes a sampling dial.
        # Nemotron-family models reject the key with HTTP 400; v1/v2 always
        # sent it and relied on the 400-retry path, which wasted a round trip.
        if tuning.get("send_temperature", True):
            payload["temperature"] = temperature
        if NO_THINK_KWARG and any(kw in model.lower() for kw in NO_THINK_MODEL_KEYWORDS) \
                and "nvidia" in base_url:
            payload["chat_template_kwargs"] = {"enable_thinking": False}
        return payload

    def post(payload: dict):
        body = json.dumps(payload).encode("utf-8")
        hdrs = {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "Authorization": f"Bearer {api_key}",
            "User-Agent": HEADERS["User-Agent"],
        }
        req = urllib.request.Request(url, data=body, headers=hdrs, method="POST")
        return urllib.request.urlopen(req, timeout=tuning["request_timeout"])

    def send(payload: dict) -> str:
        with post(payload) as resp:
            return resp.read().decode("utf-8", errors="replace")

    def send_stream(payload: dict) -> tuple[str, str, str, float]:
        """Accumulate an SSE chat-completion stream.

        Returns (content, finish_reason, reasoning, ttfb_seconds). finish_reason
        is carried on the final chunk, so intermediate None values are ignored.
        """
        content_parts: list[str] = []
        reasoning_parts: list[str] = []
        finish = "?"
        ttfb = 0.0
        t_start = time.monotonic()
        with post(payload) as resp:
            for raw in resp:
                # First line back == first byte of response. This is the number
                # that shows whether the idle-gateway theory was right.
                if ttfb == 0.0:
                    ttfb = time.monotonic() - t_start
                line = raw.strip()
                if not line or line.startswith(b":") or not line.startswith(b"data:"):
                    continue        # blank line, SSE comment, or event:/id: field
                data = line[5:].strip()
                if data == b"[DONE]":
                    break
                try:
                    event = json.loads(data.decode("utf-8", errors="replace"))
                except json.JSONDecodeError:
                    continue
                for choice in event.get("choices") or []:
                    if choice.get("finish_reason"):
                        finish = choice["finish_reason"]
                    delta = choice.get("delta") or {}
                    piece = delta.get("content")
                    if isinstance(piece, str):
                        content_parts.append(piece)
                    for field in REASONING_FIELD_NAMES:
                        val = delta.get(field)
                        if isinstance(val, str) and val.strip():
                            reasoning_parts.append(val)
                            break
        return ("".join(content_parts).strip(), finish,
                "".join(reasoning_parts).strip(), ttfb)

    def parse_nonstream(raw: str) -> tuple[str, str, str]:
        data = json.loads(raw)
        choice = (data.get("choices") or [{}])[0]
        msg = choice.get("message") or {}
        content = msg.get("content")
        if isinstance(content, list):
            content = "".join(p.get("text", "") for p in content if isinstance(p, dict))
        return (content or "").strip(), choice.get("finish_reason", "?"), _extract_reasoning(msg)

    # --- attempt the streamed call, with the chat_template_kwargs escape ---
    payload = build_payload(True)
    streamed = False
    content = finish = reasoning = ""
    ttfb = 0.0
    try:
        content, finish, reasoning, ttfb = send_stream(payload)
        streamed = True
    except HTTPError as e:
        if NO_THINK_KWARG and e.code in (400, 422) and "chat_template_kwargs" in payload:
            logger.info(f"[LLM] {model}: endpoint rejected chat_template_kwargs; "
                        f"retrying streamed without.")
            payload.pop("chat_template_kwargs", None)
            content, finish, reasoning, ttfb = send_stream(payload)
            streamed = True
        elif e.code in (400, 422):
            logger.warning(f"[LLM] {model}: stream=true rejected "
                           f"(HTTP {e.code} {e.reason}); retrying non-streamed.")
        else:
            raise

    # --- fallback: one non-streamed read ---
    if not streamed:
        np = build_payload(False)
        try:
            raw = send(np)
        except HTTPError as e:
            if NO_THINK_KWARG and e.code in (400, 422) and "chat_template_kwargs" in np:
                logger.info(f"[LLM] {model}: endpoint rejected chat_template_kwargs; "
                            f"retrying without.")
                np.pop("chat_template_kwargs", None)
                raw = send(np)
            else:
                raise
        content, finish, reasoning = parse_nonstream(raw)
        _save_debug(f"response_{model}_attempt", raw)
    else:
        # No single raw blob exists when streaming, so dump the reconstruction
        # plus the metadata that mattered. Keeps llm_debug/ usable for forensics.
        _save_debug(f"response_{model}_attempt", json.dumps({
            "model": model,
            "max_tokens": max_tokens,
            "stream": True,
            "ttfb_seconds": round(ttfb, 2),
            "finish_reason": finish,
            "content_chars": len(content),
            "reasoning_chars": len(reasoning),
            "content": content,
        }, indent=2, ensure_ascii=False))

    stream_note = "stream=yes" if streamed else "stream=no"
    ttfb_note = f" ttfb={ttfb:.1f}s" if streamed else ""
    logger.info(f"[LLM] {model}: finish_reason={finish} | cap={max_tokens} | "
                f"{stream_note}{ttfb_note} | content={len(content)} chars, "
                f"reasoning={len(reasoning)} chars")
    if not content and reasoning:
        logger.warning(f"[LLM] 'content' empty but {len(reasoning)} chars reasoning returned.")
        _save_debug(f"orphan_reasoning_{model}", reasoning)
    meta = {"stream": streamed, "ttfb": round(ttfb, 2) if streamed else None}
    return content, finish, reasoning, meta
def llm_prepare_file(user_prompt: str) -> tuple[str | None, str]:
    """Walk the model chain, escalating the budget on retry. Kept under the
    old name so main() and any external callers keep working unchanged."""
    return generate_brief(user_prompt)


def generate_brief(user_prompt: str) -> tuple[str | None, str]:
    valid_chain = [m for m in MODEL_CHAIN if m.get("model") and m.get("base_url") and m.get("api_key")]
    if not valid_chain:
        logger.error("No valid models configured (check .env).")
        return None, ""
    if not NVIDIA_API_KEY and any("nvidia" in m["base_url"] for m in valid_chain):
        logger.error("NVIDIA_API_KEY missing for NVIDIA endpoints.")
        return None, ""
    logger.info(f"[LLM] Payload: {len(user_prompt):,} chars (~{len(user_prompt)//4:,} tokens est.)")
    _save_debug("last_request_user_message", user_prompt)
    # L1: print what each model resolved to, so the log shows the budget that
    # was actually sent rather than leaving it to be inferred from a failure.
    for m in valid_chain:
        t = _tuning_for(m["model"])
        logger.info(f"[LLM] Tuning for {m['model']}: max_tokens={t['max_output_tokens']}, "
                    f"temperature={'omitted' if not t.get('send_temperature', True) else t['temperature']}, "
                    f"timeout={t['request_timeout']}s ({t['note']})")
    # L9 (v6): print the chain order explicitly. With five entries the log line
    # "PRIMARY" / "FALLBACK" no longer tells you which position a model holds,
    # and knowing the position is how you tell a designed-for failure from an
    # accidental one.
    chain_desc = " -> ".join(f"{i+1}.{_tuning_for(m['model'])['match']}"
                             for i, m in enumerate(valid_chain))
    logger.info(f"[LLM] Chain order: {chain_desc}")
    for idx, model_cfg in enumerate(valid_chain):
        label = "PRIMARY" if idx == 0 else "FALLBACK"
        model_name = model_cfg["model"]
        tuning = _tuning_for(model_name)
        base_tokens = tuning["max_output_tokens"]
        temperature = tuning["temperature"]
        prev_finish = None
        for attempt in range(1, ATTEMPTS_PER_MODEL + 1):
            # v5 (L7): escalate only on evidence of truncation. Escalating after
            # a transport failure gave Step 5 more room to think, so the gateway
            # gave up later (276.4s at 64000 vs 273.3s at 32000) and a whole
            # attempt was spent before the fallback took over.
            if attempt == 1:
                max_tokens = base_tokens
            elif prev_finish == "length":
                max_tokens = base_tokens * RETRY_TOKEN_MULTIPLIER
            else:
                max_tokens = base_tokens
            logger.info(f"[LLM] Calling {label}: {model_name} "
                        f"(attempt {attempt}/{ATTEMPTS_PER_MODEL}, max_tokens={max_tokens})")
            t0 = time.monotonic()
            try:
                text, finish, reasoning, meta = _post_chat_completion(
                    model_cfg, user_prompt, max_tokens, temperature)
                prev_finish = finish
                if finish == "length":
                    logger.warning(f"[LLM] Hit max_tokens ({max_tokens}) — "
                                   f"consider raising it for {model_name}.")
                ok, why = _validate_brief(text)
                if ok:
                    logger.info(f"[LLM] Validated OK via {model_name}")
                    return strip_code_fences(text), model_name
                # L3: recovery path. Some models emit the answer inside the
                # reasoning channel. If the content channel is empty and the
                # reasoning carries the required output header, try it.
                if not text and reasoning:
                    upper_reason = reasoning.upper()
                    if any(h in upper_reason for h in REQUIRED_HEADERS):
                        ok_r, why_r = _validate_brief(reasoning)
                        if ok_r:
                            logger.info(f"[LLM] Recovered validated brief from reasoning "
                                        f"channel of {model_name} (content channel was empty).")
                            return strip_code_fences(reasoning), model_name
                        logger.warning(f"[LLM] Reasoning channel had the header but {why_r}.")
                # L4: name the failure mode instead of reporting a bare length.
                if not text and reasoning and finish == "length":
                    detail = (f"model spent its whole {max_tokens}-token budget on "
                              f"reasoning ({len(reasoning)} chars) and emitted no answer — "
                              f"raise max_tokens for {model_name}")
                elif not text:
                    detail = f"no content returned (finish_reason={finish})"
                else:
                    detail = why
                logger.warning(f"[LLM] REJECTED {label} output: {detail}")
            except HTTPError as e:
                elapsed = time.monotonic() - t0
                if e.code == 429:
                    logger.warning(f"[LLM] {label} rate-limited (429) after "
                                   f"{elapsed:.1f}s. Backing off {LLM_RATE_LIMIT_BACKOFF}s...")
                    time.sleep(LLM_RATE_LIMIT_BACKOFF)
                elif e.code == 404:
                    logger.warning(f"[LLM] {label} model not found (404). Skipping remaining attempts.")
                    break
                else:
                    # v5 (L8): include the numbers. The old line was
                    # "HTTP 500 Internal Server Error" with no model, no
                    # budget and no elapsed time, which is why the two 500s
                    # in script_ran_but_no_step5.txt were unexplainable.
                    logger.warning(f"[LLM] {label} failed: HTTP {e.code} {e.reason} "
                                   f"after {elapsed:.1f}s | model={model_name} "
                                   f"max_tokens={max_tokens}")
                    if e.code in (500, 502, 503, 504):
                        logger.warning("[LLM] Server-side error — retrying with an "
                                       "identical budget rather than escalating.")
            except Exception as e:
                # v5 (L8): this branch is where Step 5's failure landed, and it
                # previously carried no evidence at all. Because the exception
                # fires inside urlopen() before the response dump, llm_debug/
                # was empty for both attempts and the elapsed time was the only
                # clue available.
                elapsed = time.monotonic() - t0
                logger.warning(
                    f"[LLM] {label} TRANSPORT failure after {elapsed:.1f}s | "
                    f"model={model_name} | max_tokens={max_tokens} | "
                    f"payload={len(user_prompt):,} chars "
                    f"(~{len(user_prompt)//4:,} tok) | "
                    f"{type(e).__name__}: {e}")
                logger.warning(
                    "[LLM] Retrying with an IDENTICAL budget and with streaming "
                    "disabled — identical because escalating gave the model more "
                    "time to compute, which made the caller wait longer before "
                    "the peer closed the connection.")
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
# HEADER NUMBER-AGREEMENT NORMALIZER (deterministic)
# ==============================================================================
# The model invents its own sub-headers, so it sometimes emits a singular
# "lead item" heading (e.g. `#### TOP STORY`) above two or more bullets. The
# prompt asks it not to, but prompts are advisory; this enforces it in code.
#
# Deliberately whitelist-based rather than a general pluralizer: a blanket rule
# would wrongly rewrite legitimate category headings such as `#### TECHNOLOGY`
# or `#### ENERGY` that merely happen to hold several bullets. Extend the map
# below if the model starts emitting new singular lead headings.
_HEADING_RE = re.compile(r'^(\s*#{2,6}\s+)(.*\S)\s*$')
_BULLET_RE = re.compile(r'^\s*[-*]\s+\S')
_SINGULAR_TO_PLURAL_HEADINGS = {
    "TOP STORY": "TOP STORIES",
    "LEAD STORY": "LEAD STORIES",
    "MAIN STORY": "MAIN STORIES",
    "KEY STORY": "KEY STORIES",
    "TOP HEADLINE": "TOP HEADLINES",
    "LEAD HEADLINE": "LEAD HEADLINES",
    "HEADLINE": "HEADLINES",
    "TOP THEME": "TOP THEMES",
    "MAIN THEME": "MAIN THEMES",
    "TOP UPDATE": "TOP UPDATES",
    "TOP DEVELOPMENT": "TOP DEVELOPMENTS",
}


def _apply_case(sample: str, phrase: str) -> str:
    """Render `phrase` in the same case style as `sample`."""
    if sample.isupper():
        return phrase
    if sample.istitle():
        return phrase.title()
    return phrase.lower()


def _pluralize_heading(heading: str) -> str:
    """Map a known singular lead heading to its plural form, preserving case.
    Returns the heading unchanged if it is not in the map."""
    plural = _SINGULAR_TO_PLURAL_HEADINGS.get(heading.strip().upper())
    if plural is None:
        return heading
    return _apply_case(heading.strip(), plural)


def _normalize_header_number_agreement(text: str) -> str:
    """Rewrite a singular lead sub-heading to plural when two or more bullets
    sit beneath it. Only ever goes singular -> plural; category headings are
    never touched."""
    if not text:
        return text
    lines = text.split("\n")
    headings = [(i, m) for i, line in enumerate(lines)
                if (m := _HEADING_RE.match(line))]
    for pos, (line_no, m) in enumerate(headings):
        end = headings[pos + 1][0] if pos + 1 < len(headings) else len(lines)
        bullets = sum(1 for l in lines[line_no + 1:end] if _BULLET_RE.match(l))
        if bullets < 2:
            continue
        original = m.group(2)
        updated = _pluralize_heading(original)
        if updated != original:
            lines[line_no] = m.group(1) + updated
    return "\n".join(lines)
# ==============================================================================
# ORCHESTRATION
# ==============================================================================
def main():
    print("=" * 60)
    print("  RSS INTELLIGENCE HARVESTER (ALL-IN-ONE)")
    print("  Fetch feeds -> filter -> LLM brief via NVIDIA NIM")
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
    # 4. Deterministic tidy-up: enforce singular/plural agreement on sub-headers.
    brief = _normalize_header_number_agreement(brief)
    # 5. Write output
    try:
        Path(OUTPUT_FILENAME).write_text(brief, encoding="utf-8")
        logger.info(f"SUCCESS [{model_used}]: Brief written -> {OUTPUT_FILENAME}")
    except Exception as e:
        logger.error(f"Failed to write file: {e}")
if __name__ == "__main__":
    main()
