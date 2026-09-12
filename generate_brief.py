#!/usr/bin/env python3
"""
GitHub Actions entry point:
harvest feeds -> LLM (wife prompt) -> brief.html for mobile reading.
"""

import re
import datetime
from pathlib import Path

import harvester

# ─── Override the harvester's trader prompt with the wife prompt ───
harvester.SYSTEM_PROMPT = """
ROLE: Friendly Morning News Editor.
STYLE: Warm, readable daily digest — NOT a trading brief. The reader is a
curious non-finance professional. No jargon without a plain explanation.

HEADER: Must start directly with `### YOUR MORNING BRIEF`
DISCLAIMER: Immediately beneath: `*(A fresh digest of today's news)*`
STRUCTURE: Short sections with `#### CATEGORY NAME` headings in Title Case,
generated dynamically from the actual content. Suggested sections when the
material supports them: Top Story, World News, U.S. News, Business & Money,
Tech & Science, Culture & Lifestyle, Food & Travel, Style & Home, Wellness,
Sports.

RULES:
  1. INCLUDE all lifestyle content — fashion, dining, travel, books, movies,
     TV, wellness, relationships, home, parenting. This reader wants it.
  2. 2-3 bullets per section max. Each bullet = headline + one friendly
     sentence explaining why it matters or what's interesting.
  3. Explain any finance term in parentheses (e.g., "the Fed raised rates
     (that's the cost of borrowing money)").
  4. No source attributions ("Per NYT", "According to...") — just tell the story.
  5. Single dash `-` bullets. No multi-sentence paragraphs.
  6. First line MUST be `### YOUR MORNING BRIEF`. No intro, no conclusion.
"""


def md_to_html(md: str) -> str:
    """Tiny markdown-to-HTML converter for h3/h4/bullets/paragraphs."""
    lines = md.strip().split("\n")
    out = []
    in_list = False
    for line in lines:
        s = line.strip()
        if s.startswith("### "):
            if in_list: out.append("</ul>"); in_list = False
            out.append(f"<h2>{s[4:]}</h2>")
        elif s.startswith("#### "):
            if in_list: out.append("</ul>"); in_list = False
            out.append(f"<h3>{s[5:]}</h3>")
        elif s.startswith("- "):
            if not in_list:
                out.append("<ul>"); in_list = True
            out.append(f"<li>{s[2:]}</li>")
        elif s.startswith("*(") and s.endswith(")*"):
            out.append(f"<p class='disclaimer'>{s[2:-2]}</p>")
        elif s:
            if in_list: out.append("</ul>"); in_list = False
            out.append(f"<p>{s}</p>")
    if in_list:
        out.append("</ul>")
    return "\n".join(out)


PAGE_TEMPLATE = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Morning Brief</title>
<style>
  body {{ font-family: Georgia, 'Times New Roman', serif; margin: 0;
         background: #fdf8f4; color: #33302b; }}
  .page {{ max-width: 640px; margin: 0 auto; padding: 24px 18px 60px; }}
  h2 {{ color: #a51c30; font-size: 1.7em; margin: 8px 0 4px;
       border-bottom: 3px solid #a51c30; padding-bottom: 8px; }}
  h3 {{ color: #6b5d4f; font-size: 1.15em; margin: 28px 0 8px; }}
  .disclaimer {{ color: #999; font-size: .85em; font-style: italic; }}
  ul {{ padding-left: 1.1em; }}
  li {{ margin: 12px 0; line-height: 1.55; }}
  p {{ line-height: 1.55; }}
  .footer {{ margin-top: 48px; color: #bbb; font-size: .8em;
            text-align: center; }}
</style>
</head>
<body>
<div class="page">
{content}
<div class="footer">Generated {generated}</div>
</div>
</body>
</html>"""


def main():
    print("Harvesting feeds...")
    articles = harvester.harvest_feeds()
    if not articles:
        print("No articles in window — nothing to do.")
        return

    print(f"Building prompt for {len(articles)} articles...")
    # Build the raw dump exactly like the trader pipeline does
    budget = 100_000
    lines = []
    for art in articles:
        line = f"[{art.pub_date.strftime('%H:%M')}] [{art.publication}] {art.title}"
        if art.summary:
            line += f" — {art.summary.replace(chr(10), ' ').strip()}"
        if len(line) > harvester.MAX_CHARS_PER_ITEM:
            line = line[:harvester.MAX_CHARS_PER_ITEM] + " …[truncated]"
        if len(line) + 1 <= budget:
            budget -= (len(line) + 1)
            lines.append(line)

    now_str = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    user_prompt = "\n\n".join([
        "=" * 80,
        f"NEWS RAW DATA DUMP | {now_str}",
        f"ENTRIES INCLUDED: {len(lines)}",
        "=" * 80,
        "",
        "\n".join(lines),
        "",
        "=" * 80,
        "END OF RAW DATA DUMP",
    ])

    print("Calling LLM...")
    brief, model_used = harvester.llm_prepare_file(user_prompt)
    if not brief:
        raise SystemExit("All LLM models failed validation.")

    html = PAGE_TEMPLATE.format(
        content=md_to_html(brief),
        generated=now_str + f" (via {model_used})",
    )
    Path("site/index.html").parent.mkdir(exist_ok=True)
    Path("site/index.html").write_text(html, encoding="utf-8")
    print(f"Wrote site/index.html ({len(html):,} chars).")


if __name__ == "__main__":
    main()
