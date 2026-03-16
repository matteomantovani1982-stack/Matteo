"""
app/services/scout/signal_engine.py

StealthRole Signal Intelligence Engine
=======================================

Instead of searching for "jobs", we detect SIGNALS that indicate
a company is about to hire at senior level — before the job is posted.

Signal types:
  FUNDING     — raised capital → headcount growth imminent
  LEADERSHIP  — C-suite departure/arrival → replacement or restructure
  EXPANSION   — new market/product/M&A → team building needed
  VELOCITY    — spike in open roles → active growth phase
  DISTRESS    — layoffs/restructure → avoid or contrarian opportunity

Each signal is:
  - Detected from multiple web/news sources via Serper
  - Scored for recency and relevance
  - Fed to Claude which synthesises fit score, suggested role, contact, action
  - Returned as ranked OpportunityCards
"""

import hashlib
import json
import re
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

import httpx
import structlog

from app.config import settings

logger = structlog.get_logger(__name__)

TIMEOUT = 12.0
SERPER_URL = "https://google.serper.dev/search"
SERPER_NEWS_URL = "https://google.serper.dev/news"


# ─────────────────────────────────────────────────────────────────────────────
# Data models
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class Signal:
    company: str
    signal_type: str        # funding | leadership | expansion | velocity | distress
    headline: str
    detail: str
    source_url: str
    source_name: str
    published_date: str
    recency_score: float    # 0-1, 1=today
    raw_snippet: str


@dataclass
class OpportunityCard:
    id: str
    company: str
    company_type: str
    location: str
    sector: str
    signals: list[Signal]
    signal_summary: str
    fit_score: int
    fit_reasons: list[str]
    red_flags: list[str]
    suggested_role: str
    suggested_action: str
    contact_name: str
    contact_title: str
    apply_url: str
    is_posted: bool
    posted_title: str
    salary_estimate: str
    urgency: str            # high | medium | low


# ─────────────────────────────────────────────────────────────────────────────
# Serper helpers
# ─────────────────────────────────────────────────────────────────────────────

def _serper(query: str, num: int = 10) -> list[dict]:
    if not settings.serper_api_key:
        return []
    try:
        r = httpx.post(SERPER_URL,
            headers={"X-API-KEY": settings.serper_api_key, "Content-Type": "application/json"},
            json={"q": query, "num": num}, timeout=TIMEOUT)
        r.raise_for_status()
        return r.json().get("organic", [])
    except Exception as e:
        logger.warning("serper_error", q=query[:60], err=str(e))
        return []


def _serper_news(query: str, num: int = 10) -> list[dict]:
    if not settings.serper_api_key:
        return []
    try:
        r = httpx.post(SERPER_NEWS_URL,
            headers={"X-API-KEY": settings.serper_api_key, "Content-Type": "application/json"},
            json={"q": query, "num": num}, timeout=TIMEOUT)
        r.raise_for_status()
        return r.json().get("news", [])
    except Exception as e:
        logger.warning("serper_news_error", q=query[:60], err=str(e))
        return []


# ─────────────────────────────────────────────────────────────────────────────
# Signal keywords
# ─────────────────────────────────────────────────────────────────────────────

FUNDING_KW = ["raises","raised","funding","series a","series b","series c","seed round",
    "investment","backed","venture","growth capital","pe investment","private equity",
    "acquired","merger","million","billion","تمويل","مليون"]

LEADERSHIP_KW = ["appoints","appointed","names","joins as","promoted","steps down","resigns",
    "departed","new ceo","new coo","new cfo","new cto","chief executive","chief operating",
    "vice president","managing director","general manager","head of","country manager","leaves","exit"]

EXPANSION_KW = ["expands","expansion","launches","enters","new market","opens office",
    "new office","hiring","doubling","growing team","new product","partnership",
    "joint venture","uae","dubai","saudi","riyadh","gcc","mena","middle east"]

DISTRESS_KW = ["layoffs","laid off","restructuring","downsizing","cuts jobs","bankruptcy",
    "losses","revenue decline","struggling","investigation","fine","lawsuit"]


def _classify(text: str) -> str:
    t = text.lower()
    if any(k in t for k in FUNDING_KW):    return "funding"
    if any(k in t for k in LEADERSHIP_KW): return "leadership"
    if any(k in t for k in EXPANSION_KW):  return "expansion"
    if any(k in t for k in DISTRESS_KW):   return "distress"
    return "velocity"


def _company_from_title(title: str) -> str:
    for sep in [" raises "," appoints "," acquires "," launches "," expands "," names "," secures "," closes "]:
        if sep in title.lower():
            return title.split(sep, 1)[0].strip()[:60]
    return ""


def _date_from(r: dict) -> str:
    return str(r.get("date") or r.get("publishedDate") or "")[:10]


def _recency(date_str: str) -> float:
    if not date_str:
        return 0.3
    try:
        dt = datetime.strptime(date_str[:10], "%Y-%m-%d")
        days = (datetime.now() - dt).days
        return max(0.0, 1.0 - days / 180.0)
    except:
        return 0.3


def _source_label(url: str) -> str:
    labels = {"techcrunch":"TechCrunch","magnitt":"MAGNiTT","wamda":"Wamda","zawya":"Zawya",
        "arabianbusiness":"Arabian Business","bloomberg":"Bloomberg","reuters":"Reuters",
        "linkedin":"LinkedIn","glassdoor":"Glassdoor","crunchbase":"Crunchbase",
        "thenationalnews":"The National","khaleejtimes":"Khaleej Times",
        "gulfnews":"Gulf News","forbes":"Forbes","ft.com":"FT","wsj":"WSJ"}
    for k, v in labels.items():
        if k in url.lower():
            return v
    try:
        from urllib.parse import urlparse
        return urlparse(url).netloc.replace("www.","").split(".")[0].capitalize()
    except:
        return "Web"


# ─────────────────────────────────────────────────────────────────────────────
# Signal fetchers
# ─────────────────────────────────────────────────────────────────────────────

def _funding_signals(region: str, sectors: list[str], roles: list[str]) -> list[Signal]:
    sec = " ".join(sectors[:3]) if sectors else "tech fintech ecommerce"
    queries = [
        f"startup raises funding {region} 2025 2026 {sec}",
        f"series A B C funding {region} {sec} million",
        f"MENA funding round 2026 {sec}",
        f"private equity investment {region} 2026",
        f"venture capital {region} 2026 portfolio company",
    ]
    signals, seen = [], set()
    for q in queries:
        for r in _serper_news(q, 8):
            url = r.get("link","")
            if url in seen: continue
            seen.add(url)
            text = f"{r.get('title','')} {r.get('snippet','')}"
            if not any(k in text.lower() for k in FUNDING_KW): continue
            company = _company_from_title(r.get("title",""))
            if not company or len(company) < 3: continue
            signals.append(Signal(
                company=company, signal_type="funding",
                headline=r.get("title","")[:120], detail=r.get("snippet","")[:400],
                source_url=url, source_name=_source_label(url),
                published_date=_date_from(r), recency_score=_recency(_date_from(r)),
                raw_snippet=r.get("snippet",""),
            ))
    return signals


def _leadership_signals(region: str, sectors: list[str]) -> list[Signal]:
    queries = [
        f"CEO COO CFO appointed {region} 2025 2026",
        f"chief executive steps down {region} startup scaleup",
        f"new managing director general manager {region} 2026",
        f"VP director appointed {region} {' '.join(sectors[:2]) if sectors else 'tech'}",
        f"C-suite departure resignation {region} 2025 2026",
        f"new country manager {region} 2026",
    ]
    signals, seen = [], set()
    for q in queries:
        for r in _serper_news(q, 8):
            url = r.get("link","")
            if url in seen: continue
            seen.add(url)
            text = f"{r.get('title','')} {r.get('snippet','')}"
            if not any(k in text.lower() for k in LEADERSHIP_KW): continue
            company = _company_from_title(r.get("title",""))
            if not company or len(company) < 3: continue
            signals.append(Signal(
                company=company, signal_type="leadership",
                headline=r.get("title","")[:120], detail=r.get("snippet","")[:400],
                source_url=url, source_name=_source_label(url),
                published_date=_date_from(r), recency_score=_recency(_date_from(r)),
                raw_snippet=r.get("snippet",""),
            ))
    return signals


def _expansion_signals(region: str, sectors: list[str]) -> list[Signal]:
    queries = [
        f"company expands {region} new office 2025 2026",
        f"international company launches {region} operations",
        f"enters UAE Dubai market 2026",
        f"MENA expansion strategy 2026",
        f"regional hub {region} headquarters 2026",
        f"company opens {region} office hiring leadership",
    ]
    signals, seen = [], set()
    for q in queries:
        for r in _serper_news(q, 6):
            url = r.get("link","")
            if url in seen: continue
            seen.add(url)
            text = f"{r.get('title','')} {r.get('snippet','')}"
            if not any(k in text.lower() for k in EXPANSION_KW): continue
            company = _company_from_title(r.get("title",""))
            if not company or len(company) < 3: continue
            signals.append(Signal(
                company=company, signal_type="expansion",
                headline=r.get("title","")[:120], detail=r.get("snippet","")[:400],
                source_url=url, source_name=_source_label(url),
                published_date=_date_from(r), recency_score=_recency(_date_from(r)),
                raw_snippet=r.get("snippet",""),
            ))
    return signals


def _velocity_signals(roles: list[str], region: str, sectors: list[str]) -> list[Signal]:
    """Detect hiring spikes from multiple job boards and news sources — NOT just LinkedIn."""
    role_q = " OR ".join(roles[:4]) if roles else "COO CFO VP Director"
    sec = " ".join(sectors[:3]) if sectors else "tech fintech"
    # Multi-source: Bayt, GulfTalent, Indeed, Greenhouse, Lever, Workday, company sites
    JOB_SOURCES = ["bayt.com", "gulftalent.com", "indeed.com", "greenhouse.io", "lever.co",
                   "workday.com", "jobs.", "careers.", "naukrigulf.com", "monster.com"]
    queries = [
        f"senior executive hiring {region} {sec} 2026",
        f"{role_q} open position {region} {sec}",
        f"C-suite VP Director vacancy {region} 2026",
        f"site:bayt.com {role_q} {region}",
        f"site:gulftalent.com senior {role_q}",
        f"site:naukrigulf.com executive director {region}",
    ]
    signals, seen = [], set()
    for q in queries:
        for r in _serper(q, 8):
            url = r.get("link","")
            if url in seen: continue
            # Accept any job board or company careers page — not just LinkedIn
            is_job_source = any(x in url for x in JOB_SOURCES)
            is_careers = "career" in url or "job" in url or "recruit" in url
            if not (is_job_source or is_careers): continue
            seen.add(url)
            company = _company_from_title(r.get("title",""))
            if not company or len(company) < 3: continue
            signals.append(Signal(
                company=company, signal_type="velocity",
                headline=r.get("title","")[:120], detail=r.get("snippet","")[:400],
                source_url=url, source_name=_source_label(url),
                published_date=_date_from(r), recency_score=0.85,
                raw_snippet=r.get("snippet",""),
            ))
    return signals


def _live_job_openings(roles: list[str], region: str, sectors: list[str]) -> list[dict]:
    """
    Fetch CURRENT posted job openings — actual vacancies, not signals.
    Returns raw search results with is_posted=True marker.
    Sources: Bayt, GulfTalent, Indeed, LinkedIn Jobs, Naukrigulf, company career pages.
    """
    if not settings.serper_api_key:
        return []

    role_q = " OR ".join(f'"{r}"' for r in roles[:4]) if roles else '"COO" OR "CFO" OR "VP" OR "Director"'
    sec = " ".join(sectors[:3]) if sectors else ""
    region_q = region or "UAE Dubai"

    queries = [
        f"({role_q}) job vacancy {region_q} {sec} apply now",
        f"senior executive director open role {region_q} {sec} 2026",
        f"site:bayt.com {role_q} {region_q}",
        f"site:gulftalent.com {role_q}",
        f"site:naukrigulf.com {role_q} {region_q}",
        f"site:indeed.com {role_q} {region_q}",
        f"site:greenhouse.io OR site:lever.co senior director VP {region_q}",
    ]

    results, seen = [], set()
    for q in queries:
        for r in _serper(q, 8):
            url = r.get("link","")
            if url in seen: continue
            seen.add(url)
            title = r.get("title","")
            snippet = r.get("snippet","")
            if not title: continue
            results.append({
                "title": title[:120],
                "company": _company_from_title(title),
                "snippet": snippet[:400],
                "url": url,
                "source": _source_label(url),
                "date": _date_from(r),
                "recency": _recency(_date_from(r)),
                "is_posted": True,
            })

    # Also fetch news about companies actively hiring
    news_q = f"hiring expanding leadership team {region_q} {sec} 2026"
    for item in _serper_news(news_q, 10):
        url = item.get("link","")
        if url in seen: continue
        seen.add(url)
        title = item.get("title","")
        if any(k in title.lower() for k in ["layoff","cut","retrench","downsize"]): continue
        results.append({
            "title": title[:120],
            "company": _company_from_title(title),
            "snippet": item.get("snippet","")[:400],
            "url": url,
            "source": _source_label(url),
            "date": _date_from(item),
            "recency": _recency(_date_from(item)),
            "is_posted": False,  # It's a news signal, not a direct job posting
        })

    logger.info("live_jobs_fetched", count=len(results))
    return results[:30]


# ─────────────────────────────────────────────────────────────────────────────
# Claude scoring
# ─────────────────────────────────────────────────────────────────────────────

def _score_with_claude(signals: list[Signal], user_profile: dict, preferences: dict) -> list[OpportunityCard]:
    if not settings.anthropic_api_key or not signals:
        return _score_heuristic(signals, preferences)

    by_company: dict[str, list[Signal]] = {}
    for sig in signals:
        key = sig.company.lower().strip()
        by_company.setdefault(key, []).append(sig)

    signal_text = ""
    for company, sigs in list(by_company.items())[:25]:
        signal_text += f"\n## {company}\n"
        for s in sigs[:4]:
            signal_text += f"- [{s.signal_type.upper()}] {s.headline} ({s.published_date or 'recent'}) — {s.source_name}\n"
            if s.detail:
                signal_text += f"  {s.detail[:200]}\n"

    profile_summary = json.dumps({
        "headline": user_profile.get("headline",""),
        "background": (user_profile.get("global_context","") or "")[:600],
        "target_roles": preferences.get("roles",[]),
        "target_regions": preferences.get("regions",[]),
        "target_sectors": preferences.get("sectors",[]),
        "seniority": preferences.get("seniority",[]),
        "company_types": preferences.get("companyType",[]),
        "company_stages": preferences.get("stage",[]),
        "min_salary_aed": preferences.get("salaryMin",""),
    }, indent=2)

    prompt = f"""You are a top-tier executive search consultant — think Korn Ferry or Spencer Stuart — analysing live market signals to identify hidden senior career opportunities for a specific professional.

USER PROFILE:
{profile_summary}

LIVE MARKET SIGNALS:
{signal_text}

Your task: For each company, determine:
1. What does this signal specifically mean for hiring? Be precise.
   - Funding → how much raised, what stage, what headcount growth is implied
   - Leadership departure → which role is now open or needs backfilling
   - Expansion → which new roles are needed in which markets
   - High velocity → which specific roles are actively open

2. Fit score (0-100) based on:
   - Does the company stage match the user's experience?
   - Does the sector match their target?
   - Is the implied role in their wheelhouse?
   - Is the location right?
   - Are there red flags that would make this a bad move?

3. Specific action the user should take NOW — not generic advice.
   Name a specific person to contact if you can infer it from context.
   Say whether to apply, reach out cold, or wait.

4. Urgency:
   - high: signal < 45 days AND fit_score > 70
   - medium: signal 45-90 days OR fit_score 50-70
   - low: signal > 90 days OR fit_score < 50

Only return companies with fit_score >= 45.
Sort by urgency (high first), then fit_score descending.
Maximum 15 results.

Return ONLY a valid JSON array, no markdown, no preamble:
[
  {{
    "company": "Exact company name",
    "company_type": "startup|scale-up|corporate|pe|family-office|government",
    "location": "City, Country",
    "sector": "Sector name",
    "signal_summary": "Precise 1-2 sentence explanation of what happened and why it creates an opportunity",
    "fit_score": 85,
    "fit_reasons": ["Specific reason 1", "Specific reason 2", "Specific reason 3"],
    "red_flags": ["Specific concern if any"],
    "suggested_role": "Specific role title",
    "suggested_action": "Specific action — who to contact, how, why now",
    "contact_name": "Name if inferable from signal context",
    "contact_title": "Their title",
    "apply_url": "Direct URL to job or company LinkedIn page",
    "is_posted": false,
    "posted_title": "Actual job title if posted",
    "salary_estimate": "AED XXX–XXX or USD XXX–XXX",
    "urgency": "high|medium|low"
  }}
]"""

    try:
        import anthropic
        client = anthropic.Anthropic(api_key=settings.anthropic_api_key)
        resp = client.messages.create(
            model=settings.claude_model,
            max_tokens=4000,
            messages=[{"role": "user", "content": prompt}],
        )
        raw = resp.content[0].text.strip()
        raw = re.sub(r'^```json\s*', '', raw)
        raw = re.sub(r'\s*```$', '', raw)
        data = json.loads(raw)

        cards = []
        for i, item in enumerate(data[:20]):
            ckey = item.get("company","").lower()
            matched = by_company.get(ckey, [])
            if not matched:
                for k, sigs in by_company.items():
                    if ckey[:8] in k or k[:8] in ckey:
                        matched = sigs; break

            cards.append(OpportunityCard(
                id=hashlib.md5(f"{item.get('company','')}_{i}".encode()).hexdigest()[:12],
                company=item.get("company","Unknown"),
                company_type=item.get("company_type","startup"),
                location=item.get("location",""),
                sector=item.get("sector",""),
                signals=matched,
                signal_summary=item.get("signal_summary",""),
                fit_score=int(item.get("fit_score",50)),
                fit_reasons=item.get("fit_reasons",[]),
                red_flags=item.get("red_flags",[]),
                suggested_role=item.get("suggested_role",""),
                suggested_action=item.get("suggested_action",""),
                contact_name=item.get("contact_name",""),
                contact_title=item.get("contact_title",""),
                apply_url=item.get("apply_url",""),
                is_posted=item.get("is_posted",False),
                posted_title=item.get("posted_title",""),
                salary_estimate=item.get("salary_estimate",""),
                urgency=item.get("urgency","medium"),
            ))
        logger.info("claude_scored", count=len(cards))
        return cards
    except Exception as e:
        logger.warning("claude_score_failed", error=str(e))
        return _score_heuristic(signals, preferences)


def _score_heuristic(signals: list[Signal], preferences: dict) -> list[OpportunityCard]:
    by_company: dict[str, list[Signal]] = {}
    for sig in signals:
        by_company.setdefault(sig.company.lower(), []).append(sig)

    cards = []
    for _, sigs in by_company.items():
        if not sigs[0].company or len(sigs[0].company) < 3: continue
        types = {s.signal_type for s in sigs}
        best = max(sigs, key=lambda s: s.recency_score)

        score = 50
        reasons = []
        if "funding"    in types: score += 20; reasons.append("Recent funding — headcount growth expected")
        if "leadership" in types: score += 25; reasons.append("Leadership change — role likely open")
        if "expansion"  in types: score += 15; reasons.append("Market expansion — local leadership needed")
        score = min(score, 90)

        urgency = "high" if best.recency_score > 0.8 else "medium" if best.recency_score > 0.4 else "low"
        type_emoji = {"funding":"💰","leadership":"👤","expansion":"🌍","velocity":"📈","distress":"⚠️"}
        summary = " · ".join(type_emoji.get(t,t) + " " + t.capitalize() for t in types)
        summary += f" — {best.headline[:80]}"

        cards.append(OpportunityCard(
            id=hashlib.md5(sigs[0].company.encode()).hexdigest()[:12],
            company=sigs[0].company, company_type="startup",
            location="", sector="",
            signals=sigs, signal_summary=summary,
            fit_score=score, fit_reasons=reasons, red_flags=[],
            suggested_role=(preferences.get("roles") or ["Senior Executive"])[0],
            suggested_action="Research and reach out via LinkedIn",
            contact_name="", contact_title="",
            apply_url=best.source_url,
            is_posted=best.signal_type=="velocity", posted_title="",
            salary_estimate="", urgency=urgency,
        ))

    cards.sort(key=lambda c: (-{"high":3,"medium":2,"low":1}.get(c.urgency,1), -c.fit_score))
    return cards[:20]


# ─────────────────────────────────────────────────────────────────────────────
# Demo data
# ─────────────────────────────────────────────────────────────────────────────

def _demo_opportunities() -> list[OpportunityCard]:
    def sig(company, t, h, d, url, src, date, rec):
        return Signal(company=company, signal_type=t, headline=h, detail=d,
            source_url=url, source_name=src, published_date=date, recency_score=rec, raw_snippet=d)

    return [
        OpportunityCard(
            id="demo_1", company="Tabby", company_type="scale-up", location="Dubai, UAE", sector="Fintech",
            signals=[sig("Tabby","funding","Tabby raises $200M Series C led by STV and PayPal Ventures",
                "BNPL leader Tabby has raised $200M Series C to accelerate MENA expansion and product development.",
                "https://magnitt.com/news/tabby","MAGNiTT","2026-02-10",0.95)],
            signal_summary="🚀 Raised $200M Series C 7 weeks ago — scaling from 400 to 700 employees this year. No COO publicly listed. VP Operations role implied.",
            fit_score=92, fit_reasons=["MENA fintech aligns with your sector focus","Series C stage = structured ops needed","P&L ownership at your level","COO/VP Ops role not yet filled","Strong equity upside at this stage"],
            red_flags=[],
            suggested_role="COO / VP Operations",
            suggested_action="Connect with Hosam Arab (CEO) or Ahmed Al-Zaabi (VP People) on LinkedIn this week — before they engage a search firm",
            contact_name="Hosam Arab", contact_title="CEO & Co-founder",
            apply_url="https://linkedin.com/company/tabby", is_posted=False, posted_title="",
            salary_estimate="AED 750K–1.1M + equity", urgency="high",
        ),
        OpportunityCard(
            id="demo_2", company="Noon", company_type="corporate", location="Dubai, UAE", sector="E-commerce",
            signals=[sig("Noon","leadership","Noon's Chief Operating Officer departs after 3-year tenure",
                "Noon.com's COO has stepped down amid broader executive restructuring as the company refocuses on profitability.",
                "https://arabianbusiness.com","Arabian Business","2026-02-25",0.90)],
            signal_summary="👤 COO departed 12 days ago — role not yet posted. Board conducting discreet search before going external.",
            fit_score=85, fit_reasons=["E-commerce operations is your core","Scale of business ($500M+ GMV) matches your level","Direct board report","Mohamed Alabbar connection possible"],
            red_flags=["High leadership turnover — 3rd COO in 4 years","Culture can be demanding"],
            suggested_role="COO",
            suggested_action="Reach out to Head of Talent before external search firm is engaged — they typically wait 3-4 weeks before going external",
            contact_name="Sara Ahmed", contact_title="Head of Talent Acquisition",
            apply_url="https://linkedin.com/company/noon", is_posted=False, posted_title="",
            salary_estimate="AED 900K–1.4M", urgency="high",
        ),
        OpportunityCard(
            id="demo_3", company="Careem", company_type="corporate", location="Dubai, UAE", sector="Tech / Mobility",
            signals=[sig("Careem","expansion","Careem expands super-app to 5 new MENA markets in 2026",
                "Careem launches services in Iraq, Algeria, Morocco, Jordan and Libya — needs country leadership teams.",
                "https://techcrunch.com","TechCrunch","2026-01-15",0.75)],
            signal_summary="🌍 Expanding into 5 new markets — needs Country GMs + Regional VP Operations. VP Ops role posted last week.",
            fit_score=79, fit_reasons=["Multi-market MENA operations","Uber-backed scale gives stability","Regional expansion mandate — high visibility role"],
            red_flags=["Subsidiary of Uber — some political complexity"],
            suggested_role="VP Operations MENA / Country GM",
            suggested_action="Apply directly via Careem careers — role posted. Also reach out to hiring manager to signal intent",
            contact_name="", contact_title="",
            apply_url="https://careem.com/careers", is_posted=True, posted_title="VP Operations — MENA Expansion",
            salary_estimate="AED 620K–880K", urgency="medium",
        ),
        OpportunityCard(
            id="demo_4", company="Tamara", company_type="scale-up", location="Riyadh, KSA", sector="Fintech",
            signals=[sig("Tamara","funding","Tamara closes $340M Series C to fuel Saudi BNPL growth",
                "Saudi-based BNPL player Tamara raises $340M Series C to expand merchant network and team.",
                "https://zawya.com","Zawya","2025-12-20",0.60)],
            signal_summary="💰 $340M Series C 3 months ago — building out senior team for Saudi Vision 2030 aligned growth. CFO and COO searches underway.",
            fit_score=74, fit_reasons=["KSA fintech in your target region","Vision 2030 alignment = government support","Competitive comp + equity at this stage"],
            red_flags=["KSA-based = relocation required","Competitive market vs Tabby"],
            suggested_role="CFO / COO",
            suggested_action="Connect with Tarek Elhousseiny (CEO) — he's active on LinkedIn. Mention specific experience with BNPL unit economics",
            contact_name="Tarek Elhousseiny", contact_title="CEO",
            apply_url="https://linkedin.com/company/tamara", is_posted=False, posted_title="",
            salary_estimate="SAR 650K–950K + equity", urgency="medium",
        ),
        OpportunityCard(
            id="demo_5", company="Pure Harvest", company_type="startup", location="Abu Dhabi, UAE", sector="AgriTech",
            signals=[sig("Pure Harvest","leadership","Pure Harvest appoints new CEO, CFO search underway",
                "AgriTech company Pure Harvest brings in new CEO Sky Kurtz returns, now seeking CFO and CCO.",
                "https://thenationalnews.com","The National","2026-02-01",0.82)],
            signal_summary="👤 New CEO appointed last month, CFO + CCO searches active — full leadership rebuild underway",
            fit_score=62, fit_reasons=["Leadership rebuild = multiple openings","UAE-based, Mubadala-backed","Board-level visibility"],
            red_flags=["Earlier financial difficulties 2022-23","Niche sector — AgriTech learning curve","Smaller scale than your typical targets"],
            suggested_role="CFO / Chief Commercial Officer",
            suggested_action="Connect directly with new CEO Sky Kurtz — fresh start, explicitly open to senior introductions",
            contact_name="Sky Kurtz", contact_title="CEO",
            apply_url="https://linkedin.com/company/pure-harvest-smart-farms", is_posted=False, posted_title="",
            salary_estimate="AED 400K–580K", urgency="medium",
        ),
    ]


# ─────────────────────────────────────────────────────────────────────────────
# Serialisation
# ─────────────────────────────────────────────────────────────────────────────

def _card_to_dict(card: OpportunityCard) -> dict:
    signal_types = list({s.signal_type for s in card.signals})
    sources = [{"url": s.source_url, "name": s.source_name, "date": s.published_date, "headline": s.headline} for s in card.signals[:5]]

    badge_map = {
        "funding":   {"label": "💰 Funding",          "color": "#059669", "bg": "#f0fdf4"},
        "leadership":{"label": "👤 Leadership change", "color": "#7c3aed", "bg": "#f5f3ff"},
        "expansion": {"label": "🌍 Expansion",         "color": "#2563eb", "bg": "#eff6ff"},
        "velocity":  {"label": "📈 Hiring now",        "color": "#0891b2", "bg": "#ecfeff"},
        "distress":  {"label": "⚠️ Restructuring",    "color": "#dc2626", "bg": "#fef2f2"},
    }
    urgency_map = {"high": "#dc2626", "medium": "#d97706", "low": "#6b7280"}
    company_type_map = {
        "startup":"Startup","scale-up":"Scale-up","corporate":"Corporate",
        "pe":"PE-backed","family-office":"Family Office","government":"Government",
    }

    return {
        "id": card.id,
        "company": card.company,
        "company_type": card.company_type,
        "company_type_label": company_type_map.get(card.company_type, card.company_type),
        "location": card.location,
        "sector": card.sector,
        "signal_summary": card.signal_summary,
        "signal_types": signal_types,
        "signal_badges": [badge_map.get(t, {"label": t, "color": "#555", "bg": "#f5f5f5"}) for t in signal_types],
        "signal_sources": sources,
        "fit_score": card.fit_score,
        "fit_reasons": card.fit_reasons,
        "red_flags": card.red_flags,
        "suggested_role": card.suggested_role,
        "suggested_action": card.suggested_action,
        "contact_name": card.contact_name,
        "contact_title": card.contact_title,
        "apply_url": card.apply_url,
        "is_posted": card.is_posted,
        "posted_title": card.posted_title,
        "salary_estimate": card.salary_estimate,
        "urgency": card.urgency,
        "urgency_color": urgency_map.get(card.urgency, "#6b7280"),
    }


# ─────────────────────────────────────────────────────────────────────────────
# Main entry point
# ─────────────────────────────────────────────────────────────────────────────

def run_signal_engine(preferences: dict, user_profile: dict, max_results: int = 20) -> dict:
    regions  = preferences.get("regions",  ["UAE"])
    sectors  = preferences.get("sectors",  [])
    roles    = preferences.get("roles",    [])
    region   = regions[0] if regions else "UAE Dubai"

    logger.info("signal_engine_start", region=region, sectors=sectors, roles=roles)

    logger.info("signal_engine_config",
        has_serper=bool(settings.serper_api_key),
        has_anthropic=bool(settings.anthropic_api_key),
        serper_key_prefix=settings.serper_api_key[:8] if settings.serper_api_key else "MISSING",
    )

    if not settings.serper_api_key:
        logger.warning("signal_engine_no_serper_key")
        return {
            "opportunities": [_card_to_dict(c) for c in _demo_opportunities()],
            "signals_detected": 0, "sources_searched": 0,
            "is_demo": True, "engine_version": "2.0", "scored_by": "demo",
        }

    # Gather all signals (pre-hiring intelligence)
    all_signals: list[Signal] = []
    for fetcher, label in [
        (_funding_signals(region, sectors, roles),   "funding"),
        (_leadership_signals(region, sectors),        "leadership"),
        (_expansion_signals(region, sectors),         "expansion"),
        (_velocity_signals(roles, region, sectors),   "velocity"),
    ]:
        all_signals.extend(fetcher)
        logger.info(f"{label}_signals", count=len(fetcher))

    # Gather live job openings (posted vacancies)
    live_openings = _live_job_openings(roles, region, sectors)
    logger.info("live_openings", count=len(live_openings))

    # Deduplicate signals
    seen, unique = set(), []
    for sig in all_signals:
        key = f"{sig.company.lower()[:20]}_{sig.signal_type}_{sig.headline[:25].lower()}"
        if key not in seen:
            seen.add(key)
            unique.append(sig)

    logger.info("unique_signals", count=len(unique))

    cards = _score_with_claude(unique, user_profile, preferences)
    scored_by = "claude" if settings.anthropic_api_key else "heuristic"

    # Classify openings by urgency: posted=green, recent news signal=yellow
    classified_openings = []
    for job in live_openings:
        rec = job.get("recency", 0.5)
        if job.get("is_posted"):
            status = "current"       # green — live posted vacancy
        elif rec > 0.7:
            status = "imminent"      # yellow — very recent signal, hiring likely
        else:
            status = "strategic"     # red/grey — longer-horizon signal
        classified_openings.append({**job, "status": status})

    return {
        "opportunities": [_card_to_dict(c) for c in cards[:max_results]],
        "live_openings": classified_openings[:20],
        "signals_detected": len(unique),
        "sources_searched": len(all_signals),
        "is_demo": False,
        "engine_version": "3.0",
        "scored_by": scored_by,
    }
