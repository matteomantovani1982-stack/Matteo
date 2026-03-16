"""
app/api/routes/scout.py — StealthRole Signal Intelligence API

GET /api/v1/scout/signals   — main engine: ranked opportunity cards from signals
GET /api/v1/scout/jobs      — legacy job search (Adzuna/JSearch/Serper)
GET /api/v1/scout/config    — which sources are active
"""
import hashlib
import httpx
import structlog
from fastapi import APIRouter, Query
from app.dependencies import CurrentUserId, DB
from app.config import settings

logger = structlog.get_logger(__name__)
router = APIRouter(prefix="/api/v1/scout", tags=["Scout"])
TIMEOUT = 12.0


# ── Helpers ───────────────────────────────────────────────────────────────────

async def _get_profile_and_prefs(db: DB, user_id: str) -> tuple[dict, dict]:
    """Load user profile + parse preferences from global_context."""
    import json
    from app.services.profile.profile_service import ProfileService
    svc = ProfileService(db)
    profile = await svc.get_active_profile(user_id)
    if not profile:
        return {}, {}
    ctx = {}
    try:
        ctx = json.loads(profile.global_context or "{}")
    except Exception:
        pass
    prefs = ctx.get("__preferences", {})
    profile_dict = {
        "headline": profile.headline or "",
        "global_context": profile.global_context or "",
    }
    return profile_dict, prefs


# ── Signal Intelligence endpoint ──────────────────────────────────────────────

@router.get("/signals")
async def get_signals(
    current_user_id: CurrentUserId,
    db: DB,
    region: str = Query(default=""),
    sectors: str = Query(default=""),   # comma-separated
    roles: str = Query(default=""),     # comma-separated
) -> dict:
    """
    Main StealthRole intelligence endpoint.
    Runs the signal engine: detects market signals, scores fit with Claude,
    returns ranked OpportunityCards.
    Runs in a thread pool to avoid blocking the async event loop.
    """
    import asyncio
    from functools import partial
    from app.services.scout.signal_engine import run_signal_engine

    profile_dict, prefs = await _get_profile_and_prefs(db, current_user_id)

    # Override prefs with explicit query params if provided
    if region:
        prefs["regions"] = [region]
    if sectors:
        prefs["sectors"] = sectors.split(",")
    if roles:
        prefs["roles"] = roles.split(",")

    # Ensure sensible defaults
    if not prefs.get("regions"):
        prefs["regions"] = ["UAE"]

    # Run synchronous signal engine in thread pool
    loop = asyncio.get_running_loop()
    result = await loop.run_in_executor(
        None,
        partial(run_signal_engine, preferences=prefs, user_profile=profile_dict, max_results=20)
    )
    return result


# ── Legacy job search endpoint ────────────────────────────────────────────────

REGION_TO_ADZUNA = {
    "UAE":("ae","Dubai"),"KSA":("sa","Riyadh"),"Qatar":("qa","Doha"),
    "Kuwait":("kw","Kuwait City"),"Bahrain":("bh","Manama"),"Oman":("om","Muscat"),
    "Egypt":("eg","Cairo"),"UK":("gb","London"),"EU":("de","Berlin"),
    "US":("us","New York"),"Canada":("ca","Toronto"),"Global":("gb","London"),
}
REGION_TO_JSEARCH = {
    "UAE":"Dubai, UAE","KSA":"Riyadh, Saudi Arabia","Qatar":"Doha, Qatar",
    "Kuwait":"Kuwait City, Kuwait","Bahrain":"Manama, Bahrain","Oman":"Muscat, Oman",
    "Egypt":"Cairo, Egypt","UK":"London, UK","EU":"Europe","US":"United States",
    "Canada":"Canada","Global":"",
}
SOURCE_COLORS = {
    "LinkedIn":"#0a66c2","Indeed":"#2164f3","Glassdoor":"#0caa41",
    "Bayt":"#e84b37","GulfTalent":"#1a4e8c","NaukriGulf":"#4a90d9",
    "Monster":"#6a0dad","Adzuna":"#d63384","Web":"#555",
}

def _adzuna(keywords: str, location: str) -> list[dict]:
    if not settings.adzuna_app_id or not settings.adzuna_app_key:
        return []
    rc, city = REGION_TO_ADZUNA.get(location, ("gb", location))
    try:
        r = httpx.get(f"https://api.adzuna.com/v1/api/jobs/{rc}/search/1",
            params={"app_id":settings.adzuna_app_id,"app_key":settings.adzuna_app_key,
                    "what":keywords,"where":city,"results_per_page":10,"content-type":"application/json"},
            timeout=TIMEOUT)
        r.raise_for_status()
        jobs = []
        for item in r.json().get("results",[]):
            sal_min = item.get("salary_min"); sal_max = item.get("salary_max")
            sal = f"${int(sal_min):,}–${int(sal_max):,}" if sal_min and sal_max else ""
            jobs.append({
                "id": hashlib.md5(item.get("redirect_url","").encode()).hexdigest(),
                "title": item.get("title","")[:120],
                "company": item.get("company",{}).get("display_name","")[:80],
                "location": item.get("location",{}).get("display_name","")[:80],
                "snippet": item.get("description","")[:400],
                "url": item.get("redirect_url",""),
                "source":"Adzuna","salary":sal,
                "posted_date": item.get("created","")[:10],
                "source_color": SOURCE_COLORS["Adzuna"],
            })
        return jobs
    except Exception as e:
        logger.warning("adzuna_failed", error=str(e))
        return []

def _jsearch(keywords: str, location: str) -> list[dict]:
    if not settings.jsearch_api_key:
        return []
    loc = REGION_TO_JSEARCH.get(location, location)
    try:
        r = httpx.get("https://jsearch.p.rapidapi.com/search",
            headers={"X-RapidAPI-Key":settings.jsearch_api_key,"X-RapidAPI-Host":"jsearch.p.rapidapi.com"},
            params={"query":f"{keywords} {loc}","page":"1","num_pages":"1","date_posted":"month"},
            timeout=TIMEOUT)
        r.raise_for_status()
        jobs = []
        for item in r.json().get("data",[]):
            link = item.get("job_apply_link","")
            src = "LinkedIn" if "linkedin" in link.lower() else "Glassdoor" if "glassdoor" in link.lower() else "Indeed"
            sal_min = item.get("job_min_salary"); sal_max = item.get("job_max_salary")
            sal = f"${int(sal_min):,}–${int(sal_max):,} {item.get('job_salary_period','')}" if sal_min and sal_max else ""
            jobs.append({
                "id": item.get("job_id", hashlib.md5(link.encode()).hexdigest()),
                "title": item.get("job_title","")[:120],
                "company": item.get("employer_name","")[:80],
                "location": ", ".join(p for p in [item.get("job_city",""),item.get("job_country","")] if p)[:80],
                "snippet": item.get("job_description","")[:400],
                "url": link or item.get("job_google_link",""),
                "source":src,"salary":sal,
                "posted_date": item.get("job_posted_at_datetime_utc","")[:10],
                "source_color": SOURCE_COLORS.get(src,"#555"),
                "is_remote": item.get("job_is_remote",False),
            })
        return jobs
    except Exception as e:
        logger.warning("jsearch_failed", error=str(e))
        return []

def _serper_jobs(keywords: str, location: str) -> list[dict]:
    if not settings.serper_api_key:
        return []
    import httpx as _httpx
    try:
        seen, jobs = set(), []
        for query in [
            f"{keywords} jobs {location} site:linkedin.com/jobs",
            f"{keywords} jobs {location} site:bayt.com OR site:gulftalent.com",
            f"{keywords} hiring {location} 2026",
        ]:
            r = _httpx.post("https://google.serper.dev/search",
                headers={"X-API-KEY":settings.serper_api_key,"Content-Type":"application/json"},
                json={"q":query,"num":5}, timeout=TIMEOUT)
            r.raise_for_status()
            for item in r.json().get("organic",[]):
                url = item.get("link","")
                if not url or url in seen: continue
                seen.add(url)
                title = item.get("title",""); company = ""
                for sep in [" at "," - "," | "," @ "]:
                    if sep in title:
                        parts = title.split(sep,1); title = parts[0].strip(); company = parts[1].strip(); break
                src = next((s for s,d in [("LinkedIn","linkedin"),("Bayt","bayt"),("GulfTalent","gulftalent"),
                    ("Glassdoor","glassdoor"),("Indeed","indeed"),("NaukriGulf","naukrigulf")] if d in url), "Web")
                jobs.append({"id":hashlib.md5(url.encode()).hexdigest(),"title":title[:120],
                    "company":company[:80],"location":location,"snippet":item.get("snippet","")[:400],
                    "url":url,"source":src,"salary":"","posted_date":"",
                    "source_color":SOURCE_COLORS.get(src,"#555")})
        return jobs
    except Exception as e:
        logger.warning("serper_jobs_failed", error=str(e))
        return []

def _demo_jobs() -> list[dict]:
    return [
        {"id":"d1","title":"COO","company":"Series B Tech Company","location":"Dubai, UAE","snippet":"","url":"https://linkedin.com/jobs","source":"LinkedIn","salary":"AED 600K–900K","posted_date":"2026-03-01","source_color":"#0a66c2","is_remote":False,"requirements":["P&L","C-suite","Series B","MENA","equity"]},
        {"id":"d2","title":"VP Commercial","company":"GCC SaaS Scale-up","location":"Riyadh, KSA","snippet":"","url":"https://bayt.com","source":"Bayt","salary":"SAR 500K–700K","posted_date":"2026-03-03","source_color":"#e84b37","is_remote":False,"requirements":["GTM","revenue","KSA","SaaS"]},
        {"id":"d3","title":"General Manager","company":"PE-backed Portfolio Co.","location":"Abu Dhabi, UAE","snippet":"","url":"https://gulftalent.com","source":"GulfTalent","salary":"AED 720K+","posted_date":"2026-02-28","source_color":"#1a4e8c","is_remote":False,"requirements":["P&L","PE","board","UAE"]},
    ]


@router.get("/jobs")
async def scout_jobs(
    current_user_id: CurrentUserId, db: DB,
    keywords: str = Query(default=""),
    location: str = Query(default=""),
) -> dict:
    profile_dict, prefs = await _get_profile_and_prefs(db, current_user_id)

    if not keywords:
        roles = prefs.get("roles",[])
        seniority = prefs.get("seniority",[])
        keywords = " ".join((roles[:2]+seniority[:1])) or "senior director manager"
    if not location:
        location = (prefs.get("regions") or ["UAE"])[0]

    seen, all_jobs = set(), []
    def add(jobs):
        for j in jobs:
            if j.get("id","") not in seen:
                seen.add(j["id"]); all_jobs.append(j)

    add(_adzuna(keywords, location))
    add(_jsearch(keywords, location))
    if len(all_jobs) < 8:
        add(_serper_jobs(keywords, location))
    if not all_jobs:
        all_jobs = _demo_jobs()
        return {"jobs":all_jobs,"total":len(all_jobs),"query":keywords,"location":location,"is_demo":True,"sources_used":["demo"]}

    sources_used = list({j["source"] for j in all_jobs})
    return {"jobs":all_jobs[:24],"total":len(all_jobs),"query":keywords,"location":location,"is_demo":False,"sources_used":sources_used}


@router.get("/config")
async def scout_config(current_user_id: CurrentUserId) -> dict:
    return {
        "adzuna": bool(settings.adzuna_app_id and settings.adzuna_app_key),
        "jsearch": bool(settings.jsearch_api_key),
        "serper": bool(settings.serper_api_key),
        "claude": bool(settings.anthropic_api_key),
        "signal_engine": bool(settings.serper_api_key),
        "demo_mode": not any([settings.adzuna_app_id, settings.jsearch_api_key, settings.serper_api_key]),
    }
