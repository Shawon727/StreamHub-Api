# StreamHub API v6.0.0 — MovieBox-TUI parity
# Providers: MovieBox · 4KHDHub · Dramachi · HubCloud · IPTV
# creator: shawon

from __future__ import annotations

import os
import re
import json
import time
import hashlib
import hmac
import base64
import random
import uuid
import html as html_lib
import asyncio
from urllib.parse import urlparse, parse_qsl, urlencode, urljoin, quote
from typing import Optional, Any, List, Dict

import httpx
try:
    from bs4 import BeautifulSoup
except ImportError:
    BeautifulSoup = None  # type: ignore

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse, Response

VERSION = "6.0.0"
CREATOR = "shawon"

app = FastAPI(
    title="StreamHub API",
    description="MovieBox-TUI parity — MovieBox · 4KHDHub · Dramachi · IPTV · HubCloud",
    version=VERSION,
    docs_url="/docs",
    redoc_url=None,
)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


def ok(**kwargs) -> dict:
    out = {"ok": True, "creator": CREATOR}
    out.update(kwargs)
    return out


def fail(msg: str, **kwargs) -> dict:
    out = {"ok": False, "creator": CREATOR, "error": msg}
    out.update(kwargs)
    return out

# =============================================================================
# MovieBox (HMAC-MD5 — MovieBox-TUI crypto.rs)
# =============================================================================

MB_SECRET = bytes([
    0xEF, 0xA8, 0x91, 0x97, 0x4E, 0xEC, 0xD3, 0x14, 0x8D, 0xF6, 0x3A, 0xA6, 0x11, 0x60, 0x2D, 0xEF,
    0xD1, 0x01, 0x25, 0x9B, 0xA5, 0x21, 0x02, 0x2C, 0x57, 0xAE, 0x05, 0x66, 0xBD, 0x8E,
])
MB_HOSTS = [
    "https://api6.aoneroom.com",
    "https://api5.aoneroom.com",
    "https://api4.aoneroom.com",
    "https://api4sg.aoneroom.com",
    "https://api3.aoneroom.com",
    "https://api6sg.aoneroom.com",
    "https://api.inmoviebox.com",
]
MB_UA = (
    "com.community.oneroom/50020126 (Linux; U; Android 14; en_US; Pixel 6; "
    "Build/AP2A.240805.005; Cronet/135.0.7012.3)"
)


def _md5_hex(data: bytes) -> str:
    return hashlib.md5(data).hexdigest()


def _mb_client_token(ts_ms: int) -> str:
    s = str(ts_ms)
    return f"{s},{_md5_hex(s[::-1].encode())}"


def _mb_sign(method: str, url: str, body: Optional[str] = None) -> tuple:
    ts = int(time.time() * 1000)
    parsed = urlparse(url)
    qs = "&".join(f"{k}={v}" for k, v in sorted(parse_qsl(parsed.query, keep_blank_values=True)))
    canon_url = parsed.path + (("?" + qs) if qs else "")
    body_bytes = (body or "").encode()
    body_hash = _md5_hex(body_bytes) if body else ""
    body_len = str(len(body_bytes)) if body else ""
    canonical = (
        f"{method.upper()}\napplication/json\napplication/json\n"
        f"{body_len}\n{ts}\n{body_hash}\n{canon_url}"
    )
    mac = hmac.new(MB_SECRET, canonical.encode(), hashlib.md5).digest()
    sig = base64.b64encode(mac).decode()
    return ts, f"{ts}|2|{sig}", _mb_client_token(ts)


def _mb_headers(method: str, url: str, body: Optional[str] = None) -> dict:
    ts, sig, tok = _mb_sign(method, url, body)
    info = {
        "package_name": "com.community.oneroom",
        "version_name": "4.0.02.0831.03",
        "version_code": 50020126,
        "os": "android",
        "os_version": "14",
        "install_ch": "ps",
        "device_id": uuid.uuid4().hex,
        "install_store": "ps",
        "gaid": str(uuid.uuid4()),
        "brand": "Google",
        "model": "Pixel 6",
        "system_language": "en",
        "net": "wifi",
        "region": "US",
        "timezone": "Asia/Dhaka",
        "sp_code": "40401",
        "X-Play-Mode": "2",
    }
    return {
        "User-Agent": MB_UA,
        "Accept": "application/json",
        "Content-Type": "application/json",
        "x-client-token": tok,
        "x-tr-signature": sig,
        "x-client-info": json.dumps(info, separators=(",", ":")),
        "x-client-status": "0",
        "x-forwarded-for": f"103.241.{random.randint(1, 250)}.{random.randint(1, 250)}",
    }


async def _mb_request(path: str, params: Optional[dict] = None, method: str = "GET", body: Optional[str] = None) -> dict:
    q = ("?" + urlencode(params)) if params else ""
    last_err = "all hosts failed"
    async with httpx.AsyncClient(timeout=18.0, follow_redirects=True) as client:
        for host in MB_HOSTS:
            url = host.rstrip("/") + path + q
            headers = _mb_headers(method, url, body)
            try:
                if method.upper() == "POST":
                    r = await client.post(url, headers=headers, content=body or "{}")
                else:
                    r = await client.get(url, headers=headers)
                if r.status_code == 200:
                    try:
                        return r.json()
                    except Exception:
                        last_err = "non-json"
                        continue
                if r.status_code == 403:
                    last_err = f"403 region blocked ({host})"
                    continue
                last_err = f"HTTP {r.status_code} ({host})"
            except Exception as e:
                last_err = str(e)[:80]
    raise HTTPException(502, f"MovieBox: {last_err}")


def _mb_item(x: dict) -> dict:
    if not isinstance(x, dict):
        return {}
    sid = str(x.get("subjectId") or x.get("id") or x.get("subject_id") or "")
    return {
        "id": sid,
        "subject_id": sid,
        "title": x.get("title") or x.get("name") or "",
        "type": x.get("subjectType") or x.get("type") or x.get("contentType") or "",
        "poster": x.get("cover") or x.get("poster") or x.get("image") or x.get("thumbnail") or "",
        "year": x.get("releaseDate") or x.get("year") or "",
        "imdb": x.get("imdbRate") or x.get("imdb") or x.get("score"),
        "description": (x.get("description") or x.get("desc") or "")[:300],
        "provider": "moviebox",
    }


@app.get("/mb/search", tags=["MovieBox"])
@app.get("/moviebox/search", tags=["MovieBox"])
async def mb_search(q: str = Query(..., min_length=1), page: int = Query(1, ge=1), limit: int = Query(20, ge=1, le=50)):
    data = await _mb_request("/wefeed-mobile-bff/subject-api/search", {"keyword": q, "page": page, "perPage": limit})
    items = []
    for key in ("items", "list", "subjects", "data"):
        arr = data.get(key) if isinstance(data, dict) else None
        if isinstance(arr, list):
            items = [_mb_item(x) for x in arr if isinstance(x, dict)]
            break
        if isinstance(arr, dict) and isinstance(arr.get("items"), list):
            items = [_mb_item(x) for x in arr["items"] if isinstance(x, dict)]
            break
    return ok(query=q, page=page, count=len(items), items=items, provider="moviebox", level="primary")


@app.get("/mb/detail/{subject_id}", tags=["MovieBox"])
async def mb_detail(subject_id: str):
    data = await _mb_request("/wefeed-mobile-bff/subject-api/get", {"subjectId": subject_id})
    d = data.get("data") if isinstance(data.get("data"), dict) else data
    item = _mb_item(d if isinstance(d, dict) else {})
    return ok(item=item, data=d, provider="moviebox", level="primary")


@app.get("/mb/stream/{subject_id}", tags=["MovieBox"])
async def mb_stream(subject_id: str, se: int = Query(0), ep: int = Query(0), quality: str = Query("0")):
    data = await _mb_request(
        "/wefeed-mobile-bff/subject-api/play-info",
        {"subjectId": subject_id, "se": se, "ep": ep, "quality": quality},
    )
    streams = []
    payload = data.get("data") if isinstance(data.get("data"), dict) else data
    for key in ("streams", "list", "playList", "resources", "data"):
        arr = payload.get(key) if isinstance(payload, dict) else None
        if isinstance(arr, list):
            for s in arr:
                if not isinstance(s, dict):
                    continue
                url = s.get("url") or s.get("playUrl") or s.get("mpd") or s.get("streamUrl")
                if not url:
                    continue
                streams.append({
                    "url": url,
                    "quality": s.get("quality") or s.get("resolution") or s.get("label"),
                    "format": s.get("format") or s.get("type") or ("dash" if ".mpd" in str(url) else "mp4"),
                    "headers": s.get("headers") or {},
                    "cookie": (s.get("headers") or {}).get("Cookie") or s.get("cookie"),
                })
            break
    return ok(subject_id=subject_id, season=se, episode=ep, streams=streams, count=len(streams), provider="moviebox", level="primary")


@app.get("/mb/home", tags=["MovieBox"])
async def mb_home():
    for path in ("/wefeed-mobile-bff/operating/home", "/wefeed-mobile-bff/subject-api/rank-list"):
        try:
            data = await _mb_request(path)
            return ok(data=data, provider="moviebox", level="primary")
        except HTTPException:
            continue
    return fail("MovieBox home unavailable (region or path)")


@app.get("/mb/trending", tags=["MovieBox"])
async def mb_trending(page: int = Query(1, ge=1)):
    data = await _mb_request("/wefeed-mobile-bff/subject-api/rank-list", {"page": page, "perPage": 20})
    return ok(data=data, provider="moviebox", level="primary")


@app.get("/mb/movies", tags=["MovieBox"])
async def mb_movies(page: int = Query(1, ge=1)):
    data = await _mb_request("/wefeed-mobile-bff/subject-api/search", {"keyword": "a", "page": page, "perPage": 20, "subjectType": 1})
    return ok(data=data, provider="moviebox", level="primary")


@app.get("/mb/series", tags=["MovieBox"])
async def mb_series(page: int = Query(1, ge=1)):
    data = await _mb_request("/wefeed-mobile-bff/subject-api/search", {"keyword": "a", "page": page, "perPage": 20, "subjectType": 2})
    return ok(data=data, provider="moviebox", level="primary")

# =============================================================================
# 4KHDHub
# =============================================================================

FK_BASES = ["https://4khdhub.one", "https://4khdhub.link"]
FK_UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"


async def _fk_get(path: str = "/", params: Optional[dict] = None) -> tuple:
    last = None
    async with httpx.AsyncClient(timeout=20.0, follow_redirects=True) as client:
        for base in FK_BASES:
            try:
                r = await client.get(base.rstrip("/") + path, params=params, headers={"User-Agent": FK_UA, "Accept": "text/html"})
                if r.status_code == 200 and len(r.text) > 1000:
                    return base, r.text
                last = f"HTTP {r.status_code}"
            except Exception as e:
                last = str(e)[:60]
    raise HTTPException(502, f"4KHDHub: {last}")


def _fk_parse_cards(html: str, base: str) -> List[dict]:
    if not BeautifulSoup:
        return []
    soup = BeautifulSoup(html, "html.parser")
    items = []
    for a in soup.select("a.movie-card"):
        href = a.get("href") or ""
        title_el = a.select_one(".movie-card-title")
        title = title_el.get_text(strip=True) if title_el else a.get_text(strip=True)
        img = a.select_one("img")
        poster = (img.get("src") or img.get("data-src") or "") if img else ""
        if poster and poster.startswith("/"):
            poster = urljoin(base, poster)
        meta = a.select_one(".movie-card-meta")
        meta_t = meta.get_text(" ", strip=True) if meta else ""
        items.append({
            "id": href.strip("/").split("/")[-1] if href else title,
            "title": title,
            "url": urljoin(base, href),
            "path": href,
            "poster": poster,
            "meta": meta_t,
            "provider": "4khdhub",
        })
    return items


@app.get("/fk/home", tags=["4KHDHub"])
@app.get("/4k/home", tags=["4KHDHub"])
async def fk_home():
    base, html = await _fk_get("/")
    items = _fk_parse_cards(html, base)
    return ok(count=len(items), items=items, provider="4khdhub", level="primary", base=base)


@app.get("/fk/search", tags=["4KHDHub"])
@app.get("/4k/search", tags=["4KHDHub"])
async def fk_search(q: str = Query(..., min_length=1)):
    base, html = await _fk_get("/", {"s": q})
    items = _fk_parse_cards(html, base)
    return ok(query=q, count=len(items), items=items, provider="4khdhub", level="primary")


@app.get("/fk/detail", tags=["4KHDHub"])
@app.get("/4k/detail", tags=["4KHDHub"])
async def fk_detail(url: str = Query(...)):
    path = urlparse(url).path if url.startswith("http") else "/" + url.lstrip("/")
    base, html = await _fk_get(path)
    if not BeautifulSoup:
        return fail("beautifulsoup4 required")
    soup = BeautifulSoup(html, "html.parser")
    h1 = soup.select_one("h1")
    title = h1.get_text(strip=True) if h1 else path
    desc_el = soup.select_one(".content-section p.mt-4") or soup.select_one(".movie-tagline")
    desc = desc_el.get_text(strip=True) if desc_el else ""
    imdb_el = soup.select_one(".imdb-score")
    imdb = imdb_el.get_text(strip=True) if imdb_el else None
    mirrors = []
    for a in soup.select("a[href]"):
        href = a.get("href") or ""
        text = a.get_text(" ", strip=True)
        low = href.lower()
        if any(x in low for x in ("hubcloud", "hubdrive", "pixeldrain", "gofile", "gdflix", "filepress")):
            mirrors.append({"label": text[:80] or href, "url": href, "host": urlparse(href).netloc})
        if re.search(r"\.(mp4|mkv|m3u8)(\?|$)", low):
            mirrors.append({"label": text[:80] or "direct", "url": href, "host": "direct"})
    episodes = []
    for ep in soup.select("#episodes .episode-download-item, .episode-download-item, .download-item"):
        et = ep.select_one(".episode-file-title, .file-title")
        ep_title = et.get_text(strip=True) if et else ep.get_text(" ", strip=True)[:100]
        links = [{"label": a.get_text(strip=True)[:60], "url": a.get("href")} for a in ep.select("a[href]") if (a.get("href") or "").startswith("http")]
        if links:
            episodes.append({"title": ep_title, "links": links})
    return ok(title=title, description=desc, imdb=imdb, url=urljoin(base, path), mirrors=mirrors, episodes=episodes, provider="4khdhub", level="primary")


# =============================================================================
# Dramachi
# =============================================================================

DR_BASE = "https://api.nodeobjects.com/"
DR_IMG = "https://static.nodeobjects.com/thumbnail/"


async def _dr_get(params: dict) -> dict:
    async with httpx.AsyncClient(timeout=20.0) as client:
        r = await client.get(DR_BASE, params=params, headers={"User-Agent": FK_UA})
        if r.status_code != 200:
            raise HTTPException(502, f"Dramachi HTTP {r.status_code}")
        return r.json()


def _dr_item(x: dict) -> dict:
    thumb = x.get("thumb") or ""
    if thumb and not thumb.startswith("http"):
        thumb = DR_IMG + thumb
    return {
        "id": str(x.get("id") or ""),
        "title": html_lib.unescape(x.get("title") or x.get("common_title") or ""),
        "year": x.get("year"),
        "category": x.get("category"),
        "content": x.get("content"),
        "views": x.get("views"),
        "poster": thumb,
        "provider": "dramachi",
    }


@app.get("/dr/search", tags=["Dramachi"])
@app.get("/dramachi/search", tags=["Dramachi"])
async def dr_search(q: str = Query(..., min_length=1), page: int = Query(1, ge=1)):
    data = await _dr_get({"interface": "search", "q": q, "filter": "all", "page": str(page)})
    items = [_dr_item(x) for x in (data.get("data") or []) if isinstance(x, dict)]
    return ok(query=q, page=page, count=len(items), items=items, provider="dramachi", level="primary")


@app.get("/dr/detail/{title_id}", tags=["Dramachi"])
async def dr_detail(title_id: str):
    data = await _dr_get({"interface": "title_v2", "id": title_id})
    return ok(id=title_id, data=data, provider="dramachi", level="primary")


@app.get("/dr/episodes/{title_id}", tags=["Dramachi"])
async def dr_episodes(title_id: str, season: str = Query("1")):
    data = await _dr_get({"interface": "eplist", "season": season, "id": title_id})
    return ok(id=title_id, season=season, data=data, provider="dramachi", level="primary")


@app.get("/dr/file", tags=["Dramachi"])
async def dr_file(fid: str = Query(...), findex: str = Query("0")):
    data = await _dr_get({"interface": "getFile", "fid": fid, "findex": findex})
    return ok(fid=fid, data=data, provider="dramachi", level="primary")


# =============================================================================
# HubCloud / Pixeldrain resolve
# =============================================================================

@app.get("/tools/resolve", tags=["Tools"])
@app.get("/hub/resolve", tags=["Tools"])
async def tools_resolve(url: str = Query(..., min_length=8)):
    low = url.lower()
    headers = {"User-Agent": FK_UA, "Referer": url}
    mirrors = []
    async with httpx.AsyncClient(timeout=25.0, follow_redirects=True) as client:
        if "pixeldrain" in low:
            m = re.search(r"pixeldrain\.(?:com|net|dev)/(?:u|api/file)/([a-zA-Z0-9]+)", url)
            fid = m.group(1) if m else url.rstrip("/").split("/")[-1]
            mirrors.append({"label": "pixeldrain direct", "url": f"https://pixeldrain.com/api/file/{fid}?download"})
            mirrors.append({"label": "pixeldrain bypass", "url": f"https://pixeldrain-bypass.gamedrive.org/{fid}"})
            return ok(input=url, mirrors=mirrors, provider="pixeldrain", level="tool")
        try:
            r = await client.get(url, headers=headers)
            text = r.text
            final = str(r.url)
        except Exception as e:
            return fail(str(e)[:100], input=url)
        for m in re.finditer(r'href=["\'](https?://[^"\']+)["\']', text, re.I):
            href = m.group(1)
            hl = href.lower()
            if any(x in hl for x in ("download", "cdn", "workers.dev", "r2.dev", "mp4", "mkv", "hubcloud", "pixeldrain", "gofile")):
                mirrors.append({"url": href, "host": urlparse(href).netloc})
        seen, uniq = set(), []
        for m in mirrors:
            if m["url"] not in seen:
                seen.add(m["url"])
                uniq.append(m)
        return ok(input=url, final_url=final, mirrors=uniq[:40], provider="hub-resolve", level="tool")


# =============================================================================
# IPTV
# =============================================================================

DEFAULT_IPTV = "https://raw.githubusercontent.com/iptv-org/iptv/master/streams/us_pluto.m3u"


def _parse_m3u(text: str) -> List[dict]:
    channels = []
    name = logo = group = tid = None
    for line in text.splitlines():
        line = line.strip()
        if line.startswith("#EXTINF:"):
            logo_m = re.search(r'tvg-logo="([^"]*)"', line)
            group_m = re.search(r'group-title="([^"]*)"', line)
            id_m = re.search(r'tvg-id="([^"]*)"', line)
            logo = logo_m.group(1) if logo_m else None
            group = group_m.group(1) if group_m else None
            tid = id_m.group(1) if id_m else None
            name = line.split(",", 1)[-1].strip() if "," in line else "Channel"
        elif line and not line.startswith("#"):
            channels.append({"name": name or "Channel", "url": line, "logo": logo, "group": group, "tvg_id": tid, "provider": "iptv"})
            name = logo = group = tid = None
    return channels


@app.get("/iptv/parse", tags=["IPTV"])
async def iptv_parse(url: str = Query(...)):
    async with httpx.AsyncClient(timeout=30.0, follow_redirects=True) as client:
        r = await client.get(url, headers={"User-Agent": FK_UA})
        if r.status_code != 200:
            raise HTTPException(502, f"playlist HTTP {r.status_code}")
        channels = _parse_m3u(r.text)
    groups = sorted({c.get("group") or "Other" for c in channels})
    return ok(source=url, count=len(channels), groups=groups, channels=channels[:500], provider="iptv", level="live")


@app.get("/iptv/channels", tags=["IPTV"])
async def iptv_channels(url: Optional[str] = Query(None), group: Optional[str] = Query(None), q: Optional[str] = Query(None)):
    src = url or DEFAULT_IPTV
    data = await iptv_parse(src)
    channels = data.get("channels") or []
    if group:
        channels = [c for c in channels if (c.get("group") or "").lower() == group.lower()]
    if q:
        ql = q.lower()
        channels = [c for c in channels if ql in (c.get("name") or "").lower()]
    return ok(source=src, count=len(channels), channels=channels[:300], provider="iptv", level="live")


@app.get("/iptv/playlists", tags=["IPTV"])
async def iptv_playlists():
    return ok(playlists=[{"name": "Pluto TV US", "url": DEFAULT_IPTV}], provider="iptv", level="live")


# =============================================================================
# Aggregate + Meta
# =============================================================================

@app.get("/search", tags=["Aggregate"])
@app.get("/api/search", tags=["Aggregate"])
async def aggregate_search(q: str = Query(..., min_length=1)):
    results = {"moviebox": [], "4khdhub": [], "dramachi": [], "errors": {}}

    async def mb():
        try:
            r = await mb_search(q=q, page=1, limit=10)
            results["moviebox"] = r.get("items") or []
        except Exception as e:
            results["errors"]["moviebox"] = str(e)[:100]

    async def fk():
        try:
            r = await fk_search(q=q)
            results["4khdhub"] = r.get("items") or []
        except Exception as e:
            results["errors"]["4khdhub"] = str(e)[:100]

    async def dr():
        try:
            r = await dr_search(q=q, page=1)
            results["dramachi"] = r.get("items") or []
        except Exception as e:
            results["errors"]["dramachi"] = str(e)[:100]

    await asyncio.gather(mb(), fk(), dr(), return_exceptions=True)
    total = sum(len(results[k]) for k in ("moviebox", "4khdhub", "dramachi"))
    return ok(query=q, total=total, **results, level="aggregate")


@app.get("/health", tags=["Meta"])
async def health():
    return ok(
        version=VERSION,
        providers=[
            {"id": "moviebox", "label": "MovieBox", "level": "primary", "prefix": "/mb"},
            {"id": "4khdhub", "label": "4KHDHub", "level": "primary", "prefix": "/fk"},
            {"id": "dramachi", "label": "Dramachi", "level": "primary", "prefix": "/dr"},
            {"id": "iptv", "label": "IPTV / Live TV", "level": "live", "prefix": "/iptv"},
            {"id": "hub", "label": "HubCloud resolve", "level": "tool", "prefix": "/tools"},
        ],
    )


@app.get("/providers", tags=["Meta"])
async def providers():
    return await health()

SPA_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8"/>
<meta name="viewport" content="width=device-width,initial-scale=1"/>
<title>StreamHub</title>
<link href="https://fonts.googleapis.com/css2?family=Outfit:wght@300;400;600;700&display=swap" rel="stylesheet"/>
<style>
:root{--bg:#0b0f1a;--bg2:#12182a;--card:#161d31;--line:#243049;--txt:#e8eefc;--mut:#8b9bb8;--acc:#6c8cff;--acc2:#a78bfa;--rad:18px}
*{box-sizing:border-box;margin:0;padding:0}
body{font-family:Outfit,system-ui,sans-serif;background:radial-gradient(1200px 600px at 10% -10%,#1a2450 0%,transparent 50%),radial-gradient(900px 500px at 100% 0%,#2a1548 0%,transparent 45%),var(--bg);color:var(--txt);min-height:100vh}
.wrap{max-width:1100px;margin:0 auto;padding:20px 16px 80px}
header{display:flex;align-items:center;gap:14px;flex-wrap:wrap;margin-bottom:22px}
.logo{font-weight:700;font-size:1.35rem;background:linear-gradient(135deg,var(--acc),var(--acc2));-webkit-background-clip:text;background-clip:text;color:transparent}
.badge{font-size:.7rem;padding:4px 10px;border-radius:999px;background:rgba(108,140,255,.15);color:var(--acc);border:1px solid rgba(108,140,255,.3)}
nav{display:flex;gap:8px;flex-wrap:wrap;margin-left:auto}
nav button{background:var(--card);border:1px solid var(--line);color:var(--txt);padding:8px 14px;border-radius:999px;cursor:pointer;font:inherit;font-size:.85rem}
nav button:hover,nav button.on{background:linear-gradient(135deg,var(--acc),var(--acc2));border-color:transparent;color:#fff}
.search{display:flex;gap:10px;margin:18px 0 24px}
.search input{flex:1;background:var(--bg2);border:1px solid var(--line);border-radius:14px;padding:14px 16px;color:var(--txt);font:inherit;outline:none}
.search input:focus{border-color:var(--acc);box-shadow:0 0 0 3px rgba(108,140,255,.2)}
.search button{background:linear-gradient(135deg,var(--acc),var(--acc2));border:0;color:#fff;padding:0 22px;border-radius:14px;font:inherit;font-weight:600;cursor:pointer}
.grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(150px,1fr));gap:14px}
.card{background:var(--card);border:1px solid var(--line);border-radius:var(--rad);overflow:hidden;cursor:pointer;transition:transform .2s}
.card:hover{transform:translateY(-4px);border-color:var(--acc)}
.card img{width:100%;aspect-ratio:2/3;object-fit:cover;background:#0a0e18;display:block}
.card .meta{padding:10px 12px}
.card .t{font-size:.9rem;font-weight:600;line-height:1.3;display:-webkit-box;-webkit-line-clamp:2;-webkit-box-orient:vertical;overflow:hidden}
.card .s{font-size:.72rem;color:var(--mut);margin-top:4px}
.section{margin:28px 0}
.section h2{font-size:1.05rem;margin-bottom:12px}
.pill{font-size:.65rem;padding:2px 8px;border-radius:6px;background:rgba(167,139,250,.2);color:var(--acc2);margin-left:6px}
.panel{background:var(--card);border:1px solid var(--line);border-radius:var(--rad);padding:18px}
.list .row,.list a{display:block;padding:10px 12px;border-radius:10px}
.list a:hover,.list .row:hover{background:var(--bg2)}
.empty{text-align:center;color:var(--mut);padding:40px 10px}
.err{color:#f87171}
.foot{margin-top:40px;text-align:center;color:var(--mut);font-size:.8rem}
.loader{width:28px;height:28px;border:3px solid var(--line);border-top-color:var(--acc);border-radius:50%;animation:spin .7s linear infinite;margin:30px auto}
@keyframes spin{to{transform:rotate(360deg)}}
pre{background:#0a0e18;padding:12px;border-radius:12px;overflow:auto;font-size:.75rem;max-height:360px}
.btn{display:inline-block;margin-top:10px;padding:10px 16px;border-radius:12px;background:linear-gradient(135deg,var(--acc),var(--acc2));color:#fff;font-weight:600;font-size:.85rem}
</style>
</head>
<body>
<div class="wrap">
<header>
  <div class="logo">StreamHub</div>
  <span class="badge">v6 · shawon</span>
  <nav id="nav">
    <button data-v="home" class="on">Home</button>
    <button data-v="search">Search</button>
    <button data-v="fk">4K Hub</button>
    <button data-v="drama">Drama</button>
    <button data-v="iptv">Live TV</button>
    <button data-v="api">API</button>
  </nav>
</header>
<div class="search">
  <input id="q" placeholder="Search movies, series, dramas..." autocomplete="off"/>
  <button id="go">Search</button>
</div>
<main id="root"><div class="loader"></div></main>
<div class="foot">MovieBox · 4KHDHub · Dramachi · IPTV · made by shawon</div>
</div>
<script>
const root=document.getElementById('root');
const esc=s=>String(s||'').replace(/[&<>"']/g,c=>({ '&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;' }[c]));
const poster=u=>u||'data:image/svg+xml,'+encodeURIComponent('<svg xmlns="http://www.w3.org/2000/svg" width="200" height="300"><rect fill="#161d31" width="200" height="300"/></svg>');
async function api(p){const r=await fetch(p);if(!r.ok)throw new Error(await r.text());return r.json();}
function cards(items){if(!items||!items.length)return '<div class="empty">No results</div>';return '<div class="grid">'+items.map((it,i)=>`<div class="card" data-i="${i}"><img loading="lazy" src="${esc(poster(it.poster||it.thumb))}"/><div class="meta"><div class="t">${esc(it.title)}</div><div class="s">${esc(it.provider||it.meta||it.year||'')}</div></div></div>`).join('')+'</div>';}
async function home(){root.innerHTML='<div class="loader"></div>';let fk=[],dr=[];try{fk=(await api('/fk/home')).items||[];}catch(e){}try{dr=(await api('/dr/search?q=drama')).items||[];}catch(e){}
root.innerHTML=`<section class="section"><h2>4KHDHub<span class="pill">primary</span></h2>${cards(fk.slice(0,12))}</section><section class="section"><h2>Dramachi<span class="pill">asian</span></h2>${cards(dr.slice(0,12))}</section><section class="section"><h2>Providers</h2><div class="panel list"><div class="row"><b>MovieBox</b> /mb/* HMAC</div><div class="row"><b>4KHDHub</b> /fk/*</div><div class="row"><b>Dramachi</b> /dr/*</div><div class="row"><b>IPTV</b> /iptv/*</div><div class="row"><b>Tools</b> /tools/resolve</div></div></section>`;
const items=fk;root.querySelectorAll('.card').forEach(el=>{el.onclick=()=>{const it=items[el.dataset.i];if(it&&it.url)location.hash='#/fk?url='+encodeURIComponent(it.url);};});}
async function doSearch(q){root.innerHTML='<div class="loader"></div>';try{const j=await api('/search?q='+encodeURIComponent(q));root.innerHTML=`<section class="section"><h2>MovieBox</h2>${cards(j.moviebox)}</section><section class="section"><h2>4KHDHub</h2>${cards(j['4khdhub'])}</section><section class="section"><h2>Dramachi</h2>${cards(j.dramachi)}</section>`;}catch(e){root.innerHTML=`<div class="empty err">${esc(e.message)}</div>`;}}
async function fkView(){root.innerHTML='<div class="loader"></div>';try{const j=await api('/fk/home');root.innerHTML=`<section class="section"><h2>4KHDHub</h2>${cards(j.items)}</section>`;const items=j.items||[];root.querySelectorAll('.card').forEach(el=>{el.onclick=()=>{const it=items[el.dataset.i];if(it)location.hash='#/fk?url='+encodeURIComponent(it.url);};});}catch(e){root.innerHTML=`<div class="empty err">${esc(e.message)}</div>`;}}
async function fkDetail(url){root.innerHTML='<div class="loader"></div>';try{const j=await api('/fk/detail?url='+encodeURIComponent(url));const mir=(j.mirrors||[]).map(m=>`<a class="row" href="${esc(m.url)}" target="_blank">${esc(m.label||m.host||m.url)}</a>`).join('')||'<div class="empty">No mirrors</div>';root.innerHTML=`<div class="panel"><h2>${esc(j.title)}</h2><p style="color:var(--mut);margin:8px 0">${esc(j.description||'')}</p><a class="btn" href="${esc(j.url)}" target="_blank">Open source</a><h3 style="margin:16px 0 8px">Mirrors</h3><div class="list">${mir}</div></div>`;}catch(e){root.innerHTML=`<div class="empty err">${esc(e.message)}</div>`;}}
async function dramaView(){root.innerHTML='<div class="loader"></div>';try{const j=await api('/dr/search?q=korean');root.innerHTML=`<section class="section"><h2>Dramachi</h2>${cards(j.items)}</section>`;}catch(e){root.innerHTML=`<div class="empty err">${esc(e.message)}</div>`;}}
async function iptvView(){root.innerHTML='<div class="loader"></div>';try{const j=await api('/iptv/channels');const rows=(j.channels||[]).slice(0,80).map(c=>`<a class="row" href="${esc(c.url)}" target="_blank"><b>${esc(c.name)}</b><div style="font-size:.72rem;color:var(--mut)">${esc(c.group||'')}</div></a>`).join('');root.innerHTML=`<div class="panel"><h2>Live TV (${j.count||0})</h2><div class="list">${rows}</div></div>`;}catch(e){root.innerHTML=`<div class="empty err">${esc(e.message)}</div>`;}}
function apiView(){root.innerHTML=`<div class="panel"><h2>API</h2><p style="color:var(--mut);margin:8px 0">JSON always includes creator: "shawon". OpenAPI at <a href="/docs" style="color:var(--acc)">/docs</a></p><pre>GET /mb/search?q=
GET /mb/detail/{id}
GET /mb/stream/{id}?se=&ep=
GET /fk/home | /fk/search?q= | /fk/detail?url=
GET /dr/search?q= | /dr/detail/{id} | /dr/episodes/{id}
GET /iptv/channels | /iptv/parse?url=
GET /tools/resolve?url=
GET /search?q=
GET /health</pre></div>`;}
function router(){const h=(location.hash||'#/').slice(2);const [path,qs]=h.split('?');const params=Object.fromEntries(new URLSearchParams(qs||''));document.querySelectorAll('nav button').forEach(b=>b.classList.toggle('on',b.dataset.v===(path||'home')||(path==='fk'&&b.dataset.v==='fk')));
if(!path||path==='home')return home();if(path==='search'){const q=params.q||document.getElementById('q').value;if(q)return doSearch(q);root.innerHTML='<div class="empty">Type a query</div>';return;}
if(path==='fk'){if(params.url)return fkDetail(params.url);return fkView();}if(path==='drama')return dramaView();if(path==='iptv')return iptvView();if(path==='api')return apiView();root.innerHTML='<div class="empty">Not found</div>';}
document.getElementById('go').onclick=()=>{const q=document.getElementById('q').value.trim();if(q)location.hash='#/search?q='+encodeURIComponent(q);};
document.getElementById('q').onkeydown=e=>{if(e.key==='Enter')document.getElementById('go').click();};
document.querySelectorAll('nav button').forEach(b=>b.onclick=()=>{location.hash='#/'+b.dataset.v;});
window.addEventListener('hashchange',router);router();
</script>
</body>
</html>"""


@app.get("/site", response_class=HTMLResponse, tags=["Meta"])
async def site_spa():
    return HTMLResponse(SPA_HTML)


@app.get("/", response_class=HTMLResponse, tags=["Meta"], include_in_schema=False)
async def root_spa():
    return HTMLResponse(SPA_HTML)


if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run("api:app", host="0.0.0.0", port=port)
