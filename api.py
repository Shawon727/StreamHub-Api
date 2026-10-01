"""
StreamHub API v7.0.0 — MovieBox-TUI aligned
Creator: shawon

Providers (from https://github.com/mesamirh/MovieBox-TUI + extras):
  MovieBox   /mb/*     HMAC mobile BFF (search, detail, play-info DASH/MP4)
  4KHDHub    /fk/*     catalog + GreenMotors → HubCloud → PixelDrain/CDN
  Hub resolve/tools/*  HubCloud / HubDrive / PixelDrain direct CDN
  Dramachi   /dr/*     Asian drama (api.nodeobjects.com)
  IPTV       /iptv/*   public M3U channels
  HentaiCity /hc/*     search/recent/popular + HLS CDN (hls.hentaicity.com)

All JSON responses include: creator = "shawon"
"""

from __future__ import annotations

import asyncio
import base64
import hashlib
import hmac
import json
import os
import random
import re
import time
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import parse_qsl, quote, unquote, urljoin, urlparse

import httpx
from bs4 import BeautifulSoup
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, StreamingResponse

CREATOR = "shawon"
VERSION = "7.0.0"

app = FastAPI(
    title="StreamHub API",
    version=VERSION,
    description=(
        "Multi-provider streaming API aligned with MovieBox-TUI.\n"
        "MovieBox · 4KHDHub · HubCloud · Dramachi · IPTV · HentaiCity\n"
        "Creator: shawon"
    ),
    docs_url=None,
    redoc_url=None,
)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
)


def ok(data: Any = None, **extra) -> dict:
    out: Dict[str, Any] = {"creator": CREATOR, "ok": True}
    if data is not None:
        out["data"] = data
    out.update(extra)
    return out


def fail(msg: str, **extra) -> dict:
    out: Dict[str, Any] = {"creator": CREATOR, "ok": False, "error": msg}
    out.update(extra)
    return out


def _client(timeout: float = 20.0) -> httpx.AsyncClient:
    return httpx.AsyncClient(
        timeout=httpx.Timeout(timeout, connect=8.0),
        follow_redirects=True,
        headers={"User-Agent": UA, "Accept": "*/*"},
        limits=httpx.Limits(max_connections=40, max_keepalive_connections=20),
    )


# =============================================================================
# MOVIEBOX (HMAC) — from MovieBox-TUI crypto.rs / client.rs
# =============================================================================

MB_HOSTS = [
    "https://api6.aoneroom.com",
    "https://api5.aoneroom.com",
    "https://api4.aoneroom.com",
    "https://api4sg.aoneroom.com",
    "https://api3.aoneroom.com",
    "https://api6sg.aoneroom.com",
    "https://api.inmoviebox.com",
]
MB_SECRET = bytes(
    [
        0xEF, 0xA8, 0x91, 0x97, 0x4E, 0xEC, 0xD3, 0x14, 0x8D, 0xF6,
        0x3A, 0xA6, 0x11, 0x60, 0x2D, 0xEF, 0xD1, 0x01, 0x25, 0x9B,
        0xA5, 0x21, 0x02, 0x2C, 0x57, 0xAE, 0x05, 0x66, 0xBD, 0x8E,
    ]
)
_mb_token: Optional[str] = None
_mb_token_exp: float = 0.0
_mb_host_idx = 0


def _md5_hex(data: bytes) -> str:
    return hashlib.md5(data).hexdigest()


def _mb_sign(method: str, url: str, body: Optional[str] = None) -> Dict[str, str]:
    ts = int(time.time() * 1000)
    parsed = urlparse(url)
    params = sorted(parse_qsl(parsed.query, keep_blank_values=True))
    qs = "&".join(f"{k}={v}" for k, v in params)
    canon = parsed.path + (f"?{qs}" if qs else "")
    body_b = (body or "").encode()
    body_hash = _md5_hex(body_b) if body_b else ""
    body_len = str(len(body_b)) if body_b else ""
    accept, ctype = "application/json", "application/json"
    canonical = (
        f"{method.upper()}\n{accept}\n{ctype}\n{body_len}\n{ts}\n{body_hash}\n{canon}"
    )
    sig = base64.b64encode(
        hmac.new(MB_SECRET, canonical.encode(), hashlib.md5).digest()
    ).decode()
    rev = str(ts)[::-1]
    client_token = f"{ts},{_md5_hex(rev.encode())}"
    ip = f"{random.randint(1,223)}.{random.randint(0,255)}.{random.randint(0,255)}.{random.randint(1,254)}"
    return {
        "Accept": accept,
        "Content-Type": ctype,
        "User-Agent": "MovieBox/5.1.2 (Linux; Android 13)",
        "X-Client-Token": client_token,
        "X-Tr-Signature": f"{ts}|2|{sig}",
        "X-Client-Info": "Android/13 MovieBox/5.1.2",
        "X-Forwarded-For": ip,
        "X-Real-IP": ip,
    }


async def _mb_request(
    method: str, path: str, body: Optional[str] = None, token: Optional[str] = None
) -> dict:
    global _mb_host_idx
    last_err = "hosts exhausted"
    async with _client(18) as client:
        for i in range(len(MB_HOSTS)):
            base = MB_HOSTS[(_mb_host_idx + i) % len(MB_HOSTS)]
            url = base + path
            headers = _mb_sign(method, url, body)
            if token:
                headers["Authorization"] = f"Bearer {token}"
            try:
                if method.upper() == "GET":
                    r = await client.get(url, headers=headers)
                else:
                    r = await client.post(url, content=body or "", headers=headers)
                if r.status_code in (403, 406, 429, 500, 502, 503, 504):
                    last_err = f"{base} → {r.status_code}"
                    continue
                if r.status_code >= 400:
                    last_err = f"{base} → {r.status_code}: {r.text[:160]}"
                    continue
                _mb_host_idx = (_mb_host_idx + i) % len(MB_HOSTS)
                try:
                    return r.json()
                except Exception:
                    return {"raw": r.text[:2000]}
            except Exception as e:
                last_err = str(e)
                continue
    raise HTTPException(502, detail=fail("MovieBox upstream failed", detail=last_err))


async def _mb_session() -> str:
    global _mb_token, _mb_token_exp
    if _mb_token and time.time() < _mb_token_exp:
        return _mb_token
    data = await _mb_request("POST", "/wefeed-mobile-bff/user-api/visitor-login", "{}")
    token = (
        data.get("token")
        or (data.get("data") or {}).get("token")
        or data.get("accessToken")
    )
    if not token:
        raise HTTPException(502, detail=fail("MovieBox visitor-login: no token", raw=data))
    _mb_token = str(token)
    _mb_token_exp = time.time() + 3600
    return _mb_token


def _mb_streams_from_play(payload: dict) -> List[dict]:
    streams: List[dict] = []
    data = payload.get("data") if isinstance(payload.get("data"), dict) else payload
    arr = data.get("streams") or data.get("list") or []
    if not isinstance(arr, list):
        arr = []
    for s in arr:
        if not isinstance(s, dict):
            continue
        url = (
            s.get("url")
            or s.get("playUrl")
            or s.get("streamUrl")
            or s.get("mpd")
            or s.get("dashUrl")
        )
        if not url:
            continue
        streams.append(
            {
                "id": s.get("id") or s.get("streamId"),
                "url": url,
                "quality": s.get("resolution") or s.get("quality") or s.get("format"),
                "codec": s.get("codec") or s.get("videoCodec"),
                "format": s.get("formatType") or s.get("format") or "dash",
                "size": s.get("size") or s.get("fileSize"),
                "headers": s.get("headers") or {},
            }
        )
    return streams


@app.get("/mb/home", tags=["MovieBox"])
async def mb_home(page: int = 1, tab_id: str = "1"):
    tok = await _mb_session()
    path = f"/wefeed-mobile-bff/tab-operating?page={page}&tabId={tab_id}&version="
    data = await _mb_request("GET", path, token=tok)
    return ok(data, provider="moviebox", level="primary", endpoint="home")


@app.get("/mb/search", tags=["MovieBox"])
async def mb_search(q: str = Query(..., min_length=1), page: int = 1):
    tok = await _mb_session()
    body = json.dumps({"keyword": q, "page": page, "perPage": 20})
    data = await _mb_request(
        "POST", "/wefeed-mobile-bff/subject-api/search/v2", body, token=tok
    )
    return ok(data, provider="moviebox", level="primary", endpoint="search", query=q)


@app.get("/mb/detail/{subject_id}", tags=["MovieBox"])
async def mb_detail(subject_id: str):
    tok = await _mb_session()
    path = f"/wefeed-mobile-bff/subject-api/get?subjectId={subject_id}"
    data = await _mb_request("GET", path, token=tok)
    return ok(data, provider="moviebox", level="primary", endpoint="detail")


@app.get("/mb/seasons/{subject_id}", tags=["MovieBox"])
async def mb_seasons(subject_id: str):
    tok = await _mb_session()
    path = f"/wefeed-mobile-bff/subject-api/season-info?subjectId={subject_id}"
    data = await _mb_request("GET", path, token=tok)
    return ok(data, provider="moviebox", level="primary", endpoint="seasons")


@app.get("/mb/play/{subject_id}", tags=["MovieBox"])
async def mb_play(
    subject_id: str,
    se: Optional[int] = None,
    ep: Optional[int] = None,
):
    """Play-info streams (DASH/MP4). se/ep for series."""
    tok = await _mb_session()
    if se is not None and ep is not None:
        path = (
            f"/wefeed-mobile-bff/subject-api/play-info/v2"
            f"?subjectId={subject_id}&se={se}&ep={ep}"
        )
    else:
        path = f"/wefeed-mobile-bff/subject-api/play-info/v2?subjectId={subject_id}"
    data = await _mb_request("GET", path, token=tok)
    streams = _mb_streams_from_play(data)
    return ok(
        {
            "subject_id": subject_id,
            "season": se,
            "episode": ep,
            "streams": streams,
            "count": len(streams),
            "raw": data,
        },
        provider="moviebox",
        level="primary",
        endpoint="play",
        note="Prefer streams[].url as direct CDN/DASH when present",
    )


@app.get("/mb/resource/{subject_id}", tags=["MovieBox"])
async def mb_resource(
    subject_id: str,
    se: Optional[int] = None,
    ep: Optional[int] = None,
    page: int = 1,
    per_page: int = 20,
):
    tok = await _mb_session()
    if se is not None and ep is not None:
        path = (
            f"/wefeed-mobile-bff/subject-api/resource?subjectId={subject_id}"
            f"&se={se}&ep={ep}&page={page}&perPage={per_page}"
        )
    else:
        path = (
            f"/wefeed-mobile-bff/subject-api/resource?subjectId={subject_id}"
            f"&page={page}&perPage={per_page}"
        )
    data = await _mb_request("GET", path, token=tok)
    return ok(data, provider="moviebox", level="primary", endpoint="resource")


# =============================================================================
# 4KHDHub + HubCloud / GreenMotors / PixelDrain
# =============================================================================

FK_BASE = "https://4khdhub.one"


def _fk_cards(html: str) -> List[dict]:
    soup = BeautifulSoup(html, "html.parser")
    items = []
    for a in soup.select("a.movie-card"):
        href = a.get("href") or ""
        title_el = a.select_one(".movie-card-title")
        title = (title_el.get_text(strip=True) if title_el else a.get_text(strip=True)) or ""
        if not title or not href:
            continue
        full = urljoin(FK_BASE, href)
        img = a.select_one("img")
        poster = img.get("src") if img else None
        meta_el = a.select_one(".movie-card-meta")
        meta = meta_el.get_text(" ", strip=True) if meta_el else ""
        year_m = re.search(r"(19|20)\d{2}", meta)
        items.append(
            {
                "id": urlparse(full).path,
                "title": title,
                "url": full,
                "poster": poster,
                "meta": meta,
                "year": year_m.group(0) if year_m else None,
                "type": "series" if "-series-" in href else "movie",
            }
        )
    if not items:
        # fallback relative slug links
        for a in soup.select("a[href]"):
            href = a.get("href") or ""
            if not re.match(r"^/[a-z0-9-]+-\d+/?$", href):
                continue
            title = " ".join(a.get_text().split())
            if len(title) < 3:
                continue
            full = urljoin(FK_BASE, href)
            if any(x["url"] == full for x in items):
                continue
            items.append(
                {
                    "id": href,
                    "title": title,
                    "url": full,
                    "poster": None,
                    "meta": "",
                    "year": None,
                    "type": "series" if "series" in href else "movie",
                }
            )
    return items


def _fk_downloads(html: str) -> List[dict]:
    soup = BeautifulSoup(html, "html.parser")
    releases = []
    for item in soup.select(".download-item"):
        title_el = item.select_one(".file-title")
        title = title_el.get_text(" ", strip=True) if title_el else ""
        mirrors = []
        for a in item.select("a[href]"):
            href = a.get("href") or ""
            label = " ".join(a.get_text().split()) or "mirror"
            if href.startswith("http"):
                mirrors.append({"label": label, "url": href})
        if title or mirrors:
            quality = None
            for q in ("2160p", "1080p", "720p", "480p", "4K", "UHD"):
                if q.lower() in title.lower():
                    quality = q
                    break
            releases.append({"title": title, "quality": quality, "mirrors": mirrors})
    return releases


def _rot13(s: str) -> str:
    out = []
    for c in s:
        if "a" <= c <= "m" or "A" <= c <= "M":
            out.append(chr(ord(c) + 13))
        elif "n" <= c <= "z" or "N" <= c <= "Z":
            out.append(chr(ord(c) - 13))
        else:
            out.append(c)
    return "".join(out)


def _decode_greenmotors_payload(payload: str) -> Optional[str]:
    try:
        step1 = base64.b64decode(payload).decode("utf-8", "ignore")
        step2 = base64.b64decode(step1).decode("utf-8", "ignore")
        step3 = _rot13(step2)
        step4 = base64.b64decode(step3).decode("utf-8", "ignore")
        obj = json.loads(step4)
        target_b64 = obj.get("o")
        if not target_b64:
            return None
        return base64.b64decode(target_b64).decode("utf-8", "ignore")
    except Exception:
        return None


def _pixeldrain_api(url: str) -> Optional[str]:
    try:
        p = urlparse(url)
        if "pixeldrain." not in (p.hostname or ""):
            return None
        path = p.path
        if path.startswith("/u/"):
            fid = path[3:].strip("/")
        elif path.startswith("/api/file/"):
            fid = path[len("/api/file/") :].strip("/").split("?")[0]
        else:
            return None
        if not fid:
            return None
        host = p.hostname
        return f"https://{host}/api/file/{fid}?download"
    except Exception:
        return None


async def _resolve_hubcloud(drive_url: str) -> List[dict]:
    """HubCloud /drive/xxx → PixelDrain / googleusercontent style direct links."""
    results: List[dict] = []
    async with _client(25) as client:
        r = await client.get(drive_url, headers={"Referer": "https://4khdhub.one/"})
        html = r.text
        soup = BeautifulSoup(html, "html.parser")

        # resolver button
        resolver = None
        for a in soup.select(
            "a#download, a.btn-primary, a.btn-success, a.btn[href*='/download/'], "
            "a[href*='gamerxyt.com'], a[href*='hubcloud.php']"
        ):
            href = a.get("href") or ""
            if href.startswith("https://"):
                resolver = href
                break
        page_html = html
        if resolver:
            try:
                rr = await client.get(resolver, headers={"Referer": drive_url})
                page_html = rr.text
            except Exception:
                pass

        # pixeldrain in scripts
        for prefix in (
            "https://pixeldrain.dev/u/",
            "https://pixeldrain.com/u/",
            "https://pixeldrain.dev/api/file/",
            "https://pixeldrain.com/api/file/",
        ):
            idx = 0
            while True:
                pos = page_html.find(prefix, idx)
                if pos < 0:
                    break
                end = pos
                while end < len(page_html) and page_html[end] not in "\"' \t\n\r<>\\":
                    end += 1
                cand = page_html[pos:end]
                api = _pixeldrain_api(cand)
                if api and not any(x["url"] == api for x in results):
                    results.append(
                        {
                            "label": "PixelDrain",
                            "url": api,
                            "kind": "direct",
                            "note": "CDN direct via pixeldrain ?download",
                        }
                    )
                idx = end

        soup2 = BeautifulSoup(page_html, "html.parser")
        for a in soup2.select("a[href]"):
            href = a.get("href") or ""
            label = " ".join(a.get_text().split()) or "mirror"
            if not href.startswith("http"):
                continue
            low = href.lower()
            if any(
                x in low
                for x in (
                    "pixeldrain.",
                    "googleusercontent.com",
                    "workers.dev",
                    "gofile.",
                    "gamerxyt.",
                    ".mp4",
                    ".mkv",
                    ".m3u8",
                )
            ):
                api = _pixeldrain_api(href) or href
                if not any(x["url"] == api for x in results):
                    results.append(
                        {
                            "label": label[:80],
                            "url": api,
                            "kind": "direct" if "pixeldrain" in api or "googleusercontent" in api else "mirror",
                        }
                    )
            elif "hubcloud." in low and "/drive/" in low:
                if not any(x["url"] == href for x in results):
                    results.append({"label": "HubCloud", "url": href, "kind": "hubcloud"})

    return results


async def _resolve_greenmotors(gm_url: str) -> List[dict]:
    async with _client(25) as client:
        r = await client.get(
            gm_url,
            headers={"Referer": "https://4khdhub.one/", "Accept": "text/html"},
        )
        html = r.text
        if "Failed to decode" in html or len(html) < 50:
            return [
                {
                    "label": "GreenMotors",
                    "url": gm_url,
                    "kind": "intermediate",
                    "note": "Upstream decode failed on this IP — open in browser or retry",
                }
            ]
        target = None
        for m in re.finditer(
            r"s\(\s*['\"]o['\"]\s*,\s*['\"]([^'\"]+)['\"]", html
        ):
            target = _decode_greenmotors_payload(m.group(1))
            if target:
                break
        if not target:
            # try hubcloud in page
            for m in re.findall(r'https?://[^\s"\']+hubcloud[^\s"\']+', html):
                target = m
                break
        if not target:
            return [{"label": "GreenMotors", "url": gm_url, "kind": "intermediate"}]
        if "hubcloud." in target:
            return await _resolve_hubcloud(target)
        if "hubdrive." in target:
            return await _resolve_hubdrive(target)
        api = _pixeldrain_api(target) or target
        return [{"label": "Direct", "url": api, "kind": "direct"}]


async def _resolve_hubdrive(drive_url: str) -> List[dict]:
    async with _client(20) as client:
        r = await client.get(drive_url, headers={"Referer": "https://4khdhub.one/"})
        soup = BeautifulSoup(r.text, "html.parser")
        for a in soup.select("a[href]"):
            href = a.get("href") or ""
            if "hubcloud." in href and "/drive/" in href:
                return await _resolve_hubcloud(href)
    return [{"label": "HubDrive", "url": drive_url, "kind": "intermediate"}]


@app.get("/fk/home", tags=["4KHDHub"])
async def fk_home():
    async with _client() as client:
        r = await client.get(FK_BASE + "/")
        items = _fk_cards(r.text)
    return ok(items, provider="4khdhub", level="live", count=len(items), endpoint="home")


@app.get("/fk/search", tags=["4KHDHub"])
async def fk_search(q: str = Query(..., min_length=1)):
    async with _client() as client:
        r = await client.get(FK_BASE + "/", params={"s": q})
        items = _fk_cards(r.text)
    return ok(
        items,
        provider="4khdhub",
        level="live",
        count=len(items),
        endpoint="search",
        query=q,
    )


@app.get("/fk/category/{slug}", tags=["4KHDHub"])
async def fk_category(slug: str, page: int = 1):
    path = f"/category/{slug}/"
    if page > 1:
        path = f"/category/{slug}/page/{page}/"
    async with _client() as client:
        r = await client.get(urljoin(FK_BASE, path))
        items = _fk_cards(r.text)
    return ok(
        items,
        provider="4khdhub",
        level="live",
        count=len(items),
        endpoint="category",
        slug=slug,
        page=page,
    )


@app.get("/fk/detail", tags=["4KHDHub"])
async def fk_detail(path: str = Query(..., description="e.g. /hacksaw-ridge-movie-7809/")):
    url = path if path.startswith("http") else urljoin(FK_BASE, path)
    async with _client() as client:
        r = await client.get(url)
        html = r.text
    soup = BeautifulSoup(html, "html.parser")
    h1 = soup.select_one("h1")
    title = h1.get_text(strip=True) if h1 else None
    releases = _fk_downloads(html)
    return ok(
        {
            "title": title,
            "url": url,
            "releases": releases,
            "release_count": len(releases),
        },
        provider="4khdhub",
        level="live",
        endpoint="detail",
        note="Use /fk/stream or /tools/resolve on mirror URLs for direct CDN",
    )


@app.get("/fk/stream", tags=["4KHDHub"])
async def fk_stream(
    path: str = Query(..., description="detail path or full URL"),
    resolve: bool = Query(True, description="Resolve GreenMotors → HubCloud → CDN"),
):
    """Detail page + optional full resolve to PixelDrain / googleusercontent CDN."""
    url = path if path.startswith("http") else urljoin(FK_BASE, path)
    async with _client() as client:
        r = await client.get(url)
        html = r.text
    soup = BeautifulSoup(html, "html.parser")
    h1 = soup.select_one("h1")
    title = h1.get_text(strip=True) if h1 else None
    releases = _fk_downloads(html)

    direct: List[dict] = []
    if resolve:
        # resolve first few hub mirrors in parallel
        tasks = []
        for rel in releases[:6]:
            for m in rel.get("mirrors") or []:
                mu = m.get("url") or ""
                if "greenmotors." in mu or "greenmountmotors." in mu:
                    tasks.append((_resolve_greenmotors(mu), rel.get("title"), m.get("label")))
                elif "hubcloud." in mu:
                    tasks.append((_resolve_hubcloud(mu), rel.get("title"), m.get("label")))
                elif "hubdrive." in mu:
                    tasks.append((_resolve_hubdrive(mu), rel.get("title"), m.get("label")))
                elif "pixeldrain." in mu:
                    api = _pixeldrain_api(mu) or mu
                    direct.append(
                        {
                            "release": rel.get("title"),
                            "label": m.get("label"),
                            "url": api,
                            "kind": "direct",
                        }
                    )
        for coro, rtitle, label in tasks[:8]:
            try:
                resolved = await coro
                for item in resolved:
                    direct.append(
                        {
                            "release": rtitle,
                            "label": item.get("label") or label,
                            "url": item.get("url"),
                            "kind": item.get("kind"),
                            "note": item.get("note"),
                        }
                    )
            except Exception as e:
                direct.append(
                    {
                        "release": rtitle,
                        "label": label,
                        "url": None,
                        "kind": "error",
                        "note": str(e),
                    }
                )

    # unique by url
    seen = set()
    uniq = []
    for d in direct:
        u = d.get("url")
        if not u or u in seen:
            continue
        seen.add(u)
        uniq.append(d)

    return ok(
        {
            "title": title,
            "url": url,
            "releases": releases,
            "direct_streams": uniq,
            "direct_count": len(uniq),
        },
        provider="4khdhub",
        level="live",
        endpoint="stream",
        note="direct_streams prefer PixelDrain ?download and googleusercontent CDN",
    )


@app.get("/tools/resolve", tags=["Tools"])
async def tools_resolve(url: str = Query(..., description="HubCloud / HubDrive / GreenMotors / PixelDrain URL")):
    u = url.strip()
    low = u.lower()
    try:
        if "greenmotors." in low or "greenmountmotors." in low:
            links = await _resolve_greenmotors(u)
        elif "hubdrive." in low:
            links = await _resolve_hubdrive(u)
        elif "hubcloud." in low:
            links = await _resolve_hubcloud(u)
        elif "pixeldrain." in low:
            api = _pixeldrain_api(u) or u
            links = [{"label": "PixelDrain", "url": api, "kind": "direct"}]
        else:
            links = [{"label": "passthrough", "url": u, "kind": "unknown"}]
    except Exception as e:
        return fail(str(e), provider="tools", endpoint="resolve", input=u)
    return ok(
        {"input": u, "links": links, "count": len(links)},
        provider="tools",
        level="tool",
        endpoint="resolve",
    )


@app.get("/tools/pixeldrain", tags=["Tools"])
async def tools_pixeldrain(id: str = Query(..., description="PixelDrain file id")):
    for host in ("pixeldrain.com", "pixeldrain.dev"):
        url = f"https://{host}/api/file/{id}?download"
        return ok(
            {
                "id": id,
                "download_url": url,
                "info_url": f"https://{host}/api/file/{id}",
            },
            provider="tools",
            level="tool",
            endpoint="pixeldrain",
        )


# =============================================================================
# DRAMACHI
# =============================================================================

DR_BASE = "https://api.nodeobjects.com"
DR_IMG = "https://static.nodeobjects.com/thumbnail/"


@app.get("/dr/search", tags=["Dramachi"])
async def dr_search(q: str = Query(...), page: int = 1):
    async with _client() as client:
        r = await client.get(
            f"{DR_BASE}/",
            params={"interface": "search", "q": q, "filter": "all", "page": page},
        )
        try:
            data = r.json()
        except Exception:
            data = {"raw": r.text[:1500]}
    return ok(data, provider="dramachi", level="live", endpoint="search", query=q)


@app.get("/dr/home", tags=["Dramachi"])
async def dr_home(page: int = 1):
    async with _client() as client:
        r = await client.get(
            f"{DR_BASE}/",
            params={"interface": "home", "page": page},
        )
        try:
            data = r.json()
        except Exception:
            data = {"raw": r.text[:1500]}
    return ok(data, provider="dramachi", level="live", endpoint="home")


# =============================================================================
# IPTV
# =============================================================================

IPTV_SOURCES = [
    "https://iptv-org.github.io/iptv/index.m3u",
    "https://iptv-org.github.io/iptv/countries/bd.m3u",
    "https://iptv-org.github.io/iptv/countries/in.m3u",
]


def _parse_m3u(text: str) -> List[dict]:
    channels = []
    lines = text.splitlines()
    name, logo, group = None, None, None
    for line in lines:
        line = line.strip()
        if line.startswith("#EXTINF"):
            name = line.split(",")[-1].strip() if "," in line else "Channel"
            logo_m = re.search(r'tvg-logo="([^"]+)"', line)
            group_m = re.search(r'group-title="([^"]+)"', line)
            logo = logo_m.group(1) if logo_m else None
            group = group_m.group(1) if group_m else None
        elif line.startswith("http") and name:
            channels.append(
                {"name": name, "url": line, "logo": logo, "group": group}
            )
            name, logo, group = None, None, None
    return channels


@app.get("/iptv/channels", tags=["IPTV"])
async def iptv_channels(source: int = 0, q: Optional[str] = None, limit: int = 500):
    src = IPTV_SOURCES[max(0, min(source, len(IPTV_SOURCES) - 1))]
    async with _client(30) as client:
        r = await client.get(src)
        ch = _parse_m3u(r.text)
    if q:
        ql = q.lower()
        ch = [c for c in ch if ql in c["name"].lower() or ql in (c.get("group") or "").lower()]
    return ok(
        ch[:limit],
        provider="iptv",
        level="live",
        count=min(len(ch), limit),
        total=len(ch),
        source=src,
        endpoint="channels",
    )


# =============================================================================
# HENTAICITY — private adult section (HLS CDN)
# =============================================================================

HC_BASE = "https://www.hentaicity.com"


def _hc_list(html: str) -> List[dict]:
    soup = BeautifulSoup(html, "html.parser")
    items = []
    for a in soup.select("a[href*='/video/']"):
        href = a.get("href") or ""
        if "all-" in href or "popular" in href:
            continue
        # normalize click tracker links
        m = re.search(r"/video/([^/]+\.html)", href)
        if not m:
            continue
        slug = m.group(1)
        title = (a.get("title") or "").strip()
        if not title:
            title = " ".join(a.get_text().split())
        img = a.find("img")
        poster = None
        if img:
            poster = img.get("src") or img.get("data-src")
        full = href if href.startswith("http") else urljoin(HC_BASE, href)
        vid_id = slug.rsplit(".", 1)[0]
        if any(x.get("id") == vid_id for x in items):
            continue
        items.append(
            {
                "id": vid_id,
                "slug": slug,
                "title": title or vid_id,
                "url": full,
                "poster": poster,
            }
        )
    return items


def _hc_stream_from_html(html: str) -> List[dict]:
    sources = []
    for m in re.findall(
        r'https?://hls\.hentaicity\.com/[^"\'\s<>]+', html
    ):
        sources.append({"src": m, "format": "hls", "cdn": "hls.hentaicity.com"})
    for m in re.findall(r'https?://cdn\d*\.hentaicity\.com/[^"\'\s<>]+\.mp4[^"\'\s<>]*', html):
        sources.append({"src": m, "format": "mp4", "cdn": "cdn.hentaicity.com"})
    # dedupe
    seen = set()
    out = []
    for s in sources:
        if s["src"] in seen:
            continue
        seen.add(s["src"])
        out.append(s)
    return out


@app.get("/hc/recent", tags=["HentaiCity"])
async def hc_recent():
    async with _client() as client:
        r = await client.get(f"{HC_BASE}/videos/straight/all-recent.html")
        items = _hc_list(r.text)
    return ok(items, provider="hentaicity", level="private", count=len(items), endpoint="recent")


@app.get("/hc/popular", tags=["HentaiCity"])
async def hc_popular():
    async with _client() as client:
        r = await client.get(f"{HC_BASE}/videos/straight/all-popular.html")
        items = _hc_list(r.text)
    return ok(items, provider="hentaicity", level="private", count=len(items), endpoint="popular")


@app.get("/hc/search", tags=["HentaiCity"])
async def hc_search(q: str = Query(...)):
    async with _client() as client:
        r = await client.get(
            f"{HC_BASE}/search/",
            params={"q": q},
        )
        items = _hc_list(r.text)
    return ok(
        items,
        provider="hentaicity",
        level="private",
        count=len(items),
        endpoint="search",
        query=q,
    )


@app.get("/hc/watch", tags=["HentaiCity"])
async def hc_watch(
    id: Optional[str] = None,
    url: Optional[str] = None,
):
    """Resolve HLS master.m3u8 CDN for a HentaiCity video."""
    if not id and not url:
        raise HTTPException(400, detail=fail("id or url required"))
    if url:
        page = url
    else:
        # try common path patterns
        page = f"{HC_BASE}/video/{id}.html"
    async with _client() as client:
        r = await client.get(page, headers={"Referer": HC_BASE + "/"})
        if r.status_code >= 400 or not _hc_stream_from_html(r.text):
            # click tracker style
            if id:
                alt = f"{HC_BASE}/click/1-1/video/{id}.html"
                r = await client.get(alt, headers={"Referer": HC_BASE + "/"})
        html = r.text
    soup = BeautifulSoup(html, "html.parser")
    title_el = soup.select_one("h1") or soup.select_one("title")
    title = title_el.get_text(strip=True) if title_el else id
    sources = _hc_stream_from_html(html)
    return ok(
        {
            "id": id,
            "title": title,
            "page": str(r.url) if hasattr(r, "url") else page,
            "sources": sources,
            "count": len(sources),
        },
        provider="hentaicity",
        level="private",
        endpoint="watch",
        note="sources[].src is HLS CDN (hls.hentaicity.com) — play with VLC / hls.js",
    )


# =============================================================================
# AGGREGATE SEARCH
# =============================================================================

@app.get("/search", tags=["Aggregate"])
async def aggregate_search(q: str = Query(..., min_length=1)):
    results: Dict[str, Any] = {}

    async def mb():
        try:
            tok = await _mb_session()
            body = json.dumps({"keyword": q, "page": 1, "perPage": 10})
            data = await _mb_request(
                "POST", "/wefeed-mobile-bff/subject-api/search/v2", body, token=tok
            )
            results["moviebox"] = data
        except Exception as e:
            results["moviebox"] = {"error": str(e)}

    async def fk():
        try:
            async with _client() as client:
                r = await client.get(FK_BASE + "/", params={"s": q})
                results["4khdhub"] = _fk_cards(r.text)[:20]
        except Exception as e:
            results["4khdhub"] = {"error": str(e)}

    async def dr():
        try:
            async with _client() as client:
                r = await client.get(
                    f"{DR_BASE}/",
                    params={"interface": "search", "q": q, "filter": "all", "page": 1},
                )
                results["dramachi"] = r.json()
        except Exception as e:
            results["dramachi"] = {"error": str(e)}

    await asyncio.gather(mb(), fk(), dr())
    return ok(results, provider="aggregate", level="primary", endpoint="search", query=q)


@app.get("/health", tags=["Meta"])
async def health():
    return ok(
        {
            "version": VERSION,
            "providers": [
                "moviebox",
                "4khdhub",
                "hubcloud",
                "dramachi",
                "iptv",
                "hentaicity",
            ],
        }
    )


# =============================================================================
# MODERN DOCS UI + SPA shell
# =============================================================================

DOCS_HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8"/>
<meta name="viewport" content="width=device-width,initial-scale=1,viewport-fit=cover"/>
<title>StreamHub API · shawon</title>
<link rel="preconnect" href="https://fonts.googleapis.com"/>
<link href="https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;600&family=Outfit:wght@400;500;600;700&display=swap" rel="stylesheet"/>
<style>
:root{
  --bg:#07070c;--panel:#12121a;--panel2:#1a1a26;--line:#2a2a3a;
  --txt:#eef0f7;--mut:#9aa3b8;--acc:#7c5cff;--acc2:#00d4aa;
  --warn:#ffb020;--err:#ff5c7a;--radius:16px;
  --glow:0 0 40px rgba(124,92,255,.25);
}
*{box-sizing:border-box;margin:0;padding:0}
html,body{height:100%;background:var(--bg);color:var(--txt);font-family:Outfit,system-ui,sans-serif}
body{background:
  radial-gradient(1200px 600px at 10% -10%,rgba(124,92,255,.22),transparent 55%),
  radial-gradient(900px 500px at 100% 0%,rgba(0,212,170,.12),transparent 50%),
  var(--bg)}
a{color:var(--acc2);text-decoration:none}
.wrap{max-width:1120px;margin:0 auto;padding:24px 18px 80px}
header{display:flex;flex-wrap:wrap;gap:16px;align-items:center;justify-content:space-between;margin-bottom:28px}
.brand{display:flex;gap:14px;align-items:center}
.logo{width:48px;height:48px;border-radius:14px;background:linear-gradient(135deg,var(--acc),var(--acc2));
  display:grid;place-items:center;font-weight:700;font-size:18px;box-shadow:var(--glow)}
h1{font-size:1.45rem;font-weight:700;letter-spacing:-.02em}
.sub{color:var(--mut);font-size:.9rem;margin-top:2px}
.badges{display:flex;flex-wrap:wrap;gap:8px}
.badge{font-size:.72rem;padding:6px 10px;border-radius:999px;background:var(--panel2);border:1px solid var(--line);color:var(--mut)}
.badge.live{color:var(--acc2);border-color:rgba(0,212,170,.35)}
.badge.pri{color:var(--acc);border-color:rgba(124,92,255,.4)}
.search-bar{position:sticky;top:12px;z-index:20;margin-bottom:22px;
  background:rgba(18,18,26,.85);backdrop-filter:blur(12px);border:1px solid var(--line);
  border-radius:14px;padding:10px 14px;display:flex;gap:10px;align-items:center;box-shadow:var(--glow)}
.search-bar input{flex:1;background:transparent;border:0;outline:0;color:var(--txt);font:inherit;font-size:1rem}
.grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(280px,1fr));gap:14px}
.card{background:linear-gradient(180deg,var(--panel),var(--panel2));border:1px solid var(--line);
  border-radius:var(--radius);padding:16px;transition:transform .2s,border-color .2s,box-shadow .2s;cursor:pointer}
.card:hover{transform:translateY(-3px);border-color:rgba(124,92,255,.45);box-shadow:var(--glow)}
.card h3{font-size:1.05rem;margin-bottom:6px}
.card p{color:var(--mut);font-size:.88rem;line-height:1.45}
.tag{display:inline-block;margin-top:10px;font-size:.7rem;padding:4px 8px;border-radius:8px;background:rgba(124,92,255,.15);color:#cbbfff}
.panel{display:none;margin-top:18px;background:var(--panel);border:1px solid var(--line);border-radius:var(--radius);padding:18px;animation:fade .25s ease}
.panel.open{display:block}
@keyframes fade{from{opacity:0;transform:translateY(6px)}to{opacity:1;transform:none}}
.ep{font-family:JetBrains Mono,monospace;font-size:.85rem;color:var(--acc2);margin-bottom:8px}
.row{display:flex;flex-wrap:wrap;gap:8px;margin:10px 0}
input,select,button,textarea{font:inherit}
.field{flex:1;min-width:140px;background:var(--bg);border:1px solid var(--line);border-radius:10px;padding:10px 12px;color:var(--txt)}
button.btn{background:linear-gradient(135deg,var(--acc),#5a3fff);border:0;color:#fff;padding:10px 16px;border-radius:10px;font-weight:600;cursor:pointer}
button.btn:hover{filter:brightness(1.08)}
button.ghost{background:transparent;border:1px solid var(--line);color:var(--txt);padding:10px 14px;border-radius:10px;cursor:pointer}
pre{background:#0b0b12;border:1px solid var(--line);border-radius:12px;padding:14px;overflow:auto;max-height:420px;
  font-family:JetBrains Mono,monospace;font-size:.78rem;line-height:1.5;color:#d7dceb;margin-top:12px}
.hint{color:var(--mut);font-size:.82rem;margin-top:8px}
footer{margin-top:40px;text-align:center;color:var(--mut);font-size:.85rem}
.nav{display:flex;flex-wrap:wrap;gap:8px;margin-bottom:18px}
.nav a{padding:8px 12px;border-radius:10px;background:var(--panel2);border:1px solid var(--line);color:var(--mut);font-size:.85rem}
.nav a:hover{color:var(--txt);border-color:var(--acc)}
</style>
</head>
<body>
<div class="wrap">
<header>
  <div class="brand">
    <div class="logo">SH</div>
    <div>
      <h1>StreamHub API</h1>
      <div class="sub">MovieBox-TUI providers · direct CDN resolve · creator shawon</div>
    </div>
  </div>
  <div class="badges">
    <span class="badge pri">v7.0.0</span>
    <span class="badge live">MovieBox</span>
    <span class="badge live">4KHDHub</span>
    <span class="badge live">HubCloud</span>
    <span class="badge">Dramachi</span>
    <span class="badge">IPTV</span>
    <span class="badge">HentaiCity</span>
  </div>
</header>

<div class="nav">
  <a href="#mb">MovieBox</a>
  <a href="#fk">4KHDHub</a>
  <a href="#tools">Tools</a>
  <a href="#dr">Dramachi</a>
  <a href="#iptv">IPTV</a>
  <a href="#hc">HentaiCity</a>
  <a href="/health">Health</a>
</div>

<div class="search-bar">
  <span style="opacity:.6">⌕</span>
  <input id="filter" placeholder="Filter endpoints… e.g. play, resolve, stream" oninput="filterCards()"/>
</div>

<div class="grid" id="cards"></div>
<div id="panel" class="panel"></div>

<footer>
  Responses always include <code>creator: "shawon"</code> ·
  Aligned with <a href="https://github.com/mesamirh/MovieBox-TUI" target="_blank">MovieBox-TUI</a>
</footer>
</div>
<script>
const API = location.origin;
const ENDPOINTS = [
  {g:'MovieBox', id:'mb', level:'primary', path:'GET /mb/home', desc:'Home / operating tabs', try:'/mb/home?page=1'},
  {g:'MovieBox', id:'mb', level:'primary', path:'GET /mb/search', desc:'Search subjects', try:'/mb/search?q=avatar'},
  {g:'MovieBox', id:'mb', level:'primary', path:'GET /mb/detail/{id}', desc:'Title metadata', try:'/mb/detail/0'},
  {g:'MovieBox', id:'mb', level:'primary', path:'GET /mb/play/{id}', desc:'DASH/MP4 play-info streams', try:'/mb/play/0'},
  {g:'MovieBox', id:'mb', level:'primary', path:'GET /mb/seasons/{id}', desc:'Season list', try:'/mb/seasons/0'},
  {g:'MovieBox', id:'mb', level:'primary', path:'GET /mb/resource/{id}', desc:'Resources / extras', try:'/mb/resource/0'},
  {g:'4KHDHub', id:'fk', level:'live', path:'GET /fk/home', desc:'Latest cards', try:'/fk/home'},
  {g:'4KHDHub', id:'fk', level:'live', path:'GET /fk/search', desc:'Search catalog', try:'/fk/search?q=avatar'},
  {g:'4KHDHub', id:'fk', level:'live', path:'GET /fk/category/{slug}', desc:'Category page', try:'/fk/category/movies'},
  {g:'4KHDHub', id:'fk', level:'live', path:'GET /fk/detail', desc:'Releases + Hub mirrors', try:'/fk/detail?path=/hacksaw-ridge-movie-7809/'},
  {g:'4KHDHub', id:'fk', level:'live', path:'GET /fk/stream', desc:'Detail + resolve direct CDN', try:'/fk/stream?path=/hacksaw-ridge-movie-7809/&resolve=true'},
  {g:'Tools', id:'tools', level:'tool', path:'GET /tools/resolve', desc:'HubCloud / GreenMotors / PixelDrain → CDN', try:'/tools/resolve?url=https://hubcloud.ist/drive/xxx'},
  {g:'Tools', id:'tools', level:'tool', path:'GET /tools/pixeldrain', desc:'PixelDrain ?download URL', try:'/tools/pixeldrain?id=GauktM6T'},
  {g:'Dramachi', id:'dr', level:'live', path:'GET /dr/home', desc:'Drama home', try:'/dr/home'},
  {g:'Dramachi', id:'dr', level:'live', path:'GET /dr/search', desc:'Search dramas', try:'/dr/search?q=love'},
  {g:'IPTV', id:'iptv', level:'live', path:'GET /iptv/channels', desc:'Public M3U channels', try:'/iptv/channels?source=0&limit=50'},
  {g:'HentaiCity', id:'hc', level:'private', path:'GET /hc/recent', desc:'Recent videos', try:'/hc/recent'},
  {g:'HentaiCity', id:'hc', level:'private', path:'GET /hc/popular', desc:'Popular videos', try:'/hc/popular'},
  {g:'HentaiCity', id:'hc', level:'private', path:'GET /hc/search', desc:'Search', try:'/hc/search?q=anime'},
  {g:'HentaiCity', id:'hc', level:'private', path:'GET /hc/watch', desc:'HLS CDN sources', try:'/hc/watch?id=example'},
  {g:'Aggregate', id:'agg', level:'primary', path:'GET /search', desc:'Multi-provider search', try:'/search?q=avatar'},
  {g:'Meta', id:'meta', level:'tool', path:'GET /health', desc:'Health + version', try:'/health'},
];

function filterCards(){
  const q = (document.getElementById('filter').value||'').toLowerCase();
  document.querySelectorAll('.card').forEach(c=>{
    const t = c.dataset.t||'';
    c.style.display = !q || t.includes(q) ? '' : 'none';
  });
}

function render(){
  const root = document.getElementById('cards');
  root.innerHTML = ENDPOINTS.map((e,i)=>`
    <div class="card" data-t="${(e.g+' '+e.path+' '+e.desc).toLowerCase()}" onclick="openEp(${i})">
      <h3>${e.g}</h3>
      <p>${e.desc}</p>
      <div class="tag">${e.path}</div>
    </div>`).join('');
}

async function openEp(i){
  const e = ENDPOINTS[i];
  const panel = document.getElementById('panel');
  panel.className = 'panel open';
  panel.innerHTML = `
    <div class="ep">${e.path}</div>
    <p style="color:var(--mut);margin-bottom:8px">${e.desc}</p>
    <div class="row">
      <input class="field" id="tryurl" value="${API}${e.try}"/>
      <button class="btn" onclick="runTry()">Try it</button>
      <button class="ghost" onclick="navigator.clipboard.writeText(document.getElementById('tryurl').value)">Copy</button>
    </div>
    <div class="hint">4K direct links come from /fk/stream or /tools/resolve → PixelDrain / googleusercontent CDN</div>
    <pre id="out">Ready…</pre>`;
  panel.scrollIntoView({behavior:'smooth',block:'nearest'});
}

async function runTry(){
  const u = document.getElementById('tryurl').value;
  const out = document.getElementById('out');
  out.textContent = 'Loading…';
  const t0 = performance.now();
  try{
    const r = await fetch(u);
    const txt = await r.text();
    let pretty = txt;
    try{ pretty = JSON.stringify(JSON.parse(txt),null,2); }catch(_){}
    out.textContent = `${r.status} · ${Math.round(performance.now()-t0)}ms\n\n`+pretty.slice(0,12000);
  }catch(err){
    out.textContent = String(err);
  }
}

render();
</script>
</body>
</html>
"""


@app.get("/docs", response_class=HTMLResponse, include_in_schema=False)
async def docs_ui():
    return HTMLResponse(DOCS_HTML)


@app.get("/", response_class=HTMLResponse, include_in_schema=False)
async def root():
    return HTMLResponse(DOCS_HTML)


# Optional SPA placeholder for future frontend
@app.get("/site", response_class=HTMLResponse, include_in_schema=False)
async def site():
    return HTMLResponse(DOCS_HTML)
