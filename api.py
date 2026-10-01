# StreamHub API v7.1.0 — MovieBox-TUI aligned — creator: shawon
from __future__ import annotations

import asyncio
import base64
import hashlib
import hmac
import json
import random
import re
import time
import uuid
from typing import Any, Dict, List, Optional
from urllib.parse import parse_qsl, urljoin, urlparse

import httpx
from bs4 import BeautifulSoup
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse

CREATOR = "shawon"
VERSION = "7.1.0"

app = FastAPI(title="StreamHub API", version=VERSION, docs_url=None, redoc_url=None)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

UA_WEB = (
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


def _client(timeout: float = 22.0) -> httpx.AsyncClient:
    return httpx.AsyncClient(
        timeout=httpx.Timeout(timeout, connect=8.0),
        follow_redirects=True,
        headers={"User-Agent": UA_WEB, "Accept": "*/*"},
        limits=httpx.Limits(max_connections=40, max_keepalive_connections=20),
    )


# =============================================================================
# MOVIEBOX — fixed HMAC + x-client-info (DeviceId)
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
_mb_device_id = "".join(random.choice("0123456789abcdef") for _ in range(32))
_mb_gaid = str(uuid.uuid4())


def _md5_hex(data: bytes) -> str:
    return hashlib.md5(data).hexdigest()


def _mb_client_info() -> tuple:
    vc = 50020119
    ci = json.dumps(
        {
            "package_name": "com.community.oneroom",
            "version_name": "4.0.01.0813.03",
            "version_code": vc,
            "os": "android",
            "os_version": "13",
            "install_ch": "ps",
            "device_id": _mb_device_id,
            "install_store": "ps",
            "gaid": _mb_gaid,
            "brand": "Redmi",
            "model": "23078RKD5C",
            "system_language": "en",
            "net": "NETWORK_WIFI",
            "region": "US",
            "timezone": "Asia/Kolkata",
            "sp_code": "40401",
            "X-Play-Mode": "2",
        },
        separators=(",", ":"),
    )
    ua = (
        f"com.community.oneroom/{vc} (Linux; U; Android 13; en_US; "
        f"23078RKD5C; Build/TQ2A.230405.003; Cronet/135.0.7012.3)"
    )
    return ua, ci


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
    canonical = f"{method.upper()}\n{accept}\n{ctype}\n{body_len}\n{ts}\n{body_hash}\n{canon}"
    sig = base64.b64encode(
        hmac.new(MB_SECRET, canonical.encode(), hashlib.md5).digest()
    ).decode()
    rev = str(ts)[::-1]
    ua, ci = _mb_client_info()
    ip = f"103.241.{random.randint(1,254)}.{random.randint(1,254)}"
    return {
        "Accept": accept,
        "Content-Type": ctype,
        "Connection": "keep-alive",
        "User-Agent": ua,
        "x-client-token": f"{ts},{_md5_hex(rev.encode())}",
        "x-tr-signature": f"{ts}|2|{sig}",
        "x-client-info": ci,
        "x-client-status": "0",
        "x-forwarded-for": ip,
    }


def _mb_unwrap(payload: dict) -> dict:
    if isinstance(payload.get("data"), dict):
        return payload["data"]
    return payload


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
                    last_err = f"{base} -> {r.status_code}"
                    continue
                if r.status_code >= 400:
                    last_err = f"{base} -> {r.status_code}: {r.text[:160]}"
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
    inner = _mb_unwrap(data)
    token = inner.get("token") or data.get("token")
    if not token:
        raise HTTPException(502, detail=fail("MovieBox login failed", raw=data))
    _mb_token = str(token)
    _mb_token_exp = time.time() + 3500
    return _mb_token


def _dash_from_cookie(sign_cookie: str) -> Optional[dict]:
    """Build playable DASH URL + cookie from MovieBox signCookie field."""
    if not sign_cookie:
        return None
    m = re.search(r"urlprefix=([A-Za-z0-9+/=]+)", sign_cookie)
    if not m:
        return None
    raw = m.group(1)
    pad = raw + "=" * ((4 - len(raw) % 4) % 4)
    try:
        base = base64.b64decode(pad).decode("utf-8", "ignore")
    except Exception:
        return None
    if not base.startswith("http"):
        return None
    mpd = base.rstrip("/") + "/index.mpd"
    return {
        "url": mpd,
        "format": "DASH",
        "kind": "dash",
        "cookie": sign_cookie if sign_cookie.startswith("Edge-Cache-Cookie=") else f"Edge-Cache-Cookie={sign_cookie}",
        "base": base,
        "note": "Play with VLC/mpv; send Cookie header",
    }


def _mb_streams_from_play(payload: dict) -> List[dict]:
    streams: List[dict] = []
    data = _mb_unwrap(payload)
    arr = data.get("streams") or data.get("list") or []
    if not isinstance(arr, list):
        arr = []
    for s in arr:
        if not isinstance(s, dict):
            continue
        url = s.get("url") or s.get("playUrl") or s.get("streamUrl") or s.get("mpd") or s.get("dashUrl")
        cookie = s.get("signCookie") or s.get("cookie") or ""
        if url:
            streams.append(
                {
                    "id": s.get("id") or s.get("streamId"),
                    "url": url,
                    "quality": s.get("resolutions") or s.get("resolution") or s.get("quality"),
                    "codec": s.get("codecName") or s.get("codec"),
                    "format": s.get("format") or "MP4",
                    "size": s.get("size") or s.get("fileSize"),
                    "duration": s.get("duration"),
                    "cookie": cookie or None,
                    "kind": "mp4",
                }
            )
        dash = _dash_from_cookie(cookie)
        if dash:
            dash["id"] = s.get("id")
            dash["quality"] = s.get("resolutions") or s.get("resolution")
            dash["codec"] = s.get("codecName") or "h265"
            streams.append(dash)
    return streams


@app.get("/mb/home", tags=["MovieBox"])
async def mb_home(page: int = 1, tab_id: str = "1"):
    tok = await _mb_session()
    path = f"/wefeed-mobile-bff/tab-operating?page={page}&tabId={tab_id}&version="
    data = await _mb_request("GET", path, token=tok)
    return ok(_mb_unwrap(data), provider="moviebox", level="primary", endpoint="home", raw_code=data.get("code"))


@app.get("/mb/search", tags=["MovieBox"])
async def mb_search(q: str = Query(..., min_length=1), page: int = 1):
    tok = await _mb_session()
    body = json.dumps({"keyword": q, "page": page, "perPage": 20})
    data = await _mb_request("POST", "/wefeed-mobile-bff/subject-api/search/v2", body, token=tok)
    inner = _mb_unwrap(data)
    # flatten subjects for easier clients
    items = []
    for block in inner.get("results") or []:
        for s in block.get("subjects") or []:
            items.append(
                {
                    "subjectId": s.get("subjectId") or s.get("id"),
                    "title": s.get("title"),
                    "type": s.get("subjectType"),
                    "year": (s.get("releaseDate") or "")[:4],
                    "genre": s.get("genre"),
                    "cover": (s.get("cover") or {}).get("url") if isinstance(s.get("cover"), dict) else s.get("cover"),
                    "imdb": s.get("imdbRating") or s.get("rating"),
                }
            )
    return ok(
        {"items": items, "count": len(items), "pager": inner.get("pager"), "raw": inner},
        provider="moviebox",
        level="primary",
        endpoint="search",
        query=q,
    )


@app.get("/mb/detail/{subject_id}", tags=["MovieBox"])
async def mb_detail(subject_id: str):
    tok = await _mb_session()
    path = f"/wefeed-mobile-bff/subject-api/get?subjectId={subject_id}"
    data = await _mb_request("GET", path, token=tok)
    return ok(_mb_unwrap(data), provider="moviebox", level="primary", endpoint="detail")


@app.get("/mb/seasons/{subject_id}", tags=["MovieBox"])
async def mb_seasons(subject_id: str):
    tok = await _mb_session()
    path = f"/wefeed-mobile-bff/subject-api/season-info?subjectId={subject_id}"
    data = await _mb_request("GET", path, token=tok)
    return ok(_mb_unwrap(data), provider="moviebox", level="primary", endpoint="seasons")


@app.get("/mb/play/{subject_id}", tags=["MovieBox"])
async def mb_play(subject_id: str, se: Optional[int] = None, ep: Optional[int] = None):
    tok = await _mb_session()
    if se is not None and ep is not None:
        path = f"/wefeed-mobile-bff/subject-api/play-info/v2?subjectId={subject_id}&se={se}&ep={ep}"
    else:
        path = f"/wefeed-mobile-bff/subject-api/play-info/v2?subjectId={subject_id}"
    data = await _mb_request("GET", path, token=tok)
    inner = _mb_unwrap(data)
    streams = _mb_streams_from_play(data)
    return ok(
        {
            "subject_id": subject_id,
            "season": se,
            "episode": ep,
            "title": inner.get("title"),
            "streams": streams,
            "count": len(streams),
            "displayResolutions": inner.get("displayResolutions"),
        },
        provider="moviebox",
        level="primary",
        endpoint="play",
        note="Use streams with kind=dash + Cookie header for real multi-quality; kind=mp4 may be single HEVC file",
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
        path = f"/wefeed-mobile-bff/subject-api/resource?subjectId={subject_id}&se={se}&ep={ep}&page={page}&perPage={per_page}"
    else:
        path = f"/wefeed-mobile-bff/subject-api/resource?subjectId={subject_id}&page={page}&perPage={per_page}"
    data = await _mb_request("GET", path, token=tok)
    inner = _mb_unwrap(data)
    links = []
    for item in inner.get("list") or []:
        link = item.get("resourceLink") or item.get("url")
        if link:
            links.append(
                {
                    "title": item.get("title"),
                    "url": link,
                    "size": item.get("size"),
                    "episode": item.get("episode"),
                    "resourceId": item.get("resourceId"),
                    "codec": item.get("codecName"),
                }
            )
    return ok(
        {"links": links, "count": len(links), "pager": inner.get("pager"), "raw": inner},
        provider="moviebox",
        level="primary",
        endpoint="resource",
    )


# =============================================================================
# 4KHDHub + resolve tools
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
        return f"https://{p.hostname}/api/file/{fid}?download"
    except Exception:
        return None


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


async def _resolve_hubcloud(drive_url: str) -> List[dict]:
    results: List[dict] = []
    async with _client(28) as client:
        r = await client.get(drive_url, headers={"Referer": "https://4khdhub.one/"})
        html = r.text
        soup = BeautifulSoup(html, "html.parser")
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
                    results.append({"label": "PixelDrain", "url": api, "kind": "direct"})
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
                    ".mp4",
                    ".mkv",
                    ".m3u8",
                )
            ):
                api = _pixeldrain_api(href) or href
                if not any(x["url"] == api for x in results):
                    kind = "direct" if ("pixeldrain" in api or "googleusercontent" in api) else "mirror"
                    results.append({"label": label[:80], "url": api, "kind": kind})
    return results


async def _resolve_hubdrive(drive_url: str) -> List[dict]:
    async with _client(20) as client:
        r = await client.get(drive_url, headers={"Referer": "https://4khdhub.one/"})
        for a in BeautifulSoup(r.text, "html.parser").select("a[href]"):
            href = a.get("href") or ""
            if "hubcloud." in href and "/drive/" in href:
                return await _resolve_hubcloud(href)
    return [{"label": "HubDrive", "url": drive_url, "kind": "intermediate"}]


async def _resolve_greenmotors(gm_url: str) -> List[dict]:
    async with _client(25) as client:
        r = await client.get(
            gm_url,
            headers={
                "Referer": "https://4khdhub.one/",
                "Accept": "text/html,application/xhtml+xml",
                "User-Agent": UA_WEB,
            },
        )
        html = r.text
        if "Failed to decode" in html or len(html) < 80:
            return [
                {
                    "label": "GreenMotors",
                    "url": gm_url,
                    "kind": "intermediate",
                    "note": "Gateway blocked this IP — open mirror in browser or pass a hubcloud.ist/drive URL to /tools/resolve",
                }
            ]
        target = None
        for m in re.finditer(r"s\(\s*['\"]o['\"]\s*,\s*['\"]([^'\"]+)['\"]", html):
            target = _decode_greenmotors_payload(m.group(1))
            if target:
                break
        if not target:
            for m in re.findall(r"https?://[^\s\"']+hubcloud[^\s\"']+", html):
                target = m
                break
        if not target:
            return [{"label": "GreenMotors", "url": gm_url, "kind": "intermediate"}]
        if "hubcloud." in target:
            return await _resolve_hubcloud(target)
        if "hubdrive." in target:
            return await _resolve_hubdrive(target)
        return [{"label": "Direct", "url": _pixeldrain_api(target) or target, "kind": "direct"}]


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
    return ok(items, provider="4khdhub", level="live", count=len(items), endpoint="search", query=q)


@app.get("/fk/category/{slug}", tags=["4KHDHub"])
async def fk_category(slug: str, page: int = 1):
    path = f"/category/{slug}/" if page <= 1 else f"/category/{slug}/page/{page}/"
    async with _client() as client:
        r = await client.get(urljoin(FK_BASE, path))
        items = _fk_cards(r.text)
    return ok(items, provider="4khdhub", level="live", count=len(items), endpoint="category", slug=slug, page=page)


@app.get("/fk/detail", tags=["4KHDHub"])
async def fk_detail(path: str = Query(...)):
    url = path if path.startswith("http") else urljoin(FK_BASE, path)
    async with _client() as client:
        r = await client.get(url)
        html = r.text
    soup = BeautifulSoup(html, "html.parser")
    h1 = soup.select_one("h1")
    title = h1.get_text(strip=True) if h1 else None
    releases = _fk_downloads(html)
    return ok(
        {"title": title, "url": url, "releases": releases, "release_count": len(releases)},
        provider="4khdhub",
        level="live",
        endpoint="detail",
    )


@app.get("/fk/stream", tags=["4KHDHub"])
async def fk_stream(path: str = Query(...), resolve: bool = Query(True)):
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
        tasks = []
        for rel in releases[:8]:
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
                    direct.append({"release": rel.get("title"), "label": m.get("label"), "url": api, "kind": "direct"})
        for coro, rtitle, label in tasks[:10]:
            try:
                for item in await coro:
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
                direct.append({"release": rtitle, "label": label, "url": None, "kind": "error", "note": str(e)})
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
            "how_to_get_cdn": (
                "1) Pick a HubCloud mirror from releases if present. "
                "2) Call /tools/resolve?url=HUBCLOUD_DRIVE_URL. "
                "3) Use PixelDrain ?download or googleusercontent URL returned."
            ),
        },
        provider="4khdhub",
        level="live",
        endpoint="stream",
    )


@app.get("/tools/resolve", tags=["Tools"])
async def tools_resolve(url: str = Query(...)):
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
            links = [{"label": "PixelDrain", "url": _pixeldrain_api(u) or u, "kind": "direct"}]
        else:
            links = [{"label": "passthrough", "url": u, "kind": "unknown"}]
    except Exception as e:
        return fail(str(e), provider="tools", endpoint="resolve", input=u)
    return ok({"input": u, "links": links, "count": len(links)}, provider="tools", level="tool", endpoint="resolve")


@app.get("/tools/pixeldrain", tags=["Tools"])
async def tools_pixeldrain(id: str = Query(...)):
    return ok(
        {
            "id": id,
            "download_url": f"https://pixeldrain.com/api/file/{id}?download",
            "info_url": f"https://pixeldrain.com/api/file/{id}",
            "alt": f"https://pixeldrain.dev/api/file/{id}?download",
        },
        provider="tools",
        level="tool",
        endpoint="pixeldrain",
    )


# =============================================================================
# Dramachi / IPTV / HentaiCity
# =============================================================================

DR_BASE = "https://api.nodeobjects.com"


@app.get("/dr/search", tags=["Dramachi"])
async def dr_search(q: str = Query(...), page: int = 1):
    async with _client() as client:
        r = await client.get(f"{DR_BASE}/", params={"interface": "search", "q": q, "filter": "all", "page": page})
        try:
            data = r.json()
        except Exception:
            data = {"raw": r.text[:1500]}
    return ok(data, provider="dramachi", level="live", endpoint="search", query=q)


@app.get("/dr/home", tags=["Dramachi"])
async def dr_home(page: int = 1):
    async with _client() as client:
        r = await client.get(f"{DR_BASE}/", params={"interface": "home", "page": page})
        try:
            data = r.json()
        except Exception:
            data = {"raw": r.text[:1500]}
    return ok(data, provider="dramachi", level="live", endpoint="home")


IPTV_SOURCES = [
    "https://iptv-org.github.io/iptv/index.m3u",
    "https://iptv-org.github.io/iptv/countries/bd.m3u",
    "https://iptv-org.github.io/iptv/countries/in.m3u",
]


def _parse_m3u(text: str) -> List[dict]:
    channels = []
    name = logo = group = None
    for line in text.splitlines():
        line = line.strip()
        if line.startswith("#EXTINF"):
            name = line.split(",")[-1].strip() if "," in line else "Channel"
            logo_m = re.search(r'tvg-logo="([^"]+)"', line)
            group_m = re.search(r'group-title="([^"]+)"', line)
            logo = logo_m.group(1) if logo_m else None
            group = group_m.group(1) if group_m else None
        elif line.startswith("http") and name:
            channels.append({"name": name, "url": line, "logo": logo, "group": group})
            name = logo = group = None
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
    return ok(ch[:limit], provider="iptv", level="live", count=min(len(ch), limit), total=len(ch), source=src, endpoint="channels")


HC_BASE = "https://www.hentaicity.com"


def _hc_list(html: str) -> List[dict]:
    soup = BeautifulSoup(html, "html.parser")
    items = []
    for a in soup.select("a[href*='/video/']"):
        href = a.get("href") or ""
        if "all-" in href or "popular" in href:
            continue
        m = re.search(r"/video/([^/]+\.html)", href)
        if not m:
            continue
        slug = m.group(1)
        title = (a.get("title") or "").strip() or " ".join(a.get_text().split())
        img = a.find("img")
        poster = (img.get("src") or img.get("data-src")) if img else None
        full = href if href.startswith("http") else urljoin(HC_BASE, href)
        vid_id = slug.rsplit(".", 1)[0]
        if any(x.get("id") == vid_id for x in items):
            continue
        items.append({"id": vid_id, "slug": slug, "title": title or vid_id, "url": full, "poster": poster})
    return items


def _hc_stream_from_html(html: str) -> List[dict]:
    sources = []
    for m in re.findall(r'https?://hls\.hentaicity\.com/[^"\'\s<>]+', html):
        sources.append({"src": m, "format": "hls", "cdn": "hls.hentaicity.com"})
    for m in re.findall(r'https?://cdn\d*\.hentaicity\.com/[^"\'\s<>]+\.mp4[^"\'\s<>]*', html):
        sources.append({"src": m, "format": "mp4", "cdn": "cdn.hentaicity.com"})
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
        r = await client.get(f"{HC_BASE}/search/", params={"q": q})
        items = _hc_list(r.text)
    return ok(items, provider="hentaicity", level="private", count=len(items), endpoint="search", query=q)


@app.get("/hc/watch", tags=["HentaiCity"])
async def hc_watch(id: Optional[str] = None, url: Optional[str] = None):
    if not id and not url:
        raise HTTPException(400, detail=fail("id or url required"))
    page = url if url else f"{HC_BASE}/video/{id}.html"
    async with _client() as client:
        r = await client.get(page, headers={"Referer": HC_BASE + "/"})
        if r.status_code >= 400 or not _hc_stream_from_html(r.text):
            if id:
                r = await client.get(f"{HC_BASE}/click/1-1/video/{id}.html", headers={"Referer": HC_BASE + "/"})
        html = r.text
    soup = BeautifulSoup(html, "html.parser")
    title_el = soup.select_one("h1") or soup.select_one("title")
    title = title_el.get_text(strip=True) if title_el else id
    sources = _hc_stream_from_html(html)
    return ok(
        {"id": id, "title": title, "page": str(r.url), "sources": sources, "count": len(sources)},
        provider="hentaicity",
        level="private",
        endpoint="watch",
    )


@app.get("/search", tags=["Aggregate"])
async def aggregate_search(q: str = Query(..., min_length=1)):
    results: Dict[str, Any] = {}

    async def mb():
        try:
            tok = await _mb_session()
            body = json.dumps({"keyword": q, "page": 1, "perPage": 10})
            data = await _mb_request("POST", "/wefeed-mobile-bff/subject-api/search/v2", body, token=tok)
            results["moviebox"] = _mb_unwrap(data)
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
                r = await client.get(f"{DR_BASE}/", params={"interface": "search", "q": q, "filter": "all", "page": 1})
                results["dramachi"] = r.json()
        except Exception as e:
            results["dramachi"] = {"error": str(e)}

    await asyncio.gather(mb(), fk(), dr())
    return ok(results, provider="aggregate", level="primary", endpoint="search", query=q)


@app.get("/health", tags=["Meta"])
async def health():
    return ok({"version": VERSION, "providers": ["moviebox", "4khdhub", "hubcloud", "dramachi", "iptv", "hentaicity"]})


# =============================================================================
# DOCS UI — modern redesign
# =============================================================================

DOCS_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8"/>
<meta name="viewport" content="width=device-width,initial-scale=1,viewport-fit=cover"/>
<title>StreamHub · API Docs</title>
<link rel="preconnect" href="https://fonts.googleapis.com"/>
<link href="https://fonts.googleapis.com/css2?family=DM+Sans:wght@400;500;600;700&family=IBM+Plex+Mono:wght@400;500&display=swap" rel="stylesheet"/>
<style>
:root{
  --bg:#05060a; --elev:#0c0e16; --card:#12151f; --line:rgba(255,255,255,.08);
  --txt:#f2f4fb; --mut:#8b93a7; --acc:#6c5ce7; --acc2:#00cec9; --ok:#00b894; --bad:#ff7675;
  --r:18px; --shadow:0 20px 50px rgba(0,0,0,.45);
}
*{box-sizing:border-box;margin:0;padding:0}
body{font-family:'DM Sans',system-ui,sans-serif;background:var(--bg);color:var(--txt);min-height:100vh;
background-image:
  radial-gradient(ellipse 80% 50% at 20% -20%, rgba(108,92,231,.35), transparent),
  radial-gradient(ellipse 60% 40% at 100% 0%, rgba(0,206,201,.18), transparent),
  linear-gradient(180deg,#05060a 0%,#080a12 100%)}
a{color:var(--acc2)}
.shell{max-width:1180px;margin:0 auto;padding:20px 16px 64px}
.top{display:flex;flex-wrap:wrap;align-items:center;justify-content:space-between;gap:16px;margin-bottom:22px}
.brand{display:flex;gap:14px;align-items:center}
.mark{width:52px;height:52px;border-radius:16px;background:linear-gradient(135deg,#6c5ce7,#a29bfe 40%,#00cec9);
  display:grid;place-items:center;font-weight:700;font-size:1.1rem;box-shadow:0 8px 30px rgba(108,92,231,.4)}
h1{font-size:1.55rem;font-weight:700;letter-spacing:-.03em}
.sub{color:var(--mut);font-size:.9rem;margin-top:2px}
.pills{display:flex;flex-wrap:wrap;gap:8px}
.pill{font-size:.72rem;font-weight:600;padding:7px 12px;border-radius:999px;background:rgba(255,255,255,.04);border:1px solid var(--line);color:var(--mut)}
.pill.on{color:#c3b7ff;border-color:rgba(108,92,231,.5);background:rgba(108,92,231,.12)}
.pill.live{color:#7dffe8;border-color:rgba(0,206,201,.4)}
.stats{display:grid;grid-template-columns:repeat(4,1fr);gap:10px;margin-bottom:20px}
@media(max-width:700px){.stats{grid-template-columns:repeat(2,1fr)}}
.stat{background:var(--card);border:1px solid var(--line);border-radius:14px;padding:14px 16px}
.stat b{display:block;font-size:1.35rem;font-weight:700}.stat span{color:var(--mut);font-size:.78rem}
.search{position:sticky;top:10px;z-index:30;margin-bottom:18px;display:flex;align-items:center;gap:10px;
  background:rgba(12,14,22,.92);backdrop-filter:blur(16px);border:1px solid var(--line);border-radius:14px;
  padding:12px 16px;box-shadow:var(--shadow)}
.search input{flex:1;border:0;outline:0;background:transparent;color:var(--txt);font:inherit;font-size:1rem}
.tabs{display:flex;flex-wrap:wrap;gap:8px;margin-bottom:16px}
.tab{padding:8px 14px;border-radius:12px;border:1px solid var(--line);background:var(--elev);color:var(--mut);font-size:.85rem;font-weight:500;cursor:pointer}
.tab.active,.tab:hover{color:var(--txt);border-color:rgba(108,92,231,.5);background:rgba(108,92,231,.12)}
.grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(270px,1fr));gap:12px}
.card{background:linear-gradient(165deg,rgba(255,255,255,.04),rgba(255,255,255,.01));
  border:1px solid var(--line);border-radius:var(--r);padding:16px;cursor:pointer;transition:.2s cubic-bezier(.2,.8,.2,1)}
.card:hover{transform:translateY(-4px);border-color:rgba(108,92,231,.45);box-shadow:0 12px 40px rgba(108,92,231,.15)}
.card .g{font-size:.72rem;font-weight:600;color:var(--acc2);text-transform:uppercase;letter-spacing:.06em;margin-bottom:6px}
.card h3{font-size:.98rem;font-weight:600;margin-bottom:6px}
.card p{color:var(--mut);font-size:.84rem;line-height:1.45}
.method{display:inline-block;margin-top:10px;font-family:'IBM Plex Mono',monospace;font-size:.72rem;
  padding:4px 8px;border-radius:8px;background:rgba(0,206,201,.1);color:#7dffe8}
.drawer{display:none;margin-top:18px;background:var(--card);border:1px solid var(--line);border-radius:20px;padding:20px;box-shadow:var(--shadow)}
.drawer.open{display:block;animation:up .28s ease}
@keyframes up{from{opacity:0;transform:translateY(10px)}to{opacity:1;transform:none}}
.ep{font-family:'IBM Plex Mono',monospace;font-size:.88rem;color:#a29bfe;margin-bottom:8px}
.row{display:flex;flex-wrap:wrap;gap:8px;margin:12px 0}
.field{flex:1;min-width:160px;background:var(--bg);border:1px solid var(--line);border-radius:12px;padding:12px 14px;color:var(--txt);font:inherit}
.btn{background:linear-gradient(135deg,#6c5ce7,#5a4bd1);border:0;color:#fff;padding:12px 18px;border-radius:12px;font-weight:600;cursor:pointer;font:inherit}
.btn:hover{filter:brightness(1.08)}.ghost{background:transparent;border:1px solid var(--line);color:var(--txt);padding:12px 14px;border-radius:12px;cursor:pointer;font:inherit}
pre{background:#07080f;border:1px solid var(--line);border-radius:14px;padding:14px;overflow:auto;max-height:440px;
  font-family:'IBM Plex Mono',monospace;font-size:.78rem;line-height:1.55;color:#d6dcef;margin-top:12px;white-space:pre-wrap}
.hint{color:var(--mut);font-size:.82rem;margin-top:8px;line-height:1.5}
footer{margin-top:40px;text-align:center;color:var(--mut);font-size:.84rem}
</style>
</head>
<body>
<div class="shell">
  <div class="top">
    <div class="brand">
      <div class="mark">SH</div>
      <div>
        <h1>StreamHub API</h1>
        <div class="sub">MovieBox · 4K CDN · HubCloud · IPTV · HentaiCity — by shawon</div>
      </div>
    </div>
    <div class="pills">
      <span class="pill on">v7.1.0</span>
      <span class="pill live">MovieBox live</span>
      <span class="pill live">4KHDHub</span>
      <span class="pill">HLS · DASH</span>
    </div>
  </div>
  <div class="stats">
    <div class="stat"><b id="sc">—</b><span>Endpoints</span></div>
    <div class="stat"><b>6</b><span>Providers</span></div>
    <div class="stat"><b>JSON</b><span>creator: shawon</span></div>
    <div class="stat"><b>CDN</b><span>DASH · HLS · PD</span></div>
  </div>
  <div class="search">
    <span style="opacity:.45">⌕</span>
    <input id="q" placeholder="Search endpoints — play, resolve, stream, search…" oninput="filterCards()"/>
  </div>
  <div class="tabs" id="tabs"></div>
  <div class="grid" id="cards"></div>
  <div class="drawer" id="drawer"></div>
  <footer>All responses include <code>creator: "shawon"</code> · Built from MovieBox-TUI crypto + HubCloud resolve</footer>
</div>
<script>
const API = location.origin;
const E = [
  {g:'MovieBox', p:'GET /mb/search', d:'Search movies & series (returns subjectId)', t:'/mb/search?q=avatar'},
  {g:'MovieBox', p:'GET /mb/play/{id}', d:'MP4 + DASH streams with Cookie for real CDN', t:'/mb/play/1654274595068805784'},
  {g:'MovieBox', p:'GET /mb/detail/{id}', d:'Full metadata', t:'/mb/detail/1654274595068805784'},
  {g:'MovieBox', p:'GET /mb/resource/{id}', d:'Alternate resource links', t:'/mb/resource/1654274595068805784'},
  {g:'MovieBox', p:'GET /mb/home', d:'Home operating tabs', t:'/mb/home'},
  {g:'MovieBox', p:'GET /mb/seasons/{id}', d:'Season info for series', t:'/mb/seasons/1654274595068805784'},
  {g:'4KHDHub', p:'GET /fk/home', d:'Latest catalog cards', t:'/fk/home'},
  {g:'4KHDHub', p:'GET /fk/search', d:'Search 4K catalog', t:'/fk/search?q=avatar'},
  {g:'4KHDHub', p:'GET /fk/detail', d:'Releases + GreenMotors/Hub mirrors', t:'/fk/detail?path=/hacksaw-ridge-movie-7809/'},
  {g:'4KHDHub', p:'GET /fk/stream', d:'Try resolve to PixelDrain/CDN', t:'/fk/stream?path=/hacksaw-ridge-movie-7809/&resolve=true'},
  {g:'4KHDHub', p:'GET /fk/category/{slug}', d:'movies, series, netflix…', t:'/fk/category/movies'},
  {g:'Tools', p:'GET /tools/resolve', d:'HubCloud / HubDrive / PixelDrain → direct CDN', t:'/tools/resolve?url=https://hubcloud.ist/drive/xxx'},
  {g:'Tools', p:'GET /tools/pixeldrain', d:'Build PixelDrain ?download URL', t:'/tools/pixeldrain?id=GauktM6T'},
  {g:'Dramachi', p:'GET /dr/search', d:'Asian drama search', t:'/dr/search?q=love'},
  {g:'Dramachi', p:'GET /dr/home', d:'Drama home feed', t:'/dr/home'},
  {g:'IPTV', p:'GET /iptv/channels', d:'Public live TV M3U', t:'/iptv/channels?limit=40'},
  {g:'HentaiCity', p:'GET /hc/recent', d:'Recent videos', t:'/hc/recent'},
  {g:'HentaiCity', p:'GET /hc/popular', d:'Popular videos', t:'/hc/popular'},
  {g:'HentaiCity', p:'GET /hc/search', d:'Search', t:'/hc/search?q=anime'},
  {g:'HentaiCity', p:'GET /hc/watch', d:'HLS master.m3u8 CDN', t:'/hc/watch?id=weak-teacher-1-clumsy-busty-anime-teacher-rips-her-stockings-in-class-6IvrFW0nkUP'},
  {g:'Aggregate', p:'GET /search', d:'Search MovieBox + 4K + Drama together', t:'/search?q=avatar'},
  {g:'Meta', p:'GET /health', d:'Version & providers', t:'/health'},
];
let active = 'All';
function groups(){return ['All',...new Set(E.map(e=>e.g))]}
function renderTabs(){
  document.getElementById('tabs').innerHTML = groups().map(g=>
    `<button class="tab ${g===active?'active':''}" onclick="active='${g}';renderTabs();filterCards()">${g}</button>`
  ).join('');
}
function filterCards(){
  const q=(document.getElementById('q').value||'').toLowerCase();
  document.querySelectorAll('.card').forEach(c=>{
    const okG = active==='All' || c.dataset.g===active;
    const okQ = !q || (c.dataset.t||'').includes(q);
    c.style.display = okG && okQ ? '' : 'none';
  });
}
function render(){
  document.getElementById('sc').textContent = E.length;
  document.getElementById('cards').innerHTML = E.map((e,i)=>`
    <div class="card" data-g="${e.g}" data-t="${(e.g+' '+e.p+' '+e.d).toLowerCase()}" onclick="openEp(${i})">
      <div class="g">${e.g}</div>
      <h3>${e.d}</h3>
      <p>Try live against this server</p>
      <div class="method">${e.p}</div>
    </div>`).join('');
}
async function openEp(i){
  const e=E[i]; const d=document.getElementById('drawer'); d.className='drawer open';
  d.innerHTML=`<div class="ep">${e.p}</div><p style="color:var(--mut)">${e.d}</p>
  <div class="row"><input class="field" id="tryurl" value="${API}${e.t}"/>
  <button class="btn" onclick="runTry()">Try it</button>
  <button class="ghost" onclick="navigator.clipboard.writeText(document.getElementById('tryurl').value)">Copy</button></div>
  <div class="hint"><b>MovieBox play:</b> use <code>streams[].url</code> where <code>kind=dash</code> and send <code>Cookie: streams[].cookie</code>.
  <br/><b>4K CDN:</b> detail → mirrors → if you have <code>hubcloud.ist/drive/…</code> call <code>/tools/resolve</code>.</div>
  <pre id="out">Ready…</pre>`;
  d.scrollIntoView({behavior:'smooth',block:'nearest'});
}
async function runTry(){
  const u=document.getElementById('tryurl').value; const out=document.getElementById('out');
  out.textContent='Loading…'; const t0=performance.now();
  try{
    const r=await fetch(u); const txt=await r.text();
    let pretty=txt; try{pretty=JSON.stringify(JSON.parse(txt),null,2)}catch(_){}
    out.textContent=`${r.status} · ${Math.round(performance.now()-t0)}ms\\n\\n`+pretty.slice(0,14000);
  }catch(err){out.textContent=String(err)}
}
renderTabs(); render();
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


@app.get("/site", response_class=HTMLResponse, include_in_schema=False)
async def site():
    return HTMLResponse(DOCS_HTML)
