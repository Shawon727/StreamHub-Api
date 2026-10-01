# StreamHub API v7.5.0 — creator: shawon
from __future__ import annotations

import asyncio, base64, hashlib, hmac, json, random, re, time, uuid
from typing import Any, Dict, List, Optional
from urllib.parse import parse_qsl, urljoin, urlparse, quote

import httpx
from bs4 import BeautifulSoup
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse

CREATOR, VERSION = "shawon", "7.5.0"
app = FastAPI(title="StreamHub API", version=VERSION, docs_url=None, redoc_url=None)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
MOBILE_UA = "Mozilla/5.0 (Linux; Android 13; Pixel 7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Mobile Safari/537.36"

def ok(data=None, **extra):
    o = {"creator": CREATOR, "ok": True}
    if data is not None:
        o["data"] = data
    o.update(extra)
    return o

def fail(msg, **extra):
    o = {"creator": CREATOR, "ok": False, "error": msg}
    o.update(extra)
    return o

def _client(timeout=25.0):
    return httpx.AsyncClient(
        timeout=httpx.Timeout(timeout, connect=10.0),
        follow_redirects=True,
        headers={"User-Agent": UA, "Accept": "*/*", "Accept-Language": "en-US,en;q=0.9"},
        limits=httpx.Limits(max_connections=40, max_keepalive_connections=20),
    )

# ========== MOVIEBOX ==========
MB_HOSTS = [
    "https://api6.aoneroom.com", "https://api5.aoneroom.com", "https://api4.aoneroom.com",
    "https://api4sg.aoneroom.com", "https://api3.aoneroom.com", "https://api6sg.aoneroom.com",
    "https://api.inmoviebox.com",
]
MB_SECRET = bytes([0xEF,0xA8,0x91,0x97,0x4E,0xEC,0xD3,0x14,0x8D,0xF6,0x3A,0xA6,0x11,0x60,0x2D,0xEF,0xD1,0x01,0x25,0x9B,0xA5,0x21,0x02,0x2C,0x57,0xAE,0x05,0x66,0xBD,0x8E])
_mb_token = None
_mb_token_exp = 0.0
_mb_host_idx = 0
_mb_device_id = "".join(random.choice("0123456789abcdef") for _ in range(32))
_mb_gaid = str(uuid.uuid4())

def _md5_hex(b: bytes) -> str:
    return hashlib.md5(b).hexdigest()

def _mb_client_info():
    vc = 50020119
    ci = json.dumps({
        "package_name": "com.community.oneroom", "version_name": "4.0.01.0813.03", "version_code": vc,
        "os": "android", "os_version": "13", "install_ch": "ps", "device_id": _mb_device_id,
        "install_store": "ps", "gaid": _mb_gaid, "brand": "Redmi", "model": "23078RKD5C",
        "system_language": "en", "net": "NETWORK_WIFI", "region": "US", "timezone": "Asia/Kolkata",
        "sp_code": "40401", "X-Play-Mode": "2",
    }, separators=(",", ":"))
    ua = f"com.community.oneroom/{vc} (Linux; U; Android 13; en_US; 23078RKD5C; Build/TQ2A.230405.003; Cronet/135.0.7012.3)"
    return ua, ci

def _mb_sign(method, url, body=None):
    ts = int(time.time() * 1000)
    p = urlparse(url)
    params = sorted(parse_qsl(p.query, keep_blank_values=True))
    qs = "&".join(f"{k}={v}" for k, v in params)
    canon = p.path + (f"?{qs}" if qs else "")
    body_b = (body or "").encode()
    bh = _md5_hex(body_b) if body_b else ""
    bl = str(len(body_b)) if body_b else ""
    accept = ctype = "application/json"
    canonical = f"{method.upper()}\n{accept}\n{ctype}\n{bl}\n{ts}\n{bh}\n{canon}"
    sig = base64.b64encode(hmac.new(MB_SECRET, canonical.encode(), hashlib.md5).digest()).decode()
    rev = str(ts)[::-1]
    ua, ci = _mb_client_info()
    return {
        "Accept": accept, "Content-Type": ctype, "Connection": "keep-alive", "User-Agent": ua,
        "x-client-token": f"{ts},{_md5_hex(rev.encode())}", "x-tr-signature": f"{ts}|2|{sig}",
        "x-client-info": ci, "x-client-status": "0",
        "x-forwarded-for": f"103.241.{random.randint(1,254)}.{random.randint(1,254)}",
    }

def _mb_unwrap(payload):
    if isinstance(payload, dict) and isinstance(payload.get("data"), dict):
        return payload["data"]
    return payload if isinstance(payload, dict) else {}

async def _mb_request(method, path, body=None, token=None):
    global _mb_host_idx
    last = "hosts exhausted"
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
                    last = f"{base}->{r.status_code}"
                    continue
                if r.status_code >= 400:
                    last = f"{base}->{r.status_code}:{r.text[:120]}"
                    continue
                _mb_host_idx = (_mb_host_idx + i) % len(MB_HOSTS)
                try:
                    return r.json()
                except Exception:
                    return {"raw": r.text[:2000]}
            except Exception as e:
                last = str(e)
                continue
    raise HTTPException(502, detail=fail("MovieBox upstream failed", detail=last))

async def _mb_session():
    global _mb_token, _mb_token_exp
    if _mb_token and time.time() < _mb_token_exp:
        return _mb_token
    data = await _mb_request("POST", "/wefeed-mobile-bff/user-api/visitor-login", "{}")
    token = _mb_unwrap(data).get("token") or data.get("token")
    if not token:
        raise HTTPException(502, detail=fail("MovieBox login failed", raw=data))
    _mb_token = str(token)
    _mb_token_exp = time.time() + 3500
    return _mb_token

def _dash_from_cookie(sign_cookie):
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
    cookie = sign_cookie if sign_cookie.startswith("Edge-Cache-Cookie=") else f"Edge-Cache-Cookie={sign_cookie}"
    return {
        "url": base.rstrip("/") + "/index.mpd", "format": "DASH", "kind": "dash",
        "cookie": cookie, "base": base, "note": "Send Cookie header when playing",
    }

def _mb_streams(payload):
    streams = []
    data = _mb_unwrap(payload)
    for s in (data.get("streams") or data.get("list") or []):
        if not isinstance(s, dict):
            continue
        url = s.get("url") or s.get("playUrl") or s.get("streamUrl")
        cookie = s.get("signCookie") or s.get("cookie") or ""
        if url:
            streams.append({
                "id": s.get("id"), "url": url, "quality": s.get("resolutions") or s.get("quality"),
                "codec": s.get("codecName") or s.get("codec"), "format": s.get("format") or "MP4",
                "size": s.get("size"), "duration": s.get("duration"), "cookie": cookie or None, "kind": "mp4",
            })
        dash = _dash_from_cookie(cookie)
        if dash:
            dash["id"] = s.get("id")
            dash["quality"] = s.get("resolutions")
            dash["codec"] = s.get("codecName") or "h265"
            streams.append(dash)
    return streams

def _mb_items_from_search(inner):
    items = []
    for block in inner.get("results") or []:
        for s in block.get("subjects") or []:
            cover = s.get("cover")
            items.append({
                "subjectId": s.get("subjectId") or s.get("id"),
                "title": s.get("title"),
                "type": s.get("subjectType"),
                "year": (s.get("releaseDate") or "")[:4],
                "genre": s.get("genre"),
                "cover": cover.get("url") if isinstance(cover, dict) else cover,
                "imdb": s.get("imdbRating") or s.get("rating"),
            })
    return items

@app.get("/mb/home", tags=["MovieBox"])
async def mb_home(page: int = 1, tab_id: str = "1"):
    tok = await _mb_session()
    data = await _mb_request("GET", f"/wefeed-mobile-bff/tab-operating?page={page}&tabId={tab_id}&version=", token=tok)
    return ok(_mb_unwrap(data), provider="moviebox", level="primary", endpoint="home")

@app.get("/mb/search", tags=["MovieBox"])
async def mb_search(q: str = Query(..., min_length=1), page: int = 1):
    tok = await _mb_session()
    body = json.dumps({"keyword": q, "page": page, "perPage": 20})
    data = await _mb_request("POST", "/wefeed-mobile-bff/subject-api/search/v2", body, token=tok)
    inner = _mb_unwrap(data)
    items = _mb_items_from_search(inner)
    return ok({"items": items, "count": len(items), "pager": inner.get("pager")}, provider="moviebox", level="primary", endpoint="search", query=q)

@app.get("/mb/movies", tags=["MovieBox"])
async def mb_movies(q: str = Query("a", min_length=1), page: int = 1):
    tok = await _mb_session()
    body = json.dumps({"keyword": q, "page": page, "perPage": 20, "subjectType": 1})
    data = await _mb_request("POST", "/wefeed-mobile-bff/subject-api/search/v2", body, token=tok)
    items = _mb_items_from_search(_mb_unwrap(data))
    return ok({"items": items, "count": len(items)}, provider="moviebox", level="primary", endpoint="movies", query=q)

@app.get("/mb/series", tags=["MovieBox"])
async def mb_series(q: str = Query("a", min_length=1), page: int = 1):
    tok = await _mb_session()
    body = json.dumps({"keyword": q, "page": page, "perPage": 20, "subjectType": 2})
    data = await _mb_request("POST", "/wefeed-mobile-bff/subject-api/search/v2", body, token=tok)
    items = _mb_items_from_search(_mb_unwrap(data))
    return ok({"items": items, "count": len(items)}, provider="moviebox", level="primary", endpoint="series", query=q)


@app.get("/mb/adult", tags=["MovieBox"])
async def mb_adult(q: str = Query("sex", min_length=1), page: int = 1):
    """18+ / Adult content search. Filters results whose genre contains Adult/Erotic/AiAdult."""
    tok = await _mb_session()
    body = json.dumps({"keyword": q, "page": page, "perPage": 20, "subjectType": 0})
    data = await _mb_request("POST", "/wefeed-mobile-bff/subject-api/search/v2", body, token=tok)
    items = _mb_items_from_search(_mb_unwrap(data))
    adult_keys = ("adult", "erotic", "aiadult", "hot", "xxx")
    filtered = []
    for it in items:
        g = (it.get("genre") or "").lower()
        if any(k in g for k in adult_keys):
            filtered.append(it)
    # if filter too strict, still return all for this query (often already adult)
    use = filtered if filtered else items
    return ok(
        {"items": use, "count": len(use), "filtered_adult": len(filtered), "total_raw": len(items)},
        provider="moviebox", level="primary", endpoint="adult", query=q,
        note="18+ catalog via search; prefer tab 9 home for curated adult shelves.",
    )

@app.get("/mb/adult/home", tags=["MovieBox"])
async def mb_adult_home(page: int = 1):
    """Curated 18+ home — MovieBox tabId=9 (Porn / Adult shelves)."""
    tok = await _mb_session()
    data = await _mb_request("GET", f"/wefeed-mobile-bff/tab-operating?page={page}&tabId=9&version=", token=tok)
    return ok(_mb_unwrap(data), provider="moviebox", level="primary", endpoint="adult_home", tab_id="9")

@app.get("/mb/tab/{tab_id}", tags=["MovieBox"])
async def mb_tab(tab_id: str, page: int = 1):
    tok = await _mb_session()
    data = await _mb_request("GET", f"/wefeed-mobile-bff/tab-operating?page={page}&tabId={tab_id}&version=", token=tok)
    return ok(_mb_unwrap(data), provider="moviebox", level="primary", endpoint="tab", tab_id=tab_id)

@app.get("/mb/detail/{subject_id}", tags=["MovieBox"])
async def mb_detail(subject_id: str):
    tok = await _mb_session()
    data = await _mb_request("GET", f"/wefeed-mobile-bff/subject-api/get?subjectId={subject_id}", token=tok)
    return ok(_mb_unwrap(data), provider="moviebox", level="primary", endpoint="detail")

@app.get("/mb/seasons/{subject_id}", tags=["MovieBox"])
async def mb_seasons(subject_id: str):
    tok = await _mb_session()
    data = await _mb_request("GET", f"/wefeed-mobile-bff/subject-api/season-info?subjectId={subject_id}", token=tok)
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
    streams = _mb_streams(data)
    return ok({
        "subject_id": subject_id, "season": se, "episode": ep, "title": inner.get("title"),
        "streams": streams, "count": len(streams), "displayResolutions": inner.get("displayResolutions"),
    }, provider="moviebox", level="primary", endpoint="play",
       note="kind=dash needs Cookie header; kind=mp4 is direct URL")

@app.get("/mb/resource/{subject_id}", tags=["MovieBox"])
async def mb_resource(subject_id: str, se: Optional[int] = None, ep: Optional[int] = None, page: int = 1, per_page: int = 20):
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
            links.append({"title": item.get("title"), "url": link, "size": item.get("size"),
                          "episode": item.get("episode"), "resourceId": item.get("resourceId")})
    return ok({"links": links, "count": len(links), "pager": inner.get("pager")}, provider="moviebox", level="primary", endpoint="resource")

# ========== 4KHDHub + HubCloud ==========
FK_BASE = "https://4khdhub.one"

def _fk_cards(html):
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
        items.append({
            "id": urlparse(full).path, "title": title, "url": full, "poster": poster,
            "meta": meta, "year": year_m.group(0) if year_m else None,
            "type": "series" if "-series-" in href else "movie",
        })
    return items

def _fk_downloads(html):
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

def _rot13(s):
    out = []
    for c in s:
        if "a" <= c <= "m" or "A" <= c <= "M":
            out.append(chr(ord(c) + 13))
        elif "n" <= c <= "z" or "N" <= c <= "Z":
            out.append(chr(ord(c) - 13))
        else:
            out.append(c)
    return "".join(out)

def _decode_gm_payload(payload):
    try:
        step1 = base64.b64decode(payload).decode("utf-8", "ignore")
        step2 = base64.b64decode(step1).decode("utf-8", "ignore")
        step3 = _rot13(step2)
        step4 = base64.b64decode(step3).decode("utf-8", "ignore")
        obj = json.loads(step4)
        return base64.b64decode(obj["o"]).decode("utf-8", "ignore")
    except Exception:
        return None

def _pixeldrain_api(url):
    try:
        p = urlparse(url)
        if "pixeldrain." not in (p.hostname or ""):
            return None
        path = p.path
        if path.startswith("/u/"):
            fid = path[3:].strip("/")
        elif path.startswith("/api/file/"):
            fid = path[len("/api/file/"):].strip("/").split("?")[0]
        else:
            return None
        return f"https://{p.hostname}/api/file/{fid}?download" if fid else None
    except Exception:
        return None

def _extract_cdn_urls(html):
    found = []
    patterns = [
        r'https://[a-f0-9]+\.r2\.cloudflarestorage\.com/[^"\'\s<>]+',
        r'https://gpdl\.hubcloud\.ist/\?id=[^"\'\s<>]+',
        r'https://[^"\'\s<>]*googleusercontent\.com/[^"\'\s<>]+',
        r'https://pixeldrain\.(?:com|dev)/(?:u|api/file)/[A-Za-z0-9_-]+',
        r'https://[^"\'\s<>]+\.workers\.dev/[^"\'\s<>]+',
    ]
    for pat in patterns:
        for m in re.findall(pat, html):
            u = m.replace("&amp;", "&")
            if u not in found:
                found.append(u)
    return found

async def _resolve_hubcloud(drive_url):
    results = []
    async with _client(35) as client:
        r = await client.get(drive_url, headers={"Referer": "https://4khdhub.one/"})
        html = r.text
        m = re.search(r'https://gamerxyt\.com/hubcloud\.php[^"\']+', html)
        resolver = m.group(0) if m else None
        page = html
        if resolver:
            try:
                rr = await client.get(resolver, headers={"Referer": drive_url, "User-Agent": UA})
                page = rr.text
            except Exception:
                pass
        for u in _extract_cdn_urls(page):
            results.append({"label": "CDN", "url": _pixeldrain_api(u) or u, "kind": "direct"})
        for prefix in ("https://pixeldrain.dev/u/", "https://pixeldrain.com/u/", "https://pixeldrain.dev/api/file/", "https://pixeldrain.com/api/file/"):
            idx = 0
            while True:
                pos = page.find(prefix, idx)
                if pos < 0:
                    break
                end = pos
                while end < len(page) and page[end] not in "\"' \t\n\r<>\\":
                    end += 1
                api = _pixeldrain_api(page[pos:end])
                if api and not any(x["url"] == api for x in results):
                    results.append({"label": "PixelDrain", "url": api, "kind": "direct"})
                idx = end
    seen = set()
    out = []
    for x in results:
        if x["url"] in seen:
            continue
        seen.add(x["url"])
        out.append(x)
    return out

async def _resolve_hubdrive(drive_url):
    async with _client(20) as client:
        r = await client.get(drive_url, headers={"Referer": "https://4khdhub.one/"})
        for a in BeautifulSoup(r.text, "html.parser").select("a[href]"):
            href = a.get("href") or ""
            if "hubcloud." in href and "/drive/" in href:
                return await _resolve_hubcloud(href)
    return [{"label": "HubDrive", "url": drive_url, "kind": "intermediate"}]

async def _resolve_greenmotors(gm_url):
    async with _client(30) as client:
        r = await client.get(gm_url, headers={"Referer": "https://4khdhub.one/", "Accept": "text/html", "User-Agent": UA})
        html = r.text
        m = re.search(r"s\(\s*['\"]o['\"]\s*,\s*['\"]([^'\"]+)['\"]", html)
        target = _decode_gm_payload(m.group(1)) if m else None
        if not target:
            for mm in re.findall(r"https?://[^\s\"']+hubcloud[^\s\"']+", html):
                target = mm
                break
        if not target:
            return [{"label": "GreenMotors", "url": gm_url, "kind": "intermediate",
                     "note": "Could not decode gateway on this IP"}]
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
    return ok({"title": title, "url": url, "releases": releases, "release_count": len(releases)},
              provider="4khdhub", level="live", endpoint="detail")

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
    direct = []
    if resolve:
        tasks = []
        for rel in releases[:6]:
            for m in rel.get("mirrors") or []:
                if len(tasks) >= 8:
                    break
                mu = m.get("url") or ""
                lab = m.get("label")
                if "greenmotors." in mu or "greenmountmotors." in mu:
                    tasks.append((_resolve_greenmotors(mu), rel.get("title"), lab))
                elif "hubcloud." in mu:
                    tasks.append((_resolve_hubcloud(mu), rel.get("title"), lab))
                elif "hubdrive." in mu:
                    tasks.append((_resolve_hubdrive(mu), rel.get("title"), lab))
                elif "pixeldrain." in mu:
                    api = _pixeldrain_api(mu) or mu
                    direct.append({"release": rel.get("title"), "label": lab, "url": api, "kind": "direct"})
        for coro, rtitle, label in tasks:
            try:
                for item in await coro:
                    direct.append({
                        "release": rtitle, "label": item.get("label") or label,
                        "url": item.get("url"), "kind": item.get("kind"), "note": item.get("note"),
                    })
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
    return ok({"title": title, "url": url, "releases": releases, "direct_streams": uniq, "direct_count": len(uniq)},
              provider="4khdhub", level="live", endpoint="stream")

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
    return ok({
        "id": id,
        "download_url": f"https://pixeldrain.com/api/file/{id}?download",
        "info_url": f"https://pixeldrain.com/api/file/{id}",
        "alt": f"https://pixeldrain.dev/api/file/{id}?download",
    }, provider="tools", level="tool", endpoint="pixeldrain")

# ========== Dramachi (search works; home uses search catalog) ==========
DR_BASE = "https://api.nodeobjects.com"

def _dr_normalize(raw):
    """Always return {items:[], page meta}."""
    if raw is None:
        return {"items": [], "count": 0}
    if isinstance(raw, list):
        items = raw
    elif isinstance(raw, dict):
        items = raw.get("data") or raw.get("results") or raw.get("items") or []
        if not isinstance(items, list):
            items = []
    else:
        items = []
    out = []
    for it in items:
        if not isinstance(it, dict):
            continue
        thumb = it.get("thumb") or ""
        out.append({
            "id": str(it.get("id") or ""),
            "title": it.get("title") or it.get("common_title"),
            "year": it.get("year"),
            "views": it.get("views"),
            "content": it.get("content") or "movies",
            "thumb": thumb,
            "poster": f"https://static.nodeobjects.com/thumbnail/{thumb}" if thumb else None,
            "common_title": it.get("common_title"),
            "category": it.get("category"),
            "date": it.get("date"),
        })
    meta = {}
    if isinstance(raw, dict):
        for k in ("last_page", "next_page", "prev_page", "prev_lastpage", "adjuction"):
            if k in raw:
                meta[k] = raw[k]
    return {"items": out, "count": len(out), **meta}

async def _dr_search_raw(q: str, page: int = 1, filter_: str = "all"):
    async with _client() as client:
        r = await client.get(f"{DR_BASE}/", params={"interface": "search", "q": q or "a", "filter": filter_, "page": page})
        try:
            return r.json()
        except Exception:
            return None

@app.get("/dr/search", tags=["Dramachi"])
async def dr_search(q: str = Query("a"), page: int = 1, filter: str = Query("all", description="all|movies|series")):
    q = (q or "a").strip() or "a"
    raw = await _dr_search_raw(q, page, filter)
    data = _dr_normalize(raw)
    # If empty, try alternate filters / query variants
    if data["count"] == 0 and filter != "all":
        raw = await _dr_search_raw(q, page, "all")
        data = _dr_normalize(raw)
    if data["count"] == 0 and len(q) > 2:
        raw = await _dr_search_raw(q[:3], page, "all")
        data = _dr_normalize(raw)
        data["note"] = "broadened query prefix"
    return ok(data, provider="dramachi", level="live", endpoint="search", query=q, filter=filter)

@app.get("/dr/home", tags=["Dramachi"])
async def dr_home(page: int = 1, filter: str = Query("all")):
    """Upstream has no home interface — uses search catalog (q empty / letter)."""
    # Prefer empty query which returns broad catalog
    raw = await _dr_search_raw("", page, filter)
    if not raw or not (isinstance(raw, dict) and (raw.get("data") or [])):
        raw = await _dr_search_raw("the", page, filter)
    data = _dr_normalize(raw)
    return ok(data, provider="dramachi", level="live", endpoint="home", page=page,
              note="Dramachi has no /home upstream; this is a catalog via search.")

@app.get("/dr/detail", tags=["Dramachi"])
async def dr_detail(id: str = Query(...), content: str = Query("movies")):
    """No public stream API. Returns poster + re-search match if found."""
    # Try to find item by scanning search pages for this id
    found = None
    for q in (id, "a", "the", "love"):
        raw = await _dr_search_raw(q, 1, "all")
        data = _dr_normalize(raw)
        for it in data["items"]:
            if it["id"] == str(id):
                found = it
                break
        if found:
            break
    if found:
        return ok({
            "id": id, "content": content, "title": found.get("title"),
            "poster": found.get("poster"), "year": found.get("year"),
            "streams": [], "stream_available": False,
            "item": found,
        }, provider="dramachi", level="live", endpoint="detail",
           note="No public CDN/stream from Dramachi API. Metadata + poster only.")
    return ok({
        "id": id, "content": content, "poster": None, "streams": [], "stream_available": False,
    }, provider="dramachi", level="live", endpoint="detail",
       note="Item not found in catalog sample. No public stream endpoint exists upstream.")

@app.get("/dr/thumb", tags=["Dramachi"])
async def dr_thumb(name: str = Query(..., description="thumb filename from search")):
    url = f"https://static.nodeobjects.com/thumbnail/{name}"
    return ok({"thumb": name, "url": url}, provider="dramachi", level="live", endpoint="thumb")

# ========== IPTV ==========
IPTV_SOURCES = [
    "https://iptv-org.github.io/iptv/index.m3u",
    "https://iptv-org.github.io/iptv/countries/bd.m3u",
    "https://iptv-org.github.io/iptv/countries/in.m3u",
]

def _parse_m3u(text):
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

# ========== HentaiCity ==========
HC_BASE = "https://www.hentaicity.com"
# Verified working folder/id pairs (CDN returns 200) used when datacenter IP is blocked
_HC_SEED = [
    {"folder": "0267", "vid": "38191", "id": "seed-0267-38191", "title": "Sample stream A (CDN seed)"},
    {"folder": "0449", "vid": "38135", "id": "seed-0449-38135", "title": "Sample stream B (CDN seed)"},
    {"folder": "0730", "vid": "38155", "id": "seed-0730-38155", "title": "Sample stream C (CDN seed)"},
    {"folder": "0131", "vid": "38132", "id": "seed-0131-38132", "title": "Sample stream D (CDN seed)"},
    {"folder": "0739", "vid": "38186", "id": "seed-0739-38186", "title": "Sample stream E (CDN seed)"},
]

def _hc_cdn(folder: str, vid: str) -> dict:
    base_flv = f"https://www.hentaicity.com/flv/{folder}/{vid}"
    hls = (
        f"https://hls.hentaicity.com/_hls/flv/{folder}/{vid}/"
        f",default,mobile,480p,720p,1080p,.mp4.urlset/master.m3u8"
    )
    return {
        "folder": folder, "video_id": vid, "hls": hls,
        "mp4": {
            "mobile": f"{base_flv}/mobile.mp4",
            "default": f"{base_flv}/default.mp4",
            "480p": f"{base_flv}/480p.mp4",
            "720p": f"{base_flv}/720p.mp4",
            "1080p": f"{base_flv}/1080p.mp4",
        },
        "poster": f"https://cdn1.images.hentaicity.com/videos/{folder}/{vid}/main.jpg",
        "poster_hd": f"https://cdn1.images.hentaicity.com/videos/{folder}/{vid}/1080p.jpg",
        "trailer": f"https://cdn1.hentaicity.com/{folder}/{vid}/trailer.mp4",
    }

def _hc_list(html: str) -> list:
    soup = BeautifulSoup(html, "html.parser")
    items = []
    seen = set()
    for a in soup.select("a[href]"):
        href = a.get("href") or ""
        m = re.search(r"/video/([^/]+\.html)", href)
        if not m or "all-" in href:
            continue
        slug = m.group(1)
        vid_key = slug.rsplit(".", 1)[0]
        if vid_key in seen:
            continue
        seen.add(vid_key)
        img = a.find("img")
        poster = folder = vid_num = None
        if img:
            poster = img.get("src") or img.get("data-src") or img.get("data-original")
            if poster:
                fm = re.search(r"/videos/(\d+)/(\d+)/", poster)
                if fm:
                    folder, vid_num = fm.group(1), fm.group(2)
        title = (a.get("title") or "").strip()
        if not title and img:
            title = (img.get("alt") or img.get("title") or "").strip()
        if not title:
            title = " ".join(a.get_text().split())
        if not title or (len(title) <= 8 and any(ch.isdigit() for ch in title)):
            title = vid_key.replace("-", " ")[:90]
        full = href if href.startswith("http") else urljoin(HC_BASE, href)
        item = {
            "id": vid_key, "slug": slug, "title": title, "url": full,
            "poster": poster, "folder": folder, "numeric_id": vid_num,
        }
        if folder and vid_num:
            item["streams"] = _hc_cdn(folder, vid_num)
        items.append(item)
    return items

def _hc_seed_items():
    out = []
    for s in _HC_SEED:
        streams = _hc_cdn(s["folder"], s["vid"])
        out.append({
            "id": s["id"], "title": s["title"], "folder": s["folder"], "numeric_id": s["vid"],
            "poster": streams["poster"], "streams": streams, "seed": True,
        })
    return out

async def _hc_fetch_list(path: str) -> tuple:
    """Returns (items, blocked: bool, note)."""
    headers_list = [
        {"User-Agent": UA, "Referer": "https://www.google.com/", "Accept": "text/html", "Accept-Language": "en-US,en;q=0.9"},
        {"User-Agent": MOBILE_UA, "Referer": HC_BASE + "/", "Accept": "text/html"},
    ]
    async with _client(22) as client:
        for headers in headers_list:
            try:
                r = await client.get(urljoin(HC_BASE, path), headers=headers)
                if "defendonlineprivacy" in str(r.url) or "defendonlineprivacy" in r.text[:800]:
                    continue
                items = _hc_list(r.text)
                if items:
                    return items, False, "live"
            except Exception:
                continue
    return _hc_seed_items(), True, "HentaiCity HTML blocked on this server IP. Returning CDN seed items (real working streams). Use /hc/cdn?folder=&vid= for any id."

@app.get("/hc/recent", tags=["HentaiCity"])
async def hc_recent():
    items, blocked, note = await _hc_fetch_list("/videos/straight/all-recent.html")
    return ok(items, provider="hentaicity", level="private", count=len(items), endpoint="recent",
              blocked=blocked, note=note)

@app.get("/hc/popular", tags=["HentaiCity"])
async def hc_popular():
    items, blocked, note = await _hc_fetch_list("/videos/straight/all-popular.html")
    return ok(items, provider="hentaicity", level="private", count=len(items), endpoint="popular",
              blocked=blocked, note=note)

@app.get("/hc/search", tags=["HentaiCity"])
async def hc_search(q: str = Query(..., min_length=1)):
    q = q.strip()
    paths = [
        f"/search/video/{quote(q)}",
        f"/search/?q={quote(q)}",
        f"/search/video/{quote(q.replace(' ', '-'))}",
        f"/videos/straight/all-recent.html",  # last resort list
    ]
    items, blocked, note = [], True, ""
    for path in paths:
        items, blocked, note = await _hc_fetch_list(path)
        if items and not blocked:
            # if we used recent as fallback, filter by keyword in title
            if "all-recent" in path:
                ql = q.lower()
                filtered = [it for it in items if ql in (it.get("title") or "").lower() or ql in (it.get("id") or "").lower()]
                if filtered:
                    items = filtered
                    note = "filtered from recent list"
                else:
                    continue
            break
    if blocked or not items:
        # seed with keyword in title if possible
        seeds = _hc_seed_items()
        ql = q.lower()
        filtered = [s for s in seeds if ql in s["title"].lower()]
        items = filtered or seeds
        blocked = True
        note = note or "HentaiCity HTML blocked or no matches — CDN seed returned. Use folder+vid on /hc/cdn"
    return ok(items, provider="hentaicity", level="private", count=len(items), endpoint="search", query=q,
              blocked=blocked, note=note)

@app.get("/hc/watch", tags=["HentaiCity"])
async def hc_watch(
    id: Optional[str] = None,
    url: Optional[str] = None,
    folder: Optional[str] = None,
    vid: Optional[str] = Query(None, description="numeric id inside folder"),
):
    if folder and vid:
        cdn = _hc_cdn(folder, vid)
        sources = [{"src": cdn["hls"], "format": "hls"}] + [
            {"src": u, "format": "mp4", "quality": q} for q, u in cdn["mp4"].items()
        ]
        return ok({
            "id": id, "folder": folder, "vid": vid, "streams": cdn, "sources": sources, "count": len(sources),
        }, provider="hentaicity", level="private", endpoint="watch")
    if not id and not url:
        raise HTTPException(400, detail=fail("Provide id, url, or folder+vid"))
    pages = []
    if url:
        pages.append(url)
    if id:
        pages += [f"{HC_BASE}/video/{id}.html", f"{HC_BASE}/click/1-1/video/{id}.html"]
    html = ""
    final = pages[0]
    async with _client() as client:
        for page in pages:
            try:
                r = await client.get(page, headers={"Referer": HC_BASE + "/", "User-Agent": UA})
                html = r.text
                final = str(r.url)
                if "defendonlineprivacy" in final:
                    continue
                if re.search(r"/flv/(\d+)/(\d+)/", html):
                    break
            except Exception:
                continue
    fm = re.search(r"/flv/(\d+)/(\d+)/", html) or re.search(r"/videos/(\d+)/(\d+)/", html)
    if fm:
        cdn = _hc_cdn(fm.group(1), fm.group(2))
        sources = [{"src": cdn["hls"], "format": "hls"}] + [
            {"src": u, "format": "mp4", "quality": q} for q, u in cdn["mp4"].items()
        ]
        return ok({
            "id": id, "page": final, "streams": cdn, "sources": sources, "count": len(sources),
        }, provider="hentaicity", level="private", endpoint="watch")
    return ok({
        "id": id, "page": final, "sources": [], "count": 0,
        "hint": "HTML blocked. Use /hc/recent item folder+numeric_id → /hc/watch?folder=&vid= or /hc/cdn",
    }, provider="hentaicity", level="private", endpoint="watch")

@app.get("/hc/cdn", tags=["HentaiCity"])
async def hc_cdn(folder: str = Query(...), vid: str = Query(...)):
    """Always works — builds direct HLS + MP4 URLs. No scrape."""
    return ok(_hc_cdn(folder, vid), provider="hentaicity", level="private", endpoint="cdn")

# ========== Aggregate / Meta ==========
@app.get("/search", tags=["Aggregate"])
async def aggregate_search(q: str = Query(..., min_length=1)):
    results = {}
    async def mb():
        try:
            tok = await _mb_session()
            body = json.dumps({"keyword": q, "page": 1, "perPage": 10})
            data = await _mb_request("POST", "/wefeed-mobile-bff/subject-api/search/v2", body, token=tok)
            results["moviebox"] = {"items": _mb_items_from_search(_mb_unwrap(data))}
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
            raw = await _dr_search_raw(q, 1, "all")
            results["dramachi"] = _dr_normalize(raw)
        except Exception as e:
            results["dramachi"] = {"error": str(e)}
    await asyncio.gather(mb(), fk(), dr())
    return ok(results, provider="aggregate", level="primary", endpoint="search", query=q)

@app.get("/health", tags=["Meta"])
async def health():
    return ok({"version": VERSION, "providers": ["moviebox", "4khdhub", "hubcloud", "dramachi", "iptv", "hentaicity"]})

# ========== DOCS UI (mobile-first) ==========
DOCS_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8"/>
<meta name="viewport" content="width=device-width,initial-scale=1,viewport-fit=cover"/>
<title>StreamHub API</title>
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&family=JetBrains+Mono:wght@400;500&display=swap" rel="stylesheet"/>
<style>
:root{--bg:#0a0a0c;--card:#141418;--bd:#2a2a32;--tx:#f4f4f5;--mu:#9ca3af;--ac:#8b5cf6;--cy:#22d3ee;--ok:#34d399}
*{box-sizing:border-box;margin:0;padding:0}
body{font-family:Inter,system-ui,sans-serif;background:var(--bg);color:var(--tx);min-height:100vh;
background-image:radial-gradient(ellipse 100% 80% at 50% -30%,rgba(139,92,246,.2),transparent)}
.top{position:sticky;top:0;z-index:20;background:rgba(10,10,12,.92);backdrop-filter:blur(14px);border-bottom:1px solid var(--bd);padding:12px 16px}
.brand{display:flex;align-items:center;gap:10px;margin-bottom:10px}
.logo{width:36px;height:36px;border-radius:10px;background:linear-gradient(135deg,#8b5cf6,#22d3ee);display:grid;place-items:center;font-weight:700;font-size:12px}
.brand h1{font-size:16px;font-weight:700;line-height:1.2}.brand small{color:var(--mu);font-size:11px}
.search{display:flex;align-items:center;gap:8px;background:var(--card);border:1px solid var(--bd);border-radius:10px;padding:10px 12px}
.search:focus-within{border-color:#8b5cf688;box-shadow:0 0 0 3px #8b5cf622}
.search input{flex:1;border:0;outline:0;background:transparent;color:var(--tx);font:inherit;font-size:14px}
.tabs{display:flex;gap:6px;overflow-x:auto;padding:10px 16px 0;-webkit-overflow-scrolling:touch;scrollbar-width:none}
.tabs::-webkit-scrollbar{display:none}
.tab{flex:0 0 auto;padding:7px 12px;border-radius:999px;border:1px solid var(--bd);background:var(--card);color:var(--mu);font-size:12px;font-weight:500;cursor:pointer}
.tab.on,.tab:hover{border-color:#8b5cf688;color:var(--tx);background:#1a1525}
.wrap{padding:12px 16px 80px;max-width:720px;margin:0 auto}
.hint-box{background:var(--card);border:1px solid var(--bd);border-radius:12px;padding:12px 14px;margin-bottom:14px;font-size:13px;color:var(--mu);line-height:1.5}
.hint-box b{color:var(--tx)}
.ep{background:var(--card);border:1px solid var(--bd);border-radius:12px;margin-bottom:10px;overflow:hidden}
.ep-h{display:flex;align-items:center;gap:10px;padding:12px 14px;cursor:pointer}
.m{font-family:JetBrains Mono,monospace;font-size:10px;font-weight:700;padding:3px 7px;border-radius:5px;background:#22d3ee18;color:var(--cy)}
.p{font-family:JetBrains Mono,monospace;font-size:12px;font-weight:500;flex:1;word-break:break-all}
.arrow{color:var(--mu);transition:.2s}.ep.open .arrow{transform:rotate(90deg)}
.ep-b{display:none;padding:0 14px 14px;border-top:1px solid var(--bd)}
.ep.open .ep-b{display:block}
.how{margin:10px 0;font-size:12.5px;color:var(--mu);line-height:1.55}
.how b{color:var(--tx)}.how code{font-family:JetBrains Mono,monospace;font-size:11px;background:#8b5cf618;color:#c4b5fd;padding:1px 5px;border-radius:4px}
.fields{display:grid;gap:8px;margin:8px 0}
.field{display:flex;flex-direction:column;gap:4px}
.field label{font-size:11px;color:var(--mu);font-weight:500}
.field input{background:var(--bg);border:1px solid var(--bd);border-radius:8px;padding:10px 12px;color:var(--tx);font-family:JetBrains Mono,monospace;font-size:12px}
.actions{display:flex;flex-wrap:wrap;gap:8px;margin:10px 0}
.btn{background:linear-gradient(135deg,#8b5cf6,#6d28d9);border:0;color:#fff;padding:10px 16px;border-radius:8px;font-weight:600;font-size:13px;cursor:pointer}
.btn:active{transform:scale(.98)}.ghost{background:transparent;border:1px solid var(--bd);color:var(--tx);padding:10px 14px;border-radius:8px;font-size:12px;cursor:pointer}
pre{background:#050506;border:1px solid var(--bd);border-radius:8px;padding:12px;overflow:auto;max-height:320px;font-family:JetBrains Mono,monospace;font-size:11px;line-height:1.5;color:#e4e4e7;white-space:pre-wrap;word-break:break-word}
.sec{font-size:11px;font-weight:600;color:var(--mu);text-transform:uppercase;letter-spacing:.06em;margin:18px 0 8px}
.status{font-size:11px;color:var(--ok);margin-top:6px}
footer{text-align:center;color:var(--mu);font-size:12px;padding:20px 0 10px}
</style>
</head>
<body>
<header class="top">
  <div class="brand"><div class="logo">SH</div><div><h1>StreamHub API</h1><small>v7.5.0 · creator: shawon</small></div></div>
  <div class="search"><span style="opacity:.4;font-size:13px">⌕</span><input id="q" placeholder="Filter endpoints…" oninput="filt()"/></div>
</header>
<div class="tabs" id="tabs"></div>
<main class="wrap">
  <div class="hint-box"><b>How to test:</b> open an endpoint → fill parameters → <b>Try it</b>. Response shows below. Every JSON has <code>creator: "shawon"</code>.</div>
  <div id="list"></div>
  <footer>StreamHub · made by shawon</footer>
</main>
<script>
const API = location.origin;
const E = [
{g:'MovieBox',p:'/mb/search',how:'Search all titles. Copy <code>subjectId</code> for play/detail.',params:[{n:'q',v:'avatar'},{n:'page',v:'1'}]},
{g:'MovieBox',p:'/mb/movies',how:'Movies only (subjectType=1).',params:[{n:'q',v:'love'},{n:'page',v:'1'}]},
{g:'MovieBox',p:'/mb/series',how:'Series only (subjectType=2).',params:[{n:'q',v:'love'},
{g:'MovieBox',p:'/mb/adult',how:'18+ search. Genre filtered Adult/Erotic. Default q=sex.',params:[{n:'q',v:'sex'},{n:'page',v:'1'}]},
{g:'MovieBox',p:'/mb/adult/home',how:'Curated adult home (tabId=9 — Porn Top Videos shelves).',params:[{n:'page',v:'1'}]},
{n:'page',v:'1'}]},
{g:'MovieBox',p:'/mb/play/{id}',how:'Returns MP4 + DASH. For DASH send the <code>cookie</code> header. Series: add se & ep.',params:[{n:'id',v:'1654274595068805784',path:true},{n:'se',v:''},{n:'ep',v:''}]},
{g:'MovieBox',p:'/mb/detail/{id}',how:'Full metadata for a subjectId.',params:[{n:'id',v:'1654274595068805784',path:true}]},
{g:'MovieBox',p:'/mb/resource/{id}',how:'Extra resource/download links.',params:[{n:'id',v:'1654274595068805784',path:true}]},
{g:'MovieBox',p:'/mb/seasons/{id}',how:'Season list for series.',params:[{n:'id',v:'1654274595068805784',path:true}]},
{g:'MovieBox',p:'/mb/home',how:'Home shelves. Optional tab_id 1–4.',params:[{n:'tab_id',v:'1'},{n:'page',v:'1'}]},
{g:'MovieBox',p:'/mb/tab/{tab_id}',how:'Tab feed: try 1, 2, 3, 4.',params:[{n:'tab_id',v:'1',path:true},{n:'page',v:'1'}]},
{g:'4KHDHub',p:'/fk/home',how:'Latest catalog cards. Use <code>id</code> path in detail/stream.',params:[]},
{g:'4KHDHub',p:'/fk/search',how:'Search 4K catalog.',params:[{n:'q',v:'avatar'}]},
{g:'4KHDHub',p:'/fk/detail',how:'Releases + GreenMotors mirrors.',params:[{n:'path',v:'/hacksaw-ridge-movie-7809/'}]},
{g:'4KHDHub',p:'/fk/stream',how:'Resolves to R2 / gpdl / googleusercontent CDN. Set resolve=true.',params:[{n:'path',v:'/hacksaw-ridge-movie-7809/'},{n:'resolve',v:'true'}]},
{g:'4KHDHub',p:'/fk/category/{slug}',how:'Category: movies, series, netflix…',params:[{n:'slug',v:'movies',path:true},{n:'page',v:'1'}]},
{g:'Tools',p:'/tools/resolve',how:'HubCloud or GreenMotors URL → direct CDN links.',params:[{n:'url',v:'https://hubcloud.ist/drive/1qg90m0nr2599rq'}]},
{g:'Tools',p:'/tools/pixeldrain',how:'Build PixelDrain download URL from file id.',params:[{n:'id',v:'GauktM6T'}]},
{g:'Dramachi',p:'/dr/home',how:'Catalog (upstream has no home — uses search). Returns items + poster URLs.',params:[{n:'page',v:'1'},{n:'filter',v:'all'}]},
{g:'Dramachi',p:'/dr/search',how:'Search dramas/movies. filter=all|movies|series.',params:[{n:'q',v:'love'},{n:'page',v:'1'},{n:'filter',v:'all'}]},
{g:'Dramachi',p:'/dr/detail',how:'Metadata + poster only. <b>No public stream CDN</b> from Dramachi.',params:[{n:'id',v:'524'},{n:'content',v:'movies'}]},
{g:'Dramachi',p:'/dr/thumb',how:'Poster from thumb filename in search results.',params:[{n:'name',v:'godlovescaviar2012h.jpg'}]},
{g:'IPTV',p:'/iptv/channels',how:'Live M3U channels. source 0=global 1=BD 2=IN.',params:[{n:'source',v:'0'},{n:'limit',v:'30'},{n:'q',v:''}]},
{g:'HentaiCity',p:'/hc/recent',how:'Recent list. If server IP blocked, returns CDN seed items (streams still work).',params:[]},
{g:'HentaiCity',p:'/hc/popular',how:'Popular list (same shape as recent).',params:[]},
{g:'HentaiCity',p:'/hc/search',how:'Search. On block falls back to seed CDN items.',params:[{n:'q',v:'anime'}]},
{g:'HentaiCity',p:'/hc/watch',how:'Best: pass folder + vid from list item. Builds all qualities.',params:[{n:'folder',v:'0267'},{n:'vid',v:'38191'}]},
{g:'HentaiCity',p:'/hc/cdn',how:'No scrape. Always builds HLS + MP4 links from folder+vid.',params:[{n:'folder',v:'0267'},{n:'vid',v:'38191'}]},
{g:'Aggregate',p:'/search',how:'MovieBox + 4K + Dramachi in one response.',params:[{n:'q',v:'batman'}]},
{g:'Meta',p:'/health',how:'Version and provider list.',params:[]},
];
const groups=[...new Set(E.map(e=>e.g))];
let activeG='ALL';
function buildUrl(e,i){
  let path=e.p;
  const qs=[];
  (e.params||[]).forEach(pr=>{
    const el=document.getElementById('f'+i+'_'+pr.n);
    const val=el?el.value.trim():(pr.v||'');
    if(pr.path){ path=path.replace('{'+pr.n+'}', encodeURIComponent(val||pr.v)); }
    else if(val!=='') qs.push(encodeURIComponent(pr.n)+'='+encodeURIComponent(val));
  });
  // home uses tab_id as query for /mb/home
  if(e.p==='/mb/home'){ /* tab_id already in qs */ }
  return API+path+(qs.length?'?'+qs.join('&'):'');
}
function render(){
  const tabs=document.getElementById('tabs');
  tabs.innerHTML=['ALL',...groups].map(g=>`<button class="tab ${activeG===g?'on':''}" onclick="setG('${g}')">${g}</button>`).join('');
  const q=(document.getElementById('q').value||'').toLowerCase();
  let html='',last='';
  E.forEach((e,i)=>{
    if(activeG!=='ALL'&&e.g!==activeG) return;
    const hay=(e.g+' '+e.p+' '+e.how).toLowerCase();
    if(q&&!hay.includes(q)) return;
    if(e.g!==last){html+=`<div class="sec">${e.g}</div>`;last=e.g}
    const fields=(e.params||[]).map(pr=>`<div class="field"><label>${pr.n}${pr.path?' (path)':''}</label><input id="f${i}_${pr.n}" value="${(pr.v||'').replace(/"/g,'&quot;')}"/></div>`).join('');
    html+=`<div class="ep" id="ep${i}">
      <div class="ep-h" onclick="tog(${i})"><span class="m">GET</span><span class="p">${e.p}</span><span class="arrow">›</span></div>
      <div class="ep-b">
        <div class="how">${e.how}</div>
        <div class="fields">${fields}</div>
        <div class="actions">
          <button class="btn" onclick="run(${i})">Try it</button>
          <button class="ghost" onclick="copyUrl(${i})">Copy URL</button>
        </div>
        <div class="status" id="st${i}"></div>
        <pre id="o${i}">Ready</pre>
      </div></div>`;
  });
  document.getElementById('list').innerHTML=html||'<div class="hint-box">No endpoints match filter.</div>';
}
function setG(g){activeG=g;render()}
function filt(){render()}
function tog(i){const el=document.getElementById('ep'+i);const o=el.classList.contains('open');
  document.querySelectorAll('.ep').forEach(e=>e.classList.remove('open')); if(!o) el.classList.add('open')}
function copyUrl(i){navigator.clipboard.writeText(buildUrl(E[i],i))}
async function run(i){
  const u=buildUrl(E[i],i); const out=document.getElementById('o'+i); const st=document.getElementById('st'+i);
  out.textContent='Loading…'; st.textContent='';
  const t0=performance.now();
  try{
    const r=await fetch(u); const t=await r.text();
    let p=t; try{p=JSON.stringify(JSON.parse(t),null,2)}catch(_){}
    st.textContent=r.status+' · '+Math.round(performance.now()-t0)+' ms';
    out.textContent=p.slice(0,16000);
  }catch(e){st.textContent='error'; out.textContent=String(e)}
}
render();
</script>
</body></html>
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
