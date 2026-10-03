# StreamHub API v8.1.1 — creator: shawon
from __future__ import annotations

import asyncio, base64, hashlib, hmac, json, random, re, time, uuid
from typing import Any, Dict, List, Optional
from urllib.parse import parse_qsl, urljoin, urlparse, quote

import httpx
from bs4 import BeautifulSoup
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse

CREATOR, VERSION = "shawon", "8.1.1"
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


@app.get("/mb/cdn/{subject_id}", tags=["MovieBox"])
async def mb_cdn(subject_id: str, se: Optional[int] = None, ep: Optional[int] = None):
    """All MovieBox CDN links: play MP4, DASH (with cookie), resource mirrors."""
    tok = await _mb_session()
    if se is not None and ep is not None:
        play_path = f"/wefeed-mobile-bff/subject-api/play-info/v2?subjectId={subject_id}&se={se}&ep={ep}"
        res_path = f"/wefeed-mobile-bff/subject-api/resource?subjectId={subject_id}&se={se}&ep={ep}&page=1&perPage=20"
    else:
        play_path = f"/wefeed-mobile-bff/subject-api/play-info/v2?subjectId={subject_id}"
        res_path = f"/wefeed-mobile-bff/subject-api/resource?subjectId={subject_id}&page=1&perPage=20"
    play_data = await _mb_request("GET", play_path, token=tok)
    res_data = await _mb_request("GET", res_path, token=tok)
    inner = _mb_unwrap(play_data)
    streams = _mb_streams(play_data)
    mp4s = []
    dashes = []
    for s in streams:
        if s.get("kind") == "dash":
            dashes.append(s)
        else:
            mp4s.append({
                "url": s.get("url"), "quality": s.get("quality"), "codec": s.get("codec"),
                "format": s.get("format") or "MP4", "size": s.get("size"), "cookie": s.get("cookie"),
                "kind": "mp4", "source": "play",
            })
    resources = []
    for item in (_mb_unwrap(res_data).get("list") or []):
        link = item.get("resourceLink") or item.get("url")
        if not link:
            continue
        resources.append({
            "url": link, "quality": item.get("resolution"), "codec": item.get("codecName"),
            "size": item.get("size"), "title": item.get("title"), "sourceUrl": item.get("sourceUrl"),
            "kind": "mp4" if ".mp4" in str(link).lower() else "link", "source": "resource",
        })
        if link not in [m.get("url") for m in mp4s]:
            mp4s.append({
                "url": link, "quality": item.get("resolution"), "codec": item.get("codecName"),
                "size": item.get("size"), "kind": "mp4" if ".mp4" in str(link).lower() else "link",
                "source": "resource",
            })
    # unique mp4 by url
    seen = set()
    uniq_mp4 = []
    for m in mp4s:
        u = m.get("url")
        if not u or u in seen:
            continue
        seen.add(u)
        uniq_mp4.append(m)
    return ok({
        "subject_id": subject_id, "title": inner.get("title"),
        "mp4": uniq_mp4, "dash": dashes, "resources": resources,
        "mp4_count": len(uniq_mp4), "dash_count": len(dashes),
        "displayResolutions": inner.get("displayResolutions"),
    }, provider="moviebox", level="primary", endpoint="cdn",
       note="mp4 = direct CDN; dash needs Cookie header from each item")

@app.get("/mb/mp4/{subject_id}", tags=["MovieBox"])
async def mb_mp4(subject_id: str, se: Optional[int] = None, ep: Optional[int] = None):
    """Only direct MP4 CDN URLs (macdn / resource)."""
    data = await mb_cdn(subject_id, se=se, ep=ep)
    payload = data.get("data") or {}
    only = [x for x in (payload.get("mp4") or []) if x.get("url")]
    return ok({
        "subject_id": subject_id, "title": payload.get("title"),
        "urls": [x["url"] for x in only], "items": only, "count": len(only),
    }, provider="moviebox", level="primary", endpoint="mp4")

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
        r'https://macdn\.aoneroom\.com/[^"\'\s<>]+\.mp4[^"\'\s<>]*',
        r'https://sbcdn\d*\.hakunaymatata\.com/[^"\'\s<>]+',
        r'https://[^"\'\s<>]+\.(?:mp4|mkv|m3u8)(?:\?[^"\'\s<>]*)?',
        r'https://cdn\d*\.[^"\'\s<>]+/(?:[^"\'\s<>]+\.(?:mp4|mkv))',
        r'https://[^"\'\s<>]*hubcloud[^"\'\s<>]*/(?:file|drive|download)[^"\'\s<>]*',
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


@app.get("/tools/mp4", tags=["Tools"])
async def tools_mp4(url: str = Query(..., description="Any page or media URL")):
    """Extract all MP4 / MKV / M3U8 / known CDN links from a page or return direct media URL."""
    u = url.strip()
    low = u.lower()
    direct = []
    if any(low.endswith(ext) or ext in low for ext in (".mp4", ".mkv", ".m3u8", ".mpd")):
        direct.append({"url": u, "kind": "direct", "format": low.rsplit(".", 1)[-1][:4]})
    if "pixeldrain." in low:
        direct.append({"url": _pixeldrain_api(u) or u, "kind": "direct", "format": "file"})
    page_links = []
    if not direct or "hubcloud" in low or "greenmotors" in low or "http" in low:
        try:
            if "greenmotors." in low:
                page_links = await _resolve_greenmotors(u)
            elif "hubcloud." in low:
                page_links = await _resolve_hubcloud(u)
            elif "hubdrive." in low:
                page_links = await _resolve_hubdrive(u)
            else:
                async with _client(30) as client:
                    r = await client.get(u, headers={"User-Agent": UA, "Referer": "https://www.google.com/"})
                    for found in _extract_cdn_urls(r.text):
                        page_links.append({"url": found, "kind": "extracted", "label": "CDN"})
                    for m in re.findall(r'https?://[^\s"\'<>]+\.(?:mp4|mkv|m3u8)(?:\?[^\s"\'<>]*)?', r.text):
                        page_links.append({"url": m.replace("&amp;", "&"), "kind": "media", "label": "file"})
        except Exception as e:
            return fail(str(e), input=u, provider="tools", endpoint="mp4")
    all_links = direct + page_links
    seen = set()
    uniq = []
    for x in all_links:
        uu = x.get("url") if isinstance(x, dict) else x
        if not uu or uu in seen:
            continue
        seen.add(uu)
        uniq.append(x if isinstance(x, dict) else {"url": uu, "kind": "link"})
    return ok({"input": u, "links": uniq, "count": len(uniq)}, provider="tools", level="tool", endpoint="mp4")

@app.get("/tools/cdn-types", tags=["Tools"])
async def tools_cdn_types():
    """List known CDN host patterns this API can resolve."""
    return ok({
        "moviebox_mp4": ["macdn.aoneroom.com"],
        "moviebox_dash": ["sbcdn*.hakunaymatata.com", "Edge-Cache-Cookie required"],
        "hubcloud": ["*.r2.cloudflarestorage.com", "gpdl.hubcloud.ist", "googleusercontent.com"],
        "pixeldrain": ["pixeldrain.com/api/file/{id}?download"],
        "hentaicity": ["hentaicity.com/flv/{folder}/{id}/*.mp4", "hls.hentaicity.com"],
        "iptv": ["*.m3u8 from iptv-org"],
    }, provider="tools", endpoint="cdn-types")


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

@app.get("/play", tags=["Aggregate"])
async def aggregate_play(q: str = Query(..., min_length=1)):
    """Search then return MovieBox CDN (mp4+dash) for first result + 4KHDHub paths."""
    out = {"query": q, "moviebox": None, "4khdhub": None}
    try:
        tok = await _mb_session()
        body = json.dumps({"keyword": q, "page": 1, "perPage": 5})
        data = await _mb_request("POST", "/wefeed-mobile-bff/subject-api/search/v2", body, token=tok)
        items = _mb_items_from_search(_mb_unwrap(data))
        if items:
            sid = items[0]["subjectId"]
            cdn = await mb_cdn(sid)
            out["moviebox"] = {"item": items[0], "cdn": cdn.get("data")}
    except Exception as e:
        out["moviebox"] = {"error": str(e)}
    try:
        async with _client() as client:
            r = await client.get(FK_BASE + "/", params={"s": q})
            cards = _fk_cards(r.text)[:5]
            out["4khdhub"] = {"items": cards, "hint": "Use /fk/stream?path={id}&resolve=true for R2/gpdl CDN"}
    except Exception as e:
        out["4khdhub"] = {"error": str(e)}
    return ok(out, provider="aggregate", level="primary", endpoint="play")


# ========== HindiAnime (hindianime.site) ==========
HA_BASE = "https://www.hindianime.site"
HA_STREAM = "https://stream.hindianime.site"

async def _ha_get(path: str, params: dict = None, host: str = None):
    host = host or HA_BASE
    async with _client(35) as client:
        r = await client.get(
            urljoin(host, path),
            params=params or {},
            headers={
                "User-Agent": UA,
                "Accept": "application/json,*/*",
                "Referer": HA_BASE + "/",
                "Origin": HA_BASE,
            },
        )
        ct = r.headers.get("content-type") or ""
        if "json" in ct:
            try:
                return r.json()
            except Exception:
                return {"raw": r.text[:2000]}
        txt = r.text.strip()
        if txt.startswith("{") or txt.startswith("["):
            try:
                return json.loads(txt)
            except Exception:
                pass
        return {"ok_http": r.status_code, "html": True, "raw": txt[:400]}

def _ha_abs_stream(url: Optional[str]) -> Optional[str]:
    if not url or not isinstance(url, str):
        return None
    url = url.strip()
    if url.startswith("<iframe"):
        m = re.search(r"src=['\"]([^'\"]+)", url)
        url = m.group(1) if m else url
    if url.startswith("/api/proxy/"):
        return HA_STREAM + url
    if url.startswith("/api/"):
        return HA_BASE + url
    if url.startswith("/"):
        return HA_BASE + url
    return url

def _ha_kind(url: str, declared: str = None) -> str:
    u = (url or "").lower()
    d = (declared or "").lower()
    if "get_video" in u or u.endswith(".mp4") or ".mp4?" in u:
        return "mp4"
    if "p2p-master" in u or "/api/proxy/" in u:
        return "hls_proxy"
    if ".m3u8" in u or "master.m3u8" in u or "/hls" in u:
        return "hls"
    if any(x in u for x in ("streamtape.com/e/", "filesforever", "abyssplayer", "p2pplay", "embed")) or d == "iframe":
        return "iframe"
    if d == "direct":
        return "mp4"
    return d or "link"

def _ha_push(servers: list, seen: set, name: str, url: str, **extra):
    abs_u = _ha_abs_stream(url)
    if not abs_u or abs_u in seen:
        return
    # skip dead known p2p error pages as primary later; still list as proxy
    kind = _ha_kind(abs_u, extra.get("type"))
    seen.add(abs_u)
    item = {"name": name, "type": kind, "url": abs_u}
    for k in ("audio", "langLabel", "streamtapeId", "hash", "source"):
        if extra.get(k) is not None:
            item[k] = extra[k]
    servers.append(item)

async def _ha_probe(url: str) -> dict:
    """Quick health check — skip dead p2p/zephyrix proxies."""
    try:
        async with _client(12) as client:
            r = await client.get(
                url,
                headers={
                    "User-Agent": UA,
                    "Referer": HA_BASE + "/watch",
                    "Origin": HA_BASE,
                    "Accept": "*/*",
                },
            )
            ct = (r.headers.get("content-type") or "").lower()
            body = r.text[:200] if r.content else ""
            dead = (
                r.status_code in (404, 500, 502, 503)
                or "p2pplay returned" in body
                or "temporarily offline" in body
                or "access denied" in body.lower()
                or "403 forbidden" in body.lower()
            )
            ok_media = (
                r.status_code in (200, 206)
                and not dead
                and (
                    "mpegurl" in ct
                    or "application/vnd.apple" in ct
                    or "video/" in ct
                    or "octet-stream" in ct
                    or body.strip().startswith("#EXTM3U")
                    or (len(r.content) > 500 and "text/html" not in ct)
                )
            )
            return {"url": url, "status": r.status_code, "alive": ok_media and not dead, "dead": dead, "content_type": ct}
    except Exception as e:
        return {"url": url, "status": 0, "alive": False, "dead": True, "error": str(e)[:80]}

def _ha_collect_payload(payload: dict, servers: list, seen: set, source: str):
    if not isinstance(payload, dict) or payload.get("html"):
        return
    su = payload.get("streamUrl")
    if su:
        _ha_push(servers, seen, payload.get("server") or "Primary", su, type=payload.get("type"), source=source,
                 hash=payload.get("videoHash"))
    if payload.get("embedUrl"):
        _ha_push(servers, seen, "Embed", payload["embedUrl"], type="iframe", source=source)
    for s in payload.get("servers") or []:
        if not isinstance(s, dict):
            continue
        _ha_push(
            servers, seen,
            s.get("name") or "Server",
            s.get("url") or s.get("streamUrl") or "",
            type=s.get("type"), audio=s.get("audio"), langLabel=s.get("langLabel"),
            streamtapeId=s.get("streamtapeId"), hash=s.get("hash") or payload.get("videoHash"),
            source=source,
        )
    # hash-only → try both proxy paths
    vh = payload.get("videoHash")
    if vh and isinstance(vh, str):
        for path, name in (
            (f"/api/proxy/master.m3u8?hash={vh}", "Zephyrix HLS"),
            (f"/api/proxy/p2p-master.m3u8?hash={vh}", "P2P HLS"),
        ):
            _ha_push(servers, seen, name, path, type="hls_proxy", hash=vh, source=source)

@app.get("/ha/catalog", tags=["HindiAnime"])
async def ha_catalog(kind: str = Query("all", description="all|movies|series")):
    """Full catalog from hindianime.site — movies + series with posters and watch links."""
    data = await _ha_get("/api/catalog.json")
    if not isinstance(data, dict) or data.get("html"):
        return fail("catalog failed", provider="hindianime")
    movies = data.get("movies") or []
    series = data.get("series") or []
    if kind == "movies":
        return ok({"movies": movies, "count": len(movies)}, provider="hindianime", endpoint="catalog")
    if kind == "series":
        return ok({"series": series, "count": len(series)}, provider="hindianime", endpoint="catalog")
    return ok({
        "totalMovies": data.get("totalMovies"), "totalSeries": data.get("totalSeries"),
        "movies": movies, "series": series, "count": len(movies) + len(series),
    }, provider="hindianime", level="live", endpoint="catalog")

@app.get("/ha/home", tags=["HindiAnime"])
async def ha_home():
    """Home sections: topAiring, mostPopular, latestEpisodes, latestMovies, genres…"""
    data = await _ha_get("/api/home-sections")
    return ok(data, provider="hindianime", level="live", endpoint="home")

@app.get("/ha/hero", tags=["HindiAnime"])
async def ha_hero():
    data = await _ha_get("/api/hero.json")
    return ok(data, provider="hindianime", level="live", endpoint="hero")

@app.get("/ha/sections", tags=["HindiAnime"])
async def ha_sections():
    data = await _ha_get("/api/sections.json")
    return ok(data, provider="hindianime", level="live", endpoint="sections")

@app.get("/ha/spotlights", tags=["HindiAnime"])
async def ha_spotlights():
    data = await _ha_get("/api/spotlights.json")
    return ok(data, provider="hindianime", level="live", endpoint="spotlights")

@app.get("/ha/top10", tags=["HindiAnime"])
async def ha_top10():
    data = await _ha_get("/api/top10")
    return ok(data, provider="hindianime", level="live", endpoint="top10")

@app.get("/ha/browse", tags=["HindiAnime"])
async def ha_browse():
    data = await _ha_get("/api/browse-index.json")
    return ok(data, provider="hindianime", level="live", endpoint="browse")

@app.get("/ha/search", tags=["HindiAnime"])
async def ha_search(q: str = Query(..., min_length=1)):
    """Search catalog by title / id / genre."""
    data = await _ha_get("/api/catalog.json")
    if not isinstance(data, dict) or data.get("html"):
        return fail("catalog failed", provider="hindianime", endpoint="search")
    ql = q.lower().strip()
    items = []
    for bucket, typ in ((data.get("movies") or [], "movie"), (data.get("series") or [], "series")):
        for a in bucket:
            hay = f"{a.get('title','')} {a.get('id','')} {' '.join(a.get('genres') or [])}".lower()
            if ql in hay:
                items.append({**a, "kind": typ})
    return ok({"items": items, "count": len(items), "query": q}, provider="hindianime", level="live", endpoint="search")

@app.get("/ha/details", tags=["HindiAnime"])
async def ha_details(url: Optional[str] = None, id: Optional[str] = None):
    """Episodes + server list. Pass catalog id=slug OR url=watchanimeworld link."""
    if url is not None and not isinstance(url, str):
        url = None
    if id is not None and not isinstance(id, str):
        id = None
    link, meta = url, None
    if not link and id:
        cat = await _ha_get("/api/catalog.json")
        if isinstance(cat, dict) and not cat.get("html"):
            for a in (cat.get("movies") or []) + (cat.get("series") or []):
                if a.get("id") == id:
                    link, meta = a.get("link"), a
                    break
    if not link:
        raise HTTPException(400, detail=fail("url or id required", provider="hindianime"))
    data = await _ha_get("/api/details", {"url": link})
    if isinstance(data, dict) and meta:
        data.setdefault("catalog", meta)
    return ok(data, provider="hindianime", level="live", endpoint="details", link=link)

@app.get("/ha/episodes", tags=["HindiAnime"])
async def ha_episodes(url: Optional[str] = None, id: Optional[str] = None, season: Optional[int] = None):
    """Episode list for a title (optional season filter)."""
    det = await ha_details(url=url, id=id)
    data = det.get("data") or {}
    seasons = data.get("seasons") or {}
    if season is not None:
        eps = seasons.get(str(season)) or []
        return ok({"season": season, "episodes": eps, "count": len(eps)}, provider="hindianime", endpoint="episodes")
    flat = []
    for sk, eps in (seasons.items() if isinstance(seasons, dict) else []):
        for e in eps or []:
            flat.append(e)
    return ok({
        "title": data.get("title"), "type": data.get("type"),
        "availableSeasons": data.get("availableSeasons"),
        "seasons": seasons, "episodes": flat, "count": len(flat),
    }, provider="hindianime", endpoint="episodes")

@app.get("/ha/stream", tags=["HindiAnime"])
async def ha_stream(
    title: Optional[str] = None,
    url: Optional[str] = None,
    season: int = 1,
    episode: int = 1,
    lang: str = "Hindi",
    is_movie: bool = False,
    id: Optional[str] = None,
    probe: bool = Query(True, description="Health-check links; drops dead p2p/zephyrix"),
):
    """
    Resolve playable servers for an episode/movie.
    - Merges resolve by episode url + title + stream host
    - Prefers external HLS (non-proxy) when alive
    - Drops p2p-master links that return HTTP 404/500
    - Streamtape get_video is IP-locked on many servers → listed as browser/mp4_attempt
    """
    ep_meta = None
    if id and not title:
        det = await ha_details(id=id)
        data = det.get("data") or {}
        title = data.get("title") or id
        is_movie = str(data.get("type") or "").upper() == "MOVIE"
        seasons = data.get("seasons") or {}
        eps = seasons.get(str(season)) or []
        for e in eps:
            if int(e.get("episode") or 0) == int(episode):
                url = e.get("url") or url
                ep_meta = e
                break
        if not url and eps:
            url = eps[0].get("url")
            ep_meta = eps[0]

    servers: list = []
    seen: set = set()

    # 1) resolve by episode page url (often real external m3u8)
    if url:
        p_url = await _ha_get("/api/resolve-stream", {"url": url})
        _ha_collect_payload(p_url if isinstance(p_url, dict) else {}, servers, seen, "resolve-url")
        p_stream = await _ha_get("/api/resolve-stream", {"url": url}, host=HA_STREAM)
        _ha_collect_payload(p_stream if isinstance(p_stream, dict) else {}, servers, seen, "stream-host-url")

    # 2) resolve by title (streamtape + mirrors)
    if title:
        p_title = await _ha_get("/api/resolve-stream", {
            "title": title, "season": str(season), "episode": str(episode),
            "lang": lang, "isMovie": "true" if is_movie else "false",
        })
        _ha_collect_payload(p_title if isinstance(p_title, dict) else {}, servers, seen, "resolve-title")
        p_title2 = await _ha_get("/api/resolve-stream", {
            "title": title, "season": str(season), "episode": str(episode),
            "lang": lang, "isMovie": "true" if is_movie else "false",
        }, host=HA_STREAM)
        _ha_collect_payload(p_title2 if isinstance(p_title2, dict) else {}, servers, seen, "stream-host-title")

    # 3) episode servers from details (abyss / filesforever / p2p iframe)
    if ep_meta:
        for s in ep_meta.get("servers") or []:
            if not isinstance(s, dict):
                continue
            raw = s.get("url") or ""
            if raw.startswith("/api/clean-abyss"):
                # clean-abyss is an HTML player wrapper — expose abyss + stream clean URL
                m = re.search(r"[?&]v=([^&]+)", raw)
                if m:
                    vid = m.group(1)
                    _ha_push(servers, seen, "Abyss Player", f"https://play.abyssplayer.com/{vid}", type="iframe", source="details")
                    _ha_push(servers, seen, "Clean Abyss page", HA_STREAM + f"/api/clean-abyss?v={vid}", type="iframe", source="details")
            else:
                _ha_push(servers, seen, s.get("name") or "Detail server", raw, type=s.get("type"), audio=s.get("audio"), source="details")
        if ep_meta.get("abyssId"):
            vid = ep_meta["abyssId"]
            _ha_push(servers, seen, "Abyss", f"https://play.abyssplayer.com/{vid}", type="iframe", source="details")

    # probe + rank
    probes = []
    if probe:
        candidates = [s for s in servers if s.get("type") in ("hls", "hls_proxy", "mp4") and s.get("url")]
        results = await asyncio.gather(*[_ha_probe(s["url"]) for s in candidates]) if candidates else []
        by_url = {r["url"]: r for r in results}
        probes = results
        for s in servers:
            pr = by_url.get(s["url"])
            if pr:
                s["alive"] = pr.get("alive")
                s["http_status"] = pr.get("status")
                if pr.get("dead"):
                    s["dead"] = True

    def rank(s):
        # lower is better
        alive = s.get("alive") is True
        dead = s.get("dead") is True
        t = s.get("type")
        u = s.get("url") or ""
        if dead:
            return 90
        if alive and t == "hls" and "proxy" not in u:
            return 0
        if alive and t == "mp4":
            return 1
        if alive and t == "hls_proxy":
            return 2
        if t == "iframe":
            return 3
        if t == "hls" and "proxy" not in u:
            return 4
        if t == "mp4":
            return 5
        if t == "hls_proxy":
            return 6
        return 10

    ranked = sorted(servers, key=rank)
    # working direct = alive hls/mp4, or non-proxy hls even if probe skipped
    direct = []
    for s in ranked:
        if s.get("dead"):
            continue
        if s.get("type") in ("hls", "mp4") and "p2p-master" not in (s.get("url") or ""):
            if probe and s.get("type") in ("hls", "mp4") and s.get("alive") is False:
                continue
            direct.append(s)
        elif s.get("type") == "hls_proxy" and s.get("alive") is True:
            direct.append(s)

    return ok({
        "title": title,
        "season": season,
        "episode": episode,
        "lang": lang,
        "episode_url": url,
        "servers": ranked,
        "direct": direct,
        "direct_count": len(direct),
        "embeds": [s for s in ranked if s.get("type") == "iframe"],
        "probes": probes if probe else [],
        "how": (
            "1) Use data.direct[] first (alive HLS/MP4). "
            "2) Skip URLs with dead=true or p2pplay 404. "
            "3) Streamtape get_video often IP-locked — open iframe embeds in browser/WebView. "
            "4) Set probe=false to skip health checks."
        ),
    }, provider="hindianime", level="live", endpoint="stream", creator="shawon")

@app.get("/ha/trailer", tags=["HindiAnime"])
async def ha_trailer(title: str = Query(...)):
    data = await _ha_get("/api/find-trailer", {"title": title})
    return ok(data, provider="hindianime", level="live", endpoint="trailer")

@app.get("/ha/proxy", tags=["HindiAnime"])
async def ha_proxy(hash: str = Query(..., description="videoHash from resolve-stream")):
    """Build HLS proxy URLs. Many hashes return p2p 404 or zephyrix offline — always probe."""
    urls = [
        f"{HA_STREAM}/api/proxy/master.m3u8?hash={hash}",
        f"{HA_STREAM}/api/proxy/p2p-master.m3u8?hash={hash}",
    ]
    probes = await asyncio.gather(*[_ha_probe(u) for u in urls])
    alive = [p for p in probes if p.get("alive")]
    return ok({
        "hash": hash,
        "hls_urls": urls,
        "probes": probes,
        "alive": alive,
        "how": "Only use alive[].url. p2pplay HTTP 404 means that hash is dead upstream.",
    }, provider="hindianime", endpoint="proxy")


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


# ========== Kartoons (api.kartoons.to) ==========
# Public (no login): shows, movies, episodes meta, comments, app/update, random-episode, search via ?search=
# Challenge (Turnstile + POW): /links, popularity, schedules, community, collections, ads, suggestions
# Auth (Bearer token): ratings POST, watchlist, user/*, stremio, notifications
KT_API = "https://api.kartoons.to/api"
KT_ORIGIN = "https://kartoons.to"

def _kt_headers(auth: Optional[str] = None, extra: Optional[dict] = None) -> dict:
    h = {
        "User-Agent": UA,
        "Origin": KT_ORIGIN,
        "Referer": KT_ORIGIN + "/",
        "Accept": "application/json, text/plain, */*",
    }
    if auth:
        token = auth if auth.lower().startswith("bearer ") else f"Bearer {auth}"
        h["Authorization"] = token
    if extra:
        h.update(extra)
    return h

async def _kt_req(method: str, path: str, params: dict = None, json_body: dict = None, auth: Optional[str] = None, timeout: float = 30.0):
    url = KT_API + (path if path.startswith("/") else "/" + path)
    async with _client(timeout) as client:
        headers = _kt_headers(auth)
        if json_body is not None:
            headers["Content-Type"] = "application/json"
        if method.upper() == "GET":
            r = await client.get(url, params=params or {}, headers=headers)
        elif method.upper() == "DELETE":
            r = await client.request("DELETE", url, params=params or {}, headers=headers)
        else:
            r = await client.post(url, params=params or {}, json=json_body, headers=headers)
        try:
            data = r.json()
        except Exception:
            data = {"raw": r.text[:2000]}
        return r.status_code, data

def _kt_wrap(status: int, data, endpoint: str, access: str = "public", **extra):
    challenge = isinstance(data, dict) and (data.get("challenge_required") or "Are you a human" in str(data.get("message") or ""))
    auth_needed = status in (401, 403) and not challenge and isinstance(data, dict) and (
        "auth" in str(data.get("message") or "").lower() or "login" in str(data.get("message") or "").lower() or "token" in str(data.get("message") or "").lower()
    )
    if challenge:
        return fail(
            data.get("message") or "Human challenge required (Cloudflare Turnstile)",
            provider="kartoons", endpoint=endpoint, access=access, status=status,
            challenge_required=True,
            how="Call GET /kt/challenge/pow?content=episode:{episodeId} then POST /kt/challenge/verify with turnstile_token + nonce + solution, then retry /kt/episode/{id}/links with same cookies is not enough — pass turnstile_token query to /kt/stream.",
            data=data, **extra,
        )
    if status >= 400:
        return fail(
            (data.get("message") if isinstance(data, dict) else None) or f"HTTP {status}",
            provider="kartoons", endpoint=endpoint, access=access, status=status,
            auth_required=auth_needed or access == "auth", data=data, **extra,
        )
    payload = data.get("data") if isinstance(data, dict) and "data" in data else data
    out = ok(payload, provider="kartoons", level="live", endpoint=endpoint, access=access, **extra)
    if isinstance(data, dict):
        for k in ("pagination", "season", "show", "related", "success", "message", "version", "versionCode", "downloadUrl"):
            if k in data and k not in out:
                out[k] = data[k]
    return out

def _kt_leading_zero_bits(digest: bytes) -> int:
    n = 0
    for b in digest:
        if b == 0:
            n += 8
        else:
            n += 8 - b.bit_length()
            break
    return n

def _kt_solve_pow(nonce: str, bits: int, max_iters: int = 8_000_000) -> Optional[str]:
    target = int(bits)
    for i in range(max_iters):
        sol = str(i)
        h = hashlib.sha256(f"{nonce}:{sol}".encode()).digest()
        if _kt_leading_zero_bits(h) >= target:
            return sol
    return None

# ---- Public catalog ----
@app.get("/kt/shows", tags=["Kartoons-Public"])
async def kt_shows(page: int = 1, limit: int = 20, search: Optional[str] = None):
    """List / search shows. Public — no login. Optional search= keyword."""
    params = {"page": page, "limit": limit}
    if search:
        params["search"] = search
    st, data = await _kt_req("GET", "/shows", params=params)
    return _kt_wrap(st, data, "shows", "public", how="Use item.slug with /kt/show/{slug}")

@app.get("/kt/shows/featured", tags=["Kartoons-Public"])
async def kt_shows_featured():
    st, data = await _kt_req("GET", "/shows/featured")
    return _kt_wrap(st, data, "shows_featured", "public")

@app.get("/kt/show/{slug}", tags=["Kartoons-Public"])
async def kt_show(slug: str):
    """Show details + seasons list. Public."""
    st, data = await _kt_req("GET", f"/shows/{slug}")
    return _kt_wrap(st, data, "show", "public", how="Seasons have slug e.g. season-1 → /kt/show/{slug}/season/season-1/episodes")

@app.get("/kt/show/{slug}/season/{season_slug}/episodes", tags=["Kartoons-Public"])
async def kt_season_episodes(slug: str, season_slug: str, page: int = 1, limit: int = 30):
    """Paginated episodes for a season. Public. Copy episode _id for /kt/episode/{id}/links or /kt/stream."""
    st, data = await _kt_req("GET", f"/shows/{slug}/season/{season_slug}/episodes", {"page": page, "limit": limit})
    return _kt_wrap(st, data, "season_episodes", "public")

@app.get("/kt/show/{slug}/season/{season_slug}/all-episodes", tags=["Kartoons-Public"])
async def kt_all_episodes(slug: str, season_slug: str):
    st, data = await _kt_req("GET", f"/shows/{slug}/season/{season_slug}/all-episodes")
    return _kt_wrap(st, data, "all_episodes", "public")

@app.get("/kt/show/{slug}/random-episode", tags=["Kartoons-Public"])
async def kt_random_episode(slug: str):
    st, data = await _kt_req("GET", f"/shows/{slug}/random-episode")
    return _kt_wrap(st, data, "random_episode", "public")

@app.get("/kt/episode/{episode_id}", tags=["Kartoons-Public"])
async def kt_episode(episode_id: str):
    """Episode metadata (title, duration, links_count). Public. Stream URLs need /kt/stream."""
    st, data = await _kt_req("GET", f"/shows/episode/{episode_id}")
    return _kt_wrap(st, data, "episode", "public")

@app.get("/kt/movies", tags=["Kartoons-Public"])
async def kt_movies(page: int = 1, limit: int = 20, search: Optional[str] = None):
    params = {"page": page, "limit": limit}
    if search:
        params["search"] = search
    st, data = await _kt_req("GET", "/movies", params=params)
    return _kt_wrap(st, data, "movies", "public")

@app.get("/kt/movies/featured", tags=["Kartoons-Public"])
async def kt_movies_featured():
    st, data = await _kt_req("GET", "/movies/featured")
    return _kt_wrap(st, data, "movies_featured", "public")

@app.get("/kt/movie/{movie_id}", tags=["Kartoons-Public"])
async def kt_movie(movie_id: str):
    """Movie details by slug or id. Public."""
    st, data = await _kt_req("GET", f"/movies/{movie_id}")
    return _kt_wrap(st, data, "movie", "public")

@app.get("/kt/show/{slug}/comments", tags=["Kartoons-Public"])
async def kt_show_comments(slug: str, limit: int = 20, count_only: bool = False):
    params = {"limit": limit}
    if count_only:
        params["count_only"] = "true"
    st, data = await _kt_req("GET", f"/shows/{slug}/comments", params)
    return _kt_wrap(st, data, "show_comments", "public")

@app.get("/kt/episode/{episode_id}/comments", tags=["Kartoons-Public"])
async def kt_episode_comments(episode_id: str, limit: int = 20):
    st, data = await _kt_req("GET", f"/shows/episode/{episode_id}/comments", {"limit": limit})
    return _kt_wrap(st, data, "episode_comments", "public")

@app.get("/kt/app/update", tags=["Kartoons-Public"])
async def kt_app_update():
    st, data = await _kt_req("GET", "/app/update")
    return _kt_wrap(st, data, "app_update", "public")

# ---- Challenge / stream (Turnstile) ----
@app.get("/kt/challenge/pow", tags=["Kartoons-Challenge"])
async def kt_challenge_pow(content: str = Query(..., description="e.g. episode:{episodeId} or movie:{slug}")):
    """Get POW challenge. Solve: sha256(nonce:solution) must have >= bits leading zero bits."""
    st, data = await _kt_req("GET", "/challenge/pow", {"content": content})
    out = _kt_wrap(st, data, "challenge_pow", "challenge")
    if st == 200 and isinstance(data, dict) and data.get("data"):
        out["how"] = "Solve POW locally or call /kt/challenge/solve?content=... then POST /kt/challenge/verify with turnstile_token from Cloudflare widget on kartoons.to"
    return out

@app.get("/kt/challenge/solve", tags=["Kartoons-Challenge"])
async def kt_challenge_solve(content: str = Query(...)):
    """Fetch POW + solve nonce (bits=16 is fast). Still need turnstile_token for verify."""
    st, data = await _kt_req("GET", "/challenge/pow", {"content": content})
    if st != 200 or not isinstance(data, dict) or not data.get("data"):
        return _kt_wrap(st, data, "challenge_solve", "challenge")
    powd = data["data"]
    sol = _kt_solve_pow(powd.get("nonce") or "", int(powd.get("bits") or 16))
    return ok({
        "content": content,
        "nonce": powd.get("nonce"),
        "solution": sol,
        "bits": powd.get("bits"),
        "algo": powd.get("algo"),
        "ttl": powd.get("ttl"),
        "turnstile_required": True,
        "how": "POST /kt/challenge/verify with body {turnstile_token, content, nonce, solution}. Get turnstile_token from browser on kartoons.to.",
    }, provider="kartoons", endpoint="challenge_solve", access="challenge")

@app.post("/kt/challenge/verify", tags=["Kartoons-Challenge"])
async def kt_challenge_verify(
    turnstile_token: str = Query(...),
    content: str = Query(...),
    nonce: str = Query(...),
    solution: str = Query(...),
):
    """Verify Turnstile + POW. Upstream requires real Cloudflare turnstile_token."""
    body = {"turnstile_token": turnstile_token, "content": content, "nonce": nonce, "solution": solution}
    st, data = await _kt_req("POST", "/challenge/verify", json_body=body)
    return _kt_wrap(st, data, "challenge_verify", "challenge")

@app.get("/kt/episode/{episode_id}/links", tags=["Kartoons-Challenge"])
async def kt_episode_links(episode_id: str, turnstile_token: Optional[str] = None):
    """Stream/CDN link list for episode. Usually challenge_required without valid Turnstile session."""
    # Optional: try POW+verify if token given
    if turnstile_token:
        content = f"episode:{episode_id}"
        st0, pow_raw = await _kt_req("GET", "/challenge/pow", {"content": content})
        if st0 == 200 and isinstance(pow_raw, dict) and pow_raw.get("data"):
            powd = pow_raw["data"]
            sol = _kt_solve_pow(powd.get("nonce") or "", int(powd.get("bits") or 16))
            if sol:
                await _kt_req("POST", "/challenge/verify", json_body={
                    "turnstile_token": turnstile_token,
                    "content": content,
                    "nonce": powd.get("nonce"),
                    "solution": sol,
                })
    st, data = await _kt_req("GET", f"/shows/episode/{episode_id}/links")
    return _kt_wrap(st, data, "episode_links", "challenge", how="Without Turnstile → 403. Pass turnstile_token from browser, or open embeds on site.")

@app.get("/kt/movie/{movie_id}/links", tags=["Kartoons-Challenge"])
async def kt_movie_links(movie_id: str, turnstile_token: Optional[str] = None):
    if turnstile_token:
        content = f"movie:{movie_id}"
        st0, pow_raw = await _kt_req("GET", "/challenge/pow", {"content": content})
        if st0 == 200 and isinstance(pow_raw, dict) and pow_raw.get("data"):
            powd = pow_raw["data"]
            sol = _kt_solve_pow(powd.get("nonce") or "", int(powd.get("bits") or 16))
            if sol:
                await _kt_req("POST", "/challenge/verify", json_body={
                    "turnstile_token": turnstile_token, "content": content,
                    "nonce": powd.get("nonce"), "solution": sol,
                })
    st, data = await _kt_req("GET", f"/movies/{movie_id}/links")
    return _kt_wrap(st, data, "movie_links", "challenge")

@app.get("/kt/stream", tags=["Kartoons-Challenge"])
async def kt_stream(
    episode_id: Optional[str] = None,
    movie_id: Optional[str] = None,
    turnstile_token: Optional[str] = None,
):
    """
    Convenience stream resolver.
    1) Loads episode/movie meta (public)
    2) Tries /links (needs Turnstile on this IP)
    3) Returns normalized servers[] when available
    """
    if not episode_id and not movie_id:
        raise HTTPException(400, detail=fail("episode_id or movie_id required", provider="kartoons"))
    meta = None
    links_data = None
    status = 0
    if episode_id:
        st_m, meta_raw = await _kt_req("GET", f"/shows/episode/{episode_id}")
        meta = meta_raw.get("data") if isinstance(meta_raw, dict) else meta_raw
        if turnstile_token:
            content = f"episode:{episode_id}"
            st0, pow_raw = await _kt_req("GET", "/challenge/pow", {"content": content})
            if st0 == 200 and isinstance(pow_raw, dict) and pow_raw.get("data"):
                powd = pow_raw["data"]
                sol = _kt_solve_pow(powd.get("nonce") or "", int(powd.get("bits") or 16))
                if sol:
                    await _kt_req("POST", "/challenge/verify", json_body={
                        "turnstile_token": turnstile_token, "content": content,
                        "nonce": powd.get("nonce"), "solution": sol,
                    })
        status, links_raw = await _kt_req("GET", f"/shows/episode/{episode_id}/links")
        links_data = links_raw
    else:
        st_m, meta_raw = await _kt_req("GET", f"/movies/{movie_id}")
        meta = meta_raw.get("data") if isinstance(meta_raw, dict) else meta_raw
        if turnstile_token:
            content = f"movie:{movie_id}"
            st0, pow_raw = await _kt_req("GET", "/challenge/pow", {"content": content})
            if st0 == 200 and isinstance(pow_raw, dict) and pow_raw.get("data"):
                powd = pow_raw["data"]
                sol = _kt_solve_pow(powd.get("nonce") or "", int(powd.get("bits") or 16))
                if sol:
                    await _kt_req("POST", "/challenge/verify", json_body={
                        "turnstile_token": turnstile_token, "content": content,
                        "nonce": powd.get("nonce"), "solution": sol,
                    })
        status, links_raw = await _kt_req("GET", f"/movies/{movie_id}/links")
        links_data = links_raw

    servers = []
    challenge = isinstance(links_data, dict) and links_data.get("challenge_required")
    raw_links = []
    if isinstance(links_data, dict) and not challenge:
        payload = links_data.get("data") if "data" in links_data else links_data
        if isinstance(payload, list):
            raw_links = payload
        elif isinstance(payload, dict):
            raw_links = payload.get("links") or payload.get("servers") or payload.get("sources") or []
            if not raw_links and payload.get("url"):
                raw_links = [payload]
    for item in raw_links or []:
        if not isinstance(item, dict):
            continue
        url = item.get("url") or item.get("link") or item.get("file") or item.get("src")
        if not url:
            continue
        kind = "hls" if ".m3u8" in str(url) else ("mp4" if ".mp4" in str(url) or "get_video" in str(url) else "link")
        if "embed" in str(url) or item.get("type") == "iframe":
            kind = "iframe"
        servers.append({
            "name": item.get("name") or item.get("label") or item.get("server") or "Server",
            "type": kind,
            "url": url,
            "quality": item.get("quality") or item.get("resolution"),
            "audio": item.get("audio") or item.get("lang"),
            "raw": item,
        })
    return ok({
        "meta": meta,
        "servers": servers,
        "direct": [s for s in servers if s["type"] in ("mp4", "hls")],
        "direct_count": len([s for s in servers if s["type"] in ("mp4", "hls")]),
        "links_http": status,
        "challenge_required": bool(challenge),
        "links_raw": links_data if challenge or status >= 400 else None,
        "how": (
            "Public: meta always. Stream CDN: needs Cloudflare Turnstile from browser on kartoons.to, "
            "then pass turnstile_token=... here. POW is auto-solved server-side."
        ),
    }, provider="kartoons", endpoint="stream", access="challenge" if challenge else "public")

# Challenge-gated catalog helpers (same upstream 403 without token)
@app.get("/kt/popularity/shows", tags=["Kartoons-Challenge"])
async def kt_pop_shows():
    st, data = await _kt_req("GET", "/popularity/shows")
    return _kt_wrap(st, data, "popularity_shows", "challenge")

@app.get("/kt/popularity/movies", tags=["Kartoons-Challenge"])
async def kt_pop_movies():
    st, data = await _kt_req("GET", "/popularity/movies")
    return _kt_wrap(st, data, "popularity_movies", "challenge")

@app.get("/kt/popularity/trending", tags=["Kartoons-Challenge"])
async def kt_pop_trending():
    st, data = await _kt_req("GET", "/popularity/trending")
    return _kt_wrap(st, data, "popularity_trending", "challenge")

@app.get("/kt/search/suggestions", tags=["Kartoons-Challenge"])
async def kt_search_suggestions(q: str, limit: int = 10):
    st, data = await _kt_req("GET", "/search/suggestions", {"q": q, "limit": limit})
    return _kt_wrap(st, data, "search_suggestions", "challenge", how="Public alternative: /kt/shows?search=q or /kt/movies?search=q")

@app.get("/kt/schedules/upcoming", tags=["Kartoons-Challenge"])
async def kt_schedules_upcoming():
    st, data = await _kt_req("GET", "/schedules/upcoming")
    return _kt_wrap(st, data, "schedules_upcoming", "challenge")

@app.get("/kt/schedules/weekly", tags=["Kartoons-Challenge"])
async def kt_schedules_weekly():
    st, data = await _kt_req("GET", "/schedules/weekly")
    return _kt_wrap(st, data, "schedules_weekly", "challenge")

@app.get("/kt/community/board", tags=["Kartoons-Challenge"])
async def kt_community_board():
    st, data = await _kt_req("GET", "/community/board")
    return _kt_wrap(st, data, "community_board", "challenge")

@app.get("/kt/community/posts", tags=["Kartoons-Challenge"])
async def kt_community_posts():
    st, data = await _kt_req("GET", "/community/posts")
    return _kt_wrap(st, data, "community_posts", "challenge")

@app.get("/kt/collections", tags=["Kartoons-Challenge"])
async def kt_collections():
    st, data = await _kt_req("GET", "/collections")
    return _kt_wrap(st, data, "collections", "challenge")

@app.get("/kt/suggestions/episode/{episode_id}", tags=["Kartoons-Challenge"])
async def kt_suggestions_episode(episode_id: str):
    st, data = await _kt_req("GET", f"/suggestions/episode/{episode_id}")
    return _kt_wrap(st, data, "suggestions_episode", "challenge")

# ---- Auth required (pass ?auth=TOKEN or Authorization header via auth query) ----
@app.get("/kt/auth/me", tags=["Kartoons-Auth"])
async def kt_auth_me(auth: str = Query(..., description="JWT / Bearer token from kartoons login")):
    st, data = await _kt_req("GET", "/auth/me", auth=auth)
    return _kt_wrap(st, data, "auth_me", "auth")

@app.post("/kt/auth/login", tags=["Kartoons-Auth"])
async def kt_auth_login(email: str = Query(...), password: str = Query(...)):
    st, data = await _kt_req("POST", "/auth/login", json_body={"email": email, "password": password})
    return _kt_wrap(st, data, "auth_login", "auth", how="Save returned token; pass as auth= to other /kt/auth and /kt/user routes")

@app.get("/kt/user/me", tags=["Kartoons-Auth"])
async def kt_user_me(auth: str = Query(...)):
    st, data = await _kt_req("GET", "/user/me", auth=auth)
    return _kt_wrap(st, data, "user_me", "auth")

@app.get("/kt/user/continue-watching", tags=["Kartoons-Auth"])
async def kt_continue_watching(auth: str = Query(...)):
    st, data = await _kt_req("GET", "/user/continue-watching", auth=auth)
    return _kt_wrap(st, data, "continue_watching", "auth")

@app.get("/kt/watchlist", tags=["Kartoons-Auth"])
async def kt_watchlist(auth: str = Query(...)):
    st, data = await _kt_req("GET", "/watchlist", auth=auth)
    return _kt_wrap(st, data, "watchlist", "auth")

@app.post("/kt/watchlist/add", tags=["Kartoons-Auth"])
async def kt_watchlist_add(auth: str = Query(...), content_type: str = Query("show"), content_id: str = Query(...), status: str = Query("plan_to_watch")):
    st, data = await _kt_req("POST", "/watchlist/add", params={"content_type": content_type, "content_id": content_id, "status": status}, auth=auth)
    return _kt_wrap(st, data, "watchlist_add", "auth")

@app.get("/kt/ratings/content/{ctype}/{cid}", tags=["Kartoons-Auth"])
async def kt_ratings_get(ctype: str, cid: str, auth: Optional[str] = None):
    st, data = await _kt_req("GET", f"/ratings/content/{ctype}/{cid}", auth=auth)
    return _kt_wrap(st, data, "ratings_get", "auth" if st in (401, 403) else "public")

@app.get("/kt/notifications", tags=["Kartoons-Auth"])
async def kt_notifications(auth: str = Query(...)):
    st, data = await _kt_req("GET", "/notifications", auth=auth)
    return _kt_wrap(st, data, "notifications", "auth")

@app.get("/kt/user/stremio/status", tags=["Kartoons-Auth"])
async def kt_stremio_status(auth: str = Query(...)):
    st, data = await _kt_req("GET", "/user/stremio/status", auth=auth)
    return _kt_wrap(st, data, "stremio_status", "auth")


@app.post("/kt/device/code", tags=["Kartoons-Public"])
async def kt_device_code():
    """Start TV/device link flow. User opens link_url and enters code — then poll /kt/device/status."""
    st, data = await _kt_req("POST", "/app/device/code", json_body={})
    # upstream returns fields at top-level sometimes
    if isinstance(data, dict) and data.get("success") and "data" not in data and data.get("code"):
        data = {"success": True, "data": {k: data[k] for k in data if k != "success"}}
    return _kt_wrap(st, data, "device_code", "public", how="Poll GET /kt/device/status?code= until linked")

@app.get("/kt/device/status", tags=["Kartoons-Public"])
async def kt_device_status(code: str, fullinfo: bool = True):
    st, data = await _kt_req("GET", "/app/device/status", {"code": code, "fullinfo": str(fullinfo).lower()})
    return _kt_wrap(st, data, "device_status", "public")

@app.get("/kt/stremio/stream", tags=["Kartoons-Auth"])
async def kt_stremio_stream(
    token: str = Query(..., description="Kartoons account token after login"),
    episode_id: Optional[str] = None,
    movie_id: Optional[str] = None,
    kind: str = Query("series", description="series|movie"),
):
    """Authenticated Stremio stream list. Real login token required (401 if fake)."""
    if not episode_id and not movie_id:
        raise HTTPException(400, detail=fail("episode_id or movie_id required", provider="kartoons"))
    cid = episode_id or movie_id
    k = "movie" if (movie_id or kind == "movie") else "series"
    path = f"/stremio/stream/{k}/{cid}.json"
    st, data = await _kt_req("GET", path, params={"token": token})
    servers = []
    if isinstance(data, dict):
        streams = data.get("streams") or []
        if isinstance(data.get("data"), list):
            streams = data["data"]
        if isinstance(streams, list):
            for s in streams:
                if not isinstance(s, dict):
                    continue
                url = s.get("url") or s.get("externalUrl")
                if not url:
                    continue
                servers.append({
                    "name": s.get("name") or s.get("title") or "Stremio",
                    "type": "hls" if ".m3u8" in str(url) else ("mp4" if ".mp4" in str(url) else "link"),
                    "url": url, "raw": s,
                })
    return _kt_wrap(st, data, "stremio_stream", "auth", servers=servers, direct_count=len(servers),
                    how="POST /kt/auth/login → token. No captcha bypass.")


@app.get("/kt/info", tags=["Kartoons-Public"])
async def kt_info():
    """Overview of Kartoons integration — public vs challenge vs auth."""
    return ok({
        "base": KT_API,
        "public": [
            "GET /kt/shows", "GET /kt/shows/featured", "GET /kt/show/{slug}",
            "GET /kt/show/{slug}/season/{season}/episodes", "GET /kt/show/{slug}/season/{season}/all-episodes",
            "GET /kt/show/{slug}/random-episode", "GET /kt/episode/{id}",
            "GET /kt/movies", "GET /kt/movies/featured", "GET /kt/movie/{id}",
            "GET /kt/show/{slug}/comments", "GET /kt/app/update", "POST /kt/device/code", "GET /kt/device/status",
        ],
        "challenge_turnstile": [
            "GET /kt/episode/{id}/links", "GET /kt/movie/{id}/links", "GET /kt/stream",
            "GET /kt/popularity/*", "GET /kt/search/suggestions", "GET /kt/schedules/*",
            "GET /kt/community/*", "GET /kt/collections", "GET /kt/suggestions/episode/{id}",
        ],
        "auth_login": [
            "POST /kt/auth/login", "GET /kt/auth/me", "GET /kt/user/me",
            "GET /kt/user/continue-watching", "GET /kt/watchlist", "POST /kt/watchlist/add",
            "GET /kt/notifications", "GET /kt/user/stremio/status", "GET /kt/stremio/stream?token=&episode_id=",
        ],
        "stream_cdn_note": (
            "NO server-side bypass exists for direct CDN without human/auth. "
            "Tested: all User-Agents, missing Origin, Android app headers, okhttp, Stremio UA — still challenge_required on /links. "
            "Paths: (1) Browser Turnstile token → /kt/stream?turnstile_token= "
            "(2) Logged-in user token → /kt/stremio/stream?token=&episode_id= "
            "(3) Device link: POST /kt/device/code then user opens link_url and approves. "
            "Captcha farms / Turnstile fraud are not supported."
        ),
        "example_flow": [
            "1. GET /kt/shows?search=shin-chan",
            "2. GET /kt/show/shin-chan-68170",
            "3. GET /kt/show/shin-chan-68170/season/season-1/episodes?limit=30",
            "4. GET /kt/episode/{episode_id}  (see links_count)",
            "5. GET /kt/stream?episode_id=...&turnstile_token=...  (CDN when captcha ok)",
        ],
    }, provider="kartoons", endpoint="info")


@app.get("/health", tags=["Meta"])
async def health():
    return ok({"version": VERSION, "providers": ["moviebox", "4khdhub", "hubcloud", "dramachi", "iptv", "hentaicity", "hindianime", "kartoons", "tools", "aggregate"]})

# ========== Stream proxy (needed for DASH cookies, CORS-blocked HLS, IP-locked MP4) ==========
from fastapi import Request
from fastapi.responses import StreamingResponse, Response
from starlette.background import BackgroundTask

_PRIVATE_HOST = re.compile(r"^(localhost|127\.|10\.|0\.|192\.168\.|169\.254\.|172\.(1[6-9]|2\d|3[01])\.|\[?::1|\[?fe80|\[?fc|\[?fd)", re.I)
_PASS_HEADERS = ("content-type", "content-length", "content-range", "accept-ranges", "last-modified", "etag")

def _safe_target(u: str) -> str:
    p = urlparse(u or "")
    if p.scheme not in ("http", "https") or not p.hostname or _PRIVATE_HOST.match(p.hostname):
        raise HTTPException(400, detail=fail("Blocked or invalid URL"))
    return u

def _b64u_decode(s: str) -> str:
    return base64.urlsafe_b64decode(s + "=" * ((4 - len(s) % 4) % 4)).decode("utf-8", "ignore")

def _gx_link(u: str, cookie: str = "") -> str:
    return "/gx?u=" + quote(u, safe="") + (("&c=" + quote(cookie, safe="")) if cookie else "")

def _rewrite_m3u8(text: str, base_url: str, cookie: str = "") -> str:
    out = []
    for line in text.splitlines():
        raw = line.strip()
        if not raw:
            out.append(line)
        elif raw.startswith("#"):
            out.append(re.sub(r'URI="([^"]+)"', lambda m: 'URI="' + _gx_link(urljoin(base_url, m.group(1)), cookie) + '"', line))
        else:
            out.append(_gx_link(urljoin(base_url, raw), cookie))
    return "\n".join(out) + "\n"

async def _proxy_fetch(request: Request, url: str, cookie: str = "", referer: str = "", dash_token: str = "", dash_base: str = ""):
    _safe_target(url)
    headers = {"User-Agent": UA, "Accept": "*/*", "Accept-Encoding": "identity"}
    if referer:
        headers["Referer"] = referer
    rng = request.headers.get("range")
    if rng:
        headers["Range"] = rng
    if cookie:
        headers["Cookie"] = cookie
    client = httpx.AsyncClient(timeout=httpx.Timeout(40.0, connect=12.0), follow_redirects=True)
    try:
        req = client.build_request("GET", url, headers=headers)
        r = await client.send(req, stream=True)
    except Exception as e:
        await client.aclose()
        raise HTTPException(502, detail=fail("Upstream unreachable", detail=str(e)[:160]))
    ct = (r.headers.get("content-type") or "").lower()
    path_l = urlparse(url).path.lower()
    is_m3u8 = path_l.endswith(".m3u8") or "mpegurl" in ct
    is_mpd = path_l.endswith(".mpd") or "dash+xml" in ct
    if r.status_code >= 400:
        body = (await r.aread())[:300]
        await r.aclose(); await client.aclose()
        return Response(content=body, status_code=r.status_code, media_type="text/plain")
    if is_m3u8 or is_mpd:
        raw = await r.aread()
        await r.aclose(); await client.aclose()
        text = raw.decode("utf-8", "ignore")
        if is_m3u8:
            return Response(_rewrite_m3u8(text, str(r.url), cookie), media_type="application/vnd.apple.mpegurl",
                            headers={"Cache-Control": "no-store"})
        if dash_base:
            def _bu(m):
                u = m.group(1).strip()
                if u.startswith(dash_base):
                    rel = u[len(dash_base):].lstrip("/")
                    return "<BaseURL>" + (rel or "./") + "</BaseURL>"
                return m.group(0)
            text = re.sub(r"<BaseURL>\s*(https?://[^<]+?)\s*</BaseURL>", _bu, text)
        return Response(text, media_type="application/dash+xml", headers={"Cache-Control": "no-store"})
    out_headers = {k: v for k, v in r.headers.items() if k.lower() in _PASS_HEADERS}
    out_headers.setdefault("Accept-Ranges", "bytes")
    out_headers["Cache-Control"] = "no-store"

    async def _close():
        await r.aclose(); await client.aclose()

    return StreamingResponse(r.aiter_raw(), status_code=r.status_code, headers=out_headers, background=BackgroundTask(_close))

@app.get("/px/{token}/{path:path}", tags=["Proxy"], include_in_schema=False)
async def px_dash(token: str, path: str, request: Request):
    """Cookie-signed DASH proxy: /px/<b64url{b:base,c:cookie}>/index.mpd (+ segments)."""
    try:
        info = json.loads(_b64u_decode(token))
        base, cookie = str(info.get("b") or "").rstrip("/"), str(info.get("c") or "")
    except Exception:
        raise HTTPException(400, detail=fail("Bad token"))
    if not base:
        raise HTTPException(400, detail=fail("Bad token"))
    target = base + "/" + path
    if request.url.query:
        target += "?" + request.url.query
    return await _proxy_fetch(request, target, cookie=cookie, dash_base=base)

@app.get("/gx", tags=["Proxy"], include_in_schema=False)
async def gx_proxy(request: Request, u: str = Query(...), c: str = Query("")):
    """Generic media proxy with Range support; rewrites HLS playlists so every segment also flows through here."""
    host = urlparse(u).hostname or ""
    ref = "https://" + host + "/" if host else ""
    if "hindianime" in host:
        ref = HA_BASE + "/"
    return await _proxy_fetch(request, u, cookie=c, referer=ref)


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
  <div class="brand"><div class="logo">SH</div><div><h1>StreamHub API</h1><small>v7.8.0 · creator: shawon</small></div></div>
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
{g:'MovieBox',p:'/mb/search',how:'Search titles. Copy subjectId → play/cdn/detail.',params:[{n:'q',v:'avatar'},{n:'page',v:'1'}]},
{g:'MovieBox',p:'/mb/movies',how:'Movies only (subjectType=1).',params:[{n:'q',v:'love'},{n:'page',v:'1'}]},
{g:'MovieBox',p:'/mb/series',how:'Series only (subjectType=2).',params:[{n:'q',v:'love'},{n:'page',v:'1'}]},
{g:'MovieBox',p:'/mb/adult',how:'18+ search (Adult/Erotic genres).',params:[{n:'q',v:'sex'},{n:'page',v:'1'}]},
{g:'MovieBox',p:'/mb/adult/home',how:'Curated adult home (tabId=9).',params:[{n:'page',v:'1'}]},
{g:'MovieBox',p:'/mb/home',how:'Home shelves. tab_id 1–4.',params:[{n:'tab_id',v:'1'},{n:'page',v:'1'}]},
{g:'MovieBox',p:'/mb/tab/{tab_id}',how:'Raw tab feed.',params:[{n:'tab_id',v:'1',path:true},{n:'page',v:'1'}]},
{g:'MovieBox',p:'/mb/detail/{id}',how:'Full metadata for subjectId.',params:[{n:'id',v:'1654274595068805784',path:true}]},
{g:'MovieBox',p:'/mb/seasons/{id}',how:'Season list for series.',params:[{n:'id',v:'1654274595068805784',path:true}]},
{g:'MovieBox',p:'/mb/play/{id}',how:'MP4 + DASH streams. DASH needs Cookie header.',params:[{n:'id',v:'1654274595068805784',path:true},{n:'se',v:''},{n:'ep',v:''}]},
{g:'MovieBox',p:'/mb/cdn/{id}',how:'All MP4 + DASH CDN (play + resource).',params:[{n:'id',v:'1654274595068805784',path:true}]},
{g:'MovieBox',p:'/mb/mp4/{id}',how:'Only direct MP4 CDN URLs.',params:[{n:'id',v:'1654274595068805784',path:true}]},
{g:'MovieBox',p:'/mb/resource/{id}',how:'Extra resource/download links.',params:[{n:'id',v:'1654274595068805784',path:true}]},
{g:'4KHDHub',p:'/fk/home',how:'Latest catalog cards.',params:[]},
{g:'4KHDHub',p:'/fk/search',how:'Search 4K catalog.',params:[{n:'q',v:'avatar'}]},
{g:'4KHDHub',p:'/fk/detail',how:'Releases + GreenMotors mirrors.',params:[{n:'path',v:'/hacksaw-ridge-movie-7809/'}]},
{g:'4KHDHub',p:'/fk/stream',how:'Resolve R2 / gpdl / googleusercontent CDN. resolve=true.',params:[{n:'path',v:'/hacksaw-ridge-movie-7809/'},{n:'resolve',v:'true'}]},
{g:'4KHDHub',p:'/fk/category/{slug}',how:'Category: movies, series, netflix…',params:[{n:'slug',v:'movies',path:true},{n:'page',v:'1'}]},
{g:'Tools',p:'/tools/resolve',how:'HubCloud / GreenMotors URL → direct CDN.',params:[{n:'url',v:'https://hubcloud.ist/drive/1qg90m0nr2599rq'}]},
{g:'Tools',p:'/tools/pixeldrain',how:'PixelDrain download URL from file id.',params:[{n:'id',v:'GauktM6T'}]},
{g:'Tools',p:'/tools/mp4',how:'Extract MP4/MKV/M3U8 links from a page.',params:[{n:'url',v:'https://hubcloud.ist/drive/1qg90m0nr2599rq'}]},
{g:'Tools',p:'/tools/cdn-types',how:'Known CDN host patterns.',params:[]},
{g:'Dramachi',p:'/dr/home',how:'Catalog via search fallback.',params:[{n:'page',v:'1'},{n:'filter',v:'all'}]},
{g:'Dramachi',p:'/dr/search',how:'Search dramas/movies.',params:[{n:'q',v:'love'},{n:'page',v:'1'},{n:'filter',v:'all'}]},
{g:'Dramachi',p:'/dr/detail',how:'Metadata only (no public stream CDN).',params:[{n:'id',v:'524'},{n:'content',v:'movies'}]},
{g:'Dramachi',p:'/dr/thumb',how:'Poster by thumb filename.',params:[{n:'name',v:'godlovescaviar2012h.jpg'}]},
{g:'IPTV',p:'/iptv/channels',how:'Live M3U. source 0=global 1=BD 2=IN.',params:[{n:'source',v:'0'},{n:'limit',v:'30'},{n:'q',v:''}]},
{g:'HentaiCity',p:'/hc/recent',how:'Recent list (seed CDN if IP blocked).',params:[]},
{g:'HentaiCity',p:'/hc/popular',how:'Popular list.',params:[]},
{g:'HentaiCity',p:'/hc/search',how:'Search (seed fallback on block).',params:[{n:'q',v:'anime'}]},
{g:'HentaiCity',p:'/hc/watch',how:'Build streams from folder+vid.',params:[{n:'folder',v:'0267'},{n:'vid',v:'38191'}]},
{g:'HentaiCity',p:'/hc/cdn',how:'Always builds HLS+MP4 from folder+vid.',params:[{n:'folder',v:'0267'},{n:'vid',v:'38191'}]},
{g:'HindiAnime',p:'/ha/catalog',how:'Full movies+series catalog (hindianime.site).',params:[{n:'kind',v:'all'}]},
{g:'HindiAnime',p:'/ha/home',how:'Home: topAiring, popular, latest…',params:[]},
{g:'HindiAnime',p:'/ha/search',how:'Search catalog by title/genre.',params:[{n:'q',v:'naruto'}]},
{g:'HindiAnime',p:'/ha/details',how:'Episodes + servers. id=slug or url=link.',params:[{n:'id',v:'clevatess'}]},
{g:'HindiAnime',p:'/ha/episodes',how:'Episode list. Optional season.',params:[{n:'id',v:'clevatess'},{n:'season',v:'1'}]},
{g:'HindiAnime',p:'/ha/stream',how:'Playable servers. Uses data.direct[] (alive HLS). Dead p2p (404) dropped. probe=true by default.',params:[{n:'id',v:'solo-leveling'},{n:'season',v:'1'},{n:'episode',v:'1'},{n:'lang',v:'Hindi'},{n:'probe',v:'true'}]},
{g:'HindiAnime',p:'/ha/hero',how:'Hero carousel JSON.',params:[]},
{g:'HindiAnime',p:'/ha/top10',how:'Trending + popular.',params:[]},
{g:'HindiAnime',p:'/ha/spotlights',how:'Spotlight rotation.',params:[]},
{g:'HindiAnime',p:'/ha/trailer',how:'YouTube trailer lookup.',params:[{n:'title',v:'naruto'}]},
{g:'HindiAnime',p:'/ha/proxy',how:'HLS proxy URLs for a videoHash + probe status.',params:[{n:'hash',v:'kqdr8'}]},

{g:'Kartoons',p:'/kt/info',how:'Overview: which routes are public / Turnstile / login. Read this first.',params:[]},
{g:'Kartoons',p:'/kt/shows',how:'PUBLIC list/search shows. Use search= keyword. Copy slug → /kt/show/{slug}.',params:[{n:'search',v:'shin'},{n:'page',v:'1'},{n:'limit',v:'10'}]},
{g:'Kartoons',p:'/kt/shows/featured',how:'PUBLIC featured shows.',params:[]},
{g:'Kartoons',p:'/kt/show/{slug}',how:'PUBLIC show details + seasons[]. Use season.slug for episodes.',params:[{n:'slug',v:'shin-chan-68170',path:true}]},
{g:'Kartoons',p:'/kt/show/{slug}/season/{season_slug}/episodes',how:'PUBLIC episode list. Copy episode _id for stream/links.',params:[{n:'slug',v:'shin-chan-68170',path:true},{n:'season_slug',v:'season-1',path:true},{n:'page',v:'1'},{n:'limit',v:'30'}]},
{g:'Kartoons',p:'/kt/show/{slug}/season/{season_slug}/all-episodes',how:'PUBLIC all episodes (no pagination).',params:[{n:'slug',v:'shin-chan-68170',path:true},{n:'season_slug',v:'season-1',path:true}]},
{g:'Kartoons',p:'/kt/show/{slug}/random-episode',how:'PUBLIC random episode id for a show.',params:[{n:'slug',v:'shin-chan-68170',path:true}]},
{g:'Kartoons',p:'/kt/episode/{episode_id}',how:'PUBLIC episode meta (title, duration, links_count). CDN needs /kt/stream.',params:[{n:'episode_id',v:'6867877f57ee07b9b7401910',path:true}]},
{g:'Kartoons',p:'/kt/movies',how:'PUBLIC list/search movies.',params:[{n:'search',v:'ponyo'},{n:'page',v:'1'},{n:'limit',v:'10'}]},
{g:'Kartoons',p:'/kt/movies/featured',how:'PUBLIC featured movies.',params:[]},
{g:'Kartoons',p:'/kt/movie/{movie_id}',how:'PUBLIC movie details (slug or id).',params:[{n:'movie_id',v:'fandub-princess-mononoke-6ac07',path:true}]},
{g:'Kartoons',p:'/kt/show/{slug}/comments',how:'PUBLIC comments. count_only=true for count.',params:[{n:'slug',v:'shin-chan-68170',path:true},{n:'limit',v:'5'}]},
{g:'Kartoons',p:'/kt/app/update',how:'PUBLIC app version + APK download URL.',params:[]},
{g:'Kartoons',p:'/kt/challenge/pow',how:'CHALLENGE: get POW nonce. content=episode:{id}',params:[{n:'content',v:'episode:6867877f57ee07b9b7401910'}]},
{g:'Kartoons',p:'/kt/challenge/solve',how:'CHALLENGE: auto-solve POW. Still need browser turnstile_token for verify.',params:[{n:'content',v:'episode:6867877f57ee07b9b7401910'}]},
{g:'Kartoons',p:'/kt/stream',how:'CHALLENGE: stream CDN. Without turnstile → challenge_required. With token may return servers[].',params:[{n:'episode_id',v:'6867877f57ee07b9b7401910'},{n:'turnstile_token',v:''}]},
{g:'Kartoons',p:'/kt/episode/{episode_id}/links',how:'CHALLENGE: raw links list (Turnstile). Prefer /kt/stream.',params:[{n:'episode_id',v:'6867877f57ee07b9b7401910',path:true},{n:'turnstile_token',v:''}]},
{g:'Kartoons',p:'/kt/movie/{movie_id}/links',how:'CHALLENGE: movie stream links (Turnstile).',params:[{n:'movie_id',v:'fandub-princess-mononoke-6ac07',path:true}]},
{g:'Kartoons',p:'/kt/popularity/shows',how:'CHALLENGE: popular shows (often 403 without Turnstile).',params:[]},
{g:'Kartoons',p:'/kt/popularity/movies',how:'CHALLENGE: popular movies.',params:[]},
{g:'Kartoons',p:'/kt/popularity/trending',how:'CHALLENGE: trending.',params:[]},
{g:'Kartoons',p:'/kt/search/suggestions',how:'CHALLENGE. Public alt: /kt/shows?search=',params:[{n:'q',v:'shin'},{n:'limit',v:'5'}]},
{g:'Kartoons',p:'/kt/schedules/upcoming',how:'CHALLENGE: upcoming schedule.',params:[]},
{g:'Kartoons',p:'/kt/schedules/weekly',how:'CHALLENGE: weekly schedule.',params:[]},
{g:'Kartoons',p:'/kt/community/board',how:'CHALLENGE: community board.',params:[]},
{g:'Kartoons',p:'/kt/community/posts',how:'CHALLENGE: community posts.',params:[]},
{g:'Kartoons',p:'/kt/collections',how:'CHALLENGE: collections list.',params:[]},
{g:'Kartoons',p:'/kt/suggestions/episode/{episode_id}',how:'CHALLENGE: related episode suggestions.',params:[{n:'episode_id',v:'6867877f57ee07b9b7401910',path:true}]},
{g:'Kartoons',p:'/kt/auth/login',how:'AUTH: login → save token. Then pass auth=TOKEN on user routes. (POST — Try it may need fetch POST)',params:[{n:'email',v:''},{n:'password',v:''}]},
{g:'Kartoons',p:'/kt/auth/me',how:'AUTH: current user. Requires auth= token.',params:[{n:'auth',v:''}]},
{g:'Kartoons',p:'/kt/user/me',how:'AUTH: user profile.',params:[{n:'auth',v:''}]},
{g:'Kartoons',p:'/kt/user/continue-watching',how:'AUTH: continue watching list.',params:[{n:'auth',v:''}]},
{g:'Kartoons',p:'/kt/watchlist',how:'AUTH: watchlist.',params:[{n:'auth',v:''}]},
{g:'Kartoons',p:'/kt/watchlist/add',how:'AUTH POST: add to watchlist.',params:[{n:'auth',v:''},{n:'content_type',v:'show'},{n:'content_id',v:''},{n:'status',v:'plan_to_watch'}]},
{g:'Kartoons',p:'/kt/ratings/content/{ctype}/{cid}',how:'Ratings for show/movie. May need auth.',params:[{n:'ctype',v:'show',path:true},{n:'cid',v:'shin-chan-68170',path:true}]},
{g:'Kartoons',p:'/kt/notifications',how:'AUTH: notifications.',params:[{n:'auth',v:''}]},
{g:'Kartoons',p:'/kt/user/stremio/status',how:'AUTH: Stremio integration status.',params:[{n:'auth',v:''}]},
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

# ========== FULL MOVIE WEBSITE (served at / and /site) ==========
SITE_HTML = r'''<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1,viewport-fit=cover">
<meta name="referrer" content="no-referrer">
<meta name="theme-color" content="#05060f">
<title>StreamHub — stream, download, discover</title>
<link rel="icon" href="data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 64 64'%3E%3Crect width='64' height='64' rx='16' fill='%23060913'/%3E%3Ccircle cx='32' cy='32' r='17' fill='none' stroke='%23ffb224' stroke-width='4'/%3E%3Cpath d='M27 23l16 9-16 9z' fill='%23ffb224'/%3E%3C/svg%3E">
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=Unbounded:wght@500;600;700&family=Manrope:wght@400;500;600;700;800&family=Hind+Siliguri:wght@400;500;600&display=swap" rel="stylesheet">
<style>
:root{--void:#05060f;--ink:#0a0d20;--panel:rgba(17,22,50,.64);--panel2:rgba(28,34,72,.78);--line:rgba(165,178,255,.13);--line2:rgba(165,178,255,.28);--txt:#f0f2ff;--dim:#939bc2;--ion:#5e8bff;--flare:#ff4f78;--mint:#37e6b0;--r:16px;--dockh:66px;--sb:env(safe-area-inset-bottom,0px);--st:env(safe-area-inset-top,0px);--fd:'Unbounded','Hind Siliguri',system-ui,sans-serif;--ff:'Manrope','Hind Siliguri',system-ui,sans-serif}
*{box-sizing:border-box;margin:0;padding:0;-webkit-tap-highlight-color:transparent}
html{color-scheme:dark;scroll-padding-top:70px}
body{background:var(--void);color:var(--txt);font:500 15px/1.5 var(--ff);min-height:100vh;overflow-x:hidden;-webkit-font-smoothing:antialiased}
body.lock{overflow:hidden}
a{color:inherit;text-decoration:none}
button,input,select{font:inherit;color:inherit}
button{background:none;border:0;cursor:pointer}
img{display:block;max-width:100%}
::selection{background:var(--amber);color:var(--on)}
:focus-visible{outline:2px solid var(--amber);outline-offset:2px;border-radius:8px}
::-webkit-scrollbar{width:8px;height:8px}::-webkit-scrollbar-thumb{background:rgba(150,180,255,.2);border-radius:8px}
.sr{position:absolute;width:1px;height:1px;overflow:hidden;clip:rect(0 0 0 0)}

/* ambient */
#aurora{position:fixed;inset:0;z-index:-2;overflow:hidden;background:radial-gradient(120% 80% at 50% -10%,#101b3d 0%,var(--void) 60%)}
#aurora i{position:absolute;width:60vmax;height:60vmax;border-radius:50%;filter:blur(90px);opacity:.28;animation:drift 26s ease-in-out infinite alternate}
#aurora i:nth-child(1){background:var(--ion);left:-20vmax;top:-18vmax}
#aurora i:nth-child(2){background:#7a3cff;right:-24vmax;top:20vh;animation-duration:32s;opacity:.18}
#aurora i:nth-child(3){background:var(--amber2);left:20vw;bottom:-34vmax;animation-duration:38s;opacity:.12}
@keyframes drift{to{transform:translate3d(8vmax,6vmax,0) scale(1.15)}}
#grain{position:fixed;inset:0;z-index:-1;pointer-events:none;opacity:.05;background-image:url("data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' width='160' height='160'%3E%3Cfilter id='n'%3E%3CfeTurbulence baseFrequency='.9' numOctaves='2'/%3E%3C/filter%3E%3Crect width='160' height='160' filter='url(%23n)'/%3E%3C/svg%3E")}

/* splash (one orchestrated moment) */
#splash{position:fixed;inset:0;z-index:999;background:var(--void);display:grid;place-items:center;transition:opacity .6s .1s,visibility .6s .1s}
#splash.out{opacity:0;visibility:hidden}
#splash svg{width:92px;height:92px;overflow:visible}
#splash .ring{fill:none;stroke:var(--amber);stroke-width:3;stroke-dasharray:140;stroke-dashoffset:140;animation:draw 1s .1s cubic-bezier(.6,0,.2,1) forwards;filter:drop-shadow(0 0 10px color-mix(in srgb,var(--a1) 70%,transparent))}
#splash .tri{fill:var(--amber);opacity:0;transform-origin:center;animation:pop .5s .75s cubic-bezier(.2,1.4,.4,1) forwards}
#splash b{display:block;margin-top:18px;font:600 20px var(--fd);letter-spacing:.14em;text-align:center;opacity:0;animation:rise .6s .8s forwards}
@keyframes draw{to{stroke-dashoffset:0}}@keyframes pop{from{opacity:0;transform:scale(.4)}to{opacity:1;transform:none}}@keyframes rise{from{opacity:0;transform:translateY(8px)}to{opacity:1;transform:none}}

/* top bar */
#top{position:fixed;top:0;left:0;right:0;z-index:40;padding:calc(10px + var(--st)) 14px 10px;display:flex;align-items:center;gap:14px;transition:background .3s,backdrop-filter .3s}
#top.solid{background:rgba(6,9,19,.72);backdrop-filter:blur(18px) saturate(1.4);-webkit-backdrop-filter:blur(18px) saturate(1.4);box-shadow:0 1px 0 var(--line)}
.logo{display:flex;align-items:center;gap:9px;font:600 15px var(--fd);letter-spacing:.04em}
.logo svg{width:28px;height:28px}
.logo .ring{fill:none;stroke:var(--amber);stroke-width:3.2}.logo .tri{fill:var(--amber)}
#nav{display:none;gap:4px;margin-left:10px;overflow-x:auto;scrollbar-width:none}
#nav a{padding:7px 13px;border-radius:99px;color:var(--dim);font-weight:700;font-size:13.5px;white-space:nowrap;transition:.2s}
#nav a:hover{color:var(--txt);background:rgba(255,255,255,.05)}
#nav a.on{color:var(--on);background:linear-gradient(135deg,var(--amber),var(--amber2));box-shadow:0 4px 18px color-mix(in srgb,var(--a2) 35%,transparent)}
.tspace{flex:1}
.ibtn{width:40px;height:40px;border-radius:50%;display:grid;place-items:center;background:rgba(255,255,255,.06);border:1px solid var(--line);transition:.2s;flex:none}
.ibtn:hover{background:rgba(255,255,255,.12);border-color:var(--line2)}
.ibtn svg{width:19px;height:19px}
@media(min-width:900px){#nav{display:flex}#top{padding-inline:28px}}

/* dock */
#dock{position:fixed;left:50%;bottom:calc(10px + var(--sb));transform:translateX(-50%);z-index:45;width:min(480px,94vw);height:var(--dockh);display:flex;padding:6px;border-radius:24px;background:rgba(12,18,36,.78);backdrop-filter:blur(20px) saturate(1.5);-webkit-backdrop-filter:blur(20px) saturate(1.5);border:1px solid var(--line2);box-shadow:0 18px 50px rgba(0,0,0,.55),inset 0 1px 0 rgba(255,255,255,.06)}
#dock a,#dock button{flex:1;display:flex;flex-direction:column;align-items:center;justify-content:center;gap:2px;color:var(--dim);font-size:10.5px;font-weight:700;border-radius:18px;position:relative;z-index:1;transition:color .25s}
#dock svg{width:22px;height:22px;transition:transform .35s cubic-bezier(.2,1.5,.4,1)}
#dock .on{color:var(--amber)}#dock .on svg{transform:translateY(-2px) scale(1.12)}
#dockglow{position:absolute;top:6px;bottom:6px;left:6px;border-radius:18px;background:linear-gradient(180deg,color-mix(in srgb,var(--a1) 18%,transparent),color-mix(in srgb,var(--a1) 4%,transparent));border:1px solid color-mix(in srgb,var(--a1) 30%,transparent);transition:transform .4s cubic-bezier(.3,1.3,.5,1),width .3s;z-index:0}
@media(min-width:900px){#dock{display:none}}

/* layout */
main{min-height:100vh;padding-bottom:calc(var(--dockh) + 40px + var(--sb))}
.pg{animation:pgin .45s cubic-bezier(.2,.8,.2,1)}
@keyframes pgin{from{opacity:0;transform:translateY(10px)}to{opacity:1;transform:none}}
.wrap{padding:0 16px;max-width:1500px;margin:0 auto}
.ph{padding:calc(76px + var(--st)) 16px 6px;max-width:1500px;margin:0 auto}
.ph h1{font:700 clamp(24px,5.6vw,40px)/1.1 var(--fd);letter-spacing:-.01em}
.ph p{color:var(--dim);margin-top:8px;max-width:60ch}
@media(min-width:900px){.wrap,.ph{padding-inline:32px}}

/* chips + inputs */
.chips{display:flex;gap:8px;overflow-x:auto;padding:14px 16px 6px;scrollbar-width:none;max-width:1500px;margin:0 auto}
@media(min-width:900px){.chips{padding-inline:32px}}
.chip{padding:8px 15px;border-radius:99px;background:rgba(255,255,255,.05);border:1px solid var(--line);color:var(--dim);font-weight:700;font-size:13px;white-space:nowrap;transition:.2s}
.chip:hover{color:var(--txt);border-color:var(--line2)}
.chip.on{background:color-mix(in srgb,var(--a1) 14%,transparent);border-color:color-mix(in srgb,var(--a1) 55%,transparent);color:var(--amber)}
.sbox{display:flex;align-items:center;gap:10px;margin:14px 16px 0;padding:0 14px;height:48px;border-radius:14px;background:rgba(255,255,255,.05);border:1px solid var(--line);max-width:640px;transition:.2s}
@media(min-width:900px){.sbox{margin-inline:32px}}
.sbox:focus-within{border-color:color-mix(in srgb,var(--a1) 60%,transparent);box-shadow:0 0 0 4px color-mix(in srgb,var(--a1) 10%,transparent)}
.sbox svg{width:19px;height:19px;color:var(--dim);flex:none}
.sbox input{flex:1;background:none;border:0;outline:0;height:100%;min-width:0}
.sbox input::placeholder{color:#66729a}

/* buttons */
.btn{display:inline-flex;align-items:center;justify-content:center;gap:8px;height:46px;padding:0 22px;border-radius:14px;font-weight:800;font-size:14.5px;transition:transform .2s,box-shadow .3s,background .2s;position:relative;overflow:hidden;white-space:nowrap}
.btn svg{width:18px;height:18px;flex:none}
.btn:active{transform:scale(.97)}
.btn.pri{color:var(--on);background:linear-gradient(135deg,color-mix(in srgb,var(--a1) 78%,#fff),var(--a2));box-shadow:0 8px 28px color-mix(in srgb,var(--a2) 38%,transparent)}
.btn.pri:hover{box-shadow:0 10px 38px color-mix(in srgb,var(--a2) 60%,transparent)}
.btn.ghost{background:rgba(255,255,255,.08);border:1px solid var(--line2);backdrop-filter:blur(8px)}
.btn.ghost:hover{background:rgba(255,255,255,.14)}
.btn.sm{height:36px;padding:0 14px;font-size:13px;border-radius:11px}
.btn.on{background:color-mix(in srgb,var(--a1) 16%,transparent);border-color:color-mix(in srgb,var(--a1) 60%,transparent);color:var(--amber)}
.btn[disabled]{opacity:.5;pointer-events:none}

/* hero */
#hero{position:relative;height:clamp(470px,82vh,780px);overflow:hidden;touch-action:pan-y}
.hs{position:absolute;inset:0;opacity:0;visibility:hidden;transition:opacity 1s,visibility 1s}
.hs.on{opacity:1;visibility:visible}
.hs .bg{position:absolute;inset:-4%;background-size:cover;background-position:center 20%;transform:scale(1.06)}
.hs.on .bg{animation:kb 9s ease-out forwards}
@keyframes kb{to{transform:scale(1.14) translate3d(-1.2%,-1%,0)}}
.hs::after{content:"";position:absolute;inset:0;background:linear-gradient(0deg,var(--void) 4%,rgba(6,9,19,.55) 38%,rgba(6,9,19,.1) 70%),linear-gradient(90deg,rgba(6,9,19,.88) 0%,rgba(6,9,19,.25) 55%,transparent)}
#hero .beam{position:absolute;inset:-30% -10%;z-index:2;pointer-events:none;background:linear-gradient(105deg,transparent 42%,color-mix(in srgb,var(--a1) 9%,transparent) 49%,rgba(255,255,255,.07) 50%,transparent 58%);animation:beam 7s ease-in-out infinite;mix-blend-mode:screen}
@keyframes beam{0%,100%{transform:translateX(-30%)}50%{transform:translateX(30%)}}
.hc{position:absolute;z-index:3;left:0;right:0;bottom:0;padding:0 16px 64px;max-width:1500px;margin:auto}
@media(min-width:900px){.hc{padding:0 32px 80px}}
.hc h2{font:700 clamp(26px,6.4vw,58px)/1.06 var(--fd);max-width:16ch;letter-spacing:-.02em;text-shadow:0 6px 40px rgba(0,0,0,.6)}
.hs.on .hc h2{animation:rise .8s .15s both}.hs.on .hc .meta,.hs.on .hc .acts,.hs.on .hc .desc{animation:rise .8s .3s both}
.meta{display:flex;flex-wrap:wrap;gap:8px;margin:14px 0;align-items:center;color:#cdd6f0;font-weight:700;font-size:13px}
.tag{padding:3px 9px;border-radius:7px;background:rgba(255,255,255,.1);border:1px solid var(--line);font-size:12px;font-weight:800}
.tag.hot{background:rgba(255,79,120,.16);border-color:rgba(255,79,120,.5);color:#ff94ac}
.tag.am{background:color-mix(in srgb,var(--a1) 14%,transparent);border-color:color-mix(in srgb,var(--a1) 50%,transparent);color:var(--amber)}
.desc{color:#b9c3df;max-width:56ch;display:-webkit-box;-webkit-line-clamp:3;-webkit-box-orient:vertical;overflow:hidden;margin-bottom:18px;font-size:14.5px}
.acts{display:flex;gap:10px;flex-wrap:wrap}
#hdots{position:absolute;z-index:4;right:16px;bottom:34px;display:flex;gap:6px}
@media(min-width:900px){#hdots{right:32px;bottom:50px}}
#hdots button{width:30px;height:4px;border-radius:4px;background:rgba(255,255,255,.22);overflow:hidden;padding:0;position:relative}
#hdots button::after{content:"";position:absolute;inset:0;background:var(--amber);transform:scaleX(0);transform-origin:left}
#hdots button.on::after{animation:fillbar var(--dur,7s) linear forwards}
#hdots button.done::after{transform:none}
@keyframes fillbar{to{transform:scaleX(1)}}

/* rows */
.row{margin-top:26px}
.rh{display:flex;align-items:center;justify-content:space-between;padding:0 16px;margin-bottom:12px;max-width:1500px;margin-inline:auto}
@media(min-width:900px){.rh{padding-inline:32px}}
.rh h2{font:600 15px/1.2 var(--fd);display:flex;align-items:center;gap:10px}
.rh h2::before{content:"";width:4px;height:18px;border-radius:3px;background:linear-gradient(var(--amber),var(--amber2));box-shadow:0 0 12px color-mix(in srgb,var(--a2) 70%,transparent)}
.rh .more{font-size:12.5px;color:var(--dim);font-weight:700}.rh .more:hover{color:var(--amber)}
.arrows{display:none;gap:6px}
@media(hover:hover) and (min-width:900px){.arrows{display:flex}}
.arrows button{width:32px;height:32px;border-radius:50%;border:1px solid var(--line);display:grid;place-items:center;transition:.2s}
.arrows button:hover{border-color:var(--amber);color:var(--amber)}
.arrows svg{width:15px;height:15px}
.rail{display:flex;gap:12px;overflow-x:auto;scroll-snap-type:x proximity;padding:6px 16px 18px;scrollbar-width:none;max-width:1500px;margin:0 auto;-webkit-mask-image:linear-gradient(90deg,transparent 0,#000 16px,#000 calc(100% - 28px),transparent 100%);mask-image:linear-gradient(90deg,transparent 0,#000 16px,#000 calc(100% - 28px),transparent 100%)}
@media(min-width:900px){.rail{padding-inline:32px;gap:16px}}
.rail .card{flex:0 0 clamp(118px,31vw,176px);scroll-snap-align:start}
@media(min-width:900px){.rail .card{flex-basis:178px}}
.grid{display:grid;gap:14px;grid-template-columns:repeat(auto-fill,minmax(112px,1fr));padding:16px;max-width:1500px;margin:0 auto}
@media(min-width:600px){.grid{grid-template-columns:repeat(auto-fill,minmax(150px,1fr))}}
@media(min-width:900px){.grid{padding:20px 32px;gap:18px;grid-template-columns:repeat(auto-fill,minmax(170px,1fr))}}

/* cards */
.card{display:block;position:relative;border-radius:var(--r);--mx:50%;--my:50%}
.poster{position:relative;aspect-ratio:2/3;border-radius:var(--r);overflow:hidden;background:linear-gradient(160deg,#18233f,#0d1428);border:1px solid var(--line);transition:transform .4s cubic-bezier(.2,.8,.2,1),border-color .3s,box-shadow .4s}
.poster img{width:100%;height:100%;object-fit:cover;transition:transform .6s,opacity .4s;opacity:0}
.poster img.ld{opacity:1}
.poster.noimg::after{content:attr(data-t);position:absolute;inset:0;display:grid;place-items:center;font:700 44px var(--fd);color:rgba(255,255,255,.18)}
.poster::before{content:"";position:absolute;inset:6px;border-radius:11px;pointer-events:none;z-index:3;opacity:0;transition:opacity .3s,inset .3s;background:linear-gradient(var(--amber),var(--amber)) 0 0/16px 2px no-repeat,linear-gradient(var(--amber),var(--amber)) 0 0/2px 16px no-repeat,linear-gradient(var(--amber),var(--amber)) 100% 0/16px 2px no-repeat,linear-gradient(var(--amber),var(--amber)) 100% 0/2px 16px no-repeat,linear-gradient(var(--amber),var(--amber)) 0 100%/16px 2px no-repeat,linear-gradient(var(--amber),var(--amber)) 0 100%/2px 16px no-repeat,linear-gradient(var(--amber),var(--amber)) 100% 100%/16px 2px no-repeat,linear-gradient(var(--amber),var(--amber)) 100% 100%/2px 16px no-repeat}
.shine{position:absolute;inset:0;z-index:2;pointer-events:none;opacity:0;transition:opacity .3s;background:radial-gradient(circle at var(--mx) var(--my),rgba(255,255,255,.22),transparent 55%)}
@media(hover:hover){.card:hover .poster{transform:translateY(-6px) scale(1.025);border-color:color-mix(in srgb,var(--a1) 50%,transparent);box-shadow:0 18px 44px rgba(0,0,0,.6),0 0 30px color-mix(in srgb,var(--a2) 18%,transparent)}.card:hover .poster::before{opacity:1;inset:8px}.card:hover .shine{opacity:1}.card:hover img{transform:scale(1.06)}}
.card:focus-visible .poster::before{opacity:1}
.rt{position:absolute;z-index:3;top:8px;left:8px;padding:3px 8px;border-radius:8px;background:rgba(6,9,19,.78);backdrop-filter:blur(6px);font:800 11.5px var(--ff);color:var(--amber)}
.bd{position:absolute;z-index:3;top:8px;right:8px;padding:3px 7px;border-radius:7px;font:800 10.5px var(--ff);background:color-mix(in srgb,var(--a2) 85%,transparent);color:#fff}
.bd.s{background:rgba(255,79,120,.88)}.bd.k{background:color-mix(in srgb,var(--a1) 92%,transparent);color:var(--on)}
.pg-bar{position:absolute;z-index:3;left:0;right:0;bottom:0;height:3px;background:rgba(255,255,255,.18)}.pg-bar i{display:block;height:100%;background:var(--amber);box-shadow:0 0 8px var(--amber)}
.ct{padding:9px 3px 0}
.ct h3{font:700 13.5px/1.3 var(--ff);display:-webkit-box;-webkit-line-clamp:2;-webkit-box-orient:vertical;overflow:hidden}
.ct p{font-size:12px;color:var(--dim);margin-top:2px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.top10 .card{flex-basis:clamp(150px,40vw,210px)}
.top10 .num{position:absolute;left:-6px;bottom:30px;z-index:4;font:700 74px/1 var(--fd);color:transparent;-webkit-text-stroke:2px var(--amber);text-shadow:0 0 24px color-mix(in srgb,var(--a2) 50%,transparent);pointer-events:none}

/* skeleton */
.sk{background:linear-gradient(100deg,rgba(255,255,255,.04) 30%,rgba(255,255,255,.1) 50%,rgba(255,255,255,.04) 70%) 0 0/300% 100%;animation:sh 1.4s linear infinite;border-radius:var(--r)}
@keyframes sh{to{background-position:-300% 0}}
.skrow{display:flex;gap:12px;padding:6px 16px;overflow:hidden}.skrow .sk{flex:0 0 clamp(118px,31vw,176px);aspect-ratio:2/3}
.skhero{height:clamp(470px,82vh,780px);border-radius:0}
.empty{padding:60px 20px;text-align:center;color:var(--dim)}
.empty b{display:block;color:var(--txt);font:600 17px var(--fd);margin-bottom:8px}
.empty .btn{margin-top:18px}
.more-wrap{display:flex;justify-content:center;padding:10px 0 30px}

/* live tv */
.chs{display:grid;gap:10px;padding:16px;grid-template-columns:repeat(auto-fill,minmax(150px,1fr));max-width:1500px;margin:0 auto}
@media(min-width:900px){.chs{padding:20px 32px;grid-template-columns:repeat(auto-fill,minmax(210px,1fr))}}
.ch{display:flex;align-items:center;gap:11px;padding:11px;border-radius:14px;background:var(--panel);border:1px solid var(--line);text-align:left;transition:.25s;min-width:0}
.ch:hover{border-color:color-mix(in srgb,var(--a1) 50%,transparent);transform:translateY(-3px);box-shadow:0 10px 30px rgba(0,0,0,.4)}
.ch .lg{width:42px;height:42px;border-radius:10px;background:#fff1;display:grid;place-items:center;flex:none;overflow:hidden;font:700 15px var(--fd);color:var(--dim)}
.ch .lg img{width:100%;height:100%;object-fit:contain;padding:3px}
.ch div{min-width:0}.ch b{display:block;font-size:13px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}.ch small{color:var(--dim);font-size:11.5px}
.live{display:inline-flex;align-items:center;gap:5px;color:var(--flare);font-weight:800;font-size:11px}
.live::before{content:"";width:6px;height:6px;border-radius:50%;background:var(--flare);animation:pulse 1.4s infinite}
@keyframes pulse{50%{opacity:.25;transform:scale(.7)}}

/* detail overlay */
#detail{position:fixed;inset:0;z-index:60;background:var(--void);overflow-y:auto;overscroll-behavior:contain;visibility:hidden;transform:translateY(40px);opacity:0;transition:transform .45s cubic-bezier(.2,.8,.2,1),opacity .35s,visibility .45s}
#detail.open{visibility:visible;transform:none;opacity:1}
#dbg{position:fixed;inset:0;z-index:-1;background-size:cover;background-position:center;filter:blur(46px) saturate(1.3);opacity:.32;transform:scale(1.2)}
.dtop{position:sticky;top:0;z-index:5;display:flex;align-items:center;gap:10px;padding:calc(10px + var(--st)) 14px 10px;background:linear-gradient(180deg,rgba(6,9,19,.92),transparent)}
.dbody{max-width:1180px;margin:0 auto;padding:0 16px 120px}
.dmain{display:grid;gap:22px}
@media(min-width:900px){.dbody{padding-inline:32px}.dmain.has-side{grid-template-columns:230px 1fr;align-items:start}}
.dposter{display:none;border-radius:var(--r);overflow:hidden;border:1px solid var(--line2);box-shadow:0 24px 60px rgba(0,0,0,.6)}
.dposter img{width:100%}
@media(min-width:900px){.dposter{display:block}}
.dh h1{font:700 clamp(22px,5vw,38px)/1.12 var(--fd);letter-spacing:-.015em}
.dh .acts{margin:16px 0}
.dd{color:#b9c3df;line-height:1.7;max-width:70ch}
.tabs{display:flex;gap:4px;margin:26px 0 14px;border-bottom:1px solid var(--line);overflow-x:auto;scrollbar-width:none}
.tabs button{padding:11px 16px;font-weight:800;color:var(--dim);position:relative;white-space:nowrap;font-size:14px}
.tabs button.on{color:var(--amber)}
.tabs button.on::after{content:"";position:absolute;left:10px;right:10px;bottom:-1px;height:2px;background:var(--amber);box-shadow:0 0 10px var(--amber);border-radius:2px}
.sel{display:flex;gap:8px;flex-wrap:wrap;margin:8px 0 14px}
.eps{display:grid;gap:8px;grid-template-columns:repeat(auto-fill,minmax(56px,1fr));margin-bottom:10px}
.ep{height:44px;border-radius:11px;background:rgba(255,255,255,.05);border:1px solid var(--line);font-weight:800;transition:.2s}
.ep:hover{border-color:var(--line2)}.ep.on{background:linear-gradient(135deg,var(--amber),var(--amber2));color:var(--on);border-color:transparent;box-shadow:0 6px 20px color-mix(in srgb,var(--a2) 35%,transparent)}
.eplist{display:grid;gap:8px}
.epi{display:flex;align-items:center;gap:12px;padding:12px 14px;border-radius:13px;background:rgba(255,255,255,.04);border:1px solid var(--line);text-align:left;transition:.2s}
.epi:hover{border-color:var(--line2)}.epi.on{border-color:color-mix(in srgb,var(--a1) 60%,transparent);background:color-mix(in srgb,var(--a1) 8%,transparent)}
.epi b{width:32px;font:700 14px var(--fd);color:var(--amber)}.epi span{flex:1;font-weight:700;font-size:13.5px}
.panel{background:var(--panel);border:1px solid var(--line);border-radius:var(--r);padding:16px}
.dl{display:grid;gap:10px}
.dli{display:flex;align-items:center;gap:12px;padding:13px 14px;border-radius:14px;background:var(--panel);border:1px solid var(--line);flex-wrap:wrap}
.dli .inf{flex:1;min-width:170px}.dli .inf b{display:block;font-size:14px;word-break:break-word}.dli .inf small{color:var(--dim);font-size:12px}
.dli .ba{display:flex;gap:6px;flex-wrap:wrap}
.note{padding:12px 14px;border-radius:12px;background:color-mix(in srgb,var(--a2) 10%,transparent);border:1px solid color-mix(in srgb,var(--a2) 30%,transparent);color:#c9d6ff;font-size:13.5px;margin:10px 0}
.note.warn{background:color-mix(in srgb,var(--a1) 9%,transparent);border-color:color-mix(in srgb,var(--a1) 35%,transparent);color:color-mix(in srgb,var(--a1) 55%,#fff)}
.cast{display:flex;gap:14px;overflow-x:auto;padding-bottom:8px;scrollbar-width:none}
.cast div{flex:0 0 78px;text-align:center;font-size:12px;color:var(--dim)}
.cast img,.cast i{width:64px;height:64px;border-radius:50%;object-fit:cover;margin:0 auto 6px;background:#1a2442;display:grid;place-items:center;font-style:normal;font-weight:800}
.cast b{display:block;color:var(--txt);font-size:12px;line-height:1.25}
.pcover{position:relative;aspect-ratio:16/9;border-radius:18px;overflow:hidden;background:#0a0f20 center/cover;border:1px solid var(--line2);box-shadow:0 24px 70px rgba(0,0,0,.55);cursor:pointer}
.pcover::after{content:"";position:absolute;inset:0;background:radial-gradient(circle,rgba(0,0,0,.05),rgba(6,9,19,.7))}
.pcover .pb{position:absolute;z-index:2;inset:0;margin:auto;width:76px;height:76px;border-radius:50%;display:grid;place-items:center;background:linear-gradient(135deg,color-mix(in srgb,var(--a1) 78%,#fff),var(--a2));box-shadow:0 0 0 10px color-mix(in srgb,var(--a1) 18%,transparent),0 12px 40px color-mix(in srgb,var(--a2) 60%,transparent);transition:transform .3s}
.pcover:hover .pb{transform:scale(1.1)}.pcover .pb svg{width:30px;height:30px;color:var(--on);margin-left:3px}

/* player */
.pl{position:relative;aspect-ratio:16/9;background:#000;border-radius:18px;overflow:hidden;border:1px solid var(--line2);box-shadow:0 24px 70px rgba(0,0,0,.6);user-select:none;-webkit-user-select:none;outline:0}
.pl.fs{border-radius:0;border:0;aspect-ratio:auto;width:100%;height:100%}
.pl video,.pl iframe{position:absolute;inset:0;width:100%;height:100%;background:#000;border:0}
.pl video.fill{object-fit:cover}
.pl iframe{display:none}.pl.if iframe{display:block}.pl.if video{display:none}
.pl-ui{position:absolute;inset:0;z-index:3;display:flex;flex-direction:column;justify-content:space-between;opacity:1;transition:opacity .3s;background:linear-gradient(180deg,rgba(0,0,0,.6),transparent 28%,transparent 62%,rgba(0,0,0,.78))}
.pl.idle .pl-ui{opacity:0;cursor:none}.pl.if .pl-ui{background:none;pointer-events:none;height:auto;inset:0 0 auto 0}.pl.if .pl-bot,.pl.if .pl-big{display:none}
.pl-top{display:flex;align-items:center;gap:10px;padding:12px 14px;pointer-events:auto}
.pl-title{flex:1;font-weight:800;font-size:14px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;text-shadow:0 2px 8px #000}
.pl-src{font-size:11.5px;font-weight:800;padding:3px 9px;border-radius:7px;background:rgba(255,255,255,.14)}
.pl-big{position:absolute;inset:0;margin:auto;width:70px;height:70px;border-radius:50%;background:rgba(10,15,30,.55);backdrop-filter:blur(8px);border:1px solid var(--line2);display:grid;place-items:center;transition:transform .25s,opacity .25s}
.pl-big svg{width:30px;height:30px;margin-left:3px}.pl.playing .pl-big{opacity:0;pointer-events:none;transform:scale(1.3)}
.pl-spin{position:absolute;inset:0;margin:auto;width:46px;height:46px;border-radius:50%;border:3px solid rgba(255,255,255,.15);border-top-color:var(--amber);animation:spin .8s linear infinite;display:none;pointer-events:none}
.pl.busy .pl-spin{display:block}@keyframes spin{to{transform:rotate(360deg)}}
.pl-msg{position:absolute;left:14px;right:14px;bottom:78px;text-align:center;font-weight:700;font-size:13.5px;padding:9px 14px;border-radius:12px;background:rgba(10,15,30,.86);border:1px solid var(--line2);display:none;pointer-events:auto}
.pl-msg.show{display:block}
.pl-bot{padding:0 12px 10px;pointer-events:auto}
.pl-bar{position:relative;height:22px;display:flex;align-items:center;cursor:pointer;touch-action:none}
.pl-bar::before{content:"";position:absolute;left:0;right:0;height:4px;border-radius:4px;background:rgba(255,255,255,.2);transition:height .15s}
.pl-bar:hover::before{height:6px}
.pl-bar .buf,.pl-bar .fill{position:absolute;left:0;height:4px;border-radius:4px;pointer-events:none}
.pl-bar .buf{background:rgba(255,255,255,.28)}.pl-bar .fill{background:linear-gradient(90deg,var(--amber),var(--amber2));box-shadow:0 0 12px color-mix(in srgb,var(--a2) 80%,transparent)}
.pl-bar .knob{position:absolute;width:14px;height:14px;border-radius:50%;background:#fff;margin-left:-7px;box-shadow:0 0 0 4px color-mix(in srgb,var(--a1) 35%,transparent);pointer-events:none;transition:transform .15s}
.pl-bar .tip{position:absolute;bottom:22px;padding:2px 7px;border-radius:6px;background:#000c;font-size:11px;font-weight:800;transform:translateX(-50%);display:none}
.pl-bar:hover .tip{display:block}
.pl-row{display:flex;align-items:center;gap:2px}
.pb{width:40px;height:40px;border-radius:50%;display:grid;place-items:center;flex:none;transition:background .2s}
.pb:hover{background:rgba(255,255,255,.14)}.pb svg{width:21px;height:21px}
.pl-time{font-size:12.5px;font-weight:800;margin:0 8px;white-space:nowrap;font-variant-numeric:tabular-nums}
.pl-sp{flex:1}
.vol{width:0;opacity:0;transition:width .25s,opacity .25s;accent-color:var(--amber);height:4px}
@media(hover:hover){.pl-row:hover .vol{width:70px;opacity:1}}
@media(max-width:560px){.pl-row .pb[data-a=mute]{display:none}.pb{width:36px;height:36px}.pb svg{width:19px;height:19px}.pl-time{margin:0 4px;font-size:11.5px}.pl-top{padding:10px}}
.pl-menu{position:absolute;right:10px;bottom:70px;z-index:6;width:min(290px,calc(100% - 20px));max-height:78%;overflow-y:auto;border-radius:16px;background:rgba(10,15,30,.94);backdrop-filter:blur(16px);border:1px solid var(--line2);padding:8px;display:none;pointer-events:auto;box-shadow:0 20px 60px #000a}
.pl-menu.show{display:block;animation:pgin .25s}
.pl-menu h4{font:600 11.5px var(--fd);color:var(--amber);padding:10px 10px 6px}
.pl-menu button{display:flex;width:100%;align-items:center;justify-content:space-between;gap:10px;padding:9px 10px;border-radius:10px;text-align:left;font-size:13px;font-weight:700}
.pl-menu button:hover{background:rgba(255,255,255,.08)}.pl-menu button.on{color:var(--amber)}.pl-menu button.on::after{content:"✓"}.pl-menu button.dead{opacity:.4;text-decoration:line-through}
.pl-menu small{color:var(--dim);font-weight:600}
.pl-seek{position:absolute;top:50%;width:90px;height:90px;margin-top:-45px;border-radius:50%;display:grid;place-items:center;background:rgba(255,255,255,.14);font-weight:800;opacity:0;pointer-events:none;z-index:4}
.pl-seek.l{left:12%}.pl-seek.r{right:12%}.pl-seek.go{animation:seekp .6s}
@keyframes seekp{0%{opacity:0;transform:scale(.6)}30%{opacity:1}100%{opacity:0;transform:scale(1.15)}}
.srcs{display:flex;gap:8px;flex-wrap:wrap;margin-top:12px}
.srcs .chip.dead{opacity:.4;text-decoration:line-through}
#theatre{position:fixed;inset:0;z-index:80;background:rgba(3,5,12,.94);backdrop-filter:blur(14px);display:none;overflow-y:auto}
#theatre.open{display:block;animation:pgin .3s}
.thin{max-width:1100px;margin:0 auto;padding:calc(14px + var(--st)) 14px 40px}

/* more sheet */
#sheet{position:fixed;inset:0;z-index:70;background:rgba(3,5,12,.6);backdrop-filter:blur(6px);opacity:0;visibility:hidden;transition:.3s}
#sheet.open{opacity:1;visibility:visible}
#sheet .in{position:absolute;left:0;right:0;bottom:0;padding:18px 16px calc(24px + var(--sb));border-radius:26px 26px 0 0;background:var(--ink);border:1px solid var(--line2);transform:translateY(100%);transition:transform .4s cubic-bezier(.2,.9,.2,1);max-width:560px;margin:auto}
#sheet.open .in{transform:none}
#sheet .grab{width:44px;height:4px;border-radius:4px;background:var(--line2);margin:0 auto 16px}
.mg{display:grid;grid-template-columns:repeat(3,1fr);gap:10px}
.mg a{display:flex;flex-direction:column;align-items:center;gap:8px;padding:16px 6px;border-radius:16px;background:rgba(255,255,255,.04);border:1px solid var(--line);font-size:12.5px;font-weight:800;transition:.2s}
.mg a:hover,.mg a.on{border-color:color-mix(in srgb,var(--a1) 50%,transparent);color:var(--amber)}.mg svg{width:24px;height:24px}

/* gate */
.gate{min-height:70vh;display:grid;place-items:center;padding:100px 20px 20px;text-align:center}
.gate .box{max-width:420px;padding:34px 26px;border-radius:26px;background:var(--panel);border:1px solid var(--line2);box-shadow:0 30px 80px #0008}
.gate h1{font:700 26px var(--fd);margin-bottom:12px}.gate p{color:var(--dim);margin-bottom:22px}.gate .acts{justify-content:center}
.moon{width:74px;height:74px;margin:0 auto 18px;border-radius:50%;background:radial-gradient(circle at 34% 34%,#fff4d6,#ffb224 55%,#b35a00);box-shadow:0 0 60px color-mix(in srgb,var(--a2) 50%,transparent);position:relative}
.moon::after{content:"";position:absolute;inset:0;border-radius:50%;background:radial-gradient(circle at 70% 40%,transparent 38%,var(--panel) 40%)}

/* toast */
#toast{position:fixed;left:50%;bottom:calc(var(--dockh) + 34px + var(--sb));transform:translate(-50%,20px);z-index:300;padding:11px 18px;border-radius:14px;background:rgba(20,30,58,.96);border:1px solid var(--line2);font-weight:700;font-size:13.5px;opacity:0;pointer-events:none;transition:.3s;max-width:90vw;text-align:center}
#toast.show{opacity:1;transform:translate(-50%,0)}
@media(min-width:900px){#toast{bottom:30px}}
@media(prefers-reduced-motion:reduce){*,*::before,*::after{animation-duration:.01ms!important;animation-iteration-count:1!important;transition-duration:.01ms!important;scroll-behavior:auto!important}}
/* ================= v2: living colour system + motion ================= */
@property --a1{syntax:'<color>';inherits:true;initial-value:#ffb224}
@property --a2{syntax:'<color>';inherits:true;initial-value:#ff5e3a}
@property --ang{syntax:'<angle>';inherits:false;initial-value:0deg}
body{--a1:#ffb224;--a2:#ff5e3a;--on:#1b0d00;--amber:var(--a1);--amber2:var(--a2);transition:--a1 1s ease,--a2 1s ease;background:var(--void)}
body[data-sec=movies]{--a1:#4da3ff;--a2:#7a5cff;--on:#04122e}
body[data-sec=series]{--a1:#b36bff;--a2:#ff4fd8;--on:#17062b}
body[data-sec=anime]{--a1:#ff5fa2;--a2:#ff9a5c;--on:#2a0716}
body[data-sec="4k"]{--a1:#19e6d1;--a2:#4da3ff;--on:#02201d}
body[data-sec=drama]{--a1:#ff6b81;--a2:#ffb36b;--on:#2b0710}
body[data-sec=live]{--a1:#ff4057;--a2:#ff8a3d;--on:#2a0508}
body[data-sec=midnight]{--a1:#9d8cff;--a2:#4f6bff;--on:#0c0a2e}
body[data-sec=downloads]{--a1:#3ff0a0;--a2:#19c3ff;--on:#012616}
body[data-sec=library]{--a1:#ffd166;--a2:#ff9f43;--on:#241500}
body[data-sec=search]{--a1:#7ee8ff;--a2:#a98bff;--on:#04172a}
::selection{background:var(--a1);color:var(--on)}

/* aurora follows the accent */
#aurora{background:radial-gradient(130% 90% at 50% -15%,color-mix(in srgb,var(--a2) 14%,#0b1030) 0%,var(--void) 62%)}
#aurora i{opacity:.24;filter:blur(100px)}
#aurora i:nth-child(1){background:var(--a1);left:-24vmax;top:-22vmax;opacity:.2}
#aurora i:nth-child(2){background:var(--a2);right:-26vmax;top:18vh;opacity:.2;animation-duration:30s}
#aurora i:nth-child(3){background:#6a3dff;left:18vw;bottom:-38vmax;opacity:.14;animation-duration:36s}
#spot{position:fixed;left:0;top:0;width:560px;height:560px;margin:-280px 0 0 -280px;z-index:-1;pointer-events:none;border-radius:50%;background:radial-gradient(circle,color-mix(in srgb,var(--a1) 13%,transparent),transparent 62%);opacity:0;transition:opacity .6s;will-change:transform}
@media(hover:hover) and (min-width:900px){#spot.on{opacity:1}}
#prog{position:fixed;top:0;left:0;right:0;height:3px;z-index:120;transform-origin:left;transform:scaleX(0);background:linear-gradient(90deg,var(--a1),var(--a2));box-shadow:0 0 14px var(--a1);pointer-events:none;transition:transform .12s linear}
#stars{position:fixed;inset:0;z-index:-1;pointer-events:none;opacity:0;transition:opacity 1.2s}
body[data-sec=midnight] #stars{opacity:1}
#stars b{position:absolute;width:2px;height:2px;border-radius:50%;background:#dfe6ff;box-shadow:0 0 6px #aab8ff;animation:twk 3.4s ease-in-out infinite}
#stars em{position:absolute;top:-10px;width:2px;height:90px;background:linear-gradient(#fff,transparent);opacity:0;transform:rotate(35deg);animation:shoot 7s linear infinite}
@keyframes twk{50%{opacity:.15;transform:scale(.6)}}
@keyframes shoot{0%{opacity:0;transform:translate3d(0,0,0) rotate(35deg)}4%{opacity:.9}16%{opacity:0;transform:translate3d(-340px,420px,0) rotate(35deg)}100%{opacity:0}}

/* splash: light sweep + iris open */
#splash{background:radial-gradient(circle at 50% 46%,#10163a,var(--void) 62%);transition:opacity .7s .15s,visibility .7s .15s,clip-path .9s cubic-bezier(.7,0,.2,1);clip-path:circle(150% at 50% 50%)}
#splash.out{clip-path:circle(0% at 50% 50%)}
#splash svg{width:104px;height:104px}
#splash .ring{stroke:#ffb224;filter:drop-shadow(0 0 14px rgba(255,178,36,.8))}
#splash .tri{fill:#ffb224}
#splash b{background:linear-gradient(100deg,#fff 30%,#ffb224 50%,#fff 70%) 0 0/250% 100%;-webkit-background-clip:text;background-clip:text;color:transparent;animation:rise .6s .8s forwards,sweep 1.6s .9s ease-in-out forwards;letter-spacing:.3em}
@keyframes sweep{to{background-position:-150% 0}}

/* top bar + nav pill */
#top.solid{background:rgba(5,6,15,.66)}
#nav{position:relative}
#navpill{position:absolute;top:50%;left:0;height:32px;margin-top:-16px;border-radius:99px;background:linear-gradient(135deg,var(--a1),var(--a2));box-shadow:0 6px 22px color-mix(in srgb,var(--a2) 45%,transparent);transition:transform .45s cubic-bezier(.3,1.25,.5,1),width .35s,opacity .3s;opacity:0;pointer-events:none}
#nav a{position:relative;z-index:1}
#nav a.on{background:none;box-shadow:none;color:var(--on)}
.logo span{background:linear-gradient(100deg,#fff,color-mix(in srgb,var(--a1) 70%,#fff));-webkit-background-clip:text;background-clip:text;color:transparent}
.logo svg{filter:drop-shadow(0 0 8px color-mix(in srgb,var(--a1) 70%,transparent))}
.logo .ring{stroke:var(--a1)}.logo .tri{fill:var(--a1)}
.ibtn{background:rgba(255,255,255,.05);backdrop-filter:blur(10px)}
.ibtn:hover{border-color:var(--a1);color:var(--a1);transform:translateY(-1px)}

/* dock */
#dock{background:rgba(10,13,32,.72);border-color:var(--line2);box-shadow:0 18px 50px rgba(0,0,0,.6),0 0 0 1px rgba(255,255,255,.03) inset,0 10px 40px -10px color-mix(in srgb,var(--a1) 30%,transparent)}
#dockglow{background:linear-gradient(180deg,color-mix(in srgb,var(--a1) 24%,transparent),color-mix(in srgb,var(--a2) 6%,transparent));border-color:color-mix(in srgb,var(--a1) 45%,transparent);box-shadow:0 0 24px color-mix(in srgb,var(--a1) 25%,transparent)}
#dock .on svg{filter:drop-shadow(0 0 8px var(--a1))}

/* headings */
.ph h1{background:linear-gradient(95deg,#fff 20%,color-mix(in srgb,var(--a1) 55%,#fff) 100%);-webkit-background-clip:text;background-clip:text;color:transparent;display:inline-block}
.ph h1::after{content:"";display:block;width:60px;height:4px;border-radius:4px;margin-top:12px;background:linear-gradient(90deg,var(--a1),var(--a2));box-shadow:0 0 16px var(--a1);animation:grow .9s cubic-bezier(.2,.9,.2,1) both}
@keyframes grow{from{width:0}}
.rh h2::before{background:linear-gradient(var(--a1),var(--a2));box-shadow:0 0 14px var(--a1);animation:pulsebar 3.2s ease-in-out infinite}
@keyframes pulsebar{50%{transform:scaleY(.62)}}

/* chips */
.chip{backdrop-filter:blur(8px);transition:.25s cubic-bezier(.2,.9,.2,1)}
.chip:hover{transform:translateY(-2px)}
.chip.on{background:linear-gradient(135deg,color-mix(in srgb,var(--a1) 24%,transparent),color-mix(in srgb,var(--a2) 20%,transparent));border-color:color-mix(in srgb,var(--a1) 70%,transparent);color:#fff;box-shadow:0 6px 22px -6px color-mix(in srgb,var(--a1) 55%,transparent)}
.sbox:focus-within{border-color:var(--a1);box-shadow:0 0 0 4px color-mix(in srgb,var(--a1) 14%,transparent),0 12px 40px -12px color-mix(in srgb,var(--a1) 40%,transparent)}
.sbox{background:rgba(255,255,255,.04);backdrop-filter:blur(10px)}

/* buttons: sheen + ripple */
.btn.pri{background:linear-gradient(120deg,color-mix(in srgb,var(--a1) 80%,#fff),var(--a1) 40%,var(--a2));background-size:180% 100%;box-shadow:0 10px 30px -6px color-mix(in srgb,var(--a2) 60%,transparent),inset 0 1px 0 rgba(255,255,255,.35)}
.btn.pri:hover{background-position:100% 0;transform:translateY(-2px);box-shadow:0 16px 40px -6px color-mix(in srgb,var(--a2) 75%,transparent),inset 0 1px 0 rgba(255,255,255,.4)}
.btn.pri::after{content:"";position:absolute;top:0;bottom:0;width:40%;left:-60%;background:linear-gradient(100deg,transparent,rgba(255,255,255,.45),transparent);transform:skewX(-20deg);transition:left .7s}
.btn.pri:hover::after{left:130%}
.btn.ghost{background:rgba(255,255,255,.06);border-color:var(--line2)}
.btn.ghost:hover{border-color:var(--a1);transform:translateY(-2px);background:color-mix(in srgb,var(--a1) 10%,rgba(255,255,255,.06))}
.btn.on{color:var(--a1)}
.btn .rip,.pb .rip{position:absolute;border-radius:50%;pointer-events:none;background:rgba(255,255,255,.45);transform:scale(0);animation:rip .6s ease-out forwards}
@keyframes rip{to{transform:scale(1);opacity:0}}
.btn.busy{pointer-events:none;opacity:.85}
.btn.busy::before{content:"";width:14px;height:14px;border-radius:50%;border:2px solid currentColor;border-top-color:transparent;animation:spin .7s linear infinite;flex:none}

/* hero */
#hero{margin-bottom:-1px}
.hs::after{background:linear-gradient(0deg,var(--void) 3%,rgba(5,6,15,.55) 38%,rgba(5,6,15,.08) 70%),linear-gradient(90deg,rgba(5,6,15,.9) 0%,rgba(5,6,15,.25) 55%,transparent),radial-gradient(70% 60% at 85% 30%,color-mix(in srgb,var(--a2) 18%,transparent),transparent)}
.hc{transform:translateY(calc(var(--py,0)*-.1px));will-change:transform}
.hc h2{max-width:18ch}
.w{display:inline-block;overflow:hidden;vertical-align:top;padding:.06em .14em .12em 0;margin-right:.05em}
.w i{display:inline-block;font-style:normal;transform:translateY(112%) rotate(4deg)}
.hs.on .w i{animation:wordup .85s cubic-bezier(.2,.9,.2,1) calc(.12s + var(--i)*.07s) forwards}
@keyframes wordup{to{transform:none}}
.hs.on .hc h2{animation:none}
.tag.am{background:color-mix(in srgb,var(--a1) 16%,transparent);border-color:color-mix(in srgb,var(--a1) 50%,transparent);color:var(--a1)}
#hdots button::after{background:linear-gradient(90deg,var(--a1),var(--a2))}
.hthumbs{display:none}
@media(min-width:900px){
  .hthumbs{position:absolute;z-index:4;right:32px;top:50%;transform:translateY(-50%);display:flex;flex-direction:column;gap:10px}
  .hthumbs button{width:54px;height:78px;border-radius:11px;overflow:hidden;border:2px solid transparent;opacity:.55;transition:.35s cubic-bezier(.2,.9,.2,1);position:relative;background:#141a3a}
  .hthumbs button img{width:100%;height:100%;object-fit:cover}
  .hthumbs button:hover{opacity:.9;transform:translateX(-4px)}
  .hthumbs button.on{opacity:1;border-color:var(--a1);width:64px;height:92px;transform:translateX(-8px);box-shadow:0 10px 30px -6px color-mix(in srgb,var(--a1) 60%,transparent)}
  #hdots{right:120px}
}

/* cards: 3D tilt + rotating border + play bubble */
.rail,.grid{perspective:1200px}
.card{transition:transform .3s}
.poster{background:linear-gradient(160deg,#1a2350,#0b1030);transform-style:preserve-3d;will-change:transform}
.poster::before{inset:0;padding:2px;border-radius:var(--r);opacity:0;background:conic-gradient(from var(--ang),var(--a1),var(--a2),#7a5cff,var(--a1));-webkit-mask:linear-gradient(#000 0 0) content-box,linear-gradient(#000 0 0);-webkit-mask-composite:xor;mask:linear-gradient(#000 0 0) content-box exclude,linear-gradient(#000 0 0);animation:ang 3.2s linear infinite;transition:opacity .35s}
@keyframes ang{to{--ang:360deg}}
.poster::after{content:"";position:absolute;inset:0;z-index:2;pointer-events:none;background:linear-gradient(0deg,rgba(5,6,15,.82),transparent 46%);opacity:0;transition:opacity .35s}
.poster.noimg::after{opacity:1;background:none}
.pp{position:absolute;z-index:4;left:50%;top:50%;width:50px;height:50px;margin:-25px 0 0 -25px;border-radius:50%;display:grid;place-items:center;background:linear-gradient(135deg,var(--a1),var(--a2));color:var(--on);box-shadow:0 10px 30px color-mix(in srgb,var(--a2) 60%,transparent);transform:scale(.4);opacity:0;transition:transform .45s cubic-bezier(.2,1.5,.4,1),opacity .3s;pointer-events:none}
.pp svg{width:20px;height:20px;margin-left:2px}
@media(hover:hover){
  .card:hover .poster{transform:translateY(-8px) scale(1.035) rotateX(var(--rx,0deg)) rotateY(var(--ry,0deg));border-color:transparent;box-shadow:0 26px 54px -10px rgba(0,0,0,.75),0 0 40px -6px color-mix(in srgb,var(--a1) 40%,transparent)}
  .card:hover .poster::before{opacity:1}.card:hover .poster::after{opacity:1}.card:hover .pp{transform:scale(1);opacity:1}
  .card:hover .ct h3{color:var(--a1)}
}
.ct h3{transition:color .25s}
.rt{background:rgba(5,6,15,.72);border:1px solid color-mix(in srgb,var(--a1) 30%,transparent);color:var(--a1);backdrop-filter:blur(8px)}
.bd{background:color-mix(in srgb,var(--a2) 88%,transparent);color:#fff}
.bd.k{background:linear-gradient(135deg,var(--a1),var(--a2));color:var(--on)}
.pg-bar i{background:linear-gradient(90deg,var(--a1),var(--a2));box-shadow:0 0 10px var(--a1)}
.top10 .num{-webkit-text-stroke:2px var(--a1);text-shadow:0 0 28px color-mix(in srgb,var(--a1) 60%,transparent)}
/* staggered reveal */
.card[data-rv]{opacity:0;transform:translateY(22px) scale(.95)}
.card[data-rv].in{opacity:1;transform:none;transition:opacity .7s cubic-bezier(.2,.9,.2,1) var(--d,0ms),transform .7s cubic-bezier(.2,.9,.2,1) var(--d,0ms)}
.row .rh{opacity:0;transform:translateX(-14px);transition:.7s cubic-bezier(.2,.9,.2,1)}
.row.in .rh{opacity:1;transform:none}

/* skeleton in accent */
.sk{background:linear-gradient(100deg,rgba(255,255,255,.04) 30%,color-mix(in srgb,var(--a1) 12%,rgba(255,255,255,.08)) 50%,rgba(255,255,255,.04) 70%) 0 0/300% 100%}

/* live tv */
.ch:hover{border-color:var(--a1);box-shadow:0 14px 34px -10px color-mix(in srgb,var(--a1) 45%,transparent)}
.live{color:var(--a1)}.live::before{background:var(--a1);box-shadow:0 0 10px var(--a1)}

/* detail */
.dposter{transform:perspective(900px) rotateY(-6deg);transition:transform .6s cubic-bezier(.2,.9,.2,1);border-color:color-mix(in srgb,var(--a1) 30%,var(--line2));box-shadow:0 30px 70px rgba(0,0,0,.7),0 0 50px -10px color-mix(in srgb,var(--a1) 40%,transparent)}
.dposter:hover{transform:perspective(900px) rotateY(0deg) translateY(-4px)}
.dh h1{background:linear-gradient(95deg,#fff 30%,color-mix(in srgb,var(--a1) 50%,#fff));-webkit-background-clip:text;background-clip:text;color:transparent}
.dbody>*{animation:pgin .55s cubic-bezier(.2,.8,.2,1) both}.dbody>*:nth-child(2){animation-delay:.08s}.dbody>*:nth-child(3){animation-delay:.14s}.dbody>*:nth-child(4){animation-delay:.2s}
.tabs{position:relative}
.tabs button.on::after{display:none}
.tabs .ink{position:absolute;left:0;bottom:-1px;height:3px;border-radius:3px;background:linear-gradient(90deg,var(--a1),var(--a2));box-shadow:0 0 14px var(--a1);transition:transform .45s cubic-bezier(.3,1.2,.5,1),width .35s}
.tabs button.on{color:var(--a1)}
.ep.on{background:linear-gradient(135deg,var(--a1),var(--a2));color:var(--on);box-shadow:0 8px 24px -4px color-mix(in srgb,var(--a2) 60%,transparent)}
.epi.on{border-color:var(--a1);background:linear-gradient(90deg,color-mix(in srgb,var(--a1) 14%,transparent),transparent)}
.epi b{color:var(--a1)}
.dli{transition:.25s}.dli:hover{border-color:var(--line2);transform:translateX(3px)}
.panel{backdrop-filter:blur(12px)}
.pcover .pb{background:linear-gradient(135deg,var(--a1),var(--a2));box-shadow:0 0 0 10px color-mix(in srgb,var(--a1) 18%,transparent),0 14px 44px color-mix(in srgb,var(--a2) 65%,transparent);animation:ringp 2.4s ease-out infinite}
.pcover .pb svg{color:var(--on)}
@keyframes ringp{0%{box-shadow:0 0 0 0 color-mix(in srgb,var(--a1) 55%,transparent),0 14px 44px color-mix(in srgb,var(--a2) 65%,transparent)}100%{box-shadow:0 0 0 34px transparent,0 14px 44px color-mix(in srgb,var(--a2) 65%,transparent)}}
.pcover{border-color:color-mix(in srgb,var(--a1) 25%,var(--line2))}

/* player skin */
.pl{border-color:color-mix(in srgb,var(--a1) 25%,var(--line2));box-shadow:0 40px 100px -24px color-mix(in srgb,var(--a2) 55%,transparent),0 20px 60px rgba(0,0,0,.6)}
.pl-bar .fill{background:linear-gradient(90deg,var(--a1),var(--a2));box-shadow:0 0 14px var(--a1)}
.pl-bar .knob{box-shadow:0 0 0 4px color-mix(in srgb,var(--a1) 35%,transparent),0 0 14px var(--a1)}
.pl-big{background:linear-gradient(135deg,color-mix(in srgb,var(--a1) 85%,transparent),color-mix(in srgb,var(--a2) 85%,transparent));color:var(--on);border:0;box-shadow:0 10px 40px color-mix(in srgb,var(--a2) 60%,transparent)}
.pl-spin{border-top-color:var(--a1)}
.pl-menu h4{color:var(--a1)}
.pl-menu button.on{color:var(--a1)}
.vol{accent-color:var(--a1)}

/* midnight gate */
.gate .box{background:linear-gradient(160deg,rgba(40,36,110,.55),rgba(10,12,40,.8));box-shadow:0 40px 100px rgba(10,10,60,.7),0 0 80px -20px var(--a1)}
.moon{background:radial-gradient(circle at 34% 34%,#fff,#d7ceff 40%,#7b6bff 75%,#3a2fa8);box-shadow:0 0 80px rgba(157,140,255,.7),0 0 160px rgba(79,107,255,.35);animation:floaty 5s ease-in-out infinite}
@keyframes floaty{50%{transform:translateY(-8px)}}

/* more sheet / toast */
.mg a:hover,.mg a.on{border-color:var(--a1);color:var(--a1);transform:translateY(-3px)}
#toast{border-color:color-mix(in srgb,var(--a1) 40%,var(--line2));box-shadow:0 12px 40px rgba(0,0,0,.5),0 0 30px -8px var(--a1)}
.note{border-radius:13px}
.pl-msg{border-color:color-mix(in srgb,var(--a1) 35%,var(--line2))}
.cast i{background:linear-gradient(135deg,var(--a1),var(--a2));color:var(--on)}
.fkout{display:grid;gap:8px;margin-top:6px}
.fkout:empty{display:none}
.tag.k4{background:linear-gradient(135deg,var(--a1),var(--a2));color:var(--on);border:0}
@media(prefers-reduced-motion:reduce){.card[data-rv]{opacity:1;transform:none}.w i{transform:none}.row .rh{opacity:1;transform:none}}
</style>
</head>
<body>
<svg width="0" height="0" style="position:absolute" aria-hidden="true"><defs>
<symbol id="i-home" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M3 11l9-8 9 8v9a1 1 0 0 1-1 1h-5v-6H9v6H4a1 1 0 0 1-1-1z"/></symbol>
<symbol id="i-film" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="3" y="4" width="18" height="16" rx="3"/><path d="M7 4v16M17 4v16M3 9h4M3 15h4M17 9h4M17 15h4"/></symbol>
<symbol id="i-tv" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="3" y="6" width="18" height="12" rx="2"/><path d="M8 21h8M9 3l3 3 3-3"/></symbol>
<symbol id="i-spark" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M12 3l1.8 5.2L19 10l-5.2 1.8L12 17l-1.8-5.2L5 10l5.2-1.8zM19 16l.8 2.2L22 19l-2.2.8L19 22l-.8-2.2L16 19l2.2-.8z"/></symbol>
<symbol id="i-4k" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="2.5" y="5" width="19" height="14" rx="3"/><path d="M7 9v3.5h3M10 9v6M13.5 9v6M13.5 12l3-3M13.5 12l3 3"/></symbol>
<symbol id="i-heart" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M12 21s-8-5.2-8-11a4.6 4.6 0 0 1 8-3 4.6 4.6 0 0 1 8 3c0 5.8-8 11-8 11z"/></symbol>
<symbol id="i-live" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="2.4"/><path d="M7.8 7.8a6 6 0 0 0 0 8.4M16.2 7.8a6 6 0 0 1 0 8.4M4.9 4.9a10 10 0 0 0 0 14.2M19.1 4.9a10 10 0 0 1 0 14.2"/></symbol>
<symbol id="i-moon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M20 14.5A8.5 8.5 0 1 1 9.5 4a7 7 0 0 0 10.5 10.5z"/></symbol>
<symbol id="i-dl" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M12 3v12M7 11l5 5 5-5M4 20h16"/></symbol>
<symbol id="i-lib" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M5 4v16M10 4v16M15 5l4 15"/></symbol>
<symbol id="i-search" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round"><circle cx="11" cy="11" r="7"/><path d="M20 20l-3.5-3.5"/></symbol>
<symbol id="i-more" viewBox="0 0 24 24" fill="currentColor"><circle cx="5" cy="12" r="2"/><circle cx="12" cy="12" r="2"/><circle cx="19" cy="12" r="2"/></symbol>
<symbol id="i-play" viewBox="0 0 24 24" fill="currentColor"><path d="M7 4.5v15a1 1 0 0 0 1.5.9l12-7.5a1 1 0 0 0 0-1.8l-12-7.5A1 1 0 0 0 7 4.5z"/></symbol>
<symbol id="i-pause" viewBox="0 0 24 24" fill="currentColor"><rect x="6" y="4" width="4.5" height="16" rx="1.2"/><rect x="13.5" y="4" width="4.5" height="16" rx="1.2"/></symbol>
<symbol id="i-back" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.4" stroke-linecap="round" stroke-linejoin="round"><path d="M15 5l-7 7 7 7"/></symbol>
<symbol id="i-next" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.4" stroke-linecap="round" stroke-linejoin="round"><path d="M9 5l7 7-7 7"/></symbol>
<symbol id="i-x" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.4" stroke-linecap="round"><path d="M6 6l12 12M18 6L6 18"/></symbol>
<symbol id="i-plus" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.4" stroke-linecap="round"><path d="M12 5v14M5 12h14"/></symbol>
<symbol id="i-check" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.6" stroke-linecap="round" stroke-linejoin="round"><path d="M5 12.5l4.5 4.5L19 7"/></symbol>
<symbol id="i-gear" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="3"/><path d="M19.4 15a1.7 1.7 0 0 0 .3 1.8l.1.1a2 2 0 1 1-2.8 2.8l-.1-.1a1.7 1.7 0 0 0-1.8-.3 1.7 1.7 0 0 0-1 1.5V21a2 2 0 1 1-4 0v-.1a1.7 1.7 0 0 0-1.1-1.5 1.7 1.7 0 0 0-1.8.3l-.1.1a2 2 0 1 1-2.8-2.8l.1-.1a1.7 1.7 0 0 0 .3-1.8 1.7 1.7 0 0 0-1.5-1H3a2 2 0 1 1 0-4h.1a1.7 1.7 0 0 0 1.5-1.1 1.7 1.7 0 0 0-.3-1.8l-.1-.1a2 2 0 1 1 2.8-2.8l.1.1a1.7 1.7 0 0 0 1.8.3H9a1.7 1.7 0 0 0 1-1.5V3a2 2 0 1 1 4 0v.1a1.7 1.7 0 0 0 1 1.5 1.7 1.7 0 0 0 1.8-.3l.1-.1a2 2 0 1 1 2.8 2.8l-.1.1a1.7 1.7 0 0 0-.3 1.8V9a1.7 1.7 0 0 0 1.5 1H21a2 2 0 1 1 0 4h-.1a1.7 1.7 0 0 0-1.5 1z"/></symbol>
<symbol id="i-fs" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round"><path d="M4 9V4h5M20 9V4h-5M4 15v5h5M20 15v5h-5"/></symbol>
<symbol id="i-pip" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="3" y="5" width="18" height="14" rx="2.5"/><rect x="12" y="11" width="7" height="5" rx="1" fill="currentColor"/></symbol>
<symbol id="i-vol" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M4 9.5v5h3.5L12 18.5v-13L7.5 9.5zM16 9a4 4 0 0 1 0 6M18.6 6.4a8 8 0 0 1 0 11.2"/></symbol>
<symbol id="i-mute" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M4 9.5v5h3.5L12 18.5v-13L7.5 9.5zM16 9.5l5 5M21 9.5l-5 5"/></symbol>
<symbol id="i-b10" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M4 12a8 8 0 1 0 2.5-5.8M4 4v4.5h4.5"/><text x="12" y="15.5" font-size="7.5" font-weight="800" text-anchor="middle" fill="currentColor" stroke="none">10</text></symbol>
<symbol id="i-f10" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M20 12a8 8 0 1 1-2.5-5.8M20 4v4.5h-4.5"/><text x="12" y="15.5" font-size="7.5" font-weight="800" text-anchor="middle" fill="currentColor" stroke="none">10</text></symbol>
<symbol id="i-share" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="18" cy="5" r="2.6"/><circle cx="6" cy="12" r="2.6"/><circle cx="18" cy="19" r="2.6"/><path d="M8.3 10.8l7.4-4.3M8.3 13.2l7.4 4.3"/></symbol>
<symbol id="i-copy" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="9" y="9" width="11" height="11" rx="2.5"/><path d="M5 15V6a2 2 0 0 1 2-2h9"/></symbol>
<symbol id="i-ext" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M14 4h6v6M20 4l-9 9M18 14v5a1 1 0 0 1-1 1H5a1 1 0 0 1-1-1V7a1 1 0 0 1 1-1h5"/></symbol>
<symbol id="i-link" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M10 14a4.5 4.5 0 0 0 6.4 0l3-3a4.5 4.5 0 0 0-6.4-6.4l-1 1M14 10a4.5 4.5 0 0 0-6.4 0l-3 3a4.5 4.5 0 0 0 6.4 6.4l1-1"/></symbol>
<symbol id="i-bolt" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M13 2L4 14h7l-1 8 9-12h-7z"/></symbol>
<symbol id="i-api" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M8 6l-6 6 6 6M16 6l6 6-6 6M14 4l-4 16"/></symbol>
</defs></svg>

<div id="splash"><div><svg viewBox="0 0 64 64"><circle class="ring" cx="32" cy="32" r="22"/><path class="tri" d="M27 23l16 9-16 9z"/></svg><b>STREAMHUB</b></div></div>
<div id="aurora"><i></i><i></i><i></i></div><div id="grain"></div><div id="spot"></div><div id="stars"></div><div id="prog"></div>

<header id="top">
  <a class="logo" href="#/home" aria-label="StreamHub home"><svg viewBox="0 0 64 64"><circle class="ring" cx="32" cy="32" r="22" fill="none"/><path class="tri" d="M27 23l16 9-16 9z"/></svg><span>StreamHub</span></a>
  <nav id="nav" aria-label="Sections"></nav>
  <div class="tspace"></div>
  <a class="ibtn" href="#/search" aria-label="Search"><svg><use href="#i-search"/></svg></a>
  <a class="ibtn" href="#/library" aria-label="My library"><svg><use href="#i-heart"/></svg></a>
</header>

<main id="view" aria-live="polite"></main>

<nav id="dock" aria-label="Main"><span id="dockglow"></span></nav>

<div id="sheet"><div class="in"><div class="grab"></div><div class="mg" id="moreGrid"></div></div></div>
<div id="detail" aria-modal="true" role="dialog"><div id="dbg"></div><div id="dcontent"></div></div>
<div id="theatre"><div class="thin" id="tcontent"></div></div>
<div id="toast" role="status"></div>

<script src="https://cdnjs.cloudflare.com/ajax/libs/hls.js/1.5.15/hls.min.js" defer></script>
<script src="https://cdnjs.cloudflare.com/ajax/libs/dashjs/4.7.4/dash.all.min.js" defer></script>
<script>
/* ===== core utils + pure data normalizers (no DOM) ===== */
const enc=encodeURIComponent;
const sleep=ms=>new Promise(r=>setTimeout(r,ms));
const esc=s=>String(s==null?'':s).replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const pick=(o,...ks)=>{if(!o||typeof o!=='object')return undefined;for(const k of ks){const v=o[k];if(v!==undefined&&v!==null&&v!=='')return v}return undefined};
const imgOf=v=>{if(!v)return '';if(typeof v==='string')return v;if(Array.isArray(v))return imgOf(v[0]);return v.url||v.src||v.imageUrl||v.thumbnail||v.image||''};
const humanize=k=>String(k).replace(/[_-]+/g,' ').replace(/([a-z])([A-Z])/g,'$1 $2').replace(/\b\w/g,c=>c.toUpperCase()).trim();
const slugFromLink=l=>{try{const p=new URL(l).pathname.split('/').filter(Boolean);return p[p.length-1]||''}catch(e){return ''}};
const fmtTime=s=>{s=Math.max(0,Math.floor(s||0));const h=Math.floor(s/3600),m=Math.floor(s%3600/60),x=s%60;return(h?h+':'+String(m).padStart(2,'0'):m)+':'+String(x).padStart(2,'0')};
const fmtSize=v=>{if(v==null||v==='')return '';let n=Number(v);if(!isFinite(n)){return String(v)}if(n<=0)return '';const u=['B','KB','MB','GB','TB'];let i=0;while(n>=1024&&i<4){n/=1024;i++}return(n>=100||i===0?n.toFixed(0):n.toFixed(1))+' '+u[i]};
const qNum=q=>{const m=String(q==null?'':q).match(/(\d{3,4})/);if(m)return +m[1];if(/4k|uhd/i.test(q))return 2160;return 0};
const fmtDur=s=>{s=Number(s);if(!s)return '';if(s>1000)s=s;const h=Math.floor(s/3600),m=Math.round(s%3600/60);return h?h+'h '+m+'m':m+'m'};
const rtNum=v=>{const n=parseFloat(v);return isFinite(n)&&n>0?n.toFixed(1):''};

/* ---- MovieBox ---- */
function mbSubj(s){
  if(!s||typeof s!=='object')return null;
  const id=pick(s,'subjectId','id'),title=pick(s,'title','name');
  if(!id||!title)return null;
  const st=pick(s,'subjectType','type');
  const cover=imgOf(s.cover)||imgOf(s.poster)||imgOf(s.thumbnail)||imgOf(s.stills)||imgOf(s.image);
  const rd=String(pick(s,'releaseDate','year')||'');
  return{provider:'mb',id:String(id),title:String(title),poster:cover,backdrop:imgOf(s.stills)||'',year:rd.slice(0,4),
    rating:rtNum(pick(s,'imdbRatingValue','imdb','imdbRating','rating')),genre:pick(s,'genre')||'',
    type:(st==2||st==='2'||st==='series'||st==='tv')?'series':'movie',_st:st==null?'':String(st),desc:pick(s,'description','postTitle')||''};
}
/* walk a tab-operating payload; return {hero:[], sections:[{title,items}]} */
function mbSections(data){
  const hero=[],secs=[],byTitle=new Map();
  const looks=a=>a&&typeof a==='object'&&(a.subjectId||(a.subject&&a.subject.subjectId))&&(a.title||(a.subject&&a.subject.title));
  const toItem=(a,banner)=>{
    let s=a;
    if(a.subject&&typeof a.subject==='object'){s={...a.subject};if(!imgOf(s.cover)&&a.image)s.cover=a.image}
    const it=mbSubj(s);if(!it)return null;
    if(banner){const w=imgOf(a.image)||imgOf(a.stills)||imgOf(s.stills);it.backdrop=w||it.poster;if(!it.desc&&a.title&&a.title!==it.title)it.desc=a.title}
    return it;
  };
  const add=(title,items,banner)=>{
    items=items.filter(Boolean);if(!items.length)return;
    if(banner){const seen=new Set(hero.map(h=>h.id));items.forEach(i=>{if(!seen.has(i.id)){seen.add(i.id);hero.push(i)}});return}
    const t=title||'Featured';let sec=byTitle.get(t);
    if(!sec){sec={title:t,items:[]};byTitle.set(t,sec);secs.push(sec)}
    const seen=new Set(sec.items.map(h=>h.id));items.forEach(i=>{if(!seen.has(i.id)){seen.add(i.id);sec.items.push(i)}});
  };
  const walk=(node,title,banner,depth)=>{
    if(!node||typeof node!=='object'||depth>7)return;
    if(Array.isArray(node)){
      const subs=node.filter(looks);
      if(subs.length&&subs.length>=Math.min(2,node.length)){add(title,subs.map(a=>toItem(a,banner)),banner);return}
      node.forEach(n=>walk(n,title,banner,depth+1));return;
    }
    const isB=banner||/banner/i.test(String(node.type||''));
    const t=(typeof node.title==='string'&&node.title)||(typeof node.name==='string'&&node.name)||title;
    for(const k of Object.keys(node)){
      const v=node[k];if(!v||typeof v!=='object')continue;
      walk(v,t,isB||/banner/i.test(k),depth+1);
    }
  };
  walk(data,'',false,0);
  return{hero,sections:secs.filter(s=>s.items.length)};
}
function mbDetail(data){
  const d=(data&&data.subject&&typeof data.subject==='object')?data.subject:(data||{});
  const base=mbSubj({...d,subjectId:d.subjectId||d.id||(data&&data.subjectId)||'0',title:d.title||'Untitled'})||{};
  const tr=d.trailer&&(d.trailer.videoAddress&&d.trailer.videoAddress.url||d.trailer.url)||'';
  const stars=((data&&data.stars)||d.stars||d.staffList||[]).map(x=>({name:pick(x,'name','staffName'),role:pick(x,'character','role'),img:imgOf(pick(x,'avatarUrl','avatar','image'))})).filter(x=>x.name);
  return{...base,desc:pick(d,'description','synopsis')||base.desc||'',duration:Number(d.duration)||0,country:pick(d,'countryName','country')||'',trailer:tr,stars,
    poster:base.poster||imgOf(d.cover),backdrop:imgOf(d.stills)||base.poster,
    subtitles:pick(d,'subtitles')||''};
}
function mbSeasons(data){
  let arr=null;
  (function f(n,dep){if(arr||!n||typeof n!=='object'||dep>5)return;
    if(Array.isArray(n)){if(n.length&&n[0]&&typeof n[0]==='object'&&(n[0].se!==undefined||n[0].season!==undefined)){arr=n;return}n.forEach(x=>f(x,dep+1));return}
    if(Array.isArray(n.seasons)&&n.seasons.length){arr=n.seasons;return}
    Object.keys(n).forEach(k=>f(n[k],dep+1));
  })(data,0);
  if(!arr)return[];
  return arr.map(s=>{
    const se=Number(pick(s,'se','season','seasonNumber'));
    let max=Number(pick(s,'maxEp','episodeCount','epCount','totalEpisodes'))||0;
    if(!max&&s.allEp){max=String(s.allEp).split(',').filter(Boolean).length}
    return{se,max:max||0};
  }).filter(s=>s.se>=0).sort((a,b)=>a.se-b.se);
}
function mbStreams(cdn){
  const out=[];const d=cdn||{};
  (d.mp4||[]).forEach(m=>{if(!m.url)return;const k=/\.(mp4|m4v|webm)(\?|$)/i.test(m.url)||m.kind==='mp4'?'mp4':(/\.m3u8/i.test(m.url)?'hls':'file');
    out.push({kind:k,url:m.url,quality:m.quality,q:qNum(m.quality),size:m.size,codec:m.codec,label:'MP4',source:m.source||'play',dl:true,cookie:m.cookie||''})});
  (d.dash||[]).forEach(m=>{if(!m.base&&!m.url)return;out.push({kind:'dash',url:m.url,base:m.base,cookie:m.cookie,quality:m.quality,q:qNum(m.quality),size:m.size,codec:m.codec,label:'DASH',dl:false})});
  return out;
}
/* ---- HindiAnime ---- */
function haItem(a,kind){
  if(!a||typeof a!=='object')return null;
  const title=pick(a,'title','name');if(!title)return null;
  const link=pick(a,'link','url','href','watchUrl')||'';
  let id=pick(a,'id','slug');if(!id&&link)id=slugFromLink(link);
  const gen=Array.isArray(a.genres)?a.genres.join(', '):(a.genre||'');
  const t=String(pick(a,'type','kind')||kind||'').toLowerCase();
  return{provider:'ha',id:id?String(id):'',link,title:String(title),poster:imgOf(pick(a,'poster','image','img','thumbnail','cover','thumb','banner','portrait')),
    backdrop:imgOf(pick(a,'backdrop','banner','landscape','cover','image')),year:String(pick(a,'year','releaseYear')||'').slice(0,4),
    genre:gen,type:t.includes('movie')?'movie':'series',rating:rtNum(pick(a,'rating','score')),desc:pick(a,'description','overview','synopsis')||'',
    ep:pick(a,'episode','latestEpisode','episodeLabel')||''};
}
/* generic: find arrays of objects that map via fn, label by key name */
function genericSections(data,fn,minItems){
  const secs=[];const seen=new Set();minItems=minItems||1;
  (function w(n,title,dep){
    if(!n||typeof n!=='object'||dep>6)return;
    if(Array.isArray(n)){
      const items=n.map(x=>typeof x==='object'?fn(x):null).filter(Boolean);
      if(items.length>=minItems&&items.length>=n.length*0.5){const k=title+'|'+items.length+items[0].title;if(!seen.has(k)){seen.add(k);secs.push({title:title||'Featured',items})}return}
      n.forEach(x=>w(x,title,dep+1));return;
    }
    for(const k of Object.keys(n)){const v=n[k];if(v&&typeof v==='object')w(v,Array.isArray(v)?humanize(k):(typeof n.title==='string'?n.title:humanize(k)),dep+1)}
  })(data,'',0);
  return secs;
}
function haEpisodes(data){
  const d=data||{};const seasons=d.seasons;const out={};
  if(seasons&&typeof seasons==='object'&&!Array.isArray(seasons)){
    for(const sk of Object.keys(seasons)){out[sk]=(seasons[sk]||[]).map((e,i)=>({season:+sk||1,episode:Number(pick(e,'episode','number','ep'))||i+1,title:pick(e,'title','name')||'',url:pick(e,'url','link')||'',thumb:imgOf(pick(e,'thumb','image','poster')),raw:e}))}
  }else if(Array.isArray(seasons)){out['1']=seasons.map((e,i)=>({season:1,episode:Number(pick(e,'episode','number','ep'))||i+1,title:pick(e,'title','name')||'',url:pick(e,'url','link')||'',raw:e}))}
  return out;
}
function haServers(d){
  d=d||{};const all=(d.servers||[]).map(s=>{
    const t=s.type||'link';
    return{kind:t==='iframe'?'iframe':t==='hls_proxy'?'hls':t==='hls'?'hls':t==='mp4'?'mp4':'file',url:s.url,label:s.name||'Server',audio:s.audio||s.langLabel||'',alive:s.alive,dead:!!s.dead,hash:s.hash,raw:t}});
  return all.filter(s=>s.url);
}
/* ---- 4KHDHub ---- */
function fkItem(a){
  if(!a||!a.title)return null;
  return{provider:'fk',id:String(a.id||a.url||''),title:a.title,poster:a.poster||'',year:a.year||'',genre:a.meta||'',type:a.type==='series'?'series':'movie',rating:'',quality:/4k|2160|uhd/i.test((a.meta||'')+(a.title||''))?'4K':''};
}
/* ---- Dramachi ---- */
function drItem(a){
  if(!a||!a.title)return null;
  return{provider:'dr',id:String(a.id||''),title:a.title,poster:a.poster||'',year:a.year||'',genre:a.category||'',content:a.content||'movies',type:(a.content||'')==='series'?'series':'movie',views:a.views,rating:''};
}
/* ---- routing hrefs ---- */
function hrefOf(it){
  switch(it.provider){
    case'mb':return'#/mb/'+enc(it.id);
    case'ha':return it.id?'#/ha/'+enc(it.id):'#/hau/'+enc(it.link);
    case'fk':return'#/fk/'+enc(it.id);
    case'dr':return'#/dr/'+enc(it.id)+'/'+enc(it.content||'movies');
  }return'#/home';
}
function extIntent(url){
  try{const u=new URL(url);return'intent:'+url.replace(/^https?:\/\//,'')+'#Intent;scheme='+u.protocol.replace(':','')+';type=video/*;end'}catch(e){return url}
}
function b64u(s){return btoa(unescape(encodeURIComponent(s))).replace(/\+/g,'-').replace(/\//g,'_').replace(/=+$/,'')}
function dashProxyUrl(base,cookie){return'/px/'+b64u(JSON.stringify({b:base,c:cookie||''}))+'/index.mpd'}
function gxUrl(u,extra){return'/gx?u='+enc(u)+(extra&&extra.cookie?'&c='+enc(extra.cookie):'')}
if(typeof module!=='undefined')module.exports={mbSubj,mbSections,mbDetail,mbSeasons,mbStreams,haItem,genericSections,haEpisodes,haServers,fkItem,drItem,hrefOf,dashProxyUrl,gxUrl,qNum,fmtSize,fmtTime,extIntent,b64u,humanize};

/* ===== Player: MP4 / DASH / HLS / iframe with fallbacks ===== */
const ico=(n,cls)=>'<svg'+(cls?' class="'+cls+'"':'')+'><use href="#i-'+n+'"/></svg>';
class Player{
  constructor(host,opts){
    this.host=host;this.o=opts||{};this.sources=[];this.idx=-1;this.ctx={};this.dash=null;this.hls=null;this.started=false;this.dead=false;
    this.speed=1;this.fit=false;this.menuOpen=false;this.lastSave=0;this.tapT=0;this.tapX=0;this.wl=null;this.ptype='mouse';
    host.innerHTML='<div class="pl idle" tabindex="0">'+
    '<video playsinline webkit-playsinline preload="auto"></video><iframe allowfullscreen allow="autoplay; fullscreen; picture-in-picture; encrypted-media" referrerpolicy="no-referrer"></iframe>'+
    '<div class="pl-ui"><div class="pl-top"><span class="pl-title"></span><span class="pl-src"></span></div>'+
    '<button class="pl-big" aria-label="Play">'+ico('play')+'</button><div class="pl-spin"></div><div class="pl-seek l">−10s</div><div class="pl-seek r">+10s</div>'+
    '<div class="pl-msg"></div><div class="pl-menu"></div>'+
    '<div class="pl-bot"><div class="pl-bar"><div class="buf"></div><div class="fill"></div><div class="knob"></div><span class="tip">0:00</span></div>'+
    '<div class="pl-row"><button class="pb" data-a="pp" aria-label="Play/Pause">'+ico('play')+'</button><button class="pb" data-a="b10" aria-label="Back 10 seconds">'+ico('b10')+'</button><button class="pb" data-a="f10" aria-label="Forward 10 seconds">'+ico('f10')+'</button>'+
    '<button class="pb" data-a="mute" aria-label="Mute">'+ico('vol')+'</button><input class="vol" type="range" min="0" max="1" step="0.05" value="1" aria-label="Volume">'+
    '<span class="pl-time">0:00 / 0:00</span><span class="pl-sp"></span>'+
    '<button class="pb" data-a="next" aria-label="Next episode" style="display:none">'+ico('next')+'</button>'+
    '<button class="pb" data-a="gear" aria-label="Settings">'+ico('gear')+'</button><button class="pb" data-a="pip" aria-label="Picture in picture">'+ico('pip')+'</button><button class="pb" data-a="fs" aria-label="Fullscreen">'+ico('fs')+'</button></div></div></div>'+
    '<input type="file" accept=".srt,.vtt" hidden></div>';
    const q=s=>host.querySelector(s);
    this.el=q('.pl');this.v=q('video');this.f=q('iframe');this.ui=q('.pl-ui');this.msg=q('.pl-msg');this.menu=q('.pl-menu');
    this.bar=q('.pl-bar');this.fill=q('.fill');this.buf=q('.buf');this.knob=q('.knob');this.tip=q('.tip');this.time=q('.pl-time');
    this.title=q('.pl-title');this.srcTag=q('.pl-src');this.ppBtn=q('[data-a=pp]');this.vol=q('.vol');this.file=q('input[type=file]');this.nextBtn=q('[data-a=next]');
    this._bind();
  }
  _bind(){
    const v=this.v,el=this.el;this._on=[];
    const on=(t,e,f,o)=>{t.addEventListener(e,f,o);this._on.push([t,e,f,o])};
    this.on=on;
    on(v,'play',()=>this._state());on(v,'pause',()=>{this._state();this._save(true)});
    on(v,'playing',()=>{this.started=true;this._busy(false);this._hideMsg();clearTimeout(this.wd);this._state();this._lock(true);this._media();this._pic()});
    on(v,'waiting',()=>this._busy(true));on(v,'seeking',()=>this._busy(true));on(v,'canplay',()=>this._busy(false));on(v,'seeked',()=>this._busy(false));
    on(v,'loadedmetadata',()=>{this._tracks();const r=this.ctx.resume;if(r&&r>20&&v.duration&&r<v.duration*0.95&&!this.resumed){this.resumed=true;try{v.currentTime=r}catch(e){}}v.playbackRate=this.speed});
    on(v,'loadeddata',()=>{clearTimeout(this.wd)});
    on(v,'timeupdate',()=>{this._tick();this._save()});on(v,'progress',()=>this._buffer());
    on(v,'ended',()=>{this._save(true);if(this.ctx.onEnded)this.ctx.onEnded()});
    on(v,'volumechange',()=>{this.vol.value=v.muted?0:v.volume;this.el.querySelector('[data-a=mute] use').setAttribute('href',v.muted||!v.volume?'#i-mute':'#i-vol')});
    on(v,'error',()=>{if(Date.now()<this._ign||this.dead)return;if(this.sources[this.idx]&&this.sources[this.idx].kind==='iframe')return;const e=v.error;this._fail(e?('media error '+e.code):'playback error')});
    on(document,'fullscreenchange',()=>this._fsch());on(document,'webkitfullscreenchange',()=>this._fsch());
    on(this.vol,'input',()=>{v.muted=false;v.volume=+this.vol.value});
    on(el,'pointerdown',e=>{this.ptype=e.pointerType||'mouse'},true);
    on(el,'pointermove',()=>this._poke());
    on(this.ui,'click',e=>this._uiClick(e));
    on(this.host,'click',e=>{const b=e.target.closest('[data-a]');if(b)this._act(b.dataset.a,b);const m=e.target.closest('[data-m]');if(m){e.stopPropagation();this._menuAct(m.dataset.m,m.dataset.v)}});
    // seek bar drag
    let drag=false;const seekTo=ev=>{const r=this.bar.getBoundingClientRect();const p=Math.min(1,Math.max(0,(ev.clientX-r.left)/r.width));if(isFinite(v.duration)){v.currentTime=p*v.duration}};
    on(this.bar,'pointerdown',e=>{drag=true;this.bar.setPointerCapture&&this.bar.setPointerCapture(e.pointerId);seekTo(e)});
    on(this.bar,'pointermove',e=>{const r=this.bar.getBoundingClientRect();const p=Math.min(1,Math.max(0,(e.clientX-r.left)/r.width));this.tip.style.left=(p*100)+'%';this.tip.textContent=fmtTime((v.duration||0)*p);if(drag)seekTo(e)});
    on(this.bar,'pointerup',()=>{drag=false});on(this.bar,'pointercancel',()=>{drag=false});
    on(el,'keydown',e=>this._key(e));
    on(this.file,'change',()=>this._loadSub());
    on(document,'visibilitychange',()=>{if(!document.hidden&&this.v&&!this.v.paused)this._lock(true)});
    on(window,'pagehide',()=>this._save(true));
  }
  /* ---------- sources ---------- */
  setSources(list,ctx){
    this.sources=list.map(s=>({...s}));this.ctx=ctx||{};this.title.textContent=this.ctx.title||'';this.resumed=false;
    this.nextBtn.style.display=this.ctx.next?'':'none';
    if(!this.sources.length){this._showMsg('No playable sources were returned for this title.',true);this._chips();return}
    this.play(this._best());
  }
  _best(){
    const L=this.sources;const mp=L.map((s,i)=>({s,i})).filter(x=>x.s.kind==='mp4'&&!x.s.dead);
    const pickQ=t=>mp.find(x=>x.s.q===t);
    const hit=pickQ(720)||pickQ(1080)||pickQ(480)||pickQ(360);
    if(hit)return hit.i;
    if(mp.length)return mp.sort((a,b)=>b.s.q-a.s.q)[0].i;
    const order=['hls','dash','iframe','file'];
    for(const k of order){const i=L.findIndex(s=>s.kind===k&&!s.dead);if(i>=0)return i}
    return 0;
  }
  _teardown(){
    clearTimeout(this.wd);clearTimeout(this.pw);this._ign=Date.now()+600;
    if(this.dash){try{this.dash.reset()}catch(e){}this.dash=null}
    if(this.hls){try{this.hls.destroy()}catch(e){}this.hls=null}
    try{this.v.pause();this.v.removeAttribute('src');while(this.v.firstChild)this.v.removeChild(this.v.firstChild);this.v.load()}catch(e){}
    this.f.src='about:blank';
  }
  play(i,keep){
    if(i<0||i>=this.sources.length)return;
    const s=this.sources[i];let t=0;
    if(keep&&this.started&&isFinite(this.v.currentTime))t=this.v.currentTime;
    this._teardown();this.idx=i;this.started=false;this.keepT=t;this._hideMsg();this._busy(true);this._menuShow(false);
    this.srcTag.textContent=(s.label||s.kind.toUpperCase())+(s.quality?' · '+s.quality:'');
    this.el.classList.toggle('if',s.kind==='iframe');
    if(s.kind==='iframe'){this._busy(false);this.f.src=s.url;this._showMsg('Embedded player — quality & audio are controlled inside it.',false,4500);this._chips();return}
    const url=this._url(s);const v=this.v;
    this.wd=setTimeout(()=>{if(!this.started&&!this.dead&&this.idx===i)this._fail('timed out')},s.kind==='dash'?26000:20000);
    try{
      if(s.kind==='dash')this._dash(url);
      else if(s.kind==='hls'||/\.m3u8(\?|$)/i.test(s.url))this._hlsLoad(url,s);
      else{v.src=url;v.load()}
      const p=v.play();if(p&&p.catch)p.catch(()=>{this._busy(false)});
    }catch(e){this._fail(e.message||'engine error')}
    if(t){const seek=()=>{try{v.currentTime=t}catch(e){}};v.addEventListener('loadedmetadata',seek,{once:true})}
    this._chips();
  }
  _url(s){
    if(s.kind==='dash')return dashProxyUrl(s.base||String(s.url||'').replace(/\/index\.mpd.*$/,''),s.cookie);
    if(s.proxied)return gxUrl(s.url,{cookie:s.cookie});
    return s.url;
  }
  _dash(url){
    if(!window.dashjs){this._fail('DASH engine not loaded');return}
    const p=dashjs.MediaPlayer().create();this.dash=p;
    try{p.updateSettings({streaming:{abr:{autoSwitchBitrate:{video:true,audio:true}}}})}catch(e){}
    const E=dashjs.MediaPlayer.events;
    p.on(E.ERROR,e=>{if(!this.started&&this.dash===p){const m=e&&e.error&&(e.error.message||e.error.code)||'stream error';this._fail('DASH: '+m)}});
    p.on(E.STREAM_INITIALIZED,()=>{
      if(this.dash!==p)return;
      let vt=[];try{vt=p.getTracksFor('video')||[]}catch(e){}
      const sup=vt.filter(t=>{const c=t.codec||(t.mimeType&&t.codecs?t.mimeType+';codecs="'+t.codecs+'"':'');if(!c||!window.MediaSource||!MediaSource.isTypeSupported)return true;try{return MediaSource.isTypeSupported(c)}catch(e){return true}});
      if(vt.length&&!sup.length){const cc=String((vt[0].codec||vt[0].codecs||'')).match(/hvc1|hev1|hevc/i);this._noCodec(cc?'H.265 (HEVC)':'this video codec');return}
      this._tracks();this._chips()});
    p.on(E.QUALITY_CHANGE_RENDERED,()=>{if(this.menuOpen)this._menuRender()});
    p.initialize(this.v,url,true);
  }
  _hlsLoad(url,s){
    const v=this.v;
    if(window.Hls&&Hls.isSupported()){
      const h=new Hls({enableWorker:true,maxBufferLength:45,manifestLoadingMaxRetry:2,levelLoadingMaxRetry:2,fragLoadingMaxRetry:3});this.hls=h;let rec=0;
      h.on(Hls.Events.MANIFEST_PARSED,()=>{this._tracks();this._chips()});
      h.on(Hls.Events.AUDIO_TRACKS_UPDATED,()=>this._tracks());
      h.on(Hls.Events.SUBTITLE_TRACKS_UPDATED,()=>this._tracks());
      h.on(Hls.Events.ERROR,(ev,d)=>{
        if(!d.fatal)return;
        if(d.type===Hls.ErrorTypes.MEDIA_ERROR&&rec++<2){h.recoverMediaError();return}
        this._fail('HLS: '+(d.details||d.type));
      });
      h.loadSource(url);h.attachMedia(v);
    }else if(v.canPlayType('application/vnd.apple.mpegurl')){v.src=url;v.load()}
    else this._fail('HLS not supported in this browser');
  }
  _pic(){
    clearTimeout(this.pw);const i=this.idx,s=this.sources[i];
    if(!s||s.kind==='iframe'||this.ctx.live)return;
    this.pw=setTimeout(()=>{
      if(this.dead||this.idx!==i||this.v.paused||this.v.ended)return;
      if(this.v.videoWidth===0&&this.v.currentTime>0.8)this._noCodec('this video format');
    },4500);
  }
  _noCodec(what){
    if(this.dead)return;const i=this.idx,s=this.sources[i];if(!s)return;
    clearTimeout(this.wd);clearTimeout(this.pw);s.dead=true;s.err='picture not supported by this browser';
    const n=this._nextAlive(i);
    if(n>=0){this._showMsg('This browser can’t draw '+what+' (you would only get sound). Switching to '+(this.sources[n].label||this.sources[n].kind)+(this.sources[n].quality?' '+this.sources[n].quality:'')+'…',false,5000);this.play(n)}
    else{this._busy(false);try{this.v.pause()}catch(e){}this._chips();this._showMsg('This browser can’t decode '+what+'. Open it in VLC or MX Player (Settings → Open in app) — they play it fine.',true)}
  }
  _extUrl(){
    const s=this.sources[this.idx];if(!s)return '';
    if(s.kind==='dash')return location.origin+dashProxyUrl(s.base||String(s.url||'').replace(/\/index\.mpd.*$/,''),s.cookie);
    return s.proxied?location.origin+gxUrl(s.url,{cookie:s.cookie}):s.url;
  }
  _fail(why){
    if(this.dead)return;const i=this.idx,s=this.sources[i];if(!s||s.kind==='iframe')return;
    clearTimeout(this.wd);
    if(this.started){this._busy(false);return}
    if(s.kind!=='dash'&&!s.proxied&&/^https?:/i.test(s.url||'')){s.proxied=true;this._showMsg('Direct link failed ('+why+') — retrying through proxy…',false,3000);this.play(i);return}
    s.dead=true;s.err=why;
    const nxt=this._nextAlive(i);
    if(nxt>=0){this._showMsg('That source failed ('+why+'). Switching to '+(this.sources[nxt].label||this.sources[nxt].kind)+(this.sources[nxt].quality?' '+this.sources[nxt].quality:'')+'…',false,3500);this.play(nxt)}
    else{this._busy(false);this._chips();this._showMsg('All sources failed. Try the Download tab, an external player, or another server.',true)}
    if(this.o.onChange)this.o.onChange(this.idx,this.sources);
  }
  _nextAlive(from){
    const L=this.sources;const order=[];
    for(let k=1;k<=L.length;k++){const j=(from+k)%L.length;if(j!==from)order.push(j)}
    const alive=order.filter(j=>!L[j].dead);
    const pref=alive.find(j=>L[j].kind!=='iframe')??alive[0];
    return pref===undefined?-1:pref;
  }
  /* ---------- tracks + menu ---------- */
  _tracks(){
    this.vt=[];this.at=[];this.st=[];
    if(this.dash&&this.dash.getBitrateInfoListFor){
      try{this.vt=(this.dash.getBitrateInfoListFor('video')||[]).map(b=>({i:b.qualityIndex,label:(b.height?b.height+'p':Math.round(b.bitrate/1000)+'k'),sub:Math.round((b.bitrate||0)/1000)+' kbps'}))}catch(e){}
      try{this.at=(this.dash.getTracksFor('audio')||[]).map((t,k)=>({i:k,t,label:(t.labels&&t.labels[0]&&t.labels[0].text)||t.lang||('Audio '+(k+1))}))}catch(e){}
    }else if(this.hls){
      this.vt=(this.hls.levels||[]).map((l,k)=>({i:k,label:l.height?l.height+'p':Math.round(l.bitrate/1000)+'k',sub:Math.round((l.bitrate||0)/1000)+' kbps'}));
      this.at=(this.hls.audioTracks||[]).map((t,k)=>({i:k,label:t.name||t.lang||('Audio '+(k+1))}));
      this.st=(this.hls.subtitleTracks||[]).map((t,k)=>({i:k,label:t.name||t.lang||('Sub '+(k+1))}));
    }else if(this.v.audioTracks&&this.v.audioTracks.length>1){
      this.at=[...this.v.audioTracks].map((t,k)=>({i:k,label:t.label||t.language||('Audio '+(k+1))}));
    }
    this.vt.sort((a,b)=>(parseInt(b.label)||0)-(parseInt(a.label)||0));
    if(this.menuOpen)this._menuRender();
  }
  _menuRender(){
    const s=this.sources[this.idx]||{};let h='';
    const sp=[0.5,0.75,1,1.25,1.5,2];
    if(this.vt.length>1){
      let cur=-2;
      if(this.dash){try{const ab=this.dash.getSettings().streaming.abr.autoSwitchBitrate.video;cur=ab?-1:this.dash.getQualityFor('video')}catch(e){cur=-1}}
      else if(this.hls)cur=this.hls.autoLevelEnabled?-1:this.hls.currentLevel;
      h+='<h4>Quality</h4><button data-m="q" data-v="-1" class="'+(cur===-1?'on':'')+'">Auto</button>'+this.vt.map(q=>'<button data-m="q" data-v="'+q.i+'" class="'+(cur===q.i?'on':'')+'">'+esc(q.label)+' <small>'+q.sub+'</small></button>').join('');
    }
    if(this.at.length>1){
      let cur=0;
      if(this.dash){try{const c=this.dash.getCurrentTrackFor('audio');cur=this.at.findIndex(a=>a.t&&c&&a.t.id===c.id&&a.t.index===c.index);if(cur<0)cur=0}catch(e){}}
      else if(this.hls)cur=this.hls.audioTrack;else{cur=[...this.v.audioTracks].findIndex(t=>t.enabled)}
      h+='<h4>Audio</h4>'+this.at.map(a=>'<button data-m="a" data-v="'+a.i+'" class="'+(cur===a.i?'on':'')+'">'+esc(a.label)+'</button>').join('');
    }
    const tt=[...this.v.textTracks||[]].filter(t=>t.kind==='subtitles'||t.kind==='captions');
    h+='<h4>Subtitles</h4><button data-m="s" data-v="off" class="'+(!tt.some(t=>t.mode==='showing')&&!(this.hls&&this.hls.subtitleTrack>=0)?'on':'')+'">Off</button>';
    h+=this.st.map(a=>'<button data-m="hs" data-v="'+a.i+'" class="'+(this.hls&&this.hls.subtitleTrack===a.i?'on':'')+'">'+esc(a.label)+'</button>').join('');
    h+=tt.map((t,k)=>'<button data-m="s" data-v="'+k+'" class="'+(t.mode==='showing'?'on':'')+'">'+esc(t.label||t.language||'Subtitle '+(k+1))+'</button>').join('');
    h+='<button data-m="sfile">Load subtitle file…</button>';
    h+='<h4>Speed</h4>'+sp.map(x=>'<button data-m="sp" data-v="'+x+'" class="'+(this.speed===x?'on':'')+'">'+x+'×</button>').join('');
    h+='<h4>Picture</h4><button data-m="fit" data-v="0" class="'+(!this.fit?'on':'')+'">Fit</button><button data-m="fit" data-v="1" class="'+(this.fit?'on':'')+'">Fill screen</button>';
    h+='<h4>Tools</h4><button data-m="copy">Copy stream link</button><button data-m="ext">Open in app (VLC / MX Player)</button>';
    if(this.sources.length>1){
      h+='<h4>Source</h4>'+this.sources.map((x,i)=>'<button data-m="src" data-v="'+i+'" class="'+(i===this.idx?'on':'')+(x.dead?' dead':'')+'">'+esc((x.label||x.kind)+(x.quality?' · '+x.quality:''))+(x.size?' <small>'+fmtSize(x.size)+'</small>':'')+'</button>').join('');
    }
    this.menu.innerHTML=h;
  }
  _menuShow(on){this.menuOpen=on;this.menu.classList.toggle('show',on);if(on)this._menuRender()}
  _menuAct(a,val){
    const v=this.v;
    if(a==='q'){const i=+val;
      if(this.dash){try{if(i<0)this.dash.updateSettings({streaming:{abr:{autoSwitchBitrate:{video:true}}}});else{this.dash.updateSettings({streaming:{abr:{autoSwitchBitrate:{video:false}}}});this.dash.setQualityFor('video',i)}}catch(e){}}
      else if(this.hls)this.hls.currentLevel=i}
    else if(a==='a'){const i=+val;if(this.dash){try{this.dash.setCurrentTrack(this.at[i].t)}catch(e){}}else if(this.hls)this.hls.audioTrack=i;else if(v.audioTracks){[...v.audioTracks].forEach((t,k)=>t.enabled=k===i)}}
    else if(a==='hs'){this.hls.subtitleTrack=+val}
    else if(a==='s'){[...v.textTracks].forEach(t=>t.mode='disabled');if(this.hls)this.hls.subtitleTrack=-1;if(val!=='off'){const tt=[...v.textTracks].filter(t=>t.kind==='subtitles'||t.kind==='captions');if(tt[+val])tt[+val].mode='showing'}}
    else if(a==='sfile'){this.file.click();return}
    else if(a==='sp'){this.speed=+val;v.playbackRate=this.speed}
    else if(a==='fit'){this.fit=val==='1';v.classList.toggle('fill',this.fit)}
    else if(a==='copy'){const u=this._extUrl();if(u)copyText(u);return}
    else if(a==='ext'){const u=this._extUrl();if(u){location.href=extIntent(u);toast('Opening in your video app…')}return}
    else if(a==='src'){const i=+val;this.sources[i].dead=false;this.sources[i].proxied=false;this.play(i,true);return}
    this._menuRender();
  }
  _loadSub(){
    const f=this.file.files&&this.file.files[0];if(!f)return;
    const rd=new FileReader();rd.onload=()=>{
      let t=String(rd.result||'');
      if(!/^\s*WEBVTT/.test(t))t='WEBVTT\n\n'+t.replace(/\r/g,'').replace(/(\d{2}:\d{2}:\d{2}),(\d{3})/g,'$1.$2');
      const url=URL.createObjectURL(new Blob([t],{type:'text/vtt'}));
      const tr=document.createElement('track');tr.kind='subtitles';tr.label=f.name.replace(/\.[^.]+$/,'');tr.src=url;tr.default=true;this.v.appendChild(tr);
      setTimeout(()=>{[...this.v.textTracks].forEach(x=>x.mode='disabled');const last=this.v.textTracks[this.v.textTracks.length-1];if(last)last.mode='showing';this._menuRender();toast('Subtitle loaded')},120);
    };rd.readAsText(f);this.file.value='';
  }
  /* ---------- chrome ---------- */
  _chips(){if(this.o.onChange)this.o.onChange(this.idx,this.sources);if(this.menuOpen)this._menuRender()}
  _busy(b){this.el.classList.toggle('busy',b)}
  _showMsg(t,sticky,ms){this.msg.textContent=t;this.msg.classList.add('show');this.el.classList.remove('idle');clearTimeout(this.mt);if(!sticky)this.mt=setTimeout(()=>this._hideMsg(),ms||3000)}
  _hideMsg(){this.msg.classList.remove('show')}
  _state(){const p=!this.v.paused&&!this.v.ended;this.el.classList.toggle('playing',p);this.ppBtn.innerHTML=ico(p?'pause':'play');if(!p){this.el.classList.remove('idle');this._lock(false)}else this._poke()}
  _tick(){
    const v=this.v,d=v.duration,t=v.currentTime;if(!isFinite(d)||!d){this.time.textContent=fmtTime(t);return}
    const p=t/d*100;this.fill.style.width=p+'%';this.knob.style.left=p+'%';this.time.textContent=fmtTime(t)+' / '+fmtTime(d);
  }
  _buffer(){const v=this.v;try{if(v.buffered.length&&v.duration){this.buf.style.width=(v.buffered.end(v.buffered.length-1)/v.duration*100)+'%'}}catch(e){}}
  _poke(){this.el.classList.remove('idle');clearTimeout(this.it);this.it=setTimeout(()=>{if(!this.v.paused&&!this.menuOpen&&!this.el.classList.contains('if'))this.el.classList.add('idle')},3200)}
  _uiClick(e){
    if(e.target.closest('.pl-bot,.pl-menu,.pl-top,.pl-msg'))return;
    if(this.menuOpen){this._menuShow(false);return}
    const now=Date.now(),x=e.clientX,r=this.el.getBoundingClientRect();
    if(now-this.tapT<320&&Math.abs(x-this.tapX)<90){clearTimeout(this.tapTimer);this.tapT=0;const left=(x-r.left)<r.width/2;this._seekBy(left?-10:10);const b=this.el.querySelector('.pl-seek.'+(left?'l':'r'));b.classList.remove('go');void b.offsetWidth;b.classList.add('go');return}
    this.tapT=now;this.tapX=x;
    clearTimeout(this.tapTimer);
    this.tapTimer=setTimeout(()=>{
      if(e.target.closest('.pl-big')){this._act('pp');return}
      if(this.ptype==='touch'){if(this.el.classList.contains('idle'))this._poke();else if(!this.v.paused)this.el.classList.add('idle');else this._act('pp')}
      else this._act('pp');
    },240);
  }
  _seekBy(s){const v=this.v;if(isFinite(v.duration))v.currentTime=Math.min(v.duration,Math.max(0,v.currentTime+s));this._poke()}
  _act(a){
    const v=this.v;
    if(a==='pp'){v.paused?v.play().catch(()=>{}):v.pause()}
    else if(a==='b10')this._seekBy(-10);else if(a==='f10')this._seekBy(10);
    else if(a==='mute')v.muted=!v.muted;
    else if(a==='gear')this._menuShow(!this.menuOpen);
    else if(a==='pip'){try{if(document.pictureInPictureElement)document.exitPictureInPicture();else if(v.requestPictureInPicture)v.requestPictureInPicture();else toast('Picture-in-picture is not supported here')}catch(e){toast('Picture-in-picture is not available')}}
    else if(a==='fs')this._fs();
    else if(a==='next'&&this.ctx.next)this.ctx.next();
    this._poke();
  }
  _fs(){
    const el=this.el,d=document;
    if(d.fullscreenElement||d.webkitFullscreenElement){(d.exitFullscreen||d.webkitExitFullscreen).call(d);return}
    const rq=el.requestFullscreen||el.webkitRequestFullscreen;
    if(rq){Promise.resolve(rq.call(el)).then(()=>{try{screen.orientation&&screen.orientation.lock&&screen.orientation.lock('landscape').catch(()=>{})}catch(e){}}).catch(()=>{})}
    else if(this.v.webkitEnterFullscreen)this.v.webkitEnterFullscreen();
  }
  _fsch(){const f=document.fullscreenElement||document.webkitFullscreenElement;this.el.classList.toggle('fs',f===this.el);if(!f){try{screen.orientation&&screen.orientation.unlock&&screen.orientation.unlock()}catch(e){}}}
  _key(e){
    const k=e.key;if(e.target.tagName==='INPUT')return;const v=this.v;let h=true;
    if(k===' '||k==='k')this._act('pp');else if(k==='ArrowRight')this._seekBy(5);else if(k==='ArrowLeft')this._seekBy(-5);
    else if(k==='l')this._seekBy(10);else if(k==='j')this._seekBy(-10);
    else if(k==='ArrowUp')v.volume=Math.min(1,v.volume+.1);else if(k==='ArrowDown')v.volume=Math.max(0,v.volume-.1);
    else if(k==='m')v.muted=!v.muted;else if(k==='f')this._fs();else if(k==='n'&&this.ctx.next)this.ctx.next();
    else if(/^[0-9]$/.test(k)&&isFinite(v.duration))v.currentTime=v.duration*(+k/10);else h=false;
    if(h){e.preventDefault();this._poke()}
  }
  _lock(on){
    try{if(on&&!this.wl&&navigator.wakeLock){navigator.wakeLock.request('screen').then(l=>{this.wl=l;l.addEventListener('release',()=>{this.wl=null})}).catch(()=>{})}
    else if(!on&&this.wl){this.wl.release();this.wl=null}}catch(e){}
  }
  _media(){
    if(!('mediaSession' in navigator))return;
    try{navigator.mediaSession.metadata=new MediaMetadata({title:this.ctx.title||'StreamHub',artist:this.ctx.sub||'StreamHub',artwork:this.ctx.poster?[{src:this.ctx.poster,sizes:'512x512'}]:[]});
      navigator.mediaSession.setActionHandler('play',()=>this.v.play());navigator.mediaSession.setActionHandler('pause',()=>this.v.pause());
      navigator.mediaSession.setActionHandler('seekbackward',()=>this._seekBy(-10));navigator.mediaSession.setActionHandler('seekforward',()=>this._seekBy(10));
      navigator.mediaSession.setActionHandler('nexttrack',this.ctx.next?()=>this.ctx.next():null)}catch(e){}
  }
  _save(force){
    if(!this.ctx.onProgress)return;const now=Date.now();if(!force&&now-this.lastSave<5000)return;
    const v=this.v;if(!this.started||!isFinite(v.duration)||v.duration<=0)return;this.lastSave=now;this.ctx.onProgress(v.currentTime,v.duration);
  }
  destroy(){
    this.dead=true;this._save(true);this._teardown();this._lock(false);clearTimeout(this.it);clearTimeout(this.mt);clearTimeout(this.tapTimer);clearTimeout(this.pw);
    this._on.forEach(([t,e,f,o])=>t.removeEventListener(e,f,o));
    const f=document.fullscreenElement||document.webkitFullscreenElement;if(f&&this.el.contains(f)){try{(document.exitFullscreen||document.webkitExitFullscreen).call(document)}catch(e){}}
    this.host.innerHTML='';
  }
}
/* source chips (outside the player) */
function srcChips(list,idx){
  if(!list.length)return '';
  return list.map((s,i)=>'<button class="chip'+(i===idx?' on':'')+(s.dead?' dead':'')+'" data-src="'+i+'" title="'+esc(s.err||'')+'">'+esc((s.label||s.kind)+(s.quality?' '+s.quality:'')+(s.audio?' · '+s.audio:''))+'</button>').join('');
}

/* ===== core: store, api, ui helpers, router, shared components ===== */
const _rt=r=>typeof r==='string'?document.querySelector(r):(r||document);
const $=(s,r)=>{const t=_rt(r);return t?t.querySelector(s):null},$$=(s,r)=>{const t=_rt(r);return t?[...t.querySelectorAll(s)]:[]};
const store={get(k,d){try{const v=localStorage.getItem(k);return v==null?d:JSON.parse(v)}catch(e){return d}},set(k,v){try{localStorage.setItem(k,JSON.stringify(v))}catch(e){}}};
const lib={
  list(){return store.get('sh_list',[])},
  has(k){return this.list().some(x=>x.key===k)},
  toggle(it){let l=this.list();const k=it.provider+':'+it.id;if(l.some(x=>x.key===k)){l=l.filter(x=>x.key!==k);store.set('sh_list',l);return false}
    l.unshift({key:k,provider:it.provider,id:it.id,title:it.title,poster:it.poster,year:it.year,type:it.type,content:it.content,link:it.link,rating:it.rating,genre:it.genre});store.set('sh_list',l.slice(0,300));return true},
  hist(){return store.get('sh_hist',[])},
  save(it,t,d,extra){let h=this.hist();const k=it.provider+':'+it.id;h=h.filter(x=>x.key!==k);
    h.unshift({key:k,provider:it.provider,id:it.id,title:it.title,poster:it.poster,type:it.type,content:it.content,link:it.link,t,d,ts:Date.now(),...(extra||{})});store.set('sh_hist',h.slice(0,40))},
  pos(it){const k=it.provider+':'+it.id;return this.hist().find(x=>x.key===k)},
};
let toastT;function toast(m){const t=$('#toast');t.textContent=m;t.classList.add('show');clearTimeout(toastT);toastT=setTimeout(()=>t.classList.remove('show'),2600)}
function copyText(t){const done=()=>toast('Copied to clipboard');if(navigator.clipboard&&navigator.clipboard.writeText){navigator.clipboard.writeText(t).then(done).catch(()=>fb())}else fb();function fb(){const a=document.createElement('textarea');a.value=t;a.style.cssText='position:fixed;opacity:0';document.body.appendChild(a);a.select();try{document.execCommand('copy');done()}catch(e){toast('Copy failed')}a.remove()}}

const _cache=new Map();
async function api(path,o){
  o=o||{};const ttl=o.ttl==null?120000:o.ttl,retry=o.retry==null?1:o.retry,tmo=o.timeout||45000;
  const c=_cache.get(path);if(!o.fresh&&c&&Date.now()-c.t<ttl)return c.v;
  let err;
  for(let i=0;i<=retry;i++){
    const ac=new AbortController(),to=setTimeout(()=>ac.abort(),tmo);
    try{
      const r=await fetch(path,{signal:ac.signal});clearTimeout(to);
      const j=await r.json().catch(()=>null);
      if(!r.ok||!j||j.ok===false){const d=j&&(j.error||(j.detail&&(j.detail.error||j.detail.detail||(typeof j.detail==='string'?j.detail:''))));throw new Error(d||('Server returned '+r.status))}
      _cache.set(path,{t:Date.now(),v:j});return j;
    }catch(e){clearTimeout(to);err=e.name==='AbortError'?new Error('The request timed out'):e;if(i<retry)await sleep(500*(i+1))}
  }
  throw err;
}
const D=j=>j&&j.data!==undefined?j.data:j;

/* ---- image + cards ---- */
function imgErr(i){const p=i.parentElement;i.remove();p.classList.add('noimg')}
function imgOk(i){i.classList.add('ld')}
function poster(it,extra){
  const t=(it.title||'?').trim().charAt(0).toUpperCase();
  return'<div class="poster'+(it.poster?'':' noimg')+'" data-t="'+esc(t)+'">'+(it.poster?'<img loading="lazy" decoding="async" referrerpolicy="no-referrer" alt="" src="'+esc(it.poster)+'" onload="imgOk(this)" onerror="imgErr(this)">':'')+'<span class="shine"></span><span class="pp">'+ico('play')+'</span>'+(extra||'')+'</div>';
}
function card(it,opts){
  opts=opts||{};
  let ex='';
  if(it.rating)ex+='<b class="rt">★ '+esc(it.rating)+'</b>';
  if(it.quality==='4K')ex+='<span class="bd k">4K</span>';else if(it.type==='series'&&!opts.noBadge)ex+='<span class="bd">SERIES</span>';
  if(opts.progress)ex+='<div class="pg-bar"><i style="width:'+Math.min(100,opts.progress)+'%"></i></div>';
  if(opts.num)ex+='<span class="num">'+opts.num+'</span>';
  const meta=[it.year,(it.genre||'').split(',')[0],it.ep].filter(Boolean).join(' • ');
  return'<a class="card" data-rv href="'+hrefOf(it)+'">'+poster(it,ex)+'<div class="ct"><h3>'+esc(it.title)+'</h3><p>'+esc(meta||(it.type==='series'?'Series':'Movie'))+'</p></div></a>';
}
function row(title,items,opts){
  opts=opts||{};if(!items||!items.length)return '';
  const id='r'+Math.random().toString(36).slice(2,8);
  return'<section class="row'+(opts.cls?' '+opts.cls:'')+'"><div class="rh"><h2>'+esc(title)+'</h2><div class="arrows"><button aria-label="Scroll left" data-sc="'+id+'" data-d="-1">'+ico('back')+'</button><button aria-label="Scroll right" data-sc="'+id+'" data-d="1">'+ico('next')+'</button></div></div><div class="rail" id="'+id+'">'+items.map((it,i)=>card(it,{progress:it._p,num:opts.num?i+1:0})).join('')+'</div></section>';
}
const skelRow=()=>'<div class="row"><div class="rh"><h2 style="opacity:.3">Loading…</h2></div><div class="skrow">'+'<div class="sk"></div>'.repeat(8)+'</div></div>';
const skelGrid=n=>'<div class="grid">'+'<div class="sk" style="aspect-ratio:2/3"></div>'.repeat(n||12)+'</div>';
function emptyBox(t,sub,btn){return'<div class="empty"><b>'+esc(t)+'</b>'+esc(sub||'')+(btn?'<br><button class="btn pri" data-retry>'+esc(btn)+'</button>':'')+'</div>'}
function sBox(id,ph,val){return'<label class="sbox">'+ico('search')+'<input id="'+id+'" type="search" enterkeyhint="search" autocomplete="off" placeholder="'+esc(ph)+'" value="'+esc(val||'')+'"></label>'}
function chipsBar(items,cur,attr){return'<div class="chips">'+items.map(c=>'<button class="chip'+(c.v===cur?' on':'')+'" data-'+attr+'="'+esc(c.v)+'">'+esc(c.l)+'</button>').join('')+'</div>'}
function debounce(f,ms){let t;return(...a)=>{clearTimeout(t);t=setTimeout(()=>f(...a),ms)}}
function lazyMore(btn,fn){ // IntersectionObserver auto "load more"
  if(!btn||!('IntersectionObserver' in window))return;
  const io=new IntersectionObserver(es=>{if(es[0].isIntersecting&&!btn.disabled){fn()}},{rootMargin:'600px'});io.observe(btn);btn._io=io;
}

/* ---- navigation model ---- */
const NAV=[
  {k:'home',l:'Home',i:'home',dock:1},{k:'movies',l:'Movies',i:'film',dock:1},{k:'series',l:'Series',i:'tv'},
  {k:'anime',l:'Anime',i:'spark',dock:1},{k:'4k',l:'4K Hub',i:'4k'},{k:'drama',l:'Drama',i:'tv'},
  {k:'live',l:'Live TV',i:'live',dock:1},{k:'midnight',l:'Midnight',i:'moon'},{k:'downloads',l:'Downloads',i:'dl'},{k:'library',l:'Library',i:'lib'}
];
function buildNav(){
  $('#nav').innerHTML='<span id="navpill"></span>'+NAV.map(n=>'<a href="#/'+n.k+'" data-k="'+n.k+'">'+n.l+'</a>').join('');
  const dock=$('#dock');const items=NAV.filter(n=>n.dock);
  dock.insertAdjacentHTML('beforeend',items.map(n=>'<a href="#/'+n.k+'" data-k="'+n.k+'">'+ico(n.i)+'<span>'+n.l+'</span></a>').join('')+'<button id="moreBtn" aria-label="More sections">'+ico('more')+'<span>More</span></button>');
  $('#moreGrid').innerHTML=NAV.map(n=>'<a href="#/'+n.k+'" data-k="'+n.k+'">'+ico(n.i)+n.l+'</a>').join('')+'<a href="/docs" target="_blank" rel="noopener">'+ico('api')+'API Docs</a>';
  $('#moreBtn').onclick=()=>$('#sheet').classList.add('open');
  $('#sheet').onclick=e=>{if(e.target.id==='sheet'||e.target.closest('a'))$('#sheet').classList.remove('open')};
}
function setActive(k){
  document.body.dataset.sec=k||'search';
  if(window.fxPill)fxPill();
  $$('#nav a,#dock a,#moreGrid a').forEach(a=>a.classList.toggle('on',a.dataset.k===k));
  const dockKeys=NAV.filter(n=>n.dock).map(n=>n.k);const idx=dockKeys.indexOf(k);const items=$$('#dock > a,#dock > button');
  const g=$('#dockglow');const target=idx>=0?items[idx]:items[items.length-1];
  if(target&&g){g.style.width=target.offsetWidth+'px';g.style.transform='translateX('+(target.offsetLeft-6)+'px)'}
  if(idx<0&&$('#moreBtn'))$('#moreBtn').classList.add('on');else if($('#moreBtn'))$('#moreBtn').classList.remove('on');
}

/* ---- hero carousel ---- */
const Hero={timer:0,i:0,n:0,
  html(slides){
    if(!slides.length)return '';
    return'<section id="hero" aria-label="Featured">'+slides.map((s,i)=>{
      const bg=s.backdrop||s.poster;
      return'<div class="hs'+(i===0?' on':'')+'" data-i="'+i+'"><div class="bg" style="background-image:url(\''+esc(bg)+'\')"></div><div class="hc"><h2 aria-label="'+esc(s.title)+'">'+String(s.title).split(/\s+/).map((w,k)=>'<span class="w" aria-hidden="true" style="--i:'+k+'"><i>'+esc(w)+'</i></span>').join(' ')+'</h2><div class="meta">'+
      (s.rating?'<span class="tag am">★ '+esc(s.rating)+'</span>':'')+(s.year?'<span>'+esc(s.year)+'</span>':'')+(s.genre?'<span>'+esc(String(s.genre).split(',').slice(0,3).join(' / '))+'</span>':'')+'<span class="tag">'+(s.type==='series'?'Series':'Movie')+'</span></div>'+
      (s.desc?'<p class="desc">'+esc(s.desc)+'</p>':'')+'<div class="acts"><a class="btn pri" href="'+hrefOf(s)+'?play=1">'+ico('play')+'Play now</a><a class="btn ghost" href="'+hrefOf(s)+'">Details</a></div></div></div>';
    }).join('')+'<div class="beam"></div><div class="hthumbs">'+slides.map((s,i)=>'<button aria-label="Slide '+(i+1)+'" data-h="'+i+'"'+(i===0?' class="on"':'')+'>'+(s.poster?'<img alt="" referrerpolicy="no-referrer" src="'+esc(s.poster)+'" onerror="this.remove()">':'')+'</button>').join('')+'</div><div id="hdots">'+slides.map((s,i)=>'<button aria-label="Slide '+(i+1)+'" data-h="'+i+'"'+(i===0?' class="on"':'')+'></button>').join('')+'</div></section>';
  },
  start(){
    this.stop();const h=$('#hero');if(!h)return;const sl=$$('.hs',h);this.n=sl.length;this.i=0;if(this.n<2)return;
    const go=i=>{this.i=(i+this.n)%this.n;sl.forEach((s,k)=>s.classList.toggle('on',k===this.i));$$('.hthumbs button',h).forEach((b,k)=>b.classList.toggle('on',k===this.i));$$('#hdots button',h).forEach((b,k)=>{b.classList.remove('on','done');if(k<this.i)b.classList.add('done');if(k===this.i){void b.offsetWidth;b.classList.add('on')}});this.arm()};
    this.go=go;this.arm=()=>{clearTimeout(this.timer);this.timer=setTimeout(()=>go(this.i+1),7000)};
    h.addEventListener('click',e=>{const b=e.target.closest('[data-h]');if(b)go(+b.dataset.h)});
    let x0=null;h.addEventListener('touchstart',e=>{x0=e.touches[0].clientX;clearTimeout(this.timer)},{passive:true});
    h.addEventListener('touchend',e=>{if(x0!=null){const dx=e.changedTouches[0].clientX-x0;if(Math.abs(dx)>50)go(this.i+(dx<0?1:-1));else this.arm()}x0=null});
    this.arm();
  },
  stop(){clearTimeout(this.timer)}
};

/* ---- global delegated interactions ---- */
document.addEventListener('click',e=>{
  const sc=e.target.closest('[data-sc]');if(sc){const r=document.getElementById(sc.dataset.sc);if(r)r.scrollBy({left:(+sc.dataset.d)*r.clientWidth*.8,behavior:'smooth'})}
});
document.addEventListener('pointermove',e=>{
  const c=e.target.closest&&e.target.closest('.card');if(!c||e.pointerType!=='mouse')return;
  const r=c.getBoundingClientRect(),px=(e.clientX-r.left)/r.width,py=(e.clientY-r.top)/r.height;
  c.style.setProperty('--mx',(px*100)+'%');c.style.setProperty('--my',(py*100)+'%');
  const p=c.querySelector('.poster');if(p){p.style.setProperty('--ry',((px-.5)*12)+'deg');p.style.setProperty('--rx',((.5-py)*12)+'deg')}
},{passive:true});
document.addEventListener('pointerout',e=>{const c=e.target.closest&&e.target.closest('.card');if(c&&!c.contains(e.relatedTarget)){const p=c.querySelector('.poster');if(p){p.style.removeProperty('--rx');p.style.removeProperty('--ry')}}},{passive:true});
window.addEventListener('scroll',()=>{$('#top').classList.toggle('solid',scrollY>30)},{passive:true});

/* ===== fx: motion layer (pill, reveal, ripple, spotlight, progress, parallax, stars) ===== */
(function(){
  const reduce=window.matchMedia&&matchMedia('(prefers-reduced-motion: reduce)').matches;
  /* nav pill slides under the active link */
  window.fxPill=function(){
    const pill=document.getElementById('navpill'),nav=document.getElementById('nav');if(!pill||!nav)return;
    requestAnimationFrame(()=>{
      const a=nav.querySelector('a.on');
      if(!a||!nav.offsetWidth){pill.style.opacity=0;return}
      pill.style.width=a.offsetWidth+'px';pill.style.transform='translateX('+a.offsetLeft+'px)';pill.style.opacity=1;
    });
  };
  window.addEventListener('resize',()=>window.fxPill&&fxPill());
  /* staggered reveal for cards + row headers */
  const io='IntersectionObserver' in window?new IntersectionObserver(es=>{
    es.forEach(en=>{
      if(!en.isIntersecting)return;const el=en.target;io.unobserve(el);
      if(el.matches('.card')){
        const sib=el.parentElement?[...el.parentElement.children].filter(x=>x.classList.contains('card')):[];
        const idx=Math.max(0,sib.indexOf(el))%9;el.style.setProperty('--d',(idx*55)+'ms');
      }
      el.classList.add('in');
    });
  },{rootMargin:'0px 0px -4% 0px',threshold:.04}):null;
  const watch=root=>{
    if(!root||root.nodeType!==1)return;
    const els=[...(root.matches&&root.matches('.card[data-rv],.row')?[root]:[]),...root.querySelectorAll('.card[data-rv],.row')];
    els.forEach(el=>{if(el._rv)return;el._rv=1;if(!io||reduce){el.classList.add('in');return}io.observe(el)});
  };
  new MutationObserver(m=>m.forEach(r=>r.addedNodes.forEach(watch))).observe(document.body,{childList:true,subtree:true});
  watch(document.body);
  /* ripple on buttons */
  document.addEventListener('pointerdown',e=>{
    const b=e.target.closest&&e.target.closest('.btn,.pb');if(!b||reduce)return;
    const r=b.getBoundingClientRect(),d=Math.max(r.width,r.height)*2;
    const s=document.createElement('span');s.className='rip';s.style.cssText='width:'+d+'px;height:'+d+'px;left:'+(e.clientX-r.left-d/2)+'px;top:'+(e.clientY-r.top-d/2)+'px';
    if(getComputedStyle(b).position==='static')b.style.position='relative';
    b.appendChild(s);setTimeout(()=>s.remove(),650);
  },{passive:true});
  /* cursor spotlight (mouse only) */
  const spot=document.getElementById('spot');let tx=0,ty=0,cx=0,cy=0,raf=0;
  if(spot&&matchMedia('(hover:hover)').matches&&!reduce){
    window.addEventListener('pointermove',e=>{if(e.pointerType!=='mouse')return;tx=e.clientX;ty=e.clientY;spot.classList.add('on');if(!raf)raf=requestAnimationFrame(loop)},{passive:true});
    const loop=()=>{cx+=(tx-cx)*.12;cy+=(ty-cy)*.12;spot.style.transform='translate3d('+cx+'px,'+cy+'px,0)';raf=(Math.abs(tx-cx)+Math.abs(ty-cy)>.5)?requestAnimationFrame(loop):0};
  }
  /* scroll progress + hero parallax */
  const prog=document.getElementById('prog'),det=document.getElementById('detail');let tick=0;
  const upd=()=>{
    tick=0;
    const opened=det.classList.contains('open');
    const el=opened?det:document.scrollingElement||document.documentElement;
    const max=(el.scrollHeight-el.clientHeight)||1,y=opened?det.scrollTop:window.scrollY;
    prog.style.transform='scaleX('+Math.min(1,Math.max(0,y/max))+')';
    const h=document.getElementById('hero');if(h&&!opened&&y<900)h.style.setProperty('--py',y);
  };
  const sched=()=>{if(!tick)tick=requestAnimationFrame(upd)};
  window.addEventListener('scroll',sched,{passive:true});det.addEventListener('scroll',sched,{passive:true});
  /* midnight stars */
  const st=document.getElementById('stars');
  if(st){let h='';for(let i=0;i<70;i++){h+='<b style="left:'+(Math.random()*100).toFixed(1)+'%;top:'+(Math.random()*100).toFixed(1)+'%;animation-delay:'+(Math.random()*4).toFixed(2)+'s;animation-duration:'+(2.5+Math.random()*3).toFixed(1)+'s;opacity:'+(.3+Math.random()*.7).toFixed(2)+'"></b>'}
    h+='<em style="left:78%;animation-delay:1s"></em><em style="left:55%;animation-delay:4.5s"></em><em style="left:92%;animation-delay:8s"></em>';st.innerHTML=h}
  /* theme-color follows the accent */
  const meta=document.querySelector('meta[name=theme-color]');
  const TC={home:'#1a1008',movies:'#07112a',series:'#150a2b',anime:'#220a18',  '4k':'#041a1d',drama:'#220a10',live:'#220609',midnight:'#0b0a26',downloads:'#04201a',library:'#1d1405',search:'#071626'};
  new MutationObserver(()=>{if(meta)meta.content=TC[document.body.dataset.sec]||'#05060f'}).observe(document.body,{attributes:true,attributeFilter:['data-sec']});
})();

/* ===== pages ===== */
const V=$('#view');
const App={nav:0,pageKey:null,navCount:0};
const SEEN=new Map();
const _card=card;card=function(it,o){SEEN.set(it.provider+':'+it.id,it);return _card(it,o)};
const guessKind=u=>/\.m3u8(\?|$)/i.test(u)?'hls':/\.(mp4|m4v|webm|mov|mkv)(\?|$)/i.test(u)?'mp4':'file';
const fail=(box,e,retry)=>{box.innerHTML=emptyBox("Couldn't load this section",(e&&e.message)||'Network error',retry?'Try again':'');const b=$('[data-retry]',box);if(b&&retry)b.onclick=retry};
const notice=(prov,e)=>'<div class="wrap"><div class="note warn">'+esc(prov)+' is not responding right now ('+esc(e&&e.message||'error')+'). Other sections still work.</div></div>';

/* ----- HOME ----- */
async function pgHome(){
  const my=App.nav;
  V.innerHTML='<div class="pg"><div id="heroBox"><div class="sk skhero"></div></div><div id="rows"></div></div>';
  const rows=$('#rows');const cont=lib.hist().map(h=>({...h,_p:h.d?Math.round(h.t/h.d*100):0}));
  rows.innerHTML=(cont.length?row('Continue watching',cont.slice(0,14)):'')+(lib.list().length?row('My list',lib.list().slice(0,20)):'')+'<div id="rmb">'+skelRow()+'</div><div id="rha"></div><div id="rfk"></div><div id="rdr"></div>';
  let heroSet=false,mbDone=false,haHero=null;
  const setHero=sl=>{if(heroSet||my!==App.nav||!sl.length)return;heroSet=true;$('#heroBox').innerHTML=Hero.html(sl.slice(0,7));Hero.start()};
  const fallbackHero=()=>{if(!heroSet&&haHero)setHero(haHero);else if(!heroSet&&mbDone&&!haHero)$('#heroBox').innerHTML='<div style="height:70px"></div>'};
  api('/mb/home').then(j=>{
    if(my!==App.nav)return;const r=mbSections(D(j));
    let hs=r.hero.length?r.hero:(r.sections[0]?r.sections[0].items.filter(x=>x.poster):[]);
    mbDone=true;setHero(hs);fallbackHero();
    $('#rmb').innerHTML=r.sections.slice(0,9).map(s=>row(s.title,s.items)).join('')||notice('MovieBox','no shelves returned');
  }).catch(e=>{if(my===App.nav){mbDone=true;$('#rmb').innerHTML=notice('MovieBox',e);fallbackHero()}});
  api('/ha/home').then(j=>{
    if(my!==App.nav)return;const secs=genericSections(D(j),a=>haItem(a),3).filter(s=>!/genre/i.test(s.title)).slice(0,4);
    $('#rha').innerHTML=secs.map(s=>row('Anime • '+s.title,s.items)).join('');
    const first=secs[0]&&secs[0].items.filter(x=>x.poster);if(first&&first.length)haHero=first;if(mbDone)fallbackHero();
  }).catch(()=>{});
  api('/ha/top10').then(j=>{if(my!==App.nav)return;const s=genericSections(D(j),a=>haItem(a),3)[0];if(s)$('#rha').insertAdjacentHTML('afterbegin',row('Top 10 anime',s.items.slice(0,10),{cls:'top10',num:1}))}).catch(()=>{});
  api('/fk/home').then(j=>{if(my!==App.nav)return;const it=(D(j)||[]).map(fkItem).filter(Boolean);$('#rfk').innerHTML=row('Fresh in 4K Hub',it)}).catch(()=>{});
  api('/dr/home').then(j=>{if(my!==App.nav)return;const it=(D(j).items||[]).map(drItem).filter(Boolean);$('#rdr').innerHTML=row('Drama & world cinema',it)}).catch(()=>{});
}

/* ----- MOVIES / SERIES (MovieBox search-driven browse) ----- */
const KW=[['Popular','a'],['Action','action'],['Comedy','comedy'],['Romance','romance'],['Horror','horror'],['Thriller','thriller'],['Sci-Fi','sci-fi'],['Animation','animation'],['Crime','crime'],['Bangla','bangla'],['Hindi','hindi'],['Tamil','tamil'],['Telugu','telugu'],['Korean','korean'],['Dubbed','dubbed']].map(x=>({l:x[0],v:x[1]}));
function pgBrowse(kind){
  const my=App.nav,ep=kind==='series'?'/mb/series':'/mb/movies';
  const st={q:'a',page:1,seen:new Set(),busy:false,done:false};
  V.innerHTML='<div class="pg"><div class="ph"><h1>'+(kind==='series'?'Series':'Movies')+'</h1><p>'+(kind==='series'?'Binge-ready shows, season by season.':'Hollywood, Bollywood, Bangla and more — straight from MovieBox.')+'</p></div>'+sBox('bq','Search '+(kind==='series'?'series':'movies')+'…')+'<div id="bc">'+chipsBar(KW,'a','kw')+'</div><div id="bg" class="grid"></div><div class="more-wrap"><button class="btn ghost" id="bm">Load more</button></div></div>';
  const g=$('#bg'),bm=$('#bm');
  const load=async reset=>{
    if(st.busy)return;st.busy=true;bm.disabled=true;
    if(reset){st.page=1;st.seen.clear();st.done=false;g.innerHTML=skelGrid(12).replace('<div class="grid">','').replace(/<\/div>$/,'')}
    try{
      const j=await api(ep+'?q='+enc(st.q)+'&page='+st.page,{ttl:180000});if(my!==App.nav)return;
      const items=(D(j).items||[]).map(mbSubj).filter(Boolean).filter(x=>!st.seen.has(x.id));
      items.forEach(x=>st.seen.add(x.id));
      if(reset)g.innerHTML='';
      g.insertAdjacentHTML('beforeend',items.map(x=>card(x)).join(''));
      if(!items.length){st.done=true;if(reset)g.innerHTML='<div class="empty" style="grid-column:1/-1"><b>Nothing found</b>Try another keyword.</div>'}else st.page++;
    }catch(e){if(reset)g.innerHTML='<div style="grid-column:1/-1">'+emptyBox("Couldn't load",e.message,'Try again')+'</div>',$('[data-retry]',g).onclick=()=>load(true)}
    finally{st.busy=false;bm.disabled=st.done;bm.style.display=st.done?'none':''}
  };
  bm.onclick=()=>load(false);lazyMore(bm,()=>load(false));
  $('#bc').onclick=e=>{const c=e.target.closest('[data-kw]');if(!c)return;st.q=c.dataset.kw;$('#bq').value='';$$('.chip',$('#bc')).forEach(x=>x.classList.toggle('on',x===c));load(true)};
  $('#bq').oninput=debounce(e=>{const v=e.target.value.trim();st.q=v||'a';$$('.chip',$('#bc')).forEach(x=>x.classList.toggle('on',!v&&x.dataset.kw==='a'));load(true)},450);
  load(true);
}

/* ----- ANIME ----- */
function pgAnime(){
  const my=App.nav;
  V.innerHTML='<div class="pg"><div id="ah"></div><div class="ph" style="padding-top:'+'calc(76px + var(--st))'+'"><h1>Anime</h1><p>Hindi-dubbed and subbed series & movies with multiple servers.</p></div>'+sBox('aq','Search anime by title or genre…')+'<div id="arows"></div><div class="rh" style="margin-top:30px"><h2>Full catalog</h2></div><div id="ac"></div><div id="ag" class="grid"></div><div class="more-wrap"><button class="btn ghost" id="am">Show more</button></div></div>';
  api('/ha/hero').then(j=>{if(my!==App.nav)return;const s=genericSections(D(j),a=>haItem(a),1)[0];if(s&&s.items.length){const sl=s.items.filter(x=>x.poster||x.backdrop).slice(0,6);if(sl.length){$('#ah').innerHTML=Hero.html(sl);$('.ph',V).style.paddingTop='20px';Hero.start()}}}).catch(()=>{});
  api('/ha/home').then(j=>{if(my!==App.nav)return;const secs=genericSections(D(j),a=>haItem(a),3).filter(s=>!/genre/i.test(s.title));$('#arows').innerHTML=secs.slice(0,6).map(s=>row(s.title,s.items)).join('')}).catch(()=>{});
  api('/ha/top10').then(j=>{if(my!==App.nav)return;const s=genericSections(D(j),a=>haItem(a),3)[0];if(s)$('#arows').insertAdjacentHTML('afterbegin',row('Top 10 today',s.items.slice(0,10),{cls:'top10',num:1}))}).catch(()=>{});
  const st={all:[],kind:'all',genre:'',q:'',shown:48};
  const draw=()=>{
    const q=st.q.toLowerCase();
    const list=st.all.filter(x=>(st.kind==='all'||x.type===st.kind)&&(!st.genre||(x.genre||'').toLowerCase().includes(st.genre.toLowerCase()))&&(!q||(x.title+' '+x.genre).toLowerCase().includes(q)));
    $('#ag').innerHTML=list.slice(0,st.shown).map(x=>card(x)).join('')||'<div class="empty" style="grid-column:1/-1"><b>No matches</b>Try a different filter.</div>';
    $('#am').style.display=list.length>st.shown?'':'none';st.list=list;
  };
  $('#ag').innerHTML=skelGrid(12).replace('<div class="grid">','').replace(/<\/div>$/,'');
  api('/ha/catalog',{ttl:600000}).then(j=>{
    if(my!==App.nav)return;const d=D(j);
    st.all=[...(d.series||[]).map(a=>haItem(a,'series')),...(d.movies||[]).map(a=>haItem(a,'movie'))].filter(x=>x&&x.title);
    const gc={};st.all.forEach(x=>(x.genre||'').split(',').map(s=>s.trim()).filter(Boolean).forEach(g=>gc[g]=(gc[g]||0)+1));
    const gl=Object.entries(gc).sort((a,b)=>b[1]-a[1]).slice(0,14).map(x=>({l:x[0],v:x[0]}));
    $('#ac').innerHTML='<div class="chips"><button class="chip on" data-k="all">All</button><button class="chip" data-k="series">Series</button><button class="chip" data-k="movie">Movies</button></div>'+(gl.length?'<div class="chips" style="padding-top:0">'+'<button class="chip on" data-g="">Any genre</button>'+gl.map(c=>'<button class="chip" data-g="'+esc(c.v)+'">'+esc(c.l)+'</button>').join('')+'</div>':'');
    draw();
  }).catch(e=>fail($('#ag'),e,pgAnime));
  $('#ac').onclick=e=>{const c=e.target.closest('.chip');if(!c)return;const grp=c.parentElement;$$('.chip',grp).forEach(x=>x.classList.toggle('on',x===c));if('k' in c.dataset)st.kind=c.dataset.k;if('g' in c.dataset)st.genre=c.dataset.g;st.shown=48;draw()};
  $('#am').onclick=()=>{st.shown+=48;draw()};lazyMore($('#am'),()=>{if($('#am').style.display!=='none'){st.shown+=48;draw()}});
  $('#aq').oninput=debounce(e=>{st.q=e.target.value.trim();st.shown=48;draw()},250);
}

/* ----- 4K HUB ----- */
function pg4k(){
  const my=App.nav,st={slug:'',q:'',page:1,seen:new Set(),busy:false,done:false};
  const cats=[{l:'Latest',v:''},{l:'Movies',v:'movies'},{l:'Series',v:'series'},{l:'Anime',v:'anime'},{l:'4K UHD',v:'4k'}];
  V.innerHTML='<div class="pg"><div class="ph"><h1>4K Hub</h1><p>High-bitrate releases. Open a title to resolve direct download and stream links.</p></div>'+sBox('kq','Search 4K titles…')+'<div id="kc">'+chipsBar(cats,'','cat')+'</div><div id="kg" class="grid"></div><div class="more-wrap"><button class="btn ghost" id="km">Load more</button></div></div>';
  const g=$('#kg'),km=$('#km');
  const load=async reset=>{
    if(st.busy)return;st.busy=true;km.disabled=true;
    if(reset){st.page=1;st.seen.clear();st.done=false;g.innerHTML=skelGrid(12).replace('<div class="grid">','').replace(/<\/div>$/,'')}
    try{
      let path=st.q?'/fk/search?q='+enc(st.q):(st.slug?'/fk/category/'+enc(st.slug)+'?page='+st.page:'/fk/home');
      const j=await api(path,{ttl:180000});if(my!==App.nav)return;
      const items=(D(j)||[]).map(fkItem).filter(Boolean).filter(x=>!st.seen.has(x.id));items.forEach(x=>st.seen.add(x.id));
      if(reset)g.innerHTML='';g.insertAdjacentHTML('beforeend',items.map(x=>card(x)).join(''));
      if(!items.length||st.q||!st.slug){st.done=true}else st.page++;
      if(!items.length&&reset)g.innerHTML='<div class="empty" style="grid-column:1/-1"><b>Nothing here</b>Try another category or search.</div>';
    }catch(e){if(reset){g.innerHTML='<div style="grid-column:1/-1">'+emptyBox("Couldn't load 4K Hub",e.message,'Try again')+'</div>';$('[data-retry]',g).onclick=()=>load(true)}}
    finally{st.busy=false;km.disabled=st.done;km.style.display=st.done?'none':''}
  };
  km.onclick=()=>load(false);lazyMore(km,()=>load(false));
  $('#kc').onclick=e=>{const c=e.target.closest('[data-cat]');if(!c)return;st.slug=c.dataset.cat;st.q='';$('#kq').value='';$$('.chip',$('#kc')).forEach(x=>x.classList.toggle('on',x===c));load(true)};
  $('#kq').oninput=debounce(e=>{st.q=e.target.value.trim();$$('.chip',$('#kc')).forEach(x=>x.classList.remove('on'));load(true)},500);
  load(true);
}

/* ----- DRAMA (Dramachi) ----- */
function pgDrama(){
  const my=App.nav,st={q:'',filter:'all',page:1,seen:new Set(),busy:false,done:false};
  V.innerHTML='<div class="pg"><div class="ph"><h1>Drama</h1><p>K-drama, C-drama, Turkish and world series & films. Pick a title to find where it streams.</p></div>'+sBox('dq','Search dramas…')+'<div id="dc">'+chipsBar([{l:'All',v:'all'},{l:'Series',v:'series'},{l:'Movies',v:'movies'}],'all','f')+'</div><div id="dg" class="grid"></div><div class="more-wrap"><button class="btn ghost" id="dm">Load more</button></div></div>';
  const g=$('#dg'),dm=$('#dm');
  const load=async reset=>{
    if(st.busy)return;st.busy=true;dm.disabled=true;
    if(reset){st.page=1;st.seen.clear();st.done=false;g.innerHTML=skelGrid(12).replace('<div class="grid">','').replace(/<\/div>$/,'')}
    try{
      const path=st.q?'/dr/search?q='+enc(st.q)+'&page='+st.page+'&filter='+st.filter:'/dr/home?page='+st.page+'&filter='+st.filter;
      const j=await api(path,{ttl:180000});if(my!==App.nav)return;const d=D(j);
      const items=(d.items||[]).map(drItem).filter(Boolean).filter(x=>!st.seen.has(x.id));items.forEach(x=>st.seen.add(x.id));
      if(reset)g.innerHTML='';g.insertAdjacentHTML('beforeend',items.map(x=>card(x)).join(''));
      if(!items.length||(d.last_page&&st.page>=d.last_page))st.done=true;else st.page++;
      if(!items.length&&reset)g.innerHTML='<div class="empty" style="grid-column:1/-1"><b>Nothing found</b>Try another title.</div>';
    }catch(e){if(reset){g.innerHTML='<div style="grid-column:1/-1">'+emptyBox("Couldn't load dramas",e.message,'Try again')+'</div>';$('[data-retry]',g).onclick=()=>load(true)}}
    finally{st.busy=false;dm.disabled=st.done;dm.style.display=st.done?'none':''}
  };
  dm.onclick=()=>load(false);lazyMore(dm,()=>load(false));
  $('#dc').onclick=e=>{const c=e.target.closest('[data-f]');if(!c)return;st.filter=c.dataset.f;$$('.chip',$('#dc')).forEach(x=>x.classList.toggle('on',x===c));load(true)};
  $('#dq').oninput=debounce(e=>{st.q=e.target.value.trim();load(true)},500);
  load(true);
}

/* ----- LIVE TV ----- */
function pgLive(){
  const my=App.nav,st={src:store.get('sh_iptv',1),all:[],q:'',group:'',shown:90};
  V.innerHTML='<div class="pg"><div class="ph"><h1>Live TV</h1><p>Thousands of free channels. Tap one to watch.</p></div>'+sBox('lq','Search channels…')+'<div id="lsrc">'+chipsBar([{l:'World',v:0},{l:'Bangladesh',v:1},{l:'India',v:2}].map(x=>({l:x.l,v:String(x.v)})),String(st.src),'src')+'</div><div id="lgp"></div><div id="lg" class="chs"></div><div class="more-wrap"><button class="btn ghost" id="lm">Show more</button></div></div>';
  const draw=()=>{
    const q=st.q.toLowerCase();
    const l=st.all.filter(c=>(!st.group||c.group===st.group)&&(!q||(c.name+' '+(c.group||'')).toLowerCase().includes(q)));st.list=l;
    $('#lg').innerHTML=l.slice(0,st.shown).map((c,i)=>'<button class="ch" data-ci="'+i+'"><span class="lg">'+(c.logo?'<img loading="lazy" referrerpolicy="no-referrer" src="'+esc(c.logo)+'" alt="" onerror="this.remove()">':esc((c.name||'?')[0]))+'</span><div><b>'+esc(c.name)+'</b><small><span class="live">LIVE</span> '+esc(c.group||'')+'</small></div></button>').join('')||'<div class="empty" style="grid-column:1/-1"><b>No channels</b>Try another source or search.</div>';
    $('#lm').style.display=l.length>st.shown?'':'none';
  };
  const load=async()=>{
    $('#lg').innerHTML='<div class="empty" style="grid-column:1/-1">Loading channels…</div>';
    try{
      const j=await api('/iptv/channels?source='+st.src+'&limit=4000',{ttl:600000,timeout:60000});if(my!==App.nav)return;
      st.all=D(j)||[];st.group='';st.shown=90;
      const gc={};st.all.forEach(c=>{if(c.group)gc[c.group]=(gc[c.group]||0)+1});
      const gl=Object.entries(gc).sort((a,b)=>b[1]-a[1]).slice(0,16);
      $('#lgp').innerHTML='<div class="chips" style="padding-top:0"><button class="chip on" data-grp="">All</button>'+gl.map(x=>'<button class="chip" data-grp="'+esc(x[0])+'">'+esc(x[0])+'</button>').join('')+'</div>';
      draw();
    }catch(e){fail($('#lg'),e,load)}
  };
  $('#lsrc').onclick=e=>{const c=e.target.closest('[data-src]');if(!c)return;st.src=+c.dataset.src;store.set('sh_iptv',st.src);$$('.chip',$('#lsrc')).forEach(x=>x.classList.toggle('on',x===c));load()};
  $('#lgp').onclick=e=>{const c=e.target.closest('[data-grp]');if(!c)return;st.group=c.dataset.grp;st.shown=90;$$('.chip',$('#lgp')).forEach(x=>x.classList.toggle('on',x===c));draw()};
  $('#lq').oninput=debounce(e=>{st.q=e.target.value.trim();st.shown=90;draw()},250);
  $('#lm').onclick=()=>{st.shown+=90;draw()};
  $('#lg').onclick=e=>{const b=e.target.closest('[data-ci]');if(!b)return;const c=st.list[+b.dataset.ci];if(!c)return;
    openTheatre({title:c.name,sub:c.group||'Live channel',poster:c.logo,live:true,sources:[{kind:'hls',url:c.url,label:'Live HLS'}],extra:'<div class="note">Live channels depend on the broadcaster. If one is offline, pick another from the list.</div>'})};
  load();
}

/* ----- MIDNIGHT (18+) ----- */
function pgMidnight(){
  const my=App.nav;
  if(!store.get('sh_18',false)){
    V.innerHTML='<div class="pg gate"><div class="box"><div class="moon"></div><h1>Midnight</h1><p>This section contains adult-only titles. Continue only if you are 18 or older and it is legal where you live.</p><div class="acts"><button class="btn pri" id="y18">I am 18 or older</button><a class="btn ghost" href="#/home">Take me back</a></div></div></div>';
    $('#y18').onclick=()=>{store.set('sh_18',true);pgMidnight()};return;
  }
  const st={q:'',page:1,seen:new Set(),busy:false,done:false};
  V.innerHTML='<div class="pg"><div class="ph"><h1>Midnight</h1><p>Late-night shelves, 18+ only.</p><div class="acts" style="margin-top:12px"><button class="btn sm ghost" id="lock18">Lock Midnight</button></div></div>'+sBox('mq','Search Midnight…')+'<div id="mrows">'+skelRow()+skelRow()+'</div><div id="mg" class="grid" style="display:none"></div><div class="more-wrap"><button class="btn ghost" id="mm" style="display:none">Load more</button></div></div>';
  $('#lock18').onclick=()=>{store.set('sh_18',false);toast('Midnight locked');pgMidnight()};
  api('/mb/adult/home').then(j=>{if(my!==App.nav)return;const r=mbSections(D(j));const secs=r.sections;if(r.hero.length)secs.unshift({title:'Featured',items:r.hero});$('#mrows').innerHTML=secs.map(s=>row(s.title,s.items)).join('')||emptyBox('No shelves right now','Try searching instead.')}).catch(e=>{$('#mrows').innerHTML=notice('Midnight',e)});
  const g=$('#mg'),mm=$('#mm');
  const load=async reset=>{
    if(st.busy||!st.q)return;st.busy=true;mm.disabled=true;
    if(reset){st.page=1;st.seen.clear();st.done=false;g.innerHTML=skelGrid(9).replace('<div class="grid">','').replace(/<\/div>$/,'')}
    try{const j=await api('/mb/adult?q='+enc(st.q)+'&page='+st.page,{ttl:180000});if(my!==App.nav)return;
      const items=(D(j).items||[]).map(mbSubj).filter(Boolean).filter(x=>!st.seen.has(x.id));items.forEach(x=>st.seen.add(x.id));
      if(reset)g.innerHTML='';g.insertAdjacentHTML('beforeend',items.map(x=>card(x)).join(''));
      if(!items.length){st.done=true;if(reset)g.innerHTML='<div class="empty" style="grid-column:1/-1"><b>No results</b></div>'}else st.page++}
    catch(e){if(reset)g.innerHTML='<div style="grid-column:1/-1">'+emptyBox("Couldn't search",e.message)+'</div>'}
    finally{st.busy=false;mm.disabled=st.done;mm.style.display=st.done?'none':''}
  };
  mm.onclick=()=>load(false);lazyMore(mm,()=>load(false));
  $('#mq').oninput=debounce(e=>{st.q=e.target.value.trim();const on=!!st.q;$('#mrows').style.display=on?'none':'';g.style.display=on?'':'none';mm.style.display=on?'':'none';if(on)load(true)},500);
}

/* ----- shared link rows (download / play / copy) ----- */
function linkRow(l){
  const u=l.url||'';const k=l.kind&&l.kind!=='direct'?l.kind:guessKind(u);const sz=fmtSize(l.size);
  const sub=[l.quality,sz,l.codec,(l.note||'')].filter(Boolean).join(' • ');
  return'<div class="dli"><div class="inf"><b>'+esc(l.label||l.title||l.release||'Direct link')+'</b><small>'+esc(sub||k.toUpperCase())+'</small></div><div class="ba">'+
    (k==='mp4'||k==='hls'||k==='file'?'<button class="btn sm pri" data-play="'+esc(u)+'" data-pk="'+esc(k)+'" data-pt="'+esc(l.label||l.title||'Video')+'">'+ico('play')+'Play</button>':'')+
    '<a class="btn sm ghost" href="'+esc(u)+'" target="_blank" rel="noopener" download>'+ico('dl')+'Download</a>'+
    '<button class="btn sm ghost" data-copy="'+esc(u)+'">'+ico('copy')+'Copy</button>'+
    '<a class="btn sm ghost" href="'+esc(extIntent(u))+'">'+ico('ext')+'Open in app</a></div></div>';
}
document.addEventListener('click',e=>{
  const c=e.target.closest('[data-copy]');if(c){copyText(c.dataset.copy);return}
  const p=e.target.closest('[data-play]');if(p){openTheatre({title:p.dataset.pt||'Video',sources:[{kind:p.dataset.pk==='hls'?'hls':'mp4',url:p.dataset.play,label:(p.dataset.pk||'file').toUpperCase()}]})}
});

/* ----- DOWNLOADS ----- */
function pgDownloads(){
  const my=App.nav;
  V.innerHTML='<div class="pg"><div class="ph"><h1>Downloads</h1><p>Find a title to get every direct link, or paste any page or file link to extract downloadable media.</p></div>'+sBox('xq','Search a movie, series or anime…')+'<div id="xr"></div>'+
  '<div class="rh" style="margin-top:34px"><h2>Paste a link</h2></div><div class="wrap"><div class="panel"><label class="sbox" style="margin:0;max-width:none">'+ico('link')+'<input id="xl" type="url" placeholder="https://hubcloud… / pixeldrain… / any page with a video"></label><div class="acts" style="margin-top:12px"><button class="btn pri" id="xgo">Extract links</button></div><div id="xo" class="dl" style="margin-top:14px"></div></div></div></div>';
  const run=async q=>{
    const box=$('#xr');if(!q){box.innerHTML='';return}
    box.innerHTML=skelRow();
    const dl=h=>h.replace(/href="([^"]+)"/,'href="$1?dl=1"');
    const [a,b,c]=await Promise.allSettled([api('/mb/search?q='+enc(q)),api('/fk/search?q='+enc(q)),api('/ha/search?q='+enc(q))]);
    if(my!==App.nav)return;let h='';
    if(a.status==='fulfilled'){const it=(D(a.value).items||[]).map(mbSubj).filter(Boolean);h+=row('MovieBox — direct MP4',it)}
    if(b.status==='fulfilled'){const it=(D(b.value)||[]).map(fkItem).filter(Boolean);h+=row('4K Hub — resolved links',it)}
    if(c.status==='fulfilled'){const it=(D(c.value).items||[]).map(x=>haItem(x,x.kind)).filter(Boolean);h+=row('Anime',it)}
    box.innerHTML=(h||emptyBox('No results','Try different words.'));
    $$('a.card',box).forEach(x=>x.setAttribute('href',x.getAttribute('href')+'?dl=1'));
  };
  $('#xq').oninput=debounce(e=>run(e.target.value.trim()),500);
  $('#xgo').onclick=async()=>{
    const u=$('#xl').value.trim();const o=$('#xo');if(!/^https?:\/\//i.test(u)){toast('Paste a full link starting with http');return}
    o.innerHTML='<div class="note">Resolving…</div>';
    try{
      const [r1,r2]=await Promise.allSettled([api('/tools/resolve?url='+enc(u),{ttl:0,retry:0,timeout:60000}),api('/tools/mp4?url='+enc(u),{ttl:0,retry:0,timeout:60000})]);
      const L=[];const seen=new Set();
      [r1,r2].forEach(r=>{if(r.status==='fulfilled'){((D(r.value)||{}).links||[]).forEach(x=>{if(x&&x.url&&!seen.has(x.url)&&x.kind!=='error'){seen.add(x.url);L.push(x)}})}});
      o.innerHTML=L.length?L.map(linkRow).join(''):'<div class="note warn">No direct links found. The page may need a browser, or the link has expired.</div>';
    }catch(e){o.innerHTML='<div class="note warn">'+esc(e.message)+'</div>'}
  };
}

/* ----- LIBRARY ----- */
function pgLibrary(){
  const l=lib.list(),h=lib.hist().map(x=>({...x,_p:x.d?Math.round(x.t/x.d*100):0}));
  V.innerHTML='<div class="pg"><div class="ph"><h1>My library</h1><p>Saved on this device only.</p></div>'+
  (h.length?row('Continue watching',h)+'<div class="wrap"><button class="btn sm ghost" id="clh">Clear history</button></div>':'')+
  (l.length?'<div class="rh" style="margin-top:28px"><h2>My list</h2></div><div class="grid">'+l.map(x=>card(x)).join('')+'</div>':'')+
  (!h.length&&!l.length?emptyBox('Nothing saved yet','Tap “My list” on any title to keep it here.'):'')+'</div>';
  const b=$('#clh');if(b)b.onclick=()=>{store.set('sh_hist',[]);toast('History cleared');pgLibrary()};
}

/* ----- SEARCH ----- */
function pgSearch(q){
  const my=App.nav;
  V.innerHTML='<div class="pg"><div class="ph"><h1>Search</h1><p>One box across MovieBox, 4K Hub, Drama and Anime.</p></div>'+sBox('sq','Type a title…',q)+'<div id="sr"></div></div>';
  const run=async qq=>{
    const box=$('#sr');if(!qq){box.innerHTML=emptyBox('Start typing','Results appear as you type.');return}
    box.innerHTML=skelRow()+skelRow();
    const [a,b]=await Promise.allSettled([api('/search?q='+enc(qq)),api('/ha/search?q='+enc(qq))]);
    if(my!==App.nav)return;let h='';
    if(a.status==='fulfilled'){const d=D(a.value)||{};
      const mb=((d.moviebox||{}).items||[]).map(mbSubj).filter(Boolean);h+=row('MovieBox',mb);
      const fk=Array.isArray(d['4khdhub'])?d['4khdhub'].map(fkItem).filter(Boolean):[];h+=row('4K Hub',fk);
      const dr=((d.dramachi||{}).items||[]).map(drItem).filter(Boolean);h+=row('Drama',dr)}
    if(b.status==='fulfilled'){const it=(D(b.value).items||[]).map(x=>haItem(x,x.kind)).filter(Boolean);h=row('Anime',it)+h}
    box.innerHTML=h||emptyBox('No results for “'+qq+'”','Check the spelling or try fewer words.');
  };
  const inp=$('#sq');inp.focus();
  inp.oninput=debounce(e=>{const v=e.target.value.trim();history.replaceState(null,'','#/search'+(v?'?q='+enc(v):''));App.pageKey='search'+(v?'?q='+enc(v):'');run(v)},450);
  run(q);
}

/* ===== detail views, theatre, router, boot ===== */
const DT={pl:null,id:0};
const killPlayer=()=>{if(DT.pl){try{DT.pl.destroy()}catch(e){}DT.pl=null}};
function closeDetail(){DT.id++;killPlayer();$('#detail').classList.remove('open');document.body.classList.remove('lock')}
function showDetail(html,bg){
  killPlayer();const d=$('#detail');$('#dcontent').innerHTML=html;$('#dbg').style.backgroundImage=bg?'url('+JSON.stringify(bg)+')':'none';
  d.classList.add('open');document.body.classList.add('lock');d.scrollTop=0;
}
function goBack(){if(App.navCount>1)history.back();else location.hash='#/home'}
const topBar=()=>'<div class="dtop"><button class="ibtn" data-close aria-label="Back">'+ico('back')+'</button></div>';
function detailErr(msg,retry){showDetail(topBar()+'<div class="dbody">'+emptyBox("Couldn't open this title",msg,retry?'Try again':'')+'</div>');const b=$('[data-retry]','#dcontent');if(b&&retry)b.onclick=retry}
function mountPlayer(box,sources,ctx){
  killPlayer();
  box.innerHTML='<div id="plh"></div><div class="srcs" id="srcs"></div>';
  const srcs=$('#srcs',box);
  const pl=new Player($('#plh',box),{onChange:(i,l)=>{srcs.innerHTML=srcChips(l,i)}});
  DT.pl=pl;
  srcs.onclick=e=>{const c=e.target.closest('[data-src]');if(!c)return;const i=+c.dataset.src;pl.sources[i].dead=false;pl.sources[i].proxied=false;pl.play(i,true)};
  pl.setSources(sources,ctx);
  return pl;
}
const cover=(img,label)=>'<div class="pcover" id="pcv" style="background-image:url('+esc(JSON.stringify(img||''))+')" role="button" tabindex="0" aria-label="Play '+esc(label)+'"><span class="pb">'+ico('play')+'</span></div>';
function metaTags(it,extra){
  return(it.rating?'<span class="tag am">★ '+esc(it.rating)+'</span>':'')+(it.year?'<span class="tag">'+esc(it.year)+'</span>':'')+
    '<span class="tag">'+(it.type==='series'?'Series':'Movie')+'</span>'+(it.duration?'<span class="tag">'+fmtDur(it.duration)+'</span>':'')+
    (it.country?'<span class="tag">'+esc(it.country)+'</span>':'')+(extra||'')+
    (it.genre?String(it.genre).split(/[,/]/).slice(0,4).map(g=>'<span class="tag">'+esc(g.trim())+'</span>').join(''):'');
}
function detailShell(it,btns,body){
  const inl=lib.has(it.provider+':'+it.id);
  return'<div class="dtop"><button class="ibtn" data-close aria-label="Back">'+ico('back')+'</button><div style="flex:1"></div><button class="ibtn" data-share aria-label="Share">'+ico('share')+'</button></div>'+
  '<div class="dbody"><div class="dmain'+(it.poster?' has-side':'')+'">'+(it.poster?'<div class="dposter"><img referrerpolicy="no-referrer" alt="" src="'+esc(it.poster)+'" onerror="this.parentElement.remove()"></div>':'')+
  '<div class="dh"><h1>'+esc(it.title)+'</h1><div class="meta">'+metaTags(it)+'</div><div class="acts">'+(btns||'')+
  '<button class="btn ghost'+(inl?' on':'')+'" data-listbtn>'+ico(inl?'check':'plus')+'My list</button></div>'+(it.desc?'<p class="dd">'+esc(it.desc)+'</p>':'')+'</div></div>'+body+'</div>';
}
document.addEventListener('click',e=>{
  if(e.target.closest('[data-close]')){goBack();return}
  const sh=e.target.closest('[data-share]');
  if(sh){const u=location.href;if(navigator.share)navigator.share({title:document.title,url:u}).catch(()=>{});else copyText(u)}
});
function bindList(it){
  const b=$('[data-listbtn]','#dcontent');if(!b)return;
  b.onclick=()=>{const on=lib.toggle(it);b.classList.toggle('on',on);b.innerHTML=ico(on?'check':'plus')+'My list';toast(on?'Added to My list':'Removed from My list')};
}
function tabsUI(box,defs,start){
  box.innerHTML='<div class="tabs">'+defs.map(d=>'<button data-t="'+d.k+'">'+d.l+'</button>').join('')+'<span class="ink"></span></div><div id="tabc"></div>';
  const bar=box.querySelector('.tabs'),ink=box.querySelector('.ink');
  const go=k=>{
    $$('.tabs button',box).forEach(b=>b.classList.toggle('on',b.dataset.t===k));
    const on=bar.querySelector('button.on');if(on){ink.style.width=on.offsetWidth+'px';ink.style.transform='translateX('+on.offsetLeft+'px)'}
    const d=defs.find(x=>x.k===k);const c=$('#tabc',box);c.onclick=null;c.innerHTML='';c.style.animation='none';void c.offsetWidth;c.style.animation='';c.classList.add('pg');d.fn(c)};
  bar.onclick=e=>{const b=e.target.closest('[data-t]');if(b)go(b.dataset.t)};
  go(start&&defs.some(d=>d.k===start)?start:defs[0].k);
  setTimeout(()=>{const on=bar.querySelector('button.on');if(on){ink.style.width=on.offsetWidth+'px';ink.style.transform='translateX('+on.offsetLeft+'px)'}},120);
  return go;
}

/* ===== MovieBox detail ===== */
async function dtMB(id,q){
  const my=++DT.id,seen=SEEN.get('mb:'+id);
  showDetail(topBar()+'<div class="dbody"><div class="sk" style="height:34px;width:60%;margin:20px 0"></div><div class="sk" style="aspect-ratio:16/9"></div></div>',seen&&seen.poster);
  let det,seasons=[];
  try{
    const [a,b]=await Promise.allSettled([api('/mb/detail/'+enc(id)),api('/mb/seasons/'+enc(id))]);
    if(a.status!=='fulfilled')throw a.reason;
    det=mbDetail(D(a.value));if(b.status==='fulfilled')seasons=mbSeasons(D(b.value)).filter(s=>s.se>0);
  }catch(e){if(my===DT.id)detailErr(e.message,()=>dtMB(id,q));return}
  if(my!==DT.id)return;
  const stRaw=String(det._st||''),series=(stRaw==='2'||stRaw==='series')?true:(stRaw==='1'||stRaw==='movie')?false:(seasons.length>0||(seen&&seen.type==='series'));
  const it={provider:'mb',id:String(id),title:det.title&&det.title!=='Untitled'?det.title:(seen&&seen.title)||'Untitled',poster:det.poster||(seen&&seen.poster)||'',backdrop:det.backdrop,year:det.year,rating:det.rating,genre:det.genre,type:series?'series':'movie',desc:det.desc,duration:det.duration,country:det.country};
  if(series&&!seasons.length)seasons=[{se:1,max:24}];
  const pos=lib.pos(it);const st={se:(pos&&pos.se)||(seasons[0]&&seasons[0].se)||1,ep:(pos&&pos.ep)||1};
  showDetail(detailShell(it,'<button class="btn pri" data-playbtn>'+ico('play')+(pos?'Resume':'Play')+'</button>'+(det.trailer?'<button class="btn ghost" data-trailer>'+ico('film')+'Trailer</button>':''),
    '<div id="pbox" style="margin-top:22px">'+cover(it.backdrop||it.poster,it.title)+'</div><div id="tabsbox"></div><div id="rel"></div>'),it.backdrop||it.poster);
  bindList(it);
  const box=$('#pbox');
  const curSeason=()=>seasons.find(s=>s.se===st.se)||seasons[0];
  const nextEp=()=>{if(!series)return null;const s=curSeason();if(st.ep<s.max)return{se:st.se,ep:st.ep+1};const n=seasons.find(x=>x.se>st.se);return n?{se:n.se,ep:1}:null};
  async function play(){
    if(my!==DT.id)return;
    box.innerHTML='<div class="pcover" style="background-image:url('+esc(JSON.stringify(it.backdrop||it.poster||''))+')"><span class="pb" style="animation:spin 1s linear infinite;background:none;box-shadow:none;border:4px solid rgba(255,255,255,.2);border-top-color:var(--amber)"></span></div>';
    try{
      const list=await mbStreamsFor(id,series?st.se:null,series?st.ep:null);
      if(my!==DT.id)return;
      const n=nextEp();
      const p=lib.pos(it);const same=p&&(!series||(p.se===st.se&&p.ep===st.ep));
      mountPlayer(box,list,{title:it.title+(series?' · S'+st.se+' E'+st.ep:''),sub:'MovieBox',poster:it.poster,resume:same?p.t:0,
        onProgress:(t,d)=>lib.save(it,t,d,{se:st.se,ep:st.ep}),
        next:n?()=>{st.se=n.se;st.ep=n.ep;epUI&&epUI();play()}:null,onEnded:()=>{if(n){st.se=n.se;st.ep=n.ep;epUI&&epUI();play()}}});
      box.scrollIntoView({behavior:'smooth',block:'center'});
    }catch(e){box.innerHTML=cover(it.backdrop||it.poster,it.title)+'<div class="note warn" style="margin-top:12px">'+esc(e.message)+' — <a href="#" data-again style="text-decoration:underline">try again</a></div>';$('[data-again]',box).onclick=ev=>{ev.preventDefault();play()};$('#pcv',box).onclick=play}
  }
  $('#pcv').onclick=play;$('[data-playbtn]','#dcontent').onclick=play;
  const tb=$('[data-trailer]','#dcontent');if(tb)tb.onclick=()=>openTheatre({title:it.title+' — Trailer',poster:it.poster,sources:[{kind:guessKind(det.trailer),url:det.trailer,label:'Trailer'}]});
  let epUI=null;
  const defs=[{k:'ov',l:'Overview',fn:c=>{
    c.innerHTML='<div class="panel"><p class="dd">'+esc(it.desc||'No description available.')+'</p></div>'+(det.subtitles?'<div class="note" style="margin-top:12px">Subtitles: '+esc(det.subtitles)+'</div>':'');
  }}];
  if(series)defs.push({k:'ep',l:'Episodes',fn:c=>{
    epUI=()=>{
      c.innerHTML='<div class="sel">'+seasons.map(s=>'<button class="chip'+(s.se===st.se?' on':'')+'" data-se="'+s.se+'">Season '+s.se+'</button>').join('')+'</div><div class="eps">'+Array.from({length:curSeason().max||24},(_,i)=>i+1).map(n=>'<button class="ep'+(n===st.ep?' on':'')+'" data-ep="'+n+'">'+n+'</button>').join('')+'</div>';
    };epUI();
    c.onclick=e=>{const s=e.target.closest('[data-se]'),p=e.target.closest('[data-ep]');if(s){st.se=+s.dataset.se;st.ep=1;epUI()}else if(p){st.ep=+p.dataset.ep;epUI();play()}};
  }});
  defs.push({k:'dl',l:'Download',fn:async c=>{
    c.innerHTML='<div class="note">Direct links for '+(series?'<b>S'+st.se+' E'+st.ep+'</b>':'this title')+'. Pick a quality and tap Download, or open it in your video app.</div><div class="dl" id="dll">'+'<div class="sk" style="height:64px"></div>'.repeat(3)+'</div>';
    try{
      const list=await mbStreamsFor(id,series?st.se:null,series?st.ep:null);if(my!==DT.id)return;
      const mp=list.filter(s=>s.kind!=='dash').sort((a,b)=>b.q-a.q),ds=list.filter(s=>s.kind==='dash');
      $('#dll',c).innerHTML=(mp.map(s=>linkRow({label:'MP4 '+(s.quality||''),url:s.url,quality:'',size:s.size,codec:s.codec,kind:s.kind==='mp4'?'mp4':'file'})).join('')+
        ds.map(s=>{const abs=location.origin+dashProxyUrl(s.base||String(s.url||'').replace(/\/index\.mpd.*$/,''),s.cookie);return'<div class="dli"><div class="inf"><b>DASH '+esc(s.quality||'')+(s.codec?' <span class="tag">'+esc(String(s.codec).toUpperCase())+'</span>':'')+'</b><small>Adaptive stream. Plays through the server link below; VLC / MX Player can open it directly.</small></div><div class="ba"><button class="btn sm pri" data-dashplay="'+list.indexOf(s)+'">'+ico('play')+'Play</button><button class="btn sm ghost" data-copy="'+esc(abs)+'">'+ico('copy')+'Copy link</button><a class="btn sm ghost" href="'+esc(extIntent(abs))+'">'+ico('ext')+'Open in app</a></div></div>'}).join(''))||'<div class="note warn">No download links were returned for this title.</div>';
      $$('[data-dashplay]',c).forEach(b=>b.onclick=()=>{play()});
    }catch(e){$('#dll',c).innerHTML='<div class="note warn">'+esc(e.message)+'</div>'}
  }});
  if(det.stars.length)defs.push({k:'ca',l:'Cast',fn:c=>{c.innerHTML='<div class="cast">'+det.stars.slice(0,20).map(s=>'<div>'+(s.img?'<img referrerpolicy="no-referrer" alt="" src="'+esc(s.img)+'" onerror="this.outerHTML=\'<i>'+esc(s.name[0])+'</i>\'">':'<i>'+esc(s.name[0])+'</i>')+'<b>'+esc(s.name)+'</b>'+esc(s.role||'')+'</div>').join('')+'</div>'}});
  tabsUI($('#tabsbox'),defs,q.get('dl')?'dl':null);
  const g=String(it.genre||'').split(/[,/]/)[0].trim();
  if(g)api('/mb/search?q='+enc(g)).then(j=>{if(my!==DT.id)return;const items=(D(j).items||[]).map(mbSubj).filter(x=>x&&x.id!==String(id));$('#rel').innerHTML=row('More like this',items)}).catch(()=>{});
  if(q.get('play')){play()}
}
async function mbStreamsFor(id,se,ep){
  const j=await api('/mb/cdn/'+enc(id)+(se!=null?'?se='+se+'&ep='+ep:''),{ttl:240000,timeout:60000});
  const list=mbStreams(D(j));
  if(!list.length)throw new Error('MovieBox returned no streams for this selection');
  return list;
}

/* ===== Anime (HindiAnime) detail ===== */
async function dtHA(key,q,isUrl){
  const my=++DT.id,seen=SEEN.get('ha:'+key);
  showDetail(topBar()+'<div class="dbody"><div class="sk" style="height:34px;width:60%;margin:20px 0"></div><div class="sk" style="aspect-ratio:16/9"></div></div>',seen&&seen.poster);
  let d;
  try{const j=await api('/ha/details?'+(isUrl?'url=':'id=')+enc(key),{timeout:60000});d=D(j)||{};}
  catch(e){if(my===DT.id)detailErr(e.message,()=>dtHA(key,q,isUrl));return}
  if(my!==DT.id)return;
  const cat=d.catalog||{};
  const eps=haEpisodes(d);const seasonKeys=Object.keys(eps).sort((a,b)=>a-b);
  const type=String(d.type||cat.type||'').toLowerCase().includes('movie')||!seasonKeys.length?'movie':'series';
  const it={provider:'ha',id:isUrl?slugFromLink(key):String(key),link:isUrl?key:(cat.link||''),title:d.title||cat.title||(seen&&seen.title)||'Untitled',
    poster:imgOf(pick(d,'poster','image','thumbnail'))||imgOf(cat.poster)||(seen&&seen.poster)||'',
    backdrop:imgOf(pick(d,'backdrop','banner'))||'',year:String(pick(d,'year')||cat.year||'').slice(0,4),
    genre:(Array.isArray(d.genres)?d.genres.join(', '):d.genres)||(Array.isArray(cat.genres)?cat.genres.join(', '):''),type,rating:rtNum(pick(d,'rating')||cat.rating),
    desc:pick(d,'description','overview','synopsis')||cat.description||''};
  const pos=lib.pos(it);const st={se:+((pos&&pos.se)||seasonKeys[0]||1),ep:(pos&&pos.ep)||1,gen:0};
  showDetail(detailShell(it,'<button class="btn pri" data-playbtn>'+ico('play')+(pos?'Resume':'Play')+'</button>','<div id="pbox" style="margin-top:22px">'+cover(it.backdrop||it.poster,it.title)+'</div><div id="tabsbox"></div><div id="rel"></div>'),it.backdrop||it.poster);
  bindList(it);
  const box=$('#pbox');
  const curEps=()=>eps[String(st.se)]||[];
  const nextEp=()=>{if(type==='movie')return null;const l=curEps();if(l.find(e=>e.episode===st.ep+1))return{se:st.se,ep:st.ep+1};const k=seasonKeys[seasonKeys.indexOf(String(st.se))+1];return k?{se:+k,ep:(eps[k][0]&&eps[k][0].episode)||1}:null};
  async function fetchServers(){
    const p='/ha/stream?'+(isUrl&&!it.id?'':'id='+enc(it.id))+(isUrl?'&url='+enc(key)+'&title='+enc(it.title):'')+'&season='+st.se+'&episode='+st.ep+(type==='movie'?'&is_movie=true':'');
    const j=await api(p.replace('?&','?'),{ttl:240000,timeout:70000,retry:0});
    return haServers(D(j));
  }
  async function play(){
    if(my!==DT.id)return;const gen=++st.gen;
    box.innerHTML='<div class="pcover" style="background-image:url('+esc(JSON.stringify(it.backdrop||it.poster||''))+')"><span class="pb" style="animation:spin 1s linear infinite;background:none;box-shadow:none;border:4px solid rgba(255,255,255,.2);border-top-color:var(--amber)"></span></div><div class="note" style="margin-top:12px">Checking servers — this can take a few seconds…</div>';
    try{
      const list=await fetchServers();if(my!==DT.id||gen!==st.gen)return;
      if(!list.length)throw new Error('No servers were found for this episode');
      const n=nextEp(),p=lib.pos(it),same=p&&(type==='movie'||(p.se===st.se&&p.ep===st.ep));
      mountPlayer(box,list,{title:it.title+(type==='series'?' · S'+st.se+' E'+st.ep:''),sub:'Anime',poster:it.poster,resume:same?p.t:0,
        onProgress:(t,dd)=>lib.save(it,t,dd,{se:st.se,ep:st.ep}),next:n?()=>{st.se=n.se;st.ep=n.ep;epUI&&epUI();play()}:null,onEnded:()=>{if(n){st.se=n.se;st.ep=n.ep;epUI&&epUI();play()}}});
      box.scrollIntoView({behavior:'smooth',block:'center'});
    }catch(e){box.innerHTML=cover(it.backdrop||it.poster,it.title)+'<div class="note warn" style="margin-top:12px">'+esc(e.message)+' — <a href="#" data-again style="text-decoration:underline">try again</a></div>';$('[data-again]',box).onclick=ev=>{ev.preventDefault();play()};$('#pcv',box).onclick=play}
  }
  $('#pcv').onclick=play;$('[data-playbtn]','#dcontent').onclick=play;
  let epUI=null;
  const defs=[{k:'ov',l:'Overview',fn:c=>{c.innerHTML='<div class="panel"><p class="dd">'+esc(it.desc||'No description available.')+'</p></div>'}}];
  if(type==='series')defs.push({k:'ep',l:'Episodes',fn:c=>{
    epUI=()=>{c.innerHTML=(seasonKeys.length>1?'<div class="sel">'+seasonKeys.map(k=>'<button class="chip'+(+k===st.se?' on':'')+'" data-se="'+k+'">Season '+k+'</button>').join('')+'</div>':'')+
      '<div class="eplist">'+curEps().map(e=>'<button class="epi'+(e.episode===st.ep?' on':'')+'" data-ep="'+e.episode+'"><b>'+e.episode+'</b><span>'+esc(e.title||('Episode '+e.episode))+'</span>'+ico('play')+'</button>').join('')+'</div>'};epUI();
    c.onclick=e=>{const s=e.target.closest('[data-se]'),p=e.target.closest('[data-ep]');if(s){st.se=+s.dataset.se;st.ep=(curEps()[0]&&curEps()[0].episode)||1;epUI()}else if(p){st.ep=+p.dataset.ep;epUI();play()}};
  }});
  defs.push({k:'dl',l:'Download',fn:async c=>{
    c.innerHTML='<div class="note">Download links for '+(type==='series'?'<b>S'+st.se+' E'+st.ep+'</b>':'this title')+'. Embedded servers (iframe) can’t be saved directly — use the ones marked MP4 / HLS.</div><div class="dl" id="dll">'+'<div class="sk" style="height:64px"></div>'.repeat(2)+'</div>';
    try{
      const list=await fetchServers();if(my!==DT.id)return;
      const rows=list.filter(s=>!s.dead);
      $('#dll',c).innerHTML=rows.map(s=>s.kind==='iframe'?'<div class="dli"><div class="inf"><b>'+esc(s.label)+'</b><small>Embedded player'+(s.audio?' • '+esc(s.audio):'')+'</small></div><div class="ba"><a class="btn sm ghost" target="_blank" rel="noopener" href="'+esc(s.url)+'">'+ico('ext')+'Open</a></div></div>':linkRow({label:s.label,url:s.url,kind:s.kind,note:(s.audio||'')+(s.kind==='hls'?' • stream playlist':'')})).join('')||'<div class="note warn">No usable servers right now.</div>';
    }catch(e){$('#dll',c).innerHTML='<div class="note warn">'+esc(e.message)+'</div>'}
  }});
  defs.push({k:'tr',l:'Trailer',fn:async c=>{
    c.innerHTML='<div class="sk" style="aspect-ratio:16/9"></div>';
    try{const j=await api('/ha/trailer?title='+enc(it.title));const t=D(j)||{};const u=pick(t,'url','embedUrl','trailerUrl','link')||(t.videoId?'https://www.youtube.com/embed/'+t.videoId:'')||(t.id&&typeof t.id==='string'&&t.id.length===11?'https://www.youtube.com/embed/'+t.id:'');
      c.innerHTML=u?'<div class="pl if" style="aspect-ratio:16/9"><iframe src="'+esc(String(u).replace('watch?v=','embed/'))+'" allowfullscreen allow="autoplay; encrypted-media; picture-in-picture" referrerpolicy="no-referrer" style="position:absolute;inset:0;width:100%;height:100%;border:0"></iframe></div>':'<div class="note">No trailer found.</div>'}
    catch(e){c.innerHTML='<div class="note warn">'+esc(e.message)+'</div>'}
  }});
  tabsUI($('#tabsbox'),defs,q.get('dl')?'dl':null);
  const g=String(it.genre||'').split(',')[0].trim();
  if(g)api('/ha/search?q='+enc(g)).then(j=>{if(my!==DT.id)return;const items=(D(j).items||[]).map(x=>haItem(x,x.kind)).filter(x=>x&&x.id!==it.id).slice(0,20);$('#rel').innerHTML=row('More like this',items)}).catch(()=>{});
  if(q.get('play'))play();
}

/* ===== 4K Hub detail (every link goes through the tools API) ===== */
const RES=new Map();
const hostOf=u=>{try{return new URL(u).hostname.replace(/^www\./,'')}catch(e){return ''}};
const relQ=t=>(String(t||'').match(/2160p|4k|uhd|1080p|720p|480p/i)||[''])[0];
function toolsResolve(url){
  if(RES.has(url))return RES.get(url);
  const p=(async()=>{
    const out=[],seen=new Set();
    const add=(l,def)=>{if(!l||!l.url||seen.has(l.url)||l.kind==='error')return;seen.add(l.url);out.push({...l,label:l.label||def,kind:guessKind(l.url)})};
    const [a,b]=await Promise.allSettled([api('/tools/resolve?url='+enc(url),{ttl:600000,timeout:75000,retry:0}),api('/tools/mp4?url='+enc(url),{ttl:600000,timeout:75000,retry:0})]);
    if(a.status==='fulfilled')((D(a.value)||{}).links||[]).forEach(l=>add(l,'Link'));
    if(b.status==='fulfilled')((D(b.value)||{}).links||[]).forEach(l=>add(l,'MP4'));
    if(guessKind(url)!=='file')add({url,label:'Direct'},'Direct');
    if(!out.length)throw new Error('The tools API could not extract a file from this link');
    out.sort((x,y)=>(x.kind==='mp4'?0:1)-(y.kind==='mp4'?0:1));
    return out;
  })();
  RES.set(url,p);p.catch(()=>RES.delete(url));return p;
}
function fkRow(x){
  const q=relQ((x.release||'')+' '+(x.label||''));
  return'<div class="dli fkr" data-u="'+esc(x.url)+'" data-t="'+esc(x.label||'Link')+'"><div class="inf"><b>'+esc(x.label||'Link')+(q?' <span class="tag k4">'+esc(q.toUpperCase())+'</span>':'')+'</b><small>'+esc(String(x.release||'').slice(0,110)||hostOf(x.url))+'</small></div>'+
  '<div class="ba"><button class="btn sm pri" data-fk="stream">'+ico('play')+'Stream</button><button class="btn sm ghost" data-fk="dl">'+ico('dl')+'Download</button></div><div class="fkout" style="flex-basis:100%"></div></div>';
}
async function dtFK(id,q){
  const my=++DT.id,seen=SEEN.get('fk:'+id);
  showDetail(topBar()+'<div class="dbody"><div class="sk" style="height:34px;width:60%;margin:20px 0"></div><div class="note">Collecting links… this can take up to a minute for big releases.</div><div class="sk" style="height:90px;margin-top:12px"></div></div>',seen&&seen.poster);
  let d;
  try{const j=await api('/fk/stream?path='+enc(id)+'&resolve=true',{ttl:300000,timeout:100000,retry:0});d=D(j)||{}}
  catch(e){if(my===DT.id)detailErr(e.message,()=>dtFK(id,q));return}
  if(my!==DT.id)return;
  const it={provider:'fk',id:String(id),title:d.title||(seen&&seen.title)||'Untitled',poster:(seen&&seen.poster)||'',year:(seen&&seen.year)||'',genre:(seen&&seen.genre)||'',type:(seen&&seen.type)||'movie',desc:''};
  const rels=d.releases||[];
  const direct=(d.direct_streams||[]).filter(x=>x.url);
  const mirrors=[];rels.forEach(r=>(r.mirrors||[]).forEach(m=>{if(m.url)mirrors.push({label:m.label||hostOf(m.url),url:m.url,release:r.title})}));
  const cands=[];const sc=new Set();[...direct,...mirrors].forEach(x=>{if(!sc.has(x.url)){sc.add(x.url);cands.push(x)}});
  showDetail(detailShell(it,(cands.length?'<button class="btn pri" data-playbtn>'+ico('play')+'Play</button>':'')+'<a class="btn ghost" target="_blank" rel="noopener" href="'+esc(d.url||'#')+'">'+ico('ext')+'Source page</a>',
    '<div id="pbox" style="margin-top:22px;'+(cands.length?'':'display:none')+'">'+cover(it.poster,it.title)+'</div><div id="tabsbox"></div>'),it.poster);
  bindList(it);
  const box=$('#pbox');
  const playLinks=(L,title)=>{
    const src=L.map(l=>({kind:l.kind==='hls'?'hls':'mp4',url:l.url,label:String(l.label||'Link').slice(0,22),quality:(()=>{const qq=relQ(l.quality||l.label||l.url).toUpperCase();return qq&&!String(l.label||'').toUpperCase().includes(qq)?qq:''})(),size:l.size}));
    box.style.display='';mountPlayer(box,src,{title:it.title,sub:title||'4K Hub',poster:it.poster,onProgress:(t,dd)=>lib.save(it,t,dd)});box.scrollIntoView({behavior:'smooth',block:'center'});
  };
  async function playFirst(btn){
    if(btn)btn.classList.add('busy');
    let lastErr=null;
    for(const c of cands.slice(0,8)){
      if(my!==DT.id)return;
      try{const L=await toolsResolve(c.url);if(my!==DT.id)return;playLinks(L,c.label);if(btn)btn.classList.remove('busy');return}catch(e){lastErr=e}
    }
    if(btn)btn.classList.remove('busy');
    toast((lastErr&&lastErr.message)||'No playable link found');
  }
  const pc=$('#pcv');if(pc)pc.onclick=()=>playFirst(null);const pb=$('[data-playbtn]','#dcontent');if(pb)pb.onclick=()=>playFirst(pb);
  const defs=[{k:'dl',l:'Direct links ('+direct.length+')',fn:c=>{
    c.innerHTML=(direct.length?'<div class="note">Every link is run through the tools API first, so Stream and Download get the real file link. MKV files may not play in the browser — use “Open in app”.</div><div class="dl">'+direct.map(fkRow).join('')+'</div>':'<div class="note warn">No direct links on this page — open the Mirrors tab.</div>');
  }},{k:'mi',l:'Mirrors ('+mirrors.length+')',fn:c=>{
    const by={};mirrors.forEach(m=>{(by[m.release||'Release']=by[m.release||'Release']||[]).push(m)});
    c.innerHTML=Object.keys(by).map(k=>'<div class="panel" style="margin-bottom:12px"><b style="word-break:break-word;display:block;margin-bottom:10px">'+esc(k)+'</b><div class="dl">'+by[k].map(fkRow).join('')+'</div></div>').join('')||'<div class="note warn">No mirrors found.</div>';
  }}];
  const tabHost=$('#tabsbox');
  tabsUI(tabHost,defs,direct.length||!mirrors.length?'dl':'mi');
  tabHost.addEventListener('click',async e=>{
    const b=e.target.closest('[data-fk]');if(!b)return;
    const row=b.closest('.fkr'),u=row.dataset.u,out=$('.fkout',row),mode=b.dataset.fk;
    b.classList.add('busy');out.innerHTML='';
    try{
      const L=await toolsResolve(u);if(my!==DT.id)return;
      if(mode==='stream'){playLinks(L,row.dataset.t);out.innerHTML=''}
      else out.innerHTML='<div class="note" style="margin:0">Resolved '+L.length+' link'+(L.length>1?'s':'')+' — tap Download on the one you want.</div>'+L.map(l=>linkRow({label:l.label||'File',url:l.url,kind:l.kind==='file'?'file':l.kind,size:l.size,quality:l.quality,note:l.filename||''})).join('');
    }catch(err){out.innerHTML='<div class="note warn" style="margin:0">'+esc(err.message)+' — <a target="_blank" rel="noopener" href="'+esc(u)+'" style="text-decoration:underline">open the page manually</a></div>'}
    finally{b.classList.remove('busy')}
  });
}

/* ===== Drama detail (metadata + where to watch) ===== */
async function dtDR(id,content,q){
  const my=++DT.id,seen=SEEN.get('dr:'+id);
  showDetail(topBar()+'<div class="dbody"><div class="sk" style="height:34px;width:60%;margin:20px 0"></div><div class="sk" style="height:120px"></div></div>',seen&&seen.poster);
  let d={};
  try{d=D(await api('/dr/detail?id='+enc(id)+'&content='+enc(content)))||{}}catch(e){}
  if(my!==DT.id)return;
  const it={provider:'dr',id:String(id),content,title:d.title||(seen&&seen.title)||'Untitled',poster:d.poster||(seen&&seen.poster)||'',year:d.year||(seen&&seen.year)||'',genre:(seen&&seen.genre)||'',type:content==='series'?'series':'movie',desc:''};
  showDetail(detailShell(it,'','<div class="note">Dramachi lists titles but doesn’t host video. Below is everything we can find for “'+esc(it.title)+'” on the other providers.</div><div id="wh">'+skelRow()+'</div>'),it.poster);
  bindList(it);
  const qq=it.title.replace(/\(.*?\)/g,'').trim();
  const [a,b,c]=await Promise.allSettled([api('/mb/search?q='+enc(qq)),api('/fk/search?q='+enc(qq)),api('/ha/search?q='+enc(qq))]);
  if(my!==DT.id)return;let h='';
  if(a.status==='fulfilled')h+=row('Stream on MovieBox',(D(a.value).items||[]).map(mbSubj).filter(Boolean));
  if(b.status==='fulfilled')h+=row('Download in 4K Hub',(D(b.value)||[]).map(fkItem).filter(Boolean));
  if(c.status==='fulfilled')h+=row('On Anime',(D(c.value).items||[]).map(x=>haItem(x,x.kind)).filter(Boolean));
  $('#wh').innerHTML=h||emptyBox('Not found elsewhere','No matching titles on the other providers yet.');
}

/* ===== Theatre (standalone player overlay) ===== */
function openTheatre(o){
  const t=$('#theatre');t.classList.add('open');document.body.classList.add('lock');
  $('#tcontent').innerHTML='<div style="display:flex;align-items:center;gap:10px;margin-bottom:12px"><button class="ibtn" data-tclose aria-label="Close">'+ico('x')+'</button><b style="font:600 15px var(--fd)">'+esc(o.title||'')+'</b></div><div id="tbox"></div>'+(o.extra||'');
  const killer=DT.pl;DT.pl=null;if(killer)try{killer.destroy()}catch(e){}
  window._tpl=null;
  const box=$('#tbox');box.innerHTML='<div id="plh"></div><div class="srcs" id="srcs"></div>';
  const srcs=$('#srcs',box);
  const pl=new Player($('#plh',box),{onChange:(i,l)=>{srcs.innerHTML=srcChips(l,i)}});
  window._tpl=pl;pl.setSources(o.sources,{title:o.title,sub:o.sub||'StreamHub',poster:o.poster,live:!!o.live});
  srcs.onclick=e=>{const c=e.target.closest('[data-src]');if(c){const i=+c.dataset.src;pl.sources[i].dead=false;pl.sources[i].proxied=false;pl.play(i,true)}};
  $('[data-tclose]',t).onclick=closeTheatre;
}
function closeTheatre(){
  const t=$('#theatre');if(!t.classList.contains('open'))return;
  if(window._tpl){try{window._tpl.destroy()}catch(e){}window._tpl=null}
  t.classList.remove('open');$('#tcontent').innerHTML='';
  if(!$('#detail').classList.contains('open'))document.body.classList.remove('lock');
}
document.addEventListener('keydown',e=>{if(e.key==='Escape'){if($('#theatre').classList.contains('open'))closeTheatre();else if($('#detail').classList.contains('open')&&!document.fullscreenElement)goBack();else $('#sheet').classList.remove('open')}});

/* ===== router ===== */
const PAGES={home:pgHome,movies:()=>pgBrowse('movies'),series:()=>pgBrowse('series'),anime:pgAnime,'4k':pg4k,drama:pgDrama,live:pgLive,midnight:pgMidnight,downloads:pgDownloads,library:pgLibrary};
function route(){
  const raw=(location.hash||'#/home').slice(1);const [path,qs]=raw.split('?');
  const parts=path.split('/').filter(Boolean).map(s=>{try{return decodeURIComponent(s)}catch(e){return s}});
  const q=new URLSearchParams(qs||'');const k=parts[0]||'home';App.navCount++;
  closeTheatre();
  if(['mb','ha','hau','fk','dr'].includes(k)){
    if(!parts[1]){location.hash='#/home';return}
    if(!$('#view').children.length){App.pageKey=null;App.nav++;PAGES.home()} // direct link: render home behind
    if(k==='mb')dtMB(parts[1],q);else if(k==='ha')dtHA(parts[1],q,false);else if(k==='hau')dtHA(parts[1],q,true);else if(k==='fk')dtFK(parts[1],q);else dtDR(parts[1],parts[2]||'movies',q);
    return;
  }
  closeDetail();
  const key=k+(k==='search'?(q.get('q')?'?q='+q.get('q'):''):'');
  setActive(k==='search'?'':k);
  if(key===App.pageKey&&$('#view').children.length)return;
  App.pageKey=key;App.nav++;Hero.stop();
  window.scrollTo(0,0);
  if(k==='search')pgSearch(q.get('q')||'');
  else (PAGES[k]||PAGES.home)();
}
window.addEventListener('hashchange',route);
buildNav();
route();
let _splashDone=false;const hideSplash=()=>{if(_splashDone)return;_splashDone=true;$('#splash').classList.add('out');setTimeout(()=>$('#splash').remove(),900)};
setTimeout(hideSplash,1500);window.addEventListener('load',()=>setTimeout(hideSplash,900));
window.addEventListener('resize',debounce(()=>{const a=$('#dock a.on');if(a){const g=$('#dockglow');g.style.width=a.offsetWidth+'px';g.style.transform='translateX('+(a.offsetLeft-6)+'px)'}},150));
if('serviceWorker' in navigator){/* intentionally none: always-fresh API data */}

</script>
</body>
</html>
'''

@app.get("/", response_class=HTMLResponse, include_in_schema=False)
async def root():
    return HTMLResponse(SITE_HTML, headers={"Cache-Control": "no-cache"})

@app.get("/site", response_class=HTMLResponse, include_in_schema=False)
async def site():
    return HTMLResponse(SITE_HTML, headers={"Cache-Control": "no-cache"})
