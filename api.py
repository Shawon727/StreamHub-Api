# StreamHub API v7.3.0 — fixed MovieBox + 4K CDN chain + modern docs — creator: shawon
from __future__ import annotations

import asyncio, base64, hashlib, hmac, json, random, re, time, uuid
from typing import Any, Dict, List, Optional
from urllib.parse import parse_qsl, urljoin, urlparse, unquote

import httpx
from bs4 import BeautifulSoup
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse

CREATOR, VERSION = "shawon", "7.3.0"
app = FastAPI(title="StreamHub API", version=VERSION, docs_url=None, redoc_url=None)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"

def ok(data=None, **extra):
    o = {"creator": CREATOR, "ok": True}
    if data is not None: o["data"] = data
    o.update(extra); return o

def fail(msg, **extra):
    o = {"creator": CREATOR, "ok": False, "error": msg}; o.update(extra); return o

def _client(timeout=25.0):
    return httpx.AsyncClient(
        timeout=httpx.Timeout(timeout, connect=10.0),
        follow_redirects=True,
        headers={"User-Agent": UA, "Accept": "*/*", "Accept-Language": "en-US,en;q=0.9"},
        limits=httpx.Limits(max_connections=40, max_keepalive_connections=20),
    )

# ========== MOVIEBOX ==========
MB_HOSTS = [
    "https://api6.aoneroom.com","https://api5.aoneroom.com","https://api4.aoneroom.com",
    "https://api4sg.aoneroom.com","https://api3.aoneroom.com","https://api6sg.aoneroom.com",
    "https://api.inmoviebox.com",
]
MB_SECRET = bytes([0xEF,0xA8,0x91,0x97,0x4E,0xEC,0xD3,0x14,0x8D,0xF6,0x3A,0xA6,0x11,0x60,0x2D,0xEF,0xD1,0x01,0x25,0x9B,0xA5,0x21,0x02,0x2C,0x57,0xAE,0x05,0x66,0xBD,0x8E])
_mb_token=None; _mb_token_exp=0.0; _mb_host_idx=0
_mb_device_id="".join(random.choice("0123456789abcdef") for _ in range(32))
_mb_gaid=str(uuid.uuid4())

def _md5_hex(b: bytes) -> str: return hashlib.md5(b).hexdigest()

def _mb_client_info():
    vc=50020119
    ci=json.dumps({"package_name":"com.community.oneroom","version_name":"4.0.01.0813.03","version_code":vc,"os":"android","os_version":"13","install_ch":"ps","device_id":_mb_device_id,"install_store":"ps","gaid":_mb_gaid,"brand":"Redmi","model":"23078RKD5C","system_language":"en","net":"NETWORK_WIFI","region":"US","timezone":"Asia/Kolkata","sp_code":"40401","X-Play-Mode":"2"},separators=(",",":"))
    ua=f"com.community.oneroom/{vc} (Linux; U; Android 13; en_US; 23078RKD5C; Build/TQ2A.230405.003; Cronet/135.0.7012.3)"
    return ua, ci

def _mb_sign(method, url, body=None):
    ts=int(time.time()*1000)
    p=urlparse(url); params=sorted(parse_qsl(p.query, keep_blank_values=True))
    qs="&".join(f"{k}={v}" for k,v in params); canon=p.path+(f"?{qs}" if qs else "")
    body_b=(body or "").encode(); bh=_md5_hex(body_b) if body_b else ""; bl=str(len(body_b)) if body_b else ""
    accept=ctype="application/json"
    canonical=f"{method.upper()}\n{accept}\n{ctype}\n{bl}\n{ts}\n{bh}\n{canon}"
    sig=base64.b64encode(hmac.new(MB_SECRET, canonical.encode(), hashlib.md5).digest()).decode()
    rev=str(ts)[::-1]; ua,ci=_mb_client_info()
    return {"Accept":accept,"Content-Type":ctype,"Connection":"keep-alive","User-Agent":ua,
        "x-client-token":f"{ts},{_md5_hex(rev.encode())}","x-tr-signature":f"{ts}|2|{sig}",
        "x-client-info":ci,"x-client-status":"0",
        "x-forwarded-for":f"103.241.{random.randint(1,254)}.{random.randint(1,254)}"}

def _mb_unwrap(payload):
    if isinstance(payload.get("data"), dict): return payload["data"]
    return payload

async def _mb_request(method, path, body=None, token=None):
    global _mb_host_idx
    last="hosts exhausted"
    async with _client(18) as client:
        for i in range(len(MB_HOSTS)):
            base=MB_HOSTS[(_mb_host_idx+i)%len(MB_HOSTS)]; url=base+path
            headers=_mb_sign(method,url,body)
            if token: headers["Authorization"]=f"Bearer {token}"
            try:
                r=await (client.get(url,headers=headers) if method.upper()=="GET" else client.post(url,content=body or "",headers=headers))
                if r.status_code in (403,406,429,500,502,503,504):
                    last=f"{base}->{r.status_code}"; continue
                if r.status_code>=400:
                    last=f"{base}->{r.status_code}:{r.text[:120]}"; continue
                _mb_host_idx=(_mb_host_idx+i)%len(MB_HOSTS)
                try: return r.json()
                except Exception: return {"raw":r.text[:2000]}
            except Exception as e:
                last=str(e); continue
    raise HTTPException(502, detail=fail("MovieBox upstream failed", detail=last))

async def _mb_session():
    global _mb_token, _mb_token_exp
    if _mb_token and time.time()<_mb_token_exp: return _mb_token
    data=await _mb_request("POST","/wefeed-mobile-bff/user-api/visitor-login","{}")
    token=_mb_unwrap(data).get("token") or data.get("token")
    if not token: raise HTTPException(502, detail=fail("MovieBox login failed", raw=data))
    _mb_token=str(token); _mb_token_exp=time.time()+3500; return _mb_token

def _dash_from_cookie(sign_cookie):
    if not sign_cookie: return None
    m=re.search(r"urlprefix=([A-Za-z0-9+/=]+)", sign_cookie)
    if not m: return None
    raw=m.group(1); pad=raw+"="*((4-len(raw)%4)%4)
    try: base=base64.b64decode(pad).decode("utf-8","ignore")
    except Exception: return None
    if not base.startswith("http"): return None
    cookie=sign_cookie if sign_cookie.startswith("Edge-Cache-Cookie=") else f"Edge-Cache-Cookie={sign_cookie}"
    return {"url":base.rstrip("/")+"/index.mpd","format":"DASH","kind":"dash","cookie":cookie,"base":base,
            "note":"Send Cookie header when playing"}

def _mb_streams(payload):
    streams=[]; data=_mb_unwrap(payload)
    for s in (data.get("streams") or data.get("list") or []):
        if not isinstance(s, dict): continue
        url=s.get("url") or s.get("playUrl") or s.get("streamUrl")
        cookie=s.get("signCookie") or s.get("cookie") or ""
        if url:
            streams.append({"id":s.get("id"),"url":url,"quality":s.get("resolutions") or s.get("quality"),
                "codec":s.get("codecName") or s.get("codec"),"format":s.get("format") or "MP4",
                "size":s.get("size"),"duration":s.get("duration"),"cookie":cookie or None,"kind":"mp4"})
        dash=_dash_from_cookie(cookie)
        if dash:
            dash["id"]=s.get("id"); dash["quality"]=s.get("resolutions"); dash["codec"]=s.get("codecName") or "h265"
            streams.append(dash)
    return streams

@app.get("/mb/home", tags=["MovieBox"])
async def mb_home(page: int = 1, tab_id: str = "1"):
    tok=await _mb_session()
    data=await _mb_request("GET", f"/wefeed-mobile-bff/tab-operating?page={page}&tabId={tab_id}&version=", token=tok)
    return ok(_mb_unwrap(data), provider="moviebox", level="primary", endpoint="home")

@app.get("/mb/search", tags=["MovieBox"])
async def mb_search(q: str = Query(..., min_length=1), page: int = 1):
    tok=await _mb_session()
    body=json.dumps({"keyword":q,"page":page,"perPage":20})
    data=await _mb_request("POST","/wefeed-mobile-bff/subject-api/search/v2", body, token=tok)
    inner=_mb_unwrap(data); items=[]
    for block in inner.get("results") or []:
        for s in block.get("subjects") or []:
            cover=s.get("cover")
            items.append({"subjectId":s.get("subjectId") or s.get("id"),"title":s.get("title"),
                "type":s.get("subjectType"),"year":(s.get("releaseDate") or "")[:4],"genre":s.get("genre"),
                "cover":cover.get("url") if isinstance(cover,dict) else cover,"imdb":s.get("imdbRating") or s.get("rating")})
    return ok({"items":items,"count":len(items),"pager":inner.get("pager")}, provider="moviebox", level="primary", endpoint="search", query=q)

@app.get("/mb/detail/{subject_id}", tags=["MovieBox"])
async def mb_detail(subject_id: str):
    tok=await _mb_session()
    data=await _mb_request("GET", f"/wefeed-mobile-bff/subject-api/get?subjectId={subject_id}", token=tok)
    return ok(_mb_unwrap(data), provider="moviebox", level="primary", endpoint="detail")

@app.get("/mb/seasons/{subject_id}", tags=["MovieBox"])
async def mb_seasons(subject_id: str):
    tok=await _mb_session()
    data=await _mb_request("GET", f"/wefeed-mobile-bff/subject-api/season-info?subjectId={subject_id}", token=tok)
    return ok(_mb_unwrap(data), provider="moviebox", level="primary", endpoint="seasons")

@app.get("/mb/play/{subject_id}", tags=["MovieBox"])
async def mb_play(subject_id: str, se: Optional[int] = None, ep: Optional[int] = None):
    tok=await _mb_session()
    if se is not None and ep is not None:
        path=f"/wefeed-mobile-bff/subject-api/play-info/v2?subjectId={subject_id}&se={se}&ep={ep}"
    else:
        path=f"/wefeed-mobile-bff/subject-api/play-info/v2?subjectId={subject_id}"
    data=await _mb_request("GET", path, token=tok)
    inner=_mb_unwrap(data); streams=_mb_streams(data)
    return ok({"subject_id":subject_id,"season":se,"episode":ep,"title":inner.get("title"),
        "streams":streams,"count":len(streams),"displayResolutions":inner.get("displayResolutions")},
        provider="moviebox", level="primary", endpoint="play",
        note="kind=dash needs Cookie header; kind=mp4 is direct URL")

@app.get("/mb/resource/{subject_id}", tags=["MovieBox"])
async def mb_resource(subject_id: str, se: Optional[int] = None, ep: Optional[int] = None, page: int = 1, per_page: int = 20):
    tok=await _mb_session()
    if se is not None and ep is not None:
        path=f"/wefeed-mobile-bff/subject-api/resource?subjectId={subject_id}&se={se}&ep={ep}&page={page}&perPage={per_page}"
    else:
        path=f"/wefeed-mobile-bff/subject-api/resource?subjectId={subject_id}&page={page}&perPage={per_page}"
    data=await _mb_request("GET", path, token=tok)
    inner=_mb_unwrap(data); links=[]
    for item in inner.get("list") or []:
        link=item.get("resourceLink") or item.get("url")
        if link: links.append({"title":item.get("title"),"url":link,"size":item.get("size"),
            "episode":item.get("episode"),"resourceId":item.get("resourceId")})
    return ok({"links":links,"count":len(links),"pager":inner.get("pager")}, provider="moviebox", level="primary", endpoint="resource")

# ========== 4KHDHub + HubCloud CDN (GreenMotors s('o') payload) ==========
FK_BASE = "https://4khdhub.one"

def _fk_cards(html):
    soup = BeautifulSoup(html, "html.parser"); items = []
    for a in soup.select("a.movie-card"):
        href = a.get("href") or ""
        title_el = a.select_one(".movie-card-title")
        title = (title_el.get_text(strip=True) if title_el else a.get_text(strip=True)) or ""
        if not title or not href: continue
        full = urljoin(FK_BASE, href)
        img = a.select_one("img"); poster = img.get("src") if img else None
        meta_el = a.select_one(".movie-card-meta"); meta = meta_el.get_text(" ", strip=True) if meta_el else ""
        year_m = re.search(r"(19|20)\d{2}", meta)
        items.append({"id": urlparse(full).path, "title": title, "url": full, "poster": poster,
            "meta": meta, "year": year_m.group(0) if year_m else None,
            "type": "series" if "-series-" in href else "movie"})
    return items

def _fk_downloads(html):
    soup = BeautifulSoup(html, "html.parser"); releases = []
    for item in soup.select(".download-item"):
        title_el = item.select_one(".file-title")
        title = title_el.get_text(" ", strip=True) if title_el else ""
        mirrors = []
        for a in item.select("a[href]"):
            href = a.get("href") or ""; label = " ".join(a.get_text().split()) or "mirror"
            if href.startswith("http"): mirrors.append({"label": label, "url": href})
        if title or mirrors:
            quality = None
            for q in ("2160p", "1080p", "720p", "480p", "4K", "UHD"):
                if q.lower() in title.lower(): quality = q; break
            releases.append({"title": title, "quality": quality, "mirrors": mirrors})
    return releases

def _rot13(s):
    out = []
    for c in s:
        if "a" <= c <= "m" or "A" <= c <= "M": out.append(chr(ord(c) + 13))
        elif "n" <= c <= "z" or "N" <= c <= "Z": out.append(chr(ord(c) - 13))
        else: out.append(c)
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
        if "pixeldrain." not in (p.hostname or ""): return None
        path = p.path
        if path.startswith("/u/"): fid = path[3:].strip("/")
        elif path.startswith("/api/file/"): fid = path[len("/api/file/"):].strip("/").split("?")[0]
        else: return None
        return f"https://{p.hostname}/api/file/{fid}?download" if fid else None
    except Exception:
        return None

def _extract_cdn_urls(html):
    """Pull direct CDN links from HubCloud resolver HTML."""
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
        # gamerxyt resolver button
        m = re.search(r'https://gamerxyt\.com/hubcloud\.php[^"\']+', html)
        resolver = m.group(0) if m else None
        if not resolver:
            soup = BeautifulSoup(html, "html.parser")
            for a in soup.select("a[href*='gamerxyt'], a[href*='hubcloud.php'], a#download, a.btn"):
                href = a.get("href") or ""
                if href.startswith("https://") and ("gamerxyt" in href or "download" in href.lower()):
                    resolver = href; break
        page = html
        if resolver:
            try:
                rr = await client.get(resolver, headers={"Referer": drive_url, "User-Agent": UA})
                page = rr.text
            except Exception:
                pass
        for u in _extract_cdn_urls(page):
            api = _pixeldrain_api(u) or u
            kind = "direct"
            if "r2.cloudflarestorage" in api or "googleusercontent" in api or "gpdl.hubcloud" in api:
                kind = "direct"
            elif "pixeldrain" in api:
                kind = "direct"
            results.append({"label": "CDN", "url": api, "kind": kind})
        # pixeldrain in scripts
        for prefix in ("https://pixeldrain.dev/u/", "https://pixeldrain.com/u/", "https://pixeldrain.dev/api/file/", "https://pixeldrain.com/api/file/"):
            idx = 0
            while True:
                pos = page.find(prefix, idx)
                if pos < 0: break
                end = pos
                while end < len(page) and page[end] not in "\"' \t\n\r<>\\": end += 1
                api = _pixeldrain_api(page[pos:end])
                if api and not any(x["url"] == api for x in results):
                    results.append({"label": "PixelDrain", "url": api, "kind": "direct"})
                idx = end
    # dedupe
    seen = set(); out = []
    for x in results:
        if x["url"] in seen: continue
        seen.add(x["url"]); out.append(x)
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
    """New GreenMotors flow: page embeds s('o','PAYLOAD') then redirects."""
    async with _client(30) as client:
        r = await client.get(gm_url, headers={"Referer": "https://4khdhub.one/", "Accept": "text/html", "User-Agent": UA})
        html = r.text
        # Extract localStorage payload: s('o','....', timeout)
        m = re.search(r"s\(\s*['\"]o['\"]\s*,\s*['\"]([^'\"]+)['\"]", html)
        target = None
        if m:
            target = _decode_gm_payload(m.group(1))
        if not target:
            for mm in re.findall(r"https?://[^\s\"']+hubcloud[^\s\"']+", html):
                target = mm; break
        if not target:
            if "Failed to decode" in html or len(html) < 80:
                return [{"label": "GreenMotors", "url": gm_url, "kind": "intermediate",
                         "note": "Could not decode gateway on this IP"}]
            return [{"label": "GreenMotors", "url": gm_url, "kind": "intermediate"}]
        if "hubcloud." in target:
            return await _resolve_hubcloud(target)
        if "hubdrive." in target:
            return await _resolve_hubdrive(target)
        return [{"label": "Direct", "url": _pixeldrain_api(target) or target, "kind": "direct"}]


@app.get("/mb/movies", tags=["MovieBox"])
async def mb_movies(q: str = Query("a", min_length=1), page: int = 1):
    """Search limited to movies (subjectType=1)."""
    tok = await _mb_session()
    body = json.dumps({"keyword": q, "page": page, "perPage": 20, "subjectType": 1})
    data = await _mb_request("POST", "/wefeed-mobile-bff/subject-api/search/v2", body, token=tok)
    inner = _mb_unwrap(data)
    items = []
    for block in inner.get("results") or []:
        for s in block.get("subjects") or []:
            if s.get("subjectType") not in (1, None, "1"):
                # still include if API ignores filter
                pass
            cover = s.get("cover")
            items.append({
                "subjectId": s.get("subjectId") or s.get("id"),
                "title": s.get("title"),
                "type": s.get("subjectType"),
                "year": (s.get("releaseDate") or "")[:4],
                "genre": s.get("genre"),
                "cover": cover.get("url") if isinstance(cover, dict) else cover,
            })
    return ok({"items": items, "count": len(items)}, provider="moviebox", level="primary", endpoint="movies", query=q)

@app.get("/mb/series", tags=["MovieBox"])
async def mb_series(q: str = Query("a", min_length=1), page: int = 1):
    """Search limited to series (subjectType=2)."""
    tok = await _mb_session()
    body = json.dumps({"keyword": q, "page": page, "perPage": 20, "subjectType": 2})
    data = await _mb_request("POST", "/wefeed-mobile-bff/subject-api/search/v2", body, token=tok)
    inner = _mb_unwrap(data)
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
            })
    return ok({"items": items, "count": len(items)}, provider="moviebox", level="primary", endpoint="series", query=q)

@app.get("/mb/tab/{tab_id}", tags=["MovieBox"])
async def mb_tab(tab_id: str, page: int = 1):
    """Homepage tabs: 1=home, 2=movies-ish, 3=series-ish, 4=charts (try 1-4)."""
    tok = await _mb_session()
    path = f"/wefeed-mobile-bff/tab-operating?page={page}&tabId={tab_id}&version="
    data = await _mb_request("GET", path, token=tok)
    return ok(_mb_unwrap(data), provider="moviebox", level="primary", endpoint="tab", tab_id=tab_id)

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
        r = await client.get(url); html = r.text
    soup = BeautifulSoup(html, "html.parser")
    h1 = soup.select_one("h1"); title = h1.get_text(strip=True) if h1 else None
    releases = _fk_downloads(html)
    return ok({"title": title, "url": url, "releases": releases, "release_count": len(releases)},
        provider="4khdhub", level="live", endpoint="detail")

@app.get("/fk/stream", tags=["4KHDHub"])
async def fk_stream(path: str = Query(...), resolve: bool = Query(True)):
    url = path if path.startswith("http") else urljoin(FK_BASE, path)
    async with _client() as client:
        r = await client.get(url); html = r.text
    soup = BeautifulSoup(html, "html.parser")
    h1 = soup.select_one("h1"); title = h1.get_text(strip=True) if h1 else None
    releases = _fk_downloads(html)
    direct = []
    if resolve:
        tasks = []
        for rel in releases[:6]:
            for m in rel.get("mirrors") or []:
                mu = m.get("url") or ""
                lab = m.get("label")
                if len(tasks) >= 8: break
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
                    direct.append({"release": rtitle, "label": item.get("label") or label,
                        "url": item.get("url"), "kind": item.get("kind"), "note": item.get("note")})
            except Exception as e:
                direct.append({"release": rtitle, "label": label, "url": None, "kind": "error", "note": str(e)})
    seen = set(); uniq = []
    for d in direct:
        u = d.get("url")
        if not u or u in seen: continue
        seen.add(u); uniq.append(d)
    return ok({"title": title, "url": url, "releases": releases,
        "direct_streams": uniq, "direct_count": len(uniq)},
        provider="4khdhub", level="live", endpoint="stream")

@app.get("/tools/resolve", tags=["Tools"])
async def tools_resolve(url: str = Query(...)):
    u = url.strip(); low = u.lower()
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
    return ok({"id": id, "download_url": f"https://pixeldrain.com/api/file/{id}?download",
        "info_url": f"https://pixeldrain.com/api/file/{id}",
        "alt": f"https://pixeldrain.dev/api/file/{id}?download"},
        provider="tools", level="tool", endpoint="pixeldrain")

# ========== Dramachi / IPTV / HentaiCity / Aggregate ==========
DR_BASE = "https://api.nodeobjects.com"

@app.get("/dr/search", tags=["Dramachi"])
async def dr_search(q: str = Query(...), page: int = 1):
    async with _client() as client:
        r = await client.get(f"{DR_BASE}/", params={"interface": "search", "q": q, "filter": "all", "page": page})
        try: data = r.json()
        except Exception: data = {"raw": r.text[:1500]}
    return ok(data, provider="dramachi", level="live", endpoint="search", query=q)

@app.get("/dr/home", tags=["Dramachi"])
async def dr_home(page: int = 1):
    async with _client() as client:
        r = await client.get(f"{DR_BASE}/", params={"interface": "home", "page": page})
        try: data = r.json()
        except Exception: data = {"raw": r.text[:1500]}
    return ok(data, provider="dramachi", level="live", endpoint="home")

@app.get("/dr/detail", tags=["Dramachi"])
async def dr_detail(id: str = Query(...), content: str = Query("movies")):
    """Try common detail interfaces (upstream often returns null — no public stream API)."""
    results = {}
    async with _client() as client:
        for iface in ("detail", "info", "get", "movie", "play", "stream", "sources", "watch"):
            try:
                r = await client.get(f"{DR_BASE}/", params={"interface": iface, "id": id, "content": content})
                if r.text and r.text.strip() not in ("null", ""):
                    try:
                        results[iface] = r.json()
                    except Exception:
                        results[iface] = r.text[:500]
            except Exception as e:
                results[iface] = {"error": str(e)}
    has = {k: v for k, v in results.items() if v and v != "null"}
    return ok(
        {"id": id, "content": content, "available": has, "all": results},
        provider="dramachi",
        level="live",
        endpoint="detail",
        note="Dramachi public API exposes search/list; stream CDN is not published. Use search results thumb via static.nodeobjects.com/thumbnail/{thumb}",
    )

@app.get("/dr/thumb", tags=["Dramachi"])
async def dr_thumb(name: str = Query(..., description="thumb filename from search, e.g. godlovescaviar2012h.jpg")):
    url = f"https://static.nodeobjects.com/thumbnail/{name}"
    return ok({"thumb": name, "url": url}, provider="dramachi", level="live", endpoint="thumb")

IPTV_SOURCES = [
    "https://iptv-org.github.io/iptv/index.m3u",
    "https://iptv-org.github.io/iptv/countries/bd.m3u",
    "https://iptv-org.github.io/iptv/countries/in.m3u",
]

def _parse_m3u(text):
    channels = []; name = logo = group = None
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
        r = await client.get(src); ch = _parse_m3u(r.text)
    if q:
        ql = q.lower()
        ch = [c for c in ch if ql in c["name"].lower() or ql in (c.get("group") or "").lower()]
    return ok(ch[:limit], provider="iptv", level="live", count=min(len(ch), limit), total=len(ch), source=src, endpoint="channels")

HC_BASE = "https://www.hentaicity.com"

def _hc_cdn(folder: str, vid: str) -> dict:
    """Build all direct stream URLs from folder/id (no page fetch needed)."""
    base_flv = f"https://www.hentaicity.com/flv/{folder}/{vid}"
    hls = (
        f"https://hls.hentaicity.com/_hls/flv/{folder}/{vid}/"
        f",default,mobile,480p,720p,1080p,.mp4.urlset/master.m3u8"
    )
    return {
        "folder": folder,
        "video_id": vid,
        "hls": hls,
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
        poster = None
        folder = vid_num = None
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
            "id": vid_key,
            "slug": slug,
            "title": title or vid_key,
            "url": full,
            "poster": poster,
            "folder": folder,
            "numeric_id": vid_num,
        }
        if folder and vid_num:
            item["streams"] = _hc_cdn(folder, vid_num)
        items.append(item)
    return items

def _hc_stream_from_html(html: str) -> list:
    sources = []
    for m in re.findall(r'https?://hls\.hentaicity\.com/[^"\'\s<>]+', html):
        sources.append({"src": m, "format": "hls", "cdn": "hls.hentaicity.com"})
    for m in re.findall(r'https?://(?:www\.)?hentaicity\.com/flv/\d+/\d+/[^"\'\s<>]+\.mp4', html):
        sources.append({"src": m, "format": "mp4", "cdn": "hentaicity.com"})
    fm = re.search(r"/flv/(\d+)/(\d+)/", html) or re.search(r"/videos/(\d+)/(\d+)/", html)
    if fm:
        cdn = _hc_cdn(fm.group(1), fm.group(2))
        sources.insert(0, {"src": cdn["hls"], "format": "hls", "cdn": "hls.hentaicity.com"})
        for q, u in cdn["mp4"].items():
            sources.append({"src": u, "format": "mp4", "quality": q, "cdn": "hentaicity.com"})
    seen = set(); out = []
    for s in sources:
        if s["src"] in seen:
            continue
        seen.add(s["src"]); out.append(s)
    return out

@app.get("/hc/recent", tags=["HentaiCity"])
async def hc_recent():
    async with _client() as client:
        r = await client.get(
            f"{HC_BASE}/videos/straight/all-recent.html",
            headers={"Referer": "https://www.google.com/", "User-Agent": UA},
        )
        items = _hc_list(r.text)
    return ok(items, provider="hentaicity", level="private", count=len(items), endpoint="recent")

@app.get("/hc/popular", tags=["HentaiCity"])
async def hc_popular():
    async with _client() as client:
        r = await client.get(
            f"{HC_BASE}/videos/straight/all-popular.html",
            headers={"Referer": "https://www.google.com/", "User-Agent": UA},
        )
        items = _hc_list(r.text)
    return ok(items, provider="hentaicity", level="private", count=len(items), endpoint="popular")

@app.get("/hc/search", tags=["HentaiCity"])
async def hc_search(q: str = Query(...)):
    async with _client() as client:
        # primary search path from research
        for path, params in [
            (f"{HC_BASE}/search/video/{q}", None),
            (f"{HC_BASE}/search/", {"q": q}),
        ]:
            r = await client.get(path, params=params, headers={"Referer": HC_BASE + "/", "User-Agent": UA})
            items = _hc_list(r.text)
            if items:
                break
    return ok(items, provider="hentaicity", level="private", count=len(items), endpoint="search", query=q)

@app.get("/hc/watch", tags=["HentaiCity"])
async def hc_watch(
    id: Optional[str] = None,
    url: Optional[str] = None,
    folder: Optional[str] = None,
    vid: Optional[str] = Query(None, description="numeric video id inside folder"),
):
    """Resolve HLS + all MP4 qualities. Prefer folder+vid if known (works even when HTML is blocked)."""
    if folder and vid:
        cdn = _hc_cdn(folder, vid)
        sources = [{"src": cdn["hls"], "format": "hls"}] + [
            {"src": u, "format": "mp4", "quality": q} for q, u in cdn["mp4"].items()
        ]
        return ok(
            {"id": id, "folder": folder, "vid": vid, "streams": cdn, "sources": sources, "count": len(sources)},
            provider="hentaicity", level="private", endpoint="watch",
        )
    if not id and not url:
        raise HTTPException(400, detail=fail("Provide id, url, or folder+vid"))
    pages = []
    if url:
        pages.append(url)
    if id:
        pages += [
            f"{HC_BASE}/video/{id}.html",
            f"{HC_BASE}/click/1-1/video/{id}.html",
        ]
    html = ""
    final = pages[0]
    async with _client() as client:
        for page in pages:
            r = await client.get(page, headers={"Referer": HC_BASE + "/", "User-Agent": UA})
            html = r.text
            final = str(r.url)
            if "defendonlineprivacy" in final or "defendonlineprivacy" in html:
                continue
            if _hc_stream_from_html(html) or re.search(r"/flv/(\d+)/(\d+)/", html):
                break
    soup = BeautifulSoup(html, "html.parser")
    title_el = soup.select_one("h1") or soup.select_one("title")
    title = title_el.get_text(strip=True) if title_el else id
    sources = _hc_stream_from_html(html)
    fm = re.search(r"/flv/(\d+)/(\d+)/", html) or re.search(r"/videos/(\d+)/(\d+)/", html)
    streams = _hc_cdn(fm.group(1), fm.group(2)) if fm else None
    if not sources and not streams:
        return ok(
            {"id": id, "title": title, "page": final, "sources": [], "count": 0,
             "hint": "HTML blocked on this IP. Use /hc/recent item.streams or /hc/watch?folder=&vid="},
            provider="hentaicity", level="private", endpoint="watch",
        )
    return ok(
        {"id": id, "title": title, "page": final, "sources": sources, "streams": streams, "count": len(sources)},
        provider="hentaicity", level="private", endpoint="watch",
    )

@app.get("/hc/cdn", tags=["HentaiCity"])
async def hc_cdn(folder: str = Query(...), vid: str = Query(...)):
    """Direct CDN builder — no scrape. folder+vid from /hc/recent item."""
    cdn = _hc_cdn(folder, vid)
    return ok(cdn, provider="hentaicity", level="private", endpoint="cdn")

@app.get("/search", tags=["Aggregate"])
async def aggregate_search(q: str = Query(..., min_length=1)):
    results = {}
    async def mb():
        try:
            tok = await _mb_session()
            body = json.dumps({"keyword": q, "page": 1, "perPage": 10})
            data = await _mb_request("POST", "/wefeed-mobile-bff/subject-api/search/v2", body, token=tok)
            results["moviebox"] = _mb_unwrap(data)
        except Exception as e: results["moviebox"] = {"error": str(e)}
    async def fk():
        try:
            async with _client() as client:
                r = await client.get(FK_BASE + "/", params={"s": q})
                results["4khdhub"] = _fk_cards(r.text)[:20]
        except Exception as e: results["4khdhub"] = {"error": str(e)}
    async def dr():
        try:
            async with _client() as client:
                r = await client.get(f"{DR_BASE}/", params={"interface": "search", "q": q, "filter": "all", "page": 1})
                results["dramachi"] = r.json()
        except Exception as e: results["dramachi"] = {"error": str(e)}
    await asyncio.gather(mb(), fk(), dr())
    return ok(results, provider="aggregate", level="primary", endpoint="search", query=q)

@app.get("/health", tags=["Meta"])
async def health():
    return ok({"version": VERSION, "providers": ["moviebox", "4khdhub", "hubcloud", "dramachi", "iptv", "hentaicity"]})

DOCS_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8"/>
<meta name="viewport" content="width=device-width,initial-scale=1,viewport-fit=cover"/>
<title>StreamHub API · shawon</title>
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&family=JetBrains+Mono:wght@400;500&display=swap" rel="stylesheet"/>
<style>
:root{--bg:#09090b;--s1:#141416;--s2:#1c1c1f;--bd:#2e2e33;--tx:#f4f4f5;--mu:#a1a1aa;--ac:#a78bfa;--cy:#22d3ee;--ok:#34d399}
*{box-sizing:border-box;margin:0;padding:0}
body{font-family:Inter,system-ui,sans-serif;background:var(--bg);color:var(--tx);min-height:100vh;
background-image:radial-gradient(ellipse 90% 60% at 10% -10%,rgba(167,139,250,.22),transparent),radial-gradient(ellipse 70% 50% at 100% 0%,rgba(34,211,238,.12),transparent)}
a{color:var(--cy)}.layout{display:grid;grid-template-columns:250px 1fr;min-height:100vh}
@media(max-width:860px){.layout{grid-template-columns:1fr}.side{position:relative;height:auto;border-right:0;border-bottom:1px solid var(--bd)}}
.side{background:rgba(20,20,22,.9);backdrop-filter:blur(12px);border-right:1px solid var(--bd);padding:18px 14px;position:sticky;top:0;height:100vh;overflow:auto}
.logo{display:flex;gap:10px;align-items:center;margin-bottom:20px;padding:4px 6px}
.mark{width:38px;height:38px;border-radius:11px;background:linear-gradient(135deg,#a78bfa,#22d3ee);display:grid;place-items:center;font-weight:700;font-size:13px;animation:pulse 3s ease infinite}
@keyframes pulse{0%,100%{box-shadow:0 0 0 0 rgba(167,139,250,.35)}50%{box-shadow:0 0 24px 4px rgba(167,139,250,.25)}}
.logo h1{font-size:15px;font-weight:700}.logo small{color:var(--mu);font-size:11px}
.nav-label{font-size:10px;font-weight:600;color:var(--mu);text-transform:uppercase;letter-spacing:.07em;padding:14px 8px 6px}
.nav a{display:block;padding:8px 10px;border-radius:8px;color:var(--mu);font-size:13px;font-weight:500;margin-bottom:2px;transition:.15s}
.nav a:hover{background:var(--s2);color:var(--tx);transform:translateX(3px)}
.main{padding:28px 28px 70px;max-width:900px}
.hero h2{font-size:26px;font-weight:700;letter-spacing:-.03em;margin-bottom:6px}
.hero p{color:var(--mu);font-size:14px;margin-bottom:18px}
.stats{display:grid;grid-template-columns:repeat(4,1fr);gap:10px;margin-bottom:20px}
@media(max-width:600px){.stats{grid-template-columns:1fr 1fr}.main{padding:18px 14px 50px}}
.stat{background:var(--s1);border:1px solid var(--bd);border-radius:12px;padding:12px 14px;transition:.2s}
.stat:hover{border-color:#a78bfa66;transform:translateY(-2px)}
.stat b{font-size:18px;display:block}.stat span{font-size:11px;color:var(--mu)}
.search{display:flex;gap:8px;align-items:center;background:var(--s1);border:1px solid var(--bd);border-radius:10px;padding:10px 12px;margin-bottom:16px;transition:.2s}
.search:focus-within{border-color:#a78bfa88;box-shadow:0 0 0 3px #a78bfa22}
.search input{flex:1;border:0;outline:0;background:transparent;color:var(--tx);font:inherit;font-size:14px}
.sec{font-size:12px;font-weight:600;color:var(--mu);text-transform:uppercase;letter-spacing:.06em;margin:22px 0 10px}
.ep{background:var(--s1);border:1px solid var(--bd);border-radius:12px;margin-bottom:8px;overflow:hidden;transition:.2s;animation:fadeUp .35s ease both}
.ep:hover{border-color:#a78bfa55}
@keyframes fadeUp{from{opacity:0;transform:translateY(8px)}to{opacity:1;transform:none}}
.ep-head{display:flex;align-items:center;gap:10px;padding:12px 14px;cursor:pointer;user-select:none}
.method{font-family:JetBrains Mono,monospace;font-size:10px;font-weight:600;padding:3px 7px;border-radius:5px;background:#22d3ee18;color:var(--cy)}
.path{font-family:JetBrains Mono,monospace;font-size:12.5px;font-weight:500;flex:1}
.chev{color:var(--mu);transition:.2s;font-size:12px}.ep.open .chev{transform:rotate(90deg)}
.ep-body{display:none;padding:0 14px 14px;border-top:1px solid var(--bd)}
.ep.open .ep-body{display:block;animation:fadeUp .25s ease}
.how{background:var(--bg);border:1px solid var(--bd);border-radius:8px;padding:10px 12px;margin:10px 0;font-size:12.5px;color:var(--mu);line-height:1.55}
.how b{color:var(--tx)}.how code{font-family:JetBrains Mono,monospace;font-size:11px;color:var(--ac);background:#a78bfa15;padding:1px 5px;border-radius:4px}
.row{display:flex;flex-wrap:wrap;gap:8px;margin:8px 0}
.field{flex:1;min-width:120px;background:var(--bg);border:1px solid var(--bd);border-radius:8px;padding:9px 11px;color:var(--tx);font:inherit;font-size:12px;font-family:JetBrains Mono,monospace}
.btn{background:linear-gradient(135deg,#8b5cf6,#7c3aed);border:0;color:#fff;padding:9px 14px;border-radius:8px;font-weight:600;font-size:12px;cursor:pointer;transition:.15s}
.btn:hover{filter:brightness(1.1);transform:translateY(-1px)}.ghost{background:transparent;border:1px solid var(--bd);color:var(--tx);padding:9px 12px;border-radius:8px;cursor:pointer;font-size:12px}
pre{background:#000;border:1px solid var(--bd);border-radius:8px;padding:11px;overflow:auto;max-height:360px;font-family:JetBrains Mono,monospace;font-size:11.5px;line-height:1.5;color:#e4e4e7;white-space:pre-wrap}
footer{margin-top:36px;padding-top:16px;border-top:1px solid var(--bd);font-size:12px;color:var(--mu);text-align:center}
</style></head>
<body>
<div class="layout">
<aside class="side">
  <div class="logo"><div class="mark">SH</div><div><h1>StreamHub</h1><small>v7.3.0 · shawon</small></div></div>
  <div class="nav-label">Providers</div>
  <nav class="nav">
    <a href="#MovieBox">MovieBox</a>
    <a href="#4KHDHub">4KHDHub</a>
    <a href="#Tools">Tools / CDN</a>
    <a href="#Dramachi">Dramachi</a>
    <a href="#IPTV">IPTV</a>
    <a href="#HentaiCity">HentaiCity</a>
    <a href="#Aggregate">Aggregate</a>
  </nav>
</aside>
<main class="main">
  <div class="hero">
    <h2>API Reference</h2>
    <p>Expand any endpoint → read how it works → edit URL → <b>Try it</b>. All JSON includes <code>creator: "shawon"</code>.</p>
  </div>
  <div class="stats">
    <div class="stat"><b id="n">0</b><span>Endpoints</span></div>
    <div class="stat"><b>6</b><span>Providers</span></div>
    <div class="stat"><b>CDN</b><span>R2 · DASH · HLS · MP4</span></div>
    <div class="stat"><b>JSON</b><span>Clean responses</span></div>
  </div>
  <div class="search"><span style="opacity:.35">/</span><input id="q" placeholder="Filter endpoints…" oninput="filt()"/></div>
  <div id="list"></div>
  <footer>StreamHub API · made by shawon</footer>
</main>
</div>
<script>
const API=location.origin;
const E=[
{g:'MovieBox',p:'/mb/search',d:'Search all titles',h:'Query <code>q</code>. Returns <code>items[].subjectId</code> for play/detail.',t:'/mb/search?q=avatar'},
{g:'MovieBox',p:'/mb/movies',d:'Search movies only',h:'subjectType=1 filter. Use any keyword <code>q</code>.',t:'/mb/movies?q=love'},
{g:'MovieBox',p:'/mb/series',d:'Search series only',h:'subjectType=2 filter.',t:'/mb/series?q=love'},
{g:'MovieBox',p:'/mb/play/{id}',d:'Stream MP4 + DASH',h:'Replace id with subjectId. Use <code>streams</code> where <code>kind=dash</code> and send <code>Cookie</code> header. Optional <code>?se=1&ep=1</code> for series.',t:'/mb/play/1654274595068805784'},
{g:'MovieBox',p:'/mb/detail/{id}',d:'Full metadata',h:'subjectId from search.',t:'/mb/detail/1654274595068805784'},
{g:'MovieBox',p:'/mb/resource/{id}',d:'Extra download links',h:'Alternate resourceLink list.',t:'/mb/resource/1654274595068805784'},
{g:'MovieBox',p:'/mb/seasons/{id}',d:'Season list',h:'Series only.',t:'/mb/seasons/1654274595068805784'},
{g:'MovieBox',p:'/mb/home',d:'Home feed',h:'tabId default 1. Or use /mb/tab/{1-4}.',t:'/mb/home'},
{g:'MovieBox',p:'/mb/tab/{tab_id}',d:'Tab feed 1–4',h:'Try tab_id 1,2,3,4 for different home shelves.',t:'/mb/tab/1'},
{g:'4KHDHub',p:'/fk/home',d:'Latest catalog',h:'Returns cards with <code>id</code> path for detail/stream.',t:'/fk/home'},
{g:'4KHDHub',p:'/fk/search',d:'Search 4K catalog',h:'<code>q</code> keyword.',t:'/fk/search?q=avatar'},
{g:'4KHDHub',p:'/fk/detail',d:'Releases + mirrors',h:'path=/slug-movie-123/. Shows GreenMotors mirrors.',t:'/fk/detail?path=/hacksaw-ridge-movie-7809/'},
{g:'4KHDHub',p:'/fk/stream',d:'Direct CDN resolve',h:'Same path + resolve=true → R2 / gpdl / googleusercontent URLs in <code>direct_streams</code>.',t:'/fk/stream?path=/hacksaw-ridge-movie-7809/&resolve=true'},
{g:'4KHDHub',p:'/fk/category/{slug}',d:'Category page',h:'slug: movies, series, netflix, anime…',t:'/fk/category/movies'},
{g:'Tools',p:'/tools/resolve',d:'HubCloud → CDN',h:'Pass hubcloud.ist/drive/… or greenmotors URL. Returns r2.cloudflarestorage direct links.',t:'/tools/resolve?url=https://hubcloud.ist/drive/1qg90m0nr2599rq'},
{g:'Tools',p:'/tools/pixeldrain',d:'PixelDrain download',h:'File id only → ?download URL.',t:'/tools/pixeldrain?id=GauktM6T'},
{g:'Dramachi',p:'/dr/search',d:'Drama search',h:'Returns list with thumb filename. No public stream API upstream.',t:'/dr/search?q=love'},
{g:'Dramachi',p:'/dr/home',d:'Drama home',h:'Home feed if provided by upstream.',t:'/dr/home'},
{g:'Dramachi',p:'/dr/detail',d:'Detail probe',h:'Tries multiple interfaces; often null. Use /dr/thumb for posters.',t:'/dr/detail?id=524&content=movies'},
{g:'Dramachi',p:'/dr/thumb',d:'Poster URL',h:'name = thumb from search (e.g. godlovescaviar2012h.jpg).',t:'/dr/thumb?name=godlovescaviar2012h.jpg'},
{g:'IPTV',p:'/iptv/channels',d:'Live TV M3U',h:'source=0 global, 1=BD, 2=IN. Optional q filter.',t:'/iptv/channels?limit=30'},
{g:'HentaiCity',p:'/hc/recent',d:'Recent + CDN map',h:'Each item may include <code>streams</code> (hls + all mp4) from poster folder/id.',t:'/hc/recent'},
{g:'HentaiCity',p:'/hc/popular',d:'Popular list',h:'Same shape as recent.',t:'/hc/popular'},
{g:'HentaiCity',p:'/hc/search',d:'Search videos',h:'q=keyword.',t:'/hc/search?q=anime'},
{g:'HentaiCity',p:'/hc/watch',d:'Watch / scrape',h:'id=slug or folder+vid. Prefer folder&vid if HTML blocked.',t:'/hc/watch?folder=0449&vid=38135'},
{g:'HentaiCity',p:'/hc/cdn',d:'CDN builder only',h:'No scrape. folder + vid → all quality URLs.',t:'/hc/cdn?folder=0267&vid=38191'},
{g:'Aggregate',p:'/search',d:'Multi search',h:'MovieBox + 4K + Drama in one call.',t:'/search?q=batman'},
{g:'Meta',p:'/health',d:'Health',h:'Version and provider list.',t:'/health'},
];
function filt(){const q=(document.getElementById('q').value||'').toLowerCase();
document.querySelectorAll('.ep').forEach(el=>{el.style.display=!q||(el.dataset.t||'').includes(q)?'':'none'})}
function render(){
 document.getElementById('n').textContent=E.length; let h='',last='';
 E.forEach((e,i)=>{
  if(e.g!==last){h+=`<div class="sec" id="${e.g}">${e.g}</div>`;last=e.g}
  h+=`<div class="ep" style="animation-delay:${(i%8)*0.04}s" data-t="${(e.g+' '+e.p+' '+e.d).toLowerCase()}" id="ep${i}">
   <div class="ep-head" onclick="tog(${i})"><span class="method">GET</span><span class="path">${e.p}</span><span class="chev">›</span></div>
   <div class="ep-body"><div class="how"><b>${e.d}</b><br/>${e.h}</div>
   <div class="row"><input class="field" id="u${i}" value="${API}${e.t}"/>
   <button class="btn" onclick="run(${i})">Try it</button>
   <button class="ghost" onclick="navigator.clipboard.writeText(document.getElementById('u${i}').value)">Copy</button></div>
   <pre id="o${i}">Ready — press Try it</pre></div></div>`;
 });
 document.getElementById('list').innerHTML=h;
}
function tog(i){const el=document.getElementById('ep'+i);const o=el.classList.contains('open');
 document.querySelectorAll('.ep').forEach(e=>e.classList.remove('open')); if(!o) el.classList.add('open')}
async function run(i){const u=document.getElementById('u'+i).value;const out=document.getElementById('o'+i);
 out.textContent='Loading…';const t0=performance.now();
 try{const r=await fetch(u);const t=await r.text();let p=t;try{p=JSON.stringify(JSON.parse(t),null,2)}catch(_){}
 out.textContent=r.status+' · '+Math.round(performance.now()-t0)+'ms\\n\\n'+p.slice(0,14000)}catch(e){out.textContent=String(e)}}
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
