# StreamHub API v7.2.0 — fixed MovieBox + 4K CDN chain + modern docs — creator: shawon
from __future__ import annotations

import asyncio, base64, hashlib, hmac, json, random, re, time, uuid
from typing import Any, Dict, List, Optional
from urllib.parse import parse_qsl, urljoin, urlparse, unquote

import httpx
from bs4 import BeautifulSoup
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse

CREATOR, VERSION = "shawon", "7.2.0"
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

def _hc_list(html):
    soup = BeautifulSoup(html, "html.parser"); items = []; seen = set()
    for a in soup.select("a[href]"):
        href = a.get("href") or ""
        m = re.search(r"/video/([^/]+\.html)", href)
        if not m: continue
        if "all-" in href: continue
        slug = m.group(1); vid_id = slug.rsplit(".", 1)[0]
        if vid_id in seen: continue
        seen.add(vid_id)
        title = (a.get("title") or "").strip() or " ".join(a.get_text().split())
        img = a.find("img")
        poster = None
        if img: poster = img.get("src") or img.get("data-src") or img.get("data-original")
        full = href if href.startswith("http") else urljoin(HC_BASE, href)
        items.append({"id": vid_id, "slug": slug, "title": title or vid_id, "url": full, "poster": poster})
    return items

def _hc_stream_from_html(html):
    sources = []
    for m in re.findall(r'https?://hls\.hentaicity\.com/[^"\'\s<>]+', html):
        sources.append({"src": m, "format": "hls", "cdn": "hls.hentaicity.com"})
    for m in re.findall(r'https?://cdn\d*\.hentaicity\.com/[^"\'\s<>]+\.mp4[^"\'\s<>]*', html):
        sources.append({"src": m, "format": "mp4", "cdn": "cdn.hentaicity.com"})
    seen = set(); out = []
    for s in sources:
        if s["src"] in seen: continue
        seen.add(s["src"]); out.append(s)
    return out

@app.get("/hc/recent", tags=["HentaiCity"])
async def hc_recent():
    async with _client() as client:
        r = await client.get(f"{HC_BASE}/videos/straight/all-recent.html",
            headers={"Referer": "https://www.google.com/", "User-Agent": UA})
        items = _hc_list(r.text)
    return ok(items, provider="hentaicity", level="private", count=len(items), endpoint="recent")

@app.get("/hc/popular", tags=["HentaiCity"])
async def hc_popular():
    async with _client() as client:
        r = await client.get(f"{HC_BASE}/videos/straight/all-popular.html",
            headers={"Referer": "https://www.google.com/", "User-Agent": UA})
        items = _hc_list(r.text)
    return ok(items, provider="hentaicity", level="private", count=len(items), endpoint="popular")

@app.get("/hc/search", tags=["HentaiCity"])
async def hc_search(q: str = Query(...)):
    async with _client() as client:
        r = await client.get(f"{HC_BASE}/search/", params={"q": q},
            headers={"Referer": HC_BASE + "/", "User-Agent": UA})
        items = _hc_list(r.text)
    return ok(items, provider="hentaicity", level="private", count=len(items), endpoint="search", query=q)

@app.get("/hc/watch", tags=["HentaiCity"])
async def hc_watch(id: Optional[str] = None, url: Optional[str] = None):
    if not id and not url:
        raise HTTPException(400, detail=fail("id or url required"))
    pages = []
    if url: pages.append(url)
    if id:
        pages += [f"{HC_BASE}/video/{id}.html", f"{HC_BASE}/click/1-1/video/{id}.html"]
    html = ""; final = pages[0]
    async with _client() as client:
        for page in pages:
            r = await client.get(page, headers={"Referer": HC_BASE + "/", "User-Agent": UA})
            html = r.text; final = str(r.url)
            if _hc_stream_from_html(html): break
    soup = BeautifulSoup(html, "html.parser")
    title_el = soup.select_one("h1") or soup.select_one("title")
    title = title_el.get_text(strip=True) if title_el else id
    sources = _hc_stream_from_html(html)
    return ok({"id": id, "title": title, "page": final, "sources": sources, "count": len(sources)},
        provider="hentaicity", level="private", endpoint="watch")

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
<title>StreamHub API</title>
<link rel="preconnect" href="https://fonts.googleapis.com"/>
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&family=JetBrains+Mono:wght@400;500&display=swap" rel="stylesheet"/>
<style>
:root{
  --bg:#09090b;--s1:#18181b;--s2:#27272a;--bd:#3f3f46;--tx:#fafafa;--mu:#a1a1aa;
  --ac:#8b5cf6;--ac2:#22d3ee;--ok:#34d399;--radius:12px;
}
*{box-sizing:border-box;margin:0;padding:0}
body{font-family:Inter,system-ui,sans-serif;background:var(--bg);color:var(--tx);min-height:100vh;line-height:1.5}
a{color:var(--ac2);text-decoration:none}
.layout{display:grid;grid-template-columns:260px 1fr;min-height:100vh}
@media(max-width:900px){.layout{grid-template-columns:1fr}.side{display:none}.mob{display:flex!important}}
.side{background:var(--s1);border-right:1px solid var(--bd);padding:20px 16px;position:sticky;top:0;height:100vh;overflow:auto}
.logo{display:flex;align-items:center;gap:10px;margin-bottom:24px;padding:0 8px}
.logo-mark{width:36px;height:36px;border-radius:10px;background:linear-gradient(135deg,#8b5cf6,#22d3ee);display:grid;place-items:center;font-weight:700;font-size:14px}
.logo h1{font-size:16px;font-weight:700}.logo span{font-size:11px;color:var(--mu)}
.nav-label{font-size:11px;font-weight:600;color:var(--mu);text-transform:uppercase;letter-spacing:.06em;padding:12px 8px 6px}
.nav a{display:block;padding:8px 10px;border-radius:8px;color:var(--mu);font-size:13px;font-weight:500;margin-bottom:2px}
.nav a:hover,.nav a.on{background:var(--s2);color:var(--tx)}
.main{padding:28px 32px 80px;max-width:920px}
.mob{display:none;gap:8px;flex-wrap:wrap;margin-bottom:16px}
.pill{font-size:12px;padding:6px 12px;border-radius:999px;background:var(--s1);border:1px solid var(--bd);color:var(--mu)}
.pill.v{color:#c4b5fd;border-color:#7c3aed55}
.hero{margin-bottom:28px}
.hero h2{font-size:28px;font-weight:700;letter-spacing:-.03em;margin-bottom:8px}
.hero p{color:var(--mu);font-size:15px;max-width:560px}
.stats{display:grid;grid-template-columns:repeat(4,1fr);gap:12px;margin:20px 0 28px}
@media(max-width:600px){.stats{grid-template-columns:1fr 1fr}.main{padding:20px 16px 60px}}
.stat{background:var(--s1);border:1px solid var(--bd);border-radius:var(--radius);padding:14px}
.stat b{font-size:20px;display:block}.stat span{font-size:12px;color:var(--mu)}
.search{display:flex;align-items:center;gap:10px;background:var(--s1);border:1px solid var(--bd);border-radius:10px;padding:10px 14px;margin-bottom:20px}
.search input{flex:1;border:0;outline:0;background:transparent;color:var(--tx);font:inherit;font-size:14px}
.ep{background:var(--s1);border:1px solid var(--bd);border-radius:var(--radius);margin-bottom:10px;overflow:hidden;transition:.15s}
.ep:hover{border-color:#8b5cf688}
.ep-head{display:flex;align-items:center;gap:12px;padding:14px 16px;cursor:pointer}
.method{font-family:'JetBrains Mono',monospace;font-size:11px;font-weight:600;padding:3px 8px;border-radius:6px;background:#22d3ee22;color:#22d3ee}
.ep-path{font-family:'JetBrains Mono',monospace;font-size:13px;font-weight:500;flex:1}
.ep-desc{font-size:13px;color:var(--mu);display:none}
@media(min-width:700px){.ep-desc{display:block;max-width:280px;text-align:right}}
.ep-body{display:none;padding:0 16px 16px;border-top:1px solid var(--bd)}
.ep.open .ep-body{display:block;padding-top:14px}
.row{display:flex;flex-wrap:wrap;gap:8px;margin-bottom:10px}
.field{flex:1;min-width:140px;background:var(--bg);border:1px solid var(--bd);border-radius:8px;padding:10px 12px;color:var(--tx);font:inherit;font-size:13px;font-family:'JetBrains Mono',monospace}
.btn{background:var(--ac);border:0;color:#fff;padding:10px 16px;border-radius:8px;font-weight:600;font-size:13px;cursor:pointer}
.btn:hover{filter:brightness(1.1)}
.ghost{background:transparent;border:1px solid var(--bd);color:var(--tx);padding:10px 12px;border-radius:8px;cursor:pointer;font-size:13px}
pre{background:#000;border:1px solid var(--bd);border-radius:8px;padding:12px;overflow:auto;max-height:380px;font-family:'JetBrains Mono',monospace;font-size:12px;line-height:1.55;color:#e4e4e7;white-space:pre-wrap}
.hint{font-size:12px;color:var(--mu);margin:8px 0}
.sec{font-size:13px;font-weight:600;color:var(--mu);text-transform:uppercase;letter-spacing:.05em;margin:28px 0 12px}
footer{margin-top:40px;padding-top:20px;border-top:1px solid var(--bd);font-size:13px;color:var(--mu);text-align:center}
</style>
</head>
<body>
<div class="layout">
<aside class="side">
  <div class="logo"><div class="logo-mark">SH</div><div><h1>StreamHub</h1><span>API · shawon</span></div></div>
  <div class="nav-label">Providers</div>
  <nav class="nav">
    <a href="#MovieBox" class="on">MovieBox</a>
    <a href="#4KHDHub">4KHDHub</a>
    <a href="#Tools">Tools / CDN</a>
    <a href="#Dramachi">Dramachi</a>
    <a href="#IPTV">IPTV</a>
    <a href="#HentaiCity">HentaiCity</a>
    <a href="#Aggregate">Aggregate</a>
  </nav>
  <div class="nav-label">Meta</div>
  <nav class="nav"><a href="/health">Health</a></nav>
</aside>
<main class="main">
  <div class="mob">
    <span class="pill v">v7.2.0</span>
    <span class="pill">creator: shawon</span>
  </div>
  <div class="hero">
    <h2>API Reference</h2>
    <p>MovieBox DASH/MP4 · 4K HubCloud R2 CDN · IPTV · Drama · HentaiCity HLS. Every response includes <code>creator: "shawon"</code>.</p>
  </div>
  <div class="stats">
    <div class="stat"><b id="n">0</b><span>Endpoints</span></div>
    <div class="stat"><b>6</b><span>Providers</span></div>
    <div class="stat"><b>CDN</b><span>R2 · DASH · HLS</span></div>
    <div class="stat"><b>JSON</b><span>Clean responses</span></div>
  </div>
  <div class="search"><span style="opacity:.4">/</span><input id="q" placeholder="Filter endpoints…" oninput="filt()"/></div>
  <div id="list"></div>
  <footer>StreamHub API · made by shawon · MovieBox-TUI compatible resolve chain</footer>
</main>
</div>
<script>
const API=location.origin;
const E=[
{g:'MovieBox',m:'GET',p:'/mb/search',d:'Search movies & series',t:'/mb/search?q=avatar'},
{g:'MovieBox',m:'GET',p:'/mb/play/{id}',d:'MP4 + DASH streams (with Cookie)',t:'/mb/play/1654274595068805784'},
{g:'MovieBox',m:'GET',p:'/mb/detail/{id}',d:'Title metadata',t:'/mb/detail/1654274595068805784'},
{g:'MovieBox',m:'GET',p:'/mb/resource/{id}',d:'Extra resource links',t:'/mb/resource/1654274595068805784'},
{g:'MovieBox',m:'GET',p:'/mb/home',d:'Home tabs',t:'/mb/home'},
{g:'MovieBox',m:'GET',p:'/mb/seasons/{id}',d:'Season list',t:'/mb/seasons/1654274595068805784'},
{g:'4KHDHub',m:'GET',p:'/fk/home',d:'Latest catalog',t:'/fk/home'},
{g:'4KHDHub',m:'GET',p:'/fk/search',d:'Search catalog',t:'/fk/search?q=avatar'},
{g:'4KHDHub',m:'GET',p:'/fk/detail',d:'Releases + mirrors',t:'/fk/detail?path=/hacksaw-ridge-movie-7809/'},
{g:'4KHDHub',m:'GET',p:'/fk/stream',d:'Resolve to R2/PixelDrain CDN',t:'/fk/stream?path=/hacksaw-ridge-movie-7809/&resolve=true'},
{g:'4KHDHub',m:'GET',p:'/fk/category/{slug}',d:'Category browse',t:'/fk/category/movies'},
{g:'Tools',m:'GET',p:'/tools/resolve',d:'HubCloud/GreenMotors → direct CDN',t:'/tools/resolve?url=https://hubcloud.ist/drive/1qg90m0nr2599rq'},
{g:'Tools',m:'GET',p:'/tools/pixeldrain',d:'PixelDrain download URL',t:'/tools/pixeldrain?id=GauktM6T'},
{g:'Dramachi',m:'GET',p:'/dr/search',d:'Drama search',t:'/dr/search?q=love'},
{g:'Dramachi',m:'GET',p:'/dr/home',d:'Drama home',t:'/dr/home'},
{g:'IPTV',m:'GET',p:'/iptv/channels',d:'Live TV channels',t:'/iptv/channels?limit=30'},
{g:'HentaiCity',m:'GET',p:'/hc/recent',d:'Recent videos',t:'/hc/recent'},
{g:'HentaiCity',m:'GET',p:'/hc/popular',d:'Popular',t:'/hc/popular'},
{g:'HentaiCity',m:'GET',p:'/hc/search',d:'Search',t:'/hc/search?q=anime'},
{g:'HentaiCity',m:'GET',p:'/hc/watch',d:'HLS CDN stream',t:'/hc/watch?id=weak-teacher-1-clumsy-busty-anime-teacher-rips-her-stockings-in-class-6IvrFW0nkUP'},
{g:'Aggregate',m:'GET',p:'/search',d:'Multi-provider search',t:'/search?q=batman'},
{g:'Meta',m:'GET',p:'/health',d:'Health check',t:'/health'},
];
let openIdx=-1;
function filt(){
  const q=(document.getElementById('q').value||'').toLowerCase();
  document.querySelectorAll('.ep').forEach(el=>{
    el.style.display=!q||(el.dataset.t||'').includes(q)?'':'none';
  });
}
function render(){
  document.getElementById('n').textContent=E.length;
  let html='', last='';
  E.forEach((e,i)=>{
    if(e.g!==last){html+=`<div class="sec" id="${e.g}">${e.g}</div>`;last=e.g}
    html+=`<div class="ep" data-t="${(e.g+' '+e.p+' '+e.d).toLowerCase()}" id="ep${i}">
      <div class="ep-head" onclick="toggle(${i})"><span class="method">${e.m}</span>
      <span class="ep-path">${e.p}</span><span class="ep-desc">${e.d}</span></div>
      <div class="ep-body">
        <p class="hint">${e.d}</p>
        <div class="row"><input class="field" id="u${i}" value="${API}${e.t}"/>
        <button class="btn" onclick="run(${i})">Try it</button>
        <button class="ghost" onclick="navigator.clipboard.writeText(document.getElementById('u${i}').value)">Copy</button></div>
        <pre id="o${i}">Ready</pre>
      </div></div>`;
  });
  document.getElementById('list').innerHTML=html;
}
function toggle(i){
  const el=document.getElementById('ep'+i);
  const was=el.classList.contains('open');
  document.querySelectorAll('.ep').forEach(e=>e.classList.remove('open'));
  if(!was) el.classList.add('open');
}
async function run(i){
  const u=document.getElementById('u'+i).value; const out=document.getElementById('o'+i);
  out.textContent='Loading…'; const t0=performance.now();
  try{
    const r=await fetch(u); const t=await r.text();
    let p=t; try{p=JSON.stringify(JSON.parse(t),null,2)}catch(_){}
    out.textContent=r.status+' · '+Math.round(performance.now()-t0)+'ms\\n\\n'+p.slice(0,14000);
  }catch(e){out.textContent=String(e)}
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

@app.get("/site", response_class=HTMLResponse, include_in_schema=False)
async def site():
    return HTMLResponse(DOCS_HTML)
