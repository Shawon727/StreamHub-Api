# StreamHub API v8.2.3 — creator: shawon
from __future__ import annotations

import asyncio, base64, gzip, hashlib, hmac, json, random, re, time, uuid
from typing import Any, Dict, List, Optional
from urllib.parse import parse_qsl, urljoin, urlparse, quote

import httpx
from bs4 import BeautifulSoup
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, Response, StreamingResponse, PlainTextResponse

CREATOR, VERSION = "shawon", "8.2.3"
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


def _mb_dash_token(base: str, cookie: str) -> str:
    """b64url token for /px/{token}/… proxy."""
    payload = json.dumps({"b": base.rstrip("/"), "c": cookie or ""}, separators=(",", ":")).encode()
    return base64.urlsafe_b64encode(payload).decode().rstrip("=")

def _mb_proxy_mpd_url(base: str, cookie: str) -> str:
    tok = _mb_dash_token(base, cookie)
    return f"/px/{tok}/index.mpd"

def _xml_local(tag: str) -> str:
    if not tag:
        return ""
    return tag.split("}")[-1] if "}" in tag else tag

def _parse_mpd_reps(mpd_xml: str, base: str, cookie: str) -> list:
    """Parse DASH MPD → list of representations with cookie-free play_url (via /px proxy)."""
    import xml.etree.ElementTree as ET
    out = []
    try:
        root = ET.fromstring(mpd_xml)
    except Exception:
        return out
    base = (base or "").rstrip("/")
    tok = _mb_dash_token(base, cookie) if base else ""

    def abs_url(u: str) -> str:
        if not u:
            return ""
        if u.startswith("http"):
            return u
        if u.startswith("/"):
            # host from base
            try:
                from urllib.parse import urlparse as _up
                p = _up(base)
                return f"{p.scheme}://{p.netloc}{u}"
            except Exception:
                return base + u
        return base + "/" + u.lstrip("/")

    def proxy(u: str) -> str:
        if not u or not tok:
            return u
        # relative to base for /px
        if base and u.startswith(base):
            rel = u[len(base):].lstrip("/")
        else:
            rel = u
            if "://" in u:
                # full URL different host — still try path only
                try:
                    from urllib.parse import urlparse as _up
                    rel = _up(u).path.lstrip("/")
                except Exception:
                    rel = u
        return f"/px/{tok}/{rel}"

    for period in root.iter():
        if _xml_local(period.tag) != "Period":
            continue
        for aset in period:
            if _xml_local(aset.tag) != "AdaptationSet":
                continue
            content_type = aset.attrib.get("contentType") or aset.attrib.get("mimeType") or ""
            for rep in aset:
                if _xml_local(rep.tag) != "Representation":
                    continue
                rid = rep.attrib.get("id") or ""
                bw = rep.attrib.get("bandwidth")
                w = rep.attrib.get("width")
                h = rep.attrib.get("height")
                codecs = rep.attrib.get("codecs") or ""
                mime = rep.attrib.get("mimeType") or content_type or ""
                # BaseURL under rep or adaptation
                base_url = ""
                for child in list(rep) + list(aset):
                    if _xml_local(child.tag) == "BaseURL" and child.text:
                        base_url = abs_url(child.text.strip())
                        break
                # SegmentTemplate
                media_tmpl = init_tmpl = None
                timescale = None
                for child in list(rep) + list(aset):
                    if _xml_local(child.tag) == "SegmentTemplate":
                        media_tmpl = child.attrib.get("media")
                        init_tmpl = child.attrib.get("initialization")
                        timescale = child.attrib.get("timescale")
                        break
                quality = f"{h}p" if h else (f"{int(bw)//1000}k" if bw else rid or "auto")
                kind = "video" if ("video" in (mime or "").lower() or h) else ("audio" if "audio" in (mime or "").lower() else "stream")
                item = {
                    "id": rid,
                    "quality": quality,
                    "width": int(w) if w and str(w).isdigit() else None,
                    "height": int(h) if h and str(h).isdigit() else None,
                    "bandwidth": int(bw) if bw and str(bw).isdigit() else None,
                    "codecs": codecs,
                    "mime": mime,
                    "kind": kind,
                    "base_url": base_url or None,
                }
                if init_tmpl:
                    init_u = abs_url(init_tmpl.replace("$RepresentationID$", rid).replace("$Bandwidth$", str(bw or "")))
                    item["init_segment"] = init_u
                    item["init_play"] = proxy(init_u)
                if media_tmpl:
                    item["media_template"] = media_tmpl
                # Full MPD play via proxy — works without client cookie
                item["play_url"] = f"/px/{tok}/index.mpd" if tok else None
                item["play_absolute_hint"] = "Prefix with your API origin. dash.js / VLC open play_url — cookie injected by server."
                out.append(item)
    # de-dupe by id+bandwidth
    seen = set()
    uniq = []
    for x in out:
        k = (x.get("id"), x.get("bandwidth"), x.get("height"))
        if k in seen:
            continue
        seen.add(k)
        uniq.append(x)
    # video first higher quality
    uniq.sort(key=lambda x: (0 if x.get("kind") == "video" else 1, -(x.get("height") or 0), -(x.get("bandwidth") or 0)))
    return uniq

async def _mb_fetch_mpd(base: str, cookie: str) -> str:
    mpd_url = base.rstrip("/") + "/index.mpd"
    headers = {
        "User-Agent": UA,
        "Accept": "application/dash+xml,application/xml,*/*",
        "Referer": "https://www.moviebox.ph/",
    }
    if cookie:
        headers["Cookie"] = cookie if "Edge-Cache-Cookie" in cookie else f"Edge-Cache-Cookie={cookie}"
    async with _client(25.0) as client:
        r = await client.get(mpd_url, headers=headers)
        if r.status_code >= 400:
            raise HTTPException(502, detail=fail("MPD fetch failed", status=r.status_code, url=mpd_url))
        return r.text

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
    # Attach cookie-free play_url under each dash stream (extractor)
    enriched = []
    for s in streams:
        s2 = dict(s)
        if s2.get("kind") == "dash" and s2.get("base"):
            s2["play_url"] = _mb_proxy_mpd_url(s2["base"], s2.get("cookie") or "")
            s2["cookie_required_on_client"] = False
            s2["dash_extractor"] = {
                "play_url": s2["play_url"],
                "note": "Use play_url on API host — server injects Edge-Cache-Cookie. Client plays without cookie.",
            }
        elif s2.get("kind") == "mp4" or s2.get("url"):
            s2["play_url"] = s2.get("url")
            s2["cookie_required_on_client"] = False
        enriched.append(s2)
    return ok({
        "subject_id": subject_id, "season": se, "episode": ep, "title": inner.get("title"),
        "streams": enriched, "count": len(enriched), "displayResolutions": inner.get("displayResolutions"),
        "dash_extractor": [x for x in enriched if x.get("kind") == "dash"],
    }, provider="moviebox", level="primary", endpoint="play",
       note="dash: use play_url (proxied, no client cookie). mp4: direct url.")



@app.get("/mb/dash/extract", tags=["MovieBox"])
async def mb_dash_extract(
    subject_id: str = Query(..., description="MovieBox subjectId"),
    se: Optional[int] = None,
    ep: Optional[int] = None,
    mpd: Optional[str] = Query(None, description="Optional direct MPD URL"),
    cookie: Optional[str] = Query(None, description="Edge-Cache-Cookie if mpd given"),
    base: Optional[str] = Query(None, description="CDN base urlprefix decoded"),
):
    """
    DASH link extractor — parses MPD representations and returns **cookie-free play_url**
    (server-side /px proxy injects Edge-Cache-Cookie). Use play_url in dash.js / VLC.
    Also returns each quality track from the MPD.
    """
    streams_out = []
    title = None
    if subject_id and not mpd:
        tok = await _mb_session()
        if se is not None and ep is not None:
            path = f"/wefeed-mobile-bff/subject-api/play-info/v2?subjectId={subject_id}&se={se}&ep={ep}"
        else:
            path = f"/wefeed-mobile-bff/subject-api/play-info/v2?subjectId={subject_id}"
        data = await _mb_request("GET", path, token=tok)
        inner = _mb_unwrap(data)
        title = inner.get("title")
        for s in _mb_streams(data):
            if s.get("kind") != "dash":
                # still list mp4 as already-direct
                streams_out.append({
                    "source": "mp4",
                    "quality": s.get("quality"),
                    "url": s.get("url"),
                    "play_url": s.get("url"),
                    "kind": "mp4",
                    "codec": s.get("codec"),
                    "cookie_required": False,
                })
                continue
            b = s.get("base") or ""
            c = s.get("cookie") or ""
            try:
                xml = await _mb_fetch_mpd(b, c)
                reps = _parse_mpd_reps(xml, b, c)
            except Exception as e:
                reps = []
                xml_err = str(e)
            else:
                xml_err = None
            streams_out.append({
                "source": "dash",
                "quality": s.get("quality"),
                "codec": s.get("codec"),
                "mpd": s.get("url"),
                "base": b,
                "cookie_required_on_client": False,
                "play_url": _mb_proxy_mpd_url(b, c),
                "play_how": "Open play_url on THIS API host (relative /px/...). Server adds cookie. Client needs NO cookie.",
                "representations": reps,
                "representation_count": len(reps),
                "error": xml_err,
            })
    elif mpd or base:
        b = (base or "").rstrip("/")
        if not b and mpd:
            b = mpd.replace("/index.mpd", "").rstrip("/")
        c = cookie or ""
        if c and not c.startswith("Edge-Cache-Cookie"):
            c = f"Edge-Cache-Cookie={c}"
        xml = await _mb_fetch_mpd(b, c)
        reps = _parse_mpd_reps(xml, b, c)
        streams_out.append({
            "source": "dash",
            "mpd": b + "/index.mpd",
            "base": b,
            "play_url": _mb_proxy_mpd_url(b, c),
            "cookie_required_on_client": False,
            "representations": reps,
            "representation_count": len(reps),
        })
    else:
        return fail("subject_id or mpd/base required", provider="moviebox", endpoint="dash_extract")

    return ok({
        "subject_id": subject_id,
        "title": title,
        "dash_extractor": streams_out,
        "count": len(streams_out),
        "how": "Use dash_extractor[].play_url — full URL = https://YOUR-API-HOST + play_url. No client Cookie header needed.",
    }, provider="moviebox", level="primary", endpoint="dash_extract")

@app.get("/mb/dash/extract/{subject_id}", tags=["MovieBox"])
async def mb_dash_extract_path(subject_id: str, se: Optional[int] = None, ep: Optional[int] = None):
    return await mb_dash_extract(subject_id=subject_id, se=se, ep=ep)

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
HC_MAX_PAGE = 140  # site has all-recent-1 … all-recent-140

_HC_SEED = [
    {"folder": "0498", "vid": "38179", "id": "fuck-the-spire", "title": "Fuck the Spire - JOI game play"},
    {"folder": "0626", "vid": "38136", "id": "weak-teacher-2", "title": "Weak Teacher 2 (ecchi anime)"},
    {"folder": "0449", "vid": "38135", "id": "weak-teacher-1", "title": "Weak Teacher 1 (ecchi anime)"},
    {"folder": "0267", "vid": "38191", "id": "confession-4", "title": "Confession... 4 - Busty hentai teen"},
    {"folder": "0120", "vid": "38190", "id": "shrouded-in-mist-2", "title": "Shrouded in Mist 2"},
    {"folder": "0341", "vid": "38189", "id": "lonely-snow-widow-1", "title": "The Lonely Snow Widow and the Cursed Ring 1"},
    {"folder": "0616", "vid": "33668", "id": "my-sister-is-cute-1", "title": "My Sister is Cute 1"},
    {"folder": "0745", "vid": "32125", "id": "fandel-tales", "title": "Fandel Tales: The Cursed Prince"},
]

def _hc_cdn(folder: str, vid: str) -> dict:
    base_flv = f"https://www.hentaicity.com/flv/{folder}/{vid}"
    hls = (
        f"https://hls.hentaicity.com/_hls/flv/{folder}/{vid}/"
        f",default,mobile,480p,720p,1080p,.mp4.urlset/master.m3u8"
    )
    return {
        "folder": folder, "video_id": str(vid), "hls": hls,
        "mp4": {
            "mobile": f"{base_flv}/mobile.mp4",
            "default": f"{base_flv}/default.mp4",
            "480p": f"{base_flv}/480p.mp4",
            "720p": f"{base_flv}/720p.mp4",
            "1080p": f"{base_flv}/1080p.mp4",
        },
        "poster": f"https://cdn1.images.hentaicity.com/videos/{folder}/{vid}/main.jpg",
        "poster_hd": f"https://cdn1.images.hentaicity.com/videos/{folder}/{vid}/1080p.jpg",
        "thumb": f"https://cdn1.images.hentaicity.com/videos/{folder}/{vid}/main.jpg",
        "trailer": f"https://cdn1.hentaicity.com/{folder}/{vid}/trailer.mp4",
    }

def _hc_abs_img(src: Optional[str]) -> Optional[str]:
    if not src:
        return None
    s = src.strip()
    if s.startswith("data:"):
        return None
    if s.startswith("//"):
        s = "https:" + s
    if s.startswith("/"):
        s = HC_BASE + s
    return s

def _hc_clean_title(title: str, fallback: str) -> str:
    t = " ".join((title or "").split()).strip()
    junk = {"view", "play", "watch", "video", "more", "hd", "new"}
    if not t or t.lower() in junk or len(t) <= 2:
        t = fallback.replace("-", " ")
    t = re.sub(r"\s*\|\s*HentaiCity.*$", "", t, flags=re.I)
    return t[:160]

def _hc_list(html: str) -> list:
    soup = BeautifulSoup(html or "", "html.parser")
    items, seen = [], set()
    for a in soup.select("a[href*='/video/']"):
        href = a.get("href") or ""
        m = re.search(r"/video/([^/?#]+\.html)", href)
        if not m or "all-" in href:
            continue
        slug = m.group(1)
        vid_key = slug.rsplit(".", 1)[0]
        if vid_key in seen:
            continue
        seen.add(vid_key)
        img = a.find("img")
        if not img and a.parent:
            img = a.parent.find("img")
        poster = folder = vid_num = None
        if img:
            poster = _hc_abs_img(
                img.get("data-src")
                or img.get("data-original")
                or img.get("data-lazy-src")
                or img.get("data-srcset")
                or img.get("src")
            )
            # srcset first url
            if poster and " " in poster and "," in (img.get("data-srcset") or ""):
                poster = _hc_abs_img(poster.split(",")[0].strip().split(" ")[0])
            if poster:
                fm = re.search(r"/videos/(\d+)/(\d+)/", poster)
                if fm:
                    folder, vid_num = fm.group(1), fm.group(2)
        title = (a.get("title") or "").strip()
        if not title and img:
            title = (img.get("alt") or img.get("title") or "").strip()
        if not title:
            title = " ".join(a.get_text().split())
        title = _hc_clean_title(title, vid_key)
        full = href if href.startswith("http") else urljoin(HC_BASE, href)
        # Prefer deterministic CDN poster when folder/vid known (always works)
        streams = None
        if folder and vid_num:
            streams = _hc_cdn(folder, vid_num)
            poster = streams["poster"]
        item = {
            "id": vid_key,
            "slug": slug,
            "title": title,
            "url": full,
            "poster": poster,
            "thumbnail": poster,
            "folder": folder,
            "numeric_id": vid_num,
            "trailer": streams["trailer"] if streams else None,
            "streams": streams,
        }
        items.append(item)
    return items

def _hc_seed_items() -> list:
    out = []
    for s in _HC_SEED:
        streams = _hc_cdn(s["folder"], s["vid"])
        out.append({
            "id": s["id"], "title": s["title"], "folder": s["folder"], "numeric_id": s["vid"],
            "poster": streams["poster"], "thumbnail": streams["poster"], "trailer": streams["trailer"],
            "streams": streams, "seed": True, "url": f"{HC_BASE}/video/{s['id']}.html",
        })
    return out

def _hc_page_path(kind: str, page: int, tag: Optional[str] = None) -> str:
    """Build list URL. kind=recent|popular|category. page starts at 1."""
    page = max(1, min(int(page or 1), HC_MAX_PAGE))
    if kind == "popular":
        base = "all-popular"
    elif kind == "category" and tag:
        tag = re.sub(r"[^a-z0-9]", "", tag.lower()) or "cartoon"
        # sort embedded in tag path later
        base = tag
    else:
        base = "all-recent"
    if kind == "category" and tag:
        # caller passes full like cartoon-popular
        if page <= 1:
            return f"/videos/straight/{tag}.html"
        return f"/videos/straight/{tag}-{page}.html"
    if page <= 1:
        return f"/videos/straight/{base}.html"
    return f"/videos/straight/{base}-{page}.html"

async def _hc_get(path: str, params: dict = None) -> tuple:
    headers_list = [
        {"User-Agent": UA, "Referer": HC_BASE + "/", "Accept": "text/html,application/xhtml+xml", "Accept-Language": "en-US,en;q=0.9"},
        {"User-Agent": MOBILE_UA, "Referer": "https://www.google.com/", "Accept": "text/html"},
    ]
    async with _client(28.0) as client:
        for headers in headers_list:
            try:
                r = await client.get(urljoin(HC_BASE, path), params=params or {}, headers=headers)
                text = r.text or ""
                if "defendonlineprivacy" in str(r.url) or "defendonlineprivacy" in text[:1200]:
                    continue
                if len(text) < 400:
                    continue
                return text, False
            except Exception:
                continue
    return None, True

async def _hc_fetch_list(path: str) -> tuple:
    text, blocked = await _hc_get(path)
    if text:
        items = [i for i in _hc_list(text) if i.get("folder") and i.get("numeric_id")]
        if not items:
            items = _hc_list(text)
        if items:
            return items, False, "live", path
    return _hc_seed_items(), True, "blocked_or_empty_seed", path

async def _hc_fetch_pages(kind: str, page: int, pages: int = 1, tag: Optional[str] = None) -> tuple:
    """Fetch one or more consecutive pages and merge unique items."""
    pages = max(1, min(int(pages or 1), 10))
    page = max(1, int(page or 1))
    all_items, seen = [], set()
    blocked_any, note = False, "live"
    last_path = ""
    for i in range(pages):
        pnum = page + i
        if pnum > HC_MAX_PAGE:
            break
        if kind == "category" and tag:
            path = _hc_page_path("category", pnum, tag)
        else:
            path = _hc_page_path(kind, pnum)
        last_path = path
        items, blocked, n, _ = await _hc_fetch_list(path)
        blocked_any = blocked_any or blocked
        if blocked:
            note = n
        for it in items:
            k = (it.get("folder"), it.get("numeric_id") or it.get("id"))
            if k in seen:
                continue
            seen.add(k)
            it["page"] = pnum
            all_items.append(it)
    has_more = (page + pages - 1) < HC_MAX_PAGE and not blocked_any
    return all_items, blocked_any, note, last_path, has_more

@app.get("/hc/home", tags=["HentaiCity"])
async def hc_home(pages: int = Query(2, ge=1, le=5, description="Pages per shelf to merge")):
    recent, br, nr, _, mr = await _hc_fetch_pages("recent", 1, pages)
    popular, bp, np_, _, mp = await _hc_fetch_pages("popular", 1, pages)
    cartoon, bc, nc, _, mc = await _hc_fetch_pages("category", 1, 1, "cartoon-popular")
    return ok({
        "recent": recent,
        "popular": popular,
        "cartoon": cartoon,
        "sections": [
            {"title": "Most Recent", "items": recent, "kind": "recent"},
            {"title": "Most Popular", "items": popular, "kind": "popular"},
            {"title": "Cartoon", "items": cartoon, "kind": "category", "tag": "cartoon-popular"},
        ],
        "pagination": {"max_page": HC_MAX_PAGE, "recent_has_more": mr, "popular_has_more": mp},
    }, provider="hentaicity", level="private", endpoint="home", blocked=br or bp, note=nr if br else np_)

@app.get("/hc/recent", tags=["HentaiCity"])
async def hc_recent(
    page: int = Query(1, ge=1, le=HC_MAX_PAGE),
    pages: int = Query(1, ge=1, le=10, description="Merge N pages starting at page"),
):
    items, blocked, note, path, has_more = await _hc_fetch_pages("recent", page, pages)
    return ok({
        "items": items,
        "page": page,
        "pages_merged": pages,
        "next_page": page + pages if has_more else None,
        "has_more": has_more,
        "max_page": HC_MAX_PAGE,
        "path": path,
    }, provider="hentaicity", level="private", count=len(items), endpoint="recent", blocked=blocked, note=note)

@app.get("/hc/popular", tags=["HentaiCity"])
async def hc_popular(
    page: int = Query(1, ge=1, le=HC_MAX_PAGE),
    pages: int = Query(1, ge=1, le=10),
):
    items, blocked, note, path, has_more = await _hc_fetch_pages("popular", page, pages)
    return ok({
        "items": items,
        "page": page,
        "pages_merged": pages,
        "next_page": page + pages if has_more else None,
        "has_more": has_more,
        "max_page": HC_MAX_PAGE,
        "path": path,
    }, provider="hentaicity", level="private", count=len(items), endpoint="popular", blocked=blocked, note=note)

@app.get("/hc/category", tags=["HentaiCity"])
async def hc_category(
    tag: str = Query("cartoon", description="cartoon, 3d, anal, bigtits, babe, blowjob…"),
    sort: str = Query("popular", description="popular|recent"),
    page: int = Query(1, ge=1, le=HC_MAX_PAGE),
    pages: int = Query(1, ge=1, le=5),
):
    tag = re.sub(r"[^a-z0-9]", "", tag.lower()) or "cartoon"
    sort = "recent" if sort == "recent" else "popular"
    full = f"{tag}-{sort}"
    items, blocked, note, path, has_more = await _hc_fetch_pages("category", page, pages, full)
    return ok({
        "items": items, "tag": tag, "sort": sort, "page": page,
        "next_page": page + pages if has_more else None, "has_more": has_more, "max_page": HC_MAX_PAGE,
    }, provider="hentaicity", level="private", count=len(items), endpoint="category", blocked=blocked, note=note)

@app.get("/hc/categories", tags=["HentaiCity"])
async def hc_categories():
    tags = [
        "cartoon", "3d", "anal", "babe", "bigdick", "bigtits", "blowjob", "comics",
        "cumshot", "hardcore", "lesbian", "milf", "teen", "creampie", "hentai",
    ]
    return ok([
        {"tag": x, "popular": f"/hc/category?tag={x}&sort=popular", "recent": f"/hc/category?tag={x}&sort=recent"}
        for x in tags
    ], provider="hentaicity", endpoint="categories", count=len(tags))

@app.get("/hc/search", tags=["HentaiCity"])
async def hc_search(q: str = Query(..., min_length=1), page: int = Query(1, ge=1, le=20)):
    q = q.strip()
    text, blocked = await _hc_get("/customsearch.php", {
        "search": q, "search_type": "videos", "main_cat": "straight",
    })
    items = [i for i in (_hc_list(text) if text else []) if i.get("folder")]
    note = "live"
    if not items:
        slug = re.sub(r"[^a-z0-9\-]+", "-", q.lower()).strip("-")
        text2, blocked2 = await _hc_get(f"/search/videos/{slug}")
        blocked = blocked or blocked2
        if text2:
            items = [i for i in _hc_list(text2) if i.get("folder")]
    if not items:
        ql = q.lower()
        items = [x for x in _hc_seed_items() if ql in x["title"].lower() or ql in x["id"].lower()]
        if not items:
            items = _hc_seed_items()
        note, blocked = "blocked_or_empty_seed", True
    return ok({
        "items": items, "query": q, "page": page, "suggestions": [i["title"] for i in items[:8]],
    }, provider="hentaicity", level="private", count=len(items), endpoint="search", blocked=blocked, note=note)

@app.get("/hc/suggest", tags=["HentaiCity"])
async def hc_suggest(q: str = Query(..., min_length=1)):
    """Typeahead suggestions from search results (titles + ids)."""
    q = q.strip()
    if len(q) < 2:
        return ok({"suggestions": [], "items": []}, provider="hentaicity", endpoint="suggest", query=q)
    # reuse search (lightweight)
    res = await hc_search(q=q, page=1)
    data = res.get("data") or {}
    items = data.get("items") or []
    suggestions = []
    for it in items[:12]:
        suggestions.append({
            "title": it.get("title"),
            "id": it.get("id"),
            "folder": it.get("folder"),
            "numeric_id": it.get("numeric_id"),
            "poster": it.get("poster"),
        })
    return ok({"suggestions": suggestions, "count": len(suggestions)}, provider="hentaicity", endpoint="suggest", query=q)

@app.get("/hc/recommend", tags=["HentaiCity"])
async def hc_recommend(
    folder: Optional[str] = None,
    vid: Optional[str] = None,
    tag: str = Query("cartoon"),
    limit: int = Query(24, ge=1, le=60),
):
    """Recommendations: mix popular + category near current video."""
    popular, _, _, _, _ = await _hc_fetch_pages("popular", 1, 2)
    tag = re.sub(r"[^a-z0-9]", "", tag.lower()) or "cartoon"
    cat, _, _, _, _ = await _hc_fetch_pages("category", 1, 1, f"{tag}-popular")
    recent, _, _, _, _ = await _hc_fetch_pages("recent", 1, 1)
    merged, seen = [], set()
    for it in popular + cat + recent:
        k = (it.get("folder"), it.get("numeric_id"))
        if not k[0] or k in seen:
            continue
        if folder and vid and str(it.get("folder")) == str(folder) and str(it.get("numeric_id")) == str(vid):
            continue
        seen.add(k)
        merged.append(it)
        if len(merged) >= limit:
            break
    return ok({"items": merged, "count": len(merged), "based_on": {"folder": folder, "vid": vid, "tag": tag}},
              provider="hentaicity", endpoint="recommend")

@app.get("/hc/watch", tags=["HentaiCity"])
async def hc_watch(
    folder: Optional[str] = None,
    vid: Optional[str] = None,
    id: Optional[str] = None,
    url: Optional[str] = None,
):
    title = None
    if (not folder or not vid) and url:
        text, _ = await _hc_get(url if url.startswith("http") else url)
        if text:
            fm = re.search(r"/videos/(\d+)/(\d+)/", text)
            if fm:
                folder, vid = fm.group(1), fm.group(2)
            items = _hc_list(text)
            if items:
                title = items[0].get("title")
    if folder and vid:
        streams = _hc_cdn(folder, vid)
        rec = await hc_recommend(folder=folder, vid=vid, limit=18)
        rec_items = (rec.get("data") or {}).get("items") or []
        return ok({
            "title": title or f"{folder}/{vid}",
            "folder": folder,
            "numeric_id": vid,
            "streams": streams,
            "qualities": list(streams["mp4"].keys()) + ["hls"],
            "trailer": streams["trailer"],
            "poster": streams["poster"],
            "recommend": rec_items,
        }, provider="hentaicity", level="private", endpoint="watch")
    return fail("Need folder+vid", provider="hentaicity", endpoint="watch",
                how="GET /hc/recent?page=1 → folder + numeric_id → /hc/watch?folder=&vid=")

@app.get("/hc/cdn", tags=["HentaiCity"])
async def hc_cdn(folder: str, vid: str):
    return ok(_hc_cdn(folder, vid), provider="hentaicity", level="private", endpoint="cdn")

@app.get("/hc/streams", tags=["HentaiCity"])
async def hc_streams(folder: str, vid: str):
    s = _hc_cdn(folder, vid)
    out = [{"label": q, "quality": q, "kind": "mp4", "url": u} for q, u in s["mp4"].items()]
    out.append({"label": "HLS", "quality": "adaptive", "kind": "hls", "url": s["hls"]})
    return ok({"folder": folder, "vid": vid, "poster": s["poster"], "trailer": s["trailer"], "sources": out, "count": len(out)},
              provider="hentaicity", endpoint="streams")

@app.get("/hc/feed", tags=["HentaiCity"])
async def hc_feed(
    page: int = Query(1, ge=1, le=HC_MAX_PAGE),
    pages: int = Query(3, ge=1, le=10),
    mix: str = Query("recent", description="recent|popular|both"),
):
    """Bulk feed for infinite scroll — merge multiple list pages."""
    items = []
    if mix in ("recent", "both"):
        a, _, _, _, ha = await _hc_fetch_pages("recent", page, pages)
        items.extend(a)
        has_more = ha
    else:
        has_more = False
    if mix in ("popular", "both"):
        b, _, _, _, hb = await _hc_fetch_pages("popular", page, pages)
        items.extend(b)
        has_more = has_more or hb
    seen, uniq = set(), []
    for it in items:
        k = (it.get("folder"), it.get("numeric_id"))
        if k in seen:
            continue
        seen.add(k)
        uniq.append(it)
    return ok({
        "items": uniq,
        "page": page,
        "next_page": page + pages if has_more else None,
        "has_more": has_more,
        "max_page": HC_MAX_PAGE,
        "count": len(uniq),
    }, provider="hentaicity", endpoint="feed")

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
        "Accept-Language": "en-US,en;q=0.9",
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
            continue
        for i in range(7, -1, -1):
            if b & (1 << i):
                return n
            n += 1
        break
    return n

def _kt_pow_solve(nonce: str, bits: int = 16, max_iters: int = 5_000_000) -> Optional[str]:
    """sha256(nonce + ':' + solution) leading zero bits >= bits."""
    if not nonce:
        return None
    target = int(bits or 16)
    for i in range(max_iters):
        dig = hashlib.sha256(f"{nonce}:{i}".encode()).digest()
        if _kt_leading_zero_bits(dig) >= target:
            return str(i)
    return None

KT_AES_KEY_STR = "bca9e0df1a5abb32906ca3f63ac04cef"

def _kt_b64url_decode(s: str) -> bytes:
    s = s.replace("-", "+").replace("_", "/")
    pad = len(s) % 4
    if pad:
        s += "=" * (4 - pad)
    return base64.b64decode(s)

def _kt_decrypt_url(enc: str) -> str:
    """AES-256-CBC decrypt kartoons link URL (IV||ciphertext, base64url)."""
    if not enc or not isinstance(enc, str):
        return enc or ""
    if enc.startswith("http://") or enc.startswith("https://"):
        return enc
    try:
        raw = _kt_b64url_decode(enc.strip())
        if len(raw) < 17:
            return enc
        iv, ct = raw[:16], raw[16:]
        key = (KT_AES_KEY_STR.ljust(32)[:32]).encode("utf-8")
        dec = None
        try:
            from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
            from cryptography.hazmat.backends import default_backend
            decryptor = Cipher(algorithms.AES(key), modes.CBC(iv), backend=default_backend()).decryptor()
            dec = decryptor.update(ct) + decryptor.finalize()
        except Exception:
            try:
                import subprocess
                p = subprocess.run(
                    ["openssl", "enc", "-d", "-aes-256-cbc", "-K", key.hex(), "-iv", iv.hex(), "-nopad"],
                    input=ct, capture_output=True, timeout=10,
                )
                if p.returncode == 0:
                    dec = p.stdout
            except Exception:
                dec = None
        if not dec:
            return enc
        pad = dec[-1]
        if 1 <= pad <= 16 and dec.endswith(bytes([pad]) * pad):
            dec = dec[:-pad]
        out = dec.decode("utf-8", errors="strict")
        return out if out.startswith("http") else enc
    except Exception:
        return enc


KT_ENC2_SECRET = "pmS0CAMG1Ruq49WbMyhE3fh1sOuLYEL9" + "rtFazYYljVI2j4BPSog73hW7A7xMhceHD0iwrPrVVDXLvxyWr"

def _kt_decrypt_enc2(uri: str) -> str:
    """Decrypt enc2:… AES-GCM URIs inside kartoons HLS playlists."""
    if not uri or not isinstance(uri, str) or not uri.startswith("enc2:"):
        return uri or ""
    try:
        raw_b64 = uri[5:].replace("-", "+").replace("_", "/")
        while len(raw_b64) % 4:
            raw_b64 += "="
        raw = base64.b64decode(raw_b64)
        iv, rest = raw[:12], raw[12:]
        if len(rest) < 16:
            return uri
        tag, body = rest[-16:], rest[:-16]
        key = hashlib.sha256(KT_ENC2_SECRET.encode("utf-8")).digest()
        try:
            from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
            from cryptography.hazmat.backends import default_backend
            dec = Cipher(algorithms.AES(key), modes.GCM(iv, tag), backend=default_backend()).decryptor()
            out = dec.update(body) + dec.finalize()
            return out.decode("utf-8")
        except Exception:
            return uri
    except Exception:
        return uri

def _kt_rewrite_m3u8(text: str) -> str:
    """Replace all enc2: URIs in an m3u8 with decrypted https URLs."""
    out_lines = []
    for line in text.splitlines():
        if "enc2:" not in line:
            out_lines.append(line)
            continue
        if line.strip().startswith("enc2:"):
            out_lines.append(_kt_decrypt_enc2(line.strip()))
            continue
        # URI="enc2:..."
        def _sub(m):
            return 'URI="' + _kt_decrypt_enc2(m.group(1)) + '"'
        out_lines.append(re.sub(r'URI="(enc2:[^"]+)"', _sub, line))
    return "\n".join(out_lines) + "\n"


async def _kt_fetch_links(episode_id: Optional[str] = None, movie_id: Optional[str] = None, auth: Optional[str] = None) -> dict:
    """POW solve + decrypt stream CDN links. No turnstile needed when POW headers work."""
    auth = (str(auth).strip().replace("Bearer ","").replace("bearer ","").strip() if auth else None)
    content = f"episode:{episode_id}" if episode_id else f"movie:{movie_id}"
    path = f"/shows/episode/{episode_id}/links" if episode_id else f"/movies/{movie_id}/links"
    # 1) POW
    st_pow, pow_data = await _kt_req("GET", "/challenge/pow", params={"content": content})
    nonce = bits = sol = None
    if isinstance(pow_data, dict):
        root = pow_data.get("data") if isinstance(pow_data.get("data"), dict) else pow_data
        nonce = root.get("nonce")
        bits = int(root.get("bits") or 16)
    if nonce:
        sol = await asyncio.to_thread(_kt_pow_solve, nonce, bits)
    headers_extra = {}
    if nonce and sol:
        headers_extra = {"X-Pow-Nonce": nonce, "X-Pow-Solution": sol}
    # 2) links with POW headers (custom request)
    url = KT_API + path
    async with _client(30.0) as client:
        h = _kt_headers(auth)
        h.update(headers_extra)
        r = await client.get(url, headers=h)
        try:
            data = r.json()
        except Exception:
            data = {"raw": r.text[:2000]}
        st = r.status_code
    # retry once with fresh POW if failed
    if st in (403, 428) or (isinstance(data, dict) and ("pow" in str(data).lower() or data.get("challenge_required"))):
        st_pow, pow_data = await _kt_req("GET", "/challenge/pow", params={"content": content})
        if isinstance(pow_data, dict):
            root = pow_data.get("data") if isinstance(pow_data.get("data"), dict) else pow_data
            nonce = root.get("nonce")
            bits = int(root.get("bits") or 16)
            sol = await asyncio.to_thread(_kt_pow_solve, nonce, bits) if nonce else None
        if nonce and sol:
            async with _client(30.0) as client:
                h = _kt_headers(auth)
                h["X-Pow-Nonce"] = nonce
                h["X-Pow-Solution"] = sol
                r = await client.get(url, headers=h)
                try:
                    data = r.json()
                except Exception:
                    data = {"raw": r.text[:2000]}
                st = r.status_code
    payload = data.get("data") if isinstance(data, dict) and isinstance(data.get("data"), dict) else data
    links_raw = []
    if isinstance(payload, dict):
        links_raw = payload.get("links") or []
    servers = []
    direct = []
    for L in links_raw if isinstance(links_raw, list) else []:
        if not isinstance(L, dict):
            continue
        enc = L.get("url") or ""
        plain = _kt_decrypt_url(enc)
        item = {
            "name": L.get("name") or "Server",
            "url": plain,
            "encrypted_url": enc if plain != enc else None,
            "type": "hls" if ".m3u8" in plain or "playlist" in plain else ("mp4" if ".mp4" in plain else "link"),
            "spriteVtt": L.get("spriteVtt"),
            "subtitles": L.get("subtitles") or [],
            "skipTimings": L.get("skipTimings") or [],
            "chapters": L.get("chapters") or [],
        }
        servers.append(item)
        if plain.startswith("http"):
            direct.append({"name": item["name"], "url": plain, "type": item["type"]})
    title = payload.get("title") if isinstance(payload, dict) else None
    return {
        "ok": st < 400 and len(direct) > 0,
        "status": st,
        "title": title,
        "episode_id": episode_id,
        "movie_id": movie_id,
        "servers": servers,
        "direct": direct,
        "direct_count": len(direct),
        "pow": {"nonce": (nonce or "")[:40] + "…", "solution": sol, "bits": bits},
        "raw_error": data if st >= 400 else None,
    }


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

@app.get("/kt/episode/{episode_id}/links", tags=["Kartoons-Public"])
async def kt_episode_links(
    episode_id: str,
    auth: Optional[str] = Query(None, description="JWT access_token recommended"),
    turnstile_token: Optional[str] = Query(None, description="Leave empty — POW auto-solved"),
):
    """Same as /kt/stream — POW + AES decrypt → direct CDN."""
    if not auth and turnstile_token and str(turnstile_token).startswith("eyJ"):
        auth = turnstile_token
    result = await _kt_fetch_links(episode_id=episode_id, auth=auth)
    if result.get("direct_count", 0) > 0:
        return ok(result, provider="kartoons", endpoint="episode_links", level="live")
    return fail("No links", provider="kartoons", endpoint="episode_links", data=result, status=result.get("status") or 502)


@app.get("/kt/movie/{movie_id}/links", tags=["Kartoons-Public"])
async def kt_movie_links(
    movie_id: str,
    auth: Optional[str] = Query(None),
    turnstile_token: Optional[str] = Query(None),
):
    if not auth and turnstile_token and str(turnstile_token).startswith("eyJ"):
        auth = turnstile_token
    result = await _kt_fetch_links(movie_id=movie_id, auth=auth)
    if result.get("direct_count", 0) > 0:
        return ok(result, provider="kartoons", endpoint="movie_links", level="live")
    return fail("No links", provider="kartoons", endpoint="movie_links", data=result, status=result.get("status") or 502)



@app.get("/kt/hls", tags=["Kartoons-Public"])
async def kt_hls_proxy(u: str = Query(..., description="Raw workers.dev playlist URL from /kt/stream")):
    """
    Fetch kartoons m3u8 and decrypt all enc2: URIs → plain playable HLS.
    Use this URL in VLC / ExoPlayer / hls.js:
      https://YOUR-HOST/kt/hls?u=https://v9.m3u8sap.workers.dev/playlist/...
    """
    if not u.startswith("http"):
        return fail("u must be https playlist URL", provider="kartoons", endpoint="hls")
    try:
        async with _client(45.0) as client:
            r = await client.get(u, headers={
                "User-Agent": UA,
                "Origin": KT_ORIGIN,
                "Referer": KT_ORIGIN + "/",
                "Accept": "*/*",
            })
            if r.status_code >= 400:
                return Response(content=f"# error upstream {r.status_code}\n", media_type="application/vnd.apple.mpegurl", status_code=502)
            body = _kt_rewrite_m3u8(r.text)
            return Response(
                content=body,
                media_type="application/vnd.apple.mpegurl",
                headers={
                    "Access-Control-Allow-Origin": "*",
                    "Cache-Control": "no-store",
                },
            )
    except Exception as e:
        return Response(content=f"# error {e}\n", media_type="application/vnd.apple.mpegurl", status_code=500)

@app.get("/kt/stream", tags=["Kartoons-Public"])
async def kt_stream(
    episode_id: Optional[str] = Query(None, description="Episode Mongo id"),
    movie_id: Optional[str] = Query(None, description="Movie id"),
    auth: Optional[str] = Query(None, description="Optional JWT access_token (not turnstile)"),
    turnstile_token: Optional[str] = Query(None, description="Ignored for POW path — keep empty"),
):
    """
    Direct streaming CDN (HLS/mp4) via auto POW + AES decrypt.
    Example: /kt/stream?episode_id=6867877f57ee07b9b7401910
    Optional: &auth=JWT
    Do NOT put JWT in turnstile_token.
    """
    # if user mistakenly put JWT in turnstile field
    if not auth and turnstile_token and str(turnstile_token).startswith("eyJ"):
        auth = turnstile_token
    if not episode_id and not movie_id:
        return fail("episode_id or movie_id required", provider="kartoons", endpoint="stream",
                    how="GET /kt/stream?episode_id=ID  or  /kt/stream?movie_id=ID")
    try:
        result = await _kt_fetch_links(episode_id=episode_id, movie_id=movie_id, auth=auth)
    except Exception as e:
        return fail(str(e), provider="kartoons", endpoint="stream")
    if result.get("direct_count", 0) > 0:
        # attach play_url = our /kt/hls proxy (decrypted m3u8)
        direct = []
        for d in result["direct"]:
            raw = d.get("url") or ""
            item = dict(d)
            if "workers.dev" in raw or "m3u8" in raw:
                item["play_url"] = "/kt/hls?u=" + quote(raw, safe="")
                item["how_play"] = "Open play_url on this API host in VLC/ExoPlayer (enc2 decrypted). Or raw url needs HLS player that understands enc2."
            direct.append(item)
        servers = []
        for s in result["servers"]:
            ss = dict(s)
            raw = ss.get("url") or ""
            if "workers.dev" in raw or (raw.startswith("http") and "m3u8" not in raw and "enc" not in raw):
                pass
            if raw.startswith("http"):
                ss["play_url"] = "/kt/hls?u=" + quote(raw, safe="")
            servers.append(ss)
        return ok({
            "title": result.get("title"),
            "episode_id": episode_id,
            "movie_id": movie_id,
            "direct": direct,
            "servers": servers,
            "direct_count": len(direct),
            "how": "USE play_url (not raw workers URL). Example: https://YOUR-API/kt/hls?u=...  → VLC Media → Open Network Stream. Raw workers m3u8 has enc2: lines that only our proxy decrypts.",
        }, provider="kartoons", endpoint="stream", access="public", level="live")
    return fail(
        "No stream links (POW or upstream)",
        provider="kartoons", endpoint="stream",
        status=result.get("status") or 502,
        data=result,
        how="Retry; ensure episode_id is valid. Auth optional. Turnstile not required for POW path.",
    )

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

KT_TURNSTILE_SITEKEY = "0x4AAAAAAFMBI3Lp3ohSsfYm"  # kartoons.to only — will NOT load on other domains

@app.get("/kt/captcha", tags=["Kartoons-Challenge"])
async def kt_captcha_info():
    """Turnstile cannot run on foreign domains. Use device login (no captcha) or token from kartoons.to."""
    return ok({
        "sitekey": KT_TURNSTILE_SITEKEY,
        "sitekey_domain": "kartoons.to only",
        "why_widget_fails_on_vercel": (
            "Cloudflare Turnstile sitekeys are locked to the site that created them. "
            "Loading this key on movieallshawon.vercel.app shows 'Unable to connect to website'."
        ),
        "recommended": "device_login",
        "device_login": {
            "1": "GET /kt/device/code",
            "2": "Open link_url on your phone (or https://kartoons.to/app/link)",
            "3": "Enter the code while logged into kartoons.to",
            "4": "GET /kt/device/status?code=CODE until token appears",
        },
        "manual_turnstile": {
            "1": "Open https://kartoons.to and log in normally (captcha works there)",
            "2": "Or open /kt/captcha/widget only helps with DEVICE LOGIN UI on this host",
            "3": "Password login API still needs turnstile_token issued for kartoons.to",
        },
        "widget_page": "/kt/captcha/widget",
    }, provider="kartoons", endpoint="captcha_info", access="public")

KT_CAPTCHA_HTML = r"""<!DOCTYPE html>
<html lang="en"><head>
<meta charset="utf-8"/><meta name="viewport" content="width=device-width,initial-scale=1"/>
<title>Kartoons Login — StreamHub</title>
<style>
:root{--bg:#0a0a0c;--card:#141418;--bd:#2a2a32;--tx:#f4f4f5;--mu:#9ca3af;--ok:#34d399;--bad:#f87171;--ac:#8b5cf6}
*{box-sizing:border-box}
body{font-family:system-ui,sans-serif;background:var(--bg);color:var(--tx);min-height:100vh;margin:0;padding:16px;
background-image:radial-gradient(ellipse 80% 50% at 50% -20%,rgba(139,92,246,.25),transparent)}
.wrap{max-width:440px;margin:0 auto}
.card{background:var(--card);border:1px solid var(--bd);border-radius:16px;padding:20px;margin-bottom:14px}
h1{font-size:18px;margin:0 0 6px}h2{font-size:14px;margin:0 0 10px;color:var(--tx)}
p,.mu{color:var(--mu);font-size:13px;line-height:1.5;margin:0 0 12px}
.warn{background:#3f1d1d;border:1px solid #7f1d1d;color:#fecaca;border-radius:10px;padding:10px 12px;font-size:12px;line-height:1.45;margin-bottom:12px}
.okbox{background:#052e1c;border:1px solid #065f46;color:#a7f3d0;border-radius:10px;padding:10px 12px;font-size:12px;margin-bottom:12px}
code,pre{font-family:ui-monospace,monospace}
pre{background:#050506;border:1px solid var(--bd);border-radius:10px;padding:12px;font-size:11px;word-break:break-all;white-space:pre-wrap;max-height:160px;overflow:auto}
.row{display:flex;gap:8px;flex-wrap:wrap;margin-top:10px}
button,.btn{background:linear-gradient(135deg,#8b5cf6,#6d28d9);border:0;color:#fff;padding:11px 14px;border-radius:10px;font-weight:600;font-size:13px;cursor:pointer;text-decoration:none;display:inline-block}
.ghost{background:transparent;border:1px solid var(--bd);color:var(--tx)}
.big{font-size:28px;letter-spacing:.2em;font-weight:700;text-align:center;padding:12px;background:#050506;border-radius:12px;border:1px dashed var(--bd);margin:8px 0}
.mu2{font-size:11px;color:var(--mu);text-align:center}
input{width:100%;background:#050506;border:1px solid var(--bd);border-radius:8px;padding:10px 12px;color:#fff;margin:6px 0 10px;font-size:14px}
label{font-size:11px;color:var(--mu)}
.step{display:flex;gap:10px;margin:8px 0;font-size:13px;color:var(--mu)}
.n{flex:0 0 22px;height:22px;border-radius:50%;background:#8b5cf633;color:#c4b5fd;display:grid;place-items:center;font-size:11px;font-weight:700}
a{color:#c4b5fd}
</style>
</head><body>
<div class="wrap">
  <div class="card">
    <h1>Kartoons login (no captcha on this site)</h1>
    <div class="warn"><b>Why Turnstile fails here:</b> Cloudflare sitekey belongs to <b>kartoons.to</b> only. On vercel.app it shows “Unable to connect to website”. That cannot be fixed from our API.</div>
    <p class="mu">Use <b>device link</b> instead — same account token, no captcha on this domain.</p>
    <div class="step"><span class="n">1</span><span>Tap <b>Get code</b></span></div>
    <div class="step"><span class="n">2</span><span>Open the link on your phone and enter the code (stay logged in on kartoons.to)</span></div>
    <div class="step"><span class="n">3</span><span>Wait — token JSON appears automatically</span></div>
    <div class="row">
      <button type="button" id="btnCode">Get code</button>
      <a class="btn ghost" id="openLink" href="https://kartoons.to/app/link" target="_blank" rel="noopener">Open link page</a>
    </div>
    <div class="big" id="code">————</div>
    <div class="mu2" id="meta">Code appears here · valid ~10 minutes</div>
    <pre id="out">Press “Get code”, then link your device…</pre>
  </div>
  <div class="card">
    <h2>Already have a token?</h2>
    <label>Paste access token</label>
    <input id="tok" placeholder="access_token from kartoons or device status"/>
    <div class="row">
      <button type="button" id="btnMe">Check /kt/auth/me</button>
      <button type="button" class="ghost" id="btnCopy">Copy token</button>
    </div>
    <pre id="out2" style="display:none;margin-top:10px"></pre>
  </div>
  <div class="card">
    <h2>Password login (only if you have turnstile from kartoons.to)</h2>
    <p class="mu">Token must be created <b>on kartoons.to</b>, not here. Optional advanced use.</p>
    <label>username / email</label>
    <input id="user"/>
    <label>password</label>
    <input id="pass" type="password"/>
    <label>turnstile_token (from kartoons.to browser, not this page)</label>
    <input id="ts"/>
    <button type="button" id="btnLogin">Login via API</button>
    <pre id="out3" style="display:none;margin-top:10px"></pre>
  </div>
</div>
<script>
let pollTimer=null, currentCode=null;
const $ = id => document.getElementById(id);
async function getCode(){
  $('out').textContent='Requesting code…';
  try{
    const r=await fetch('/kt/device/code'); const j=await r.json();
    const d=j.data||j;
    const code=d.code||(d.data&&d.data.code);
    const link=d.link_url||(d.data&&d.data.link_url)||'https://kartoons.to/app/link';
    if(!code){$('out').textContent=JSON.stringify(j,null,2);return}
    currentCode=code; $('code').textContent=code; $('openLink').href=link;
    $('meta').textContent='Expires in '+(d.expires_in||600)+'s · polling…';
    $('out').textContent='Code ready. Open link page, enter code, keep this screen open…\\n\\n'+JSON.stringify(j,null,2);
    if(pollTimer)clearInterval(pollTimer);
    pollTimer=setInterval(poll, 4000); poll();
  }catch(e){$('out').textContent=String(e)}
}
async function poll(){
  if(!currentCode)return;
  try{
    const r=await fetch('/kt/device/status?code='+encodeURIComponent(currentCode)+'&fullinfo=true');
    const j=await r.json();
    const d=j.data||j;
    if(d.token||d.linked){
      clearInterval(pollTimer); pollTimer=null;
      $('meta').textContent='Linked ✓ — save your token';
      $('out').textContent=JSON.stringify(j,null,2);
      if(d.token)$('tok').value=d.token;
      $('out').className='';
    }else{
      $('meta').textContent='Waiting for link… status='+(d.status||'pending');
    }
  }catch(e){}
}
$('btnCode').onclick=getCode;
$('btnCopy').onclick=()=>{const v=$('tok').value; if(v)navigator.clipboard.writeText(v)};
$('btnMe').onclick=async()=>{
  const t=$('tok').value.trim(); if(!t)return alert('Paste full access_token (starts with eyJ…)');
  if(t.length>1800){alert('Token very long — still trying…');}
  $('out2').style.display='block'; $('out2').textContent='…';
  try{
    const r=await fetch('/kt/auth/me?auth='+encodeURIComponent(t));
    const text=await r.text();
    try{$('out2').textContent=JSON.stringify(JSON.parse(text),null,2)}
    catch(_){$('out2').textContent='HTTP '+r.status+' (non-JSON):\n'+text.slice(0,2000)}
  }catch(e){$('out2').textContent=String(e)}
};
$('btnLogin').onclick=async()=>{
  const u=$('user').value.trim(), p=$('pass').value, ts=$('ts').value.trim();
  // If only JWT pasted in turnstile box, just validate session
  if(ts.startsWith('eyJ') && (!u || !p)){
    $('tok').value=ts; $('btnMe').click(); return;
  }
  if(ts.startsWith('eyJ')){
    alert('That value is an access token (JWT), not a captcha token. Use “Check /kt/auth/me” above instead.');
    $('tok').value=ts; return;
  }
  if(!u||!p)return alert('username + password');
  let url='/kt/auth/login?username='+encodeURIComponent(u)+'&password='+encodeURIComponent(p);
  if(ts)url+='&turnstile_token='+encodeURIComponent(ts);
  $('out3').style.display='block'; $('out3').textContent='…';
  try{
    const r=await fetch(url); const text=await r.text();
    try{const j=JSON.parse(text); $('out3').textContent=JSON.stringify(j,null,2);
      if(j.ok&&j.data&&j.data.token)$('tok').value=j.data.token;
    }catch(_){$('out3').textContent='HTTP '+r.status+':\n'+text.slice(0,2000)}
  }catch(e){$('out3').textContent=String(e)}
};
</script>
</body></html>
"""

@app.get("/kt/captcha/widget", response_class=HTMLResponse, tags=["Kartoons-Challenge"])
async def kt_captcha_widget():
    """Device-link login UI (Turnstile cannot run on non-kartoons domains)."""
    return HTMLResponse(KT_CAPTCHA_HTML, headers={"Cache-Control": "no-store"})

def _kt_extract_session(data: Any) -> dict:
    """Normalize login/device-link response → token + user JSON."""
    if not isinstance(data, dict):
        return {"raw": data}
    root = data.get("data") if isinstance(data.get("data"), dict) else data
    token = (
        root.get("access_token") or root.get("accessToken") or root.get("token")
        or root.get("jwt") or root.get("authToken") or data.get("access_token") or data.get("token")
    )
    # nested data.data (kartoons login shape)
    if not token and isinstance(root.get("data"), dict):
        inner = root["data"]
        token = inner.get("access_token") or inner.get("token")
        if not user:
            user = inner.get("user")
    if not token and isinstance(data.get("data"), dict):
        inner = data["data"]
        if isinstance(inner.get("data"), dict):
            inner = inner["data"]
        token = token or inner.get("access_token") or inner.get("token")
        user = user or inner.get("user")
    user = root.get("user") or root.get("account") or root.get("profile") or root.get("me")
    # sometimes user fields flat
    if not user and root.get("username"):
        user = {k: root.get(k) for k in ("_id", "id", "username", "email", "avatar", "role", "badges") if root.get(k) is not None}
    out = {
        "token": token,
        "user": user,
        "refresh_token": root.get("refresh_token") or root.get("refreshToken"),
        "expires_in": root.get("expires_in") or root.get("expiresIn"),
        "token_type": root.get("token_type") or ("Bearer" if token else None),
    }
    # keep useful extras
    for k in ("stremio", "settings", "subscription", "message", "success"):
        if k in root:
            out[k] = root[k]
    out["raw"] = data
    return out

def _kt_clean_token(auth: Optional[str]) -> str:
    if not auth:
        return ""
    a = str(auth).strip().strip('"').strip("'")
    if a.lower().startswith("bearer "):
        a = a[7:].strip()
    return a

def _kt_is_jwt(s: Optional[str]) -> bool:
    if not s or not isinstance(s, str):
        return False
    s = s.strip()
    return s.startswith("eyJ") and s.count(".") >= 2

async def _kt_me_payload(token: str) -> dict:
    """Always returns JSON-serializable dict — never raises to client as HTML 500."""
    token = _kt_clean_token(token)
    if not token:
        return fail("auth/token required", provider="kartoons", endpoint="auth_me", access="auth")
    if len(token) < 20:
        return fail("token too short — paste full access_token JWT", provider="kartoons", endpoint="auth_me")
    try:
        st, data = await _kt_req("GET", "/auth/me", auth=token)
    except Exception as e:
        return fail(f"upstream error: {e}", provider="kartoons", endpoint="auth_me", access="auth")
    if st < 400 and isinstance(data, dict):
        me = data.get("data") if isinstance(data.get("data"), dict) else data
        return ok({
            "valid": True,
            "token": token,
            "user": ({k:v for k,v in me.items() if k != "hashed_password"} if isinstance(me, dict) else None),
            "raw": data,
            "how": "Use this token as ?auth=TOKEN on /kt/watchlist, /kt/user/me, /kt/stremio/stream",
        }, provider="kartoons", endpoint="auth_me", access="auth")
    # try user/me as fallback
    try:
        st2, data2 = await _kt_req("GET", "/user/me", auth=token)
        if st2 < 400:
            me = data2.get("data") if isinstance(data2, dict) and isinstance(data2.get("data"), dict) else data2
            return ok({
                "valid": True,
                "token": token,
                "user": me,
                "raw": data2,
                "source": "user/me",
            }, provider="kartoons", endpoint="auth_me", access="auth")
    except Exception:
        pass
    msg = data.get("message") if isinstance(data, dict) else f"HTTP {st}"
    return fail(
        str(msg) if msg else f"HTTP {st}",
        provider="kartoons", endpoint="auth_me", access="auth", status=st,
        valid=False, token_preview=token[:20] + "…",
        how="Token invalid/expired. Get a new one via /kt/device/code → device status (not turnstile field).",
        data=data if isinstance(data, dict) else {"raw": str(data)[:500]},
    )

@app.get("/kt/auth/me", tags=["Kartoons-Auth"])
async def kt_auth_me(auth: str = Query(..., description="JWT access_token from device login or kartoons — NOT turnstile")):
    return await _kt_me_payload(auth)

@app.post("/kt/auth/me", tags=["Kartoons-Auth"])
async def kt_auth_me_post(auth: str = Query(None), token: str = Query(None)):
    """Same as GET — prefer this if JWT is very long for URL limits."""
    return await _kt_me_payload(auth or token or "")

async def _kt_do_login(username: str, password: str, turnstile_token: Optional[str] = None):
    """Upstream expects username + password; CAPTCHA often required."""
    # User often pastes access_token JWT into turnstile field by mistake
    if _kt_is_jwt(turnstile_token) and not password:
        return await _kt_me_payload(turnstile_token)
    if _kt_is_jwt(turnstile_token) and password:
        # still try login without misusing JWT as captcha; first validate JWT
        me = await _kt_me_payload(turnstile_token)
        if me.get("ok"):
            return ok({
                "logged_in": True,
                "token": _kt_clean_token(turnstile_token),
                "user": (me.get("data") or {}).get("user"),
                "note": "turnstile_token looked like JWT access_token — used as session token (captcha not needed)",
                "how": "You already had a login token. Do not put JWT in turnstile field.",
            }, provider="kartoons", endpoint="auth_login", access="auth")
        # JWT invalid — fall through to password login without sending JWT as captcha
        turnstile_token = None
    body = {"username": username, "password": password}
    if turnstile_token and not _kt_is_jwt(turnstile_token):
        body["turnstile_token"] = turnstile_token
        body["cf-turnstile-response"] = turnstile_token
    st, data = await _kt_req("POST", "/auth/login", json_body=body)
    # fallback email field if username rejected
    if st >= 400 and isinstance(data, dict) and "username" in str(data.get("message") or "").lower():
        st, data = await _kt_req("POST", "/auth/login", json_body={
            "email": username, "password": password,
            **({"turnstile_token": turnstile_token} if turnstile_token else {}),
        })
    sess = _kt_extract_session(data if isinstance(data, dict) else {})
    if st < 400 and sess.get("token"):
        # enrich with /auth/me when possible
        st2, me = await _kt_req("GET", "/auth/me", auth=sess["token"])
        if st2 < 400:
            sess["user"] = (me.get("data") if isinstance(me, dict) and isinstance(me.get("data"), dict) else me) or sess.get("user")
            sess["me_raw"] = me
        return ok({
            "logged_in": True,
            "token": sess.get("token"),
            "user": sess.get("user"),
            "refresh_token": sess.get("refresh_token"),
            "expires_in": sess.get("expires_in"),
            "token_type": sess.get("token_type") or "Bearer",
            "how": "Use token with ?auth=TOKEN on /kt/user/*, /kt/watchlist, /kt/stremio/stream, /kt/auth/me",
            "raw": data,
        }, provider="kartoons", endpoint="auth_login", access="auth")
    # captcha / error
    msg = (data.get("message") if isinstance(data, dict) else None) or f"HTTP {st}"
    challenge = "captcha" in str(msg).lower() or "turnstile" in str(msg).lower()
    return fail(
        str(msg),
        provider="kartoons", endpoint="auth_login", access="auth", status=st,
        captcha_required=challenge,
        how=(
            "Kartoons login needs Cloudflare Turnstile. Options: "
            "1) Pass turnstile_token from browser on kartoons.to login page. "
            "2) Use device link (no password on API): GET /kt/device/code → open link_url → enter code → GET /kt/device/status?code= until token JSON appears."
        ),
        data=data,
        session_attempt=sess,
    )

@app.api_route("/kt/auth/login", methods=["GET", "POST"], tags=["Kartoons-Auth"])
async def kt_auth_login(
    username: Optional[str] = Query(None, description="Kartoons username or email"),
    password: Optional[str] = Query(None),
    email: Optional[str] = Query(None, description="Alias for username"),
    turnstile_token: Optional[str] = Query(None, description="Cloudflare Turnstile token from browser"),
):
    """
    Login → full JSON with token + user.
    Docs Try-it uses GET (supported). Upstream often requires turnstile_token.
    Prefer /kt/device/code flow if captcha blocks password login.
    """
    user = (username or email or "").strip()
    pw = (password or "").strip()
    if not user or not pw:
        return fail(
            "username (or email) and password required",
            provider="kartoons", endpoint="auth_login",
            how="Example: /kt/auth/login?username=YOU&password=PASS&turnstile_token=...",
        )
    return await _kt_do_login(user, pw, turnstile_token)

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


@app.api_route("/kt/device/code", methods=["GET", "POST"], tags=["Kartoons-Public"])
async def kt_device_code():
    """Start TV/device link flow. User opens link_url and enters code — then poll /kt/device/status."""
    st, data = await _kt_req("POST", "/app/device/code", json_body={})
    # upstream returns fields at top-level sometimes
    if isinstance(data, dict) and data.get("success") and "data" not in data and data.get("code"):
        data = {"success": True, "data": {k: data[k] for k in data if k != "success"}}
    return _kt_wrap(st, data, "device_code", "public", how="Poll GET /kt/device/status?code= until linked")

@app.get("/kt/device/status", tags=["Kartoons-Public"])
async def kt_device_status(code: str, fullinfo: bool = True):
    """Poll after /kt/device/code. When user links device on site, response includes token — normalized below."""
    st, data = await _kt_req("GET", "/app/device/status", {"code": code, "fullinfo": str(fullinfo).lower()})
    sess = _kt_extract_session(data if isinstance(data, dict) else {})
    status = None
    if isinstance(data, dict):
        status = data.get("status") or (data.get("data") or {}).get("status") if isinstance(data.get("data"), dict) else data.get("status")
    if sess.get("token"):
        st2, me = await _kt_req("GET", "/auth/me", auth=sess["token"])
        if st2 < 400:
            sess["user"] = (me.get("data") if isinstance(me, dict) and isinstance(me.get("data"), dict) else me) or sess.get("user")
        return ok({
            "linked": True,
            "status": status or "linked",
            "token": sess.get("token"),
            "user": sess.get("user"),
            "refresh_token": sess.get("refresh_token"),
            "token_type": "Bearer",
            "how": "Save token. Call /kt/auth/me?auth=TOKEN or /kt/user/me?auth=TOKEN",
            "raw": data,
        }, provider="kartoons", endpoint="device_status", access="public")
    return ok({
        "linked": False,
        "status": status or (data.get("status") if isinstance(data, dict) else None),
        "token": None,
        "message": (data.get("message") if isinstance(data, dict) else None) or "Waiting for device link",
        "how": "Open link_url from /kt/device/code on phone, enter code, then refresh this endpoint",
        "raw": data,
    }, provider="kartoons", endpoint="device_status", access="public", http_upstream=st)

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
  <div class="brand"><div class="logo">SH</div><div><h1>StreamHub API</h1><small>v8.2.3 · creator: shawon</small></div></div>
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
{g:'MovieBox',p:'/mb/play/{id}',how:'MP4 + DASH. Each dash has play_url (cookie-free via /px proxy).',params:[{n:'id',v:'1654274595068805784',path:true},{n:'se',v:''},{n:'ep',v:''}]},
{g:'MovieBox',p:'/mb/dash/extract',how:'DASH extractor: parse MPD reps + cookie-free play_url for every quality.',params:[{n:'subject_id',v:'1654274595068805784'},{n:'se',v:''},{n:'ep',v:''}]},
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
{g:'HentaiCity',p:'/hc/home',how:'Recent+popular+cartoon. pages=2 merges 2 list pages each.',params:[{n:'pages',v:'2'}]},
{g:'HentaiCity',p:'/hc/recent',how:'Paginated recent. page=1..140, pages=N merges N pages (~34 each).',params:[{n:'page',v:'1'},{n:'pages',v:'2'}]},
{g:'HentaiCity',p:'/hc/popular',how:'Paginated popular. Same page system.',params:[{n:'page',v:'1'},{n:'pages',v:'2'}]},
{g:'HentaiCity',p:'/hc/feed',how:'Infinite-scroll feed. mix=recent|popular|both.',params:[{n:'page',v:'1'},{n:'pages',v:'3'},{n:'mix',v:'recent'}]},
{g:'HentaiCity',p:'/hc/search',how:'Official search. Returns items + suggestions titles.',params:[{n:'q',v:'school'}]},
{g:'HentaiCity',p:'/hc/suggest',how:'Typeahead: title/folder/poster suggestions while typing.',params:[{n:'q',v:'tea'}]},
{g:'HentaiCity',p:'/hc/recommend',how:'Recommended mix (popular+category). Pass folder+vid to exclude current.',params:[{n:'tag',v:'cartoon'},{n:'limit',v:'24'}]},
{g:'HentaiCity',p:'/hc/category',how:'Category list. tag=cartoon|3d|bigtits…',params:[{n:'tag',v:'cartoon'},{n:'sort',v:'popular'},{n:'page',v:'1'}]},
{g:'HentaiCity',p:'/hc/categories',how:'All category tags.',params:[]},
{g:'HentaiCity',p:'/hc/watch',how:'Streams + recommend[]. CDN poster/trailer.',params:[{n:'folder',v:'0498'},{n:'vid',v:'38179'}]},
{g:'HentaiCity',p:'/hc/streams',how:'Flat sources for quality UI.',params:[{n:'folder',v:'0498'},{n:'vid',v:'38179'}]},
{g:'HentaiCity',p:'/hc/cdn',how:'Raw CDN map.',params:[{n:'folder',v:'0498'},{n:'vid',v:'38179'}]},
{n:'vid',v:'38191'}]},
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
{g:'Kartoons',p:'/kt/hls',how:'Proxy: paste workers.dev playlist from stream → decrypted playable m3u8. VLC: Open Network Stream → this URL.',params:[{n:'u',v:'https://v9.m3u8sap.workers.dev/playlist/...'}]},
{g:'Kartoons',p:'/kt/stream',how:'AUTO POW + decrypt → direct HLS CDN. episode_id required. Optional auth=JWT. Leave turnstile empty.',params:[{n:'episode_id',v:'6867877f57ee07b9b7401910'},{n:'auth',v:''}]},
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
{g:'Kartoons',p:'/kt/captcha',how:'How to get turnstile_token + sitekey. Open /kt/captcha/widget in browser.',params:[]},
{g:'Kartoons',p:'/kt/captcha/widget',how:'HTML page with Cloudflare widget. Solve → copy token → login.',params:[]},
{g:'Kartoons',p:'/kt/auth/login',how:'AUTH login (GET+POST). Returns token+user JSON. Upstream often needs turnstile_token. No captcha? Use /kt/device/code instead.',params:[{n:'username',v:''},{n:'password',v:''},{n:'turnstile_token',v:''}]},
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
      <div class="ep-h" onclick="tog(${i})"><span class="m">${e.method||'GET'}</span><span class="p">${e.p}</span><span class="arrow">›</span></div>
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
  const e=E[i]; const u=buildUrl(e,i); const out=document.getElementById('o'+i); const st=document.getElementById('st'+i);
  out.textContent='Loading…'; st.textContent='';
  const t0=performance.now();
  try{
    const method=(e.method||'GET').toUpperCase();
    const r=await fetch(u,{method}); const t=await r.text();
    let p=t; try{p=JSON.stringify(JSON.parse(t),null,2)}catch(_){}
    st.textContent=r.status+' · '+Math.round(performance.now()-t0)+' ms';
    out.textContent=p.slice(0,16000);
  }catch(err){st.textContent='error'; out.textContent=String(err)}
}
render();
</script>
</body></html>
"""

@app.get("/docs", response_class=HTMLResponse, include_in_schema=False)
async def docs_ui():
    return HTMLResponse(DOCS_HTML)

# ========== PWA: manifest, service worker, icons ==========
import gzip, struct, zlib, math
from fastapi.responses import JSONResponse

def _png_icon(size: int) -> bytes:
    """Tiny pure-python PNG: dark rounded square + amber ring + play triangle (no PIL needed)."""
    bg, ring, tri = (5, 6, 15), (255, 178, 36), (255, 178, 36)
    cx = cy = size / 2.0
    rr, rw = size * 0.30, size * 0.045
    rad = size * 0.22
    ax, ay = cx - size * 0.07, cy - size * 0.13
    bx, by = cx - size * 0.07, cy + size * 0.13
    qx, qy = cx + size * 0.14, cy
    def inside_tri(px, py):
        d1 = (px - bx) * (ay - by) - (qx - bx) * (py - by)
        d2 = (px - qx) * (by - qy) - (ax - qx) * (py - qy)
        d3 = (px - ax) * (qy - ay) - (bx - ax) * (py - ay)
        return not ((d1 < 0 or d2 < 0 or d3 < 0) and (d1 > 0 or d2 > 0 or d3 > 0))
    rows = []
    for y in range(size):
        row = bytearray([0])
        for x in range(size):
            qx0 = min(x, size - 1 - x); qy0 = min(y, size - 1 - y)
            a = 255
            if qx0 < rad and qy0 < rad and math.hypot(rad - qx0, rad - qy0) > rad:
                a = 0
            d = math.hypot(x + .5 - cx, y + .5 - cy)
            col = bg
            if abs(d - rr) < rw / 2:
                col = ring
            elif inside_tri(x + .5, y + .5) and d < rr:
                col = tri
            row += bytes((col[0], col[1], col[2], a))
        rows.append(bytes(row))
    raw = b"".join(rows)
    def chunk(t, data):
        c = struct.pack(">I", len(data)) + t + data
        return c + struct.pack(">I", zlib.crc32(t + data) & 0xffffffff)
    return b"\x89PNG\r\n\x1a\n" + chunk(b"IHDR", struct.pack(">IIBBBBB", size, size, 8, 6, 0, 0, 0)) + chunk(b"IDAT", zlib.compress(raw, 9)) + chunk(b"IEND", b"")

_ICON_CACHE: Dict[int, bytes] = {}

@app.get("/icon-{size}.png", include_in_schema=False)
async def icon_png(size: int):
    if size not in (180, 192, 512):
        raise HTTPException(404, detail=fail("No such icon"))
    if size not in _ICON_CACHE:
        _ICON_CACHE[size] = _png_icon(size)
    return Response(_ICON_CACHE[size], media_type="image/png", headers={"Cache-Control": "public, max-age=604800"})

@app.get("/manifest.webmanifest", include_in_schema=False)
async def manifest():
    return JSONResponse({
        "name": "StreamHub", "short_name": "StreamHub", "description": "Stream, download and discover movies, series, anime and live TV.",
        "start_url": "/", "scope": "/", "display": "standalone", "orientation": "any",
        "background_color": "#05060f", "theme_color": "#05060f",
        "icons": [
            {"src": "/icon-192.png", "sizes": "192x192", "type": "image/png", "purpose": "any"},
            {"src": "/icon-512.png", "sizes": "512x512", "type": "image/png", "purpose": "any"},
            {"src": "/icon-512.png", "sizes": "512x512", "type": "image/png", "purpose": "maskable"},
        ],
    }, media_type="application/manifest+json", headers={"Cache-Control": "public, max-age=3600"})

SW_JS = """const C='sh-shell-v2';
self.addEventListener('install',e=>{self.skipWaiting();e.waitUntil(caches.open(C).then(c=>c.add('/')).catch(()=>{}))});
self.addEventListener('activate',e=>e.waitUntil(caches.keys().then(ks=>Promise.all(ks.filter(k=>k!==C).map(k=>caches.delete(k)))).then(()=>self.clients.claim())));
self.addEventListener('fetch',e=>{
  const r=e.request;
  if(r.method!=='GET'||r.mode!=='navigate')return;
  e.respondWith(fetch(r).then(x=>{const cp=x.clone();caches.open(C).then(c=>c.put('/',cp)).catch(()=>{});return x}).catch(()=>caches.match('/')));
});
"""

@app.get("/sw.js", include_in_schema=False)
async def service_worker():
    return Response(SW_JS, media_type="application/javascript", headers={"Cache-Control": "no-cache", "Service-Worker-Allowed": "/"})

# ========== FULL MOVIE WEBSITE (served at / and /site) ==========
SITE_HTML = r'''<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1,viewport-fit=cover">
<meta name="referrer" content="no-referrer">
<meta name="theme-color" content="#05060f">
<title>StreamHub — stream, download, discover</title>
<link rel="manifest" href="/manifest.webmanifest">
<link rel="apple-touch-icon" href="/icon-192.png">
<link rel="icon" href="data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 64 64'%3E%3Crect width='64' height='64' rx='16' fill='%23060913'/%3E%3Ccircle cx='32' cy='32' r='17' fill='none' stroke='%23ffb224' stroke-width='4'/%3E%3Cpath d='M27 23l16 9-16 9z' fill='%23ffb224'/%3E%3C/svg%3E">
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=Unbounded:wght@500;600;700&family=Manrope:wght@400;500;600;700;800&family=Hind+Siliguri:wght@400;500;600&display=swap" rel="stylesheet" media="print" onload="this.media='all'"><noscript><link href="https://fonts.googleapis.com/css2?family=Unbounded:wght@500;600;700&family=Manrope:wght@400;500;600;700;800&family=Hind+Siliguri:wght@400;500;600&display=swap" rel="stylesheet"></noscript>
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
#aurora i{position:absolute;width:70vmax;height:70vmax;border-radius:50%;opacity:.5}
#aurora i:nth-child(1){left:-30vmax;top:-30vmax}
#aurora i:nth-child(2){right:-34vmax;top:12vh}
#aurora i:nth-child(3){left:10vw;bottom:-46vmax}
@keyframes drift{to{transform:translate3d(8vmax,6vmax,0) scale(1.1)}}
#aurora i{animation:none}
@media(min-width:900px){html:not(.lite) #aurora i{animation:drift 30s ease-in-out infinite alternate}}
#grain{display:none}

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
#top.solid{background:rgba(6,9,19,.72);box-shadow:0 1px 0 var(--line)}
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
#dock{position:fixed;left:50%;bottom:calc(10px + var(--sb));transform:translateX(-50%);z-index:45;width:min(480px,94vw);height:var(--dockh);display:flex;padding:6px;border-radius:24px;background:rgba(12,18,36,.78);border:1px solid var(--line2);box-shadow:0 18px 50px rgba(0,0,0,.55),inset 0 1px 0 rgba(255,255,255,.06)}
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
.btn.ghost{background:rgba(255,255,255,.08);border:1px solid var(--line2);}
.btn.ghost:hover{background:rgba(255,255,255,.14)}
.btn.sm{height:36px;padding:0 14px;font-size:13px;border-radius:11px}
.btn.on{background:color-mix(in srgb,var(--a1) 16%,transparent);border-color:color-mix(in srgb,var(--a1) 60%,transparent);color:var(--amber)}
.btn[disabled]{opacity:.5;pointer-events:none}

/* hero */
#hero{position:relative;height:clamp(470px,82vh,780px);overflow:hidden;touch-action:pan-y}
.hs{position:absolute;inset:0;opacity:0;visibility:hidden;transition:opacity 1s,visibility 1s}
.hs.on{opacity:1;visibility:visible}
.hs .bg{position:absolute;inset:-4%;background-size:cover;background-position:center 20%;transform:scale(1.06)}
.hs.on .bg{animation:none}
@media(min-width:900px){html:not(.lite) .hs.on .bg{animation:kb 9s ease-out forwards}}
@keyframes kb{to{transform:scale(1.14) translate3d(-1.2%,-1%,0)}}
.hs::after{content:"";position:absolute;inset:0;background:linear-gradient(0deg,var(--void) 4%,rgba(6,9,19,.55) 38%,rgba(6,9,19,.1) 70%),linear-gradient(90deg,rgba(6,9,19,.88) 0%,rgba(6,9,19,.25) 55%,transparent)}
#hero .beam{position:absolute;inset:-30% -10%;z-index:2;pointer-events:none;background:linear-gradient(105deg,transparent 42%,color-mix(in srgb,var(--a1) 9%,transparent) 49%,rgba(255,255,255,.07) 50%,transparent 58%);animation:beam 7s ease-in-out infinite;display:none}
@media(min-width:900px){html:not(.lite) #hero .beam{display:block}}
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
.rail{display:flex;gap:12px;overflow-x:auto;scroll-snap-type:x proximity;padding:6px 16px 18px;scrollbar-width:none;max-width:1500px;margin:0 auto;}
@media(min-width:900px){html:not(.lite) .rail{-webkit-mask-image:linear-gradient(90deg,transparent 0,#000 16px,#000 calc(100% - 28px),transparent 100%);mask-image:linear-gradient(90deg,transparent 0,#000 16px,#000 calc(100% - 28px),transparent 100%)}}
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
.rt{position:absolute;z-index:3;top:8px;left:8px;padding:3px 8px;border-radius:8px;background:rgba(6,9,19,.78);font:800 11.5px var(--ff);color:var(--amber)}
.bd{position:absolute;z-index:3;top:8px;right:8px;padding:3px 7px;border-radius:7px;font:800 10.5px var(--ff);background:color-mix(in srgb,var(--a2) 85%,transparent);color:#fff}
.bd.s{background:rgba(255,79,120,.88)}.bd.k{background:color-mix(in srgb,var(--a1) 92%,transparent);color:var(--on)}
.pg-bar{position:absolute;z-index:3;left:0;right:0;bottom:0;height:3px;background:rgba(255,255,255,.18)}.pg-bar i{display:block;height:100%;background:var(--amber);box-shadow:0 0 8px var(--amber)}
.ct{padding:9px 3px 0}
.ct h3{font:700 13.5px/1.3 var(--ff);display:-webkit-box;-webkit-line-clamp:2;-webkit-box-orient:vertical;overflow:hidden}
.ct p{font-size:12px;color:var(--dim);margin-top:2px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.top10 .card{flex-basis:clamp(150px,40vw,210px)}
.top10 .num{position:absolute;left:-6px;bottom:30px;z-index:4;font:700 74px/1 var(--fd);color:transparent;-webkit-text-stroke:2px var(--amber);text-shadow:0 0 24px color-mix(in srgb,var(--a2) 50%,transparent);pointer-events:none}

/* skeleton */
.sk{background:rgba(255,255,255,.07);animation:skp 1.3s ease-in-out infinite;border-radius:var(--r)}
@keyframes skp{50%{opacity:.45}}
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
.pl-big{position:absolute;inset:0;margin:auto;width:70px;height:70px;border-radius:50%;background:rgba(10,15,30,.55);border:1px solid var(--line2);display:grid;place-items:center;transition:transform .25s,opacity .25s}
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
.pl-menu{position:absolute;right:10px;bottom:70px;z-index:6;width:min(290px,calc(100% - 20px));max-height:78%;overflow-y:auto;border-radius:16px;background:rgba(10,15,30,.94);border:1px solid var(--line2);padding:8px;display:none;pointer-events:auto;box-shadow:0 20px 60px #000a}
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
#theatre{position:fixed;inset:0;z-index:80;background:rgba(3,5,12,.94);display:none;overflow-y:auto}
#theatre.open{display:block;animation:pgin .3s}
.thin{max-width:1100px;margin:0 auto;padding:calc(14px + var(--st)) 14px 40px}

/* more sheet */
#sheet{position:fixed;inset:0;z-index:70;background:rgba(3,5,12,.6);opacity:0;visibility:hidden;transition:.3s}
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
body{--a1:#ffb224;--a2:#ff5e3a;--on:#1b0d00;--amber:var(--a1);--amber2:var(--a2);background:var(--void)}
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
#aurora i:nth-child(1){background:radial-gradient(circle,color-mix(in srgb,var(--a1) 34%,transparent),transparent 62%)}
#aurora i:nth-child(2){background:radial-gradient(circle,color-mix(in srgb,var(--a2) 32%,transparent),transparent 62%)}
#aurora i:nth-child(3){background:radial-gradient(circle,rgba(106,61,255,.26),transparent 62%)}
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
.ibtn{background:rgba(255,255,255,.05);}
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
.chip{transition:.25s cubic-bezier(.2,.9,.2,1)}
.chip:hover{transform:translateY(-2px)}
.chip.on{background:linear-gradient(135deg,color-mix(in srgb,var(--a1) 24%,transparent),color-mix(in srgb,var(--a2) 20%,transparent));border-color:color-mix(in srgb,var(--a1) 70%,transparent);color:#fff;box-shadow:0 6px 22px -6px color-mix(in srgb,var(--a1) 55%,transparent)}
.sbox:focus-within{border-color:var(--a1);box-shadow:0 0 0 4px color-mix(in srgb,var(--a1) 14%,transparent),0 12px 40px -12px color-mix(in srgb,var(--a1) 40%,transparent)}
.sbox{background:rgba(255,255,255,.04);}

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
.card{transition:transform .3s}
.poster{background:linear-gradient(160deg,#1a2350,#0b1030)}
.poster::before{inset:0;padding:2px;border-radius:var(--r);opacity:0;background:conic-gradient(from var(--ang),var(--a1),var(--a2),#7a5cff,var(--a1));-webkit-mask:linear-gradient(#000 0 0) content-box,linear-gradient(#000 0 0);-webkit-mask-composite:xor;mask:linear-gradient(#000 0 0) content-box exclude,linear-gradient(#000 0 0);transition:opacity .35s}
@keyframes ang{to{--ang:360deg}}
.poster::after{content:"";position:absolute;inset:0;z-index:2;pointer-events:none;background:linear-gradient(0deg,rgba(5,6,15,.82),transparent 46%);opacity:0;transition:opacity .35s}
.poster.noimg::after{opacity:1;background:none}
.pp{position:absolute;z-index:4;left:50%;top:50%;width:50px;height:50px;margin:-25px 0 0 -25px;border-radius:50%;display:grid;place-items:center;background:linear-gradient(135deg,var(--a1),var(--a2));color:var(--on);box-shadow:0 10px 30px color-mix(in srgb,var(--a2) 60%,transparent);transform:scale(.4);opacity:0;transition:transform .45s cubic-bezier(.2,1.5,.4,1),opacity .3s;pointer-events:none}
.pp svg{width:20px;height:20px;margin-left:2px}
@media(hover:hover){
  .card:hover .poster{transform:perspective(800px) translateY(-8px) scale(1.035) rotateX(var(--rx,0deg)) rotateY(var(--ry,0deg));border-color:transparent;box-shadow:0 26px 54px -10px rgba(0,0,0,.75),0 0 40px -6px color-mix(in srgb,var(--a1) 40%,transparent)}
  .card:hover .poster::before{opacity:1;animation:ang 3.2s linear infinite}.card:hover .poster::after{opacity:1}.card:hover .pp{transform:scale(1);opacity:1}
  .card:hover .ct h3{color:var(--a1)}
}
.ct h3{transition:color .25s}
.rt{background:rgba(5,6,15,.72);border:1px solid color-mix(in srgb,var(--a1) 30%,transparent);color:var(--a1);}
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
.sk{background:color-mix(in srgb,var(--a1) 8%,rgba(255,255,255,.06))}

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
.panel{}
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
/* ================= v3: performance + settings + gestures ================= */
.row{content-visibility:auto;contain-intrinsic-size:auto 340px}
#top.solid{background:rgba(5,6,15,.9)}
#dock{background:rgba(10,13,32,.94)}
@media(min-width:900px){html:not(.lite) #top.solid{background:rgba(5,6,15,.66);-webkit-backdrop-filter:blur(16px) saturate(1.4);backdrop-filter:blur(16px) saturate(1.4)}}
@media(min-width:900px){html:not(.lite) #theatre{-webkit-backdrop-filter:blur(10px);backdrop-filter:blur(10px)}}
.pl-menu{background:rgba(10,15,30,.97)}
#sheet{background:rgba(3,5,12,.72)}
/* lite mode: kill everything decorative */
html.lite #aurora i,html.lite #spot,html.lite #stars em,html.lite .hs .bg,html.lite #hero .beam{animation:none!important}
html.lite #spot{display:none}
html.lite .card[data-rv]{transform:none}
html.lite .card[data-rv].in{transition:opacity .35s var(--d,0ms)}
html.lite .row .rh{transform:none}
html.lite .rh h2::before,html.lite .moon,html.lite .pcover .pb,html.lite .ph h1::after{animation:none!important}
html.lite .poster,html.lite .poster img{transition:none}
html.lite .card:hover .poster{transform:none;box-shadow:none}
html.lite .poster::before,html.lite .poster::after,html.lite .pp{display:none}
html.lite .hc{transform:none!important}
html.lite .w i{transform:none;animation:none!important}
html.lite #stars b{animation:none}
/* touch devices: no hover chrome */
@media(hover:none){.poster::before,.pp{display:none}.card:hover .poster{transform:none}}
/* card size preference */
html[data-size=s] .grid{grid-template-columns:repeat(auto-fill,minmax(100px,1fr))}
html[data-size=l] .grid{grid-template-columns:repeat(auto-fill,minmax(150px,1fr))}
@media(min-width:600px){html[data-size=s] .grid{grid-template-columns:repeat(auto-fill,minmax(130px,1fr))}html[data-size=l] .grid{grid-template-columns:repeat(auto-fill,minmax(200px,1fr))}}
@media(min-width:900px){html[data-size=s] .grid{grid-template-columns:repeat(auto-fill,minmax(140px,1fr))}html[data-size=l] .grid{grid-template-columns:repeat(auto-fill,minmax(230px,1fr))}}

/* settings sheet */
#cfg{position:fixed;inset:0;z-index:90;background:rgba(3,5,12,.74);opacity:0;visibility:hidden;transition:opacity .25s,visibility .25s}
#cfg.open{opacity:1;visibility:visible}
#cfg .in{position:absolute;left:0;right:0;bottom:0;max-height:88vh;overflow-y:auto;overscroll-behavior:contain;padding:18px 18px calc(26px + var(--sb));border-radius:26px 26px 0 0;background:var(--ink);border:1px solid var(--line2);transform:translateY(100%);transition:transform .38s cubic-bezier(.2,.9,.2,1);max-width:620px;margin:auto}
#cfg.open .in{transform:none}
@media(min-width:700px){#cfg .in{top:50%;bottom:auto;transform:translateY(-46%) scale(.97);border-radius:26px;opacity:0;max-height:84vh}#cfg.open .in{transform:translateY(-50%);opacity:1}}
#cfg h3{font:600 17px var(--fd);margin:2px 0 14px;display:flex;justify-content:space-between;align-items:center}
#cfg h4{font:600 11.5px var(--fd);color:var(--a1);margin:18px 0 8px;letter-spacing:.04em}
.seg{display:flex;gap:6px;flex-wrap:wrap}
.seg button{padding:9px 14px;border-radius:12px;background:rgba(255,255,255,.05);border:1px solid var(--line);font-weight:800;font-size:13px;transition:.2s}
.seg button.on{background:linear-gradient(135deg,var(--a1),var(--a2));color:var(--on);border-color:transparent}
.krow{display:flex;justify-content:space-between;gap:12px;padding:7px 0;color:var(--dim);font-size:13px;border-bottom:1px solid var(--line)}
.krow kbd{font:700 11.5px var(--ff);padding:2px 8px;border-radius:7px;background:rgba(255,255,255,.08);border:1px solid var(--line2);color:var(--txt)}
.hide-touch{display:none}@media(hover:hover) and (min-width:900px){.hide-touch{display:block}}
/* scroll-top FAB */
#fab{position:fixed;right:16px;bottom:calc(var(--dockh) + 30px + var(--sb));z-index:44;width:46px;height:46px;border-radius:50%;display:grid;place-items:center;background:linear-gradient(135deg,var(--a1),var(--a2));color:var(--on);box-shadow:0 10px 30px color-mix(in srgb,var(--a2) 55%,transparent);transform:scale(.4);opacity:0;pointer-events:none;transition:transform .35s cubic-bezier(.2,1.5,.4,1),opacity .25s}
#fab.show{transform:none;opacity:1;pointer-events:auto}
#fab svg{width:20px;height:20px;transform:rotate(90deg)}
@media(min-width:900px){#fab{bottom:28px;right:28px}}
/* gestures HUD + next episode */
.pl-hud{position:absolute;top:16%;left:50%;transform:translateX(-50%);z-index:7;padding:9px 16px;border-radius:14px;background:rgba(8,10,26,.88);border:1px solid var(--line2);font-weight:800;font-size:14px;display:none;pointer-events:none;white-space:nowrap}
.pl-hud.show{display:block}
.pl-next{position:absolute;right:14px;bottom:84px;z-index:7;display:none;align-items:center;gap:10px;padding:10px 12px 10px 16px;border-radius:16px;background:rgba(8,10,26,.94);border:1px solid color-mix(in srgb,var(--a1) 45%,var(--line2));box-shadow:0 14px 40px #000a;font-weight:800;font-size:13.5px;animation:pgin .3s}
.pl-next.show{display:flex}.pl-next i{font-style:normal;color:var(--a1)}
.pl-next button{height:34px;padding:0 13px;border-radius:10px;font-weight:800;font-size:12.5px;background:rgba(255,255,255,.1)}
.pl-next button[data-nx=go]{background:linear-gradient(135deg,var(--a1),var(--a2));color:var(--on)}
.pl.fs .pl-ui{touch-action:none}.pl-ui{touch-action:pan-y}
/* search extras */
.mic{width:34px;height:34px;border-radius:50%;display:grid;place-items:center;flex:none;transition:.2s}
.mic svg{width:18px;height:18px}.mic.rec{background:var(--flare);color:#fff;animation:pulse 1s infinite}
.recent{display:flex;gap:8px;flex-wrap:wrap;padding:14px 16px 0;max-width:1500px;margin:0 auto}
@media(min-width:900px){.recent{padding-inline:32px}}
.recent .chip{display:inline-flex;align-items:center;gap:6px}.recent .chip i{font-style:normal;opacity:.6}
#offline{position:fixed;left:0;right:0;top:0;z-index:130;padding:calc(6px + var(--st)) 12px 6px;text-align:center;font-weight:800;font-size:12.5px;background:var(--flare);color:#fff;transform:translateY(-110%);transition:transform .3s}
#offline.show{transform:none}

/* detail backdrop: blur only where the GPU can afford it */
#dbg{filter:none;opacity:.2;transform:none}
@media(min-width:900px){html:not(.lite) #dbg{filter:blur(40px) saturate(1.3);opacity:.32;transform:scale(1.2)}}



.hc-sug{position:absolute;left:0;right:0;top:100%;z-index:30;margin-top:6px;background:rgba(12,14,28,.96);border:1px solid var(--line);border-radius:14px;max-height:280px;overflow:auto;box-shadow:0 16px 40px #000a;backdrop-filter:blur(12px)}
.hc-sug-i{display:flex;align-items:center;gap:10px;width:100%;padding:10px 12px;border:0;background:transparent;color:var(--txt);text-align:left;cursor:pointer;font:600 13px var(--ff)}
.hc-sug-i:hover{background:rgba(255,255,255,.06)}
.hc-sug-i img{width:48px;height:32px;object-fit:cover;border-radius:6px;background:#222}
.trbadge{position:absolute;left:8px;bottom:8px;z-index:3;font:700 9px/1 var(--ff);letter-spacing:.04em;padding:5px 7px;border-radius:8px;background:rgba(0,0,0,.72);color:#fff;backdrop-filter:blur(6px);opacity:.9;pointer-events:none}
.poster.trailer-on video{position:absolute;inset:0;width:100%;height:100%;object-fit:cover;z-index:2;border-radius:inherit}
.poster.trailer-on img{opacity:0}
.poster.trailer-on .trbadge{background:var(--flare,#ff4f78)}
.hc-q{display:flex;flex-wrap:wrap;gap:8px;margin:12px 0}
.hc-q button{padding:8px 12px;border-radius:12px;border:1px solid var(--line);background:var(--panel);color:var(--txt);font-weight:700;font-size:12px}
.hc-q button.on{background:linear-gradient(135deg,var(--a1),var(--a2));border-color:transparent;color:#fff}
/* v8.2 futuristic polish */
@keyframes aurora{0%{background-position:0% 50%}50%{background-position:100% 50%}100%{background-position:0% 50%}}
@keyframes floaty{0%,100%{transform:translateY(0)}50%{transform:translateY(-6px)}}
@keyframes glowPulse{0%,100%{box-shadow:0 0 0 0 rgba(94,139,255,.35)}50%{box-shadow:0 0 24px 2px rgba(94,139,255,.25)}}
.hero-aurora{background:linear-gradient(120deg,#5e8bff33,#ff4f7833,#37e6b033,#5e8bff33);background-size:300% 300%;animation:aurora 12s ease infinite}
.card:hover{transform:translateY(-4px) scale(1.02);transition:transform .35s cubic-bezier(.2,.8,.2,1),box-shadow .35s}
.btn.pri{animation:glowPulse 3.5s ease-in-out infinite}
.dock{backdrop-filter:blur(18px) saturate(1.4)}
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
<symbol id="i-mic" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="9" y="3" width="6" height="11" rx="3"/><path d="M5 11a7 7 0 0 0 14 0M12 18v3"/></symbol>
<symbol id="i-dice" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="4" y="4" width="16" height="16" rx="3.5"/><circle cx="9" cy="9" r="1.2" fill="currentColor"/><circle cx="15" cy="15" r="1.2" fill="currentColor"/><circle cx="15" cy="9" r="1.2" fill="currentColor"/><circle cx="9" cy="15" r="1.2" fill="currentColor"/></symbol>
<symbol id="i-api" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M8 6l-6 6 6 6M16 6l6 6-6 6M14 4l-4 16"/></symbol>
</defs></svg>

<div id="splash"><div><svg viewBox="0 0 64 64"><circle class="ring" cx="32" cy="32" r="22"/><path class="tri" d="M27 23l16 9-16 9z"/></svg><b>STREAMHUB</b></div></div>
<div id="aurora"><i></i><i></i><i></i></div><div id="spot"></div><div id="stars"></div><div id="prog"></div>

<header id="top">
  <a class="logo" href="#/home" aria-label="StreamHub home"><svg viewBox="0 0 64 64"><circle class="ring" cx="32" cy="32" r="22" fill="none"/><path class="tri" d="M27 23l16 9-16 9z"/></svg><span>StreamHub</span></a>
  <nav id="nav" aria-label="Sections"></nav>
  <div class="tspace"></div>
  <a class="ibtn" href="#/search" aria-label="Search"><svg><use href="#i-search"/></svg></a>
  <a class="ibtn" href="#/library" aria-label="My library"><svg><use href="#i-heart"/></svg></a>
  <button class="ibtn" id="cfgBtn" aria-label="Settings"><svg><use href="#i-gear"/></svg></button>
</header>

<main id="view" aria-live="polite"></main>

<nav id="dock" aria-label="Main"><span id="dockglow"></span></nav>

<div id="sheet"><div class="in"><div class="grab"></div><div class="mg" id="moreGrid"></div></div></div>
<div id="detail" aria-modal="true" role="dialog"><div id="dbg"></div><div id="dcontent"></div></div>
<div id="theatre"><div class="thin" id="tcontent"></div></div>
<div id="toast" role="status"></div>
<div id="cfg"><div class="in" id="cfgIn"></div></div>
<button id="fab" aria-label="Back to top"><svg><use href="#i-back"/></svg></button>
<div id="offline">You are offline — showing what is already loaded</div>

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
    out.push({kind:k,url:m.url,play_url:m.play_url||m.url,quality:m.quality,q:qNum(m.quality),size:m.size,codec:m.codec,label:'MP4',source:m.source||'play',dl:true,cookie:m.cookie||''})});
  // prefer dash_extractor / streams from play API
  const dashList=(d.dash_extractor||d.dash||[]).concat((d.streams||[]).filter(x=>x&&x.kind==='dash'));
  dashList.forEach(m=>{
    if(!m.base&&!m.url&&!m.play_url)return;
    const play=m.play_url||(m.base?dashProxyUrl(m.base,m.cookie):null);
    out.push({kind:'dash',url:m.url||m.mpd,base:m.base,cookie:m.cookie,play_url:play,quality:m.quality,q:qNum(m.quality),size:m.size,codec:m.codec,label:'DASH · no cookie',dl:false,reps:m.representations||m.dash_extractor&&m.representations});
  });
  // pure mp4 from streams
  (d.streams||[]).filter(x=>x&&x.kind!=='dash'&&x.url).forEach(m=>{
    if(out.some(o=>o.url===m.url))return;
    out.push({kind:'mp4',url:m.url,play_url:m.play_url||m.url,quality:m.quality,q:qNum(m.quality),size:m.size,codec:m.codec,label:'MP4',source:'play',dl:true});
  });
  // mp4 first, then dash by quality
  out.sort((a,b)=>(a.kind==='mp4'||a.kind==='hls'?0:1)-(b.kind==='mp4'||b.kind==='hls'?0:1)||(b.q-a.q));
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
    case'hc':return'#/hc/'+enc(it.folder||'')+'/'+enc(it.numeric_id||it.vid||it.id||'');
  }return'#/home';
}
function extIntent(url){
  try{const u=new URL(url);return'intent:'+url.replace(/^https?:\/\//,'')+'#Intent;scheme='+u.protocol.replace(':','')+';type=video/*;end'}catch(e){return url}
}
function b64u(s){return btoa(unescape(encodeURIComponent(s))).replace(/\+/g,'-').replace(/\//g,'_').replace(/=+$/,'')}
function hcItem(a){
  if(!a||typeof a!=='object')return null;
  const folder=a.folder||(a.streams&&a.streams.folder);
  const vid=a.numeric_id||a.video_id||a.vid||(a.streams&&a.streams.video_id);
  const streams=a.streams||(folder&&vid?null:null);
  const tr=(streams&&streams.trailer)||a.trailer||(folder&&vid?('https://cdn1.hentaicity.com/'+folder+'/'+vid+'/trailer.mp4'):'');
  const poster=a.poster||(streams&&streams.poster)||(folder&&vid?('https://cdn1.images.hentaicity.com/videos/'+folder+'/'+vid+'/main.jpg'):'');
  if(!folder||!vid)return null;
  return{provider:'hc',id:String(a.id||(folder+'-'+vid)),folder:String(folder),numeric_id:String(vid),title:String(a.title||'Video'),poster,trailer:tr,streams:streams||null,type:'movie',genre:'HentaiCity',_adult:1};
}
function dashProxyUrl(base,cookie){return'/px/'+b64u(JSON.stringify({b:base,c:cookie||''}))+'/index.mpd'}
function gxUrl(u,extra){return'/gx?u='+enc(u)+(extra&&extra.cookie?'&c='+enc(extra.cookie):'')}
if(typeof module!=='undefined')module.exports={mbSubj,mbSections,mbDetail,mbSeasons,mbStreams,haItem,genericSections,haEpisodes,haServers,fkItem,drItem,hrefOf,dashProxyUrl,gxUrl,qNum,fmtSize,fmtTime,extIntent,b64u,humanize};

/* ===== settings (shared) + lazy engine loader ===== */
const CFG=Object.assign({perf:'auto',q:'auto',autonext:true,size:'m'},(()=>{try{return JSON.parse(localStorage.getItem('sh_cfg'))||{}}catch(e){return {}}})());
const LIBS={hls:'https://cdnjs.cloudflare.com/ajax/libs/hls.js/1.5.15/hls.min.js',dash:'https://cdnjs.cloudflare.com/ajax/libs/dashjs/4.7.4/dash.all.min.js'};
const _libP={};
function loadLib(k){
  const has=k==='hls'?()=>window.Hls:()=>window.dashjs;
  if(has())return Promise.resolve();
  if(_libP[k])return _libP[k];
  return _libP[k]=new Promise((res,rej)=>{const s=document.createElement('script');s.src=LIBS[k];s.onload=()=>res();s.onerror=()=>{delete _libP[k];rej(new Error('Could not load the '+k.toUpperCase()+' engine — check your connection'))};document.head.appendChild(s)});
}
/* ===== Player: MP4 / DASH / HLS / iframe with fallbacks ===== */
const ico=(n,cls)=>'<svg'+(cls?' class="'+cls+'"':'')+'><use href="#i-'+n+'"/></svg>';
class Player{
  constructor(host,opts){
    this.host=host;this.o=opts||{};this.vt=[];this.at=[];this.st=[];this.sources=[];this.idx=-1;this.ctx={};this.dash=null;this.hls=null;this.started=false;this.dead=false;
    this.speed=1;this.fit=false;this.menuOpen=false;this.lastSave=0;this.tapT=0;this.tapX=0;this.wl=null;this.ptype='mouse';
    host.innerHTML='<div class="pl idle" tabindex="0">'+
    '<video playsinline webkit-playsinline preload="auto"></video><iframe allowfullscreen allow="autoplay; fullscreen; picture-in-picture; encrypted-media" referrerpolicy="no-referrer"></iframe>'+
    '<div class="pl-ui"><div class="pl-top"><span class="pl-title"></span><span class="pl-src"></span></div>'+
    '<button class="pl-big" aria-label="Play">'+ico('play')+'</button><div class="pl-spin"></div><div class="pl-seek l">−10s</div><div class="pl-seek r">+10s</div>'+
    '<div class="pl-msg"></div><div class="pl-hud"></div><div class="pl-next"></div><div class="pl-menu"></div>'+
    '<div class="pl-bot"><div class="pl-bar"><div class="buf"></div><div class="fill"></div><div class="knob"></div><span class="tip">0:00</span></div>'+
    '<div class="pl-row"><button class="pb" data-a="pp" aria-label="Play/Pause">'+ico('play')+'</button><button class="pb" data-a="b10" aria-label="Back 10 seconds">'+ico('b10')+'</button><button class="pb" data-a="f10" aria-label="Forward 10 seconds">'+ico('f10')+'</button>'+
    '<button class="pb" data-a="mute" aria-label="Mute">'+ico('vol')+'</button><input class="vol" type="range" min="0" max="1" step="0.05" value="1" aria-label="Volume">'+
    '<span class="pl-time">0:00 / 0:00</span><span class="pl-sp"></span>'+
    '<button class="pb" data-a="next" aria-label="Next episode" style="display:none">'+ico('next')+'</button>'+
    '<button class="pb" data-a="gear" aria-label="Settings">'+ico('gear')+'</button><button class="pb" data-a="pip" aria-label="Picture in picture">'+ico('pip')+'</button><button class="pb" data-a="fs" aria-label="Fullscreen">'+ico('fs')+'</button></div></div></div>'+
    '<input type="file" accept=".srt,.vtt" hidden></div>';
    const q=s=>host.querySelector(s);
    this.el=q('.pl');this.v=q('video');this.f=q('iframe');this.ui=q('.pl-ui');this.msg=q('.pl-msg');this.menu=q('.pl-menu');this.hud=q('.pl-hud');this.nextBox=q('.pl-next');
    this.bar=q('.pl-bar');this.fill=q('.fill');this.buf=q('.buf');this.knob=q('.knob');this.tip=q('.tip');this.time=q('.pl-time');
    this.title=q('.pl-title');this.srcTag=q('.pl-src');this.ppBtn=q('[data-a=pp]');this.vol=q('.vol');this.file=q('input[type=file]');this.nextBtn=q('[data-a=next]');
    this._bind();
    try{const sv=JSON.parse(localStorage.getItem('sh_vol')||'{}');if(typeof sv.v==='number')this.v.volume=sv.v;if(sv.m)this.v.muted=true;if(sv.s)this.speed=sv.s}catch(e){}
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
    on(v,'ended',()=>{this._save(true);if(this.ctx.onEnded&&CFG.autonext!==false)this._countdown(this.ctx.onEnded)});
    on(v,'volumechange',()=>{try{localStorage.setItem('sh_vol',JSON.stringify({v:v.volume,m:v.muted,s:this.speed}))}catch(e){}this.vol.value=v.muted?0:v.volume;this.el.querySelector('[data-a=mute] use').setAttribute('href',v.muted||!v.volume?'#i-mute':'#i-vol')});
    on(v,'error',()=>{if(Date.now()<this._ign||this.dead)return;if(this.sources[this.idx]&&this.sources[this.idx].kind==='iframe')return;const e=v.error;this._fail(e?('media error '+e.code):'playback error')});
    on(document,'fullscreenchange',()=>this._fsch());on(document,'webkitfullscreenchange',()=>this._fsch());
    on(this.vol,'input',()=>{v.muted=false;v.volume=+this.vol.value});
    on(el,'pointerdown',e=>{this.ptype=e.pointerType||'mouse'},true);
    on(el,'pointermove',()=>this._poke());
    on(this.ui,'click',e=>this._uiClick(e));
    on(this.host,'click',e=>{const nx=e.target.closest('[data-nx]');if(nx){const go=nx.dataset.nx==='go';const cb=this.nextCb;this._countStop();if(go&&cb)cb();return}const b=e.target.closest('[data-a]');if(b)this._act(b.dataset.a,b);const m=e.target.closest('[data-m]');if(m){e.stopPropagation();this._menuAct(m.dataset.m,m.dataset.v)}});
    // seek bar drag
    let drag=false;const seekTo=ev=>{const r=this.bar.getBoundingClientRect();const p=Math.min(1,Math.max(0,(ev.clientX-r.left)/r.width));if(isFinite(v.duration)){v.currentTime=p*v.duration}};
    on(this.bar,'pointerdown',e=>{drag=true;this.bar.setPointerCapture&&this.bar.setPointerCapture(e.pointerId);seekTo(e)});
    on(this.bar,'pointermove',e=>{const r=this.bar.getBoundingClientRect();const p=Math.min(1,Math.max(0,(e.clientX-r.left)/r.width));this.tip.style.left=(p*100)+'%';this.tip.textContent=fmtTime((v.duration||0)*p);if(drag)seekTo(e)});
    on(this.bar,'pointerup',()=>{drag=false});on(this.bar,'pointercancel',()=>{drag=false});
    this._gestures(on);
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
    if(mp.length){
      const want=CFG.q==='auto'?720:(+CFG.q||720);
      const withQ=mp.filter(x=>x.s.q);
      if(withQ.length){
        const exact=withQ.find(x=>x.s.q===want);if(exact)return exact.i;
        const lower=withQ.filter(x=>x.s.q<want).sort((a,b)=>b.s.q-a.s.q)[0];if(lower)return lower.i;
        return withQ.sort((a,b)=>a.s.q-b.s.q)[0].i;
      }
      return mp[0].i;
    }
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
    this._countStop();this._teardown();this.idx=i;this.started=false;this.keepT=t;this._hideMsg();this._busy(true);this._menuShow(false);
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
    if(s.play_url)return s.play_url.startsWith('http')?s.play_url:(s.play_url.startsWith('/')?s.play_url:('/'+s.play_url));
    if(s.kind==='dash')return dashProxyUrl(s.base||String(s.url||'').replace(/\/index\.mpd.*$/,''),s.cookie);
    if(s.proxied)return gxUrl(s.url,{cookie:s.cookie});
    return s.url;
  }
  _dash(url){
    if(!window.dashjs){const i=this.idx;this._busy(true);loadLib('dash').then(()=>{if(!this.dead&&this.idx===i&&!this.dash)this._dash(url)}).catch(e=>this._fail(e.message));return}
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
    if(!window.Hls&&!v.canPlayType('application/vnd.apple.mpegurl')){const i=this.idx;this._busy(true);loadLib('hls').then(()=>{if(!this.dead&&this.idx===i&&!this.hls)this._hlsLoad(url,s)}).catch(e=>this._fail(e.message));return}
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
  _gestures(on){
    const el=this.ui;let g=null;this.bright=1;
    const clamp=(x,a,b)=>Math.min(b,Math.max(a,x));
    on(el,'pointerdown',e=>{
      if(e.pointerType!=='touch'||e.target.closest('.pl-bot,.pl-menu,.pl-top,.pl-msg,.pl-next'))return;
      const r=this.el.getBoundingClientRect(),v=this.v;
      g={x:e.clientX,y:e.clientY,id:e.pointerId,mode:null,t0:v.currentTime,vol0:v.muted?0:v.volume,br0:this.bright,left:(e.clientX-r.left)<r.width/2,w:r.width,h:r.height,tgt:v.currentTime};
      g.hold=setTimeout(()=>{if(g&&!g.mode&&!v.paused&&!this.el.classList.contains('if')){g.mode='hold';v.playbackRate=2;this._hudShow('2× speed')}},520);
    });
    on(el,'pointermove',e=>{
      if(!g||e.pointerId!==g.id)return;const dx=e.clientX-g.x,dy=e.clientY-g.y,v=this.v;
      if(!g.mode){
        if(Math.abs(dx)>16&&Math.abs(dx)>Math.abs(dy)*1.2){g.mode='seek';clearTimeout(g.hold)}
        else if(Math.abs(dy)>16&&this.el.classList.contains('fs')){g.mode=g.left?'bright':'vol';clearTimeout(g.hold)}
        else if(Math.abs(dy)>16){clearTimeout(g.hold);g=null;return}
      }
      if(g.mode==='seek'&&isFinite(v.duration)){
        const span=Math.min(240,v.duration);g.tgt=clamp(g.t0+dx/g.w*span,0,v.duration);
        const d=Math.round(g.tgt-g.t0);this._hudShow((d>=0?'▶▶ +':'◀◀ ')+d+'s  ·  '+fmtTime(g.tgt));
      }else if(g.mode==='vol'){
        const nv=clamp(g.vol0-dy/g.h*1.5,0,1);v.muted=false;v.volume=nv;this._hudShow('Volume '+Math.round(nv*100)+'%');
      }else if(g.mode==='bright'){
        const nb=clamp(g.br0-dy/g.h*1.4,.35,1.7);this.bright=nb;v.style.filter='brightness('+nb.toFixed(2)+')';this._hudShow('Brightness '+Math.round(nb*100)+'%');
      }
    });
    const end=e=>{
      if(!g||(e&&e.pointerId!==g.id))return;clearTimeout(g.hold);
      if(g.mode==='seek'&&isFinite(this.v.duration))this.v.currentTime=g.tgt;
      if(g.mode==='hold')this.v.playbackRate=this.speed;
      if(g.mode)this._sup=Date.now()+400;
      g=null;
    };
    on(el,'pointerup',end);on(el,'pointercancel',end);
  }
  _countdown(cb){
    const b=this.nextBox;let n=6;clearInterval(this.nt);
    const draw=()=>{b.innerHTML='Next episode in <i>'+n+'</i><button data-nx="go">Play now</button><button data-nx="no">Cancel</button>'};
    draw();b.classList.add('show');this.nextCb=cb;
    this.nt=setInterval(()=>{n--;if(n<=0){clearInterval(this.nt);b.classList.remove('show');cb()}else draw()},1000);
  }
  _countStop(){clearInterval(this.nt);this.nextBox&&this.nextBox.classList.remove('show')}
  _hudShow(t){this.hud.textContent=t;this.hud.classList.add('show');clearTimeout(this.ht);this.ht=setTimeout(()=>this.hud.classList.remove('show'),900)}
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
    if(s.play_url){const p=s.play_url;return p.startsWith('http')?p:(location.origin+(p.startsWith('/')?p:'/'+p));}
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
    const vt=this.vt||[],at=this.at||[],stt=this.st||[];
    if(vt.length>1){
      let cur=-2;
      if(this.dash){try{const ab=this.dash.getSettings().streaming.abr.autoSwitchBitrate.video;cur=ab?-1:this.dash.getQualityFor('video')}catch(e){cur=-1}}
      else if(this.hls)cur=this.hls.autoLevelEnabled?-1:this.hls.currentLevel;
      h+='<h4>Quality</h4><button data-m="q" data-v="-1" class="'+(cur===-1?'on':'')+'">Auto</button>'+this.vt.map(q=>'<button data-m="q" data-v="'+q.i+'" class="'+(cur===q.i?'on':'')+'">'+esc(q.label)+' <small>'+q.sub+'</small></button>').join('');
    }
    if(!this.vt.length){
      const mq=this.sources.map((x,i)=>({x,i})).filter(o=>o.x.kind==='mp4'&&o.x.q&&!o.x.dead).sort((a,b)=>b.x.q-a.x.q);
      if(mq.length>1)h+='<h4>Quality</h4>'+mq.map(o=>'<button data-m="src" data-v="'+o.i+'" class="'+(o.i===this.idx?'on':'')+'">'+o.x.q+'p <small>'+(fmtSize(o.x.size)||'')+'</small></button>').join('');
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
    h+='<h4>Tools</h4><button data-m="skip">Skip intro (+85 s)</button><button data-m="copy">Copy stream link</button><button data-m="ext">Open in app (VLC / MX Player)</button>';
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
    else if(a==='sp'){this.speed=+val;v.playbackRate=this.speed;try{localStorage.setItem('sh_vol',JSON.stringify({v:v.volume,m:v.muted,s:this.speed}))}catch(e){}}
    else if(a==='fit'){this.fit=val==='1';v.classList.toggle('fill',this.fit)}
    else if(a==='skip'){this._seekBy(85);this._menuShow(false);return}
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
    if(Date.now()<(this._sup||0))return;
    if(e.target.closest('.pl-bot,.pl-menu,.pl-top,.pl-msg,.pl-next'))return;
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
    this.dead=true;this._save(true);this._teardown();this._lock(false);clearTimeout(this.it);clearTimeout(this.mt);clearTimeout(this.tapTimer);clearTimeout(this.pw);clearInterval(this.nt);clearTimeout(this.ht);
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
const PERSIST=/^\/(mb\/home|ha\/home|ha\/top10|ha\/hero|fk\/home|dr\/home|mb\/adult\/home)(\?|$)/;
function ssGet(p){try{const r=sessionStorage.getItem('sh_c:'+p);if(!r)return null;const o=JSON.parse(r);return Date.now()-o.t<600000?o.v:null}catch(e){return null}}
function ssSet(p,v){try{const s=JSON.stringify({t:Date.now(),v});if(s.length<250000)sessionStorage.setItem('sh_c:'+p,s)}catch(e){}}
async function api(path,o){
  o=o||{};const ttl=o.ttl==null?120000:o.ttl,retry=o.retry==null?1:o.retry,tmo=o.timeout||45000;
  const c=_cache.get(path);if(!o.fresh&&c&&Date.now()-c.t<ttl)return c.v;
  if(!o.fresh&&PERSIST.test(path)){const sv=ssGet(path);if(sv){_cache.set(path,{t:Date.now(),v:sv});return sv}}
  let err;
  for(let i=0;i<=retry;i++){
    const ac=new AbortController(),to=setTimeout(()=>ac.abort(),tmo);
    try{
      const r=await fetch(path,{signal:ac.signal});clearTimeout(to);
      const j=await r.json().catch(()=>null);
      if(!r.ok||!j||j.ok===false){const d=j&&(j.error||(j.detail&&(j.detail.error||j.detail.detail||(typeof j.detail==='string'?j.detail:''))));throw new Error(d||('Server returned '+r.status))}
      _cache.set(path,{t:Date.now(),v:j});if(PERSIST.test(path))ssSet(path,j);return j;
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
  const tr=it.trailer||(it.streams&&it.streams.trailer)||'';
  return'<div class="poster'+(it.poster?'':' noimg')+'" data-t="'+esc(t)+'"'+(tr?' data-trailer="'+esc(tr)+'"':'')+'>'+(it.poster?'<img loading="lazy" decoding="async" referrerpolicy="no-referrer" alt="" src="'+esc(it.poster)+'" onload="imgOk(this)" onerror="imgErr(this)">':'')+'<span class="shine"></span><span class="pp">'+ico('play')+'</span>'+(tr?'<span class="trbadge">HOLD · TRAILER</span>':'')+(extra||'')+'</div>';
}
function card(it,opts){
  opts=opts||{};
  let ex='';
  if(it.rating)ex+='<b class="rt">★ '+esc(it.rating)+'</b>';
  if(it.quality==='4K')ex+='<span class="bd k">4K</span>';else if(it.provider==='hc')ex+='<span class="bd" style="background:#ff4f78">HC</span>';else if(it.type==='series'&&!opts.noBadge)ex+='<span class="bd">SERIES</span>';
  if(opts.progress)ex+='<div class="pg-bar"><i style="width:'+Math.min(100,opts.progress)+'%"></i></div>';
  if(opts.num)ex+='<span class="num">'+opts.num+'</span>';
  const meta=[it.year,(it.genre||'').split(',')[0],it.ep].filter(Boolean).join(' • ');
  return'<a class="card" data-rv href="'+hrefOf(it)+'" data-provider="'+esc(it.provider||'')+'">'+poster(it,ex)+'<div class="ct"><h3>'+esc(it.title)+'</h3><p>'+esc(meta||(it.provider==='hc'?'HentaiCity':(it.type==='series'?'Series':'Movie')))+'</p></div></a>';
}
function row(title,items,opts){
  opts=opts||{};if(!items||!items.length)return '';
  const id='r'+Math.random().toString(36).slice(2,8);
  return'<section class="row'+(opts.cls?' '+opts.cls:'')+'"><div class="rh"><h2>'+esc(title)+'</h2><div class="arrows"><button aria-label="Scroll left" data-sc="'+id+'" data-d="-1">'+ico('back')+'</button><button aria-label="Scroll right" data-sc="'+id+'" data-d="1">'+ico('next')+'</button></div></div><div class="rail" id="'+id+'">'+items.map((it,i)=>card(it,{progress:it._p,num:opts.num?i+1:0})).join('')+'</div></section>';
}
const skelRow=()=>'<div class="row"><div class="rh"><h2 style="opacity:.3">Loading…</h2></div><div class="skrow">'+'<div class="sk"></div>'.repeat(8)+'</div></div>';
const skelGrid=n=>'<div class="grid">'+'<div class="sk" style="aspect-ratio:2/3"></div>'.repeat(n||12)+'</div>';
function emptyBox(t,sub,btn){return'<div class="empty"><b>'+esc(t)+'</b>'+esc(sub||'')+(btn?'<br><button class="btn pri" data-retry>'+esc(btn)+'</button>':'')+'</div>'}
const SpeechR=window.SpeechRecognition||window.webkitSpeechRecognition;
function sBox(id,ph,val,mic){return'<label class="sbox">'+ico('search')+'<input id="'+id+'" type="search" enterkeyhint="search" autocomplete="off" placeholder="'+esc(ph)+'" value="'+esc(val||'')+'">'+(mic&&SpeechR?'<button type="button" class="mic" data-mic="'+id+'" aria-label="Voice search">'+ico('mic')+'</button>':'')+'</label>'}
function bindMic(root){
  const b=$('[data-mic]',root);if(!b)return;const inp=document.getElementById(b.dataset.mic);
  b.onclick=()=>{
    try{const r=new SpeechR();r.interimResults=false;r.maxAlternatives=1;r.lang=navigator.language||'en-US';
      r.onresult=ev=>{inp.value=ev.results[0][0].transcript;inp.dispatchEvent(new Event('input'))};
      r.onend=()=>b.classList.remove('rec');r.onerror=()=>{b.classList.remove('rec');toast('Could not hear that — try again')};
      b.classList.add('rec');r.start()}catch(e){toast('Voice search is not available here')}
  };
}
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
  {k:'live',l:'Live TV',i:'live',dock:1},{k:'midnight',l:'Midnight',i:'moon'},{k:'hentaicity',l:'HentaiCity',i:'moon'},{k:'downloads',l:'Downloads',i:'dl'},{k:'library',l:'Library',i:'lib'}
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

/* ===== fx + app shell features: motion, perf mode, settings, shortcuts, PWA ===== */
(function(){
  const root=document.documentElement,body=document.body;
  const reduce=window.matchMedia&&matchMedia('(prefers-reduced-motion: reduce)').matches;
  const saveCfg=()=>{try{localStorage.setItem('sh_cfg',JSON.stringify(CFG))}catch(e){}};
  /* ---- performance mode ---- */
  const autoLite=()=>reduce||(navigator.deviceMemory&&navigator.deviceMemory<=4)||(navigator.hardwareConcurrency&&navigator.hardwareConcurrency<=4)||(navigator.connection&&navigator.connection.saveData);
  const isLite=()=>CFG.perf==='lite'||(CFG.perf==='auto'&&!!autoLite());
  window.applyCfg=function(){root.classList.toggle('lite',isLite());root.dataset.size=CFG.size||'m'};
  applyCfg();
  /* ---- nav pill ---- */
  window.fxPill=function(){
    const pill=document.getElementById('navpill'),nav=document.getElementById('nav');if(!pill||!nav)return;
    requestAnimationFrame(()=>{const a=nav.querySelector('a.on');if(!a||!nav.offsetWidth){pill.style.opacity=0;return}pill.style.width=a.offsetWidth+'px';pill.style.transform='translateX('+a.offsetLeft+'px)';pill.style.opacity=1});
  };
  window.addEventListener('resize',()=>fxPill());
  /* ---- staggered reveal (scoped to the view + detail only) ---- */
  const io='IntersectionObserver' in window?new IntersectionObserver(es=>{
    es.forEach(en=>{
      if(!en.isIntersecting)return;const el=en.target;io.unobserve(el);
      if(el.matches('.card')){const sib=el.parentElement?el.parentElement.children:[];let idx=0;for(let i=0;i<sib.length;i++){if(sib[i]===el){idx=i;break}}el.style.setProperty('--d',((idx%9)*50)+'ms')}
      el.classList.add('in');
    });
  },{rootMargin:'0px 0px -3% 0px',threshold:.03}):null;
  const watch=r=>{
    if(!r||r.nodeType!==1)return;
    const list=r.querySelectorAll?r.querySelectorAll('.card[data-rv],.row'):[];
    const self=r.matches&&r.matches('.card[data-rv],.row')?[r]:[];
    [...self,...list].forEach(el=>{if(el._rv)return;el._rv=1;if(!io||reduce){el.classList.add('in');return}io.observe(el)});
  };
  const mo=new MutationObserver(m=>{for(const r of m)for(const n of r.addedNodes)watch(n)});
  ['view','detail','theatre'].forEach(id=>{const e=document.getElementById(id);if(e)mo.observe(e,{childList:true,subtree:true})});
  /* ---- ripple ---- */
  document.addEventListener('pointerdown',e=>{
    if(root.classList.contains('lite'))return;
    const b=e.target.closest&&e.target.closest('.btn');if(!b)return;
    const r=b.getBoundingClientRect(),d=Math.max(r.width,r.height)*2;
    const s=document.createElement('span');s.className='rip';s.style.cssText='width:'+d+'px;height:'+d+'px;left:'+(e.clientX-r.left-d/2)+'px;top:'+(e.clientY-r.top-d/2)+'px';
    b.appendChild(s);setTimeout(()=>s.remove(),650);
  },{passive:true});
  /* ---- cursor spotlight (desktop, full mode) ---- */
  const spot=document.getElementById('spot');let tx=0,ty=0,cx=0,cy=0,raf=0;
  if(spot&&matchMedia('(hover:hover) and (min-width:900px)').matches){
    const loop=()=>{cx+=(tx-cx)*.14;cy+=(ty-cy)*.14;spot.style.transform='translate3d('+cx+'px,'+cy+'px,0)';raf=(Math.abs(tx-cx)+Math.abs(ty-cy)>.6)?requestAnimationFrame(loop):0};
    window.addEventListener('pointermove',e=>{if(e.pointerType!=='mouse'||root.classList.contains('lite'))return;tx=e.clientX;ty=e.clientY;spot.classList.add('on');if(!raf)raf=requestAnimationFrame(loop)},{passive:true});
  }
  /* ---- scroll: progress bar, hero parallax, FAB ---- */
  const prog=document.getElementById('prog'),det=document.getElementById('detail'),fab=document.getElementById('fab');let tick=0;
  const upd=()=>{
    tick=0;const opened=det.classList.contains('open');
    const y=opened?det.scrollTop:window.scrollY;
    const el=opened?det:document.documentElement;const max=(el.scrollHeight-el.clientHeight)||1;
    prog.style.transform='scaleX('+Math.min(1,Math.max(0,y/max))+')';
    fab.classList.toggle('show',y>700);
    if(!opened&&y<900&&innerWidth>=900&&!root.classList.contains('lite')){const hc=document.querySelector('#hero .hs.on .hc');if(hc)hc.style.transform='translateY('+(y*-.1)+'px)'}
  };
  const sched=()=>{if(!tick)tick=requestAnimationFrame(upd)};
  window.addEventListener('scroll',sched,{passive:true});det.addEventListener('scroll',sched,{passive:true});
  fab.onclick=()=>{const t=det.classList.contains('open')?det:window;t.scrollTo({top:0,behavior:reduce?'auto':'smooth'})};
  /* ---- midnight stars ---- */
  const st=document.getElementById('stars');
  if(st){let h='';const n=matchMedia('(max-width:900px)').matches?36:70;for(let i=0;i<n;i++)h+='<b style="left:'+(Math.random()*100).toFixed(1)+'%;top:'+(Math.random()*100).toFixed(1)+'%;animation-delay:'+(Math.random()*4).toFixed(2)+'s;animation-duration:'+(2.5+Math.random()*3).toFixed(1)+'s;opacity:'+(.3+Math.random()*.7).toFixed(2)+'"></b>';
    h+='<em style="left:78%;animation-delay:1s"></em><em style="left:55%;animation-delay:4.5s"></em>';st.innerHTML=h}
  /* ---- theme colour ---- */
  const meta=document.createElement('meta');meta.name='theme-color';meta.content='#05060f';document.head.appendChild(meta);
  const TC={home:'#1a1008',movies:'#07112a',series:'#150a2b',anime:'#220a18','4k':'#041a1d',drama:'#220a10',live:'#220609',midnight:'#0b0a26',hentaicity:'#2a0818',downloads:'#04201a',library:'#1d1405',search:'#071626'};
  new MutationObserver(()=>{meta.content=TC[body.dataset.sec]||'#05060f'}).observe(body,{attributes:true,attributeFilter:['data-sec']});
  /* ---- offline bar ---- */
  const off=document.getElementById('offline');
  const net=()=>off.classList.toggle('show',!navigator.onLine);window.addEventListener('online',()=>{net();toast('Back online')});window.addEventListener('offline',net);net();
  /* ---- PWA ---- */
  let deferred=null;
  window.addEventListener('beforeinstallprompt',e=>{e.preventDefault();deferred=e;const b=document.getElementById('instBtn');if(b)b.style.display=''});
  window.addEventListener('appinstalled',()=>{deferred=null;toast('Installed — find StreamHub on your home screen')});
  if('serviceWorker' in navigator&&location.protocol.startsWith('http'))window.addEventListener('load',()=>navigator.serviceWorker.register('/sw.js').catch(()=>{}));
  /* ---- settings sheet ---- */
  const cfg=document.getElementById('cfg'),cin=document.getElementById('cfgIn');
  const seg=(k,opts)=>'<div class="seg">'+opts.map(o=>'<button data-cfg="'+k+'" data-v="'+o[0]+'" class="'+(String(CFG[k])===String(o[0])?'on':'')+'">'+o[1]+'</button>').join('')+'</div>';
  function drawCfg(){
    cin.innerHTML='<h3>Settings <button class="ibtn" data-cfgx aria-label="Close">'+ico('x')+'</button></h3>'+
    '<h4>PERFORMANCE</h4>'+seg('perf',[['auto','Auto'],['lite','Lite (smoothest)'],['full','Full effects']])+
    '<p style="color:var(--dim);font-size:12.5px;margin-top:8px">Lite turns off blur, tilt and background animation. Auto picks Lite on low-memory phones.</p>'+
    '<h4>DEFAULT VIDEO QUALITY</h4>'+seg('q',[['auto','Auto'],['1080','1080p'],['720','720p'],['480','480p'],['360','360p']])+
    '<h4>AUTOPLAY NEXT EPISODE</h4>'+seg('autonext',[['true','On'],['false','Off']])+
    '<h4>CARD SIZE</h4>'+seg('size',[['s','Small'],['m','Medium'],['l','Large']])+
    '<h4>APP</h4><div class="seg"><button id="instBtn" style="'+(deferred?'':'display:none')+'">'+ico('dl')+' Install StreamHub</button><button data-act="clearhist">Clear watch history</button><button data-act="clearlist">Clear My list</button><button data-act="lock18">Lock Midnight</button><button data-act="reload">Clear cache &amp; reload</button></div>'+
    (deferred?'':'<p style="color:var(--dim);font-size:12.5px;margin-top:8px">To install: browser menu → “Add to Home screen” (Android / iOS) or the install icon in the address bar (PC).</p>')+
    '<div class="hide-touch"><h4>KEYBOARD SHORTCUTS</h4>'+[['/','Search'],['?','Settings'],['Space / K','Play / pause'],['← → / J L','Seek 5 s / 10 s'],['↑ ↓','Volume'],['M','Mute'],['F','Fullscreen'],['N','Next episode'],['0–9','Jump to 0–90 %'],['Esc','Close / back']].map(k=>'<div class="krow"><span>'+k[1]+'</span><kbd>'+k[0]+'</kbd></div>').join('')+'</div>'+
    '<h4>GESTURES (PHONE)</h4><p style="color:var(--dim);font-size:13px;line-height:1.7">Double-tap left / right: ∓10 s · Swipe sideways: scrub · Hold: 2× speed · Fullscreen: swipe up/down on the right for volume, left for brightness.</p>'+
    '<h4>ABOUT</h4><p style="color:var(--dim);font-size:13px">StreamHub · <a href="/docs" target="_blank" rel="noopener" style="color:var(--a1);text-decoration:underline">API docs</a></p>';
  }
  const openCfg=()=>{drawCfg();cfg.classList.add('open');document.body.classList.add('lock')};
  const closeCfg=()=>{cfg.classList.remove('open');if(!det.classList.contains('open')&&!document.getElementById('theatre').classList.contains('open'))document.body.classList.remove('lock')};
  window.openCfg=openCfg;
  document.getElementById('cfgBtn').onclick=openCfg;
  cfg.addEventListener('click',e=>{
    if(e.target===cfg||e.target.closest('[data-cfgx]')){closeCfg();return}
    const b=e.target.closest('[data-cfg]');
    if(b){const k=b.dataset.cfg;let v=b.dataset.v;if(v==='true')v=true;else if(v==='false')v=false;CFG[k]=v;saveCfg();applyCfg();drawCfg();return}
    const a=e.target.closest('[data-act]');
    if(a){
      const x=a.dataset.act;
      if(x==='clearhist'){store.set('sh_hist',[]);toast('Watch history cleared')}
      else if(x==='clearlist'){store.set('sh_list',[]);toast('My list cleared')}
      else if(x==='lock18'){store.set('sh_18',false);toast('Midnight locked')}
      else if(x==='reload'){try{sessionStorage.clear()}catch(err){}if(window.caches)caches.keys().then(ks=>ks.forEach(k=>caches.delete(k)));location.reload()}
      return;
    }
    if(e.target.closest('#instBtn')&&deferred){deferred.prompt();deferred.userChoice.finally(()=>{deferred=null;closeCfg()})}
  });
  /* ---- keyboard shortcuts ---- */
  document.addEventListener('keydown',e=>{
    const t=e.target,typing=t&&(t.tagName==='INPUT'||t.tagName==='TEXTAREA'||t.isContentEditable);
    if(e.key==='Escape'&&cfg.classList.contains('open')){closeCfg();e.stopPropagation();return}
    if(typing||e.ctrlKey||e.metaKey||e.altKey)return;
    if(e.key==='/'){e.preventDefault();location.hash='#/search'}
    else if(e.key==='?'){e.preventDefault();openCfg()}
  },true);
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
  V.innerHTML='<div class="pg"><div id="heroBox"><div class="sk skhero"></div></div><div class="wrap" style="margin-top:16px"><button class="btn sm ghost" id="sur">'+ico('dice')+'Surprise me</button></div><div id="rows"></div></div>';
  $('#sur').onclick=()=>{const pool=[...SEEN.values()].filter(x=>x.poster&&!x._adult&&x.provider!=='dr');if(!pool.length){toast('Still loading — try again in a second');return}const it=pool[Math.floor(Math.random()*pool.length)];toast('Tonight: '+it.title);location.hash=hrefOf(it)+'?play=1'};
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
  api('/mb/adult/home').then(j=>{if(my!==App.nav)return;const r=mbSections(D(j));const secs=r.sections;if(r.hero.length)secs.unshift({title:'Featured',items:r.hero});secs.forEach(s=>s.items.forEach(i=>{i._adult=1}));$('#mrows').innerHTML=secs.map(s=>row(s.title,s.items)).join('')||''}).catch(e=>{$('#mrows').innerHTML=notice('Midnight MovieBox',e)});
  // HentaiCity shelves on Midnight
  api('/hc/home').then(j=>{
    if(my!==App.nav)return;const d=D(j)||{};
    let h='<div class="rh" style="margin-top:8px"><h2>HentaiCity</h2><a class="btn sm ghost" href="#/hentaicity">Open full page</a></div>';
    (d.sections||[{title:'Recent',items:d.recent||[]},{title:'Popular',items:d.popular||[]}]).forEach(sec=>{
      const items=(sec.items||[]).map(hcItem).filter(Boolean);
      items.forEach(x=>SEEN.set('hc:'+x.folder+'/'+x.numeric_id,x));
      h+=row('HC · '+(sec.title||'Videos'),items);
    });
    $('#mrows').insertAdjacentHTML('beforeend',h);
  }).catch(()=>{});
  const g=$('#mg'),mm=$('#mm');
  const load=async reset=>{
    if(st.busy||!st.q)return;st.busy=true;mm.disabled=true;
    if(reset){st.page=1;st.seen.clear();st.done=false;g.innerHTML=skelGrid(9).replace('<div class="grid">','').replace(/<\/div>$/,'')}
    try{const j=await api('/mb/adult?q='+enc(st.q)+'&page='+st.page,{ttl:180000});if(my!==App.nav)return;
      const items=(D(j).items||[]).map(mbSubj).filter(Boolean).filter(x=>!st.seen.has(x.id));items.forEach(x=>{st.seen.add(x.id);x._adult=1});
      if(reset)g.innerHTML='';g.insertAdjacentHTML('beforeend',items.map(x=>card(x)).join(''));
      if(!items.length){st.done=true;if(reset)g.innerHTML='<div class="empty" style="grid-column:1/-1"><b>No results</b></div>'}else st.page++}
    catch(e){if(reset)g.innerHTML='<div style="grid-column:1/-1">'+emptyBox("Couldn't search",e.message)+'</div>'}
    finally{st.busy=false;mm.disabled=st.done;mm.style.display=st.done?'none':''}
  };
  mm.onclick=()=>load(false);lazyMore(mm,()=>load(false));
  $('#mq').oninput=debounce(e=>{st.q=e.target.value.trim();const on=!!st.q;$('#mrows').style.display=on?'none':'';g.style.display=on?'':'none';mm.style.display=on?'':'none';if(on)load(true)},500);
}


/* ----- HENTAICITY ----- */
function pgHentaiCity(){
  const my=App.nav;
  if(!store.get('sh_18',false)){
    V.innerHTML='<div class="pg gate"><div class="box"><div class="moon"></div><h1>HentaiCity</h1><p>Adult-only streams. Continue only if you are 18+ and it is legal where you live.</p><div class="acts"><button class="btn pri" id="y18">I am 18 or older</button><a class="btn ghost" href="#/home">Back</a></div></div></div>';
    $('#y18').onclick=()=>{store.set('sh_18',true);pgHentaiCity()};return;
  }
  const st={mode:'feed',page:1,busy:false,done:false,q:''};
  V.innerHTML='<div class="pg"><div class="ph"><h1>HentaiCity</h1><p>Thousands of videos · hold poster for trailer · infinite scroll</p><div class="acts" style="margin-top:12px"><a class="btn sm ghost" href="#/midnight">Midnight</a><button class="btn sm ghost" id="lock18">Lock 18+</button></div></div>'
    +'<div style="position:relative;max-width:560px;margin:0 auto 8px">'+sBox('hcq','Search HentaiCity…')+'<div id="hcsug" class="hc-sug" style="display:none"></div></div>'
    +'<div class="chips" id="hcchips">'
    +[['Feed','feed'],['Recent','recent'],['Popular','popular'],['Cartoon','cartoon'],['3D','3d'],['Big Tits','bigtits']].map((x,i)=>'<button class="chip'+(i===0?' on':'')+'" data-mode="'+x[1]+'">'+x[0]+'</button>').join('')
    +'</div>'
    +'<div id="hcrows"></div><div id="hcg" class="grid"></div>'
    +'<div class="more-wrap"><button class="btn ghost" id="hcmore">Load more</button></div></div>';
  $('#lock18').onclick=()=>{store.set('sh_18',false);toast('Locked');pgHentaiCity()};
  const g=$('#hcg'), more=$('#hcmore'), rows=$('#hcrows'), sug=$('#hcsug');
  const addCards=(items,reset)=>{
    const cards=items.map(hcItem).filter(Boolean);
    cards.forEach(x=>SEEN.set('hc:'+x.folder+'/'+x.numeric_id,x));
    if(reset)g.innerHTML='';
    g.insertAdjacentHTML('beforeend',cards.map(x=>card(x)).join(''));
  };
  const load=async reset=>{
    if(st.busy||st.done&&!reset)return;st.busy=true;more.disabled=true;
    if(reset){st.page=1;st.done=false;g.innerHTML=skelGrid(12).replace('<div class="grid">','').replace(/<\/div>$/,'')}
    try{
      let j,items=[],has=false,next=null;
      if(st.q){
        j=await api('/hc/search?q='+enc(st.q),{ttl:120000});
        const d=D(j)||{};items=d.items||[];has=false;
      }else if(st.mode==='feed'){
        j=await api('/hc/feed?page='+st.page+'&pages=2&mix=recent',{ttl:180000,timeout:90000});
        const d=D(j)||{};items=d.items||[];has=!!d.has_more;next=d.next_page;
      }else if(st.mode==='recent'||st.mode==='popular'){
        j=await api('/hc/'+st.mode+'?page='+st.page+'&pages=2',{ttl:180000,timeout:90000});
        const d=D(j)||{};items=d.items||[];has=!!d.has_more;next=d.next_page;
      }else{
        j=await api('/hc/category?tag='+enc(st.mode)+'&sort=popular&page='+st.page+'&pages=2',{ttl:180000,timeout:90000});
        const d=D(j)||{};items=d.items||[];has=!!d.has_more;next=d.next_page;
      }
      if(my!==App.nav)return;
      addCards(items,reset);
      if(!items.length&&reset)g.innerHTML='<div class="empty" style="grid-column:1/-1"><b>No videos</b><p>Try another tab or search.</p></div>';
      if(has&&next){st.page=next;st.done=false}else if(has){st.page+=2;st.done=false}else{st.done=true}
    }catch(e){if(reset)g.innerHTML='<div style="grid-column:1/-1">'+emptyBox('Failed',e.message)+'</div>'}
    finally{st.busy=false;more.disabled=st.done;more.style.display=st.done?'none':''}
  };
  more.onclick=()=>load(false);lazyMore(more,()=>load(false));
  $('#hcchips').onclick=e=>{const b=e.target.closest('[data-mode]');if(!b)return;
    $$('#hcchips .chip').forEach(c=>c.classList.remove('on'));b.classList.add('on');
    st.mode=b.dataset.mode;st.q='';$('#hcq').value='';sug.style.display='none';rows.innerHTML='';load(true)};
  // search + suggestions
  $('#hcq').oninput=debounce(async e=>{
    const q=e.target.value.trim();st.q=q;
    if(q.length<2){sug.style.display='none';if(!q){rows.innerHTML='';load(true)}return}
    try{
      const j=await api('/hc/suggest?q='+enc(q),{ttl:60000});
      const list=(D(j).suggestions||[]);
      if(!list.length){sug.style.display='none'}
      else{
        sug.style.display='';
        sug.innerHTML=list.map(s=>'<button type="button" class="hc-sug-i" data-f="'+esc(s.folder||'')+'" data-v="'+esc(s.numeric_id||'')+'" data-t="'+esc(s.title||'')+'">'
          +(s.poster?'<img alt="" src="'+esc(s.poster)+'" referrerpolicy="no-referrer" onerror="this.style.display=\'none\'">':'')
          +'<span>'+esc(s.title||'')+'</span></button>').join('');
      }
    }catch(_){sug.style.display='none'}
    load(true);
  },400);
  sug.onclick=e=>{const b=e.target.closest('[data-f]');if(!b||!b.dataset.f)return;
    location.hash='#/hc/'+enc(b.dataset.f)+'/'+enc(b.dataset.v)};
  // shelves on top once
  api('/hc/home?pages=1').then(j=>{
    if(my!==App.nav)return;const d=D(j)||{};
    rows.innerHTML=(d.sections||[]).slice(0,2).map(sec=>row('HC · '+(sec.title||''),(sec.items||[]).map(hcItem).filter(Boolean))).join('');
  }).catch(()=>{});
  load(true);
}

function hcSourcesfunction hcSources(streams){
  if(!streams)return[];
  const out=[];
  const mp4=streams.mp4||{};
  [['1080p',mp4['1080p']],['720p',mp4['720p']],['480p',mp4['480p']],['default',mp4.default],['mobile',mp4.mobile]].forEach(([lab,u])=>{
    if(u)out.push({kind:'mp4',url:u,label:lab,quality:lab,q:qNum(lab)});
  });
  if(streams.hls)out.push({kind:'hls',url:streams.hls,label:'HLS adaptive',quality:'HLS',q:2000});
  return out;
}

async function dtHC(folder,vid,q){
  const my=++DT.id;
  const key=folder+'/'+vid;
  const seen=SEEN.get('hc:'+key);
  showDetail(topBar()+'<div class="dbody"><div class="sk" style="height:34px;width:60%;margin:20px 0"></div><div class="sk" style="aspect-ratio:16/9"></div></div>',seen&&seen.poster);
  let d={}, streams=null;
  try{
    const j=await api('/hc/watch?folder='+enc(folder)+'&vid='+enc(vid),{ttl:300000});
    d=D(j)||{};
    streams=d.streams||d;
  }catch(e){
    try{const j2=await api('/hc/cdn?folder='+enc(folder)+'&vid='+enc(vid));streams=D(j2)||{}}catch(_){}
  }
  if(my!==DT.id)return;
  if(!streams||!(streams.mp4||streams.hls)){
    streams=_hc_client_cdn(folder,vid);
  }
  const it={provider:'hc',id:key,folder,numeric_id:vid,title:d.title||(seen&&seen.title)||('HC '+vid),
    poster:streams.poster||(seen&&seen.poster)||'',trailer:streams.trailer||'',type:'movie',genre:'HentaiCity',_adult:1};
  SEEN.set('hc:'+key,it);
  const srcs=hcSources(streams);
  showDetail(detailShell(it,
    '<button class="btn pri" data-playbtn>'+ico('play')+'Play</button>'+(it.trailer?'<button class="btn ghost" data-trailerbtn>'+ico('play')+'Trailer</button>':''),
    '<div id="pbox" style="margin-top:22px">'+cover(it.poster,it.title)+'</div>'
    +'<div class="panel" style="margin-top:16px"><b>Quality</b><div class="hc-q" id="hcqbtns"></div><div class="note">Pick a quality — MP4 is direct CDN. HLS is adaptive.</div></div>'
    +'<div id="tabsbox"></div>'),it.poster);
  bindList(it);
  const box=$('#pbox');
  const qbox=$('#hcqbtns');
  let cur=0;
  const renderQ=()=>{
    qbox.innerHTML=srcs.map((s,i)=>'<button type="button" class="'+(i===cur?'on':'')+'" data-qi="'+i+'">'+esc(s.label||s.quality)+'</button>').join('')||'<span class="note">No streams</span>';
  };
  renderQ();
  qbox.onclick=e=>{const b=e.target.closest('[data-qi]');if(!b)return;cur=+b.dataset.qi;renderQ();play()};
  function play(){
    if(!srcs.length){toast('No streams');return}
    const ordered=srcs.slice(cur).concat(srcs.slice(0,cur));
    box.innerHTML='';
    mountPlayer(box,ordered,{title:it.title,sub:'HentaiCity',poster:it.poster,onProgress:(t,dd)=>lib.save(it,t,dd)});
    box.scrollIntoView({behavior:'smooth',block:'center'});
  }
  $('[data-playbtn]','#dcontent').onclick=play;
  const tb=$('[data-trailerbtn]','#dcontent');
  if(tb)tb.onclick=()=>{
    openTheatre({title:it.title+' · Trailer',sources:[{kind:'mp4',url:it.trailer,label:'Trailer'}],poster:it.poster});
  };
  const pc=$('#pcv');if(pc)pc.onclick=play;
  tabsUI($('#tabsbox'),[
    {k:'dl',l:'Downloads',fn:c=>{
      c.innerHTML='<div class="dl">'+srcs.map(s=>linkRow({label:s.label,url:s.url,kind:s.kind,quality:s.quality})).join('')+'</div>';
    }},
    {k:'rec',l:'Recommend',fn:async c=>{
      c.innerHTML='<div class="skrow">'+'<div class="sk"></div>'.repeat(6)+'</div>';
      try{
        const j=await api('/hc/recommend?folder='+enc(folder)+'&vid='+enc(vid)+'&limit=24',{ttl:180000});
        const items=(D(j).items||[]).map(hcItem).filter(Boolean);
        items.forEach(x=>SEEN.set('hc:'+x.folder+'/'+x.numeric_id,x));
        c.innerHTML=items.length?'<div class="grid">'+items.map(x=>card(x)).join('')+'</div>':'<div class="note">No recommendations</div>';
      }catch(e){c.innerHTML='<div class="note warn">'+esc(e.message)+'</div>'}
    }},
    {k:'info',l:'Info',fn:c=>{
      c.innerHTML='<div class="panel"><p class="dd">Folder <b>'+esc(folder)+'</b> · ID <b>'+esc(vid)+'</b></p>'
        +'<p class="dd">Hold poster for trailer · use quality chips to switch · Recommend tab for more.</p>'
        +(it.trailer?'<p class="dd"><a href="'+esc(it.trailer)+'" target="_blank" rel="noopener">Open trailer URL</a></p>':'')
        +'</div>';
    }}
  ],'dl');
  // auto recommend row under player
  api('/hc/recommend?folder='+enc(folder)+'&vid='+enc(vid)+'&limit=16').then(j=>{
    if(my!==DT.id)return;
    const items=(D(j).items||[]).map(hcItem).filter(Boolean);
    if(!items.length)return;
    items.forEach(x=>SEEN.set('hc:'+x.folder+'/'+x.numeric_id,x));
    const host=$('#tabsbox');
    if(host)host.insertAdjacentHTML('beforebegin','<div id="hcrec">'+row('More like this',items)+'</div>');
  }).catch(()=>{});
  if(q.get('play'))play();
}
function _hc_client_cdn(folder,vid){
  const base='https://www.hentaicity.com/flv/'+folder+'/'+vid;
  return{folder,video_id:vid,
    hls:'https://hls.hentaicity.com/_hls/flv/'+folder+'/'+vid+'/,default,mobile,480p,720p,1080p,.mp4.urlset/master.m3u8',
    mp4:{mobile:base+'/mobile.mp4',default:base+'/default.mp4','480p':base+'/480p.mp4','720p':base+'/720p.mp4','1080p':base+'/1080p.mp4'},
    poster:'https://cdn1.images.hentaicity.com/videos/'+folder+'/'+vid+'/main.jpg',
    trailer:'https://cdn1.hentaicity.com/'+folder+'/'+vid+'/trailer.mp4'};
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
const recents={get:()=>store.get('sh_recent',[]),add(q){q=q.trim();if(q.length<2)return;let l=this.get().filter(x=>x.toLowerCase()!==q.toLowerCase());l.unshift(q);store.set('sh_recent',l.slice(0,10))},clear(){store.set('sh_recent',[])}};
function pgSearch(q){
  const my=App.nav;
  V.innerHTML='<div class="pg"><div class="ph"><h1>Search</h1><p>One box across MovieBox, 4K Hub, Drama and Anime.</p></div>'+sBox('sq','Type a title…',q,true)+'<div id="sr"></div></div>';
  bindMic(V);
  const run=async qq=>{
    const box=$('#sr');
    if(!qq){
      const rc=recents.get();
      box.innerHTML=(rc.length?'<div class="recent">'+rc.map(r=>'<button class="chip" data-rc="'+esc(r)+'">'+esc(r)+'</button>').join('')+'<button class="chip" data-rcx><i>clear</i></button></div>':'')+emptyBox('Start typing','Results appear as you type.');
      return;
    }
    box.innerHTML=skelRow()+skelRow();
    const [a,b]=await Promise.allSettled([api('/search?q='+enc(qq)),api('/ha/search?q='+enc(qq))]);
    if(my!==App.nav)return;let h='';
    if(a.status==='fulfilled'){const d=D(a.value)||{};
      const mb=((d.moviebox||{}).items||[]).map(mbSubj).filter(Boolean);h+=row('MovieBox',mb);
      const fk=Array.isArray(d['4khdhub'])?d['4khdhub'].map(fkItem).filter(Boolean):[];h+=row('4K Hub',fk);
      const dr=((d.dramachi||{}).items||[]).map(drItem).filter(Boolean);h+=row('Drama',dr)}
    if(b.status==='fulfilled'){const it=(D(b.value).items||[]).map(x=>haItem(x,x.kind)).filter(Boolean);h=row('Anime',it)+h}
    box.innerHTML=h||emptyBox('No results for “'+qq+'”','Check the spelling or try fewer words.');
    if(h)recents.add(qq);
  };
  const inp=$('#sq');if(!(window.matchMedia&&matchMedia('(pointer:coarse)').matches&&q))inp.focus();
  inp.oninput=debounce(e=>{const v=e.target.value.trim();history.replaceState(null,'','#/search'+(v?'?q='+enc(v):''));App.pageKey='search'+(v?'?q='+enc(v):'');run(v)},450);
  $('#sr').onclick=e=>{const c=e.target.closest('[data-rc]');if(c){inp.value=c.dataset.rc;inp.dispatchEvent(new Event('input'));return}if(e.target.closest('[data-rcx]')){recents.clear();run('')}};
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
  const qs=(se!=null?'?se='+se+'&ep='+ep:'');
  const [cdn,play,ex]=await Promise.allSettled([
    api('/mb/cdn/'+enc(id)+qs,{ttl:240000,timeout:60000}),
    api('/mb/play/'+enc(id)+qs,{ttl:240000,timeout:60000}),
    api('/mb/dash/extract?subject_id='+enc(id)+(se!=null?'&se='+se+'&ep='+ep:''),{ttl:240000,timeout:90000}),
  ]);
  const bag={};
  if(cdn.status==='fulfilled')Object.assign(bag,D(cdn.value)||{});
  if(play.status==='fulfilled'){const p=D(play.value)||{};bag.streams=p.streams||[];bag.dash_extractor=p.dash_extractor||bag.dash_extractor}
  if(ex.status==='fulfilled'){const e=D(ex.value)||{};bag.dash_extractor=e.dash_extractor||bag.dash_extractor}
  const list=mbStreams(bag);
  // ensure every dash has play_url for cookie-free play
  list.forEach(s=>{
    if(s.kind==='dash'&&!s.play_url&&s.base)s.play_url=dashProxyUrl(s.base,s.cookie||'');
  });
  if(!list.length)throw new Error('MovieBox returned no streams for this selection');
  // MP4 first in player
  list.sort((a,b)=>(a.kind==='mp4'?0:a.kind==='hls'?1:2)-(b.kind==='mp4'?0:b.kind==='hls'?1:2)||(b.q-a.q));
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

/* ---- hold poster → trailer preview ---- */
(function(){
  let holdT=null, active=null;
  function stop(){
    if(holdT){clearTimeout(holdT);holdT=null}
    if(active){
      const v=active.querySelector('video[data-tr]');
      if(v){try{v.pause()}catch(e){} v.remove()}
      active.classList.remove('trailer-on');active=null;
    }
  }
  function start(poster){
    const url=poster.getAttribute('data-trailer');if(!url)return;
    stop();
    holdT=setTimeout(()=>{
      holdT=null;active=poster;poster.classList.add('trailer-on');
      const v=document.createElement('video');
      v.setAttribute('data-tr','1');v.muted=true;v.loop=true;v.playsInline=true;v.setAttribute('playsinline','');
      v.src=url;v.style.cssText='position:absolute;inset:0;width:100%;height:100%;object-fit:cover;z-index:2;border-radius:inherit';
      poster.appendChild(v);v.play().catch(()=>{});
    },420);
  }
  document.addEventListener('pointerdown',e=>{
    const p=e.target.closest('.poster[data-trailer]');if(!p)return;
    // don't navigate yet
    start(p);
  },{passive:true});
  document.addEventListener('pointerup',stop,{passive:true});
  document.addEventListener('pointercancel',stop,{passive:true});
  document.addEventListener('pointerleave',e=>{if(e.target.closest&&e.target.closest('.poster'))stop()},{passive:true});
  document.addEventListener('click',e=>{
    const p=e.target.closest('.poster[data-trailer]');
    if(p&&p.classList.contains('trailer-on')){e.preventDefault();e.stopPropagation();stop()}
  },true);
})();

const PAGES={home:pgHome,movies:()=>pgBrowse('movies'),series:()=>pgBrowse('series'),anime:pgAnime,'4k':pg4k,drama:pgDrama,live:pgLive,midnight:pgMidnight,hentaicity:pgHentaiCity,downloads:pgDownloads,library:pgLibrary};
function route(){
  const raw=(location.hash||'#/home').slice(1);const [path,qs]=raw.split('?');
  const parts=path.split('/').filter(Boolean).map(s=>{try{return decodeURIComponent(s)}catch(e){return s}});
  const q=new URLSearchParams(qs||'');const k=parts[0]||'home';App.navCount++;
  closeTheatre();
  if(['mb','ha','hau','fk','dr','hc'].includes(k)){
    if(!parts[1]){location.hash='#/home';return}
    if(!$('#view').children.length){App.pageKey=null;App.nav++;PAGES.home()} // direct link: render home behind
    if(k==='mb')dtMB(parts[1],q);else if(k==='ha')dtHA(parts[1],q,false);else if(k==='hau')dtHA(parts[1],q,true);else if(k==='fk')dtFK(parts[1],q);else if(k==='hc')dtHC(parts[1],parts[2]||'',q);else dtDR(parts[1],parts[2]||'movies',q);
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
SITE_GZ = gzip.compress(SITE_HTML.encode("utf-8"), 9)

def _site_response(request: Request):
    h = {"Cache-Control": "no-cache", "Vary": "Accept-Encoding"}
    if "gzip" in request.headers.get("accept-encoding", ""):
        h["Content-Encoding"] = "gzip"
        return Response(SITE_GZ, media_type="text/html; charset=utf-8", headers=h)
    return HTMLResponse(SITE_HTML, headers=h)

@app.get("/", response_class=HTMLResponse, include_in_schema=False)
async def root(request: Request):
    return _site_response(request)

@app.get("/site", response_class=HTMLResponse, include_in_schema=False)
async def site(request: Request):
    return _site_response(request)
