# StreamHub API v9.5.0 — Unified Streaming Engine + Cine-Glass UI/UX + Interactive Docs
# Modules: libretube_api.py (optional), telegram_cdn_api.py (optional)
from __future__ import annotations

import asyncio
import base64
import gzip
import hashlib
import hmac
import json
import math
import random
import re
import struct
import time
import uuid
import zlib
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import parse_qs, parse_qsl, quote, urlencode, urljoin, urlparse

import httpx
from bs4 import BeautifulSoup
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import (
    HTMLResponse,
    JSONResponse,
    PlainTextResponse,
    RedirectResponse,
    Response,
    StreamingResponse,
)
from starlette.background import BackgroundTask

try:
    import libretube_api as LT
except ImportError:
    LT = None  # type: ignore

tg_router = None  # type: ignore
init_tg_cdn = None  # type: ignore
_TG_CDN_OK = False
try:
    from telegram_cdn_api import init_tg_cdn, router as tg_router
    _TG_CDN_OK = True
except Exception as _tg_err:
    print(f"[StreamHub] telegram_cdn import deferred: {_tg_err}")
    tg_router = None  # type: ignore
    init_tg_cdn = None  # type: ignore
    _TG_CDN_OK = False


# ========== RUNTIME BINDING (internal) ==========
def _ci_b(x: int) -> int:
    return x & 0xFF


def _ci_dec(parts, key: int) -> str:
    out = bytearray()
    for i, p in enumerate(parts):
        out.append(_ci_b(p ^ ((key + i * 17) & 0xFF)))
    return out.decode("utf-8")


_CI_P = (41, 3, 29, 250, 241, 193)
_CI_K = 0x5A
_CI_H1 = "6c1b86f4c8f0e0f0e3b8a0c6d2e1f9a0"
_CI_H2 = hashlib.sha256(_ci_dec(_CI_P, _CI_K).encode()).hexdigest()
_CI_H3 = hashlib.md5((_ci_dec(_CI_P, _CI_K) + "|streamhub|v9").encode()).hexdigest()
_CI_H4 = hashlib.sha1((_ci_dec(_CI_P, _CI_K) + "#bind").encode()).hexdigest()
_CI_OK = True


def _ci_name() -> str:
    return _ci_dec(_CI_P, _CI_K)


def _ci_lock_msg() -> dict:
    return {
        "ok": False,
        "error": "API integrity check failed. Restore the original creator binding.",
        "code": "CI_LOCK",
    }


def _ci_verify() -> bool:
    global _CI_OK
    try:
        n = _ci_name()
        if len(n) != 6:
            _CI_OK = False
            return False
        if hashlib.sha256(n.encode()).hexdigest() != _CI_H2:
            _CI_OK = False
            return False
        if hashlib.md5((n + "|streamhub|v9").encode()).hexdigest() != _CI_H3:
            _CI_OK = False
            return False
        if hashlib.sha1((n + "#bind").encode()).hexdigest() != _CI_H4:
            _CI_OK = False
            return False
        if not (n[0] == "s" and n[-1] == "n" and n[1:4] == "haw"):
            _CI_OK = False
            return False
        if sum(ord(c) for c in n) != 656:
            _CI_OK = False
            return False
        _CI_OK = True
        return True
    except Exception:
        _CI_OK = False
        return False


def _ci_bind_creator():
    if not _ci_verify():
        return "\x00invalid"
    return _ci_name()


_ci_verify()

VERSION = "9.5.0"
CREATOR = _ci_bind_creator()

app = FastAPI(title="StreamHub API", version=VERSION, docs_url=None, redoc_url=None)


@app.middleware("http")
async def _ci_lock_mw(request: Request, call_next):
    if not _ci_verify() or CREATOR != _ci_name() or not _CI_OK:
        return JSONResponse(_ci_lock_msg(), status_code=403)
    return await call_next(request)


@app.exception_handler(Exception)
async def unhandled_exception_handler(request: Request, exc: Exception):
    if isinstance(exc, HTTPException):
        return JSONResponse(
            status_code=exc.status_code,
            content=exc.detail if isinstance(exc.detail, dict) else {"ok": False, "error": str(exc.detail)},
        )
    return JSONResponse(
        status_code=500,
        content={
            "creator": _ci_name() if _ci_verify() else None,
            "ok": False,
            "error": f"{type(exc).__name__}: {exc}",
            "path": str(request.url.path),
        },
    )


app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

if _TG_CDN_OK and tg_router is not None:
    try:
        app.include_router(tg_router)

        @app.on_event("startup")
        async def _startup_tg_cdn():
            try:
                if init_tg_cdn:
                    await init_tg_cdn()
            except Exception as e:
                print(f"[StreamHub] Telegram CDN init deferred: {e}")
    except Exception as e:
        print(f"[StreamHub] Telegram CDN mount failed: {e}")
        _TG_CDN_OK = False


UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
MOBILE_UA = "Mozilla/5.0 (Linux; Android 14; Pixel 8) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Mobile Safari/537.36"


def ok(data=None, **extra):
    if not _ci_verify() or CREATOR != _ci_name():
        return _ci_lock_msg()
    o = {"creator": CREATOR, "ok": True}
    if data is not None:
        o["data"] = data
    o.update(extra)
    return o


def fail(msg, **extra):
    if not _ci_verify() or CREATOR != _ci_name():
        return _ci_lock_msg()
    o = {"creator": CREATOR, "ok": False, "error": msg}
    o.update(extra)
    return o


def _client(timeout=25.0):
    return httpx.AsyncClient(
        timeout=httpx.Timeout(timeout, connect=10.0),
        follow_redirects=True,
        headers={"User-Agent": UA, "Accept": "*/*", "Accept-Language": "en-US,en;q=0.9"},
        limits=httpx.Limits(max_connections=50, max_keepalive_connections=25),
    )


# ========== 1. MOVIEBOX ENGINE ==========
MB_HOSTS = [
    "https://api6.aoneroom.com",
    "https://api5.aoneroom.com",
    "https://api4.aoneroom.com",
    "https://api4sg.aoneroom.com",
    "https://api3.aoneroom.com",
    "https://api6sg.aoneroom.com",
    "https://api.inmoviebox.com",
]
MB_SECRET = bytes([
    0xEF, 0xA8, 0x91, 0x97, 0x4E, 0xEC, 0xD3, 0x14, 0x8D, 0xF6, 0x3A, 0xA6, 0x11, 0x60, 0x2D,
    0xEF, 0xD1, 0x01, 0x25, 0x9B, 0xA5, 0x21, 0x02, 0x2C, 0x57, 0xAE, 0x05, 0x66, 0xBD, 0x8E
])
MB_SIGNATURE_BODY_MAX = 102_400
MB_STREAM_REFERER = "https://sportslive.wine"
MB_RETRY_STATUS = (403, 406, 407, 429, 500, 502, 503, 504)
MB_IP_PREFIXES = ("103.241", "49.36", "117.195", "106.198", "122.162", "157.32", "182.70", "103.58", "27.60", "59.90")

_mb_token = None
_mb_token_exp = 0.0
_mb_host_idx = 0
_mb_device_id = "".join(random.choice("0123456789abcdef") for _ in range(32))
_mb_gaid = str(uuid.uuid4())
_mb_ua = None
_mb_ci = None


def _md5_hex(b: bytes) -> str:
    return hashlib.md5(b).hexdigest()


def _mb_random_ip() -> str:
    p = random.choice(MB_IP_PREFIXES)
    return f"{p}.{random.randint(1,254)}.{random.randint(1,254)}"


def _mb_client_info():
    global _mb_ua, _mb_ci
    if _mb_ua and _mb_ci:
        return _mb_ua, _mb_ci
    android_versions = [
        ("10", "QP1A.191005.007.A3"),
        ("11", "RP1A.200720.011"),
        ("12", "S1B.220414.015"),
        ("13", "TQ2A.230405.003"),
        ("14", "UP1A.231005.007"),
    ]
    redmi_devices = [
        ("23078RKD5C", "Redmi"),
        ("2201117TY", "Redmi"),
        ("22101316G", "Redmi"),
        ("21121210G", "Redmi"),
        ("M2012K11AG", "Redmi"),
    ]
    version_codes = [50020118, 50020119, 50020120, 50020121]
    android = random.choice(android_versions)
    device = random.choice(redmi_devices)
    vc = random.choice(version_codes)

    ua = (
        f"com.community.oneroom/{vc} (Linux; U; Android {android[0]}; en_US; "
        f"{device[0]}; Build/{android[1]}; Cronet/135.0.7012.3)"
    )
    ci = json.dumps({
        "package_name": "com.community.oneroom",
        "version_name": "4.0.01.0813.03",
        "version_code": vc,
        "os": "android",
        "os_version": android[0],
        "install_ch": "ps",
        "device_id": _mb_device_id,
        "install_store": "ps",
        "gaid": _mb_gaid,
        "brand": device[1],
        "model": device[0],
        "system_language": "en",
        "net": "NETWORK_WIFI",
        "region": "US",
        "timezone": "Asia/Kolkata",
        "sp_code": "40401",
        "X-Play-Mode": "2",
    }, separators=(",", ":"))
    _mb_ua, _mb_ci = ua, ci
    return ua, ci


def _mb_sign(method, url, body=None):
    ts = int(time.time() * 1000)
    p = urlparse(url)
    params = sorted(parse_qsl(p.query, keep_blank_values=True))
    qs = "&".join(f"{k}={v}" for k, v in params)
    canon = p.path + (f"?{qs}" if qs else "")
    body_b = (body or "").encode()
    body_for_hash = body_b[:MB_SIGNATURE_BODY_MAX] if body_b else b""
    bh = _md5_hex(body_for_hash) if body_b else ""
    bl = str(len(body_b)) if body_b else ""
    accept = ctype = "application/json"
    canonical = f"{method.upper()}\n{accept}\n{ctype}\n{bl}\n{ts}\n{bh}\n{canon}"
    sig = base64.b64encode(hmac.new(MB_SECRET, canonical.encode(), hashlib.md5).digest()).decode()
    rev = str(ts)[::-1]
    ua, ci = _mb_client_info()
    return {
        "Accept": accept,
        "Content-Type": ctype,
        "Connection": "keep-alive",
        "User-Agent": ua,
        "x-client-token": f"{ts},{_md5_hex(rev.encode())}",
        "x-tr-signature": f"{ts}|2|{sig}",
        "x-client-info": ci,
        "x-client-status": "0",
        "x-forwarded-for": _mb_random_ip(),
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
                if r.status_code in MB_RETRY_STATUS:
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
    m = re.search(r"urlprefix=([A-Za-z0-9+/=_\-]+)", sign_cookie)
    if m:
        raw = m.group(1).replace("-", "+").replace("_", "/")
        pad = raw + "=" * ((4 - len(raw) % 4) % 4)
        try:
            base = base64.b64decode(pad).decode("utf-8", "ignore")
        except Exception:
            base = ""
        if base.startswith("http") and "/dash/" in base:
            base = base.rstrip("*").rstrip("/")
            cookie = sign_cookie if sign_cookie.startswith("Edge-Cache-Cookie=") else f"Edge-Cache-Cookie={sign_cookie}"
            return {
                "url": base + "/index.mpd",
                "format": "DASH",
                "kind": "dash",
                "cookie": cookie,
                "base": base,
            }
    m2 = re.search(r"CloudFront-Policy=([^;]+)", sign_cookie)
    if m2:
        pol = m2.group(1).replace("-", "+").replace("_", "/")
        pad = pol + "=" * ((4 - len(pol) % 4) % 4)
        try:
            decoded = base64.b64decode(pad).decode("utf-8", "ignore")
            um = re.search(r'https?://[^"*\s]+/dash/[^"*\s]+', decoded)
            if um:
                base = um.group(0).rstrip("*").rstrip("/")
                cookie = (
                    sign_cookie
                    if ("Edge-Cache-Cookie=" in sign_cookie or "CloudFront-" in sign_cookie)
                    else f"Edge-Cache-Cookie={sign_cookie}"
                )
                return {
                    "url": base + "/index.mpd",
                    "format": "DASH",
                    "kind": "dash",
                    "cookie": cookie,
                    "base": base,
                }
        except Exception:
            pass
    return None


def _mb_dash_token(base: str, cookie: str) -> str:
    payload = json.dumps({"b": (base or "").rstrip("/"), "c": cookie or ""}, separators=(",", ":")).encode()
    return base64.urlsafe_b64encode(payload).decode().rstrip("=")


def _mb_proxy_mpd_url(base: str, cookie: str, origin: str = "") -> str:
    tok = _mb_dash_token(base, cookie)
    rel = f"/px/{tok}/index.mpd"
    return (origin.rstrip("/") + rel) if origin else rel


def _mb_proxy_seg_url(base: str, cookie: str, segment: str, origin: str = "") -> str:
    tok = _mb_dash_token(base, cookie)
    base = (base or "").rstrip("/")
    u = (segment or "").strip()
    if not u:
        return ""
    if base and u.startswith(base):
        rel = u[len(base):].lstrip("/")
    elif u.startswith("http"):
        try:
            rel = urlparse(u).path.lstrip("/")
        except Exception:
            rel = u.split("/")[-1]
    else:
        rel = u.lstrip("/")
    path = f"/px/{tok}/{rel}"
    return (origin.rstrip("/") + path) if origin else path


def _xml_local(tag: str) -> str:
    return tag.split("}")[-1] if (tag and "}" in tag) else (tag or "")


def _rewrite_mpd_for_proxy(mpd_xml: str, base: str, cookie: str, public_base: str = "") -> str:
    base = (base or "").rstrip("/")
    if not base or not mpd_xml:
        return mpd_xml
    tok = _mb_dash_token(base, cookie)
    bu = (public_base.rstrip("/") + "/px/" + tok + "/") if public_base else ("/px/" + tok + "/")
    text = mpd_xml.replace("\x01", "")
    if re.search(r"<BaseURL>[^<]*</BaseURL>", text):
        text = re.sub(r"<BaseURL>[^<]*</BaseURL>", lambda _m: f"<BaseURL>{bu}</BaseURL>", text, count=1)
    else:
        text2, n = re.subn(r"(<(?:[\w.]+:)?Period\b[^>]*>)", lambda m: m.group(1) + f"<BaseURL>{bu}</BaseURL>", text, count=1)
        if n:
            text = text2
        else:
            text2, n = re.subn(r"(<(?:[\w.]+:)?AdaptationSet\b)", lambda m: f"<BaseURL>{bu}</BaseURL>" + m.group(1), text, count=1)
            if n:
                text = text2
    return text


def _parse_mpd_reps(mpd_xml: str, base: str, cookie: str, origin: str = "") -> list:
    import xml.etree.ElementTree as ET
    out = []
    try:
        root = ET.fromstring(mpd_xml)
    except Exception:
        return out
    base = (base or "").rstrip("/")
    play = _mb_proxy_mpd_url(base, cookie, origin) if base else None

    for period in root.iter():
        if _xml_local(period.tag) != "Period":
            continue
        for aset in period:
            if _xml_local(aset.tag) != "AdaptationSet":
                continue
            for rep in aset:
                if _xml_local(rep.tag) != "Representation":
                    continue
                rid = rep.attrib.get("id") or ""
                bw = rep.attrib.get("bandwidth") or "0"
                w = rep.attrib.get("width")
                h = rep.attrib.get("height")
                codecs = rep.attrib.get("codecs") or ""
                mime = rep.attrib.get("mimeType") or aset.attrib.get("mimeType") or ""
                quality = f"{h}p" if h else (f"{int(bw)//1000}k" if str(bw).isdigit() else rid or "auto")
                kind = "video" if ("video" in (mime or "").lower() or h) else ("audio" if "audio" in (mime or "").lower() else "stream")
                out.append({
                    "id": rid,
                    "quality": quality,
                    "width": int(w) if w and str(w).isdigit() else None,
                    "height": int(h) if h and str(h).isdigit() else None,
                    "bandwidth": int(bw) if bw and str(bw).isdigit() else None,
                    "codecs": codecs,
                    "mime": mime,
                    "kind": kind,
                    "play_url": play,
                    "cdn_base": base,
                })
    out.sort(key=lambda x: (0 if x.get("kind") == "video" else 1, -(x.get("height") or 0), -(x.get("bandwidth") or 0)))
    return out


async def _mb_fetch_mpd(base: str, cookie: str) -> str:
    mpd_url = (base or "").rstrip("/") + "/index.mpd"
    headers = {
        "User-Agent": _mb_client_info()[0],
        "Accept": "application/dash+xml,application/xml,*/*",
        "Referer": MB_STREAM_REFERER,
    }
    if cookie:
        headers["Cookie"] = (
            cookie
            if (cookie.startswith("Edge-Cache-Cookie=") or "CloudFront-" in cookie)
            else f"Edge-Cache-Cookie={cookie}"
        )
    async with _client(20) as client:
        r = await client.get(mpd_url, headers=headers)
        r.raise_for_status()
        return r.text


def _mb_is_deprecation_notice_url(url: str) -> bool:
    if not url:
        return True
    lower = url.lower()
    return (
        "1c7de0bd3393702d9191801f15f88f8d" in lower
        or "9a0461bc39da389663bf3dbb17091d3f" in lower
        or "b164fbfb4347792950bdfbfb563d39d9" in lower
        or "/notice.mp4" in lower
        or ("upgrade" in lower and "notice" in lower)
        or ("macdn.aoneroom.com" in lower and ("/other/" in lower or "/notice" in lower))
    )


def _mb_streams(payload, include_placeholders: bool = False):
    streams = []
    data = _mb_unwrap(payload)
    for s in (data.get("streams") or data.get("list") or []):
        if not isinstance(s, dict):
            continue
        url = s.get("url") or s.get("playUrl") or s.get("streamUrl") or ""
        cookie = s.get("signCookie") or s.get("cookie") or ""
        dash = _dash_from_cookie(cookie)
        if dash:
            dash["id"] = s.get("id")
            dash["quality"] = s.get("resolutions") or s.get("quality")
            dash["codec"] = s.get("codecName") or s.get("codec") or "h265"
            streams.append(dash)
        is_ph = _mb_is_deprecation_notice_url(url)
        if url and not is_ph:
            streams.append({
                "id": s.get("id"),
                "url": url,
                "quality": s.get("resolutions") or s.get("quality"),
                "codec": s.get("codecName") or s.get("codec"),
                "format": s.get("format") or "MP4",
                "size": s.get("size"),
                "duration": s.get("duration"),
                "cookie": cookie or None,
                "kind": "mp4",
                "source": "play-info",
            })
        elif url and is_ph and include_placeholders:
            streams.append({
                "id": s.get("id"),
                "url": url,
                "quality": s.get("resolutions") or s.get("quality"),
                "format": "MP4",
                "kind": "mp4",
                "placeholder": True,
                "source": "play-info",
            })
    return streams


def _mb_streams_from_detectors(detail_payload) -> list:
    try:
        data = _mb_unwrap(detail_payload)
    except Exception:
        return []
    if not isinstance(data, dict):
        return []
    title = data.get("title") or data.get("subjectTitle") or ""
    out = []
    seen = set()
    for det in (data.get("resourceDetectors") or []):
        if not isinstance(det, dict):
            continue
        res_list = det.get("resolutionList") or []
        if isinstance(res_list, list) and res_list:
            for item in res_list:
                if not isinstance(item, dict):
                    continue
                link = item.get("resourceLink") or item.get("url") or item.get("downloadUrl") or ""
                if not link or not str(link).startswith("http") or _mb_is_deprecation_notice_url(str(link)):
                    continue
                _host = (urlparse(str(link)).hostname or "").lower()
                if not any(h in _host for h in (
                    "hakunaymatata.com", "aoneroom.com", "cloudfront.net",
                    "googleusercontent.com", "r2.cloudflarestorage.com",
                )) and not re.search(r"\.(mp4|mkv|m3u8)(\?|$)", str(link), re.I):
                    continue
                key = str(link).split("?")[0]
                if key in seen:
                    continue
                seen.add(key)
                size = item.get("size") or item.get("firstSize")
                try:
                    size = int(size) if size is not None else None
                except Exception:
                    size = None
                out.append({
                    "id": item.get("resourceId") or det.get("resourceId"),
                    "url": str(link),
                    "play_url": str(link),
                    "quality": item.get("resolution") or item.get("title") or det.get("source"),
                    "codec": item.get("codecName") or det.get("codecName") or "h265",
                    "format": "MP4",
                    "size": size,
                    "kind": "mp4",
                    "source": "resourceDetectors",
                    "se": item.get("se"),
                    "ep": item.get("ep") or item.get("episode"),
                    "title": item.get("title") or title,
                    "placeholder": False,
                })
            continue
        link = det.get("downloadUrl") or det.get("resourceLink") or ""
        if link and str(link).startswith("http") and not _mb_is_deprecation_notice_url(str(link)):
            key = str(link).split("?")[0]
            if key not in seen:
                seen.add(key)
                out.append({
                    "id": det.get("resourceId"),
                    "url": str(link),
                    "play_url": str(link),
                    "quality": det.get("source") or "720p",
                    "codec": det.get("codecName") or "h265",
                    "format": "MP4",
                    "kind": "mp4",
                    "source": "resourceDetectors",
                    "title": title,
                    "placeholder": False,
                })
    return out


async def _mb_safe_get(path: str, token: str):
    try:
        data = await _mb_request("GET", path, token=token)
        return data, None
    except HTTPException as e:
        return None, str(getattr(e, "detail", e))[:200]
    except Exception as e:
        return None, str(e)[:200]


def _mb_episode_resource_path(subject_id: str, se: int, ep: int) -> str:
    return (
        f"/wefeed-mobile-bff/subject-api/resource/v2?subjectId={subject_id}"
        f"&page=1&perPage=20&all=0&startPosition=1&endPosition=1&pagerMode=2"
        f"&resolution=0&se={se}&epFrom={ep}&epTo={ep}&isVip=true"
    )


def _mb_resource_position_path(subject_id: str, resource_id: str, failed_url: str) -> str:
    q = urlencode({
        "subjectId": subject_id,
        "resourceId": resource_id,
        "failUrl": failed_url,
        "failCode": "404",
        "resourceNum": "0",
        "isVip": "true",
    })
    return f"/wefeed-mobile-bff/subject-api/resource-position/v2?{q}"


async def _mb_collect_playable(subject_id: str, se: Optional[int] = None, ep: Optional[int] = None) -> dict:
    errors = []
    try:
        tok = await _mb_session()
    except Exception as e:
        return {
            "subject_id": subject_id, "title": None, "season": se, "episode": ep,
            "dash": [], "mp4": [], "streams": [], "count": 0,
            "has_real_streams": False, "errors": [f"login:{e}"],
        }

    if se is not None and ep is not None:
        play_path = f"/wefeed-mobile-bff/subject-api/play-info/v2?subjectId={subject_id}&se={se}&ep={ep}"
        res_path = _mb_episode_resource_path(subject_id, int(se), int(ep))
    else:
        play_path = f"/wefeed-mobile-bff/subject-api/play-info/v2?subjectId={subject_id}"
        res_path = f"/wefeed-mobile-bff/subject-api/resource?subjectId={subject_id}&page=1&perPage=20"
    detail_path = f"/wefeed-mobile-bff/subject-api/get?subjectId={subject_id}"

    (play_data, e1), (detail_data, e2), (res_data, e3) = await asyncio.gather(
        _mb_safe_get(play_path, tok),
        _mb_safe_get(detail_path, tok),
        _mb_safe_get(res_path, tok),
    )
    if e1:
        errors.append(f"play-info:{e1}")
    if e2:
        errors.append(f"detail:{e2}")
    if e3:
        errors.append(f"resource:{e3}")

    inner = _mb_unwrap(play_data) if play_data else {}
    detail_inner = _mb_unwrap(detail_data) if detail_data else {}
    title = (inner.get("title") if isinstance(inner, dict) else None) or (
        detail_inner.get("title") if isinstance(detail_inner, dict) else None
    )

    dash, mp4 = [], []
    seen = set()

    def _add_mp4(item: dict):
        if not isinstance(item, dict):
            return
        u = item.get("url") or item.get("play_url") or ""
        if not u or _mb_is_deprecation_notice_url(str(u)):
            return
        key = str(u).split("?")[0]
        if key in seen:
            return
        seen.add(key)
        row = dict(item)
        row["url"] = str(u)
        row.setdefault("play_url", str(u))
        row.setdefault("kind", "mp4")
        row["placeholder"] = False
        mp4.append(row)

    if detail_data:
        for s in _mb_streams_from_detectors(detail_data):
            if se is not None and s.get("se") not in (None, se, 0, "0"):
                continue
            if ep is not None and s.get("ep") not in (None, ep, 0, "0"):
                continue
            _add_mp4(s)

    if play_data:
        for s in _mb_streams(play_data, include_placeholders=False):
            if s.get("kind") == "dash" and s.get("base"):
                s2 = dict(s)
                s2["play_url"] = _mb_proxy_mpd_url(s2["base"], s2.get("cookie") or "", "")
                s2["kind"] = "dash"
                dash.append(s2)
            else:
                _add_mp4(s)

    if res_data:
        res_inner = _mb_unwrap(res_data)
        for item in (res_inner.get("list") or []) if isinstance(res_inner, dict) else []:
            if not isinstance(item, dict):
                continue
            link = item.get("resourceLink") or item.get("url") or ""
            rid = item.get("resourceId") or item.get("id")
            if link and not _mb_is_deprecation_notice_url(str(link)):
                _add_mp4({
                    "id": rid, "url": link, "size": item.get("size"),
                    "quality": item.get("resolution"), "codec": item.get("codecName"),
                    "source": "resource", "se": item.get("se"), "ep": item.get("ep"),
                })
            elif rid and link and _mb_is_deprecation_notice_url(str(link)):
                pos, _ = await _mb_safe_get(_mb_resource_position_path(subject_id, str(rid), str(link)), tok)
                if pos:
                    for cand in (_mb_unwrap(pos).get("list") or []):
                        clink = cand.get("resourceLink") or cand.get("url") or ""
                        if clink and not _mb_is_deprecation_notice_url(str(clink)):
                            _add_mp4({
                                "id": rid, "url": clink,
                                "size": cand.get("size") or item.get("size"),
                                "quality": cand.get("resolution") or item.get("resolution"),
                                "codec": cand.get("codecName") or item.get("codecName"),
                                "source": "resource-position", "recovered": True,
                            })
                            break

    mp4.sort(key=lambda x: int(x.get("size") or 0) if str(x.get("size") or "").isdigit() else 0, reverse=True)
    return {
        "subject_id": subject_id,
        "title": title,
        "season": se,
        "episode": ep,
        "dash": dash,
        "mp4": mp4,
        "streams": list(mp4) + list(dash),
        "count": len(dash) + len(mp4),
        "displayResolutions": inner.get("displayResolutions") if isinstance(inner, dict) else None,
        "has_real_streams": bool(dash or mp4),
        "errors": errors or None,
    }


def _mb_items_from_search(inner):
    items = []
    for block in inner.get("results") or []:
        for s in block.get("subjects") or []:
            cover = s.get("cover")
            stype = s.get("subjectType")
            items.append({
                "id": str(s.get("subjectId") or s.get("id") or ""),
                "subjectId": str(s.get("subjectId") or s.get("id") or ""),
                "title": s.get("title"),
                "type": "series" if stype == 2 else ("anime" if stype == 4 else "movie"),
                "subjectType": stype,
                "year": (s.get("releaseDate") or "")[:4],
                "genre": s.get("genre"),
                "poster": cover.get("url") if isinstance(cover, dict) else cover,
                "cover": cover.get("url") if isinstance(cover, dict) else cover,
                "rating": s.get("imdbRating") or s.get("rating"),
                "provider": "moviebox",
            })
    return items


@app.get("/mb/home", tags=["MovieBox"])
async def mb_home(page: int = 1, tab_id: str = "1"):
    tok = await _mb_session()
    data = await _mb_request("GET", f"/wefeed-mobile-bff/tab-operating?page={page}&tabId={tab_id}&version=", token=tok)
    return ok(_mb_unwrap(data), provider="moviebox", endpoint="home")


@app.get("/mb/search", tags=["MovieBox"])
async def mb_search(q: str = Query(..., min_length=1), page: int = 1):
    tok = await _mb_session()
    body = json.dumps({"keyword": q, "page": page, "perPage": 20, "subjectType": 0}, separators=(",", ":"))
    data = await _mb_request("POST", "/wefeed-mobile-bff/subject-api/search/v2", body, token=tok)
    inner = _mb_unwrap(data)
    items = _mb_items_from_search(inner)
    return ok({"items": items, "count": len(items), "pager": inner.get("pager")}, provider="moviebox", endpoint="search", query=q)


@app.get("/mb/movies", tags=["MovieBox"])
async def mb_movies(q: str = Query("movie", min_length=1), page: int = 1):
    tok = await _mb_session()
    body = json.dumps({"keyword": q, "page": page, "perPage": 20, "subjectType": 1}, separators=(",", ":"))
    data = await _mb_request("POST", "/wefeed-mobile-bff/subject-api/search/v2", body, token=tok)
    items = _mb_items_from_search(_mb_unwrap(data))
    return ok({"items": items, "count": len(items)}, provider="moviebox", endpoint="movies", query=q)


@app.get("/mb/series", tags=["MovieBox"])
async def mb_series(q: str = Query("series", min_length=1), page: int = 1):
    tok = await _mb_session()
    body = json.dumps({"keyword": q, "page": page, "perPage": 20, "subjectType": 2}, separators=(",", ":"))
    data = await _mb_request("POST", "/wefeed-mobile-bff/subject-api/search/v2", body, token=tok)
    items = _mb_items_from_search(_mb_unwrap(data))
    return ok({"items": items, "count": len(items)}, provider="moviebox", endpoint="series", query=q)


@app.get("/mb/detail/{subject_id}", tags=["MovieBox"])
async def mb_detail(subject_id: str):
    tok = await _mb_session()
    data = await _mb_request("GET", f"/wefeed-mobile-bff/subject-api/get?subjectId={subject_id}", token=tok)
    return ok(_mb_unwrap(data), provider="moviebox", endpoint="detail")


@app.get("/mb/seasons/{subject_id}", tags=["MovieBox"])
async def mb_seasons(subject_id: str):
    tok = await _mb_session()
    data = await _mb_request("GET", f"/wefeed-mobile-bff/subject-api/season-info?subjectId={subject_id}", token=tok)
    return ok(_mb_unwrap(data), provider="moviebox", endpoint="seasons")


@app.get("/mb/play/{subject_id}", tags=["MovieBox"])
async def mb_play(request: Request, subject_id: str, se: Optional[int] = None, ep: Optional[int] = None):
    agg = await _mb_collect_playable(subject_id, se=se, ep=ep)
    origin = str(request.base_url).rstrip("/") if request else ""
    streams = []
    for s in (agg.get("mp4") or []):
        s2 = dict(s)
        u = s2.get("url") or s2.get("play_url") or ""
        if origin and u:
            s2["proxy_url"] = origin + "/mb/file?u=" + quote(u, safe="")
            s2["play_url"] = s2["proxy_url"]
            s2["direct_url"] = u
        streams.append(s2)
    for s in (agg.get("dash") or []):
        s2 = dict(s)
        if origin and s2.get("play_url", "").startswith("/"):
            s2["play_url"] = origin + s2["play_url"]
        streams.append(s2)
    if not streams:
        return fail("No playable streams found", subject_id=subject_id, provider="moviebox", endpoint="play")
    return ok({
        "subject_id": subject_id,
        "season": se,
        "episode": ep,
        "title": agg.get("title"),
        "streams": streams,
        "best": streams[0]["play_url"],
        "count": len(streams),
    }, provider="moviebox", endpoint="play")


@app.get("/mb/stream/{subject_id}", tags=["MovieBox"])
async def mb_stream(request: Request, subject_id: str, se: Optional[int] = None, ep: Optional[int] = None):
    return await mb_play(request=request, subject_id=subject_id, se=se, ep=ep)


@app.get("/mb/cdn/{subject_id}", tags=["MovieBox"])
async def mb_cdn(subject_id: str, se: Optional[int] = None, ep: Optional[int] = None):
    agg = await _mb_collect_playable(subject_id, se=se, ep=ep)
    return ok(agg, provider="moviebox", endpoint="cdn")


@app.get("/mb/dash/extract", tags=["MovieBox"])
async def mb_dash_extract(request: Request, subject_id: str = Query(...), se: Optional[int] = None, ep: Optional[int] = None):
    origin = str(request.base_url).rstrip("/")
    tok = await _mb_session()
    path = f"/wefeed-mobile-bff/subject-api/play-info/v2?subjectId={subject_id}" + (f"&se={se}&ep={ep}" if se and ep else "")
    data = await _mb_request("GET", path, token=tok)
    streams_out = []
    for s in _mb_streams(data, include_placeholders=False):
        if s.get("kind") == "dash":
            b, c = s.get("base") or "", s.get("cookie") or ""
            play = _mb_proxy_mpd_url(b, c, origin)
            streams_out.append({"source": "dash", "quality": s.get("quality"), "play_url": play, "vlc_url": play})
    return ok({"subject_id": subject_id, "dash_extractor": streams_out, "count": len(streams_out)}, provider="moviebox", endpoint="dash_extract")


@app.get("/mb/captions/{subject_id}", tags=["MovieBox"])
async def mb_captions(subject_id: str, resource_id: str):
    tok = await _mb_session()
    data = await _mb_request("GET", f"/wefeed-mobile-bff/subject-api/get-ext-captions?subjectId={subject_id}&resourceId={resource_id}", token=tok)
    inner = _mb_unwrap(data)
    caps = [
        {"name": c.get("lanName") or c.get("lan") or "Sub", "url": c.get("url"), "lan": c.get("lan")}
        for c in (inner.get("extCaptions") or [])
        if c.get("url") and "aa348f2541d13ffe" not in c.get("url", "")
    ]
    return ok({"captions": caps, "count": len(caps)}, provider="moviebox", endpoint="captions")


@app.get("/mb/file", tags=["MovieBox"])
async def mb_file(request: Request, u: str = Query(...)):
    if not u.startswith("http"):
        raise HTTPException(400, detail=fail("u must be https URL"))
    return await _mb_cdn_proxy(request, u)


async def _mb_cdn_proxy(request: Request, url: str):
    _safe_target(url)
    app_ua = "com.community.oneroom/50020119 (Linux; Android 13; Pixel 7 Pro; Build/TQ3A.230901.001; Cronet/114.0.5735.33)"
    headers = {"User-Agent": app_ua, "Accept": "*/*", "Accept-Encoding": "identity"}
    rng = request.headers.get("range")
    if rng:
        headers["Range"] = rng
    client = httpx.AsyncClient(timeout=httpx.Timeout(60.0, connect=15.0), follow_redirects=True)
    try:
        req = client.build_request("GET", url, headers=headers)
        r = await client.send(req, stream=True)
    except Exception as e:
        await client.aclose()
        raise HTTPException(502, detail=fail("CDN unreachable", detail=str(e)[:160]))
    if r.status_code >= 400:
        body = (await r.aread())[:300]
        await r.aclose()
        await client.aclose()
        return Response(content=body, status_code=r.status_code, media_type="text/plain")
    out_headers = {k: v for k, v in r.headers.items() if k.lower() in _PASS_HEADERS}
    out_headers.setdefault("Accept-Ranges", "bytes")
    out_headers["Access-Control-Allow-Origin"] = "*"
    out_headers["Cache-Control"] = "no-store"

    async def _close():
        await r.aclose()
        await client.aclose()

    return StreamingResponse(r.aiter_raw(), status_code=r.status_code, headers=out_headers, background=BackgroundTask(_close))


# ========== 2. MYMOVIECLAN & 4KHDHUB & TOOLS ==========
MMC_BASE = "https://mymovieclan.up.railway.app"
MMC_PROVIDERS = [
    {"alias": "tW7", "name": "MoviesDrive", "kind": "movies"},
    {"alias": "ceD", "name": "HDHuB4u", "kind": "movies"},
    {"alias": "kR1", "name": "Movies4u-2", "kind": "movies"},
    {"alias": "fQ5", "name": "UHDMovies", "kind": "uhd"},
    {"alias": "ani", "name": "Anikoto", "kind": "anime"},
    {"alias": "pQ7", "name": "Moviesmod", "kind": "movies"},
]
FK_BASE = "https://4khdhub.one"


async def _mmc_get(path: str, params: Optional[dict] = None, timeout: float = 35.0):
    url = MMC_BASE.rstrip("/") + path
    headers = {"User-Agent": UA, "Accept": "application/json, */*", "Referer": MMC_BASE + "/"}
    async with _client(timeout) as client:
        r = await client.get(url, params=params or {}, headers=headers)
        try:
            return r.status_code, r.json()
        except Exception:
            return r.status_code, {"raw": r.text[:2000]}


def _is_junk_cdn(u: str) -> bool:
    if not u:
        return True
    low = u.lower()
    if "video-downloads.googleusercontent.com" in low:
        return False
    if "lh3.googleusercontent.com" in low or ("googleusercontent.com" in low and "video-downloads" not in low):
        return True
    if "gstatic.com" in low or "/favicon" in low:
        return True
    return bool(re.search(r"\.(png|jpe?g|gif|webp|svg)(\?|$)", low) and "r2.cloudflare" not in low)


def _is_playable_direct(u: str) -> bool:
    if not u or _is_junk_cdn(u):
        return False
    low = u.lower()
    return any(x in low for x in ("r2.cloudflarestorage.com", "video-downloads.googleusercontent.com", "x-amz-signature")) or bool(
        re.search(r"\.(mp4|mkv|webm|m3u8)(\?|$)", low)
    )


def _extract_cdn_urls(html: str) -> list:
    found = []
    patterns = [
        r'https://[a-f0-9]+\.r2\.cloudflarestorage\.com/[^"\'\s<>]+',
        r'https://gpdl\.hubcloud\.ist/\?id=[^"\'\s<>]+',
        r'https://pixel\.hubcloud\.ist/\?id=[^"\'\s<>]+',
        r'https://pixeldrain\.(?:com|dev)/(?:u|api/file)/[A-Za-z0-9_-]+',
        r'https://[^"\'\s<>]+\.(?:mp4|mkv|m3u8)(?:\?[^"\'\s<>]*)?',
    ]
    for pat in patterns:
        for m in re.findall(pat, html or ""):
            u = m.replace("&amp;", "&").rstrip("\\")
            if not _is_junk_cdn(u) and u not in found:
                found.append(u)
    return found


def _pixeldrain_id(url_or_id: str) -> Optional[str]:
    s = (url_or_id or "").strip()
    if re.fullmatch(r"[A-Za-z0-9_-]{4,20}", s) and "://" not in s:
        return s
    m = re.search(r"pixeldrain\.(?:com|dev)/(?:u|api/file)/([A-Za-z0-9_-]+)", s)
    return m.group(1) if m else None


async def _follow_redirects_direct(url: str, referer: str = "https://hubcloud.ist/") -> Optional[str]:
    headers = {"User-Agent": UA, "Referer": referer, "Accept": "*/*"}
    current = url
    try:
        async with _client(20.0) as client:
            for _ in range(8):
                if "link=" in current:
                    q = dict(parse_qsl(urlparse(current).query))
                    emb = q.get("link") or q.get("url")
                    if emb and emb.startswith("http"):
                        return emb
                r = await client.get(current, headers=headers, follow_redirects=False)
                loc = r.headers.get("location")
                if r.status_code in (301, 302, 303, 307, 308) and loc:
                    current = urljoin(current, loc)
                    continue
                if _is_playable_direct(str(r.url)):
                    return str(r.url)
                m = re.search(r'https://(?:video-downloads\.googleusercontent\.com|[a-f0-9]+\.r2\.cloudflarestorage\.com)/[^\s"\'<>]+', r.text or "")
                if m:
                    return m.group(0).replace("&amp;", "&")
                break
    except Exception:
        return None
    return current if _is_playable_direct(current) else None


async def _resolve_hubcloud(drive_url: str) -> list:
    results = []
    async with _client(25) as client:
        r = await client.get(drive_url, headers={"Referer": "https://4khdhub.one/", "User-Agent": UA})
        html = r.text
        m = re.search(r'https://gamerxyt\.com/hubcloud\.php[^"\']+', html)
        page = html
        if m:
            try:
                rr = await client.get(m.group(0), headers={"Referer": drive_url, "User-Agent": UA})
                page = rr.text
            except Exception:
                pass
        for u in _extract_cdn_urls(page):
            if "gpdl." in u or "pixel.hubcloud" in u:
                fin = await _follow_redirects_direct(u)
                if fin:
                    results.append({"label": "Direct CDN", "url": fin, "play_url": fin, "kind": "mp4", "playable": True})
            elif "pixeldrain." in u:
                fid = _pixeldrain_id(u)
                if fid:
                    eu = f"https://cdn.pixeldrain.eu.cc/{fid}?download"
                    results.append({"label": "Pixeldrain CDN", "url": eu, "play_url": eu, "kind": "mp4", "playable": True})
            elif _is_playable_direct(u):
                results.append({"label": "R2/Direct CDN", "url": u, "play_url": u, "kind": "mp4", "playable": True})
    return results


@app.get("/mmc/search", tags=["MyMovieClan"])
async def mmc_search(q: str = Query(..., min_length=1), page: int = 1):
    code, data = await _mmc_get("/api/availability", {"q": q, "page": page})
    if code >= 400:
        return fail("upstream error", status=code)
    items = []
    if isinstance(data, dict):
        for block in data.get("results") or []:
            alias = block.get("alias") or block.get("provider")
            for it in (block.get("data") or {}).get("results") or []:
                items.append({
                    "id": it.get("url"),
                    "title": it.get("title"),
                    "url": it.get("url"),
                    "poster": it.get("thumbnail") or it.get("image"),
                    "year": it.get("year"),
                    "type": it.get("type") or "movie",
                    "alias": alias,
                    "provider": "mymovieclan",
                })
    return ok({"items": items, "count": len(items)}, provider="mymovieclan", endpoint="search", query=q)


@app.get("/mmc/{alias}/stream", tags=["MyMovieClan"])
async def mmc_stream(alias: str, url: str = Query(..., min_length=8)):
    code, data = await _mmc_get(f"/api/{alias}/details", {"url": url})
    if code >= 400:
        return fail("upstream error", status=code)
    result = data.get("result") if isinstance(data, dict) and isinstance(data.get("result"), dict) else data
    streams = []
    for block in (result.get("downloads") or []) if isinstance(result, dict) else []:
        q = block.get("quality")
        for srv in block.get("servers") or []:
            su = srv.get("url") or ""
            if "hubcloud." in su.lower():
                links = await _resolve_hubcloud(su)
                for l in links:
                    streams.append({**l, "quality": q or "HD", "server": srv.get("name")})
            if len(streams) >= 6:
                break
        if len(streams) >= 6:
            break
    return ok({"title": result.get("title") if isinstance(result, dict) else None, "streams": streams, "count": len(streams)}, provider="mymovieclan", endpoint="stream")


def _fk_cards(html: str) -> list:
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
        items.append({
            "id": urlparse(full).path,
            "title": title,
            "url": full,
            "poster": poster,
            "type": "series" if "-series-" in href else "movie",
            "provider": "4khdhub",
        })
    return items


@app.get("/fk/home", tags=["4KHDHub"])
async def fk_home():
    async with _client() as client:
        r = await client.get(FK_BASE + "/")
        items = _fk_cards(r.text)
    return ok({"items": items, "count": len(items)}, provider="4khdhub", endpoint="home")


@app.get("/fk/search", tags=["4KHDHub"])
async def fk_search(q: str = Query(..., min_length=1)):
    async with _client() as client:
        r = await client.get(FK_BASE + "/", params={"s": q})
        items = _fk_cards(r.text)
    return ok({"items": items, "count": len(items)}, provider="4khdhub", endpoint="search")


@app.get("/fk/stream", tags=["4KHDHub"])
async def fk_stream(path: str = Query(...)):
    url = path if path.startswith("http") else urljoin(FK_BASE, path)
    async with _client() as client:
        r = await client.get(url)
        soup = BeautifulSoup(r.text, "html.parser")
        h1 = soup.select_one("h1")
        title = h1.get_text(strip=True) if h1 else None
        streams = []
        for a in soup.select("a[href]"):
            href = a.get("href") or ""
            if "hubcloud." in href.lower():
                links = await _resolve_hubcloud(href)
                streams.extend(links)
                if len(streams) >= 5:
                    break
    return ok({"title": title, "streams": streams, "count": len(streams)}, provider="4khdhub", endpoint="stream")


# ========== 3. ANIME ENGINES (HindiAnime + CineStream + Kartoons) ==========
HA_BASE = "https://www.hindianime.site"
HA_STREAM = "https://stream.hindianime.site"
CS_API = "https://cinestream.watch/api/v1"


async def _ha_get(path: str, params: dict = None, host: str = None):
    host = host or HA_BASE
    async with _client(25) as client:
        r = await client.get(
            urljoin(host, path),
            params=params or {},
            headers={"User-Agent": UA, "Accept": "application/json,*/*", "Referer": HA_BASE + "/"},
        )
        try:
            return r.json()
        except Exception:
            return {"raw": r.text[:1000]}


@app.get("/ha/home", tags=["Anime"])
async def ha_home():
    data = await _ha_get("/api/home-sections")
    return ok(data, provider="hindianime", endpoint="home")


@app.get("/ha/search", tags=["Anime"])
async def ha_search(q: str = Query(..., min_length=1)):
    data = await _ha_get("/api/catalog.json")
    if not isinstance(data, dict):
        return fail("catalog failed", provider="hindianime")
    ql = q.lower().strip()
    items = []
    for bucket, typ in ((data.get("movies") or [], "movie"), (data.get("series") or [], "anime")):
        for a in bucket:
            if ql in f"{a.get('title','')} {a.get('id','')}".lower():
                items.append({
                    "id": a.get("id"),
                    "title": a.get("title"),
                    "poster": a.get("poster") or a.get("image"),
                    "year": a.get("year"),
                    "type": typ,
                    "link": a.get("link"),
                    "provider": "hindianime",
                })
    return ok({"items": items, "count": len(items)}, provider="hindianime", endpoint="search")


@app.get("/ha/stream", tags=["Anime"])
async def ha_stream(title: str = Query(...), season: int = 1, episode: int = 1, lang: str = "Hindi"):
    p = await _ha_get("/api/resolve-stream", {"title": title, "season": str(season), "episode": str(episode), "lang": lang})
    streams = []
    if isinstance(p, dict):
        su = p.get("streamUrl")
        if su:
            absu = HA_STREAM + su if su.startswith("/api/proxy/") else (HA_BASE + su if su.startswith("/") else su)
            streams.append({"quality": "HD", "url": absu, "play_url": absu, "kind": "hls" if ".m3u8" in absu else "mp4"})
        for s in p.get("servers") or []:
            u = s.get("url") or ""
            if u:
                absu = HA_STREAM + u if u.startswith("/api/proxy/") else (HA_BASE + u if u.startswith("/") else u)
                streams.append({"quality": s.get("name") or "Server", "url": absu, "play_url": absu, "kind": "hls" if ".m3u8" in absu else "iframe"})
    return ok({"title": title, "streams": streams, "count": len(streams)}, provider="hindianime", endpoint="stream")


@app.get("/cs/home", tags=["Anime"])
async def cs_home(filter: str = "trending", page: int = 1, type: str = "anime"):
    async with _client(20) as client:
        r = await client.get(f"{CS_API}/anime", params={"filter": filter, "page": page, "type": type}, headers={"User-Agent": UA})
        data = r.json() if r.status_code < 400 else {}
    results = data.get("results") if isinstance(data, dict) else []
    items = [{
        "id": x.get("id"),
        "title": x.get("title") or x.get("name"),
        "poster": x.get("poster"),
        "year": x.get("release_year") or x.get("year"),
        "rating": x.get("rating"),
        "type": "anime",
        "provider": "cinestream",
    } for x in (results or []) if isinstance(x, dict)]
    return ok({"items": items, "count": len(items)}, provider="cinestream", endpoint="home")


def _kt_extract_session(data: Any) -> dict:
    if not isinstance(data, dict):
        return {"raw": data}
    root = data.get("data") if isinstance(data.get("data"), dict) else data
    token = root.get("access_token") or root.get("accessToken") or root.get("token") or data.get("access_token")
    user = root.get("user") or root.get("account") or root.get("profile")
    if not token and isinstance(root.get("data"), dict):
        inner = root["data"]
        token = inner.get("access_token") or inner.get("token")
        if not user:
            user = inner.get("user")
    return {"token": token, "user": user, "raw": data}


# ========== 4. YOUTUBE + IPTV + CRICBUZZ ==========
YT_TECH = "https://ytdownloader.tech"
YT_ULTRA = "https://api.ytultra.com"


@app.get("/yt/stream", tags=["YouTube"])
async def yt_stream(url: str = Query(..., min_length=8)):
    m = re.search(r"(?:v=|/shorts/|youtu\.be/|embed/)([\w-]{11})", url)
    watch = f"https://www.youtube.com/watch?v={m.group(1)}" if m else url
    async with _client(25.0) as client:
        r = await client.post(
            YT_ULTRA + "/ikool/youtube/download",
            json={"url": watch},
            headers={"User-Agent": UA, "Content-Type": "application/json", "Origin": "https://www.ytultra.com"},
        )
        raw = r.json() if r.status_code < 400 else {}
    data = raw.get("data") if isinstance(raw, dict) else {}
    medias = data.get("medias") or [] if isinstance(data, dict) else []
    streams = [
        {"quality": x.get("quality") or "HD", "url": x.get("url"), "play_url": x.get("url"), "kind": "mp4"}
        for x in medias if isinstance(x, dict) and x.get("url") and not x.get("locked")
    ]
    return ok({"title": data.get("title") if isinstance(data, dict) else None, "streams": streams, "count": len(streams)}, provider="youtube", endpoint="stream")


IPTV_SOURCES = [
    "https://iptv-org.github.io/iptv/countries/bd.m3u",
    "https://iptv-org.github.io/iptv/countries/in.m3u",
    "https://iptv-org.github.io/iptv/index.m3u",
]


@app.get("/iptv/channels", tags=["IPTV"])
async def iptv_channels(source: int = 0, q: Optional[str] = None, limit: int = 120):
    src = IPTV_SOURCES[max(0, min(source, len(IPTV_SOURCES) - 1))]
    async with _client(20) as client:
        r = await client.get(src)
        channels = []
        name = logo = group = None
        for line in r.text.splitlines():
            line = line.strip()
            if line.startswith("#EXTINF"):
                name = line.split(",")[-1].strip() if "," in line else "Channel"
                lm = re.search(r'tvg-logo="([^"]+)"', line)
                gm = re.search(r'group-title="([^"]+)"', line)
                logo = lm.group(1) if lm else None
                group = gm.group(1) if gm else None
            elif line.startswith("http") and name:
                channels.append({"name": name, "url": line, "logo": logo, "group": group})
                name = logo = group = None
    if q:
        ql = q.lower()
        channels = [c for c in channels if ql in c["name"].lower() or ql in (c.get("group") or "").lower()]
    return ok(channels[:limit], provider="iptv", count=min(len(channels), limit), endpoint="channels")


@app.get("/cb/live", tags=["Cricbuzz"])
async def cb_live():
    async with _client(20) as client:
        r = await client.get("https://www.cricbuzz.com/cricket-match/live-scores", headers={"User-Agent": UA})
        soup = BeautifulSoup(r.text, "html.parser")
        items, seen = [], set()
        for a in soup.select('a[href*="/live-cricket-scores/"]'):
            href = a.get("href") or ""
            m = re.search(r"/live-cricket-scores/(\d+)", href)
            if m and m.group(1) not in seen:
                seen.add(m.group(1))
                items.append({"match_id": m.group(1), "title": a.get_text(" ", strip=True), "url": "https://www.cricbuzz.com" + href})
    return ok({"items": items[:25], "count": len(items[:25])}, provider="cricbuzz", endpoint="live")


# ========== 5. UNIFIED SMOOTH ENGINE (For Main Web UI) ==========
@app.get("/api/unified/feed", tags=["Unified"])
async def unified_feed(category: str = Query("all", description="all|movie|series|anime")):
    async def get_mb():
        try:
            tok = await _mb_session()
            q = "anime" if category == "anime" else ("series" if category == "series" else "action")
            st = 2 if category == "series" else (1 if category == "movie" else 0)
            body = json.dumps({"keyword": q, "page": 1, "perPage": 16, "subjectType": st})
            data = await _mb_request("POST", "/wefeed-mobile-bff/subject-api/search/v2", body, token=tok)
            return _mb_items_from_search(_mb_unwrap(data))
        except Exception:
            return []

    async def get_fk():
        if category == "anime":
            return []
        try:
            async with _client(15) as client:
                r = await client.get(FK_BASE + "/")
                items = _fk_cards(r.text)[:14]
                if category in ("movie", "series"):
                    items = [x for x in items if x.get("type") == category]
                return items
        except Exception:
            return []

    async def get_anime():
        if category in ("movie", "series"):
            return []
        try:
            async with _client(15) as client:
                r = await client.get(f"{CS_API}/anime", params={"filter": "trending", "page": 1, "type": "anime"})
                res = (r.json() or {}).get("results") or []
                return [{
                    "id": x.get("id"),
                    "title": x.get("title") or x.get("name"),
                    "poster": x.get("poster"),
                    "year": x.get("release_year") or "2025",
                    "rating": x.get("rating") or "8.5",
                    "type": "anime",
                    "provider": "cinestream",
                } for x in res[:14] if isinstance(x, dict)]
        except Exception:
            return []

    mb_res, fk_res, an_res = await asyncio.gather(get_mb(), get_fk(), get_anime())
    hero = (mb_res[:3] + fk_res[:2] + an_res[:2])[:5]
    return ok({
        "hero": hero,
        "shelves": [
            {"title": "Trending Now (MovieBox)", "items": mb_res},
            {"title": "4K Ultra HD Releases", "items": fk_res},
            {"title": "Popular Anime & Dubs", "items": an_res},
        ],
    }, endpoint="unified_feed")


@app.get("/api/unified/search", tags=["Unified"])
async def unified_search(q: str = Query(..., min_length=1)):
    async def s_mb():
        try:
            res = await mb_search(q=q, page=1)
            return res.get("data", {}).get("items", [])
        except Exception:
            return []

    async def s_fk():
        try:
            res = await fk_search(q=q)
            return res.get("data", {}).get("items", [])
        except Exception:
            return []

    async def s_ha():
        try:
            res = await ha_search(q=q)
            return res.get("data", {}).get("items", [])
        except Exception:
            return []

    mb_i, fk_i, ha_i = await asyncio.gather(s_mb(), s_fk(), s_ha())
    merged = mb_i + fk_i + ha_i
    return ok({"items": merged, "count": len(merged)}, endpoint="unified_search")


@app.get("/api/unified/play", tags=["Unified"])
async def unified_play(
    request: Request,
    provider: str = Query(...),
    id: str = Query(...),
    title: str = Query(""),
    se: Optional[int] = None,
    ep: Optional[int] = None,
):
    p = provider.lower().strip()
    streams = []
    meta = {"title": title, "seasons": []}

    if p == "moviebox":
        res = await mb_play(request=request, subject_id=id, se=se, ep=ep)
        if res.get("ok"):
            streams.extend(res["data"].get("streams", []))
        try:
            s_res = await mb_seasons(id)
            meta["seasons"] = (s_res.get("data") or {}).get("seasons") or []
        except Exception:
            pass
    elif p == "4khdhub":
        res = await fk_stream(path=id)
        if res.get("ok"):
            streams.extend(res["data"].get("streams", []))
    elif p in ("hindianime", "cinestream"):
        res = await ha_stream(title=title or id, season=se or 1, episode=ep or 1)
        if res.get("ok"):
            streams.extend(res["data"].get("streams", []))

    if not streams and title:
        try:
            sr = await mb_search(q=title, page=1)
            hits = sr.get("data", {}).get("items", [])
            if hits:
                fb = await mb_play(request=request, subject_id=hits[0]["id"], se=se, ep=ep)
                if fb.get("ok"):
                    streams.extend(fb["data"].get("streams", []))
        except Exception:
            pass

    return ok({
        "streams": streams,
        "best": streams[0]["play_url"] if streams else None,
        "meta": meta,
        "count": len(streams),
    }, endpoint="unified_play")


# ========== 6. STREAM PROXY (/px & /gx) ==========
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


async def _proxy_fetch(request: Request, url: str, cookie: str = "", referer: str = "", dash_base: str = "", public_base: str = ""):
    _safe_target(url)
    headers = {"User-Agent": UA, "Accept": "*/*", "Accept-Encoding": "identity"}
    if referer:
        headers["Referer"] = referer
    if request.headers.get("range"):
        headers["Range"] = request.headers.get("range")
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
    if r.status_code >= 400:
        body = (await r.aread())[:300]
        await r.aclose()
        await client.aclose()
        return Response(content=body, status_code=r.status_code, media_type="text/plain")
    if path_l.endswith(".m3u8") or "mpegurl" in ct:
        raw = await r.aread()
        await r.aclose()
        await client.aclose()
        return Response(_rewrite_m3u8(raw.decode("utf-8", "ignore"), str(r.url), cookie), media_type="application/vnd.apple.mpegurl", headers={"Cache-Control": "no-store", "Access-Control-Allow-Origin": "*"})
    if path_l.endswith(".mpd") or "dash+xml" in ct:
        raw = await r.aread()
        await r.aclose()
        await client.aclose()
        text = raw.decode("utf-8", "ignore")
        if dash_base:
            text = _rewrite_mpd_for_proxy(text, dash_base, cookie, public_base=public_base or str(request.base_url).rstrip("/"))
        return Response(text, media_type="application/dash+xml", headers={"Cache-Control": "no-store", "Access-Control-Allow-Origin": "*"})

    out_headers = {k: v for k, v in r.headers.items() if k.lower() in _PASS_HEADERS}
    out_headers.setdefault("Accept-Ranges", "bytes")
    out_headers["Access-Control-Allow-Origin"] = "*"
    out_headers["Cache-Control"] = "no-store"

    async def _close():
        await r.aclose()
        await client.aclose()

    return StreamingResponse(r.aiter_raw(), status_code=r.status_code, headers=out_headers, background=BackgroundTask(_close))


@app.get("/px/{token}/{path:path}", tags=["Proxy"], include_in_schema=False)
async def px_dash(token: str, path: str, request: Request):
    try:
        info = json.loads(_b64u_decode(token))
        base, cookie = str(info.get("b") or "").rstrip("/"), str(info.get("c") or "")
    except Exception:
        raise HTTPException(400, detail=fail("Bad token"))
    if not base:
        raise HTTPException(400, detail=fail("Bad token"))
    rel = (path or "").lstrip("/")
    target = base + "/" + rel + (f"?{request.url.query}" if request.url.query else "")
    return await _proxy_fetch(request, target, cookie=cookie, dash_base=base, public_base=str(request.base_url).rstrip("/"))


@app.get("/gx", tags=["Proxy"], include_in_schema=False)
async def gx_proxy(request: Request, u: str = Query(...), c: str = Query("")):
    host = (urlparse(u).hostname or "").lower()
    if any(x in host for x in ("hakunaymatata.com", "aoneroom.com")):
        return await _mb_cdn_proxy(request, u)
    ref = "https://" + host + "/" if host else ""
    if "hindianime" in host:
        ref = HA_BASE + "/"
    return await _proxy_fetch(request, u, cookie=c, referer=ref)


@app.get("/health", tags=["Meta"])
async def health():
    return ok({
        "version": VERSION,
        "status": "operational",
        "providers": ["moviebox", "mymovieclan", "4khdhub", "hindianime", "cinestream", "youtube", "iptv", "cricbuzz"],
    })


# ========== 7. INTERACTIVE API DOCS UI (/docs) ==========
DOCS_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8"/>
<meta name="viewport" content="width=device-width,initial-scale=1"/>
<title>StreamHub API Docs — v9.5.0</title>
<link href="https://fonts.googleapis.com/css2?family=Plus+Jakarta+Sans:wght@400;500;600;700;800&family=JetBrains+Mono:wght@400;500;600&display=swap" rel="stylesheet"/>
<style>
:root{
  --bg:#06070d;--card:rgba(18,20,33,.78);--card2:rgba(11,13,22,.9);--bd:rgba(255,255,255,.08);
  --tx:#f8fafc;--mu:#94a3b8;--ac:#8b5cf6;--ac2:#06b6d4;--ok:#10b981;--err:#ef4444;
}
*{box-sizing:border-box;margin:0;padding:0}
body{
  font-family:'Plus Jakarta Sans',system-ui,sans-serif;background:var(--bg);color:var(--tx);min-height:100vh;
  background-image:radial-gradient(circle at 15% -10%,rgba(139,92,246,.22),transparent 45%),
                   radial-gradient(circle at 85% 10%,rgba(6,182,212,.15),transparent 40%);
}
.nav{
  position:sticky;top:0;z-index:50;backdrop-filter:blur(20px);background:rgba(6,7,13,.82);
  border-bottom:1px solid var(--bd);padding:14px 24px;display:flex;align-items:center;justify-content:space-between;gap:16px;flex-wrap:wrap;
}
.brand{display:flex;align-items:center;gap:12px}
.logo{
  width:40px;height:40px;border-radius:12px;background:linear-gradient(135deg,var(--ac),var(--ac2));
  display:grid;place-items:center;font-weight:800;font-size:15px;box-shadow:0 8px 24px rgba(139,92,246,.35);
}
.brand h1{font-size:18px;font-weight:800;letter-spacing:-.02em}
.brand span{font-size:12px;color:var(--ac2);font-weight:600}
.nav-links{display:flex;gap:10px}
.nav-btn{
  padding:8px 14px;border-radius:10px;background:rgba(255,255,255,.06);border:1px solid var(--bd);
  color:var(--tx);text-decoration:none;font-size:13px;font-weight:600;transition:.2s;
}
.nav-btn:hover{background:rgba(139,92,246,.2);border-color:var(--ac)}
.hero{max-width:960px;margin:28px auto 12px;padding:0 20px}
.search-box{
  display:flex;align-items:center;gap:12px;background:var(--card);border:1px solid var(--bd);
  border-radius:16px;padding:14px 18px;box-shadow:0 12px 32px rgba(0,0,0,.4);transition:.25s;
}
.search-box:focus-within{border-color:var(--ac);box-shadow:0 0 0 4px rgba(139,92,246,.18)}
.search-box input{flex:1;background:transparent;border:0;outline:0;color:var(--tx);font-size:15px}
.tabs{display:flex;gap:8px;overflow-x:auto;padding:16px 20px 8px;max-width:960px;margin:0 auto;scrollbar-width:none}
.tab{
  padding:8px 16px;border-radius:99px;background:var(--card);border:1px solid var(--bd);color:var(--mu);
  font-size:13px;font-weight:700;cursor:pointer;white-space:nowrap;transition:all .25s cubic-bezier(.22,1,.36,1);
}
.tab:hover{color:var(--tx);transform:translateY(-1px)}
.tab.on{background:linear-gradient(135deg,var(--ac),#6366f1);color:#fff;border-color:transparent;box-shadow:0 6px 20px rgba(139,92,246,.35)}
.wrap{max-width:960px;margin:0 auto;padding:12px 20px 80px}
.ep{
  background:var(--card);border:1px solid var(--bd);border-radius:16px;margin-bottom:12px;overflow:hidden;
  transition:all .3s cubic-bezier(.22,1,.36,1);animation:fadeUp .4s both;
}
@keyframes fadeUp{from{opacity:0;transform:translateY(12px)}to{opacity:1;transform:none}}
.ep:hover{border-color:rgba(139,92,246,.4)}
.ep.open{border-color:var(--ac);box-shadow:0 16px 40px rgba(0,0,0,.5)}
.ep-h{display:flex;align-items:center;gap:12px;padding:16px 18px;cursor:pointer}
.badge{
  font-family:'JetBrains Mono',monospace;font-size:11px;font-weight:700;padding:4px 10px;border-radius:8px;
  background:rgba(6,182,212,.15);color:var(--ac2);
}
.path{font-family:'JetBrains Mono',monospace;font-size:13.5px;font-weight:600;flex:1}
.desc{font-size:13px;color:var(--mu);padding:0 18px 14px}
.body{display:none;padding:16px 18px;border-top:1px solid var(--bd);background:var(--card2)}
.ep.open .body{display:block;animation:fadeUp .25s ease}
.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(200px,1fr));gap:12px;margin-bottom:14px}
.fld label{display:block;font-size:11px;font-weight:700;color:var(--mu);margin-bottom:6px;text-transform:uppercase}
.fld input{
  width:100%;padding:10px 12px;border-radius:10px;background:#07080f;border:1px solid var(--bd);
  color:var(--tx);font-family:'JetBrains Mono',monospace;font-size:13px;outline:0;
}
.fld input:focus{border-color:var(--ac2)}
.btns{display:flex;gap:10px;flex-wrap:wrap}
.btn{
  padding:10px 18px;border-radius:10px;border:0;font-weight:700;font-size:13px;cursor:pointer;transition:.2s;
}
.btn:active{transform:scale(.96)}
.btn-run{background:linear-gradient(135deg,var(--ac),var(--ac2));color:#fff}
.btn-copy{background:rgba(255,255,255,.08);color:var(--tx);border:1px solid var(--bd)}
.meta-res{margin-top:12px;font-family:'JetBrains Mono',monospace;font-size:12px;font-weight:600}
.meta-res.ok{color:var(--ok)}.meta-res.bad{color:var(--err)}
pre{
  margin-top:10px;background:#05060b;border:1px solid var(--bd);border-radius:12px;padding:14px;
  font-family:'JetBrains Mono',monospace;font-size:12px;line-height:1.5;max-height:360px;overflow:auto;color:#e2e8f0;
}
</style>
</head>
<body>
<div class="nav">
  <div class="brand">
    <div class="logo">SH</div>
    <div><h1>StreamHub API</h1><span>v9.5.0 · Interactive Playground</span></div>
  </div>
  <div class="nav-links">
    <a class="nav-btn" href="/">🎬 Open Web App UI</a>
    <a class="nav-btn" href="/health" target="_blank">⚡ Health JSON</a>
  </div>
</div>
<div class="hero">
  <div class="search-box">
    <span>🔍</span>
    <input id="q" placeholder="Search endpoints (e.g. unified, moviebox, anime, youtube, iptv)..." />
  </div>
</div>
<div class="tabs" id="tabs"></div>
<div class="wrap" id="list"></div>
<script>
const ENDPOINTS = [
  {g:"Unified",p:"/api/unified/feed",d:"Aggregated Home Feed (MovieBox + 4KHDHub + Anime).",params:[{n:"category",v:"all"}]},
  {g:"Unified",p:"/api/unified/search",d:"Fast multi-provider parallel search.",params:[{n:"q",v:"avengers"}]},
  {g:"Unified",p:"/api/unified/play",d:"Smart stream resolver with auto-fallback.",params:[{n:"provider",v:"moviebox"},{n:"id",v:"1654274595068805784"},{n:"title",v:"Avatar"}]},
  {g:"MovieBox",p:"/mb/search",d:"Search MovieBox movies & series.",params:[{n:"q",v:"inception"},{n:"page",v:"1"}]},
  {g:"MovieBox",p:"/mb/play/{id}",d:"Get proxied MP4 & DASH streams.",params:[{n:"id",v:"1654274595068805784",path:true},{n:"se",v:""},{n:"ep",v:""}]},
  {g:"MovieBox",p:"/mb/detail/{id}",d:"Full metadata & cast details.",params:[{n:"id",v:"1654274595068805784",path:true}]},
  {g:"MovieBox",p:"/mb/seasons/{id}",d:"Seasons & episode counts.",params:[{n:"id",v:"1654274595068805784",path:true}]},
  {g:"Anime",p:"/cs/home",d:"CineStream trending/popular anime.",params:[{n:"filter",v:"trending"},{n:"type",v:"anime"}]},
  {g:"Anime",p:"/ha/search",d:"HindiAnime catalog search.",params:[{n:"q",v:"naruto"}]},
  {g:"Anime",p:"/ha/stream",d:"Resolve Hindi dubbed anime HLS/MP4.",params:[{n:"title",v:"Naruto"},{n:"season",v:"1"},{n:"episode",v:"1"}]},
  {g:"4K & MMC",p:"/fk/home",d:"Latest 4KHDHub releases.",params:[]},
  {g:"4K & MMC",p:"/fk/search",d:"Search 4KHDHub titles.",params:[{n:"q",v:"spider"}]},
  {g:"4K & MMC",p:"/mmc/search",d:"Multi-provider MyMovieClan search.",params:[{n:"q",v:"avatar"}]},
  {g:"YouTube",p:"/yt/stream",d:"Direct unlocked YouTube video streams.",params:[{n:"url",v:"https://www.youtube.com/watch?v=dQw4w9WgXcQ"}]},
  {g:"Live & Sports",p:"/iptv/channels",d:"Live TV M3U channels (0=BD, 1=IN, 2=Global).",params:[{n:"source",v:"0"},{n:"q",v:""}]},
  {g:"Live & Sports",p:"/cb/live",d:"Cricbuzz live cricket scores.",params:[]}
];
let curTab = "All";
const groups = ["All", ...new Set(ENDPOINTS.map(e => e.g))];

function buildUrl(ep, vals){
  let p = ep.p;
  const qs = [];
  (ep.params||[]).forEach(x => {
    const v = (vals[x.n] ?? x.v ?? "").trim();
    if(x.path) p = p.replace("{"+x.n+"}", encodeURIComponent(v));
    else if(v) qs.push(encodeURIComponent(x.n)+"="+encodeURIComponent(v));
  });
  return location.origin + p + (qs.length ? "?"+qs.join("&") : "");
}

function render(){
  document.getElementById("tabs").innerHTML = groups.map(g =>
    `<button class="tab ${g===curTab?'on':''}" onclick="curTab='${g}';render()">${g}</button>`
  ).join("");
  const q = document.getElementById("q").value.toLowerCase();
  const list = ENDPOINTS.filter(e => (curTab==="All" || e.g===curTab) && (e.p+e.d+e.g).toLowerCase().includes(q));
  document.getElementById("list").innerHTML = list.map((ep, i) => `
    <div class="ep" id="ep-${i}" style="animation-delay:${i*35}ms">
      <div class="ep-h" onclick="document.getElementById('ep-${i}').classList.toggle('open')">
        <span class="badge">GET</span>
        <span class="path">${ep.p}</span>
        <span>▾</span>
      </div>
      <div class="desc">${ep.d}</div>
      <div class="body">
        <div class="grid">
          ${(ep.params||[]).map(p => `<div class="fld"><label>${p.n}</label><input data-k="${p.n}" value="${p.v||''}"/></div>`).join("")}
        </div>
        <div class="btns">
          <button class="btn btn-run" onclick="runEp(${i})">▶ Execute Request</button>
          <button class="btn btn-copy" onclick="copyEp(${i})">📋 Copy URL</button>
        </div>
        <div class="meta-res" id="st-${i}"></div>
        <pre id="out-${i}" hidden></pre>
      </div>
    </div>
  `).join("");
  window._vis = list;
}

async function runEp(i){
  const ep = window._vis[i];
  const box = document.getElementById("ep-"+i);
  const vals = {};
  box.querySelectorAll("input").forEach(inp => vals[inp.dataset.k] = inp.value);
  const url = buildUrl(ep, vals);
  const st = document.getElementById("st-"+i), out = document.getElementById("out-"+i);
  st.className = "meta-res"; st.textContent = "⏳ Fetching..."; out.hidden = false; out.textContent = "";
  const t0 = performance.now();
  try{
    const r = await fetch(url);
    const txt = await r.text();
    const ms = Math.round(performance.now() - t0);
    st.className = "meta-res " + (r.ok ? "ok" : "bad");
    st.textContent = `HTTP ${r.status} · ${ms}ms · ${url}`;
    try { out.textContent = JSON.stringify(JSON.parse(txt), null, 2); } catch(_) { out.textContent = txt; }
  }catch(e){
    st.className = "meta-res bad"; st.textContent = "Request Failed"; out.textContent = String(e);
  }
}

function copyEp(i){
  const ep = window._vis[i];
  const box = document.getElementById("ep-"+i);
  const vals = {};
  box.querySelectorAll("input").forEach(inp => vals[inp.dataset.k] = inp.value);
  navigator.clipboard.writeText(buildUrl(ep, vals));
  const st = document.getElementById("st-"+i);
  st.className = "meta-res ok"; st.textContent = "✓ Copied URL to clipboard!";
}

document.getElementById("q").addEventListener("input", render);
render();
</script>
</body>
</html>
"""


@app.get("/docs", response_class=HTMLResponse, include_in_schema=False)
async def docs_ui():
    return HTMLResponse(DOCS_HTML)


# ========== 8. PWA ASSETS ==========
def _png_icon(size: int) -> bytes:
    bg, ring, tri = (6, 8, 18), (139, 92, 246), (6, 182, 212)
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
            qx0 = min(x, size - 1 - x)
            qy0 = min(y, size - 1 - y)
            a = 0 if (qx0 < rad and qy0 < rad and math.hypot(rad - qx0, rad - qy0) > rad) else 255
            d = math.hypot(x + 0.5 - cx, y + 0.5 - cy)
            col = ring if abs(d - rr) < rw / 2 else (tri if inside_tri(x + 0.5, y + 0.5) and d < rr else bg)
            row += bytes((col[0], col[1], col[2], a))
        rows.append(bytes(row))
    raw = b"".join(rows)

    def chunk(t, data):
        return struct.pack(">I", len(data)) + t + data + struct.pack(">I", zlib.crc32(t + data) & 0xFFFFFFFF)

    return b"\x89PNG\r\n\x1a\n" + chunk(b"IHDR", struct.pack(">IIBBBBB", size, size, 8, 6, 0, 0, 0)) + chunk(b"IDAT", zlib.compress(raw, 9)) + chunk(b"IEND", b"")


_ICON_CACHE: Dict[int, bytes] = {}


@app.get("/icon-{size}.png", include_in_schema=False)
async def icon_png(size: int):
    if size not in (180, 192, 512):
        raise HTTPException(404)
    if size not in _ICON_CACHE:
        _ICON_CACHE[size] = _png_icon(size)
    return Response(_ICON_CACHE[size], media_type="image/png", headers={"Cache-Control": "public, max-age=604800"})


@app.get("/manifest.webmanifest", include_in_schema=False)
async def manifest():
    return JSONResponse({
        "name": "StreamHub",
        "short_name": "StreamHub",
        "start_url": "/",
        "display": "standalone",
        "background_color": "#05060f",
        "theme_color": "#05060f",
        "icons": [{"src": "/icon-192.png", "sizes": "192x192", "type": "image/png"}],
    })


# ========== 9. MAIN CINE-GLASS WEB UI (/ & /site) ==========
SITE_HTML = r"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1,viewport-fit=cover">
<meta name="referrer" content="no-referrer">
<meta name="theme-color" content="#05060f">
<title>StreamHub — Cinema, Anime & Live TV</title>
<link rel="manifest" href="/manifest.webmanifest">
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=Unbounded:wght@600;700&family=Plus+Jakarta+Sans:wght@400;500;600;700;800&display=swap" rel="stylesheet">
<script src="https://cdn.jsdelivr.net/npm/hls.js@1.5.13/dist/hls.min.js"></script>
<script src="https://cdn.dashjs.org/latest/dash.all.min.js"></script>
<style>
:root{
  --void:#05060f;--ink:#0b0e1e;--panel:rgba(18,22,42,.68);--line:rgba(255,255,255,.09);
  --tx:#f8fafc;--dim:#94a3b8;--ac:#8b5cf6;--ac2:#06b6d4;--pink:#f43f5e;--amber:#f59e0b;
  --spring:cubic-bezier(.22,1,.36,1);--bounce:cubic-bezier(.34,1.56,.64,1);
}
*{box-sizing:border-box;margin:0;padding:0;-webkit-tap-highlight-color:transparent}
html{color-scheme:dark;scroll-behavior:smooth}
body{
  background:var(--void);color:var(--tx);font-family:'Plus Jakarta Sans',system-ui,sans-serif;
  min-height:100vh;overflow-x:hidden;
}

/* Ambient Aurora Motion */
.aurora{position:fixed;inset:0;z-index:-1;pointer-events:none;overflow:hidden}
.aurora::before,.aurora::after{
  content:"";position:absolute;width:65vmax;height:65vmax;border-radius:50%;filter:blur(90px);opacity:.26;
  animation:floatOrb 18s ease-in-out infinite alternate;
}
.aurora::before{background:radial-gradient(circle,var(--ac),transparent 70%);top:-20vmax;left:-15vmax}
.aurora::after{background:radial-gradient(circle,var(--ac2),transparent 70%);bottom:-25vmax;right:-15vmax;animation-delay:-9s}
@keyframes floatOrb{0%{transform:translate(0,0) scale(1)}100%{transform:translate(6vmax,4vmax) scale(1.12)}}

/* Header */
header{
  position:fixed;top:0;left:0;right:0;z-index:50;padding:14px 24px;
  display:flex;align-items:center;gap:16px;transition:all .35s var(--spring);
}
header.scrolled{background:rgba(5,6,15,.84);backdrop-filter:blur(22px);border-bottom:1px solid var(--line);padding:10px 24px}
.brand{display:flex;align-items:center;gap:10px;font-family:'Unbounded',sans-serif;font-weight:700;font-size:17px;cursor:pointer}
.brand-icon{
  width:34px;height:34px;border-radius:10px;background:linear-gradient(135deg,var(--ac),var(--ac2));
  display:grid;place-items:center;box-shadow:0 0 24px rgba(139,92,246,.5);
}
.nav-pills{display:flex;gap:6px;overflow-x:auto;scrollbar-width:none}
.pill{
  padding:8px 16px;border-radius:99px;font-size:13px;font-weight:700;color:var(--dim);
  background:rgba(255,255,255,.04);border:1px solid transparent;cursor:pointer;white-space:nowrap;
  transition:all .3s var(--spring);
}
.pill:hover{color:var(--tx);background:rgba(255,255,255,.09)}
.pill.on{color:#fff;background:linear-gradient(135deg,var(--ac),#6366f1);box-shadow:0 6px 20px rgba(139,92,246,.4)}
.spacer{flex:1}
.search-wrap{
  position:relative;display:flex;align-items:center;background:rgba(255,255,255,.07);
  border:1px solid var(--line);border-radius:99px;padding:7px 14px;width:230px;transition:all .35s var(--spring);
}
.search-wrap:focus-within{width:300px;border-color:var(--ac2);background:rgba(11,14,30,.92);box-shadow:0 0 0 4px rgba(6,182,212,.15)}
.search-wrap input{background:none;border:0;outline:0;color:var(--tx);font-size:13.5px;width:100%;margin-left:8px}
.docs-link{
  padding:8px 14px;border-radius:99px;background:rgba(6,182,212,.14);border:1px solid rgba(6,182,212,.35);
  color:var(--ac2);font-size:12.5px;font-weight:800;text-decoration:none;white-space:nowrap;transition:.25s;
}
.docs-link:hover{background:var(--ac2);color:#05060f}

/* Hero Spotlight */
.hero{
  position:relative;height:74vh;min-height:480px;max-height:740px;display:flex;align-items:flex-end;
  padding:0 28px 56px;overflow:hidden;
}
.hero-bg{
  position:absolute;inset:0;background-size:cover;background-position:center 20%;
  transform:scale(1.04);transition:opacity .8s ease,transform 8s ease-out;
}
.hero::after{
  content:"";position:absolute;inset:0;
  background:linear-gradient(0deg,var(--void) 4%,rgba(5,6,15,.55) 45%,rgba(5,6,15,.2) 100%),
             linear-gradient(90deg,rgba(5,6,15,.9) 0%,rgba(5,6,15,.2) 60%,transparent 100%);
}
.hero-content{position:relative;z-index:2;max-width:650px;animation:heroRise .7s var(--spring) both}
@keyframes heroRise{from{opacity:0;transform:translateY(24px)}to{opacity:1;transform:none}}
.hero-badge{
  display:inline-flex;align-items:center;gap:6px;padding:5px 12px;border-radius:99px;
  background:rgba(139,92,246,.22);border:1px solid rgba(139,92,246,.5);color:#ddd6fe;
  font-size:11.5px;font-weight:800;text-transform:uppercase;letter-spacing:.06em;margin-bottom:14px;
}
.hero h1{font-family:'Unbounded',sans-serif;font-size:clamp(26px,4.5vw,48px);line-height:1.1;margin-bottom:14px}
.hero-meta{display:flex;gap:12px;align-items:center;color:var(--dim);font-size:13.5px;font-weight:700;margin-bottom:22px}
.hero-btns{display:flex;gap:12px;flex-wrap:wrap}
.btn-play{
  display:inline-flex;align-items:center;gap:10px;padding:13px 26px;border-radius:14px;border:0;
  background:linear-gradient(135deg,var(--ac),var(--ac2));color:#fff;font-weight:800;font-size:15px;
  cursor:pointer;box-shadow:0 12px 32px rgba(139,92,246,.45);transition:transform .25s var(--bounce),box-shadow .25s;
}
.btn-play:hover{transform:translateY(-2px) scale(1.02);box-shadow:0 16px 40px rgba(6,182,212,.5)}
.btn-sec{
  display:inline-flex;align-items:center;gap:8px;padding:13px 20px;border-radius:14px;
  background:rgba(255,255,255,.1);backdrop-filter:blur(12px);border:1px solid var(--line);
  color:#fff;font-weight:700;font-size:14px;cursor:pointer;transition:.25s;
}
.btn-sec:hover{background:rgba(255,255,255,.18)}

/* Content Shelves & 3D Interactive Cards */
.main{max-width:1480px;margin:0 auto;padding:10px 24px 90px}
.shelf{margin-bottom:36px}
.shelf-head{display:flex;align-items:center;justify-content:space-between;margin-bottom:16px}
.shelf-head h2{font-family:'Unbounded',sans-serif;font-size:17px;display:flex;align-items:center;gap:10px}
.shelf-head h2::before{content:"";width:4px;height:18px;border-radius:4px;background:linear-gradient(var(--ac),var(--ac2))}
.grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(165px,1fr));gap:18px}
.card{
  position:relative;border-radius:16px;overflow:hidden;background:var(--ink);border:1px solid var(--line);
  cursor:pointer;transition:transform .38s var(--spring),box-shadow .38s var(--spring),border-color .3s;
  animation:cardIn .5s var(--spring) both;
}
@keyframes cardIn{from{opacity:0;transform:translateY(18px) scale(.96)}to{opacity:1;transform:none}}
.card:hover{
  transform:translateY(-8px) scale(1.03);border-color:rgba(6,182,212,.6);
  box-shadow:0 22px 44px rgba(0,0,0,.65),0 0 28px rgba(139,92,246,.28);
}
.poster{position:relative;aspect-ratio:2/3;background:#11162d;overflow:hidden}
.poster img{width:100%;height:100%;object-fit:cover;transition:transform .6s var(--spring)}
.card:hover .poster img{transform:scale(1.08)}
.card-tag{
  position:absolute;top:10px;right:10px;padding:3px 8px;border-radius:7px;font-size:10.5px;font-weight:800;
  background:rgba(5,6,15,.82);backdrop-filter:blur(8px);border:1px solid var(--line);text-transform:uppercase;
}
.card-rate{
  position:absolute;top:10px;left:10px;padding:3px 8px;border-radius:7px;font-size:11px;font-weight:800;
  background:rgba(245,158,11,.2);color:#fbbf24;backdrop-filter:blur(8px);
}
.play-overlay{
  position:absolute;inset:0;background:radial-gradient(circle,rgba(139,92,246,.35),rgba(5,6,15,.75));
  display:grid;place-items:center;opacity:0;transition:opacity .3s;
}
.card:hover .play-overlay{opacity:1}
.play-circle{
  width:52px;height:52px;border-radius:50%;background:linear-gradient(135deg,var(--ac),var(--ac2));
  display:grid;place-items:center;font-size:18px;transform:scale(.7);transition:transform .35s var(--bounce);
}
.card:hover .play-circle{transform:scale(1)}
.card-info{padding:12px}
.card-info h3{font-size:14px;font-weight:700;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.card-info p{font-size:12px;color:var(--dim);margin-top:4px;display:flex;justify-content:space-between}

/* Live TV Cards */
.tv-card{
  display:flex;align-items:center;gap:12px;padding:12px;border-radius:14px;background:var(--panel);
  border:1px solid var(--line);cursor:pointer;transition:all .25s var(--spring);
}
.tv-card:hover{border-color:var(--ac2);transform:translateY(-3px)}
.tv-logo{width:46px;height:46px;border-radius:10px;background:#070913;object-fit:contain;padding:4px}

/* Theatre Player Modal */
.modal{
  position:fixed;inset:0;z-index:100;background:rgba(3,4,10,.92);backdrop-filter:blur(24px);
  display:none;overflow-y:auto;padding:24px;animation:fadeIn .3s ease;
}
.modal.open{display:block}
@keyframes fadeIn{from{opacity:0}to{opacity:1}}
.modal-inner{max-width:1060px;margin:0 auto}
.modal-top{display:flex;align-items:center;justify-content:space-between;margin-bottom:16px;gap:12px}
.close-btn{
  padding:9px 16px;border-radius:12px;background:rgba(255,255,255,.1);border:1px solid var(--line);
  color:#fff;font-weight:700;cursor:pointer;
}
.player-box{
  position:relative;aspect-ratio:16/9;background:#000;border-radius:20px;overflow:hidden;
  border:1px solid rgba(139,92,246,.4);box-shadow:0 28px 70px rgba(0,0,0,.85);
}
video{width:100%;height:100%;background:#000}
.loader-ov{
  position:absolute;inset:0;display:grid;place-items:center;background:rgba(5,6,15,.85);z-index:5;
  font-weight:700;color:var(--ac2);gap:10px;text-align:center;padding:20px;
}
.spinner{
  width:42px;height:42px;border:3px solid rgba(255,255,255,.15);border-top-color:var(--ac2);
  border-radius:50%;animation:spin .8s linear infinite;margin:0 auto 10px;
}
@keyframes spin{to{transform:rotate(360deg)}}
.controls-bar{
  margin-top:18px;background:var(--panel);border:1px solid var(--line);border-radius:16px;padding:16px;
}
.stream-pills{display:flex;gap:8px;flex-wrap:wrap;margin-top:10px}
.s-pill{
  padding:8px 14px;border-radius:10px;background:rgba(255,255,255,.07);border:1px solid var(--line);
  color:var(--tx);font-size:12.5px;font-weight:700;cursor:pointer;transition:.2s;
}
.s-pill.on{background:linear-gradient(135deg,var(--ac),var(--ac2));border-color:transparent}
.ep-grid{display:flex;gap:8px;flex-wrap:wrap;margin-top:12px}
.ep-btn{
  min-width:44px;height:38px;border-radius:10px;background:rgba(255,255,255,.06);border:1px solid var(--line);
  color:var(--tx);font-weight:700;cursor:pointer;
}
.ep-btn.on{background:var(--ac);border-color:transparent}
.sk{height:240px;border-radius:16px;background:linear-gradient(90deg,#0f1326 25%,#1a203d 50%,#0f1326 75%);background-size:200% 100%;animation:shimmer 1.4s infinite}
@keyframes shimmer{to{background-position:-200% 0}}
@media(max-width:680px){
  header{padding:10px 14px;flex-wrap:wrap}
  .search-wrap{width:100%;order:3}
  .search-wrap:focus-within{width:100%}
  .hero{height:60vh;padding:0 16px 36px}
  .grid{grid-template-columns:repeat(auto-fill,minmax(135px,1fr));gap:12px}
}
</style>
</head>
<body>
<div class="aurora"></div>

<header id="hdr">
  <div class="brand" onclick="switchTab('all')">
    <div class="brand-icon">▶</div>
    <span>StreamHub</span>
  </div>
  <div class="nav-pills" id="nav">
    <button class="pill on" data-t="all" onclick="switchTab('all')">Discover</button>
    <button class="pill" data-t="movie" onclick="switchTab('movie')">Movies</button>
    <button class="pill" data-t="series" onclick="switchTab('series')">Series</button>
    <button class="pill" data-t="anime" onclick="switchTab('anime')">Anime</button>
    <button class="pill" data-t="tv" onclick="switchTab('tv')">Live TV</button>
    <button class="pill" data-t="saved" onclick="switchTab('saved')">Watchlist</button>
  </div>
  <div class="spacer"></div>
  <div class="search-wrap">
    <span>🔍</span>
    <input id="searchInp" placeholder="Search movies, series, anime..." />
  </div>
  <a class="docs-link" href="/docs">⚡ API Docs</a>
</header>

<section class="hero" id="hero">
  <div class="hero-bg" id="heroBg"></div>
  <div class="hero-content" id="heroContent">
    <div class="hero-badge">✨ StreamHub v9.5 Ultra</div>
    <h1>Loading Cinema...</h1>
    <div class="hero-meta">Multi-CDN Adaptive Streaming Engine</div>
  </div>
</section>

<main class="main" id="appMain">
  <div class="grid">
    <div class="sk"></div><div class="sk"></div><div class="sk"></div><div class="sk"></div><div class="sk"></div><div class="sk"></div>
  </div>
</main>

<!-- Theatre Player Modal -->
<div class="modal" id="playerModal">
  <div class="modal-inner">
    <div class="modal-top">
      <h2 id="mTitle" style="font-family:'Unbounded',sans-serif;font-size:18px">Now Playing</h2>
      <div style="display:flex;gap:8px">
        <button class="close-btn" id="wlBtn" onclick="toggleWatchlist()">+ Watchlist</button>
        <button class="close-btn" onclick="closePlayer()">✕ Close</button>
      </div>
    </div>
    <div class="player-box">
      <div class="loader-ov" id="pLoader">
        <div>
          <div class="spinner"></div>
          <div id="pStatus">Resolving fastest CDN stream...</div>
        </div>
      </div>
      <video id="vid" controls playsinline crossorigin="anonymous"></video>
    </div>
    <div class="controls-bar">
      <div style="font-size:12px;color:var(--dim);font-weight:700;text-transform:uppercase">Available Streams & Qualities</div>
      <div class="stream-pills" id="streamPills"></div>
      <div id="epBox" style="display:none;margin-top:16px">
        <div style="font-size:12px;color:var(--dim);font-weight:700;text-transform:uppercase">Episodes</div>
        <div class="ep-grid" id="epGrid"></div>
      </div>
    </div>
  </div>
</div>

<script>
let currentTab = 'all';
let heroItems = [];
let heroIdx = 0;
let activeItem = null;
let hlsInst = null;
let dashInst = null;
let watchlist = JSON.parse(localStorage.getItem('sh_watchlist') || '[]');

window.addEventListener('scroll', () => {
  document.getElementById('hdr').classList.toggle('scrolled', window.scrollY > 30);
});

function setHero(item){
  if(!item) return;
  const bg = item.poster || item.cover || '';
  document.getElementById('heroBg').style.backgroundImage = bg ? `url('${bg}')` : 'none';
  document.getElementById('heroContent').innerHTML = `
    <div class="hero-badge">🔥 Featured ${(item.type||'Movie').toUpperCase()} · ${item.provider||'StreamHub'}</div>
    <h1>${esc(item.title)}</h1>
    <div class="hero-meta">
      <span>⭐ ${item.rating || '8.8'}</span>
      <span>•</span>
      <span>${item.year || '2025'}</span>
      <span>•</span>
      <span>1080p / 4K Adaptive</span>
    </div>
    <div class="hero-btns">
      <button class="btn-play" onclick='openItem(${JSON.stringify(item).replace(/'/g,"&#39;")})'>▶ Watch Now</button>
      <button class="btn-sec" onclick='saveQuick(${JSON.stringify(item).replace(/'/g,"&#39;")})'>+ My List</button>
    </div>
  `;
}

setInterval(() => {
  if(heroItems.length > 1 && !document.getElementById('playerModal').classList.contains('open')){
    heroIdx = (heroIdx + 1) % heroItems.length;
    setHero(heroItems[heroIdx]);
  }
}, 6500);

function esc(s){ return String(s||'').replace(/[&<>"']/g, m => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[m])); }

function renderCard(item, idx=0){
  const poster = item.poster || item.cover || 'https://placehold.co/300x450/0b0e1e/8b5cf6?text=StreamHub';
  return `
    <div class="card" style="animation-delay:${(idx%12)*40}ms" onclick='openItem(${JSON.stringify(item).replace(/'/g,"&#39;")})'>
      <div class="poster">
        <img src="${esc(poster)}" loading="lazy" alt="${esc(item.title)}"/>
        ${item.rating ? `<span class="card-rate">★ ${esc(item.rating)}</span>` : ''}
        <span class="card-tag">${esc(item.type || 'HD')}</span>
        <div class="play-overlay"><div class="play-circle">▶</div></div>
      </div>
      <div class="card-info">
        <h3>${esc(item.title)}</h3>
        <p><span>${esc(item.year || 'HD')}</span><span>${esc(item.provider || '')}</span></p>
      </div>
    </div>
  `;
}

async function switchTab(tab){
  currentTab = tab;
  document.querySelectorAll('.pill').forEach(b => b.classList.toggle('on', b.dataset.t === tab));
  const main = document.getElementById('appMain');
  document.getElementById('hero').style.display = (tab === 'tv' || tab === 'saved') ? 'none' : 'flex';

  if(tab === 'saved'){
    main.style.paddingTop = '100px';
    main.innerHTML = `<div class="shelf"><div class="shelf-head"><h2>My Watchlist (${watchlist.length})</h2></div>
      ${watchlist.length ? `<div class="grid">${watchlist.map((x,i)=>renderCard(x,i)).join('')}</div>` : '<p style="color:var(--dim)">No saved titles yet.</p>'}
    </div>`;
    return;
  }

  if(tab === 'tv'){
    main.style.paddingTop = '100px';
    main.innerHTML = `<div class="grid"><div class="sk"></div><div class="sk"></div><div class="sk"></div><div class="sk"></div></div>`;
    const r = await fetch('/iptv/channels?source=0&limit=80');
    const j = await r.json();
    const chs = j.data || [];
    main.innerHTML = `<div class="shelf"><div class="shelf-head"><h2>Live TV Channels (${chs.length})</h2></div>
      <div class="grid" style="grid-template-columns:repeat(auto-fill,minmax(220px,1fr))">
        ${chs.map(c => `
          <div class="tv-card" onclick='playLive(${JSON.stringify(c).replace(/'/g,"&#39;")})'>
            <img class="tv-logo" src="${esc(c.logo || 'https://placehold.co/60x60/0b0e1e/06b6d4?text=TV')}" onerror="this.src='https://placehold.co/60x60/0b0e1e/06b6d4?text=TV'"/>
            <div style="overflow:hidden"><div style="font-weight:700;font-size:13.5px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis">${esc(c.name)}</div>
            <div style="font-size:11px;color:#f43f5e;font-weight:800">● LIVE · ${esc(c.group||'General')}</div></div>
          </div>
        `).join('')}
      </div></div>`;
    return;
  }

  main.style.paddingTop = '10px';
  main.innerHTML = `<div class="grid"><div class="sk"></div><div class="sk"></div><div class="sk"></div><div class="sk"></div><div class="sk"></div><div class="sk"></div></div>`;
  try{
    const r = await fetch(`/api/unified/feed?category=${tab}`);
    const j = await r.json();
    const d = j.data || {};
    heroItems = d.hero || [];
    if(heroItems.length) setHero(heroItems[0]);
    main.innerHTML = (d.shelves || []).filter(s => (s.items||[]).length).map(s => `
      <div class="shelf">
        <div class="shelf-head"><h2>${esc(s.title)}</h2></div>
        <div class="grid">${s.items.map((it, idx) => renderCard(it, idx)).join('')}</div>
      </div>
    `).join('');
  }catch(e){
    main.innerHTML = `<p style="padding:40px;color:#f43f5e">Failed to load feed. Check connection.</p>`;
  }
}

let searchTimer = null;
document.getElementById('searchInp').addEventListener('input', e => {
  clearTimeout(searchTimer);
  const q = e.target.value.trim();
  if(!q){ switchTab(currentTab); return; }
  searchTimer = setTimeout(async () => {
    document.getElementById('hero').style.display = 'none';
    const main = document.getElementById('appMain');
    main.style.paddingTop = '100px';
    main.innerHTML = `<div class="grid"><div class="sk"></div><div class="sk"></div><div class="sk"></div><div class="sk"></div></div>`;
    const r = await fetch('/api/unified/search?q=' + encodeURIComponent(q));
    const j = await r.json();
    const items = (j.data && j.data.items) || [];
    main.innerHTML = `<div class="shelf"><div class="shelf-head"><h2>Search Results for "${esc(q)}" (${items.length})</h2></div>
      <div class="grid">${items.map((it, idx) => renderCard(it, idx)).join('')}</div></div>`;
  }, 450);
});

async function openItem(item, se=1, ep=1){
  activeItem = item;
  const modal = document.getElementById('playerModal');
  modal.classList.add('open');
  document.getElementById('mTitle').textContent = item.title + (item.type !== 'movie' ? ` · S${se} E${ep}` : '');
  document.getElementById('pLoader').style.display = 'grid';
  document.getElementById('pStatus').textContent = 'Resolving fastest CDN stream...';
  document.getElementById('streamPills').innerHTML = '';
  updateWlBtn();

  const epBox = document.getElementById('epBox');
  if(item.type === 'series' || item.type === 'anime'){
    epBox.style.display = 'block';
    document.getElementById('epGrid').innerHTML = Array.from({length: 24}, (_,i) => i+1).map(n =>
      `<button class="ep-btn ${n===ep?'on':''}" onclick='openItem(activeItem, ${se}, ${n})'>${n}</button>`
    ).join('');
  } else {
    epBox.style.display = 'none';
  }

  try{
    const qs = new URLSearchParams({provider: item.provider || 'moviebox', id: item.id, title: item.title || '', se, ep});
    const r = await fetch('/api/unified/play?' + qs.toString());
    const j = await r.json();
    const streams = (j.data && j.data.streams) || [];
    if(!streams.length){
      document.getElementById('pStatus').textContent = '⚠ No direct stream found for this source. Try another title.';
      return;
    }
    document.getElementById('streamPills').innerHTML = streams.map((s, idx) => `
      <button class="s-pill ${idx===0?'on':''}" onclick='attachStream(${JSON.stringify(s).replace(/'/g,"&#39;")}, this)'>
        ${esc(s.quality || 'Auto')} (${esc((s.kind||'mp4').toUpperCase())})
      </button>
    `).join('');
    attachStream(streams[0]);
  }catch(e){
    document.getElementById('pStatus').textContent = 'Error loading stream: ' + e.message;
  }
}

function playLive(ch){
  activeItem = {id: ch.url, title: ch.name, type: 'live', provider: 'iptv'};
  document.getElementById('playerModal').classList.add('open');
  document.getElementById('mTitle').textContent = '🔴 LIVE: ' + ch.name;
  document.getElementById('epBox').style.display = 'none';
  document.getElementById('streamPills').innerHTML = `<button class="s-pill on">Live HLS Stream</button>`;
  attachStream({play_url: '/gx?u=' + encodeURIComponent(ch.url), kind: 'hls'});
}

function attachStream(s, btnEl){
  if(btnEl){
    document.querySelectorAll('.s-pill').forEach(b => b.classList.remove('on'));
    btnEl.classList.add('on');
  }
  const vid = document.getElementById('vid');
  const url = s.play_url || s.url;
  document.getElementById('pLoader').style.display = 'none';

  if(hlsInst){ hlsInst.destroy(); hlsInst = null; }
  if(dashInst){ dashInst.reset(); dashInst = null; }

  if((s.kind === 'dash' || url.includes('.mpd')) && window.dashjs){
    dashInst = dashjs.MediaPlayer().create();
    dashInst.initialize(vid, url, true);
  } else if((s.kind === 'hls' || url.includes('.m3u8')) && window.Hls && Hls.isSupported()){
    hlsInst = new Hls();
    hlsInst.loadSource(url);
    hlsInst.attachMedia(vid);
    hlsInst.on(Hls.Events.MANIFEST_PARSED, () => vid.play().catch(()=>{}));
  } else {
    vid.src = url;
    vid.play().catch(()=>{});
  }
}

function closePlayer(){
  const vid = document.getElementById('vid');
  vid.pause(); vid.removeAttribute('src'); vid.load();
  if(hlsInst){ hlsInst.destroy(); hlsInst = null; }
  if(dashInst){ dashInst.reset(); dashInst = null; }
  document.getElementById('playerModal').classList.remove('open');
}

function updateWlBtn(){
  if(!activeItem) return;
  const exists = watchlist.some(x => x.id === activeItem.id);
  document.getElementById('wlBtn').textContent = exists ? '✓ In Watchlist' : '+ Watchlist';
}

function toggleWatchlist(){
  if(!activeItem) return;
  const idx = watchlist.findIndex(x => x.id === activeItem.id);
  if(idx >= 0) watchlist.splice(idx, 1);
  else watchlist.unshift(activeItem);
  localStorage.setItem('sh_watchlist', JSON.stringify(watchlist));
  updateWlBtn();
}

function saveQuick(item){
  activeItem = item;
  toggleWatchlist();
  alert('Updated Watchlist: ' + item.title);
}

switchTab('all');
</script>
</body>
</html>
"""


@app.get("/", response_class=HTMLResponse, include_in_schema=False)
@app.get("/site", response_class=HTMLResponse, include_in_schema=False)
async def main_site():
    return HTMLResponse(SITE_HTML)
