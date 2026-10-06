"""
LibreTube / Piped / Invidious / SoundCloud / CCC helpers for StreamHub API.
Extracted from com.github.libretube APK APIs.
"""
from __future__ import annotations

import asyncio
import re
from typing import Any, Dict, List, Optional, Tuple

import httpx

UA = "Mozilla/5.0 (Linux; Android 13) AppleWebKit/537.36 Chrome/120.0.0.0 Mobile Safari/537.36 LibreTube"

# Working-first order (tested 2026-10)
INVIDIOUS_INSTANCES = [
    "https://invidious.flokinet.to",
    "https://inv.nadeko.net",
    "https://invidious.private.coffee",
    "https://vid.puffyan.us",
    "https://yewtu.be",
]

PIPED_INSTANCES = [
    "https://pipedapi.ducks.party",
    "https://api.piped.private.coffee",
    "https://pipedapi.kavin.rocks",
    "https://pipedapi.lunar.icu",
    "https://pipedapi.adminforge.de",
]

SC_API = "https://api-v2.soundcloud.com"
SC_CLIENT_FALLBACK = "iZIs9mchVcX5lhVRyQGGAYlNPVldzAoX"
CCC_API = "https://api.media.ccc.de"
SPONSOR_API = "https://sponsor.ajay.app/api"
DEARROW_THUMB = "https://dearrow-thumb.ajay.app/api/v1/getThumbnail"

_sc_client_id: Optional[str] = None


def _client(timeout: float = 15.0) -> httpx.AsyncClient:
    return httpx.AsyncClient(
        timeout=httpx.Timeout(timeout, connect=8.0),
        follow_redirects=True,
        limits=httpx.Limits(max_connections=20, max_keepalive_connections=5),
        headers={"User-Agent": UA, "Accept": "application/json"},
    )



def inv_proxy_url(instance: str, video_id: str, itag) -> str:
    """Playable URL via Invidious companion (avoids googlevideo IP 403)."""
    return f"{instance.rstrip('/')}/companion/latest_version?id={video_id}&itag={itag}&local=true"

def extract_video_id(raw: str) -> str:
    s = (raw or "").strip()
    if re.fullmatch(r"[A-Za-z0-9_-]{6,20}", s):
        return s
    m = re.search(r"(?:v=|/shorts/|youtu\.be/|embed/)([A-Za-z0-9_-]{6,20})", s)
    return m.group(1) if m else s


async def _fetch_json(url: str, params: Optional[dict] = None, timeout: float = 14.0) -> Tuple[Optional[Any], Optional[str]]:
    try:
        async with _client(timeout) as client:
            r = await client.get(url, params=params)
            if r.status_code >= 400:
                return None, f"HTTP {r.status_code}"
            if not r.text or r.text[:1] not in "{[":
                return None, "non-json"
            return r.json(), None
    except Exception as e:
        return None, str(e)[:120]


# ---------- Invidious ----------
async def inv_video(video_id: str) -> Tuple[Optional[dict], str, Optional[str]]:
    """Return (data, instance, error). Prefer instances with formatStreams."""
    vid = extract_video_id(video_id)
    errors = []
    for base in INVIDIOUS_INSTANCES:
        data, err = await _fetch_json(f"{base}/api/v1/videos/{vid}")
        if err:
            errors.append(f"{base}: {err}")
            continue
        if not isinstance(data, dict):
            errors.append(f"{base}: bad body")
            continue
        if data.get("error"):
            errors.append(f"{base}: {data.get('error')}")
            continue
        n = len(data.get("formatStreams") or []) + len(data.get("adaptiveFormats") or [])
        if n == 0 and not data.get("title"):
            errors.append(f"{base}: empty")
            continue
        return data, base, None
    return None, "", "; ".join(errors[:4]) or "all invidious failed"


def _height(q) -> int:
    if not q:
        return 0
    m = re.search(r"(\d{3,4})", str(q))
    return int(m.group(1)) if m else 0


def _pick_best_audio(audios: list) -> Optional[dict]:
    """Prefer m4a/mp4 audio medium, then highest bitrate."""
    if not audios:
        return None
    def score(a):
        mime = (a.get("mimeType") or "").lower()
        br = int(a.get("bitrate") or 0)
        q = str(a.get("quality") or "")
        s = br
        if "mp4" in mime or "mp4a" in mime:
            s += 50_000
        if "MEDIUM" in q:
            s += 20_000
        if "HIGH" in q:
            s += 30_000
        return s
    return max(audios, key=score)


def _build_qualities(muxed: list, videos: list, audios: list) -> list:
    """
    Each entry is playable with audio:
    - kind=muxed → single url (video+audio)
    - kind=paired → video_url + audio_url (play together in ExoPlayer/VLC dual)
    """
    best_audio = _pick_best_audio(audios)
    by_h = {}
    # muxed first
    for m in muxed:
        h = _height(m.get("quality"))
        by_h[h or 360] = {
            "label": m.get("quality") or f"{h}p",
            "height": h or 360,
            "kind": "muxed",
            "url": m.get("url"),
            "video_url": m.get("url"),
            "audio_url": None,  # already in mux
            "mimeType": m.get("mimeType"),
            "has_audio": True,
            "play_mode": "single",
        }
    # adaptive video-only paired with best audio
    for v in videos:
        if not v.get("videoOnly") and v.get("url") in {x.get("url") for x in muxed}:
            continue
        if not v.get("videoOnly") and not v.get("mimeType", "").startswith("video"):
            continue
        # only pure video tracks for pairing
        mime = (v.get("mimeType") or "")
        if "audio" in mime and not mime.startswith("video"):
            continue
        h = _height(v.get("quality"))
        if not h:
            continue
        # prefer avc/mp4 video when overwriting same height
        prev = by_h.get(h)
        if prev and prev.get("kind") == "muxed":
            continue  # keep muxed for that height
        if prev and prev.get("kind") == "paired":
            # prefer mp4/avc
            if "avc" not in mime and "mp4" not in mime:
                continue
        if not best_audio:
            continue
        by_h[h] = {
            "label": v.get("quality") or f"{h}p",
            "height": h,
            "kind": "paired",
            "url": None,
            "video_url": v.get("url"),
            "audio_url": best_audio.get("url"),
            "video_mime": mime,
            "audio_mime": best_audio.get("mimeType"),
            "audio_quality": best_audio.get("quality"),
            "has_audio": True,
            "play_mode": "dual",  # client must play video+audio together
        }
    qualities = [by_h[k] for k in sorted(by_h.keys(), reverse=True)]
    return qualities


def normalize_invidious(data: dict, instance: str, vid: str) -> dict:
    videos, audios, muxed = [], [], []
    for f in (data.get("formatStreams") or []):
        itag = f.get("itag")
        raw = f.get("url")
        if not itag and not raw:
            continue
        play = inv_proxy_url(instance, vid, itag) if itag else raw
        item = {
            "url": play,
            "raw_url": raw,
            "itag": itag,
            "quality": f.get("qualityLabel") or f.get("quality"),
            "mimeType": f.get("type"),
            "bitrate": f.get("bitrate"),
            "container": f.get("container"),
            "videoOnly": False,
            "source": "invidious-muxed",
        }
        muxed.append(item)
        videos.append(item)
    for f in (data.get("adaptiveFormats") or []):
        itag = f.get("itag")
        raw = f.get("url")
        if not itag and not raw:
            continue
        t = (f.get("type") or "")
        play = inv_proxy_url(instance, vid, itag) if itag else raw
        item = {
            "url": play,
            "raw_url": raw,
            "itag": itag,
            "quality": f.get("qualityLabel") or f.get("quality") or f.get("audioQuality"),
            "mimeType": t,
            "bitrate": f.get("bitrate"),
            "container": f.get("container"),
            "videoOnly": t.startswith("video") and "audio" not in t.split(";")[0],
            "source": "invidious-adaptive",
        }
        if t.startswith("audio"):
            audios.append(item)
        else:
            videos.append(item)

    qualities = _build_qualities(muxed, videos, audios)
    # ensure every quality uses proxy urls (already from items)
    direct = None
    default_quality = None
    for q in qualities:
        if q.get("kind") == "muxed" and q.get("url"):
            direct = q["url"]
            default_quality = q
            break
    if not default_quality and qualities:
        default_quality = qualities[0]
        direct = default_quality.get("url") or default_quality.get("video_url")

    thumbs = data.get("videoThumbnails") or []
    thumb = thumbs[0].get("url") if thumbs else None
    hls = data.get("hlsUrl") or data.get("hls")
    dash = data.get("dashUrl") or data.get("dash")
    if dash and str(dash).startswith("/"):
        dash = instance.rstrip("/") + str(dash)
    if hls and str(hls).startswith("/"):
        hls = instance.rstrip("/") + str(hls)
    # prefer companion dash if relative handled; also expose local latest list

    return {
        "videoId": vid,
        "title": data.get("title"),
        "description": (data.get("description") or "")[:500],
        "thumbnail": thumb or f"https://i.ytimg.com/vi/{vid}/hqdefault.jpg",
        "duration": data.get("lengthSeconds"),
        "uploader": data.get("author"),
        "uploaderUrl": data.get("authorUrl"),
        "views": data.get("viewCount"),
        "likes": data.get("likeCount"),
        "hls": hls,
        "dash": dash,
        "qualities": qualities,
        "default_quality": default_quality,
        "videoStreams": videos,
        "audioStreams": audios,
        "muxedStreams": muxed,
        "direct_play": direct,
        "cdn_count": len(videos) + len(audios),
        "instance": instance,
        "provider": "invidious",
        "recommended": (data.get("recommendedVideos") or [])[:12],
        "how_to_play": (
            "ALL urls are Invidious companion proxy (no googlevideo 403). "
            "qualities[].kind=muxed → play url alone (video+audio). "
            "kind=paired → play video_url + audio_url TOGETHER. "
            "dash/hls → single adaptive URL with audio."
        ),
    }


async def piped_get(path: str, params: Optional[dict] = None) -> Tuple[Optional[Any], str, Optional[str]]:
    errors = []
    for base in PIPED_INSTANCES:
        url = base.rstrip("/") + (path if path.startswith("/") else "/" + path)
        data, err = await _fetch_json(url, params)
        if err:
            errors.append(f"{base}: {err}")
            continue
        if isinstance(data, dict) and data.get("error"):
            err_s = str(data.get("error"))
            if "SignInConfirmNotBot" in err_s or "LOGIN_REQUIRED" in err_s:
                errors.append(f"{base}: bot-check")
                continue
            # still return if has streams
            if not (data.get("videoStreams") or data.get("audioStreams") or data.get("title")):
                errors.append(f"{base}: {err_s[:80]}")
                continue
        return data, base, None
    return None, "", "; ".join(errors[:5]) or "all piped failed"


def normalize_piped(data: dict, instance: str, vid: str) -> dict:
    videos, audios = [], []
    for v in (data.get("videoStreams") or []):
        u = v.get("url")
        if not u:
            continue
        videos.append({
            "url": u,
            "quality": v.get("quality"),
            "mimeType": v.get("mimeType"),
            "codec": v.get("codec"),
            "bitrate": v.get("bitrate"),
            "videoOnly": v.get("videoOnly"),
            "format": v.get("format"),
            "source": "piped",
        })
    for a in (data.get("audioStreams") or []):
        u = a.get("url")
        if not u:
            continue
        audios.append({
            "url": u,
            "quality": a.get("quality"),
            "mimeType": a.get("mimeType"),
            "codec": a.get("codec"),
            "bitrate": a.get("bitrate"),
            "format": a.get("format"),
            "source": "piped",
        })
    muxed = [v for v in videos if not v.get("videoOnly")]
    only_video = [v for v in videos if v.get("videoOnly")]
    qualities = _build_qualities(muxed, only_video or videos, audios)
    direct = None
    default_quality = None
    for q in qualities:
        if q.get("kind") == "muxed" and q.get("url"):
            direct = q["url"]
            default_quality = q
            break
    if not default_quality and qualities:
        default_quality = qualities[0]
        direct = default_quality.get("url") or default_quality.get("video_url")
    if not direct:
        direct = data.get("hls") or data.get("dash")
    return {
        "videoId": vid,
        "title": data.get("title"),
        "description": (data.get("description") or "")[:500],
        "thumbnail": data.get("thumbnailUrl") or f"https://i.ytimg.com/vi/{vid}/hqdefault.jpg",
        "duration": data.get("duration"),
        "uploader": data.get("uploader"),
        "uploaderUrl": data.get("uploaderUrl"),
        "views": data.get("views"),
        "likes": data.get("likes"),
        "hls": data.get("hls"),
        "dash": data.get("dash"),
        "qualities": qualities,
        "default_quality": default_quality,
        "videoStreams": videos,
        "audioStreams": audios,
        "muxedStreams": muxed,
        "direct_play": direct,
        "cdn_count": len(videos) + len(audios),
        "related": (data.get("relatedStreams") or [])[:12],
        "subtitles": data.get("subtitles") or [],
        "instance": instance,
        "provider": "piped",
        "error": data.get("error"),
        "how_to_play": (
            "qualities[].kind=muxed → single url. "
            "kind=paired → video_url + audio_url together."
        ),
    }


async def get_streams(video_id: str) -> dict:
    """Invidious first (real googlevideo CDN), then Piped."""
    vid = extract_video_id(video_id)

    # 1) Invidious (best CDN)
    inv_data, inv_inst, inv_err = await inv_video(vid)
    if inv_data and (inv_data.get("formatStreams") or inv_data.get("adaptiveFormats")):
        return normalize_invidious(inv_data, inv_inst, vid)

    # 2) Piped fallback
    piped_data, piped_inst, piped_err = await piped_get(f"/streams/{vid}")
    if piped_data and isinstance(piped_data, dict):
        out = normalize_piped(piped_data, piped_inst, vid)
        if out.get("cdn_count"):
            return out
        if out.get("title"):
            out["note"] = "Metadata only — streams blocked by YouTube bot-check on instance IP"
            out["errors"] = [x for x in (inv_err, piped_err) if x]
            return out

    return {
        "videoId": vid,
        "title": (inv_data or {}).get("title") if isinstance(inv_data, dict) else None,
        "videoStreams": [],
        "audioStreams": [],
        "muxedStreams": [],
        "direct_play": None,
        "cdn_count": 0,
        "thumbnail": f"https://i.ytimg.com/vi/{vid}/hqdefault.jpg",
        "errors": [x for x in (inv_err, piped_err) if x],
        "note": "All extractors failed (YouTube bot-check or instance down). Retry later.",
        "embed_url": f"https://www.youtube.com/embed/{vid}?autoplay=1&rel=0",
    }


async def piped_path(path: str, params: Optional[dict] = None) -> Tuple[Any, str]:
    data, inst, err = await piped_get(path, params)
    if data is None:
        raise RuntimeError(err or "piped failed")
    return data, inst


# ---------- SoundCloud ----------
async def sc_client_id() -> str:
    global _sc_client_id
    if _sc_client_id:
        return _sc_client_id
    try:
        async with _client(12.0) as client:
            r = await client.get("https://soundcloud.com", headers={"User-Agent": UA, "Accept": "text/html"})
            scripts = re.findall(r'src="(https://a-v2\.sndcdn\.com/assets/[^"]+\.js)"', r.text)
            for su in scripts[:6]:
                try:
                    js = (await client.get(su)).text
                    m = re.search(r'client_id["\s:=]+["\']([a-zA-Z0-9]{32})["\']', js)
                    if m:
                        _sc_client_id = m.group(1)
                        return _sc_client_id
                except Exception:
                    continue
    except Exception:
        pass
    return SC_CLIENT_FALLBACK


async def sc_get(path: str, params: Optional[dict] = None) -> dict:
    cid = await sc_client_id()
    p = dict(params or {})
    p["client_id"] = cid
    async with _client(15.0) as client:
        r = await client.get(SC_API + path, params=p)
        if r.status_code == 401:
            p["client_id"] = SC_CLIENT_FALLBACK
            r = await client.get(SC_API + path, params=p)
        r.raise_for_status()
        return r.json()


async def sc_stream_url(track: dict) -> Optional[str]:
    media = (track.get("media") or {}).get("transcodings") or []
    progressive = None
    for tr in media:
        if (tr.get("format") or {}).get("protocol") == "progressive":
            progressive = tr.get("url")
            break
    if not progressive and media:
        progressive = media[0].get("url")
    if not progressive:
        return None
    cid = await sc_client_id()
    async with _client(12.0) as client:
        r = await client.get(progressive, params={"client_id": cid})
        if r.status_code >= 400:
            return None
        return (r.json() or {}).get("url")
