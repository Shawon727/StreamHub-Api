"""
LibreTube / Piped / Invidious helpers for StreamHub API.
Home feed, Shorts, Live, region select, CDN proxy streams.
"""
from __future__ import annotations

import re
from typing import Any, Dict, List, Optional, Tuple

import httpx

UA = "Mozilla/5.0 (Linux; Android 13) AppleWebKit/537.36 Chrome/120.0.0.0 Mobile Safari/537.36 LibreTube"

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

# Common YouTube GL regions for UI select
REGIONS = [
    {"code": "US", "name": "United States"},
    {"code": "IN", "name": "India"},
    {"code": "BD", "name": "Bangladesh"},
    {"code": "GB", "name": "United Kingdom"},
    {"code": "PK", "name": "Pakistan"},
    {"code": "SA", "name": "Saudi Arabia"},
    {"code": "AE", "name": "UAE"},
    {"code": "CA", "name": "Canada"},
    {"code": "AU", "name": "Australia"},
    {"code": "DE", "name": "Germany"},
    {"code": "JP", "name": "Japan"},
    {"code": "KR", "name": "South Korea"},
    {"code": "BR", "name": "Brazil"},
    {"code": "ID", "name": "Indonesia"},
    {"code": "NG", "name": "Nigeria"},
    {"code": "PH", "name": "Philippines"},
    {"code": "TR", "name": "Turkey"},
    {"code": "EG", "name": "Egypt"},
    {"code": "FR", "name": "France"},
    {"code": "MX", "name": "Mexico"},
]

TRENDING_TYPES = ["default", "music", "gaming", "movies"]

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


def normalize_region(region: str) -> str:
    r = (region or "US").strip().upper()
    if len(r) != 2:
        return "US"
    return r


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


def _card_from_inv(item: dict) -> dict:
    """Normalize Invidious search/trending item to a card."""
    vid = item.get("videoId") or item.get("id")
    length = item.get("lengthSeconds")
    thumb = None
    thumbs = item.get("videoThumbnails") or []
    if thumbs:
        thumb = thumbs[0].get("url")
    if not thumb or (isinstance(thumb, str) and thumb.startswith("/")):
        thumb = f"https://i.ytimg.com/vi/{vid}/hqdefault.jpg" if vid else None
    return {
        "videoId": vid,
        "title": item.get("title"),
        "author": item.get("author"),
        "authorId": item.get("authorId"),
        "thumbnail": thumb,
        "duration": length,
        "viewCount": item.get("viewCount"),
        "publishedText": item.get("publishedText"),
        "liveNow": bool(item.get("liveNow")),
        "isShort": bool(length is not None and 0 < length <= 60 and not item.get("liveNow")),
        "type": item.get("type") or "video",
        "url": f"https://www.youtube.com/watch?v={vid}" if vid else None,
        "shorts_url": f"https://www.youtube.com/shorts/{vid}" if vid else None,
    }


# ---------- Invidious video ----------
async def inv_video(video_id: str) -> Tuple[Optional[dict], str, Optional[str]]:
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
        if n == 0 and not data.get("title") and not data.get("liveNow"):
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
    best_audio = _pick_best_audio(audios)
    by_h: Dict[int, dict] = {}
    for m in muxed:
        h = _height(m.get("quality"))
        by_h[h or 360] = {
            "label": m.get("quality") or f"{h}p",
            "height": h or 360,
            "kind": "muxed",
            "url": m.get("url"),
            "video_url": m.get("url"),
            "audio_url": None,
            "mimeType": m.get("mimeType"),
            "itag": m.get("itag"),
            "has_audio": True,
            "play_mode": "single",
        }
    for v in videos:
        mime = (v.get("mimeType") or "")
        if "audio" in mime and not mime.startswith("video"):
            continue
        h = _height(v.get("quality"))
        if not h:
            continue
        prev = by_h.get(h)
        if prev and prev.get("kind") == "muxed":
            continue
        if prev and prev.get("kind") == "paired" and "avc" not in mime and "mp4" not in mime:
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
            "itag": v.get("itag"),
            "audio_itag": best_audio.get("itag"),
            "has_audio": True,
            "play_mode": "dual",
        }
    return [by_h[k] for k in sorted(by_h.keys(), reverse=True)]


def normalize_invidious(data: dict, instance: str, vid: str) -> dict:
    videos, audios, muxed = [], [], []
    live_now = bool(data.get("liveNow"))
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

    # Live: prefer DASH/HLS; companion itag often 403 on live
    if live_now:
        if dash:
            direct = dash
        elif hls:
            direct = hls
        default_quality = {
            "label": "live",
            "kind": "live",
            "url": direct,
            "video_url": direct,
            "audio_url": None,
            "has_audio": True,
            "play_mode": "single",
            "note": "Live — use dash/hls in a player that supports MPEG-DASH/HLS",
        }
        qualities = [default_quality] + qualities

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
        "liveNow": live_now,
        "isUpcoming": bool(data.get("isUpcoming")),
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
        "embed_url": f"https://www.youtube.com/embed/{vid}?autoplay=1",
        "how_to_play": (
            "VOD: qualities muxed=single url; paired=video_url+audio_url together. "
            "LIVE: use dash or hls (or embed_url). Companion itag proxy may 403 on live."
        ),
    }


# ---------- Piped ----------
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
            if not (data.get("videoStreams") or data.get("audioStreams") or data.get("title") or data.get("items")):
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
        "liveNow": bool(data.get("livestream")),
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
        "embed_url": f"https://www.youtube.com/embed/{vid}?autoplay=1",
        "how_to_play": "qualities muxed=single; paired=video+audio dual play.",
    }


async def get_streams(video_id: str) -> dict:
    """Invidious first (proxy CDN), then Piped. Live uses dash/hls."""
    vid = extract_video_id(video_id)
    inv_data, inv_inst, inv_err = await inv_video(vid)
    if inv_data and (
        inv_data.get("formatStreams")
        or inv_data.get("adaptiveFormats")
        or inv_data.get("liveNow")
        or inv_data.get("dashUrl")
    ):
        return normalize_invidious(inv_data, inv_inst, vid)

    piped_data, piped_inst, piped_err = await piped_get(f"/streams/{vid}")
    if piped_data and isinstance(piped_data, dict):
        out = normalize_piped(piped_data, piped_inst, vid)
        if out.get("cdn_count") or out.get("hls") or out.get("dash"):
            return out
        if out.get("title"):
            out["note"] = "Metadata only — streams blocked"
            out["errors"] = [x for x in (inv_err, piped_err) if x]
            return out

    return {
        "videoId": vid,
        "title": (inv_data or {}).get("title") if isinstance(inv_data, dict) else None,
        "videoStreams": [],
        "audioStreams": [],
        "muxedStreams": [],
        "qualities": [],
        "direct_play": None,
        "cdn_count": 0,
        "thumbnail": f"https://i.ytimg.com/vi/{vid}/hqdefault.jpg",
        "errors": [x for x in (inv_err, piped_err) if x],
        "note": "All extractors failed. Retry later or use embed_url.",
        "embed_url": f"https://www.youtube.com/embed/{vid}?autoplay=1",
    }


async def piped_path(path: str, params: Optional[dict] = None) -> Tuple[Any, str]:
    data, inst, err = await piped_get(path, params)
    if data is None:
        raise RuntimeError(err or "piped failed")
    return data, inst


# ---------- Home / Trending / Shorts / Live ----------
async def inv_trending(region: str = "US", type_: str = "default") -> Tuple[List[dict], str, Optional[str]]:
    region = normalize_region(region)
    type_ = (type_ or "default").lower()
    if type_ not in TRENDING_TYPES:
        type_ = "default"
    errors = []
    for base in INVIDIOUS_INSTANCES:
        data, err = await _fetch_json(f"{base}/api/v1/trending", {"region": region, "type": type_})
        if err:
            errors.append(f"{base}: {err}")
            continue
        if isinstance(data, list) and data:
            return [_card_from_inv(x) for x in data if x.get("videoId") or x.get("type") == "video"], base, None
    return [], "", "; ".join(errors[:3])


async def inv_popular(region: str = "US") -> Tuple[List[dict], str, Optional[str]]:
    """Popular feed (region hint only — some instances ignore region)."""
    errors = []
    for base in INVIDIOUS_INSTANCES:
        data, err = await _fetch_json(f"{base}/api/v1/popular")
        if err:
            errors.append(f"{base}: {err}")
            continue
        if isinstance(data, list) and data:
            return [_card_from_inv(x) for x in data if x.get("videoId")], base, None
    return [], "", "; ".join(errors[:3])


async def inv_search(
    q: str,
    region: str = "US",
    type_: str = "video",
    page: int = 1,
) -> Tuple[List[dict], str, Optional[str]]:
    region = normalize_region(region)
    params = {"q": q, "region": region, "type": type_, "page": page}
    errors = []
    for base in INVIDIOUS_INSTANCES:
        data, err = await _fetch_json(f"{base}/api/v1/search", params)
        if err:
            errors.append(f"{base}: {err}")
            continue
        if isinstance(data, list):
            return [_card_from_inv(x) for x in data], base, None
    return [], "", "; ".join(errors[:3])


async def get_home(region: str = "US") -> dict:
    """YouTube-like home: popular + trending sections."""
    region = normalize_region(region)
    popular, p_inst, p_err = await inv_popular(region)
    trending, t_inst, t_err = await inv_trending(region, "default")
    music, m_inst, _ = await inv_trending(region, "music")
    gaming, g_inst, _ = await inv_trending(region, "gaming")
    movies, mo_inst, _ = await inv_trending(region, "movies")
    return {
        "region": region,
        "sections": [
            {"id": "popular", "title": "Popular", "items": popular[:20]},
            {"id": "trending", "title": "Trending", "items": trending[:20]},
            {"id": "music", "title": "Music", "items": music[:12]},
            {"id": "gaming", "title": "Gaming", "items": gaming[:12]},
            {"id": "movies", "title": "Movies", "items": movies[:12]},
        ],
        "instances": {"popular": p_inst, "trending": t_inst, "music": m_inst},
        "errors": [x for x in (p_err, t_err) if x],
        "how": "Pick videoId → /lt/streams?id= or /lt/play?id=&quality=best. Change region=IN|BD|US|...",
    }


async def get_shorts(region: str = "US", q: str = "shorts", page: int = 1) -> dict:
    """
    Shorts feed: pull trending/popular + targeted searches, keep duration <= 60s.
    (YouTube has no public shorts shelf on Invidious — we filter by length.)
    """
    region = normalize_region(region)
    q = (q or "shorts").strip()
    seen = set()
    items = []
    inst = ""

    async def _add(cards, require_short_dur=True):
        nonlocal items, seen
        for c in cards:
            vid = c.get("videoId")
            if not vid or vid in seen:
                continue
            if c.get("liveNow"):
                continue
            dur = c.get("duration")
            if require_short_dur:
                if not isinstance(dur, int) or dur <= 0 or dur > 60:
                    continue
            else:
                if isinstance(dur, int) and dur > 60:
                    continue
            c["isShort"] = True
            if not c.get("thumbnail") or str(c.get("thumbnail", "")).startswith("/"):
                c["thumbnail"] = f"https://i.ytimg.com/vi/{vid}/hqdefault.jpg"
            seen.add(vid)
            items.append(c)

    # 1) Trending / popular short clips
    for type_ in ("default", "music", "gaming"):
        cards, inst, _ = await inv_trending(region, type_)
        await _add(cards, require_short_dur=True)
    pop, inst, _ = await inv_popular(region)
    await _add(pop, require_short_dur=True)

    # 2) Search phrases that often return real short videos
    searches = [q, f"{q} short video", "funny short", "viral short video", "wait for it", "oddly satisfying"]
    for query in searches:
        cards, inst, _ = await inv_search(query, region=region, type_="video", page=page)
        await _add(cards, require_short_dur=True)
        if len(items) >= 30:
            break

    return {
        "region": region,
        "query": q,
        "items": items[:40],
        "count": len(items[:40]),
        "instance": inst,
        "how": "Play with /lt/streams?id=VIDEO_ID (companion proxy)",
    }


async def get_live(region: str = "US", q: str = "live") -> dict:
    """Live streams list. Playback: dash/hls or embed (itag proxy often fails on live)."""
    region = normalize_region(region)
    cards, inst, err = await inv_search(q or "live", region=region, type_="video")
    # also news live
    cards2, _, _ = await inv_search("live news", region=region, type_="video")
    seen = set()
    items = []
    for c in cards + cards2:
        vid = c.get("videoId")
        if not vid or vid in seen:
            continue
        if c.get("liveNow") or c.get("duration") == 0:
            c["liveNow"] = True
            c["play_hint"] = f"/lt/streams?id={vid} → use dash or embed_url"
            seen.add(vid)
            items.append(c)
    return {
        "region": region,
        "query": q,
        "items": items[:40],
        "count": len(items[:40]),
        "instance": inst,
        "error": err,
        "how": (
            "Live playback: GET /lt/streams?id=ID → prefer dash/hls single URL, "
            "or embed_url. Companion itag proxy often returns 403 for live."
        ),
    }


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
