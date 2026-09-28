# FastMusic — googlevideo only (no savenow / loader.to / ad CDNs)
from __future__ import annotations

import asyncio
import io
import re
import time
from typing import Optional, Dict, Any, List, Tuple

import httpx
from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import Response, StreamingResponse

router = APIRouter(tags=["FastMusic"])

_CACHE: Dict[str, Any] = {}
_TTL = 480
_UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/122.0.0.0 Safari/537.36"
_UA_YT = "com.google.android.youtube/20.10.38 (Linux; U; Android 14) gzip"

# Blocked ad / broken CDNs — never return or proxy these
_BLOCKED = (
    "savenow.to",
    "savenow.",
    "loader.to",
    "affadaffa.com",
    "p.savenow",
    "byclickdownload",
    "convertx",
)


def _H():
    import api as _api
    return _api


def _vid(id: str) -> str:
    return (id or "").replace("yt:", "").strip()


def _ok_vid(vid: str) -> bool:
    return bool(re.match(r"^[\w-]{6,20}$", vid or ""))


def _blocked(url: str) -> bool:
    u = (url or "").lower()
    return any(b in u for b in _BLOCKED)


def _is_googlevideo(url: str) -> bool:
    u = (url or "").lower()
    return "googlevideo.com" in u or "googleusercontent.com" in u


def _thumb_candidates(vid: str) -> List[str]:
    return [
        f"https://i.ytimg.com/vi/{vid}/maxresdefault.jpg",
        f"https://i.ytimg.com/vi/{vid}/hq720.jpg",
        f"https://i.ytimg.com/vi/{vid}/sddefault.jpg",
        f"https://i.ytimg.com/vi/{vid}/hqdefault.jpg",
    ]


def _item(video_id: str, title: str = "", author: str = "", thumb: str = None, duration=None) -> dict:
    vid = _vid(video_id)
    t1 = thumb or (f"https://i.ytimg.com/vi/{vid}/hq720.jpg" if vid else None)
    return {
        "videoId": vid,
        "title": title or vid,
        "author": author or "Unknown",
        "artist": author or "Unknown",
        "thumbnail": t1,
        "duration": duration,
        "url": f"/stream?id={vid}&type=audio" if vid else None,
    }


def _pick_audio(data: dict) -> Tuple[Optional[str], str, Optional[int], str]:
    streams = list(data.get("audio_streams") or [])
    ua = data.get("client_ua") or _UA_YT

    def score(a):
        url = a.get("url") or ""
        if not url or _blocked(url):
            return -1
        if not _is_googlevideo(url) and "youtube.com" not in url:
            return -1
        itag = str(a.get("itag") or "")
        mime = (a.get("mime") or "").lower()
        s = 300 if itag == "140" else 200 if itag == "139" else 100 if "mp4" in mime else 10
        s += min(int(a.get("bitrate") or 0) // 1000, 40)
        return s

    for a in sorted(streams, key=score, reverse=True):
        if score(a) < 0:
            continue
        mime = (a.get("mime") or "").lower()
        ct = "audio/mp4" if ("mp4" in mime or "mp4a" in mime) else "audio/webm"
        clen = a.get("contentLength") or data.get("content_length")
        try:
            clen = int(clen) if clen else None
        except Exception:
            clen = None
        return a["url"], ct, clen, ua
    url = data.get("audio_url")
    if url and not _blocked(url) and _is_googlevideo(url):
        return url, "audio/mp4", data.get("content_length"), ua
    return None, "audio/mp4", None, ua


def _pick_video(data: dict) -> Tuple[Optional[str], str, Optional[int], str]:
    streams = list(data.get("video_streams") or [])
    ua = data.get("client_ua") or _UA_YT
    cand = [v for v in streams if v.get("url") and not _blocked(v["url"]) and _is_googlevideo(v["url"])]
    cand.sort(key=lambda x: (1 if x.get("progressive") else 0, int(x.get("height") or 0)), reverse=True)
    if cand:
        v = cand[0]
        mime = (v.get("mime") or "video/mp4").split(";")[0]
        clen = v.get("contentLength")
        try:
            clen = int(clen) if clen else None
        except Exception:
            clen = None
        return v["url"], mime, clen, ua
    url = data.get("video_url")
    if url and not _blocked(url) and _is_googlevideo(url):
        return url, "video/mp4", None, ua
    return None, "video/mp4", None, ua


async def _innertube(vid: str) -> dict:
    """Prefer api helper; local ANDROID/IOS race if needed."""
    try:
        api = _H()
        data = await asyncio.wait_for(api._innertube_player(vid, prefer="auto"), timeout=6.0)
        if data.get("ok"):
            return data
    except Exception:
        pass

    clients = [
        {
            "context": {
                "client": {
                    "clientName": "ANDROID",
                    "clientVersion": "20.10.38",
                    "androidSdkVersion": 34,
                    "hl": "en",
                    "gl": "US",
                }
            },
            "ua": _UA_YT,
            "cn": "3",
            "name": "ANDROID",
        },
        {
            "context": {
                "client": {
                    "clientName": "IOS",
                    "clientVersion": "20.10.4",
                    "deviceMake": "Apple",
                    "deviceModel": "iPhone16,2",
                    "osName": "iPhone",
                    "osVersion": "18.2",
                    "hl": "en",
                    "gl": "US",
                }
            },
            "ua": "com.google.ios.youtube/20.10.4 (iPhone16,2; U; CPU iOS 18_2 like Mac OS X)",
            "cn": "5",
            "name": "IOS",
        },
    ]

    async def one(cl):
        body = {
            "context": cl["context"],
            "videoId": vid,
            "contentCheckOk": True,
            "racyCheckOk": True,
        }
        headers = {
            "User-Agent": cl["ua"],
            "Content-Type": "application/json",
            "X-YouTube-Client-Name": cl["cn"],
            "X-YouTube-Client-Version": cl["context"]["client"]["clientVersion"],
        }
        try:
            async with httpx.AsyncClient(timeout=8.0, follow_redirects=True) as c:
                r = await c.post(
                    "https://www.youtube.com/youtubei/v1/player?prettyPrint=false",
                    json=body,
                    headers=headers,
                )
                if r.status_code != 200:
                    return {"ok": False}
                j = r.json()
        except Exception:
            return {"ok": False}
        st = (j.get("playabilityStatus") or {}).get("status")
        if st and st not in ("OK", "LIVE_STREAM_OFFLINE"):
            return {"ok": False, "error": (j.get("playabilityStatus") or {}).get("reason") or st}
        sd = j.get("streamingData") or {}
        formats = list(sd.get("adaptiveFormats") or []) + list(sd.get("formats") or [])
        audio, video = [], []
        for f in formats:
            url = f.get("url")
            if not url or _blocked(url):
                continue
            entry = {
                "url": url,
                "itag": f.get("itag"),
                "mime": f.get("mimeType") or "",
                "bitrate": f.get("bitrate") or f.get("averageBitrate") or 0,
                "contentLength": f.get("contentLength"),
                "approxDurationMs": f.get("approxDurationMs"),
                "height": f.get("height"),
                "progressive": "video/" in (f.get("mimeType") or "") and "audio" in (f.get("mimeType") or ""),
            }
            mime = entry["mime"]
            if "audio" in mime and "video" not in mime.split(";")[0]:
                audio.append(entry)
            elif "video" in mime:
                video.append(entry)
        if not audio and not video:
            return {"ok": False, "error": "no direct urls"}
        vd = j.get("videoDetails") or {}
        duration = None
        try:
            duration = int(vd.get("lengthSeconds") or 0) or None
        except Exception:
            pass
        thumbs = (vd.get("thumbnail") or {}).get("thumbnails") or []
        thumb = thumbs[-1]["url"] if thumbs else f"https://i.ytimg.com/vi/{vid}/hq720.jpg"
        return {
            "ok": True,
            "provider": f"innertube-{cl['name']}",
            "title": vd.get("title") or vid,
            "thumb": thumb,
            "duration": duration,
            "audio_url": audio[0]["url"] if audio else None,
            "video_url": video[0]["url"] if video else None,
            "audio_streams": audio,
            "video_streams": video,
            "client_ua": cl["ua"],
            "content_length": audio[0].get("contentLength") if audio else None,
        }

    tasks = [asyncio.create_task(one(cl)) for cl in clients]
    done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED, timeout=6.0)
    best = None
    for t in done:
        try:
            r = t.result()
            if r.get("ok") and (r.get("audio_url") or r.get("audio_streams")):
                best = r
                break
        except Exception:
            pass
    for t in pending:
        t.cancel()
    return best or {"ok": False, "error": "innertube failed"}


async def _resolve(vid: str, want_video: bool = False) -> dict:
    now = time.time()
    key = f"{vid}:{'v' if want_video else 'a'}"
    hit = _CACHE.get(key)
    if hit and now - hit.get("ts", 0) < _TTL and hit.get("url") and not _blocked(hit["url"]):
        return {"ok": True, **hit}

    data = await _innertube(vid)
    if not data.get("ok"):
        return {"ok": False, "error": data.get("error") or "stream unavailable (no googlevideo)"}

    if want_video:
        url, mime, clen, ua = _pick_video(data)
        if not url:
            url, mime, clen, ua = _pick_audio(data)
    else:
        url, mime, clen, ua = _pick_audio(data)

    if not url or _blocked(url):
        return {"ok": False, "error": "no clean googlevideo url (savenow/loader blocked)"}

    out = {
        "ok": True,
        "url": url,
        "mime": mime,
        "title": data.get("title") or vid,
        "thumb": data.get("thumb") or f"https://i.ytimg.com/vi/{vid}/hq720.jpg",
        "duration": data.get("duration"),
        "provider": data.get("provider") or "innertube",
        "content_length": int(clen) if clen else None,
        "ua": ua or _UA_YT,
        "ts": now,
    }
    _CACHE[key] = out
    return out


async def _proxy(url: str, mime: str, request: Request, *, ua: str, content_length=None, duration=None):
    if _blocked(url):
        raise HTTPException(502, "blocked CDN (savenow/loader)")
    is_gv = _is_googlevideo(url)
    headers = {
        "User-Agent": ua or (_UA_YT if is_gv else _UA),
        "Accept": "*/*",
        "Accept-Encoding": "identity",
        "Referer": "https://www.youtube.com/",
        "Origin": "https://www.youtube.com",
    }
    range_h = request.headers.get("range") or request.headers.get("Range")
    if range_h:
        headers["Range"] = range_h

    client = httpx.AsyncClient(timeout=httpx.Timeout(12.0, connect=8.0, read=60.0), follow_redirects=True)
    try:
        upstream = await client.send(client.build_request("GET", url, headers=headers), stream=True)
    except Exception as e:
        await client.aclose()
        raise HTTPException(502, f"upstream: {e}")

    if upstream.status_code >= 400:
        code = upstream.status_code
        await upstream.aclose()
        await client.aclose()
        raise HTTPException(code, f"upstream HTTP {code}")

    aiter = upstream.aiter_bytes(64 * 1024)
    try:
        first = await aiter.__anext__()
    except StopAsyncIteration:
        await upstream.aclose()
        await client.aclose()
        raise HTTPException(502, "empty")

    if first[:120].lower().startswith(b"<!doctype") or first[:6].lower().startswith(b"<html"):
        await upstream.aclose()
        await client.aclose()
        raise HTTPException(502, "html/ad page")

    ct = (upstream.headers.get("content-type") or mime or "audio/mp4").split(";")[0].strip().lower()
    if first[:3] == b"ID3":
        ct = "audio/mpeg"
    elif b"ftyp" in first[:32]:
        ct = "video/mp4" if "video" in (mime or "") else "audio/mp4"
    elif "webm" in ct:
        ct = ct
    elif "mpeg" in ct or "mp3" in ct:
        ct = "audio/mpeg"
    elif "mp4" in ct:
        ct = ct if "video" in ct else ("video/mp4" if "video" in (mime or "") else "audio/mp4")

    out_h = {
        "Content-Type": ct,
        "Accept-Ranges": "bytes",
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Expose-Headers": "Content-Length, Content-Range, Accept-Ranges, Content-Type",
        "Cache-Control": "public, max-age=120",
    }
    if upstream.headers.get("content-length"):
        out_h["Content-Length"] = upstream.headers["content-length"]
    elif content_length and not range_h:
        out_h["Content-Length"] = str(content_length)
    if upstream.headers.get("content-range"):
        out_h["Content-Range"] = upstream.headers["content-range"]
    if duration:
        try:
            out_h["X-Duration-Seconds"] = str(int(duration))
        except Exception:
            pass
    if content_length:
        try:
            out_h["X-Size-MB"] = f"{int(content_length) / (1024 * 1024):.2f}"
        except Exception:
            pass

    async def body():
        try:
            yield first
            async for chunk in aiter:
                yield chunk
        finally:
            await upstream.aclose()
            await client.aclose()

    return StreamingResponse(body(), status_code=upstream.status_code, media_type=ct, headers=out_h)


def _crop_letterbox(img_bytes: bytes) -> bytes:
    try:
        from PIL import Image
    except Exception:
        return img_bytes
    im = Image.open(io.BytesIO(img_bytes)).convert("RGB")
    w, h = im.size
    px = im.load()

    def row_dark(y, thresh=28):
        step = max(1, w // 40)
        s = sum(sum(px[x, y][:3]) / 3 for x in range(0, w, step))
        return (s / max(1, w // step)) < thresh

    def col_dark(x, thresh=28):
        step = max(1, h // 40)
        s = sum(sum(px[x, y][:3]) / 3 for y in range(0, h, step))
        return (s / max(1, h // step)) < thresh

    top = 0
    while top < h // 3 and row_dark(top):
        top += 1
    bot = h - 1
    while bot > h * 2 // 3 and row_dark(bot):
        bot -= 1
    left = 0
    while left < w // 3 and col_dark(left):
        left += 1
    right = w - 1
    while right > w * 2 // 3 and col_dark(right):
        right -= 1
    if bot - top < h // 4 or right - left < w // 4:
        side = min(w, h)
        left, top = (w - side) // 2, (h - side) // 2
        right, bot = left + side - 1, top + side - 1
    im = im.crop((left, top, right + 1, bot + 1))
    w2, h2 = im.size
    side = min(w2, h2)
    left2, top2 = (w2 - side) // 2, (h2 - side) // 2
    im = im.crop((left2, top2, left2 + side, top2 + side))
    if side > 720:
        im = im.resize((720, 720), Image.LANCZOS)
    buf = io.BytesIO()
    im.save(buf, format="JPEG", quality=90, optimize=True)
    return buf.getvalue()


# ─── STREAM (googlevideo only) ─────────────────────────────────────────────

@router.get("/stream")
@router.get("/download")
@router.get("/fm/download")
@router.get("/api/download")
async def stream(
    request: Request,
    id: str = Query(...),
    type: str = Query("audio"),
    quality: str = Query("best"),
    json: bool = Query(False),
    redirect: bool = Query(False),
    mp4: bool = Query(False),
    video: bool = Query(False),
):
    """
    YouTube audio/video via googlevideo only.
    No savenow.to / loader.to links.
    json=true → directUrl (googlevideo) + size_mb
    mp4=true → video stream
    """
    vid = _vid(id)
    if not _ok_vid(vid):
        raise HTTPException(400, "invalid video id")

    want_video = bool(mp4 or video or (type or "").lower() in ("video", "mp4"))
    resolved = await _resolve(vid, want_video=want_video)
    if not resolved.get("ok"):
        raise HTTPException(502, resolved.get("error") or "no clean stream")

    url = resolved["url"]
    if _blocked(url):
        raise HTTPException(502, "blocked CDN")

    mime = resolved.get("mime") or ("video/mp4" if want_video else "audio/mp4")
    clen = resolved.get("content_length")
    ua = resolved.get("ua") or _UA_YT
    duration = resolved.get("duration")
    size_mb = round(int(clen) / (1024 * 1024), 2) if clen else None

    if json:
        # Only expose directUrl if it's real googlevideo (not ad CDN)
        direct = url if _is_googlevideo(url) and not _blocked(url) else None
        return {
            "ok": True,
            "videoId": vid,
            "title": resolved.get("title"),
            "thumbnail": f"/thumbnailHD?id={vid}",
            "thumbnail_raw": resolved.get("thumb"),
            "url": f"/stream?id={vid}&type={'video' if want_video else 'audio'}",
            "play_url": f"/stream?id={vid}&type={'video' if want_video else 'audio'}",
            "directUrl": direct,
            "download_url": direct,
            "mime": mime,
            "format": "mp4" if want_video else "mp3",
            "duration": duration,
            "contentLength": clen,
            "size_bytes": clen,
            "size_mb": size_mb,
            "provider": resolved.get("provider"),
            "seekable": True,
            "cdn": "googlevideo" if direct else None,
        }

    # never 302 to external — always proxy so ExoPlayer gets stable host + Range
    return await _proxy(url, mime, request, ua=ua, content_length=clen, duration=duration)


@router.get("/player")
async def player_json(id: str = Query(...)):
    vid = _vid(id)
    data = await _innertube(vid)
    if not data.get("ok"):
        return {"ok": False, "videoId": vid, "error": data.get("error"), "play_url": f"/stream?id={vid}"}
    streams = []
    for a in data.get("audio_streams") or []:
        if a.get("url") and not _blocked(a["url"]):
            streams.append({
                "url": a["url"] if _is_googlevideo(a["url"]) else None,
                "itag": a.get("itag"),
                "mimeType": a.get("mime"),
                "bitrate": a.get("bitrate"),
                "contentLength": a.get("contentLength"),
                "kind": "audio",
            })
    return {
        "ok": True,
        "videoId": vid,
        "title": data.get("title"),
        "thumbnail": f"/thumbnailHD?id={vid}",
        "duration": data.get("duration"),
        "provider": data.get("provider"),
        "streams": streams,
        "play_url": f"/stream?id={vid}&type=audio",
        "video_url": f"/stream?id={vid}&mp4=true",
    }


@router.get("/thumbnailHD")
@router.get("/thumbnail")
async def thumbnail_hd(id: str = Query(...), crop: bool = Query(True)):
    vid = _vid(id)
    if not _ok_vid(vid):
        raise HTTPException(400, "invalid video id")
    async with httpx.AsyncClient(timeout=10.0, follow_redirects=True) as client:
        img = None
        for url in _thumb_candidates(vid):
            try:
                r = await client.get(url, headers={"User-Agent": _UA})
                if r.status_code == 200 and len(r.content) > 2000:
                    if len(r.content) < 5000 and "maxres" in url:
                        continue
                    img = r.content
                    break
            except Exception:
                continue
        if not img:
            raise HTTPException(404, "thumbnail not found")
        if crop:
            img = _crop_letterbox(img)
        return Response(
            content=img,
            media_type="image/jpeg",
            headers={
                "Cache-Control": "public, max-age=86400",
                "Content-Length": str(len(img)),
                "Access-Control-Allow-Origin": "*",
            },
        )


@router.get("/search")
async def search(q: str = Query(..., min_length=1), limit: int = Query(20, ge=1, le=40)):
    api = _H()
    songs = await api._ytm_search_songs(q, limit=limit)
    items = [
        _item(s["video_id"], s.get("title") or "", s.get("artist") or "", s.get("thumb"), s.get("duration"))
        for s in songs
    ]
    return {"ok": True, "query": q, "result": items, "items": items, "count": len(items)}


@router.get("/v2/home")
@router.get("/home")
async def home():
    api = _H()
    sections = []
    try:
        for sec in await api._ytm_home_sections():
            items = [
                _item(s["video_id"], s.get("title") or "", s.get("artist") or "", s.get("thumb"))
                for s in (sec.get("items") or [])
                if s.get("video_id")
            ]
            if items:
                sections.append({"title": sec.get("title") or "Home", "items": items})
    except Exception:
        pass
    if not sections:
        songs = await api._ytm_search_songs("Top songs this week", limit=16)
        items = [_item(s["video_id"], s.get("title") or "", s.get("artist") or "", s.get("thumb")) for s in songs]
        sections = [{"title": "Trending", "items": items}]
    flat = [i for s in sections for i in (s.get("items") or [])]
    return {"ok": True, "result": sections, "items": flat}


@router.get("/next")
@router.get("/related")
async def next_songs(id: str = Query(...), limit: int = Query(25, ge=1, le=50)):
    api = _H()
    vid = _vid(id)
    songs = await api._ytm_next_songs(vid, limit=limit)
    items = [_item(s["video_id"], s.get("title") or "", s.get("artist") or "", s.get("thumb")) for s in songs]
    return {"ok": bool(items), "videoId": vid, "result": items, "items": items, "count": len(items)}


@router.get("/getAlbum")
@router.get("/album")
async def get_album(id: str = Query(...)):
    api = _H()
    try:
        data = await api._ytm_post("browse", {"browseId": id})
        raw = []
        api._ytm_walk(data, "musicResponsiveListItemRenderer", raw)
        songs, seen = [], set()
        for it in raw:
            e = api._ytm_parse_item(it)
            if e and e["video_id"] not in seen:
                seen.add(e["video_id"])
                songs.append(_item(e["video_id"], e.get("title") or "", e.get("artist") or "", e.get("thumb")))
        return {"ok": bool(songs), "albumId": id, "result": songs, "items": songs}
    except Exception as e:
        return {"ok": False, "result": [], "error": str(e)[:100]}


@router.get("/getArtists")
@router.get("/artist")
async def get_artists(id: str = Query(...)):
    api = _H()
    try:
        data = await api._ytm_post("browse", {"browseId": id})
        raw = []
        api._ytm_walk(data, "musicResponsiveListItemRenderer", raw)
        songs, seen = [], set()
        for it in raw:
            e = api._ytm_parse_item(it)
            if e and e["video_id"] not in seen:
                seen.add(e["video_id"])
                songs.append(_item(e["video_id"], e.get("title") or "", e.get("artist") or "", e.get("thumb")))
        return {"ok": bool(songs), "artistId": id, "result": songs, "items": songs[:50]}
    except Exception as e:
        return {"ok": False, "result": [], "error": str(e)[:100]}


@router.get("/music/lyrics/plain")
@router.get("/lyrics")
@router.get("/songs/lyrics")
async def lyrics(
    id: Optional[str] = Query(None),
    q: Optional[str] = Query(None),
    title: Optional[str] = Query(None),
    artist: Optional[str] = Query(""),
):
    query = (q or title or "").strip()
    vid = _vid(id) if id else ""
    if vid and not query:
        data = await _innertube(vid)
        if data.get("ok"):
            query = data.get("title") or vid
        else:
            query = vid
    if not query:
        raise HTTPException(400, "id, q, or title required")
    async with httpx.AsyncClient(timeout=12.0) as client:
        r = await client.get("https://lrclib.net/api/search", params={"q": f"{query} {artist or ''}".strip()})
        arr = r.json() if r.status_code == 200 else []
    if not arr:
        return {"ok": False, "success": False, "lyrics": None, "text": None, "message": {"lyrics": None}}
    best = arr[0]
    plain = best.get("plainLyrics") or best.get("syncedLyrics") or ""
    return {
        "ok": True,
        "success": True,
        "videoId": vid or None,
        "title": best.get("trackName") or query,
        "author": best.get("artistName") or artist,
        "lyrics": plain,
        "text": plain,
        "synced": best.get("syncedLyrics"),
        "message": {"lyrics": plain, "text": plain, "synced": best.get("syncedLyrics")},
    }


@router.get("/info")
async def info(id: str = Query(...)):
    vid = _vid(id)
    resolved = await _resolve(vid, want_video=False)
    clen = resolved.get("content_length") if resolved.get("ok") else None
    return {
        "ok": resolved.get("ok", False),
        "videoId": vid,
        "title": resolved.get("title") if resolved.get("ok") else vid,
        "thumbnail": f"/thumbnailHD?id={vid}",
        "duration": resolved.get("duration") if resolved.get("ok") else None,
        "contentLength": clen,
        "size_mb": round(int(clen) / (1024 * 1024), 2) if clen else None,
        "url": f"/stream?id={vid}&type=audio",
        "play_url": f"/stream?id={vid}&type=audio",
        "error": None if resolved.get("ok") else resolved.get("error"),
    }


@router.get("/fm")
async def fm_index():
    return {
        "ok": True,
        "name": "FastMusic",
        "cdn": "googlevideo only — savenow/loader blocked",
        "stream": {
            "audio": "GET /stream?id=VIDEO_ID",
            "video": "GET /stream?id=VIDEO_ID&mp4=true",
            "json": "GET /stream?id=VIDEO_ID&json=true → directUrl (googlevideo) + size_mb",
        },
        "catalog": ["search", "home", "next", "album", "artist", "lyrics", "thumbnailHD", "info", "player"],
    }
