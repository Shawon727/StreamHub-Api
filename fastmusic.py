# FastMusic — clean stream + catalog (SimpMusic / vivi style)
# Place next to api.py. Only this file needs music stream fixes.
from __future__ import annotations

import asyncio
import io
import re
import time
from typing import Optional, Dict, Any, List, Tuple

import httpx
from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import RedirectResponse, Response, StreamingResponse

router = APIRouter(tags=["FastMusic"])

_CACHE: Dict[str, Any] = {}
_TTL = 600
_UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/122.0.0.0 Safari/537.36"
_UA_YT = "com.google.android.youtube/20.10.38 (Linux; U; Android 14) gzip"


def _H():
    import api as _api
    return _api


def _vid(id: str) -> str:
    return (id or "").replace("yt:", "").strip()


def _ok_vid(vid: str) -> bool:
    return bool(re.match(r"^[\w-]{6,20}$", vid or ""))


def _thumb_candidates(vid: str) -> List[str]:
    # HD first, then clean sizes (less letterbox than hqdefault)
    return [
        f"https://i.ytimg.com/vi/{vid}/maxresdefault.jpg",
        f"https://i.ytimg.com/vi/{vid}/hq720.jpg",
        f"https://i.ytimg.com/vi/{vid}/sddefault.jpg",
        f"https://i.ytimg.com/vi/{vid}/hqdefault.jpg",
        f"https://i.ytimg.com/vi/{vid}/mqdefault.jpg",
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


def _best_audio(data: dict) -> Tuple[Optional[str], str, Optional[str], Optional[str]]:
    streams = list(data.get("audio_streams") or [])
    ua = data.get("client_ua") or _UA_YT

    def score(a):
        if not a.get("url"):
            return -1
        itag = str(a.get("itag") or "")
        mime = (a.get("mime") or "").lower()
        s = 300 if itag == "140" else 200 if itag == "139" else 100 if "mp4" in mime else 10
        s += min(int(a.get("bitrate") or 0) // 1000, 40)
        return s

    for a in sorted(streams, key=score, reverse=True):
        if a.get("url"):
            mime = (a.get("mime") or "").lower()
            ct = "audio/mp4" if ("mp4" in mime or "mp4a" in mime) else "audio/webm"
            return a["url"], ct, a.get("contentLength") or data.get("content_length"), ua
    if data.get("audio_url"):
        u = data["audio_url"]
        return u, ("audio/webm" if "webm" in u else "audio/mp4"), data.get("content_length"), ua
    return None, "audio/mp4", None, ua


def _best_video(data: dict) -> Tuple[Optional[str], str, Optional[str], Optional[str]]:
    streams = list(data.get("video_streams") or [])
    ua = data.get("client_ua") or _UA_YT
    # progressive or highest with url
    progressive = [v for v in streams if v.get("url") and v.get("progressive")]
    if progressive:
        progressive.sort(key=lambda x: int(x.get("height") or 0), reverse=True)
        v = progressive[0]
        return v["url"], "video/mp4", v.get("contentLength"), ua
    with_url = [v for v in streams if v.get("url")]
    with_url.sort(key=lambda x: int(x.get("height") or 0), reverse=True)
    if with_url:
        v = with_url[0]
        mime = (v.get("mime") or "video/mp4").split(";")[0]
        return v["url"], mime, v.get("contentLength"), ua
    if data.get("video_url"):
        return data["video_url"], "video/mp4", None, ua
    return None, "video/mp4", None, ua


async def _head_len(url: str, ua: str) -> Optional[int]:
    try:
        headers = {"User-Agent": ua or _UA, "Range": "bytes=0-0"}
        if "googlevideo.com" in url:
            headers["Referer"] = "https://www.youtube.com/"
        async with httpx.AsyncClient(timeout=8.0, follow_redirects=True) as c:
            r = await c.get(url, headers=headers)
            cr = r.headers.get("content-range") or ""
            m = re.search(r"/(\d+)$", cr)
            if m:
                return int(m.group(1))
            if r.headers.get("content-length"):
                return int(r.headers["content-length"])
    except Exception:
        pass
    return None


async def _resolve(vid: str, want_video: bool = False) -> dict:
    now = time.time()
    key = f"{vid}:{'v' if want_video else 'a'}"
    hit = _CACHE.get(key)
    if hit and now - hit.get("ts", 0) < _TTL and hit.get("url"):
        return {"ok": True, **hit}

    api = _H()
    title, thumb, duration = vid, f"https://i.ytimg.com/vi/{vid}/hq720.jpg", None
    url = mime = clen = ua = provider = None

    try:
        data = await asyncio.wait_for(api._innertube_player(vid, prefer="auto"), timeout=5.0)
        if data.get("ok"):
            title = data.get("title") or title
            thumb = data.get("thumb") or thumb
            duration = data.get("duration")
            if want_video:
                url, mime, clen, ua = _best_video(data)
                if not url:
                    url, mime, clen, ua = _best_audio(data)
            else:
                url, mime, clen, ua = _best_audio(data)
            provider = data.get("provider") or "innertube"
    except Exception:
        pass

    if not url:
        try:
            fmt = "mp4" if want_video else "mp3"
            ld = await api._loader_to_youtube(vid, fmt=fmt)
            if ld.get("ok") and ld.get("url"):
                url = ld["url"]
                title = ld.get("title") or title
                thumb = ld.get("thumb") or thumb
                mime = "video/mp4" if want_video else "audio/mpeg"
                provider = "loader.to"
                ua = _UA
                if ld.get("duration"):
                    duration = ld.get("duration")
                if ld.get("contentLength") or ld.get("filesize"):
                    clen = ld.get("contentLength") or ld.get("filesize")
            else:
                return {"ok": False, "error": (ld or {}).get("error") or "no stream"}
        except Exception as e:
            return {"ok": False, "error": str(e)[:120]}

    if url and not clen:
        clen = await _head_len(url, ua or _UA)

    out = {
        "ok": True,
        "url": url,
        "mime": mime or ("video/mp4" if want_video else "audio/mp4"),
        "title": title,
        "thumb": thumb,
        "duration": duration,
        "provider": provider,
        "content_length": int(clen) if clen else None,
        "ua": ua or _UA_YT,
        "ts": now,
        "want_video": want_video,
    }
    _CACHE[key] = out
    return out


async def _proxy(url: str, mime: str, request: Request, *, ua: str, content_length=None, duration=None):
    is_gv = "googlevideo.com" in url
    headers = {
        "User-Agent": ua or (_UA_YT if is_gv else _UA),
        "Accept": "*/*",
        "Accept-Encoding": "identity",
        "Referer": "https://www.youtube.com/" if is_gv else "https://loader.to/",
    }
    if is_gv:
        headers["Origin"] = "https://www.youtube.com"
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
        raise HTTPException(502, "html/ad blocked")

    ct = (upstream.headers.get("content-type") or mime or "audio/mp4").split(";")[0].strip().lower()
    if first[:3] == b"ID3":
        ct = "audio/mpeg"
    elif b"ftyp" in first[:32]:
        ct = "video/mp4" if "video" in (mime or "") else "audio/mp4"
    elif "webm" in ct:
        ct = "video/webm" if "video" in ct else "audio/webm"
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
    """Remove near-black bars (top/bottom or sides) like SimpMusic square cover."""
    try:
        from PIL import Image
        import numpy as np  # may fail
    except Exception:
        try:
            from PIL import Image
        except Exception:
            return img_bytes
        im = Image.open(io.BytesIO(img_bytes)).convert("RGB")
        w, h = im.size
        # sample rows/cols for blackness
        px = im.load()

        def row_dark(y, thresh=28):
            s = 0
            step = max(1, w // 40)
            for x in range(0, w, step):
                r, g, b = px[x, y]
                s += (r + g + b) / 3
            return (s / max(1, w // step)) < thresh

        def col_dark(x, thresh=28):
            s = 0
            step = max(1, h // 40)
            for y in range(0, h, step):
                r, g, b = px[x, y]
                s += (r + g + b) / 3
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
            # fallback center square
            side = min(w, h)
            left = (w - side) // 2
            top = (h - side) // 2
            right = left + side - 1
            bot = top + side - 1
        im = im.crop((left, top, right + 1, bot + 1))
        # optional square pad/crop to 1:1
        w2, h2 = im.size
        side = min(w2, h2)
        left2 = (w2 - side) // 2
        top2 = (h2 - side) // 2
        im = im.crop((left2, top2, left2 + side, top2 + side))
        if side > 720:
            im = im.resize((720, 720), Image.LANCZOS)
        buf = io.BytesIO()
        im.save(buf, format="JPEG", quality=90, optimize=True)
        return buf.getvalue()
    return img_bytes


# ─── STREAM ────────────────────────────────────────────────────────────────

@router.get("/stream")
@router.get("/download")
@router.get("/fm/download")
@router.get("/api/download")
async def stream(
    request: Request,
    id: str = Query(...),
    type: str = Query("audio", description="audio | video"),
    quality: str = Query("best"),
    json: bool = Query(False, description="true = JSON meta + directUrl"),
    redirect: bool = Query(False, description="302 to CDN (googlevideo only)"),
    mp4: bool = Query(False, description="true = video/mp4 stream"),
    video: bool = Query(False, description="alias of mp4=true"),
):
    """
    Play / download YouTube audio or video.

    - json=false → binary stream (ExoPlayer / browser)
    - json=true  → meta + directUrl + size_mb
    - mp4=true or type=video → video/mp4 (else audio)
    """
    vid = _vid(id)
    if not _ok_vid(vid):
        raise HTTPException(400, "invalid video id")

    want_video = bool(mp4 or video or (type or "").lower() in ("video", "mp4"))
    resolved = await _resolve(vid, want_video=want_video)
    if not resolved.get("ok"):
        raise HTTPException(502, resolved.get("error") or "stream unavailable")

    url = resolved["url"]
    mime = resolved.get("mime") or ("video/mp4" if want_video else "audio/mp4")
    clen = resolved.get("content_length")
    ua = resolved.get("ua") or _UA_YT
    duration = resolved.get("duration")
    size_mb = round(int(clen) / (1024 * 1024), 2) if clen else None

    if json:
        return {
            "ok": True,
            "videoId": vid,
            "title": resolved.get("title"),
            "thumbnail": f"https://movieallshawon.vercel.app/thumbnailHD?id={vid}",
            "thumbnail_raw": resolved.get("thumb") or f"https://i.ytimg.com/vi/{vid}/hq720.jpg",
            "url": f"/stream?id={vid}&type={'video' if want_video else 'audio'}&mp4={'true' if want_video else 'false'}",
            "play_url": f"/stream?id={vid}&type={'video' if want_video else 'audio'}",
            "directUrl": url,  # CDN / googlevideo / loader
            "download_url": url,
            "mime": mime,
            "format": "mp4" if want_video else "mp3",
            "duration": duration,
            "contentLength": clen,
            "size_bytes": clen,
            "size_mb": size_mb,
            "provider": resolved.get("provider"),
            "seekable": True,
        }

    if redirect and "googlevideo.com" in url:
        return RedirectResponse(url=url, status_code=302)

    try:
        return await _proxy(url, mime, request, ua=ua, content_length=clen, duration=duration)
    except HTTPException as e:
        if e.status_code in (403, 401) and "googlevideo" in url:
            _CACHE.pop(f"{vid}:{'v' if want_video else 'a'}", None)
            api = _H()
            ld = await api._loader_to_youtube(vid, fmt="mp4" if want_video else "mp3")
            if ld.get("ok") and ld.get("url"):
                return await _proxy(
                    ld["url"],
                    "video/mp4" if want_video else "audio/mpeg",
                    request,
                    ua=_UA,
                    duration=duration,
                )
        raise


@router.get("/player")
async def player_json(id: str = Query(...)):
    vid = _vid(id)
    api = _H()
    data = await api._innertube_player(vid, prefer="auto")
    if not data.get("ok"):
        return {
            "ok": False,
            "videoId": vid,
            "error": data.get("error"),
            "play_url": f"/stream?id={vid}&type=audio",
        }
    streams = []
    for a in data.get("audio_streams") or []:
        streams.append({
            "url": a.get("url"),
            "itag": a.get("itag"),
            "mimeType": a.get("mime") or a.get("mimeType"),
            "bitrate": a.get("bitrate"),
            "contentLength": a.get("contentLength"),
            "kind": "audio",
        })
    for v in data.get("video_streams") or []:
        streams.append({
            "url": v.get("url"),
            "itag": v.get("itag"),
            "mimeType": v.get("mime") or v.get("mimeType"),
            "height": v.get("height"),
            "kind": "video",
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
        "video_url": f"/stream?id={vid}&type=video&mp4=true",
    }


# ─── THUMBNAIL (HD + crop bars) ────────────────────────────────────────────

@router.get("/thumbnailHD")
@router.get("/thumbnail")
async def thumbnail_hd(id: str = Query(...), crop: bool = Query(True), proxy: bool = Query(True)):
    vid = _vid(id)
    if not _ok_vid(vid):
        raise HTTPException(400, "invalid video id")

    async with httpx.AsyncClient(timeout=10.0, follow_redirects=True) as client:
        img = None
        for url in _thumb_candidates(vid):
            try:
                r = await client.get(url, headers={"User-Agent": _UA})
                if r.status_code == 200 and len(r.content) > 2000:
                    # maxres sometimes is placeholder small
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
        if not proxy:
            # still return body (already fetched)
            pass
        return Response(
            content=img,
            media_type="image/jpeg",
            headers={
                "Cache-Control": "public, max-age=86400",
                "Content-Length": str(len(img)),
                "Access-Control-Allow-Origin": "*",
            },
        )


# ─── SEARCH / HOME / NEXT ──────────────────────────────────────────────────

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
    api = _H()
    query = (q or title or "").strip()
    vid = _vid(id) if id else ""
    if vid and not query:
        try:
            pl = await asyncio.wait_for(api._innertube_player(vid), timeout=3.0)
            if pl.get("ok"):
                query = pl.get("title") or vid
        except Exception:
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
    }


@router.get("/fm")
async def fm_index():
    return {
        "ok": True,
        "name": "FastMusic",
        "stream": {
            "audio": "GET /stream?id=VIDEO_ID&type=audio",
            "video_mp4": "GET /stream?id=VIDEO_ID&mp4=true",
            "json": "GET /stream?id=VIDEO_ID&json=true  → directUrl + size_mb",
            "seek": "Range bytes → 206",
        },
        "catalog": ["search", "home", "next", "album", "artist", "lyrics", "thumbnailHD", "info", "player"],
    }
