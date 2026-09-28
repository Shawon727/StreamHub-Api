# FastMusic — SimpMusic-style fast stream (InnerTube ANDROID + Range proxy)
from __future__ import annotations

import asyncio
import re
import time
from typing import Optional, Dict, Any, Tuple

import httpx
from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import RedirectResponse, Response, StreamingResponse

router = APIRouter(tags=["FastMusic"])

_CACHE: Dict[str, Any] = {}
_TTL = 600
_UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/121.0.0.0 Safari/537.36"
_UA_YT = "com.google.android.youtube/20.10.38 (Linux; U; Android 14) gzip"


def _H():
    import api as _api
    return _api


def _thumb(vid: str) -> str:
    return f"https://i.ytimg.com/vi/{vid}/hqdefault.jpg"


def _item(video_id: str, title: str = "", author: str = "", thumb: str = None, duration=None, **extra) -> dict:
    vid = (video_id or "").replace("yt:", "").strip()
    t1 = thumb or (_thumb(vid) if vid else None)
    out = {
        "videoId": vid,
        "title": title or vid,
        "author": author or "Unknown",
        "thumbnail": t1,
        "duration": duration,
        "subtitle": extra.get("subtitle") or author,
        "text": title or "",
    }
    return {k: v for k, v in out.items() if v is not None}


def _best_audio(data: dict) -> Tuple[Optional[str], str, Optional[str], Optional[str]]:
    """Return url, mime, content_length, client_ua."""
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

    streams = sorted(streams, key=score, reverse=True)
    for a in streams:
        if a.get("url"):
            mime = (a.get("mime") or "").lower()
            ct = "audio/mp4" if ("mp4" in mime or "mp4a" in mime) else "audio/webm"
            return a["url"], ct, a.get("contentLength") or data.get("content_length"), ua
    if data.get("audio_url"):
        u = data["audio_url"]
        ct = "audio/webm" if "webm" in u else "audio/mp4"
        return u, ct, data.get("content_length"), ua
    return None, "audio/mp4", None, ua


async def _resolve(vid: str) -> dict:
    now = time.time()
    hit = _CACHE.get(vid)
    if hit and now - hit.get("ts", 0) < _TTL and hit.get("url"):
        return {"ok": True, **hit}

    api = _H()
    title, thumb, duration = vid, _thumb(vid), None
    url = mime = clen = ua = provider = None

    # 1) InnerTube — fast (SimpMusic path)
    try:
        data = await asyncio.wait_for(api._innertube_player(vid, prefer="auto"), timeout=5.0)
        if data.get("ok"):
            title = data.get("title") or title
            thumb = data.get("thumb") or thumb
            duration = data.get("duration")
            url, mime, clen, ua = _best_audio(data)
            provider = data.get("provider") or "innertube"
    except Exception:
        pass

    # 2) loader only if needed (slow)
    if not url:
        try:
            ld = await api._loader_to_youtube(vid, fmt="mp3")
            if ld.get("ok") and ld.get("url"):
                url = ld["url"]
                title = ld.get("title") or title
                thumb = ld.get("thumb") or thumb
                mime = "audio/mpeg"
                provider = "loader.to"
                ua = _UA
                clen = None
            else:
                return {"ok": False, "error": (ld or {}).get("error") or "no stream"}
        except Exception as e:
            return {"ok": False, "error": str(e)[:120]}

    out = {
        "ok": True,
        "url": url,
        "mime": mime or "audio/mp4",
        "title": title,
        "thumb": thumb,
        "duration": duration,
        "provider": provider,
        "content_length": clen,
        "ua": ua or _UA_YT,
        "ts": now,
    }
    _CACHE[vid] = out
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

    low = first[:120].lower()
    if low.startswith(b"<!doctype") or low.startswith(b"<html"):
        await upstream.aclose()
        await client.aclose()
        raise HTTPException(502, "html/ad blocked")

    ct = (upstream.headers.get("content-type") or mime or "audio/mp4").split(";")[0].strip().lower()
    if first[:3] == b"ID3":
        ct = "audio/mpeg"
    elif b"ftyp" in first[:32]:
        ct = "audio/mp4"
    elif "webm" in ct:
        ct = "audio/webm"
    elif "mpeg" in ct or "mp3" in ct:
        ct = "audio/mpeg"
    elif "mp4" in ct or "m4a" in ct:
        ct = "audio/mp4"

    out_h = {
        "Content-Type": ct,
        "Accept-Ranges": "bytes",
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Expose-Headers": "Content-Length, Content-Range, Accept-Ranges, Content-Type",
        "Cache-Control": "public, max-age=120",
    }
    # Content-Length critical for ExoPlayer duration/seek
    if upstream.headers.get("content-length"):
        out_h["Content-Length"] = upstream.headers["content-length"]
    elif content_length and not range_h:
        out_h["Content-Length"] = str(content_length)
    if upstream.headers.get("content-range"):
        out_h["Content-Range"] = upstream.headers["content-range"]
    elif range_h and content_length:
        try:
            total = int(content_length)
            part = range_h.replace("bytes=", "").split(",")[0]
            a, b = (part.split("-") + [""])[:2]
            start = int(a) if a else 0
            end = int(b) if b else total - 1
            out_h["Content-Range"] = f"bytes {start}-{end}/{total}"
        except Exception:
            pass
    if duration:
        try:
            out_h["X-Duration-Seconds"] = str(int(duration))
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


@router.get("/download")
@router.get("/fm/download")
@router.get("/api/download")
async def download(
    request: Request,
    id: str = Query(...),
    type: str = Query("audio"),
    quality: str = Query("best"),
    json: bool = Query(False),
    redirect: bool = Query(False),
):
    """SimpMusic-style: resolve InnerTube fast, proxy with Range for seek."""
    vid = (id or "").replace("yt:", "").strip()
    if not re.match(r"^[\w-]{6,20}$", vid):
        raise HTTPException(400, "invalid video id")

    resolved = await _resolve(vid)
    if not resolved.get("ok"):
        raise HTTPException(502, resolved.get("error") or "stream unavailable")

    url = resolved["url"]
    mime = resolved.get("mime") or "audio/mp4"
    clen = resolved.get("content_length")
    ua = resolved.get("ua") or _UA_YT
    duration = resolved.get("duration")

    if json:
        return {
            "ok": True,
            "videoId": vid,
            "title": resolved.get("title"),
            "thumbnail": resolved.get("thumb"),
            "url": f"/download?id={vid}&type=audio&quality=best",
            "mime": mime,
            "duration": duration,
            "contentLength": clen,
            "provider": resolved.get("provider"),
            "seekable": True,
        }

    if redirect and "googlevideo.com" in url:
        return RedirectResponse(url=url, status_code=302)

    try:
        return await _proxy(url, mime, request, ua=ua, content_length=clen, duration=duration)
    except HTTPException as e:
        if e.status_code in (403, 401) and "googlevideo" in url:
            # invalidate + loader
            _CACHE.pop(vid, None)
            api = _H()
            ld = await api._loader_to_youtube(vid, fmt="mp3")
            if ld.get("ok") and ld.get("url"):
                _CACHE[vid] = {
                    "ok": True,
                    "url": ld["url"],
                    "mime": "audio/mpeg",
                    "title": ld.get("title") or resolved.get("title"),
                    "thumb": ld.get("thumb") or resolved.get("thumb"),
                    "duration": duration,
                    "provider": "loader.to",
                    "content_length": None,
                    "ua": _UA,
                    "ts": time.time(),
                }
                return await _proxy(ld["url"], "audio/mpeg", request, ua=_UA, duration=duration)
        raise


@router.get("/thumbnailHD")
@router.get("/thumbnail")
@router.get("/fm/thumbnailHD")
async def thumbnail_hd(id: str = Query(...), proxy: bool = Query(True)):
    vid = (id or "").replace("yt:", "").strip()
    if not re.match(r"^[\w-]{6,20}$", vid):
        raise HTTPException(400, "invalid video id")
    url = _thumb(vid)
    if not proxy:
        return RedirectResponse(url=url, status_code=302)
    async with httpx.AsyncClient(timeout=8.0, follow_redirects=True) as client:
        r = await client.get(url, headers={"User-Agent": _UA})
        if r.status_code == 200 and len(r.content) > 1000:
            return Response(
                content=r.content,
                media_type="image/jpeg",
                headers={"Cache-Control": "public, max-age=86400", "Content-Length": str(len(r.content))},
            )
    raise HTTPException(404, "thumbnail not found")


@router.get("/search")
@router.get("/fm/search")
async def search(q: str = Query(..., min_length=1), type: str = Query("song"), limit: int = Query(20, ge=1, le=40)):
    api = _H()
    songs = await api._ytm_search_songs(q, limit=limit)
    items = [
        _item(s["video_id"], s.get("title") or "", s.get("artist") or "", s.get("thumb"), s.get("duration"))
        for s in songs
    ]
    return {"result": items, "results": items, "ok": True, "query": q, "type": type}


@router.get("/v2/home")
@router.get("/fm/home")
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
                sections.append({"title": sec.get("title") or "Home", "contents": items, "items": items})
    except Exception:
        pass
    if not sections:
        for title, q in (("You might also like", "Top songs this week"), ("Trending", "Trending music")):
            try:
                songs = await api._ytm_search_songs(q, limit=12)
                items = [
                    _item(s["video_id"], s.get("title") or "", s.get("artist") or "", s.get("thumb")) for s in songs
                ]
                if items:
                    sections.append({"title": title, "contents": items, "items": items})
            except Exception:
                continue
    flat = [i for s in sections for i in (s.get("items") or [])]
    return {"result": sections, "results": sections, "items": flat, "ok": bool(sections)}


@router.get("/next")
@router.get("/fm/next")
async def next_songs(id: str = Query(...)):
    api = _H()
    vid = id.replace("yt:", "").strip()
    songs = await api._ytm_next_songs(vid, limit=25)
    items = [_item(s["video_id"], s.get("title") or "", s.get("artist") or "", s.get("thumb")) for s in songs]
    return {"result": items, "results": items, "items": items, "ok": bool(items), "videoId": vid}


@router.get("/music/lyrics/plain")
@router.get("/fm/lyrics")
async def lyrics_plain(id: str = Query(...)):
    api = _H()
    vid = id.replace("yt:", "").strip()
    title = vid
    try:
        pl = await asyncio.wait_for(api._innertube_player(vid), timeout=3.0)
        if pl.get("ok"):
            title = pl.get("title") or title
    except Exception:
        pass
    async with httpx.AsyncClient(timeout=12.0) as client:
        r = await client.get("https://lrclib.net/api/search", params={"q": title})
        arr = r.json() if r.status_code == 200 else []
    if not arr:
        return {"ok": False, "lyrics": None, "text": None, "result": None, "videoId": vid}
    best = arr[0]
    plain = best.get("plainLyrics") or best.get("syncedLyrics") or ""
    return {
        "ok": True,
        "videoId": vid,
        "title": best.get("trackName") or title,
        "author": best.get("artistName") or "",
        "lyrics": plain,
        "text": plain,
        "result": plain,
        "synced": best.get("syncedLyrics"),
    }


@router.get("/songs/lyrics")
async def songs_lyrics(
    id: Optional[str] = Query(None),
    q: Optional[str] = Query(None),
    title: Optional[str] = Query(None),
    artist: Optional[str] = Query(""),
    type: Optional[str] = Query(None),
):
    if id and not (q or title):
        return await lyrics_plain(id=id)
    query = (q or title or "").strip()
    if not query:
        raise HTTPException(400, "q, title, or id required")
    async with httpx.AsyncClient(timeout=12.0) as client:
        r = await client.get("https://lrclib.net/api/search", params={"q": f"{query} {artist or ''}".strip()})
        arr = r.json() if r.status_code == 200 else []
    if not arr:
        return {"success": False, "message": {"lyrics": None, "text": None}}
    best = arr[0]
    plain = best.get("plainLyrics") or best.get("syncedLyrics") or ""
    return {
        "success": True,
        "ok": True,
        "message": {
            "lyrics": plain,
            "text": plain,
            "title": best.get("trackName"),
            "artist": best.get("artistName"),
        },
        "lyrics": plain,
        "text": plain,
    }


@router.get("/getAlbum")
@router.get("/fm/album")
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
        return {"result": songs, "results": songs, "items": songs, "ok": bool(songs)}
    except Exception as e:
        return {"ok": False, "result": [], "error": str(e)[:100]}


@router.get("/getArtists")
@router.get("/getArtist")
@router.get("/fm/artist")
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
        return {"result": songs, "results": songs, "items": songs[:40], "ok": bool(songs)}
    except Exception as e:
        return {"ok": False, "result": [], "error": str(e)[:100]}


@router.get("/fm/info")
async def info(id: str = Query(...)):
    vid = id.replace("yt:", "").strip()
    return {"ok": True, "videoId": vid, "thumbnail": _thumb(vid), "url": f"/download?id={vid}&type=audio&quality=best"}
