# FastMusic — ExoPlayer-safe, no ad redirects
# Deploy next to api.py
from __future__ import annotations

import asyncio
import re
import time
from collections import OrderedDict
from typing import Optional, Dict, Any, Tuple

import httpx
from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import RedirectResponse, Response, StreamingResponse

router = APIRouter(tags=["FastMusic"])

_STREAM_CACHE: Dict[str, Any] = {}
_STREAM_TTL = 900
# small in-memory audio body cache for seekable responses (max ~40MB)
_AUDIO_BODY: "OrderedDict[str, Tuple[bytes, str, float]]" = OrderedDict()
_AUDIO_BODY_MAX = 8
_AUDIO_BODY_TTL = 600

_UA_YT = "com.google.android.youtube/19.29.37 (Linux; U; Android 13) gzip"
_UA_WEB = (
    "Mozilla/5.0 (Linux; Android 13; Pixel 7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Mobile Safari/537.36"
)


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


def _is_ad_host(url: str) -> bool:
    u = (url or "").lower()
    bad = ("doubleclick", "googlesyndication", "adnxs", "popads", "propeller", "adsterra", "onclick")
    return any(b in u for b in bad)


def _is_savenow(url: str) -> bool:
    u = (url or "").lower()
    return any(x in u for x in ("savenow.to", "loader.to", "oceansaver", "affadaffa"))


def _pick_audio(data: dict) -> Tuple[Optional[str], str]:
    streams = list(data.get("audio_streams") or [])
    def score(a):
        if not a.get("url"):
            return -1
        itag = str(a.get("itag") or "")
        mime = (a.get("mime") or a.get("mimeType") or "").lower()
        s = 0
        if itag == "140":
            s += 200
        elif itag == "139":
            s += 150
        elif "mp4a" in mime or ("mp4" in mime and "webm" not in mime):
            s += 100
        elif "webm" in mime or "opus" in mime:
            s += 15
        s += min(int(a.get("bitrate") or 0) // 1000, 40)
        return s
    for a in sorted(streams, key=score, reverse=True):
        url = a.get("url")
        if not url:
            continue
        mime = (a.get("mime") or a.get("mimeType") or "audio/mp4").lower()
        ct = "audio/webm" if ("webm" in mime or "opus" in mime) else "audio/mp4"
        return url, ct
    url = data.get("audio_url")
    if url:
        return url, ("audio/webm" if "webm" in url else "audio/mp4")
    return None, "audio/mp4"


async def _loader_direct(vid: str) -> dict:
    """loader.to → real audio bytes host. Never return ad landing pages."""
    api = _H()
    # try mp3 then m4a
    for fmt in ("mp3", "m4a"):
        try:
            ld = await asyncio.wait_for(api._loader_to_youtube(vid, fmt=fmt), timeout=22.0)
        except Exception as e:
            ld = {"ok": False, "error": str(e)[:80]}
        if not ld.get("ok") or not ld.get("url"):
            continue
        url = ld["url"]
        if _is_ad_host(url):
            continue
        # probe: must be audio, not HTML
        try:
            async with httpx.AsyncClient(timeout=20.0, follow_redirects=True) as client:
                h = {
                    "User-Agent": _UA_WEB,
                    "Referer": "https://loader.to/",
                    "Accept": "*/*",
                }
                r = await client.get(url, headers=h)
                ct = (r.headers.get("content-type") or "").lower()
                body = r.content
                if r.status_code >= 400:
                    continue
                if "text/html" in ct or body[:15].lower().startswith(b"<!doctype") or body[:6].lower().startswith(b"<html"):
                    continue  # ad page
                if not (body[:3] == b"ID3" or body[4:8] == b"ftyp" or b"ftyp" in body[:64] or ct.startswith("audio/") or "mpeg" in ct or "mp4" in ct or "octet-stream" in ct):
                    # still might be audio without sniff — if savenow host accept
                    if not _is_savenow(str(r.url)):
                        continue
                # normalize mime
                if body[:3] == b"ID3" or "mpeg" in ct or "mp3" in ct:
                    mime = "audio/mpeg"
                elif b"ftyp" in body[:64] or "mp4" in ct:
                    mime = "audio/mp4"
                else:
                    mime = "audio/mpeg"
                return {
                    "ok": True,
                    "url": str(r.url),
                    "title": ld.get("title") or vid,
                    "thumb": ld.get("thumb") or _thumb(vid),
                    "mime": mime,
                    "provider": "loader-verified",
                    "body": body,  # already downloaded — use for seekable response
                    "duration": None,
                }
        except Exception:
            continue
    return {"ok": False, "error": "loader no clean audio"}


async def _resolve_fast(vid: str, type_: str = "audio") -> dict:
    now = time.time()
    hit = _STREAM_CACHE.get(vid)
    if hit and now - hit["ts"] < _STREAM_TTL and (hit.get("data") or {}).get("ok"):
        return hit["data"]

    # body cache
    if vid in _AUDIO_BODY:
        body, mime, ts = _AUDIO_BODY[vid]
        if now - ts < _AUDIO_BODY_TTL:
            return {
                "ok": True,
                "url": f"body://{vid}",
                "title": vid,
                "thumb": _thumb(vid),
                "mime": mime,
                "provider": "memory-cache",
                "body": body,
                "duration": None,
            }

    api = _H()
    want_audio = (type_ or "audio").lower() in ("audio", "mp3", "m4a", "bestaudio", "best")

    async def do_innertube():
        try:
            return await api._innertube_player(vid, prefer="auto")
        except Exception as e:
            return {"ok": False, "error": str(e)[:120]}

    it_task = asyncio.create_task(do_innertube())
    ld_task = asyncio.create_task(_loader_direct(vid))

    stream_url = None
    mime = "audio/mp4"
    title = vid
    thumb = _thumb(vid)
    duration = None
    provider = None
    body = None
    err = None
    data = None

    # prefer innertube within 3.5s
    try:
        done, _ = await asyncio.wait({it_task}, timeout=3.5)
        if it_task.done():
            data = it_task.result()
        if data and data.get("ok"):
            title = data.get("title") or title
            thumb = data.get("thumb") or thumb
            duration = data.get("duration")
            provider = data.get("provider") or "innertube"
            if want_audio:
                stream_url, mime = _pick_audio(data)
            else:
                stream_url = data.get("video_url") or data.get("audio_url")
                mime = "video/mp4"
            if stream_url and not _is_ad_host(stream_url):
                ld_task.cancel()
            else:
                stream_url = None
        else:
            err = (data or {}).get("error")
    except Exception as e:
        err = str(e)[:80]

    if not stream_url:
        try:
            ld = await ld_task
            if ld.get("ok"):
                stream_url = ld.get("url")
                title = ld.get("title") or title
                thumb = ld.get("thumb") or thumb
                mime = ld.get("mime") or "audio/mpeg"
                provider = ld.get("provider") or "loader"
                body = ld.get("body")
                duration = ld.get("duration") or duration
            else:
                err = ld.get("error") or err
        except Exception as e:
            err = str(e)[:80]
    elif not ld_task.done():
        ld_task.cancel()

    if not stream_url and not body:
        out = {"ok": False, "error": err or "no stream"}
        _STREAM_CACHE[vid] = {"ts": now, "data": out}
        return out

    if body is not None:
        # store seekable body
        _AUDIO_BODY[vid] = (body, mime, now)
        while len(_AUDIO_BODY) > _AUDIO_BODY_MAX:
            _AUDIO_BODY.popitem(last=False)

    out = {
        "ok": True,
        "url": stream_url or f"body://{vid}",
        "title": title,
        "thumb": thumb,
        "mime": mime,
        "duration": duration,
        "provider": provider,
        "body": body,
    }
    # don't put raw body into long stream cache (memory) — strip for meta cache
    meta = {k: v for k, v in out.items() if k != "body"}
    if body is not None:
        meta["has_body"] = True
    _STREAM_CACHE[vid] = {"ts": now, "data": meta}
    try:
        if data and data.get("ok"):
            api._innertube_cache[vid] = {"ts": now, "data": data}
    except Exception:
        pass
    return out


async def _fetch_audio_bytes(url: str) -> Tuple[bytes, str]:
    headers = {
        "User-Agent": _UA_YT if "googlevideo.com" in url else _UA_WEB,
        "Accept": "*/*",
        "Accept-Encoding": "identity",
    }
    if "googlevideo.com" in url:
        headers["Referer"] = "https://www.youtube.com/"
    else:
        headers["Referer"] = "https://loader.to/"
    async with httpx.AsyncClient(timeout=httpx.Timeout(30.0, read=120.0), follow_redirects=True) as client:
        r = await client.get(url, headers=headers)
        if r.status_code >= 400:
            raise HTTPException(r.status_code, f"upstream {r.status_code}")
        body = r.content
        if body[:15].lower().startswith(b"<!doctype") or body[:6].lower().startswith(b"<html"):
            raise HTTPException(502, "upstream returned HTML (ad page)")
        ct = (r.headers.get("content-type") or "").lower()
        if body[:3] == b"ID3":
            mime = "audio/mpeg"
        elif b"ftyp" in body[:64]:
            mime = "audio/mp4"
        elif "webm" in ct:
            mime = "audio/webm"
        elif "mp4" in ct:
            mime = "audio/mp4"
        else:
            mime = "audio/mpeg"
        return body, mime


def _serve_bytes(body: bytes, mime: str, request: Request, duration=None):
    """Full body with Content-Length — ExoPlayer seek works."""
    total = len(body)
    range_h = request.headers.get("range") or request.headers.get("Range")
    headers = {
        "Content-Type": mime,
        "Accept-Ranges": "bytes",
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Expose-Headers": "Content-Length, Content-Range, Accept-Ranges, Content-Type",
        "Cache-Control": "public, max-age=300",
    }
    if duration:
        try:
            headers["X-Duration-Seconds"] = str(int(duration))
        except Exception:
            pass
    if range_h and range_h.startswith("bytes="):
        try:
            part = range_h.replace("bytes=", "").split(",")[0]
            start_s, end_s = (part.split("-") + [""])[:2]
            start = int(start_s) if start_s else 0
            end = int(end_s) if end_s else total - 1
            end = min(end, total - 1)
            start = max(0, start)
            chunk = body[start : end + 1]
            headers["Content-Length"] = str(len(chunk))
            headers["Content-Range"] = f"bytes {start}-{end}/{total}"
            return Response(content=chunk, status_code=206, media_type=mime, headers=headers)
        except Exception:
            pass
    headers["Content-Length"] = str(total)
    return Response(content=body, status_code=200, media_type=mime, headers=headers)


async def _proxy_range_stream(url: str, mime: str, request: Request, duration=None):
    """Proxy with Range for googlevideo (already has Content-Length)."""
    headers = {
        "User-Agent": _UA_YT if "googlevideo.com" in url else _UA_WEB,
        "Accept": "*/*",
        "Accept-Encoding": "identity",
    }
    if "googlevideo.com" in url:
        headers["Referer"] = "https://www.youtube.com/"
        headers["Origin"] = "https://www.youtube.com"
    else:
        headers["Referer"] = "https://loader.to/"
    range_h = request.headers.get("range") or request.headers.get("Range")
    if range_h:
        headers["Range"] = range_h

    client = httpx.AsyncClient(timeout=httpx.Timeout(20.0, read=120.0), follow_redirects=True)
    try:
        upstream = await client.send(client.build_request("GET", url, headers=headers), stream=True)
    except Exception as e:
        await client.aclose()
        raise HTTPException(502, f"upstream: {e}")

    if upstream.status_code >= 400:
        await upstream.aclose()
        await client.aclose()
        raise HTTPException(upstream.status_code, f"upstream {upstream.status_code}")

    ct = (upstream.headers.get("content-type") or mime or "audio/mp4").split(";")[0].strip()
    if "webm" in ct:
        ct = "audio/webm"
    elif "mpeg" in ct or "mp3" in ct:
        ct = "audio/mpeg"
    elif "mp4" in ct or "m4a" in ct or "aac" in ct:
        ct = "audio/mp4"
    else:
        ct = mime or "audio/mp4"

    out_h = {
        "Content-Type": ct,
        "Accept-Ranges": "bytes",
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Expose-Headers": "Content-Length, Content-Range, Accept-Ranges, Content-Type",
        "Cache-Control": "no-store",
    }
    if upstream.headers.get("content-length"):
        out_h["Content-Length"] = upstream.headers["content-length"]
    if upstream.headers.get("content-range"):
        out_h["Content-Range"] = upstream.headers["content-range"]
    if duration:
        try:
            out_h["X-Duration-Seconds"] = str(int(duration))
        except Exception:
            pass

    async def body():
        try:
            async for chunk in upstream.aiter_bytes(64 * 1024):
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
    """
    App: /download?id=VIDEO_ID&type=audio&quality=best

    - Never exposes savenow/ad URLs to the client
    - Returns real audio bytes with Content-Length (seekbar works)
    - Prefer googlevideo; fallback loader verified audio
    """
    vid = (id or "").replace("yt:", "").strip()
    if not re.match(r"^[\w-]{6,20}$", vid):
        raise HTTPException(400, "invalid video id")

    resolved = await _resolve_fast(vid, type_=type)
    if not resolved.get("ok"):
        raise HTTPException(502, resolved.get("error") or "stream unavailable")

    url = resolved.get("url") or ""
    mime = resolved.get("mime") or "audio/mp4"
    duration = resolved.get("duration")
    body = resolved.get("body")

    # restore body from memory cache
    if body is None and vid in _AUDIO_BODY:
        body, mime, _ = _AUDIO_BODY[vid]

    if json:
        return {
            "ok": True,
            "videoId": vid,
            "title": resolved.get("title"),
            "thumbnail": resolved.get("thumb"),
            "url": f"/download?id={vid}&type=audio&quality=best",
            "directUrl": None if (_is_savenow(url) or url.startswith("body://")) else url,
            "mime": mime,
            "duration": duration,
            "provider": resolved.get("provider"),
            "seekable": True,
            "note": "Use /download as MediaSource URI (proxied, no ads)",
        }

    # NEVER 302 to savenow (ads). Only optional redirect for clean googlevideo.
    if redirect and url.startswith("http") and "googlevideo.com" in url and not _is_savenow(url):
        return RedirectResponse(url=url, status_code=302)

    # Prefer full body response (seek + duration)
    if body is None and (_is_savenow(url) or url.startswith("body://") or not url.startswith("http")):
        if url.startswith("http"):
            body, mime = await _fetch_audio_bytes(url)
            _AUDIO_BODY[vid] = (body, mime, time.time())
            while len(_AUDIO_BODY) > _AUDIO_BODY_MAX:
                _AUDIO_BODY.popitem(last=False)

    if body is not None:
        return _serve_bytes(body, mime, request, duration=duration)

    # googlevideo: stream proxy with Range
    if url.startswith("http"):
        try:
            # If no content-length expected issues on some hosts — buffer small files
            if _is_savenow(url):
                body, mime = await _fetch_audio_bytes(url)
                _AUDIO_BODY[vid] = (body, mime, time.time())
                return _serve_bytes(body, mime, request, duration=duration)
            return await _proxy_range_stream(url, mime, request, duration=duration)
        except HTTPException:
            # last chance: buffer
            try:
                body, mime = await _fetch_audio_bytes(url)
                return _serve_bytes(body, mime, request, duration=duration)
            except Exception as e:
                raise HTTPException(502, str(e)[:120])

    raise HTTPException(502, "no playable audio")


@router.get("/thumbnailHD")
@router.get("/thumbnail")
@router.get("/fm/thumbnailHD")
async def thumbnail_hd(id: str = Query(...), proxy: bool = Query(True)):
    vid = (id or "").replace("yt:", "").strip()
    if not re.match(r"^[\w-]{6,20}$", vid):
        raise HTTPException(400, "invalid video id")
    candidates = [
        f"https://i.ytimg.com/vi/{vid}/hqdefault.jpg",
        f"https://i.ytimg.com/vi/{vid}/sddefault.jpg",
        f"https://i.ytimg.com/vi/{vid}/mqdefault.jpg",
    ]
    if not proxy:
        return RedirectResponse(url=candidates[0], status_code=302)
    async with httpx.AsyncClient(timeout=8.0, follow_redirects=True) as client:
        for u in candidates:
            try:
                r = await client.get(u, headers={"User-Agent": _UA_WEB})
                if r.status_code == 200 and len(r.content) > 2000:
                    return Response(
                        content=r.content,
                        media_type="image/jpeg",
                        headers={
                            "Cache-Control": "public, max-age=86400",
                            "Access-Control-Allow-Origin": "*",
                            "Content-Length": str(len(r.content)),
                        },
                    )
            except Exception:
                continue
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
                for s in (sec.get("items") or []) if s.get("video_id")
            ]
            if items:
                sections.append({"title": sec.get("title") or "Home", "contents": items, "items": items})
    except Exception:
        pass
    if not sections:
        for title, q in (("You might also like", "Top songs this week"), ("Trending", "Trending music")):
            try:
                songs = await api._ytm_search_songs(q, limit=12)
                items = [_item(s["video_id"], s.get("title") or "", s.get("artist") or "", s.get("thumb")) for s in songs]
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
    hit = _STREAM_CACHE.get(vid)
    if hit and (hit.get("data") or {}).get("title"):
        title = hit["data"].get("title") or title
    else:
        try:
            pl = await api._innertube_player(vid)
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
        "ok": True, "videoId": vid,
        "title": best.get("trackName") or title,
        "author": best.get("artistName") or "",
        "lyrics": plain, "text": plain, "result": plain,
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
        "success": True, "ok": True,
        "message": {"lyrics": plain, "text": plain, "title": best.get("trackName"), "artist": best.get("artistName")},
        "lyrics": plain, "text": plain,
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
    resolved = await _resolve_fast(vid)
    return {
        "ok": True, "videoId": vid,
        "title": resolved.get("title") or vid,
        "thumbnail": resolved.get("thumb") or _thumb(vid),
        "duration": resolved.get("duration"),
        "url": f"/download?id={vid}&type=audio&quality=best",
        "provider": resolved.get("provider"),
    }
