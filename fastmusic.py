# FastMusic — full YouTube Music API (SimpMusic / vivi-music parity)
# Deploy next to api.py only. Edit this file for music APIs.
# Uses api.py helpers: _innertube_player, _loader_to_youtube, _ytm_*
from __future__ import annotations

import asyncio
import re
import time
from typing import Optional, Dict, Any, List, Tuple

import httpx
from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import RedirectResponse, Response, StreamingResponse

router = APIRouter(tags=["FastMusic"])

_CACHE: Dict[str, Any] = {}
_TTL = 600
_UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/121.0.0.0 Safari/537.36"
_UA_YT = "com.google.android.youtube/20.10.38 (Linux; U; Android 14) gzip"

# ─── helpers ───────────────────────────────────────────────────────────────

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
        "video_id": vid,
        "id": vid,
        "title": title or vid,
        "author": author or "Unknown",
        "artist": author or "Unknown",
        "thumbnail": t1,
        "thumb": t1,
        "duration": duration,
        "subtitle": extra.get("subtitle") or author,
        "text": title or "",
        "url": f"/download?id={vid}&type=audio&quality=best" if vid else None,
        "stream": f"/download?id={vid}&type=audio&quality=best" if vid else None,
        "browseId": extra.get("browseId"),
        "playlistId": extra.get("playlistId"),
        "type": extra.get("type") or "song",
    }
    return {k: v for k, v in out.items() if v is not None}


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


async def _resolve(vid: str) -> dict:
    now = time.time()
    hit = _CACHE.get(vid)
    if hit and now - hit.get("ts", 0) < _TTL and hit.get("url"):
        return {"ok": True, **hit}

    api = _H()
    title, thumb, duration = vid, _thumb(vid), None
    url = mime = clen = ua = provider = None

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

    if not url:
        try:
            ld = await api._loader_to_youtube(vid, fmt="mp3")
            if ld.get("ok") and ld.get("url"):
                url = ld["url"]
                title = ld.get("title") or title
                thumb = ld.get("thumb") or thumb
                mime, provider, ua = "audio/mpeg", "loader.to", _UA
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

    if first[:120].lower().startswith(b"<!doctype") or first[:6].lower().startswith(b"<html"):
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
    elif "mp4" in ct:
        ct = "audio/mp4"

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

    async def body():
        try:
            yield first
            async for chunk in aiter:
                yield chunk
        finally:
            await upstream.aclose()
            await client.aclose()

    return StreamingResponse(body(), status_code=upstream.status_code, media_type=ct, headers=out_h)


async def _browse_sections(browse_id: str, params: str = None) -> List[dict]:
    """Generic YTM browse → sections of songs."""
    api = _H()
    payload = {"browseId": browse_id}
    if params:
        payload["params"] = params
    try:
        data = await api._ytm_post("browse", payload)
    except Exception as e:
        return []
    sections = []
    # reuse home parser style
    try:
        home = await api._ytm_home_sections() if browse_id == "FEmusic_home" else None
        if home:
            return home
    except Exception:
        pass
    raw = []
    api._ytm_walk(data, "musicResponsiveListItemRenderer", raw)
    songs, seen = [], set()
    for it in raw:
        e = api._ytm_parse_item(it)
        if e and e.get("video_id") and e["video_id"] not in seen:
            seen.add(e["video_id"])
            songs.append(e)
    if songs:
        sections.append({"title": browse_id, "items": songs})
    # also shelf renderers
    shelves = []
    api._ytm_walk(data, "musicCarouselShelfRenderer", shelves)
    for sh in shelves:
        title = ""
        try:
            title = api._ytm_text((sh.get("header") or {}).get("musicCarouselShelfBasicHeaderRenderer", {}).get("title"))
        except Exception:
            pass
        items_raw = sh.get("contents") or []
        items = []
        for it in items_raw:
            rend = it.get("musicResponsiveListItemRenderer") or it.get("musicTwoRowItemRenderer") or it
            e = api._ytm_parse_item(rend) if isinstance(rend, dict) else None
            if e and e.get("video_id"):
                items.append(e)
        if items:
            sections.append({"title": title or "Shelf", "items": items})
    return sections or ([{"title": "Results", "items": songs}] if songs else [])


def _sections_to_items(sections: List[dict]) -> List[dict]:
    out = []
    for sec in sections:
        for s in sec.get("items") or []:
            out.append(
                _item(
                    s.get("video_id") or s.get("videoId") or "",
                    s.get("title") or "",
                    s.get("artist") or s.get("author") or "",
                    s.get("thumb") or s.get("thumbnail"),
                    s.get("duration"),
                )
            )
    return out


# ─── STREAM (SimpMusic player) ─────────────────────────────────────────────

@router.get("/download")
@router.get("/fm/download")
@router.get("/api/download")
@router.get("/stream")
@router.get("/fm/stream")
async def download(
    request: Request,
    id: str = Query(...),
    type: str = Query("audio"),
    quality: str = Query("best"),
    json: bool = Query(False),
    redirect: bool = Query(False),
):
    vid = (id or "").replace("yt:", "").strip()
    if not re.match(r"^[\w-]{6,20}$", vid):
        raise HTTPException(400, "invalid video id")
    resolved = await _resolve(vid)
    if not resolved.get("ok"):
        raise HTTPException(502, resolved.get("error") or "stream unavailable")
    url, mime = resolved["url"], resolved.get("mime") or "audio/mp4"
    clen, ua, duration = resolved.get("content_length"), resolved.get("ua") or _UA_YT, resolved.get("duration")
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
            _CACHE.pop(vid, None)
            api = _H()
            ld = await api._loader_to_youtube(vid, fmt="mp3")
            if ld.get("ok") and ld.get("url"):
                _CACHE[vid] = {
                    "ok": True, "url": ld["url"], "mime": "audio/mpeg",
                    "title": ld.get("title") or resolved.get("title"),
                    "thumb": ld.get("thumb") or resolved.get("thumb"),
                    "duration": duration, "provider": "loader.to",
                    "content_length": None, "ua": _UA, "ts": time.time(),
                }
                return await _proxy(ld["url"], "audio/mpeg", request, ua=_UA, duration=duration)
        raise


@router.get("/player")
@router.get("/fm/player")
@router.get("/ytm/player")
async def player_json(id: str = Query(...)):
    """Full player response (formats + metadata) — SimpMusic player()."""
    vid = id.replace("yt:", "").strip()
    api = _H()
    data = await api._innertube_player(vid, prefer="auto")
    if not data.get("ok"):
        # still return stream endpoint
        return {
            "ok": False,
            "videoId": vid,
            "error": data.get("error"),
            "play_url": f"/download?id={vid}&type=audio&quality=best",
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
            "bitrate": v.get("bitrate"),
            "height": v.get("height"),
            "kind": "video",
        })
    return {
        "ok": True,
        "videoId": vid,
        "title": data.get("title"),
        "thumbnail": data.get("thumb"),
        "duration": data.get("duration"),
        "provider": data.get("provider"),
        "audio_url": data.get("audio_url"),
        "video_url": data.get("video_url"),
        "streams": streams,
        "play_url": f"/download?id={vid}&type=audio&quality=best",
        "contentLength": data.get("content_length"),
    }


# ─── SEARCH / SUGGESTIONS ──────────────────────────────────────────────────

@router.get("/search")
@router.get("/fm/search")
@router.get("/ytm/search")
async def search(q: str = Query(..., min_length=1), type: str = Query("song"), limit: int = Query(20, ge=1, le=50)):
    api = _H()
    songs = await api._ytm_search_songs(q, limit=limit)
    items = [
        _item(s["video_id"], s.get("title") or "", s.get("artist") or "", s.get("thumb"), s.get("duration"))
        for s in songs
    ]
    return {"ok": True, "query": q, "type": type, "result": items, "results": items, "items": items, "count": len(items)}


@router.get("/search/suggestions")
@router.get("/fm/suggestions")
@router.get("/ytm/suggestions")
async def search_suggestions(q: str = Query(..., min_length=1)):
    api = _H()
    try:
        data = await api._ytm_post("music/get_search_suggestions", {"input": q})
    except Exception:
        data = {}
    suggestions = []
    raw = []
    try:
        api._ytm_walk(data, "searchSuggestionRenderer", raw)
        for it in raw:
            t = api._ytm_text(it.get("suggestion") or it.get("navigationEndpoint"))
            if t:
                suggestions.append(t)
    except Exception:
        pass
    if not suggestions:
        songs = await api._ytm_search_songs(q, limit=8)
        suggestions = [f"{s.get('title')} - {s.get('artist')}" for s in songs if s.get("title")]
    return {"ok": True, "query": q, "suggestions": suggestions[:15]}


# ─── HOME / EXPLORE / CHARTS / MOODS ────────────────────────────────────────

@router.get("/v2/home")
@router.get("/fm/home")
@router.get("/ytm/home")
@router.get("/home")
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
    flat = _sections_to_items(sections)
    return {"ok": bool(sections), "result": sections, "results": sections, "items": flat}


@router.get("/explore")
@router.get("/fm/explore")
@router.get("/ytm/explore")
async def explore():
    sections = await _browse_sections("FEmusic_explore")
    items = _sections_to_items(sections)
    # map to _item
    mapped = []
    for sec in sections:
        mapped.append({
            "title": sec.get("title"),
            "items": [
                _item(s.get("video_id") or "", s.get("title") or "", s.get("artist") or "", s.get("thumb"))
                for s in (sec.get("items") or [])
            ],
        })
    return {"ok": True, "result": mapped, "items": items, "provider": "ytm-explore"}


@router.get("/charts")
@router.get("/fm/charts")
@router.get("/ytm/charts")
async def charts():
    sections = await _browse_sections("FEmusic_charts")
    mapped = []
    for sec in sections:
        mapped.append({
            "title": sec.get("title"),
            "items": [
                _item(s.get("video_id") or "", s.get("title") or "", s.get("artist") or "", s.get("thumb"))
                for s in (sec.get("items") or [])
            ],
        })
    return {"ok": True, "result": mapped, "items": _sections_to_items(sections), "provider": "ytm-charts"}


@router.get("/moods")
@router.get("/fm/moods")
@router.get("/ytm/moods")
@router.get("/mood-and-genres")
async def moods():
    sections = await _browse_sections("FEmusic_moods_and_genres")
    mapped = []
    for sec in sections:
        mapped.append({
            "title": sec.get("title"),
            "items": [
                _item(s.get("video_id") or "", s.get("title") or "", s.get("artist") or "", s.get("thumb"))
                for s in (sec.get("items") or [])
            ],
        })
    return {"ok": True, "result": mapped, "items": _sections_to_items(sections), "provider": "ytm-moods"}


@router.get("/new-releases")
@router.get("/fm/new-releases")
@router.get("/ytm/new-releases")
async def new_releases():
    sections = await _browse_sections("FEmusic_new_releases")
    if not sections:
        api = _H()
        songs = await api._ytm_search_songs("new music this week", limit=20)
        items = [_item(s["video_id"], s.get("title") or "", s.get("artist") or "", s.get("thumb")) for s in songs]
        return {"ok": True, "result": [{"title": "New releases", "items": items}], "items": items}
    mapped = [{
        "title": sec.get("title"),
        "items": [_item(s.get("video_id") or "", s.get("title") or "", s.get("artist") or "", s.get("thumb"))
                  for s in (sec.get("items") or [])],
    } for sec in sections]
    return {"ok": True, "result": mapped, "items": _sections_to_items(sections)}


@router.get("/trending")
@router.get("/fm/trending")
async def trending(limit: int = Query(20, ge=1, le=40)):
    api = _H()
    songs = await api._ytm_search_songs("Top songs this week", limit=limit)
    items = [_item(s["video_id"], s.get("title") or "", s.get("artist") or "", s.get("thumb")) for s in songs]
    return {"ok": True, "result": items, "items": items, "count": len(items)}


# ─── NEXT / RELATED / QUEUE ────────────────────────────────────────────────

@router.get("/next")
@router.get("/fm/next")
@router.get("/ytm/next")
async def next_songs(id: str = Query(...), limit: int = Query(25, ge=1, le=50)):
    api = _H()
    vid = id.replace("yt:", "").strip()
    songs = await api._ytm_next_songs(vid, limit=limit)
    items = [_item(s["video_id"], s.get("title") or "", s.get("artist") or "", s.get("thumb")) for s in songs]
    return {
        "ok": bool(items),
        "videoId": vid,
        "result": items,
        "results": items,
        "items": items,
        "sections": [{"title": "Up next", "items": items}],
        "count": len(items),
    }


@router.get("/related")
@router.get("/fm/related")
@router.get("/ytm/related")
async def related(id: str = Query(...), limit: int = Query(20, ge=1, le=50)):
    return await next_songs(id=id, limit=limit)


@router.get("/queue")
@router.get("/fm/queue")
async def queue(ids: str = Query(..., description="comma-separated video ids")):
    vids = [v.strip() for v in ids.split(",") if re.match(r"^[\w-]{6,20}$", v.strip())][:30]
    api = _H()
    items = []
    for vid in vids:
        try:
            pl = await asyncio.wait_for(api._innertube_player(vid), timeout=3.0)
            if pl.get("ok"):
                items.append(_item(vid, pl.get("title") or vid, "", pl.get("thumb"), pl.get("duration")))
            else:
                items.append(_item(vid))
        except Exception:
            items.append(_item(vid))
    return {"ok": True, "items": items, "result": items, "count": len(items)}


# ─── ALBUM / ARTIST / PLAYLIST / BROWSE ────────────────────────────────────

@router.get("/getAlbum")
@router.get("/fm/album")
@router.get("/ytm/album")
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
        return {"ok": bool(songs), "albumId": id, "result": songs, "results": songs, "items": songs}
    except Exception as e:
        return {"ok": False, "result": [], "error": str(e)[:100]}


@router.get("/getArtists")
@router.get("/getArtist")
@router.get("/fm/artist")
@router.get("/ytm/artist")
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
        return {"ok": bool(songs), "artistId": id, "result": songs, "results": songs, "items": songs[:50]}
    except Exception as e:
        return {"ok": False, "result": [], "error": str(e)[:100]}


@router.get("/playlist")
@router.get("/fm/playlist")
@router.get("/ytm/playlist")
async def get_playlist(id: str = Query(...)):
    bid = id if id.startswith("VL") or id.startswith("PL") else f"VL{id}"
    return await get_album(id=bid)


@router.get("/browse")
@router.get("/fm/browse")
@router.get("/ytm/browse")
async def browse(id: str = Query(..., description="browseId"), params: Optional[str] = Query(None)):
    sections = await _browse_sections(id, params)
    mapped = [{
        "title": sec.get("title"),
        "items": [_item(s.get("video_id") or "", s.get("title") or "", s.get("artist") or "", s.get("thumb"))
                  for s in (sec.get("items") or [])],
    } for sec in sections]
    return {"ok": True, "browseId": id, "result": mapped, "items": _sections_to_items(sections)}


# ─── LYRICS / TRANSCRIPT / INFO ────────────────────────────────────────────

@router.get("/music/lyrics/plain")
@router.get("/fm/lyrics")
@router.get("/ytm/lyrics")
@router.get("/lyrics")
async def lyrics_plain(id: Optional[str] = Query(None), q: Optional[str] = Query(None), title: Optional[str] = Query(None), artist: Optional[str] = Query("")):
    api = _H()
    query = (q or title or "").strip()
    vid = (id or "").replace("yt:", "").strip() if id else ""
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
        return {"ok": False, "lyrics": None, "text": None, "result": None, "videoId": vid or None}
    best = arr[0]
    plain = best.get("plainLyrics") or best.get("syncedLyrics") or ""
    return {
        "ok": True,
        "videoId": vid or None,
        "title": best.get("trackName") or query,
        "author": best.get("artistName") or artist,
        "lyrics": plain,
        "text": plain,
        "result": plain,
        "synced": best.get("syncedLyrics"),
        "source": "lrclib",
    }


@router.get("/songs/lyrics")
async def songs_lyrics(
    id: Optional[str] = Query(None),
    q: Optional[str] = Query(None),
    title: Optional[str] = Query(None),
    artist: Optional[str] = Query(""),
    type: Optional[str] = Query(None),
):
    r = await lyrics_plain(id=id, q=q, title=title, artist=artist)
    if not r.get("ok"):
        return {"success": False, "message": {"lyrics": None, "text": None}}
    return {
        "success": True,
        "ok": True,
        "message": {
            "lyrics": r.get("lyrics"),
            "text": r.get("text"),
            "synced": r.get("synced"),
            "title": r.get("title"),
            "artist": r.get("author"),
        },
        "lyrics": r.get("lyrics"),
        "text": r.get("text"),
    }


@router.get("/transcript")
@router.get("/fm/transcript")
async def transcript(id: str = Query(...)):
    """Best-effort captions via timedtext (public)."""
    vid = id.replace("yt:", "").strip()
    async with httpx.AsyncClient(timeout=12.0, follow_redirects=True) as client:
        # list tracks
        r = await client.get(
            "https://www.youtube.com/api/timedtext",
            params={"type": "list", "v": vid},
            headers={"User-Agent": _UA},
        )
        if r.status_code != 200:
            return {"ok": False, "transcript": None, "videoId": vid}
        # try English
        r2 = await client.get(
            "https://www.youtube.com/api/timedtext",
            params={"lang": "en", "v": vid},
            headers={"User-Agent": _UA},
        )
        text = r2.text if r2.status_code == 200 else ""
        # strip xml tags lightly
        plain = re.sub(r"<[^>]+>", " ", text)
        plain = re.sub(r"\s+", " ", plain).strip()
        return {"ok": bool(plain), "videoId": vid, "transcript": plain or None, "raw_xml": text[:5000] if text else None}


@router.get("/fm/info")
@router.get("/info")
@router.get("/ytm/info")
@router.get("/media")
async def info(id: str = Query(...)):
    vid = id.replace("yt:", "").strip()
    api = _H()
    title, thumb, duration = vid, _thumb(vid), None
    try:
        pl = await asyncio.wait_for(api._innertube_player(vid), timeout=4.0)
        if pl.get("ok"):
            title = pl.get("title") or title
            thumb = pl.get("thumb") or thumb
            duration = pl.get("duration")
    except Exception:
        pass
    return {
        "ok": True,
        "videoId": vid,
        "title": title,
        "thumbnail": thumb,
        "duration": duration,
        "url": f"/download?id={vid}&type=audio&quality=best",
        "play_url": f"/download?id={vid}&type=audio&quality=best",
        "watch_url": f"https://music.youtube.com/watch?v={vid}",
    }


@router.get("/match")
@router.get("/fm/match")
async def match_song(title: str = Query(...), artist: str = Query("")):
    api = _H()
    songs = await api._ytm_search_songs(f"{title} {artist}".strip(), limit=5)
    if not songs:
        return {"ok": False, "result": None}
    s = songs[0]
    return {"ok": True, "result": _item(s["video_id"], s.get("title") or "", s.get("artist") or "", s.get("thumb"))}


# ─── THUMBNAIL ─────────────────────────────────────────────────────────────

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


# ─── API INDEX ─────────────────────────────────────────────────────────────

@router.get("/fm")
@router.get("/ytm")
@router.get("/fastmusic")
async def fm_index():
    return {
        "ok": True,
        "name": "FastMusic",
        "parity": ["SimpMusic", "vivi-music", "ClulesVibe/PaxSenix Sketchware"],
        "stream": {
            "play": "GET /download?id=VIDEO_ID&type=audio&quality=best",
            "player_meta": "GET /player?id=VIDEO_ID",
            "seek": "Range bytes supported (206)",
        },
        "catalog": {
            "search": "GET /search?q=",
            "suggestions": "GET /search/suggestions?q=",
            "home": "GET /v2/home or /home",
            "explore": "GET /explore",
            "charts": "GET /charts",
            "moods": "GET /moods",
            "new_releases": "GET /new-releases",
            "trending": "GET /trending",
            "next": "GET /next?id=",
            "related": "GET /related?id=",
            "album": "GET /album?id=BROWSE_ID",
            "artist": "GET /artist?id=BROWSE_ID",
            "playlist": "GET /playlist?id=VL...|PL...",
            "browse": "GET /browse?id=FEmusic_home",
            "queue": "GET /queue?ids=id1,id2",
            "lyrics": "GET /lyrics?id= or ?q=",
            "transcript": "GET /transcript?id=",
            "info": "GET /info?id=",
            "thumbnail": "GET /thumbnailHD?id=",
            "match": "GET /match?title=&artist=",
        },
    }
