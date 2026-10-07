#!/usr/bin/env python3
"""
StreamHub — Telegram CDN / Storage module
=========================================
creator: shawon

Telegram = storage + CDN. Works standalone OR mounted into StreamHub api.py.

Data location (default):
  <project>/data/telegram_cdn/
    telegram_cdn.db      ← SQLite metadata
    *.session            ← Telethon sessions
    tmp_*                ← temporary upload files

Env:
  TG_API_ID, TG_API_HASH          required
  TG_BOT_TOKEN                    bot mode
  TG_PHONE, TG_SESSION            userbot mode
  TG_STORAGE_CHAT                 default "me"
  TG_API_KEY                      optional write protection
  TG_DATA_DIR                     override data folder

Standalone:
  python telegram_cdn_api.py

Integrated (api.py):
  from telegram_cdn_api import router as tg_router, init_tg_cdn
  app.include_router(tg_router)
  # startup: await init_tg_cdn()
"""

from __future__ import annotations

import mimetypes
import os
import re
import sqlite3
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, AsyncIterator, Dict, List, Optional, Tuple

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

from fastapi import (
    APIRouter,
    FastAPI,
    File,
    Form,
    HTTPException,
    Query,
    Request,
    UploadFile,
)
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, StreamingResponse

try:
    from telethon import TelegramClient
    from telethon.tl.types import (
        DocumentAttributeFilename,
        DocumentAttributeVideo,
        Message,
        MessageMediaDocument,
    )
    from telethon.errors import FloodWaitError, RPCError
    TELETHON_OK = True
except ImportError:
    TELETHON_OK = False
    TelegramClient = None  # type: ignore
    Message = Any  # type: ignore
    MessageMediaDocument = type  # type: ignore
    DocumentAttributeFilename = type  # type: ignore
    DocumentAttributeVideo = type  # type: ignore
    FloodWaitError = Exception  # type: ignore
    RPCError = Exception  # type: ignore

try:
    import httpx
except ImportError:
    httpx = None  # type: ignore

# ══════════════════════════════════════════════════════
# Config & data paths
# ══════════════════════════════════════════════════════

CREATOR = "shawon"
TG_VERSION = "1.0.0"

# Project root = directory containing this file (or cwd when frozen)
_MODULE_DIR = Path(__file__).resolve().parent
# Vercel / serverless: only /tmp is writable — never mkdir under the package path
_ON_SERVERLESS = bool(os.getenv("VERCEL") or os.getenv("AWS_LAMBDA_FUNCTION_NAME") or os.getenv("FUNCTION_TARGET"))
_DEFAULT_DATA = Path("/tmp/streamhub_telegram_cdn") if _ON_SERVERLESS else (_MODULE_DIR / "data" / "telegram_cdn")

TG_API_ID = int(os.getenv("TG_API_ID", "0") or 0)
TG_API_HASH = os.getenv("TG_API_HASH", "").strip()
TG_BOT_TOKEN = os.getenv("TG_BOT_TOKEN", "").strip()
TG_SESSION = os.getenv("TG_SESSION", "streamhub_tg")
TG_PHONE = os.getenv("TG_PHONE", "").strip()
TG_STORAGE_CHAT = os.getenv("TG_STORAGE_CHAT", "me").strip()
TG_API_KEY = os.getenv("TG_API_KEY", "").strip()
DATA_DIR = Path(os.getenv("TG_DATA_DIR", str(_DEFAULT_DATA))).expanduser().resolve()
DB_PATH = DATA_DIR / "telegram_cdn.db"
CHUNK_SIZE = 1024 * 1024
MAX_UPLOAD_MB = int(os.getenv("TG_MAX_UPLOAD_MB", "20480"))

def _ensure_data_dir() -> None:
    try:
        DATA_DIR.mkdir(parents=True, exist_ok=True)
    except OSError as e:
        print(f"[TG-CDN] data dir not writable ({DATA_DIR}): {e}")

_ensure_data_dir()

# ══════════════════════════════════════════════════════
# Helpers
# ══════════════════════════════════════════════════════

def ok(data: Any = None, **extra) -> dict:
    o: Dict[str, Any] = {"creator": CREATOR, "ok": True}
    if data is not None:
        o["data"] = data
    o.update(extra)
    return o


def fail(msg: str, **extra) -> dict:
    o: Dict[str, Any] = {"creator": CREATOR, "ok": False, "error": msg}
    o.update(extra)
    return o


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _safe_name(name: str) -> str:
    name = (name or "video").strip()
    name = re.sub(r"[^\w.\- ()\[\]]+", "_", name)
    return name[:180] or "video"


def _guess_mime(filename: str) -> str:
    mime, _ = mimetypes.guess_type(filename)
    return mime or "application/octet-stream"


def _require_api_key(request: Request) -> None:
    if not TG_API_KEY:
        return
    key = request.headers.get("x-api-key") or request.query_params.get("api_key")
    if key != TG_API_KEY:
        raise HTTPException(401, detail=fail("Invalid or missing API key"))


# ══════════════════════════════════════════════════════
# Database  →  DATA_DIR / telegram_cdn.db
# ══════════════════════════════════════════════════════

def _db() -> sqlite3.Connection:
    conn = sqlite3.connect(str(DB_PATH), check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def init_db() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    with _db() as conn:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS videos (
                id              TEXT PRIMARY KEY,
                title           TEXT NOT NULL,
                filename        TEXT,
                mime_type       TEXT,
                size_bytes      INTEGER DEFAULT 0,
                duration        REAL,
                width           INTEGER,
                height          INTEGER,
                tg_chat_id      TEXT,
                tg_message_id   INTEGER,
                tg_file_id      TEXT,
                tg_access_hash  TEXT,
                tg_file_ref     BLOB,
                tg_dc_id        INTEGER,
                thumbnail_id    TEXT,
                tags            TEXT DEFAULT '',
                notes           TEXT DEFAULT '',
                uploaded_by     TEXT DEFAULT 'api',
                created_at      TEXT,
                updated_at      TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_videos_title ON videos(title);
            CREATE INDEX IF NOT EXISTS idx_videos_created ON videos(created_at);
            """
        )


def _row_to_dict(row: sqlite3.Row) -> dict:
    d = dict(row)
    if "tg_file_ref" in d:
        d["tg_file_ref"] = bool(d["tg_file_ref"])
    return d


def db_insert_video(meta: dict) -> str:
    vid = meta.get("id") or uuid.uuid4().hex[:12]
    now = _now()
    with _db() as conn:
        conn.execute(
            """
            INSERT INTO videos (
                id, title, filename, mime_type, size_bytes, duration,
                width, height, tg_chat_id, tg_message_id, tg_file_id,
                tg_access_hash, tg_file_ref, tg_dc_id, thumbnail_id,
                tags, notes, uploaded_by, created_at, updated_at
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                vid,
                meta.get("title") or meta.get("filename") or "Untitled",
                meta.get("filename"),
                meta.get("mime_type"),
                meta.get("size_bytes") or 0,
                meta.get("duration"),
                meta.get("width"),
                meta.get("height"),
                str(meta.get("tg_chat_id") or ""),
                meta.get("tg_message_id"),
                meta.get("tg_file_id"),
                str(meta.get("tg_access_hash") or ""),
                meta.get("tg_file_ref"),
                meta.get("tg_dc_id"),
                meta.get("thumbnail_id"),
                meta.get("tags") or "",
                meta.get("notes") or "",
                meta.get("uploaded_by") or "api",
                now,
                now,
            ),
        )
    return vid


def db_get_video(vid: str) -> Optional[dict]:
    with _db() as conn:
        row = conn.execute("SELECT * FROM videos WHERE id=?", (vid,)).fetchone()
    return _row_to_dict(row) if row else None


def db_list_videos(q: str = "", limit: int = 50, offset: int = 0) -> Tuple[List[dict], int]:
    with _db() as conn:
        if q:
            like = f"%{q}%"
            total = conn.execute(
                "SELECT COUNT(*) FROM videos WHERE title LIKE ? OR filename LIKE ? OR tags LIKE ?",
                (like, like, like),
            ).fetchone()[0]
            rows = conn.execute(
                """SELECT * FROM videos
                   WHERE title LIKE ? OR filename LIKE ? OR tags LIKE ?
                   ORDER BY created_at DESC LIMIT ? OFFSET ?""",
                (like, like, like, limit, offset),
            ).fetchall()
        else:
            total = conn.execute("SELECT COUNT(*) FROM videos").fetchone()[0]
            rows = conn.execute(
                "SELECT * FROM videos ORDER BY created_at DESC LIMIT ? OFFSET ?",
                (limit, offset),
            ).fetchall()
    return [_row_to_dict(r) for r in rows], total


def db_delete_video(vid: str) -> bool:
    with _db() as conn:
        cur = conn.execute("DELETE FROM videos WHERE id=?", (vid,))
        return cur.rowcount > 0


def db_update_video(vid: str, **fields) -> bool:
    if not fields:
        return False
    fields["updated_at"] = _now()
    cols = ", ".join(f"{k}=?" for k in fields)
    vals = list(fields.values()) + [vid]
    with _db() as conn:
        cur = conn.execute(f"UPDATE videos SET {cols} WHERE id=?", vals)
        return cur.rowcount > 0


# ══════════════════════════════════════════════════════
# Telegram client
# ══════════════════════════════════════════════════════

_client: Any = None
_connected = False


async def get_client() -> Any:
    global _client, _connected
    if not TELETHON_OK:
        raise HTTPException(
            503,
            detail=fail(
                "Telethon not installed",
                tip="pip install telethon",
            ),
        )
    if not TG_API_ID or not TG_API_HASH:
        raise HTTPException(
            503,
            detail=fail(
                "TG_API_ID / TG_API_HASH missing",
                how="https://my.telegram.org → API development tools",
                data_dir=str(DATA_DIR),
            ),
        )

    if _client is not None and _connected:
        return _client

    session_path = str(DATA_DIR / TG_SESSION)
    if TG_BOT_TOKEN:
        _client = TelegramClient(session_path + "_bot", TG_API_ID, TG_API_HASH)
        await _client.start(bot_token=TG_BOT_TOKEN)
    else:
        _client = TelegramClient(session_path, TG_API_ID, TG_API_HASH)
        await _client.start(phone=TG_PHONE or None)

    _connected = True
    me = await _client.get_me()
    print(f"[TG-CDN] Connected as {getattr(me, 'username', None) or me.id} | data={DATA_DIR}")
    return _client


async def init_tg_cdn() -> None:
    """Call from StreamHub startup (optional eager connect)."""
    init_db()
    print(f"[TG-CDN] DB → {DB_PATH}")
    if TELETHON_OK and TG_API_ID and TG_API_HASH:
        try:
            await get_client()
        except Exception as e:
            print(f"[TG-CDN] connect deferred: {e}")


async def resolve_storage_chat(chat: Optional[str] = None):
    client = await get_client()
    target = chat or TG_STORAGE_CHAT or "me"
    try:
        return await client.get_entity(target)
    except Exception as e:
        raise HTTPException(
            400,
            detail=fail(
                f"Cannot resolve storage chat '{target}'",
                detail=str(e)[:200],
                tip="Use @username, numeric id, or 'me'",
            ),
        )


def _extract_doc_info(msg: Message) -> dict:
    info: Dict[str, Any] = {
        "tg_message_id": msg.id,
        "tg_chat_id": str(msg.chat_id) if msg.chat_id else "",
        "size_bytes": 0,
        "filename": None,
        "mime_type": None,
        "duration": None,
        "width": None,
        "height": None,
        "tg_file_id": None,
        "tg_access_hash": None,
        "tg_file_ref": None,
        "tg_dc_id": None,
    }
    media = msg.media
    if not media or not isinstance(media, MessageMediaDocument):
        if getattr(msg, "file", None):
            info["size_bytes"] = msg.file.size or 0
            info["filename"] = msg.file.name
            info["mime_type"] = msg.file.mime_type
        return info

    doc = media.document
    if not doc:
        return info

    info["size_bytes"] = getattr(doc, "size", 0) or 0
    info["mime_type"] = getattr(doc, "mime_type", None)
    info["tg_file_id"] = str(getattr(doc, "id", ""))
    info["tg_access_hash"] = str(getattr(doc, "access_hash", ""))
    info["tg_file_ref"] = getattr(doc, "file_reference", None)
    info["tg_dc_id"] = getattr(doc, "dc_id", None)

    for attr in getattr(doc, "attributes", []) or []:
        if isinstance(attr, DocumentAttributeFilename):
            info["filename"] = attr.file_name
        elif isinstance(attr, DocumentAttributeVideo):
            info["duration"] = getattr(attr, "duration", None)
            info["width"] = getattr(attr, "w", None)
            info["height"] = getattr(attr, "h", None)

    if not info["filename"]:
        ext = mimetypes.guess_extension(info["mime_type"] or "") or ".bin"
        info["filename"] = f"file_{info['tg_file_id']}{ext}"
    return info


# ══════════════════════════════════════════════════════
# Upload / stream helpers
# ══════════════════════════════════════════════════════

async def tg_upload_path(
    path: Path,
    title: Optional[str] = None,
    caption: Optional[str] = None,
    chat: Optional[str] = None,
    tags: str = "",
    notes: str = "",
) -> dict:
    if not path.is_file():
        raise HTTPException(400, detail=fail("File not found", path=str(path)))
    size = path.stat().st_size
    if size > MAX_UPLOAD_MB * 1024 * 1024:
        raise HTTPException(413, detail=fail(f"File too large ({size/1e9:.2f} GB)"))

    client = await get_client()
    entity = await resolve_storage_chat(chat)
    filename = path.name
    mime = _guess_mime(filename)

    def _progress(sent, total):
        if total and sent % (50 * 1024 * 1024) < CHUNK_SIZE:
            print(f"[TG-CDN] upload {filename}: {100*sent/total:.1f}%")

    try:
        msg = await client.send_file(
            entity,
            file=str(path),
            caption=caption or title or filename,
            force_document=True,
            progress_callback=_progress,
            attributes=[DocumentAttributeFilename(filename)],
        )
    except FloodWaitError as e:
        raise HTTPException(429, detail=fail(f"FloodWait {e.seconds}s", seconds=e.seconds))
    except RPCError as e:
        raise HTTPException(502, detail=fail("Telegram upload failed", detail=str(e)[:200]))

    info = _extract_doc_info(msg)
    meta = {
        **info,
        "title": title or path.stem,
        "filename": filename,
        "mime_type": mime or info.get("mime_type"),
        "tags": tags,
        "notes": notes,
        "uploaded_by": "api",
    }
    vid = db_insert_video(meta)
    meta["id"] = vid
    return meta


async def tg_upload_url(
    url: str,
    title: Optional[str] = None,
    filename: Optional[str] = None,
    chat: Optional[str] = None,
    tags: str = "",
    notes: str = "",
) -> dict:
    if not httpx:
        raise HTTPException(503, detail=fail("httpx not installed"))
    from urllib.parse import urlparse, unquote

    name = filename or _safe_name(unquote(urlparse(url).path).rsplit("/", 1)[-1] or "download.bin")
    tmp = DATA_DIR / f"tmp_{uuid.uuid4().hex}_{name}"
    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(600.0, connect=30.0), follow_redirects=True) as client:
            async with client.stream("GET", url) as r:
                if r.status_code >= 400:
                    raise HTTPException(502, detail=fail(f"Remote HTTP {r.status_code}"))
                with open(tmp, "wb") as f:
                    async for chunk in r.aiter_bytes(CHUNK_SIZE):
                        f.write(chunk)
        return await tg_upload_path(tmp, title=title or Path(name).stem, chat=chat, tags=tags, notes=notes)
    finally:
        if tmp.exists():
            tmp.unlink(missing_ok=True)


async def tg_stream_generator(video: dict, start: int = 0, end: Optional[int] = None) -> AsyncIterator[bytes]:
    client = await get_client()
    chat_id = video.get("tg_chat_id")
    msg_id = video.get("tg_message_id")
    if not chat_id or not msg_id:
        raise HTTPException(500, detail=fail("Missing Telegram message reference"))

    try:
        entity = await client.get_entity(
            int(chat_id) if str(chat_id).lstrip("-").isdigit() else chat_id
        )
        msg = await client.get_messages(entity, ids=int(msg_id))
    except Exception as e:
        raise HTTPException(502, detail=fail("Cannot fetch TG message", detail=str(e)[:200]))

    if not msg or not msg.media:
        raise HTTPException(404, detail=fail("Telegram media not found"))

    file_size = video.get("size_bytes") or 0
    if end is None and file_size:
        end = file_size - 1
    request_size = (end - start + 1) if (end is not None) else None

    try:
        async for chunk in client.iter_download(
            msg.media,
            offset=start,
            request_size=request_size or CHUNK_SIZE,
            chunk_size=CHUNK_SIZE,
        ):
            if not chunk:
                break
            yield chunk
            if request_size is not None:
                request_size -= len(chunk)
                if request_size <= 0:
                    break
    except FloodWaitError as e:
        raise HTTPException(429, detail=fail(f"FloodWait {e.seconds}s", seconds=e.seconds))
    except Exception as e:
        raise HTTPException(502, detail=fail("Stream failed", detail=str(e)[:200]))


def _parse_range(range_header: Optional[str], file_size: int) -> Tuple[int, int, int]:
    if not range_header or not file_size:
        return 0, max(file_size - 1, 0), 200
    m = re.match(r"bytes=(\d*)-(\d*)", range_header.strip())
    if not m:
        return 0, file_size - 1, 200
    start_s, end_s = m.group(1), m.group(2)
    if start_s == "" and end_s == "":
        return 0, file_size - 1, 200
    if start_s == "":
        length = int(end_s)
        start = max(0, file_size - length)
        end = file_size - 1
    else:
        start = int(start_s)
        end = int(end_s) if end_s else file_size - 1
    start = max(0, min(start, file_size - 1))
    end = max(start, min(end, file_size - 1))
    return start, end, 206


def _enrich(v: dict, origin: str) -> dict:
    vid = v["id"]
    v["stream_url"] = f"{origin}/tg/stream/{vid}"
    v["download_url"] = f"{origin}/tg/download/{vid}"
    v["size_mb"] = round((v.get("size_bytes") or 0) / 1e6, 2)
    v["size_gb"] = round((v.get("size_bytes") or 0) / 1e9, 3)
    return v


# ══════════════════════════════════════════════════════
# Router (mounted into StreamHub at /tg)
# ══════════════════════════════════════════════════════

router = APIRouter(prefix="/tg", tags=["Telegram CDN"])


@router.get("/health")
async def tg_health():
    return ok(
        {
            "version": TG_VERSION,
            "telethon": TELETHON_OK,
            "api_id_set": bool(TG_API_ID),
            "api_hash_set": bool(TG_API_HASH),
            "bot_token_set": bool(TG_BOT_TOKEN),
            "storage_chat": TG_STORAGE_CHAT,
            "data_dir": str(DATA_DIR),
            "db_path": str(DB_PATH),
            "connected": _connected,
        },
        provider="telegram_cdn",
        endpoint="health",
    )


@router.get("/setup")
async def tg_setup():
    return ok(
        {
            "data_storage": {
                "folder": str(DATA_DIR),
                "database": str(DB_PATH),
                "sessions": str(DATA_DIR / f"{TG_SESSION}.session"),
                "note": "All metadata in SQLite; actual video bytes stay on Telegram cloud",
            },
            "steps": [
                {
                    "step": 1,
                    "title": "Get API credentials",
                    "url": "https://my.telegram.org",
                    "do": "API development tools → api_id + api_hash",
                },
                {
                    "step": 2,
                    "title": "Bot or Userbot",
                    "bot": "TG_BOT_TOKEN from @BotFather (~2GB/file)",
                    "userbot": "TG_PHONE + TG_SESSION for large files (3–20GB practical)",
                },
                {
                    "step": 3,
                    "title": "Storage chat",
                    "do": "TG_STORAGE_CHAT=me | @channel | -100…",
                },
                {
                    "step": 4,
                    "title": "Env vars",
                    "env": {
                        "TG_API_ID": "required",
                        "TG_API_HASH": "required",
                        "TG_BOT_TOKEN": "optional",
                        "TG_PHONE": "optional userbot",
                        "TG_SESSION": "streamhub_tg",
                        "TG_STORAGE_CHAT": "me",
                        "TG_API_KEY": "optional",
                        "TG_DATA_DIR": str(DATA_DIR),
                    },
                },
                {
                    "step": 5,
                    "title": "Install",
                    "commands": [
                        "pip install telethon fastapi uvicorn aiofiles python-multipart python-dotenv httpx",
                        "python api.py   # StreamHub + /tg/* together",
                    ],
                },
            ],
            "endpoints": [
                "GET  /tg/health",
                "GET  /tg/setup",
                "GET  /tg/videos",
                "GET  /tg/videos/{id}",
                "GET  /tg/stream/{id}",
                "GET  /tg/download/{id}",
                "POST /tg/upload",
                "POST /tg/upload/path",
                "POST /tg/upload/url",
                "POST /tg/import",
                "DELETE /tg/videos/{id}",
                "PATCH /tg/videos/{id}",
                "GET  /tg/stats",
                "GET  /tg/ui",
            ],
        },
        provider="telegram_cdn",
        endpoint="setup",
    )


@router.get("/videos")
async def tg_list_videos(
    request: Request,
    q: str = Query(""),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
):
    init_db()
    items, total = db_list_videos(q=q.strip(), limit=limit, offset=offset)
    origin = str(request.base_url).rstrip("/")
    items = [_enrich(it, origin) for it in items]
    return ok(
        {"items": items, "count": len(items), "total": total, "offset": offset, "limit": limit},
        provider="telegram_cdn",
        endpoint="videos",
    )


@router.get("/videos/{video_id}")
async def tg_get_video(video_id: str, request: Request):
    init_db()
    v = db_get_video(video_id)
    if not v:
        raise HTTPException(404, detail=fail("Video not found", id=video_id))
    return ok(_enrich(v, str(request.base_url).rstrip("/")), provider="telegram_cdn", endpoint="video")


@router.post("/upload")
async def tg_upload_file(
    request: Request,
    file: UploadFile = File(...),
    title: str = Form(""),
    tags: str = Form(""),
    notes: str = Form(""),
    chat: str = Form(""),
):
    _require_api_key(request)
    init_db()
    filename = _safe_name(file.filename or "upload.bin")
    tmp = DATA_DIR / f"up_{uuid.uuid4().hex}_{filename}"
    size = 0
    try:
        with open(tmp, "wb") as f:
            while True:
                chunk = await file.read(CHUNK_SIZE)
                if not chunk:
                    break
                size += len(chunk)
                if size > MAX_UPLOAD_MB * 1024 * 1024:
                    raise HTTPException(413, detail=fail("Upload exceeds size limit"))
                f.write(chunk)
        meta = await tg_upload_path(
            tmp, title=title or Path(filename).stem, chat=chat or None, tags=tags, notes=notes
        )
        return ok(_enrich(meta, str(request.base_url).rstrip("/")), provider="telegram_cdn", endpoint="upload")
    finally:
        if tmp.exists():
            tmp.unlink(missing_ok=True)


@router.post("/upload/path")
async def tg_upload_local_path(
    request: Request,
    path: str = Form(...),
    title: str = Form(""),
    tags: str = Form(""),
    notes: str = Form(""),
    chat: str = Form(""),
):
    _require_api_key(request)
    init_db()
    p = Path(path).expanduser().resolve()
    meta = await tg_upload_path(p, title=title or p.stem, chat=chat or None, tags=tags, notes=notes)
    return ok(_enrich(meta, str(request.base_url).rstrip("/")), provider="telegram_cdn", endpoint="upload_path")


@router.post("/upload/url")
async def tg_upload_from_url(
    request: Request,
    url: str = Form(...),
    title: str = Form(""),
    filename: str = Form(""),
    tags: str = Form(""),
    notes: str = Form(""),
    chat: str = Form(""),
):
    _require_api_key(request)
    init_db()
    meta = await tg_upload_url(
        url.strip(), title=title or None, filename=filename or None,
        chat=chat or None, tags=tags, notes=notes,
    )
    return ok(_enrich(meta, str(request.base_url).rstrip("/")), provider="telegram_cdn", endpoint="upload_url")


@router.api_route("/stream/{video_id}", methods=["GET", "HEAD"])
async def tg_stream(video_id: str, request: Request):
    init_db()
    v = db_get_video(video_id)
    if not v:
        raise HTTPException(404, detail=fail("Video not found"))
    file_size = int(v.get("size_bytes") or 0)
    mime = v.get("mime_type") or "video/mp4"
    filename = v.get("filename") or f"{video_id}.mp4"

    if request.method == "HEAD":
        return JSONResponse(
            content=None,
            headers={
                "Accept-Ranges": "bytes",
                "Content-Type": mime,
                "Content-Length": str(file_size) if file_size else "0",
                "Content-Disposition": f'inline; filename="{filename}"',
            },
            status_code=200,
        )

    range_header = request.headers.get("range") or request.headers.get("Range")
    start, end, status = _parse_range(range_header, file_size)
    content_length = end - start + 1 if file_size else 0
    headers = {
        "Accept-Ranges": "bytes",
        "Content-Type": mime,
        "Content-Disposition": f'inline; filename="{filename}"',
        "Cache-Control": "public, max-age=3600",
    }
    if file_size:
        headers["Content-Length"] = str(content_length)
        if status == 206:
            headers["Content-Range"] = f"bytes {start}-{end}/{file_size}"

    return StreamingResponse(
        tg_stream_generator(v, start=start, end=end if file_size else None),
        status_code=status,
        media_type=mime,
        headers=headers,
    )


@router.get("/download/{video_id}")
async def tg_download(video_id: str, request: Request):
    init_db()
    v = db_get_video(video_id)
    if not v:
        raise HTTPException(404, detail=fail("Video not found"))
    file_size = int(v.get("size_bytes") or 0)
    mime = v.get("mime_type") or "application/octet-stream"
    filename = v.get("filename") or f"{video_id}.bin"
    range_header = request.headers.get("range") or request.headers.get("Range")
    start, end, status = _parse_range(range_header, file_size)
    content_length = end - start + 1 if file_size else 0
    headers = {
        "Accept-Ranges": "bytes",
        "Content-Type": mime,
        "Content-Disposition": f'attachment; filename="{filename}"',
    }
    if file_size:
        headers["Content-Length"] = str(content_length)
        if status == 206:
            headers["Content-Range"] = f"bytes {start}-{end}/{file_size}"
    return StreamingResponse(
        tg_stream_generator(v, start=start, end=end if file_size else None),
        status_code=status,
        media_type=mime,
        headers=headers,
    )


@router.delete("/videos/{video_id}")
async def tg_delete(video_id: str, request: Request, also_telegram: bool = Query(False)):
    _require_api_key(request)
    init_db()
    v = db_get_video(video_id)
    if not v:
        raise HTTPException(404, detail=fail("Video not found"))
    tg_deleted = False
    if also_telegram and v.get("tg_message_id") and v.get("tg_chat_id"):
        try:
            client = await get_client()
            chat = v["tg_chat_id"]
            entity = await client.get_entity(
                int(chat) if str(chat).lstrip("-").isdigit() else chat
            )
            await client.delete_messages(entity, int(v["tg_message_id"]))
            tg_deleted = True
        except Exception as e:
            print(f"[TG-CDN] TG delete failed: {e}")
    db_delete_video(video_id)
    return ok(
        {"id": video_id, "deleted": True, "telegram_message_deleted": tg_deleted},
        provider="telegram_cdn",
        endpoint="delete",
    )


@router.patch("/videos/{video_id}")
async def tg_patch(
    video_id: str,
    request: Request,
    title: Optional[str] = Form(None),
    tags: Optional[str] = Form(None),
    notes: Optional[str] = Form(None),
):
    _require_api_key(request)
    init_db()
    if not db_get_video(video_id):
        raise HTTPException(404, detail=fail("Video not found"))
    fields = {}
    if title is not None:
        fields["title"] = title
    if tags is not None:
        fields["tags"] = tags
    if notes is not None:
        fields["notes"] = notes
    db_update_video(video_id, **fields)
    return ok(db_get_video(video_id), provider="telegram_cdn", endpoint="patch")


@router.post("/import")
async def tg_import(
    request: Request,
    chat: str = Form(...),
    message_id: int = Form(...),
    title: str = Form(""),
    tags: str = Form(""),
    notes: str = Form(""),
):
    _require_api_key(request)
    init_db()
    client = await get_client()
    try:
        entity = await client.get_entity(chat)
        msg = await client.get_messages(entity, ids=message_id)
    except Exception as e:
        raise HTTPException(400, detail=fail("Cannot load message", detail=str(e)[:200]))
    if not msg or not msg.media:
        raise HTTPException(404, detail=fail("No media in that message"))
    info = _extract_doc_info(msg)
    meta = {
        **info,
        "title": title or info.get("filename") or f"msg_{message_id}",
        "tags": tags,
        "notes": notes,
        "uploaded_by": "import",
    }
    vid = db_insert_video(meta)
    meta["id"] = vid
    return ok(_enrich(meta, str(request.base_url).rstrip("/")), provider="telegram_cdn", endpoint="import")


@router.get("/stats")
async def tg_stats():
    init_db()
    with _db() as conn:
        total = conn.execute("SELECT COUNT(*) FROM videos").fetchone()[0]
        size = conn.execute("SELECT COALESCE(SUM(size_bytes),0) FROM videos").fetchone()[0]
    return ok(
        {
            "videos": total,
            "total_bytes": size,
            "total_gb": round(size / 1e9, 3),
            "storage_chat": TG_STORAGE_CHAT,
            "data_dir": str(DATA_DIR),
            "db_path": str(DB_PATH),
        },
        provider="telegram_cdn",
        endpoint="stats",
    )


# ── Web UI ───────────────────────────────────────────

UI_HTML = """<!DOCTYPE html>
<html lang="bn"><head>
<meta charset="utf-8"/><meta name="viewport" content="width=device-width,initial-scale=1"/>
<title>StreamHub · Telegram CDN</title>
<style>
:root{--bg:#0b0d14;--card:#141824;--line:#1e2436;--txt:#e8ecf7;--dim:#8b93a7;--a1:#6c8cff;--a2:#3dd6c6;--ok:#3ecf8e;--err:#ff6b7a;--warn:#ffb020;--r:14px;--font:system-ui,sans-serif}
*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--txt);font-family:var(--font)}
.wrap{max-width:960px;margin:0 auto;padding:20px 16px 80px}
header{display:flex;gap:12px;align-items:center;flex-wrap:wrap;margin-bottom:18px}
h1{font-size:1.3rem;margin:0}.badge{font-size:11px;background:#1a2240;color:var(--a1);padding:3px 10px;border-radius:99px}
.card{background:var(--card);border:1px solid var(--line);border-radius:var(--r);padding:16px;margin-bottom:12px}
.card h2{margin:0 0 10px;font-size:.85rem;color:var(--dim);text-transform:uppercase;letter-spacing:.04em}
label{display:block;font-size:12px;color:var(--dim);margin:8px 0 4px}
input,textarea{width:100%;background:#0e1220;border:1px solid var(--line);border-radius:10px;color:var(--txt);padding:10px 12px;font:inherit}
.row{display:flex;gap:8px;flex-wrap:wrap;margin-top:10px}
.btn{border:0;border-radius:10px;padding:10px 14px;font:600 13px var(--font);cursor:pointer}
.btn.pri{background:linear-gradient(135deg,var(--a1),#8b6cff);color:#fff}
.btn.sec{background:#1a2240;color:var(--txt)}
.btn.danger{background:#3a1520;color:var(--err)}
.muted{color:var(--dim);font-size:13px}
.item{display:flex;gap:10px;padding:12px;border:1px solid var(--line);border-radius:12px;background:#0e1220;margin-bottom:8px;flex-wrap:wrap}
.item b{display:block}.item small{color:var(--dim);font-size:12px}
.tabs button{background:#12162a;border:1px solid var(--line);color:var(--dim);border-radius:99px;padding:7px 14px;cursor:pointer;font:600 12px var(--font);margin:0 4px 8px 0}
.tabs button.on{background:#1a2a50;color:var(--a1)}
.panel{display:none}.panel.on{display:block}
.stat{font-size:1.5rem;font-weight:700}
.alert{padding:10px;border-radius:10px;background:#1a1508;border:1px solid #5a4010;color:var(--warn);font-size:13px;margin-bottom:10px}
.toast{position:fixed;bottom:16px;left:50%;transform:translateX(-50%);background:#1a2240;padding:10px 16px;border-radius:12px;display:none;z-index:9}
.toast.show{display:block}code{background:#1a2240;padding:2px 6px;border-radius:6px;font-size:12px}
</style></head><body>
<div class="wrap">
<header><h1>📦 Telegram CDN</h1><span class="badge">StreamHub</span><span class="muted" id="hl">…</span></header>
<div class="tabs">
<button class="on" data-t="lib">Library</button>
<button data-t="up">Upload</button>
<button data-t="imp">Import</button>
<button data-t="setup">Setup</button>
</div>
<div class="panel on" id="p-lib">
<div class="card"><div class="row" style="margin:0;align-items:center">
<div style="flex:1"><div class="stat" id="sv">—</div><div class="muted"><span id="ss">—</span> GB · data in <code id="sd">…</code></div></div>
<input id="q" placeholder="Search…" style="max-width:220px;margin:0"/><button class="btn sec" id="ref">Refresh</button>
</div></div>
<div id="list"></div>
</div>
<div class="panel" id="p-up">
<div class="card"><h2>Upload file</h2>
<div class="alert">বড় ফাইল → <code>/tg/upload/path</code> (সার্ভার পাথ)</div>
<label>File</label><input type="file" id="file"/>
<label>Title</label><input id="ut"/>
<label>Tags</label><input id="ug"/>
<label>API Key</label><input id="key" type="password"/>
<div class="row"><button class="btn pri" id="bu">Upload</button></div>
<p class="muted" id="um"></p></div>
<div class="card"><h2>From URL</h2>
<label>URL</label><input id="url"/><label>Title</label><input id="urt"/>
<div class="row"><button class="btn pri" id="bur">Fetch & store</button></div>
<p class="muted" id="urm"></p>
<hr style="border-color:var(--line);margin:14px 0"/>
<h2>Server path</h2>
<label>Absolute path</label><input id="sp" placeholder="/data/movie.mkv"/>
<div class="row"><button class="btn sec" id="bp">Upload path</button></div>
<p class="muted" id="pm"></p></div>
</div>
<div class="panel" id="p-imp">
<div class="card"><h2>Import TG message</h2>
<label>Chat</label><input id="ic" placeholder="@channel"/>
<label>Message ID</label><input id="im" type="number"/>
<label>Title</label><input id="it"/>
<div class="row"><button class="btn pri" id="bi">Import</button></div>
<p class="muted" id="io"></p></div>
</div>
<div class="panel" id="p-setup"><div class="card" id="sb">Loading…</div></div>
</div>
<div class="toast" id="t"></div>
<script>
const A=location.origin+'/tg';
const $=s=>document.querySelector(s),$$=s=>[...document.querySelectorAll(s)];
const toast=m=>{const t=$('#t');t.textContent=m;t.classList.add('show');setTimeout(()=>t.classList.remove('show'),2500)};
const hdr=()=>{const k=$('#key')?.value||localStorage.getItem('tg_k')||'';if(k)localStorage.setItem('tg_k',k);return k?{'X-API-Key':k}:{}};
async function api(p,o={}){const r=await fetch(A+p,{...o,headers:{...(o.headers||{}),...hdr()}});const j=await r.json().catch(()=>({ok:false,error:'bad json'}));if(!r.ok||j.ok===false)throw new Error(j.error||j.detail?.error||('HTTP '+r.status));return j}
const esc=s=>String(s||'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
$$('.tabs button').forEach(b=>b.onclick=()=>{$$('.tabs button').forEach(x=>x.classList.toggle('on',x===b));$$('.panel').forEach(p=>p.classList.toggle('on',p.id==='p-'+b.dataset.t));if(b.dataset.t==='setup')setup();if(b.dataset.t==='lib')list()});
async function boot(){try{const h=await api('/health');const d=h.data||{};$('#hl').textContent=(d.connected?'● online':'○ offline')+(d.telethon?'':' · no telethon');$('#hl').style.color=d.connected?'var(--ok)':'var(--warn)';$('#sd').textContent=d.data_dir||''}catch(e){$('#hl').textContent='offline';$('#hl').style.color='var(--err)'}stats();list();const k=localStorage.getItem('tg_k');if(k&&$('#key'))$('#key').value=k}
async function stats(){try{const j=await api('/stats');const d=j.data||{};$('#sv').textContent=d.videos??'—';$('#ss').textContent=d.total_gb??'—';if(d.data_dir)$('#sd').textContent=d.data_dir}catch(e){}}
async function list(){const q=$('#q').value.trim();const box=$('#list');box.innerHTML='<p class="muted">Loading…</p>';try{const j=await api('/videos?limit=100'+(q?'&q='+encodeURIComponent(q):''));const items=(j.data&&j.data.items)||[];if(!items.length){box.innerHTML='<p class="muted">Empty — upload first</p>';return}
box.innerHTML=items.map(v=>`<div class="item"><div style="flex:1;min-width:160px"><b>${esc(v.title||v.filename)}</b><small>${esc(v.filename||'')} · ${v.size_gb||0} GB</small></div>
<div class="row" style="margin:0"><a class="btn sec" href="${v.stream_url}" target="_blank">Stream</a>
<a class="btn sec" href="${v.download_url}" target="_blank">DL</a>
<button class="btn sec" data-c="${esc(v.stream_url)}">Copy</button>
<button class="btn danger" data-d="${v.id}">Del</button></div></div>`).join('')}catch(e){box.innerHTML='<p class="muted" style="color:var(--err)">'+esc(e.message)+'</p>'}}
$('#list').onclick=async e=>{const c=e.target.closest('[data-c]');if(c){navigator.clipboard.writeText(c.dataset.c);toast('Copied');return}
const d=e.target.closest('[data-d]');if(!d||!confirm('Delete?'))return;try{await api('/videos/'+d.dataset.d,{method:'DELETE'});toast('Deleted');list();stats()}catch(err){toast(err.message)}};
$('#ref').onclick=()=>{list();stats()};$('#q').oninput=(()=>{let t;return()=>{clearTimeout(t);t=setTimeout(list,400)}})();
$('#bu').onclick=async()=>{const f=$('#file').files[0];if(!f)return toast('Choose file');const fd=new FormData();fd.append('file',f);fd.append('title',$('#ut').value);fd.append('tags',$('#ug').value);
$('#bu').disabled=true;$('#um').textContent='Uploading…';try{const r=await fetch(A+'/upload',{method:'POST',body:fd,headers:hdr()});const j=await r.json();if(!r.ok||!j.ok)throw new Error(j.error||'fail');$('#um').textContent='OK '+j.data.id;toast('Uploaded');list();stats()}catch(e){$('#um').textContent=e.message}finally{$('#bu').disabled=false}};
$('#bur').onclick=async()=>{const fd=new FormData();fd.append('url',$('#url').value.trim());fd.append('title',$('#urt').value);$('#bur').disabled=true;$('#urm').textContent='Working…';
try{const r=await fetch(A+'/upload/url',{method:'POST',body:fd,headers:hdr()});const j=await r.json();if(!r.ok||!j.ok)throw new Error(j.error||'fail');$('#urm').textContent='OK '+j.data.id;list();stats()}catch(e){$('#urm').textContent=e.message}finally{$('#bur').disabled=false}};
$('#bp').onclick=async()=>{const fd=new FormData();fd.append('path',$('#sp').value.trim());fd.append('title',$('#ut').value);$('#bp').disabled=true;$('#pm').textContent='Uploading…';
try{const r=await fetch(A+'/upload/path',{method:'POST',body:fd,headers:hdr()});const j=await r.json();if(!r.ok||!j.ok)throw new Error(j.error||'fail');$('#pm').textContent='OK '+j.data.id;list();stats()}catch(e){$('#pm').textContent=e.message}finally{$('#bp').disabled=false}};
$('#bi').onclick=async()=>{const fd=new FormData();fd.append('chat',$('#ic').value.trim());fd.append('message_id',$('#im').value);fd.append('title',$('#it').value);
try{const r=await fetch(A+'/import',{method:'POST',body:fd,headers:hdr()});const j=await r.json();if(!r.ok||!j.ok)throw new Error(j.error||'fail');$('#io').textContent='OK '+j.data.id;list();stats()}catch(e){$('#io').textContent=e.message}};
async function setup(){try{const j=await api('/setup');const d=j.data||{};const st=d.steps||[];$('#sb').innerHTML=`<p><b>Data folder</b><br><code>${esc((d.data_storage||{}).folder)}</code></p>
<p><b>Database</b><br><code>${esc((d.data_storage||{}).database)}</code></p>
<p class="muted">${esc((d.data_storage||{}).note||'')}</p>
${st.map(s=>`<div style="margin:12px 0"><b>Step ${s.step}: ${esc(s.title)}</b><p class="muted">${esc(s.do||s.bot||'')}</p>
${s.env?'<pre style="background:#0e1220;padding:10px;border-radius:10px;font-size:12px;overflow:auto">'+esc(JSON.stringify(s.env,null,2))+'</pre>':''}</div>`).join('')}
<p><a href="/tg/docs" target="_blank">Swagger /tg/docs</a> · <a href="/docs" target="_blank">Main StreamHub docs</a></p>`}catch(e){$('#sb').innerHTML=esc(e.message)}}
boot();
</script></body></html>
"""


@router.get("/ui", response_class=HTMLResponse)
async def tg_ui():
    return HTMLResponse(UI_HTML)


@router.get("/")
async def tg_root():
    return RedirectResponse("/tg/ui")


# ══════════════════════════════════════════════════════
# Standalone app (optional)
# ══════════════════════════════════════════════════════

def create_standalone_app() -> FastAPI:
    from contextlib import asynccontextmanager

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        await init_tg_cdn()
        yield
        global _client, _connected
        if _client:
            try:
                await _client.disconnect()
            except Exception:
                pass
            _client = None
            _connected = False

    app = FastAPI(
        title="StreamHub Telegram CDN",
        version=TG_VERSION,
        lifespan=lifespan,
        docs_url="/tg/docs",
        redoc_url="/tg/redoc",
    )
    app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])
    app.include_router(router)
    return app


# Only create standalone app when run directly (not when imported by StreamHub api.py)
app = None
if __name__ == "__main__":
    import uvicorn

    app = create_standalone_app()
    port = int(os.getenv("PORT", "8080"))
    print(f"\n  Telegram CDN  data → {DATA_DIR}")
    print(f"  UI → http://127.0.0.1:{port}/tg/ui\n")
    uvicorn.run(app, host="0.0.0.0", port=port, reload=False)
