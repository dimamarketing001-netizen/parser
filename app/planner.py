"""Планировщик публикаций ЛК (2026-11-23): каналы, посты, расписание, фоновая отправка.

Данные — в studio.sqlite3 (таблицы planner_*), файлы медиа — data_dir/planner_media/.
Каналы:
  telegram — бот (токен от @BotFather) + канал/чат (@username или -100…); бот должен быть админом канала.
  manual   — площадки без API: в назначенное время пост становится «пора выложить» + уведомление,
             человек выкладывает сам (готовый текст и файлы) и отмечает «Выложено».
Отправка в Telegram идёт через PLANNER_TG_API_BASE (по умолчанию https://api.telegram.org): из WSL
api.telegram.org недоступен, поэтому на проде это адрес прокси на сервере (+ заголовок PLANNER_TG_RELAY_TOKEN).
Посты принадлежат пользователю (team_id NULL) или команде (team_id) — права как у дизайнов.
"""
from __future__ import annotations

import asyncio
import base64
import json
import os
import re
import secrets
import sqlite3
import time
from contextlib import closing
from pathlib import Path
from typing import Any, Callable

import httpx
from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field

from .teams import RANK as TEAM_RANK, db_path, init_db as teams_init_db, member_role

MEDIA_MAX = 60_000_000          # байт на файл (видео)
TICK = 20                       # период проверки расписания, с
RETRY_MINUTES = (2, 10, 30)     # повторы после ошибки отправки
STATUSES = ("draft", "scheduled", "publishing", "published", "partial", "failed", "due")


def connect(path: Path) -> sqlite3.Connection:
    c = sqlite3.connect(path, timeout=15)
    c.row_factory = sqlite3.Row
    return c


def init_db(path: Path) -> None:
    teams_init_db(path)
    with closing(connect(path)) as c:
        c.executescript("""
        CREATE TABLE IF NOT EXISTS planner_channels (
            id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER NOT NULL, team_id INTEGER,
            kind TEXT NOT NULL, name TEXT NOT NULL, config TEXT NOT NULL DEFAULT '{}', created_at REAL NOT NULL);
        CREATE TABLE IF NOT EXISTS planner_posts (
            id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER NOT NULL, team_id INTEGER,
            text TEXT NOT NULL DEFAULT '', media TEXT NOT NULL DEFAULT '[]', channels TEXT NOT NULL DEFAULT '[]',
            scheduled_at REAL, status TEXT NOT NULL DEFAULT 'draft', results TEXT NOT NULL DEFAULT '{}',
            attempts INTEGER NOT NULL DEFAULT 0, next_try REAL, design_id INTEGER, note TEXT NOT NULL DEFAULT '',
            created_at REAL NOT NULL, updated_at REAL NOT NULL, updated_by INTEGER);
        CREATE INDEX IF NOT EXISTS ix_planner_due ON planner_posts(status, scheduled_at);
        CREATE TABLE IF NOT EXISTS planner_media (
            id TEXT PRIMARY KEY, user_id INTEGER NOT NULL, team_id INTEGER, kind TEXT NOT NULL, mime TEXT NOT NULL,
            name TEXT NOT NULL DEFAULT '', size INTEGER NOT NULL, width INTEGER, height INTEGER, thumb TEXT NOT NULL DEFAULT '',
            created_at REAL NOT NULL);
        """)
        c.commit()


class ChannelIn(BaseModel):
    kind: str = Field(pattern="^(telegram|manual)$")
    name: str = Field(min_length=1, max_length=80)
    bot_token: str = Field(default="", max_length=120)
    chat: str = Field(default="", max_length=120)
    platform: str = Field(default="", max_length=40)     # для manual: Instagram, VK, Дзен…
    team_id: int | None = None


class PostIn(BaseModel):
    text: str = Field(default="", max_length=8000)
    media: list[str] = Field(default_factory=list, max_length=10)     # id из /media
    channels: list[int] = Field(default_factory=list, max_length=20)
    scheduled_at: float | None = None      # unix; None — черновик
    publish_now: bool = False
    design_id: int | None = None
    note: str = Field(default="", max_length=500)
    team_id: int | None = None             # только при создании


class MediaIn(BaseModel):
    data_url: str
    name: str = Field(default="", max_length=200)
    thumb: str = Field(default="", max_length=300_000)
    width: int | None = None
    height: int | None = None


TG_LIMIT_CAPTION = 1024
TG_LIMIT_TEXT = 4096


def _mask(tok: str) -> str:
    return (tok[:6] + "…" + tok[-4:]) if len(tok) > 12 else ""


def build_router(principal_dep: Callable, require_user: Callable, data_dir: Path,
                 notify: Callable[[int, str, str, str], None] | None = None) -> APIRouter:
    path = db_path(data_dir)
    init_db(path)
    media_dir = Path(os.environ.get("PLANNER_MEDIA_DIR") or (Path(path).parent / "planner_media"))
    media_dir.mkdir(parents=True, exist_ok=True)
    router = APIRouter(prefix="/api/v1/planner")
    tg_base = (os.environ.get("PLANNER_TG_API_BASE") or "https://api.telegram.org").rstrip("/")
    tg_relay_token = os.environ.get("PLANNER_TG_RELAY_TOKEN", "")

    def uid(p: dict) -> int:
        return require_user(p)

    # ---------------------------------------------------------------- права
    def scope_where(c: sqlite3.Connection, user_id: int, team: int | None) -> tuple[str, list]:
        if team:
            if not member_role(c, team, user_id):
                raise HTTPException(status_code=404, detail="Команда не найдена")
            return "team_id=?", [team]
        return "user_id=? AND team_id IS NULL", [user_id]

    def access(c: sqlite3.Connection, row: sqlite3.Row | None, user_id: int, need: str = "editor") -> sqlite3.Row:
        if row and row["user_id"] == user_id and not row["team_id"]:
            return row
        role = member_role(c, row["team_id"], user_id) if row and row["team_id"] else None
        if row and row["user_id"] == user_id and row["team_id"] and role:
            return row
        if not role:
            raise HTTPException(status_code=404, detail="Не найдено")
        if TEAM_RANK[role] < TEAM_RANK[need]:
            raise HTTPException(status_code=403, detail="В команде у вас роль «наблюдатель» — только просмотр")
        return row

    def team_ok(c: sqlite3.Connection, team_id: int | None, user_id: int) -> None:
        if team_id:
            role = member_role(c, team_id, user_id)
            if not role or TEAM_RANK[role] < TEAM_RANK["editor"]:
                raise HTTPException(status_code=403, detail="Нужна роль редактора в команде")

    # ---------------------------------------------------------------- каналы
    def ch_out(r: sqlite3.Row) -> dict[str, Any]:
        cfg = json.loads(r["config"] or "{}")
        return {"id": r["id"], "kind": r["kind"], "name": r["name"], "team_id": r["team_id"],
                "chat": cfg.get("chat", ""), "platform": cfg.get("platform", ""), "title": cfg.get("title", ""),
                "bot": cfg.get("bot_username", ""), "token_mask": _mask(cfg.get("bot_token", "")),
                "created_at": r["created_at"]}

    async def tg_call(token: str, method: str, data: dict | None = None, files: dict | None = None,
                      timeout: float = 60) -> dict:
        headers = {"X-Relay-Token": tg_relay_token} if tg_relay_token else {}
        async with httpx.AsyncClient(timeout=timeout) as cl:
            r = await cl.post(f"{tg_base}/bot{token}/{method}", data=data, files=files, headers=headers)
        try:
            j = r.json()
        except ValueError:
            raise RuntimeError(f"Telegram недоступен (HTTP {r.status_code})")
        if not j.get("ok"):
            raise RuntimeError(j.get("description") or f"Ошибка Telegram ({r.status_code})")
        return j["result"]

    @router.get("/channels")
    def list_channels(team: int | None = None, principal: dict = Depends(principal_dep)):
        user_id = uid(principal)
        with closing(connect(path)) as c:
            where, args = scope_where(c, user_id, team)
            rows = c.execute(f"SELECT * FROM planner_channels WHERE {where} ORDER BY created_at", args).fetchall()
        return {"channels": [ch_out(r) for r in rows]}

    @router.post("/channels")
    async def add_channel(p: ChannelIn, principal: dict = Depends(principal_dep)):
        user_id = uid(principal)
        cfg: dict[str, Any] = {}
        if p.kind == "telegram":
            tok, chat = p.bot_token.strip(), p.chat.strip()
            if not re.match(r"^\d{5,}:[A-Za-z0-9_-]{30,}$", tok):
                raise HTTPException(status_code=422, detail="Токен бота выглядит неверно — возьмите его у @BotFather")
            if not chat:
                raise HTTPException(status_code=422, detail="Укажите канал: @username или числовой id")
            if re.match(r"^(https?://)?t\.me/", chat):
                chat = "@" + chat.rstrip("/").split("/")[-1]
            try:
                me = await tg_call(tok, "getMe", timeout=20)
                info = await tg_call(tok, "getChat", {"chat_id": chat}, timeout=20)
                admins = await tg_call(tok, "getChatAdministrators", {"chat_id": chat}, timeout=20)
            except Exception as exc:  # noqa: BLE001
                raise HTTPException(status_code=422, detail=f"Не удалось проверить канал: {exc}") from exc
            if not any(a.get("user", {}).get("id") == me.get("id") for a in admins):
                raise HTTPException(status_code=422, detail="Бот не админ канала — добавьте его администратором с правом публикации")
            cfg = {"bot_token": tok, "chat": str(info.get("id")), "title": info.get("title") or chat,
                   "username": info.get("username", ""), "bot_username": me.get("username", "")}
        else:
            cfg = {"platform": p.platform.strip()[:40] or "Другое"}
        with closing(connect(path)) as c:
            team_ok(c, p.team_id, user_id)
            cur = c.execute("INSERT INTO planner_channels(user_id,team_id,kind,name,config,created_at) VALUES(?,?,?,?,?,?)",
                            (user_id, p.team_id or None, p.kind, p.name.strip(), json.dumps(cfg, ensure_ascii=False), time.time()))
            c.commit()
            return ch_out(c.execute("SELECT * FROM planner_channels WHERE id=?", (cur.lastrowid,)).fetchone())

    @router.delete("/channels/{channel_id}")
    def delete_channel(channel_id: int, principal: dict = Depends(principal_dep)):
        user_id = uid(principal)
        with closing(connect(path)) as c:
            access(c, c.execute("SELECT * FROM planner_channels WHERE id=?", (channel_id,)).fetchone(), user_id, "admin")
            c.execute("DELETE FROM planner_channels WHERE id=?", (channel_id,))
            c.commit()
        return {"ok": True}

    # ---------------------------------------------------------------- медиа
    @router.post("/media")
    def add_media(p: MediaIn, team: int | None = None, principal: dict = Depends(principal_dep)):
        user_id = uid(principal)
        m = re.match(r"^data:((image|video)/(png|jpeg|webp|gif|mp4|webm|quicktime));base64,(.+)$", p.data_url.strip(), re.S)
        if not m:
            raise HTTPException(status_code=422, detail="Нужна картинка (PNG/JPEG/WebP/GIF) или видео (MP4/WebM)")
        raw = base64.b64decode(m.group(4))
        if len(raw) > MEDIA_MAX:
            raise HTTPException(status_code=413, detail="Файл больше 60 МБ")
        mid = secrets.token_urlsafe(12)
        ext = {"png": "png", "jpeg": "jpg", "webp": "webp", "gif": "gif", "mp4": "mp4", "webm": "webm", "quicktime": "mov"}[m.group(3)]
        (media_dir / f"{mid}.{ext}").write_bytes(raw)
        with closing(connect(path)) as c:
            team_ok(c, team, user_id)
            c.execute("INSERT INTO planner_media(id,user_id,team_id,kind,mime,name,size,width,height,thumb,created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                      (mid, user_id, team or None, m.group(2), m.group(1), p.name[:200], len(raw), p.width, p.height,
                       p.thumb if p.thumb.startswith("data:image/") else "", time.time()))
            c.commit()
        return {"id": mid, "kind": m.group(2), "mime": m.group(1), "size": len(raw), "thumb": p.thumb, "width": p.width, "height": p.height}

    def media_file(r: sqlite3.Row) -> Path:
        ext = {"image/png": "png", "image/jpeg": "jpg", "image/webp": "webp", "image/gif": "gif",
               "video/mp4": "mp4", "video/webm": "webm", "video/quicktime": "mov"}[r["mime"]]
        return media_dir / f"{r['id']}.{ext}"

    @router.get("/media/{media_id}")
    def get_media(media_id: str, principal: dict = Depends(principal_dep)):
        user_id = uid(principal)
        with closing(connect(path)) as c:
            r = c.execute("SELECT * FROM planner_media WHERE id=?", (media_id,)).fetchone()
            access(c, r, user_id, "viewer")
        f = media_file(r)
        if not f.exists():
            raise HTTPException(status_code=404, detail="Файл не найден")
        return FileResponse(f, media_type=r["mime"], filename=r["name"] or f.name)

    def media_meta(c: sqlite3.Connection, ids: list[str]) -> list[dict]:
        out = []
        for i in ids:
            r = c.execute("SELECT id,kind,mime,name,size,width,height,thumb FROM planner_media WHERE id=?", (i,)).fetchone()
            if r:
                out.append(dict(r))
        return out

    # ---------------------------------------------------------------- посты
    def post_out(c: sqlite3.Connection, r: sqlite3.Row) -> dict[str, Any]:
        return {"id": r["id"], "team_id": r["team_id"], "user_id": r["user_id"], "text": r["text"],
                "media": media_meta(c, json.loads(r["media"] or "[]")), "channels": json.loads(r["channels"] or "[]"),
                "scheduled_at": r["scheduled_at"], "status": r["status"], "results": json.loads(r["results"] or "{}"),
                "attempts": r["attempts"], "next_try": r["next_try"], "design_id": r["design_id"], "note": r["note"],
                "created_at": r["created_at"], "updated_at": r["updated_at"], "updated_by": r["updated_by"]}

    def check_post(c: sqlite3.Connection, p: PostIn, team_id: int | None, user_id: int) -> None:
        for ch in p.channels:
            r = c.execute("SELECT user_id,team_id FROM planner_channels WHERE id=?", (ch,)).fetchone()
            if not r or (r["team_id"] or None) != (team_id or None) or (not team_id and r["user_id"] != user_id):
                raise HTTPException(status_code=422, detail="Канал не найден в этом пространстве")
        for m in p.media:
            r = c.execute("SELECT user_id,team_id FROM planner_media WHERE id=?", (m,)).fetchone()
            if not r or not ((r["team_id"] and r["team_id"] == team_id) or r["user_id"] == user_id):
                raise HTTPException(status_code=422, detail="Файл не найден")
        if (p.scheduled_at or p.publish_now) and not p.channels:
            raise HTTPException(status_code=422, detail="Выберите хотя бы один канал")
        if (p.scheduled_at or p.publish_now) and not p.text.strip() and not p.media:
            raise HTTPException(status_code=422, detail="Пустой пост — добавьте текст или файл")

    def status_for(p: PostIn) -> tuple[str, float | None]:
        if p.publish_now:
            return "scheduled", time.time()
        if p.scheduled_at:
            return "scheduled", p.scheduled_at
        return "draft", None

    @router.get("/posts")
    def list_posts(team: int | None = None, start: float | None = None, end: float | None = None,
                   principal: dict = Depends(principal_dep)):
        user_id = uid(principal)
        with closing(connect(path)) as c:
            where, args = scope_where(c, user_id, team)
            if start is not None and end is not None:
                where += " AND (scheduled_at IS NULL OR (scheduled_at>=? AND scheduled_at<?))"
                args += [start, end]
            rows = c.execute(f"SELECT * FROM planner_posts WHERE {where} ORDER BY COALESCE(scheduled_at, 9e18), id DESC LIMIT 1000", args).fetchall()
            return {"posts": [post_out(c, r) for r in rows]}

    @router.get("/stats")
    def stats(principal: dict = Depends(principal_dep)):
        user_id = uid(principal)
        with closing(connect(path)) as c:
            n = lambda st: c.execute("SELECT COUNT(*) FROM planner_posts WHERE user_id=? AND status=?", (user_id, st)).fetchone()[0]
            return {"scheduled": n("scheduled"), "published": n("published"), "failed": n("failed"), "drafts": n("draft")}

    @router.post("/posts")
    def create_post(p: PostIn, principal: dict = Depends(principal_dep)):
        user_id, now = uid(principal), time.time()
        with closing(connect(path)) as c:
            team_ok(c, p.team_id, user_id)
            check_post(c, p, p.team_id, user_id)
            st, at = status_for(p)
            cur = c.execute("INSERT INTO planner_posts(user_id,team_id,text,media,channels,scheduled_at,status,design_id,note,created_at,updated_at,updated_by) "
                            "VALUES(?,?,?,?,?,?,?,?,?,?,?,?)", (user_id, p.team_id or None, p.text, json.dumps(p.media), json.dumps(p.channels),
                                                                 at, st, p.design_id, p.note, now, now, user_id))
            c.commit()
            r = c.execute("SELECT * FROM planner_posts WHERE id=?", (cur.lastrowid,)).fetchone()
            out = post_out(c, r)
        if p.publish_now:
            wake.set()
        return out

    @router.put("/posts/{post_id}")
    def update_post(post_id: int, p: PostIn, principal: dict = Depends(principal_dep)):
        user_id = uid(principal)
        with closing(connect(path)) as c:
            r = access(c, c.execute("SELECT * FROM planner_posts WHERE id=?", (post_id,)).fetchone(), user_id)
            if r["status"] in ("publishing", "published"):
                raise HTTPException(status_code=409, detail="Пост уже опубликован — сделайте копию")
            check_post(c, p, r["team_id"], user_id)
            st, at = status_for(p)
            c.execute("UPDATE planner_posts SET text=?,media=?,channels=?,scheduled_at=?,status=?,note=?,attempts=0,next_try=NULL,"
                      "results='{}',updated_at=?,updated_by=? WHERE id=?",
                      (p.text, json.dumps(p.media), json.dumps(p.channels), at, st, p.note, time.time(), user_id, post_id))
            c.commit()
            out = post_out(c, c.execute("SELECT * FROM planner_posts WHERE id=?", (post_id,)).fetchone())
        if p.publish_now:
            wake.set()
        return out

    class MoveIn(BaseModel):
        scheduled_at: float

    @router.patch("/posts/{post_id}/time")
    def move_post(post_id: int, p: MoveIn, principal: dict = Depends(principal_dep)):
        user_id = uid(principal)
        with closing(connect(path)) as c:
            r = access(c, c.execute("SELECT * FROM planner_posts WHERE id=?", (post_id,)).fetchone(), user_id)
            if r["status"] in ("publishing", "published"):
                raise HTTPException(status_code=409, detail="Опубликованный пост не переносится")
            st = "scheduled" if r["status"] in ("scheduled", "failed", "due", "partial") and json.loads(r["channels"]) else r["status"]
            c.execute("UPDATE planner_posts SET scheduled_at=?, status=?, attempts=0, next_try=NULL, updated_at=?, updated_by=? WHERE id=?",
                      (p.scheduled_at, st if r["status"] != "draft" else "draft", time.time(), user_id, post_id))
            c.commit()
            return post_out(c, c.execute("SELECT * FROM planner_posts WHERE id=?", (post_id,)).fetchone())

    @router.post("/posts/{post_id}/done")
    def mark_done(post_id: int, principal: dict = Depends(principal_dep)):
        """Ручной канал: человек выложил сам."""
        user_id = uid(principal)
        with closing(connect(path)) as c:
            r = access(c, c.execute("SELECT * FROM planner_posts WHERE id=?", (post_id,)).fetchone(), user_id)
            res = json.loads(r["results"] or "{}")
            for k, v in res.items():
                if v.get("manual") and not v.get("ok"):
                    v.update(ok=True, at=time.time(), by=user_id)
            all_ok = res and all(v.get("ok") for v in res.values())
            c.execute("UPDATE planner_posts SET results=?, status=?, updated_at=? WHERE id=?",
                      (json.dumps(res, ensure_ascii=False), "published" if all_ok else r["status"], time.time(), post_id))
            c.commit()
            return post_out(c, c.execute("SELECT * FROM planner_posts WHERE id=?", (post_id,)).fetchone())

    @router.delete("/posts/{post_id}")
    def delete_post(post_id: int, principal: dict = Depends(principal_dep)):
        user_id = uid(principal)
        with closing(connect(path)) as c:
            access(c, c.execute("SELECT * FROM planner_posts WHERE id=?", (post_id,)).fetchone(), user_id)
            c.execute("DELETE FROM planner_posts WHERE id=?", (post_id,))
            c.commit()
        return {"ok": True}

    # ---------------------------------------------------------------- отправка
    async def send_telegram(cfg: dict, text: str, media: list[sqlite3.Row]) -> dict:
        tok, chat = cfg["bot_token"], cfg["chat"]
        text = text.strip()
        if not media:
            if not text:
                raise RuntimeError("Пустой пост")
            parts = [text[i:i + TG_LIMIT_TEXT] for i in range(0, len(text), TG_LIMIT_TEXT)]
            first = None
            for part in parts:
                res = await tg_call(tok, "sendMessage", {"chat_id": chat, "text": part})
                first = first or res
            return {"message_id": first.get("message_id")}
        cap = text if len(text) <= TG_LIMIT_CAPTION else ""
        files = [(m, media_file(m)) for m in media]
        if len(files) == 1:
            m, f = files[0]
            method, field = ("sendVideo", "video") if m["kind"] == "video" else ("sendPhoto", "photo")
            if m["mime"] == "image/gif":
                method, field = "sendAnimation", "animation"
            data = {"chat_id": chat}
            if cap:
                data["caption"] = cap
            res = await tg_call(tok, method, data, {field: (f.name, f.read_bytes(), m["mime"])}, timeout=180)
        else:
            group, up = [], {}
            for i, (m, f) in enumerate(files[:10]):
                key = f"f{i}"
                item = {"type": "video" if m["kind"] == "video" else "photo", "media": f"attach://{key}"}
                if i == 0 and cap:
                    item["caption"] = cap
                group.append(item)
                up[key] = (f.name, f.read_bytes(), m["mime"])
            res = await tg_call(tok, "sendMediaGroup", {"chat_id": chat, "media": json.dumps(group)}, up, timeout=240)
            res = res[0] if isinstance(res, list) and res else {}
        if text and not cap:  # длинный текст — отдельным сообщением после медиа
            await tg_call(tok, "sendMessage", {"chat_id": chat, "text": text[:TG_LIMIT_TEXT]})
        return {"message_id": res.get("message_id")}

    def post_link(cfg: dict, msg_id: Any) -> str:
        u = cfg.get("username")
        return f"https://t.me/{u}/{msg_id}" if u and msg_id else ""

    async def publish(post_id: int) -> None:
        with closing(connect(path)) as c:
            r = c.execute("SELECT * FROM planner_posts WHERE id=?", (post_id,)).fetchone()
            if not r or r["status"] != "publishing":
                return
            res = json.loads(r["results"] or "{}")
            media = [m for m in (c.execute("SELECT * FROM planner_media WHERE id=?", (i,)).fetchone()
                                 for i in json.loads(r["media"] or "[]")) if m]
            chans = {ch["id"]: ch for ch in (c.execute("SELECT * FROM planner_channels WHERE id=?", (i,)).fetchone()
                                              for i in json.loads(r["channels"] or "[]")) if ch}
        for cid, ch in chans.items():
            key = str(cid)
            if res.get(key, {}).get("ok"):
                continue
            cfg = json.loads(ch["config"] or "{}")
            if ch["kind"] == "manual":
                res[key] = {"ok": False, "manual": True, "at": time.time(), "name": ch["name"]}
                continue
            try:
                out = await send_telegram(cfg, r["text"], media)
                res[key] = {"ok": True, "at": time.time(), "message_id": out.get("message_id"),
                            "link": post_link(cfg, out.get("message_id")), "name": ch["name"]}
            except Exception as exc:  # noqa: BLE001
                res[key] = {"ok": False, "error": str(exc)[:300], "at": time.time(), "name": ch["name"]}
        auto = [v for v in res.values() if not v.get("manual")]
        manual = [v for v in res.values() if v.get("manual")]
        failed = [v for v in auto if not v.get("ok")]
        attempts = r["attempts"] + 1
        if failed and attempts <= len(RETRY_MINUTES):
            status, next_try = "scheduled", time.time() + RETRY_MINUTES[attempts - 1] * 60
        elif failed:
            status, next_try = ("partial" if any(v.get("ok") for v in auto) else "failed"), None
        elif manual and not all(v.get("ok") for v in manual):
            status, next_try = "due", None
        else:
            status, next_try = "published", None
        with closing(connect(path)) as c:
            c.execute("UPDATE planner_posts SET status=?, results=?, attempts=?, next_try=?, updated_at=? WHERE id=?",
                      (status, json.dumps(res, ensure_ascii=False), attempts, next_try, time.time(), post_id))
            c.commit()
        if notify:
            preview = (r["text"] or "Пост с медиа").strip().replace("\n", " ")[:80]
            if status == "failed" or status == "partial":
                err = "; ".join(f"{v.get('name')}: {v.get('error')}" for v in failed)[:300]
                notify(r["user_id"], "Пост не опубликован", f"«{preview}» — {err}", "posts-feed")
            elif status == "due":
                names = ", ".join(v.get("name", "") for v in manual)
                notify(r["user_id"], "Пора выложить пост вручную", f"{names}: «{preview}»", "posts-feed")

    wake = asyncio.Event()

    async def scheduler() -> None:
        while True:
            try:
                now = time.time()
                with closing(connect(path)) as c:
                    ids = [x["id"] for x in c.execute(
                        "SELECT id FROM planner_posts WHERE status='scheduled' AND scheduled_at<=? AND (next_try IS NULL OR next_try<=?) "
                        "ORDER BY scheduled_at LIMIT 20", (now, now))]
                    for i in ids:
                        c.execute("UPDATE planner_posts SET status='publishing' WHERE id=? AND status='scheduled'", (i,))
                    # зависшие «publishing» (сервис перезапускали посреди отправки) — вернуть в очередь
                    c.execute("UPDATE planner_posts SET status='scheduled' WHERE status='publishing' AND updated_at<?", (now - 900,))
                    c.commit()
                for i in ids:
                    await publish(i)
            except Exception as exc:  # noqa: BLE001
                print(f"[planner] ошибка планировщика: {exc}", flush=True)
            try:
                await asyncio.wait_for(wake.wait(), timeout=TICK)
            except asyncio.TimeoutError:
                pass
            wake.clear()

    @router.on_event("startup")
    async def _start() -> None:
        if os.environ.get("PLANNER_SCHEDULER", "1") != "0":
            asyncio.get_event_loop().create_task(scheduler())

    return router
