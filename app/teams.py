"""Команды ЛК (2026-11-23): общие дизайны Студии, бренд-кит, планировщик публикаций.

Хранится в той же базе, что и Студия (studio.sqlite3): teams, team_members, team_invites.
Пользователи — из основной базы ЛК (users), их передаёт main.py через `users_lookup`.
Роли: owner > admin > editor > viewer. Владелец один; удалить команду может только он.
"""
from __future__ import annotations

import json
import secrets
import sqlite3
import time
from contextlib import closing
from pathlib import Path
from typing import Any, Callable

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field

ROLES = ("owner", "admin", "editor", "viewer")
RANK = {r: i for i, r in enumerate(reversed(ROLES))}  # viewer 0 … owner 3
INVITE_TTL = 7 * 24 * 3600


def db_path(data_dir: Path) -> Path:
    import os
    return Path(os.environ.get("STUDIO_SQLITE_PATH") or (data_dir / "studio.sqlite3"))


def connect(path: Path) -> sqlite3.Connection:
    c = sqlite3.connect(path, timeout=15)
    c.row_factory = sqlite3.Row
    return c


def init_db(path: Path) -> None:
    with closing(connect(path)) as c:
        c.execute("PRAGMA journal_mode=WAL")
        c.executescript("""
        CREATE TABLE IF NOT EXISTS teams (
            id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL, owner_id INTEGER NOT NULL,
            brand TEXT NOT NULL DEFAULT '{}', created_at REAL NOT NULL);
        CREATE TABLE IF NOT EXISTS team_members (
            team_id INTEGER NOT NULL, user_id INTEGER NOT NULL, role TEXT NOT NULL, joined_at REAL NOT NULL,
            PRIMARY KEY(team_id, user_id));
        CREATE INDEX IF NOT EXISTS ix_team_members_user ON team_members(user_id);
        CREATE TABLE IF NOT EXISTS team_invites (
            token TEXT PRIMARY KEY, team_id INTEGER NOT NULL, role TEXT NOT NULL, created_by INTEGER NOT NULL,
            created_at REAL NOT NULL, expires_at REAL NOT NULL, uses INTEGER NOT NULL DEFAULT 0);
        """)
        c.commit()


def member_role(c: sqlite3.Connection, team_id: int, user_id: int) -> str | None:
    r = c.execute("SELECT role FROM team_members WHERE team_id=? AND user_id=?", (team_id, user_id)).fetchone()
    return r["role"] if r else None


def need_role(c: sqlite3.Connection, team_id: int, user_id: int, at_least: str) -> str:
    """Роль пользователя в команде; 404, если не участник (не раскрываем чужие команды), 403 — мало прав."""
    role = member_role(c, team_id, user_id)
    if not role:
        raise HTTPException(status_code=404, detail="Команда не найдена")
    if RANK[role] < RANK[at_least]:
        raise HTTPException(status_code=403, detail="Недостаточно прав в команде")
    return role


def user_team_ids(c: sqlite3.Connection, user_id: int) -> list[int]:
    return [r["team_id"] for r in c.execute("SELECT team_id FROM team_members WHERE user_id=?", (user_id,))]


class TeamIn(BaseModel):
    name: str = Field(min_length=1, max_length=80)


class MemberIn(BaseModel):
    email: str = Field(min_length=3, max_length=255)
    role: str = Field(default="editor")


class RoleIn(BaseModel):
    role: str


class InviteIn(BaseModel):
    role: str = Field(default="editor")


class BrandIn(BaseModel):
    brand: dict[str, Any] = Field(default_factory=dict)


def build_router(principal_dep: Callable, require_user: Callable, data_dir: Path,
                 users_lookup: Callable[[list[int] | None, str | None], list[dict]],
                 notify: Callable[[int, str, str, str], None] | None = None) -> APIRouter:
    """users_lookup(ids, email) → [{id,email,name}] из основной базы ЛК; notify(user_id, title, text, link)."""
    path = db_path(data_dir)
    init_db(path)
    router = APIRouter(prefix="/api/v1/teams")

    def uid(p: dict) -> int:
        return require_user(p)

    def check_role(role: str, allow_owner: bool = False) -> str:
        if role not in ROLES or (role == "owner" and not allow_owner):
            raise HTTPException(status_code=422, detail="Роль: admin, editor или viewer")
        return role

    def team_out(c: sqlite3.Connection, t: sqlite3.Row, me: int) -> dict[str, Any]:
        n = c.execute("SELECT COUNT(*) FROM team_members WHERE team_id=?", (t["id"],)).fetchone()[0]
        return {"id": t["id"], "name": t["name"], "role": member_role(c, t["id"], me), "members": n,
                "owner_id": t["owner_id"], "created_at": t["created_at"]}

    @router.get("")
    def my_teams(principal: dict = Depends(principal_dep)):
        me = uid(principal)
        with closing(connect(path)) as c:
            rows = c.execute("SELECT t.* FROM teams t JOIN team_members m ON m.team_id=t.id WHERE m.user_id=? "
                             "ORDER BY t.created_at", (me,)).fetchall()
            return {"teams": [team_out(c, t, me) for t in rows]}

    @router.post("")
    def create_team(p: TeamIn, principal: dict = Depends(principal_dep)):
        me = uid(principal)
        now = time.time()
        with closing(connect(path)) as c:
            if c.execute("SELECT COUNT(*) FROM teams WHERE owner_id=?", (me,)).fetchone()[0] >= 10:
                raise HTTPException(status_code=429, detail="Не больше 10 своих команд")
            cur = c.execute("INSERT INTO teams(name,owner_id,created_at) VALUES(?,?,?)", (p.name.strip(), me, now))
            c.execute("INSERT INTO team_members(team_id,user_id,role,joined_at) VALUES(?,?,?,?)", (cur.lastrowid, me, "owner", now))
            c.commit()
            t = c.execute("SELECT * FROM teams WHERE id=?", (cur.lastrowid,)).fetchone()
            return team_out(c, t, me)

    @router.get("/{team_id}")
    def get_team(team_id: int, principal: dict = Depends(principal_dep)):
        me = uid(principal)
        with closing(connect(path)) as c:
            role = need_role(c, team_id, me, "viewer")
            t = c.execute("SELECT * FROM teams WHERE id=?", (team_id,)).fetchone()
            ms = c.execute("SELECT user_id,role,joined_at FROM team_members WHERE team_id=? ORDER BY joined_at", (team_id,)).fetchall()
            invites = []
            if RANK[role] >= RANK["admin"]:
                c.execute("DELETE FROM team_invites WHERE expires_at<?", (time.time(),))
                c.commit()
                invites = [dict(r) for r in c.execute("SELECT token,role,created_at,expires_at,uses FROM team_invites WHERE team_id=? "
                                                      "ORDER BY created_at DESC", (team_id,))]
            out = team_out(c, t, me)
        info = {u["id"]: u for u in users_lookup([m["user_id"] for m in ms], None)}
        out["members"] = [{"user_id": m["user_id"], "role": m["role"], "joined_at": m["joined_at"],
                           "email": info.get(m["user_id"], {}).get("email", ""), "name": info.get(m["user_id"], {}).get("name", "")}
                          for m in ms]
        out["invites"] = invites
        out["brand"] = json.loads(t["brand"] or "{}")
        return out

    @router.patch("/{team_id}")
    def rename_team(team_id: int, p: TeamIn, principal: dict = Depends(principal_dep)):
        me = uid(principal)
        with closing(connect(path)) as c:
            need_role(c, team_id, me, "admin")
            c.execute("UPDATE teams SET name=? WHERE id=?", (p.name.strip(), team_id))
            c.commit()
        return {"ok": True}

    @router.delete("/{team_id}")
    def delete_team(team_id: int, principal: dict = Depends(principal_dep)):
        me = uid(principal)
        with closing(connect(path)) as c:
            need_role(c, team_id, me, "owner")
            # общие дизайны возвращаются владельцам-авторам как личные
            cols = {r[1] for r in c.execute("PRAGMA table_info(studio_designs)")}
            if "team_id" in cols:
                c.execute("UPDATE studio_designs SET team_id=NULL WHERE team_id=?", (team_id,))
            for tbl in ("team_members", "team_invites"):
                c.execute(f"DELETE FROM {tbl} WHERE team_id=?", (team_id,))
            c.execute("DELETE FROM teams WHERE id=?", (team_id,))
            c.commit()
        return {"ok": True}

    @router.post("/{team_id}/members")
    def add_member(team_id: int, p: MemberIn, principal: dict = Depends(principal_dep)):
        me = uid(principal)
        role = check_role(p.role)
        found = users_lookup(None, p.email.strip().casefold())
        if not found:
            raise HTTPException(status_code=404, detail="Пользователя с таким email нет в ЛК — пришлите ему ссылку-приглашение после того, как администратор создаст доступ")
        u = found[0]
        with closing(connect(path)) as c:
            need_role(c, team_id, me, "admin")
            if member_role(c, team_id, u["id"]):
                raise HTTPException(status_code=409, detail="Уже в команде")
            c.execute("INSERT INTO team_members(team_id,user_id,role,joined_at) VALUES(?,?,?,?)", (team_id, u["id"], role, time.time()))
            c.commit()
            name = c.execute("SELECT name FROM teams WHERE id=?", (team_id,)).fetchone()["name"]
        if notify:
            notify(u["id"], f"Вас добавили в команду «{name}»", "Общие дизайны и публикации команды теперь доступны вам.", "team-members")
        return {"ok": True, "user_id": u["id"]}

    @router.patch("/{team_id}/members/{user_id}")
    def set_role(team_id: int, user_id: int, p: RoleIn, principal: dict = Depends(principal_dep)):
        me = uid(principal)
        role = check_role(p.role, allow_owner=True)
        with closing(connect(path)) as c:
            my = need_role(c, team_id, me, "admin")
            cur = member_role(c, team_id, user_id)
            if not cur:
                raise HTTPException(status_code=404, detail="Участник не найден")
            if cur == "owner" or role == "owner":
                # передача владения — только владельцем; прежний владелец становится админом
                if my != "owner" or role != "owner" or user_id == me:
                    raise HTTPException(status_code=403, detail="Роль владельца меняет только владелец (передачей)")
                c.execute("UPDATE team_members SET role='admin' WHERE team_id=? AND user_id=?", (team_id, me))
                c.execute("UPDATE teams SET owner_id=? WHERE id=?", (user_id, team_id))
            c.execute("UPDATE team_members SET role=? WHERE team_id=? AND user_id=?", (role, team_id, user_id))
            c.commit()
        return {"ok": True}

    @router.delete("/{team_id}/members/{user_id}")
    def remove_member(team_id: int, user_id: int, principal: dict = Depends(principal_dep)):
        me = uid(principal)
        with closing(connect(path)) as c:
            if user_id != me:
                need_role(c, team_id, me, "admin")
            cur = member_role(c, team_id, user_id)
            if not cur:
                raise HTTPException(status_code=404, detail="Участник не найден")
            if cur == "owner":
                raise HTTPException(status_code=409, detail="Владелец не может выйти — сначала передайте владение или удалите команду")
            c.execute("DELETE FROM team_members WHERE team_id=? AND user_id=?", (team_id, user_id))
            c.commit()
        return {"ok": True}

    @router.post("/{team_id}/invites")
    def make_invite(team_id: int, p: InviteIn, principal: dict = Depends(principal_dep)):
        me = uid(principal)
        role = check_role(p.role)
        now = time.time()
        with closing(connect(path)) as c:
            need_role(c, team_id, me, "admin")
            token = secrets.token_urlsafe(18)
            c.execute("INSERT INTO team_invites(token,team_id,role,created_by,created_at,expires_at) VALUES(?,?,?,?,?,?)",
                      (token, team_id, role, me, now, now + INVITE_TTL))
            c.commit()
        return {"token": token, "role": role, "expires_at": now + INVITE_TTL}

    @router.delete("/{team_id}/invites/{token}")
    def drop_invite(team_id: int, token: str, principal: dict = Depends(principal_dep)):
        me = uid(principal)
        with closing(connect(path)) as c:
            need_role(c, team_id, me, "admin")
            c.execute("DELETE FROM team_invites WHERE team_id=? AND token=?", (team_id, token))
            c.commit()
        return {"ok": True}

    def invite_row(c: sqlite3.Connection, token: str) -> sqlite3.Row:
        r = c.execute("SELECT i.*, t.name AS team_name FROM team_invites i JOIN teams t ON t.id=i.team_id WHERE i.token=?", (token,)).fetchone()
        if not r or r["expires_at"] < time.time():
            raise HTTPException(status_code=404, detail="Приглашение не найдено или истекло")
        return r

    @router.get("/join/{token}")
    def invite_info(token: str, principal: dict = Depends(principal_dep)):
        me = uid(principal)
        with closing(connect(path)) as c:
            r = invite_row(c, token)
            return {"team_id": r["team_id"], "team_name": r["team_name"], "role": r["role"],
                    "already": bool(member_role(c, r["team_id"], me))}

    @router.post("/join/{token}")
    def join(token: str, principal: dict = Depends(principal_dep)):
        me = uid(principal)
        with closing(connect(path)) as c:
            r = invite_row(c, token)
            if not member_role(c, r["team_id"], me):
                c.execute("INSERT INTO team_members(team_id,user_id,role,joined_at) VALUES(?,?,?,?)", (r["team_id"], me, r["role"], time.time()))
                c.execute("UPDATE team_invites SET uses=uses+1 WHERE token=?", (token,))
                c.commit()
        if notify:
            for a in users_admins(r["team_id"]):
                if a != me:
                    notify(a, f"Новый участник в команде «{r['team_name']}»", "Присоединился по ссылке-приглашению.", "team-members")
        return {"team_id": r["team_id"], "team_name": r["team_name"]}

    def users_admins(team_id: int) -> list[int]:
        with closing(connect(path)) as c:
            return [x["user_id"] for x in c.execute("SELECT user_id FROM team_members WHERE team_id=? AND role IN ('owner','admin')", (team_id,))]

    @router.put("/{team_id}/brand")
    def put_brand(team_id: int, p: BrandIn, principal: dict = Depends(principal_dep)):
        me = uid(principal)
        data = json.dumps(p.brand, ensure_ascii=False)
        if len(data) > 8_000_000:
            raise HTTPException(status_code=413, detail="Бренд-кит слишком большой — уменьшите логотипы")
        with closing(connect(path)) as c:
            need_role(c, team_id, me, "admin")
            c.execute("UPDATE teams SET brand=? WHERE id=?", (data, team_id))
            c.commit()
        return {"ok": True}

    return router
