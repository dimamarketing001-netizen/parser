"""Вкладка «Консоль» в ЛК (2026-11-23) — служебные функции только для роли superuser («Создатель»).

Заменяет старую серверную консоль (/login, /dashboard, /runs/…, /admin/users, /api/admin/*), которая
удалена: работаем только в ЛК (/app). Здесь:
  * обзор: запуски, очередь, креативы, воркеры, фоновые очереди (Gemini, эмбеддинги, таксономия);
  * капча: живой снимок Chrome воркера по запуску, клики/текст/клавиши (browser_sessions);
  * пользователи платформы: список, создание (с временным паролем), роль, отключение/включение,
    сброс пароля.
Роли пользователей платформы (users.role): superuser — создатель, всё + Консоль; admin — вся база в
библиотеке/аналитике; marketer — свои запуски. Не путать с ролями в командах (teams.py: owner/admin/…).
"""
from __future__ import annotations

import json
import secrets
from contextlib import closing
from typing import Any, Callable

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field

SUPERUSER = "superuser"
PLATFORM_ROLES = ("superuser", "admin", "marketer")
ROLE_TITLES = {"superuser": "Создатель", "admin": "Администратор", "marketer": "Маркетолог"}


def is_superuser(principal: dict) -> bool:
    return principal.get("kind") == "user" and principal.get("role") == SUPERUSER


def is_platform_admin(principal: dict) -> bool:
    """Видит всю базу (как прежний admin). superuser включает admin."""
    return principal.get("kind") != "user" or principal.get("role") in ("admin", SUPERUSER)


class ConsoleBrowserCommand(BaseModel):
    action: str = Field(pattern="^(click|key|text|scroll|refresh)$")
    x_ratio: float | None = None
    y_ratio: float | None = None
    key: str = Field(default="", max_length=20)
    text: str = Field(default="", max_length=2000)
    scroll_y: int = 0


class ConsoleUserIn(BaseModel):
    email: str = Field(min_length=3, max_length=255)
    role: str = Field(default="marketer", pattern="^(superuser|admin|marketer)$")
    name: str = Field(default="", max_length=120)


class ConsoleUserPatch(BaseModel):
    role: str | None = Field(default=None, pattern="^(superuser|admin|marketer)$")
    is_active: bool | None = None


def build_router(principal_dep: Callable, connect: Callable, hash_password: Callable, now_iso: Callable,
                 status_extra: Callable[[], dict] | None = None) -> APIRouter:
    r = APIRouter(prefix="/api/v1/console", tags=["console"])

    def su(principal: dict = Depends(principal_dep)) -> dict:
        if not is_superuser(principal):
            raise HTTPException(status_code=403, detail="Консоль доступна только создателю (роль superuser)")
        return principal

    @r.get("/overview")
    def overview(principal: dict = Depends(su)):
        with closing(connect()) as db:
            one = lambda q, a=(): db.execute(q, a).fetchone()[0]
            jobs = {
                "total": one("SELECT COUNT(*) FROM jobs"),
                "queued": one("SELECT COUNT(*) FROM jobs WHERE status='queued'"),
                "running": one("SELECT COUNT(*) FROM jobs WHERE status='running'"),
                "failed_24h": one("SELECT COUNT(*) FROM jobs WHERE status='failed' AND finished_at>=datetime('now','-1 day')"),
            }
            active = [dict(x) for x in db.execute(
                "SELECT j.id,j.status,j.city,j.key_text,j.variant,j.started_at,j.created_at,j.total_creatives,j.worker_id,"
                "b.active AS browser_active,b.captcha_required,b.updated_at AS browser_updated_at "
                "FROM jobs j LEFT JOIN browser_sessions b ON b.job_id=j.id "
                "WHERE j.status IN ('queued','running') ORDER BY j.id DESC LIMIT 50").fetchall()]
            recent = [dict(x) for x in db.execute(
                "SELECT id,status,city,key_text,variant,created_at,finished_at,total_creatives,substr(error_text,1,200) AS error_text "
                "FROM jobs ORDER BY id DESC LIMIT 20").fetchall()]
            workers = [dict(x) for x in db.execute(
                "SELECT worker_id,hostname,version,last_seen,state,current_job_id,last_error FROM workers ORDER BY last_seen DESC").fetchall()]
            captcha = [dict(x) for x in db.execute(
                "SELECT b.job_id,b.url,b.title,b.updated_at,j.key_text,j.city FROM browser_sessions b JOIN jobs j ON j.id=b.job_id "
                # только идущие запуски: старые сессии завершённых запусков остаются active=1 в таблице
                "WHERE b.active=1 AND b.captcha_required=1 AND j.status='running' ORDER BY b.updated_at DESC").fetchall()]
            creatives = {
                "total": one("SELECT COUNT(*) FROM creatives"),
                "occurrences": one("SELECT COUNT(*) FROM occurrences"),
                "ai_intent_pending": one("SELECT COUNT(*) FROM creatives WHERE ai_intent IS NULL"),
                "taxonomy_pending": one("SELECT COUNT(*) FROM creatives WHERE ai_intent_main IS NULL"),
                "embeddings_pending": one(
                    "SELECT COUNT(*) FROM creatives c LEFT JOIN creative_embeddings ce ON ce.creative_id=c.id "
                    "WHERE ce.creative_id IS NULL AND (COALESCE(c.title,'')<>'' OR COALESCE(c.description,'')<>'')"),
            }
            users = {"total": one("SELECT COUNT(*) FROM users"), "active": one("SELECT COUNT(*) FROM users WHERE is_active=1")}
            schedules = one("SELECT COUNT(*) FROM scan_schedules WHERE status='active'")
        out = {"jobs": jobs, "active": active, "recent": recent, "workers": workers, "captcha": captcha,
               "creatives": creatives, "users": users, "schedules_active": schedules}
        if status_extra:
            out.update(status_extra())
        return out

    @r.get("/jobs/{job_id}/browser")
    def browser_state(job_id: int, principal: dict = Depends(su)):
        with closing(connect()) as db:
            row = db.execute("SELECT * FROM browser_sessions WHERE job_id=?", (job_id,)).fetchone()
        if not row:
            return {"active": False, "captcha_required": False}
        return {
            "active": bool(row["active"]), "captcha_required": bool(row["captcha_required"]),
            "url": row["url"], "title": row["title"], "screenshot_b64": row["screenshot_b64"],
            "viewport_width": row["viewport_width"], "viewport_height": row["viewport_height"],
            "updated_at": row["updated_at"],
        }

    @r.post("/jobs/{job_id}/browser")
    def browser_command(job_id: int, payload: ConsoleBrowserCommand, principal: dict = Depends(su)):
        command: dict[str, Any] = {"action": payload.action}
        if payload.action == "click":
            if payload.x_ratio is None or payload.y_ratio is None:
                raise HTTPException(status_code=400, detail="x_ratio/y_ratio обязательны для click")
            command.update({"x_ratio": min(max(payload.x_ratio, 0.0), 1.0), "y_ratio": min(max(payload.y_ratio, 0.0), 1.0)})
        elif payload.action == "key":
            command["key"] = payload.key
        elif payload.action == "text":
            command["text"] = payload.text
        elif payload.action == "scroll":
            command["scroll_y"] = payload.scroll_y
        with closing(connect()) as db:
            row = db.execute("SELECT job_id FROM browser_sessions WHERE job_id=?", (job_id,)).fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="Браузерная сессия для этого запуска не активна")
            db.execute("UPDATE browser_sessions SET pending_command_json=? WHERE job_id=?", (json.dumps(command), job_id))
            db.commit()
        return {"ok": True}

    @r.get("/users")
    def users(principal: dict = Depends(su)):
        with closing(connect()) as db:
            rows = db.execute(
                "SELECT u.id,u.email,u.name,u.role,u.is_active,u.created_at,u.last_login_at,"
                "(SELECT COUNT(*) FROM jobs j WHERE j.owner_user_id=u.id) AS runs "
                "FROM users u ORDER BY u.id").fetchall()
        return {"items": [dict(x) | {"role_title": ROLE_TITLES.get(x["role"], x["role"])} for x in rows],
                "roles": [{"role": k, "title": v} for k, v in ROLE_TITLES.items()]}

    @r.post("/users")
    def create_user(payload: ConsoleUserIn, principal: dict = Depends(su)):
        email = payload.email.strip().casefold()
        if "@" not in email:
            raise HTTPException(status_code=422, detail="Нужен email")
        password = secrets.token_urlsafe(9)
        with closing(connect()) as db:
            if db.execute("SELECT 1 FROM users WHERE email=?", (email,)).fetchone():
                raise HTTPException(status_code=409, detail=f"Email {email} уже занят")
            cur = db.execute("INSERT INTO users(email,password_hash,role,is_active,created_at,name) VALUES(?,?,?,1,?,?)",
                             (email, hash_password(password), payload.role, now_iso(), payload.name.strip() or None))
            db.commit()
        return {"id": cur.lastrowid, "email": email, "role": payload.role, "password": password}

    def _guard_last_superuser(db, user_id: int, new_role: str | None, new_active: bool | None) -> None:
        row = db.execute("SELECT role FROM users WHERE id=?", (user_id,)).fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Пользователь не найден")
        losing = row["role"] == SUPERUSER and ((new_role and new_role != SUPERUSER) or new_active is False)
        if losing and db.execute("SELECT COUNT(*) FROM users WHERE role=? AND is_active=1", (SUPERUSER,)).fetchone()[0] <= 1:
            raise HTTPException(status_code=409, detail="Нельзя оставить платформу без создателя")

    @r.patch("/users/{user_id}")
    def patch_user(user_id: int, payload: ConsoleUserPatch, principal: dict = Depends(su)):
        with closing(connect()) as db:
            _guard_last_superuser(db, user_id, payload.role, payload.is_active)
            if payload.role:
                db.execute("UPDATE users SET role=? WHERE id=?", (payload.role, user_id))
            if payload.is_active is not None:
                db.execute("UPDATE users SET is_active=? WHERE id=?", (1 if payload.is_active else 0, user_id))
                if not payload.is_active:
                    db.execute("DELETE FROM user_tokens WHERE user_id=?", (user_id,))
            db.commit()
        return {"ok": True}

    @r.post("/users/{user_id}/reset-password")
    def reset_password(user_id: int, principal: dict = Depends(su)):
        password = secrets.token_urlsafe(9)
        with closing(connect()) as db:
            if not db.execute("SELECT 1 FROM users WHERE id=?", (user_id,)).fetchone():
                raise HTTPException(status_code=404, detail="Пользователь не найден")
            db.execute("UPDATE users SET password_hash=? WHERE id=?", (hash_password(password), user_id))
            if user_id != principal.get("user_id"):
                db.execute("DELETE FROM user_tokens WHERE user_id=?", (user_id,))
            db.commit()
        return {"ok": True, "password": password}

    return r
