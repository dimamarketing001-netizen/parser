"""Домены и размещение сайтов раздела «Сайты» ЛК «Реклама» (2026-11-18).

Домены — из аккаунта reg.ru (тот же, откуда ParsYa берёт домены для
регистрации). reg.ru API пускает только с прод-сервера, поэтому всё делает
агент ~/regru_monitor/sites_agent.py на проде (ssh, как ispmgr_mail_api):
список + A-записи, A → наш сервер, vhost nginx → addAvto, SSL certbot.
Статус сайта пишем в addAvto (PUT /api/sites), сами страницы отдаёт addAvto.

2026-11-19: аккаунтов reg.ru несколько (на проде REGRU_USERNAME_2 … в .env
агента) — у домена account/account_login, список аккаунтов — /regru-accounts.
"""
from __future__ import annotations

import asyncio
import json
import os
import re
import time
from typing import Any, Callable

import httpx
from fastapi import APIRouter, Depends, Header, HTTPException
from pydantic import BaseModel, Field

SSH_HOST = os.getenv("SITES_SSH_HOST") or os.getenv("ISPMGR_SSH_HOST") or "62.109.6.186"
SSH_USER = os.getenv("SITES_SSH_USER") or os.getenv("ISPMGR_SSH_USER") or "dima"
AGENT = "cd ~/regru_monitor && .venv/bin/python3 sites_agent.py"
SSL_WAIT_MINUTES = int(os.getenv("SITES_SSL_WAIT_MINUTES", "120"))  # wildcard *.зона: первый выпуск ждёт DNS reg.ru

_list_cache: dict[str, Any] = {"at": 0.0, "data": None}
_jobs: dict[str, dict[str, Any]] = {}


async def agent(*args: str, timeout: float = 300) -> dict[str, Any]:
    for a in args:
        if not all(ch.isalnum() or ch in ".-_" for ch in a):
            raise HTTPException(status_code=422, detail=f"Недопустимый аргумент: {a}")
    proc = await asyncio.create_subprocess_exec(
        "ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=15", f"{SSH_USER}@{SSH_HOST}", f"{AGENT} {' '.join(args)}",
        stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
    try:
        out, err = await asyncio.wait_for(proc.communicate(), timeout)
    except asyncio.TimeoutError:
        proc.kill()
        return {"ok": False, "error": f"агент не ответил за {int(timeout)} с"}
    line = (out.decode(errors="replace").strip().splitlines() or [""])[-1]
    try:
        return json.loads(line)
    except ValueError:
        return {"ok": False, "error": (err.decode(errors="replace") or line)[-400:] or f"код {proc.returncode}"}


class DeployIn(BaseModel):
    domain: str = Field(min_length=3, max_length=255)
    force: bool = False
    account: int | None = Field(default=None, ge=1, le=9)
    template_id: int | None = None
    project_id: int | None = None


def build_router(principal_dep: Callable, require_user: Callable, addavto_base: str, addavto_user: str,
                 addavto_password: str, addavto_auth: Callable | None = None) -> APIRouter:
    router = APIRouter()

    async def addavto(method: str, path: str, payload: dict | None = None) -> Any:
        if not addavto_base:
            raise HTTPException(status_code=503, detail="addAvto не настроен")
        async with httpx.AsyncClient(auth=(addavto_user, addavto_password), timeout=httpx.Timeout(60.0, connect=10.0),
                                     transport=httpx.AsyncHTTPTransport(retries=3)) as c:
            r = await c.request(method, f"{addavto_base}/api/{path}", json=payload)
        if r.status_code >= 400:
            raise HTTPException(status_code=r.status_code, detail=(r.json().get("detail") if "json" in r.headers.get("content-type", "") else r.text[:300]))
        return r.json()

    async def set_status(domain: str, status: str, note: str, **extra: Any) -> None:
        try:
            await addavto("PUT", "sites", {"domain": domain, "status": status, "note": note[:480],
                                           **{k: v for k, v in extra.items() if v}})
        except Exception:  # noqa: BLE001 - статус вторичен, job хранит своё
            pass

    def job_note(d: str, text: str, state: str | None = None) -> None:
        j = _jobs.setdefault(d, {"domain": d, "log": [], "state": "running", "started": time.time()})
        j["log"].append(time.strftime("%H:%M:%S ") + text)
        j["log"] = j["log"][-30:]
        if state:
            j["state"] = state

    async def deploy(d: str, force: bool, template_id: int | None, project_id: int | None,
                     account: int | None = None) -> None:
        job_note(d, "Направляю A-записи на сервер (DNS регистратора)…")
        r = await agent("point", d, *(["--force"] if force else []), *(["--account", str(account)] if account else []))
        if not r.get("ok"):
            job_note(d, "Ошибка: " + str(r.get("error")), "error")
            await set_status(d, "error", str(r.get("error")), template_id=template_id, project_id=project_id)
            return
        job_note(d, f"DNS ({r.get('account_login') or 'reg.ru'}): " + "; ".join(r.get("done") or ["уже настроено"]))
        await set_status(d, "dns", "A-записи на сервер; ждём DNS и SSL", template_id=template_id, project_id=project_id)
        r = await agent("nginx", d)
        if not r.get("ok"):
            job_note(d, "nginx: " + str(r.get("error")), "error")
            await set_status(d, "error", "nginx: " + str(r.get("error")))
            return
        job_note(d, "nginx: сайт подключён (http)")
        deadline = time.time() + SSL_WAIT_MINUTES * 60
        while True:
            r = await agent("ssl", d)
            if r.get("ok"):
                job_note(d, "SSL выпущен — сайт работает по https", "done")
                await set_status(d, "live", "https работает")
                return
            if time.time() > deadline:
                job_note(d, "SSL не выпущен: " + str(r.get("error")), "error")
                await set_status(d, "error", "SSL: " + str(r.get("error")) + " — нажмите «Выпустить SSL» позже")
                return
            job_note(d, "SSL ждёт: " + str(r.get("error"))[:200])
            await set_status(d, "ssl", str(r.get("error"))[:300])
            await asyncio.sleep(120)

    async def domain_list(refresh: bool = False) -> dict[str, Any]:
        if refresh or not _list_cache["data"] or time.time() - _list_cache["at"] > 300:
            r = await agent("list", timeout=240)
            if not r.get("ok"):
                raise HTTPException(status_code=502, detail="Список доменов регистраторов: " + str(r.get("error")))
            _list_cache.update(at=time.time(), data=r)
        return _list_cache["data"]

    def start_deploy(d: str, force: bool, template_id: int | None, project_id: int | None,
                     account: int | None) -> dict[str, Any]:
        j = _jobs.get(d)
        if j and j.get("state") == "running":
            return {k: v for k, v in j.items() if not k.startswith("_")}
        _jobs[d] = {"domain": d, "log": [], "state": "running", "started": time.time()}
        _jobs[d]["_task"] = asyncio.create_task(deploy(d, force, template_id, project_id, account))
        _list_cache["at"] = 0
        return {k: v for k, v in _jobs[d].items() if not k.startswith("_")}

    # ---- addAvto (сервер → сервер, токен PARSYA_ADDAVTO_TOKEN): автозамена на новый домен ----
    @router.get("/api/addavto/sites/free-domains")
    async def addavto_free_domains(accounts: str = "", authorization: str | None = Header(default=None)):
        """Свободные домены выбранных регистраторов (id аккаунтов через запятую):
        A-запись не на чужом сервере и на домене ещё нет сайта addAvto."""
        if addavto_auth is None:
            raise HTTPException(status_code=503, detail="addAvto auth не настроен")
        addavto_auth(authorization)
        want = {int(x) for x in re.split(r"[,\s]+", accounts) if x.strip().isdigit()}
        data = await domain_list()
        try:
            ours = {x["domain"] for x in (await addavto("GET", "sites")).get("items") or []}
        except HTTPException:
            ours = set()
        items = [{"domain": it["domain"], "name": it.get("name") or it["domain"], "account": it.get("account"),
                  "account_login": it.get("account_login"), "expires": it.get("expires")}
                 for it in data["items"]
                 if not it.get("occupied") and it["domain"] not in ours and (not want or it.get("account") in want)]
        return {"items": items, "accounts": data.get("accounts") or []}

    @router.post("/api/addavto/sites/deploy")
    async def addavto_deploy(payload: DeployIn, authorization: str | None = Header(default=None)):
        if addavto_auth is None:
            raise HTTPException(status_code=503, detail="addAvto auth не настроен")
        addavto_auth(authorization)
        try:
            d = payload.domain.strip().lower().removeprefix("www.").encode("idna").decode("ascii")
        except UnicodeError as exc:
            raise HTTPException(status_code=422, detail="Неверный домен") from exc
        await set_status(d, "dns", "Размещение начато (автозамена)", template_id=payload.template_id,
                         project_id=payload.project_id)
        return start_deploy(d, payload.force, payload.template_id, payload.project_id, payload.account)

    @router.get("/api/v1/sites/domains")
    async def domains(refresh: bool = False, principal: dict = Depends(principal_dep)):
        """Домены reg.ru: занят (A на чужой IP) / свободен / наш сервер, +
        сайт addAvto, если уже заведён."""
        require_user(principal)
        data = await domain_list(refresh)
        try:
            ours = {s["domain"]: s for s in (await addavto("GET", "sites")).get("items") or []}
        except HTTPException:
            ours = {}
        items = []
        for it in data["items"]:
            s = ours.get(it["domain"])
            job = {k: v for k, v in (_jobs.get(it["domain"]) or {}).items() if not k.startswith("_")}
            items.append({**it, "site": s, "job": job or None})
        return {"ip": data.get("ip"), "items": items, "accounts": data.get("accounts") or [],
                "cached_at": _list_cache["at"]}

    @router.get("/api/v1/sites/regru-accounts")
    async def regru_accounts(principal: dict = Depends(principal_dep)):
        """Аккаунты reg.ru: логин, доступ к API с сервера, число доменов (живой запрос)."""
        require_user(principal)
        r = await agent("accounts", timeout=120)
        if not r.get("ok"):
            raise HTTPException(status_code=502, detail="Аккаунты reg.ru: " + str(r.get("error")))
        return {"ip": r.get("ip"), "accounts": r.get("accounts") or []}

    @router.post("/api/v1/sites/deploy")
    async def deploy_start(payload: DeployIn, principal: dict = Depends(principal_dep)):
        """Разместить сайт на домене: A @/www → сервер (reg.ru), vhost nginx,
        SSL (ждёт DNS до SITES_SSL_WAIT_MINUTES). Фоном; прогресс — /deploy-status."""
        require_user(principal)
        d = payload.domain.strip().lower()
        d = d[4:] if d.startswith("www.") else d
        try:
            d = d.encode("idna").decode("ascii")
        except UnicodeError as exc:
            raise HTTPException(status_code=422, detail="Неверный домен") from exc
        j = _jobs.get(d)
        if j and j.get("state") == "running":
            return {k: v for k, v in j.items() if not k.startswith("_")}
        await set_status(d, "dns", "Размещение начато", template_id=payload.template_id, project_id=payload.project_id)
        return start_deploy(d, payload.force, payload.template_id, payload.project_id, payload.account)

    @router.get("/api/v1/sites/deploy-status")
    async def deploy_status(principal: dict = Depends(principal_dep)):
        require_user(principal)
        return {"items": [{k: v for k, v in j.items() if not k.startswith("_")} for j in _jobs.values()]}

    @router.post("/api/v1/sites/ssl")
    async def ssl_retry(payload: DeployIn, principal: dict = Depends(principal_dep)):
        """Повторить выпуск SSL (когда DNS обновился позже ожидания)."""
        require_user(principal)
        d = payload.domain.strip().lower().removeprefix("www.")
        r = await agent("ssl", d)
        if r.get("ok"):
            await set_status(d, "live", "https работает")
            return r
        raise HTTPException(status_code=409, detail=str(r.get("error")))

    return router
