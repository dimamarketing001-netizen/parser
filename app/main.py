from __future__ import annotations

import base64
import gzip
import hashlib
import hmac
import json
import os
import re
import secrets
import sqlite3
import threading
import time
from contextlib import closing
from contextvars import ContextVar
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import httpx
import snowballstemmer

from app import embeddings as emb
from app import ai_intent as aii

from fastapi import Depends, FastAPI, Form, Header, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field
from starlette.middleware.sessions import SessionMiddleware

APP_DIR = Path(__file__).resolve().parent
DATA_DIR = Path(os.getenv("PARSYA_DATA_DIR", APP_DIR / "data")).resolve()
DATA_DIR.mkdir(parents=True, exist_ok=True)
DB_PATH = Path(os.getenv("PARSYA_SQLITE_PATH", DATA_DIR / "control.sqlite3")).resolve()
ADMIN_USER = os.getenv("PARSYA_ADMIN_USER", "admin")
ADMIN_PASSWORD = os.getenv("PARSYA_ADMIN_PASSWORD", "")
WORKER_TOKEN = os.getenv("PARSYA_WORKER_TOKEN", "")
PUBLIC_API_TOKEN = os.getenv("PARSYA_PUBLIC_API_TOKEN", "")
# Separate, narrower-scope token for addAvto (a different project on the same
# server) to read Yandex-account Metrika OAuth tokens from the pool — kept
# distinct from WORKER_TOKEN so a leak on addAvto's side can't reach job
# management. Optional: unset simply means these endpoints always 401.
ADDAVTO_TOKEN = os.getenv("PARSYA_ADDAVTO_TOKEN", "")
# Outbound direction: this server proxying the marketer frontend's calls INTO
# addAvto's own API (separate domain, HTTP Basic auth, no CORS of its own).
# Optional: unset means the /api/v1/ads/* proxy always responds 503.
ADDAVTO_BASE_URL = os.getenv("PARSYA_ADDAVTO_BASE_URL", "").rstrip("/")
ADDAVTO_ADMIN_USER = os.getenv("PARSYA_ADDAVTO_ADMIN_USER", "")
ADDAVTO_ADMIN_PASSWORD = os.getenv("PARSYA_ADDAVTO_ADMIN_PASSWORD", "")
SECRET_KEY = os.getenv("PARSYA_SECRET_KEY", "")
COOKIE_SECURE = os.getenv("PARSYA_COOKIE_SECURE", "1").strip().lower() in {"1", "true", "yes"}
MAX_HTML_CHARS = 2_000_000

# 2026-11-03: design tokens + base components, matching the client-facing
# "NEON / Ad Intelligence" SaaS frontend (saas-frontend repo,
# /app/ad-intelligence.html) — Dima wants one consistent look across the
# client app, this admin, and addAvto. Starting point: this admin's shell
# (sidebar/topbar/headline) and the Обзор (dashboard) page specifically,
# per his explicit ordering ("design-токены + одна страница админки").
# Plain string, NOT an f-string — every existing body-builder below still
# emits literal class names (.panel/.metric/.btn/table/.badge/...), so
# this re-skins those classes' CSS rather than renaming them, keeping
# every other already-written page working unmodified.
DESIGN_TOKENS_CSS = """
:root{--bg:#f7f7fb;--panel:#fff;--ink:#20212a;--muted:#9294a3;--line:#ececf3;--green:#39ff88;--green-dark:#20a66a;--violet:#8c72f6;--nav:#fff;--nav-muted:#777b8d;--red:#c0392b;--amber:#c18b3e}
*{box-sizing:border-box}
body{margin:0;background:var(--bg);color:var(--ink);font-family:"DM Sans",ui-sans-serif,system-ui,sans-serif;font-size:13px;letter-spacing:-.012em}
a{color:var(--violet);text-decoration:none}a:hover{text-decoration:underline}
button,input,select,textarea{font:inherit}
.shell{min-height:100vh;display:grid;grid-template-columns:258px minmax(0,1fr)}
aside.sidebar{background:#fff;color:#292a36;padding:27px 17px 20px;border-right:1px solid #eeeef4;display:flex;flex-direction:column;gap:10px}
.brand{display:flex;align-items:center;gap:12px;padding:0 11px 22px}
.brand-mark{width:39px;height:39px;border-radius:14px;background:#24242f;color:var(--green);display:grid;place-items:center;font-size:19px;font-weight:800;box-shadow:0 5px 12px #25253216}
.brand b{display:block;font-size:15px;letter-spacing:.13em;color:#242530}
.brand small{display:block;color:#9b9dac;letter-spacing:.14em;font-size:8px;margin-top:3px}
nav{display:grid;gap:6px}
nav a{color:#777b8c;border-radius:12px;padding:12px 13px;display:flex;align-items:center;gap:11px;font-size:12px;transition:background .18s,color .18s}
nav a svg{width:16px;height:16px;flex-shrink:0}
nav a:hover{background:#f7f6fc;color:#363448}
nav a.active{background:#f0edff;color:#7057dd;font-weight:700}
nav a.nav-back{color:#9a9baa;margin-bottom:6px;padding-bottom:17px;border-bottom:1px solid #f0f0f5;border-radius:0}
nav a.nav-back:hover{background:transparent;color:#363448}
.side-bottom{margin-top:auto;border-top:1px solid #f0f0f5;padding-top:16px;color:#9a9baa;font-size:11px;display:flex;align-items:center;gap:10px}
.dot{display:inline-block;width:7px;height:7px;border-radius:50%;background:var(--green-dark);flex-shrink:0}
.logout{margin-left:auto;background:#f7f6fc;border:1px solid #ececf3;color:#6c6f80;padding:7px 11px;border-radius:9px;cursor:pointer;font-size:10px;font-weight:700}
.logout:hover{border-color:#d4cdf8;color:#7057dd}
main.main{min-width:0}
.topbar{height:72px;background:#f7f7fb;border-bottom:1px solid #eeeef4;display:flex;align-items:center;justify-content:space-between;padding:0 34px;gap:15px}
.crumb{font-size:11px;color:#a0a1af}.crumb strong{color:#373746;font-weight:700}
.top-actions{display:flex;align-items:center;gap:10px}
.profiledot{width:36px;height:36px;border-radius:12px;background:#282735;color:var(--green);display:grid;place-items:center;font-weight:800;border:3px solid #fff;box-shadow:0 0 0 1px #e9e8f0}
.content{max-width:1500px;padding:32px 34px 48px;margin:0 auto}
.headline{display:flex;justify-content:space-between;align-items:center;gap:20px;margin-bottom:27px}
.eyebrow{font-size:9px;color:#7158dc;font-weight:800;letter-spacing:.16em;text-transform:uppercase;display:flex;align-items:center;gap:8px}
.eyebrow:before{content:"";width:7px;height:7px;border-radius:50%;background:var(--green);box-shadow:0 0 0 4px #39ff8825}
.h1{font:800 clamp(22px,3vw,30px)/1.2 Manrope,sans-serif;letter-spacing:-.045em;margin:10px 0 7px;color:#22232e}
.subtitle{font-size:12px;color:var(--muted);line-height:1.65;max-width:620px}
.primary,.secondary,.btn{border-radius:12px;padding:11px 15px;display:inline-flex;align-items:center;justify-content:center;gap:8px;font-weight:700;font-size:11px;white-space:nowrap;border:1px solid transparent;cursor:pointer;transition:transform .15s,box-shadow .15s,background .15s}
.primary,.btn{background:#272633;color:#fff;border-color:#272633;box-shadow:0 6px 14px #25243514}
.primary:hover,.btn:hover{background:#3a384b;transform:translateY(-1px);text-decoration:none}
.secondary,.btn.secondary{background:#fff;border-color:var(--line);color:#626477}
.secondary:hover,.btn.secondary:hover{border-color:#d4cdf8;background:#fbfaff;text-decoration:none}
.btn.danger{background:#fdecea;border-color:#f5c2bd;color:var(--red)}
h2{font:800 15px Manrope,sans-serif;margin:0 0 15px;letter-spacing:-.025em;color:#272733}
.muted{color:var(--muted)}
.summary,.grid{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:16px;margin-bottom:25px}
.metric,.stat{padding:21px 22px;background:#fff;border:1px solid var(--line);border-radius:18px;box-shadow:0 5px 20px #2b294a04;min-width:0}
.metric .label,.stathead{display:flex;align-items:center;justify-content:space-between;gap:8px;color:#858799;font-size:11px}
.metric .value,.statvalue{font:800 27px Manrope,sans-serif;letter-spacing:-.045em;margin:14px 0 6px;color:#242431}
.metric .hint,.statfoot{font-size:10px;color:#9a9baa}
.staticon{width:36px;height:36px;border-radius:12px;background:#e9faef;color:var(--green-dark);display:grid;place-items:center;flex-shrink:0}
.staticon svg{width:16px;height:16px}
.staticon.violet{background:#f0edff;color:#8066e8}.staticon.gray{background:#f2f2f7;color:#73758b}.staticon.amber{background:#fff5e7;color:var(--amber)}
.panel,.card{background:#fff;border:1px solid var(--line);border-radius:19px;box-shadow:0 5px 20px #2b294a04}
.panel{padding:22px;margin-bottom:20px}
.panel-head{display:flex;align-items:center;justify-content:space-between;gap:10px;margin-bottom:16px}
.table-wrap{overflow:auto}
table{width:100%;border-collapse:collapse;white-space:nowrap}
th{color:#9a9baa;font-size:10px;text-align:left;font-weight:700;letter-spacing:.04em;text-transform:uppercase;padding:10px;border-bottom:1px solid var(--line)}
td{padding:13px 10px;border-bottom:1px solid #f1f1f6;vertical-align:middle;color:#45465a}
tr:last-child td{border-bottom:0}
.badge,.status{display:inline-flex;align-items:center;gap:6px;border-radius:9px;padding:6px 9px;font-size:10px;font-weight:700;background:#f2f2f7;color:#5c5e70}
.badge:before,.status:before{content:"";width:6px;height:6px;border-radius:50%;background:currentColor}
.badge.ok,.badge.completed,.status.completed{background:#eaf8ef;color:var(--green-dark)}
.badge.running,.status.running{background:#eaf3ff;color:#2868b2}
.badge.failed,.badge.error,.status.failed{background:#fdecea;color:var(--red)}
.badge.queued,.status.queued{background:#f0edff;color:#7158dc}
.formgrid{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:16px}
label{display:block;color:#596371;font-size:11px;font-weight:700;margin-bottom:8px}
input,select,textarea{width:100%;background:#fff;border:1px solid var(--line);color:var(--ink);padding:12px 13px;border-radius:12px;outline:none;transition:border-color .15s}
input:focus,select:focus,textarea:focus{border-color:var(--violet)}
.field{margin-bottom:16px}
.help{font-size:11px;color:var(--muted);margin-top:7px;line-height:1.6}
.empty{padding:36px;text-align:center;color:var(--muted);border:1px dashed #dfe2e9;border-radius:16px;background:#fff}
.filters{display:flex;gap:10px;align-items:center;flex-wrap:wrap;margin-bottom:16px}
.filters input,.filters select{width:auto;min-width:170px}
.creative-grid{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:18px}
.creative{overflow:hidden;border:1px solid var(--line);border-radius:20px;background:#fff;box-shadow:0 5px 20px #2b294a04;transition:transform .2s,box-shadow .2s}
.creative:hover{transform:translateY(-3px);box-shadow:0 16px 34px #2a284511}
.creative-preview{height:195px;background:#f0eff8;color:#172235;overflow:hidden;display:flex;align-items:center;justify-content:center}
.creative-preview iframe{width:100%;height:100%;border:0;background:white}
.creative-preview img{max-width:100%;max-height:100%;object-fit:contain}
.creative-info{padding:17px 18px}
.creative-info h3{font-size:13px;line-height:1.4;margin:0 0 8px;overflow-wrap:anywhere;color:#272733}
.creative-meta{display:flex;flex-wrap:wrap;gap:6px;margin:9px 0}
.mono{font-family:ui-monospace,SFMono-Regular,Menlo,monospace;font-size:11px;white-space:pre-wrap;overflow-wrap:anywhere}
.log{max-height:600px;overflow:auto;background:#191a24;color:#d9e3f7;padding:15px;border-radius:12px;border:1px solid var(--line)}
.detail-grid{display:grid;grid-template-columns:minmax(0,1fr) minmax(0,1fr);gap:18px}
.html-frame{width:100%;height:650px;border:1px solid var(--line);background:white;border-radius:12px}
.notice{border:1px solid #e8e4fa;background:#f8f6ff;border-radius:14px;padding:13px 15px;color:#6751a9;margin-bottom:16px;font-size:11px;line-height:1.7}
.error-text{color:var(--red)}
.right{text-align:right}
.footnote{font-size:11px;color:var(--muted);margin-top:16px}
.joblist{display:flex;flex-direction:column;gap:11px}
.job{display:grid;grid-template-columns:minmax(0,1.3fr) minmax(90px,.6fr) minmax(90px,.6fr) auto;gap:14px;align-items:center;padding:16px 18px;background:#fff;border:1px solid var(--line);border-radius:15px;box-shadow:0 4px 18px #28274204}
.jobname{display:flex;align-items:center;gap:11px;min-width:0}
.jobicon{width:36px;height:36px;display:grid;place-items:center;border-radius:12px;background:#f0edfd;color:#8065d8;flex-shrink:0}
.jobicon svg{width:16px;height:16px}
.jobname strong{display:block;font-size:12px;margin-bottom:4px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.jobname span,.jobmeta span{font-size:10px;color:#8b94a0}
.jobmeta strong{display:block;font-size:11px;margin-bottom:3px}
@media(max-width:1050px){.summary,.grid{grid-template-columns:repeat(2,minmax(0,1fr))}.creative-grid{grid-template-columns:repeat(2,minmax(0,1fr))}.job{grid-template-columns:minmax(0,1fr) auto}.jobmeta{display:none}}
@media(max-width:720px){.shell{grid-template-columns:1fr}aside.sidebar{display:none!important}.topbar{padding:0 18px}.content{padding:22px 16px 35px}.headline{flex-direction:column;align-items:stretch}.summary,.grid{gap:9px}.metric,.stat{padding:15px}.metric .value,.statvalue{font-size:22px}.creative-grid,.formgrid,.detail-grid{grid-template-columns:1fr}.h1{font-size:22px}.job{grid-template-columns:1fr;gap:10px}}

/* Библиотека креативов — живой поиск/фильтры/закладки/сохранённые
   поиски, см. ad-intelligence.html (фаза 2, 2026-11-03) */
.librarycontrols{background:#fff;border:1px solid var(--line);border-radius:21px;padding:17px 18px 14px;margin-bottom:24px;box-shadow:0 5px 20px #2b294a04}
.toolbar{display:flex;gap:10px;align-items:center;flex-wrap:wrap;margin-bottom:13px}
.search{flex:1;min-width:230px;display:flex;align-items:center;gap:9px;padding:0 13px;background:#fff;border:1px solid var(--line);border-radius:12px;height:45px;color:#9294a3}
.search svg{width:15px;height:15px;flex-shrink:0}
.search input{width:100%;min-width:0;border:0;outline:0;background:transparent;color:#2b2c3a;font-size:12px}
.filter{height:45px;border:1px solid var(--line);background:#fff;border-radius:12px;padding:0 12px;color:#45465a;outline:none;font-size:11px}
.quickfilters{display:flex;gap:8px;align-items:center;flex-wrap:wrap}
.quickfilter{display:inline-flex;align-items:center;gap:7px;border:1px solid transparent;background:transparent;color:#858799;border-radius:10px;padding:9px 12px;font:600 10px "DM Sans",sans-serif;cursor:pointer;transition:all .18s}
.quickfilter svg{width:13px;height:13px}
.quickfilter:hover{background:#f7f6fc;color:#5e50a9}
.quickfilter.active{background:#f0edff;color:#7158dc;border-color:#e6e0ff}
.activefilters{display:flex;align-items:center;gap:10px;flex-wrap:wrap;border-top:1px solid #f0f0f5;padding-top:13px;margin-top:13px}
.activefilters[hidden]{display:none!important}
.activefilterlabel{font-size:9px;color:#9a9baa}
.filterchips{display:flex;gap:7px;flex-wrap:wrap;flex:1}
.filterchip{display:inline-flex;align-items:center;gap:9px;border:1px solid #e8e4fa;background:#f8f6ff;border-radius:8px;padding:7px 9px;color:#7059cf;font-size:10px;cursor:pointer}
.clearfilters{border:0;background:none;color:#858799;font:600 10px "DM Sans",sans-serif;cursor:pointer}
.clearfilters:hover{color:#7158dc}
.resulthead{display:flex;justify-content:space-between;align-items:center;gap:15px;margin:0 0 15px}
.resulthead p{color:var(--muted);font-size:10px;margin:0}
.resultactions{display:flex;gap:8px;flex-wrap:wrap}
.creative .save{position:absolute;top:12px;right:12px;z-index:3;width:34px;height:34px;border:1px solid #e4e8ec;border-radius:10px;background:#ffffffed;color:#65707d;display:grid;place-items:center;cursor:pointer}
.creative .save svg{width:15px;height:15px}
.creative .save.saved{background:#e6f8ed;color:var(--green-dark);border-color:#ccebd7}
.creative-preview{position:relative}
.creative[hidden]{display:none!important}
.savedsearches{margin:0 0 18px;padding:16px;background:#fafbfc;border:1px solid var(--line);border-radius:15px}
.savedsearches[hidden]{display:none!important}
.savedsearchhead{display:flex;align-items:flex-start;justify-content:space-between;gap:12px;margin-bottom:12px}
.savedsearchhead strong{font-size:12px;color:#303743}
.savedsearchhead p{font-size:10px;color:#838b98;line-height:1.5;margin:4px 0 0}
.savedsearchlist{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:10px}
.savedsearchitem{min-width:0;display:flex;align-items:center;gap:12px;padding:12px;background:#fff;border:1px solid #e4e7ee;border-radius:12px}
.savedsearchbody{min-width:0;flex:1}
.savedsearchbody strong{display:block;font-size:11px;color:#303743;overflow-wrap:anywhere}
.savedsearchbody span{display:block;font-size:10px;color:#858c99;line-height:1.5;margin-top:4px;overflow-wrap:anywhere}
.savedsearchactions{display:flex;gap:5px;flex-shrink:0}
.savedsearchactions button{width:31px;height:31px;border:1px solid #e6e8ee;border-radius:8px;background:#fff;color:#586273;display:grid;place-items:center;cursor:pointer}
.savedsearchactions button svg{width:13px;height:13px}
.savedsearchactions button[data-delete-saved-search]{color:#c0392b}
.empty-filter{grid-column:1/-1;padding:55px 22px;text-align:center;border:1px dashed #dedbea;border-radius:22px;background:#fff}
.empty-filter[hidden]{display:none!important}
.empty-filter-icon{display:grid;place-items:center;width:52px;height:52px;margin:0 auto 15px;border-radius:17px;background:#f0edff;color:#8066e8}
.empty-filter-icon svg{width:23px;height:23px}
.empty-filter h3{font:800 17px Manrope,sans-serif;letter-spacing:-.04em;color:#2b2c39;margin:0 0 8px}
.empty-filter p{font-size:11px;line-height:1.7;color:#8c8e9f;max-width:410px;margin:0 auto 18px}
@media(max-width:760px){.librarycontrols{padding:13px;border-radius:18px}.toolbar{flex-wrap:wrap}.search{min-width:100%}.savedsearchlist{grid-template-columns:1fr}}

/* Intent-picker — скоуп библиотеки к одному поисковому запросу (фаза 3,
   2026-11-03), та же серверная стемминг/эмбеддинг-логика, что у
   публичного /api/v1/creatives */
.intentbar{display:flex;align-items:center;justify-content:space-between;gap:16px;flex-wrap:wrap;margin:0 0 18px;padding:14px 18px;background:#f8f6ff;border:1px solid #e8e4fa;border-radius:16px}
.intentbar-left{display:flex;align-items:center;gap:12px;min-width:0}
.intentbar-left>i{color:#7059cf;flex-shrink:0}
.intentbar-copy{min-width:0}
.intentbar-copy strong{display:block;font-size:13px;color:#2f2b4a;overflow-wrap:anywhere}
.intentbar-copy span{display:block;font-size:10px;color:#8178a8;margin-top:2px}
.intentbar-actions{display:flex;align-items:center;gap:10px;flex-wrap:wrap}
.intentpicker{position:relative;margin:-6px 0 18px;padding:16px 18px;background:#fff;border:1px solid #e8e4fa;border-radius:16px;box-shadow:0 10px 30px #2b294a0f}
.intentpicker[hidden]{display:none!important}
.intentpicker-head{display:flex;align-items:center;justify-content:space-between;gap:10px;margin-bottom:10px}
.intentpicker-head strong{font-size:12px;color:#303743}
.intentpicker-close{border:0;background:none;color:#9a9baa;cursor:pointer;font-size:16px;line-height:1}
.intentpicker-search{width:100%;padding:9px 12px;border:1px solid var(--line);border-radius:10px;font-size:12px;margin-bottom:10px;box-sizing:border-box}
.intentpicker-list{max-height:260px;overflow-y:auto;display:flex;flex-direction:column;gap:4px}
.intentrow{display:flex;align-items:center;gap:10px;padding:9px 11px;border-radius:10px;cursor:pointer}
.intentrow:hover{background:#f8f6ff}
.intentrow-copy{min-width:0;flex:1;display:flex;align-items:baseline;justify-content:space-between;gap:8px}
.intentrow-copy strong{font-size:12px;color:#2f2b4a;overflow-wrap:anywhere}
.intentrow-copy span{font-size:10px;color:#9a9baa;flex-shrink:0;white-space:nowrap}
.intentpicker-foot{display:flex;justify-content:flex-end;margin-top:12px;padding-top:12px;border-top:1px solid #f1eefc}
"""


if not ADMIN_PASSWORD:
    raise RuntimeError("PARSYA_ADMIN_PASSWORD must be set in the environment.")
if len(SECRET_KEY) < 32:
    raise RuntimeError("PARSYA_SECRET_KEY must be a random value of at least 32 characters.")
if len(WORKER_TOKEN) < 32:
    raise RuntimeError("PARSYA_WORKER_TOKEN must be a random value of at least 32 characters.")

FRONTEND_ORIGINS = [o.strip() for o in os.getenv("PARSYA_FRONTEND_ORIGINS", "").split(",") if o.strip()]

app = FastAPI(title="ParsYa Control Plane", docs_url=None, redoc_url=None)
app.add_middleware(
    SessionMiddleware,
    secret_key=SECRET_KEY,
    session_cookie="parsya_session",
    same_site="lax",
    https_only=COOKIE_SECURE,
    max_age=60 * 60 * 12,
)
if FRONTEND_ORIGINS:
    # The marketer-facing SaaS frontend is a separate static site/domain and authenticates
    # with a per-user bearer token (not a cookie), so credentials are not needed here.
    app.add_middleware(
        CORSMiddleware,
        allow_origins=FRONTEND_ORIGINS,
        allow_methods=["GET", "POST", "DELETE"],
        allow_headers=["Authorization", "Content-Type", "X-API-Key"],
        allow_credentials=False,
    )

# 2026-11-03: эта панель теперь доступна и под своим собственным доменом
# (парсер.союзюристовроссии.рф, корень "/" — как и раньше) И как подвкладка
# внутри addAvto (реклама.союзюристовроссии.рф/parser/...), через nginx
# reverse-proxy. Все ссылки/формы/редиректы в этом файле — захардкоженные
# абсолютные пути ("/dashboard" и т.п.), поэтому при обращении через
# addAvto их нужно дополнять префиксом "/parser", иначе браузер уйдёт на
# корень чужого домена. Префикс приходит от nginx только для этого
# конкретного маршрута (заголовок X-Forwarded-Prefix) — прямые запросы на
# собственный домен этот заголовок не шлют, и P("") там всегда "" (ничего
# не меняется, старое поведение сохранено).
_URL_PREFIX: ContextVar[str] = ContextVar("url_prefix", default="")


def P(path: str = "") -> str:
    return _URL_PREFIX.get() + path


@app.middleware("http")
async def _apply_forwarded_prefix(request: Request, call_next):
    # NOTE: this middleware runs OUTSIDE SessionMiddleware (registered
    # after it, which — per Starlette's add_middleware() — makes it the
    # outer layer), so request.session isn't attached yet here. Only set
    # the ContextVar; any session-touching auto-login lives in
    # auth_page() instead, which only ever runs from inside a route
    # handler (fully inside the stack, session guaranteed present).
    token = _URL_PREFIX.set(request.headers.get("x-forwarded-prefix", "").rstrip("/"))
    try:
        return await call_next(request)
    finally:
        _URL_PREFIX.reset(token)


def now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def create_notification(db, user_id: int | None, type_: str, title: str, text: str = "", link: str = "") -> None:
    """Inserts an in-app notification for one user, unless they've muted this type in
    users.notification_prefs (JSON {type: bool}; a type absent from the JSON, or no prefs
    saved at all, defaults to enabled — opt-out, not opt-in). Caller is expected to be
    inside an existing db transaction; this does not commit.

    user_id can legitimately be unknown (e.g. a job whose owner_user_id is NULL, from
    before SaaS accounts existed) — silently no-ops rather than erroring, since a
    notification with no recipient has nowhere to go."""
    if not user_id:
        return
    row = db.execute("SELECT notification_prefs FROM users WHERE id=?", (user_id,)).fetchone()
    if not row:
        return
    prefs = {}
    if row["notification_prefs"]:
        try:
            prefs = json.loads(row["notification_prefs"])
        except (TypeError, ValueError):
            prefs = {}
    if prefs.get(type_) is False:
        return
    db.execute(
        "INSERT INTO notifications(user_id,type,title,text,link,created_at) VALUES(?,?,?,?,?,?)",
        (user_id, type_, title, text or None, link or None, now_iso()),
    )


def _pylower(value: str | None) -> str:
    # SQLite's built-in LOWER()/COLLATE NOCASE only fold ASCII A-Z — useless for the
    # Russian search queries (key_text) that "intent" filtering matches on. Registered
    # as a real SQLite function (not done in Python before the query) so it can be used
    # directly inside WHERE/JOIN clauses built across many endpoints (see _intent_exists_sql).
    return (value or "").strip().lower()


def connect() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, timeout=15)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA busy_timeout=15000")
    conn.create_function("PYLOWER", 1, _pylower)
    return conn


def _parse_intents(intent: str) -> list[str]:
    """`intent` is a comma-separated list of exact key_text values (the frontend lets a
    viewer select several at once — e.g. both "Пенсия" and "Увеличение пенсии" — rather
    than silently auto-merging differently-worded runs of the same real-world topic;
    see _related_intents for the stemming-based *suggestion* of what to add)."""
    return [part.strip() for part in (intent or "").split(",") if part.strip()]


def _intent_exists_sql(creative_alias: str, intents: list[str]) -> str:
    """Intent = one of a set of exact (case/whitespace-insensitive) search queries
    (jobs.key_text) a creative was captured under. Used everywhere a creative-derived
    report should default to showing only the active search(es)' own results (see
    api_list_intents and the `intent` param accepted by every creative-derived
    listing/analytics endpoint below)."""
    placeholders = ",".join(["PYLOWER(?)"] * len(intents))
    return (
        f"EXISTS (SELECT 1 FROM occurrences io JOIN jobs ij ON ij.id=io.job_id "
        f"WHERE io.creative_id={creative_alias}.id AND PYLOWER(ij.key_text) IN ({placeholders}))"
    )


_RU_STEMMER = snowballstemmer.stemmer("russian")
# Short/generic words that shouldn't by themselves count as a shared "topic" between two
# differently-worded search queries — stemming alone isn't enough: "красные кроссовки"
# and "красные туфли" share the color but are different products, so colors/sizes/filler
# words are excluded from the stem-overlap used to suggest related intents.
_INTENT_STOPWORDS = {
    "и", "или", "для", "как", "что", "это", "все", "без", "под", "про", "где", "when",
    "быстро", "срочно", "дешево", "дешёво", "лучший", "лучшая", "лучшее", "новый", "новая",
    "онлайн", "купить", "цена", "цены", "недорого",
    "красный", "красная", "красные", "синий", "синяя", "синие", "белый", "белая", "белые",
    "черный", "чёрный", "черная", "чёрная", "черные", "чёрные", "зеленый", "зелёный",
}


def _intent_stems(key_text: str) -> set[str]:
    words = re.findall(r"[a-zа-яё]+", (key_text or "").lower())
    stems = set()
    for word in words:
        if len(word) < 4 or word in _INTENT_STOPWORDS:
            continue
        stem = _RU_STEMMER.stemWord(word)
        if len(stem) >= 3:
            stems.add(stem)
    return stems


def _related_intents(anchor_texts: list[str], all_intents: list[str]) -> list[str]:
    """Other key_texts that share at least one meaningful stem with any of anchor_texts —
    a *suggestion* surfaced in the UI (never silently applied) for "these other searches
    look like the same real-world topic, want to include them too?" (2026-10-22, see
    api_list_intents). Deliberately conservative: shared stopword-only/short overlaps
    don't count, to avoid chaining unrelated queries through a common filler word."""
    anchor_stems: set[str] = set()
    for text in anchor_texts:
        anchor_stems |= _intent_stems(text)
    if not anchor_stems:
        return []
    related = []
    anchor_lower = {t.strip().lower() for t in anchor_texts}
    for candidate in all_intents:
        if candidate.strip().lower() in anchor_lower:
            continue
        if _intent_stems(candidate) & anchor_stems:
            related.append(candidate)
    return related


# Below cosine similarity, a creative is considered off-topic for the active intent even
# if it was captured by a matching job. Calibrated 2026-10-23 on real data: the 5 pension-
# phrasing intents cluster at 0.77-0.87 with each other, "социальные выплаты" (a genuinely
# related but differently-worded topic) sits at 0.64-0.68, and the real false-positive
# that triggered this feature (a banquet-hall ad captured during a pension search) sits at
# 0.54-0.56 — this threshold keeps the first two groups and excludes the third.
SEMANTIC_INTENT_THRESHOLD = 0.62


def _get_cached_intent_embedding(db: sqlite3.Connection, key_text: str) -> list[float] | None:
    """Cache-only lookup, NEVER calls Gemini — this runs inside live request handling
    (api_list_creatives), which must stay fast. Computing a missing embedding is the
    background drain loop's job (_process_one_pending_embedding), not a query's."""
    key = key_text.strip().lower()
    if not key:
        return None
    row = db.execute("SELECT embedding FROM intent_embeddings WHERE key_text_lower=?", (key,)).fetchone()
    return emb.unpack_embedding(row["embedding"]) if row else None


def _semantic_relevant_creative_ids(db: sqlite3.Connection, intents: list[str]) -> set[int] | None:
    """Creative ids considered relevant to the active intent(s) (averaged embedding if
    several are selected), via TWO signals: (1) cosine similarity of a real Gemini
    embedding ≥ SEMANTIC_INTENT_THRESHOLD, or (2) the creative's own title+description
    literally stem-matches one of the active intents (catches obvious cases without
    depending on an embedding being computed yet). Rows with skip_reason set (keyword-
    matched its OWN capturing job, or a permanently failed embedding attempt — see
    _process_one_pending_embedding) are excluded from the embedding-based check, since
    their `embedding` column is a placeholder, not real data.
    Returns None if no selected intent has a cached embedding at all (Gemini hasn't
    gotten to it yet) — the caller falls back to job-based matching alone, see
    _semantic_intent_where. Never fails the request for individual creatives lacking
    an embedding; those are left for the job-based filter to decide."""
    vectors = [v for v in (_get_cached_intent_embedding(db, t) for t in intents) if v is not None]
    intent_stems: set[str] = set()
    for t in intents:
        intent_stems |= _intent_stems(t)
    if not vectors and not intent_stems:
        return None
    dim = len(vectors[0]) if vectors else 0
    avg = [sum(v[i] for v in vectors) / len(vectors) for i in range(dim)] if vectors else None
    rows = db.execute(
        "SELECT ce.creative_id, ce.embedding, c.title, c.description FROM creative_embeddings ce "
        "JOIN creatives c ON c.id = ce.creative_id WHERE ce.skip_reason IS NULL"
    ).fetchall()
    if not rows and not vectors:
        return None
    matched: set[int] = set()
    for row in rows:
        if avg is not None and emb.cosine_similarity(avg, emb.unpack_embedding(row["embedding"])) >= SEMANTIC_INTENT_THRESHOLD:
            matched.add(row["creative_id"])
            continue
        if intent_stems and (_intent_stems((row["title"] or "") + " " + (row["description"] or "")) & intent_stems):
            matched.add(row["creative_id"])
    return matched if vectors else (matched or None)


def _semantic_intent_where(db: sqlite3.Connection, intents: list[str], creative_alias: str = "c") -> tuple[str, list[str]] | None:
    """SQL WHERE fragment combining semantic relevance (primary) with job-based matching
    (fallback, only for creatives with no USABLE embedding — i.e. none at all, or a
    skip_reason placeholder) — see _semantic_relevant_creative_ids. Returns None when no
    usable signal exists for any selected intent, so the caller falls back to
    _intent_exists_sql alone. Uses a per-connection temp table instead of a huge
    `IN (...)` param list to stay well under SQLite's bound-parameter limit."""
    semantic_ids = _semantic_relevant_creative_ids(db, intents)
    if semantic_ids is None:
        return None
    db.execute("CREATE TEMP TABLE IF NOT EXISTS semantic_match_ids (creative_id INTEGER PRIMARY KEY)")
    db.execute("DELETE FROM temp.semantic_match_ids")
    db.executemany("INSERT OR IGNORE INTO temp.semantic_match_ids(creative_id) VALUES(?)", [(i,) for i in semantic_ids])
    job_sql = _intent_exists_sql(creative_alias, intents)
    where = (
        f"(EXISTS (SELECT 1 FROM temp.semantic_match_ids sm WHERE sm.creative_id={creative_alias}.id) "
        f"OR (NOT EXISTS (SELECT 1 FROM creative_embeddings ce WHERE ce.creative_id={creative_alias}.id AND ce.skip_reason IS NULL) AND {job_sql}))"
    )
    return where, list(intents)


def _creative_needs_semantic_check(db: sqlite3.Connection, creative_id: int, title: str | None, description: str | None) -> bool:
    """False if this creative's own text already stem-matches ANY of its capturing jobs'
    key_texts — job-based matching already correctly includes it for that same search
    (the only intent it's confirmed relevant to so far), so there's no need to spend a
    Gemini call on it. True (needs a real embedding) otherwise — this is exactly the
    "false positive" pattern that triggered building semantic matching at all: a creative
    captured under a search whose own text has nothing to do with that search's topic."""
    text_stems = _intent_stems((title or "") + " " + (description or ""))
    job_rows = db.execute(
        "SELECT DISTINCT j.key_text FROM occurrences o JOIN jobs j ON j.id=o.job_id WHERE o.creative_id=?",
        (creative_id,),
    ).fetchall()
    for row in job_rows:
        if text_stems & _intent_stems(row["key_text"]):
            return False
    return True


_last_gemini_alert_at = 0.0
_GEMINI_ALERT_COOLDOWN_SECONDS = 1800  # avoid spamming Telegram if the same hard failure repeats every drain-loop tick


def _gemini_alert(message: str) -> None:
    """Wired as compute_embedding_single's on_hard_failure (2026-10-23, Dima's request) —
    throttled so a persistent failure (e.g. a model genuinely removed) sends one Telegram
    alert per cooldown window, not one per failed item."""
    global _last_gemini_alert_at
    now = time.monotonic()
    if now - _last_gemini_alert_at < _GEMINI_ALERT_COOLDOWN_SECONDS:
        return
    _last_gemini_alert_at = now
    notify_telegram_alert(f"⚠️ Ошибка Gemini embeddings (семантический интент): {message}")


def _process_one_pending_embedding() -> bool:
    """One step of the background drain loop (_embedding_drain_loop): embeds exactly one
    missing intent (priority — rare, cheap, unblocks query-time matching fast) or one
    creative (newest first — ORDER BY id DESC, so live scraping gets covered before the
    historical backlog). Returns False only when there is genuinely nothing left to do
    (caller should idle-poll), never on a Gemini failure — a failure still marks forward
    progress (skip_reason='embedding_failed') so the loop can't get stuck retrying the
    same item forever."""
    with closing(connect()) as db:
        intent_row = db.execute(
            "SELECT MIN(j.key_text) AS key_text FROM jobs j "
            "LEFT JOIN intent_embeddings ie ON ie.key_text_lower = PYLOWER(j.key_text) "
            "WHERE j.key_text IS NOT NULL AND TRIM(j.key_text) <> '' AND ie.key_text_lower IS NULL "
            "GROUP BY PYLOWER(j.key_text) LIMIT 1"
        ).fetchone()
        if intent_row:
            key_text = intent_row["key_text"]
            result = emb.compute_embedding_single(key_text, on_hard_failure=_gemini_alert)
            if result:
                vector, model = result
                db.execute(
                    "INSERT INTO intent_embeddings(key_text_lower,display_text,embedding,model,computed_at) VALUES(?,?,?,?,?) "
                    "ON CONFLICT(key_text_lower) DO UPDATE SET embedding=excluded.embedding, model=excluded.model, computed_at=excluded.computed_at",
                    (key_text.strip().lower(), key_text, emb.pack_embedding(vector), model, now_iso()),
                )
                db.commit()
            return True

        row = db.execute(
            "SELECT c.id, c.title, c.description FROM creatives c "
            "LEFT JOIN creative_embeddings ce ON ce.creative_id=c.id "
            "WHERE ce.creative_id IS NULL AND (COALESCE(c.title,'')<>'' OR COALESCE(c.description,'')<>'') "
            "ORDER BY c.id DESC LIMIT 1"
        ).fetchone()
        if not row:
            return False

        now = now_iso()
        if not _creative_needs_semantic_check(db, row["id"], row["title"], row["description"]):
            db.execute(
                "INSERT INTO creative_embeddings(creative_id,embedding,model,computed_at,skip_reason) VALUES(?,?,?,?,?)",
                (row["id"], b"", "none", now, "keyword_match"),
            )
            db.commit()
            return True

        text = emb.creative_embedding_text(row["title"], row["description"]) or "реклама"
        result = emb.compute_embedding_single(text, on_hard_failure=_gemini_alert)
        if result:
            vector, model = result
            db.execute(
                "INSERT INTO creative_embeddings(creative_id,embedding,model,computed_at) VALUES(?,?,?,?)",
                (row["id"], emb.pack_embedding(vector), model, now),
            )
        else:
            db.execute(
                "INSERT INTO creative_embeddings(creative_id,embedding,model,computed_at,skip_reason) VALUES(?,?,?,?,?)",
                (row["id"], b"", "none", now, "embedding_failed"),
            )
        db.commit()
        return True


def _embedding_drain_loop() -> None:
    """Runs forever in a background thread (started at app startup, see
    _start_embedding_drain_thread) — the production ingestion path (2026-10-23, Dima's
    design): one creative/intent at a time, no batching, so a single rate-limit wait only
    delays the next item, never a whole batch. compute_embedding_single already waits out
    429s internally, so a quota exhaustion just makes this loop naturally slow down, not
    fail — no manual pacing/backoff tuning needed here."""
    while True:
        try:
            did_work = _process_one_pending_embedding()
        except Exception as exc:  # noqa: BLE001 - a background loop must never die
            print(f"[embeddings] drain loop error: {exc}", flush=True)
            did_work = False
        time.sleep(0.5 if did_work else 30)


def init_db() -> None:
    with closing(connect()) as db:
        db.execute("PRAGMA journal_mode=WAL")
        db.executescript("""
        CREATE TABLE IF NOT EXISTS jobs (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          status TEXT NOT NULL DEFAULT 'queued',
          city TEXT NOT NULL,
          key_text TEXT NOT NULL,
          created_at TEXT NOT NULL,
          started_at TEXT,
          finished_at TEXT,
          worker_id TEXT,
          exit_code INTEGER,
          log_text TEXT NOT NULL DEFAULT '',
          error_text TEXT,
          result_json TEXT,
          total_creatives INTEGER NOT NULL DEFAULT 0
        );
        CREATE INDEX IF NOT EXISTS ix_jobs_status_created ON jobs(status, created_at);
        CREATE TABLE IF NOT EXISTS workers (
          worker_id TEXT PRIMARY KEY,
          hostname TEXT,
          version TEXT,
          last_seen TEXT NOT NULL,
          state TEXT NOT NULL DEFAULT 'idle',
          current_job_id INTEGER,
          last_error TEXT
        );
        CREATE TABLE IF NOT EXISTS creatives (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          creative_hash TEXT NOT NULL UNIQUE,
          platform TEXT NOT NULL,
          domain TEXT,
          title TEXT,
          description TEXT,
          image_url TEXT,
          destination_url TEXT,
          source_page_url TEXT,
          detection_method TEXT,
          recognition_status TEXT NOT NULL DEFAULT 'recognized',
          captured_at TEXT NOT NULL,
          html_gzip BLOB,
          html_sha256 TEXT,
          first_job_id INTEGER,
          last_job_id INTEGER,
          FOREIGN KEY(first_job_id) REFERENCES jobs(id),
          FOREIGN KEY(last_job_id) REFERENCES jobs(id)
        );
        CREATE INDEX IF NOT EXISTS ix_creatives_platform ON creatives(platform);
        CREATE INDEX IF NOT EXISTS ix_creatives_captured ON creatives(captured_at);
        CREATE INDEX IF NOT EXISTS ix_creatives_status ON creatives(recognition_status);
        CREATE TABLE IF NOT EXISTS occurrences (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          job_id INTEGER NOT NULL,
          creative_id INTEGER NOT NULL,
          platform TEXT NOT NULL,
          source_page_url TEXT,
          detection_method TEXT,
          recognition_status TEXT NOT NULL DEFAULT 'recognized',
          observed_at TEXT NOT NULL,
          UNIQUE(job_id, creative_id, platform, source_page_url),
          FOREIGN KEY(job_id) REFERENCES jobs(id) ON DELETE CASCADE,
          FOREIGN KEY(creative_id) REFERENCES creatives(id) ON DELETE CASCADE
        );
        CREATE INDEX IF NOT EXISTS ix_occurrences_job ON occurrences(job_id);
        CREATE INDEX IF NOT EXISTS ix_occurrences_platform ON occurrences(platform);
        CREATE TABLE IF NOT EXISTS site_results (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          job_id INTEGER NOT NULL,
          platform TEXT NOT NULL,
          url TEXT,
          status TEXT NOT NULL DEFAULT 'unknown',
          duration_seconds REAL,
          scrolls_done INTEGER NOT NULL DEFAULT 0,
          ad_signal_detected INTEGER NOT NULL DEFAULT 0,
          unique_creatives INTEGER NOT NULL DEFAULT 0,
          partial_creatives INTEGER NOT NULL DEFAULT 0,
          detection_methods TEXT,
          dom_snapshot_path TEXT,
          error_text TEXT,
          UNIQUE(job_id, platform),
          FOREIGN KEY(job_id) REFERENCES jobs(id) ON DELETE CASCADE
        );
        CREATE INDEX IF NOT EXISTS ix_site_results_job ON site_results(job_id);
        CREATE INDEX IF NOT EXISTS ix_site_results_status ON site_results(status);
        -- Other clickable links/buttons of an ad (sitelinks, a second CTA) beyond the one
        -- "main" destination_url already on `creatives` — each with its own UTM, synced
        -- from ParsYa's parsya_creative_secondary_links (worker/agent.py's _sync_secondary_links).
        CREATE TABLE IF NOT EXISTS creative_secondary_links (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          creative_id INTEGER NOT NULL,
          link_text TEXT,
          tracking_url TEXT,
          final_url TEXT,
          final_url_domain TEXT,
          utm_source TEXT,
          utm_medium TEXT,
          utm_campaign TEXT,
          utm_content TEXT,
          utm_term TEXT,
          resolve_status TEXT,
          UNIQUE(creative_id, tracking_url),
          FOREIGN KEY(creative_id) REFERENCES creatives(id) ON DELETE CASCADE
        );
        CREATE INDEX IF NOT EXISTS ix_secondary_links_creative ON creative_secondary_links(creative_id);

        -- UTM of the creative's OWN (primary) destination_url — distinct from
        -- creative_secondary_links above (those are OTHER clickable links of the same ad).
        -- Synced from ParsYa's parsya_creative_destination_urls (worker/agent.py's
        -- _sync_creative_utm), one row per creative (latest resolve wins), independent of
        -- advertiser disclosure — UTM exists whether or not AdTune ever revealed a name/ИНН.
        CREATE TABLE IF NOT EXISTS creative_utm (
          creative_id INTEGER PRIMARY KEY,
          final_url TEXT,
          final_url_domain TEXT,
          utm_source TEXT,
          utm_medium TEXT,
          utm_campaign TEXT,
          utm_content TEXT,
          utm_term TEXT,
          resolve_status TEXT,
          resolved_at TEXT,
          FOREIGN KEY(creative_id) REFERENCES creatives(id) ON DELETE CASCADE
        );
        CREATE INDEX IF NOT EXISTS ix_creative_utm_campaign ON creative_utm(utm_campaign);
        CREATE INDEX IF NOT EXISTS ix_creative_utm_term ON creative_utm(utm_term);

        -- Semantic intent matching (2026-10-23): a creative can be captured under a search
        -- job ("Увеличение пенсии") whose own ad text has nothing to do with that search —
        -- RSYA targets audiences, not page content, so job-based intent scoping alone lets
        -- through unrelated ads (confirmed real example: a banquet-hall ad captured during
        -- a pension search). These tables cache Gemini embeddings (gemini-embedding-001,
        -- see embeddings.py) so creative-vs-intent *content* relevance can be computed
        -- without a live API call on every request. embedding is a packed float32 BLOB.
        CREATE TABLE IF NOT EXISTS creative_embeddings (
          creative_id INTEGER PRIMARY KEY,
          embedding BLOB NOT NULL,
          model TEXT NOT NULL,
          computed_at TEXT NOT NULL,
          FOREIGN KEY(creative_id) REFERENCES creatives(id) ON DELETE CASCADE
        );
        CREATE TABLE IF NOT EXISTS intent_embeddings (
          key_text_lower TEXT PRIMARY KEY,
          display_text TEXT NOT NULL,
          embedding BLOB NOT NULL,
          model TEXT NOT NULL,
          computed_at TEXT NOT NULL
        );

        -- Advertiser-disclosure coverage %, grouped by platform/detection_method/creative_type.
        -- Mirrors MySQL's parsya_advertiser_coverage_report VIEW (global aggregate, not
        -- scoped to one job/run) — fully replaced on every sync, not accumulated.
        CREATE TABLE IF NOT EXISTS advertiser_coverage_report (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          platform TEXT,
          detection_method TEXT,
          creative_type TEXT,
          total_creatives INTEGER NOT NULL DEFAULT 0,
          disclosed_creatives INTEGER NOT NULL DEFAULT 0,
          disclosed_pct REAL NOT NULL DEFAULT 0,
          synced_at TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS ix_coverage_platform ON advertiser_coverage_report(platform);

        -- Landing-page/UTM change history per creative — one row per DISTINCT change
        -- (deduped upstream by change_signature in parsya_creative_destination_history),
        -- append-only: a creative whose campaign/landing page changes over time gets a new
        -- row here each time, not an overwrite. Dima (2026-10-19): "да, важно видеть, как
        -- менялась кампания". Deliberately NOT synced until there was real accumulated
        -- history to show (empty-state UI isn't worth building against) — now there is.
        CREATE TABLE IF NOT EXISTS creative_destination_history (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          creative_id INTEGER NOT NULL,
          change_signature TEXT NOT NULL,
          tracking_url TEXT,
          final_url TEXT,
          final_url_domain TEXT,
          utm_source TEXT,
          utm_medium TEXT,
          utm_campaign TEXT,
          utm_content TEXT,
          utm_term TEXT,
          first_seen_at TEXT,
          last_seen_at TEXT,
          UNIQUE(creative_id, change_signature),
          FOREIGN KEY(creative_id) REFERENCES creatives(id) ON DELETE CASCADE
        );
        CREATE INDEX IF NOT EXISTS ix_dest_history_creative ON creative_destination_history(creative_id);

        -- Live remote-captcha-solving channel: the parser subprocess posts a screenshot
        -- here roughly once a second while a CAPTCHA blocks it, an admin viewing the run
        -- queues one command (click/key/text/scroll/refresh) at a time, and the parser
        -- polls and applies it. Polling-based by design, matching the rest of this app.
        CREATE TABLE IF NOT EXISTS browser_sessions (
          job_id INTEGER PRIMARY KEY,
          active INTEGER NOT NULL DEFAULT 0,
          captcha_required INTEGER NOT NULL DEFAULT 0,
          url TEXT,
          title TEXT,
          screenshot_b64 TEXT,
          viewport_width INTEGER,
          viewport_height INTEGER,
          updated_at TEXT NOT NULL,
          pending_command_json TEXT,
          FOREIGN KEY(job_id) REFERENCES jobs(id) ON DELETE CASCADE
        );

        -- User-curated collections ("Подборки") of saved creatives. An item is keyed by
        -- content_fingerprint when known (so saving one platform variant of a creative
        -- shows it saved everywhere that creative appears), falling back to creative_hash.
        CREATE TABLE IF NOT EXISTS collections (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          owner_user_id INTEGER NOT NULL,
          name TEXT NOT NULL,
          description TEXT,
          created_at TEXT NOT NULL,
          FOREIGN KEY(owner_user_id) REFERENCES users(id) ON DELETE CASCADE
        );
        CREATE INDEX IF NOT EXISTS ix_collections_owner ON collections(owner_user_id);
        CREATE TABLE IF NOT EXISTS collection_items (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          collection_id INTEGER NOT NULL,
          group_key TEXT NOT NULL,
          representative_creative_id INTEGER NOT NULL,
          added_at TEXT NOT NULL,
          UNIQUE(collection_id, group_key),
          FOREIGN KEY(collection_id) REFERENCES collections(id) ON DELETE CASCADE,
          FOREIGN KEY(representative_creative_id) REFERENCES creatives(id) ON DELETE CASCADE
        );
        CREATE INDEX IF NOT EXISTS ix_collection_items_collection ON collection_items(collection_id);

        -- Advertiser identity (ИНН) and the links that tie a creative to it. Populated by
        -- the WSL worker from ParsYa's advertiser-enrichment tables (parsya_advertiser_*).
        CREATE TABLE IF NOT EXISTS advertisers (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          identity_key TEXT NOT NULL UNIQUE,
          inn TEXT,
          canonical_name TEXT,
          first_seen_at TEXT NOT NULL,
          last_seen_at TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS ix_advertisers_inn ON advertisers(inn);

        CREATE TABLE IF NOT EXISTS creative_advertiser_links (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          creative_id INTEGER NOT NULL,
          advertiser_id INTEGER NOT NULL,
          association_method TEXT NOT NULL,
          confidence REAL,
          first_seen_at TEXT NOT NULL,
          last_seen_at TEXT NOT NULL,
          UNIQUE(creative_id, advertiser_id, association_method),
          FOREIGN KEY(creative_id) REFERENCES creatives(id) ON DELETE CASCADE,
          FOREIGN KEY(advertiser_id) REFERENCES advertisers(id) ON DELETE CASCADE
        );
        CREATE INDEX IF NOT EXISTS ix_cal_advertiser ON creative_advertiser_links(advertiser_id);
        CREATE INDEX IF NOT EXISTS ix_cal_creative ON creative_advertiser_links(creative_id);

        -- campaign_id / banner_id / client_id / creative_id observed for a creative, so the
        -- same visual/text creative served under several ad-platform campaigns stays one
        -- row in `creatives` while every campaign run it appeared in is still queryable.
        CREATE TABLE IF NOT EXISTS creative_identifiers (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          creative_id INTEGER NOT NULL,
          identifier_type TEXT NOT NULL,
          identifier_value TEXT NOT NULL,
          first_seen_at TEXT NOT NULL,
          last_seen_at TEXT NOT NULL,
          UNIQUE(creative_id, identifier_type, identifier_value),
          FOREIGN KEY(creative_id) REFERENCES creatives(id) ON DELETE CASCADE
        );
        CREATE INDEX IF NOT EXISTS ix_ci_creative ON creative_identifiers(creative_id);
        CREATE INDEX IF NOT EXISTS ix_ci_value ON creative_identifiers(identifier_type, identifier_value);

        -- Marketer-facing SaaS accounts. Separate from the single env-configured
        -- PARSYA_ADMIN_USER/PASSWORD above, which remains the ops console login and is
        -- untouched by this table. An admin-role row here manages these accounts from
        -- /admin/users; it does not grant access to the ops console itself.
        CREATE TABLE IF NOT EXISTS users (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          email TEXT NOT NULL UNIQUE,
          password_hash TEXT NOT NULL,
          role TEXT NOT NULL DEFAULT 'marketer',
          is_active INTEGER NOT NULL DEFAULT 1,
          created_at TEXT NOT NULL,
          last_login_at TEXT
        );
        CREATE TABLE IF NOT EXISTS user_tokens (
          token_hash TEXT PRIMARY KEY,
          user_id INTEGER NOT NULL,
          created_at TEXT NOT NULL,
          expires_at TEXT NOT NULL,
          FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE
        );
        CREATE INDEX IF NOT EXISTS ix_user_tokens_user ON user_tokens(user_id);

        -- In-app notification center (2026-11-06). One row per event per recipient; a
        -- user's own notification_prefs (on the users table) gate whether a given type
        -- gets inserted at all, so this table never accumulates rows for muted types.
        CREATE TABLE IF NOT EXISTS notifications (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          user_id INTEGER NOT NULL,
          type TEXT NOT NULL,
          title TEXT NOT NULL,
          text TEXT,
          link TEXT,
          created_at TEXT NOT NULL,
          read_at TEXT,
          FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE
        );
        CREATE INDEX IF NOT EXISTS ix_notifications_user ON notifications(user_id, created_at DESC);
        """)
        # Profile fields added 2026-11-06 (display name, avatar as a small data: URL —
        # avatars are resized client-side to a tiny square before upload, so storing them
        # inline keeps this simple without a static-file-serving route; notification_prefs
        # is a JSON object of {type: bool}, missing/absent type defaults to enabled).
        existing_user_cols = {row["name"] for row in db.execute("PRAGMA table_info(users)").fetchall()}
        for col, coltype in (("name", "TEXT"), ("avatar_data_url", "TEXT"), ("notification_prefs", "TEXT")):
            if col not in existing_user_cols:
                db.execute(f"ALTER TABLE users ADD COLUMN {col} {coltype}")
        # content_fingerprint groups creative rows that are the same image/text/title
        # creative re-detected under a different creative_hash (e.g. a different platform
        # markup or a different campaign run). ALTER TABLE ADD COLUMN has no IF NOT EXISTS
        # in SQLite, so guard it with a pragma check to stay idempotent across restarts.
        existing_cols = {row["name"] for row in db.execute("PRAGMA table_info(creatives)").fetchall()}
        if "content_fingerprint" not in existing_cols:
            db.execute("ALTER TABLE creatives ADD COLUMN content_fingerprint TEXT")
        db.execute("CREATE INDEX IF NOT EXISTS ix_creatives_fingerprint ON creatives(content_fingerprint)")
        # Extra per-creative signals (media format, CTA button, price/discount, app-store ids)
        # synced from ParsYa's own enrichment tables — see worker/agent.py's
        # _sync_creative_extras. None of this existed when the table was first created.
        for col, coltype in (
            ("creative_type", "TEXT"), ("video_src", "TEXT"), ("video_poster", "TEXT"),
            ("video_duration", "TEXT"), ("cta_text", "TEXT"), ("price_text", "TEXT"),
            ("price_rub", "REAL"), ("discount_percent", "REAL"), ("erid", "TEXT"),
            ("app_package_id", "TEXT"), ("app_store_id", "TEXT"),
            ("carousel_id", "TEXT"), ("carousel_slide_index", "INTEGER"),
        ):
            if col not in existing_cols:
                db.execute(f"ALTER TABLE creatives ADD COLUMN {col} {coltype}")
        db.execute("CREATE INDEX IF NOT EXISTS ix_creatives_carousel ON creatives(carousel_id)")
        # owner_user_id lets a SaaS user (see `users`) self-serve: launch their own runs from
        # the frontend and only ever see their own jobs. NULL means an ops-console run created
        # from the admin HTML pages, not tied to any SaaS account.
        job_cols = {row["name"] for row in db.execute("PRAGMA table_info(jobs)").fetchall()}
        if "owner_user_id" not in job_cols:
            db.execute("ALTER TABLE jobs ADD COLUMN owner_user_id INTEGER REFERENCES users(id)")
        db.execute("CREATE INDEX IF NOT EXISTS ix_jobs_owner ON jobs(owner_user_id)")
        # skip_reason (2026-10-23): a creative whose own capturing job's key_text already
        # keyword/stem-matches its title+description doesn't need a real Gemini embedding —
        # job-based matching already correctly includes it for that same search, and that's
        # the only intent it's confirmed relevant to anyway. Such rows get embedding=b''
        # (placeholder, keeps the NOT NULL constraint) and skip_reason set, so the drain
        # loop (_process_one_pending_embedding) never re-considers them, while query-time
        # code (_semantic_relevant_creative_ids) treats skip_reason-marked rows as "no real
        # embedding" and falls back to job-based matching alone for them.
        ce_cols = {row["name"] for row in db.execute("PRAGMA table_info(creative_embeddings)").fetchall()}
        if "skip_reason" not in ce_cols:
            db.execute("ALTER TABLE creative_embeddings ADD COLUMN skip_reason TEXT")
        # landing_domain_mismatch (2026-10-22): the advertiser's OWN declared site domain
        # (from the resolved Yandex/VK advertiser profile) vs. the domain the ad actually
        # lands on — a real divergence signal (e.g. renting someone else's landing page, an
        # affiliate funnel, or just an app-install ad landing on apps.apple.com instead of a
        # website). Mirrors MySQL's parsya_creative_full view, which already computes this;
        # queued since 2026-10-18 as "always 0 in practice" back then — confirmed now showing
        # 118 real mismatches out of ~3950 rows, worth exposing. See
        # worker/agent.py's _sync_landing_mismatch.
        utm_cols = {row["name"] for row in db.execute("PRAGMA table_info(creative_utm)").fetchall()}
        for col, coltype in (("advertised_site_domain", "TEXT"), ("landing_domain_mismatch", "INTEGER")):
            if col not in utm_cols:
                db.execute(f"ALTER TABLE creative_utm ADD COLUMN {col} {coltype}")
        db.execute("CREATE INDEX IF NOT EXISTS ix_creative_utm_mismatch ON creative_utm(landing_domain_mismatch)")
        # Advertiser contact info (address/phone/own declared site) from their resolved
        # Yandex/VK profile — real data (confirmed 2026-10-22: 197/1422 profiles have an
        # address, 194/1422 a phone), genuinely useful competitive-intel for a marketer
        # beyond just the name+ИНН already shown.
        advertiser_cols = {row["name"] for row in db.execute("PRAGMA table_info(advertisers)").fetchall()}
        for col in ("address", "phone", "website_url"):
            if col not in advertiser_cols:
                db.execute(f"ALTER TABLE advertisers ADD COLUMN {col} TEXT")
        db.commit()


init_db()
threading.Thread(target=_embedding_drain_loop, daemon=True, name="embedding-drain").start()


# --- Расписание повторных сканирований (2026-11-13) ---------------------------
# «N раз в день в течение 7/14/30 дней». Планировщик (тред ниже) раз в 30 с ставит в
# очередь обычные jobs для расписаний, у которых наступил next_run_at; воркер забирает
# их как любые другие. Один и тот же key_text => воркер выдаёт тот же аккаунт Яндекса.
SCHEDULE_DAYS = (7, 14, 30)


def init_schedules() -> None:
    with closing(connect()) as db:
        db.execute("""CREATE TABLE IF NOT EXISTS scan_schedules (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          owner_user_id INTEGER NOT NULL,
          city TEXT NOT NULL,
          key_text TEXT NOT NULL,
          runs_per_day INTEGER NOT NULL,
          duration_days INTEGER NOT NULL,
          status TEXT NOT NULL DEFAULT 'active',
          created_at TEXT NOT NULL,
          end_at TEXT NOT NULL,
          next_run_at TEXT,
          runs_created INTEGER NOT NULL DEFAULT 0,
          runs_skipped INTEGER NOT NULL DEFAULT 0,
          finished_at TEXT
        )""")
        db.execute("CREATE INDEX IF NOT EXISTS ix_sched_status_next ON scan_schedules(status, next_run_at)")
        job_cols = {row["name"] for row in db.execute("PRAGMA table_info(jobs)").fetchall()}
        if "schedule_id" not in job_cols:
            db.execute("ALTER TABLE jobs ADD COLUMN schedule_id INTEGER")
        if "attempts" not in job_cols:
            db.execute("ALTER TABLE jobs ADD COLUMN attempts INTEGER NOT NULL DEFAULT 0")
        if "retry_at" not in job_cols:
            db.execute("ALTER TABLE jobs ADD COLUMN retry_at TEXT")
        db.execute("CREATE INDEX IF NOT EXISTS ix_jobs_schedule ON jobs(schedule_id)")
        db.commit()


init_schedules()


def _sched_tick() -> None:
    now = datetime.now(timezone.utc)
    with closing(connect()) as db:
        db.execute("BEGIN IMMEDIATE")
        due = db.execute(
            "SELECT * FROM scan_schedules WHERE status='active' AND next_run_at IS NOT NULL AND next_run_at<=?",
            (now.replace(microsecond=0).isoformat(),),
        ).fetchall()
        for sc in due:
            interval = timedelta(hours=24 / sc["runs_per_day"])
            end_at = datetime.fromisoformat(sc["end_at"])
            busy = db.execute(
                "SELECT 1 FROM jobs WHERE schedule_id=? AND status IN ('queued','running') LIMIT 1", (sc["id"],)
            ).fetchone()
            planned = datetime.fromisoformat(sc["next_run_at"])
            created, skipped = sc["runs_created"], sc["runs_skipped"]
            if busy:
                # Предыдущий запуск ещё идёт — слот пропускаем, не копим очередь.
                skipped += 1
            else:
                db.execute(
                    "INSERT INTO jobs(status,city,key_text,created_at,owner_user_id,schedule_id) VALUES('queued',?,?,?,?,?)",
                    (sc["city"], sc["key_text"], now_iso(), sc["owner_user_id"], sc["id"]),
                )
                created += 1
            nxt = planned + interval
            while nxt <= now:  # после простоя сервиса не догоняем пачкой
                nxt += interval
                skipped += 1
            if nxt >= end_at:
                db.execute(
                    "UPDATE scan_schedules SET status='finished',next_run_at=NULL,finished_at=?,runs_created=?,runs_skipped=? WHERE id=?",
                    (now_iso(), created, skipped, sc["id"]),
                )
            else:
                db.execute(
                    "UPDATE scan_schedules SET next_run_at=?,runs_created=?,runs_skipped=? WHERE id=?",
                    (nxt.replace(microsecond=0).isoformat(), created, skipped, sc["id"]),
                )
        db.commit()


def _scheduler_loop() -> None:
    while True:
        try:
            _sched_tick()
        except Exception as exc:  # noqa: BLE001 - фоновый цикл не должен падать
            print(f"[scheduler] error: {exc}", flush=True)
        time.sleep(30)


threading.Thread(target=_scheduler_loop, daemon=True, name="scan-scheduler").start()


# --- Интент креатива (Gemini), 2026-11-13 ---------------------------------------
# creatives.ai_intent — о чём реклама НА САМОМ ДЕЛЕ (определяет Gemini по заголовку и
# тексту). Не путать с ключевым словом запуска (jobs.key_text) — по какому запросу
# парсер искал. См. app/ai_intent.py.
def init_ai_intent() -> None:
    with closing(connect()) as db:
        cols = {row["name"] for row in db.execute("PRAGMA table_info(creatives)").fetchall()}
        for col, typ in (("ai_intent", "TEXT"), ("ai_intent_at", "TEXT"), ("ai_intent_model", "TEXT")):
            if col not in cols:
                db.execute(f"ALTER TABLE creatives ADD COLUMN {col} {typ}")
        db.execute("CREATE INDEX IF NOT EXISTS ix_creatives_ai_intent ON creatives(ai_intent)")
        # Решения Gemini «подходит ли интент креатива под ключевое слово запуска» (кэш пар).
        db.execute("""CREATE TABLE IF NOT EXISTS intent_keyword_match (
          keyword_key TEXT NOT NULL, intent_key TEXT NOT NULL, match INTEGER NOT NULL,
          model TEXT, judged_at TEXT, PRIMARY KEY(keyword_key, intent_key))""")
        # Креативы совсем без текста классифицировать нечем — помечаем сразу, без вызова Gemini.
        db.execute(
            "UPDATE creatives SET ai_intent=?, ai_intent_at=?, ai_intent_model='no-text' "
            "WHERE ai_intent IS NULL AND COALESCE(title,'')='' AND COALESCE(description,'')=''",
            (aii.UNKNOWN, now_iso()),
        )
        db.commit()


init_ai_intent()
_ai_intent_fail_counts: dict[int, int] = {}


_judge_backoff: dict[str, float] = {}
_judge_missing: dict[tuple[str, str], int] = {}


def _judge_pending_matches() -> bool:
    """Gemini решает, подходит ли ИНТЕНТ креатива под ключевое слово запуска (пары кэшируются в
    intent_keyword_match). Одна пачка = одно ключевое слово + до 40 его ещё не оценённых интентов."""
    with closing(connect()) as db:
        rows = db.execute(
            "SELECT j.key_text AS kw, MIN(c.ai_intent) AS intent FROM jobs j "
            "JOIN occurrences o ON o.job_id=j.id JOIN creatives c ON c.id=o.creative_id "
            "WHERE c.ai_intent IS NOT NULL AND c.ai_intent<>? AND j.key_text IS NOT NULL AND TRIM(j.key_text)<>'' "
            "AND NOT EXISTS (SELECT 1 FROM intent_keyword_match m WHERE m.keyword_key=PYLOWER(j.key_text) "
            "AND m.intent_key=PYLOWER(c.ai_intent)) GROUP BY PYLOWER(j.key_text), PYLOWER(c.ai_intent) "
            "ORDER BY MAX(j.id) DESC LIMIT 400", (aii.UNKNOWN,),
        ).fetchall()
        now_m = time.monotonic()
        by_kw: dict[str, list[str]] = {}
        for r in rows:
            kw = r["kw"]
            if _judge_backoff.get(_pylower(kw), 0) > now_m:
                continue
            if _intent_stems(kw) & _intent_stems(r["intent"]):
                continue  # совпадение основы считаем без Gemini
            by_kw.setdefault(kw, []).append(r["intent"])
        if not by_kw:
            return False
        kw, intents = next(iter(by_kw.items()))
        intents = intents[:40]
        result = aii.judge_intent_matches(kw, intents, on_hard_failure=_gemini_alert)
        if result is None:
            _judge_backoff[_pylower(kw)] = time.monotonic() + 600
            return False
        stamp = now_iso()
        for it in intents:
            k = it.strip().lower()
            if k in result:
                db.execute("INSERT OR REPLACE INTO intent_keyword_match(keyword_key,intent_key,match,model,judged_at) VALUES(?,?,?,?,?)",
                           (_pylower(kw), k, 1 if result[k] else 0, aii.last_model_used, stamp))
                _judge_missing.pop((_pylower(kw), k), None)
            else:
                n = _judge_missing.get((_pylower(kw), k), 0) + 1
                _judge_missing[(_pylower(kw), k)] = n
                if n >= 2:  # модель дважды «забыла» интент — считаем неподходящим, чтобы не зациклиться
                    db.execute("INSERT OR REPLACE INTO intent_keyword_match(keyword_key,intent_key,match,model,judged_at) VALUES(?,?,0,?,?)",
                               (_pylower(kw), k, "missing", stamp))
        db.commit()
        return True


def _process_ai_intent_batch() -> bool:
    """Один шаг фонового цикла: до BATCH_SIZE неразмеченных креативов (новые первыми) — один
    запрос к Gemini на пачку. True, если что-то обработали. При сбое пачки пробуем по одному;
    креатив, который 3 раза подряд не размечается, до перезапуска сервиса пропускаем.
    Сначала — оценка пар «ключевое слово → интент» (их мало, а от них зависят цифры «по интенту»)."""
    if _judge_pending_matches():
        return True
    with closing(connect()) as db:
        skip = [i for i, n in _ai_intent_fail_counts.items() if n >= 3]
        skip_sql = f" AND id NOT IN ({','.join('?' * len(skip))})" if skip else ""
        rows = db.execute(
            "SELECT id,title,description FROM creatives WHERE ai_intent IS NULL"
            + skip_sql + " ORDER BY id DESC LIMIT ?", skip + [aii.BATCH_SIZE],
        ).fetchall()
        if not rows:
            return False
        known = [r["ai_intent"] for r in db.execute(
            "SELECT ai_intent FROM creatives WHERE ai_intent IS NOT NULL AND ai_intent<>? "
            "GROUP BY ai_intent ORDER BY COUNT(*) DESC LIMIT ?", (aii.UNKNOWN, aii.MAX_KNOWN)).fetchall()]
        items = [dict(r) for r in rows]
        result = aii.classify_batch(items, known, on_hard_failure=_gemini_alert)
        if result is None and len(items) > 1:
            result = {}
            for it in items:
                one = aii.classify_batch([it], known, on_hard_failure=None)
                if one:
                    result.update(one)
        result = result or {}
        now = now_iso()
        for it in items:
            intent = result.get(it["id"])
            if intent:
                db.execute("UPDATE creatives SET ai_intent=?, ai_intent_at=?, ai_intent_model=? WHERE id=?",
                           (intent, now, aii.last_model_used, it["id"]))
                _ai_intent_fail_counts.pop(it["id"], None)
            else:
                _ai_intent_fail_counts[it["id"]] = _ai_intent_fail_counts.get(it["id"], 0) + 1
        db.commit()
        return bool(result)


def _ai_intent_loop() -> None:
    time.sleep(20)
    while True:
        try:
            did = _process_ai_intent_batch()
        except Exception as exc:  # noqa: BLE001 - фоновый цикл не должен падать
            print(f"[ai-intent] loop error: {exc}", flush=True)
            did = False
            time.sleep(60)
        time.sleep(4 if did else 30)


if os.getenv("PARSYA_AI_INTENT_ENABLED", "1").strip().lower() not in {"0", "false", "no", "off"}:
    threading.Thread(target=_ai_intent_loop, daemon=True, name="ai-intent").start()


def esc(value: Any) -> str:
    text = "" if value is None else str(value)
    return (text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
            .replace('"', "&quot;").replace("'", "&#39;"))


def auth_page(request: Request) -> bool:
    if request.session.get("authenticated"):
        return True
    # Auto-login when reached via addAvto's own proxy: that route sits
    # behind addAvto's Basic Auth (same login/password), so getting here
    # at all already proves the visitor authenticated there — showing
    # this app's OWN separate login screen on top would just be a second,
    # confusing password prompt for no real security gain (2026-11-03,
    # Dima: "авторизация по сути не нужна"). SAFE only because
    # nginx-parser.conf (this app's native domain) always overwrites
    # X-Forwarded-Prefix to empty — a client can't forge this header to
    # self-authenticate there directly.
    if P():
        request.session["authenticated"] = True
        request.session["username"] = "addavto"
        request.session.setdefault("csrf", secrets.token_urlsafe(32))
        return True
    return False


def require_admin(request: Request) -> None:
    if not auth_page(request):
        raise HTTPException(status_code=303, headers={"Location": P("/login")})


def csrf_token(request: Request) -> str:
    token = request.session.get("csrf")
    if not token:
        token = secrets.token_urlsafe(32)
        request.session["csrf"] = token
    return str(token)


def check_csrf(request: Request, token: str) -> None:
    expected = str(request.session.get("csrf") or "")
    if not expected or not hmac.compare_digest(expected, token):
        raise HTTPException(status_code=403, detail="Invalid CSRF token")


def page(request: Request, title: str, body: str, active: str = "") -> HTMLResponse:
    user = request.session.get("username", "admin")
    nav = ""
    if auth_page(request):
        links = [
            ("/dashboard", "Обзор", "dashboard", "layout-grid"),
            ("/runs", "Запуски", "runs", "radar"),
            ("/creatives", "Креативы", "creatives", "layers"),
            ("/advertisers", "Рекламодатели", "advertisers", "building-2"),
            ("/new-run", "Новый запуск", "new-run", "plus"),
            ("/admin/users", "Пользователи SaaS", "users", "users"),
        ]
        nav = '<aside class="sidebar"><div class="brand"><span class="brand-mark">P</span><div><b>ParsYa</b><small>CONTROL PLANE</small></div></div><nav>'
        # Открыто как вкладка внутри addAvto (/parser/...) — явная ссылка
        # назад, как у "Конверсии в Метрику" (lk.html: "← Рекламные
        # кампании"), а не только надежда на кнопку "назад" браузера.
        if P():
            nav += '<a class="nav-back" href="/">← Рекламные кампании</a>'
        nav += "".join(
            f'<a class="{ "active" if key == active else "" }" href="{P(href)}"><i data-lucide="{icon}"></i>{label}</a>'
            for href, label, key, icon in links
        )
        nav += '</nav><div class="side-bottom"><span class="dot"></span> ' + esc(user) + '<form method="post" action="' + P("/logout") + '"><input type="hidden" name="csrf_token" value="' + esc(csrf_token(request)) + '"><button class="logout">Выйти</button></form></div></aside>'
    crumb_label = next((label for _, label, key in [("", "Обзор", "dashboard"), ("", "Запуски", "runs"), ("", "Креативы", "creatives"), ("", "Новый запуск", "new-run"), ("", "Пользователи SaaS", "users")] if key == active), title)
    html = f"""<!doctype html>
<html lang="ru"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>{esc(title)} · ParsYa</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=DM+Sans:wght@400;500;600;700&family=Manrope:wght@400;500;600;700;800&display=swap" rel="stylesheet">
<script src="https://unpkg.com/lucide@latest"></script>
<style>
{DESIGN_TOKENS_CSS}
</style></head><body><div class="shell">{nav}<main class="main"><header class="topbar"><div style="display:flex;align-items:center;gap:12px"><div class="crumb">ParsYa <span style="margin:0 7px;color:#c5cad1">/</span><strong>{esc(crumb_label)}</strong></div></div><div class="top-actions"><div class="profiledot">{esc((user or "A")[0].upper())}</div></div></header><div class="content"><section class="headline"><div><div class="eyebrow">CONTROL PLANE</div><h1 class="h1">{esc(title)}</h1><div class="subtitle">Удалённое управление ParsYa · воркер подключается к серверу исходящим HTTPS</div></div>{'<a class="primary" href="' + P('/new-run') + '"><i data-lucide="plus"></i><span>Новый запуск</span></a>' if auth_page(request) and active != 'new-run' else ''}</section>{body}<div class="footnote">ParsYa Control Plane · HTML креативов отображается в изолированном iframe без выполнения скриптов.</div></div></main></div>
<script>if(window.lucide)lucide.createIcons();</script>
</body></html>"""
    return HTMLResponse(html)


def admin_or_redirect(request: Request):
    if not auth_page(request):
        return RedirectResponse(P("/login"), status_code=303)
    return None


def worker_auth(authorization: str | None) -> None:
    if not WORKER_TOKEN or not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Worker authentication required")
    provided = authorization[7:].strip()
    if not hmac.compare_digest(provided, WORKER_TOKEN):
        raise HTTPException(status_code=401, detail="Invalid worker token")


def addavto_auth(authorization: str | None) -> None:
    if not ADDAVTO_TOKEN or not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="addAvto authentication required")
    provided = authorization[7:].strip()
    if not hmac.compare_digest(provided, ADDAVTO_TOKEN):
        raise HTTPException(status_code=401, detail="Invalid addAvto token")


def _yandex_pool_mysql_config() -> dict[str, Any]:
    password = os.getenv("PARSYA_DB_PASSWORD", "")
    if not password:
        raise HTTPException(status_code=503, detail="PARSYA_DB_PASSWORD не задан на сервере — пул недоступен")
    return {
        "host": os.getenv("PARSYA_DB_HOST", "localhost"),
        "port": int(os.getenv("PARSYA_DB_PORT", "3306")),
        "user": os.getenv("PARSYA_DB_USER", "parsya"),
        "password": password,
        "database": os.getenv("PARSYA_DB_NAME", "Data_Science"),
        "charset": "utf8mb4",
        "connection_timeout": int(os.getenv("PARSYA_DB_CONNECT_TIMEOUT", "10")),
    }


PBKDF2_ITERATIONS = 310_000


def hash_password(password: str) -> str:
    salt = secrets.token_bytes(16)
    digest = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, PBKDF2_ITERATIONS)
    return f"{salt.hex()}:{digest.hex()}"


def verify_password(password: str, stored: str) -> bool:
    try:
        salt_hex, digest_hex = stored.split(":", 1)
        salt = bytes.fromhex(salt_hex)
    except (ValueError, AttributeError):
        return False
    candidate = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, PBKDF2_ITERATIONS)
    return hmac.compare_digest(candidate.hex(), digest_hex)


def issue_user_token(db: sqlite3.Connection, user_id: int, days_valid: int = 30) -> str:
    token = secrets.token_urlsafe(32)
    token_hash = hashlib.sha256(token.encode("utf-8")).hexdigest()
    now = datetime.now(timezone.utc)
    expires = now.replace(microsecond=0) + timedelta(days=days_valid)
    db.execute(
        "INSERT INTO user_tokens(token_hash,user_id,created_at,expires_at) VALUES(?,?,?,?)",
        (token_hash, user_id, now.isoformat(), expires.isoformat()),
    )
    return token


def resolve_user_token(token: str) -> sqlite3.Row | None:
    token_hash = hashlib.sha256(token.encode("utf-8")).hexdigest()
    with closing(connect()) as db:
        row = db.execute(
            "SELECT u.id,u.email,u.role,u.is_active,t.expires_at FROM user_tokens t "
            "JOIN users u ON u.id=t.user_id WHERE t.token_hash=?",
            (token_hash,),
        ).fetchone()
        if not row or not row["is_active"]:
            return None
        if row["expires_at"] < now_iso():
            db.execute("DELETE FROM user_tokens WHERE token_hash=?", (token_hash,))
            db.commit()
            return None
        return row


def public_api_principal(
    x_api_key: str | None = Header(default=None, alias="X-API-Key"),
    authorization: str | None = Header(default=None),
) -> dict[str, Any]:
    """Accepts either the operator/server-to-server X-API-Key, or a per-user bearer
    token issued by /api/v1/auth/login for the marketer-facing frontend."""
    if authorization and authorization.startswith("Bearer "):
        user = resolve_user_token(authorization[7:].strip())
        if not user:
            raise HTTPException(status_code=401, detail="Invalid or expired session token")
        return {"kind": "user", "user_id": user["id"], "email": user["email"], "role": user["role"]}
    if PUBLIC_API_TOKEN and x_api_key and hmac.compare_digest(x_api_key, PUBLIC_API_TOKEN):
        return {"kind": "api_key"}
    if not PUBLIC_API_TOKEN and not x_api_key and not authorization:
        raise HTTPException(status_code=503, detail="Public API is not configured")
    raise HTTPException(status_code=401, detail="Invalid credentials: provide X-API-Key or Authorization: Bearer <token>")


class PollIn(BaseModel):
    worker_id: str = Field(min_length=1, max_length=120)
    hostname: str = Field(default="", max_length=255)
    version: str = Field(default="1.0.0", max_length=40)


class LogIn(BaseModel):
    text: str = Field(min_length=1, max_length=30000)


class FinishIn(BaseModel):
    status: str = Field(pattern="^(succeeded|failed)$")
    exit_code: int | None = None
    error: str = Field(default="", max_length=5000)
    result: dict[str, Any] = Field(default_factory=dict)


class CreativeIn(BaseModel):
    creative_hash: str = Field(default="", max_length=128)
    content_fingerprint: str = Field(default="", max_length=128)
    platform: str = Field(min_length=1, max_length=255)
    domain: str = Field(default="", max_length=512)
    title: str = Field(default="", max_length=12000)
    description: str = Field(default="", max_length=30000)
    image_url: str = Field(default="", max_length=5000)
    destination_url: str = Field(default="", max_length=8000)
    source_page_url: str = Field(default="", max_length=8000)
    detection_method: str = Field(default="unknown", max_length=128)
    recognition_status: str = Field(default="recognized", max_length=32)
    captured_at: str = Field(default="")
    html: str = Field(default="", max_length=MAX_HTML_CHARS)


class CreativesBatch(BaseModel):
    creatives: list[CreativeIn] = Field(max_length=20)


class CreativeExtrasIn(BaseModel):
    creative_hash: str = Field(min_length=1, max_length=128)
    creative_type: str = Field(default="", max_length=16)
    video_src: str = Field(default="", max_length=2000)
    video_poster: str = Field(default="", max_length=2000)
    video_duration: str = Field(default="", max_length=32)
    cta_text: str = Field(default="", max_length=120)
    price_text: str = Field(default="", max_length=255)
    price_rub: float | None = None
    discount_percent: float | None = None
    erid: str = Field(default="", max_length=64)
    app_package_id: str = Field(default="", max_length=255)
    app_store_id: str = Field(default="", max_length=64)
    carousel_id: str = Field(default="", max_length=32)
    carousel_slide_index: int | None = None


class CreativeExtrasBatch(BaseModel):
    extras: list[CreativeExtrasIn] = Field(max_length=200)


class SecondaryLinkIn(BaseModel):
    creative_hash: str = Field(min_length=1, max_length=128)
    link_text: str = Field(default="", max_length=300)
    tracking_url: str = Field(default="", max_length=4000)
    final_url: str = Field(default="", max_length=4000)
    final_url_domain: str = Field(default="", max_length=253)
    utm_source: str = Field(default="", max_length=255)
    utm_medium: str = Field(default="", max_length=255)
    utm_campaign: str = Field(default="", max_length=500)
    utm_content: str = Field(default="", max_length=500)
    utm_term: str = Field(default="", max_length=500)
    resolve_status: str = Field(default="", max_length=32)


class SecondaryLinksBatch(BaseModel):
    links: list[SecondaryLinkIn] = Field(max_length=400)


class CreativeUtmIn(BaseModel):
    creative_hash: str = Field(min_length=1, max_length=128)
    final_url: str = Field(default="", max_length=4000)
    final_url_domain: str = Field(default="", max_length=253)
    utm_source: str = Field(default="", max_length=255)
    utm_medium: str = Field(default="", max_length=255)
    utm_campaign: str = Field(default="", max_length=500)
    utm_content: str = Field(default="", max_length=500)
    utm_term: str = Field(default="", max_length=500)
    resolve_status: str = Field(default="", max_length=32)
    resolved_at: str = Field(default="", max_length=64)


class CreativeUtmBatch(BaseModel):
    items: list[CreativeUtmIn] = Field(max_length=200)


class DestinationHistoryIn(BaseModel):
    creative_hash: str = Field(min_length=1, max_length=128)
    change_signature: str = Field(min_length=1, max_length=128)
    tracking_url: str = Field(default="", max_length=4000)
    final_url: str = Field(default="", max_length=4000)
    final_url_domain: str = Field(default="", max_length=253)
    utm_source: str = Field(default="", max_length=255)
    utm_medium: str = Field(default="", max_length=255)
    utm_campaign: str = Field(default="", max_length=500)
    utm_content: str = Field(default="", max_length=500)
    utm_term: str = Field(default="", max_length=500)
    first_seen_at: str = Field(default="", max_length=64)
    last_seen_at: str = Field(default="", max_length=64)


class DestinationHistoryBatch(BaseModel):
    items: list[DestinationHistoryIn] = Field(max_length=400)


class LandingMismatchIn(BaseModel):
    creative_hash: str = Field(min_length=1, max_length=128)
    advertised_site_domain: str = Field(default="", max_length=253)
    landing_domain_mismatch: bool = False


class LandingMismatchBatch(BaseModel):
    items: list[LandingMismatchIn] = Field(max_length=200)


class IdentifierIn(BaseModel):
    type: str = Field(min_length=1, max_length=64)
    value: str = Field(min_length=1, max_length=500)


class CreativeIdentifiersIn(BaseModel):
    creative_hash: str = Field(min_length=1, max_length=128)
    identifiers: list[IdentifierIn] = Field(default_factory=list, max_length=50)


class CreativeIdentifiersBatch(BaseModel):
    items: list[CreativeIdentifiersIn] = Field(max_length=200)


class CoverageReportRowIn(BaseModel):
    platform: str = Field(default="", max_length=255)
    detection_method: str = Field(default="", max_length=128)
    creative_type: str = Field(default="", max_length=64)
    total_creatives: int = 0
    disclosed_creatives: int = 0
    disclosed_pct: float = 0


class CoverageReportBatch(BaseModel):
    rows: list[CoverageReportRowIn] = Field(max_length=2000)


class AdvertiserLinkIn(BaseModel):
    creative_hash: str = Field(min_length=1, max_length=128)
    content_fingerprint: str = Field(default="", max_length=128)
    advertiser_identity_key: str = Field(min_length=1, max_length=191)
    advertiser_inn: str = Field(default="", max_length=32)
    advertiser_name: str = Field(default="", max_length=500)
    association_method: str = Field(default="unknown", max_length=80)
    confidence: float | None = None
    identifiers: list[IdentifierIn] = Field(default_factory=list, max_length=50)
    address: str = Field(default="", max_length=1000)
    phone: str = Field(default="", max_length=500)
    website_url: str = Field(default="", max_length=2000)


class AdvertiserLinksBatch(BaseModel):
    links: list[AdvertiserLinkIn] = Field(max_length=100)


class LoginIn(BaseModel):
    email: str = Field(min_length=3, max_length=255)
    password: str = Field(min_length=1, max_length=200)


class CreateUserIn(BaseModel):
    email: str = Field(min_length=3, max_length=255)
    password: str = Field(min_length=8, max_length=200)
    role: str = Field(default="marketer", pattern="^(marketer|admin)$")


class ChangePasswordIn(BaseModel):
    current_password: str = Field(min_length=1, max_length=200)
    new_password: str = Field(min_length=8, max_length=200)


class ProfileUpdateIn(BaseModel):
    name: str = Field(default="", max_length=200)


class AvatarUploadIn(BaseModel):
    # A small data: URL (client resizes to a square thumbnail before sending) — see
    # create_notification's neighboring note on why this isn't a static file upload.
    data_url: str = Field(default="", max_length=400_000)


class NotificationPrefsIn(BaseModel):
    prefs: dict[str, bool] = Field(default_factory=dict)


class RunCreateIn(BaseModel):
    city: str = Field(min_length=1, max_length=100)
    key_text: str = Field(min_length=1, max_length=300)


class BrowserStateIn(BaseModel):
    active: bool = True
    captcha_required: bool = False
    url: str = Field(default="", max_length=8000)
    title: str = Field(default="", max_length=2000)
    screenshot_b64: str = Field(default="", max_length=4_000_000)
    viewport_width: int = 0
    viewport_height: int = 0


class BrowserCommandIn(BaseModel):
    action: str = Field(pattern="^(click|key|text|scroll|refresh)$")
    x_ratio: float | None = None
    y_ratio: float | None = None
    key: str = Field(default="", max_length=20)
    text: str = Field(default="", max_length=2000)
    scroll_y: int = 0


class CollectionCreateIn(BaseModel):
    name: str = Field(min_length=1, max_length=120)
    description: str = Field(default="", max_length=500)


class CollectionItemIn(BaseModel):
    group_key: str = Field(min_length=1, max_length=191)
    creative_id: int


class SiteResultIn(BaseModel):
    platform: str = Field(min_length=1, max_length=255)
    url: str = Field(default="", max_length=8000)
    status: str = Field(default="unknown", max_length=64)
    duration_seconds: float | None = None
    scrolls_done: int = 0
    ad_signal_detected: bool = False
    unique_creatives: int = 0
    partial_creatives: int = 0
    detection_methods: str = Field(default="", max_length=12000)
    dom_snapshot_path: str = Field(default="", max_length=8000)
    error_text: str = Field(default="", max_length=30000)


class SiteResultsBatch(BaseModel):
    sites: list[SiteResultIn] = Field(max_length=100)


@app.get("/health")
def health():
    return {"status": "ok", "service": "parsya-control-plane"}


@app.get("/login", response_class=HTMLResponse)
def login_get(request: Request, error: str = ""):
    if auth_page(request):
        return RedirectResponse(P("/dashboard"), status_code=303)
    body = f"""<div style="max-width:440px;margin:8vh auto" class="panel"><div class="brand" style="padding:0 0 20px"><span class="brand-mark">P</span><div><b>ParsYa</b><small>CONTROL PLANE</small></div></div><p class="muted">Войдите в панель управления парсером.</p>{'<div class="notice error-text">'+esc(error)+'</div>' if error else ''}<form method="post" action="{P('/login')}"><input type="hidden" name="csrf_token" value="{esc(csrf_token(request))}"><div class="field"><label>Логин</label><input name="username" required autocomplete="username"></div><div class="field"><label>Пароль</label><input type="password" name="password" required autocomplete="current-password"></div><button class="btn" style="width:100%">Войти</button></form></div>"""
    # Login page has no sidebar.
    return HTMLResponse(page(request, "Вход", body).body.decode("utf-8").replace('<div class="app"><main>', '<div class="app"><main>'))


@app.post("/login")
def login_post(
    request: Request,
    username: str = Form(...),
    password: str = Form(...),
    csrf_token_value: str = Form(..., alias="csrf_token"),
):
    check_csrf(request, csrf_token_value)
    # hmac.compare_digest() raises TypeError on non-ASCII str input (seen
    # live 2026-11-03: a non-Latin login attempt 500'd instead of just
    # failing auth) — bytes comparison has no such restriction.
    if (
        hmac.compare_digest(username.encode("utf-8"), ADMIN_USER.encode("utf-8"))
        and hmac.compare_digest(password.encode("utf-8"), ADMIN_PASSWORD.encode("utf-8"))
    ):
        request.session.clear()
        request.session["authenticated"] = True
        request.session["username"] = username
        request.session["csrf"] = secrets.token_urlsafe(32)
        return RedirectResponse(P("/dashboard"), status_code=303)
    return RedirectResponse(P("/login") + "?error=" + ("Неверный%20логин%20или%20пароль"), status_code=303)


@app.post("/logout")
def logout(request: Request, csrf_token_value: str = Form(..., alias="csrf_token")):
    check_csrf(request, csrf_token_value)
    request.session.clear()
    return RedirectResponse(P("/login"), status_code=303)


@app.get("/")
def root(request: Request):
    return RedirectResponse(P("/dashboard") if auth_page(request) else P("/login"), status_code=303)


@app.get("/dashboard", response_class=HTMLResponse)
def dashboard(request: Request):
    redirect = admin_or_redirect(request)
    if redirect:
        return redirect
    with closing(connect()) as db:
        jobs = db.execute("SELECT COUNT(*) n FROM jobs").fetchone()["n"]
        active = db.execute("SELECT COUNT(*) n FROM jobs WHERE status IN ('queued','running')").fetchone()["n"]
        creatives = db.execute("SELECT COUNT(*) n FROM creatives").fetchone()["n"]
        occurrences = db.execute("SELECT COUNT(*) n FROM occurrences").fetchone()["n"]
        recent = db.execute("SELECT * FROM jobs ORDER BY id DESC LIMIT 8").fetchall()
        worker_rows = db.execute("SELECT * FROM workers ORDER BY last_seen DESC").fetchall()
        top_sites = db.execute("SELECT platform,COUNT(*) n FROM occurrences GROUP BY platform ORDER BY n DESC LIMIT 8").fetchall()
    def badge(status: str) -> str:
        return f'<span class="badge {esc(status)}">{esc(status)}</span>'
    def job_icon(status: str) -> str:
        lucide_name = {"running": "loader-circle", "queued": "clock", "failed": "circle-x"}.get(status, "circle-check")
        return f'<span class="jobicon"><i data-lucide="{lucide_name}"></i></span>'
    job_cards = "".join(
        f'<div class="job"><div class="jobname">{job_icon(r["status"])}<div><strong>#{r["id"]} · {esc(r["key_text"])}</strong><span>{esc(r["city"])} · {esc(r["created_at"])}</span></div></div>'
        f'<div class="jobmeta"><strong>{r["total_creatives"]}</strong><span>креативов</span></div>'
        f'<div>{badge(r["status"])}</div>'
        f'<a class="secondary" href="{P("/runs/")}{r["id"]}">Подробнее</a></div>'
        for r in recent
    ) or '<div class="empty"><strong>Запусков пока нет.</strong> Создайте первый запуск.</div>'
    workers = "".join(f'<tr><td>{esc(w["worker_id"])}</td><td>{esc(w["hostname"])}</td><td>{badge(w["state"])}</td><td>{esc(w["last_seen"])}</td><td>{("#"+str(w["current_job_id"])) if w["current_job_id"] else "—"}</td></tr>' for w in worker_rows) or '<tr><td colspan="5" class="empty">Воркер ещё не подключался</td></tr>'
    sites = "".join(f'<tr><td>{esc(s["platform"])}</td><td>{s["n"]}</td></tr>' for s in top_sites) or '<tr><td colspan="2" class="empty">Данные появятся после синхронизации</td></tr>'
    body = f"""<div class="summary">
<article class="stat"><div class="stathead">Всего запусков <span class="staticon"><i data-lucide="radar"></i></span></div><div class="statvalue">{jobs}</div><div class="statfoot">За всё время</div></article>
<article class="stat"><div class="stathead">В очереди / выполняются <span class="staticon violet"><i data-lucide="loader-circle"></i></span></div><div class="statvalue">{active}</div><div class="statfoot">Незавершённые задания</div></article>
<article class="stat"><div class="stathead">Уникальные креативы <span class="staticon gray"><i data-lucide="layers"></i></span></div><div class="statvalue">{creatives}</div><div class="statfoot">С сохранённым HTML при наличии</div></article>
<article class="stat"><div class="stathead">Обнаружения <span class="staticon amber"><i data-lucide="scan-search"></i></span></div><div class="statvalue">{occurrences}</div><div class="statfoot">С учётом повторов в разных запусках</div></article>
</div>
<div class="panel"><div class="panel-head"><h2>Последние запуски</h2><a href="{P("/runs")}">Все запуски →</a></div><div class="joblist">{job_cards}</div></div>
<div class="detail-grid"><div class="panel"><div class="panel-head"><h2>Состояние WSL-воркера</h2></div><div class="table-wrap"><table><thead><tr><th>Worker ID</th><th>Узел</th><th>Статус</th><th>Последний сигнал</th><th>Задание</th></tr></thead><tbody>{workers}</tbody></table></div></div><div class="panel"><div class="panel-head"><h2>Площадки по обнаружениям</h2><a href="{P("/creatives")}">Креативы →</a></div><table><thead><tr><th>Площадка</th><th>Обнаружения</th></tr></thead><tbody>{sites}</tbody></table></div></div>
<div class="notice">Сервер не подключается к WSL напрямую. WSL-воркер сам опрашивает эту панель по HTTPS и забирает задания.</div>"""
    return page(request, "Обзор", body, "dashboard")


@app.get("/new-run", response_class=HTMLResponse)
def new_run_get(request: Request):
    redirect = admin_or_redirect(request)
    if redirect:
        return redirect
    body = f"""<div class="panel" style="max-width:850px"><h2>Параметры запуска</h2><p class="muted">Задание попадёт в очередь. WSL-воркер заберёт его при следующем опросе.</p><form method="post" action="{P('/new-run')}"><input type="hidden" name="csrf_token" value="{esc(csrf_token(request))}"><div class="formgrid"><div class="field"><label>Город</label><input name="city" value="Екатеринбург" maxlength="100" required><div class="help">Используется существующей логикой парсера для выбора прокси.</div></div><div class="field"><label>Поисковый запрос / ключевая фраза</label><input name="key_text" value="увеличение пенсии" maxlength="300" required><div class="help">Например: «банкротство физических лиц».</div></div></div><div class="notice">В первой версии воркер выполняет только заранее заданный тип задания «запуск ParsYa». Произвольные команды оболочки через панель не передаются.</div><button class="btn">＋ Добавить в очередь</button> <a class="btn secondary" href="{P('/runs')}">Отмена</a></form></div>"""
    return page(request, "Новый запуск", body, "new-run")


@app.post("/new-run")
def new_run_post(
    request: Request,
    city: str = Form(...),
    key_text: str = Form(...),
    csrf_token_value: str = Form(..., alias="csrf_token"),
):
    redirect = admin_or_redirect(request)
    if redirect:
        return redirect
    check_csrf(request, csrf_token_value)
    city, key_text = city.strip(), key_text.strip()
    if not city or len(city) > 100 or not key_text or len(key_text) > 300:
        raise HTTPException(status_code=400, detail="Проверьте город и ключевой запрос.")
    with closing(connect()) as db:
        cur = db.execute(
            "INSERT INTO jobs(status,city,key_text,created_at) VALUES('queued',?,?,?)",
            (city, key_text, now_iso()),
        )
        job_id = cur.lastrowid
        db.commit()
    return RedirectResponse(P(f"/runs/{job_id}"), status_code=303)


@app.get("/runs", response_class=HTMLResponse)
def runs_page(request: Request):
    redirect = admin_or_redirect(request)
    if redirect:
        return redirect
    with closing(connect()) as db:
        rows_data = db.execute("SELECT * FROM jobs ORDER BY id DESC LIMIT 200").fetchall()
    rows = "".join(f'<tr><td><a href="{P("/runs/")}{r["id"]}">#{r["id"]}</a></td><td>{esc(r["city"])}</td><td>{esc(r["key_text"])}</td><td><span class="badge {esc(r["status"])}">{esc(r["status"])}</span></td><td>{esc(r["worker_id"] or "—")}</td><td>{esc(r["created_at"])}</td><td>{esc(r["started_at"] or "—")}</td><td>{esc(r["finished_at"] or "—")}</td><td>{r["total_creatives"]}</td></tr>' for r in rows_data) or '<tr><td colspan="9" class="empty">Запусков пока нет</td></tr>'
    body = f'<div class="panel"><div class="panel-head"><h2>История заданий</h2><a class="btn" href="{P("/new-run")}">＋ Создать запуск</a></div><div class="table-wrap"><table><thead><tr><th>ID</th><th>Город</th><th>Запрос</th><th>Статус</th><th>Воркер</th><th>Создан</th><th>Старт</th><th>Окончен</th><th>Креативы</th></tr></thead><tbody>{rows}</tbody></table></div></div>'
    return page(request, "Запуски", body, "runs")


@app.get("/runs/{job_id}", response_class=HTMLResponse)
def run_detail(request: Request, job_id: int):
    redirect = admin_or_redirect(request)
    if redirect:
        return redirect
    with closing(connect()) as db:
        job = db.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
        occurrences = db.execute("SELECT o.platform,COUNT(*) n FROM occurrences o WHERE o.job_id=? GROUP BY o.platform ORDER BY n DESC", (job_id,)).fetchall()
        site_results = db.execute("SELECT * FROM site_results WHERE job_id=? ORDER BY platform", (job_id,)).fetchall()
    if not job:
        raise HTTPException(status_code=404, detail="Запуск не найден")
    sites = "".join(f'<tr><td>{esc(r["platform"])}</td><td>{r["n"]}</td></tr>' for r in occurrences) or '<tr><td colspan="2" class="empty">Воркер ещё не отправил креативы для этого запуска</td></tr>'
    def site_badge(status: str) -> str:
        return f'<span class="badge {esc(status)}">{esc(status)}</span>'
    scan_rows = "".join(
        f'<tr><td>{esc(s["platform"])}</td><td>{site_badge(s["status"])}</td><td>{s["unique_creatives"]}</td><td>{"да" if s["ad_signal_detected"] else "нет"}</td><td>{esc(s["duration_seconds"] or "—")}</td><td class="error-text">{esc(s["error_text"] or "—")}</td></tr>'
        for s in site_results
    ) or '<tr><td colspan="6" class="empty">Результаты по площадкам ещё не синхронизированы</td></tr>'
    result = esc(job["result_json"] or "{}")
    browser_panel = f"""<div class="panel" id="browserPanel" style="display:none">
  <div class="panel-head"><h2>CAPTCHA — ручное решение</h2><span class="muted" id="browserUrl"></span></div>
  <div style="position:relative;max-width:900px;border:1px solid var(--line);border-radius:10px;overflow:hidden;cursor:crosshair" id="browserScreenWrap">
    <img id="browserScreen" class="remote-browser-screen" style="display:block;width:100%;max-height:65vh;object-fit:contain;background:#07101d" alt="Снимок браузера воркера">
  </div>
  <div style="display:flex;gap:8px;margin-top:12px;flex-wrap:wrap;align-items:center">
    <input id="browserText" placeholder="Текст для поля ввода" style="flex:1;min-width:200px">
    <button type="button" class="btn secondary" id="browserSendText">Отправить текст</button>
    <button type="button" class="btn secondary" id="browserEnter">Enter</button>
    <button type="button" class="btn secondary" id="browserTab">Tab</button>
    <button type="button" class="btn secondary" id="browserEsc">Esc</button>
    <button type="button" class="btn secondary" id="browserRefresh">Обновить страницу воркера</button>
  </div>
  <p class="help">Кликните прямо по снимку — клик будет передан в реальный Chrome воркера по тем же координатам. Снимок обновляется примерно раз в секунду.</p>
</div>
<script>
(function(){{
  const jobId = {job_id};
  const panel = document.getElementById('browserPanel');
  const img = document.getElementById('browserScreen');
  const urlLabel = document.getElementById('browserUrl');
  async function sendCommand(command) {{
    await fetch(`{P('')}/api/admin/jobs/${{jobId}}/browser/command`, {{
      method: 'POST', headers: {{'Content-Type': 'application/json'}}, body: JSON.stringify(command),
    }});
  }}
  async function poll() {{
    try {{
      const r = await fetch(`{P('')}/api/admin/jobs/${{jobId}}/browser/state`);
      const data = await r.json();
      if (data.active && data.captcha_required) {{
        panel.style.display = '';
        if (data.screenshot_b64) img.src = 'data:image/png;base64,' + data.screenshot_b64;
        urlLabel.textContent = data.title || data.url || '';
      }} else {{
        panel.style.display = 'none';
      }}
    }} catch (e) {{}}
    setTimeout(poll, 1200);
  }}
  document.getElementById('browserScreenWrap').addEventListener('click', (e) => {{
    const rect = img.getBoundingClientRect();
    const x_ratio = (e.clientX - rect.left) / rect.width;
    const y_ratio = (e.clientY - rect.top) / rect.height;
    sendCommand({{action: 'click', x_ratio, y_ratio}});
  }});
  document.getElementById('browserSendText').addEventListener('click', () => {{
    const text = document.getElementById('browserText').value;
    if (text) sendCommand({{action: 'text', text}});
  }});
  document.getElementById('browserEnter').addEventListener('click', () => sendCommand({{action: 'key', key: 'ENTER'}}));
  document.getElementById('browserTab').addEventListener('click', () => sendCommand({{action: 'key', key: 'TAB'}}));
  document.getElementById('browserEsc').addEventListener('click', () => sendCommand({{action: 'key', key: 'ESCAPE'}}));
  document.getElementById('browserRefresh').addEventListener('click', () => sendCommand({{action: 'refresh'}}));
  poll();
}})();
</script>"""
    body = f"""<div class="grid"><div class="metric"><div class="label">Статус</div><div class="value" style="font-size:21px">{esc(job["status"])}</div><div class="hint">Код завершения: {esc(job["exit_code"])}</div></div><div class="metric"><div class="label">Город</div><div class="value" style="font-size:21px">{esc(job["city"])}</div></div><div class="metric"><div class="label">Креативы синхронизированы</div><div class="value">{job["total_creatives"]}</div></div><div class="metric"><div class="label">Воркер</div><div class="value" style="font-size:18px;overflow-wrap:anywhere">{esc(job["worker_id"] or "ожидание")}</div></div></div>
{browser_panel}
<div class="detail-grid"><div class="panel"><h2>Детали задания</h2><table><tbody><tr><th>Ключевой запрос</th><td>{esc(job["key_text"])}</td></tr><tr><th>Создан</th><td>{esc(job["created_at"])}</td></tr><tr><th>Начат</th><td>{esc(job["started_at"] or "—")}</td></tr><tr><th>Завершён</th><td>{esc(job["finished_at"] or "—")}</td></tr><tr><th>Ошибка</th><td class="error-text">{esc(job["error_text"] or "—")}</td></tr></tbody></table><h2 style="margin-top:22px">Результаты сканирования площадок</h2><div class="table-wrap"><table><thead><tr><th>Площадка</th><th>Статус</th><th>Креативы</th><th>Сигнал рекламы</th><th>Время, сек.</th><th>Ошибка</th></tr></thead><tbody>{scan_rows}</tbody></table></div><h2 style="margin-top:22px">Обнаружения по рекламным площадкам</h2><table><thead><tr><th>Площадка</th><th>Обнаружения</th></tr></thead><tbody>{sites}</tbody></table></div><div class="panel"><div class="panel-head"><h2>Лог воркера</h2><button class="btn secondary" onclick="location.reload()">Обновить</button></div><div class="log mono">{esc(job["log_text"] or "Ожидание подключения воркера…")}</div></div></div><div class="panel"><h2>Результат</h2><pre class="log mono">{result}</pre></div>"""
    return page(request, f"Запуск #{job_id}", body, "runs")


@app.get("/advertisers", response_class=HTMLResponse)
def advertisers_page(request: Request, q: str = ""):
    """Рекламодатели, распознанные по ИНН/названию и привязанные к
    креативам (advertisers/creative_advertiser_links — те же таблицы,
    что уже питают /api/v1/creatives?inn=...). Реальные данные, не демо —
    единственная из вкладок референса, для которой в проекте уже готов
    бэкенд (2026-11-03, фаза 4)."""
    redirect = admin_or_redirect(request)
    if redirect:
        return redirect
    where_sql, params = "", []
    if q.strip():
        like = "%" + q.strip()[:150] + "%"
        where_sql = "WHERE a.inn LIKE ? OR a.canonical_name LIKE ?"
        params = [like, like]
    with closing(connect()) as db:
        rows = db.execute(
            f"""
            SELECT a.id, a.inn, a.canonical_name, a.first_seen_at, a.last_seen_at,
                   COUNT(DISTINCT l.creative_id) AS creatives_count,
                   COUNT(DISTINCT c.platform) AS platforms_count
            FROM advertisers a
            LEFT JOIN creative_advertiser_links l ON l.advertiser_id = a.id
            LEFT JOIN creatives c ON c.id = l.creative_id
            {where_sql}
            GROUP BY a.id
            ORDER BY a.last_seen_at DESC
            LIMIT 200
            """,
            params,
        ).fetchall()
    rows_html = "".join(
        f'<tr><td><strong>{esc(r["canonical_name"] or "—")}</strong></td><td class="mono">{esc(r["inn"] or "—")}</td>'
        f'<td><a href="{P("/creatives?q=")}{esc(r["canonical_name"] or r["inn"] or "")}">{r["creatives_count"]} →</a></td>'
        f'<td>{r["platforms_count"]}</td><td>{esc(r["first_seen_at"])}</td><td>{esc(r["last_seen_at"])}</td></tr>'
        for r in rows
    ) or '<tr><td colspan="6" class="empty">Рекламодатели пока не распознаны. Они появляются автоматически, когда парсер находит ИНН/название на странице креатива.</td></tr>'
    body = f"""<div class="panel">
<form method="get" action="{P('/advertisers')}" class="filters"><input name="q" placeholder="Поиск по ИНН или названию" value="{esc(q)}"><button class="btn">Найти</button><a class="btn secondary" href="{P('/advertisers')}">Сбросить</a><span class="muted">Всего: {len(rows)}</span></form>
<div class="table-wrap"><table><thead><tr><th>Рекламодатель</th><th>ИНН</th><th>Креативов</th><th>Площадок</th><th>Впервые замечен</th><th>Последний раз</th></tr></thead><tbody>{rows_html}</tbody></table></div>
</div>"""
    return page(request, "Рекламодатели", body, "advertisers")


@app.get("/api/admin/intents")
def api_admin_intents(request: Request):
    """Тот же список интентов, что и публичный /api/v1/intents, но под
    сессионной авторизацией админки (не bearer-токен SaaS-фронтенда) —
    питает intent-picker на /creatives. Переиспользует ровно ту же логику
    (стемминг-поиск похожих интентов), что уже production-проверена в
    публичном API."""
    if not auth_page(request):
        raise HTTPException(status_code=401, detail="Требуется вход")
    with closing(connect()) as db:
        rows = db.execute(
            """
            SELECT PYLOWER(j.key_text) AS intent_key,
                   MIN(j.key_text) AS display_text,
                   COUNT(DISTINCT j.id) AS runs_count,
                   COUNT(DISTINCT o.creative_id) AS creatives_count,
                   MAX(j.created_at) AS last_run_at
            FROM jobs j
            LEFT JOIN occurrences o ON o.job_id = j.id
            WHERE j.key_text IS NOT NULL AND TRIM(j.key_text) <> ''
            GROUP BY intent_key
            ORDER BY last_run_at DESC
            """
        ).fetchall()
    all_texts = [r["display_text"] for r in rows]
    items = [
        {
            "intent": r["display_text"], "runs_count": r["runs_count"],
            "creatives_count": r["creatives_count"],
            "related": _related_intents([r["display_text"]], all_texts),
        }
        for r in rows
    ]
    return {"items": items}


@app.get("/creatives", response_class=HTMLResponse)
def creatives_page(request: Request, intent: str = ""):
    redirect = admin_or_redirect(request)
    if redirect:
        return redirect
    intents = _parse_intents(intent)
    with closing(connect()) as db:
        platforms = db.execute("SELECT DISTINCT platform FROM creatives ORDER BY platform").fetchall()
        if intents:
            # Та же серверная логика, что у /api/v1/creatives: сначала
            # пробуем семантический (эмбеддинги) скоуп, если для хотя бы
            # одного интента есть кэш; иначе — точное совпадение key_text.
            combined = _semantic_intent_where(db, intents)
            if combined:
                where_sql, params = combined
            else:
                where_sql, params = _intent_exists_sql("c", intents), intents
            items = db.execute(
                f"SELECT c.* FROM creatives c WHERE {where_sql} ORDER BY c.captured_at DESC LIMIT 300", params
            ).fetchall()
        else:
            items = db.execute("SELECT * FROM creatives ORDER BY captured_at DESC LIMIT 300").fetchall()
    platform_options = '<option value="all">Все площадки</option>' + "".join(f'<option value="{esc(p["platform"])}">{esc(p["platform"])}</option>' for p in platforms)
    cards = []
    for item in items:
        preview = f'<img src="{esc(item["image_url"])}" alt="" loading="lazy" referrerpolicy="no-referrer">' if item["image_url"] and item["image_url"].startswith(("https://", "http://")) else '<span class="muted">HTML-креатив</span>'
        search_blob = esc(" ".join(filter(None, [item["title"], item["domain"], item["description"], item["platform"]])).lower())
        cards.append(
            f'<article class="card creative" data-platform="{esc(item["platform"])}" data-status="{esc(item["recognition_status"])}" data-id="{item["id"]}" data-search="{search_blob}">'
            f'<div class="creative-preview"><a href="{P("/creatives/")}{item["id"]}">{preview}</a><button type="button" class="save" data-save-id="{item["id"]}" aria-label="Сохранить креатив"><i data-lucide="bookmark"></i></button></div>'
            f'<div class="creative-info"><h3><a href="{P("/creatives/")}{item["id"]}">{esc(item["title"] or item["domain"] or "Без заголовка")}</a></h3>'
            f'<div class="muted">{esc(item["platform"])} · {esc(item["domain"] or "домен не определён")}</div>'
            f'<div class="creative-meta"><span class="badge">{esc(item["detection_method"])}</span><span class="badge {"ok" if item["recognition_status"] == "recognized" else "running"}">{esc(item["recognition_status"])}</span></div>'
            f'<div class="muted">{esc(item["captured_at"])}</div></div></article>'
        )
    intent_label = esc(intents[0]) if intents else "Все запросы"
    intent_meta = (
        f"Показаны только креативы по запросу «{esc(intents[0])}»" if intents
        else "Фильтр по интенту выключен — показаны креативы всех запросов"
    )
    body = f"""<section class="intentbar" id="intentBar">
  <div class="intentbar-left"><i data-lucide="target"></i><div class="intentbar-copy"><strong id="intentBarLabel">{intent_label}</strong><span id="intentBarMeta">{intent_meta}</span></div></div>
  <div class="intentbar-actions">
    <button class="secondary" type="button" id="intentBarPicker"><i data-lucide="chevron-down"></i> Выбрать интент</button>
    {'<a class="secondary" href="' + P('/creatives') + '">Сбросить интент</a>' if intents else ''}
  </div>
</section>
<div class="intentpicker" id="intentPicker" hidden>
  <div class="intentpicker-head"><strong>Какой интент (поисковый запрос) показывать?</strong><button class="intentpicker-close" type="button" id="intentPickerClose" aria-label="Закрыть">×</button></div>
  <input class="intentpicker-search" id="intentPickerSearch" placeholder="Поиск по запросам…" autocomplete="off">
  <div class="intentpicker-list" id="intentPickerList"><div class="empty">Загрузка…</div></div>
  <div class="intentpicker-foot"><button class="secondary" type="button" id="intentPickerClear">Показать все (сбросить)</button></div>
</div>
<div class="librarycontrols">
  <div class="toolbar">
    <label class="search"><i data-lucide="search"></i><input id="libQuery" placeholder="Поиск по тексту, домену или площадке…" autocomplete="off"></label>
    <select class="filter" id="libPlatform">{platform_options}</select>
    <select class="filter" id="libStatus"><option value="all">Все статусы</option><option value="recognized">recognized</option><option value="partial">partial</option></select>
    <button class="secondary" type="button" id="libReset"><i data-lucide="rotate-ccw"></i> Сбросить</button>
  </div>
  <div class="quickfilters">
    <button class="quickfilter active" type="button" data-quick="all">Все креативы</button>
    <button class="quickfilter" type="button" data-quick="saved"><i data-lucide="bookmark"></i> Сохранённые</button>
  </div>
  <div class="activefilters" id="libActiveFilters" hidden><span class="activefilterlabel">Активные фильтры</span><div class="filterchips" id="libFilterChips"></div><button class="clearfilters" type="button" id="libClearFilters">Очистить всё</button></div>
</div>
<div class="savedsearches" id="libSavedSearches" hidden>
  <div class="savedsearchhead"><div><strong>Сохранённые поиски</strong><p>Наборы фильтров хранятся в этом браузере.</p></div></div>
  <div class="savedsearchlist" id="libSavedSearchList"></div>
</div>
<div class="resulthead">
  <div><h2>Библиотека рекламных креативов</h2><p id="libResultCount" aria-live="polite">{len(items)} креативов</p></div>
  <div class="resultactions">
    <button class="secondary" type="button" id="libSaveSearch"><i data-lucide="bookmark-plus"></i> Сохранить поиск</button>
    <button class="secondary" type="button" id="libExport"><i data-lucide="download"></i> Экспорт JSON</button>
  </div>
</div>
<div class="empty-filter" id="libEmptyFilter" hidden><div class="empty-filter-icon"><i data-lucide="search-x"></i></div><h3>Ничего не найдено</h3><p>По текущему запросу и фильтрам нет совпадений.</p><button class="secondary" type="button" id="libEmptyReset">Очистить поиск и фильтры</button></div>
<div class="creative-grid" id="libCards">{''.join(cards) if cards else '<div class="empty">Креативы пока не синхронизированы с WSL. Убедитесь, что воркер подключён и MySQL-доступ настроен.</div>'}</div>
<script>
(function(){{
  const cards = Array.from(document.querySelectorAll('#libCards .creative'));
  const queryEl = document.getElementById('libQuery');
  const platformEl = document.getElementById('libPlatform');
  const statusEl = document.getElementById('libStatus');
  const resultCount = document.getElementById('libResultCount');
  const emptyFilter = document.getElementById('libEmptyFilter');
  const activeFilters = document.getElementById('libActiveFilters');
  const filterChips = document.getElementById('libFilterChips');
  let quickMode = 'all';

  function savedIds() {{
    try {{ return JSON.parse(localStorage.getItem('parsya_saved_creatives') || '[]'); }} catch(e) {{ return []; }}
  }}
  function setSavedIds(ids) {{
    localStorage.setItem('parsya_saved_creatives', JSON.stringify(ids));
  }}
  function refreshSaveButtons() {{
    const ids = savedIds().map(String);
    document.querySelectorAll('[data-save-id]').forEach(btn => {{
      btn.classList.toggle('saved', ids.includes(btn.dataset.saveId));
    }});
  }}
  document.querySelectorAll('[data-save-id]').forEach(btn => {{
    btn.addEventListener('click', (e) => {{
      e.preventDefault(); e.stopPropagation();
      const id = btn.dataset.saveId;
      let ids = savedIds().map(String);
      if (ids.includes(id)) ids = ids.filter(x => x !== id); else ids.push(id);
      setSavedIds(ids);
      refreshSaveButtons();
      applyFilters();
    }});
  }});

  function applyFilters() {{
    const q = (queryEl.value || '').trim().toLowerCase();
    const platform = platformEl.value;
    const status = statusEl.value;
    const saved = savedIds().map(String);
    let visible = 0;
    cards.forEach(card => {{
      let ok = true;
      if (q && !card.dataset.search.includes(q)) ok = false;
      if (platform !== 'all' && card.dataset.platform !== platform) ok = false;
      if (status !== 'all' && card.dataset.status !== status) ok = false;
      if (quickMode === 'saved' && !saved.includes(card.dataset.id)) ok = false;
      card.hidden = !ok;
      if (ok) visible++;
    }});
    resultCount.textContent = visible + ' ' + (visible % 10 === 1 && visible % 100 !== 11 ? 'креатив' : (visible % 10 >= 2 && visible % 10 <= 4 && (visible % 100 < 10 || visible % 100 >= 20) ? 'креатива' : 'креативов'));
    emptyFilter.hidden = visible !== 0;
    const chips = [];
    if (q) chips.push(['Запрос: «' + q + '»', () => {{ queryEl.value = ''; }}]);
    if (platform !== 'all') chips.push(['Площадка: ' + platform, () => {{ platformEl.value = 'all'; }}]);
    if (status !== 'all') chips.push(['Статус: ' + status, () => {{ statusEl.value = 'all'; }}]);
    activeFilters.hidden = chips.length === 0;
    filterChips.innerHTML = '';
    chips.forEach(([label, clear]) => {{
      const chip = document.createElement('span');
      chip.className = 'filterchip';
      chip.innerHTML = label + ' <span>&times;</span>';
      chip.addEventListener('click', () => {{ clear(); applyFilters(); }});
      filterChips.appendChild(chip);
    }});
  }}

  let debounce = null;
  queryEl.addEventListener('input', () => {{ clearTimeout(debounce); debounce = setTimeout(applyFilters, 150); }});
  platformEl.addEventListener('change', applyFilters);
  statusEl.addEventListener('change', applyFilters);
  document.getElementById('libReset').addEventListener('click', () => {{
    queryEl.value = ''; platformEl.value = 'all'; statusEl.value = 'all'; quickMode = 'all';
    document.querySelectorAll('.quickfilter').forEach(b => b.classList.toggle('active', b.dataset.quick === 'all'));
    applyFilters();
  }});
  document.getElementById('libClearFilters').addEventListener('click', () => document.getElementById('libReset').click());
  document.getElementById('libEmptyReset').addEventListener('click', () => document.getElementById('libReset').click());
  document.querySelectorAll('.quickfilter').forEach(btn => {{
    btn.addEventListener('click', () => {{
      quickMode = btn.dataset.quick;
      document.querySelectorAll('.quickfilter').forEach(b => b.classList.toggle('active', b === btn));
      applyFilters();
    }});
  }});

  function loadSavedSearches() {{
    try {{ return JSON.parse(localStorage.getItem('parsya_saved_searches') || '[]'); }} catch(e) {{ return []; }}
  }}
  function renderSavedSearches() {{
    const list = loadSavedSearches();
    const panel = document.getElementById('libSavedSearches');
    const container = document.getElementById('libSavedSearchList');
    panel.hidden = list.length === 0;
    container.innerHTML = '';
    list.forEach((s, i) => {{
      const item = document.createElement('div');
      item.className = 'savedsearchitem';
      item.innerHTML = '<div class="savedsearchbody"><strong></strong><span></span></div><div class="savedsearchactions"><button type="button" data-apply-saved-search="' + i + '"><i data-lucide="play"></i></button><button type="button" data-delete-saved-search="' + i + '"><i data-lucide="trash-2"></i></button></div>';
      item.querySelector('strong').textContent = s.name;
      item.querySelector('span').textContent = [s.q && ('«' + s.q + '»'), s.platform !== 'all' && s.platform, s.status !== 'all' && s.status].filter(Boolean).join(' · ') || 'Без фильтров';
      container.appendChild(item);
    }});
    container.querySelectorAll('[data-apply-saved-search]').forEach(btn => btn.addEventListener('click', () => {{
      const s = loadSavedSearches()[Number(btn.dataset.applySavedSearch)];
      if (!s) return;
      queryEl.value = s.q || ''; platformEl.value = s.platform || 'all'; statusEl.value = s.status || 'all';
      applyFilters();
    }}));
    container.querySelectorAll('[data-delete-saved-search]').forEach(btn => btn.addEventListener('click', () => {{
      const list2 = loadSavedSearches();
      list2.splice(Number(btn.dataset.deleteSavedSearch), 1);
      localStorage.setItem('parsya_saved_searches', JSON.stringify(list2));
      renderSavedSearches();
      if (window.lucide) lucide.createIcons();
    }}));
    if (window.lucide) lucide.createIcons();
  }}
  document.getElementById('libSaveSearch').addEventListener('click', () => {{
    const name = prompt('Название сохранённого поиска:');
    if (!name) return;
    const list = loadSavedSearches();
    list.push({{name: name, q: queryEl.value, platform: platformEl.value, status: statusEl.value}});
    localStorage.setItem('parsya_saved_searches', JSON.stringify(list));
    renderSavedSearches();
  }});
  document.getElementById('libExport').addEventListener('click', () => {{
    const visible = cards.filter(c => !c.hidden).map(c => ({{
      id: c.dataset.id, platform: c.dataset.platform, status: c.dataset.status,
      title: c.querySelector('h3 a') ? c.querySelector('h3 a').textContent : '',
    }}));
    const blob = new Blob([JSON.stringify(visible, null, 2)], {{type: 'application/json'}});
    const a = document.createElement('a');
    a.href = URL.createObjectURL(blob);
    a.download = 'creatives-export.json';
    a.click();
  }});

  refreshSaveButtons();
  renderSavedSearches();
  applyFilters();
}})();

(function(){{
  const INTENT_BASE = "{P('/creatives')}";
  const pickerBtn = document.getElementById('intentBarPicker');
  const picker = document.getElementById('intentPicker');
  const closeBtn = document.getElementById('intentPickerClose');
  const clearBtn = document.getElementById('intentPickerClear');
  const searchEl = document.getElementById('intentPickerSearch');
  const listEl = document.getElementById('intentPickerList');
  let loaded = null;

  function renderList(filterText) {{
    const term = (filterText || '').trim().toLowerCase();
    const items = (loaded || []).filter(it => !term || it.intent.toLowerCase().includes(term));
    if (!items.length) {{
      listEl.innerHTML = '<div class="empty">Ничего не найдено</div>';
      return;
    }}
    listEl.innerHTML = items.map(it =>
      '<div class="intentrow" data-intent-value="' + encodeURIComponent(it.intent) + '">' +
      '<div class="intentrow-copy"><strong></strong><span>' + it.creatives_count + ' кр. · ' + it.runs_count + ' запусков</span></div></div>'
    ).join('');
    listEl.querySelectorAll('.intentrow').forEach((row, i) => {{
      row.querySelector('strong').textContent = items[i].intent;
      row.addEventListener('click', () => {{
        window.location.href = INTENT_BASE + '?intent=' + row.dataset.intentValue;
      }});
    }});
  }}

  function openPicker() {{
    picker.hidden = false;
    if (!loaded) {{
      fetch("{P('/api/admin/intents')}").then(r => r.json()).then(data => {{
        loaded = data.items || [];
        renderList(searchEl.value);
      }}).catch(() => {{ listEl.innerHTML = '<div class="empty">Не удалось загрузить список</div>'; }});
    }}
  }}
  pickerBtn && pickerBtn.addEventListener('click', () => {{ picker.hidden ? openPicker() : (picker.hidden = true); }});
  closeBtn && closeBtn.addEventListener('click', () => {{ picker.hidden = true; }});
  clearBtn && clearBtn.addEventListener('click', () => {{ window.location.href = INTENT_BASE; }});
  searchEl && searchEl.addEventListener('input', () => renderList(searchEl.value));
}})();
</script>"""
    return page(request, "Библиотека рекламы", body, "creatives")


@app.get("/creatives/{creative_id}", response_class=HTMLResponse)
def creative_detail(request: Request, creative_id: int):
    redirect = admin_or_redirect(request)
    if redirect:
        return redirect
    with closing(connect()) as db:
        item = db.execute("SELECT * FROM creatives WHERE id=?", (creative_id,)).fetchone()
        events = db.execute("SELECT o.*,j.city,j.key_text FROM occurrences o JOIN jobs j ON j.id=o.job_id WHERE o.creative_id=? ORDER BY o.id DESC LIMIT 100", (creative_id,)).fetchall()
    if not item:
        raise HTTPException(status_code=404, detail="Креатив не найден")
    html_text = ""
    if item["html_gzip"]:
        try:
            html_text = gzip.decompress(item["html_gzip"]).decode("utf-8", errors="replace")
        except Exception:
            html_text = "<!-- Не удалось распаковать HTML -->"
    srcdoc = esc(html_text)
    ev_rows = "".join(f'<tr><td><a href="{P("/runs/")}{e["job_id"]}">#{e["job_id"]}</a></td><td>{esc(e["platform"])}</td><td>{esc(e["observed_at"])}</td><td>{esc(e["detection_method"])}</td><td>{esc(e["city"])}</td><td>{esc(e["key_text"])}</td></tr>' for e in events) or '<tr><td colspan="6" class="empty">История обнаружений отсутствует</td></tr>'
    body = f"""<p><a href="{P('/creatives')}">← К библиотеке</a></p><div class="detail-grid"><div class="panel"><h2>Метаданные креатива</h2><table><tbody><tr><th>Площадка</th><td>{esc(item["platform"])}</td></tr><tr><th>Рекламодатель</th><td>{esc(item["domain"] or "—")}</td></tr><tr><th>Заголовок</th><td>{esc(item["title"] or "—")}</td></tr><tr><th>Описание</th><td>{esc(item["description"] or "—")}</td></tr><tr><th>Метод</th><td>{esc(item["detection_method"] or "—")}</td></tr><tr><th>Распознавание</th><td>{esc(item["recognition_status"])}</td></tr><tr><th>Первый запуск</th><td>{esc(item["first_job_id"] or "—")}</td></tr><tr><th>Последний запуск</th><td>{esc(item["last_job_id"] or "—")}</td></tr><tr><th>Сохранён</th><td>{esc(item["captured_at"])}</td></tr><tr><th>SHA-256 HTML</th><td class="mono">{esc(item["html_sha256"] or "—")}</td></tr><tr><th>Ссылка назначения</th><td>{'<a href="'+esc(item["destination_url"])+'" rel="noopener noreferrer" target="_blank">Открыть сайт</a>' if item["destination_url"] and item["destination_url"].startswith(("https://","http://")) else "—"}</td></tr></tbody></table></div><div class="panel"><h2>Оригинальный HTML · изолированный предпросмотр</h2>{'<iframe class="html-frame" sandbox="" referrerpolicy="no-referrer" srcdoc="'+srcdoc+'"></iframe>' if html_text else '<div class="empty">Для этого креатива HTML не сохранён.</div>'}<p><a class="btn secondary" href="{P(f'/api/creatives/{creative_id}/html')}">Скачать HTML</a></p></div></div><div class="panel"><h2>История обнаружений</h2><div class="table-wrap"><table><thead><tr><th>Запуск</th><th>Площадка</th><th>Дата</th><th>Метод</th><th>Город</th><th>Запрос</th></tr></thead><tbody>{ev_rows}</tbody></table></div></div>"""
    return page(request, f"Креатив #{creative_id}", body, "creatives")


@app.get("/api/creatives/{creative_id}/html")
def creative_html_download(request: Request, creative_id: int):
    if not auth_page(request):
        return RedirectResponse(P("/login"), status_code=303)
    with closing(connect()) as db:
        row = db.execute("SELECT title,html_gzip FROM creatives WHERE id=?", (creative_id,)).fetchone()
    if not row or not row["html_gzip"]:
        raise HTTPException(status_code=404, detail="HTML не найден")
    try:
        raw = gzip.decompress(row["html_gzip"])
    except Exception as exc:
        raise HTTPException(status_code=500, detail="HTML повреждён") from exc
    from fastapi.responses import Response
    return Response(raw, media_type="text/html; charset=utf-8", headers={"Content-Disposition": f'attachment; filename="parsya-creative-{creative_id}.html"', "X-Content-Type-Options": "nosniff"})


@app.get("/admin/users", response_class=HTMLResponse)
def admin_users_page(request: Request, created_email: str = "", created_password: str = "", error: str = ""):
    redirect = admin_or_redirect(request)
    if redirect:
        return redirect
    with closing(connect()) as db:
        rows = db.execute("SELECT id,email,role,is_active,created_at,last_login_at FROM users ORDER BY created_at DESC").fetchall()
    def role_badge(role: str) -> str:
        return f'<span class="badge {"ok" if role == "admin" else ""}">{esc(role)}</span>'
    def status_badge(active: int) -> str:
        return '<span class="badge ok">активен</span>' if active else '<span class="badge failed">отключён</span>'
    table_rows = "".join(
        f'<tr><td>{esc(u["email"])}</td><td>{role_badge(u["role"])}</td><td>{status_badge(u["is_active"])}</td>'
        f'<td>{esc(u["created_at"])}</td><td>{esc(u["last_login_at"] or "—")}</td>'
        f'<td class="right">{"" if not u["is_active"] else "<form method=\"post\" action=\"" + P("/admin/users/") + str(u["id"]) + "/deactivate\" style=\"display:inline\"><input type=\"hidden\" name=\"csrf_token\" value=\"" + esc(csrf_token(request)) + "\"><button class=\"btn secondary\">Отключить</button></form>"}</td></tr>'
        for u in rows
    ) or '<tr><td colspan="6" class="empty">Пользователей SaaS пока нет</td></tr>'
    notice = ""
    if created_email:
        notice = (f'<div class="notice">Аккаунт <b>{esc(created_email)}</b> создан. Временный пароль: '
                  f'<span class="mono">{esc(created_password)}</span> — передайте его маркетологу и попросите сменить при первом входе.</div>')
    if error:
        notice = f'<div class="notice error-text">{esc(error)}</div>'
    body = f"""{notice}
<div class="panel"><div class="panel-head"><h2>Новый аккаунт маркетолога</h2></div>
<form method="post" action="{P('/admin/users')}" class="formgrid">
<input type="hidden" name="csrf_token" value="{esc(csrf_token(request))}">
<div class="field"><label>Email</label><input type="email" name="email" required maxlength="255"></div>
<div class="field"><label>Роль</label><select name="role"><option value="marketer">Маркетолог</option><option value="admin">Админ SaaS</option></select></div>
<div class="field" style="grid-column:1/-1"><button class="btn">Создать и выдать пароль</button></div>
</form>
<div class="help">Пароль генерируется автоматически и показывается один раз. Эти аккаунты — для входа во фронтенд-сервис (не в эту панель воркера).</div>
</div>
<div class="panel"><div class="panel-head"><h2>Аккаунты SaaS-фронтенда</h2></div>
<div class="table-wrap"><table><thead><tr><th>Email</th><th>Роль</th><th>Статус</th><th>Создан</th><th>Последний вход</th><th></th></tr></thead><tbody>{table_rows}</tbody></table></div></div>"""
    return page(request, "Пользователи SaaS", body, "users")


@app.post("/admin/users", response_class=HTMLResponse)
def admin_users_create(
    request: Request, email: str = Form(...), role: str = Form("marketer"),
    csrf_token_value: str = Form(..., alias="csrf_token"),
):
    redirect = admin_or_redirect(request)
    if redirect:
        return redirect
    check_csrf(request, csrf_token_value)
    email = email.strip().casefold()[:255]
    role = role if role in {"marketer", "admin"} else "marketer"
    temp_password = secrets.token_urlsafe(9)
    with closing(connect()) as db:
        existing = db.execute("SELECT id FROM users WHERE email=?", (email,)).fetchone()
        if existing:
            return RedirectResponse(P(f"/admin/users?error=Email+{email}+уже+занят"), status_code=303)
        db.execute(
            "INSERT INTO users(email,password_hash,role,is_active,created_at) VALUES(?,?,?,1,?)",
            (email, hash_password(temp_password), role, now_iso()),
        )
        db.commit()
    return RedirectResponse(P(f"/admin/users?created_email={email}&created_password={temp_password}"), status_code=303)


@app.post("/admin/users/{user_id}/deactivate")
def admin_users_deactivate(request: Request, user_id: int, csrf_token_value: str = Form(..., alias="csrf_token")):
    redirect = admin_or_redirect(request)
    if redirect:
        return redirect
    check_csrf(request, csrf_token_value)
    with closing(connect()) as db:
        db.execute("UPDATE users SET is_active=0 WHERE id=?", (user_id,))
        db.execute("DELETE FROM user_tokens WHERE user_id=?", (user_id,))
        db.commit()
    return RedirectResponse(P("/admin/users"), status_code=303)


@app.post("/api/worker/poll")
def worker_poll(payload: PollIn, authorization: str | None = Header(default=None)):
    worker_auth(authorization)
    with closing(connect()) as db:
        db.execute("BEGIN IMMEDIATE")
        db.execute(
            "INSERT INTO workers(worker_id,hostname,version,last_seen,state,current_job_id) VALUES(?,?,?,?, 'idle',NULL) "
            "ON CONFLICT(worker_id) DO UPDATE SET hostname=excluded.hostname,version=excluded.version,last_seen=excluded.last_seen",
            (payload.worker_id, payload.hostname, payload.version, now_iso()),
        )
        job = db.execute("SELECT * FROM jobs WHERE status='queued' AND (retry_at IS NULL OR retry_at<=?) ORDER BY id LIMIT 1",
                         (now_iso(),)).fetchone()
        if not job:
            _sync_worker_state(db, payload.worker_id)
            db.commit()
            return {"job": None, "poll_after_seconds": 5}
        db.execute("UPDATE jobs SET status='running',started_at=?,worker_id=? WHERE id=? AND status='queued'", (now_iso(), payload.worker_id, job["id"]))
        db.execute("UPDATE workers SET state='running',current_job_id=?,last_seen=? WHERE worker_id=?", (job["id"], now_iso(), payload.worker_id))
        db.commit()
        return {"job": {"id": job["id"], "type": "run_parser", "city": job["city"], "key_text": job["key_text"]}, "poll_after_seconds": 2}


@app.post("/api/worker/heartbeat")
def worker_heartbeat(payload: PollIn, authorization: str | None = Header(default=None)):
    worker_auth(authorization)
    with closing(connect()) as db:
        db.execute(
            "INSERT INTO workers(worker_id,hostname,version,last_seen,state) VALUES(?,?,?,?, 'idle') "
            "ON CONFLICT(worker_id) DO UPDATE SET hostname=excluded.hostname,version=excluded.version,last_seen=excluded.last_seen",
            (payload.worker_id, payload.hostname, payload.version, now_iso()),
        )
        db.commit()
    return {"ok": True, "server_time": now_iso()}


@app.post("/api/worker/jobs/{job_id}/log")
def worker_log(job_id: int, payload: LogIn, authorization: str | None = Header(default=None)):
    worker_auth(authorization)
    with closing(connect()) as db:
        cur = db.execute("UPDATE jobs SET log_text=substr(log_text || ?, -500000) WHERE id=? AND status='running'", (payload.text, job_id))
        db.commit()
        if cur.rowcount != 1:
            raise HTTPException(status_code=404, detail="Активное задание не найдено")
    return {"ok": True}


@app.post("/api/worker/jobs/{job_id}/creatives")
def worker_creatives(job_id: int, payload: CreativesBatch, authorization: str | None = Header(default=None)):
    worker_auth(authorization)
    accepted = 0
    new_count = 0
    with closing(connect()) as db:
        job = db.execute("SELECT id,status FROM jobs WHERE id=?", (job_id,)).fetchone()
        if not job:
            raise HTTPException(status_code=404, detail="Задание не найдено")
        for item in payload.creatives:
            platform = item.platform.strip()[:255]
            source_url = item.source_page_url.strip()[:8000]
            digest = hashlib.sha256(item.html.encode("utf-8", "replace")).hexdigest() if item.html else ""
            content_hash = item.creative_hash.strip()
            if not content_hash:
                canonical = "|".join([platform, item.domain, item.title, item.description, item.image_url])
                content_hash = hashlib.sha256(canonical.encode("utf-8", "replace")).hexdigest()
            if not platform:
                continue
            captured_at = item.captured_at or now_iso()
            html_gzip = gzip.compress(item.html.encode("utf-8", "replace"), compresslevel=6, mtime=0) if item.html else None
            exists = db.execute("SELECT id FROM creatives WHERE creative_hash=?", (content_hash,)).fetchone()
            cur = db.execute(
                "INSERT INTO creatives(creative_hash,content_fingerprint,platform,domain,title,description,image_url,destination_url,source_page_url,detection_method,recognition_status,captured_at,html_gzip,html_sha256,first_job_id,last_job_id) "
                "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(creative_hash) DO UPDATE SET "
                "last_job_id=excluded.last_job_id,"
                "content_fingerprint=CASE WHEN COALESCE(creatives.content_fingerprint,'')='' THEN excluded.content_fingerprint ELSE creatives.content_fingerprint END,"
                "domain=CASE WHEN creatives.domain='' THEN excluded.domain ELSE creatives.domain END,"
                "title=CASE WHEN creatives.title='' THEN excluded.title ELSE creatives.title END,"
                "description=CASE WHEN creatives.description='' THEN excluded.description ELSE creatives.description END,"
                "image_url=CASE WHEN creatives.image_url='' THEN excluded.image_url ELSE creatives.image_url END,"
                "destination_url=CASE WHEN creatives.destination_url='' THEN excluded.destination_url ELSE creatives.destination_url END,"
                "html_gzip=CASE WHEN excluded.html_gzip IS NOT NULL THEN excluded.html_gzip ELSE creatives.html_gzip END,"
                "html_sha256=CASE WHEN excluded.html_sha256<>'' THEN excluded.html_sha256 ELSE creatives.html_sha256 END,"
                "recognition_status=CASE WHEN excluded.recognition_status='recognized' THEN 'recognized' ELSE creatives.recognition_status END",
                (content_hash, item.content_fingerprint, platform, item.domain, item.title, item.description, item.image_url, item.destination_url,
                 source_url, item.detection_method, item.recognition_status, captured_at, html_gzip, digest,
                 job_id, job_id),
            )
            creative_row = db.execute("SELECT id FROM creatives WHERE creative_hash=?", (content_hash,)).fetchone()
            creative_id = creative_row["id"]
            db.execute(
                "INSERT OR IGNORE INTO occurrences(job_id,creative_id,platform,source_page_url,detection_method,recognition_status,observed_at) VALUES(?,?,?,?,?,?,?)",
                (job_id, creative_id, platform, source_url, item.detection_method, item.recognition_status, captured_at),
            )
            accepted += 1
            if not exists:
                new_count += 1
        db.execute("UPDATE jobs SET total_creatives=(SELECT COUNT(DISTINCT creative_id) FROM occurrences WHERE job_id=?) WHERE id=?", (job_id, job_id))
        db.commit()
    return {"ok": True, "accepted": accepted, "new_creatives": new_count}


@app.post("/api/worker/jobs/{job_id}/extras")
def worker_creative_extras(job_id: int, payload: CreativeExtrasBatch, authorization: str | None = Header(default=None)):
    """Media format (video src/poster/duration), CTA button text, price/discount and
    app-store ids — synced separately from the base creative row since they come from
    ParsYa's own enrichment tables (parsya_creative_media_classification/_attributes),
    not from the scraper's core detection loop. Only fills currently-empty fields, same
    rule as worker_creatives, so a later run never overwrites a value with a blank."""
    worker_auth(authorization)
    accepted = 0
    with closing(connect()) as db:
        job = db.execute("SELECT id FROM jobs WHERE id=?", (job_id,)).fetchone()
        if not job:
            raise HTTPException(status_code=404, detail="Задание не найдено")
        for item in payload.extras:
            creative_hash = item.creative_hash.strip()
            if not creative_hash:
                continue
            row = db.execute("SELECT id FROM creatives WHERE creative_hash=?", (creative_hash,)).fetchone()
            if not row:
                continue
            db.execute(
                "UPDATE creatives SET "
                "creative_type=CASE WHEN COALESCE(creative_type,'')='' THEN ? ELSE creative_type END,"
                "video_src=CASE WHEN COALESCE(video_src,'')='' THEN ? ELSE video_src END,"
                "video_poster=CASE WHEN COALESCE(video_poster,'')='' THEN ? ELSE video_poster END,"
                "video_duration=CASE WHEN COALESCE(video_duration,'')='' THEN ? ELSE video_duration END,"
                "cta_text=CASE WHEN COALESCE(cta_text,'')='' THEN ? ELSE cta_text END,"
                "price_text=CASE WHEN COALESCE(price_text,'')='' THEN ? ELSE price_text END,"
                "price_rub=CASE WHEN price_rub IS NULL THEN ? ELSE price_rub END,"
                "discount_percent=CASE WHEN discount_percent IS NULL THEN ? ELSE discount_percent END,"
                "erid=CASE WHEN COALESCE(erid,'')='' THEN ? ELSE erid END,"
                "app_package_id=CASE WHEN COALESCE(app_package_id,'')='' THEN ? ELSE app_package_id END,"
                "app_store_id=CASE WHEN COALESCE(app_store_id,'')='' THEN ? ELSE app_store_id END,"
                "carousel_id=CASE WHEN COALESCE(carousel_id,'')='' THEN ? ELSE carousel_id END,"
                "carousel_slide_index=CASE WHEN carousel_slide_index IS NULL THEN ? ELSE carousel_slide_index END "
                "WHERE creative_hash=?",
                (item.creative_type, item.video_src, item.video_poster, item.video_duration,
                 item.cta_text, item.price_text, item.price_rub, item.discount_percent,
                 item.erid, item.app_package_id, item.app_store_id,
                 item.carousel_id, item.carousel_slide_index, creative_hash),
            )
            accepted += 1
        db.commit()
    return {"ok": True, "accepted": accepted}


@app.post("/api/worker/jobs/{job_id}/secondary-links")
def worker_secondary_links(job_id: int, payload: SecondaryLinksBatch, authorization: str | None = Header(default=None)):
    """Other clickable links/buttons of an ad (sitelinks, a second CTA) beyond the one
    main destination_url — each with its own UTM. Upserts by (creative_id, tracking_url)
    so a re-sync of the same job just refreshes resolved UTM fields in place."""
    worker_auth(authorization)
    accepted = 0
    with closing(connect()) as db:
        job = db.execute("SELECT id FROM jobs WHERE id=?", (job_id,)).fetchone()
        if not job:
            raise HTTPException(status_code=404, detail="Задание не найдено")
        for item in payload.links:
            creative_hash = item.creative_hash.strip()
            tracking_url = item.tracking_url.strip()
            if not creative_hash or not tracking_url:
                continue
            row = db.execute("SELECT id FROM creatives WHERE creative_hash=?", (creative_hash,)).fetchone()
            if not row:
                continue
            db.execute(
                "INSERT INTO creative_secondary_links"
                "(creative_id,link_text,tracking_url,final_url,final_url_domain,"
                " utm_source,utm_medium,utm_campaign,utm_content,utm_term,resolve_status) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?) "
                "ON CONFLICT(creative_id,tracking_url) DO UPDATE SET "
                "link_text=excluded.link_text, final_url=excluded.final_url, "
                "final_url_domain=excluded.final_url_domain, utm_source=excluded.utm_source, "
                "utm_medium=excluded.utm_medium, utm_campaign=excluded.utm_campaign, "
                "utm_content=excluded.utm_content, utm_term=excluded.utm_term, "
                "resolve_status=excluded.resolve_status",
                (row["id"], item.link_text, tracking_url, item.final_url, item.final_url_domain,
                 item.utm_source, item.utm_medium, item.utm_campaign, item.utm_content, item.utm_term,
                 item.resolve_status),
            )
            accepted += 1
        db.commit()
    return {"ok": True, "accepted": accepted}


@app.post("/api/worker/jobs/{job_id}/utm")
def worker_creative_utm(job_id: int, payload: CreativeUtmBatch, authorization: str | None = Header(default=None)):
    """UTM of the creative's OWN (primary) destination_url — one row per creative, latest
    resolve wins. Independent of advertiser disclosure: a creative with no revealed name/ИНН
    still has real UTM data worth filtering/reporting on."""
    worker_auth(authorization)
    accepted = 0
    with closing(connect()) as db:
        job = db.execute("SELECT id FROM jobs WHERE id=?", (job_id,)).fetchone()
        if not job:
            raise HTTPException(status_code=404, detail="Задание не найдено")
        for item in payload.items:
            creative_hash = item.creative_hash.strip()
            if not creative_hash:
                continue
            row = db.execute("SELECT id FROM creatives WHERE creative_hash=?", (creative_hash,)).fetchone()
            if not row:
                continue
            db.execute(
                "INSERT INTO creative_utm"
                "(creative_id,final_url,final_url_domain,utm_source,utm_medium,utm_campaign,"
                " utm_content,utm_term,resolve_status,resolved_at) "
                "VALUES (?,?,?,?,?,?,?,?,?,?) "
                "ON CONFLICT(creative_id) DO UPDATE SET "
                "final_url=excluded.final_url, final_url_domain=excluded.final_url_domain, "
                "utm_source=excluded.utm_source, utm_medium=excluded.utm_medium, "
                "utm_campaign=excluded.utm_campaign, utm_content=excluded.utm_content, "
                "utm_term=excluded.utm_term, resolve_status=excluded.resolve_status, "
                "resolved_at=excluded.resolved_at",
                (row["id"], item.final_url, item.final_url_domain, item.utm_source, item.utm_medium,
                 item.utm_campaign, item.utm_content, item.utm_term, item.resolve_status, item.resolved_at),
            )
            accepted += 1
        db.commit()
    return {"ok": True, "accepted": accepted}


@app.post("/api/worker/jobs/{job_id}/destination-history")
def worker_destination_history(job_id: int, payload: DestinationHistoryBatch, authorization: str | None = Header(default=None)):
    """Append-only landing-page/UTM change log per creative, synced from MySQL's
    parsya_creative_destination_history (already deduped upstream by change_signature —
    this just mirrors each distinct change as its own row, never overwrites one)."""
    worker_auth(authorization)
    accepted = 0
    with closing(connect()) as db:
        job = db.execute("SELECT id FROM jobs WHERE id=?", (job_id,)).fetchone()
        if not job:
            raise HTTPException(status_code=404, detail="Задание не найдено")
        for item in payload.items:
            creative_hash = item.creative_hash.strip()
            if not creative_hash:
                continue
            row = db.execute("SELECT id FROM creatives WHERE creative_hash=?", (creative_hash,)).fetchone()
            if not row:
                continue
            db.execute(
                "INSERT INTO creative_destination_history"
                "(creative_id,change_signature,tracking_url,final_url,final_url_domain,"
                " utm_source,utm_medium,utm_campaign,utm_content,utm_term,first_seen_at,last_seen_at) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?,?) "
                "ON CONFLICT(creative_id,change_signature) DO UPDATE SET last_seen_at=excluded.last_seen_at",
                (row["id"], item.change_signature, item.tracking_url, item.final_url, item.final_url_domain,
                 item.utm_source, item.utm_medium, item.utm_campaign, item.utm_content, item.utm_term,
                 item.first_seen_at, item.last_seen_at),
            )
            accepted += 1
        db.commit()
    return {"ok": True, "accepted": accepted}


@app.post("/api/worker/jobs/{job_id}/landing-mismatch")
def worker_landing_mismatch(job_id: int, payload: LandingMismatchBatch, authorization: str | None = Header(default=None)):
    """advertised_site_domain (from the advertiser's own resolved profile) vs. the domain
    the ad actually lands on — mirrors MySQL's parsya_creative_full.landing_domain_mismatch.
    Only UPDATEs an existing creative_utm row (written by the main UTM sync); a creative
    with no UTM/destination data yet has nothing meaningful to compare against."""
    worker_auth(authorization)
    accepted = 0
    with closing(connect()) as db:
        job = db.execute("SELECT id,owner_user_id FROM jobs WHERE id=?", (job_id,)).fetchone()
        if not job:
            raise HTTPException(status_code=404, detail="Задание не найдено")
        for item in payload.items:
            creative_hash = item.creative_hash.strip()
            if not creative_hash:
                continue
            row = db.execute("SELECT id,title,domain FROM creatives WHERE creative_hash=?", (creative_hash,)).fetchone()
            if not row:
                continue
            was_mismatch = db.execute(
                "SELECT landing_domain_mismatch FROM creative_utm WHERE creative_id=?", (row["id"],)
            ).fetchone()
            is_new_mismatch = bool(item.landing_domain_mismatch) and not (was_mismatch and was_mismatch["landing_domain_mismatch"])
            result = db.execute(
                "UPDATE creative_utm SET advertised_site_domain=?, landing_domain_mismatch=? WHERE creative_id=?",
                (item.advertised_site_domain, 1 if item.landing_domain_mismatch else 0, row["id"]),
            )
            if result.rowcount:
                accepted += 1
                if is_new_mismatch:
                    create_notification(
                        db, job["owner_user_id"], "landing_mismatch",
                        f"Несовпадение посадочной: «{row['title'] or row['domain'] or creative_hash}»",
                        f"Реклама ведёт не на {item.advertised_site_domain or 'сайт рекламодателя'}",
                        "intel-advertisers",
                    )
        db.commit()
    return {"ok": True, "accepted": accepted}


@app.post("/api/worker/jobs/{job_id}/identifiers")
def worker_creative_identifiers(job_id: int, payload: CreativeIdentifiersBatch, authorization: str | None = Header(default=None)):
    """campaign_id/banner_id/client_id/creative_id candidates (markup-derived), for ANY
    creative already uploaded in this run — independent of advertiser disclosure. Separated
    from /advertiser-links on purpose: the old path only synced identifiers for creatives
    that already had a resolved advertiser link, silently dropping them for everything else
    (most creatives, historically)."""
    worker_auth(authorization)
    accepted = 0
    with closing(connect()) as db:
        job = db.execute("SELECT id FROM jobs WHERE id=?", (job_id,)).fetchone()
        if not job:
            raise HTTPException(status_code=404, detail="Задание не найдено")
        now = now_iso()
        for item in payload.items:
            creative_hash = item.creative_hash.strip()
            if not creative_hash:
                continue
            row = db.execute("SELECT id FROM creatives WHERE creative_hash=?", (creative_hash,)).fetchone()
            if not row:
                continue
            creative_id = int(row["id"])
            for ident in item.identifiers:
                id_type = ident.type.strip()[:64]
                id_value = ident.value.strip()[:500]
                if not id_type or not id_value:
                    continue
                db.execute(
                    "INSERT INTO creative_identifiers(creative_id,identifier_type,identifier_value,first_seen_at,last_seen_at) "
                    "VALUES(?,?,?,?,?) ON CONFLICT(creative_id,identifier_type,identifier_value) DO UPDATE SET last_seen_at=excluded.last_seen_at",
                    (creative_id, id_type, id_value, now, now),
                )
            accepted += 1
        db.commit()
    return {"ok": True, "accepted": accepted}


@app.post("/api/worker/coverage-report")
def worker_coverage_report(payload: CoverageReportBatch, authorization: str | None = Header(default=None)):
    """Advertiser-disclosure coverage %, grouped by platform/detection_method/creative_type —
    mirrors MySQL's parsya_advertiser_coverage_report VIEW. A global aggregate, not scoped to
    one job: each sync fully REPLACES the table rather than accumulating rows."""
    worker_auth(authorization)
    now = now_iso()
    with closing(connect()) as db:
        db.execute("DELETE FROM advertiser_coverage_report")
        for row in payload.rows:
            db.execute(
                "INSERT INTO advertiser_coverage_report"
                "(platform,detection_method,creative_type,total_creatives,disclosed_creatives,disclosed_pct,synced_at) "
                "VALUES (?,?,?,?,?,?,?)",
                (row.platform, row.detection_method, row.creative_type,
                 row.total_creatives, row.disclosed_creatives, row.disclosed_pct, now),
            )
        db.commit()
    return {"ok": True, "accepted": len(payload.rows)}


@app.post("/api/worker/jobs/{job_id}/advertiser-links")
def worker_advertiser_links(job_id: int, payload: AdvertiserLinksBatch, authorization: str | None = Header(default=None)):
    """Accepts advertiser (ИНН) identity links for creatives already uploaded in this job.

    A creative that has not been synced yet (no matching creative_hash) is skipped rather
    than guessed at: the worker always sends creatives before advertiser-links for the same
    job, so a miss here means the creative genuinely was not part of this run's batch.
    """
    worker_auth(authorization)
    accepted = 0
    skipped_unknown_creative = 0
    with closing(connect()) as db:
        job = db.execute("SELECT id FROM jobs WHERE id=?", (job_id,)).fetchone()
        if not job:
            raise HTTPException(status_code=404, detail="Задание не найдено")
        now = now_iso()
        for link in payload.links:
            creative = db.execute("SELECT id FROM creatives WHERE creative_hash=?", (link.creative_hash,)).fetchone()
            if not creative:
                skipped_unknown_creative += 1
                continue
            creative_id = int(creative["id"])
            if link.content_fingerprint:
                db.execute(
                    "UPDATE creatives SET content_fingerprint=? WHERE id=? AND COALESCE(content_fingerprint,'')=''",
                    (link.content_fingerprint, creative_id),
                )
            inn = link.advertiser_inn.strip() or None
            # Dima (2026-10-22): "режется название у организаций, вижу только ОБЩЕСТВО С
            # ОГРАНИЧЕННОЙ ОТВЕТСТВЕННОСТЬЮ, а самого имени нет". Root cause was a regex bug
            # in parsya_advertiser_enrichment.py (now fixed) that truncated names at an
            # escaped quote inside the raw JSON — but the OLD "fill only if currently empty"
            # rule here meant a truncated name, once written, could NEVER self-heal on a
            # later sync even after the source bug was fixed (the field wasn't empty, just
            # wrong). Now prefers the LONGER non-empty name on conflict — a later, correctly
            # un-truncated sync overwrites a shorter/truncated one; a shorter/empty value
            # never overwrites a longer one already stored.
            db.execute(
                "INSERT INTO advertisers(identity_key,inn,canonical_name,address,phone,website_url,first_seen_at,last_seen_at) "
                "VALUES(?,?,?,?,?,?,?,?) "
                "ON CONFLICT(identity_key) DO UPDATE SET "
                "inn=COALESCE(advertisers.inn,excluded.inn),"
                "canonical_name=CASE "
                "WHEN COALESCE(excluded.canonical_name,'')='' THEN advertisers.canonical_name "
                "WHEN COALESCE(advertisers.canonical_name,'')='' THEN excluded.canonical_name "
                "WHEN LENGTH(excluded.canonical_name)>LENGTH(advertisers.canonical_name) THEN excluded.canonical_name "
                "ELSE advertisers.canonical_name END,"
                "address=COALESCE(NULLIF(advertisers.address,''),excluded.address),"
                "phone=COALESCE(NULLIF(advertisers.phone,''),excluded.phone),"
                "website_url=COALESCE(NULLIF(advertisers.website_url,''),excluded.website_url),"
                "last_seen_at=excluded.last_seen_at",
                (link.advertiser_identity_key, inn, link.advertiser_name or None,
                 link.address or None, link.phone or None, link.website_url or None, now, now),
            )
            advertiser = db.execute(
                "SELECT id FROM advertisers WHERE identity_key=?", (link.advertiser_identity_key,)
            ).fetchone()
            advertiser_id = int(advertiser["id"])
            db.execute(
                "INSERT INTO creative_advertiser_links(creative_id,advertiser_id,association_method,confidence,first_seen_at,last_seen_at) "
                "VALUES(?,?,?,?,?,?) ON CONFLICT(creative_id,advertiser_id,association_method) DO UPDATE SET "
                "confidence=excluded.confidence,last_seen_at=excluded.last_seen_at",
                (creative_id, advertiser_id, link.association_method, link.confidence, now, now),
            )
            for ident in link.identifiers:
                id_type = ident.type.strip()[:64]
                id_value = ident.value.strip()[:500]
                if not id_type or not id_value:
                    continue
                db.execute(
                    "INSERT INTO creative_identifiers(creative_id,identifier_type,identifier_value,first_seen_at,last_seen_at) "
                    "VALUES(?,?,?,?,?) ON CONFLICT(creative_id,identifier_type,identifier_value) DO UPDATE SET last_seen_at=excluded.last_seen_at",
                    (creative_id, id_type, id_value, now, now),
                )
            accepted += 1
        db.commit()
    return {"ok": True, "accepted": accepted, "skipped_unknown_creative": skipped_unknown_creative}


@app.post("/api/worker/jobs/{job_id}/sites")
def worker_site_results(job_id: int, payload: SiteResultsBatch, authorization: str | None = Header(default=None)):
    worker_auth(authorization)
    with closing(connect()) as db:
        job = db.execute("SELECT id FROM jobs WHERE id=?", (job_id,)).fetchone()
        if not job:
            raise HTTPException(status_code=404, detail="Задание не найдено")
        for site in payload.sites:
            db.execute(
                "INSERT INTO site_results(job_id,platform,url,status,duration_seconds,scrolls_done,ad_signal_detected,unique_creatives,partial_creatives,detection_methods,dom_snapshot_path,error_text) "
                "VALUES(?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(job_id,platform) DO UPDATE SET "
                "url=excluded.url,status=excluded.status,duration_seconds=excluded.duration_seconds,scrolls_done=excluded.scrolls_done,"
                "ad_signal_detected=excluded.ad_signal_detected,unique_creatives=excluded.unique_creatives,partial_creatives=excluded.partial_creatives,"
                "detection_methods=excluded.detection_methods,dom_snapshot_path=excluded.dom_snapshot_path,error_text=excluded.error_text",
                (job_id, site.platform.strip(), site.url, site.status, site.duration_seconds, site.scrolls_done,
                 1 if site.ad_signal_detected else 0, site.unique_creatives, site.partial_creatives,
                 site.detection_methods, site.dom_snapshot_path, site.error_text),
            )
        db.commit()
    return {"ok": True, "sites_received": len(payload.sites)}


def _sync_worker_state(db, worker_id: str, last_error: str | None = None) -> None:
    """Воркер ведёт несколько запусков параллельно: он 'running', пока идёт хотя бы один."""
    cur = db.execute("SELECT MAX(id) m FROM jobs WHERE worker_id=? AND status='running'", (worker_id,)).fetchone()
    running = cur["m"] if cur else None
    db.execute("UPDATE workers SET state=?,current_job_id=?,last_seen=?,last_error=COALESCE(?,last_error) WHERE worker_id=?",
               ("running" if running else "idle", running, now_iso(), last_error, worker_id))


MAX_ACTIVE_SCHEDULES = int(os.getenv("PARSYA_MAX_ACTIVE_SCHEDULES", "50"))
MAX_MANUAL_RUNS = int(os.getenv("PARSYA_MAX_MANUAL_RUNS", "20"))
JOB_MAX_ATTEMPTS = int(os.getenv("PARSYA_JOB_MAX_ATTEMPTS", "10"))
JOB_RETRY_DELAYS_MIN = [2, 5, 10, 15, 20, 30]  # дальше — каждые 30 минут


@app.post("/api/worker/jobs/{job_id}/finish")
def worker_finish(job_id: int, payload: FinishIn, authorization: str | None = Header(default=None)):
    worker_auth(authorization)
    with closing(connect()) as db:
        job = db.execute("SELECT worker_id,owner_user_id,key_text,city,attempts FROM jobs WHERE id=?", (job_id,)).fetchone()
        if not job:
            raise HTTPException(status_code=404, detail="Задание не найдено")
        if payload.status == "failed":
            done = (job["attempts"] or 0) + 1
            if done < JOB_MAX_ATTEMPTS:
                # Сбой чаще всего инфраструктурный (WSL без сети/DNS, VPN) — пробуем снова позже, с растущей паузой
                delay = JOB_RETRY_DELAYS_MIN[min(done - 1, len(JOB_RETRY_DELAYS_MIN) - 1)]
                retry_at = (datetime.now(timezone.utc) + timedelta(minutes=delay)).isoformat(timespec="seconds")
                note = f"\n\n[control] Попытка {done} из {JOB_MAX_ATTEMPTS} не удалась ({payload.error or 'ошибка'}). Повтор через {delay} мин.\n"
                db.execute(
                    "UPDATE jobs SET status='queued',attempts=?,retry_at=?,started_at=NULL,worker_id=NULL,finished_at=NULL,"
                    "exit_code=?,error_text=?,log_text=COALESCE(log_text,'')||? WHERE id=?",
                    (done, retry_at, payload.exit_code, f"Попытка {done} из {JOB_MAX_ATTEMPTS}: {payload.error or 'ошибка'}", note, job_id))
                if job["worker_id"]:
                    _sync_worker_state(db, job["worker_id"], payload.error or None)
                db.commit()
                return {"ok": True, "retry_at": retry_at, "attempt": done}
        db.execute(
            "UPDATE jobs SET attempts=attempts+1,status=?,finished_at=?,exit_code=?,error_text=?,result_json=?,total_creatives=(SELECT COUNT(DISTINCT creative_id) FROM occurrences WHERE job_id=?) WHERE id=?",
            (payload.status, now_iso(), payload.exit_code, payload.error or None,
             json.dumps(payload.result, ensure_ascii=False), job_id, job_id),
        )
        if job["worker_id"]:
            _sync_worker_state(db, job["worker_id"], payload.error or None)
        label = job["key_text"] or f"№{job_id}"
        if payload.status == "succeeded":
            total = db.execute("SELECT total_creatives FROM jobs WHERE id=?", (job_id,)).fetchone()["total_creatives"]
            create_notification(db, job["owner_user_id"], "scan_done", f"Сканирование «{label}» завершено",
                                 f"{job['city'] or ''} · найдено {total or 0}".strip(" ·"), f"intel-scans")
        elif payload.status == "failed":
            create_notification(db, job["owner_user_id"], "scan_failed", f"Сканирование «{label}» завершилось с ошибкой",
                                 payload.error or "", "intel-scans")
        db.commit()
    return {"ok": True}


def notify_telegram_captcha(job_id: int, url: str = "", title: str = "") -> bool:
    """Asks the production server (info517.fvds.ru) to send the Telegram CAPTCHA alert,
    instead of calling api.telegram.org directly from here.

    Dima (2026-10-22): "уведомления в телегу почему-то перестали приходить, нужно
    проправить". Root cause, confirmed live: this WSL machine's network cannot reach
    api.telegram.org at all (DNS resolves fine, but both the IPv6 and IPv4 routes time out —
    confirmed via a direct `curl -v`, while github.com/ya.ru work normally from the same
    host) — a network-level block, not a code bug. The production server (which already has
    its own working copy of the Telegram-send code, confirmed live) does NOT have this
    block, so it now does the actual sending via a small relay endpoint
    (POST /api/relay/telegram-captcha, same worker-token auth as any other worker call).
    Decided with Dima: keep the rest of the control plane on WSL, but this one piece of
    functionality has to live where the network actually allows it."""
    relay_url = os.getenv("PARSYA_TELEGRAM_RELAY_URL", "").strip()
    if not relay_url:
        print("[telegram] PARSYA_TELEGRAM_RELAY_URL не задан — relay отключён.", flush=True)
        return False
    if not WORKER_TOKEN:
        print("[telegram] PARSYA_WORKER_TOKEN не задан — relay отключён.", flush=True)
        return False

    import urllib.request

    request = urllib.request.Request(
        relay_url,
        data=json.dumps({"job_id": job_id, "url": url, "title": title}, ensure_ascii=False).encode("utf-8"),
        headers={"Content-Type": "application/json", "Authorization": f"Bearer {WORKER_TOKEN}"},
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=10) as response:
        result = json.loads(response.read().decode("utf-8"))

    if not result.get("telegram_notified"):
        raise RuntimeError(f"Relay не подтвердил отправку: {result}")

    print(f"[telegram] Уведомление о CAPTCHA отправлено (через relay) для запуска #{job_id}.", flush=True)
    return True


def notify_telegram_alert(text: str) -> bool:
    """Generic sibling of notify_telegram_captcha, same relay mechanism (WSL can't reach
    api.telegram.org directly, the production server can) and same chat, for alerts that
    aren't about a CAPTCHA — currently only wired to genuine (non-quota) Gemini embedding
    failures (2026-10-23, Dima: "если получаем ошибку от Gemini, например модель
    перестала существовать или 500, нужно послать уведомление в телеграм в тот же чат
    куда [падает] прохождение капчи"). Best-effort: never raises, so a Telegram outage
    can't also break whatever was already failing."""
    relay_url = os.getenv("PARSYA_TELEGRAM_ALERT_RELAY_URL", "").strip()
    if not relay_url:
        captcha_relay = os.getenv("PARSYA_TELEGRAM_RELAY_URL", "").strip()
        if captcha_relay.endswith("/telegram-captcha"):
            relay_url = captcha_relay[: -len("/telegram-captcha")] + "/telegram-alert"
    if not relay_url or not WORKER_TOKEN:
        print("[telegram] PARSYA_TELEGRAM_ALERT_RELAY_URL/WORKER_TOKEN не задан — alert-relay отключён.", flush=True)
        return False
    try:
        import urllib.request

        request = urllib.request.Request(
            relay_url,
            data=json.dumps({"text": text}, ensure_ascii=False).encode("utf-8"),
            headers={"Content-Type": "application/json", "Authorization": f"Bearer {WORKER_TOKEN}"},
            method="POST",
        )
        with urllib.request.urlopen(request, timeout=10) as response:
            result = json.loads(response.read().decode("utf-8"))
        if not result.get("telegram_notified"):
            raise RuntimeError(f"Relay не подтвердил отправку: {result}")
        print("[telegram] Alert отправлен (через relay).", flush=True)
        return True
    except Exception as exc:  # noqa: BLE001 - best-effort notification, never raise
        print(f"[telegram] Не удалось отправить alert: {exc}", flush=True)
        return False


@app.post("/api/worker/jobs/{job_id}/browser/state")
def worker_browser_state(job_id: int, payload: BrowserStateIn, authorization: str | None = Header(default=None)):
    """Parser subprocess posts a screenshot here roughly once a second while blocked on a
    CAPTCHA, so an admin watching the run can see and solve it (see /admin/jobs/.../browser)."""
    worker_auth(authorization)
    now = now_iso()
    with closing(connect()) as db:
        previous = db.execute(
            "SELECT active,captcha_required FROM browser_sessions WHERE job_id=?", (job_id,)
        ).fetchone()
        # Notify only at the START of a new manual-CAPTCHA episode — not on every ~1s
        # screenshot poll while the same episode is still ongoing.
        should_notify = bool(
            payload.active and payload.captcha_required
            and (previous is None or not previous["active"] or not previous["captcha_required"])
        )
        db.execute(
            "INSERT INTO browser_sessions(job_id,active,captcha_required,url,title,screenshot_b64,viewport_width,viewport_height,updated_at,pending_command_json) "
            "VALUES(?,?,?,?,?,?,?,?,?,NULL) ON CONFLICT(job_id) DO UPDATE SET "
            "active=excluded.active,captcha_required=excluded.captcha_required,url=excluded.url,title=excluded.title,"
            "screenshot_b64=excluded.screenshot_b64,viewport_width=excluded.viewport_width,"
            "viewport_height=excluded.viewport_height,updated_at=excluded.updated_at",
            (job_id, 1 if payload.active else 0, 1 if payload.captcha_required else 0, payload.url, payload.title,
             payload.screenshot_b64, payload.viewport_width, payload.viewport_height, now),
        )
        if should_notify:
            job = db.execute("SELECT owner_user_id,key_text FROM jobs WHERE id=?", (job_id,)).fetchone()
            if job:
                create_notification(db, job["owner_user_id"], "captcha_required",
                                     f"Нужна помощь: капча в задании «{job['key_text'] or job_id}»",
                                     "Откройте задание и решите капчу вручную", "intel-scans")
        db.commit()
    telegram_notified = False
    if should_notify:
        try:
            telegram_notified = notify_telegram_captcha(job_id, payload.url, payload.title)
        except Exception as exc:
            # A Telegram failure must never block the local manual-CAPTCHA panel itself.
            print(f"[telegram] Не удалось отправить уведомление: {type(exc).__name__}: {exc}", flush=True)
    return {"ok": True, "telegram_notified": telegram_notified}


@app.get("/api/worker/jobs/{job_id}/browser/command")
def worker_browser_command(job_id: int, authorization: str | None = Header(default=None)):
    """Parser subprocess polls this for the one pending click/key/text/scroll/refresh
    command an admin queued; delivered at most once (cleared on read)."""
    worker_auth(authorization)
    command = None
    with closing(connect()) as db:
        row = db.execute("SELECT pending_command_json FROM browser_sessions WHERE job_id=?", (job_id,)).fetchone()
        if row and row["pending_command_json"]:
            command = json.loads(row["pending_command_json"])
            db.execute("UPDATE browser_sessions SET pending_command_json=NULL WHERE job_id=?", (job_id,))
            db.commit()
    return {"command": command}


@app.post("/api/worker/jobs/{job_id}/browser/close")
def worker_browser_close(job_id: int, authorization: str | None = Header(default=None)):
    worker_auth(authorization)
    with closing(connect()) as db:
        db.execute(
            "UPDATE browser_sessions SET active=0,captcha_required=0,pending_command_json=NULL,updated_at=? WHERE job_id=?",
            (now_iso(), job_id),
        )
        db.commit()
    return {"ok": True}


@app.get("/api/admin/jobs/{job_id}/browser/state")
def admin_browser_state(request: Request, job_id: int):
    if not auth_page(request):
        raise HTTPException(status_code=401)
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


@app.post("/api/admin/jobs/{job_id}/browser/command")
def admin_browser_command(request: Request, job_id: int, payload: BrowserCommandIn):
    if not auth_page(request):
        raise HTTPException(status_code=401)
    command: dict[str, Any] = {"action": payload.action}
    if payload.action == "click":
        if payload.x_ratio is None or payload.y_ratio is None:
            raise HTTPException(status_code=400, detail="x_ratio/y_ratio обязательны для click")
        command.update({"x_ratio": payload.x_ratio, "y_ratio": payload.y_ratio})
    elif payload.action == "key":
        command["key"] = payload.key
    elif payload.action == "text":
        command["text"] = payload.text
    elif payload.action == "scroll":
        command["scroll_y"] = payload.scroll_y
    with closing(connect()) as db:
        row = db.execute("SELECT job_id FROM browser_sessions WHERE job_id=?", (job_id,)).fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Браузерная сессия для этого задания не активна")
        db.execute("UPDATE browser_sessions SET pending_command_json=? WHERE job_id=?", (json.dumps(command), job_id))
        db.commit()
    return {"ok": True}


@app.get("/api/worker/status")
def worker_status(authorization: str | None = Header(default=None)):
    worker_auth(authorization)
    with closing(connect()) as db:
        rows = db.execute("SELECT worker_id,hostname,version,last_seen,state,current_job_id,last_error FROM workers ORDER BY last_seen DESC").fetchall()
    return {"workers": [dict(row) for row in rows]}


# --- addAvto integration: read-only access to the shared yandex_accounts
# pool in MySQL (WSL), so addAvto (running on the production server, reached
# over the reverse SSH tunnel at 127.0.0.1:8091) can attach a real Yandex ID
# + its Metrika OAuth token to a project without maintaining its own
# separate account list. See ADS_MANAGER_OVERVIEW.md / TASKS.md in ParsYa. ---

def _yandex_pool_row_to_dict(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "login": row["login"],
        "status": row["status"],
        "is_free": bool(row["is_free"]),
        "has_metrika_token": row.get("metrika_access_token") is not None,
        "metrika_token_expires_at": row["metrika_token_expires_at"].isoformat() if row.get("metrika_token_expires_at") else None,
        "metrika_connected_at": row["metrika_connected_at"].isoformat() if row.get("metrika_connected_at") else None,
        # Для дашборда пула в ЛК (2026-11-12). Без паролей/cookies/токенов.
        "created_at": row["created_at"].isoformat() if row.get("created_at") else None,
        "updated_at": row["updated_at"].isoformat() if row.get("updated_at") else None,
        "last_error": (str(row.get("last_error") or "")[:300] or None),
        "has_cookies": bool(row.get("has_cookies")),
        "direct_account_id": row.get("direct_account_id"),
        "direct_connected_at": row["direct_connected_at"].isoformat() if row.get("direct_connected_at") else None,
    }


@app.get("/api/addavto/yandex-pool-accounts")
def addavto_list_yandex_pool_accounts(authorization: str | None = Header(default=None)):
    addavto_auth(authorization)
    import mysql.connector
    conn = mysql.connector.connect(**_yandex_pool_mysql_config())
    try:
        cursor = conn.cursor(dictionary=True)
        cursor.execute(
            "SELECT login, status, is_free, metrika_access_token, metrika_token_expires_at, "
            "metrika_connected_at, created_at, updated_at, last_error, direct_account_id, direct_connected_at, "
            "(cookies_json IS NOT NULL AND cookies_json <> \"\") AS has_cookies "
            "FROM yandex_accounts ORDER BY login"
        )
        return {"items": [_yandex_pool_row_to_dict(row) for row in cursor.fetchall()]}
    finally:
        conn.close()


@app.get("/api/addavto/yandex-pool-accounts/{login}")
def addavto_get_yandex_pool_account(login: str, authorization: str | None = Header(default=None)):
    addavto_auth(authorization)
    import mysql.connector
    conn = mysql.connector.connect(**_yandex_pool_mysql_config())
    try:
        cursor = conn.cursor(dictionary=True)
        cursor.execute(
            "SELECT login, status, is_free, metrika_access_token, metrika_refresh_token, "
            "metrika_token_expires_at, metrika_connected_at FROM yandex_accounts WHERE login=%s",
            (login,),
        )
        row = cursor.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Аккаунт не найден в пуле ParsYa")
        if not row.get("metrika_access_token"):
            raise HTTPException(status_code=409, detail="У этого аккаунта ещё нет Metrika OAuth-токена")
        result = _yandex_pool_row_to_dict(row)
        result["metrika_access_token"] = row["metrika_access_token"]
        result["metrika_refresh_token"] = row["metrika_refresh_token"]
        return result
    finally:
        conn.close()


def _run_addavto_selenium_script(
    script: str,
    args: list[str],
    stdin_payload: dict[str, Any],
    timeout: int,
    timeout_detail: str,
) -> tuple[dict[str, Any] | None, dict[str, Any] | None, str]:
    """Запускает Selenium-скрипт ParsYa (нужен реальный Chrome/дисплей,
    поэтому только здесь, в WSL) и разбирает его RESULT_JSON:/ERROR_JSON:.

    Секреты (логин/пароль/cookies кабинета Click.ru) идут через stdin, не
    аргументами — иначе их видно в `ps`. Возвращает (result, error, tail):
    ровно одно из result/error не None, иначе — 502 с хвостом вывода.
    """
    import subprocess

    parsya_root = Path(__file__).resolve().parent.parent.parent
    command = [str(parsya_root / ".venv" / "bin" / "python3"), str(parsya_root / script), *args]

    env = dict(os.environ)
    env.setdefault("DISPLAY", ":0")
    env.setdefault("WAYLAND_DISPLAY", "wayland-0")

    # subprocess.run's own timeout kills via SIGKILL directly, which the
    # child can't catch at all — its own cleanup (releasing the pool-login
    # claim) never runs, leaving the login stuck "claimed" forever (seen
    # live on the first real timeout). Send SIGTERM first and give the
    # child's own handler (provision_next_direct_account.py) a few seconds
    # to release the claim before escalating to SIGKILL.
    proc = subprocess.Popen(
        command, cwd=str(parsya_root), env=env,
        stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
    )
    try:
        stdout, stderr = proc.communicate(input=json.dumps(stdin_payload), timeout=timeout)
    except subprocess.TimeoutExpired:
        proc.terminate()
        try:
            proc.communicate(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.communicate()
        raise HTTPException(status_code=504, detail=timeout_detail)

    for line in stdout.splitlines():
        if line.startswith("RESULT_JSON:"):
            return json.loads(line[len("RESULT_JSON:"):]), None, ""
        if line.startswith("ERROR_JSON:"):
            return None, json.loads(line[len("ERROR_JSON:"):]), ""

    raise HTTPException(
        status_code=502,
        detail=f"Скрипт {script} завершился без распознаваемого результата (exit={proc.returncode}): "
        f"{stdout[-2000:]}\n{stderr[-2000:]}",
    )


@app.post("/api/addavto/provision-direct-account")
def addavto_provision_direct_account(
    payload: dict[str, Any] | None = None,
    authorization: str | None = Header(default=None),
):
    """Claim one not-yet-provisioned pool login and run the full
    Selenium-driven pipeline (pool login -> Click.ru YANDEX_DIRECT
    integration -> real Direct account) for it, via
    provision_next_direct_account.py. This needs a real browser/display,
    so it only runs here on WSL (addAvto itself has no Selenium) —
    addAvto calls this once per new account it needs to bulk-generate
    campaigns on, in series (not in parallel: one Chrome session at a time
    to avoid resource contention/captcha-triggering concurrency).

    `payload.clickru` (с 2026-11-10): кабинет Click.ru выбранного в визарде
    подключения {connection_id, username, password, cookies} — уходит в
    скрипт через stdin. Без него скрипт работает по-старому (.env ParsYa).
    В ответе может быть `clickru_cookies` — обновлённая сессия кабинета.

    Synchronous and slow (tens of seconds to ~2 minutes per call, same as
    running the script by hand) — callers should budget accordingly.
    """
    addavto_auth(authorization)
    payload = payload or {}
    args: list[str] = []
    name_prefix = payload.get("account_name_prefix")
    if name_prefix:
        args.append(str(name_prefix)[:20])
    stdin_payload: dict[str, Any] = {}
    if isinstance(payload.get("clickru"), dict):
        stdin_payload["clickru"] = payload["clickru"]

    result, error, _ = _run_addavto_selenium_script(
        "provision_next_direct_account.py", args, stdin_payload, 280,
        "Провижининг аккаунта не успел завершиться за 280с",
    )
    if error is not None:
        raise HTTPException(status_code=409, detail=error.get("error", "Ошибка провижининга"))
    return result


# Логины пула — почты (denis2457@argumenty-i-fakty.ru), поэтому «@» и «+»
# допустимы (2026-11-11: «Некорректный логин» на «Открыть Директ»).
_POOL_LOGIN_RE = re.compile(r"^[A-Za-z0-9._%+@-]{1,128}$")
# Логин организации Директа из Click.ru — porg-xxxx (без «@»).
_ORG_LOGIN_RE = re.compile(r"^[A-Za-z0-9._-]{1,64}$")


def _pool_login_cookies(login: str) -> list[dict[str, Any]]:
    import mysql.connector
    conn = mysql.connector.connect(**_yandex_pool_mysql_config())
    try:
        cursor = conn.cursor(dictionary=True)
        cursor.execute("SELECT cookies_json FROM yandex_accounts WHERE login=%s", (login,))
        row = cursor.fetchone()
    finally:
        conn.close()
    if not row:
        raise HTTPException(status_code=404, detail="Аккаунт не найден в пуле ParsYa")
    try:
        cookies = json.loads(row.get("cookies_json") or "[]")
    except ValueError:
        cookies = []
    if not cookies:
        raise HTTPException(status_code=409, detail="У аккаунта в пуле нет сохранённой сессии Яндекса")
    return cookies


_POOL_BROWSER_TARGETS = {
    "metrika": "https://metrika.yandex.ru/list",
    "direct": "https://direct.yandex.ru/dna/grid/campaigns",
}


@app.post("/api/addavto/open-pool-browser")
def addavto_open_pool_browser(payload: dict[str, Any] | None = None, authorization: str | None = Header(default=None)):
    """ЛК «Реклама» → проект → аккаунт → «Метрика в браузере» (2026-11-10):
    сервис Яндекса пулового логина в Chrome на этом ПК с его cookies из
    пула — глазами проверить, что сделал скрипт (счётчик и цель создаются
    в Метрике пулового Yandex ID). Окно остаётся открытым; каждый раз
    отдельный временный профиль (open_pool_browser.py). Какие логины можно
    открывать, решает addAvto (только аккаунты кампаний проекта)."""
    addavto_auth(authorization)
    payload = payload or {}
    login = str(payload.get("login") or "")
    if not _POOL_LOGIN_RE.match(login):
        raise HTTPException(status_code=422, detail="Некорректный логин")
    url = _POOL_BROWSER_TARGETS.get(str(payload.get("target") or "metrika"))
    if url is None:
        raise HTTPException(status_code=422, detail="target: metrika или direct")
    # Директ аккаунта Click.ru (porg-…) — организация, где пуловый ID
    # «Участник»: скрипт выберет её в Яндекс ID и откроет Директ с ulogin.
    org_login = str(payload.get("org_login") or "")
    if org_login:
        if not _ORG_LOGIN_RE.match(org_login):
            raise HTTPException(status_code=422, detail="Некорректный логин организации")
        url = f"https://direct.yandex.ru/dna/grid/campaigns?ulogin={org_login}"
    cookies = _pool_login_cookies(login)
    result, error, _ = _run_addavto_selenium_script(
        "open_pool_browser.py", [], {"login": login, "cookies": cookies, "url": url, "org_login": org_login},
        150, "Chrome не открылся за 150 с",
    )
    if error is not None:
        raise HTTPException(status_code=502, detail=error.get("error", "Не удалось открыть Chrome"))
    return {"ok": True, "logged_in": bool(result.get("logged_in")), "url": result.get("url"),
            "error": result.get("error") or ""}


@app.post("/api/addavto/clickru-web-login")
def addavto_clickru_web_login(
    payload: dict[str, Any] | None = None,
    authorization: str | None = Header(default=None),
):
    """Вход в веб-кабинет click.ru для подключения Click.ru из addAvto
    (2026-11-10, clickru_web_session.py): сохранённые cookies, иначе вход
    по паролю. Тот же Chrome и IP, что и у провижининга.
    Ответ: {ok, relogged, cookies}. 409 — вход не удался (неверный
    логин/пароль, капча): addAvto помечает подключение. 502 — сбой
    браузера/скрипта, про саму учётку ничего не известно.
    """
    addavto_auth(authorization)
    payload = payload or {}
    username = str(payload.get("username") or "").strip()
    password = str(payload.get("password") or "")
    if not username or not password:
        raise HTTPException(status_code=422, detail="Нужны логин и пароль кабинета Click.ru")
    cookies = payload.get("cookies") if isinstance(payload.get("cookies"), list) else []

    result, error, _ = _run_addavto_selenium_script(
        "clickru_web_session.py", [],
        {"username": username, "password": password, "cookies": cookies}, 150,
        "Вход в кабинет Click.ru не успел завершиться за 150с",
    )
    if error is not None:
        status_code = 409 if error.get("login_failed") else 502
        raise HTTPException(status_code=status_code, detail=error.get("error", "Ошибка входа в Click.ru"))
    return result


# ---------------------------------------------------------------------------
# Единый шлюз Gemini для addAvto (2026-11-10, Dima: «запросы к Gemini — так же,
# как на беке в WSL, через одну функцию»). Ключ GEMINI_API_KEY живёт только
# здесь, в .env parsya-control; сеть до Google — через WireGuard wg-gemini
# (см. app/embeddings.py), прод-сервер ни ключа, ни прокси не знает.
# addAvto шлёт сюда готовое тело generateContent и получает ответ Google как
# есть (статус тоже) — ретраи и запасные модели остаются на его стороне.
# ---------------------------------------------------------------------------
GEMINI_GENERATE_URL = "https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent"
_GEMINI_MODEL_RE = re.compile(r"^[A-Za-z0-9._-]{1,80}$")


async def gemini_generate_content(model: str, body: dict[str, Any], timeout: float = 300.0) -> tuple[int, Any]:
    """Один запрос generateContent с ключом и сетью WSL. Возвращает (HTTP-статус,
    JSON-ответ Google или {"error": текст}). Не бросает на 4xx/5xx Google —
    решение (ретрай, другая модель) принимает вызывающий."""
    api_key = os.getenv("GEMINI_API_KEY") or os.getenv("GOOGLE_API_KEY")
    if not api_key:
        return 503, {"error": {"message": "GEMINI_API_KEY не задан в .env parsya-control"}}
    async with httpx.AsyncClient(timeout=timeout) as client:
        resp = await client.post(
            GEMINI_GENERATE_URL.format(model=model), json=body,
            headers={"x-goog-api-key": api_key, "Content-Type": "application/json"},
        )
    try:
        return resp.status_code, resp.json()
    except ValueError:
        return resp.status_code, {"error": {"message": resp.text[:2000]}}


@app.post("/api/addavto/gemini/generate")
async def addavto_gemini_generate(payload: dict[str, Any] | None = None, authorization: str | None = Header(default=None)):
    """Тело: {model, body} — body это тело generateContent как есть. Ответ —
    ответ Google с тем же статусом. 504 — Google не ответил за 300 с."""
    addavto_auth(authorization)
    payload = payload or {}
    model = str(payload.get("model") or "")
    if not _GEMINI_MODEL_RE.match(model):
        raise HTTPException(status_code=422, detail="Некорректное имя модели Gemini")
    body = payload.get("body")
    if not isinstance(body, dict):
        raise HTTPException(status_code=422, detail="body должен быть объектом generateContent")
    try:
        status_code, data = await gemini_generate_content(model, body)
    except httpx.TimeoutException:
        return JSONResponse(status_code=504, content={"error": {"message": "Gemini не ответил за 300 с (WSL)"}})
    except httpx.HTTPError as exc:
        return JSONResponse(status_code=502, content={"error": {"message": f"Сеть WSL → Gemini: {type(exc).__name__}: {exc}"}})
    return JSONResponse(status_code=status_code, content=data)


# ---------------------------------------------------------------------------
# addAvto «Директ Озон»: карточка Ozon через настоящий Chrome здесь, в WSL
# (2026-11-10). С IP прод-сервера Ozon отдаёт 403 любому браузеру; Chrome,
# запущенный процессом с --remote-debugging-port, с графикой WSLg и домашним
# IP открывает карточку без капчи (ozon_fetch.py). Если капча всё же
# появилась — ручное прохождение, как у парсера: скрипт шлёт скриншоты в
# _OZON_CAPTCHA, человек видит их в окне запуска визарда ЛК или по ссылке из
# Telegram (/app/ozon-captcha.html) и отправляет клики/перетаскивания.
# Автоматического решения капчи нет. Состояние — в памяти процесса (uvicorn
# здесь один), отдельно от browser_sessions парсера (те привязаны к jobs).
# ---------------------------------------------------------------------------
_OZON_SID_RE = re.compile(r"^[A-Za-z0-9_-]{8,64}$")
_OZON_CAPTCHA: dict[str, dict[str, Any]] = {}
_OZON_CAPTCHA_LOCK = threading.Lock()
# Один Chrome за раз: постоянный профиль (cookies Ozon) нельзя открыть дважды.
_OZON_FETCH_LOCK = threading.Lock()
OZON_CAPTCHA_PAGE_URL = os.getenv(
    "PARSYA_OZON_CAPTCHA_PAGE_URL",
    "https://xn--80ak2afbe.xn--b1ajdba5acbodeeeaj1qb.xn--p1ai/app/ozon-captcha.html",
)


class OzonCaptchaStateIn(BaseModel):
    active: bool = True
    captcha_required: bool = False
    url: str = Field(default="", max_length=8000)
    title: str = Field(default="", max_length=2000)
    screenshot_b64: str = Field(default="", max_length=4_000_000)
    viewport_width: int = 0
    viewport_height: int = 0


class OzonCaptchaCommandIn(BaseModel):
    action: str = Field(pattern="^(click|drag|key|text|refresh)$")
    x_ratio: float | None = Field(default=None, ge=0, le=1)
    y_ratio: float | None = Field(default=None, ge=0, le=1)
    to_x_ratio: float | None = Field(default=None, ge=0, le=1)
    to_y_ratio: float | None = Field(default=None, ge=0, le=1)
    key: str = Field(default="", max_length=20)
    text: str = Field(default="", max_length=500)


def _ozon_sid(session_id: str) -> str:
    if not _OZON_SID_RE.match(session_id or ""):
        raise HTTPException(status_code=400, detail="Некорректный id сессии")
    return session_id


def _ozon_captcha_gc() -> None:
    cutoff = time.time() - 3600
    for sid in [s for s, v in _OZON_CAPTCHA.items() if v.get("touched", 0) < cutoff]:
        _OZON_CAPTCHA.pop(sid, None)


@app.post("/api/addavto/ozon-fetch")
def addavto_ozon_fetch(payload: dict[str, Any] | None = None, authorization: str | None = Header(default=None)):
    """Данные карточки Ozon (страницы в формате {widgetStates, seo}) через
    Chrome в WSL. Тело: {path, extra_paths, session_id, label}. Долго: до
    ~1 мин без капчи, до ~8 мин, если капчу проходят вручную.
    409 — капчу не прошли вовремя; 503 — Chrome занят другим запуском."""
    addavto_auth(authorization)
    payload = payload or {}
    path = str(payload.get("path") or "")
    if not path.startswith("/product/"):
        raise HTTPException(status_code=422, detail="Нужен путь карточки Ozon вида /product/...")
    sid = _ozon_sid(str(payload.get("session_id") or ""))
    label = str(payload.get("label") or "")[:200]
    if not _OZON_FETCH_LOCK.acquire(timeout=240):
        raise HTTPException(status_code=503, detail="Chrome для Ozon занят другим запуском — повторите через минуту")
    try:
        with _OZON_CAPTCHA_LOCK:
            _ozon_captcha_gc()
            _OZON_CAPTCHA[sid] = {"active": True, "captcha_required": False, "label": label, "touched": time.time(),
                                  "notified": False, "command": None}
        result, error, _ = _run_addavto_selenium_script(
            "ozon_fetch.py", [],
            {"path": path, "extra_paths": [str(p) for p in (payload.get("extra_paths") or [])][:3],
             "session_id": sid, "captcha_timeout": 480,
             # фото товара скачиваются в том же Chrome (CDN Ozon режет сервер)
             "max_images": max(0, min(int(payload.get("max_images") or 0), 12))},
            600, "Chrome не открыл карточку Ozon за 10 минут",
        )
    finally:
        _OZON_FETCH_LOCK.release()
        with _OZON_CAPTCHA_LOCK:
            if sid in _OZON_CAPTCHA:
                _OZON_CAPTCHA[sid].update(active=False, captcha_required=False, command=None, touched=time.time())
    if error is not None:
        status_code = 409 if error.get("captcha_timeout") else 502
        raise HTTPException(status_code=status_code, detail=error.get("error", "Ошибка Chrome/Ozon"))
    return result


@app.post("/api/addavto/ozon-captcha/{session_id}/state")
def addavto_ozon_captcha_state(session_id: str, payload: OzonCaptchaStateIn, authorization: str | None = Header(default=None)):
    """ozon_fetch.py шлёт сюда скриншот ~раз в секунду, пока ждёт капчу."""
    addavto_auth(authorization)
    sid = _ozon_sid(session_id)
    notify = False
    with _OZON_CAPTCHA_LOCK:
        s = _OZON_CAPTCHA.setdefault(sid, {"label": "", "notified": False, "command": None})
        s.update(payload.model_dump(), touched=time.time(), updated_at=now_iso())
        if payload.captcha_required and not s.get("notified"):
            s["notified"] = notify = True
        if not payload.active:
            s["command"] = None
        label = s.get("label") or ""
    if notify:
        text = (f"Ozon показал капчу при генерации объявлений{(' — ' + label) if label else ''}.\n"
                f"Пройти вручную: {OZON_CAPTCHA_PAGE_URL}?s={sid}\n"
                "(или в окне запуска визарда; окно Chrome открыто и на ПК в WSL)")
        threading.Thread(target=notify_telegram_alert, args=(text,), daemon=True).start()
    return {"ok": True}


@app.get("/api/addavto/ozon-captcha/{session_id}/command")
def addavto_ozon_captcha_command(session_id: str, authorization: str | None = Header(default=None)):
    addavto_auth(authorization)
    sid = _ozon_sid(session_id)
    with _OZON_CAPTCHA_LOCK:
        s = _OZON_CAPTCHA.get(sid) or {}
        command, s["command"] = s.get("command"), None
    return {"command": command}


@app.get("/api/v1/ozon-captcha/{session_id}")
def api_ozon_captcha_state(session_id: str, principal: dict = Depends(public_api_principal)):
    """Окно капчи Ozon в ЛК: состояние и последний скриншот."""
    require_user_principal(principal)
    sid = _ozon_sid(session_id)
    with _OZON_CAPTCHA_LOCK:
        s = dict(_OZON_CAPTCHA.get(sid) or {})
    if not s:
        return {"active": False, "captcha_required": False, "known": False}
    return {
        "known": True, "active": bool(s.get("active")), "captcha_required": bool(s.get("captcha_required")),
        "label": s.get("label") or "", "url": s.get("url") or "", "title": s.get("title") or "",
        "screenshot_b64": (s.get("screenshot_b64") or "") if s.get("captcha_required") else "",
        "viewport_width": s.get("viewport_width") or 0, "viewport_height": s.get("viewport_height") or 0,
        "updated_at": s.get("updated_at") or "",
    }


@app.post("/api/v1/ozon-captcha/{session_id}/command")
def api_ozon_captcha_command(session_id: str, payload: OzonCaptchaCommandIn, principal: dict = Depends(public_api_principal)):
    require_user_principal(principal)
    sid = _ozon_sid(session_id)
    if payload.action in ("click", "drag") and (payload.x_ratio is None or payload.y_ratio is None):
        raise HTTPException(status_code=422, detail="Нужны координаты x_ratio/y_ratio")
    if payload.action == "drag" and (payload.to_x_ratio is None or payload.to_y_ratio is None):
        raise HTTPException(status_code=422, detail="Для перетаскивания нужны to_x_ratio/to_y_ratio")
    with _OZON_CAPTCHA_LOCK:
        s = _OZON_CAPTCHA.get(sid)
        if not s or not s.get("active"):
            raise HTTPException(status_code=404, detail="Сессия капчи Ozon не активна")
        s["command"] = payload.model_dump(exclude_none=True)
    return {"ok": True}


@app.post("/api/v1/auth/login")
def api_login(payload: LoginIn):
    email = payload.email.strip().casefold()
    with closing(connect()) as db:
        row = db.execute("SELECT id,password_hash,role,is_active FROM users WHERE email=?", (email,)).fetchone()
        if not row or not row["is_active"] or not verify_password(payload.password, row["password_hash"]):
            # Same error for "no such user" and "wrong password" so login cannot be used to
            # enumerate registered marketer emails.
            raise HTTPException(status_code=401, detail="Invalid email or password")
        token = issue_user_token(db, int(row["id"]))
        db.execute("UPDATE users SET last_login_at=? WHERE id=?", (now_iso(), row["id"]))
        db.commit()
    return {"token": token, "role": row["role"], "email": email}


@app.post("/api/v1/auth/logout")
def api_logout(authorization: str | None = Header(default=None)):
    if authorization and authorization.startswith("Bearer "):
        token_hash = hashlib.sha256(authorization[7:].strip().encode("utf-8")).hexdigest()
        with closing(connect()) as db:
            db.execute("DELETE FROM user_tokens WHERE token_hash=?", (token_hash,))
            db.commit()
    return {"ok": True}


def _paging(limit: int, offset: int) -> tuple[int, int]:
    return max(1, min(limit, 100)), max(0, offset)


def _creative_row_to_dict(row: sqlite3.Row) -> dict[str, Any]:
    return {
        "id": row["id"],
        "creative_hash": row["creative_hash"],
        "content_fingerprint": row["content_fingerprint"],
        "platform": row["platform"],
        "domain": row["domain"],
        "title": row["title"],
        "description": row["description"],
        "image_url": row["image_url"],
        "destination_url": row["destination_url"],
        "detection_method": row["detection_method"],
        "recognition_status": row["recognition_status"],
        "captured_at": row["captured_at"],
        "creative_type": row["creative_type"],
        "video_src": row["video_src"],
        "video_poster": row["video_poster"],
        "video_duration": row["video_duration"],
        "cta_text": row["cta_text"],
        "price_text": row["price_text"],
        "price_rub": row["price_rub"],
        "discount_percent": row["discount_percent"],
        "erid": row["erid"],
        "app_package_id": row["app_package_id"],
        "app_store_id": row["app_store_id"],
        "carousel_id": row["carousel_id"],
        "carousel_slide_index": row["carousel_slide_index"],
    }


@app.get("/api/v1/advertisers")
def api_list_advertisers(q: str = "", intent: str = "", limit: int = 25, offset: int = 0, principal: dict = Depends(public_api_principal)):
    limit, offset = _paging(limit, offset)
    with closing(connect()) as db:
        where_parts, params = [], []
        if q:
            where_parts.append("(a.inn LIKE ? OR a.canonical_name LIKE ?)")
            params.extend([f"%{q}%", f"%{q}%"])
        intents = _parse_intents(intent)
        if intents:
            where_parts.append(
                "EXISTS (SELECT 1 FROM creative_advertiser_links l2 JOIN creatives c ON c.id=l2.creative_id "
                f"WHERE l2.advertiser_id=a.id AND {_intent_exists_sql('c', intents)})"
            )
            params.extend(intents)
        where = ("WHERE " + " AND ".join(where_parts)) if where_parts else ""
        total = db.execute(f"SELECT COUNT(*) FROM advertisers a {where}", params).fetchone()[0]
        rows = db.execute(
            f"SELECT a.id,a.inn,a.canonical_name,a.address,a.phone,a.website_url,a.first_seen_at,a.last_seen_at,"
            f"(SELECT COUNT(*) FROM creative_advertiser_links l WHERE l.advertiser_id=a.id) AS creatives_count "
            f"FROM advertisers a {where} ORDER BY a.last_seen_at DESC LIMIT ? OFFSET ?",
            params + [limit, offset],
        ).fetchall()
        items = [
            {"id": r["id"], "inn": r["inn"], "name": r["canonical_name"], "creatives_count": r["creatives_count"],
             "address": r["address"], "phone": r["phone"], "website_url": r["website_url"],
             "first_seen_at": r["first_seen_at"], "last_seen_at": r["last_seen_at"]}
            for r in rows
        ]
    return {"items": items, "total": total, "limit": limit, "offset": offset}


@app.get("/api/v1/advertisers/{inn}/creatives")
def api_advertiser_creatives(inn: str, limit: int = 50, offset: int = 0, principal: dict = Depends(public_api_principal)):
    limit, offset = _paging(limit, offset)
    with closing(connect()) as db:
        advertiser = db.execute("SELECT id,inn,canonical_name FROM advertisers WHERE inn=?", (inn,)).fetchone()
        if not advertiser:
            raise HTTPException(status_code=404, detail="Рекламодатель с таким ИНН не найден")
        creative_rows = db.execute(
            "SELECT c.* FROM creatives c "
            "JOIN creative_advertiser_links l ON l.creative_id=c.id "
            "WHERE l.advertiser_id=? ORDER BY c.captured_at DESC",
            (advertiser["id"],),
        ).fetchall()
        # Group creative rows that are the same underlying ad (same content_fingerprint)
        # into one "variant" — this is how a marketer sees "one creative, run under several
        # campaigns/platforms" instead of N unrelated-looking rows.
        groups: dict[str, dict[str, Any]] = {}
        order: list[str] = []
        for row in creative_rows:
            key = row["content_fingerprint"] or f"hash:{row['creative_hash']}"
            if key not in groups:
                groups[key] = {
                    "group_key": key,
                    "representative": _creative_row_to_dict(row),
                    "creative_ids": [],
                    "platforms": set(),
                    "campaign_ids": set(),
                    "banner_ids": set(),
                    "client_ids": set(),
                    "first_seen_at": row["captured_at"],
                    "last_seen_at": row["captured_at"],
                }
                order.append(key)
            group = groups[key]
            group["creative_ids"].append(row["id"])
            group["platforms"].add(row["platform"])
            group["first_seen_at"] = min(group["first_seen_at"], row["captured_at"])
            group["last_seen_at"] = max(group["last_seen_at"], row["captured_at"])
            ident_rows = db.execute(
                "SELECT identifier_type,identifier_value FROM creative_identifiers WHERE creative_id=?", (row["id"],)
            ).fetchall()
            for ident in ident_rows:
                bucket = {"campaign_id_candidate": "campaign_ids", "banner_id_candidate": "banner_ids",
                          "client_id_candidate": "client_ids"}.get(ident["identifier_type"])
                if bucket:
                    group[bucket].add(ident["identifier_value"])
        variants = []
        for key in order[offset:offset + limit]:
            group = groups[key]
            variants.append({
                **{k: v for k, v in group.items() if k not in {"platforms", "campaign_ids", "banner_ids", "client_ids"}},
                "platforms": sorted(group["platforms"]),
                "campaign_ids": sorted(group["campaign_ids"]),
                "banner_ids": sorted(group["banner_ids"]),
                "client_ids": sorted(group["client_ids"]),
            })
    return {
        "advertiser": {"id": advertiser["id"], "inn": advertiser["inn"], "name": advertiser["canonical_name"]},
        "variants": variants,
        "total_variants": len(order),
        "total_creative_rows": len(creative_rows),
        "limit": limit, "offset": offset,
    }


def _attach_intents(db: sqlite3.Connection, items: list[dict[str, Any]]) -> None:
    """Attaches `intents`: the distinct search queries (jobs.key_text) each creative was
    captured under, so the UI can show "this ad belongs to «X»" on every card/detail
    view regardless of whether intent-scoping is currently filtering the list or not."""
    ids = [item["id"] for item in items if item.get("id") is not None]
    if not ids:
        return
    by_id: dict[int, list[str]] = {}
    for chunk in (ids[i:i + 400] for i in range(0, len(ids), 400)):
        placeholders = ",".join(["?"] * len(chunk))
        rows = db.execute(
            f"SELECT DISTINCT o.creative_id, j.key_text FROM occurrences o JOIN jobs j ON j.id=o.job_id "
            f"WHERE o.creative_id IN ({placeholders}) AND j.key_text IS NOT NULL AND j.key_text<>''",
            chunk,
        ).fetchall()
        for row in rows:
            by_id.setdefault(row["creative_id"], []).append(row["key_text"])
    for item in items:
        item["intents"] = by_id.get(item.get("id"), [])
    # Запуски, в которых креатив найден: ключевое слово запуска + ссылка (job_id) на запуск.
    runs_by_id: dict[int, list[dict[str, Any]]] = {}
    for chunk in (ids[i:i + 400] for i in range(0, len(ids), 400)):
        placeholders = ",".join(["?"] * len(chunk))
        for row in db.execute(
            f"SELECT o.creative_id, j.id AS job_id, j.key_text, j.city, j.status, MAX(o.observed_at) AS seen_at "
            f"FROM occurrences o JOIN jobs j ON j.id=o.job_id WHERE o.creative_id IN ({placeholders}) "
            f"GROUP BY o.creative_id, j.id ORDER BY j.id DESC",
            chunk,
        ).fetchall():
            runs_by_id.setdefault(row["creative_id"], []).append({
                "job_id": row["job_id"], "key_text": row["key_text"], "city": row["city"],
                "status": row["status"], "seen_at": row["seen_at"],
            })
    ai_by_id: dict[int, Any] = {}
    for chunk in (ids[i:i + 400] for i in range(0, len(ids), 400)):
        placeholders = ",".join(["?"] * len(chunk))
        for row in db.execute(f"SELECT id, ai_intent FROM creatives WHERE id IN ({placeholders})", chunk).fetchall():
            ai_by_id[row["id"]] = row["ai_intent"]
    for item in items:
        item["runs"] = runs_by_id.get(item.get("id"), [])[:20]
        item["ai_intent"] = ai_by_id.get(item.get("id"))  # None = ещё не определён


@app.get("/api/v1/creatives")
def api_list_creatives(
    inn: str = "", platform: str = "", q: str = "", status: str = "", job_id: int | None = None,
    has_advertiser: bool | None = None, has_app: bool | None = None, has_discount: bool | None = None,
    campaign_id: str = "", utm_campaign: str = "", keyword: str = "", intent: str = "", ai_intent: str = "", only_relevant: str = "",
    date_from: str = "", date_to: str = "",
    sort: str = "recent", limit: int = 50, offset: int = 0,
    principal: dict = Depends(public_api_principal),
):
    limit, offset = _paging(limit, offset)
    with closing(connect()) as db:
        joins = []
        where_parts = []
        params: list[Any] = []
        intents = _parse_intents(intent)
        if intents:
            combined = _semantic_intent_where(db, intents)
            if combined:
                where_sql, extra_params = combined
                where_parts.append(where_sql)
                params.extend(extra_params)
            else:
                where_parts.append(_intent_exists_sql("c", intents))
                params.extend(intents)
        ai_list = [x.strip().lower() for x in (ai_intent or "").split(",") if x.strip()]
        if ai_list:
            if "__pending__" in ai_list:
                ai_list = [x for x in ai_list if x != "__pending__"]
                ai_sql = "c.ai_intent IS NULL"
                if ai_list:
                    ai_sql = f"(c.ai_intent IS NULL OR PYLOWER(c.ai_intent) IN ({','.join(['?'] * len(ai_list))}))"
                    params.extend(ai_list)
                where_parts.append(ai_sql)
            else:
                where_parts.append(f"PYLOWER(c.ai_intent) IN ({','.join(['?'] * len(ai_list))})")
                params.extend(ai_list)
        if inn:
            joins.append("JOIN creative_advertiser_links l ON l.creative_id=c.id JOIN advertisers a ON a.id=l.advertiser_id")
            where_parts.append("a.inn=?")
            params.append(inn)
        elif has_advertiser is not None:
            exists_sql = "EXISTS (SELECT 1 FROM creative_advertiser_links cal WHERE cal.creative_id=c.id)"
            where_parts.append(exists_sql if has_advertiser else f"NOT {exists_sql}")
        if has_app:
            where_parts.append("(COALESCE(c.app_package_id,'')<>'' OR COALESCE(c.app_store_id,'')<>'')")
        if has_discount:
            where_parts.append("c.discount_percent IS NOT NULL")
        if campaign_id:
            joins.append(
                "JOIN creative_identifiers ci_campaign ON ci_campaign.creative_id=c.id "
                "AND ci_campaign.identifier_type='campaign_id_candidate'"
            )
            where_parts.append("ci_campaign.identifier_value=?")
            params.append(campaign_id)
        if utm_campaign:
            joins.append("JOIN creative_utm cu_campaign ON cu_campaign.creative_id=c.id")
            where_parts.append("cu_campaign.utm_campaign=?")
            params.append(utm_campaign)
        if keyword:
            joins.append("JOIN creative_utm cu_keyword ON cu_keyword.creative_id=c.id")
            where_parts.append("cu_keyword.utm_term=?")
            params.append(keyword)
        job_rel: dict[int, bool | None] | None = None
        if job_id is not None:
            joins.append("JOIN occurrences jo ON jo.creative_id=c.id")
            where_parts.append("jo.job_id=?")
            params.append(job_id)
            jrow = db.execute("SELECT key_text FROM jobs WHERE id=?", (job_id,)).fetchone()
            if jrow:
                job_rel = _job_relevance(db, job_id, jrow["key_text"])["ids"]
                if only_relevant in ("1", "yes", "true"):
                    keep = [i for i, v in job_rel.items() if v is True] or [-1]
                    where_parts.append(f"c.id IN ({','.join(['?'] * len(keep))})")
                    params.extend(keep)
                elif only_relevant == "no":
                    keep = [i for i, v in job_rel.items() if v is False] or [-1]
                    where_parts.append(f"c.id IN ({','.join(['?'] * len(keep))})")
                    params.extend(keep)
        if platform:
            where_parts.append("c.platform=?")
            params.append(platform)
        if status:
            where_parts.append("c.recognition_status=?")
            params.append(status)
        if q:
            where_parts.append("(c.title LIKE ? OR c.domain LIKE ? OR c.description LIKE ?)")
            params.extend([f"%{q}%", f"%{q}%", f"%{q}%"])
        if date_from:
            where_parts.append("c.captured_at>=?")
            params.append(date_from)
        if date_to:
            where_parts.append("c.captured_at<=?")
            params.append(date_to)
        join_sql = " ".join(joins)
        where = ("WHERE " + " AND ".join(where_parts)) if where_parts else ""
        order_sql = {
            "recent": "c.captured_at DESC",
            "oldest": "c.captured_at ASC",
            "domain": "c.domain ASC, c.captured_at DESC",
            "discount": "c.discount_percent DESC, c.captured_at DESC",
        }.get(sort, "c.captured_at DESC")
        total = db.execute(f"SELECT COUNT(DISTINCT c.id) FROM creatives c {join_sql} {where}", params).fetchone()[0]
        rows = db.execute(
            f"SELECT DISTINCT c.* FROM creatives c {join_sql} {where} ORDER BY {order_sql} LIMIT ? OFFSET ?",
            params + [limit, offset],
        ).fetchall()
        items = [_creative_row_to_dict(r) for r in rows]
        _attach_intents(db, items)
        if job_rel is not None:
            for it in items:
                it["job_match"] = job_rel.get(it["id"])  # True — по теме, False — не по теме, None — ещё определяется
    return {"items": items, "total": total, "limit": limit, "offset": offset}


@app.get("/api/v1/platforms")
def api_list_platforms(principal: dict = Depends(public_api_principal)):
    with closing(connect()) as db:
        rows = db.execute("SELECT platform, COUNT(*) n FROM creatives GROUP BY platform ORDER BY n DESC").fetchall()
    return {"items": [{"platform": r["platform"], "creatives_count": r["n"]} for r in rows]}


@app.get("/api/v1/ai-intents")
def api_list_ai_intents(job_id: int | None = None, principal: dict = Depends(public_api_principal)):
    """Интенты креативов, определённые Gemini (о чём реклама на самом деле) — для фильтра
    библиотеки. С job_id — только интенты креативов этого запуска, без — все. НЕ то же
    самое, что /api/v1/intents (ключевые слова запусков)."""
    require_user_principal(principal)
    scope = " AND c.id IN (SELECT creative_id FROM occurrences WHERE job_id=?)" if job_id is not None else ""
    args: list[Any] = [job_id] if job_id is not None else []
    with closing(connect()) as db:
        rows = db.execute(
            "SELECT MIN(c.ai_intent) AS intent, COUNT(*) AS n FROM creatives c WHERE c.ai_intent IS NOT NULL" + scope +
            " GROUP BY PYLOWER(c.ai_intent) ORDER BY (PYLOWER(MIN(c.ai_intent))=?) ASC, n DESC LIMIT 500",
            args + [aii.UNKNOWN],
        ).fetchall()
        pending = db.execute("SELECT COUNT(*) FROM creatives c WHERE c.ai_intent IS NULL" + scope, args).fetchone()[0]
        total = db.execute("SELECT COUNT(*) FROM creatives c WHERE 1=1" + scope, args).fetchone()[0]
    return {"items": [{"intent": r["intent"], "creatives_count": r["n"]} for r in rows],
            "pending": pending, "total": total, "model": aii.last_model_used, "job_id": job_id}


@app.get("/api/v1/intents")
def api_list_intents(principal: dict = Depends(public_api_principal)):
    """Every distinct search query (jobs.key_text) run so far, with how many creatives/runs
    it produced — populates the global "intent" switcher that scopes the whole ЛК (library
    + every analytics report) to one search's own results by default (2026-10-22, see the
    `intent` param accepted by every creative-derived listing below). Grouped case/space-
    insensitively (PYLOWER) so "Кроссовки" and "кроссовки " count as the same intent."""
    require_user_principal(principal)
    with closing(connect()) as db:
        rows = db.execute(
            """
            SELECT PYLOWER(j.key_text) AS intent_key,
                   MIN(j.key_text) AS display_text,
                   COUNT(DISTINCT j.id) AS runs_count,
                   COUNT(DISTINCT o.creative_id) AS creatives_count,
                   MAX(j.created_at) AS last_run_at
            FROM jobs j
            LEFT JOIN occurrences o ON o.job_id = j.id
            WHERE j.key_text IS NOT NULL AND TRIM(j.key_text) <> ''
            GROUP BY intent_key
            ORDER BY last_run_at DESC
            """
        ).fetchall()
    all_texts = [r["display_text"] for r in rows]
    items = []
    for r in rows:
        text = r["display_text"]
        items.append({
            "intent": text, "runs_count": r["runs_count"],
            "creatives_count": r["creatives_count"], "last_run_at": r["last_run_at"],
            "related": _related_intents([text], all_texts),
        })
    return {"items": items}


@app.get("/api/v1/creatives/{creative_id}")
def api_creative_detail(creative_id: int, principal: dict = Depends(public_api_principal)):
    with closing(connect()) as db:
        row = db.execute("SELECT * FROM creatives WHERE id=?", (creative_id,)).fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Креатив не найден")
        advertisers = db.execute(
            "SELECT a.inn,a.canonical_name,a.address,a.phone,a.website_url,l.association_method,l.confidence "
            "FROM creative_advertiser_links l "
            "JOIN advertisers a ON a.id=l.advertiser_id WHERE l.creative_id=?",
            (creative_id,),
        ).fetchall()
        identifiers = db.execute(
            "SELECT identifier_type,identifier_value FROM creative_identifiers WHERE creative_id=?",
            (creative_id,),
        ).fetchall()
        occurrences = db.execute(
            "SELECT job_id,platform,source_page_url,observed_at FROM occurrences WHERE creative_id=? ORDER BY observed_at DESC LIMIT 100",
            (creative_id,),
        ).fetchall()
        secondary_links = db.execute(
            "SELECT link_text,tracking_url,final_url,final_url_domain,utm_source,utm_medium,"
            "utm_campaign,utm_content,utm_term,resolve_status FROM creative_secondary_links WHERE creative_id=?",
            (creative_id,),
        ).fetchall()
        utm_row = db.execute(
            "SELECT final_url,final_url_domain,utm_source,utm_medium,utm_campaign,utm_content,"
            "utm_term,resolve_status,resolved_at,advertised_site_domain,landing_domain_mismatch "
            "FROM creative_utm WHERE creative_id=?",
            (creative_id,),
        ).fetchone()
        destination_history = db.execute(
            "SELECT final_url,final_url_domain,utm_source,utm_medium,utm_campaign,utm_content,"
            "utm_term,first_seen_at,last_seen_at FROM creative_destination_history "
            "WHERE creative_id=? ORDER BY first_seen_at DESC LIMIT 50",
            (creative_id,),
        ).fetchall()
        creative_dict = _creative_row_to_dict(row)
        _attach_intents(db, [creative_dict])
    return {
        "creative": creative_dict,
        "advertisers": [dict(r) for r in advertisers],
        "identifiers": [dict(r) for r in identifiers],
        "occurrences": [dict(r) for r in occurrences],
        "secondary_links": [dict(r) for r in secondary_links],
        "utm": dict(utm_row) if utm_row else None,
        "destination_history": [dict(r) for r in destination_history],
    }


def require_user_principal(principal: dict) -> int:
    if principal.get("kind") != "user":
        raise HTTPException(status_code=403, detail="Для этого действия нужен вход пользователя, а не серверный API-ключ")
    return int(principal["user_id"])


@app.post("/api/v1/auth/change-password")
def api_change_password(payload: ChangePasswordIn, principal: dict = Depends(public_api_principal)):
    user_id = require_user_principal(principal)
    with closing(connect()) as db:
        row = db.execute("SELECT password_hash FROM users WHERE id=?", (user_id,)).fetchone()
        if not row or not verify_password(payload.current_password, row["password_hash"]):
            raise HTTPException(status_code=401, detail="Текущий пароль указан неверно")
        db.execute("UPDATE users SET password_hash=? WHERE id=?", (hash_password(payload.new_password), user_id))
        db.commit()
    return {"ok": True}


@app.get("/api/v1/profile")
def api_get_profile(principal: dict = Depends(public_api_principal)):
    user_id = require_user_principal(principal)
    with closing(connect()) as db:
        row = db.execute(
            "SELECT email,role,name,avatar_data_url,notification_prefs FROM users WHERE id=?", (user_id,)
        ).fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Пользователь не найден")
    prefs = {}
    if row["notification_prefs"]:
        try:
            prefs = json.loads(row["notification_prefs"])
        except (TypeError, ValueError):
            prefs = {}
    return {
        "email": row["email"], "role": row["role"], "name": row["name"] or "",
        "avatar_data_url": row["avatar_data_url"] or "", "notification_prefs": prefs,
    }


@app.patch("/api/v1/profile")
def api_update_profile(payload: ProfileUpdateIn, principal: dict = Depends(public_api_principal)):
    user_id = require_user_principal(principal)
    with closing(connect()) as db:
        db.execute("UPDATE users SET name=? WHERE id=?", (payload.name.strip()[:200] or None, user_id))
        db.commit()
    return {"ok": True}


@app.post("/api/v1/profile/avatar")
def api_upload_avatar(payload: AvatarUploadIn, principal: dict = Depends(public_api_principal)):
    user_id = require_user_principal(principal)
    data_url = payload.data_url.strip()
    if data_url and not data_url.startswith("data:image/"):
        raise HTTPException(status_code=422, detail="Ожидается изображение (data: URL)")
    with closing(connect()) as db:
        db.execute("UPDATE users SET avatar_data_url=? WHERE id=?", (data_url or None, user_id))
        db.commit()
    return {"ok": True}


@app.put("/api/v1/profile/notification-prefs")
def api_set_notification_prefs(payload: NotificationPrefsIn, principal: dict = Depends(public_api_principal)):
    user_id = require_user_principal(principal)
    with closing(connect()) as db:
        db.execute("UPDATE users SET notification_prefs=? WHERE id=?", (json.dumps(payload.prefs), user_id))
        db.commit()
    return {"ok": True}


@app.get("/api/v1/notifications")
def api_list_notifications(limit: int = 30, principal: dict = Depends(public_api_principal)):
    user_id = require_user_principal(principal)
    limit = max(1, min(limit, 100))
    with closing(connect()) as db:
        rows = db.execute(
            "SELECT id,type,title,text,link,created_at,read_at FROM notifications WHERE user_id=? "
            "ORDER BY created_at DESC LIMIT ?",
            (user_id, limit),
        ).fetchall()
        unread = db.execute(
            "SELECT COUNT(*) FROM notifications WHERE user_id=? AND read_at IS NULL", (user_id,)
        ).fetchone()[0]
    return {"items": [dict(r) for r in rows], "unread_count": unread}


@app.post("/api/v1/notifications/read-all")
def api_mark_all_notifications_read(principal: dict = Depends(public_api_principal)):
    user_id = require_user_principal(principal)
    with closing(connect()) as db:
        db.execute("UPDATE notifications SET read_at=? WHERE user_id=? AND read_at IS NULL", (now_iso(), user_id))
        db.commit()
    return {"ok": True}


@app.post("/api/v1/runs")
def api_create_run(payload: RunCreateIn, principal: dict = Depends(public_api_principal)):
    """Lets a logged-in SaaS user queue a scan with their own parameters, self-service —
    the WSL worker picks it up exactly like an ops-console run; only ownership differs."""
    user_id = require_user_principal(principal)
    city, key_text = payload.city.strip(), payload.key_text.strip()
    with closing(connect()) as db:
        active = db.execute(
            "SELECT COUNT(*) FROM jobs WHERE owner_user_id=? AND schedule_id IS NULL AND status IN ('queued','running')", (user_id,)
        ).fetchone()[0]
        if active >= MAX_MANUAL_RUNS:
            raise HTTPException(status_code=429, detail=f"У вас уже {MAX_MANUAL_RUNS} запусков в очереди/выполняются — подождите их завершения")
        cur = db.execute(
            "INSERT INTO jobs(status,city,key_text,created_at,owner_user_id) VALUES('queued',?,?,?,?)",
            (city, key_text, now_iso(), user_id),
        )
        job_id = cur.lastrowid
        db.commit()
    return {"id": job_id, "status": "queued", "city": city, "key_text": key_text}


class ScheduleCreateIn(BaseModel):
    city: str = Field(min_length=1, max_length=100)
    key_text: str = Field(min_length=1, max_length=300)
    runs_per_day: int = Field(ge=1, le=24)
    duration_days: int


def _schedule_dict(r) -> dict[str, Any]:
    d = dict(r)
    d["runs_total"] = d["runs_per_day"] * d["duration_days"]
    return d


@app.post("/api/v1/schedules")
def api_create_schedule(payload: ScheduleCreateIn, principal: dict = Depends(public_api_principal)):
    user_id = require_user_principal(principal)
    if payload.duration_days not in SCHEDULE_DAYS:
        raise HTTPException(status_code=422, detail="Длительность: 7, 14 или 30 дней")
    city, key_text = payload.city.strip(), payload.key_text.strip()
    if not city or not key_text:
        raise HTTPException(status_code=422, detail="Укажите город и запрос")
    now = datetime.now(timezone.utc).replace(microsecond=0)
    with closing(connect()) as db:
        active = db.execute(
            "SELECT COUNT(*) FROM scan_schedules WHERE owner_user_id=? AND status IN ('active','paused')", (user_id,)
        ).fetchone()[0]
        if active >= MAX_ACTIVE_SCHEDULES:
            raise HTTPException(status_code=429, detail=f"Уже {MAX_ACTIVE_SCHEDULES} активных расписаний — остановите ненужные")
        cur = db.execute(
            "INSERT INTO scan_schedules(owner_user_id,city,key_text,runs_per_day,duration_days,created_at,end_at,next_run_at) "
            "VALUES(?,?,?,?,?,?,?,?)",
            (user_id, city, key_text, payload.runs_per_day, payload.duration_days, now.isoformat(),
             (now + timedelta(days=payload.duration_days)).isoformat(), now.isoformat()),
        )
        db.commit()
        sid = cur.lastrowid
    _sched_tick()  # первый запуск ставим в очередь сразу, а не через 30 секунд
    with closing(connect()) as db:
        row = db.execute("SELECT * FROM scan_schedules WHERE id=?", (sid,)).fetchone()
    return _schedule_dict(row)


@app.get("/api/v1/schedules")
def api_list_schedules(principal: dict = Depends(public_api_principal)):
    user_id = require_user_principal(principal)
    with closing(connect()) as db:
        rows = db.execute(
            "SELECT * FROM scan_schedules WHERE owner_user_id=? ORDER BY "
            "CASE status WHEN 'active' THEN 0 WHEN 'paused' THEN 1 ELSE 2 END, id DESC LIMIT 100", (user_id,)
        ).fetchall()
    return {"items": [_schedule_dict(r) for r in rows]}


class ScheduleActionIn(BaseModel):
    action: str


@app.post("/api/v1/schedules/{schedule_id}/action")
def api_schedule_action(schedule_id: int, payload: ScheduleActionIn, principal: dict = Depends(public_api_principal)):
    """pause | resume | cancel. Уже поставленные в очередь jobs не трогаем."""
    user_id = require_user_principal(principal)
    with closing(connect()) as db:
        sc = db.execute("SELECT * FROM scan_schedules WHERE id=? AND owner_user_id=?", (schedule_id, user_id)).fetchone()
        if not sc:
            raise HTTPException(status_code=404, detail="Расписание не найдено")
        if sc["status"] in ("finished", "cancelled"):
            raise HTTPException(status_code=409, detail="Расписание уже завершено")
        now = datetime.now(timezone.utc).replace(microsecond=0)
        if payload.action == "pause" and sc["status"] == "active":
            db.execute("UPDATE scan_schedules SET status='paused' WHERE id=?", (schedule_id,))
        elif payload.action == "resume" and sc["status"] == "paused":
            interval = timedelta(hours=24 / sc["runs_per_day"])
            db.execute("UPDATE scan_schedules SET status='active',next_run_at=? WHERE id=?",
                       ((now + interval).isoformat(), schedule_id))
        elif payload.action == "cancel":
            db.execute("UPDATE scan_schedules SET status='cancelled',next_run_at=NULL,finished_at=? WHERE id=?",
                       (now.isoformat(), schedule_id))
        else:
            raise HTTPException(status_code=422, detail="Недопустимое действие")
        db.commit()
        row = db.execute("SELECT * FROM scan_schedules WHERE id=?", (schedule_id,)).fetchone()
    return _schedule_dict(row)


_job_rel_cache: dict[tuple, dict[str, Any]] = {}

# --- Оценка времени запуска (2026-11-13) --------------------------------------------
# Раньше ETA брался как медиана ВСЕХ успешных запусков, среди которых много оборванных
# (мёртвая сессия, ошибка Дзена, режим одной площадки) — выходило ~33 мин при реальных
# ~2 часах. Теперь берём среднее по последним ПОЛНЫМ запускам: обработаны все площадки
# (N из N, N ≥ 90), нет «invalid session id» и нет критической ошибки Дзена.
_eta_cache: dict[str, Any] = {"at": 0.0, "data": None}
_FULL_RUN_SITES_RE = re.compile(r"Обработано площадок: (\d+) из (\d+)")
ETA_LAST_FULL_RUNS = 5


def _full_run_stats(db: sqlite3.Connection) -> dict[str, Any]:
    now = time.monotonic()
    if _eta_cache["data"] is not None and now - _eta_cache["at"] < 600:
        return _eta_cache["data"]
    durations: list[tuple[int, float]] = []
    for row in db.execute(
        "SELECT id, started_at, finished_at, log_text FROM jobs WHERE status='succeeded' "
        "AND started_at IS NOT NULL AND finished_at IS NOT NULL AND log_text LIKE '%Обработано площадок%' "
        "ORDER BY id DESC LIMIT 60"
    ).fetchall():
        log = row["log_text"] or ""
        m = None
        for m in _FULL_RUN_SITES_RE.finditer(log):
            pass
        if not m or int(m.group(1)) != int(m.group(2)) or int(m.group(2)) < 90:
            continue
        if "invalid session id" in log or "критическая ошибка на этапе парсинга" in log:
            continue
        secs = (datetime.fromisoformat(row["finished_at"]) - datetime.fromisoformat(row["started_at"])).total_seconds()
        if secs > 0:
            durations.append((row["id"], secs))
        if len(durations) >= ETA_LAST_FULL_RUNS:
            break
    data = {
        "avg_full_seconds": round(sum(d for _, d in durations) / len(durations)) if durations else None,
        "samples": len(durations),
        "job_ids": [i for i, _ in durations],
    }
    _eta_cache.update(at=now, data=data)
    return data


_STAGE_SITE_RE = re.compile(r"ПЛОЩАДКА (\d+)/(\d+):")
_STAGE_DZEN_RE = re.compile(r"ДЗЕН: основной проход (\d+)/(\d+)")


def _job_stage(db: sqlite3.Connection, job_id: int) -> dict[str, Any] | None:
    row = db.execute("SELECT substr(log_text, -40000) AS tail FROM jobs WHERE id=?", (job_id,)).fetchone()
    tail = (row["tail"] if row else "") or ""
    site = None
    for site in _STAGE_SITE_RE.finditer(tail):
        pass
    if site:
        return {"kind": "sites", "index": int(site.group(1)), "total": int(site.group(2))}
    dz = None
    for dz in _STAGE_DZEN_RE.finditer(tail):
        pass
    if dz:
        return {"kind": "dzen_main", "index": int(dz.group(1)), "total": int(dz.group(2))}
    sc = None
    for sc in re.finditer(r"Дзен: основной скролл (\d+)/(\d+)", tail):
        pass
    if sc:
        return {"kind": "dzen_main", "index": None, "total": None, "scroll": int(sc.group(1)), "scroll_total": int(sc.group(2))}
    if "ДЗЕН: ПРЕДВАРИТЕЛЬНЫЙ ПОИСК" in tail:
        return {"kind": "dzen_search"}
    return None


def _job_relevance(db: sqlite3.Connection, job_id: int, key_text: str) -> dict[str, Any]:
    """Сколько креативов запуска ПОДХОДЯТ под его ключевое слово (2026-11-13).
    Основа — ИНТЕНТ креатива (ai_intent от Gemini): (1) его основа слова совпадает с основой
    ключевого слова («пенсии и льготы» ↔ «увеличение пенсии») либо (2) Gemini признал пару
    «ключевое слово → интент» подходящей (кэш intent_keyword_match). Реклама, просто нацеленная
    на ту же аудиторию («имплантация зубов для пенсионеров»), НЕ подходит. Раньше учитывалась
    ещё близость embedding ≥ 0.62 и совпадение слов в тексте — давало ложные «по теме»
    (недвижимость, стоматология). Если пара ещё не оценена или интент не определён —
    «ещё определяется» (None). Возвращает {found, relevant, pending, ids:{id: True|False|None}}."""
    kw_key = _pylower(key_text)
    stamp_row = db.execute(
        "SELECT COUNT(DISTINCT c.id), SUM(c.ai_intent IS NOT NULL) FROM occurrences o JOIN creatives c ON c.id=o.creative_id "
        "WHERE o.job_id=?", (job_id,),
    ).fetchone()
    judged = db.execute("SELECT COUNT(*) FROM intent_keyword_match WHERE keyword_key=?", (kw_key,)).fetchone()[0]
    stamp = (job_id, key_text, tuple(stamp_row), judged)
    cached = _job_rel_cache.get(stamp[:2])
    if cached and cached["stamp"] == stamp:
        return cached["data"]
    kw_stems = _intent_stems(key_text)
    match_map = {r["intent_key"]: bool(r["match"]) for r in db.execute(
        "SELECT intent_key, match FROM intent_keyword_match WHERE keyword_key=?", (kw_key,)).fetchall()}
    rows = db.execute(
        "SELECT c.id, c.title, c.description, c.ai_intent FROM occurrences o JOIN creatives c ON c.id=o.creative_id "
        "WHERE o.job_id=? GROUP BY c.id", (job_id,),
    ).fetchall()
    ids: dict[int, bool | None] = {}
    for r in rows:
        ai = r["ai_intent"]
        if ai is None:
            ids[r["id"]] = None  # интент ещё не определён
        elif ai == aii.UNKNOWN:
            # тему по тексту не поняли — засчитываем только прямое совпадение слов запроса
            ids[r["id"]] = True if kw_stems & _intent_stems((r["title"] or "") + " " + (r["description"] or "")) else False
        elif kw_stems & _intent_stems(ai):
            ids[r["id"]] = True
        else:
            ids[r["id"]] = match_map.get(ai.strip().lower())  # True/False от Gemini или None — ещё не оценено
    data = {
        "found": len(ids),
        "relevant": sum(1 for v in ids.values() if v is True),
        "pending": sum(1 for v in ids.values() if v is None),
        "ids": ids,
    }
    _job_rel_cache[stamp[:2]] = {"stamp": stamp, "data": data}
    return data


def _relevance_public(rel: dict[str, Any]) -> dict[str, Any]:
    return {k: rel[k] for k in ("found", "relevant", "pending")}


@app.get("/api/v1/runs")
def api_list_runs(limit: int = 25, offset: int = 0, schedule_id: int | None = None, principal: dict = Depends(public_api_principal)):
    user_id = require_user_principal(principal)
    limit, offset = _paging(limit, offset)
    with closing(connect()) as db:
        flt, args = "owner_user_id=?", [user_id]
        if schedule_id is not None:
            flt += " AND schedule_id=?"
            args.append(schedule_id)
        total = db.execute(f"SELECT COUNT(*) FROM jobs WHERE {flt}", args).fetchone()[0]
        rows = db.execute(
            "SELECT id,status,city,key_text,created_at,started_at,finished_at,total_creatives,error_text,schedule_id,attempts,retry_at "
            f"FROM jobs WHERE {flt} ORDER BY id DESC LIMIT ? OFFSET ?",
            (*args, limit, offset),
        ).fetchall()
        items = [dict(r) for r in rows]
        for it in items:
            if (it.get("total_creatives") or 0) > 0 or it["status"] in ("running", "succeeded"):
                it["relevance"] = _relevance_public(_job_relevance(db, it["id"], it["key_text"]))
            if it["status"] == "running":
                it["stage"] = _job_stage(db, it["id"])
        eta = _full_run_stats(db)
    return {"items": items, "total": total, "limit": limit, "offset": offset, "eta": eta}


@app.get("/api/v1/runs/{job_id}")
def api_run_detail(job_id: int, principal: dict = Depends(public_api_principal)):
    user_id = require_user_principal(principal)
    with closing(connect()) as db:
        job = db.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
        if not job or job["owner_user_id"] != user_id:
            raise HTTPException(status_code=404, detail="Запуск не найден")
        sites = db.execute(
            "SELECT platform,status,unique_creatives,partial_creatives,ad_signal_detected,error_text "
            "FROM site_results WHERE job_id=? ORDER BY platform", (job_id,),
        ).fetchall()
        relevance = _relevance_public(_job_relevance(db, job["id"], job["key_text"]))
    return {
        "id": job["id"], "status": job["status"], "city": job["city"], "key_text": job["key_text"],
        "created_at": job["created_at"], "started_at": job["started_at"], "finished_at": job["finished_at"],
        "total_creatives": job["total_creatives"], "error_text": job["error_text"],
        "relevance": relevance,
        "sites": [dict(r) for r in sites],
    }


@app.get("/api/v1/collections")
def api_list_collections(principal: dict = Depends(public_api_principal)):
    user_id = require_user_principal(principal)
    with closing(connect()) as db:
        rows = db.execute(
            "SELECT c.id,c.name,c.description,c.created_at,"
            "(SELECT COUNT(*) FROM collection_items i WHERE i.collection_id=c.id) AS items_count "
            "FROM collections c WHERE c.owner_user_id=? ORDER BY c.created_at DESC",
            (user_id,),
        ).fetchall()
    return {"items": [dict(r) for r in rows]}


@app.post("/api/v1/collections")
def api_create_collection(payload: CollectionCreateIn, principal: dict = Depends(public_api_principal)):
    user_id = require_user_principal(principal)
    with closing(connect()) as db:
        cur = db.execute(
            "INSERT INTO collections(owner_user_id,name,description,created_at) VALUES(?,?,?,?)",
            (user_id, payload.name.strip(), payload.description.strip(), now_iso()),
        )
        collection_id = cur.lastrowid
        create_notification(db, user_id, "collection_created", f"Подборка «{payload.name.strip()}» создана",
                             "", "intel-collections")
        db.commit()
    return {"id": collection_id, "name": payload.name.strip()}


def _require_own_collection(db, collection_id, user_id):
    row = db.execute("SELECT id FROM collections WHERE id=? AND owner_user_id=?", (collection_id, user_id)).fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="Подборка не найдена")


@app.delete("/api/v1/collections/{collection_id}")
def api_delete_collection(collection_id: int, principal: dict = Depends(public_api_principal)):
    user_id = require_user_principal(principal)
    with closing(connect()) as db:
        _require_own_collection(db, collection_id, user_id)
        db.execute("DELETE FROM collections WHERE id=?", (collection_id,))
        db.commit()
    return {"ok": True}


@app.get("/api/v1/collections/{collection_id}/items")
def api_list_collection_items(collection_id: int, principal: dict = Depends(public_api_principal)):
    user_id = require_user_principal(principal)
    with closing(connect()) as db:
        _require_own_collection(db, collection_id, user_id)
        rows = db.execute(
            "SELECT i.group_key, i.added_at, c.* FROM collection_items i "
            "JOIN creatives c ON c.id=i.representative_creative_id "
            "WHERE i.collection_id=? ORDER BY i.added_at DESC",
            (collection_id,),
        ).fetchall()
    items = []
    for r in rows:
        d = _creative_row_to_dict(r)
        d["group_key"] = r["group_key"]
        d["added_at"] = r["added_at"]
        items.append(d)
    return {"items": items}


@app.post("/api/v1/collections/{collection_id}/items")
def api_add_collection_item(collection_id: int, payload: CollectionItemIn, principal: dict = Depends(public_api_principal)):
    user_id = require_user_principal(principal)
    with closing(connect()) as db:
        _require_own_collection(db, collection_id, user_id)
        creative = db.execute("SELECT id FROM creatives WHERE id=?", (payload.creative_id,)).fetchone()
        if not creative:
            raise HTTPException(status_code=404, detail="Креатив не найден")
        db.execute(
            "INSERT INTO collection_items(collection_id,group_key,representative_creative_id,added_at) "
            "VALUES(?,?,?,?) ON CONFLICT(collection_id,group_key) DO NOTHING",
            (collection_id, payload.group_key, payload.creative_id, now_iso()),
        )
        db.commit()
    return {"ok": True}


@app.delete("/api/v1/collections/{collection_id}/items/{group_key}")
def api_remove_collection_item(collection_id: int, group_key: str, principal: dict = Depends(public_api_principal)):
    user_id = require_user_principal(principal)
    with closing(connect()) as db:
        _require_own_collection(db, collection_id, user_id)
        db.execute("DELETE FROM collection_items WHERE collection_id=? AND group_key=?", (collection_id, group_key))
        db.commit()
    return {"ok": True}


_ADDAVTO_METHODS = {"GET", "POST", "PUT", "DELETE"}


@app.api_route("/api/v1/ads/{path:path}", methods=sorted(_ADDAVTO_METHODS))
async def api_ads_proxy(path: str, request: Request, principal: dict = Depends(public_api_principal)):
    """Transparent proxy into addAvto's own API (separate domain, HTTP Basic
    auth, no CORS) so the marketer frontend can call it under its own
    bearer-token session without addAvto's backend having to change at all.
    `/api/v1/ads/<rest>` maps straight onto addAvto's `/api/<rest>` — no
    per-route reshaping, since addAvto's REST surface is already a clean
    fit (projects, accounts, clickru-connections, metrika/connections,
    report, ...). Errors (404/409/422/...) pass through as-is so the
    frontend can surface addAvto's real validation messages."""
    require_user_principal(principal)
    if not ADDAVTO_BASE_URL or not ADDAVTO_ADMIN_USER or not ADDAVTO_ADMIN_PASSWORD:
        raise HTTPException(status_code=503, detail="addAvto integration is not configured (PARSYA_ADDAVTO_BASE_URL/ADMIN_USER/ADMIN_PASSWORD)")
    body = await request.body()
    try:
        # retries=4: repeat CONNECT errors only (WSL DNS blips -> 502, 2026-11-13)
        async with httpx.AsyncClient(auth=(ADDAVTO_ADMIN_USER, ADDAVTO_ADMIN_PASSWORD),
                                     # read 115s: adding a Click.ru connection logs into the cabinet
                                     # via Chrome (~40s); 30s gave 502 + duplicate connection (2026-11-13).
                                     # Front nginx waits 120s.
                                     timeout=httpx.Timeout(115.0, connect=10.0),
                                     transport=httpx.AsyncHTTPTransport(retries=4)) as client:
            upstream = await client.request(
                request.method,
                f"{ADDAVTO_BASE_URL}/api/{path}",
                params=list(request.query_params.multi_items()),
                content=body or None,
                headers={"content-type": request.headers.get("content-type", "application/json")},
            )
    except httpx.RequestError as err:
        raise HTTPException(status_code=502, detail=f"addAvto unreachable: {err}") from err
    content_type = upstream.headers.get("content-type", "")
    if "application/json" in content_type:
        return JSONResponse(content=upstream.json(), status_code=upstream.status_code)
    return JSONResponse(content={"detail": upstream.text[:2000]}, status_code=upstream.status_code)


@app.get("/api/v1/coverage")
def api_coverage(intent: str = "", principal: dict = Depends(public_api_principal)):
    """Real per-platform quality stats for the "Источники и качество" screen — no
    placeholder numbers, everything here comes straight from synced creative data."""
    require_user_principal(principal)
    intents = _parse_intents(intent)
    intent_where = f"WHERE {_intent_exists_sql('c', intents)}" if intents else ""
    intent_params = intents
    with closing(connect()) as db:
        platform_rows = db.execute(
            "SELECT platform, COUNT(*) total, "
            "SUM(CASE WHEN recognition_status='recognized' THEN 1 ELSE 0 END) recognized, "
            "SUM(CASE WHEN recognition_status='partial' THEN 1 ELSE 0 END) partial, "
            f"MAX(captured_at) last_captured_at "
            f"FROM creatives c {intent_where} GROUP BY platform ORDER BY total DESC",
            intent_params,
        ).fetchall()
        with_advertiser_rows = db.execute(
            f"SELECT c.platform, COUNT(DISTINCT c.id) n FROM creatives c "
            f"JOIN creative_advertiser_links l ON l.creative_id=c.id {intent_where} GROUP BY c.platform",
            intent_params,
        ).fetchall()
        with_advertiser = {r["platform"]: r["n"] for r in with_advertiser_rows}
        totals = {
            "creatives": db.execute(f"SELECT COUNT(*) FROM creatives c {intent_where}", intent_params).fetchone()[0],
            "advertisers": db.execute(
                (f"SELECT COUNT(DISTINCT l.advertiser_id) FROM creative_advertiser_links l "
                 f"JOIN creatives c ON c.id=l.creative_id {intent_where}") if intents else
                "SELECT COUNT(*) FROM advertisers",
                intent_params,
            ).fetchone()[0],
            "creatives_with_advertiser": db.execute(
                f"SELECT COUNT(DISTINCT l.creative_id) FROM creative_advertiser_links l "
                f"JOIN creatives c ON c.id=l.creative_id {intent_where}",
                intent_params,
            ).fetchone()[0],
        }
    platforms = []
    for r in platform_rows:
        platforms.append({
            "platform": r["platform"], "total": r["total"], "recognized": r["recognized"],
            "partial": r["partial"], "with_advertiser": with_advertiser.get(r["platform"], 0),
            "last_captured_at": r["last_captured_at"],
        })
    return {"platforms": platforms, "totals": totals}


@app.get("/api/v1/analytics/cross-platform")
def api_cross_platform_reuse(intent: str = "", principal: dict = Depends(public_api_principal)):
    """Creatives (grouped by content_fingerprint) seen on more than one platform —
    the clearest signal that a campaign is real and scaled, not a one-off test."""
    require_user_principal(principal)
    intents = _parse_intents(intent)
    intent_sql = f" AND {_intent_exists_sql('c', intents)}" if intents else ""
    with closing(connect()) as db:
        rows = db.execute(
            "SELECT c.content_fingerprint, COUNT(DISTINCT o.platform) AS platform_count, "
            "GROUP_CONCAT(DISTINCT o.platform) AS platforms, "
            "MIN(c.captured_at) AS first_seen_at, MAX(c.captured_at) AS last_seen_at "
            "FROM creatives c JOIN occurrences o ON o.creative_id=c.id "
            "WHERE c.content_fingerprint IS NOT NULL AND c.content_fingerprint != ''"
            + intent_sql +
            " GROUP BY c.content_fingerprint HAVING platform_count > 1 "
            "ORDER BY platform_count DESC, last_seen_at DESC LIMIT 30",
            intents,
        ).fetchall()
        items = []
        for r in rows:
            rep = db.execute(
                "SELECT domain,title,description,image_url,detection_method FROM creatives "
                "WHERE content_fingerprint=? ORDER BY captured_at DESC LIMIT 1",
                (r["content_fingerprint"],),
            ).fetchone()
            items.append({
                "content_fingerprint": r["content_fingerprint"],
                "platform_count": r["platform_count"],
                "platforms": sorted(p for p in (r["platforms"] or "").split(",") if p),
                "first_seen_at": r["first_seen_at"], "last_seen_at": r["last_seen_at"],
                "domain": rep["domain"] if rep else None,
                "title": rep["title"] if rep else None,
                "description": rep["description"] if rep else None,
                "image_url": rep["image_url"] if rep else None,
                "detection_method": rep["detection_method"] if rep else None,
            })
    return {"items": items}


@app.get("/api/v1/analytics/carousels")
def api_carousels(intent: str = "", principal: dict = Depends(public_api_principal)):
    """Creatives sharing a carousel_id were shown side-by-side in the same scrolling ad
    carousel (sibling slides) — Dima (2026-10-19): competitive context, "who shows next
    to mine" — not a cross-platform/time signal like the other analytics views."""
    require_user_principal(principal)
    intents = _parse_intents(intent)
    intent_sql = f" AND {_intent_exists_sql('c', intents)}" if intents else ""
    with closing(connect()) as db:
        groups = db.execute(
            "SELECT carousel_id, COUNT(*) AS n, platform, MAX(captured_at) AS last_seen_at "
            "FROM creatives c WHERE carousel_id IS NOT NULL AND carousel_id != ''"
            + intent_sql +
            " GROUP BY carousel_id HAVING n > 1 ORDER BY last_seen_at DESC LIMIT 30",
            intents,
        ).fetchall()
        items = []
        for g in groups:
            members = db.execute(
                "SELECT id,domain,title,image_url,carousel_slide_index,detection_method FROM creatives "
                "WHERE carousel_id=? ORDER BY COALESCE(carousel_slide_index,999)",
                (g["carousel_id"],),
            ).fetchall()
            items.append({
                "carousel_id": g["carousel_id"], "platform": g["platform"],
                "member_count": g["n"], "last_seen_at": g["last_seen_at"],
                "members": [dict(m) for m in members],
            })
    return {"items": items}


@app.get("/api/v1/analytics/campaigns")
def api_campaigns(intent: str = "", principal: dict = Depends(public_api_principal)):
    """Distinct campaign_id_candidate values (markup-derived, from creative_identifiers)
    with creative counts — each clickable into the library filtered by that campaign
    (same UX as advertiser/job_id filtering). Not gated on advertiser disclosure."""
    require_user_principal(principal)
    intents = _parse_intents(intent)
    intent_sql = f" AND {_intent_exists_sql('c', intents)}" if intents else ""
    with closing(connect()) as db:
        rows = db.execute(
            "SELECT ci.identifier_value AS campaign_id, COUNT(DISTINCT ci.creative_id) AS n, "
            "MAX(c.captured_at) AS last_seen_at "
            "FROM creative_identifiers ci JOIN creatives c ON c.id=ci.creative_id "
            "WHERE ci.identifier_type='campaign_id_candidate'"
            + intent_sql +
            " GROUP BY ci.identifier_value ORDER BY n DESC LIMIT 200",
            intents,
        ).fetchall()
    return {"items": [dict(r) for r in rows]}


# utm_term placeholder literals seen in real captured UTM data (job #65) — a tracking
# template left unresolved (e.g. the advertiser's ad system never substituted a real
# keyword), not an actual search term. Counting these as "keywords" would be noise.
_UTM_TERM_PLACEHOLDERS = {
    "", "---autotargeting", "{keyword}", "{keyword:}", "{target}", "{adtarget}",
    "{targeting}", "{phrase}", "{phraseid}", "undefined", "none", "null",
}


@app.get("/api/v1/analytics/keywords")
def api_keywords(intent: str = "", principal: dict = Depends(public_api_principal)):
    """Distinct non-empty utm_term values (from creative_utm, synced regardless of
    advertiser disclosure) with creative counts — "Ключевые слова" tab. Filters out known
    tracking-template placeholder literals rather than counting them as real keywords."""
    require_user_principal(principal)
    intents = _parse_intents(intent)
    intent_sql = f" AND {_intent_exists_sql('c', intents)}" if intents else ""
    with closing(connect()) as db:
        rows = db.execute(
            "SELECT cu.utm_term AS keyword, COUNT(DISTINCT cu.creative_id) AS n, "
            "MAX(c.captured_at) AS last_seen_at "
            "FROM creative_utm cu JOIN creatives c ON c.id=cu.creative_id "
            "WHERE COALESCE(cu.utm_term,'') != ''"
            + intent_sql +
            " GROUP BY cu.utm_term ORDER BY n DESC LIMIT 500",
            intents,
        ).fetchall()
        items = [
            dict(r) for r in rows
            if (r["keyword"] or "").strip().lower() not in _UTM_TERM_PLACEHOLDERS
        ][:200]
    return {"items": items}


@app.get("/api/v1/analytics/coverage")
def api_coverage(principal: dict = Depends(public_api_principal)):
    """Advertiser-disclosure coverage %, grouped by platform/detection_method/creative_type —
    synced from MySQL's parsya_advertiser_coverage_report VIEW. Shows exactly where the
    AdTune pipeline still needs work, visually, not just in log analysis.
    Deliberately NOT intent-scoped (2026-10-22): this table is a pre-aggregated global
    rollup from MySQL with no creative_id/job_id back-reference in SQLite, so there's
    nothing to join against per-intent without recomputing it from raw rows — a bigger
    change than this endpoint's own health-metric purpose justifies right now."""
    require_user_principal(principal)
    with closing(connect()) as db:
        rows = db.execute(
            "SELECT platform, detection_method, creative_type, total_creatives, "
            "disclosed_creatives, disclosed_pct FROM advertiser_coverage_report "
            "ORDER BY total_creatives DESC LIMIT 500"
        ).fetchall()
    return {"items": [dict(r) for r in rows]}


@app.get("/api/v1/analytics/fastest-growing")
def api_fastest_growing(intent: str = "", principal: dict = Depends(public_api_principal)):
    """Advertisers whose creative count THIS week outpaces the PRIOR week — richer signal
    than the flat "new in 7 days" feed (which only shows *who has any new creative*, not
    who's actually ramping up spend/activity vs their own baseline). From the backlog
    ("Fastest-growing advertisers" idea, queued 2026-10-19): compares each advertiser's
    distinct-creative count in [now-7d, now) against [now-14d, now-7d), by captured_at."""
    require_user_principal(principal)
    intents = _parse_intents(intent)
    intent_sql = f" AND {_intent_exists_sql('c', intents)}" if intents else ""
    with closing(connect()) as db:
        now = datetime.now(timezone.utc)
        week_start = (now - timedelta(days=7)).replace(microsecond=0).isoformat()
        prior_week_start = (now - timedelta(days=14)).replace(microsecond=0).isoformat()
        rows = db.execute(
            f"""
            SELECT a.id, a.inn, a.canonical_name,
                   SUM(CASE WHEN c.captured_at >= ? THEN 1 ELSE 0 END) AS this_week,
                   SUM(CASE WHEN c.captured_at >= ? AND c.captured_at < ? THEN 1 ELSE 0 END) AS prior_week
            FROM advertisers a
            JOIN creative_advertiser_links l ON l.advertiser_id = a.id
            JOIN creatives c ON c.id = l.creative_id
            WHERE 1=1{intent_sql}
            GROUP BY a.id
            HAVING this_week >= 2 AND this_week > prior_week
            ORDER BY (this_week - prior_week) DESC, this_week DESC
            LIMIT 50
            """,
            (week_start, prior_week_start, week_start) + tuple(intents),
        ).fetchall()
        items = [
            {
                "advertiser_id": r["id"], "inn": r["inn"], "canonical_name": r["canonical_name"],
                "this_week": r["this_week"], "prior_week": r["prior_week"],
                "growth": r["this_week"] - r["prior_week"],
            }
            for r in rows
        ]
    return {"items": items}


@app.get("/api/v1/analytics/landing-mismatch")
def api_landing_mismatch(intent: str = "", principal: dict = Depends(public_api_principal)):
    """Creatives where the advertiser's OWN declared site domain (from their resolved
    Yandex/VK profile) differs from the domain the ad actually lands on — mirrors MySQL's
    parsya_creative_full.landing_domain_mismatch, but recomputed per-sync in
    worker/agent.py's _sync_landing_mismatch with punycode/IDN + www-prefix normalization
    (2026-10-22, see normalize_domain_for_compare) — 118 raw mismatches turned out to be
    66 real ones once punycode/Unicode-IDN and www. variants were no longer miscounted.
    Remaining caveat: some of the 66 are still benign (an app-install ad landing on
    apps.apple.com instead of a website, or a genuine subdomain change) rather than
    something suspicious — this surfaces candidates to look at, not a severity verdict."""
    require_user_principal(principal)
    intents = _parse_intents(intent)
    intent_sql = f" AND {_intent_exists_sql('c', intents)}" if intents else ""
    with closing(connect()) as db:
        rows = db.execute(
            """
            SELECT c.id, c.domain, c.title, c.platform, c.image_url,
                   u.final_url_domain, u.advertised_site_domain
            FROM creative_utm u JOIN creatives c ON c.id = u.creative_id
            WHERE u.landing_domain_mismatch = 1
            """
            + intent_sql +
            " ORDER BY c.captured_at DESC LIMIT 100",
            intents,
        ).fetchall()
    return {"items": [dict(r) for r in rows]}


@app.get("/api/admin/status")
def admin_status(request: Request):
    if not auth_page(request):
        raise HTTPException(status_code=401)
    with closing(connect()) as db:
        return {
            "jobs": db.execute("SELECT COUNT(*) FROM jobs").fetchone()[0],
            "queued": db.execute("SELECT COUNT(*) FROM jobs WHERE status='queued'").fetchone()[0],
            "running": db.execute("SELECT COUNT(*) FROM jobs WHERE status='running'").fetchone()[0],
            "creatives": db.execute("SELECT COUNT(*) FROM creatives").fetchone()[0],
            "occurrences": db.execute("SELECT COUNT(*) FROM occurrences").fetchone()[0],
        }


@app.get("/api/admin/embeddings-status")
def admin_embeddings_status(request: Request):
    """Read-only progress check for the background embedding drain loop
    (_embedding_drain_loop, started at process startup — see init_db()'s call site).
    2026-10-23: this replaced a manually-triggered batch endpoint — embeddings are now
    computed continuously, one item at a time, in the background; nothing to trigger."""
    if not auth_page(request):
        raise HTTPException(status_code=401)
    with closing(connect()) as db:
        remaining_intents = db.execute(
            "SELECT COUNT(DISTINCT PYLOWER(j.key_text)) FROM jobs j "
            "LEFT JOIN intent_embeddings ie ON ie.key_text_lower = PYLOWER(j.key_text) "
            "WHERE j.key_text IS NOT NULL AND TRIM(j.key_text) <> '' AND ie.key_text_lower IS NULL"
        ).fetchone()[0]
        remaining_creatives = db.execute(
            "SELECT COUNT(*) FROM creatives c LEFT JOIN creative_embeddings ce ON ce.creative_id=c.id "
            "WHERE ce.creative_id IS NULL AND (COALESCE(c.title,'')<>'' OR COALESCE(c.description,'')<>'')"
        ).fetchone()[0]
        done_intents = db.execute("SELECT COUNT(*) FROM intent_embeddings").fetchone()[0]
        done_creatives_real = db.execute("SELECT COUNT(*) FROM creative_embeddings WHERE skip_reason IS NULL").fetchone()[0]
        done_creatives_skipped = db.execute(
            "SELECT skip_reason, COUNT(*) n FROM creative_embeddings WHERE skip_reason IS NOT NULL GROUP BY skip_reason"
        ).fetchall()
    return {
        "intents_done": done_intents, "intents_remaining": remaining_intents,
        "creatives_with_real_embedding": done_creatives_real,
        "creatives_skipped": {r["skip_reason"]: r["n"] for r in done_creatives_skipped},
        "creatives_remaining": remaining_creatives,
    }
