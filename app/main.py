from __future__ import annotations

import base64
import gzip
import hashlib
import hmac
import json
import os
import secrets
import sqlite3
from contextlib import closing
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from fastapi import Depends, FastAPI, Form, Header, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, RedirectResponse
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
SECRET_KEY = os.getenv("PARSYA_SECRET_KEY", "")
COOKIE_SECURE = os.getenv("PARSYA_COOKIE_SECURE", "1").strip().lower() in {"1", "true", "yes"}
MAX_HTML_CHARS = 2_000_000

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


def now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def connect() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, timeout=15)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA busy_timeout=15000")
    return conn


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
        """)
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
        db.commit()


init_db()


def esc(value: Any) -> str:
    text = "" if value is None else str(value)
    return (text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
            .replace('"', "&quot;").replace("'", "&#39;"))


def auth_page(request: Request) -> bool:
    return bool(request.session.get("authenticated"))


def require_admin(request: Request) -> None:
    if not auth_page(request):
        raise HTTPException(status_code=303, headers={"Location": "/login"})


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
            ("/dashboard", "Обзор", "dashboard"),
            ("/runs", "Запуски", "runs"),
            ("/creatives", "Креативы", "creatives"),
            ("/new-run", "Новый запуск", "new-run"),
            ("/admin/users", "Пользователи SaaS", "users"),
        ]
        nav = '<aside class="sidebar"><div class="brand"><span class="brand-mark">P</span><div><b>ParsYa</b><small>CONTROL PLANE</small></div></div><nav>'
        nav += "".join(
            f'<a class="{ "active" if key == active else "" }" href="{href}">{label}</a>'
            for href, label, key in links
        )
        nav += '</nav><div class="side-bottom"><span class="dot"></span> Панель управления<br><small>Пользователь: ' + esc(user) + '</small><form method="post" action="/logout"><input type="hidden" name="csrf_token" value="' + esc(csrf_token(request)) + '"><button class="logout">Выйти</button></form></div></aside>'
    html = f"""<!doctype html>
<html lang="ru"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>{esc(title)} · ParsYa</title>
<style>
:root{{--bg:#0b1220;--panel:#111c2e;--panel2:#16243a;--line:#263750;--text:#e8eef8;--muted:#96a8c0;--blue:#64a5ff;--green:#48d6a0;--amber:#ffcb70;--red:#ff7c88}}
*{{box-sizing:border-box}}body{{margin:0;background:var(--bg);color:var(--text);font:14px/1.5 Inter,ui-sans-serif,system-ui,-apple-system,Segoe UI,sans-serif}}a{{color:var(--blue);text-decoration:none}}a:hover{{text-decoration:underline}}button,input,select,textarea{{font:inherit}}.app{{display:flex;min-height:100vh}}.sidebar{{width:238px;flex:0 0 238px;padding:24px 16px;background:#0d1728;border-right:1px solid var(--line);display:flex;flex-direction:column}}.brand{{display:flex;align-items:center;gap:11px;padding:4px 10px 30px}}.brand-mark{{height:38px;width:38px;border-radius:12px;background:linear-gradient(135deg,#5eb1ff,#7169ff);display:grid;place-items:center;font-size:22px;font-weight:800;color:#061020}}.brand b{{display:block;font-size:17px;letter-spacing:.3px}}.brand small{{display:block;color:var(--muted);font-size:9px;letter-spacing:1.6px}}nav{{display:grid;gap:7px}}nav a{{padding:11px 12px;border-radius:9px;color:#a9bad0}}nav a.active,nav a:hover{{background:#1a2a43;color:#fff;text-decoration:none}}.side-bottom{{margin-top:auto;color:var(--muted);font-size:12px;padding:18px 10px 0}}.dot{{display:inline-block;width:7px;height:7px;border-radius:50%;background:var(--green);margin-right:6px}}.logout{{margin-top:12px;background:none;border:1px solid var(--line);color:var(--muted);padding:7px 12px;border-radius:8px;cursor:pointer}}main{{flex:1;min-width:0;padding:30px clamp(18px,3vw,44px);max-width:1700px}}.topline{{display:flex;justify-content:space-between;gap:16px;align-items:center;margin-bottom:24px}}h1{{margin:0;font-size:26px;letter-spacing:-.5px}}h2{{font-size:16px;margin:0 0 16px}}.muted{{color:var(--muted)}}.grid{{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:14px;margin-bottom:24px}}.metric,.panel,.card{{background:var(--panel);border:1px solid var(--line);border-radius:14px}}.metric{{padding:19px}}.metric .label{{color:var(--muted);font-size:12px}}.metric .value{{font-size:29px;font-weight:750;margin-top:7px;letter-spacing:-.5px}}.metric .hint{{font-size:11px;color:var(--muted);margin-top:2px}}.panel{{padding:21px;margin-bottom:20px}}.panel-head{{display:flex;align-items:center;justify-content:space-between;gap:10px;margin-bottom:15px}}.btn{{display:inline-flex;align-items:center;justify-content:center;gap:7px;background:#5d9eff;color:#071326;border:0;border-radius:9px;padding:10px 14px;font-weight:700;cursor:pointer}}.btn:hover{{background:#83b6ff;text-decoration:none}}.btn.secondary{{background:#1c2c44;color:var(--text);border:1px solid var(--line)}}.btn.danger{{background:#442333;color:#ffb9c0}}.table-wrap{{overflow:auto}}table{{width:100%;border-collapse:collapse;white-space:nowrap}}th{{color:var(--muted);font-size:11px;text-align:left;font-weight:600;padding:10px;border-bottom:1px solid var(--line)}}td{{padding:12px 10px;border-bottom:1px solid #202e43;vertical-align:middle}}tr:last-child td{{border-bottom:0}}.badge{{display:inline-flex;border-radius:99px;padding:4px 8px;font-size:11px;background:#26354c;color:#c5d7ef}}.badge.ok,.badge.completed{{background:#133a37;color:#73e5bc}}.badge.running{{background:#3e321a;color:#ffce7c}}.badge.failed,.badge.error{{background:#44232c;color:#ffadb5}}.badge.queued{{background:#25314a;color:#a9c8ff}}.formgrid{{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:16px}}label{{display:block;color:#b3c4d9;font-size:12px;margin-bottom:7px}}input,select,textarea{{width:100%;background:#0b1525;border:1px solid #30425d;color:var(--text);padding:11px 12px;border-radius:9px;outline:none}}input:focus,select:focus,textarea:focus{{border-color:var(--blue)}}.field{{margin-bottom:16px}}.help{{font-size:12px;color:var(--muted);margin-top:6px}}.empty{{padding:32px;text-align:center;color:var(--muted)}}.filters{{display:flex;gap:10px;align-items:center;flex-wrap:wrap;margin-bottom:16px}}.filters input,.filters select{{width:auto;min-width:170px}}.creative-grid{{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:15px}}.creative{{overflow:hidden}}.creative-preview{{height:195px;background:#e8edf3;color:#172235;overflow:hidden;display:flex;align-items:center;justify-content:center}}.creative-preview iframe{{width:100%;height:100%;border:0;background:white}}.creative-preview img{{max-width:100%;max-height:100%;object-fit:contain}}.creative-info{{padding:15px}}.creative-info h3{{font-size:14px;line-height:1.4;margin:0 0 8px;overflow-wrap:anywhere}}.creative-meta{{display:flex;flex-wrap:wrap;gap:6px;margin:9px 0}}.mono{{font-family:ui-monospace,SFMono-Regular,Menlo,monospace;font-size:11px;white-space:pre-wrap;overflow-wrap:anywhere}}.log{{max-height:600px;overflow:auto;background:#07101d;padding:15px;border-radius:10px;border:1px solid var(--line);color:#bdd0e6}}.detail-grid{{display:grid;grid-template-columns: minmax(0,1fr) minmax(0,1fr);gap:18px}}.html-frame{{width:100%;height:650px;border:1px solid var(--line);background:white;border-radius:10px}}.notice{{border:1px solid #33445d;background:#142139;border-radius:10px;padding:12px 14px;color:#b8cbe2;margin-bottom:16px}}.error-text{{color:#ff9fa8}}.right{{text-align:right}}.footnote{{font-size:11px;color:var(--muted);margin-top:15px}}@media(max-width:1050px){{.grid{{grid-template-columns:repeat(2,minmax(0,1fr))}}.creative-grid{{grid-template-columns:repeat(2,minmax(0,1fr))}}}}@media(max-width:720px){{.app{{display:block}}.sidebar{{width:100%;padding:12px;display:block;border-right:0;border-bottom:1px solid var(--line)}}.brand{{padding:4px 6px 12px}}nav{{display:flex;overflow:auto}}nav a{{white-space:nowrap;padding:8px}}.side-bottom{{display:none}}main{{padding:22px 14px}}.grid{{gap:8px}}.metric{{padding:13px}}.metric .value{{font-size:23px}}.creative-grid,.formgrid,.detail-grid{{grid-template-columns:1fr}}h1{{font-size:22px}}.topline{{align-items:flex-start}}}}
</style></head><body><div class="app">{nav}<main><div class="topline"><div><h1>{esc(title)}</h1><div class="muted">Удалённое управление ParsYa · воркер подключается к серверу исходящим HTTPS</div></div>{'<a class="btn" href="/new-run">＋ Новый запуск</a>' if auth_page(request) and active != 'new-run' else ''}</div>{body}<div class="footnote">ParsYa Control Plane · HTML креативов отображается в изолированном iframe без выполнения скриптов.</div></main></div></body></html>"""
    return HTMLResponse(html)


def admin_or_redirect(request: Request):
    if not auth_page(request):
        return RedirectResponse("/login", status_code=303)
    return None


def worker_auth(authorization: str | None) -> None:
    if not WORKER_TOKEN or not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Worker authentication required")
    provided = authorization[7:].strip()
    if not hmac.compare_digest(provided, WORKER_TOKEN):
        raise HTTPException(status_code=401, detail="Invalid worker token")


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
        return RedirectResponse("/dashboard", status_code=303)
    body = f"""<div style="max-width:440px;margin:8vh auto" class="panel"><div class="brand" style="padding:0 0 20px"><span class="brand-mark">P</span><div><b>ParsYa</b><small>CONTROL PLANE</small></div></div><p class="muted">Войдите в панель управления парсером.</p>{'<div class="notice error-text">'+esc(error)+'</div>' if error else ''}<form method="post" action="/login"><input type="hidden" name="csrf_token" value="{esc(csrf_token(request))}"><div class="field"><label>Логин</label><input name="username" required autocomplete="username"></div><div class="field"><label>Пароль</label><input type="password" name="password" required autocomplete="current-password"></div><button class="btn" style="width:100%">Войти</button></form></div>"""
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
    if hmac.compare_digest(username, ADMIN_USER) and hmac.compare_digest(password, ADMIN_PASSWORD):
        request.session.clear()
        request.session["authenticated"] = True
        request.session["username"] = username
        request.session["csrf"] = secrets.token_urlsafe(32)
        return RedirectResponse("/dashboard", status_code=303)
    return RedirectResponse("/login?error=" + ("Неверный%20логин%20или%20пароль"), status_code=303)


@app.post("/logout")
def logout(request: Request, csrf_token_value: str = Form(..., alias="csrf_token")):
    check_csrf(request, csrf_token_value)
    request.session.clear()
    return RedirectResponse("/login", status_code=303)


@app.get("/")
def root(request: Request):
    return RedirectResponse("/dashboard" if auth_page(request) else "/login", status_code=303)


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
    rows = "".join(f'<tr><td><a href="/runs/{r["id"]}">#{r["id"]}</a></td><td>{esc(r["city"])}</td><td>{esc(r["key_text"])}</td><td>{badge(r["status"])}</td><td>{esc(r["created_at"])}</td><td>{r["total_creatives"]}</td></tr>' for r in recent) or '<tr><td colspan="6" class="empty">Запусков пока нет. Создайте первый запуск.</td></tr>'
    workers = "".join(f'<tr><td>{esc(w["worker_id"])}</td><td>{esc(w["hostname"])}</td><td>{badge(w["state"])}</td><td>{esc(w["last_seen"])}</td><td>{("#"+str(w["current_job_id"])) if w["current_job_id"] else "—"}</td></tr>' for w in worker_rows) or '<tr><td colspan="5" class="empty">Воркер ещё не подключался</td></tr>'
    sites = "".join(f'<tr><td>{esc(s["platform"])}</td><td>{s["n"]}</td></tr>' for s in top_sites) or '<tr><td colspan="2" class="empty">Данные появятся после синхронизации</td></tr>'
    body = f"""<div class="grid"><div class="metric"><div class="label">Всего запусков</div><div class="value">{jobs}</div><div class="hint">За всё время</div></div><div class="metric"><div class="label">В очереди / выполняются</div><div class="value">{active}</div><div class="hint">Незавершённые задания</div></div><div class="metric"><div class="label">Уникальные креативы</div><div class="value">{creatives}</div><div class="hint">С сохранённым HTML при наличии</div></div><div class="metric"><div class="label">Обнаружения</div><div class="value">{occurrences}</div><div class="hint">С учётом повторов в разных запусках</div></div></div>
<div class="panel"><div class="panel-head"><h2>Последние запуски</h2><a href="/runs">Все запуски →</a></div><div class="table-wrap"><table><thead><tr><th>ID</th><th>Город</th><th>Запрос</th><th>Статус</th><th>Создан</th><th>Креативы</th></tr></thead><tbody>{rows}</tbody></table></div></div>
<div class="detail-grid"><div class="panel"><div class="panel-head"><h2>Состояние WSL-воркера</h2></div><div class="table-wrap"><table><thead><tr><th>Worker ID</th><th>Узел</th><th>Статус</th><th>Последний сигнал</th><th>Задание</th></tr></thead><tbody>{workers}</tbody></table></div></div><div class="panel"><div class="panel-head"><h2>Площадки по обнаружениям</h2><a href="/creatives">Креативы →</a></div><table><thead><tr><th>Площадка</th><th>Обнаружения</th></tr></thead><tbody>{sites}</tbody></table></div></div>
<div class="notice">Сервер не подключается к WSL напрямую. WSL-воркер сам опрашивает эту панель по HTTPS и забирает задания.</div>"""
    return page(request, "Обзор", body, "dashboard")


@app.get("/new-run", response_class=HTMLResponse)
def new_run_get(request: Request):
    redirect = admin_or_redirect(request)
    if redirect:
        return redirect
    body = f"""<div class="panel" style="max-width:850px"><h2>Параметры запуска</h2><p class="muted">Задание попадёт в очередь. WSL-воркер заберёт его при следующем опросе.</p><form method="post" action="/new-run"><input type="hidden" name="csrf_token" value="{esc(csrf_token(request))}"><div class="formgrid"><div class="field"><label>Город</label><input name="city" value="Екатеринбург" maxlength="100" required><div class="help">Используется существующей логикой парсера для выбора прокси.</div></div><div class="field"><label>Поисковый запрос / ключевая фраза</label><input name="key_text" value="увеличение пенсии" maxlength="300" required><div class="help">Например: «банкротство физических лиц».</div></div></div><div class="notice">В первой версии воркер выполняет только заранее заданный тип задания «запуск ParsYa». Произвольные команды оболочки через панель не передаются.</div><button class="btn">＋ Добавить в очередь</button> <a class="btn secondary" href="/runs">Отмена</a></form></div>"""
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
    return RedirectResponse(f"/runs/{job_id}", status_code=303)


@app.get("/runs", response_class=HTMLResponse)
def runs_page(request: Request):
    redirect = admin_or_redirect(request)
    if redirect:
        return redirect
    with closing(connect()) as db:
        rows_data = db.execute("SELECT * FROM jobs ORDER BY id DESC LIMIT 200").fetchall()
    rows = "".join(f'<tr><td><a href="/runs/{r["id"]}">#{r["id"]}</a></td><td>{esc(r["city"])}</td><td>{esc(r["key_text"])}</td><td><span class="badge {esc(r["status"])}">{esc(r["status"])}</span></td><td>{esc(r["worker_id"] or "—")}</td><td>{esc(r["created_at"])}</td><td>{esc(r["started_at"] or "—")}</td><td>{esc(r["finished_at"] or "—")}</td><td>{r["total_creatives"]}</td></tr>' for r in rows_data) or '<tr><td colspan="9" class="empty">Запусков пока нет</td></tr>'
    body = f'<div class="panel"><div class="panel-head"><h2>История заданий</h2><a class="btn" href="/new-run">＋ Создать запуск</a></div><div class="table-wrap"><table><thead><tr><th>ID</th><th>Город</th><th>Запрос</th><th>Статус</th><th>Воркер</th><th>Создан</th><th>Старт</th><th>Окончен</th><th>Креативы</th></tr></thead><tbody>{rows}</tbody></table></div></div>'
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
    await fetch(`/api/admin/jobs/${{jobId}}/browser/command`, {{
      method: 'POST', headers: {{'Content-Type': 'application/json'}}, body: JSON.stringify(command),
    }});
  }}
  async function poll() {{
    try {{
      const r = await fetch(`/api/admin/jobs/${{jobId}}/browser/state`);
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


@app.get("/creatives", response_class=HTMLResponse)
def creatives_page(request: Request, q: str = "", platform: str = "", status: str = ""):
    redirect = admin_or_redirect(request)
    if redirect:
        return redirect
    where, params = [], []
    if q.strip():
        like = "%" + q.strip()[:150] + "%"
        where.append("(title LIKE ? OR domain LIKE ? OR description LIKE ? OR platform LIKE ?)")
        params.extend([like, like, like, like])
    if platform:
        where.append("platform=?")
        params.append(platform[:255])
    if status:
        where.append("recognition_status=?")
        params.append(status[:32])
    where_sql = " WHERE " + " AND ".join(where) if where else ""
    with closing(connect()) as db:
        platforms = db.execute("SELECT DISTINCT platform FROM creatives ORDER BY platform").fetchall()
        total = db.execute("SELECT COUNT(*) n FROM creatives" + where_sql, params).fetchone()["n"]
        items = db.execute("SELECT * FROM creatives" + where_sql + " ORDER BY captured_at DESC LIMIT 150", params).fetchall()
    platform_options = '<option value="">Все площадки</option>' + "".join(f'<option value="{esc(p["platform"])}" {"selected" if platform == p["platform"] else ""}>{esc(p["platform"])}</option>' for p in platforms)
    cards = []
    for item in items:
        preview = f'<img src="{esc(item["image_url"])}" alt="" loading="lazy" referrerpolicy="no-referrer">' if item["image_url"] and item["image_url"].startswith(("https://", "http://")) else '<span class="muted">HTML-креатив</span>'
        cards.append(f'<article class="card creative"><a class="creative-preview" href="/creatives/{item["id"]}">{preview}</a><div class="creative-info"><h3><a href="/creatives/{item["id"]}">{esc(item["title"] or item["domain"] or "Без заголовка")}</a></h3><div class="muted">{esc(item["platform"])} · {esc(item["domain"] or "домен не определён")}</div><div class="creative-meta"><span class="badge">{esc(item["detection_method"])}</span><span class="badge {"ok" if item["recognition_status"] == "recognized" else "running"}">{esc(item["recognition_status"])}</span></div><div class="muted">{esc(item["captured_at"])}</div></div></article>')
    body = f"""<div class="panel"><form method="get" action="/creatives" class="filters"><input name="q" placeholder="Поиск по тексту / домену" value="{esc(q)}"><select name="platform">{platform_options}</select><select name="status"><option value="">Все статусы</option><option value="recognized" {"selected" if status=="recognized" else ""}>recognized</option><option value="partial" {"selected" if status=="partial" else ""}>partial</option></select><button class="btn">Фильтровать</button><a class="btn secondary" href="/creatives">Сбросить</a><span class="muted">Всего: {total}</span></form><div class="creative-grid">{''.join(cards) if cards else '<div class="empty">Креативы пока не синхронизированы с WSL. Убедитесь, что воркер подключён и MySQL-доступ настроен.</div>'}</div></div>"""
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
    ev_rows = "".join(f'<tr><td><a href="/runs/{e["job_id"]}">#{e["job_id"]}</a></td><td>{esc(e["platform"])}</td><td>{esc(e["observed_at"])}</td><td>{esc(e["detection_method"])}</td><td>{esc(e["city"])}</td><td>{esc(e["key_text"])}</td></tr>' for e in events) or '<tr><td colspan="6" class="empty">История обнаружений отсутствует</td></tr>'
    body = f"""<p><a href="/creatives">← К библиотеке</a></p><div class="detail-grid"><div class="panel"><h2>Метаданные креатива</h2><table><tbody><tr><th>Площадка</th><td>{esc(item["platform"])}</td></tr><tr><th>Рекламодатель</th><td>{esc(item["domain"] or "—")}</td></tr><tr><th>Заголовок</th><td>{esc(item["title"] or "—")}</td></tr><tr><th>Описание</th><td>{esc(item["description"] or "—")}</td></tr><tr><th>Метод</th><td>{esc(item["detection_method"] or "—")}</td></tr><tr><th>Распознавание</th><td>{esc(item["recognition_status"])}</td></tr><tr><th>Первый запуск</th><td>{esc(item["first_job_id"] or "—")}</td></tr><tr><th>Последний запуск</th><td>{esc(item["last_job_id"] or "—")}</td></tr><tr><th>Сохранён</th><td>{esc(item["captured_at"])}</td></tr><tr><th>SHA-256 HTML</th><td class="mono">{esc(item["html_sha256"] or "—")}</td></tr><tr><th>Ссылка назначения</th><td>{'<a href="'+esc(item["destination_url"])+'" rel="noopener noreferrer" target="_blank">Открыть сайт</a>' if item["destination_url"] and item["destination_url"].startswith(("https://","http://")) else "—"}</td></tr></tbody></table></div><div class="panel"><h2>Оригинальный HTML · изолированный предпросмотр</h2>{'<iframe class="html-frame" sandbox="" referrerpolicy="no-referrer" srcdoc="'+srcdoc+'"></iframe>' if html_text else '<div class="empty">Для этого креатива HTML не сохранён.</div>'}<p><a class="btn secondary" href="/api/creatives/{creative_id}/html">Скачать HTML</a></p></div></div><div class="panel"><h2>История обнаружений</h2><div class="table-wrap"><table><thead><tr><th>Запуск</th><th>Площадка</th><th>Дата</th><th>Метод</th><th>Город</th><th>Запрос</th></tr></thead><tbody>{ev_rows}</tbody></table></div></div>"""
    return page(request, f"Креатив #{creative_id}", body, "creatives")


@app.get("/api/creatives/{creative_id}/html")
def creative_html_download(request: Request, creative_id: int):
    if not auth_page(request):
        return RedirectResponse("/login", status_code=303)
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
        f'<td class="right">{"" if not u["is_active"] else "<form method=\"post\" action=\"/admin/users/" + str(u["id"]) + "/deactivate\" style=\"display:inline\"><input type=\"hidden\" name=\"csrf_token\" value=\"" + esc(csrf_token(request)) + "\"><button class=\"btn secondary\">Отключить</button></form>"}</td></tr>'
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
<form method="post" action="/admin/users" class="formgrid">
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
            return RedirectResponse(f"/admin/users?error=Email+{email}+уже+занят", status_code=303)
        db.execute(
            "INSERT INTO users(email,password_hash,role,is_active,created_at) VALUES(?,?,?,1,?)",
            (email, hash_password(temp_password), role, now_iso()),
        )
        db.commit()
    return RedirectResponse(f"/admin/users?created_email={email}&created_password={temp_password}", status_code=303)


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
    return RedirectResponse("/admin/users", status_code=303)


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
        job = db.execute("SELECT * FROM jobs WHERE status='queued' ORDER BY id LIMIT 1").fetchone()
        if not job:
            db.execute("UPDATE workers SET state='idle',current_job_id=NULL,last_seen=? WHERE worker_id=?", (now_iso(), payload.worker_id))
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
                "INSERT INTO advertisers(identity_key,inn,canonical_name,first_seen_at,last_seen_at) VALUES(?,?,?,?,?) "
                "ON CONFLICT(identity_key) DO UPDATE SET "
                "inn=COALESCE(advertisers.inn,excluded.inn),"
                "canonical_name=CASE "
                "WHEN COALESCE(excluded.canonical_name,'')='' THEN advertisers.canonical_name "
                "WHEN COALESCE(advertisers.canonical_name,'')='' THEN excluded.canonical_name "
                "WHEN LENGTH(excluded.canonical_name)>LENGTH(advertisers.canonical_name) THEN excluded.canonical_name "
                "ELSE advertisers.canonical_name END,"
                "last_seen_at=excluded.last_seen_at",
                (link.advertiser_identity_key, inn, link.advertiser_name or None, now, now),
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


@app.post("/api/worker/jobs/{job_id}/finish")
def worker_finish(job_id: int, payload: FinishIn, authorization: str | None = Header(default=None)):
    worker_auth(authorization)
    with closing(connect()) as db:
        job = db.execute("SELECT worker_id FROM jobs WHERE id=?", (job_id,)).fetchone()
        if not job:
            raise HTTPException(status_code=404, detail="Задание не найдено")
        db.execute(
            "UPDATE jobs SET status=?,finished_at=?,exit_code=?,error_text=?,result_json=?,total_creatives=(SELECT COUNT(DISTINCT creative_id) FROM occurrences WHERE job_id=?) WHERE id=?",
            (payload.status, now_iso(), payload.exit_code, payload.error or None,
             json.dumps(payload.result, ensure_ascii=False), job_id, job_id),
        )
        if job["worker_id"]:
            db.execute("UPDATE workers SET state='idle',current_job_id=NULL,last_seen=?,last_error=? WHERE worker_id=?",
                       (now_iso(), payload.error or None, job["worker_id"]))
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
def api_list_advertisers(q: str = "", limit: int = 25, offset: int = 0, principal: dict = Depends(public_api_principal)):
    limit, offset = _paging(limit, offset)
    with closing(connect()) as db:
        where, params = "", []
        if q:
            where = "WHERE a.inn LIKE ? OR a.canonical_name LIKE ?"
            params = [f"%{q}%", f"%{q}%"]
        total = db.execute(f"SELECT COUNT(*) FROM advertisers a {where}", params).fetchone()[0]
        rows = db.execute(
            f"SELECT a.id,a.inn,a.canonical_name,a.first_seen_at,a.last_seen_at,"
            f"(SELECT COUNT(*) FROM creative_advertiser_links l WHERE l.advertiser_id=a.id) AS creatives_count "
            f"FROM advertisers a {where} ORDER BY a.last_seen_at DESC LIMIT ? OFFSET ?",
            params + [limit, offset],
        ).fetchall()
        items = [
            {"id": r["id"], "inn": r["inn"], "name": r["canonical_name"], "creatives_count": r["creatives_count"],
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


@app.get("/api/v1/creatives")
def api_list_creatives(
    inn: str = "", platform: str = "", q: str = "", status: str = "", job_id: int | None = None,
    has_advertiser: bool | None = None, has_app: bool | None = None, has_discount: bool | None = None,
    campaign_id: str = "", utm_campaign: str = "", keyword: str = "",
    date_from: str = "", date_to: str = "",
    sort: str = "recent", limit: int = 50, offset: int = 0,
    principal: dict = Depends(public_api_principal),
):
    limit, offset = _paging(limit, offset)
    with closing(connect()) as db:
        joins = []
        where_parts = []
        params: list[Any] = []
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
        if job_id is not None:
            joins.append("JOIN occurrences jo ON jo.creative_id=c.id")
            where_parts.append("jo.job_id=?")
            params.append(job_id)
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
    return {"items": items, "total": total, "limit": limit, "offset": offset}


@app.get("/api/v1/platforms")
def api_list_platforms(principal: dict = Depends(public_api_principal)):
    with closing(connect()) as db:
        rows = db.execute("SELECT platform, COUNT(*) n FROM creatives GROUP BY platform ORDER BY n DESC").fetchall()
    return {"items": [{"platform": r["platform"], "creatives_count": r["n"]} for r in rows]}


@app.get("/api/v1/creatives/{creative_id}")
def api_creative_detail(creative_id: int, principal: dict = Depends(public_api_principal)):
    with closing(connect()) as db:
        row = db.execute("SELECT * FROM creatives WHERE id=?", (creative_id,)).fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Креатив не найден")
        advertisers = db.execute(
            "SELECT a.inn,a.canonical_name,l.association_method,l.confidence FROM creative_advertiser_links l "
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
            "utm_term,resolve_status,resolved_at FROM creative_utm WHERE creative_id=?",
            (creative_id,),
        ).fetchone()
    return {
        "creative": _creative_row_to_dict(row),
        "advertisers": [dict(r) for r in advertisers],
        "identifiers": [dict(r) for r in identifiers],
        "occurrences": [dict(r) for r in occurrences],
        "secondary_links": [dict(r) for r in secondary_links],
        "utm": dict(utm_row) if utm_row else None,
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


@app.post("/api/v1/runs")
def api_create_run(payload: RunCreateIn, principal: dict = Depends(public_api_principal)):
    """Lets a logged-in SaaS user queue a scan with their own parameters, self-service —
    the WSL worker picks it up exactly like an ops-console run; only ownership differs."""
    user_id = require_user_principal(principal)
    city, key_text = payload.city.strip(), payload.key_text.strip()
    with closing(connect()) as db:
        active = db.execute(
            "SELECT COUNT(*) FROM jobs WHERE owner_user_id=? AND status IN ('queued','running')", (user_id,)
        ).fetchone()[0]
        if active >= 3:
            raise HTTPException(status_code=429, detail="У вас уже 3 запуска в очереди/выполняются — подождите их завершения")
        cur = db.execute(
            "INSERT INTO jobs(status,city,key_text,created_at,owner_user_id) VALUES('queued',?,?,?,?)",
            (city, key_text, now_iso(), user_id),
        )
        job_id = cur.lastrowid
        db.commit()
    return {"id": job_id, "status": "queued", "city": city, "key_text": key_text}


@app.get("/api/v1/runs")
def api_list_runs(limit: int = 25, offset: int = 0, principal: dict = Depends(public_api_principal)):
    user_id = require_user_principal(principal)
    limit, offset = _paging(limit, offset)
    with closing(connect()) as db:
        total = db.execute("SELECT COUNT(*) FROM jobs WHERE owner_user_id=?", (user_id,)).fetchone()[0]
        rows = db.execute(
            "SELECT id,status,city,key_text,created_at,started_at,finished_at,total_creatives,error_text "
            "FROM jobs WHERE owner_user_id=? ORDER BY id DESC LIMIT ? OFFSET ?",
            (user_id, limit, offset),
        ).fetchall()
    return {"items": [dict(r) for r in rows], "total": total, "limit": limit, "offset": offset}


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
    return {
        "id": job["id"], "status": job["status"], "city": job["city"], "key_text": job["key_text"],
        "created_at": job["created_at"], "started_at": job["started_at"], "finished_at": job["finished_at"],
        "total_creatives": job["total_creatives"], "error_text": job["error_text"],
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


@app.get("/api/v1/coverage")
def api_coverage(principal: dict = Depends(public_api_principal)):
    """Real per-platform quality stats for the "Источники и качество" screen — no
    placeholder numbers, everything here comes straight from synced creative data."""
    require_user_principal(principal)
    with closing(connect()) as db:
        platform_rows = db.execute(
            "SELECT platform, COUNT(*) total, "
            "SUM(CASE WHEN recognition_status='recognized' THEN 1 ELSE 0 END) recognized, "
            "SUM(CASE WHEN recognition_status='partial' THEN 1 ELSE 0 END) partial, "
            "MAX(captured_at) last_captured_at "
            "FROM creatives GROUP BY platform ORDER BY total DESC"
        ).fetchall()
        with_advertiser_rows = db.execute(
            "SELECT c.platform, COUNT(DISTINCT c.id) n FROM creatives c "
            "JOIN creative_advertiser_links l ON l.creative_id=c.id GROUP BY c.platform"
        ).fetchall()
        with_advertiser = {r["platform"]: r["n"] for r in with_advertiser_rows}
        totals = {
            "creatives": db.execute("SELECT COUNT(*) FROM creatives").fetchone()[0],
            "advertisers": db.execute("SELECT COUNT(*) FROM advertisers").fetchone()[0],
            "creatives_with_advertiser": db.execute("SELECT COUNT(DISTINCT creative_id) FROM creative_advertiser_links").fetchone()[0],
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
def api_cross_platform_reuse(principal: dict = Depends(public_api_principal)):
    """Creatives (grouped by content_fingerprint) seen on more than one platform —
    the clearest signal that a campaign is real and scaled, not a one-off test."""
    require_user_principal(principal)
    with closing(connect()) as db:
        rows = db.execute(
            "SELECT c.content_fingerprint, COUNT(DISTINCT o.platform) AS platform_count, "
            "GROUP_CONCAT(DISTINCT o.platform) AS platforms, "
            "MIN(c.captured_at) AS first_seen_at, MAX(c.captured_at) AS last_seen_at "
            "FROM creatives c JOIN occurrences o ON o.creative_id=c.id "
            "WHERE c.content_fingerprint IS NOT NULL AND c.content_fingerprint != '' "
            "GROUP BY c.content_fingerprint HAVING platform_count > 1 "
            "ORDER BY platform_count DESC, last_seen_at DESC LIMIT 30"
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
def api_carousels(principal: dict = Depends(public_api_principal)):
    """Creatives sharing a carousel_id were shown side-by-side in the same scrolling ad
    carousel (sibling slides) — Dima (2026-10-19): competitive context, "who shows next
    to mine" — not a cross-platform/time signal like the other analytics views."""
    require_user_principal(principal)
    with closing(connect()) as db:
        groups = db.execute(
            "SELECT carousel_id, COUNT(*) AS n, platform, MAX(captured_at) AS last_seen_at "
            "FROM creatives WHERE carousel_id IS NOT NULL AND carousel_id != '' "
            "GROUP BY carousel_id HAVING n > 1 ORDER BY last_seen_at DESC LIMIT 30"
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
def api_campaigns(principal: dict = Depends(public_api_principal)):
    """Distinct campaign_id_candidate values (markup-derived, from creative_identifiers)
    with creative counts — each clickable into the library filtered by that campaign
    (same UX as advertiser/job_id filtering). Not gated on advertiser disclosure."""
    require_user_principal(principal)
    with closing(connect()) as db:
        rows = db.execute(
            "SELECT ci.identifier_value AS campaign_id, COUNT(DISTINCT ci.creative_id) AS n, "
            "MAX(c.captured_at) AS last_seen_at "
            "FROM creative_identifiers ci JOIN creatives c ON c.id=ci.creative_id "
            "WHERE ci.identifier_type='campaign_id_candidate' "
            "GROUP BY ci.identifier_value ORDER BY n DESC LIMIT 200"
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
def api_keywords(principal: dict = Depends(public_api_principal)):
    """Distinct non-empty utm_term values (from creative_utm, synced regardless of
    advertiser disclosure) with creative counts — "Ключевые слова" tab. Filters out known
    tracking-template placeholder literals rather than counting them as real keywords."""
    require_user_principal(principal)
    with closing(connect()) as db:
        rows = db.execute(
            "SELECT cu.utm_term AS keyword, COUNT(DISTINCT cu.creative_id) AS n, "
            "MAX(c.captured_at) AS last_seen_at "
            "FROM creative_utm cu JOIN creatives c ON c.id=cu.creative_id "
            "WHERE COALESCE(cu.utm_term,'') != '' "
            "GROUP BY cu.utm_term ORDER BY n DESC LIMIT 500"
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
    AdTune pipeline still needs work, visually, not just in log analysis."""
    require_user_principal(principal)
    with closing(connect()) as db:
        rows = db.execute(
            "SELECT platform, detection_method, creative_type, total_creatives, "
            "disclosed_creatives, disclosed_pct FROM advertiser_coverage_report "
            "ORDER BY total_creatives DESC LIMIT 500"
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
