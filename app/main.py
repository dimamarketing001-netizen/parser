from __future__ import annotations

import base64
import gzip
import hashlib
import hmac
import json
import os
import re
from urllib.parse import urlsplit
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
from app import taxonomy
from app import console as _console

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


_STEMMER_LOCK = threading.Lock()


def _intent_stems(key_text: str) -> set[str]:
    words = re.findall(r"[a-zа-яё]+", (key_text or "").lower())
    stems = set()
    for word in words:
        if len(word) < 4 or word in _INTENT_STOPWORDS:
            continue
        # Snowball-стеммер не потокобезопасен: параллельные запросы портили его состояние
        # (IndexError в stemWord → 500 на /runs и /creatives), поэтому под блокировкой.
        with _STEMMER_LOCK:
            try:
                stem = _RU_STEMMER.stemWord(word)
            except Exception:
                stem = word[:6]
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


_SEMANTIC_IDS_CACHE: dict[tuple, tuple[float, set[int] | None]] = {}
_SEMANTIC_IDS_LOCK = threading.Lock()


def _semantic_relevant_creative_ids(db: sqlite3.Connection, intents: list[str]) -> set[int] | None:
    """Кеш поверх _semantic_relevant_creative_ids_uncached: подбор по смыслу перебирает все
    embedding'и (≈5 с), а экран аналитики шлёт десяток запросов с одним набором ключевых слов —
    считаем один раз (остальные ждут под блокировкой) и держим результат 2 минуты."""
    cache_key = tuple(sorted(str(i).strip().lower() for i in intents))
    cached = _SEMANTIC_IDS_CACHE.get(cache_key)
    if cached and time.time() - cached[0] < 120:
        return cached[1]
    with _SEMANTIC_IDS_LOCK:
        cached = _SEMANTIC_IDS_CACHE.get(cache_key)
        if cached and time.time() - cached[0] < 120:
            return cached[1]
        result = _semantic_relevant_creative_ids_uncached(db, intents)
        if len(_SEMANTIC_IDS_CACHE) > 200:
            _SEMANTIC_IDS_CACHE.clear()
        _SEMANTIC_IDS_CACHE[cache_key] = (time.time(), result)
        return result


def _semantic_relevant_creative_ids_uncached(db: sqlite3.Connection, intents: list[str]) -> set[int] | None:
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
        CREATE INDEX IF NOT EXISTS ix_occurrences_creative ON occurrences(creative_id);
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

        -- Учётки ЛК — единственный вход в систему (старая консоль /login с PARSYA_ADMIN_USER/PASSWORD
        -- удалена 23.11.2026). role: superuser (создатель, + вкладка «Консоль»), admin (вся база), marketer.
        -- Управление пользователями — ЛК → Консоль (app/console.py).
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
        # Папки-подборки по теме (23.11.2026): вложенность и метка темы таксономии (раздел/подрубрика).
        col_cols = {r["name"] for r in db.execute("PRAGMA table_info(collections)").fetchall()}
        for col, coltype in (("parent_id", "INTEGER"), ("topic_main", "TEXT"), ("topic_sub", "TEXT")):
            if col not in col_cols:
                db.execute(f"ALTER TABLE collections ADD COLUMN {col} {coltype}")
        db.execute("CREATE INDEX IF NOT EXISTS ix_collections_parent ON collections(parent_id)")
        # Состав каруселей (17.11.2026): слайды приходят от парсера отдельно от креативов, потому что
        # одно объявление бывает в нескольких каруселях, а creatives.carousel_id хранит только одну.
        db.execute(
            """CREATE TABLE IF NOT EXISTS carousel_slides (
              carousel_id TEXT NOT NULL, slide_index INTEGER NOT NULL,
              job_id INTEGER, total INTEGER, visible INTEGER,
              title TEXT, description TEXT, domain TEXT, image_url TEXT,
              page_url TEXT, platform TEXT, creative_id INTEGER, observed_at TEXT NOT NULL,
              PRIMARY KEY (carousel_id, slide_index))"""
        )
        # Галерея одного объявления: несколько картинок в том порядке, как в рекламе.
        db.execute(
            """CREATE TABLE IF NOT EXISTS creative_galleries (
              gallery_key TEXT PRIMARY KEY, job_id INTEGER, creative_id INTEGER,
              title TEXT, description TEXT, domain TEXT, images_json TEXT NOT NULL,
              total INTEGER, visible INTEGER, platform TEXT, page_url TEXT, observed_at TEXT NOT NULL)"""
        )
        db.execute("CREATE INDEX IF NOT EXISTS ix_creative_galleries_job ON creative_galleries(job_id)")
        # Показы объявления за запуск (18.11.2026): парсер считает повторы, которые раньше отбрасывал,
        # и лучшее место в ленте за проход (1 = первое объявление прохода). Пока только Дзен.
        db.execute(
            """CREATE TABLE IF NOT EXISTS creative_impressions (
              job_id INTEGER NOT NULL, creative_hash TEXT NOT NULL,
              impressions INTEGER NOT NULL DEFAULT 0, best_pos INTEGER, passes INTEGER,
              updated_at TEXT NOT NULL, PRIMARY KEY (job_id, creative_hash))"""
        )
        db.execute("CREATE INDEX IF NOT EXISTS ix_creative_impressions_hash ON creative_impressions(creative_hash)")
        db.execute("CREATE INDEX IF NOT EXISTS ix_carousel_slides_job ON carousel_slides(job_id)")
        # Дополнительные поля объявления из сохранённой разметки (17.11.2026), см. app/creative_fields.py
        from app import creative_fields as _cf
        db.execute(_cf.SCHEMA)
        db.execute("CREATE INDEX IF NOT EXISTS ix_creative_details_ad ON creative_details(ad_id)")
        db.execute("CREATE INDEX IF NOT EXISTS ix_creative_details_campaign ON creative_details(direct_campaign_id)")
        db.execute("CREATE INDEX IF NOT EXISTS ix_carousel_slides_creative ON carousel_slides(creative_id)")
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
        # Пост VK, в котором найдена реклама: дата публикации, сообщество, статистика (parsya_vk_harvester.py)
        db.execute("""CREATE TABLE IF NOT EXISTS vk_posts (
          creative_id INTEGER PRIMARY KEY REFERENCES creatives(id) ON DELETE CASCADE,
          format TEXT, post_key TEXT, owner_id INTEGER, owner_name TEXT, owner_screen_name TEXT,
          owner_members INTEGER, post_url TEXT, posted_at TEXT,
          views INTEGER, likes INTEGER, reposts INTEGER, comments INTEGER,
          first_seen_at TEXT NOT NULL, last_seen_at TEXT NOT NULL
        )""")
        # Сообщества VK — площадки «Рекламы от автора»: откуда реклама и когда проверяли стену
        db.execute("""CREATE TABLE IF NOT EXISTS vk_communities (
          owner_id INTEGER PRIMARY KEY, name TEXT, screen_name TEXT, members INTEGER,
          monitor INTEGER NOT NULL DEFAULT 1, ads_found INTEGER NOT NULL DEFAULT 0,
          posts_checked INTEGER NOT NULL DEFAULT 0, first_ad_at TEXT, last_ad_at TEXT,
          last_checked_at TEXT, last_error TEXT, added_at TEXT NOT NULL, source TEXT
        )""")
        vk_cols = {r["name"] for r in db.execute("PRAGMA table_info(vk_communities)").fetchall()}
        for col, typ in (("feed_posts", "INTEGER NOT NULL DEFAULT 0"), ("feed_days", "REAL NOT NULL DEFAULT 0"),
                         ("feed_ads", "INTEGER NOT NULL DEFAULT 0")):
            if col not in vk_cols:  # лента с прошлой проверки: частота постов и доля рекламы
                db.execute(f"ALTER TABLE vk_communities ADD COLUMN {col} {typ}")
        db.execute("""CREATE TABLE IF NOT EXISTS vk_groups (
          owner_id INTEGER PRIMARY KEY, name TEXT, screen_name TEXT, members INTEGER, activity TEXT, city TEXT,
          verified INTEGER, site TEXT, description TEXT, type TEXT, is_closed INTEGER, first_seen_at TEXT, updated_at TEXT
        )""")
        db.execute("""CREATE TABLE IF NOT EXISTS vk_group_results (
          job_id INTEGER NOT NULL, owner_id INTEGER NOT NULL, rank INTEGER, PRIMARY KEY(job_id, owner_id)
        )""")
        db.execute("""CREATE TABLE IF NOT EXISTS vk_discover (
          query TEXT PRIMARY KEY, next_offset INTEGER NOT NULL DEFAULT 0, total INTEGER,
          added INTEGER NOT NULL DEFAULT 0, last_run_at TEXT, done INTEGER NOT NULL DEFAULT 0
        )""")
        if "source" not in {r["name"] for r in db.execute("PRAGMA table_info(scan_schedules)").fetchall()}:
            db.execute("ALTER TABLE scan_schedules ADD COLUMN source TEXT NOT NULL DEFAULT 'yandex'")
        job_cols = {row["name"] for row in db.execute("PRAGMA table_info(jobs)").fetchall()}
        if "schedule_id" not in job_cols:
            db.execute("ALTER TABLE jobs ADD COLUMN schedule_id INTEGER")
        if "attempts" not in job_cols:
            db.execute("ALTER TABLE jobs ADD COLUMN attempts INTEGER NOT NULL DEFAULT 0")
        if "retry_at" not in job_cols:
            db.execute("ALTER TABLE jobs ADD COLUMN retry_at TEXT")
        if "variant" not in job_cols:  # вариант теста «набивка профиля» (см. INTENT_TEST.md)
            db.execute("ALTER TABLE jobs ADD COLUMN variant TEXT")
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
                    "INSERT INTO jobs(status,city,key_text,created_at,owner_user_id,schedule_id,variant) VALUES('queued',?,?,?,?,?,?)",
                    (sc["city"], sc["key_text"], now_iso(), sc["owner_user_id"], sc["id"],
                     sc["source"] if "source" in sc.keys() and sc["source"] in ("vk", "vk+feed") else None),
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


VK_WALLS_HOURS = float(os.getenv("PARSYA_VK_WALLS_HOURS", "6"))


def _vk_walls_tick() -> None:
    """Раз в PARSYA_VK_WALLS_HOURS ставит задание «обход стен VK»: стены сообществ, где уже
    находили «Рекламу от автора» (vk_communities). 0 — выключено."""
    if VK_WALLS_HOURS <= 0:
        return
    with closing(connect()) as db:
        try:
            n = db.execute("SELECT COUNT(*) FROM vk_communities WHERE monitor=1").fetchone()[0]
        except sqlite3.OperationalError:
            return
        if not n:
            return
        if db.execute("SELECT 1 FROM jobs WHERE variant='vk-walls' AND status IN ('queued','running') LIMIT 1").fetchone():
            return
        last = db.execute("SELECT MAX(created_at) FROM jobs WHERE variant='vk-walls'").fetchone()[0]
        if last:
            try:
                if datetime.now(timezone.utc) - datetime.fromisoformat(last) < timedelta(hours=VK_WALLS_HOURS):
                    return
            except Exception:
                pass
        db.execute("INSERT INTO jobs(status,city,key_text,created_at,variant) VALUES('queued','Вся Россия','Мониторинг стен VK',?,'vk-walls')",
                   (now_iso(),))
        db.commit()


def _scheduler_loop() -> None:
    while True:
        try:
            _vk_walls_tick()
        except Exception as exc:  # noqa: BLE001
            print(f"[scheduler] vk-walls: {exc}", flush=True)
        try:
            _sched_tick()
        except Exception as exc:  # noqa: BLE001 - фоновый цикл не должен падать
            print(f"[scheduler] error: {exc}", flush=True)
        time.sleep(30)


threading.Thread(target=_scheduler_loop, daemon=True, name="scan-scheduler").start()


def _video_duration_tick(limit: int = 25) -> int:
    """Заполняет длительность рекламных видео по самому файлу: в разметке её нет.
    '0' = определить не удалось (чтобы не опрашивать ссылку бесконечно)."""
    from app.video_duration import probe_duration
    with closing(connect()) as db:
        rows = db.execute(
            "SELECT id, video_src FROM creatives WHERE COALESCE(video_src,'')<>'' "
            "AND COALESCE(video_duration,'')='' ORDER BY id DESC LIMIT ?", (limit,)
        ).fetchall()
    done = 0
    for row in rows:
        seconds = probe_duration(row["video_src"])
        value = (str(int(seconds)) if seconds and float(seconds).is_integer() else str(seconds)) if seconds else "0"
        with closing(connect()) as db:
            db.execute("UPDATE creatives SET video_duration=? WHERE id=? AND COALESCE(video_duration,'')=''", (value, row["id"]))
            db.commit()
        done += 1
    return done


def _video_duration_loop() -> None:
    time.sleep(20)
    while True:
        try:
            n = _video_duration_tick()
        except Exception as exc:  # noqa: BLE001 - фоновый цикл не должен падать
            print(f"[video-duration] error: {exc}", flush=True)
            n = 0
        time.sleep(3 if n else 120)


threading.Thread(target=_video_duration_loop, daemon=True, name="video-duration").start()


def _creative_details_loop() -> None:
    """Разбирает разметку объявлений в creative_details: новые объявления и, после смены
    FIELDS_VERSION, всю базу заново. Пачками, чтобы не держать базу."""
    from app import creative_fields as cf
    time.sleep(25)
    while True:
        try:
            with closing(connect()) as db:
                n = cf.process_pending(db, now_iso(), batch=200)
        except Exception as exc:  # noqa: BLE001 - фоновый цикл не должен падать
            print(f"[creative-details] error: {exc}", flush=True)
            n = 0
        time.sleep(1 if n else 60)


threading.Thread(target=_creative_details_loop, daemon=True, name="creative-details").start()


_DETAIL_JSON = ("callouts_json", "sitelinks_json", "offers_json")


def _creative_details(db: sqlite3.Connection, creative_id: int) -> dict[str, Any] | None:
    row = db.execute("SELECT * FROM creative_details WHERE creative_id=?", (creative_id,)).fetchone()
    if not row:
        return None
    out: dict[str, Any] = {}
    for k in row.keys():
        if k in ("creative_id", "version", "extracted_at"):
            continue
        v = row[k]
        if k in _DETAIL_JSON:
            try:
                v = json.loads(v) if v else []
            except ValueError:
                v = []
            k = k[:-5]
        out[k] = v
    out["is_social"] = bool(out.get("is_social"))
    # срок жизни объявления по номеру РСЯ: все копии с тем же ad_id
    if out.get("ad_id"):
        life = db.execute(
            "SELECT MIN(o.observed_at), MAX(o.observed_at), COUNT(DISTINCT o.job_id) FROM creative_details d "
            "JOIN occurrences o ON o.creative_id=d.creative_id WHERE d.ad_id=?", (out["ad_id"],)
        ).fetchone()
        out["ad_id_first_seen"], out["ad_id_last_seen"], out["ad_id_runs"] = life[0], life[1], life[2]
    return out


# --- Интент креатива (Gemini), 2026-11-13 ---------------------------------------
# creatives.ai_intent — о чём реклама НА САМОМ ДЕЛЕ (определяет Gemini по заголовку и
# тексту). Не путать с ключевым словом запуска (jobs.key_text) — по какому запросу
# парсер искал. См. app/ai_intent.py.
def init_ai_intent() -> None:
    with closing(connect()) as db:
        cols = {row["name"] for row in db.execute("PRAGMA table_info(creatives)").fetchall()}
        for col, typ in (("ai_intent", "TEXT"), ("ai_intent_at", "TEXT"), ("ai_intent_model", "TEXT"),
                         # таксономия (2026-11-23): основной интент (раздел) → под-интент (подрубрика), app/taxonomy
                         ("ai_intent_main", "TEXT"), ("ai_intent_sub", "TEXT"), ("ai_taxonomy_model", "TEXT"), ("ai_taxonomy_at", "TEXT")):
            if col not in cols:
                db.execute(f"ALTER TABLE creatives ADD COLUMN {col} {typ}")
        db.execute("CREATE INDEX IF NOT EXISTS ix_creatives_ai_intent ON creatives(ai_intent)")
        db.execute("CREATE INDEX IF NOT EXISTS ix_creatives_ai_intent_main ON creatives(ai_intent_main, ai_intent_sub)")
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


def _fill_taxonomy_by_rules(limit: int = 500) -> bool:
    """Креативы, у которых интент уже есть, а раздела/подрубрики нет (размечены до 23.11 или без текста),
    раскладываем правилами app/taxonomy — без Gemini."""
    with closing(connect()) as db:
        rows = db.execute(
            "SELECT id,title,description,cta_text,ai_intent,destination_url FROM creatives "
            "WHERE ai_intent IS NOT NULL AND ai_intent_main IS NULL LIMIT ?", (limit,)).fetchall()
        if not rows:
            return False
        stamp = now_iso()
        for r in rows:
            mn, sb, _how = taxonomy.classify(r["title"], r["description"], r["cta_text"], r["ai_intent"], r["destination_url"])
            db.execute("UPDATE creatives SET ai_intent_main=?, ai_intent_sub=?, ai_taxonomy_model=?, ai_taxonomy_at=? WHERE id=?",
                       (mn, sb, taxonomy.VERSION, stamp, r["id"]))
        db.commit()
        return True


def _process_ai_intent_batch() -> bool:
    """Один шаг фонового цикла: до BATCH_SIZE неразмеченных креативов (новые первыми) — один
    запрос к Gemini на пачку. True, если что-то обработали. При сбое пачки пробуем по одному;
    креатив, который 3 раза подряд не размечается, до перезапуска сервиса пропускаем.
    Сначала — оценка пар «ключевое слово → интент» (их мало, а от них зависят цифры «по интенту»)."""
    if _fill_taxonomy_by_rules():
        return True
    if _judge_pending_matches():
        return True
    with closing(connect()) as db:
        skip = [i for i, n in _ai_intent_fail_counts.items() if n >= 3]
        skip_sql = f" AND id NOT IN ({','.join('?' * len(skip))})" if skip else ""
        rows = db.execute(
            "SELECT id,title,description,cta_text,destination_url FROM creatives WHERE ai_intent IS NULL"
            + skip_sql + " ORDER BY id DESC LIMIT ?", skip + [aii.BATCH_SIZE],
        ).fetchall()
        if not rows:
            return False
        known = [r["ai_intent"] for r in db.execute(
            "SELECT ai_intent FROM creatives WHERE ai_intent IS NOT NULL AND ai_intent<>? "
            "GROUP BY ai_intent ORDER BY COUNT(*) DESC LIMIT ?", (aii.UNKNOWN, aii.MAX_KNOWN)).fetchall()]
        items = [dict(r) for r in rows]
        result = aii.classify_batch([{"id": it["id"], "title": it["title"], "description": it["description"]} for it in items],
                                    known, on_hard_failure=_gemini_alert)
        if result is None and len(items) > 1:
            result = {}
            for it in items:
                one = aii.classify_batch([{"id": it["id"], "title": it["title"], "description": it["description"]}],
                                         known, on_hard_failure=None)
                if one:
                    result.update(one)
        result = result or {}
        now = now_iso()
        for it in items:
            val = result.get(it["id"])
            intent, mn, sb = (val, None, None) if isinstance(val, str) else (val or (None, None, None))
            if intent:
                tax_model = aii.last_model_used
                pair = taxonomy.validate(mn, sb) if mn else None  # только точные названия таксономии
                mn, sb = pair if pair else (None, None)
                if not (mn and sb):  # Gemini не дал раздел из таксономии — раскладываем правилами
                    mn, sb, _how = taxonomy.classify(it.get("title"), it.get("description"), it.get("cta_text"),
                                                     intent, it.get("destination_url"))
                    tax_model = taxonomy.VERSION
                db.execute("UPDATE creatives SET ai_intent=?, ai_intent_at=?, ai_intent_model=?, "
                           "ai_intent_main=?, ai_intent_sub=?, ai_taxonomy_model=?, ai_taxonomy_at=? WHERE id=?",
                           (intent, now, aii.last_model_used, mn, sb, tax_model, now, it["id"]))
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


class CarouselSlideIn(BaseModel):
    carousel_id: str = Field(min_length=5, max_length=40)
    slide_index: int = Field(ge=0, le=200)
    title: str = Field(default="", max_length=300)
    description: str = Field(default="", max_length=800)
    domain: str = Field(default="", max_length=200)
    image_url: str = Field(default="", max_length=1000)
    page_url: str = Field(default="", max_length=500)


class GalleryIn(BaseModel):
    title: str = Field(min_length=1, max_length=300)
    description: str = Field(default="", max_length=800)
    domain: str = Field(default="", max_length=200)
    images: list[str] = Field(min_length=2, max_length=20)
    visible: int = Field(default=0, ge=0, le=20)
    page_url: str = Field(default="", max_length=500)


class ImpressionIn(BaseModel):
    creative_hash: str = Field(min_length=1, max_length=128)
    impressions: int = Field(default=1, ge=0, le=100000)
    best_pos: int | None = Field(default=None, ge=1, le=100000)
    passes: int | None = Field(default=None, ge=0, le=1000)


class VkMetaIn(BaseModel):
    creative_hash: str = Field(min_length=1, max_length=128)
    format: str = Field(default="", max_length=8)
    post_key: str = Field(default="", max_length=64)
    owner_id: int | None = None
    owner_name: str = Field(default="", max_length=300)
    owner_screen_name: str = Field(default="", max_length=200)
    owner_members: int | None = None
    post_url: str = Field(default="", max_length=500)
    posted_at: str = Field(default="", max_length=40)
    views: int | None = None
    likes: int | None = None
    reposts: int | None = None
    comments: int | None = None


class VkMetaBatch(BaseModel):
    items: list[VkMetaIn] = Field(default_factory=list, max_length=500)


class ImpressionsBatch(BaseModel):
    items: list[ImpressionIn] = Field(default_factory=list, max_length=2000)


class CarouselSlidesBatch(BaseModel):
    slides: list[CarouselSlideIn] = Field(default_factory=list, max_length=400)
    galleries: list[GalleryIn] = Field(default_factory=list, max_length=200)


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


def _job_source(variant: str | None) -> str:
    """Источник запуска для ЛК: vk / vk+feed / yandex (варианты тестов набивки — это тоже yandex)."""
    base = (variant or "").split("#")[0]
    return base if base in ("vk", "vk+feed") else "yandex"


class RunCreateIn(BaseModel):
    city: str = Field(min_length=1, max_length=100)
    key_text: str = Field(min_length=1, max_length=300)
    # Источник: yandex (обычный парсер РСЯ/Дзен), vk (реклама в поиске VK), vk+feed (поиск + лента VK).
    source: str = Field(default="yandex", pattern="^(yandex|vk|vk\\+feed)$")


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


# Старая серверная консоль (/login, /dashboard, /runs, /admin/users …) удалена 23.11.2026: работаем только в ЛК.
# Её функции — во вкладке «Консоль» ЛК (роль superuser, app/console.py). Старые адреса (в т.ч. пункт «Парсер»
# в сайдбаре addAvto через /parser/) ведут в ЛК.
LK_URL = os.getenv("PARSYA_LK_URL", "https://xn--80ak2afbe.xn--b1ajdba5acbodeeeaj1qb.xn--p1ai/app/ad-intelligence.html").rstrip("/")


def _to_lk(view: str = "console-overview") -> RedirectResponse:
    return RedirectResponse(f"{LK_URL}#{view}", status_code=303)


@app.get("/")
def root():
    return _to_lk("intel-library")


@app.get("/login")
@app.get("/dashboard")
@app.get("/admin/users")
def old_console_pages():
    return _to_lk("console-overview")


@app.get("/runs")
@app.get("/runs/{job_id}")
@app.get("/new-run")
def old_console_runs(job_id: int | None = None):
    return _to_lk("intel-runs")


@app.get("/creatives")
@app.get("/creatives/{creative_id}")
@app.get("/advertisers")
def old_console_library(creative_id: int | None = None):
    return _to_lk("intel-library")


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
        return {"job": {"id": job["id"], "type": "run_parser", "city": job["city"], "key_text": job["key_text"], "variant": job["variant"] if "variant" in job.keys() else None, "attempts": job["attempts"] if "attempts" in job.keys() else 0}, "poll_after_seconds": 2}


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
        # Храним начало лога (100 КБ: аккаунт, прокси, набивка профиля) + хвост 1,4 МБ; раньше оставался только
        # хвост 500 КБ, и у длинных запусков пропадали строки про аккаунт и набивку (17.11.2026).
        cur = db.execute(
            "UPDATE jobs SET log_text=CASE WHEN length(log_text)+length(?)>1500000 "
            "THEN substr(log_text,1,100000) || substr(substr(log_text,100001) || ?, -1400000) ELSE log_text || ? END "
            "WHERE id=? AND status='running'", (payload.text, payload.text, payload.text, job_id))
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


_CAROUSEL_ID_RE = re.compile(r"^(\d+)-[0-9a-f]{6,12}-(\d+)-(\d+)$")


def carousel_id_parts(carousel_id: str | None) -> tuple[int, int] | None:
    """id карусели нового вида «<запуск>-<uid>-<всего объявлений>-<видно сразу>» → (всего, видно).
    Старые id (хеш без дефисов) были общими для всех каруселей площадки — каруселью их не считаем."""
    m = _CAROUSEL_ID_RE.match(carousel_id or "")
    if not m:
        return None
    total, visible = int(m.group(2)), int(m.group(3))
    return total, max(1, min(visible, total))


def _carousel_platform(page_url: str) -> str:
    host = urlsplit(page_url if "//" in page_url else "//" + page_url).netloc.lower()
    return re.sub(r"^(www|m)\.", "", host)


def _carousel_match_creative(db: sqlite3.Connection, job_id: int | None, title: str, domain: str) -> int | None:
    """Объявление библиотеки для слайда: тот же заголовок, сначала среди объявлений этого запуска."""
    title = (title or "").strip()
    if not title:
        return None
    if job_id:
        row = db.execute(
            "SELECT c.id FROM occurrences o JOIN creatives c ON c.id=o.creative_id "
            "WHERE o.job_id=? AND c.title=? ORDER BY (c.domain=?) DESC, c.id DESC LIMIT 1",
            (job_id, title, domain or ""),
        ).fetchone()
        if row:
            return int(row["id"])
    row = db.execute(
        "SELECT id FROM creatives WHERE title=? ORDER BY (domain=?) DESC, id DESC LIMIT 1", (title, domain or "")
    ).fetchone()
    return int(row["id"]) if row else None


@app.post("/api/worker/jobs/{job_id}/impressions")
def worker_impressions(job_id: int, payload: ImpressionsBatch, authorization: str | None = Header(default=None)):
    """Счётчик показов: парсер присылает НАКОПЛЕННЫЕ за запуск значения по хэшу креатива,
    повторная отправка заменяет их (не суммирует)."""
    worker_auth(authorization)
    with closing(connect()) as db:
        if not db.execute("SELECT id FROM jobs WHERE id=?", (job_id,)).fetchone():
            raise HTTPException(status_code=404, detail="Задание не найдено")
        now = now_iso()
        for it in payload.items:
            db.execute(
                "INSERT INTO creative_impressions(job_id,creative_hash,impressions,best_pos,passes,updated_at) VALUES(?,?,?,?,?,?) "
                "ON CONFLICT(job_id,creative_hash) DO UPDATE SET impressions=excluded.impressions,"
                "best_pos=COALESCE(excluded.best_pos,creative_impressions.best_pos),"
                "passes=COALESCE(excluded.passes,creative_impressions.passes),updated_at=excluded.updated_at",
                (job_id, it.creative_hash.strip().casefold(), it.impressions, it.best_pos, it.passes, now),
            )
        db.commit()
    return {"ok": True, "accepted": len(payload.items)}


def _creative_impressions(db: sqlite3.Connection, creative_hash: str) -> dict[str, Any] | None:
    if not creative_hash:
        return None
    rows = db.execute(
        "SELECT i.job_id,i.impressions,i.best_pos,i.passes,j.key_text,j.city,j.started_at FROM creative_impressions i "
        "LEFT JOIN jobs j ON j.id=i.job_id WHERE i.creative_hash=? ORDER BY i.job_id DESC",
        (creative_hash.strip().casefold(),),
    ).fetchall()
    if not rows:
        return None
    pos = [r["best_pos"] for r in rows if r["best_pos"]]
    return {
        "total": sum(int(r["impressions"] or 0) for r in rows),
        "runs": len(rows),
        "best_pos": min(pos) if pos else None,
        "by_job": [dict(r) for r in rows[:10]],
    }


class VkCommunityCheckIn(BaseModel):
    owner_id: int
    posts_checked: int = 0
    ads_found: int = 0
    incremental: bool = False  # проверка через ленту: только новые посты с прошлой проверки
    window_days: float | None = None
    error: str = Field(default="", max_length=500)


class VkCommunityChecksBatch(BaseModel):
    items: list[VkCommunityCheckIn] = Field(default_factory=list, max_length=2000)


@app.get("/api/worker/vk-communities")
def worker_vk_communities(limit: int = 300, fresh: int | None = None, authorization: str | None = Header(default=None)):
    """Сообщества на обход стен: давно не проверенные — первыми."""
    worker_auth(authorization)
    limit = max(1, min(limit, 2000))
    with closing(connect()) as db:
        # новые найденные поиском (ещё не проверенные) — не больше VK_DISCOVER_CHECK за обход,
        # чтобы не вытеснять паблики, где реклама уже есть
        fresh = db.execute(
            "SELECT owner_id,name,screen_name,last_checked_at FROM vk_communities WHERE monitor=1 AND source='discover' "
            "AND last_checked_at IS NULL ORDER BY members DESC LIMIT ?",
            (min(VK_DISCOVER_CHECK if fresh is None else max(0, fresh), limit),)).fetchall()
        rest = db.execute(
            "SELECT owner_id,name,screen_name,last_checked_at FROM vk_communities WHERE monitor=1 "
            "AND NOT (source='discover' AND last_checked_at IS NULL) "
            "ORDER BY COALESCE(last_checked_at,'') ASC, ads_found DESC LIMIT ?", (limit,)).fetchall()
    # свежие вперемешку: сначала старые площадки с рекламой, каждый третий — новый кандидат
    items, fi = [], 0
    for i, r in enumerate(rest):
        if i % 3 == 2 and fi < len(fresh):
            items.append(dict(fresh[fi])); fi += 1
        items.append(dict(r))
    items += [dict(r) for r in fresh[fi:]]
    return {"items": items[:limit]}


VK_DISCOVER_CHECK = int(os.getenv("PARSYA_VK_DISCOVER_CHECK", "15"))
VK_DISCOVER_BACKLOG = int(os.getenv("PARSYA_VK_DISCOVER_BACKLOG", "300"))
VK_DISCOVER_TEMPLATES = ["Подслушано", "Объявления", "Барахолка", "Отдам даром", "Услуги", "Новости",
                         "Типичный", "Работа", "Доска объявлений"]


def _vk_discover_seed(db) -> None:
    cities = []
    for (c,) in db.execute("SELECT DISTINCT city FROM jobs WHERE COALESCE(city,'')<>'' AND city<>'Вся Россия'"):
        c = c.strip()
        c = c[:1].upper() + c[1:]
        if c and c not in cities:
            cities.append(c)
    queries = list(VK_DISCOVER_TEMPLATES) + [f"{t} {c}" for c in cities for t in ("Подслушано", "Объявления")]
    db.executemany("INSERT OR IGNORE INTO vk_discover(query) VALUES(?)", [(q,) for q in queries])


@app.get("/api/worker/vk-discover-queries")
def worker_vk_discover_queries(limit: int = 4, authorization: str | None = Header(default=None)):
    """Поиск новых пабликов-площадок: очередь запросов groups.search (шаблон × город), по кругу."""
    worker_auth(authorization)
    with closing(connect()) as db:
        backlog = db.execute("SELECT COUNT(*) FROM vk_communities WHERE source='discover' AND monitor=1 "
                             "AND last_checked_at IS NULL").fetchone()[0]
        if backlog >= VK_DISCOVER_BACKLOG:  # сначала проверим уже найденных кандидатов
            return {"items": [], "backlog": backlog}
        _vk_discover_seed(db)
        db.commit()
        rows = db.execute("SELECT query,next_offset FROM vk_discover WHERE done=0 "
                          "ORDER BY COALESCE(last_run_at,'') ASC, query LIMIT ?", (max(0, min(limit, 50)),)).fetchall()
    return {"items": [dict(r) for r in rows]}


class VkDiscoveredItem(BaseModel):
    owner_id: int
    name: str = Field(default="", max_length=300)
    screen_name: str = Field(default="", max_length=120)
    members: int | None = None


class VkDiscoveredIn(BaseModel):
    query: str = Field(max_length=200)
    next_offset: int = 0
    total: int | None = None
    done: bool = False
    items: list[VkDiscoveredItem] = Field(default_factory=list, max_length=1000)


@app.post("/api/worker/vk-communities/discovered")
def worker_vk_discovered(payload: VkDiscoveredIn, authorization: str | None = Header(default=None)):
    worker_auth(authorization)
    now = now_iso()
    added = 0
    with closing(connect()) as db:
        for it in payload.items:
            cur = db.execute("INSERT OR IGNORE INTO vk_communities(owner_id,name,screen_name,members,monitor,added_at,source) "
                             "VALUES(?,?,?,?,1,?,'discover')", (it.owner_id, it.name, it.screen_name, it.members, now))
            added += cur.rowcount
        db.execute("INSERT OR IGNORE INTO vk_discover(query) VALUES(?)", (payload.query,))
        db.execute("UPDATE vk_discover SET next_offset=?,total=?,added=added+?,last_run_at=?,done=? WHERE query=?",
                   (payload.next_offset, payload.total, added, now, int(payload.done), payload.query))
        db.commit()
    return {"ok": True, "added": added}


@app.get("/api/worker/vk-ord-known")
def worker_vk_ord_known(authorization: str | None = Header(default=None)):
    """ERID → рекламодатель по уже собранной рекламе VK: сборщику не надо заново открывать ОРД."""
    worker_auth(authorization)
    with closing(connect()) as db:
        rows = db.execute(
            "SELECT cr.erid AS erid, MAX(a.canonical_name) AS name, MAX(a.inn) AS inn "
            "FROM creatives cr JOIN creative_advertiser_links l ON l.creative_id=cr.id "
            "JOIN advertisers a ON a.id=l.advertiser_id "
            "WHERE cr.platform='vk.com' AND COALESCE(cr.erid,'')<>'' "
            "AND (COALESCE(a.inn,'')<>'' OR COALESCE(a.canonical_name,'')<>'') GROUP BY cr.erid").fetchall()
    return {"items": {r["erid"]: {k: r[k] for k in ("name", "inn") if r[k]} for r in rows}}


class VkGroupIn(BaseModel):
    owner_id: int
    name: str = Field(default="", max_length=300)
    screen_name: str = Field(default="", max_length=120)
    members: int | None = None
    activity: str = Field(default="", max_length=200)
    city: str = Field(default="", max_length=100)
    verified: int = 0
    site: str = Field(default="", max_length=300)
    description: str = Field(default="", max_length=1000)
    type: str = Field(default="", max_length=20)
    is_closed: int = 0
    rank: int | None = None


class VkGroupsBatch(BaseModel):
    items: list[VkGroupIn] = Field(default_factory=list, max_length=1000)


@app.post("/api/worker/jobs/{job_id}/vk-groups")
def worker_vk_groups(job_id: int, payload: VkGroupsBatch, authorization: str | None = Header(default=None)):
    """Результат поиска сообществ VK по ключу и городу."""
    worker_auth(authorization)
    now = now_iso()
    new = 0
    with closing(connect()) as db:
        for it in payload.items:
            existed = db.execute("SELECT 1 FROM vk_groups WHERE owner_id=?", (it.owner_id,)).fetchone()
            new += 0 if existed else 1
            db.execute(
                "INSERT INTO vk_groups(owner_id,name,screen_name,members,activity,city,verified,site,description,type,is_closed,first_seen_at,updated_at) "
                "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(owner_id) DO UPDATE SET name=excluded.name,screen_name=excluded.screen_name,"
                "members=excluded.members,activity=excluded.activity,city=excluded.city,verified=excluded.verified,site=excluded.site,"
                "description=excluded.description,type=excluded.type,is_closed=excluded.is_closed,updated_at=excluded.updated_at",
                (it.owner_id, it.name, it.screen_name, it.members, it.activity, it.city, it.verified, it.site, it.description,
                 it.type, it.is_closed, now, now))
            db.execute("INSERT OR REPLACE INTO vk_group_results(job_id,owner_id,rank) VALUES(?,?,?)", (job_id, it.owner_id, it.rank))
            db.execute("UPDATE vk_communities SET members=COALESCE(?,members) WHERE owner_id=?", (it.members, it.owner_id))
        db.commit()
    return {"ok": True, "new": new}


@app.post("/api/worker/vk-communities/checked")
def worker_vk_communities_checked(payload: VkCommunityChecksBatch, authorization: str | None = Header(default=None)):
    worker_auth(authorization)
    now = now_iso()
    with closing(connect()) as db:
        for it in payload.items:
            db.execute("UPDATE vk_communities SET last_checked_at=?,posts_checked=posts_checked+?,"
                       "ads_found=CASE WHEN ? THEN ads_found+? ELSE ? END,last_error=? WHERE owner_id=?",
                       (now, it.posts_checked, int(it.incremental), it.ads_found, it.ads_found, it.error or None, it.owner_id))
            if it.incremental and it.window_days:
                db.execute("UPDATE vk_communities SET feed_posts=feed_posts+?,feed_days=feed_days+?,feed_ads=feed_ads+? "
                           "WHERE owner_id=?", (it.posts_checked, it.window_days, it.ads_found, it.owner_id))
            if not it.ads_found:  # пусто или стена закрыта/ошибка
                # кандидат из поиска без рекламы в последних постах — снимаем с обхода
                db.execute("UPDATE vk_communities SET monitor=0 WHERE owner_id=? AND source='discover' "
                           "AND first_ad_at IS NULL", (it.owner_id,))
        db.commit()
    return {"ok": True}


@app.post("/api/worker/jobs/{job_id}/vk-meta")
def worker_vk_meta(job_id: int, payload: VkMetaBatch, authorization: str | None = Header(default=None)):
    """Дата публикации и статистика VK-постов. Статистику обновляем свежей, дату публикации — не трогаем."""
    worker_auth(authorization)
    accepted = 0
    now = now_iso()
    with closing(connect()) as db:
        for it in payload.items:
            row = db.execute("SELECT id FROM creatives WHERE creative_hash=?", (it.creative_hash,)).fetchone()
            if not row:
                continue
            db.execute(
                "INSERT INTO vk_posts(creative_id,format,post_key,owner_id,owner_name,owner_screen_name,owner_members,post_url,posted_at,"
                "views,likes,reposts,comments,first_seen_at,last_seen_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) "
                "ON CONFLICT(creative_id) DO UPDATE SET last_seen_at=excluded.last_seen_at,"
                "posted_at=COALESCE(NULLIF(vk_posts.posted_at,''),excluded.posted_at),"
                "owner_members=COALESCE(excluded.owner_members,vk_posts.owner_members),"
                "views=COALESCE(excluded.views,vk_posts.views),likes=COALESCE(excluded.likes,vk_posts.likes),"
                "reposts=COALESCE(excluded.reposts,vk_posts.reposts),comments=COALESCE(excluded.comments,vk_posts.comments)",
                (row["id"], it.format, it.post_key, it.owner_id, it.owner_name, it.owner_screen_name, it.owner_members,
                 it.post_url, it.posted_at, it.views, it.likes, it.reposts, it.comments, now, now),
            )
            if it.owner_id and it.owner_id < 0:  # паблик с рекламой — на мониторинг стены
                db.execute(
                    "INSERT INTO vk_communities(owner_id,name,screen_name,members,first_ad_at,last_ad_at,added_at,source) "
                    "VALUES(?,?,?,?,?,?,?,?) ON CONFLICT(owner_id) DO UPDATE SET name=excluded.name,"
                    "screen_name=COALESCE(NULLIF(excluded.screen_name,''),vk_communities.screen_name),"
                    "members=COALESCE(excluded.members,vk_communities.members),"
                    "first_ad_at=COALESCE(vk_communities.first_ad_at,excluded.first_ad_at),"
                    "monitor=CASE WHEN vk_communities.source='discover' THEN 1 ELSE vk_communities.monitor END,"
                    "last_ad_at=MAX(COALESCE(vk_communities.last_ad_at,''),excluded.last_ad_at)",
                    (it.owner_id, it.owner_name, it.owner_screen_name, it.owner_members, it.posted_at or now,
                     it.posted_at or now, now, f"job:{job_id}"),
                )
            accepted += 1
        db.commit()
    return {"ok": True, "accepted": accepted}


@app.post("/api/worker/jobs/{job_id}/carousels")
def worker_carousels(job_id: int, payload: CarouselSlidesBatch, authorization: str | None = Header(default=None)):
    """Состав каруселей из разных объявлений: парсер присылает каждый увиденный слайд
    (id карусели, место, заголовок, домен, картинка). Повторная отправка слайда его обновляет."""
    worker_auth(authorization)
    accepted = 0
    with closing(connect()) as db:
        if not db.execute("SELECT id FROM jobs WHERE id=?", (job_id,)).fetchone():
            raise HTTPException(status_code=404, detail="Задание не найдено")
        for item in payload.slides:
            parts = carousel_id_parts(item.carousel_id.strip())
            if not parts or not item.title.strip():
                continue
            total, visible = parts
            db.execute(
                "INSERT INTO carousel_slides(carousel_id,slide_index,job_id,total,visible,title,description,domain,image_url,"
                "page_url,platform,creative_id,observed_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?) "
                "ON CONFLICT(carousel_id,slide_index) DO UPDATE SET title=excluded.title,description=excluded.description,"
                "domain=excluded.domain,image_url=CASE WHEN excluded.image_url<>'' THEN excluded.image_url ELSE carousel_slides.image_url END,"
                "creative_id=COALESCE(excluded.creative_id,carousel_slides.creative_id),observed_at=excluded.observed_at",
                (item.carousel_id.strip(), item.slide_index, job_id, total, visible, item.title.strip(), item.description.strip(),
                 item.domain.strip(), item.image_url.strip(), item.page_url.strip(), _carousel_platform(item.page_url.strip()),
                 _carousel_match_creative(db, job_id, item.title, item.domain.strip()), now_iso()),
            )
            accepted += 1
        galleries = 0
        for g in payload.galleries:
            images = [u.strip()[:1000] for u in g.images if u and u.strip()]
            if len(images) < 2:
                continue
            key = hashlib.sha1("|".join([g.domain.strip().lower(), g.title.strip()] + images).encode("utf-8", "ignore")).hexdigest()
            db.execute(
                "INSERT INTO creative_galleries(gallery_key,job_id,creative_id,title,description,domain,images_json,total,visible,"
                "platform,page_url,observed_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?) "
                "ON CONFLICT(gallery_key) DO UPDATE SET job_id=excluded.job_id,observed_at=excluded.observed_at,"
                "visible=CASE WHEN excluded.visible>0 THEN excluded.visible ELSE creative_galleries.visible END,"
                "creative_id=COALESCE(excluded.creative_id,creative_galleries.creative_id)",
                (key, job_id, _carousel_match_creative(db, job_id, g.title, g.domain.strip()), g.title.strip(), g.description.strip(),
                 g.domain.strip(), json.dumps(images, ensure_ascii=False), len(images), min(g.visible, len(images)),
                 _carousel_platform(g.page_url.strip()), g.page_url.strip(), now_iso()),
            )
            galleries += 1
        db.commit()
    return {"ok": True, "accepted": accepted, "galleries": galleries}


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
        # размер карусели есть только у id нового вида; у старых (общий хеш на площадку) — None
        "carousel_total": (carousel_id_parts(row["carousel_id"]) or (None, None))[0],
        "carousel_visible": (carousel_id_parts(row["carousel_id"]) or (None, None))[1],
    }


# ---------------------------------------------------------------- сервис «ВКонтакте» (19.11.2026)
VK_COOKIES_FILE = Path(os.getenv("PARSYA_VK_COOKIES", str(Path.home() / ".config/parsya/vk/vk_cookies.json")))


def _vk_account_status() -> dict[str, Any]:
    out: dict[str, Any] = {"cookies_saved_at": None, "cookies_count": 0}
    try:
        data = json.loads(VK_COOKIES_FILE.read_text())
        out["cookies_saved_at"] = data.get("saved_at")
        out["cookies_count"] = len(data.get("cookies") or [])
    except Exception:
        pass
    return out


@app.get("/api/v1/vk/overview")
def api_vk_overview(principal: dict = Depends(public_api_principal)):
    week = (datetime.now(timezone.utc) - timedelta(days=7)).isoformat()
    with closing(connect()) as db:
        one = lambda sql, *a: db.execute(sql, a).fetchone()[0]
        has = lambda t: db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (t,)).fetchone()
        if not has("vk_communities") or not has("vk_posts"):
            return {"creatives": 0, "communities": 0}
        walls = db.execute("SELECT id,status,created_at,finished_at,total_creatives FROM jobs WHERE variant='vk-walls' ORDER BY id DESC LIMIT 1").fetchone()
        search = db.execute("SELECT id,status,key_text,city,finished_at,total_creatives FROM jobs WHERE variant IN ('vk','vk+feed') ORDER BY id DESC LIMIT 1").fetchone()
        top_comm = db.execute(
            "SELECT k.owner_id,k.name,k.screen_name,k.members,COUNT(p.creative_id) AS ads FROM vk_communities k "
            "LEFT JOIN vk_posts p ON p.owner_id=k.owner_id GROUP BY k.owner_id ORDER BY ads DESC LIMIT 6").fetchall()
        top_adv = db.execute(
            "SELECT a.inn,a.canonical_name,COUNT(*) AS ads,COUNT(DISTINCT p.owner_id) AS communities FROM creative_advertiser_links l "
            "JOIN advertisers a ON a.id=l.advertiser_id JOIN creatives c ON c.id=l.creative_id AND c.platform='vk.com' "
            "LEFT JOIN vk_posts p ON p.creative_id=c.id GROUP BY a.id ORDER BY ads DESC LIMIT 6").fetchall()
        return {
            "creatives": one("SELECT COUNT(*) FROM creatives WHERE platform='vk.com'"),
            "creatives_week": one("SELECT COUNT(*) FROM creatives WHERE platform='vk.com' AND captured_at>=?", week),
            "with_advertiser": one("SELECT COUNT(DISTINCT l.creative_id) FROM creative_advertiser_links l JOIN creatives c ON c.id=l.creative_id WHERE c.platform='vk.com'"),
            "communities": one("SELECT COUNT(*) FROM vk_communities WHERE COALESCE(source,'')<>'discover' OR first_ad_at IS NOT NULL"),
            "monitored": one("SELECT COUNT(*) FROM vk_communities WHERE monitor=1 AND (COALESCE(source,'')<>'discover' OR first_ad_at IS NOT NULL)"),
            "candidates": one("SELECT COUNT(*) FROM vk_communities WHERE source='discover' AND first_ad_at IS NULL"),
            "candidates_queued": one("SELECT COUNT(*) FROM vk_communities WHERE source='discover' AND first_ad_at IS NULL AND monitor=1 AND last_checked_at IS NULL"),
            "discovered_with_ads": one("SELECT COUNT(*) FROM vk_communities WHERE source='discover' AND first_ad_at IS NOT NULL"),
            "advertisers": one("SELECT COUNT(DISTINCT l.advertiser_id) FROM creative_advertiser_links l JOIN creatives c ON c.id=l.creative_id WHERE c.platform='vk.com'"),
            "advertisers_both": one(
                "SELECT COUNT(*) FROM (SELECT l.advertiser_id FROM creative_advertiser_links l JOIN creatives c ON c.id=l.creative_id "
                "GROUP BY l.advertiser_id HAVING SUM(c.platform='vk.com')>0 AND SUM(c.platform<>'vk.com')>0)"),
            "last_walls": dict(walls) if walls else None,
            "last_search": dict(search) if search else None,
            "walls_every_hours": VK_WALLS_HOURS,
            "account": _vk_account_status(),
            "top_communities": [dict(r) for r in top_comm],
            "top_advertisers": [{"inn": r["inn"], "name": _clean_adv_name(r["canonical_name"], r["inn"]), "ads": r["ads"],
                                 "communities": r["communities"]} for r in top_adv],
        }


@app.get("/api/v1/vk/communities")
def api_vk_communities(q: str = "", kind: str = "", limit: int = 100, offset: int = 0,
                       principal: dict = Depends(public_api_principal)):
    """kind: '' — площадки с рекламой; 'candidates' — найденные поиском, рекламы пока нет."""
    limit, offset = _paging(limit, offset)
    with closing(connect()) as db:
        has_ads = "(COALESCE(k.source,'')<>'discover' OR k.first_ad_at IS NOT NULL)"
        conds, args = [f"NOT {has_ads}" if kind == "candidates" else has_ads], []
        if q:
            conds.append("(k.name LIKE ? OR k.screen_name LIKE ?)")
            args += [f"%{q}%", f"%{q}%"]
        where = "WHERE " + " AND ".join(conds)
        total = db.execute(f"SELECT COUNT(*) FROM vk_communities k {where}", args).fetchone()[0]
        rows = db.execute(
            f"SELECT k.*, (SELECT COUNT(*) FROM vk_posts p WHERE p.owner_id=k.owner_id) AS ads_total,"
            f"(SELECT COUNT(DISTINCT l.advertiser_id) FROM vk_posts p JOIN creative_advertiser_links l ON l.creative_id=p.creative_id "
            f" WHERE p.owner_id=k.owner_id) AS advertisers,"
            f"(SELECT MAX(p.posted_at) FROM vk_posts p WHERE p.owner_id=k.owner_id) AS last_post_at "
            f"FROM vk_communities k {where} ORDER BY ads_total DESC, k.members DESC LIMIT ? OFFSET ?", args + [limit, offset]).fetchall()
    items = []
    for r in rows:
        d = dict(r)
        d["url"] = f"https://vk.com/{d['screen_name']}" if d.get("screen_name") else f"https://vk.com/club{abs(d['owner_id'])}"
        items.append(d)
    return {"items": items, "total": total}


class VkMonitorIn(BaseModel):
    monitor: bool


@app.get("/api/v1/vk/communities/{owner_id}")
def api_vk_community(owner_id: int, principal: dict = Depends(public_api_principal)):
    """Карточка паблика: подписчики, частота постов и доля рекламы (по ленте), рекламодатели, реклама по неделям."""
    with closing(connect()) as db:
        k = db.execute("SELECT * FROM vk_communities WHERE owner_id=?", (owner_id,)).fetchone()
        g = db.execute("SELECT * FROM vk_groups WHERE owner_id=?", (owner_id,)).fetchone()
        if not k and not g:
            raise HTTPException(status_code=404, detail="Паблик не найден")
        d = dict(k) if k else {"owner_id": owner_id, "monitor": 0, "feed_posts": 0, "feed_days": 0, "feed_ads": 0,
                               "source": "groups-search", "tracked": False}
        if g:  # данные из поиска сообществ: тематика, город, сайт, описание
            for col in ("name", "screen_name", "members"):
                d[col] = d.get(col) or g[col]
            d.update({col: g[col] for col in ("activity", "city", "verified", "site", "description", "type", "is_closed")})
        st = db.execute("SELECT COUNT(*) AS ads, MIN(posted_at) AS first_post, MAX(posted_at) AS last_post, "
                        "SUM(views) AS views, AVG(views) AS avg_views, AVG(likes) AS avg_likes "
                        "FROM vk_posts WHERE owner_id=?", (owner_id,)).fetchone()
        d.update({k2: st[k2] for k2 in st.keys()})
        since30 = (datetime.now(timezone.utc) - timedelta(days=30)).isoformat()
        d["ads_30d"] = db.execute("SELECT COUNT(*) FROM vk_posts WHERE owner_id=? AND posted_at>=?", (owner_id, since30)).fetchone()[0]
        d["advertisers"] = [dict(r) for r in db.execute(
            "SELECT a.id, a.canonical_name AS name, a.inn, COUNT(*) AS ads, MAX(p.posted_at) AS last_post, "
            "(SELECT COUNT(DISTINCT p2.owner_id) FROM vk_posts p2 JOIN creative_advertiser_links l2 ON l2.creative_id=p2.creative_id "
            " WHERE l2.advertiser_id=a.id) AS communities "
            "FROM vk_posts p JOIN creative_advertiser_links l ON l.creative_id=p.creative_id "
            "JOIN advertisers a ON a.id=l.advertiser_id WHERE p.owner_id=? "
            "GROUP BY a.id ORDER BY ads DESC LIMIT 30", (owner_id,)).fetchall()]
        weeks = db.execute(
            "SELECT strftime('%Y-%W', posted_at) AS w, MIN(substr(posted_at,1,10)) AS day, COUNT(*) AS ads "
            "FROM vk_posts WHERE owner_id=? AND posted_at>=? GROUP BY w ORDER BY w",
            (owner_id, (datetime.now(timezone.utc) - timedelta(weeks=12)).isoformat())).fetchall()
        d["weeks"] = [dict(r) for r in weeks]
    d["posts_per_day"] = round(d["feed_posts"] / d["feed_days"], 1) if d.get("feed_days") else None
    d["ad_share"] = round(d["feed_ads"] / d["feed_posts"], 3) if d.get("feed_posts") else None
    d["url"] = f"https://vk.com/{d['screen_name']}" if d.get("screen_name") else f"https://vk.com/club{abs(owner_id)}"
    return d


@app.post("/api/v1/vk/communities/{owner_id}/monitor")
def api_vk_community_monitor(owner_id: int, payload: VkMonitorIn, principal: dict = Depends(public_api_principal)):
    require_user_principal(principal)
    with closing(connect()) as db:
        n = db.execute("UPDATE vk_communities SET monitor=? WHERE owner_id=?", (1 if payload.monitor else 0, owner_id)).rowcount
        if not n and payload.monitor:  # сообщество из поиска сообществ — ставим на обход стен
            n = db.execute("INSERT INTO vk_communities(owner_id,name,screen_name,members,monitor,added_at,source) "
                           "SELECT owner_id,name,screen_name,members,1,?,'groups-search' FROM vk_groups WHERE owner_id=?",
                           (now_iso(), owner_id)).rowcount
        db.commit()
    if not n:
        raise HTTPException(status_code=404, detail="Сообщество не найдено")
    return {"ok": True, "monitor": payload.monitor}


class VkGroupsSearchIn(BaseModel):
    query: str = Field(min_length=2, max_length=200)
    city: str = Field(default="", max_length=100)


@app.post("/api/v1/vk/groups-search")
def api_vk_groups_search(payload: VkGroupsSearchIn, principal: dict = Depends(public_api_principal)):
    """Поиск сообществ VK по ключу и городу — задание для воркера (браузер с аккаунтом парсера)."""
    uid = require_user_principal(principal)
    city = payload.city.strip() or "Вся Россия"
    with closing(connect()) as db:
        jid = db.execute("INSERT INTO jobs(status,city,key_text,created_at,owner_user_id,variant) VALUES('queued',?,?,?,?,'vk-groups')",
                         (city, payload.query.strip(), now_iso(), uid)).lastrowid
        db.commit()
    return {"id": jid}


@app.get("/api/v1/vk/groups-searches")
def api_vk_groups_searches(limit: int = 30, principal: dict = Depends(public_api_principal)):
    with closing(connect()) as db:
        rows = db.execute(
            "SELECT j.id,j.status,j.key_text,j.city,j.created_at,j.finished_at,j.error_text,"
            "(SELECT COUNT(*) FROM vk_group_results r WHERE r.job_id=j.id) AS groups "
            "FROM jobs j WHERE j.variant='vk-groups' ORDER BY j.id DESC LIMIT ?", (max(1, min(limit, 200)),)).fetchall()
    return {"items": [dict(r) for r in rows]}


@app.get("/api/v1/vk/groups-searches/{job_id}")
def api_vk_groups_search_result(job_id: int, q: str = "", min_members: int = 0, principal: dict = Depends(public_api_principal)):
    """Сообщества из поиска + что мы о них знаем: реклама в них, мониторинг, частота постов."""
    with closing(connect()) as db:
        job = db.execute("SELECT id,status,key_text,city,created_at,finished_at,error_text FROM jobs WHERE id=? AND variant='vk-groups'",
                         (job_id,)).fetchone()
        if not job:
            raise HTTPException(status_code=404, detail="Поиск не найден")
        args: list = [job_id, max(0, min_members)]
        cond = ""
        if q:
            cond = " AND (g.name LIKE ? OR g.activity LIKE ? OR g.description LIKE ?)"
            args += [f"%{q}%"] * 3
        rows = db.execute(
            "SELECT g.*, r.rank, k.monitor, k.last_checked_at, k.feed_posts, k.feed_days, k.feed_ads, "
            "(SELECT COUNT(*) FROM vk_posts p WHERE p.owner_id=g.owner_id) AS ads_total "
            "FROM vk_group_results r JOIN vk_groups g ON g.owner_id=r.owner_id "
            "LEFT JOIN vk_communities k ON k.owner_id=g.owner_id "
            f"WHERE r.job_id=? AND COALESCE(g.members,0)>=?{cond} ORDER BY r.rank LIMIT 1000", args).fetchall()
    items = []
    for r in rows:
        d = dict(r)
        d["url"] = f"https://vk.com/{d['screen_name']}" if d.get("screen_name") else f"https://vk.com/club{abs(d['owner_id'])}"
        d["posts_per_day"] = round(d["feed_posts"] / d["feed_days"], 1) if d.get("feed_days") else None
        items.append(d)
    return {"job": dict(job), "items": items}


@app.get("/api/v1/vk/communities/{owner_id}/creative-ids")
def api_vk_community_creative_ids(owner_id: int, principal: dict = Depends(public_api_principal)):
    with closing(connect()) as db:
        ids = [r[0] for r in db.execute("SELECT creative_id FROM vk_posts WHERE owner_id=? ORDER BY posted_at DESC LIMIT 500", (owner_id,))]
    return {"ids": ids}


@app.post("/api/v1/vk/walls/run")
def api_vk_walls_run(principal: dict = Depends(public_api_principal)):
    """Обойти стены сейчас, не дожидаясь расписания."""
    require_user_principal(principal)
    with closing(connect()) as db:
        busy = db.execute("SELECT id FROM jobs WHERE variant='vk-walls' AND status IN ('queued','running') LIMIT 1").fetchone()
        if busy:
            return {"id": busy[0], "already": True}
        jid = db.execute("INSERT INTO jobs(status,city,key_text,created_at,variant) VALUES('queued','Вся Россия','Мониторинг стен VK',?,'vk-walls')",
                         (now_iso(),)).lastrowid
        db.commit()
    return {"id": jid, "already": False}


@app.get("/api/v1/advertisers")
def api_list_advertisers(q: str = "", intent: str = "", limit: int = 25, offset: int = 0, source: str = "",
                         principal: dict = Depends(public_api_principal)):
    """source: '' — все; vk — есть реклама в VK; yandex — есть в Яндексе; both — и там, и там."""
    limit, offset = _paging(limit, offset)
    with closing(connect()) as db:
        where_parts, params = [], []
        has_vk = ("EXISTS (SELECT 1 FROM creative_advertiser_links lv JOIN creatives cv ON cv.id=lv.creative_id "
                  "WHERE lv.advertiser_id=a.id AND cv.platform='vk.com')")
        has_ya = ("EXISTS (SELECT 1 FROM creative_advertiser_links ly JOIN creatives cy ON cy.id=ly.creative_id "
                  "WHERE ly.advertiser_id=a.id AND cy.platform<>'vk.com')")
        if source == "vk":
            where_parts.append(has_vk)
        elif source == "yandex":
            where_parts.append(has_ya)
        elif source == "both":
            where_parts.append(f"{has_vk} AND {has_ya}")
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
            f"(SELECT COUNT(*) FROM creative_advertiser_links l WHERE l.advertiser_id=a.id) AS creatives_count,"
            f"(SELECT COUNT(*) FROM creative_advertiser_links l JOIN creatives c ON c.id=l.creative_id "
            f" WHERE l.advertiser_id=a.id AND c.platform='vk.com') AS vk_count "
            f"FROM advertisers a {where} ORDER BY a.last_seen_at DESC LIMIT ? OFFSET ?",
            params + [limit, offset],
        ).fetchall()
        items = [
            {"id": r["id"], "inn": r["inn"], "name": _clean_adv_name(r["canonical_name"], r["inn"]), "creatives_count": r["creatives_count"],
             "vk_count": r["vk_count"], "yandex_count": r["creatives_count"] - r["vk_count"],
             "sources": [x for x, n in (("yandex", r["creatives_count"] - r["vk_count"]), ("vk", r["vk_count"])) if n > 0],
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
        reps = [v["representative"] for v in variants]
        _attach_vk(db, reps)
    vk_rows = sum(1 for r in creative_rows if r["platform"] == "vk.com")
    return {
        "advertiser": {"id": advertiser["id"], "inn": advertiser["inn"], "name": _clean_adv_name(advertiser["canonical_name"], advertiser["inn"])},
        "by_source": {"yandex": len(creative_rows) - vk_rows, "vk": vk_rows},
        "variants": variants,
        "total_variants": len(order),
        "total_creative_rows": len(creative_rows),
        "limit": limit, "offset": offset,
    }


def _attach_collections(db: sqlite3.Connection, items: list[dict[str, Any]], principal: dict) -> None:
    """`collections`: подборки текущего пользователя, в которых лежит креатив (ключ группы — content_fingerprint или hash:<creative_hash>)."""
    try:
        user_id = require_user_principal(principal)
    except Exception:
        return
    mp: dict[str, list[dict[str, Any]]] = {}
    for r in db.execute(
        "SELECT i.group_key, c.id, c.name FROM collection_items i JOIN collections c ON c.id=i.collection_id "
        "WHERE c.owner_user_id=? ORDER BY c.name", (user_id,)).fetchall():
        mp.setdefault(r["group_key"], []).append({"id": r["id"], "name": r["name"]})
    for it in items:
        found = list(mp.get(it.get("content_fingerprint") or "", [])) if it.get("content_fingerprint") else list(mp.get("hash:" + str(it.get("creative_hash")), []))
        it["collections"] = found


def _attach_stats(db: sqlite3.Connection, items: list[dict[str, Any]]) -> None:
    """Добавляет `stats`: когда объявление впервые и в последний раз встречено парсером, сколько
    дней оно в открутке и сколько раз встречено. Одно и то же объявление (общий content_fingerprint)
    считается вместе, даже если в базе оно лежит несколькими строками."""
    def _day(value: str | None):
        try:
            return datetime.fromisoformat(str(value).replace("Z", "+00:00")).date()
        except Exception:
            return None
    by_fp: dict[str, dict[str, Any]] = {}
    by_id: dict[int, dict[str, Any]] = {}
    fps = sorted({it["content_fingerprint"] for it in items if it.get("content_fingerprint")})
    ids = [it["id"] for it in items if it.get("id") is not None and not it.get("content_fingerprint")]
    sel = ("MIN(c.captured_at) AS c_first, MAX(c.captured_at) AS c_last, MIN(o.observed_at) AS o_first, "
           "MAX(o.observed_at) AS o_last, COUNT(o.id) AS shows, COUNT(DISTINCT o.job_id) AS runs, "
           "COUNT(DISTINCT o.platform) AS sites, "
           "(SELECT COUNT(DISTINCT j.city) FROM occurrences o2 JOIN jobs j ON j.id=o2.job_id "
           " JOIN creatives c2 ON c2.id=o2.creative_id WHERE {scope}) AS cities")
    for i in range(0, len(fps), 400):
        chunk = fps[i:i + 400]
        for r in db.execute(
            f"SELECT c.content_fingerprint AS k, {sel.format(scope='c2.content_fingerprint=c.content_fingerprint')} FROM creatives c LEFT JOIN occurrences o ON o.creative_id=c.id "
            f"WHERE c.content_fingerprint IN ({','.join('?' * len(chunk))}) GROUP BY c.content_fingerprint", chunk):
            by_fp[r["k"]] = dict(r)
    for i in range(0, len(ids), 400):
        chunk = ids[i:i + 400]
        for r in db.execute(
            f"SELECT c.id AS k, {sel.format(scope='c2.id=c.id')} FROM creatives c LEFT JOIN occurrences o ON o.creative_id=c.id "
            f"WHERE c.id IN ({','.join('?' * len(chunk))}) GROUP BY c.id", chunk):
            by_id[r["k"]] = dict(r)
    for it in items:
        r = by_fp.get(it.get("content_fingerprint")) if it.get("content_fingerprint") else by_id.get(it.get("id"))
        if not r:
            continue
        first = min(v for v in (r["c_first"], r["o_first"]) if v) if (r["c_first"] or r["o_first"]) else None
        last = max(v for v in (r["c_last"], r["o_last"]) if v) if (r["c_last"] or r["o_last"]) else None
        d1, d2 = _day(first), _day(last)
        it["stats"] = {
            "first_seen_at": first, "last_seen_at": last,
            "days_active": ((d2 - d1).days + 1) if d1 and d2 else None,
            "shows": max(int(r["shows"] or 0), 1), "runs": int(r["runs"] or 0),
            "sites": int(r["sites"] or 0), "cities": int(r["cities"] or 0),
        }
    _attach_vk(db, items)


def _attach_vk(db: sqlite3.Connection, items: list[dict[str, Any]]) -> None:
    """Добавляет `vk` VK-креативам: сообщество, дата публикации поста, сколько дней висит, статистика."""
    ids = [it["id"] for it in items if it.get("platform") == "vk.com" and it.get("id") is not None]
    if not ids:
        return
    rows: dict[int, dict[str, Any]] = {}
    try:
        for i in range(0, len(ids), 400):
            chunk = ids[i:i + 400]
            for r in db.execute(f"SELECT * FROM vk_posts WHERE creative_id IN ({','.join('?' * len(chunk))})", chunk):
                rows[r["creative_id"]] = dict(r)
    except sqlite3.OperationalError:
        return
    now = datetime.now(timezone.utc)
    for it in items:
        r = rows.get(it.get("id"))
        if not r:
            continue
        posted = None
        try:
            posted = datetime.fromisoformat(str(r["posted_at"]).replace("Z", "+00:00")) if r["posted_at"] else None
        except Exception:
            pass
        last = None
        try:
            last = datetime.fromisoformat(str(r["last_seen_at"]).replace("Z", "+00:00"))
        except Exception:
            pass
        it["vk"] = {
            "format": r["format"], "post_url": r["post_url"], "posted_at": r["posted_at"] or None,
            "days_since_post": (now - posted).days if posted else None,
            # «висит»: от публикации до последнего раза, когда парсер видел пост в выдаче
            "days_live": max(0, (last - posted).days) if posted and last else None,
            "last_seen_at": r["last_seen_at"],
            "owner": {"id": r["owner_id"], "name": r["owner_name"], "screen_name": r["owner_screen_name"],
                      "members": r["owner_members"],
                      "url": f"https://vk.com/{r['owner_screen_name']}" if r["owner_screen_name"] else
                             (f"https://vk.com/club{abs(r['owner_id'])}" if r["owner_id"] and r["owner_id"] < 0 else "")},
            "views": r["views"], "likes": r["likes"], "reposts": r["reposts"], "comments": r["comments"],
        }


def _attach_intents(db: sqlite3.Connection, items: list[dict[str, Any]], owner_user_id: int | None = None) -> None:
    """Attaches `intents`: the distinct search queries (jobs.key_text) each creative was
    captured under, so the UI can show "this ad belongs to «X»" on every card/detail
    view regardless of whether intent-scoping is currently filtering the list or not."""
    ids = [item["id"] for item in items if item.get("id") is not None]
    if not ids:
        return
    # Чужие запуски (и их ключевые слова) обычному пользователю не показываем: креатив из общей
    # базы он видит, а кто и по какому запросу его нашёл — нет.
    own, own_args = (" AND j.owner_user_id=?", [owner_user_id]) if owner_user_id is not None else ("", [])
    by_id: dict[int, list[str]] = {}
    for chunk in (ids[i:i + 400] for i in range(0, len(ids), 400)):
        placeholders = ",".join(["?"] * len(chunk))
        rows = db.execute(
            f"SELECT DISTINCT o.creative_id, j.key_text FROM occurrences o JOIN jobs j ON j.id=o.job_id "
            f"WHERE o.creative_id IN ({placeholders}) AND j.key_text IS NOT NULL AND j.key_text<>''{own}",
            chunk + own_args,
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
            f"FROM occurrences o JOIN jobs j ON j.id=o.job_id WHERE o.creative_id IN ({placeholders}){own} "
            f"GROUP BY o.creative_id, j.id ORDER BY j.id DESC",
            chunk + own_args,
        ).fetchall():
            runs_by_id.setdefault(row["creative_id"], []).append({
                "job_id": row["job_id"], "key_text": row["key_text"], "city": row["city"],
                "status": row["status"], "seen_at": row["seen_at"],
            })
    ai_by_id: dict[int, Any] = {}
    for chunk in (ids[i:i + 400] for i in range(0, len(ids), 400)):
        placeholders = ",".join(["?"] * len(chunk))
        for row in db.execute(f"SELECT id, ai_intent, ai_intent_main, ai_intent_sub FROM creatives WHERE id IN ({placeholders})", chunk).fetchall():
            ai_by_id[row["id"]] = (row["ai_intent"], row["ai_intent_main"], row["ai_intent_sub"])
    for item in items:
        item["runs"] = runs_by_id.get(item.get("id"), [])[:20]
        ai, ai_main, ai_sub = ai_by_id.get(item.get("id")) or (None, None, None)
        item["ai_intent"] = ai  # None = ещё не определён
        item["ai_intent_main"] = ai_main  # основной интент (раздел таксономии)
        item["ai_intent_sub"] = ai_sub    # под-интент (подрубрика)


# user_id -> (время, интенты, подходящие под ключевые слова последних запусков пользователя); живёт 5 минут
_USER_HISTORY_INTENTS: dict[int, tuple[float, list[str]]] = {}


@app.get("/api/v1/creatives")
def api_list_creatives(
    inn: str = "", platform: str = "", q: str = "", status: str = "", job_id: int | None = None,
    has_advertiser: bool | None = None, has_app: bool | None = None, has_discount: bool | None = None,
    campaign_id: str = "", utm_campaign: str = "", keyword: str = "", intent: str = "", ai_intent: str = "", only_relevant: str = "", history: str = "",
    ai_tree: str = "",
    date_from: str = "", date_to: str = "",
    sort: str = "recent", limit: int = 50, offset: int = 0,
    source: str = "", group: str = "", platform_label: str = "", format: str = "", ids: str = "", exclude_ids: str = "",
    feature: str = "",
    principal: dict = Depends(public_api_principal),
):
    """source — откуда креативы: own (свои запуски), history (подходящие по интенту из общей базы,
    найденные не в своих запусках), all (и то и другое), db (вся база — только администратор).
    group=1 — одно объявление = одна карточка (общий content_fingerprint), постраничность по карточкам."""
    limit, offset = max(1, min(limit, 200)), max(0, offset)
    user_id = principal.get("user_id") if principal.get("kind") == "user" else None
    is_admin = _console.is_platform_admin(principal)
    source = (source or "").strip().lower()
    if source not in ("own", "history", "all", "db"):
        source = ("all" if history in ("1", "yes", "true") else "own") if (job_id is not None or not is_admin) else "db"
    if source == "db" and not is_admin:
        source = "own"
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
        tree_sql, tree_params = _ai_tree_where(ai_tree)
        if tree_sql:
            where_parts.append(tree_sql)
            params.extend(tree_params)
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
        # feature — признак из разметки объявления (creative_details), можно несколько через запятую
        _feature_sql = {
            "promo": "COALESCE(cd.promo_text,'')<>''", "social": "cd.is_social=1", "warning": "COALESCE(cd.warning,'')<>''",
            "age": "COALESCE(cd.age_label,'')<>''", "old_price": "COALESCE(cd.price_old,'')<>''",
            "offers": "COALESCE(cd.offers_json,'')<>''", "callouts": "COALESCE(cd.callouts_json,'')<>''",
            "gallery": "cd.ad_format='gallery'", "app": "cd.ad_format='app'", "ad_id": "COALESCE(cd.ad_id,'')<>''",
        }
        _features = [f for f in (feature or "").split(",") if f.strip() in _feature_sql]
        if _features:
            joins.append("JOIN creative_details cd ON cd.creative_id=c.id")
            where_parts.extend(_feature_sql[f.strip()] for f in _features)
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
        history_intents: list[str] = []
        jrow = None
        if job_id is not None:
            jrow = db.execute("SELECT key_text,owner_user_id FROM jobs WHERE id=?", (job_id,)).fetchone()
            if jrow and not is_admin and jrow["owner_user_id"] != user_id:
                raise HTTPException(status_code=404, detail="Запуск не найден")
            own_sql, own_params = "c.id IN (SELECT creative_id FROM occurrences WHERE job_id=?)", [job_id]
            if jrow:
                job_rel = _job_relevance(db, job_id, jrow["key_text"])["ids"]
                history_intents = _history_intents(db, jrow["key_text"])
        else:
            own_sql = "c.id IN (SELECT o.creative_id FROM occurrences o JOIN jobs j ON j.id=o.job_id WHERE j.owner_user_id=?)"
            own_params = [user_id if user_id is not None else -1]
            if source != "db" and user_id is not None:
                cached = _USER_HISTORY_INTENTS.get(user_id)
                if cached and time.time() - cached[0] < 300:
                    history_intents = cached[1]
                else:
                    seen_i: set[str] = set()
                    for kr in db.execute("SELECT key_text, MAX(id) m FROM jobs WHERE owner_user_id=? AND key_text<>'' GROUP BY PYLOWER(key_text) ORDER BY m DESC LIMIT 20", (user_id,)).fetchall():
                        seen_i.update(_history_intents(db, kr["key_text"]))
                    history_intents = sorted(seen_i)[:600]
                    _USER_HISTORY_INTENTS[user_id] = (time.time(), history_intents)
        hist_sql = f"PYLOWER(c.ai_intent) IN ({','.join(['?'] * len(history_intents))})" if history_intents else "0"

        def _source_clause(src: str) -> tuple[str, list[Any]]:
            """Условие «откуда креативы» + фильтр «по интенту / не по интенту» для своего запуска."""
            if src == "db":
                return "", []
            rel_sql, rel_params = "", []
            if job_rel is not None and only_relevant in ("1", "yes", "true", "no"):
                want = only_relevant != "no"
                keep = [i for i, v in job_rel.items() if v is want] or [-1]
                rel_sql, rel_params = f" AND c.id IN ({','.join(['?'] * len(keep))})", keep
            own_part, own_p = f"({own_sql}{rel_sql})", own_params + rel_params
            hist_part, hist_p = f"({hist_sql} AND NOT {own_sql})", list(history_intents) + own_params
            if only_relevant == "no":  # исторические подобраны по интенту — среди «не по интенту» их нет
                hist_part, hist_p = "0", []
            if src == "own":
                return own_part, own_p
            if src == "history":
                return hist_part, hist_p
            return f"({own_part} OR {hist_part})", own_p + hist_p

        if platform_label:
            prefix = {"VK Ads": "vk-native", "Google Ads": "google-ads", "Taboola": "taboola", "Яндекс Дзен": "native-feed",
                      "VK · Реклама от автора": "vk-search", "VK · лента": "vk-feed"}
            ors, has_rsya = [], False
            for lab in [x.strip() for x in platform_label.split(",") if x.strip()]:
                if lab in prefix:
                    ors.append("COALESCE(c.detection_method,'') LIKE ?")
                    params.append(prefix[lab] + "%")
                else:
                    has_rsya = True
            if has_rsya:
                ors.append("(" + " AND ".join(["COALESCE(c.detection_method,'') NOT LIKE ?"] * len(prefix)) + ")")
                params.extend(v + "%" for v in prefix.values())
            if ors:
                where_parts.append("(" + " OR ".join(ors) + ")")
        if format:
            video = "(c.creative_type='video' OR COALESCE(c.video_src,'')<>'')"
            fsql = {"video": video, "carousel": f"(NOT {video} AND c.carousel_id IS NOT NULL)",
                    "image": f"(NOT {video} AND c.carousel_id IS NULL AND COALESCE(c.image_url,'')<>'')",
                    "native": f"(NOT {video} AND c.carousel_id IS NULL AND COALESCE(c.image_url,'')='')"}
            fors = [fsql[x] for x in format.split(",") if x in fsql]
            if fors:
                where_parts.append("(" + " OR ".join(fors) + ")")
        for raw, neg in ((ids, False), (exclude_ids, True)):
            if raw:
                id_list = [int(x) for x in raw.split(",") if x.strip().isdigit()][:2000]
                if id_list or not neg:
                    # избранное хранится по id карточки, а карточка — это всё объявление целиком (общий отпечаток)
                    qm = ",".join(["?"] * len(id_list)) or "-1"
                    where_parts.append(
                        f"{'NOT ' if neg else ''}(c.id IN ({qm}) OR (c.content_fingerprint<>'' AND c.content_fingerprint IN "
                        f"(SELECT content_fingerprint FROM creatives WHERE id IN ({qm}))))")
                    params.extend(id_list + id_list)
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
            params.append(date_to + "T23:59:59" if len(date_to) == 10 else date_to)  # дата без времени = весь этот день
        join_sql = " ".join(joins)
        grouped = group in ("1", "yes", "true")
        gk = "COALESCE(NULLIF(c.content_fingerprint,''),'hash:'||c.creative_hash)"

        def _where(src: str) -> tuple[str, list[Any]]:
            sc, sp = _source_clause(src)
            parts = where_parts + ([sc] if sc else [])
            return (("WHERE " + " AND ".join(parts)) if parts else ""), params + sp

        def _count(src: str) -> int:
            w, p = _where(src)
            what = f"COUNT(DISTINCT {gk})" if grouped else "COUNT(DISTINCT c.id)"
            return db.execute(f"SELECT {what} FROM creatives c {join_sql} {w}", p).fetchone()[0]

        where, wparams = _where(source)
        total = _count(source)
        if grouped:
            order_sql = {
                "recent": "captured_at DESC, id DESC", "oldest": "captured_at ASC, id ASC",
                "domain": "domain ASC, captured_at DESC", "discount": "discount_percent DESC, captured_at DESC",
            }.get(sort, "captured_at DESC, id DESC")
            id_rows = db.execute(
                f"WITH f AS (SELECT DISTINCT c.id, c.captured_at, c.domain, c.discount_percent, {gk} AS gk FROM creatives c {join_sql} {where}), "
                "r AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY gk ORDER BY captured_at DESC, id DESC) AS rn FROM f) "
                f"SELECT id FROM r WHERE rn=1 ORDER BY {order_sql} LIMIT ? OFFSET ?",
                wparams + [limit, offset],
            ).fetchall()
            page_ids = [r["id"] for r in id_rows]
            by_id = {r["id"]: r for r in db.execute(
                f"SELECT * FROM creatives WHERE id IN ({','.join(['?'] * len(page_ids)) or '-1'})", page_ids).fetchall()}
            rows = [by_id[i] for i in page_ids if i in by_id]
        else:
            order_sql = {
                "recent": "c.captured_at DESC",
                "oldest": "c.captured_at ASC",
                "domain": "c.domain ASC, c.captured_at DESC",
                "discount": "c.discount_percent DESC, c.captured_at DESC",
            }.get(sort, "c.captured_at DESC")
            rows = db.execute(
                f"SELECT DISTINCT c.* FROM creatives c {join_sql} {where} ORDER BY {order_sql} LIMIT ? OFFSET ?",
                wparams + [limit, offset],
            ).fetchall()
        items = [_creative_row_to_dict(r) for r in rows]
        _attach_intents(db, items, None if is_admin else user_id)
        _attach_stats(db, items)
        _attach_collections(db, items, principal)
        if grouped and items:
            fps = [it["content_fingerprint"] for it in items if it.get("content_fingerprint")]
            plats: dict[str, set[str]] = {}
            for r in db.execute(
                f"SELECT content_fingerprint AS fp, detection_method AS dm FROM creatives WHERE content_fingerprint IN ({','.join(['?'] * len(fps)) or "''"}) "
                "GROUP BY content_fingerprint, detection_method", fps).fetchall():
                plats.setdefault(r["fp"], set()).add(r["dm"] or "")
            for it in items:
                it["group_methods"] = sorted(plats.get(it.get("content_fingerprint") or "", set()))
        if source != "db" and items:
            item_ids = [it["id"] for it in items]
            own_ids = {r["id"] for r in db.execute(
                f"SELECT c.id FROM creatives c WHERE c.id IN ({','.join(['?'] * len(item_ids))}) AND {own_sql}", item_ids + own_params).fetchall()}
            for it in items:
                it["from_history"] = it["id"] not in own_ids  # найден не в своих запусках, а подобран по интенту из общей базы
                if job_rel is not None:
                    it["job_match"] = True if it["from_history"] else job_rel.get(it["id"])  # True — по теме, False — нет, None — определяется
        counts = None
        if source != "db":
            counts = {"own": _count("own") if source != "own" else total,
                      "history": _count("history") if source != "history" else total}
            counts["all"] = total if source == "all" else _count("all")
            if job_rel is not None:
                vals = list(job_rel.values())
                counts["job"] = {"found": len(vals), "relevant": sum(1 for v in vals if v is True),
                                 "irrelevant": sum(1 for v in vals if v is False), "pending": sum(1 for v in vals if v is None)}
        job_info = None
        if job_id is not None and jrow:
            jfull = db.execute("SELECT id,key_text,city,status,created_at FROM jobs WHERE id=?", (job_id,)).fetchone()
            job_info = dict(jfull) if jfull else None
    return {"items": items, "total": total, "limit": limit, "offset": offset, "source": source, "counts": counts,
            "is_admin": is_admin, "job": job_info}


@app.get("/api/v1/platforms")
def api_list_platforms(principal: dict = Depends(public_api_principal)):
    with closing(connect()) as db:
        rows = db.execute("SELECT platform, COUNT(*) n FROM creatives GROUP BY platform ORDER BY n DESC").fetchall()
    return {"items": [{"platform": r["platform"], "creatives_count": r["n"]} for r in rows]}


def _ai_tree_where(ai_tree: str) -> tuple[str, list[Any]]:
    """ai_tree — выбор в дереве фильтра: элементы через «~», уровни через «>» (раздел>подрубрика>интент)
    («Юристы~Хозяйство и уборка>Химчистка одежды»). Элемент без «>» — весь раздел. Внутри — ИЛИ."""
    parts, params = [], []
    for item in (ai_tree or "").split("~"):
        main, _, rest = item.partition(">")
        sub, _, intent = rest.partition(">")
        main, sub, intent = main.strip(), sub.strip(), intent.strip().lower()
        if not main:
            continue
        if sub and intent:  # третий уровень — конкретный интент Gemini внутри подрубрики
            parts.append("(c.ai_intent_main=? AND c.ai_intent_sub=? AND PYLOWER(c.ai_intent)=?)")
            params += [main, sub, intent]
        elif sub:
            parts.append("(c.ai_intent_main=? AND c.ai_intent_sub=?)")
            params += [main, sub]
        else:
            parts.append("c.ai_intent_main=?")
            params.append(main)
    return (("(" + " OR ".join(parts) + ")") if parts else ""), params


@app.get("/api/v1/ai-intent-tree")
def api_ai_intent_tree(job_id: int | None = None, intents: int = 1, principal: dict = Depends(public_api_principal)):
    """Дерево для фильтра: основной интент (раздел) → под-интент (подрубрика) → интенты Gemini, с числом креативов.
    С job_id — только креативы этого запуска. Пустые разделы не возвращаются (полный список — /api/v1/ai-taxonomy)."""
    require_user_principal(principal)
    scope = " AND c.id IN (SELECT creative_id FROM occurrences WHERE job_id=?)" if job_id is not None else ""
    args: list[Any] = [job_id] if job_id is not None else []
    with closing(connect()) as db:
        rows = db.execute(
            "SELECT c.ai_intent_main AS m, c.ai_intent_sub AS s, PYLOWER(c.ai_intent) AS i, COUNT(*) AS n FROM creatives c "
            "WHERE c.ai_intent_main IS NOT NULL" + scope + " GROUP BY 1,2,3", args).fetchall()
        pending = db.execute("SELECT COUNT(*) FROM creatives c WHERE c.ai_intent_main IS NULL" + scope, args).fetchone()[0]
    order = {r["main"]: k for k, r in enumerate(taxonomy.taxonomy())}
    tree: dict[str, dict] = {}
    for r in rows:
        mn = tree.setdefault(r["m"], {"main": r["m"], "count": 0, "subs": {}})
        mn["count"] += r["n"]
        sb = mn["subs"].setdefault(r["s"], {"sub": r["s"], "count": 0, "intents": {}})
        sb["count"] += r["n"]
        if intents and r["i"]:
            sb["intents"][r["i"]] = sb["intents"].get(r["i"], 0) + r["n"]
    items = []
    for mn in sorted(tree.values(), key=lambda x: (x["main"] == "Не определено", -x["count"], order.get(x["main"], 999))):
        subs = sorted(mn["subs"].values(), key=lambda x: (x["sub"] == "Другое", -x["count"]))
        for sb in subs:
            sb["intents"] = [{"intent": k, "count": v} for k, v in sorted(sb["intents"].items(), key=lambda kv: -kv[1])][:50]
        items.append({"main": mn["main"], "count": mn["count"], "subs": subs})
    return {"items": items, "pending": pending, "job_id": job_id}


@app.get("/api/v1/ai-taxonomy")
def api_ai_taxonomy(principal: dict = Depends(public_api_principal)):
    """Полная таксономия интентов (Яндекс Услуги + Яндекс Маркет + доп. разделы)."""
    require_user_principal(principal)
    return {"items": taxonomy.taxonomy()}


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


def _history_intents(db: sqlite3.Connection, key_text: str) -> list[str]:
    """Интенты креативов (в нижнем регистре), подходящие под ключевое слово: совпала основа слова
    либо Gemini признал пару «ключевое слово → интент» подходящей. По ним подбираем исторические
    креативы из других запусков для показа вместе с результатами текущего."""
    kw_stems = _intent_stems(key_text)
    matched = {r["intent_key"] for r in db.execute(
        "SELECT intent_key FROM intent_keyword_match WHERE keyword_key=? AND match=1", (_pylower(key_text),)).fetchall()}
    out = set(matched)
    for r in db.execute("SELECT DISTINCT ai_intent FROM creatives WHERE ai_intent IS NOT NULL AND ai_intent<>?", (aii.UNKNOWN,)).fetchall():
        intent = r["ai_intent"]
        if kw_stems & _intent_stems(intent):
            out.add(_pylower(intent))
    return sorted(out)[:400]


def _landing_url_key(url: str) -> tuple[str, str]:
    """(ключ страницы, домен) — так же, как в landing_parser.py: без www/m, без параметров."""
    try:
        parts = urlsplit(url)
        host = re.sub(r"^(www|m)\.", "", (parts.hostname or "").lower())
        return f"{host}{(parts.path or '/').rstrip('/') or '/'}"[:500], host
    except Exception:
        return "", ""


def _norm_legal_name(name: str) -> str:
    text = re.sub(r"[«»\"“”„'‘’\.,]", " ", (name or "").lower())
    text = re.sub(r"\b(ооо|оао|зао|пао|нао|ао|ип|общество с ограниченной ответственностью|акционерное общество|"
                  r"публичное акционерное общество|индивидуальный предприниматель)\b", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def _landing_block(db, utm: dict | None, advertisers: list[dict]) -> dict | None:
    """Блок «Посадочная страница и юрлицо» для карточки креатива: что на сайте, чей это сайт и
    совпадает ли владелец сайта с рекламодателем из маркировки объявления.
    Данные собирает landing_parser.py (таблицы landing_pages / landing_legal)."""
    if not utm or not (utm.get("final_url") or "").startswith("http"):
        return None
    key, domain = _landing_url_key(utm["final_url"])
    try:
        page = db.execute("SELECT * FROM landing_pages WHERE url_key=?", (key,)).fetchone()
        if not page:  # эту страницу не разбирали — берём любую страницу того же сайта (название сайта общее)
            page = db.execute("SELECT * FROM landing_pages WHERE domain=? AND status='ok' ORDER BY LENGTH(url_key) LIMIT 1", (domain,)).fetchone()
            same_page = False
        else:
            same_page = True
        legal = db.execute("SELECT * FROM landing_legal WHERE domain=?", (domain,)).fetchone()
    except sqlite3.OperationalError:
        return None
    if not page and not legal:
        return {"domain": domain, "status": "pending"}
    out: dict[str, Any] = {
        "domain": domain, "status": (page["status"] if page else "pending"),
        "site_name": (page["site_name"] if page else "") or "",
        "page_title": (page["page_title"] if page and same_page else "") or "",
        "h1": (page["h1"] if page and same_page else "") or "",
        "http_status": page["http_status"] if page else None,
        "fetched_at": (page["fetched_at"] if page else None),
        "domain_mismatch": bool(utm.get("landing_domain_mismatch")),
        "advertised_site_domain": utm.get("advertised_site_domain") or "",
        "legal": None, "compare": {"status": "no_data", "text": "Нет данных для сравнения"},
    }
    site_inn = ""
    if legal:
        out["legal"] = {
            "status": legal["status"], "name": legal["legal_name"] or "", "inn": legal["inn"] or "",
            "ogrn": legal["ogrn"] or "", "kpp": legal["kpp"] or "", "source_url": legal["source_url"] or "",
            "source_kind": legal["source_kind"] or "", "confidence": legal["confidence"] or "",
            "other_inns": [x for x in (legal["other_inns"] or "").split(",") if x],
        }
        site_inn = legal["inn"] or ""
    ad = [a for a in advertisers if (a.get("inn") or "").strip()]
    ad_inns = {a["inn"].strip() for a in ad}
    if legal and legal["status"] == "platform":
        out["compare"] = {"status": "platform", "text": "Реклама ведёт на стороннюю площадку (маркетплейс, магазин приложений, соцсеть) — реквизиты сайта не сравниваются"}
    elif site_inn and ad_inns:
        others = set(out["legal"]["other_inns"])
        if site_inn in ad_inns:
            out["compare"] = {"status": "match", "text": "ИНН на сайте совпадает с ИНН рекламодателя из объявления"}
        elif ad_inns & others:
            out["compare"] = {"status": "partial", "text": "ИНН рекламодателя есть на сайте, но основным юрлицом сайта указано другое"}
        else:
            ad_names = {_norm_legal_name(a.get("canonical_name") or "") for a in ad}
            same_name = _norm_legal_name(out["legal"]["name"]) in ad_names and bool(_norm_legal_name(out["legal"]["name"]))
            out["compare"] = {"status": "mismatch", "text": (
                "ИНН на сайте и в объявлении разные" + (", хотя название юрлица совпадает" if same_name else "")
                + ": рекламу размещает одно юрлицо, а сайт принадлежит другому (агентство, партнёр, группа компаний или подмена)")}
    elif site_inn and not ad_inns:
        out["compare"] = {"status": "site_only", "text": "В объявлении рекламодатель не раскрыт — юрлицо известно только по сайту"}
    elif ad_inns and legal and legal["status"] == "not_found":
        out["compare"] = {"status": "ad_only", "text": "На сайте реквизиты не найдены — юрлицо известно только из маркировки объявления"}
    return out


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
        _attach_intents(db, [creative_dict], principal.get("user_id") if not _console.is_platform_admin(principal) else None)
        _attach_stats(db, [creative_dict])
        landing = _landing_block(db, dict(utm_row) if utm_row else None, [dict(r) for r in advertisers])
        details = _creative_details(db, creative_id)
        try:
            impressions = _creative_impressions(db, str(row["creative_hash"] or ""))
        except Exception:
            impressions = None
    return {
        "creative": creative_dict,
        "details": details,
        "impressions": impressions,
        "landing": landing,
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
def api_change_password(payload: ChangePasswordIn, principal: dict = Depends(public_api_principal),
                        authorization: str | None = Header(default=None)):
    user_id = require_user_principal(principal)
    with closing(connect()) as db:
        row = db.execute("SELECT password_hash FROM users WHERE id=?", (user_id,)).fetchone()
        if not row or not verify_password(payload.current_password, row["password_hash"]):
            raise HTTPException(status_code=401, detail="Текущий пароль указан неверно")
        db.execute("UPDATE users SET password_hash=? WHERE id=?", (hash_password(payload.new_password), user_id))
        # после смены пароля остальные входы закрываем: если пароль утёк, старые сессии не должны жить
        revoked = db.execute("DELETE FROM user_tokens WHERE user_id=? AND token_hash<>?",
                             (user_id, _current_token_hash(authorization))).rowcount or 0
        db.commit()
    return {"ok": True, "revoked_sessions": revoked}


def _current_token_hash(authorization: str | None) -> str:
    if authorization and authorization.startswith("Bearer "):
        return hashlib.sha256(authorization[7:].strip().encode("utf-8")).hexdigest()
    return ""


@app.get("/api/v1/profile/sessions")
def api_profile_sessions(principal: dict = Depends(public_api_principal), authorization: str | None = Header(default=None)):
    """Действующие входы пользователя. Устройство не записывается — только когда вошли и до какого срока."""
    user_id = require_user_principal(principal)
    cur = _current_token_hash(authorization)
    with closing(connect()) as db:
        rows = db.execute("SELECT token_hash,created_at,expires_at FROM user_tokens WHERE user_id=? AND expires_at>=? "
                          "ORDER BY created_at DESC", (user_id, now_iso())).fetchall()
    return {"items": [{"id": r["token_hash"][:16], "created_at": r["created_at"], "expires_at": r["expires_at"],
                       "current": r["token_hash"] == cur} for r in rows]}


@app.post("/api/v1/auth/logout-others")
def api_logout_others(principal: dict = Depends(public_api_principal), authorization: str | None = Header(default=None)):
    user_id = require_user_principal(principal)
    with closing(connect()) as db:
        n = db.execute("DELETE FROM user_tokens WHERE user_id=? AND token_hash<>?",
                       (user_id, _current_token_hash(authorization))).rowcount or 0
        db.commit()
    return {"ok": True, "revoked": n}


@app.delete("/api/v1/profile/sessions/{session_id}")
def api_revoke_session(session_id: str, principal: dict = Depends(public_api_principal)):
    user_id = require_user_principal(principal)
    if not re.fullmatch(r"[0-9a-f]{16}", session_id or ""):
        raise HTTPException(status_code=400, detail="Некорректный идентификатор входа")
    with closing(connect()) as db:
        db.execute("DELETE FROM user_tokens WHERE user_id=? AND token_hash LIKE ?", (user_id, session_id + "%"))
        db.commit()
    return {"ok": True}


@app.get("/api/v1/profile")
def api_get_profile(principal: dict = Depends(public_api_principal)):
    user_id = require_user_principal(principal)
    with closing(connect()) as db:
        row = db.execute(
            "SELECT email,role,name,avatar_data_url,notification_prefs,created_at,last_login_at FROM users WHERE id=?", (user_id,)
        ).fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Пользователь не найден")
        one = lambda q: db.execute(q, (user_id,)).fetchone()[0] or 0
        stats = {
            "runs": one("SELECT COUNT(*) FROM jobs WHERE owner_user_id=?"),
            "creatives": one("SELECT COALESCE(SUM(total_creatives),0) FROM jobs WHERE owner_user_id=?"),
            "collections": one("SELECT COUNT(*) FROM collections WHERE owner_user_id=?"),
            "schedules": one("SELECT COUNT(*) FROM scan_schedules WHERE owner_user_id=? AND status IN ('active','paused')"),
        }
    prefs = {}
    if row["notification_prefs"]:
        try:
            prefs = json.loads(row["notification_prefs"])
        except (TypeError, ValueError):
            prefs = {}
    return {
        "email": row["email"], "role": row["role"], "name": row["name"] or "",
        "avatar_data_url": row["avatar_data_url"] or "", "notification_prefs": prefs,
        "created_at": row["created_at"], "last_login_at": row["last_login_at"], "stats": stats,
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
        variant = payload.source if payload.source != "yandex" else None
        cur = db.execute(
            "INSERT INTO jobs(status,city,key_text,created_at,owner_user_id,variant) VALUES('queued',?,?,?,?,?)",
            (city, key_text, now_iso(), user_id, variant),
        )
        job_id = cur.lastrowid
        db.commit()
    return {"id": job_id, "status": "queued", "city": city, "key_text": key_text, "source": payload.source}


class ScheduleCreateIn(BaseModel):
    city: str = Field(min_length=1, max_length=100)
    key_text: str = Field(min_length=1, max_length=300)
    runs_per_day: int = Field(ge=1, le=24)
    duration_days: int
    source: str = Field(default="yandex", pattern="^(yandex|vk|vk\\+feed)$")


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
            "INSERT INTO scan_schedules(owner_user_id,city,key_text,runs_per_day,duration_days,created_at,end_at,next_run_at,source) "
            "VALUES(?,?,?,?,?,?,?,?,?)",
            (user_id, city, key_text, payload.runs_per_day, payload.duration_days, now.isoformat(),
             (now + timedelta(days=payload.duration_days)).isoformat(), now.isoformat(), payload.source),
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
            "SELECT id,status,city,key_text,created_at,started_at,finished_at,total_creatives,error_text,schedule_id,attempts,retry_at,variant "
            f"FROM jobs WHERE {flt} ORDER BY id DESC LIMIT ? OFFSET ?",
            (*args, limit, offset),
        ).fetchall()
        items = [dict(r) for r in rows]
        for it in items:
            it["source"] = _job_source(it.pop("variant", None))
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
        "source": _job_source(job["variant"] if "variant" in job.keys() else None),
        "relevance": relevance,
        "sites": [dict(r) for r in sites],
    }


@app.get("/api/v1/collections")
def api_list_collections(principal: dict = Depends(public_api_principal)):
    user_id = require_user_principal(principal)
    with closing(connect()) as db:
        rows = db.execute(
            "SELECT c.id,c.name,c.description,c.created_at,c.parent_id,c.topic_main,c.topic_sub,"
            "(SELECT COUNT(*) FROM collection_items i WHERE i.collection_id=c.id) AS own_count,"
            "(SELECT COUNT(*) FROM collections ch WHERE ch.parent_id=c.id) AS children_count "
            "FROM collections c WHERE c.owner_user_id=? ORDER BY c.created_at DESC",
            (user_id,),
        ).fetchall()
        items = [dict(r) for r in rows]
        for it in items:
            # у папки темы («Юристы») объявления лежат во вложенных папках — считаем и показываем их тоже
            ids = [it["id"]] + [r["id"] for r in db.execute("SELECT id FROM collections WHERE parent_id=?", (it["id"],))]
            ph = ",".join("?" * len(ids))
            it["items_count"] = db.execute(
                f"SELECT COUNT(DISTINCT group_key) FROM collection_items WHERE collection_id IN ({ph})", ids).fetchone()[0]
            sub = db.execute(
                "SELECT c.image_url, i.added_at FROM collection_items i JOIN creatives c ON c.id=i.representative_creative_id "
                f"WHERE i.collection_id IN ({ph}) ORDER BY i.added_at DESC LIMIT 12", ids).fetchall()
            it["updated_at"] = sub[0]["added_at"] if sub else it["created_at"]
            it["thumbs"] = [r["image_url"] for r in sub if r["image_url"]][:4]
    return {"items": items}


def _collection_tree_ids(db, collection_id: int) -> list[int]:
    """Подборка и все вложенные в неё папки (для удаления)."""
    out, todo = [], [collection_id]
    while todo:
        cid = todo.pop()
        out.append(cid)
        todo += [r["id"] for r in db.execute("SELECT id FROM collections WHERE parent_id=?", (cid,))]
    return out


class TopicFolderIn(BaseModel):
    # «Юристы» — папка раздела с папками подрубрик внутри; «Юристы>Семейные споры» — одна папка подрубрики
    path: str = Field(min_length=1, max_length=400)
    # текущие фильтры библиотеки — папка собирается из тех же объявлений, что видны на экране
    filters: dict[str, Any] = Field(default_factory=dict)


_TOPIC_FILTER_KEYS = {"inn", "platform", "q", "status", "job_id", "has_advertiser", "has_app", "has_discount",
                      "campaign_id", "utm_campaign", "keyword", "intent", "ai_intent", "only_relevant", "history",
                      "date_from", "date_to", "source", "platform_label", "format", "ids", "exclude_ids", "feature"}
TOPIC_FOLDER_MAX = 5000


@app.post("/api/v1/collections/from-topic")
def api_collection_from_topic(payload: TopicFolderIn, principal: dict = Depends(public_api_principal)):
    """Папка-подборка из темы таксономии с текущими объявлениями. Повторный вызов для той же темы не
    создаёт дубль, а дополняет существующие папки новыми объявлениями. Раздел → папка раздела + папки
    подрубрик внутри; подрубрика → её папка внутри папки раздела (папка раздела создаётся при отсутствии)."""
    user_id = require_user_principal(principal)
    main, _, sub = payload.path.partition(">")
    main, sub = main.strip(), sub.split(">")[0].strip()
    pairs = taxonomy.valid_pairs()
    if main not in pairs or (sub and sub not in pairs[main]):
        raise HTTPException(status_code=422, detail="Такой темы нет в таксономии")
    flt = {k: v for k, v in (payload.filters or {}).items() if k in _TOPIC_FILTER_KEYS and v not in (None, "")}
    if "job_id" in flt:
        flt["job_id"] = int(flt["job_id"])
    for k in ("has_advertiser", "has_app", "has_discount"):
        if k in flt:
            flt[k] = str(flt[k]).lower() in ("1", "true", "yes")
    node = f"{main}>{sub}" if sub else main
    found: dict[str, tuple[int, str]] = {}  # group_key -> (creative_id, подрубрика)
    offset = 0
    while len(found) < TOPIC_FOLDER_MAX:
        page = api_list_creatives(**flt, ai_tree=node, group="1", limit=200, offset=offset, principal=principal)
        rows = page.get("items") or []
        for it in rows:
            key = it.get("content_fingerprint") or f"hash:{it.get('creative_hash')}"
            if key not in found:
                found[key] = (int(it["id"]), it.get("ai_intent_sub") or "Другое")
        offset += 200
        if len(rows) < 200 or offset >= (page.get("total") or 0):
            break
    if not found:
        raise HTTPException(status_code=404, detail="По этой теме с текущими фильтрами объявлений нет")

    stamp = now_iso()
    with closing(connect()) as db:
        def folder(name: str, t_main: str, t_sub: str | None, parent: int | None) -> tuple[int, bool]:
            row = db.execute("SELECT id FROM collections WHERE owner_user_id=? AND topic_main=? AND "
                             "COALESCE(topic_sub,'')=? AND COALESCE(parent_id,0)=?",
                             (user_id, t_main, t_sub or "", parent or 0)).fetchone()
            if row:
                return int(row["id"]), False
            cur = db.execute(
                "INSERT INTO collections(owner_user_id,name,description,created_at,parent_id,topic_main,topic_sub) VALUES(?,?,?,?,?,?,?)",
                (user_id, name, ("Тема «" + t_main + (" › " + t_sub if t_sub else "") + "» — папка создана из фильтра темы"),
                 stamp, parent, t_main, t_sub))
            return int(cur.lastrowid), True

        root_id, root_new = folder(main, main, None, None)
        by_sub: dict[str, list[tuple[str, int]]] = {}
        for key, (cid, s_name) in found.items():
            by_sub.setdefault(sub or s_name, []).append((key, cid))
        created, added = (1 if root_new else 0), 0
        subs_out = []
        for s_name, members in sorted(by_sub.items(), key=lambda kv: (kv[0] == "Другое", -len(kv[1]))):
            fid, is_new = folder(s_name, main, s_name, root_id)
            created += 1 if is_new else 0
            n = 0
            for key, cid in members:
                n += db.execute(
                    "INSERT INTO collection_items(collection_id,group_key,representative_creative_id,added_at) VALUES(?,?,?,?) "
                    "ON CONFLICT(collection_id,group_key) DO NOTHING", (fid, key, cid, stamp)).rowcount or 0
            added += n
            subs_out.append({"id": fid, "name": s_name, "items": len(members), "added": n, "new": is_new})
        create_notification(db, user_id, "collection_created",
                            f"Папка «{main if not sub else sub}» из темы: +{added} объявлений", "", "intel-collections")
        db.commit()
    focus = subs_out[0]["id"] if sub else root_id
    return {"root_id": root_id, "focus_id": focus, "folders": subs_out, "folders_created": created,
            "added": added, "found": len(found), "truncated": len(found) >= TOPIC_FOLDER_MAX}


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
        for cid in _collection_tree_ids(db, collection_id):  # вместе с вложенными папками
            db.execute("DELETE FROM collection_items WHERE collection_id=?", (cid,))
            db.execute("DELETE FROM collections WHERE id=?", (cid,))
        db.commit()
    return {"ok": True}


@app.get("/api/v1/collections/{collection_id}/items")
def api_list_collection_items(collection_id: int, deep: int = 0, principal: dict = Depends(public_api_principal)):
    """deep=1 — объявления папки вместе со всеми вложенными папками (папка темы «Юристы» + подрубрики), без повторов."""
    user_id = require_user_principal(principal)
    with closing(connect()) as db:
        _require_own_collection(db, collection_id, user_id)
        ids = _collection_tree_ids(db, collection_id) if deep else [collection_id]
        ph = ",".join("?" * len(ids))
        rows = db.execute(
            "SELECT i.group_key, i.added_at, i.collection_id AS folder_id, c.* FROM collection_items i "
            "JOIN creatives c ON c.id=i.representative_creative_id "
            f"WHERE i.collection_id IN ({ph}) ORDER BY i.added_at DESC",
            ids,
        ).fetchall()
    items, seen = [], set()
    for r in rows:
        if r["group_key"] in seen:
            continue
        seen.add(r["group_key"])
        d = _creative_row_to_dict(r)
        d["folder_id"] = r["folder_id"]
        d["group_key"] = r["group_key"]
        d["added_at"] = r["added_at"]
        items.append(d)
    with closing(connect()) as db:
        _attach_stats(db, items)
        _attach_intents(db, items, None if _console.is_platform_admin(principal) else user_id)
    return {"items": items}


class CollectionBulkIn(BaseModel):
    items: list[CollectionItemIn] = Field(default_factory=list, max_length=500)
    group_keys: list[str] = Field(default_factory=list, max_length=500)
    ids: list[int] = Field(default_factory=list, max_length=200)


@app.patch("/api/v1/collections/{collection_id}")
def api_update_collection(collection_id: int, payload: CollectionCreateIn, principal: dict = Depends(public_api_principal)):
    user_id = require_user_principal(principal)
    with closing(connect()) as db:
        _require_own_collection(db, collection_id, user_id)
        db.execute("UPDATE collections SET name=?, description=? WHERE id=?",
                   (payload.name.strip(), payload.description.strip(), collection_id))
        db.commit()
    return {"ok": True}


@app.post("/api/v1/collections/{collection_id}/items/bulk")
def api_add_collection_items(collection_id: int, payload: CollectionBulkIn, principal: dict = Depends(public_api_principal)):
    """Добавляет сразу несколько объявлений; уже лежащие в подборке пропускаются."""
    user_id = require_user_principal(principal)
    added = 0
    with closing(connect()) as db:
        _require_own_collection(db, collection_id, user_id)
        for it in payload.items:
            if not db.execute("SELECT 1 FROM creatives WHERE id=?", (it.creative_id,)).fetchone():
                continue
            cur = db.execute(
                "INSERT INTO collection_items(collection_id,group_key,representative_creative_id,added_at) "
                "VALUES(?,?,?,?) ON CONFLICT(collection_id,group_key) DO NOTHING",
                (collection_id, it.group_key, it.creative_id, now_iso()))
            added += cur.rowcount or 0
        db.commit()
    return {"ok": True, "added": added, "skipped": len(payload.items) - added}


@app.post("/api/v1/collections/{collection_id}/items/remove")
def api_remove_collection_items(collection_id: int, payload: CollectionBulkIn, principal: dict = Depends(public_api_principal)):
    user_id = require_user_principal(principal)
    removed = 0
    with closing(connect()) as db:
        _require_own_collection(db, collection_id, user_id)
        for key in payload.group_keys:
            removed += db.execute("DELETE FROM collection_items WHERE collection_id=? AND group_key=?",
                                  (collection_id, key)).rowcount or 0
        db.commit()
    return {"ok": True, "removed": removed}


@app.post("/api/v1/collections/{collection_id}/merge")
def api_merge_collections(collection_id: int, payload: CollectionBulkIn, principal: dict = Depends(public_api_principal)):
    """Переносит объявления из подборок `ids` в эту и удаляет опустевшие подборки."""
    user_id = require_user_principal(principal)
    with closing(connect()) as db:
        _require_own_collection(db, collection_id, user_id)
        for other in payload.ids:
            if other == collection_id:
                continue
            _require_own_collection(db, other, user_id)
            db.execute(
                "INSERT INTO collection_items(collection_id,group_key,representative_creative_id,added_at) "
                "SELECT ?,group_key,representative_creative_id,added_at FROM collection_items WHERE collection_id=? "
                "ON CONFLICT(collection_id,group_key) DO NOTHING", (collection_id, other))
            db.execute("DELETE FROM collection_items WHERE collection_id=?", (other,))
            db.execute("UPDATE collections SET parent_id=? WHERE parent_id=?", (collection_id, other))  # вложенные папки — в целевую
            db.execute("DELETE FROM collections WHERE id=?", (other,))
        db.commit()
    return {"ok": True}


@app.post("/api/v1/collections/bulk-delete")
def api_delete_collections(payload: CollectionBulkIn, principal: dict = Depends(public_api_principal)):
    user_id = require_user_principal(principal)
    with closing(connect()) as db:
        for top in payload.ids:
            if not db.execute("SELECT 1 FROM collections WHERE id=?", (top,)).fetchone():
                continue  # уже удалена вместе с родительской папкой
            _require_own_collection(db, top, user_id)
            for cid in _collection_tree_ids(db, top):
                db.execute("DELETE FROM collection_items WHERE collection_id=?", (cid,))
                db.execute("DELETE FROM collections WHERE id=?", (cid,))
        db.commit()
    return {"ok": True}


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
def api_carousels(intent: str = "", creative_id: int = 0, limit: int = 40, principal: dict = Depends(public_api_principal)):
    """Карусели из разных объявлений в том виде, как их показала площадка: порядок слайдов,
    сколько объявлений в карусели всего и сколько было видно сразу, без прокрутки.
    Источник — carousel_slides (состав присылает парсер). creative_id — только карусели с этим объявлением."""
    require_user_principal(principal)
    intents = _parse_intents(intent)
    limit = max(1, min(int(limit or 40), 100))
    where, params = [], []
    if intents:
        placeholders = ",".join(["PYLOWER(?)"] * len(intents))
        where.append(f"s.job_id IN (SELECT id FROM jobs WHERE PYLOWER(key_text) IN ({placeholders}))")
        params += intents
    if creative_id:
        where.append("s.carousel_id IN (SELECT carousel_id FROM carousel_slides WHERE creative_id=?)")
        params.append(int(creative_id))
    wsql = (" WHERE " + " AND ".join(where)) if where else ""
    with closing(connect()) as db:
        groups = db.execute(
            "SELECT s.carousel_id, COUNT(*) AS n, MAX(s.total) AS total, MAX(s.visible) AS visible, MAX(s.platform) AS platform,"
            " MAX(s.page_url) AS page_url, MAX(s.job_id) AS job_id, MAX(s.observed_at) AS last_seen_at "
            "FROM carousel_slides s" + wsql + " GROUP BY s.carousel_id HAVING n > 1 ORDER BY last_seen_at DESC LIMIT ?",
            params + [limit],
        ).fetchall()
        items = []
        for g in groups:
            rows = db.execute(
                "SELECT s.slide_index,s.title,s.domain,s.image_url,s.creative_id,s.job_id,"
                "c.title AS c_title,c.domain AS c_domain,c.image_url AS c_image,c.video_poster AS c_poster,c.creative_type AS c_type "
                "FROM carousel_slides s LEFT JOIN creatives c ON c.id=s.creative_id WHERE s.carousel_id=? ORDER BY s.slide_index",
                (g["carousel_id"],),
            ).fetchall()
            members, fixed = [], False
            for r in rows:
                cid = r["creative_id"]
                if cid is None:
                    # слайд пришёл раньше, чем объявление записалось в библиотеку — привязываем сейчас
                    cid = _carousel_match_creative(db, r["job_id"], r["title"], r["domain"] or "")
                    if cid:
                        db.execute("UPDATE carousel_slides SET creative_id=? WHERE carousel_id=? AND slide_index=?",
                                   (cid, g["carousel_id"], r["slide_index"]))
                        fixed = True
                members.append({
                    "id": cid, "title": r["title"] or r["c_title"], "domain": r["domain"] or r["c_domain"],
                    "image_url": r["c_image"] or r["c_poster"] or r["image_url"], "creative_type": r["c_type"],
                    "carousel_slide_index": r["slide_index"],
                    "visible_at_once": r["slide_index"] < (g["visible"] or 1),
                })
            if fixed:
                db.commit()
            job = db.execute("SELECT key_text,city FROM jobs WHERE id=?", (g["job_id"],)).fetchone() if g["job_id"] else None
            items.append({
                "carousel_id": g["carousel_id"], "platform": g["platform"], "page_url": g["page_url"],
                "job_id": g["job_id"], "key_text": job["key_text"] if job else None, "city": job["city"] if job else None,
                "total": g["total"] or g["n"], "visible": g["visible"] or 1,
                "member_count": g["n"], "last_seen_at": g["last_seen_at"], "members": members,
            })
        # Вторая вкладка: галереи одного объявления (несколько картинок, порядок как в рекламе).
        gwhere, gparams = [], []
        if intents:
            placeholders = ",".join(["PYLOWER(?)"] * len(intents))
            gwhere.append(f"g.job_id IN (SELECT id FROM jobs WHERE PYLOWER(key_text) IN ({placeholders}))")
            gparams += intents
        if creative_id:
            gwhere.append("g.creative_id=?")
            gparams.append(int(creative_id))
        galleries = []
        for g in db.execute(
            "SELECT g.*, j.key_text, j.city FROM creative_galleries g LEFT JOIN jobs j ON j.id=g.job_id"
            + ((" WHERE " + " AND ".join(gwhere)) if gwhere else "") + " ORDER BY g.observed_at DESC LIMIT ?",
            gparams + [limit * 2],
        ).fetchall():
            cid = g["creative_id"]
            if cid is None:
                cid = _carousel_match_creative(db, g["job_id"], g["title"], g["domain"] or "")
                if cid:
                    db.execute("UPDATE creative_galleries SET creative_id=? WHERE gallery_key=?", (cid, g["gallery_key"]))
                    db.commit()
            try:
                images = json.loads(g["images_json"] or "[]")
            except ValueError:
                images = []
            galleries.append({
                "gallery_key": g["gallery_key"], "id": cid, "title": g["title"], "description": g["description"],
                "domain": g["domain"], "images": images, "total": len(images), "visible": g["visible"] or 0,
                "platform": g["platform"], "page_url": g["page_url"], "job_id": g["job_id"],
                "key_text": g["key_text"], "city": g["city"], "last_seen_at": g["observed_at"],
            })
    return {"items": items, "galleries": galleries}


@app.get("/api/v1/analytics/ad-signals")
def api_ad_signals(intent: str = "", principal: dict = Depends(public_api_principal)):
    """Что ещё видно в самих объявлениях (разметка, creative_details): промокоды и акции,
    юридические предупреждения, возрастные метки, соцреклама, старые цены, уточнения, товары в плитках,
    форматы — и сколько живёт одно объявление РСЯ (по его номеру data-ad-id)."""
    user_id = require_user_principal(principal)
    is_admin = _console.is_platform_admin(principal) and principal.get("kind") == "user"
    where, params = ["1=1"], []
    intents = _parse_intents(intent)
    if intents:
        where.append(_intent_exists_sql("c", intents))
        params.extend(intents)
    if not is_admin:
        where.append("c.id IN (SELECT o.creative_id FROM occurrences o JOIN jobs j ON j.id=o.job_id WHERE j.owner_user_id=?)")
        params.append(user_id)
    w = " AND ".join(where)
    base = f"FROM creatives c JOIN creative_details cd ON cd.creative_id=c.id WHERE {w}"
    with closing(connect()) as db:
        t = db.execute(
            "SELECT COUNT(*) AS total, SUM(COALESCE(cd.promo_text,'')<>'') AS promo, SUM(COALESCE(cd.warning,'')<>'') AS warning,"
            " SUM(COALESCE(cd.age_label,'')<>'') AS age, SUM(cd.is_social) AS social, SUM(COALESCE(cd.price_old,'')<>'') AS old_price,"
            " SUM(COALESCE(cd.callouts_json,'')<>'') AS callouts, SUM(COALESCE(cd.offers_json,'')<>'') AS offers,"
            " SUM(COALESCE(cd.ad_id,'')<>'') AS with_ad_id, SUM(COALESCE(cd.direct_campaign_id,'')<>'') AS with_campaign " + base,
            params,
        ).fetchone()
        formats = [dict(r) for r in db.execute(f"SELECT cd.ad_format AS format, COUNT(*) AS n {base} GROUP BY 1 ORDER BY 2 DESC", params)]
        promos = [dict(r) for r in db.execute(
            f"SELECT MAX(c.id) AS id, cd.promo_text AS text, c.domain, MAX(c.title) AS title, MAX(c.captured_at) AS last_seen_at, COUNT(*) AS n "
            f"{base} AND COALESCE(cd.promo_text,'')<>'' GROUP BY cd.promo_text, c.domain ORDER BY last_seen_at DESC LIMIT 60", params)]
        warnings = [dict(r) for r in db.execute(
            f"SELECT cd.warning AS text, COUNT(*) AS n, COUNT(DISTINCT c.domain) AS domains {base} AND COALESCE(cd.warning,'')<>'' "
            f"GROUP BY cd.warning ORDER BY n DESC LIMIT 20", params)]
        ages = [dict(r) for r in db.execute(
            f"SELECT cd.age_label AS label, COUNT(*) AS n {base} AND COALESCE(cd.age_label,'')<>'' GROUP BY 1 ORDER BY 2 DESC", params)]
        callouts: dict[str, int] = {}
        for (v,) in db.execute(f"SELECT cd.callouts_json {base} AND COALESCE(cd.callouts_json,'')<>''", params):
            try:
                for x in json.loads(v):
                    callouts[x] = callouts.get(x, 0) + 1
            except ValueError:
                pass
        price_drops = [dict(r) for r in db.execute(
            f"SELECT c.id, c.title, c.domain, cd.price_current, cd.price_old, cd.discount_text, c.captured_at AS last_seen_at "
            f"{base} AND COALESCE(cd.price_old,'')<>'' ORDER BY c.id DESC LIMIT 40", params)]
        offers = []
        for r in db.execute(f"SELECT c.id, c.domain, cd.offers_json, c.captured_at {base} AND COALESCE(cd.offers_json,'')<>'' ORDER BY c.id DESC LIMIT 30", params):
            try:
                items = json.loads(r["offers_json"])
            except ValueError:
                items = []
            offers.append({"id": r["id"], "domain": r["domain"], "items": items, "last_seen_at": r["captured_at"]})
        longest = [dict(r) for r in db.execute(
            f"SELECT cd.ad_id, MAX(c.id) AS id, MAX(c.title) AS title, MAX(c.domain) AS domain,"
            f" MIN(o.observed_at) AS first_seen_at, MAX(o.observed_at) AS last_seen_at, COUNT(DISTINCT o.job_id) AS runs "
            f"FROM creatives c JOIN creative_details cd ON cd.creative_id=c.id JOIN occurrences o ON o.creative_id=c.id "
            f"WHERE {w} AND COALESCE(cd.ad_id,'')<>'' GROUP BY cd.ad_id HAVING runs>1 "
            f"ORDER BY julianday(MAX(o.observed_at))-julianday(MIN(o.observed_at)) DESC, runs DESC LIMIT 30", params)]
    return {
        "totals": {k: (t[k] or 0) for k in t.keys()},
        "formats": formats, "promos": promos, "warnings": warnings, "ages": ages,
        "callouts": [{"text": k, "n": v} for k, v in sorted(callouts.items(), key=lambda kv: -kv[1])[:40]],
        "price_drops": price_drops, "offers": offers, "longest_ads": longest,
    }


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


def _clean_adv_name(name: str | None, inn: str | None) -> str:
    """Название рекламодателя для показа: в базе встречаются обрывки дисклеймера
    («Финансовые услуги оказывает: ПАО …», «нк. Проектная декларация на сайте …»)."""
    text = re.sub(r"\s+", " ", (name or "")).strip()
    for long, short in (("ОБЩЕСТВО С ОГРАНИЧЕННОЙ ОТВЕТСТВЕННОСТЬЮ", "ООО"), ("ПУБЛИЧНОЕ АКЦИОНЕРНОЕ ОБЩЕСТВО", "ПАО"),
                        ("НЕПУБЛИЧНОЕ АКЦИОНЕРНОЕ ОБЩЕСТВО", "НАО"), ("АКЦИОНЕРНОЕ ОБЩЕСТВО", "АО"),
                        ("ИНДИВИДУАЛЬНЫЙ ПРЕДПРИНИМАТЕЛЬ", "ИП"), ("АВТОНОМНАЯ НЕКОММЕРЧЕСКАЯ ОРГАНИЗАЦИЯ", "АНО")):
        text = re.sub(re.escape(long), short, text, flags=re.IGNORECASE)
    # «ПАО Сбербанк. ПАО "СБЕРБАНК РОССИИ"» — берём первое название
    first = text.split(". ")[0].strip()
    if len(first) >= 4 and len(text) > len(first) + 4:
        text = first
    if re.search(r"противопоказан|посоветуйтесь|специалист", text, flags=re.IGNORECASE):
        text = ""
    m = re.match(r"^[^:]{0,60}(оказывает|предоставляет|осуществляет|реализует)[^:]{0,20}:\s*(.+)$", text, flags=re.IGNORECASE)
    if m:
        text = m.group(2).strip()
    if not text or text[0].islower() or "декларац" in text.lower():
        return f"ИНН {inn}" if inn else "Без названия"
    return text[:120]


@app.get("/api/v1/analytics/summary")
def api_analytics_summary(intent: str = "", principal: dict = Depends(public_api_principal)):
    """Сводка для экрана «Обзор»: о чём реклама, кто рекламируется больше всех, какие объявления
    встречаются чаще и дольше всего, на каких сайтах их показывают и где расходятся данные.
    Одно объявление (общий content_fingerprint) считается один раз. Обычный пользователь видит
    сводку только по своим запускам, администратор — по всей базе; intent сужает до запросов."""
    user_id = require_user_principal(principal)
    is_admin = _console.is_platform_admin(principal) and principal.get("kind") == "user"
    gk = "COALESCE(NULLIF(c.content_fingerprint,''),'hash:'||c.creative_hash)"
    where, params = ["1=1"], []
    intents = _parse_intents(intent)
    if intents:
        where.append(_intent_exists_sql("c", intents))
        params.extend(intents)
    if not is_admin:
        where.append("c.id IN (SELECT o.creative_id FROM occurrences o JOIN jobs j ON j.id=o.job_id WHERE j.owner_user_id=?)")
        params.append(user_id)
    w = " AND ".join(where)
    unknown = [_pylower(aii.UNKNOWN), _pylower(aii.UNKNOWN).replace("ё", "е")]
    week_ago = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")
    with closing(connect()) as db:
        total = db.execute(f"SELECT COUNT(DISTINCT {gk}) FROM creatives c WHERE {w}", params).fetchone()[0]
        fresh = db.execute(f"SELECT COUNT(DISTINCT {gk}) FROM creatives c WHERE {w} AND substr(c.captured_at,1,10)>=?", params + [week_ago]).fetchone()[0]
        discounts = db.execute(f"SELECT COUNT(DISTINCT {gk}) FROM creatives c WHERE {w} AND c.discount_percent IS NOT NULL", params).fetchone()[0]
        adv_total = db.execute(
            f"SELECT COUNT(DISTINCT l.advertiser_id) FROM creatives c JOIN creative_advertiser_links l ON l.creative_id=c.id WHERE {w}", params).fetchone()[0]

        # О чём реклама: интенты объявлений
        intent_rows = db.execute(
            f"SELECT MIN(c.ai_intent) AS intent, COUNT(DISTINCT {gk}) AS n FROM creatives c "
            f"WHERE {w} AND c.ai_intent IS NOT NULL AND PYLOWER(c.ai_intent) NOT IN (?,?) GROUP BY PYLOWER(c.ai_intent) ORDER BY n DESC LIMIT 12",
            params + unknown).fetchall()
        labelled = db.execute(
            f"SELECT COUNT(DISTINCT {gk}) FROM creatives c WHERE {w} AND c.ai_intent IS NOT NULL AND PYLOWER(c.ai_intent) NOT IN (?,?)",
            params + unknown).fetchone()[0]
        # Доля объявлений по теме выбранных запросов (если запрос выбран)
        on_topic = None
        if intents:
            match: set[str] = set()
            for kw in intents[:10]:
                match.update(_history_intents(db, kw))
            if match:
                ml = sorted(match)[:600]
                n_match = db.execute(
                    f"SELECT COUNT(DISTINCT {gk}) FROM creatives c WHERE {w} AND PYLOWER(c.ai_intent) IN ({','.join(['?'] * len(ml))})",
                    params + ml).fetchone()[0]
            else:
                n_match = 0
            on_topic = {"count": n_match, "of": labelled, "pct": round(n_match * 100 / labelled) if labelled else 0}
            match_set = match
        else:
            match_set = set()
        # Выбран запрос и по нему есть объявления по теме — списки ниже строим только по ним:
        # иначе верх занимают банки и застройщики, которых сеть показывает всем подряд.
        wt, pt = w, list(params)
        topic_only = bool(match_set)
        if topic_only:
            ml2 = sorted(match_set)[:600]
            wt = w + f" AND PYLOWER(c.ai_intent) IN ({','.join(['?'] * len(ml2))})"
            pt = params + ml2

        # Кто рекламируется больше всех
        adv_rows = db.execute(
            f"SELECT a.inn, a.canonical_name, a.website_url, COUNT(DISTINCT {gk}) AS creatives, "
            "(SELECT COUNT(*) FROM occurrences o WHERE o.creative_id IN "
            "  (SELECT l2.creative_id FROM creative_advertiser_links l2 WHERE l2.advertiser_id=a.id)) AS shows, "
            "MIN(c.captured_at) AS first_seen, MAX(c.captured_at) AS last_seen "
            f"FROM creatives c JOIN creative_advertiser_links l ON l.creative_id=c.id JOIN advertisers a ON a.id=l.advertiser_id "
            f"WHERE {wt} AND COALESCE(a.inn,'')<>'' GROUP BY a.id ORDER BY creatives DESC, shows DESC LIMIT 10", pt).fetchall()

        # Что встречается чаще и дольше всего
        top_rows = db.execute(
            f"SELECT {gk} AS gk, MAX(CASE WHEN COALESCE(c.image_url,'')<>'' THEN c.id END) AS rep_id, COUNT(o.id) AS shows, COUNT(DISTINCT o.job_id) AS runs "
            f"FROM creatives c LEFT JOIN occurrences o ON o.creative_id=c.id WHERE {wt} AND LENGTH(COALESCE(c.title,''))>8 "
            "GROUP BY gk HAVING MAX(CASE WHEN COALESCE(c.image_url,'')<>'' THEN 1 ELSE 0 END)=1 "
            "ORDER BY runs DESC, shows DESC LIMIT 6", pt).fetchall()
        rep_ids = [r["rep_id"] for r in top_rows]
        by_id = {r["id"]: r for r in db.execute(
            f"SELECT * FROM creatives WHERE id IN ({','.join(['?'] * len(rep_ids)) or '-1'})", rep_ids).fetchall()}
        top_items = [_creative_row_to_dict(by_id[i]) for i in rep_ids if i in by_id]
        _attach_intents(db, top_items, None if is_admin else user_id)
        _attach_stats(db, top_items)
        _attach_collections(db, top_items, principal)

        # Где показывают: сайты, на которых найдена реклама
        site_rows = db.execute(
            f"SELECT c.platform AS site, COUNT(DISTINCT {gk}) AS n FROM creatives c WHERE {w} AND COALESCE(c.platform,'')<>'' "
            "GROUP BY c.platform ORDER BY n DESC LIMIT 10", params).fetchall()
        sites_total = db.execute(
            f"SELECT COUNT(DISTINCT c.platform) FROM creatives c WHERE {w} AND COALESCE(c.platform,'')<>''", params).fetchone()[0]

        # Расхождения: посадочная на чужом домене; на сайте указано другое юрлицо, чем в объявлении
        landing_mismatch = db.execute(
            f"SELECT COUNT(DISTINCT {gk}) FROM creatives c JOIN creative_utm u ON u.creative_id=c.id WHERE {w} AND u.landing_domain_mismatch=1",
            params).fetchone()[0]
        legal_mismatch = 0
        try:
            legal = {r["domain"]: r["inn"] for r in db.execute(
                "SELECT domain, inn FROM landing_legal WHERE status='found' AND COALESCE(inn,'')<>''").fetchall()}
            seen_groups: set[str] = set()
            for r in db.execute(
                f"SELECT {gk} AS gk, u.final_url_domain AS d, a.inn AS inn FROM creatives c JOIN creative_utm u ON u.creative_id=c.id "
                f"JOIN creative_advertiser_links l ON l.creative_id=c.id JOIN advertisers a ON a.id=l.advertiser_id "
                f"WHERE {w} AND COALESCE(a.inn,'')<>'' AND COALESCE(u.final_url_domain,'')<>''", params).fetchall():
                site_inn = legal.get(re.sub(r"^(www|m)\.", "", (r["d"] or "").lower()))
                if site_inn and site_inn != r["inn"]:
                    seen_groups.add(r["gk"])
            legal_mismatch = len(seen_groups)
        except sqlite3.Error:
            legal_mismatch = 0
    return {
        "totals": {"creatives": total, "advertisers": adv_total, "fresh_7d": fresh, "discounts": discounts, "sites": sites_total,
                   "labelled": labelled, "landing_mismatch": landing_mismatch, "legal_mismatch": legal_mismatch},
        "on_topic": on_topic,
        "intents": [{"intent": r["intent"], "count": r["n"], "pct": round(r["n"] * 100 / labelled) if labelled else 0,
                     "on_topic": (_pylower(r["intent"]) in match_set) if intents else None} for r in intent_rows],
        "advertisers": [{"inn": r["inn"], "name": _clean_adv_name(r["canonical_name"], r["inn"]), "website_url": r["website_url"],
                         "creatives": r["creatives"], "shows": r["shows"], "first_seen_at": r["first_seen"], "last_seen_at": r["last_seen"]}
                        for r in adv_rows],
        "top_creatives": top_items,
        "sites": [{"site": r["site"], "count": r["n"], "pct": round(r["n"] * 100 / total) if total else 0} for r in site_rows],
        "scope": "all" if is_admin else "own", "topic_only": topic_only,
    }


@app.get("/api/v1/analytics/leaders")
def api_analytics_leaders(ai_intent: str = "", intent: str = "", limit: int = 24, principal: dict = Depends(public_api_principal)):
    """Сильные креативы внутри одной темы (интента): объявление тем сильнее, чем на большем числе
    сайтов и в большем числе запусков и городов оно встречается и чем дольше крутится. Такие
    объявления рекламодатель не выключает и масштабирует — значит, они окупаются.
    ai_intent — тема; если не задана, берётся тема, подходящая под выбранные запросы (intent),
    иначе самая частая. Возвращает и список тем для выбора."""
    user_id = require_user_principal(principal)
    is_admin = _console.is_platform_admin(principal) and principal.get("kind") == "user"
    limit = max(1, min(limit, 60))
    gk = "COALESCE(NULLIF(c.content_fingerprint,''),'hash:'||c.creative_hash)"
    where, params = ["1=1"], []
    intents = _parse_intents(intent)
    if intents:
        where.append(_intent_exists_sql("c", intents))
        params.extend(intents)
    if not is_admin:
        where.append("c.id IN (SELECT o.creative_id FROM occurrences o JOIN jobs j ON j.id=o.job_id WHERE j.owner_user_id=?)")
        params.append(user_id)
    w = " AND ".join(where)
    unknown = [_pylower(aii.UNKNOWN), _pylower(aii.UNKNOWN).replace("ё", "е")]
    with closing(connect()) as db:
        theme_rows = db.execute(
            f"SELECT MIN(c.ai_intent) AS intent, COUNT(DISTINCT {gk}) AS n, COUNT(DISTINCT l.advertiser_id) AS advs "
            f"FROM creatives c LEFT JOIN creative_advertiser_links l ON l.creative_id=c.id "
            f"WHERE {w} AND c.ai_intent IS NOT NULL AND PYLOWER(c.ai_intent) NOT IN (?,?) "
            "GROUP BY PYLOWER(c.ai_intent) ORDER BY n DESC LIMIT 60", params + unknown).fetchall()
        match: set[str] = set()
        for kw in intents[:10]:
            match.update(_history_intents(db, kw))
        themes = [{"intent": r["intent"], "count": r["n"], "advertisers": r["advs"],
                   "on_topic": _pylower(r["intent"]) in match} for r in theme_rows]
        themes.sort(key=lambda t: (not t["on_topic"], -t["count"]))
        chosen = (ai_intent or "").strip()
        if not chosen and themes:
            chosen = themes[0]["intent"]
        items: list[dict[str, Any]] = []
        if chosen:
            rows = db.execute(
                f"SELECT {gk} AS gk, MAX(c.id) AS any_id, MAX(CASE WHEN COALESCE(c.image_url,'')<>'' THEN c.id END) AS img_id, "
                "COUNT(DISTINCT o.platform) AS sites, COUNT(DISTINCT o.job_id) AS runs, COUNT(o.id) AS shows, "
                "COUNT(DISTINCT PYLOWER(j.city)) AS cities, MIN(COALESCE(o.observed_at,c.captured_at)) AS first_seen, "
                "MAX(COALESCE(o.observed_at,c.captured_at)) AS last_seen "
                f"FROM creatives c LEFT JOIN occurrences o ON o.creative_id=c.id LEFT JOIN jobs j ON j.id=o.job_id "
                f"WHERE {w} AND PYLOWER(c.ai_intent)=? GROUP BY gk", params + [_pylower(chosen)]).fetchall()

            def _days(a: str | None, b: str | None) -> int:
                try:
                    return (datetime.fromisoformat(str(b)[:19]).date() - datetime.fromisoformat(str(a)[:19]).date()).days + 1
                except Exception:
                    return 1
            stats = [{"gk": r["gk"], "id": r["img_id"] or r["any_id"], "sites": max(r["sites"], 1), "runs": max(r["runs"], 1),
                      "shows": max(r["shows"], 1), "cities": max(r["cities"], 1), "days": _days(r["first_seen"], r["last_seen"]),
                      "first_seen_at": r["first_seen"], "last_seen_at": r["last_seen"]} for r in rows]
            mx = {k: max([x[k] for x in stats] or [1]) for k in ("sites", "runs", "shows", "cities", "days")}
            weights = {"sites": 35, "runs": 30, "shows": 15, "cities": 10, "days": 10}
            floors = {"sites": 5, "runs": 8, "shows": 15, "cities": 4, "days": 7}
            for x in stats:
                # показатель «1» (встречено один раз, на одном сайте) силы не добавляет
                # Сравниваем с лучшим в теме, но не ниже разумной планки — иначе в маленькой теме
                # объявление, встреченное дважды, выглядело бы «лидером».
                x["score"] = round(sum(weights[k] * min(1.0, (x[k] - 1) / (max(mx[k], floors[k]) - 1)) for k in weights))
            stats.sort(key=lambda x: (-x["score"], -x["shows"], -x["id"]))
            top = stats[:limit]
            ids = [x["id"] for x in top]
            by_id = {r["id"]: r for r in db.execute(
                f"SELECT * FROM creatives WHERE id IN ({','.join(['?'] * len(ids)) or '-1'})", ids).fetchall()}
            reps = [_creative_row_to_dict(by_id[i]) for i in ids if i in by_id]
            _attach_intents(db, reps, None if is_admin else user_id)
            _attach_stats(db, reps)
            _attach_collections(db, reps, principal)
            rep_by_id = {r["id"]: r for r in reps}
            adv_by_id: dict[int, dict[str, Any]] = {}
            for r in db.execute(
                f"SELECT l.creative_id, a.inn, a.canonical_name FROM creative_advertiser_links l JOIN advertisers a ON a.id=l.advertiser_id "
                f"WHERE l.creative_id IN ({','.join(['?'] * len(ids)) or '-1'})", ids).fetchall():
                adv_by_id.setdefault(r["creative_id"], {"inn": r["inn"], "name": _clean_adv_name(r["canonical_name"], r["inn"])})
            for rank, x in enumerate(top, 1):
                rep = rep_by_id.get(x["id"])
                if not rep:
                    continue
                items.append({"rank": rank, "score": x["score"],
                              "level": "leader" if x["score"] >= 60 else ("strong" if x["score"] >= 30 else "normal"),
                              "sites": x["sites"], "runs": x["runs"], "shows": x["shows"], "cities": x["cities"], "days": x["days"],
                              "advertiser": adv_by_id.get(x["id"]), "creative": rep})
            total_in_theme = len(stats)
            repeated = sum(1 for x in stats if x["runs"] > 1 or x["sites"] > 1)
        else:
            total_in_theme, repeated = 0, 0
    return {"themes": themes, "theme": chosen, "items": items, "total_in_theme": total_in_theme, "repeated": repeated,
            "weights": {"sites": 35, "runs": 30, "shows": 15, "cities": 10, "days": 10}}


@app.get("/api/v1/analytics/daily")
def api_daily_creatives(days: int = 14, intent: str = "", principal: dict = Depends(public_api_principal)):
    """Сколько новых креативов найдено за каждый из последних дней (по дате первого обнаружения).
    Считает по всей базе, а не по последним загруженным карточкам, как раньше делал фронтенд."""
    require_user_principal(principal)
    days = max(1, min(days, 90))
    since = (datetime.now() - timedelta(days=days - 1)).strftime("%Y-%m-%d")
    where, params = "substr(c.captured_at,1,10)>=?", [since]
    intents = _parse_intents(intent)
    if intents:
        where += " AND " + _intent_exists_sql("c", intents)
        params.extend(intents)
    with closing(connect()) as db:
        rows = db.execute(
            f"SELECT substr(c.captured_at,1,10) AS day, COUNT(*) AS n FROM creatives c WHERE {where} GROUP BY day", params).fetchall()
    by_day = {r["day"]: r["n"] for r in rows}
    out = []
    for i in range(days - 1, -1, -1):
        d = (datetime.now() - timedelta(days=i)).strftime("%Y-%m-%d")
        out.append({"day": d, "count": by_day.get(d, 0)})
    return {"items": out}


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


def _console_status_extra() -> dict[str, Any]:
    with closing(connect()) as db:
        intents_left = db.execute(
            "SELECT COUNT(DISTINCT PYLOWER(j.key_text)) FROM jobs j "
            "LEFT JOIN intent_embeddings ie ON ie.key_text_lower = PYLOWER(j.key_text) "
            "WHERE j.key_text IS NOT NULL AND TRIM(j.key_text) <> '' AND ie.key_text_lower IS NULL").fetchone()[0]
    return {"gemini_model": aii.last_model_used, "intent_embeddings_pending": intents_left}


# «Консоль» ЛК (2026-11-23): служебное для создателя (роль superuser) — app/console.py.
app.include_router(_console.build_router(public_api_principal, connect, hash_password, now_iso, _console_status_extra))


# «Сайты» ЛК «Реклама» (2026-11-18): домены reg.ru и размещение сайтов (app/sites_infra.py).
from .sites_infra import build_router as _build_sites_router  # noqa: E402

app.include_router(_build_sites_router(public_api_principal, require_user_principal, ADDAVTO_BASE_URL,
                                       ADDAVTO_ADMIN_USER, ADDAVTO_ADMIN_PASSWORD))

# «Студия креативов» ЛК (2026-11-19): дизайны на сервере + ИИ (app/studio.py).
from .studio import build_router as _build_studio_router  # noqa: E402

app.include_router(_build_studio_router(public_api_principal, require_user_principal, DATA_DIR))

# Команды ЛК и планировщик публикаций (2026-11-23): app/teams.py, app/planner.py — данные в studio.sqlite3.
from .teams import build_router as _build_teams_router  # noqa: E402


def _teams_users_lookup(ids, email):
    with closing(connect()) as db:
        if email:
            rows = db.execute("SELECT id,email,name FROM users WHERE email=? AND is_active=1", (email,)).fetchall()
        else:
            ids = [int(i) for i in (ids or [])][:500]
            if not ids:
                return []
            rows = db.execute(f"SELECT id,email,name FROM users WHERE id IN ({','.join('?' * len(ids))})", ids).fetchall()
    return [{"id": r["id"], "email": r["email"], "name": r["name"] or ""} for r in rows]


def _teams_notify(user_id, title, text, link):
    with closing(connect()) as db:
        create_notification(db, user_id, "team", title, text, link)
        db.commit()


app.include_router(_build_teams_router(public_api_principal, require_user_principal, DATA_DIR, _teams_users_lookup, _teams_notify))

from .planner import build_router as _build_planner_router  # noqa: E402

app.include_router(_build_planner_router(public_api_principal, require_user_principal, DATA_DIR, _teams_notify))
