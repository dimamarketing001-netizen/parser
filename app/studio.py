"""Студия креативов ЛК (2026-11-19): хранение дизайнов на сервере + ИИ.

Раньше дизайны жили только в localStorage браузера вместе со всем стейтом ЛК
(лимит ~5 МБ, переполнение глоталось молча) — дизайны с фото терялись.
Теперь: отдельная SQLite `data/studio.sqlite3`, дизайн = JSON (картинки внутри
как data URL — так canvas не «пачкается» чужими доменами и экспорт PNG работает).

ИИ — тот же Anthropic-совместимый прокси, что у рекламы (addAvto AI_*), ключ в
.env parsya-control как STUDIO_AI_API_KEY / STUDIO_AI_API_BASE / STUDIO_AI_MODEL.
Генерации картинок у прокси нет: ИИ пишет тексты, собирает макет из слоёв и
разбирает готовую картинку (vision).
"""
from __future__ import annotations

import asyncio
import base64
import io
import ipaddress
import json
import os
import re
import secrets
import socket
import sqlite3
import time
from contextlib import closing
from pathlib import Path
from typing import Any, Callable
from urllib.parse import urlsplit

import httpx
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field

from .teams import RANK as TEAM_RANK, init_db as teams_init_db, member_role

FORMATS = {"square": (1080, 1080), "landscape": (1200, 628), "story": (1080, 1920), "wide": (1080, 607),
           "r43": (1080, 810), "r34": (810, 1080), "r45": (1080, 1350),
           # форматы площадок (ЛК: STUDIO_PLATFORMS) — обложки, превью, карточки маркетплейсов, печать
           "vk_cover": (1920, 768), "hd": (1280, 720), "yt_banner": (2560, 1440), "mp_card": (900, 1200),
           "og": (1200, 630), "slides": (1920, 1080), "a4": (1240, 1754), "bcard": (1063, 591)}
ICONS = ["star", "heart", "zap", "flame", "gift", "tag", "badge-percent", "thumbs-up", "sparkles", "trophy",
         "shield-check", "truck", "clock", "phone", "map-pin", "award", "check-circle-2", "rocket"]
FONTS = ["Onest", "Manrope", "Inter", "Montserrat", "Roboto", "Rubik", "Oswald", "PT Serif", "Play"]
ROLES = ("headline", "subheadline", "cta", "badge", "disclaimer")
MAX_DESIGN_BYTES = 30_000_000
VERSION_EVERY = float(os.getenv("STUDIO_VERSION_EVERY", "600"))
MAX_IMAGE_BYTES = 12_000_000

_HEX = re.compile(r"^#[0-9a-fA-F]{6}$")


class AIError(RuntimeError):
    pass


# ---------------------------------------------------------------- хранилище
def _db_path(data_dir: Path) -> Path:
    return Path(os.getenv("STUDIO_SQLITE_PATH", data_dir / "studio.sqlite3")).resolve()


def _connect(path: Path) -> sqlite3.Connection:
    c = sqlite3.connect(path, timeout=15)
    c.row_factory = sqlite3.Row
    return c


def init_db(path: Path) -> None:
    with closing(_connect(path)) as c:
        c.execute("PRAGMA journal_mode=WAL")
        c.executescript("""
        CREATE TABLE IF NOT EXISTS studio_designs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            local_id TEXT,
            name TEXT NOT NULL DEFAULT '',
            format TEXT NOT NULL DEFAULT 'square',
            data TEXT NOT NULL,
            thumb TEXT NOT NULL DEFAULT '',
            created_at REAL NOT NULL,
            updated_at REAL NOT NULL
        );
        CREATE INDEX IF NOT EXISTS ix_studio_user ON studio_designs(user_id, updated_at);
        CREATE UNIQUE INDEX IF NOT EXISTS ux_studio_local ON studio_designs(user_id, local_id) WHERE local_id IS NOT NULL;
        CREATE TABLE IF NOT EXISTS studio_uploads (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            name TEXT NOT NULL DEFAULT '',
            data TEXT NOT NULL,
            thumb TEXT NOT NULL DEFAULT '',
            width INTEGER, height INTEGER,
            created_at REAL NOT NULL
        );
        CREATE INDEX IF NOT EXISTS ix_studio_uploads_user ON studio_uploads(user_id, created_at);
        CREATE TABLE IF NOT EXISTS studio_versions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            design_id INTEGER NOT NULL,
            data TEXT NOT NULL,
            thumb TEXT NOT NULL DEFAULT '',
            created_at REAL NOT NULL
        );
        CREATE INDEX IF NOT EXISTS ix_studio_versions ON studio_versions(design_id, created_at);
        """)
        c.execute("""CREATE TABLE IF NOT EXISTS studio_fonts (
            id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER NOT NULL, family TEXT NOT NULL,
            filename TEXT NOT NULL DEFAULT '', fmt TEXT NOT NULL, data BLOB NOT NULL, created_at REAL NOT NULL)""")
        # Ссылка на просмотр для клиента: только картинки страниц, отправленные владельцем; токен случайный.
        c.execute("""CREATE TABLE IF NOT EXISTS studio_shares (
            token TEXT PRIMARY KEY, user_id INTEGER NOT NULL, design_id INTEGER NOT NULL, name TEXT NOT NULL DEFAULT '',
            format TEXT NOT NULL DEFAULT 'square', pages TEXT NOT NULL, views INTEGER NOT NULL DEFAULT 0,
            created_at REAL NOT NULL, updated_at REAL NOT NULL)""")
        c.execute("CREATE UNIQUE INDEX IF NOT EXISTS ux_studio_share_design ON studio_shares(design_id)")
        cols = {r[1] for r in c.execute("PRAGMA table_info(studio_designs)")}
        if "is_template" not in cols:
            c.execute("ALTER TABLE studio_designs ADD COLUMN is_template INTEGER NOT NULL DEFAULT 0")
        if "category" not in cols:
            c.execute("ALTER TABLE studio_designs ADD COLUMN category TEXT NOT NULL DEFAULT ''")
        if "team_id" not in cols:  # общие дизайны команды (NULL — личный)
            c.execute("ALTER TABLE studio_designs ADD COLUMN team_id INTEGER")
            c.execute("CREATE INDEX IF NOT EXISTS ix_studio_team ON studio_designs(team_id, updated_at)")
        if "updated_by" not in cols:
            c.execute("ALTER TABLE studio_designs ADD COLUMN updated_by INTEGER")
        c.commit()


def _meta(r: sqlite3.Row) -> dict[str, Any]:
    keys = r.keys()
    out = {"id": r["id"], "name": r["name"], "format": r["format"], "thumb": r["thumb"],
           "created_at": r["created_at"], "updated_at": r["updated_at"]}
    if "category" in keys:
        out["category"] = r["category"]
    for k in ("team_id", "updated_by", "user_id"):
        if k in keys:
            out[k] = r[k]
    return out


class DesignIn(BaseModel):
    name: str = Field(default="Новый дизайн", max_length=200)
    format: str = Field(default="square", max_length=20)
    data: dict[str, Any]
    thumb: str = Field(default="", max_length=600_000)
    local_id: str | None = Field(default=None, max_length=60)
    team_id: int | None = None          # только при создании: дизайн сразу в команде
    base_updated_at: float | None = None  # защита от перезаписи чужих правок (409, если на сервере новее)


class MoveIn(BaseModel):
    team_id: int | None = None


class UploadIn(BaseModel):
    data_url: str = Field(min_length=50, max_length=MAX_IMAGE_BYTES * 2)
    name: str = Field(default="", max_length=200)


class AsTemplateIn(BaseModel):
    name: str = Field(default="", max_length=200)
    category: str = Field(default="", max_length=60)
    team_id: int | None = None  # шаблон команды (видят все участники)


class FontIn(BaseModel):
    family: str = Field(min_length=1, max_length=60)
    filename: str = Field(default="", max_length=200)
    data_base64: str = Field(min_length=100, max_length=12_000_000)


class RemoveBgIn(BaseModel):
    image: str = Field(min_length=50, max_length=MAX_IMAGE_BYTES * 2)


class RenameIn(BaseModel):
    name: str = Field(min_length=1, max_length=200)


class ImportIn(BaseModel):
    designs: list[DesignIn] = Field(default_factory=list, max_length=200)


class FetchImageIn(BaseModel):
    url: str = Field(min_length=8, max_length=4000)
    max_dim: int = Field(default=1600, ge=200, le=4000)


class AICopyIn(BaseModel):
    brief: str = Field(default="", max_length=3000)
    fields: list[str] = Field(default_factory=lambda: ["headline", "subheadline", "cta", "badge"], max_length=6)
    count: int = Field(default=5, ge=1, le=12)
    tone: str = Field(default="", max_length=200)
    current: dict[str, str] = Field(default_factory=dict)


class ShareIn(BaseModel):
    pages: list[str] = Field(min_length=1, max_length=30)


SHARE_PAGE_MAX = 4_000_000   # длина data URL одной страницы
SHARE_TOKEN_RE = re.compile(r"^[A-Za-z0-9_-]{16,64}$")


class VideoIn(BaseModel):
    data_url: str
    name: str = Field(default="", max_length=200)


VIDEO_MAX = 80_000_000


class AudioFetchIn(BaseModel):
    url: str = Field(min_length=8, max_length=2000)


class AITranslateIn(BaseModel):
    texts: list[str] = Field(default_factory=list, max_length=300)
    lang: str = Field(min_length=2, max_length=40)


class AIDesignIn(BaseModel):
    brief: str = Field(min_length=3, max_length=3000)
    format: str = Field(default="square", max_length=20)
    count: int = Field(default=3, ge=1, le=4)
    palette: list[str] = Field(default_factory=list, max_length=8)
    has_photo: bool = False


class AICritiqueIn(BaseModel):
    image: str = Field(min_length=50, max_length=MAX_IMAGE_BYTES * 2)
    brief: str = Field(default="", max_length=2000)
    format: str = Field(default="square", max_length=20)
    texts: list[str] = Field(default_factory=list, max_length=30)


class AIImageIn(BaseModel):
    prompt: str = Field(min_length=3, max_length=4000)
    example: str = Field(default="", max_length=MAX_IMAGE_BYTES * 2)
    format: str = Field(default="square", max_length=20)
    count: int = Field(default=2, ge=1, le=4)


class AIImagePromptIn(BaseModel):
    brief: str = Field(default="", max_length=3000)
    example: str = Field(default="", max_length=MAX_IMAGE_BYTES * 2)
    format: str = Field(default="square", max_length=20)
    wish: str = Field(default="", max_length=1000)


class AIRewriteIn(BaseModel):
    title: str = Field(default="", max_length=600)
    description: str = Field(default="", max_length=3000)
    brief: str = Field(default="", max_length=2000)


# ---------------------------------------------------------------- ИИ
def ai_configured() -> bool:
    return bool(os.getenv("STUDIO_AI_API_KEY"))


def _ai_base() -> str:
    return (os.getenv("STUDIO_AI_API_BASE") or "https://claude-n-codex.com:8443").rstrip("/")


def _ai_model() -> str:
    return os.getenv("STUDIO_AI_MODEL") or "claude-sonnet-5"


async def ai_complete(system: str, content: Any, max_tokens: int = 4000) -> str:
    key = os.getenv("STUDIO_AI_API_KEY")
    if not key:
        raise AIError("ИИ не настроен: STUDIO_AI_API_KEY не задан в .env parsya-control")
    body = {"model": _ai_model(), "max_tokens": max_tokens, "system": system,
            "messages": [{"role": "user", "content": content}]}
    headers = {"x-api-key": key, "anthropic-version": "2023-06-01", "content-type": "application/json"}
    last = ""
    for attempt in range(3):
        try:
            async with httpx.AsyncClient(timeout=httpx.Timeout(180.0, connect=15.0)) as http:
                r = await http.post(_ai_base() + "/v1/messages", json=body, headers=headers)
            if r.status_code >= 500 or r.status_code == 429:
                last = f"ИИ временно недоступен ({r.status_code})"
                await asyncio.sleep(2 + attempt * 3)
                continue
            if r.status_code >= 400:
                raise AIError(f"ИИ отказал: {r.status_code} {r.text[:200]}")
            data = r.json()
            text = "".join(b.get("text", "") for b in data.get("content") or [] if b.get("type") == "text")
            if not text.strip():
                raise AIError("ИИ вернул пустой ответ")
            return text
        except httpx.HTTPError as exc:
            last = f"нет связи с ИИ: {exc.__class__.__name__}"
            await asyncio.sleep(2 + attempt * 3)
    raise AIError(last or "ИИ не ответил")


def parse_json(text: str) -> Any:
    t = text.strip()
    m = re.search(r"```(?:json)?\s*(.*?)```", t, re.S)
    if m:
        t = m.group(1).strip()
    start = min([i for i in (t.find("{"), t.find("[")) if i >= 0], default=-1)
    if start > 0:
        t = t[start:]
    try:
        return json.loads(t)
    except ValueError:
        end = max(t.rfind("}"), t.rfind("]"))
        try:
            return json.loads(t[:end + 1])
        except ValueError as exc:
            raise AIError("ИИ ответил не в формате JSON") from exc


def _num(v: Any, lo: float, hi: float, default: float) -> float:
    try:
        x = float(v)
    except (TypeError, ValueError):
        return default
    if x != x:  # NaN
        return default
    return max(lo, min(hi, x))


def _color(v: Any, default: str) -> str:
    return v if isinstance(v, str) and _HEX.match(v) else default


def _txt(v: Any, n: int) -> str:
    return re.sub(r"\s+", " ", str(v or "")).strip()[:n]


def _ml(v: Any, n: int) -> str:
    """Текст слоя: переносы строк сохраняем (модель иногда шлёт буквальное «\\n»), до 4 строк."""
    lines = [re.sub(r"[ \t]+", " ", x).strip() for x in str(v or "").replace("\\n", "\n").splitlines()]
    return "\n".join(x for x in lines if x)[:n] if any(lines) else ""


def sanitize_layers(raw_layers: Any) -> list[dict[str, Any]]:
    """Слои от ИИ → только известные типы/поля, числа в допустимых пределах."""
    out: list[dict[str, Any]] = []
    for i, l in enumerate(raw_layers if isinstance(raw_layers, list) else []):
        if not isinstance(l, dict) or len(out) >= 24:
            continue
        t = l.get("type")
        base = {"id": f"A{int(time.time() * 1000):x}{i}", "type": t,
                "x": _num(l.get("x"), -20, 100, 10), "y": _num(l.get("y"), -20, 100, 10),
                "w": _num(l.get("w"), 2, 140, 60), "h": _num(l.get("h"), 1, 140, 12),
                "rotation": _num(l.get("rotation"), 0, 360, 0), "opacity": _num(l.get("opacity"), 0, 100, 100),
                "flipH": False, "flipV": False, "skewX": 0, "skewY": 0, "blendMode": "normal"}
        if t == "text":
            text = _ml(l.get("text"), 300)
            if not text:
                continue
            base.update(text=text, fontSize=_num(l.get("fontSize"), 1.5, 20, 6), color=_color(l.get("color"), "#111111"),
                        weight="700" if str(l.get("weight", "700")) in ("700", "800", "900", "bold") else "400",
                        align=l.get("align") if l.get("align") in ("left", "center", "right") else "center",
                        vertical=False,
                        font=l.get("font") if l.get("font") in FONTS else "Onest",
                        role=l.get("role") if l.get("role") in ROLES else "",
                        shadow=bool(l.get("shadow")),
                        upper=bool(l.get("upper")),
                        lineHeight=_num(l.get("lineHeight"), 0.8, 2, 1.15),
                        letterSpacing=_num(l.get("letterSpacing"), -5, 30, 0))
            bgc = l.get("bgColor")
            if isinstance(bgc, str) and _HEX.match(bgc):
                base.update(bgColor=bgc, bgRadius=_num(l.get("bgRadius"), 0, 50, 20))
        elif t == "shape":
            shape = l.get("shape") if l.get("shape") in ("rect", "ellipse", "triangle", "line", "polygon", "star") else "rect"
            fill = l.get("fill") if isinstance(l.get("fill"), dict) else {}
            ftype = fill.get("type") if fill.get("type") in ("solid", "gradient", "radial", "none") else "solid"
            base.update(shape=shape,
                        fill={"type": ftype, "color": _color(fill.get("color") or l.get("color"), "#16c784"),
                              "color2": _color(fill.get("color2"), "#0ea5e9"), "angle": _num(fill.get("angle"), 0, 360, 90)},
                        stroke={"enabled": False, "color": "#111827", "width": 3, "dash": "solid"},
                        radius=_num(l.get("radius"), 0, 80, 12))
            if shape in ("polygon", "star"):
                base["sides"] = int(_num(l.get("sides"), 3, 12, 5 if shape == "star" else 6))
            if shape == "star":
                base["innerRadius"] = _num(l.get("innerRadius"), 10, 90, 45)
        elif t == "icon":
            base.update(icon=l.get("icon") if l.get("icon") in ICONS else "star", color=_color(l.get("color"), "#16c784"))
        elif t == "image" and l.get("placeholder"):
            base.update(src="", fx=50, fy=50, zoom=1, placeholder=True)
        else:
            continue
        out.append(base)
    return out


DESIGN_SYSTEM = f"""Ты — арт-директор performance-рекламы (Яндекс Директ, VK). Собираешь макет
рекламной картинки из слоёв для canvas-редактора. Отвечай ТОЛЬКО JSON без пояснений.

Система координат: x, y, w, h — проценты от ширины/высоты холста (0–100), x/y — левый
верхний угол слоя. fontSize — проценты от ВЫСОТЫ холста (заголовок 6–11, подзаголовок
3.5–5.5, мелкий текст 2.5–3.5). Слои рисуются по порядку: первый — самый нижний.

Типы слоёв:
- text: text, fontSize, color (#rrggbb), weight ("700"|"400"), align (left|center|right),
  font (один из {FONTS}), upper (bool), lineHeight (0.9–1.4), letterSpacing (0–8),
  bgColor (#rrggbb, необязательно — плашка под текстом, напр. кнопка), bgRadius (0–50, % от высоты
  слоя; 50 = «таблетка»), shadow (bool — тень, если текст на фото),
  role: headline | subheadline | cta | badge | disclaimer (обязательно для каждого текста)
- shape: shape (rect|ellipse|triangle|line|polygon|star), fill {{type: solid|gradient|radial,
  color, color2, angle}}, radius (0–60, для rect)
- icon: icon (один из {ICONS}), color
- image: placeholder: true — место под фото товара/человека (пользователь вставит сам)

Правила хорошего креатива: 1 главный оффер крупно, не больше 3 текстовых блоков, текста
мало (Директ режет картинки, где текст > 20% площади), высокий контраст текста и фона,
CTA-кнопка (text с bgColor) заметна, поля от края ≥ 5%, ничего не выходит за холст,
текстовые блоки не перекрывают друг друга. Цифры и выгода — в заголовке. Без
обещаний «гарантированно», «100%», «лучший» (модерация Директа).

Формат ответа: {{"designs": [{{"name": "...", "bg": {{"color": "#rrggbb"}}, "layers": [ ... ]}}]}}"""

COPY_FIELDS = {"headline": "заголовок на картинке (до 40 символов, оффер/выгода, цифры)",
               "subheadline": "подзаголовок (до 70 символов, раскрывает оффер)",
               "cta": "текст кнопки (до 18 символов, глагол: «Узнать цену», «Записаться»)",
               "badge": "плашка-акцент (до 14 символов: «-30%», «Бесплатно», «Новинка»)",
               "disclaimer": "мелкий текст-оговорка (до 90 символов)"}

CRITIQUE_SYSTEM = """Ты — опытный арт-директор и модератор рекламы Яндекс Директа. Оцени рекламную
картинку строго и по делу. Отвечай ТОЛЬКО JSON:
{"score": 1-10, "summary": "одна фраза", "strengths": ["..."],
 "issues": [{"severity": "high|medium|low", "area": "текст|контраст|композиция|CTA|модерация|оффер",
             "problem": "что не так", "fix": "конкретно что сделать"}],
 "better_texts": {"headline": "...", "subheadline": "...", "cta": "..."}}
Проверь: читается ли оффер за 1 секунду, контраст текста, размер шрифтов на телефоне,
перегруз текстом (>20% площади — риск отклонения в Директе), заметность CTA, поля,
запрещённые формулировки (гарантии, «лучший», медицинские обещания), соответствие брифу."""


# ---------------------------------------------------------------- генерация изображений
# Провайдеры по очереди: (1) тот же ИИ-прокси, OpenAI-совместимый /v1/images/* (модель
# STUDIO_IMAGE_MODELS, по умолчанию gemini-3-pro-image,gemini-3.1-flash-image,gpt-image-2); (2) Gemini (GEMINI_API_KEY, модели
# STUDIO_GEMINI_IMAGE_MODELS). Ошибки собираем и отдаём понятной фразой — на 19.11.2026
# у прокси gpt-image-2 «disabled», у Gemini бесплатный тариф без квоты на картинки.
_ASPECT = {"square": "1:1", "wide": "16:9", "landscape": "16:9", "r45": "4:5", "r43": "4:3", "r34": "3:4", "story": "9:16",
           "vk_cover": "21:9", "hd": "16:9", "yt_banner": "16:9", "mp_card": "3:4", "og": "16:9", "slides": "16:9", "a4": "3:4", "bcard": "16:9"}
_OAI_SIZE = {"1:1": "1024x1024", "16:9": "1536x1024", "4:3": "1536x1024", "4:5": "1024x1536", "3:4": "1024x1536", "9:16": "1024x1536"}


class ImageGenError(RuntimeError):
    pass


def _explain(provider: str, status: int, text: str) -> str:
    t = text.lower()
    if "disabled" in t:
        return f"{provider}: модель выключена на стороне сервиса"
    if status == 429 and "free_tier" in t:
        return f"{provider}: бесплатный тариф без квоты на картинки — нужно включить оплату"
    if status == 429:
        return f"{provider}: превышен лимит запросов, попробуйте через минуту"
    if "not supported" in t or status == 404:
        return f"{provider}: модель картинок недоступна"
    if "safety" in t or "blocked" in t or "policy" in t:
        return f"{provider}: запрос отклонён фильтром безопасности — переформулируйте"
    return f"{provider}: ошибка {status}"


async def _gen_proxy(prompt: str, example: tuple[str, bytes] | None, aspect: str, n: int) -> list[bytes]:
    """ИИ-прокси, OpenAI-совместимый /v1/images/generations. Модели картинок у прокси
    асинхронные: 202 + id → опрашиваем GET /v1/images/generations/{id} до ready/failed.
    Проверено 19.11.2026: gemini-3-pro-image — «image_service_unreachable» (временно),
    gemini-3.1-flash-image — «model_not_found» у поставщика, gpt-image-2 — «disabled»."""
    key = os.getenv("STUDIO_AI_API_KEY")
    if not key:
        raise ImageGenError("ИИ-прокси: ключ не задан")
    models = [m.strip() for m in (os.getenv("STUDIO_IMAGE_MODELS") or os.getenv("STUDIO_IMAGE_MODEL")
                                   or "gemini-3-pro-image,gemini-3.1-flash-image,gpt-image-2").split(",") if m.strip()]
    base, headers = _ai_base(), {"Authorization": f"Bearer {key}"}
    errs: list[str] = []
    async with httpx.AsyncClient(timeout=httpx.Timeout(240.0, connect=15.0)) as http:
        for model in models:
            body: dict[str, Any] = {"model": model, "prompt": prompt, "n": n}
            if model.startswith("gemini"):
                body.update(aspect_ratio=aspect, quality="fullhd")
                if example:
                    body["image"] = f"data:{example[0]};base64," + base64.b64encode(example[1]).decode()
            else:
                body["size"] = _OAI_SIZE.get(aspect, "1024x1024")
            if example and not model.startswith("gemini"):
                r = await http.post(base + "/v1/images/edits", headers=headers,
                                    data={k: str(v) for k, v in body.items()},
                                    files={"image": ("example." + example[0].split("/")[-1], example[1], example[0])})
            else:
                r = await http.post(base + "/v1/images/generations", headers=headers, json=body)
            if r.status_code >= 400:
                errs.append(model + " — " + _explain("", r.status_code, r.text).lstrip(": "))
                continue
            j = r.json()
            jid = j.get("id")
            t0 = asyncio.get_running_loop().time()
            while r.status_code == 202 and jid and not j.get("ready") and j.get("status") not in ("failed", "error", "cancelled", "succeeded", "completed"):
                if asyncio.get_running_loop().time() - t0 > 200:
                    break
                await asyncio.sleep(3)
                rr = await http.get(base + "/v1/images/generations/" + jid, headers=headers)
                if rr.status_code >= 400:
                    break
                j = rr.json()
            err = j.get("error") or {}
            if j.get("status") in ("failed", "error") or (err and not j.get("data")):
                code = err.get("code", "")
                errs.append(model + " — " + ("сервис временно недоступен" if code == "image_service_unreachable"
                                             else "не подключена у поставщика" if code == "model_not_found"
                                             else (err.get("message") or code or "генерация не удалась")))
                continue
            out = []
            for d in j.get("data") or []:
                if d.get("b64_json"):
                    out.append(base64.b64decode(d["b64_json"]))
                elif d.get("url"):
                    u = d["url"] if d["url"].startswith("http") else base + d["url"]
                    rr = await http.get(u, headers=headers if u.startswith(base) else None)
                    if rr.status_code == 200:
                        out.append(rr.content)
            if out:
                return out
            errs.append(model + " — " + ("не готова за 3 минуты" if not j.get("ready") else "пустой ответ"))
    raise ImageGenError("ИИ-прокси: " + ("; ".join(errs) or "нет моделей"))


async def _gen_gemini(prompt: str, example: tuple[str, bytes] | None, aspect: str, n: int) -> list[bytes]:
    key = os.getenv("GEMINI_API_KEY")
    if not key:
        raise ImageGenError("Gemini: ключ не задан")
    models = [m.strip() for m in (os.getenv("STUDIO_GEMINI_IMAGE_MODELS") or "gemini-3.1-flash-image,gemini-2.5-flash-image").split(",") if m.strip()]
    parts: list[dict[str, Any]] = []
    if example:
        parts.append({"inlineData": {"mimeType": example[0], "data": base64.b64encode(example[1]).decode()}})
    parts.append({"text": prompt})
    last = "Gemini: нет моделей"
    async with httpx.AsyncClient(timeout=httpx.Timeout(240.0, connect=15.0)) as http:
        for model in models:
            out: list[bytes] = []
            for _ in range(n):
                r = await http.post(f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent",
                                    params={"key": key},
                                    json={"contents": [{"role": "user", "parts": parts}],
                                          "generationConfig": {"responseModalities": ["IMAGE"], "imageConfig": {"aspectRatio": aspect}}})
                if r.status_code >= 400:
                    last = _explain("Gemini", r.status_code, r.text)
                    break
                for c in r.json().get("candidates") or []:
                    for pt in (c.get("content") or {}).get("parts") or []:
                        if pt.get("inlineData", {}).get("data"):
                            out.append(base64.b64decode(pt["inlineData"]["data"]))
            if out:
                return out
    raise ImageGenError(last)


async def generate_images(prompt: str, example: tuple[str, bytes] | None, aspect: str, n: int) -> tuple[list[bytes], str]:
    errors = []
    for name, fn in (("proxy", _gen_proxy), ("gemini", _gen_gemini)):
        try:
            return await fn(prompt, example, aspect, n), name
        except ImageGenError as exc:
            errors.append(str(exc))
        except httpx.HTTPError as exc:
            errors.append(f"{name}: нет связи ({exc.__class__.__name__})")
    raise ImageGenError("Генерация картинок сейчас недоступна. " + "; ".join(errors))


IMAGE_PROMPT_SYSTEM = """Ты — арт-директор рекламы. Пишешь промт для модели генерации изображений
(фон/фото для рекламного баннера). Отвечай ТОЛЬКО JSON: {"prompt": "...", "prompt_ru": "..."}.
prompt — на английском, 60–120 слов: объект, окружение, свет, ракурс, стиль (фотореализм,
если не сказано иное), цветовая гамма, композиция с СВОБОДНЫМ местом под текст (где именно).
Обязательно: no text, no letters, no logos, no watermarks. Без реальных брендов и знаменитостей.
prompt_ru — тот же промт кратко по-русски для человека. Если дан пример картинки — опиши её
стиль, свет, композицию и палитру и сделай промт для НОВОГО изображения в этом стиле."""


# ---------------------------------------------------------------- удаление фона, шрифты
_REMBG_PY = Path(os.getenv("STUDIO_REMBG_PYTHON", str(Path.home() / "ParsYa/.venv-rembg/bin/python")))
_REMBG_WORKER = Path(__file__).resolve().parent.parent / "tools" / "rembg_worker.py"
_rembg_lock = asyncio.Semaphore(1)


async def remove_background(raw: bytes) -> bytes:
    """Фон вырезает rembg (модель isnet-general-use) в отдельном окружении — по одной задаче за раз."""
    if not _REMBG_PY.exists():
        raise HTTPException(status_code=503, detail="Удаление фона не установлено на сервере")
    async with _rembg_lock:
        proc = await asyncio.create_subprocess_exec(str(_REMBG_PY), str(_REMBG_WORKER), os.getenv("STUDIO_REMBG_MODEL", "isnet-general-use"),
                                                    stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
        try:
            out, err = await asyncio.wait_for(proc.communicate(raw), 180)
        except asyncio.TimeoutError:
            proc.kill()
            raise HTTPException(status_code=504, detail="Удаление фона заняло слишком долго — попробуйте фото поменьше")
    if proc.returncode != 0 or not out.startswith(b"\x89PNG"):
        raise HTTPException(status_code=502, detail="Не удалось удалить фон: " + err.decode(errors="replace")[-200:])
    return out


def font_format(data: bytes) -> str | None:
    head = data[:4]
    if head in (b"\x00\x01\x00\x00", b"true"):
        return "truetype"
    if head == b"OTTO":
        return "opentype"
    if head == b"wOFF":
        return "woff"
    if head == b"wOF2":
        return "woff2"
    return None


_FONT_MIME = {"truetype": "font/ttf", "opentype": "font/otf", "woff": "font/woff", "woff2": "font/woff2"}


# ---------------------------------------------------------------- картинки по ссылке
def _public_host(host: str) -> bool:
    try:
        infos = socket.getaddrinfo(host, None)
    except OSError:
        return False
    for info in infos:
        ip = ipaddress.ip_address(info[4][0])
        if ip.is_private or ip.is_loopback or ip.is_link_local or ip.is_reserved or ip.is_multicast:
            return False
    return True


def _to_data_url(raw: bytes, max_dim: int) -> tuple[str, int, int]:
    from PIL import Image  # локальный импорт: Pillow нужен только здесь
    img = Image.open(io.BytesIO(raw))
    img.load()
    w, h = img.size
    if max(w, h) > max_dim:
        k = max_dim / max(w, h)
        img = img.resize((max(1, int(w * k)), max(1, int(h * k))), Image.LANCZOS)
    buf = io.BytesIO()
    if img.mode in ("RGBA", "LA", "P") and ("transparency" in img.info or img.mode != "P"):
        img.convert("RGBA").save(buf, "PNG", optimize=True)
        mime = "image/png"
    else:
        img.convert("RGB").save(buf, "JPEG", quality=88)
        mime = "image/jpeg"
    return f"data:{mime};base64," + base64.b64encode(buf.getvalue()).decode(), img.size[0], img.size[1]


def _data_url_parts(data_url: str) -> tuple[str, str]:
    m = re.match(r"^data:(image/(?:png|jpeg|jpg|webp|gif));base64,(.+)$", data_url.strip(), re.S)
    if not m:
        raise HTTPException(status_code=422, detail="Ожидается картинка data:image/png|jpeg;base64,…")
    mime = "image/jpeg" if m.group(1) == "image/jpg" else m.group(1)
    return mime, m.group(2)


# ---------------------------------------------------------------- router
def build_router(principal_dep: Callable, require_user: Callable, data_dir: Path) -> APIRouter:
    db_path = _db_path(data_dir)
    init_db(db_path)
    router = APIRouter(prefix="/api/v1/studio")

    def uid(principal: dict) -> int:
        return require_user(principal)

    teams_init_db(db_path)

    def own(c: sqlite3.Connection, design_id: int, user_id: int, need: str = "editor") -> sqlite3.Row:
        """Автор — всё; дизайн команды — по роли (viewer читает, editor правит, admin удаляет чужое)."""
        r = c.execute("SELECT * FROM studio_designs WHERE id=?", (design_id,)).fetchone()
        if r and r["user_id"] == user_id:
            return r
        role = member_role(c, r["team_id"], user_id) if r and r["team_id"] else None
        if not role:
            raise HTTPException(status_code=404, detail="Дизайн не найден")
        if TEAM_RANK[role] < TEAM_RANK[need]:
            raise HTTPException(status_code=403, detail="В этой команде у вас роль «наблюдатель» — можно только смотреть")
        return r

    def check_size(p: DesignIn) -> str:
        data = json.dumps(p.data, ensure_ascii=False, separators=(",", ":"))
        if len(data) > MAX_DESIGN_BYTES:
            raise HTTPException(status_code=413, detail="Дизайн слишком большой (картинки > 30 МБ) — уменьшите фото")
        return data

    def fmt(v: str) -> str:
        return v if v in FORMATS else "square"

    @router.get("/designs")
    def list_designs(q: str = "", team: int | None = None, principal: dict = Depends(principal_dep)):
        user_id = uid(principal)
        cols = "id,name,format,thumb,created_at,updated_at,team_id,updated_by,user_id"
        with closing(_connect(db_path)) as c:
            if team:
                if not member_role(c, team, user_id):
                    raise HTTPException(status_code=404, detail="Команда не найдена")
                where, args = "team_id=? AND is_template=0", [team]
            else:
                where, args = "user_id=? AND team_id IS NULL AND is_template=0", [user_id]
            if q.strip():
                where += " AND name LIKE ?"
                args.append(f"%{q.strip()}%")
            rows = c.execute(f"SELECT {cols} FROM studio_designs WHERE {where} ORDER BY updated_at DESC", args).fetchall()
        return {"designs": [_meta(r) for r in rows]}

    @router.get("/designs/{design_id}")
    def get_design(design_id: int, principal: dict = Depends(principal_dep)):
        user_id = uid(principal)
        with closing(_connect(db_path)) as c:
            r = own(c, design_id, user_id, "viewer")
            role = "author" if r["user_id"] == user_id else member_role(c, r["team_id"], user_id)
        return {**_meta(r), "data": json.loads(r["data"]), "access": role}

    @router.post("/designs")
    def create_design(p: DesignIn, principal: dict = Depends(principal_dep)):
        user_id, data, now = uid(principal), check_size(p), time.time()
        with closing(_connect(db_path)) as c:
            if p.local_id:
                ex = c.execute("SELECT * FROM studio_designs WHERE user_id=? AND local_id=?", (user_id, p.local_id)).fetchone()
                if ex:
                    return _meta(ex)
            if p.team_id:
                role = member_role(c, p.team_id, user_id)
                if not role or TEAM_RANK[role] < TEAM_RANK["editor"]:
                    raise HTTPException(status_code=403, detail="Создавать дизайны в команде может редактор и выше")
            cur = c.execute("INSERT INTO studio_designs(user_id,local_id,name,format,data,thumb,created_at,updated_at,team_id,updated_by) "
                            "VALUES(?,?,?,?,?,?,?,?,?,?)", (user_id, p.local_id, p.name.strip()[:200] or "Новый дизайн",
                                                        fmt(p.format), data, p.thumb, now, now, p.team_id or None, user_id))
            c.commit()
            r = own(c, cur.lastrowid, user_id)
        return _meta(r)

    @router.put("/designs/{design_id}")
    def update_design(design_id: int, p: DesignIn, principal: dict = Depends(principal_dep)):
        user_id, data = uid(principal), check_size(p)
        with closing(_connect(db_path)) as c:
            cur = own(c, design_id, user_id)
            if (p.base_updated_at is not None and cur["updated_at"] > p.base_updated_at + 0.001
                    and cur["updated_by"] and cur["updated_by"] != user_id):
                raise HTTPException(status_code=409, detail={"conflict": True, "updated_by": cur["updated_by"],
                                                             "updated_at": cur["updated_at"]})
            # История версий: снимок прежнего состояния не чаще раза в 10 минут, храним 30 последних.
            last = c.execute("SELECT MAX(created_at) FROM studio_versions WHERE design_id=?", (design_id,)).fetchone()[0]
            if cur["data"] != data and (not last or time.time() - last >= VERSION_EVERY):
                c.execute("INSERT INTO studio_versions(design_id,data,thumb,created_at) VALUES(?,?,?,?)",
                          (design_id, cur["data"], cur["thumb"], cur["updated_at"]))
                c.execute("DELETE FROM studio_versions WHERE design_id=? AND id NOT IN (SELECT id FROM studio_versions "
                          "WHERE design_id=? ORDER BY created_at DESC LIMIT 30)", (design_id, design_id))
            c.execute("UPDATE studio_designs SET name=?,format=?,data=?,thumb=CASE WHEN ?<>'' THEN ? ELSE thumb END,"
                      "updated_at=?,updated_by=? WHERE id=?", (p.name.strip()[:200] or "Новый дизайн", fmt(p.format), data,
                                                  p.thumb, p.thumb, time.time(), user_id, design_id))
            c.commit()
            r = own(c, design_id, user_id)
        return _meta(r)

    @router.patch("/designs/{design_id}")
    def rename_design(design_id: int, p: RenameIn, principal: dict = Depends(principal_dep)):
        user_id = uid(principal)
        with closing(_connect(db_path)) as c:
            own(c, design_id, user_id)
            c.execute("UPDATE studio_designs SET name=?, updated_at=? WHERE id=?", (p.name.strip()[:200], time.time(), design_id))
            c.commit()
            r = own(c, design_id, user_id)
        return _meta(r)

    @router.put("/designs/{design_id}/team")
    def move_design(design_id: int, p: MoveIn, principal: dict = Depends(principal_dep)):
        """Перенести дизайн в команду или обратно в личные (только автор или админ команды)."""
        user_id = uid(principal)
        with closing(_connect(db_path)) as c:
            r = own(c, design_id, user_id, "viewer")
            if r["user_id"] != user_id and TEAM_RANK.get(member_role(c, r["team_id"], user_id) or "", -1) < TEAM_RANK["admin"]:
                raise HTTPException(status_code=403, detail="Переносить чужой дизайн может только админ команды")
            if p.team_id:
                role = member_role(c, p.team_id, user_id)
                if not role or TEAM_RANK[role] < TEAM_RANK["editor"]:
                    raise HTTPException(status_code=403, detail="Нужна роль редактора в этой команде")
            c.execute("UPDATE studio_designs SET team_id=?, updated_at=?, updated_by=? WHERE id=?",
                      (p.team_id or None, time.time(), user_id, design_id))
            c.commit()
            return _meta(c.execute("SELECT * FROM studio_designs WHERE id=?", (design_id,)).fetchone())

    @router.post("/designs/{design_id}/duplicate")
    def duplicate_design(design_id: int, principal: dict = Depends(principal_dep)):
        user_id, now = uid(principal), time.time()
        with closing(_connect(db_path)) as c:
            r = own(c, design_id, user_id, "viewer")
            team = r["team_id"] if r["team_id"] and TEAM_RANK.get(member_role(c, r["team_id"], user_id) or "", -1) >= TEAM_RANK["editor"] else None
            cur = c.execute("INSERT INTO studio_designs(user_id,name,format,data,thumb,created_at,updated_at,team_id,updated_by) "
                            "VALUES(?,?,?,?,?,?,?,?,?)", (user_id, (r["name"] + " (копия)")[:200], r["format"], r["data"],
                                                      r["thumb"], now, now, team, user_id))
            c.commit()
            n = own(c, cur.lastrowid, user_id)
        return _meta(n)

    @router.post("/designs/{design_id}/as-template")
    def as_template(design_id: int, p: AsTemplateIn, principal: dict = Depends(principal_dep)):
        """Копия дизайна в «Мои шаблоны» (сам дизайн не меняется)."""
        user_id, now = uid(principal), time.time()
        with closing(_connect(db_path)) as c:
            r = own(c, design_id, user_id, "viewer")
            if p.team_id:
                role = member_role(c, p.team_id, user_id)
                if not role or TEAM_RANK[role] < TEAM_RANK["editor"]:
                    raise HTTPException(status_code=403, detail="Шаблоны команды сохраняет редактор и выше")
            cur = c.execute("INSERT INTO studio_designs(user_id,name,format,data,thumb,created_at,updated_at,is_template,category,team_id,updated_by) "
                            "VALUES(?,?,?,?,?,?,?,1,?,?,?)", (user_id, (p.name.strip() or r["name"])[:200], r["format"], r["data"],
                                                         r["thumb"], now, now, p.category.strip()[:60] or "Мои", p.team_id or None, user_id))
            c.commit()
            return _meta(own(c, cur.lastrowid, user_id))

    @router.get("/templates")
    def list_templates(principal: dict = Depends(principal_dep)):
        user_id = uid(principal)
        with closing(_connect(db_path)) as c:
            teams = [r["team_id"] for r in c.execute("SELECT team_id FROM team_members WHERE user_id=?", (user_id,))]
            q = ("SELECT id,name,format,thumb,created_at,updated_at,category,team_id,user_id FROM studio_designs WHERE is_template=1 AND "
                 "((user_id=? AND team_id IS NULL)" + (f" OR team_id IN ({','.join('?' * len(teams))})" if teams else "") + ") ORDER BY updated_at DESC")
            rows = c.execute(q, [user_id, *teams]).fetchall()
        return {"templates": [_meta(r) for r in rows]}

    @router.get("/designs/{design_id}/versions")
    def list_versions(design_id: int, principal: dict = Depends(principal_dep)):
        user_id = uid(principal)
        with closing(_connect(db_path)) as c:
            own(c, design_id, user_id, "viewer")
            rows = c.execute("SELECT id,thumb,created_at FROM studio_versions WHERE design_id=? ORDER BY created_at DESC",
                             (design_id,)).fetchall()
        return {"versions": [dict(r) for r in rows]}

    @router.post("/designs/{design_id}/versions/{version_id}/restore")
    def restore_version(design_id: int, version_id: int, principal: dict = Depends(principal_dep)):
        user_id, now = uid(principal), time.time()
        with closing(_connect(db_path)) as c:
            cur = own(c, design_id, user_id)
            v = c.execute("SELECT * FROM studio_versions WHERE id=? AND design_id=?", (version_id, design_id)).fetchone()
            if not v:
                raise HTTPException(status_code=404, detail="Версия не найдена")
            c.execute("INSERT INTO studio_versions(design_id,data,thumb,created_at) VALUES(?,?,?,?)",
                      (design_id, cur["data"], cur["thumb"], cur["updated_at"]))
            c.execute("UPDATE studio_designs SET data=?, thumb=?, updated_at=? WHERE id=?", (v["data"], v["thumb"], now, design_id))
            c.commit()
            r = own(c, design_id, user_id)
        return {**_meta(r), "data": json.loads(r["data"])}

    # ------------------------------------------------ ссылка на просмотр (публичная страница /app/share.html?t=…)
    def share_meta(r: sqlite3.Row | None) -> dict[str, Any]:
        if not r:
            return {"token": None}
        return {"token": r["token"], "views": r["views"], "pages": len(json.loads(r["pages"])),
                "created_at": r["created_at"], "updated_at": r["updated_at"]}

    @router.get("/designs/{design_id}/share")
    def get_share(design_id: int, principal: dict = Depends(principal_dep)):
        user_id = uid(principal)
        with closing(_connect(db_path)) as c:
            own(c, design_id, user_id, "viewer")
            r = c.execute("SELECT * FROM studio_shares WHERE design_id=?", (design_id,)).fetchone()
        return share_meta(r)

    @router.post("/designs/{design_id}/share")
    async def put_share(design_id: int, p: ShareIn, principal: dict = Depends(principal_dep)):
        user_id = uid(principal)
        pages: list[str] = []
        for u in p.pages:
            if len(u) > SHARE_PAGE_MAX:
                raise HTTPException(status_code=413, detail="Страница слишком тяжёлая для ссылки")
            _mime, b64 = _data_url_parts(u)
            try:
                # перекодируем: по ссылке отдаются только настоящие картинки
                data_url, _w, _h = await asyncio.to_thread(_to_data_url, base64.b64decode(b64), 1600)
            except Exception as exc:  # noqa: BLE001
                raise HTTPException(status_code=422, detail="Страница не похожа на картинку") from exc
            pages.append(data_url)
        now = time.time()
        with closing(_connect(db_path)) as c:
            d = own(c, design_id, user_id)
            old = c.execute("SELECT token FROM studio_shares WHERE design_id=?", (design_id,)).fetchone()
            if old:
                c.execute("UPDATE studio_shares SET pages=?, name=?, format=?, updated_at=? WHERE token=?",
                          (json.dumps(pages), d["name"], d["format"], now, old["token"]))
                token = old["token"]
            else:
                token = secrets.token_urlsafe(18)
                c.execute("INSERT INTO studio_shares(token,user_id,design_id,name,format,pages,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?)",
                          (token, user_id, design_id, d["name"], d["format"], json.dumps(pages), now, now))
            c.commit()
            r = c.execute("SELECT * FROM studio_shares WHERE token=?", (token,)).fetchone()
        return share_meta(r)

    @router.delete("/designs/{design_id}/share")
    def delete_share(design_id: int, principal: dict = Depends(principal_dep)):
        user_id = uid(principal)
        with closing(_connect(db_path)) as c:
            own(c, design_id, user_id)
            c.execute("DELETE FROM studio_shares WHERE design_id=?", (design_id,))
            c.commit()
        return {"token": None}

    @router.get("/share/{token}")
    def public_share(token: str):
        """Без входа: только имя дизайна и картинки страниц, которые владелец сам отправил по ссылке."""
        if not SHARE_TOKEN_RE.match(token):
            raise HTTPException(status_code=404, detail="Ссылка не найдена")
        with closing(_connect(db_path)) as c:
            r = c.execute("SELECT * FROM studio_shares WHERE token=?", (token,)).fetchone()
            if not r:
                raise HTTPException(status_code=404, detail="Ссылка не найдена или отключена")
            c.execute("UPDATE studio_shares SET views=views+1 WHERE token=?", (token,))
            c.commit()
        w, h = FORMATS.get(r["format"]) or (1080, 1080)
        return {"name": r["name"], "format": r["format"], "width": w, "height": h,
                "pages": json.loads(r["pages"]), "updated_at": r["updated_at"]}

    # ------------------------------------------------ видео для слоёв (файлы на диске; ссылка с ключом — для <video src>)
    video_dir = Path(db_path).parent / "studio_videos"
    video_dir.mkdir(parents=True, exist_ok=True)

    @router.post("/videos")
    def add_video(p: VideoIn, principal: dict = Depends(principal_dep)):
        uid(principal)
        m = re.match(r"^data:((?:video|audio)/(mp4|webm|quicktime|mpeg|mp3|ogg|wav|x-wav|aac|x-m4a));base64,(.+)$", p.data_url.strip(), re.S)
        if not m:
            raise HTTPException(status_code=422, detail="Нужно видео (MP4, WebM, MOV) или звук (MP3, M4A, OGG, WAV)")
        raw = base64.b64decode(m.group(3))
        if len(raw) > VIDEO_MAX:
            raise HTTPException(status_code=413, detail="Файл больше 80 МБ — сократите или сожмите")
        return save_media(raw, m.group(1))

    MEDIA_EXT = {"video/mp4": "mp4", "video/webm": "webm", "video/quicktime": "mov", "audio/mpeg": "mp3", "audio/mp3": "mp3",
                 "audio/mp4": "m4a", "audio/x-m4a": "m4a", "audio/aac": "m4a", "audio/ogg": "ogg", "audio/wav": "wav",
                 "audio/x-wav": "wav", "audio/webm": "weba"}
    EXT_MIME = {"mp4": "video/mp4", "webm": "video/webm", "mov": "video/quicktime", "mp3": "audio/mpeg", "m4a": "audio/mp4",
                "ogg": "audio/ogg", "wav": "audio/wav", "weba": "audio/webm"}

    def save_media(raw: bytes, mime: str) -> dict:
        vid, key = secrets.token_urlsafe(10), secrets.token_urlsafe(18)
        ext = MEDIA_EXT.get(mime, "mp4")
        (video_dir / f"{vid}.{key}.{ext}").write_bytes(raw)
        return {"id": vid, "key": key, "ext": ext, "size": len(raw), "url": f"/api/v1/studio/videos/{vid}.{ext}?k={key}"}

    @router.get("/stock-audio")
    async def stock_audio(q: str, page: int = 1, principal: dict = Depends(principal_dep)):
        """Бесплатная музыка и звуки (Openverse: Jamendo, Freesound… — лицензии с коммерческим использованием)."""
        uid(principal)
        if len(q.strip()) < 2:
            return {"results": []}
        try:
            async with httpx.AsyncClient(timeout=httpx.Timeout(25.0, connect=10.0), headers={"User-Agent": "ParsYaStudio/1.0"}) as http:
                r = await http.get("https://api.openverse.org/v1/audio/", params={
                    "q": q.strip()[:120], "page": max(1, min(page, 10)), "page_size": 20,
                    "license_type": "commercial,modification", "mature": "false"})
        except httpx.HTTPError as exc:
            raise HTTPException(status_code=502, detail="Сток музыки не отвечает") from exc
        if r.status_code >= 400:
            raise HTTPException(status_code=502, detail=f"Сток музыки: ошибка {r.status_code}")
        out = [{"id": x.get("id"), "url": x.get("url"), "title": _txt(x.get("title"), 120), "creator": _txt(x.get("creator"), 80),
                "duration": round((x.get("duration") or 0) / 1000, 1), "source": x.get("source"),
                "license": (x.get("license") or "").upper() + (" " + x["license_version"] if x.get("license_version") else ""),
                "landing": x.get("foreign_landing_url"), "attribution_needed": (x.get("license") or "") not in ("cc0", "pdm")}
               for x in r.json().get("results") or [] if x.get("url")]
        return {"results": out}

    @router.post("/stock-audio/fetch")
    async def stock_audio_fetch(p: AudioFetchIn, principal: dict = Depends(principal_dep)):
        """Скачать выбранный трек на сервер (браузер не может взять его напрямую из-за CORS)."""
        uid(principal)
        u = urlsplit(p.url)
        if u.scheme not in ("http", "https") or not u.hostname or not await asyncio.to_thread(_public_host, u.hostname):
            raise HTTPException(status_code=422, detail="Неверная ссылка на трек")
        try:
            async with httpx.AsyncClient(timeout=httpx.Timeout(60.0, connect=10.0), follow_redirects=True,
                                         headers={"User-Agent": "ParsYaStudio/1.0"}) as http:
                async with http.stream("GET", p.url) as r:
                    if r.status_code >= 400:
                        raise HTTPException(status_code=502, detail=f"Трек недоступен ({r.status_code})")
                    if r.url.host and not await asyncio.to_thread(_public_host, r.url.host):
                        raise HTTPException(status_code=422, detail="Неверная ссылка на трек")
                    buf = bytearray()
                    async for chunk in r.aiter_bytes():
                        buf += chunk
                        if len(buf) > 25_000_000:
                            raise HTTPException(status_code=413, detail="Трек больше 25 МБ")
                    ctype = (r.headers.get("content-type") or "").split(";")[0].strip().lower()
        except httpx.HTTPError as exc:
            raise HTTPException(status_code=502, detail="Трек не скачался") from exc
        if not ctype.startswith("audio/"):
            ctype = "audio/mpeg" if bytes(buf[:3]) == b"ID3" or bytes(buf[:2]) in (b"\xff\xfb", b"\xff\xf3") else ctype
        if ctype not in MEDIA_EXT:
            raise HTTPException(status_code=422, detail="Это не аудиофайл")
        return save_media(bytes(buf), ctype)

    @router.get("/videos/{name}")
    def get_video(name: str, k: str = ""):
        """Без входа, но по ссылке-ключу (тег <video> не умеет отправлять токен)."""
        m = re.match(r"^([A-Za-z0-9_-]{8,20})\.(mp4|webm|mov|mp3|m4a|ogg|wav|weba)$", name)
        if not m or not re.match(r"^[A-Za-z0-9_-]{16,40}$", k):
            raise HTTPException(status_code=404, detail="Видео не найдено")
        f = video_dir / f"{m.group(1)}.{k}.{m.group(2)}"
        if not f.exists():
            raise HTTPException(status_code=404, detail="Видео не найдено")
        from fastapi.responses import FileResponse
        return FileResponse(f, media_type=EXT_MIME[m.group(2)],
                            headers={"Cache-Control": "private, max-age=86400"})

    @router.get("/uploads")
    def list_uploads(principal: dict = Depends(principal_dep)):
        user_id = uid(principal)
        with closing(_connect(db_path)) as c:
            rows = c.execute("SELECT id,name,thumb,width,height,created_at FROM studio_uploads WHERE user_id=? "
                             "ORDER BY created_at DESC LIMIT 300", (user_id,)).fetchall()
        return {"uploads": [dict(r) for r in rows]}

    @router.get("/uploads/{upload_id}")
    def get_upload(upload_id: int, principal: dict = Depends(principal_dep)):
        user_id = uid(principal)
        with closing(_connect(db_path)) as c:
            r = c.execute("SELECT * FROM studio_uploads WHERE id=? AND user_id=?", (upload_id, user_id)).fetchone()
        if not r:
            raise HTTPException(status_code=404, detail="Файл не найден")
        return {"id": r["id"], "name": r["name"], "data_url": r["data"], "width": r["width"], "height": r["height"]}

    @router.post("/uploads")
    async def add_upload(p: UploadIn, principal: dict = Depends(principal_dep)):
        user_id = uid(principal)
        _mime, b64 = _data_url_parts(p.data_url)
        try:
            raw = base64.b64decode(b64)
            data_url, w, h = await asyncio.to_thread(_to_data_url, raw, 2000)
            thumb, _, _ = await asyncio.to_thread(_to_data_url, raw, 240)
        except Exception as exc:  # noqa: BLE001
            raise HTTPException(status_code=422, detail="Файл не похож на картинку") from exc
        with closing(_connect(db_path)) as c:
            cur = c.execute("INSERT INTO studio_uploads(user_id,name,data,thumb,width,height,created_at) VALUES(?,?,?,?,?,?,?)",
                            (user_id, p.name.strip()[:200], data_url, thumb, w, h, time.time()))
            c.commit()
        return {"id": cur.lastrowid, "name": p.name.strip()[:200], "thumb": thumb, "width": w, "height": h, "data_url": data_url}

    @router.delete("/uploads/{upload_id}")
    def delete_upload(upload_id: int, principal: dict = Depends(principal_dep)):
        user_id = uid(principal)
        with closing(_connect(db_path)) as c:
            n = c.execute("DELETE FROM studio_uploads WHERE id=? AND user_id=?", (upload_id, user_id)).rowcount
            c.commit()
        if not n:
            raise HTTPException(status_code=404, detail="Файл не найден")
        return {"ok": True}

    @router.get("/stock")
    async def stock(q: str, page: int = 1, principal: dict = Depends(principal_dep)):
        """Бесплатные фото (Openverse, лицензии, допускающие коммерческое использование)."""
        uid(principal)
        if len(q.strip()) < 2:
            return {"results": [], "total": 0}
        try:
            async with httpx.AsyncClient(timeout=httpx.Timeout(25.0, connect=10.0), headers={"User-Agent": "ParsYaStudio/1.0"}) as http:
                r = await http.get("https://api.openverse.org/v1/images/", params={
                    "q": q.strip()[:120], "page": max(1, min(page, 20)), "page_size": 20,
                    "license_type": "commercial,modification", "mature": "false"})
        except httpx.HTTPError as exc:
            raise HTTPException(status_code=502, detail="Сток фото не отвечает") from exc
        if r.status_code >= 400:
            raise HTTPException(status_code=502, detail=f"Сток фото: ошибка {r.status_code}")
        j = r.json()
        out = [{"id": x.get("id"), "thumb": x.get("thumbnail") or x.get("url"), "url": x.get("url"),
                "title": _txt(x.get("title"), 120), "creator": _txt(x.get("creator"), 80),
                "license": (x.get("license") or "").upper() + (" " + x["license_version"] if x.get("license_version") else ""),
                "license_url": x.get("license_url"), "landing": x.get("foreign_landing_url"),
                "attribution_needed": (x.get("license") or "") not in ("cc0", "pdm")}
               for x in j.get("results") or [] if x.get("url")]
        return {"results": out, "total": j.get("result_count", len(out))}

    @router.delete("/designs/{design_id}")
    def delete_design(design_id: int, principal: dict = Depends(principal_dep)):
        user_id = uid(principal)
        with closing(_connect(db_path)) as c:
            r = own(c, design_id, user_id, "viewer")
            if r["user_id"] != user_id and TEAM_RANK.get(member_role(c, r["team_id"], user_id) or "", -1) < TEAM_RANK["admin"]:
                raise HTTPException(status_code=403, detail="Чужой дизайн команды удаляет только админ или автор")
            c.execute("DELETE FROM studio_designs WHERE id=?", (design_id,))
            c.execute("DELETE FROM studio_versions WHERE design_id=?", (design_id,))
            c.execute("DELETE FROM studio_shares WHERE design_id=?", (design_id,))
            c.commit()
        return {"ok": True}

    @router.post("/import")
    def import_designs(p: ImportIn, principal: dict = Depends(principal_dep)):
        """Перенос дизайнов из localStorage (повторный вызов не создаёт дублей — по local_id)."""
        user_id, mapping = uid(principal), {}
        with closing(_connect(db_path)) as c:
            for d in p.designs:
                if not d.local_id:
                    continue
                data = check_size(d)
                ex = c.execute("SELECT id FROM studio_designs WHERE user_id=? AND local_id=?", (user_id, d.local_id)).fetchone()
                if ex:
                    mapping[d.local_id] = ex["id"]
                    continue
                ts = float(d.data.get("updatedAt") or 0) / 1000 or time.time()
                cur = c.execute("INSERT INTO studio_designs(user_id,local_id,name,format,data,thumb,created_at,updated_at) "
                                "VALUES(?,?,?,?,?,?,?,?)", (user_id, d.local_id, d.name.strip()[:200] or "Новый дизайн",
                                                            fmt(d.format), data, d.thumb, ts, ts))
                mapping[d.local_id] = cur.lastrowid
            c.commit()
        return {"imported": mapping}

    @router.post("/fetch-image")
    async def fetch_image(p: FetchImageIn, principal: dict = Depends(principal_dep)):
        """Картинка по ссылке → data URL (чужие домены не дают CORS и «пачкают» canvas)."""
        uid(principal)
        u = urlsplit(p.url.strip())
        if u.scheme not in ("http", "https") or not u.hostname:
            raise HTTPException(status_code=422, detail="Нужна ссылка http(s)")
        if not await asyncio.to_thread(_public_host, u.hostname):
            raise HTTPException(status_code=422, detail="Адрес недоступен")
        try:
            async with httpx.AsyncClient(timeout=httpx.Timeout(30.0, connect=10.0), follow_redirects=True,
                                         headers={"User-Agent": "Mozilla/5.0 ParsYaStudio"}) as http:
                async with http.stream("GET", p.url.strip()) as r:
                    if r.status_code >= 400:
                        raise HTTPException(status_code=422, detail=f"Картинка не скачалась: HTTP {r.status_code}")
                    if not await asyncio.to_thread(_public_host, r.url.host or ""):
                        raise HTTPException(status_code=422, detail="Адрес недоступен")
                    buf = bytearray()
                    async for chunk in r.aiter_bytes():
                        buf += chunk
                        if len(buf) > MAX_IMAGE_BYTES:
                            raise HTTPException(status_code=413, detail="Картинка больше 12 МБ")
        except httpx.HTTPError as exc:
            raise HTTPException(status_code=422, detail=f"Картинка не скачалась: {exc.__class__.__name__}") from exc
        try:
            data_url, w, h = await asyncio.to_thread(_to_data_url, bytes(buf), p.max_dim)
        except Exception as exc:  # noqa: BLE001 - Pillow: не картинка/битый файл
            raise HTTPException(status_code=422, detail="По ссылке не картинка или файл повреждён") from exc
        return {"data_url": data_url, "width": w, "height": h}

    @router.post("/remove-bg")
    async def remove_bg(p: RemoveBgIn, principal: dict = Depends(principal_dep)):
        uid(principal)
        _mime, b64 = _data_url_parts(p.image)
        try:
            raw = base64.b64decode(b64)
        except ValueError as exc:
            raise HTTPException(status_code=422, detail="Картинка повреждена") from exc
        png = await remove_background(raw)
        from PIL import Image
        with Image.open(io.BytesIO(png)) as im:
            w, h = im.size
        return {"data_url": "data:image/png;base64," + base64.b64encode(png).decode(), "width": w, "height": h}

    @router.get("/fonts")
    def list_fonts(principal: dict = Depends(principal_dep)):
        user_id = uid(principal)
        with closing(_connect(db_path)) as c:
            rows = c.execute("SELECT id,family,filename,fmt,created_at FROM studio_fonts WHERE user_id=? ORDER BY family",
                             (user_id,)).fetchall()
        return {"fonts": [dict(r) for r in rows]}

    @router.get("/fonts/{font_id}/file")
    def font_file(font_id: int, principal: dict = Depends(principal_dep)):
        from fastapi.responses import Response
        user_id = uid(principal)
        with closing(_connect(db_path)) as c:
            r = c.execute("SELECT fmt,data FROM studio_fonts WHERE id=? AND user_id=?", (font_id, user_id)).fetchone()
        if not r:
            raise HTTPException(status_code=404, detail="Шрифт не найден")
        return Response(content=bytes(r["data"]), media_type=_FONT_MIME.get(r["fmt"], "application/octet-stream"),
                        headers={"Cache-Control": "private, max-age=86400"})

    @router.post("/fonts")
    def add_font(p: FontIn, principal: dict = Depends(principal_dep)):
        user_id = uid(principal)
        try:
            data = base64.b64decode(p.data_base64.split(",", 1)[-1])
        except ValueError as exc:
            raise HTTPException(status_code=422, detail="Файл повреждён") from exc
        if len(data) > 8_000_000:
            raise HTTPException(status_code=413, detail="Шрифт больше 8 МБ")
        fmt = font_format(data)
        if not fmt:
            raise HTTPException(status_code=422, detail="Это не шрифт: нужен TTF, OTF, WOFF или WOFF2")
        family = re.sub(r"[^\w \-]+", "", p.family, flags=re.U).strip()[:60] or "Мой шрифт"
        with closing(_connect(db_path)) as c:
            # lower() в SQLite не понимает кириллицу — сравниваем в Python
            if any(r[0].casefold() == family.casefold() for r in c.execute("SELECT family FROM studio_fonts WHERE user_id=?", (user_id,))):
                raise HTTPException(status_code=409, detail=f"Шрифт «{family}» уже загружен")
            cur = c.execute("INSERT INTO studio_fonts(user_id,family,filename,fmt,data,created_at) VALUES(?,?,?,?,?,?)",
                            (user_id, family, p.filename[:200], fmt, data, time.time()))
            c.commit()
        return {"id": cur.lastrowid, "family": family, "fmt": fmt}

    @router.delete("/fonts/{font_id}")
    def delete_font(font_id: int, principal: dict = Depends(principal_dep)):
        user_id = uid(principal)
        with closing(_connect(db_path)) as c:
            n = c.execute("DELETE FROM studio_fonts WHERE id=? AND user_id=?", (font_id, user_id)).rowcount
            c.commit()
        if not n:
            raise HTTPException(status_code=404, detail="Шрифт не найден")
        return {"ok": True}

    @router.get("/ai/status")
    def ai_status(principal: dict = Depends(principal_dep)):
        uid(principal)
        return {"configured": ai_configured(), "model": _ai_model(), "fonts": FONTS, "icons": ICONS,
                "formats": {k: {"w": v[0], "h": v[1]} for k, v in FORMATS.items()}}

    def ai_fail(exc: Exception) -> HTTPException:
        return HTTPException(status_code=503 if "не настроен" in str(exc) else 502, detail=str(exc))

    @router.post("/ai/copy")
    async def ai_copy(p: AICopyIn, principal: dict = Depends(principal_dep)):
        uid(principal)
        fields = [f for f in p.fields if f in COPY_FIELDS] or ["headline", "subheadline", "cta"]
        if not p.brief.strip() and not any(p.current.values()):
            raise HTTPException(status_code=422, detail="Опишите, что рекламируем (бриф), или заполните текст на макете")
        spec = "\n".join(f'- "{f}": {COPY_FIELDS[f]}' for f in fields)
        cur = "\n".join(f"{k}: {v}" for k, v in p.current.items() if v)
        user = (f"Бриф: {p.brief.strip() or '—'}\n" + (f"Тон: {p.tone}\n" if p.tone else "") +
                (f"Сейчас на макете:\n{cur}\n" if cur else "") +
                f"\nНапиши {p.count} РАЗНЫХ вариантов (разные углы: выгода, боль, цифры, срочность, доверие).\n"
                f"Поля каждого варианта:\n{spec}\n\nОтвет: "
                + json.dumps({"variants": [{f: "..." for f in fields}]}, ensure_ascii=False))
        try:
            raw = parse_json(await ai_complete(
                "Ты — сильный копирайтер performance-рекламы на русском. Коротко, конкретно, без воды и "
                "без запрещённых Директом обещаний (гарантии, «лучший», «№1»). Отвечай только JSON.", user, 3000))
        except AIError as exc:
            raise ai_fail(exc) from exc
        lim = {"headline": 60, "subheadline": 110, "cta": 28, "badge": 20, "disclaimer": 140}
        variants = []
        for v in (raw.get("variants") if isinstance(raw, dict) else raw) or []:
            if isinstance(v, dict):
                item = {f: _txt(v.get(f), lim[f]) for f in fields}
                if any(item.values()):
                    variants.append(item)
        if not variants:
            raise HTTPException(status_code=502, detail="ИИ не вернул вариантов — попробуйте ещё раз")
        return {"variants": variants[:p.count]}

    @router.post("/ai/design")
    async def ai_design(p: AIDesignIn, principal: dict = Depends(principal_dep)):
        uid(principal)
        fw, fh = FORMATS.get(p.format, FORMATS["square"])
        user = (f"Бриф: {p.brief.strip()}\nФормат холста: {fw}×{fh} px "
                f"({'вертикальный' if fh > fw else 'горизонтальный' if fw > fh else 'квадрат'}).\n"
                + (f"Фирменные цвета: {', '.join(c for c in p.palette if _HEX.match(c))}\n" if p.palette else "")
                + ("Добавь слой image с placeholder: true под фото товара — пользователь вставит своё фото.\n" if p.has_photo else "")
                + f"Сделай {p.count} РАЗНЫХ по композиции и цвету варианта.")
        try:
            raw = parse_json(await ai_complete(DESIGN_SYSTEM, user, 8000))
        except AIError as exc:
            raise ai_fail(exc) from exc
        designs = []
        for d in (raw.get("designs") if isinstance(raw, dict) else raw) or []:
            if not isinstance(d, dict):
                continue
            layers = sanitize_layers(d.get("layers"))
            if not layers:
                continue
            bg = d.get("bg") if isinstance(d.get("bg"), dict) else {}
            designs.append({"name": _txt(d.get("name"), 60) or "Вариант ИИ", "format": p.format if p.format in FORMATS else "square",
                            "bg": {"type": "color", "color": _color(bg.get("color"), "#ffffff")}, "layers": layers})
        if not designs:
            raise HTTPException(status_code=502, detail="ИИ не собрал макет — попробуйте ещё раз или уточните бриф")
        return {"designs": designs[:p.count]}

    @router.post("/ai/critique")
    async def ai_critique(p: AICritiqueIn, principal: dict = Depends(principal_dep)):
        uid(principal)
        mime, b64 = _data_url_parts(p.image)
        if len(b64) * 3 // 4 > MAX_IMAGE_BYTES:
            raise HTTPException(status_code=413, detail="Картинка слишком большая")
        fw, fh = FORMATS.get(p.format, FORMATS["square"])
        note = (f"Бриф: {p.brief.strip() or 'не указан'}. Формат {fw}×{fh}."
                + (f" Тексты на макете: {' | '.join(_txt(t, 200) for t in p.texts if t)}" if p.texts else ""))
        content = [{"type": "image", "source": {"type": "base64", "media_type": mime, "data": b64}},
                   {"type": "text", "text": note}]
        try:
            raw = parse_json(await ai_complete(CRITIQUE_SYSTEM, content, 3000))
        except AIError as exc:
            raise ai_fail(exc) from exc
        if not isinstance(raw, dict):
            raise HTTPException(status_code=502, detail="ИИ ответил не по формату")
        issues = []
        for it in raw.get("issues") or []:
            if isinstance(it, dict) and it.get("problem"):
                issues.append({"severity": it.get("severity") if it.get("severity") in ("high", "medium", "low") else "medium",
                               "area": _txt(it.get("area"), 30), "problem": _txt(it.get("problem"), 300),
                               "fix": _txt(it.get("fix"), 300)})
        bt = raw.get("better_texts") if isinstance(raw.get("better_texts"), dict) else {}
        return {"score": int(_num(raw.get("score"), 1, 10, 5)), "summary": _txt(raw.get("summary"), 300),
                "strengths": [_txt(s, 200) for s in (raw.get("strengths") or [])[:6] if s],
                "issues": issues[:12],
                "better_texts": {k: _txt(bt.get(k), 110) for k in ("headline", "subheadline", "cta") if bt.get(k)}}

    @router.post("/ai/image-prompt")
    async def ai_image_prompt(p: AIImagePromptIn, principal: dict = Depends(principal_dep)):
        """Claude пишет промт для генерации картинки по брифу и/или примеру (vision)."""
        uid(principal)
        if not (p.brief.strip() or p.wish.strip() or p.example):
            raise HTTPException(status_code=422, detail="Опишите, что нужно, или приложите пример")
        fw, fh = FORMATS.get(p.format, FORMATS["square"])
        text = (f"Бриф: {p.brief.strip() or '—'}\nПожелания к картинке: {p.wish.strip() or '—'}\n"
                f"Формат {fw}×{fh}, место под текст оставить.")
        content: Any = text
        if p.example:
            mime, b64 = _data_url_parts(p.example)
            content = [{"type": "image", "source": {"type": "base64", "media_type": mime, "data": b64}},
                       {"type": "text", "text": "Пример картинки выше. " + text}]
        try:
            raw = parse_json(await ai_complete(IMAGE_PROMPT_SYSTEM, content, 1500))
        except AIError as exc:
            raise ai_fail(exc) from exc
        raw = raw if isinstance(raw, dict) else {}
        if not raw.get("prompt"):
            raise HTTPException(status_code=502, detail="ИИ не написал промт — попробуйте ещё раз")
        return {"prompt": _txt(raw.get("prompt"), 3000), "prompt_ru": _txt(raw.get("prompt_ru"), 1500)}

    @router.post("/ai/image")
    async def ai_image(p: AIImageIn, principal: dict = Depends(principal_dep)):
        """Генерация изображения: по промту или по примеру (пример + промт)."""
        uid(principal)
        example = None
        if p.example:
            mime, b64 = _data_url_parts(p.example)
            try:
                example = (mime, base64.b64decode(b64))
            except ValueError as exc:
                raise HTTPException(status_code=422, detail="Пример картинки повреждён") from exc
        aspect = _ASPECT.get(p.format, "1:1")
        try:
            raw_images, provider = await generate_images(p.prompt.strip(), example, aspect, p.count)
        except ImageGenError as exc:
            raise HTTPException(status_code=503, detail=str(exc)) from exc
        images = []
        for raw in raw_images[:p.count]:
            try:
                data_url, w, h = await asyncio.to_thread(_to_data_url, raw, 2000)
                images.append({"data_url": data_url, "width": w, "height": h})
            except Exception:  # noqa: BLE001 - битый ответ модели
                continue
        if not images:
            raise HTTPException(status_code=502, detail="Модель вернула повреждённые картинки")
        return {"images": images, "provider": provider}

    @router.post("/ai/translate")
    async def ai_translate(p: AITranslateIn, principal: dict = Depends(principal_dep)):
        """Тексты макета → другой язык (порядок сохраняется; числа, цены, ссылки — как есть)."""
        uid(principal)
        texts = [str(t)[:800] for t in p.texts]
        if not any(t.strip() for t in texts):
            raise HTTPException(status_code=422, detail="Нет текста для перевода")
        user = (f"Переведи тексты рекламного макета на язык: {p.lang.strip()}.\n"
                "Сохрани смысл и тон рекламы; длина — как у оригинала или короче (текст стоит в рамке на картинке). "
                "Числа, цены, валюту, телефоны, ссылки, адреса сайтов и названия брендов не меняй. "
                "Переносы строк (\\n) сохраняй. Пустую строку оставь пустой.\n"
                'Ответ строго JSON: {"t": ["перевод 1", "перевод 2", ...]} — столько же элементов и в том же порядке.\n\n'
                "Тексты:\n" + json.dumps(texts, ensure_ascii=False))
        try:
            raw = parse_json(await ai_complete("Ты — переводчик-локализатор рекламы. Отвечай только JSON.", user, 6000))
        except AIError as exc:
            raise ai_fail(exc) from exc
        out = raw.get("t") if isinstance(raw, dict) else raw
        if not isinstance(out, list) or len(out) != len(texts):
            raise HTTPException(status_code=502, detail="ИИ вернул перевод не полностью — попробуйте ещё раз")
        return {"texts": [str(x if x is not None else "")[:1200] for x in out]}

    @router.post("/ai/rewrite")
    async def ai_rewrite(p: AIRewriteIn, principal: dict = Depends(principal_dep)):
        """Креатив конкурента из Парсера → свой текст (не копия) под наш оффер."""
        uid(principal)
        if not (p.title.strip() or p.description.strip()):
            raise HTTPException(status_code=422, detail="Нет текста креатива")
        user = (f"Объявление конкурента:\nЗаголовок: {p.title}\nТекст: {p.description}\n"
                f"Наш оффер/бриф: {p.brief.strip() or 'тот же продукт/услуга'}\n\n"
                "Перепиши своими словами для НАШЕЙ картинки — не копируй формулировки конкурента. "
                'Ответ: {"headline": "до 40 символов", "subheadline": "до 70", "cta": "до 18", "badge": "до 14 или пусто"}')
        try:
            raw = parse_json(await ai_complete("Ты — копирайтер performance-рекламы на русском. Отвечай только JSON.", user, 1500))
        except AIError as exc:
            raise ai_fail(exc) from exc
        raw = raw if isinstance(raw, dict) else {}
        return {"headline": _txt(raw.get("headline"), 60), "subheadline": _txt(raw.get("subheadline"), 110),
                "cta": _txt(raw.get("cta"), 28), "badge": _txt(raw.get("badge"), 20)}

    return router
