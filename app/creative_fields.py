# -*- coding: utf-8 -*-
"""Дополнительные поля объявления, которые есть в сохранённой разметке (creatives.html_gzip),
но не разбирались парсером (17.11.2026, см. AD_FORMATS_RESEARCH.md).

Разбор идёт в панели по уже сохранённой разметке: так поля заполняются и задним числом по всей базе,
и для новых объявлений, без правок парсера. Результат — таблица creative_details (одна строка на объявление).
Поднимайте FIELDS_VERSION при изменении правил разбора — фоновый разбор пройдёт базу заново."""
from __future__ import annotations

import gzip
import json
import re
from typing import Any

from bs4 import BeautifulSoup

FIELDS_VERSION = 1

SCHEMA = """CREATE TABLE IF NOT EXISTS creative_details (
  creative_id INTEGER PRIMARY KEY,
  ad_id TEXT,               -- номер объявления в РСЯ (data-ad-id)
  rsya_creative_id TEXT,    -- номер креатива (data-creative-id, crid в ссылке verify)
  direct_campaign_id TEXT,  -- номер кампании Директа (cid в ссылке verify)
  direct_ad_id TEXT,        -- номер объявления Директа (aid в ссылке verify)
  direct_group_id TEXT,     -- номер группы объявлений (gid)
  warning TEXT,             -- юридическое предупреждение («Есть противопоказания…»)
  age_label TEXT,           -- возрастная метка (16+, 18+)
  is_social INTEGER NOT NULL DEFAULT 0,  -- социальная реклама
  promo_text TEXT,          -- промо-строка, промокод
  price_current TEXT,
  price_old TEXT,
  discount_text TEXT,
  callouts_json TEXT,       -- уточнения ["10 лет на рынке", …]
  sitelinks_json TEXT,      -- быстрые ссылки (только тексты)
  offers_json TEXT,         -- товары в плитке [{name, price, image}]
  cta_text TEXT,            -- текст кнопки
  packshot_title TEXT,      -- текст финального кадра видео
  app_name TEXT,            -- название приложения (реклама приложений)
  ad_format TEXT,           -- text_image | gallery | video | offers | app | social_video …
  version INTEGER NOT NULL,
  extracted_at TEXT NOT NULL
)"""

_WS = re.compile(r"\s+")
_DIGITS = re.compile(r"^\d{5,25}$")
_VERIFY_RE = re.compile(r"(?:^|[_.?&=])(cid|aid|crid|gid)\.(\d{4,20})")


def _t(el) -> str:
    if el is None:
        return ""
    return _WS.sub(" ", el.get_text(" ", strip=True)).replace(" ", " ").strip()


def _first_text(soup, *selectors) -> str:
    for sel in selectors:
        el = soup.select_one(sel)
        txt = _t(el)
        if txt:
            return txt
    return ""


def _warning(soup) -> str:
    """Предупреждение рисуется SVG-текстом (<text>) или обычным текстом в ya-ad-warning / disclaimer."""
    for el in soup.select(".ya-ad-warning, [data-name='disclaimer'], .ya-image-gallery-warning"):
        parts = [_t(x) for x in el.select("text")] or [_t(el)]
        txt = " ".join(p for p in parts if p).strip()
        if txt:
            return txt[:500]
    return ""


def _ids(html: str, soup) -> dict[str, str]:
    out: dict[str, str] = {}
    el = soup.select_one("[data-ad-id]")
    if el is not None and _DIGITS.match(str(el.get("data-ad-id") or "")):
        out["ad_id"] = el["data-ad-id"]
    el = soup.select_one("[data-creative-id]")
    if el is not None and _DIGITS.match(str(el.get("data-creative-id") or "").strip()):
        out["rsya_creative_id"] = str(el["data-creative-id"]).strip()
    # verify.yandex.ru/…custom=cid.<кампания>_aid.<объявление>_crid.<креатив>_gid.<группа>
    for m in _VERIFY_RE.finditer(html.replace("%5F", "_").replace("%2E", ".")):
        key = {"cid": "direct_campaign_id", "aid": "direct_ad_id", "crid": "rsya_creative_id", "gid": "direct_group_id"}[m.group(1)]
        out.setdefault(key, m.group(2))
    return out


def _offers(soup) -> list[dict[str, str]]:
    items = []
    for card in soup.select("[data-name='ecom-item-card'], [data-name='offer-tile']")[:20]:
        name = _first_text(card, "[data-name='ecom-item-name']", "[data-name='offer-title']", "[data-name='title']")
        price = _first_text(card, "[data-name='ecom-item-price']", "[data-name='offer-price']", "[data-name='price']")
        if not name:
            # плитка без размеченного названия: текст целиком (цена + название)
            name = _t(card)[:200]
            m = re.search(r"\d[\d\s]*\s?₽", name)
            if m and not price:
                price = m.group(0).strip()
                name = (name[:m.start()] + name[m.end():]).strip()
        img = card.select_one("img[src]")
        if name:
            items.append({"name": name[:200], "price": price[:60], "image": (img.get("src") if img is not None else "")[:500]})
    return items


def extract(html: str) -> dict[str, Any]:
    """Поля из разметки одного объявления. Пустые поля не возвращаются."""
    soup = BeautifulSoup(html or "", "html.parser")
    d: dict[str, Any] = {}
    d.update(_ids(html or "", soup))
    w = _warning(soup)
    if w:
        d["warning"] = w
    age = _first_text(soup, ".yrw-unit-category__age")
    if not age:
        m = re.search(r"\b(0|6|12|16|18)\+", _first_text(soup, ".ya-unit-category", "[data-name='ad-label']", ".ad-label"))
        age = m.group(0) if m else ""
    if age:
        d["age_label"] = age[:8]
    social = bool(soup.select_one(".ya-unit-social-ad, .unit-social, [data-name='social-ad-label']")) or \
        bool(soup.select_one("[data-social-label='true']")) or "Соцреклама" in _first_text(soup, ".ya-unit-category", ".unit-category")
    if social:
        d["is_social"] = 1
    promo = _first_text(soup, ".yrw-ya-unit-promo-description")
    if promo:
        d["promo_text"] = promo[:300]
    cur = _first_text(soup, ".ya-unit-price-current")
    old = _first_text(soup, ".ya-unit-price-old")
    disc = _first_text(soup, ".ya-unit-price-discount", ".ya-unit-discount")
    if cur:
        d["price_current"] = cur[:60]
    if old:
        d["price_old"] = old[:60]
    if disc:
        d["discount_text"] = disc[:20]
    callouts = [_t(x).strip("[] ") for x in soup.select(".ya-unit-callouts-item")]
    callouts = [c for c in callouts if c][:20]
    if callouts:
        d["callouts_json"] = json.dumps(callouts, ensure_ascii=False)
    sitelinks = []
    for x in soup.select(".ya-unit-sitelink"):
        txt = _t(x) or str(x.get("data-title") or "").strip()
        if txt.startswith("[") and txt.endswith("]"):
            # в квадратных скобках Яндекс выводит уточнения, а не быстрые ссылки
            if txt.strip("[] ") and txt.strip("[] ") not in callouts:
                callouts.append(txt.strip("[] ")[:120])
            continue
        if txt and txt not in sitelinks:
            sitelinks.append(txt[:120])
    if callouts:
        d["callouts_json"] = json.dumps(callouts[:20], ensure_ascii=False)
    if sitelinks:
        d["sitelinks_json"] = json.dumps(sitelinks[:20], ensure_ascii=False)
    offers = _offers(soup)
    if offers:
        d["offers_json"] = json.dumps(offers, ensure_ascii=False)
    cta = _first_text(soup, ".ya-unit-direct-button", "[data-name='cta-button']", "[data-name='arm-button']",
                      "[data-name='header-button']", ".ya-unit-mobile-app-button")
    if cta:
        d["cta_text"] = cta[:80]
    pack = _first_text(soup, "[data-name='packshot-title']")
    if pack:
        d["packshot_title"] = pack[:200]
    app = _first_text(soup, ".ya-unit-app-name")
    if app:
        d["app_name"] = app[:120]
    # формат объявления
    has_video = bool(soup.select_one("video, [data-name='video-slot']"))
    if offers:
        fmt = "offers"
    elif app or soup.select_one(".ya-unit-item-rmp, .ya-unit-mobile-app-button"):
        fmt = "app"
    elif soup.select_one(".ya-image-gallery"):
        fmt = "gallery"
    elif has_video:
        fmt = "video"
    elif soup.select_one(".unit-without-image"):
        fmt = "text"
    else:
        fmt = "text_image"
    d["ad_format"] = fmt
    return d


def extract_gz(html_gzip: bytes | None) -> dict[str, Any]:
    if not html_gzip:
        return {}
    try:
        html = gzip.decompress(html_gzip).decode("utf-8", "replace")
    except Exception:
        return {}
    return extract(html)


COLUMNS = ("ad_id", "rsya_creative_id", "direct_campaign_id", "direct_ad_id", "direct_group_id", "warning", "age_label",
           "is_social", "promo_text", "price_current", "price_old", "discount_text", "callouts_json", "sitelinks_json",
           "offers_json", "cta_text", "packshot_title", "app_name", "ad_format")


def save(db, creative_id: int, fields: dict[str, Any], now: str) -> None:
    vals = [fields.get(c) for c in COLUMNS]
    vals[COLUMNS.index("is_social")] = int(fields.get("is_social") or 0)
    db.execute(
        f"INSERT OR REPLACE INTO creative_details(creative_id,{','.join(COLUMNS)},version,extracted_at) "
        f"VALUES(?,{','.join('?' * len(COLUMNS))},?,?)",
        [creative_id] + vals + [FIELDS_VERSION, now],
    )


def process_pending(db, now: str, batch: int = 300) -> int:
    """Разбирает объявления без строки в creative_details или со старой версией разбора."""
    rows = db.execute(
        "SELECT c.id, c.html_gzip FROM creatives c LEFT JOIN creative_details d ON d.creative_id=c.id "
        "WHERE c.html_gzip IS NOT NULL AND (d.creative_id IS NULL OR d.version<?) ORDER BY c.id DESC LIMIT ?",
        (FIELDS_VERSION, batch),
    ).fetchall()
    for r in rows:
        save(db, int(r[0]), extract_gz(r[1]), now)
    db.commit()
    return len(rows)
