#!/usr/bin/env python3
"""Парсер посадочных страниц рекламы: название сайта, <title>, H1 и реквизиты юрлица (ИНН/ОГРН/КПП/название).

Зачем: в карточке креатива показываем, КУДА ведёт реклама и КТО владелец сайта, и сравниваем
с рекламодателем из маркировки объявления (ИНН из «О рекламодателе»). Расхождение ИНН — отдельный сигнал
помимо несовпадения домена посадочной.

Как ищем реквизиты (по убыванию надёжности):
  1. JSON-LD / microdata (Organization: taxID, vatID, legalName);
  2. страница реквизитов/контактов/«о компании» — ссылки ищем в HTML посадочной, плюс типовые пути;
  3. подвал и текст самой посадочной;
  4. политика конфиденциальности / оферта / пользовательское соглашение.
ИНН и ОГРН проверяются по контрольным суммам, поэтому случайные числа (телефоны, артикулы) отсекаются.

Запуск:
  .venv/bin/python landing_parser.py --limit 50            # тест на 50 доменах
  .venv/bin/python landing_parser.py --all                 # все посадочные из базы
  .venv/bin/python landing_parser.py --url https://site.ru # разобрать один адрес и напечатать результат
Результат: таблицы landing_pages (по адресу страницы) и landing_legal (по домену) в control.sqlite3.
"""
from __future__ import annotations

import argparse
import json
import re
import sqlite3
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from html import unescape
from pathlib import Path
from urllib.parse import urljoin, urlsplit

import requests
from bs4 import BeautifulSoup

requests.packages.urllib3.disable_warnings()  # type: ignore[attr-defined]

DB = Path(__file__).parent / "data/control.sqlite3"
UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
      "Chrome/126.0.0.0 Safari/537.36")
HEADERS = {"User-Agent": UA, "Accept-Language": "ru-RU,ru;q=0.9,en;q=0.5",
           "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"}
TIMEOUT = 14
MAX_BYTES = 2_500_000
MAX_LEGAL_PAGES = 6

# Площадки, где сайт принадлежит не рекламодателю: реквизиты там — самой площадки, сравнивать их бессмысленно.
PLATFORM_HOSTS = (
    "yandex.ru", "ya.ru", "dzen.ru", "apps.apple.com", "play.google.com", "apps.rustore.ru", "rustore.ru",
    "wildberries.ru", "ozon.ru", "market.yandex.ru", "avito.ru", "vk.com", "vk.ru", "t.me", "telegram.me",
    "ok.ru", "youtube.com", "rutube.ru", "hh.ru", "mrqz.me", "marquiz.ru", "taplink.cc", "tilda.ws",
    "apple.com", "google.com", "megamarket.ru", "aliexpress.ru", "lamoda.ru", "2gis.ru", "cian.ru",
    "domclick.ru", "auto.ru", "drom.ru", "appgallery.huawei.com", "onelink.me", "redirect.appmetrica.yandex.com",
)

LEGAL_FORMS = (
    r"Общество\s+с\s+ограниченной\s+ответственностью|Публичное\s+акционерное\s+общество|"
    r"Непубличное\s+акционерное\s+общество|Закрытое\s+акционерное\s+общество|Открытое\s+акционерное\s+общество|"
    r"Акционерное\s+общество|Индивидуальный\s+предприниматель|Автономная\s+некоммерческая\s+организация|"
    r"Благотворительный\s+фонд|Микрокредитная\s+компания|Микрофинансовая\s+компания|"
    r"Кредитный\s+потребительский\s+кооператив|Коллегия\s+адвокатов|Адвокатское\s+бюро|"
    r"ООО\s+МКК|ООО\s+МФК|ООО\s+ПКО|ООО\s+СЗ|ООО\s+УК|ООО\s+ТД|ООО\s+ТК|ООО\s+НПП|ООО\s+ПКФ|ООО\s+НПО|"
    r"АО\s+СЗ|АО\s+УК|ПАО\s+СК|АО\s+СК|"
    r"ООО|ОАО|ЗАО|ПАО|НАО|АО|ИП|АНО|НКО|ЧОУ|ФГБУ|ФГУП|ГБУ|МУП|ГУП|КПК|СПК|ПК|НП|ТСЖ"
)
QUOTE_O, QUOTE_C = "«\"“„'‘", "»\"”“'’"
# ООО «Название», ИП Иванов И. И., АО "Название" и т.п.
RE_LEGAL_NAME = re.compile(
    rf"(?<![А-Яа-яЁёA-Za-z])(?P<form>{LEGAL_FORMS})\s*"
    rf"(?:[{QUOTE_O}]\s*(?P<q>[^«»\"“”„\n]{{2,90}}?)\s*[{QUOTE_C}]"
    rf"|(?P<fio>[А-ЯЁ][а-яё\-]+\s+[А-ЯЁ][а-яё\-]+(?:\s+[А-ЯЁ][а-яё\-]+)?|[А-ЯЁ][а-яё\-]+\s+[А-ЯЁ]\.\s?[А-ЯЁ]\.?)"
    rf"|(?P<plain>[A-ZА-ЯЁ][A-Za-zА-Яа-яЁё0-9\-&+\. ]{{1,50}}?)(?=[,;\.\(\n]|\s+ИНН|\s+ОГРН|\s+[Юю]р|$))",
    re.S,
)
DIG = r"(?:\d[\s  \-]?)"
RE_INN = re.compile(rf"(?:ИНН|INN|И\.Н\.Н\.?)(?:\s*/\s*КПП)?[\s :№#\-–—]{{0,6}}(?:организации|компании|юридического лица)?[\s :№#\-–—]{{0,4}}((?:{DIG}){{10,12}})", re.I)
RE_OGRN = re.compile(rf"(?:ОГРНИП|ОГРН|OGRNIP|OGRN)[\s :№#\-–—]{{0,6}}((?:{DIG}){{13,15}})", re.I)
RE_KPP = re.compile(rf"(?:КПП|KPP)[\s :№#\-–—]{{0,6}}((?:{DIG}){{9}})", re.I)
RE_INN_KPP = re.compile(r"ИНН\s*/\s*КПП[\s :№#\-–—]{0,6}(\d{10})\s*/\s*(\d{9})", re.I)

LEGAL_LINK_RULES = [  # (вид страницы, вес, шаблон по тексту ссылки или адресу)
    ("requisites", 100, r"реквизит|rekvizit|requisit|details|bank-details|юридическ\w+\s+информац|legal-info|legal_info|правовая информация|сведения об организации|svedeniya"),
    ("contacts", 80, r"контакт|contact|kontakt"),
    ("about", 60, r"о\s+компании|о\s+нас|о\s+магазине|о\s+клинике|о\s+банке|about|o-kompanii|o-nas|company"),
    ("oferta", 50, r"оферт|oferta|offer|договор"),
    ("policy", 45, r"конфиденциальн|политик|privacy|policy|personal|персональн|соглашени|agreement|terms|условия использования|legal"),
    ("docs", 30, r"документ|docs|documents|лицензи"),
]
GUESS_PATHS = [("requisites", "/rekvizity"), ("requisites", "/requisites"), ("contacts", "/contacts"), ("contacts", "/kontakty"),
               ("about", "/about"), ("policy", "/privacy"), ("policy", "/policy"), ("policy", "/privacy-policy")]
KIND_WEIGHT = {"jsonld": 120, "requisites": 100, "contacts": 80, "landing": 70, "about": 60, "oferta": 50, "policy": 45, "docs": 30}


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def host_of(url: str) -> str:
    try:
        return re.sub(r"^(www|m)\.", "", (urlsplit(url).hostname or "").lower())
    except Exception:
        return ""


def url_key(url: str) -> str:
    """Адрес страницы без параметров и меток — одна запись на страницу, а не на каждую utm-ссылку."""
    p = urlsplit(url)
    return f"{host_of(url)}{(p.path or '/').rstrip('/') or '/'}"[:500]


def is_platform(host: str) -> bool:
    return any(host == h or host.endswith("." + h) for h in PLATFORM_HOSTS)


# ---------- контрольные суммы ----------
def _digits(s: str) -> str:
    return re.sub(r"\D", "", s or "")


def valid_inn(v: str) -> bool:
    if not v.isdigit() or len(v) not in (10, 12) or len(set(v)) == 1:
        return False
    d = [int(x) for x in v]
    if len(v) == 10:
        k = (2, 4, 10, 3, 5, 9, 4, 6, 8)
        return sum(a * b for a, b in zip(k, d)) % 11 % 10 == d[9]
    k1 = (7, 2, 4, 10, 3, 5, 9, 4, 6, 8)
    k2 = (3, 7, 2, 4, 10, 3, 5, 9, 4, 6, 8)
    return (sum(a * b for a, b in zip(k1, d)) % 11 % 10 == d[10]
            and sum(a * b for a, b in zip(k2, d)) % 11 % 10 == d[11])


def valid_ogrn(v: str) -> bool:
    if not v.isdigit() or len(set(v)) == 1:
        return False
    if len(v) == 13:
        return int(v[:12]) % 11 % 10 == int(v[12])
    if len(v) == 15:
        return int(v[:14]) % 13 % 10 == int(v[14])
    return False


# ---------- загрузка ----------
_session_local = threading.local()


def _session() -> requests.Session:
    s = getattr(_session_local, "s", None)
    if s is None:
        s = requests.Session()
        s.headers.update(HEADERS)
        _session_local.s = s
    return s


def fetch(url: str) -> tuple[str, int, str, str]:
    """→ (html, http-код, итоговый адрес, ошибка). Пробуем с проверкой сертификата и без неё."""
    last = ""
    for verify in (True, False):
        try:
            r = _session().get(url, timeout=TIMEOUT, verify=verify, stream=True, allow_redirects=True)
            ctype = (r.headers.get("content-type") or "").lower()
            if ctype and not any(x in ctype for x in ("html", "xml", "text/plain")):
                r.close()
                return "", r.status_code, r.url, f"не HTML: {ctype[:40]}"
            raw = b""
            for chunk in r.iter_content(65536):
                raw += chunk
                if len(raw) > MAX_BYTES:
                    break
            r.close()
            enc = None
            m = re.search(rb"<meta[^>]+charset=[\"']?([\w\-]+)", raw[:4000], re.I)
            if m:
                enc = m.group(1).decode("ascii", "ignore")
            elif r.encoding and r.encoding.lower() != "iso-8859-1":
                enc = r.encoding
            try:
                html = raw.decode(enc or "utf-8", errors="replace")
            except LookupError:
                html = raw.decode("utf-8", errors="replace")
            if html.count("�") > 50 and not m:
                html = raw.decode("cp1251", errors="replace")
            return html, r.status_code, r.url, ""
        except requests.exceptions.SSLError as e:
            last = f"SSL: {str(e)[:80]}"
            continue
        except Exception as e:  # noqa: BLE001
            return "", 0, url, f"{type(e).__name__}: {str(e)[:120]}"
    return "", 0, url, last


# ---------- разбор страницы ----------
def clean(s: str | None, limit: int = 300) -> str:
    return re.sub(r"\s+", " ", unescape(s or "").replace(" ", " ")).strip()[:limit]


def page_meta(html: str) -> dict:
    soup = BeautifulSoup(html, "html.parser")
    title = clean(soup.title.get_text() if soup.title else "")
    def meta(*names):
        for n in names:
            t = soup.find("meta", attrs={"property": n}) or soup.find("meta", attrs={"name": n})
            if t and t.get("content"):
                return clean(t["content"])
        return ""
    h1 = ""
    for tag in soup.find_all("h1"):
        txt = clean(tag.get_text(" "))
        if not txt and tag.find("img"):
            txt = clean(tag.find("img").get("alt"))
        if txt:
            h1 = txt
            break
    site_name = meta("og:site_name", "application-name", "apple-mobile-web-app-title")
    jsonld = []
    for s in soup.find_all("script", attrs={"type": re.compile("ld\\+json", re.I)}):
        try:
            data = json.loads(s.string or s.get_text() or "")
        except Exception:
            continue
        stack = [data]
        while stack:
            x = stack.pop()
            if isinstance(x, list):
                stack.extend(x)
            elif isinstance(x, dict):
                jsonld.append(x)
                stack.extend(v for v in x.values() if isinstance(v, (dict, list)))
    if not site_name:
        for x in jsonld:
            if str(x.get("@type", "")).lower() in ("website", "organization", "localbusiness", "corporation") and x.get("name"):
                site_name = clean(str(x["name"]))
                break
    if not site_name and title:
        parts = [p.strip() for p in re.split(r"\s+[|—–\-:•·]\s+", title) if p.strip()]
        if len(parts) > 1:
            site_name = min((parts[0], parts[-1]), key=len)[:120]
    for t in soup(["script", "style", "noscript", "svg", "template"]):
        t.decompose()
    text = re.sub(r"[ \t  ]+", " ", soup.get_text("\n"))
    text = re.sub(r"\n\s*\n+", "\n", text)
    return {"title": title, "h1": h1 or meta("og:title"), "site_name": site_name, "description": meta("description", "og:description"),
            "text": text, "soup": soup, "jsonld": jsonld}


def find_legal(text: str, raw_html: str, jsonld: list[dict], kind: str, src: str) -> list[dict]:
    """Все найденные на странице реквизиты. Каждый кандидат: {inn, ogrn, kpp, name, kind, source, weight}."""
    out: list[dict] = []
    for x in jsonld:
        inn = _digits(str(x.get("taxID") or x.get("vatID") or ""))
        if valid_inn(inn):
            out.append({"inn": inn, "ogrn": "", "kpp": "", "name": clean(str(x.get("legalName") or x.get("name") or ""), 200),
                        "kind": "jsonld", "source": src, "weight": KIND_WEIGHT["jsonld"]})
    # текст страницы + сырой HTML без тегов (реквизиты бывают в скрытых блоках, атрибутах, комментариях)
    raw_text = re.sub(r"<[^>]+>", " ", unescape(raw_html or ""))
    raw_text = re.sub(r"[ \t  ]+", " ", raw_text)
    seen: set[str] = set()
    for body, hidden in ((text, False), (raw_text, True)):
        for m in RE_INN.finditer(body):
            inn = _digits(m.group(1))
            # «ИНН/КПП 7701234567/770101001» записывают слитно — берём первые 10 или 12 цифр
            cand = [inn] + ([inn[:10]] if len(inn) > 10 else []) + ([inn[:12]] if len(inn) > 12 else [])
            inn = next((c for c in cand if valid_inn(c)), "")
            if not inn or inn in seen:
                continue
            seen.add(inn)
            lo, hi = max(0, m.start() - 400), min(len(body), m.end() + 400)
            ctx = body[lo:hi]
            ogrn = next((d for d in (_digits(x.group(1)) for x in RE_OGRN.finditer(ctx)) if valid_ogrn(d)), "")
            kpp = ""
            mk = RE_INN_KPP.search(ctx) or RE_KPP.search(ctx)
            if mk:
                kpp = _digits(mk.group(mk.lastindex))[:9]
            name = pick_name(body[max(0, m.start() - 500):m.start()], prefer_last=True) or pick_name(body[m.end():hi], prefer_last=False)
            if len(inn) == 12 and name and not name.upper().startswith(("ИП", "ИНДИВИД")):
                pass
            out.append({"inn": inn, "ogrn": ogrn, "kpp": kpp, "name": name, "kind": kind, "source": src,
                        "weight": KIND_WEIGHT.get(kind, 40) - (15 if hidden else 0)})
    if not out:  # ИНН нет — хотя бы ОГРН и название
        for m in RE_OGRN.finditer(text):
            og = _digits(m.group(1))
            if valid_ogrn(og):
                out.append({"inn": "", "ogrn": og, "kpp": "", "name": pick_name(text[max(0, m.start() - 500):m.start()], True),
                            "kind": kind, "source": src, "weight": KIND_WEIGHT.get(kind, 40) - 20})
                break
    return out


def pick_name(chunk: str, prefer_last: bool) -> str:
    found = []
    for m in RE_LEGAL_NAME.finditer(chunk):
        body = m.group("q") or m.group("fio") or m.group("plain") or ""
        body = clean(body, 120).strip(" .,;:-")
        if len(body) < 2 or re.fullmatch(r"[\d\s\-]+", body):
            continue
        form = re.sub(r"\s+", " ", m.group("form"))
        is_ip = form.upper().startswith(("ИП", "ИНДИВИД"))
        found.append(f"{form} «{body}»" if (m.group("q") or m.group("plain")) and not is_ip else f"{form} {body}")
    if not found:
        return ""
    # на странице реквизитов рядом стоит название банка — это не владелец сайта
    not_bank = [x for x in found if "банк" not in x.lower()]
    if not_bank and len(not_bank) < len(found):
        found = not_bank
    return found[-1] if prefer_last else found[0]


def legal_links(soup: BeautifulSoup, base: str) -> list[tuple[str, int, str]]:
    host = host_of(base)
    best: dict[str, tuple[str, int]] = {}
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        if href.startswith(("#", "javascript:", "mailto:", "tel:")) or re.search(r"\.(pdf|docx?|jpg|png|zip)(\?|$)", href, re.I):
            continue
        full = urljoin(base, href)
        if host_of(full) != host:
            continue
        label = clean(a.get_text(" "), 80).lower() + " " + href.lower()
        for kind, weight, pat in LEGAL_LINK_RULES:
            if re.search(pat, label, re.I):
                key = full.split("#")[0]
                if key not in best or best[key][1] < weight:
                    best[key] = (kind, weight)
                break
    ranked = sorted(((k, w, u) for u, (k, w) in best.items()), key=lambda x: -x[1])
    out, kinds = [], {}
    for kind, weight, u in ranked:  # не больше двух страниц одного вида
        if kinds.get(kind, 0) >= 2:
            continue
        kinds[kind] = kinds.get(kind, 0) + 1
        out.append((kind, weight, u))
    return out[:MAX_LEGAL_PAGES]


def choose(cands: list[dict]) -> dict:
    """Лучший кандидат: чаще встречается + найден на более надёжной странице."""
    if not cands:
        return {}
    score: dict[str, float] = {}
    for c in cands:
        key = c["inn"] or ("ogrn:" + c["ogrn"])
        score[key] = score.get(key, 0) + c["weight"] + (10 if c["name"] else 0) + (10 if c["ogrn"] else 0)
    top = max(score, key=score.get)
    same = [c for c in cands if (c["inn"] or ("ogrn:" + c["ogrn"])) == top]
    best = max(same, key=lambda c: c["weight"] + (20 if c["name"] else 0))
    res = dict(best)
    for f in ("ogrn", "kpp", "name"):
        if not res.get(f):
            res[f] = next((c[f] for c in same if c.get(f)), "")
    res["others"] = sorted({c["inn"] for c in cands if c["inn"] and c["inn"] != res.get("inn")})
    # Надёжность: реквизиты из разметки / со страницы реквизитов, контактов, подвала — высокая;
    # найдено только в политике или оферте (там часто упомянуты сторонние компании) — низкая.
    pages = {c["source"] for c in same}
    strong = any(c["kind"] in ("jsonld", "requisites", "contacts", "landing", "about") for c in same)
    res["confidence"] = "high" if strong and (len(pages) > 1 or best["weight"] >= 80) else ("medium" if strong or len(pages) > 1 else "low")
    return res


def analyze(url: str, want_legal: bool = True) -> dict:
    """Полный разбор адреса: мета страницы + реквизиты (с обходом страниц реквизитов/контактов/политики)."""
    host = host_of(url)
    res = {"url": url, "url_key": url_key(url), "domain": host, "http_status": 0, "status": "ok", "error": "",
           "title": "", "h1": "", "site_name": "", "description": "", "legal": {}, "candidates": [], "pages_checked": []}
    html, code, final, err = fetch(url)
    res["http_status"] = code
    if not html:
        res["status"], res["error"] = "error", err or f"HTTP {code}"
        return res
    if code >= 400:
        res["status"], res["error"] = "http_error", f"HTTP {code}"
    meta = page_meta(html)
    res.update(title=meta["title"], h1=meta["h1"], site_name=meta["site_name"], description=meta["description"])
    if len(meta["text"]) < 200 and not meta["h1"]:
        res["status"] = "js_only" if code < 400 else res["status"]  # страница собирается скриптами — текста в HTML нет
    if not want_legal or is_platform(host):
        if is_platform(host):
            res["legal"] = {"status": "platform"}
        return res
    cands = find_legal(meta["text"], html, meta["jsonld"], "landing", final)
    res["pages_checked"].append({"kind": "landing", "url": final, "found": len(cands)})
    links = legal_links(meta["soup"], final)
    root = f"{urlsplit(final).scheme}://{urlsplit(final).netloc}"
    if urlsplit(final).path.strip("/"):  # ссылки на реквизиты чаще есть на главной
        links.append(("home", 20, root + "/"))
    have = {u for _, _, u in links}
    tried = 0
    for kind, _w, u in links:
        if tried >= MAX_LEGAL_PAGES:
            break
        if any(c["weight"] >= 100 for c in cands):
            break  # нашли на странице реквизитов / в разметке — достаточно
        h2, c2, f2, _e = fetch(u)
        tried += 1
        if not h2 or c2 >= 400:
            continue
        m2 = page_meta(h2)
        k = "landing" if kind == "home" else kind
        got = find_legal(m2["text"], h2, m2["jsonld"], k, f2)
        res["pages_checked"].append({"kind": kind, "url": f2, "found": len(got)})
        cands += got
        if kind == "home":
            for kk, ww, uu in legal_links(m2["soup"], f2):
                if uu not in have:
                    links.append((kk, ww, uu))
                    have.add(uu)
    if not cands:  # ссылок не нашли (меню рисуется скриптами) — пробуем типовые адреса
        for kind, path in GUESS_PATHS:
            if tried >= MAX_LEGAL_PAGES + 4:
                break
            h2, c2, f2, _e = fetch(root + path)
            tried += 1
            if not h2 or c2 >= 400 or url_key(f2) == url_key(root + "/"):
                continue
            m2 = page_meta(h2)
            got = find_legal(m2["text"], h2, m2["jsonld"], kind, f2)
            res["pages_checked"].append({"kind": kind + "?", "url": f2, "found": len(got)})
            cands += got
            if got:
                break
    res["candidates"] = cands
    best = choose(cands)
    res["legal"] = ({"status": "found", **best} if best else {"status": "not_found"})
    return res


# ---------- база ----------
SCHEMA = """
CREATE TABLE IF NOT EXISTS landing_pages (
  url_key TEXT PRIMARY KEY, domain TEXT NOT NULL, final_url TEXT, http_status INTEGER, status TEXT,
  page_title TEXT, h1 TEXT, site_name TEXT, description TEXT, error TEXT, fetched_at TEXT);
CREATE INDEX IF NOT EXISTS ix_landing_pages_domain ON landing_pages(domain);
CREATE TABLE IF NOT EXISTS landing_legal (
  domain TEXT PRIMARY KEY, status TEXT, legal_name TEXT, inn TEXT, ogrn TEXT, kpp TEXT,
  source_url TEXT, source_kind TEXT, confidence TEXT, other_inns TEXT, candidates_json TEXT, pages_json TEXT, fetched_at TEXT);
CREATE INDEX IF NOT EXISTS ix_landing_legal_inn ON landing_legal(inn);
"""


def save(db: sqlite3.Connection, r: dict, with_legal: bool) -> None:
    db.execute("INSERT OR REPLACE INTO landing_pages(url_key,domain,final_url,http_status,status,page_title,h1,site_name,description,error,fetched_at) "
               "VALUES(?,?,?,?,?,?,?,?,?,?,?)",
               (r["url_key"], r["domain"], r["url"][:2000], r["http_status"], r["status"], r["title"], r["h1"], r["site_name"],
                r["description"], r["error"], now_iso()))
    if with_legal and r["status"] != "error":
        lg = r["legal"] or {}
        db.execute("INSERT OR REPLACE INTO landing_legal(domain,status,legal_name,inn,ogrn,kpp,source_url,source_kind,confidence,other_inns,candidates_json,pages_json,fetched_at) "
                   "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)",
                   (r["domain"], lg.get("status", "not_found"), lg.get("name", ""), lg.get("inn", ""), lg.get("ogrn", ""), lg.get("kpp", ""),
                    lg.get("source", ""), lg.get("kind", ""), lg.get("confidence", ""), ",".join(lg.get("others", [])),
                    json.dumps(r["candidates"], ensure_ascii=False)[:20000], json.dumps(r["pages_checked"], ensure_ascii=False)[:8000], now_iso()))


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--url")
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--all", action="store_true")
    ap.add_argument("--refresh", action="store_true", help="перечитать уже разобранные")
    ap.add_argument("--threads", type=int, default=16)
    a = ap.parse_args()
    if a.url:
        r = analyze(a.url)
        r.pop("candidates", None)
        print(json.dumps(r, ensure_ascii=False, indent=1))
        return
    db = sqlite3.connect(DB, timeout=120)
    db.executescript(SCHEMA)
    done_pages = set() if a.refresh else {r[0] for r in db.execute("SELECT url_key FROM landing_pages")}
    done_legal = set() if a.refresh else {r[0] for r in db.execute("SELECT domain FROM landing_legal")}
    urls: dict[str, str] = {}
    for (u,) in db.execute("SELECT final_url FROM creative_utm WHERE COALESCE(final_url,'')<>'' ORDER BY creative_id DESC"):
        if u.startswith("http"):
            urls.setdefault(url_key(u), u)
    # сначала по одной странице на домен (она же даёт реквизиты), потом остальные страницы — только title/h1
    by_domain: dict[str, list[str]] = {}
    for k, u in urls.items():
        by_domain.setdefault(host_of(u), []).append(k)
    tasks: list[tuple[str, bool]] = []
    for dom, keys in by_domain.items():
        need_legal = dom not in done_legal
        for i, k in enumerate(keys):
            if k in done_pages and not (need_legal and i == 0):
                continue
            tasks.append((urls[k], need_legal and i == 0))
    tasks.sort(key=lambda t: not t[1])
    if a.limit:
        tasks = tasks[:a.limit]
    elif not a.all:
        print(f"К разбору: {len(tasks)} страниц ({sum(1 for t in tasks if t[1])} доменов с поиском реквизитов). Запустите с --all или --limit N.")
        return
    print(f"Разбираю {len(tasks)} страниц, потоков {a.threads}", flush=True)
    t0, n, found = time.time(), 0, 0
    with ThreadPoolExecutor(a.threads) as ex:
        futs = {ex.submit(analyze, u, wl): (u, wl) for u, wl in tasks}
        for f in as_completed(futs):
            u, wl = futs[f]
            try:
                r = f.result()
            except Exception as e:  # noqa: BLE001
                r = {"url": u, "url_key": url_key(u), "domain": host_of(u), "http_status": 0, "status": "error", "error": f"{type(e).__name__}: {e}"[:200],
                     "title": "", "h1": "", "site_name": "", "description": "", "legal": {}, "candidates": [], "pages_checked": []}
            save(db, r, wl)
            n += 1
            found += bool(wl and (r.get("legal") or {}).get("inn"))
            if n % 25 == 0:
                db.commit()
                print(f"{n}/{len(tasks)} | ИНН найден у {found} доменов | {time.time() - t0:.0f} c", flush=True)
    db.commit()
    print(f"Готово: {n} страниц, ИНН найден у {found} доменов, {time.time() - t0:.0f} c")


if __name__ == "__main__":
    sys.exit(main())
