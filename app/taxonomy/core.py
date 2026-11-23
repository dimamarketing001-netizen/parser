# -*- coding: utf-8 -*-
"""Таксономия интентов креативов: основной интент (раздел) → под-интент (подрубрика).
Источники: Яндекс Услуги (19 разделов), Яндекс Маркет (товары), доп. разделы. См. ParsYa/intent_taxonomy/.
classify() — разбор по правилам без Gemini (rules.py + ручная таблица intent_map.py)."""
import json
import re
from functools import lru_cache
from pathlib import Path

from . import rules as R
from .intent_map import INTENT_MAP

HERE = Path(__file__).parent
VERSION = "claude-rules-v1"


@lru_cache(maxsize=1)
def taxonomy() -> list[dict]:
    return json.loads((HERE / "taxonomy.json").read_text(encoding="utf-8"))


@lru_cache(maxsize=1)
def valid_pairs() -> dict[str, set[str]]:
    return {r["main"]: {s["name"] for s in r["subs"]} for r in taxonomy()}


def _canon(value: str | None) -> str:
    return " ".join((value or "").replace("\xa0", " ").replace("ё", "е").lower().split())


@lru_cache(maxsize=1)
def _canon_index() -> tuple[dict, dict]:
    mains = {_canon(m): m for m in valid_pairs()}
    subs = {(m, _canon(s)): s for m, ss in valid_pairs().items() for s in ss}
    return mains, subs


def validate(main: str | None, sub: str | None) -> tuple[str, str] | None:
    """Пара из ответа Gemini → точные названия таксономии (регистр/ё не важны) либо None."""
    mains, subs = _canon_index()
    m = mains.get(_canon(main))
    if not m:
        return None
    s = subs.get((m, _canon(sub))) or subs.get((m, "другое"))
    return (m, s) if s else None


def prompt_list() -> str:
    """Таксономия одной строкой на раздел — для промпта Gemini."""
    return "\n".join(f"{r['main']}: " + "; ".join(s["name"] for s in r["subs"]) for r in taxonomy())


DOMAINS = [
    (r"cian\.ru|domclick|novostroy|avito\.ru/.*nedvizh|pik\.ru|samolet\.ru|etalongroup|lsr\.ru|brusnika|level\.ru|a101\.ru|forma\.ru|fsk\.ru|donstroy|granelle|kortros|atomstroy", R.RE_, "Новостройки"),
    (r"hh\.ru|superjob|rabota\.ru|zarplata\.ru|trudvsem", R.JOB, "Вакансии"),
    (r"auto\.ru|drom\.ru|autospot|carprice", R.AUTO, "Новые автомобили"),
    (r"skillbox|geekbrains|netology|skypro|practicum\.yandex|skillfactory|otus\.ru|eduson|contented|productstar|hexlet", R.EDU, "Онлайн-курсы профессий"),
    (r"sravni\.ru|banki\.ru|vbr\.ru", R.FIN, "Кредиты"),
    (r"ozon\.ru|wildberries|market\.yandex|megamarket|aliexpress|lamoda", R.SHOP, "Маркетплейсы"),
    (r"tutu\.ru|aviasales|ostrovok|travel\.yandex|onetwotrip|level\.travel|travelata|sutochno|101hotels", R.TOUR, "Отели"),
    (r"kinopoisk|ivi\.ru|okko|start\.ru|wink\.ru|premier\.one", R.FUN, "Кино, сериалы, онлайн-кинотеатры"),
    (r"prodoctorov|docdoc|napopravku|sberhealth", R.MED, "Поиск врача и запись"),
    (r"amocrm|bitrix24|retailcrm|megaplan|yclients", R.BIZ, "CRM-системы"),
]
DOMAINS = [(re.compile(p), m, s) for p, m, s in DOMAINS]


def first(rules, text, main=None):
    for rx, m, s in rules:
        if main and m != main:
            continue
        if rx.search(text):
            return m, s
    return None


def classify(title, description, cta, ai_intent, url):
    text = R.norm(" | ".join(x for x in (title, description, cta) if x))
    has_text = bool(text.strip(" |"))
    intent = R.norm(ai_intent).strip()
    intent_ok = intent not in R.UNKNOWN_INTENTS
    t_strong = first(R.RULES, text) if has_text else None
    # ручная таблица редких интентов важнее правил по строке интента
    i_strong = (INTENT_MAP.get(intent) or first(R.RULES, " " + intent + " ")) if intent_ok else None
    broad = R.INTENT_MAIN.get(intent) if intent_ok else None
    u = (url or "").lower()
    dom = next(((m, s) for rx, m, s in DOMAINS if rx.search(u)), None)
    if i_strong and not broad:
        # конкретный интент Gemini задаёт раздел; текст уточняет подрубрику внутри него
        hit = first(R.RULES, text, i_strong[0]) if has_text else None
        return (hit + ("intent+text",)) if hit else (i_strong + ("intent",))
    if broad:
        fam = R.FAMILY.get(broad, (broad,))
        for m in fam:  # текст уточняет подрубрику в разделе широкого интента (и в родственных)
            hit = first(R.RULES, text, m) if has_text else None
            if hit: return hit + ("broad+text",)
        for m in fam:
            hit = first(R.WEAK_RULES, text, m) if has_text else None
            if hit: return hit + ("broad+weak",)
        if intent in R.BROAD_OVERRIDABLE:  # в тексте нет ничего из раздела Gemini — переносим по тексту
            if t_strong: return t_strong + ("text-override",)
            if dom: return dom + ("domain",)
        return broad, R.BROAD_DEFAULT.get(intent, "Другое"), "broad"
    if t_strong: return t_strong + ("text",)
    if dom: return dom + ("domain",)
    hit = first(R.WEAK_RULES, text) if has_text else None
    if hit: return hit + ("text-weak",)
    if intent_ok:
        hit = first(R.WEAK_RULES, " " + intent + " ")
        if hit: return hit + ("intent-weak",)
    return R.UNK, "Не определено", "none"


