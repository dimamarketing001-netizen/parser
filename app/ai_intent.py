"""Gemini-классификация «интента креатива» (2026-11-13).

Два РАЗНЫХ понятия, которые нельзя путать:
  * ключевое слово запуска — по какому запросу парсер искал (jobs.key_text, например
    «увеличение пенсии»);
  * интент креатива — о чём реклама на самом деле (например «кроссовки»): его
    определяет Gemini по заголовку и тексту объявления и кладёт в creatives.ai_intent.
Реклама в РСЯ таргетируется по аудитории, а не по теме страницы, поэтому под запросом
про пенсию легко попасть на объявление про кроссовки — именно это различие здесь и нужно.
"""
from __future__ import annotations

import json
import os
import re
import time
from typing import Any, Callable

try:
    from google import genai
    from google.genai import types as genai_types
except Exception:  # pragma: no cover - optional dependency
    genai = None  # type: ignore[assignment]
    genai_types = None  # type: ignore[assignment]

from app import embeddings as emb

# Бесплатный тариф Google считает дневную квоту (RequestsPerDayPerProjectPerModel) ОТДЕЛЬНО
# для каждой модели, поэтому при её исчерпании переходим к следующей модели из списка
# (13.11: gemini-2.5-flash-lite упёрлась в дневной лимит, gemini-flash-lite-latest ответила).
MODELS = [m.strip() for m in os.getenv(
    "PARSYA_AI_INTENT_MODELS",
    "gemini-flash-lite-latest,gemini-2.5-flash-lite,gemini-3.5-flash-lite,gemini-3.1-flash-lite,gemini-2.5-flash,gemini-3.5-flash",
).split(",") if m.strip()]
MODEL = MODELS[0]
BATCH_SIZE = int(os.getenv("PARSYA_AI_INTENT_BATCH", "20"))
_blocked_until: dict[str, float] = {}   # модель -> monotonic-время, до которого она недоступна (дневная квота / сбой)
last_model_used = MODEL


def _is_daily_quota(exc: Exception) -> bool:
    details = getattr(exc, "details", None)
    if isinstance(details, dict):
        for item in details.get("error", {}).get("details", []):
            for v in item.get("violations", []) or []:
                if "PerDay" in str(v.get("quotaId", "")):
                    return True
    return False
MAX_KNOWN = 60  # сколько уже встречавшихся интентов подсказываем модели для единообразия
UNKNOWN = "не определён"

_PROMPT = """Ты классифицируешь рекламные объявления. Для КАЖДОГО объявления определи его интент —
о чём реклама на самом деле: какой товар, услуга или тема (по заголовку и тексту объявления).

Правила:
- интент — короткая тема: 1–3 слова, по-русски, строчными буквами, именительный падеж
  (например: «кроссовки», «увеличение пенсии», «банкротство физлиц», «кредитная карта»,
  «ремонт квартир», «недвижимость», «онлайн-курсы»);
- отвечай только о самом объявлении, НЕ ориентируйся на то, по какому запросу его нашли;
- если подходит уже встречавшийся интент из списка ниже — используй его ТОЧНО в такой же
  формулировке (не придумывай синонимы);
- если по тексту понять тему невозможно — «{unknown}».

Уже встречавшиеся интенты: {known}

Объявления (JSON): {items}

Верни JSON-массив объектов {{"id": <id объявления>, "intent": "<интент>"}} — по одному на каждое объявление."""


def _client() -> Any:
    if genai is None:
        raise RuntimeError("google-genai package not installed")
    key = os.getenv("GEMINI_API_KEY") or os.getenv("GOOGLE_API_KEY")
    if not key:
        raise RuntimeError("GEMINI_API_KEY/GOOGLE_API_KEY not set")
    return genai.Client(api_key=key)


def normalize_intent(value: str | None) -> str:
    # запятая/двоеточие/«|» — служебные разделители в фильтрах интерфейса и API
    text = re.sub(r"[,:;|]+", " ", (value or "").strip().lower())
    text = re.sub(r"\s+", " ", text).strip(" .\"'«»")
    if not text:
        return UNKNOWN
    return text[:80]


def classify_batch(
    items: list[dict[str, Any]],
    known_intents: list[str],
    max_wait_seconds: float = 3600,
    on_hard_failure: Callable[[str], None] | None = None,
) -> dict[int, str] | None:
    """items: [{id,title,description}]. Возвращает {id: intent} либо None при жёсткой ошибке.
    429 переживаем так же, как embeddings.py: ждём ровно столько, сколько просит Google."""
    if not items:
        return {}
    payload = [
        {"id": it["id"], "title": (it.get("title") or "")[:300], "description": (it.get("description") or "")[:500]}
        for it in items
    ]
    prompt = _PROMPT.format(
        unknown=UNKNOWN,
        known=", ".join(known_intents[:MAX_KNOWN]) or "пока нет",
        items=json.dumps(payload, ensure_ascii=False),
    )
    schema = {
        "type": "ARRAY",
        "items": {"type": "OBJECT", "properties": {"id": {"type": "INTEGER"}, "intent": {"type": "STRING"}}, "required": ["id", "intent"]},
    }
    data = _call_json(prompt, schema, max_wait_seconds, on_hard_failure)
    if data is None:
        return None
    wanted = {it["id"] for it in items}
    return {int(row["id"]): normalize_intent(row.get("intent")) for row in data if int(row["id"]) in wanted}


def _call_json(prompt: str, schema: dict, max_wait_seconds: float = 3600, on_hard_failure: Callable[[str], None] | None = None) -> Any | None:
    """Один JSON-запрос к Gemini с перебором моделей (дневная квота считается по модели) и
    ожиданием минутных 429. Возвращает разобранный JSON либо None при жёстком сбое."""
    config = genai_types.GenerateContentConfig(temperature=0, response_mime_type="application/json", response_schema=schema)
    waited = 0.0
    global last_model_used
    while True:
        now = time.monotonic()
        available = [m for m in MODELS if _blocked_until.get(m, 0) <= now]
        if not available:
            soonest = min(_blocked_until.get(m, 0) for m in MODELS) - now
            sleep_for = min(max(soonest, 30), max_wait_seconds - waited)
            if sleep_for <= 0:
                msg = f"ai-intent: все модели недоступны (дневные квоты), ждали {waited:.0f}s"
                print(f"[ai-intent] {msg}", flush=True)
                if on_hard_failure:
                    on_hard_failure(msg)
                return None
            print(f"[ai-intent] все модели на паузе (дневная квота), жду {sleep_for:.0f}s", flush=True)
            time.sleep(sleep_for)
            waited += sleep_for
            continue
        model = available[0]
        try:
            client = _client()  # держим ссылку: временный клиент закрывается GC до конца запроса
            resp = client.models.generate_content(model=model, contents=prompt, config=config)
            data = json.loads(resp.text)
            last_model_used = model
            return data
        except Exception as exc:  # noqa: BLE001
            delay = emb.parse_retry_delay_seconds(exc)
            if delay is not None and _is_daily_quota(exc):
                _blocked_until[model] = time.monotonic() + delay + 5
                print(f"[ai-intent] {model}: дневная квота исчерпана, перехожу к следующей модели", flush=True)
                continue
            if delay is not None:  # минутный лимит: ждём, как просит Google
                sleep_for = min(delay + 1, max_wait_seconds - waited)
                if sleep_for <= 0:
                    msg = f"ai-intent: gave up after waiting {waited:.0f}s for quota"
                    print(f"[ai-intent] {msg}", flush=True)
                    if on_hard_failure:
                        on_hard_failure(msg)
                    return None
                print(f"[ai-intent] {model}: rate-limited, waiting {sleep_for:.0f}s", flush=True)
                time.sleep(sleep_for)
                waited += sleep_for
                continue
            # 503/500/кривой ответ: эту модель откладываем на 10 минут и пробуем следующую
            _blocked_until[model] = time.monotonic() + 600
            print(f"[ai-intent] {model}: сбой ({type(exc).__name__}: {str(exc)[:160]}), пробую другую модель", flush=True)
            if all(_blocked_until.get(m, 0) > time.monotonic() for m in MODELS):
                msg = f"ai-intent call failed on every model, last: {type(exc).__name__}: {str(exc)[:250]}"
                if on_hard_failure:
                    on_hard_failure(msg)
                for m in MODELS:
                    _blocked_until.pop(m, None)
                return None


_JUDGE_PROMPT = """Поисковый запрос (ключевое слово запуска): «{kw}».
Для каждого интента рекламы определи, подходит ли реклама с таким интентом под этот запрос.

Подходит — ТОЛЬКО если реклама про ту же тему, товар или услугу, что и запрос, то есть человек,
искавший этот запрос, получил ответ по теме. НЕ подходит реклама, которая просто нацелена на ту
же аудиторию или находится рядом по смыслу (например, для запроса «увеличение пенсии» подходят
«пенсии и льготы», «перерасчёт пенсии», «юридическая помощь по пенсиям»; НЕ подходят
«имплантация зубов», «недвижимость», «автомобили», «вакансии», даже если реклама адресована пенсионерам).
Если интент слишком общий и сам не называет тему запроса («юридические услуги», «финансы», «медицинские услуги», «банк») — тоже НЕ подходит.

Интенты: {intents}

Верни JSON-массив объектов {{"intent": "<интент ровно как в списке>", "match": true|false}} — по одному на каждый интент."""


def judge_intent_matches(keyword: str, intents: list[str], on_hard_failure: Callable[[str], None] | None = None) -> dict[str, bool] | None:
    """{интент(lower): подходит ли под ключевое слово запуска}. None — жёсткий сбой (повторим позже)."""
    if not intents:
        return {}
    schema = {
        "type": "ARRAY",
        "items": {"type": "OBJECT", "properties": {"intent": {"type": "STRING"}, "match": {"type": "BOOLEAN"}}, "required": ["intent", "match"]},
    }
    prompt = _JUDGE_PROMPT.format(kw=keyword, intents=json.dumps(intents, ensure_ascii=False))
    data = _call_json(prompt, schema, on_hard_failure=on_hard_failure)
    if data is None:
        return None
    out = {str(row.get("intent", "")).strip().lower(): bool(row.get("match")) for row in data}
    return {i.lower(): out[i.lower()] for i in intents if i.lower() in out}
