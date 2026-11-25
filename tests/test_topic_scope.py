"""Аналитика по теме объявлений (ai_tree) и фильтры «кроме» (25.11.2026)."""
import os, sys, tempfile
from pathlib import Path
os.environ.setdefault("PARSYA_SECRET_KEY", "test-secret-key-that-is-long-enough-00000000")
os.environ.setdefault("PARSYA_WORKER_TOKEN", "test-worker-token-that-is-long-enough-00000000")
os.environ.setdefault("PARSYA_COOKIE_SECURE", "0")
os.environ.setdefault("PARSYA_AI_INTENT_ENABLED", "0")
os.environ.setdefault("PARSYA_SQLITE_PATH", str(Path(tempfile.gettempdir()) / f"parsya-test-scope-{os.getpid()}.sqlite3"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from contextlib import closing
from datetime import datetime
import pytest
import app.main as m

PR = {"kind": "user", "user_id": 78, "role": "superuser", "email": "s@t"}
NOW = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")


@pytest.fixture(scope="module", autouse=True)
def seed():
    with closing(m.connect()) as db:
        db.execute("INSERT OR IGNORE INTO users(id,email,password_hash,role,created_at) VALUES(78,'s@t','x','superuser','2026-01-01')")
        db.execute("INSERT OR IGNORE INTO jobs(id,status,city,key_text,created_at,owner_user_id) VALUES(7800,'succeeded','Москва','юрист',?,78)", (NOW,))
        rows = [(7801, "Юрист по разводу", "Юристы", "Семейные споры", "развод", "video"),
                (7802, "Взыщем долг", "Юристы", "Взыскание долгов", "долги", None),
                (7803, "Химчистка пальто", "Хозяйство и уборка", "Химчистка одежды", "химчистка", None),
                (7804, "Без темы", None, None, None, None)]
        for cid, title, mn, sb, ai, ctype in rows:
            db.execute("INSERT OR IGNORE INTO creatives(id,creative_hash,content_fingerprint,platform,domain,title,captured_at,ai_intent,ai_intent_main,ai_intent_sub,creative_type) "
                       "VALUES(?,?,?,?,?,?,?,?,?,?,?)", (cid, f"h{cid}", f"fp{cid}", "dzen", "x.ru", title, NOW, ai, mn, sb, ctype))
            db.execute("INSERT INTO occurrences(job_id,creative_id,platform,observed_at) VALUES(7800,?,?,?)", (cid, "dzen", NOW))
        db.commit()


def ids(**kw):
    r = m.api_list_creatives(limit=50, source="db", principal=PR, **{"date_from": "", **kw})
    return sorted(x["id"] for x in r["items"] if 7800 < x["id"] < 7900)


def test_tree_include_and_exclude():
    assert ids(ai_tree="Юристы") == [7801, 7802]
    assert ids(ai_tree="Юристы>Семейные споры") == [7801]
    # «кроме»: объявления без темы остаются
    assert ids(ai_tree="!Юристы") == [7803, 7804]
    assert ids(ai_tree="!Юристы>Семейные споры~Хозяйство и уборка") == [7802, 7804]


def test_other_filters_exclude():
    assert ids(ai_intent="!развод") == [7802, 7803, 7804]
    assert ids(format="video") == [7801]
    assert 7801 not in ids(format="!video")


def test_analytics_respect_topic():
    s = m.api_analytics_summary(ai_tree="Юристы", principal=PR)
    assert {(t["main"], t["sub"]) for t in s["topics"]} >= {("Юристы", "Семейные споры"), ("Юристы", "Взыскание долгов")}
    assert all(t["main"] == "Юристы" for t in s["topics"])
    s2 = m.api_analytics_summary(ai_tree="!Юристы", principal=PR)
    assert all(t["main"] != "Юристы" for t in s2["topics"])
    d = m.api_daily_creatives(days=2, ai_tree="Хозяйство и уборка", principal=PR)
    assert sum(x["count"] for x in d["items"]) >= 1
    lead = m.api_analytics_leaders(ai_tree="Юристы", principal=PR)
    themes = {t["intent"] for t in lead["themes"]}
    assert "развод" in themes and "химчистка" not in themes
    # остальные отчёты принимают ai_tree без ошибок
    cov = next(r.endpoint for r in m.app.routes if getattr(r, "path", "") == "/api/v1/coverage")
    assert cov(ai_tree="Хозяйство и уборка", principal=PR)["totals"]["creatives"] >= 1
    for fn in (cov, m.api_cross_platform_reuse, m.api_carousels, m.api_ad_signals, m.api_campaigns,
               m.api_keywords, m.api_fastest_growing, m.api_landing_mismatch, m.api_list_advertisers):
        fn(ai_tree="!Юристы>Семейные споры", principal=PR)
