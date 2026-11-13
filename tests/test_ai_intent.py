import os, sys, tempfile
from pathlib import Path
os.environ.setdefault("PARSYA_ADMIN_USER", "test-admin")
os.environ.setdefault("PARSYA_ADMIN_PASSWORD", "test-password-do-not-use")
os.environ.setdefault("PARSYA_SECRET_KEY", "test-secret-key-that-is-long-enough-00000000")
os.environ.setdefault("PARSYA_WORKER_TOKEN", "test-worker-token-that-is-long-enough-00000000")
os.environ.setdefault("PARSYA_COOKIE_SECURE", "0")
os.environ.setdefault("PARSYA_AI_INTENT_ENABLED", "0")
os.environ.setdefault("PARSYA_SQLITE_PATH", str(Path(tempfile.gettempdir()) / f"parsya-test-ai-{os.getpid()}.sqlite3"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from contextlib import closing
import app.main as m


def _seed(db):
    db.execute("INSERT OR IGNORE INTO users(id,email,password_hash,created_at) VALUES(1,'t@t','x','2026-01-01')")
    db.execute("INSERT INTO jobs(id,status,city,key_text,created_at,owner_user_id) VALUES(900,'running','Москва','увеличение пенсии','2026-11-13T00:00:00+00:00',1)")
    for i, (t, d) in enumerate([("Беговые кроссовки Nike", "скидка"), ("Пенсия: льготы", "для пенсионеров"), ("", "")]):
        db.execute("INSERT INTO creatives(id,creative_hash,platform,domain,title,description,captured_at) VALUES(?,?,?,?,?,?,?)",
                   (9000 + i, f"h{i}", "dzen", "x.ru", t, d, "2026-11-13T00:00:00+00:00"))
        db.execute("INSERT INTO occurrences(job_id,creative_id,platform,observed_at) VALUES(900,?,?,?)", (9000 + i, "dzen", "2026-11-13T00:00:01+00:00"))
    db.commit()


def test_classify_filter_and_runs(monkeypatch):
    with closing(m.connect()) as db:
        _seed(db)
    m.init_ai_intent()  # безтекстовый креатив помечается сразу, без Gemini
    monkeypatch.setattr(m.aii, "classify_batch", lambda items, known, **k: {it["id"]: ("кроссовки" if "кроссов" in (it["title"] or "").lower() else "пенсии и льготы") for it in items})
    assert m._process_ai_intent_batch() is True
    with closing(m.connect()) as db:
        rows = {r["id"]: r["ai_intent"] for r in db.execute("SELECT id,ai_intent FROM creatives WHERE id>=9000")}
        assert rows == {9000: "кроссовки", 9001: "пенсии и льготы", 9002: m.aii.UNKNOWN}
        items = [m._creative_row_to_dict(r) for r in db.execute("SELECT * FROM creatives WHERE id=9000")]
        m._attach_intents(db, items)
    it = items[0]
    assert it["ai_intent"] == "кроссовки" and it["intents"] == ["увеличение пенсии"]
    assert it["runs"][0]["job_id"] == 900 and it["runs"][0]["key_text"] == "увеличение пенсии"
    out = m.api_list_creatives(ai_intent="Кроссовки", principal={"kind": "user", "user_id": 1})
    assert [x["id"] for x in out["items"]] == [9000]
    assert m.api_list_creatives(job_id=900, ai_intent="пенсии и льготы", principal={"kind": "user", "user_id": 1})["total"] == 1
    ai = m.api_list_ai_intents(principal={"kind": "user", "user_id": 1})
    assert ai["pending"] == 0 and {x["intent"] for x in ai["items"]} >= {"кроссовки", "пенсии и льготы"}


def test_job_relevance(monkeypatch):
    with closing(m.connect()) as db:
        db.execute("INSERT OR IGNORE INTO users(id,email,password_hash,created_at) VALUES(1,'t@t','x','2026-01-01')")
        db.execute("INSERT INTO jobs(id,status,city,key_text,created_at,owner_user_id) VALUES(901,'running','Москва','увеличение пенсии','2026-11-13T00:00:00+00:00',1)")
        rows = [(9100, "Беговые кроссовки", "скидка", "кроссовки"),
                (9101, "Льготы", "для пенсионеров", "пенсии и льготы"),
                (9102, "Диван", "мебель", None)]
        for i, t, d, ai in rows:
            db.execute("INSERT INTO creatives(id,creative_hash,platform,domain,title,description,captured_at,ai_intent) VALUES(?,?,?,?,?,?,?,?)",
                       (i, f"r{i}", "dzen", "x.ru", t, d, "2026-11-13T00:00:00+00:00", ai))
            db.execute("INSERT INTO occurrences(job_id,creative_id,platform,observed_at) VALUES(901,?,?,?)", (i, "dzen", "2026-11-13T00:00:01+00:00"))
        db.commit()
        rel = m._job_relevance(db, 901, "увеличение пенсии")
    assert rel["found"] == 3 and rel["relevant"] == 1 and rel["pending"] == 1
    assert rel["ids"][9101] is True and rel["ids"][9100] is False and rel["ids"][9102] is None
    pr = {"kind": "user", "user_id": 1}
    out = m.api_list_creatives(job_id=901, only_relevant="1", principal=pr)
    assert [x["id"] for x in out["items"]] == [9101] and out["items"][0]["job_match"] is True
    allj = m.api_list_creatives(job_id=901, principal=pr)
    assert {x["id"]: x["job_match"] for x in allj["items"]} == {9100: False, 9101: True, 9102: None}
    runs = m.api_list_runs(principal=pr)
    assert [r for r in runs["items"] if r["id"] == 901][0]["relevance"]["relevant"] == 1
    assert m.api_run_detail(901, principal=pr)["relevance"]["found"] == 3
