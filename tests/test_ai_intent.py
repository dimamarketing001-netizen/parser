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
    # первый шаг — правила раскладывают по таксономии безтекстовый креатив, второй — Gemini (строки = старый формат)
    for _ in range(3):
        m._process_ai_intent_batch()
    with closing(m.connect()) as db:
        rows = {r["id"]: r["ai_intent"] for r in db.execute("SELECT id,ai_intent FROM creatives WHERE id>=9000")}
        tax = {r["id"]: (r["ai_intent_main"], r["ai_intent_sub"]) for r in db.execute("SELECT id,ai_intent_main,ai_intent_sub FROM creatives WHERE id>=9000")}
    assert tax[9000] == ("Одежда, обувь и аксессуары", "Обувь")
    assert tax[9001] == ("Госуслуги и общество", "Пенсии и льготы")
    assert tax[9002] == ("Не определено", "Не определено")
    with closing(m.connect()) as db:
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
        # пара «ключевое слово → интент» ещё не оценена Gemini => «определяется»
        rel0 = m._job_relevance(db, 901, "увеличение пенсии")
        assert rel0["ids"][9100] is None and rel0["ids"][9101] is True and rel0["ids"][9102] is None
        # Gemini оценил: «кроссовки» не подходят
        db.execute("INSERT INTO intent_keyword_match(keyword_key,intent_key,match) VALUES('увеличение пенсии','кроссовки',0)")
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


def test_ai_intents_scoped_to_job():
    pr = {"kind": "user", "user_id": 1}
    everything = m.api_list_ai_intents(principal=pr)
    scoped = m.api_list_ai_intents(job_id=901, principal=pr)
    assert scoped["total"] == 3 and scoped["job_id"] == 901
    assert {x["intent"] for x in scoped["items"]} == {"кроссовки", "пенсии и льготы"} and scoped["pending"] == 1
    assert everything["total"] >= scoped["total"]


def test_gemini_taxonomy_tuple_validated_and_tree(monkeypatch):
    """Gemini вернул раздел/подрубрику: верная пара сохраняется как есть, выдуманная — заменяется правилами."""
    with closing(m.connect()) as db:
        for i, (t, d) in enumerate([("Юрист по разводу", "раздел имущества"), ("Химчистка пальто", "за 1 день")]):
            db.execute("INSERT INTO creatives(id,creative_hash,platform,domain,title,description,captured_at) VALUES(?,?,?,?,?,?,?)",
                       (9200 + i, f"t{i}", "dzen", "x.ru", t, d, "2026-11-23T00:00:00+00:00"))
            db.execute("INSERT INTO occurrences(job_id,creative_id,platform,observed_at) VALUES(900,?,?,?)", (9200 + i, "dzen", "2026-11-23T00:00:01+00:00"))
        db.commit()
    monkeypatch.setattr(m.aii, "classify_batch", lambda items, known, **k: {
        9200: ("семейные споры", "юристы", "семейные споры"),      # регистр не важен — приводится к таксономии
        9201: ("химчистка одежды", "Чистка всего", "Пальто"),       # такого раздела нет — правила
    })
    for _ in range(3):
        m._process_ai_intent_batch()
    with closing(m.connect()) as db:
        tax = {r["id"]: (r["ai_intent_main"], r["ai_intent_sub"], r["ai_taxonomy_model"])
               for r in db.execute("SELECT id,ai_intent_main,ai_intent_sub,ai_taxonomy_model FROM creatives WHERE id IN (9200,9201)")}
    assert tax[9200][:2] == ("Юристы", "Семейные споры") and tax[9200][2] != m.taxonomy.VERSION
    assert tax[9201] == ("Хозяйство и уборка", "Химчистка одежды", m.taxonomy.VERSION)
    pr = {"kind": "user", "user_id": 1}
    tree = m.api_ai_intent_tree(job_id=900, principal=pr)
    mains = {x["main"]: x for x in tree["items"]}
    assert mains["Юристы"]["count"] == 1 and mains["Юристы"]["subs"][0]["sub"] == "Семейные споры"
    assert mains["Юристы"]["subs"][0]["intents"] == [{"intent": "семейные споры", "count": 1}]
    one = m.api_list_creatives(job_id=900, ai_tree="Юристы", principal=pr)
    assert [x["id"] for x in one["items"]] == [9200] and one["items"][0]["ai_intent_sub"] == "Семейные споры"
    both = m.api_list_creatives(job_id=900, ai_tree="Юристы~Хозяйство и уборка>Химчистка одежды", principal=pr)
    assert sorted(x["id"] for x in both["items"]) == [9200, 9201]
    assert m.api_list_creatives(job_id=900, ai_tree="Хозяйство и уборка>Уборка", principal=pr)["total"] == 0
    assert m.api_list_creatives(job_id=900, ai_tree="Юристы>Семейные споры>Семейные споры", principal=pr)["total"] == 1
    assert m.api_list_creatives(job_id=900, ai_tree="Юристы>Семейные споры>развод", principal=pr)["total"] == 0
    assert len(m.api_ai_taxonomy(principal=pr)["items"]) == 50
