"""Студия креативов: хранение дизайнов, импорт из localStorage, санитайзер макетов ИИ."""
import base64
import io
import os
import tempfile
import time
from contextlib import closing
from pathlib import Path

os.environ.setdefault("PARSYA_ADMIN_USER", "test-admin")
os.environ.setdefault("PARSYA_ADMIN_PASSWORD", "test-password-do-not-use")
os.environ.setdefault("PARSYA_SECRET_KEY", "test-secret-key-that-is-long-enough-00000000")
os.environ.setdefault("PARSYA_WORKER_TOKEN", "test-worker-token-that-is-long-enough-00000000")
os.environ.setdefault("PARSYA_COOKIE_SECURE", "0")
os.environ.setdefault("PARSYA_SQLITE_PATH", str(Path(tempfile.gettempdir()) / f"parsya-test-{os.getpid()}.sqlite3"))
os.environ.setdefault("STUDIO_SQLITE_PATH", str(Path(tempfile.gettempdir()) / f"studio-test-{os.getpid()}.sqlite3"))

import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from fastapi.testclient import TestClient  # noqa: E402
from app import main as M  # noqa: E402
from app import studio  # noqa: E402

client = TestClient(M.app)


def _user(email: str) -> dict:
    with closing(M.connect()) as db:
        row = db.execute("SELECT id FROM users WHERE email=?", (email,)).fetchone()
        if row:
            uid = row["id"]
        else:
            uid = db.execute("INSERT INTO users(email,password_hash,role,is_active,created_at) VALUES(?,?,?,1,?)",
                             (email, M.hash_password("x" * 12), "client", time.strftime("%Y-%m-%d %H:%M:%S"))).lastrowid
            db.commit()
        token = M.issue_user_token(db, uid)
        db.commit()
    return {"Authorization": f"Bearer {token}"}


def _design(name="Дизайн", layers=None):
    return {"name": name, "format": "square",
            "data": {"name": name, "format": "square", "bg": {"type": "color", "color": "#ffffff"},
                     "layers": layers or [{"id": "L1", "type": "text", "text": "Привет", "x": 10, "y": 10, "w": 80, "h": 10}]},
            "thumb": "data:image/jpeg;base64,AAAA"}


def test_crud_and_isolation():
    a, b = _user("studio-a@example.com"), _user("studio-b@example.com")
    r = client.post("/api/v1/studio/designs", json=_design("Скидка"), headers=a)
    assert r.status_code == 200, r.text
    did = r.json()["id"]
    lst = client.get("/api/v1/studio/designs", headers=a).json()["designs"]
    assert any(d["id"] == did and d["name"] == "Скидка" for d in lst)
    assert "data" not in lst[0]  # список — без тяжёлых данных
    full = client.get(f"/api/v1/studio/designs/{did}", headers=a).json()
    assert full["data"]["layers"][0]["text"] == "Привет"
    # чужой пользователь не видит и не меняет
    assert client.get(f"/api/v1/studio/designs/{did}", headers=b).status_code == 404
    assert client.delete(f"/api/v1/studio/designs/{did}", headers=b).status_code == 404
    upd = _design("Скидка 2")
    assert client.put(f"/api/v1/studio/designs/{did}", json=upd, headers=a).json()["name"] == "Скидка 2"
    assert client.patch(f"/api/v1/studio/designs/{did}", json={"name": "Новое имя"}, headers=a).json()["name"] == "Новое имя"
    assert client.patch(f"/api/v1/studio/designs/{did}", json={"name": "x"}, headers=b).status_code == 404
    client.put(f"/api/v1/studio/designs/{did}", json=upd, headers=a)
    dup = client.post(f"/api/v1/studio/designs/{did}/duplicate", headers=a).json()
    assert dup["id"] != did and dup["name"].endswith("(копия)")
    assert client.get("/api/v1/studio/designs?q=копия", headers=a).json()["designs"][0]["id"] == dup["id"]
    assert client.delete(f"/api/v1/studio/designs/{did}", headers=a).json()["ok"]
    assert client.get(f"/api/v1/studio/designs/{did}", headers=a).status_code == 404


def test_requires_user():
    assert client.get("/api/v1/studio/designs").status_code in (401, 503)


def test_import_is_idempotent():
    a = _user("studio-imp@example.com")
    d1 = {**_design("Из браузера"), "local_id": "Lloc1"}
    r1 = client.post("/api/v1/studio/import", json={"designs": [d1, {**_design("Без id")}]}, headers=a).json()
    r2 = client.post("/api/v1/studio/import", json={"designs": [d1]}, headers=a).json()
    assert r1["imported"]["Lloc1"] == r2["imported"]["Lloc1"]
    names = [d["name"] for d in client.get("/api/v1/studio/designs", headers=a).json()["designs"]]
    assert names.count("Из браузера") == 1 and "Без id" not in names


def test_sanitize_layers():
    raw = [{"type": "text", "text": "  Скидка   30% ", "x": 500, "y": -99, "fontSize": 99, "color": "red", "font": "Comic"},
           {"type": "shape", "shape": "hexagon", "fill": {"color": "#112233"}, "radius": 999},
           {"type": "icon", "icon": "nope"}, {"type": "script"}, {"type": "text", "text": ""},
           {"type": "text", "text": "Кнопка", "bgColor": "#00aa00", "weight": "800"}]
    out = studio.sanitize_layers(raw)
    assert [l["type"] for l in out] == ["text", "shape", "icon", "text"]
    t = out[0]
    assert t["text"] == "Скидка 30%" and t["x"] == 100 and t["y"] == -20 and t["fontSize"] == 20
    assert t["color"] == "#111111" and t["font"] == "Onest" and t["role"] == ""
    assert out[1]["shape"] == "rect" and out[1]["fill"]["color"] == "#112233" and out[1]["radius"] == 80
    assert out[2]["icon"] == "star"
    assert out[3]["bgColor"] == "#00aa00" and out[3]["weight"] == "700"


def test_parse_json_variants():
    assert studio.parse_json('```json\n{"a": 1}\n```') == {"a": 1}
    assert studio.parse_json('Вот ответ: {"a": [1,2]} спасибо') == {"a": [1, 2]}


def test_fetch_image_blocks_private_hosts():
    a = _user("studio-fetch@example.com")
    for url in ("http://127.0.0.1/x.png", "http://localhost:8091/health", "ftp://example.com/a.png"):
        assert client.post("/api/v1/studio/fetch-image", json={"url": url}, headers=a).status_code == 422


def test_to_data_url_resizes():
    from PIL import Image
    buf = io.BytesIO()
    Image.new("RGB", (3000, 1500), "#ff0000").save(buf, "JPEG")
    data_url, w, h = studio._to_data_url(buf.getvalue(), 1600)
    assert data_url.startswith("data:image/jpeg;base64,") and (w, h) == (1600, 800)
    assert base64.b64decode(data_url.split(",", 1)[1])[:2] == b"\xff\xd8"


def test_ai_not_configured(monkeypatch):
    a = _user("studio-ai@example.com")
    monkeypatch.delenv("STUDIO_AI_API_KEY", raising=False)
    r = client.post("/api/v1/studio/ai/copy", json={"brief": "Банкротство физлиц"}, headers=a)
    assert r.status_code == 503 and "не настроен" in r.json()["detail"]


def test_ai_design_sanitized(monkeypatch):
    a = _user("studio-ai2@example.com")

    async def fake(system, content, max_tokens=4000):
        return '{"designs":[{"name":"Синий","bg":{"color":"#0033ff"},"layers":[{"type":"text","text":"Спишем долги","fontSize":9}]},' \
               '{"name":"Пустой","layers":[]}]}'
    monkeypatch.setattr(studio, "ai_complete", fake)
    r = client.post("/api/v1/studio/ai/design", json={"brief": "Банкротство", "format": "story", "count": 3}, headers=a)
    assert r.status_code == 200, r.text
    ds = r.json()["designs"]
    assert len(ds) == 1 and ds[0]["format"] == "story" and ds[0]["bg"]["color"] == "#0033ff"
    assert ds[0]["layers"][0]["text"] == "Спишем долги"
