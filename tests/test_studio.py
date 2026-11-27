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
    ml = studio.sanitize_layers([{"type": "text", "text": "−20%\\nНА   ПЕРВЫЙ визит\n\nвторая"}])[0]["text"]
    assert ml == "−20%\nНА ПЕРВЫЙ визит\nвторая", repr(ml)


def test_parse_json_variants():
    assert studio.parse_json('```json\n{"a": 1}\n```') == {"a": 1}
    assert studio.parse_json('Вот ответ: {"a": [1,2]} спасибо') == {"a": [1, 2]}


def test_fetch_image_blocks_private_hosts():
    a = _user("studio-fetch@example.com")
    for url in ("http://127.0.0.1/x.png", "http://localhost:8091/health", "ftp://example.com/a.png"):
        assert client.post("/api/v1/studio/fetch-image", json={"url": url}, headers=a).status_code == 422



def test_fetch_feed_blocks_private_hosts():
    a = _user("studio-fetch@example.com")
    for url in ("http://127.0.0.1/x.png", "http://localhost:8091/health", "ftp://example.com/a.png"):
        assert client.post("/api/v1/studio/fetch-feed", json={"url": url}, headers=a).status_code == 422

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


def _png_bytes(color="#00ff00"):
    from PIL import Image
    b = io.BytesIO(); Image.new("RGB", (600, 400), color).save(b, "PNG"); return b.getvalue()


def test_image_fallback_and_errors(monkeypatch):
    a = _user("studio-img@example.com")

    async def proxy_fail(*args):
        raise studio.ImageGenError("ИИ-прокси: gpt-image-2 — модель выключена на стороне сервиса")

    async def gemini_ok(prompt, example, aspect, n):
        assert aspect == "9:16" and example and example[0] == "image/png"
        return [_png_bytes()] * n
    monkeypatch.setattr(studio, "_gen_proxy", proxy_fail)
    monkeypatch.setattr(studio, "_gen_gemini", gemini_ok)
    ex = "data:image/png;base64," + base64.b64encode(_png_bytes("#ff0000")).decode()
    r = client.post("/api/v1/studio/ai/image", json={"prompt": "юрист в офисе", "example": ex, "format": "story", "count": 2}, headers=a)
    assert r.status_code == 200, r.text
    j = r.json()
    assert j["provider"] == "gemini" and len(j["images"]) == 2 and j["images"][0]["data_url"].startswith("data:image/")

    async def gemini_fail(*args):
        raise studio.ImageGenError("Gemini: бесплатный тариф без квоты на картинки — нужно включить оплату")
    monkeypatch.setattr(studio, "_gen_gemini", gemini_fail)
    r = client.post("/api/v1/studio/ai/image", json={"prompt": "юрист в офисе"}, headers=a)
    assert r.status_code == 503 and "выключена" in r.json()["detail"] and "оплату" in r.json()["detail"]


def test_explain_messages():
    assert "выключена" in studio._explain("X", 409, '{"code":"image_model_disabled","message":"This image model is currently disabled."}')
    assert "оплату" in studio._explain("X", 429, "generate_content_free_tier_requests")
    assert "лимит" in studio._explain("X", 429, "rate")


def test_proxy_async_jobs(monkeypatch):
    import asyncio
    import httpx as hx
    png = _png_bytes()
    polls = {"n": 0}

    def handler(req):
        if req.method == "POST" and req.url.path == "/v1/images/generations":
            body = __import__("json").loads(req.content)
            if body["model"] == "m-down":
                return hx.Response(202, json={"id": "j1", "status": "queued", "ready": False})
            return hx.Response(202, json={"id": "j2", "status": "queued", "ready": False})
        if req.url.path == "/v1/images/generations/j1":
            return hx.Response(200, json={"id": "j1", "status": "failed", "error": {"code": "image_service_unreachable"}})
        if req.url.path == "/v1/images/generations/j2":
            polls["n"] += 1
            if polls["n"] < 2:
                return hx.Response(200, json={"id": "j2", "status": "running", "ready": False})
            return hx.Response(200, json={"id": "j2", "status": "succeeded", "ready": True, "data": [{"url": "/v1/files/x.png"}]})
        if req.url.path == "/v1/files/x.png":
            return hx.Response(200, content=png, headers={"content-type": "image/png"})
        return hx.Response(404)
    real = hx.AsyncClient
    monkeypatch.setattr(studio.httpx, "AsyncClient", lambda **kw: real(transport=hx.MockTransport(handler), **{k: v for k, v in kw.items() if k != "transport"}))
    monkeypatch.setenv("STUDIO_AI_API_KEY", "k")
    monkeypatch.setenv("STUDIO_AI_API_BASE", "http://proxy.test")
    monkeypatch.setenv("STUDIO_IMAGE_MODELS", "m-down,m-ok")
    orig_sleep = asyncio.sleep
    monkeypatch.setattr(studio.asyncio, "sleep", lambda s: orig_sleep(0))
    out = asyncio.run(studio._gen_proxy("red circle", None, "1:1", 1))
    assert out == [png]
    monkeypatch.setenv("STUDIO_IMAGE_MODELS", "m-down")
    try:
        asyncio.run(studio._gen_proxy("red circle", None, "1:1", 1))
        raise AssertionError("должна быть ошибка")
    except studio.ImageGenError as exc:
        assert "временно недоступен" in str(exc)


def test_uploads_versions(monkeypatch):
    a, b = _user("studio-up@example.com"), _user("studio-up2@example.com")
    du = "data:image/png;base64," + base64.b64encode(_png_bytes("#123456")).decode()
    up = client.post("/api/v1/studio/uploads", json={"data_url": du, "name": "logo.png"}, headers=a).json()
    assert up["id"] and up["thumb"].startswith("data:image/") and up["width"] == 600
    assert [u["id"] for u in client.get("/api/v1/studio/uploads", headers=a).json()["uploads"]] == [up["id"]]
    assert client.get(f"/api/v1/studio/uploads/{up['id']}", headers=b).status_code == 404
    assert client.delete(f"/api/v1/studio/uploads/{up['id']}", headers=a).json()["ok"]
    monkeypatch.setattr(studio, "VERSION_EVERY", 0)
    did = client.post("/api/v1/studio/designs", json=_design("Версии"), headers=a).json()["id"]
    client.put(f"/api/v1/studio/designs/{did}", json=_design("Версии", [{"id": "L1", "type": "text", "text": "Второй"}]), headers=a)
    client.put(f"/api/v1/studio/designs/{did}", json=_design("Версии", [{"id": "L1", "type": "text", "text": "Третий"}]), headers=a)
    vs = client.get(f"/api/v1/studio/designs/{did}/versions", headers=a).json()["versions"]
    assert len(vs) == 2
    r = client.post(f"/api/v1/studio/designs/{did}/versions/{vs[-1]['id']}/restore", headers=a).json()
    assert r["data"]["layers"][0]["text"] == "Привет"
    assert client.get(f"/api/v1/studio/designs/{did}/versions", headers=b).status_code == 404


def test_user_templates():
    a = _user("studio-tpl@example.com")
    did = client.post("/api/v1/studio/designs", json=_design("Акция"), headers=a).json()["id"]
    t = client.post(f"/api/v1/studio/designs/{did}/as-template", json={"category": "Акции"}, headers=a).json()
    assert t["id"] != did and t["category"] == "Акции" and t["name"] == "Акция"
    assert [x["id"] for x in client.get("/api/v1/studio/templates", headers=a).json()["templates"]] == [t["id"]]
    assert t["id"] not in [x["id"] for x in client.get("/api/v1/studio/designs", headers=a).json()["designs"]]
    assert client.get(f"/api/v1/studio/designs/{t['id']}", headers=a).json()["data"]["layers"][0]["text"] == "Привет"


def test_fonts_upload_and_format_check():
    a, b = _user("studio-font@example.com"), _user("studio-font2@example.com")
    bad = client.post("/api/v1/studio/fonts", json={"family": "X", "data_base64": base64.b64encode(b"not a font" * 20).decode()}, headers=a)
    assert bad.status_code == 422
    ttf = b"\x00\x01\x00\x00" + b"\x00" * 300
    r = client.post("/api/v1/studio/fonts", json={"family": "Мой Бренд!", "filename": "brand.ttf", "data_base64": base64.b64encode(ttf).decode()}, headers=a).json()
    assert r["family"] == "Мой Бренд" and r["fmt"] == "truetype"
    assert client.post("/api/v1/studio/fonts", json={"family": "мой бренд", "data_base64": base64.b64encode(ttf).decode()}, headers=a).status_code == 409
    f = client.get(f"/api/v1/studio/fonts/{r['id']}/file", headers=a)
    assert f.status_code == 200 and f.content == ttf and f.headers["content-type"] == "font/ttf"
    assert client.get(f"/api/v1/studio/fonts/{r['id']}/file", headers=b).status_code == 404
    assert [x["family"] for x in client.get("/api/v1/studio/fonts", headers=a).json()["fonts"]] == ["Мой Бренд"]
    assert client.delete(f"/api/v1/studio/fonts/{r['id']}", headers=a).json()["ok"]


def test_remove_bg_endpoint(monkeypatch):
    a = _user("studio-rembg@example.com")

    async def fake(raw):
        return _png_bytes("#00ff00")
    monkeypatch.setattr(studio, "remove_background", fake)
    du = "data:image/jpeg;base64," + base64.b64encode(_png_bytes()).decode()
    r = client.post("/api/v1/studio/remove-bg", json={"image": du}, headers=a).json()
    assert r["data_url"].startswith("data:image/png;base64,") and r["width"] == 600


def test_notify_only_team_members():
    a = _user("studio-notify-a@example.com")
    b = _user("studio-notify-b@example.com")
    d = client.post("/api/v1/studio/designs", json=_design("Уведомления"), headers=a).json()
    # чужой пользователь не входит в команду дизайна — уведомление не уходит
    r = client.post(f"/api/v1/studio/designs/{d['id']}/notify", json={"kind": "mention", "user_ids": [999999], "text": "@кто-то"}, headers=a)
    assert r.status_code == 200 and r.json()["sent"] == 0
    assert client.post(f"/api/v1/studio/designs/{d['id']}/notify", json={"kind": "spam"}, headers=a).status_code == 422
    # без доступа к дизайну — нельзя
    assert client.post(f"/api/v1/studio/designs/{d['id']}/notify", json={"kind": "review"}, headers=b).status_code in (403, 404)


def test_presence_lists_peers():
    a = _user("studio-pres-a@example.com")
    d = client.post("/api/v1/studio/designs", json=_design("Вместе"), headers=a).json()
    r = client.post(f"/api/v1/studio/designs/{d['id']}/presence", json={"page": 0, "x": 10, "y": 20, "sel": ["L1"]}, headers=a).json()
    assert r["peers"] == [] and r["updated_at"] and r["me"]
    b = _user("studio-pres-b@example.com")
    assert client.post(f"/api/v1/studio/designs/{d['id']}/presence", json={}, headers=b).status_code in (403, 404)


def test_face_points_clean():
    from app.studio import face_points_clean
    raw = {"people": [
        {"head_box": [100, 50, 300, 300], "yaw": 0.2, "pose": "portrait",
         "face": {"eye_l": [150, 150], "eye_r": [250, 150], "eye_l_box": [130, 140, 170, 160], "chin": [200, 290],
                  "mouth": "teeth", "nose_tip": ["x", 1], "jaw_l": [5000, -3]}},
        {"face": {"eye_l": [1, 1]}},           # нет второго глаза — отбросить
        "junk"]}
    out = face_points_clean(raw, 400, 400)
    assert len(out["people"]) == 1
    p = out["people"][0]
    assert p["eye_l"] == [0.375, 0.375] and p["eye_l_box"] == [0.325, 0.35, 0.425, 0.4]
    assert p["mouth"] == "teeth" and p["pose"] == "portrait" and p["yaw"] == 0.2
    assert "nose_tip" not in p and p["jaw_l"] == [1.0, 0.0]
    assert face_points_clean("oops", 10, 10) == {"people": []}
