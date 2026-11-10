"""Ozon для addAvto через Chrome в WSL + ручная капча (2026-11-10)."""
import os
import tempfile
from pathlib import Path

os.environ.setdefault("PARSYA_ADMIN_USER", "test-admin")
os.environ.setdefault("PARSYA_ADMIN_PASSWORD", "test-password-do-not-use")
os.environ.setdefault("PARSYA_SECRET_KEY", "test-secret-key-that-is-long-enough-00000000")
os.environ.setdefault("PARSYA_WORKER_TOKEN", "test-worker-token-that-is-long-enough-00000000")
os.environ.setdefault("PARSYA_COOKIE_SECURE", "0")
os.environ.setdefault("PARSYA_SQLITE_PATH", str(Path(tempfile.gettempdir()) / f"parsya-test-{os.getpid()}.sqlite3"))

import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from fastapi.testclient import TestClient
import app.main as m

client = TestClient(m.app)
TOKEN = "addavto-test-token-long-enough-000000"
A = {"Authorization": f"Bearer {TOKEN}"}
SID = "testsession123"


def _setup(monkeypatch):
    monkeypatch.setattr(m, "ADDAVTO_TOKEN", TOKEN)
    m.app.dependency_overrides[m.public_api_principal] = lambda: {"kind": "user", "user_id": 1, "email": "u@x", "role": "user"}
    m._OZON_CAPTCHA.clear()
    sent = []
    monkeypatch.setattr(m, "notify_telegram_alert", lambda text: sent.append(text) or True)
    return sent


def teardown_function(_):
    m.app.dependency_overrides.clear()


def test_fetch_runs_script_with_session(monkeypatch):
    _setup(monkeypatch)
    calls = []

    def fake_run(script, args, stdin_payload, timeout, detail):
        calls.append((script, stdin_payload, timeout))
        return {"pages": [{"widgetStates": {}}, None], "captcha_solved": False}, None, ""

    monkeypatch.setattr(m, "_run_addavto_selenium_script", fake_run)
    r = client.post("/api/addavto/ozon-fetch", headers=A, json={"path": "/product/x-1/", "extra_paths": ["/product/x-1/?p=2"], "session_id": SID, "label": "проект #7"})
    assert r.status_code == 200, r.text
    assert calls[0][0] == "ozon_fetch.py" and calls[0][1]["session_id"] == SID and calls[0][1]["path"] == "/product/x-1/"
    assert m._OZON_CAPTCHA[SID]["active"] is False  # после завершения сессия закрыта
    assert not m._OZON_FETCH_LOCK.locked()


def test_fetch_validation_and_auth(monkeypatch):
    _setup(monkeypatch)
    assert client.post("/api/addavto/ozon-fetch", json={"path": "/product/x/", "session_id": SID}).status_code == 401
    assert client.post("/api/addavto/ozon-fetch", headers=A, json={"path": "/evil", "session_id": SID}).status_code == 422
    assert client.post("/api/addavto/ozon-fetch", headers=A, json={"path": "/product/x/", "session_id": "../x"}).status_code == 400


def test_captcha_timeout_is_409(monkeypatch):
    _setup(monkeypatch)
    monkeypatch.setattr(m, "_run_addavto_selenium_script",
                        lambda *a: (None, {"error": "Капчу Ozon никто не прошёл вовремя", "captcha_timeout": True}, ""))
    r = client.post("/api/addavto/ozon-fetch", headers=A, json={"path": "/product/x/", "session_id": SID})
    assert r.status_code == 409 and "Капчу" in r.json()["detail"]


def test_manual_captcha_roundtrip_and_single_telegram(monkeypatch):
    sent = _setup(monkeypatch)
    m._OZON_CAPTCHA[SID] = {"active": True, "captcha_required": False, "label": "проект #7", "notified": False, "command": None}
    # пока капчи нет — скриншот не отдаётся, команды не принимаются только для неактивной
    st = {"active": True, "captcha_required": True, "url": "https://www.ozon.ru/x", "title": "Antibot Captcha",
          "screenshot_b64": "QUJD", "viewport_width": 1280, "viewport_height": 800}
    assert client.post(f"/api/addavto/ozon-captcha/{SID}/state", headers=A, json=st).status_code == 200
    assert client.post(f"/api/addavto/ozon-captcha/{SID}/state", headers=A, json=st).status_code == 200
    import time; time.sleep(0.2)
    assert len(sent) == 1 and f"ozon-captcha.html?s={SID}" in sent[0] and "проект #7" in sent[0]

    view = client.get(f"/api/v1/ozon-captcha/{SID}").json()
    assert view["captcha_required"] and view["screenshot_b64"] == "QUJD"

    bad = client.post(f"/api/v1/ozon-captcha/{SID}/command", json={"action": "drag", "x_ratio": 0.1, "y_ratio": 0.5})
    assert bad.status_code == 422
    ok = client.post(f"/api/v1/ozon-captcha/{SID}/command",
                     json={"action": "drag", "x_ratio": 0.1, "y_ratio": 0.5, "to_x_ratio": 0.6, "to_y_ratio": 0.5})
    assert ok.status_code == 200
    cmd = client.get(f"/api/addavto/ozon-captcha/{SID}/command", headers=A).json()["command"]
    assert cmd["action"] == "drag" and cmd["to_x_ratio"] == 0.6
    assert client.get(f"/api/addavto/ozon-captcha/{SID}/command", headers=A).json()["command"] is None  # выдаётся один раз

    client.post(f"/api/addavto/ozon-captcha/{SID}/state", headers=A, json={"active": False, "captcha_required": False})
    assert client.post(f"/api/v1/ozon-captcha/{SID}/command", json={"action": "refresh"}).status_code == 404
    assert client.get(f"/api/v1/ozon-captcha/{SID}").json()["screenshot_b64"] == ""


def test_ozon_captcha_view_requires_user(monkeypatch):
    monkeypatch.setattr(m, "ADDAVTO_TOKEN", TOKEN)
    m.app.dependency_overrides.clear()
    assert client.get(f"/api/v1/ozon-captcha/{SID}").status_code in (401, 503)


def test_gemini_gateway(monkeypatch):
    _setup(monkeypatch)
    seen = {}

    async def fake(model, body, timeout=300.0):
        seen.update(model=model, body=body)
        return 429, {"error": {"message": "quota"}}

    monkeypatch.setattr(m, "gemini_generate_content", fake)
    assert client.post("/api/addavto/gemini/generate", json={"model": "x", "body": {}}).status_code == 401
    assert client.post("/api/addavto/gemini/generate", headers=A, json={"model": "../x", "body": {}}).status_code == 422
    r = client.post("/api/addavto/gemini/generate", headers=A, json={"model": "gemini-3.8-flash", "body": {"contents": []}})
    assert r.status_code == 429 and r.json()["error"]["message"] == "quota"  # статус Google как есть
    assert seen == {"model": "gemini-3.8-flash", "body": {"contents": []}}
