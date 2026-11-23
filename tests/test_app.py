import os
import tempfile
from pathlib import Path

# Must be set before importing app.main because the application validates config at import.
os.environ["PARSYA_SECRET_KEY"] = "test-secret-key-that-is-long-enough-00000000"
os.environ["PARSYA_WORKER_TOKEN"] = "test-worker-token-that-is-long-enough-00000000"
os.environ["PARSYA_COOKIE_SECURE"] = "0"
os.environ.setdefault("PARSYA_AI_INTENT_ENABLED", "0")
os.environ["PARSYA_SQLITE_PATH"] = str(Path(tempfile.gettempdir()) / f"parsya-test-{os.getpid()}.sqlite3")
os.environ["PARSYA_LK_URL"] = "https://lk.test/app/ad-intelligence.html"

import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from contextlib import closing
from fastapi.testclient import TestClient
from app.main import app, connect, hash_password, now_iso

client = TestClient(app)
WORKER = {"Authorization": "Bearer test-worker-token-that-is-long-enough-00000000"}


def _user(email: str, role: str, password: str = "pw-test-123") -> dict:
    """Создаёт (или обновляет) учётку ЛК и входит через API ЛК — другого входа больше нет."""
    with closing(connect()) as db:
        row = db.execute("SELECT id FROM users WHERE email=?", (email,)).fetchone()
        if row:
            db.execute("UPDATE users SET role=?, password_hash=?, is_active=1 WHERE id=?", (role, hash_password(password), row["id"]))
        else:
            db.execute("INSERT INTO users(email,password_hash,role,is_active,created_at) VALUES(?,?,?,1,?)",
                       (email, hash_password(password), role, now_iso()))
        db.commit()
    r = client.post("/api/v1/auth/login", json={"email": email, "password": password})
    assert r.status_code == 200, r.text
    assert r.json()["role"] == role
    return {"Authorization": "Bearer " + r.json()["token"]}


def test_health_and_worker_auth():
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json()["status"] == "ok"

    bad = client.post("/api/worker/poll", json={"worker_id": "test", "hostname": "ci"})
    assert bad.status_code == 401

    good = client.post("/api/worker/poll", headers=WORKER, json={"worker_id": "ci-worker", "hostname": "ci", "version": "test"})
    assert good.status_code == 200
    assert good.json()["job"] is None


def test_old_console_is_gone_and_redirects_to_lk():
    """Старая серверная консоль удалена 23.11.2026: её адреса ведут в ЛК, входа по логину/паролю нет."""
    for path, view in (("/login", "console-overview"), ("/dashboard", "console-overview"), ("/admin/users", "console-overview"),
                       ("/runs/5", "intel-runs"), ("/new-run", "intel-runs"), ("/creatives", "intel-library"),
                       ("/creatives/1", "intel-library"), ("/", "intel-library")):
        r = client.get(path, follow_redirects=False)
        assert r.status_code == 303, path
        assert r.headers["location"].endswith(f"/app/ad-intelligence.html#{view}"), path
    assert client.post("/login", data={"username": "admin", "password": "x"}).status_code == 405
    for path in ("/api/admin/status", "/api/admin/embeddings-status", "/api/admin/jobs/1/browser/state", "/api/creatives/1/html"):
        assert client.get(path).status_code == 404, path


def test_lk_run_worker_sync_and_console():
    marketer = _user("marketer@test", "marketer")
    su = _user("creator@test", "superuser")

    created = client.post("/api/v1/runs", headers=marketer, json={"city": "Екатеринбург", "key_text": "увеличение пенсии"})
    assert created.status_code in (200, 201), created.text

    claimed = client.post("/api/worker/poll", headers=WORKER, json={"worker_id": "ci-worker", "hostname": "ci", "version": "test"})
    job = claimed.json()["job"]
    assert job["type"] == "run_parser"

    uploaded = client.post(f"/api/worker/jobs/{job['id']}/creatives", headers=WORKER, json={"creatives": [{
        "creative_hash": "sample-hash-ci", "platform": "example.test", "domain": "advertiser.test",
        "title": "Test debt ad", "description": "CI fixture", "image_url": "",
        "destination_url": "https://advertiser.test", "source_page_url": "https://example.test/article",
        "detection_method": "test-adapter", "recognition_status": "recognized",
        "captured_at": "2026-10-12T12:00:00+00:00",
        "html": "<!doctype html><html><body><h1>Test debt ad</h1></body></html>",
    }]})
    assert uploaded.status_code == 200 and uploaded.json()["accepted"] == 1

    # капча: воркер присылает снимок — создатель видит его в Консоли и кликает
    state = client.post(f"/api/worker/jobs/{job['id']}/browser/state", headers=WORKER, json={
        "active": True, "captcha_required": True, "url": "https://ya.ru/showcaptcha", "title": "Капча",
        "screenshot_b64": "iVBORw0KGgo=", "viewport_width": 800, "viewport_height": 600})
    assert state.status_code == 200, state.text
    ov = client.get("/api/v1/console/overview", headers=su)
    assert ov.status_code == 200
    body = ov.json()
    assert body["jobs"]["running"] >= 1 and body["creatives"]["total"] >= 1
    assert [c["job_id"] for c in body["captcha"]] == [job["id"]]
    assert any(a["id"] == job["id"] and a["captcha_required"] for a in body["active"])
    br = client.get(f"/api/v1/console/jobs/{job['id']}/browser", headers=su).json()
    assert br["captcha_required"] and br["screenshot_b64"] == "iVBORw0KGgo="
    assert client.post(f"/api/v1/console/jobs/{job['id']}/browser", headers=su, json={"action": "click", "x_ratio": 0.5, "y_ratio": 0.25}).status_code == 200
    cmd = client.get(f"/api/worker/jobs/{job['id']}/browser/command", headers=WORKER).json()
    assert cmd.get("command", cmd).get("action") == "click"

    # маркетолог и обычный admin в Консоль не попадают
    assert client.get("/api/v1/console/overview", headers=marketer).status_code == 403
    admin = _user("admin@test", "admin")
    assert client.get("/api/v1/console/users", headers=admin).status_code == 403

    finished = client.post(f"/api/worker/jobs/{job['id']}/finish", headers=WORKER,
                           json={"status": "succeeded", "exit_code": 0, "result": {"duration_seconds": 1}})
    assert finished.status_code == 200
    lib = client.get("/api/v1/creatives", headers=su, params={"job_id": job["id"]})
    assert lib.status_code == 200 and any(x["title"] == "Test debt ad" for x in lib.json()["items"])


def test_console_users():
    su = _user("creator@test", "superuser")
    made = client.post("/api/v1/console/users", headers=su, json={"email": "New@Test", "role": "marketer"})
    assert made.status_code == 200
    new = made.json()
    assert new["email"] == "new@test" and len(new["password"]) >= 10
    assert client.post("/api/v1/auth/login", json={"email": "new@test", "password": new["password"]}).status_code == 200
    assert client.post("/api/v1/console/users", headers=su, json={"email": "new@test"}).status_code == 409

    users = client.get("/api/v1/console/users", headers=su).json()
    me = next(u for u in users["items"] if u["email"] == "creator@test")
    assert me["role_title"] == "Создатель"

    assert client.patch(f"/api/v1/console/users/{new['id']}", headers=su, json={"role": "admin"}).status_code == 200
    reset = client.post(f"/api/v1/console/users/{new['id']}/reset-password", headers=su).json()
    assert client.post("/api/v1/auth/login", json={"email": "new@test", "password": reset["password"]}).status_code == 200
    assert client.patch(f"/api/v1/console/users/{new['id']}", headers=su, json={"is_active": False}).status_code == 200
    assert client.post("/api/v1/auth/login", json={"email": "new@test", "password": reset["password"]}).status_code == 401

    # единственного создателя нельзя разжаловать или отключить
    with closing(connect()) as db:
        db.execute("UPDATE users SET role='admin' WHERE role='superuser' AND email<>'creator@test'")
        db.commit()
    assert client.patch(f"/api/v1/console/users/{me['id']}", headers=su, json={"role": "marketer"}).status_code == 409
    assert client.patch(f"/api/v1/console/users/{me['id']}", headers=su, json={"is_active": False}).status_code == 409
