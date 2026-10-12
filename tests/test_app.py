import os
import re
import tempfile
from pathlib import Path

# Must be set before importing app.main because the application validates config at import.
os.environ["PARSYA_ADMIN_USER"] = "test-admin"
os.environ["PARSYA_ADMIN_PASSWORD"] = "test-password-do-not-use"
os.environ["PARSYA_SECRET_KEY"] = "test-secret-key-that-is-long-enough-00000000"
os.environ["PARSYA_WORKER_TOKEN"] = "test-worker-token-that-is-long-enough-00000000"
os.environ["PARSYA_COOKIE_SECURE"] = "0"
os.environ["PARSYA_SQLITE_PATH"] = str(Path(tempfile.gettempdir()) / f"parsya-test-{os.getpid()}.sqlite3")

import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from fastapi.testclient import TestClient
from app.main import app

client = TestClient(app)


def _csrf_from(html: str) -> str:
    match = re.search(r'name="csrf_token"\s+value="([^"]+)"', html)
    assert match, "CSRF token not present in form"
    return match.group(1)


def _login() -> None:
    login_page = client.get("/login")
    token = _csrf_from(login_page.text)
    response = client.post(
        "/login",
        data={"username": "test-admin", "password": "test-password-do-not-use", "csrf_token": token},
    )
    assert response.status_code == 200
    assert "Обзор" in response.text


def test_health_and_worker_auth():
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json()["status"] == "ok"

    bad = client.post("/api/worker/poll", json={"worker_id": "test", "hostname": "ci"})
    assert bad.status_code == 401

    good = client.post(
        "/api/worker/poll",
        headers={"Authorization": "Bearer test-worker-token-that-is-long-enough-00000000"},
        json={"worker_id": "ci-worker", "hostname": "ci", "version": "test"},
    )
    assert good.status_code == 200
    assert good.json()["job"] is None


def test_login_job_queue_creative_sync_and_html_view():
    _login()

    new_page = client.get("/new-run")
    csrf = _csrf_from(new_page.text)
    created = client.post(
        "/new-run",
        data={"city": "Екатеринбург", "key_text": "увеличение пенсии", "csrf_token": csrf},
    )
    assert created.status_code == 200
    assert "увеличение пенсии" in created.text

    headers = {"Authorization": "Bearer test-worker-token-that-is-long-enough-00000000"}
    claimed = client.post(
        "/api/worker/poll",
        headers=headers,
        json={"worker_id": "ci-worker", "hostname": "ci", "version": "test"},
    )
    job = claimed.json()["job"]
    assert job["type"] == "run_parser"

    uploaded = client.post(
        f"/api/worker/jobs/{job['id']}/creatives",
        headers=headers,
        json={"creatives": [{
            "creative_hash": "sample-hash-ci",
            "platform": "example.test",
            "domain": "advertiser.test",
            "title": "Test debt ad",
            "description": "CI fixture",
            "image_url": "",
            "destination_url": "https://advertiser.test",
            "source_page_url": "https://example.test/article",
            "detection_method": "test-adapter",
            "recognition_status": "recognized",
            "captured_at": "2026-10-12T12:00:00+00:00",
            "html": "<!doctype html><html><body><h1>Test debt ad</h1></body></html>",
        }]},
    )
    assert uploaded.status_code == 200
    assert uploaded.json()["accepted"] == 1

    site_sync = client.post(
        f"/api/worker/jobs/{job['id']}/sites",
        headers=headers,
        json={"sites": [{
            "platform": "example.test",
            "url": "https://example.test",
            "status": "ok",
            "duration_seconds": 3.2,
            "scrolls_done": 2,
            "ad_signal_detected": True,
            "unique_creatives": 1,
            "partial_creatives": 0,
            "detection_methods": "test-adapter",
            "error_text": "",
        }]},
    )
    assert site_sync.status_code == 200
    assert site_sync.json()["sites_received"] == 1

    finished = client.post(
        f"/api/worker/jobs/{job['id']}/finish",
        headers=headers,
        json={"status": "succeeded", "exit_code": 0, "result": {"duration_seconds": 1}},
    )
    assert finished.status_code == 200

    gallery = client.get("/creatives")
    assert gallery.status_code == 200
    assert "Test debt ad" in gallery.text

    detail = client.get("/creatives/1")
    assert detail.status_code == 200
    assert "Test debt ad" in detail.text
    assert "/api/creatives/1/html" in detail.text

    run_detail = client.get(f"/runs/{job['id']}")
    assert run_detail.status_code == 200
    assert "example.test" in run_detail.text
    assert "Результаты сканирования площадок" in run_detail.text
