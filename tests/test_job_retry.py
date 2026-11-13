import os, sys, tempfile
from pathlib import Path
os.environ.setdefault("PARSYA_ADMIN_USER", "test-admin")
os.environ.setdefault("PARSYA_ADMIN_PASSWORD", "test-password-do-not-use")
os.environ.setdefault("PARSYA_SECRET_KEY", "test-secret-key-that-is-long-enough-00000000")
os.environ.setdefault("PARSYA_WORKER_TOKEN", "test-worker-token-that-is-long-enough-00000000")
os.environ.setdefault("PARSYA_COOKIE_SECURE", "0")
os.environ.setdefault("PARSYA_SQLITE_PATH", str(Path(tempfile.gettempdir()) / f"parsya-test-retry-{os.getpid()}.sqlite3"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from contextlib import closing
import app.main as m

AUTH = "Bearer " + os.environ["PARSYA_WORKER_TOKEN"]


def test_failed_job_is_requeued_until_limit():
    with closing(m.connect()) as db:
        jid = db.execute("INSERT INTO jobs(status,city,key_text,created_at) VALUES('running','Москва','тест','2026-01-01T00:00:00+00:00')").lastrowid
        db.commit()
    for n in range(1, m.JOB_MAX_ATTEMPTS):
        r = m.worker_finish(jid, m.FinishIn(status="failed", exit_code=1, error="dns"), AUTH)
        assert r["attempt"] == n
        with closing(m.connect()) as db:
            j = db.execute("SELECT status,retry_at,attempts FROM jobs WHERE id=?", (jid,)).fetchone()
        assert j["status"] == "queued" and j["attempts"] == n and j["retry_at"]
        with closing(m.connect()) as db:  # пока пауза не вышла, воркер задание не получает
            db.execute("UPDATE jobs SET status='running' WHERE id=?", (jid,)); db.commit()
    r = m.worker_finish(jid, m.FinishIn(status="failed", exit_code=1, error="dns"), AUTH)
    assert "attempt" not in r
    with closing(m.connect()) as db:
        j = db.execute("SELECT status,attempts FROM jobs WHERE id=?", (jid,)).fetchone()
    assert j["status"] == "failed" and j["attempts"] == m.JOB_MAX_ATTEMPTS
