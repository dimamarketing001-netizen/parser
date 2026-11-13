import os, sys, tempfile
from pathlib import Path
os.environ.setdefault("PARSYA_ADMIN_USER", "test-admin")
os.environ.setdefault("PARSYA_ADMIN_PASSWORD", "test-password-do-not-use")
os.environ.setdefault("PARSYA_SECRET_KEY", "test-secret-key-that-is-long-enough-00000000")
os.environ.setdefault("PARSYA_WORKER_TOKEN", "test-worker-token-that-is-long-enough-00000000")
os.environ.setdefault("PARSYA_COOKIE_SECURE", "0")
os.environ.setdefault("PARSYA_SQLITE_PATH", str(Path(tempfile.gettempdir()) / f"parsya-test-sched-{os.getpid()}.sqlite3"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from contextlib import closing
from datetime import datetime, timedelta, timezone
import app.main as m


def _mk(db, rpd=6, days=7):
    db.execute("INSERT OR IGNORE INTO users(id,email,password_hash,created_at) VALUES(1,'t@t','x','2026-01-01')")
    now = datetime.now(timezone.utc).replace(microsecond=0)
    cur = db.execute(
        "INSERT INTO scan_schedules(owner_user_id,city,key_text,runs_per_day,duration_days,created_at,end_at,next_run_at) VALUES(1,'Москва','тест',?,?,?,?,?)",
        (rpd, days, now.isoformat(), (now + timedelta(days=days)).isoformat(), now.isoformat()))
    db.commit()
    return cur.lastrowid


def test_tick_creates_job_and_advances():
    with closing(m.connect()) as db:
        sid = _mk(db)
    m._sched_tick()
    with closing(m.connect()) as db:
        sc = db.execute("SELECT * FROM scan_schedules WHERE id=?", (sid,)).fetchone()
        jobs = db.execute("SELECT * FROM jobs WHERE schedule_id=?", (sid,)).fetchall()
    assert len(jobs) == 1 and sc["runs_created"] == 1
    gap = datetime.fromisoformat(sc["next_run_at"]) - datetime.now(timezone.utc)
    assert timedelta(hours=3, minutes=50) < gap <= timedelta(hours=4)


def test_busy_slot_is_skipped_and_end_finishes():
    with closing(m.connect()) as db:
        sid = _mk(db, rpd=24, days=7)
        db.execute("INSERT INTO jobs(status,city,key_text,created_at,schedule_id) VALUES('running','Москва','тест',?,?)", (m.now_iso(), sid))
        db.commit()
    m._sched_tick()
    with closing(m.connect()) as db:
        sc = db.execute("SELECT * FROM scan_schedules WHERE id=?", (sid,)).fetchone()
        assert sc["runs_created"] == 0 and sc["runs_skipped"] == 1
        db.execute("UPDATE scan_schedules SET end_at=?, next_run_at=? WHERE id=?",
                   ((datetime.now(timezone.utc) + timedelta(minutes=10)).isoformat(), (datetime.now(timezone.utc) - timedelta(seconds=5)).replace(microsecond=0).isoformat(), sid))
        db.execute("UPDATE jobs SET status='succeeded' WHERE schedule_id=?", (sid,))
        db.commit()
    m._sched_tick()
    with closing(m.connect()) as db:
        sc = db.execute("SELECT * FROM scan_schedules WHERE id=?", (sid,)).fetchone()
    assert sc["status"] == "finished"
