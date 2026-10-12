#!/usr/bin/env python3
"""Outbound-only ParsYa worker: polls the control plane and runs an allowlisted parser."""
from __future__ import annotations

import gzip
import json
import os
import socket
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

import requests

VERSION = "1.0.0"
POLL_SECONDS = max(2, int(os.getenv("PARSYA_POLL_SECONDS", "5")))
HTTP_TIMEOUT = 25
MAX_HTML_CHARS = 2_000_000


def load_env_file() -> None:
    env_file = Path(os.getenv("PARSYA_ENV_FILE", "/home/dima/ParsYa/.env"))
    if not env_file.is_file():
        return
    for line in env_file.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key, value = key.strip(), value.strip()
        if key and key not in os.environ:
            if len(value) >= 2 and value[0] == value[-1] and value[0] in ("'", '"'):
                value = value[1:-1]
            os.environ[key] = value


def required(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise RuntimeError(f"Required environment variable {name} is missing")
    return value


class Worker:
    def __init__(self) -> None:
        self.base = required("PARSYA_CONTROL_URL").rstrip("/")
        self.token = required("PARSYA_WORKER_TOKEN")
        self.worker_id = os.getenv("PARSYA_WORKER_ID", socket.gethostname())[:120]
        self.home = Path(os.getenv("PARSYA_HOME", "/home/dima/ParsYa")).resolve()
        self.script = Path(os.getenv("PARSYA_MAIN_SCRIPT", self.home / "ParsYa_main_with_tracking.py")).resolve()
        self.python = os.getenv("PARSYA_PYTHON", sys.executable)
        self.session = requests.Session()
        self.session.headers.update({"Authorization": f"Bearer {self.token}", "User-Agent": f"ParsYaWorker/{VERSION}"})

    def request(self, method: str, path: str, **kwargs: Any) -> requests.Response:
        response = self.session.request(method, self.base + path, timeout=HTTP_TIMEOUT, **kwargs)
        response.raise_for_status()
        return response

    def post_log(self, job_id: int, text: str) -> None:
        if not text:
            return
        try:
            self.request("POST", f"/api/worker/jobs/{job_id}/log", json={"text": text[-30000:]})
        except Exception as exc:
            print(f"[worker] Не удалось передать лог: {exc}", flush=True)

    def sync_latest_mysql_run(self, job_id: int, city: str, key_text: str) -> int:
        """Copies this job's tracked creatives and original HTML from the WSL MySQL to the panel."""
        if os.getenv("PARSYA_SYNC_MYSQL", "1").strip().lower() not in {"1", "true", "yes"}:
            return 0
        try:
            import mysql.connector
        except ImportError:
            self.post_log(job_id, "\n[worker] mysql-connector-python не установлен: синхронизация креативов пропущена.\n")
            return 0

        password = os.getenv("PARSYA_DB_PASSWORD", "")
        if not password:
            self.post_log(job_id, "\n[worker] PARSYA_DB_PASSWORD не задан; синхронизация креативов пропущена.\n")
            return 0
        cfg = {
            "host": os.getenv("PARSYA_DB_HOST", "localhost"),
            "port": int(os.getenv("PARSYA_DB_PORT", "3306")),
            "user": os.getenv("PARSYA_DB_USER", "parsya"),
            "password": password,
            "database": os.getenv("PARSYA_DB_NAME", "Data_Science"),
            "charset": "utf8mb4",
            "connection_timeout": 10,
        }
        connection = None
        sent = 0
        try:
            connection = mysql.connector.connect(**cfg)
            cursor = connection.cursor(dictionary=True)
            cursor.execute(
                "SELECT id FROM parser_runs WHERE city=%s AND key_text=%s ORDER BY id DESC LIMIT 1",
                (city, key_text),
            )
            run = cursor.fetchone()
            if not run:
                self.post_log(job_id, "\n[worker] В MySQL не найден tracked-запуск для этих параметров; синхронизация пропущена.\n")
                return 0
            run_id = int(run["id"])
            cursor.execute(
                "SELECT o.platform,o.source_page_url,o.detection_method,o.recognition_status,o.observed_at,"
                "c.creative_hash,c.domain,c.title,c.description,c.image_url,c.destination_url,"
                "h.html_gzip,h.html_sha256 "
                "FROM ad_occurrences o "
                "JOIN ad_creatives c ON c.id=o.creative_id "
                "LEFT JOIN ad_html_snapshots h ON h.id=o.html_snapshot_id "
                "WHERE o.run_id=%s ORDER BY o.id",
                (run_id,),
            )
            batch: list[dict[str, Any]] = []

            def send_batch(items: list[dict[str, Any]]) -> int:
                if not items:
                    return 0
                result = self.request("POST", f"/api/worker/jobs/{job_id}/creatives", json={"creatives": items}).json()
                return int(result.get("accepted", 0))

            for row in cursor:
                html_text = ""
                compressed = row.get("html_gzip")
                if compressed:
                    try:
                        html_text = gzip.decompress(bytes(compressed)).decode("utf-8", errors="replace")
                    except Exception:
                        html_text = ""
                # API limits are deliberate. Metadata is still sent if one HTML payload is oversized.
                if len(html_text) > MAX_HTML_CHARS:
                    html_text = ""
                batch.append({
                    "creative_hash": row.get("creative_hash") or "",
                    "platform": row.get("platform") or "unknown",
                    "domain": row.get("domain") or "",
                    "title": row.get("title") or "",
                    "description": row.get("description") or "",
                    "image_url": row.get("image_url") or "",
                    "destination_url": row.get("destination_url") or "",
                    "source_page_url": row.get("source_page_url") or "",
                    "detection_method": row.get("detection_method") or "unknown",
                    "recognition_status": row.get("recognition_status") or "recognized",
                    "captured_at": row.get("observed_at").isoformat() if hasattr(row.get("observed_at"), "isoformat") else str(row.get("observed_at") or ""),
                    "html": html_text,
                })
                if len(batch) >= 5:
                    sent += send_batch(batch)
                    batch.clear()
            if batch:
                sent += send_batch(batch)
            cursor.close()
            self.post_log(job_id, f"\n[worker] Синхронизация из MySQL завершена: run_id={run_id}, отправлено обнаружений={sent}.\n")
        except Exception as exc:
            self.post_log(job_id, f"\n[worker] Ошибка синхронизации MySQL: {type(exc).__name__}: {exc}\n")
        finally:
            if connection is not None:
                try:
                    connection.close()
                except Exception:
                    pass
        return sent

    def execute_job(self, job: dict[str, Any]) -> None:
        job_id = int(job["id"])
        city = str(job.get("city") or "").strip()
        key_text = str(job.get("key_text") or "").strip()
        self.post_log(job_id, f"[worker] Запускаю job #{job_id}; worker={self.worker_id}; city={city}; query={key_text}\n")
        if not self.script.is_file():
            error = f"Parser script not found: {self.script}"
            self.request("POST", f"/api/worker/jobs/{job_id}/finish", json={"status": "failed", "exit_code": 2, "error": error, "result": {}})
            return

        env = os.environ.copy()
        env["PARSYA_CITY"] = city
        env["PARSYA_KEY_TEXT"] = key_text
        env["PARSYA_SOURCE_MODE"] = "remote-worker"
        env["PYTHONUNBUFFERED"] = "1"
        command = [self.python, str(self.script)]
        started_at = time.time()
        exit_code = 1
        log_buffer = ""
        try:
            proc = subprocess.Popen(
                command,
                cwd=str(self.home),
                env=env,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                encoding="utf-8",
                errors="replace",
                bufsize=1,
            )
            assert proc.stdout is not None
            for line in proc.stdout:
                print(line, end="", flush=True)
                log_buffer += line
                if len(log_buffer) >= 4000 or "\n" in line:
                    self.post_log(job_id, log_buffer)
                    log_buffer = ""
            exit_code = proc.wait()
            if log_buffer:
                self.post_log(job_id, log_buffer)
            synced = self.sync_latest_mysql_run(job_id, city, key_text) if exit_code == 0 else 0
            result = {"duration_seconds": round(time.time() - started_at, 2), "sync_records_accepted": synced}
            error = "" if exit_code == 0 else f"Parser exited with code {exit_code}"
            self.request("POST", f"/api/worker/jobs/{job_id}/finish", json={
                "status": "succeeded" if exit_code == 0 else "failed",
                "exit_code": exit_code,
                "error": error,
                "result": result,
            })
        except Exception as exc:
            error = f"{type(exc).__name__}: {exc}"
            self.post_log(job_id, f"\n[worker] Ошибка исполнения: {error}\n")
            try:
                self.request("POST", f"/api/worker/jobs/{job_id}/finish", json={
                    "status": "failed", "exit_code": exit_code, "error": error,
                    "result": {"duration_seconds": round(time.time() - started_at, 2)},
                })
            except Exception:
                pass

    def run_forever(self) -> None:
        print(f"[worker] Started worker={self.worker_id}; control={self.base}; parser={self.script}", flush=True)
        while True:
            try:
                payload = {
                    "worker_id": self.worker_id,
                    "hostname": socket.gethostname(),
                    "version": VERSION,
                }
                response = self.request("POST", "/api/worker/poll", json=payload).json()
                job = response.get("job")
                if job:
                    self.execute_job(job)
                else:
                    time.sleep(max(2, int(response.get("poll_after_seconds", POLL_SECONDS))))
            except KeyboardInterrupt:
                print("[worker] Stopped by operator.", flush=True)
                return
            except Exception as exc:
                print(f"[worker] API unavailable: {type(exc).__name__}: {exc}; retry in 10s", flush=True)
                time.sleep(10)


def main() -> None:
    load_env_file()
    Worker().run_forever()


if __name__ == "__main__":
    main()
