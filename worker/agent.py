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

    def sync_latest_mysql_run(self, job_id: int, city: str, key_text: str) -> dict[str, Any]:
        """Copies tracked site summaries, creative metadata and original HTML from WSL MySQL."""
        if os.getenv("PARSYA_SYNC_MYSQL", "1").strip().lower() not in {"1", "true", "yes"}:
            return {}
        try:
            import mysql.connector
        except ImportError:
            self.post_log(job_id, "\n[worker] mysql-connector-python не установлен: синхронизация пропущена.\n")
            return {}

        password = os.getenv("PARSYA_DB_PASSWORD", "")
        if not password:
            self.post_log(job_id, "\n[worker] PARSYA_DB_PASSWORD не задан; синхронизация пропущена.\n")
            return {}
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
        sent_creatives = 0
        sent_sites = 0
        summary: dict[str, Any] = {}
        try:
            connection = mysql.connector.connect(**cfg)
            cursor = connection.cursor(dictionary=True)
            cursor.execute(
                "SELECT id,status,platforms_total,platforms_finished,creative_occurrences,unique_creatives,error_summary,started_at,finished_at "
                "FROM parser_runs WHERE city=%s AND key_text=%s ORDER BY id DESC LIMIT 1",
                (city, key_text),
            )
            run = cursor.fetchone()
            if not run:
                self.post_log(job_id, "\n[worker] В MySQL не найден tracked-запуск для этих параметров; синхронизация пропущена.\n")
                return {}
            run_id = int(run["id"])
            summary = {
                "local_run_id": run_id,
                "parser_status": str(run.get("status") or ""),
                "platforms_total": int(run.get("platforms_total") or 0),
                "platforms_finished": int(run.get("platforms_finished") or 0),
                "creative_occurrences": int(run.get("creative_occurrences") or 0),
                "unique_creatives": int(run.get("unique_creatives") or 0),
                "parser_error_summary": str(run.get("error_summary") or "")[:5000],
                "local_run_started_at": str(run.get("started_at") or ""),
                "local_run_finished_at": str(run.get("finished_at") or ""),
            }

            # Copy per-site statuses so the remote run page can show timeouts and parser errors.
            cursor.execute(
                "SELECT platform,url,status,duration_seconds,scrolls_done,ad_signal_detected,unique_creatives,"
                "partial_creatives,detection_methods,dom_snapshot_path,error_text "
                "FROM parser_site_results WHERE run_id=%s ORDER BY id",
                (run_id,),
            )
            site_batch: list[dict[str, Any]] = []

            def send_site_batch(items: list[dict[str, Any]]) -> int:
                if not items:
                    return 0
                result = self.request("POST", f"/api/worker/jobs/{job_id}/sites", json={"sites": items}).json()
                return int(result.get("sites_received", 0))

            for row in cursor:
                duration = row.get("duration_seconds")
                site_batch.append({
                    "platform": str(row.get("platform") or "unknown"),
                    "url": str(row.get("url") or ""),
                    "status": str(row.get("status") or "unknown"),
                    "duration_seconds": float(duration) if duration is not None else None,
                    "scrolls_done": int(row.get("scrolls_done") or 0),
                    "ad_signal_detected": bool(row.get("ad_signal_detected")),
                    "unique_creatives": int(row.get("unique_creatives") or 0),
                    "partial_creatives": int(row.get("partial_creatives") or 0),
                    "detection_methods": str(row.get("detection_methods") or "")[:12000],
                    "dom_snapshot_path": str(row.get("dom_snapshot_path") or "")[:8000],
                    "error_text": str(row.get("error_text") or "")[:30000],
                })
                if len(site_batch) >= 100:
                    sent_sites += send_site_batch(site_batch)
                    site_batch.clear()
            if site_batch:
                sent_sites += send_site_batch(site_batch)
            cursor.close()

            # Now export all detected ad occurrences and the compressed source HTML.
            cursor = connection.cursor(dictionary=True)
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

            def send_creative_batch(items: list[dict[str, Any]]) -> int:
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
                # Metadata remains useful even when a single source fragment is too large for the API.
                if len(html_text) > MAX_HTML_CHARS:
                    html_text = ""
                captured = row.get("observed_at")
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
                    "captured_at": captured.isoformat() if hasattr(captured, "isoformat") else str(captured or ""),
                    "html": html_text,
                })
                if len(batch) >= 5:
                    sent_creatives += send_creative_batch(batch)
                    batch.clear()
            if batch:
                sent_creatives += send_creative_batch(batch)
            cursor.close()

            summary["site_results_synced"] = sent_sites
            summary["creative_occurrences_synced"] = sent_creatives
            self.post_log(
                job_id,
                f"\n[worker] MySQL run_id={run_id}: площадок отправлено={sent_sites}, обнаружений отправлено={sent_creatives}.\n",
            )
        except Exception as exc:
            self.post_log(job_id, f"\n[worker] Ошибка синхронизации MySQL: {type(exc).__name__}: {exc}\n")
        finally:
            if connection is not None:
                try:
                    connection.close()
                except Exception:
                    pass
        return summary

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
            buffered_lines = 0
            for line in proc.stdout:
                print(line, end="", flush=True)
                log_buffer += line
                buffered_lines += 1
                # Batch stdout to avoid one network request per line slowing down Selenium.
                if len(log_buffer) >= 8000 or buffered_lines >= 25:
                    self.post_log(job_id, log_buffer)
                    log_buffer = ""
                    buffered_lines = 0
            exit_code = proc.wait()
            if log_buffer:
                self.post_log(job_id, log_buffer)
            sync_summary = self.sync_latest_mysql_run(job_id, city, key_text) if exit_code == 0 else {}
            result = {"duration_seconds": round(time.time() - started_at, 2), **sync_summary}
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
