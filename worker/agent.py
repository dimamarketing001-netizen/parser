#!/usr/bin/env python3
"""Outbound-only ParsYa worker: polls the control plane and runs an allowlisted parser."""
from __future__ import annotations

import gzip
import itertools
import json
import os
import socket
import subprocess
import sys
import time
from pathlib import Path
from datetime import datetime
from typing import Any


def _chunked(items, size):
    it = iter(items)
    while True:
        chunk = list(itertools.islice(it, size))
        if not chunk:
            return
        yield chunk


def _normalize_domain_for_compare(domain: Any) -> str:
    """Same canonicalization as parsya_advertiser_enrichment.normalize_domain_for_compare
    (2026-10-22) — kept as its own copy since the worker is a separate repo/process and
    doesn't import across repos for a single small helper. Punycode/Unicode IDN forms of
    the same domain, a bare `www.` prefix, and case differences must not count as a
    mismatch for landing_domain_mismatch."""
    domain = str(domain or "").strip().lower()
    if not domain:
        return ""
    if domain.startswith("www."):
        domain = domain[4:]
    try:
        import idna
        domain = idna.encode(domain, uts46=True).decode("ascii")
    except Exception:
        pass
    return domain

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

    def sync_latest_mysql_run(self, job_id: int, city: str, key_text: str, run_started_at: str) -> dict[str, Any]:
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
                "FROM parser_runs WHERE city=%s AND key_text=%s AND started_at >= %s ORDER BY id DESC LIMIT 1",
                (city, key_text, run_started_at),
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
            synced_creative_hashes: set[str] = set()

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
                creative_hash = row.get("creative_hash") or ""
                if creative_hash:
                    synced_creative_hashes.add(creative_hash)
                batch.append({
                    "creative_hash": creative_hash,
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

            sent_links = self._sync_advertiser_links(connection, job_id, synced_creative_hashes)
            sent_extras = self._sync_creative_extras(connection, job_id, synced_creative_hashes)
            sent_secondary = self._sync_secondary_links(connection, job_id, synced_creative_hashes)
            sent_utm = self._sync_creative_utm(connection, job_id, synced_creative_hashes)
            sent_identifiers = self._sync_creative_identifiers(connection, job_id, synced_creative_hashes)
            sent_coverage = self._sync_coverage_report(connection, job_id)
            sent_history = self._sync_destination_history(connection, job_id, synced_creative_hashes)
            sent_mismatch = self._sync_landing_mismatch(connection, job_id, synced_creative_hashes)

            summary["site_results_synced"] = sent_sites
            summary["creative_occurrences_synced"] = sent_creatives
            summary["advertiser_links_synced"] = sent_links
            summary["creative_extras_synced"] = sent_extras
            summary["secondary_links_synced"] = sent_secondary
            summary["utm_synced"] = sent_utm
            summary["identifiers_synced"] = sent_identifiers
            summary["coverage_report_synced"] = sent_coverage
            summary["destination_history_synced"] = sent_history
            summary["landing_mismatch_synced"] = sent_mismatch
            self.post_log(
                job_id,
                f"\n[worker] MySQL run_id={run_id}: площадок отправлено={sent_sites}, обнаружений отправлено={sent_creatives}, "
                f"связей рекламодателя отправлено={sent_links}, доп.сигналов отправлено={sent_extras}, "
                f"доп.ссылок отправлено={sent_secondary}, UTM отправлено={sent_utm}, "
                f"идентификаторов отправлено={sent_identifiers}, строк coverage-report отправлено={sent_coverage}, "
                f"истории кампаний отправлено={sent_history}, несовпадений домена отправлено={sent_mismatch}.\n",
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

    def _sync_advertiser_links(self, connection: Any, job_id: int, creative_hashes: set) -> int:
        """Copies ИНН/advertiser identity for creatives already uploaded in this run, from
        ParsYa's advertiser-enrichment tables (parsya_advertiser_*). Scoped to this run's
        creative hashes only, so a run never re-sends the whole history of every past link.

        campaign_id/banner_id/client_id candidates used to be synced HERE, nested inside this
        same query, gated on `if links_by_hash:` — meaning they were silently dropped for any
        creative without a resolved advertiser link (most creatives, historically). Moved to
        its own independent pass, `_sync_creative_identifiers`, so identifier sync no longer
        depends on disclosure status."""
        if not creative_hashes:
            return 0
        hashes = sorted(creative_hashes)
        links_by_hash: dict[str, dict[str, Any]] = {}
        try:
            cursor = connection.cursor(dictionary=True)
            try:
                for chunk in _chunked(hashes, 200):
                    placeholders = ",".join(["%s"] * len(chunk))
                    # LEFT JOINs pull contact info (address/phone/site) from whichever of this
                    # entity's resolved profiles has it (an entity can have >1 profile across
                    # providers/tokens) — MAX() picks any one non-null value per entity, same
                    # idea as the existing best-confidence pick for the link itself just below.
                    cursor.execute(
                        f"SELECT l.source_creative_hash, l.content_fingerprint, l.association_method, l.confidence, "
                        f"e.identity_key, e.tax_id, e.canonical_name, "
                        f"MAX(p.address) AS address, MAX(p.phone) AS phone, "
                        f"MAX(p.advertised_site_url) AS advertised_site_url "
                        f"FROM parsya_advertiser_creative_links l "
                        f"JOIN parsya_advertiser_entities e ON e.entity_id = l.entity_id "
                        f"LEFT JOIN parsya_advertiser_profile_entities pe ON pe.entity_id = e.entity_id "
                        f"LEFT JOIN parsya_advertiser_profiles p ON p.profile_id = pe.profile_id "
                        f"WHERE l.source_creative_hash IN ({placeholders}) "
                        f"GROUP BY l.source_creative_hash, l.content_fingerprint, l.association_method, "
                        f"l.confidence, e.identity_key, e.tax_id, e.canonical_name",
                        chunk,
                    )
                    for row in cursor.fetchall():
                        h = row.get("source_creative_hash") or ""
                        if not h or not row.get("identity_key"):
                            continue
                        prev = links_by_hash.get(h)
                        if prev is None or float(row.get("confidence") or 0) > float(prev.get("confidence") or 0):
                            links_by_hash[h] = row
            finally:
                cursor.close()
        except Exception as exc:
            self.post_log(job_id, f"\n[worker] Ошибка чтения advertiser-links из MySQL: {type(exc).__name__}: {exc}\n")
            return 0

        link_items: list[dict[str, Any]] = []
        for h, row in links_by_hash.items():
            link_items.append({
                "creative_hash": h,
                "content_fingerprint": str(row.get("content_fingerprint") or ""),
                "advertiser_identity_key": str(row.get("identity_key") or ""),
                "advertiser_inn": str(row.get("tax_id") or ""),
                "advertiser_name": str(row.get("canonical_name") or ""),
                "association_method": str(row.get("association_method") or "unknown")[:80],
                "confidence": float(row["confidence"]) if row.get("confidence") is not None else None,
                "address": str(row.get("address") or ""),
                "phone": str(row.get("phone") or ""),
                "website_url": str(row.get("advertised_site_url") or ""),
            })

        sent = 0
        for chunk in _chunked(link_items, 50):
            try:
                result = self.request(
                    "POST", f"/api/worker/jobs/{job_id}/advertiser-links", json={"links": chunk}
                ).json()
                sent += int(result.get("accepted", 0))
            except Exception as exc:
                self.post_log(job_id, f"\n[worker] Ошибка отправки advertiser-links: {type(exc).__name__}: {exc}\n")
        return sent

    def _sync_creative_extras(self, connection: Any, job_id: int, creative_hashes: set) -> int:
        """Copies media format (video src/poster/duration), CTA button text, price/discount
        and app-store ids for creatives already uploaded in this run, from ParsYa's own
        enrichment tables (parsya_creative_media_classification, parsya_creative_attributes)."""
        if not creative_hashes:
            return 0
        hashes = sorted(creative_hashes)
        media_by_hash: dict[str, dict[str, Any]] = {}
        attrs_by_hash: dict[str, dict[str, Any]] = {}
        try:
            cursor = connection.cursor(dictionary=True)
            try:
                for chunk in _chunked(hashes, 200):
                    placeholders = ",".join(["%s"] * len(chunk))
                    cursor.execute(
                        f"SELECT source_creative_hash, creative_type, video_src, video_poster, video_duration "
                        f"FROM parsya_creative_media_classification WHERE source_creative_hash IN ({placeholders})",
                        chunk,
                    )
                    for row in cursor.fetchall():
                        h = row.get("source_creative_hash") or ""
                        if h:
                            media_by_hash[h] = row
            finally:
                cursor.close()

            cursor = connection.cursor(dictionary=True)
            try:
                for chunk in _chunked(hashes, 200):
                    placeholders = ",".join(["%s"] * len(chunk))
                    cursor.execute(
                        f"SELECT source_creative_hash, cta_text, price_text, price_rub, discount_percent, "
                        f"erid, app_package_id, app_store_id, carousel_id, carousel_slide_index "
                        f"FROM parsya_creative_attributes WHERE source_creative_hash IN ({placeholders})",
                        chunk,
                    )
                    for row in cursor.fetchall():
                        h = row.get("source_creative_hash") or ""
                        if h:
                            attrs_by_hash[h] = row
            finally:
                cursor.close()
        except Exception as exc:
            self.post_log(job_id, f"\n[worker] Ошибка чтения creative-extras из MySQL: {type(exc).__name__}: {exc}\n")
            return 0

        extras_items: list[dict[str, Any]] = []
        for h in hashes:
            media = media_by_hash.get(h, {})
            attrs = attrs_by_hash.get(h, {})
            if not media and not attrs:
                continue
            extras_items.append({
                "creative_hash": h,
                "creative_type": str(media.get("creative_type") or ""),
                "video_src": str(media.get("video_src") or ""),
                "video_poster": str(media.get("video_poster") or ""),
                "video_duration": str(media.get("video_duration") or ""),
                "cta_text": str(attrs.get("cta_text") or ""),
                "price_text": str(attrs.get("price_text") or ""),
                "price_rub": float(attrs["price_rub"]) if attrs.get("price_rub") is not None else None,
                "discount_percent": float(attrs["discount_percent"]) if attrs.get("discount_percent") is not None else None,
                "erid": str(attrs.get("erid") or ""),
                "app_package_id": str(attrs.get("app_package_id") or ""),
                "app_store_id": str(attrs.get("app_store_id") or ""),
                "carousel_id": str(attrs.get("carousel_id") or ""),
                "carousel_slide_index": attrs.get("carousel_slide_index"),
            })

        sent = 0
        for chunk in _chunked(extras_items, 100):
            try:
                result = self.request(
                    "POST", f"/api/worker/jobs/{job_id}/extras", json={"extras": chunk}
                ).json()
                sent += int(result.get("accepted", 0))
            except Exception as exc:
                self.post_log(job_id, f"\n[worker] Ошибка отправки creative-extras: {type(exc).__name__}: {exc}\n")
        return sent

    def _sync_secondary_links(self, connection: Any, job_id: int, creative_hashes: set) -> int:
        """Copies the OTHER clickable links/buttons of each ad (sitelinks, a second CTA) —
        each with its own UTM — from parsya_creative_secondary_links, for creatives already
        uploaded in this run."""
        if not creative_hashes:
            return 0
        hashes = sorted(creative_hashes)
        link_items: list[dict[str, Any]] = []
        try:
            cursor = connection.cursor(dictionary=True)
            try:
                for chunk in _chunked(hashes, 200):
                    placeholders = ",".join(["%s"] * len(chunk))
                    cursor.execute(
                        f"SELECT source_creative_hash, link_text, tracking_url, final_url, final_url_domain, "
                        f"utm_source, utm_medium, utm_campaign, utm_content, utm_term, resolve_status "
                        f"FROM parsya_creative_secondary_links WHERE source_creative_hash IN ({placeholders})",
                        chunk,
                    )
                    for row in cursor.fetchall():
                        link_items.append({
                            "creative_hash": row.get("source_creative_hash") or "",
                            "link_text": str(row.get("link_text") or ""),
                            "tracking_url": str(row.get("tracking_url") or ""),
                            "final_url": str(row.get("final_url") or ""),
                            "final_url_domain": str(row.get("final_url_domain") or ""),
                            "utm_source": str(row.get("utm_source") or ""),
                            "utm_medium": str(row.get("utm_medium") or ""),
                            "utm_campaign": str(row.get("utm_campaign") or ""),
                            "utm_content": str(row.get("utm_content") or ""),
                            "utm_term": str(row.get("utm_term") or ""),
                            "resolve_status": str(row.get("resolve_status") or ""),
                        })
            finally:
                cursor.close()
        except Exception as exc:
            self.post_log(job_id, f"\n[worker] Ошибка чтения secondary-links из MySQL: {type(exc).__name__}: {exc}\n")
            return 0

        sent = 0
        for chunk in _chunked(link_items, 100):
            try:
                result = self.request(
                    "POST", f"/api/worker/jobs/{job_id}/secondary-links", json={"links": chunk}
                ).json()
                sent += int(result.get("accepted", 0))
            except Exception as exc:
                self.post_log(job_id, f"\n[worker] Ошибка отправки secondary-links: {type(exc).__name__}: {exc}\n")
        return sent

    def _sync_creative_utm(self, connection: Any, job_id: int, creative_hashes: set) -> int:
        """Copies UTM of each creative's OWN (primary) destination_url from
        parsya_creative_destination_urls — one row per creative, the LATEST resolve wins
        (a creative can be re-resolved across runs as its landing page/campaign changes).
        Independent of advertiser disclosure, unlike the old identifiers-in-advertiser-links
        path: UTM exists whether or not AdTune ever revealed a name/ИНН for this creative."""
        if not creative_hashes:
            return 0
        hashes = sorted(creative_hashes)
        utm_by_hash: dict[str, dict[str, Any]] = {}
        try:
            cursor = connection.cursor(dictionary=True)
            try:
                for chunk in _chunked(hashes, 200):
                    placeholders = ",".join(["%s"] * len(chunk))
                    cursor.execute(
                        f"SELECT d1.source_creative_hash, d1.final_url, d1.final_url_domain, "
                        f"d1.utm_source, d1.utm_medium, d1.utm_campaign, d1.utm_content, d1.utm_term, "
                        f"d1.resolve_status, d1.resolved_at "
                        f"FROM parsya_creative_destination_urls d1 "
                        f"INNER JOIN ("
                        f"  SELECT source_creative_hash, MAX(resolved_at) AS max_resolved "
                        f"  FROM parsya_creative_destination_urls "
                        f"  WHERE source_creative_hash IN ({placeholders}) "
                        f"  GROUP BY source_creative_hash"
                        f") d2 ON d1.source_creative_hash = d2.source_creative_hash "
                        f"AND d1.resolved_at = d2.max_resolved",
                        chunk,
                    )
                    for row in cursor.fetchall():
                        h = row.get("source_creative_hash") or ""
                        if h:
                            utm_by_hash[h] = row
            finally:
                cursor.close()
        except Exception as exc:
            self.post_log(job_id, f"\n[worker] Ошибка чтения UTM из MySQL: {type(exc).__name__}: {exc}\n")
            return 0

        utm_items: list[dict[str, Any]] = []
        for h, row in utm_by_hash.items():
            resolved_at = row.get("resolved_at")
            utm_items.append({
                "creative_hash": h,
                "final_url": str(row.get("final_url") or ""),
                "final_url_domain": str(row.get("final_url_domain") or ""),
                "utm_source": str(row.get("utm_source") or ""),
                "utm_medium": str(row.get("utm_medium") or ""),
                "utm_campaign": str(row.get("utm_campaign") or ""),
                "utm_content": str(row.get("utm_content") or ""),
                "utm_term": str(row.get("utm_term") or ""),
                "resolve_status": str(row.get("resolve_status") or ""),
                "resolved_at": resolved_at.isoformat() if hasattr(resolved_at, "isoformat") else str(resolved_at or ""),
            })

        sent = 0
        for chunk in _chunked(utm_items, 100):
            try:
                result = self.request(
                    "POST", f"/api/worker/jobs/{job_id}/utm", json={"items": chunk}
                ).json()
                sent += int(result.get("accepted", 0))
            except Exception as exc:
                self.post_log(job_id, f"\n[worker] Ошибка отправки UTM: {type(exc).__name__}: {exc}\n")
        return sent

    def _sync_destination_history(self, connection: Any, job_id: int, creative_hashes: set) -> int:
        """Copies each creative's landing-page/UTM CHANGE LOG from
        parsya_creative_destination_history — unlike _sync_creative_utm (current state,
        one row per creative, overwritten in place), this is append-only: every distinct
        change (already deduped upstream by change_signature) gets its own row, so the
        detail view can show "how this campaign evolved over time" (Dima, 2026-10-19:
        "да, важно видеть, как менялась кампания")."""
        if not creative_hashes:
            return 0
        hashes = sorted(creative_hashes)
        history_items: list[dict[str, Any]] = []
        try:
            cursor = connection.cursor(dictionary=True)
            try:
                for chunk in _chunked(hashes, 200):
                    placeholders = ",".join(["%s"] * len(chunk))
                    cursor.execute(
                        f"SELECT source_creative_hash, change_signature, tracking_url, final_url, "
                        f"final_url_domain, utm_source, utm_medium, utm_campaign, utm_content, utm_term, "
                        f"first_seen_at, last_seen_at "
                        f"FROM parsya_creative_destination_history "
                        f"WHERE source_creative_hash IN ({placeholders})",
                        chunk,
                    )
                    for row in cursor.fetchall():
                        h = row.get("source_creative_hash") or ""
                        if not h:
                            continue
                        first_seen = row.get("first_seen_at")
                        last_seen = row.get("last_seen_at")
                        history_items.append({
                            "creative_hash": h,
                            "change_signature": str(row.get("change_signature") or ""),
                            "tracking_url": str(row.get("tracking_url") or ""),
                            "final_url": str(row.get("final_url") or ""),
                            "final_url_domain": str(row.get("final_url_domain") or ""),
                            "utm_source": str(row.get("utm_source") or ""),
                            "utm_medium": str(row.get("utm_medium") or ""),
                            "utm_campaign": str(row.get("utm_campaign") or ""),
                            "utm_content": str(row.get("utm_content") or ""),
                            "utm_term": str(row.get("utm_term") or ""),
                            "first_seen_at": first_seen.isoformat() if hasattr(first_seen, "isoformat") else str(first_seen or ""),
                            "last_seen_at": last_seen.isoformat() if hasattr(last_seen, "isoformat") else str(last_seen or ""),
                        })
            finally:
                cursor.close()
        except Exception as exc:
            self.post_log(job_id, f"\n[worker] Ошибка чтения destination-history из MySQL: {type(exc).__name__}: {exc}\n")
            return 0

        sent = 0
        for chunk in _chunked(history_items, 100):
            try:
                result = self.request(
                    "POST", f"/api/worker/jobs/{job_id}/destination-history", json={"items": chunk}
                ).json()
                sent += int(result.get("accepted", 0))
            except Exception as exc:
                self.post_log(job_id, f"\n[worker] Ошибка отправки destination-history: {type(exc).__name__}: {exc}\n")
        return sent

    def _sync_landing_mismatch(self, connection: Any, job_id: int, creative_hashes: set) -> int:
        """Reads the advertiser's declared site domain and the ad's actual landing domain
        from MySQL's parsya_creative_full VIEW, then recomputes landing_domain_mismatch
        HERE (via _normalize_domain_for_compare) instead of trusting the view's own raw
        `<>` comparison. Why: the view's comparison is a byte-level string compare, which
        flags punycode vs. the equivalent Unicode/Cyrillic form of the same IDN domain
        (confirmed real example 2026-10-22: `xn--b1addek2adh0b5a9df.xn--p1ai` vs
        `деньживотных.рф` — same domain) and a bare `www.` prefix as false "mismatches".
        Recomputing in Python here fixes this for every row on every sync, including
        historical rows written before the domain-normalization fix landed in
        parsya_advertiser_enrichment.py — no separate backfill needed. Queued 2026-10-18
        as "always 0 in practice back then" — now showing real mismatches, worth exposing."""
        if not creative_hashes:
            return 0
        hashes = sorted(creative_hashes)
        items: list[dict[str, Any]] = []
        try:
            cursor = connection.cursor(dictionary=True)
            try:
                for chunk in _chunked(hashes, 200):
                    placeholders = ",".join(["%s"] * len(chunk))
                    cursor.execute(
                        f"SELECT creative_hash, advertised_site_domain, final_url_domain "
                        f"FROM parsya_creative_full WHERE creative_hash IN ({placeholders}) "
                        f"AND advertised_site_domain IS NOT NULL AND final_url_domain IS NOT NULL",
                        chunk,
                    )
                    for row in cursor.fetchall():
                        h = row.get("creative_hash") or ""
                        if not h:
                            continue
                        advertised_raw = str(row.get("advertised_site_domain") or "")
                        final_raw = str(row.get("final_url_domain") or "")
                        mismatch = _normalize_domain_for_compare(advertised_raw) != _normalize_domain_for_compare(final_raw)
                        items.append({
                            "creative_hash": h,
                            "advertised_site_domain": advertised_raw,
                            "landing_domain_mismatch": mismatch,
                        })
            finally:
                cursor.close()
        except Exception as exc:
            self.post_log(job_id, f"\n[worker] Ошибка чтения landing-mismatch из MySQL: {type(exc).__name__}: {exc}\n")
            return 0

        sent = 0
        for chunk in _chunked(items, 100):
            try:
                result = self.request(
                    "POST", f"/api/worker/jobs/{job_id}/landing-mismatch", json={"items": chunk}
                ).json()
                sent += int(result.get("accepted", 0))
            except Exception as exc:
                self.post_log(job_id, f"\n[worker] Ошибка отправки landing-mismatch: {type(exc).__name__}: {exc}\n")
        return sent

    def _sync_creative_identifiers(self, connection: Any, job_id: int, creative_hashes: set) -> int:
        """Copies campaign_id/banner_id/client_id/creative_id candidates (markup-derived)
        for ANY creative already uploaded in this run — independent of advertiser disclosure.
        Was previously only synced for creatives that already had a resolved advertiser link
        (nested inside _sync_advertiser_links), which silently dropped these for most
        creatives historically (90%+ before the AdTune click-reveal fixes). Separated into
        its own pass so campaign-ID filtering/analytics works regardless of disclosure status."""
        if not creative_hashes:
            return 0
        hashes = sorted(creative_hashes)
        identifiers_by_hash: dict[str, dict[str, set]] = {}
        try:
            cursor = connection.cursor(dictionary=True)
            try:
                for chunk in _chunked(hashes, 200):
                    placeholders = ",".join(["%s"] * len(chunk))
                    cursor.execute(
                        f"SELECT source_creative_hash, client_ids_json, campaign_ids_json, "
                        f"banner_ids_json, creative_ids_json "
                        f"FROM parsya_ad_creative_identifier_snapshots "
                        f"WHERE source_creative_hash IN ({placeholders})",
                        chunk,
                    )
                    for row in cursor.fetchall():
                        h = row.get("source_creative_hash") or ""
                        if not h:
                            continue
                        bucket = identifiers_by_hash.setdefault(h, {
                            "client_id_candidate": set(), "campaign_id_candidate": set(),
                            "banner_id_candidate": set(), "creative_id_candidate": set(),
                        })
                        for id_type, field in (
                            ("client_id_candidate", "client_ids_json"),
                            ("campaign_id_candidate", "campaign_ids_json"),
                            ("banner_id_candidate", "banner_ids_json"),
                            ("creative_id_candidate", "creative_ids_json"),
                        ):
                            try:
                                values = json.loads(row.get(field) or "[]")
                            except Exception:
                                values = []
                            bucket[id_type].update(str(v) for v in values if v)
            finally:
                cursor.close()
        except Exception as exc:
            self.post_log(job_id, f"\n[worker] Ошибка чтения identifiers из MySQL: {type(exc).__name__}: {exc}\n")
            return 0

        identifier_items: list[dict[str, Any]] = []
        for h, ids in identifiers_by_hash.items():
            identifiers = [
                {"type": id_type, "value": value}
                for id_type, values in ids.items() for value in values
            ][:50]
            if identifiers:
                identifier_items.append({"creative_hash": h, "identifiers": identifiers})

        sent = 0
        for chunk in _chunked(identifier_items, 100):
            try:
                result = self.request(
                    "POST", f"/api/worker/jobs/{job_id}/identifiers", json={"items": chunk}
                ).json()
                sent += int(result.get("accepted", 0))
            except Exception as exc:
                self.post_log(job_id, f"\n[worker] Ошибка отправки identifiers: {type(exc).__name__}: {exc}\n")
        return sent

    def _sync_coverage_report(self, connection: Any, job_id: int) -> int:
        """Copies parsya_advertiser_coverage_report (a MySQL VIEW: % disclosed grouped by
        platform/detection_method/creative_type) into SQLite — a global aggregate, not scoped
        to this job's creatives, so this fully replaces the small table each time rather than
        growing it. Cheap to run on every sync; the view itself does the heavy aggregation."""
        rows_out: list[dict[str, Any]] = []
        try:
            cursor = connection.cursor(dictionary=True)
            try:
                cursor.execute(
                    "SELECT platform, detection_method, creative_type, total_creatives, "
                    "creatives_with_inn, coverage_percent FROM parsya_advertiser_coverage_report"
                )
                for row in cursor.fetchall():
                    rows_out.append({
                        "platform": str(row.get("platform") or ""),
                        "detection_method": str(row.get("detection_method") or ""),
                        "creative_type": str(row.get("creative_type") or ""),
                        "total_creatives": int(row.get("total_creatives") or 0),
                        "disclosed_creatives": int(row.get("creatives_with_inn") or 0),
                        "disclosed_pct": float(row.get("coverage_percent") or 0),
                    })
            finally:
                cursor.close()
        except Exception as exc:
            self.post_log(job_id, f"\n[worker] Ошибка чтения coverage-report из MySQL: {type(exc).__name__}: {exc}\n")
            return 0

        if not rows_out:
            return 0
        try:
            result = self.request(
                "POST", "/api/worker/coverage-report", json={"rows": rows_out}
            ).json()
            return int(result.get("accepted", 0))
        except Exception as exc:
            self.post_log(job_id, f"\n[worker] Ошибка отправки coverage-report: {type(exc).__name__}: {exc}\n")
            return 0

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
        env["PARSYA_JOB_ID"] = str(job_id)
        env["PYTHONUNBUFFERED"] = "1"
        command = [self.python, str(self.script)]
        started_at = time.time()
        run_started_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
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
            last_sync = time.time()
            sync_interval = max(5, int(os.getenv("PARSYA_SYNC_INTERVAL_SECONDS", "20")))
            for line in proc.stdout:
                print(line, end="", flush=True)
                log_buffer += line
                buffered_lines += 1
                # Batch stdout to avoid one network request per line slowing down Selenium.
                if len(log_buffer) >= 8000 or buffered_lines >= 25:
                    self.post_log(job_id, log_buffer)
                    log_buffer = ""
                    buffered_lines = 0
                # Sync whatever has already landed in local MySQL so the control-plane (and
                # a user watching their run from the SaaS frontend) sees creatives arrive
                # progressively, not only once the whole multi-platform crawl finishes.
                now = time.time()
                if now - last_sync >= sync_interval:
                    last_sync = now
                    try:
                        self.sync_latest_mysql_run(job_id, city, key_text, run_started_at)
                    except Exception as sync_exc:
                        self.post_log(job_id, f"\n[worker] Промежуточная синхронизация пропущена: {type(sync_exc).__name__}: {sync_exc}\n")
            exit_code = proc.wait()
            if log_buffer:
                self.post_log(job_id, log_buffer)
            # Sync any partial results too, even if the parser exits with an error.
            sync_summary = self.sync_latest_mysql_run(job_id, city, key_text, run_started_at)
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
