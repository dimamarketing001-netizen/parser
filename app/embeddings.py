"""Gemini text embeddings for semantic intent matching (2026-10-23).

Why this exists: job-based intent scoping (jobs.key_text) answers "which search
captured this creative", not "is this ad actually about that topic" — RSYA targets
audiences, not page content, so a pension-search run can capture a banquet-hall ad with
zero semantic connection to pensions. This module computes embeddings for search queries
and creative text so relevance can be scored by real content similarity instead.

Network note: reaching generativelanguage.googleapis.com from this WSL machine requires
the wg-gemini WireGuard interface (see parsya_production_architecture memory / wg-quick@wg-gemini
systemd unit) — scoped only to Google's published IPv4 range (172.217.0.0/16), not a
full-tunnel VPN, so it never touches SSH/other WSL traffic.
"""
from __future__ import annotations

import os
import struct
import time
from typing import Any

try:
    from google import genai
except Exception:  # pragma: no cover - optional dependency
    genai = None  # type: ignore[assignment]

# Only [0] is actually used (compute_embedding_single always embeds with EMBEDDING_MODELS[0]).
# Multi-model fallback was tried and dropped (2026-10-23): gemini-embedding-2/-2-preview
# silently return just 1 vector for an N-text batch instead of raising, AND mixing
# embeddings from different models in one cosine-similarity comparison would be
# meaningless anyway (different models encode into different vector spaces). The other
# two stay listed for reference, not as live fallbacks.
EMBEDDING_MODELS = [
    "models/gemini-embedding-001",
    "models/gemini-embedding-2",
    "models/gemini-embedding-2-preview",
]
EMBED_DIM = 3072
def _get_client() -> Any:
    # Deliberately NOT cached as a module-level singleton (2026-10-23): observed, reproducibly,
    # that reusing one genai.Client across multiple embed_content calls within the same
    # long-lived process silently degrades to returning only 1 of N embeddings on the second+
    # call (no exception) - a fresh client every time is slightly wasteful but has been 100%
    # reliable in testing (100/100 every call), while the cached-singleton version was not.
    if genai is None:
        raise RuntimeError("google-genai package not installed")
    api_key = os.getenv("GEMINI_API_KEY") or os.getenv("GOOGLE_API_KEY")
    if not api_key:
        raise RuntimeError("GEMINI_API_KEY/GOOGLE_API_KEY not set")
    return genai.Client(api_key=api_key)




def parse_retry_delay_seconds(exc: Exception) -> float | None:
    """Google's 429 response includes a structured RetryInfo.retryDelay (e.g. "38s",
    "2283s") - exact, not a guess. Returns None if the exception has no such field (a
    non-quota error, or a quota error in an unexpected shape)."""
    details = getattr(exc, "details", None)
    if not isinstance(details, dict):
        return None
    for item in details.get("error", {}).get("details", []):
        if str(item.get("@type", "")).endswith("RetryInfo"):
            raw = str(item.get("retryDelay", ""))
            if raw.endswith("s"):
                try:
                    return float(raw[:-1])
                except ValueError:
                    return None
    return None


def compute_embedding_single(text: str, max_wait_seconds: float = 3600) -> tuple[list[float], str] | None:
    """One embedding, one Gemini request - the production ingestion path (2026-10-23,
    Dima's design): no batching, wait out 429s for exactly as long as Google says
    (parse_retry_delay_seconds) and retry, up to max_wait_seconds total, instead of
    giving up. Deliberately uses ONLY the primary model (EMBEDDING_MODELS[0]), not the
    batch function's multi-model fallback - mixing embeddings from different models in
    the same cosine-similarity comparison would be meaningless (different models encode
    into different vector spaces), so every embedding in creative_embeddings/
    intent_embeddings must come from the one consistent model."""
    text = (text or "").strip()
    if not text:
        return None
    client = _get_client()
    model = EMBEDDING_MODELS[0]
    waited = 0.0
    while True:
        try:
            resp = client.models.embed_content(model=model, contents=[text])
            vectors = [list(e.values) for e in resp.embeddings]
            if len(vectors) == 1:
                return vectors[0], model
            print(f"[embeddings] {model} returned {len(vectors)} vectors for 1 text, giving up on this item", flush=True)
            return None
        except Exception as exc:  # noqa: BLE001 - deliberate: retry until success or timeout
            delay = parse_retry_delay_seconds(exc)
            if delay is None:
                print(f"[embeddings] single-item call failed (non-quota): {exc}", flush=True)
                return None
            sleep_for = min(delay + 1, max_wait_seconds - waited)
            if sleep_for <= 0:
                print(f"[embeddings] gave up after waiting {waited:.0f}s for quota", flush=True)
                return None
            print(f"[embeddings] rate-limited, waiting {sleep_for:.0f}s before retrying", flush=True)
            time.sleep(sleep_for)
            waited += sleep_for


def pack_embedding(vector: list[float]) -> bytes:
    return struct.pack(f"<{len(vector)}f", *vector)


def unpack_embedding(blob: bytes) -> list[float]:
    n = len(blob) // 4
    return list(struct.unpack(f"<{n}f", blob))


def cosine_similarity(a: list[float], b: list[float]) -> float:
    dot = sum(x * y for x, y in zip(a, b))
    na = sum(x * x for x in a) ** 0.5
    nb = sum(x * x for x in b) ** 0.5
    if na == 0 or nb == 0:
        return 0.0
    return dot / (na * nb)


def creative_embedding_text(title: str | None, description: str | None) -> str:
    """What gets embedded for a creative — title+description, the two fields that
    actually carry the ad's own topic (not domain/platform, which are about the
    advertiser/placement, not the ad's subject)."""
    parts = [p.strip() for p in (title or "", description or "") if p and p.strip()]
    return " — ".join(parts)
