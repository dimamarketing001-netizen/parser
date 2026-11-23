"""Длительность рекламного видео по ссылке: читаем только заголовок файла (WebM/MP4)."""
from __future__ import annotations

import struct
from typing import Optional

import requests

_UA = {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0 Safari/537.36"}


def _fetch(url: str, start: int, length: int, timeout: float = 15) -> bytes:
    r = requests.get(url, headers={**_UA, "Range": f"bytes={start}-{start + length - 1}"}, timeout=timeout, stream=True)
    if r.status_code not in (200, 206):
        return b""
    data = b""
    for chunk in r.iter_content(65536):
        data += chunk
        if len(data) >= length:
            break
    r.close()
    return data[:length]


def _webm_duration(data: bytes) -> Optional[float]:
    """EBML: Segment/Info/TimecodeScale (0x2AD7B1) и Duration (0x4489)."""
    scale = 1_000_000
    i = data.find(b"\x2a\xd7\xb1")
    if i >= 0 and i + 4 < len(data):
        n = data[i + 3] & 0x7F
        if 1 <= n <= 8 and i + 4 + n <= len(data):
            scale = int.from_bytes(data[i + 4:i + 4 + n], "big") or scale
    i = data.find(b"\x44\x89")
    while i >= 0:
        size = data[i + 2] if i + 2 < len(data) else 0
        if size == 0x84 and i + 7 <= len(data):
            val = struct.unpack(">f", data[i + 3:i + 7])[0]
        elif size == 0x88 and i + 11 <= len(data):
            val = struct.unpack(">d", data[i + 3:i + 11])[0]
        else:
            i = data.find(b"\x44\x89", i + 1)
            continue
        seconds = val * scale / 1e9
        if 0.2 < seconds < 3 * 3600:
            return round(seconds, 1)
        i = data.find(b"\x44\x89", i + 1)
    return None


def _mp4_duration(data: bytes) -> Optional[float]:
    i = data.find(b"mvhd")
    if i < 0 or i + 32 > len(data):
        return None
    version = data[i + 4]
    if version == 1:
        if i + 36 > len(data):
            return None
        scale = struct.unpack(">I", data[i + 24:i + 28])[0]
        dur = struct.unpack(">Q", data[i + 28:i + 36])[0]
    else:
        scale = struct.unpack(">I", data[i + 16:i + 20])[0]
        dur = struct.unpack(">I", data[i + 20:i + 24])[0]
    if not scale:
        return None
    seconds = dur / scale
    return round(seconds, 1) if 0.2 < seconds < 3 * 3600 else None


def probe_duration(url: str) -> Optional[float]:
    """Секунды или None. Сеть: 1–2 запроса по ~128–512 КБ."""
    if not url or not url.startswith("http"):
        return None
    try:
        head = _fetch(url, 0, 131072)
        if not head:
            return None
        if head[:4] == b"\x1a\x45\xdf\xa3":
            return _webm_duration(head)
        got = _mp4_duration(head)
        if got:
            return got
        if b"ftyp" in head[:16]:
            r = requests.head(url, headers=_UA, timeout=10, allow_redirects=True)
            total = int(r.headers.get("Content-Length") or 0)
            if total > 131072:
                tail = _fetch(url, max(0, total - 524288), 524288)
                return _mp4_duration(tail)
    except Exception:
        return None
    return None
