#!/usr/bin/env python3
"""Idempotently make the existing ParsYa script accept city/query from worker environment."""
from __future__ import annotations
import re
import shutil
import sys
from pathlib import Path

def main() -> int:
    if len(sys.argv) != 2:
        print("Usage: python patch_parser_env.py /absolute/path/to/ParsYa_main_with_tracking.py")
        return 2
    path = Path(sys.argv[1]).expanduser().resolve()
    if not path.is_file():
        print(f"ERROR: script not found: {path}")
        return 2
    text = path.read_text(encoding="utf-8")
    old_city = 'city = "Екатеринбург"'
    new_city = 'city = os.environ.get("PARSYA_CITY", "Екатеринбург")'
    old_query = 'Key_text = "увеличение пенсии" + f" {city}"'
    new_query = 'Key_text = os.environ.get("PARSYA_KEY_TEXT") or ("увеличение пенсии" + f" {city}")'
    changed = False
    if new_city not in text:
        if old_city not in text:
            print("ERROR: expected city assignment not found; file left untouched.")
            return 3
        text = text.replace(old_city, new_city, 1)
        changed = True
    if new_query not in text:
        if old_query not in text:
            print("ERROR: expected Key_text assignment not found; file left untouched.")
            return 3
        text = text.replace(old_query, new_query, 1)
        changed = True
    if changed:
        backup = path.with_suffix(path.suffix + ".before_remote_worker.bak")
        if not backup.exists():
            shutil.copy2(path, backup)
        path.write_text(text, encoding="utf-8")
        print(f"Patched: {path}")
        print(f"Backup:  {backup}")
    else:
        print("Already patched; no changes made.")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
