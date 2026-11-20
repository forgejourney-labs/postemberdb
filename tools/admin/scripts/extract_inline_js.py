#!/usr/bin/env python3
"""Extract the single inline Admin application script for a Node syntax check."""
from pathlib import Path
import re
import sys

source = Path(__file__).resolve().parents[1] / "static" / "index.html"
html = source.read_text(encoding="utf-8")
scripts = re.findall(r"<script>(.*?)</script>", html, flags=re.DOTALL | re.IGNORECASE)
if len(scripts) != 1:
    raise SystemExit(f"expected exactly one inline application script, found {len(scripts)}")
target = Path("/tmp/postemberdb-admin-inline.js")
target.write_text(scripts[0], encoding="utf-8")
print(f"Extracted {len(scripts[0])} JavaScript characters from {source}")
