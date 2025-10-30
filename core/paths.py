# site_backend/core/paths.py
from __future__ import annotations
import os
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_UPLOAD_ROOT = PROJECT_ROOT / "data" / "uploads"

# Allow env override but resolve to absolute and ensure exists
UPLOAD_ROOT = Path(os.getenv("UPLOAD_ROOT", str(DEFAULT_UPLOAD_ROOT))).resolve()
UPLOAD_ROOT.mkdir(parents=True, exist_ok=True)
