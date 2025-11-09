from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Optional

from fastapi import APIRouter, Depends, Response
from fastapi.responses import RedirectResponse
from neo4j import Session
from urllib.parse import urlparse, urlencode, parse_qsl, urlunparse

from site_backend.core.neo_driver import session_dep

from .service import get_public_profile

router = APIRouter(tags=["account"])

# If you store avatars locally under /uploads/avatars/aa/bb/<sha>.webp, this just works.
# If you serve from S3/CDN, make sure your stored avatar_url is a public https URL; we 302 to it.

def _initials_svg(text: str, size: int = 80) -> bytes:
    import re as _re
    initials = "".join([p[0] for p in _re.split(r"[^\w]+", text) if p][:2]).upper() or "U"
    return f"""<svg xmlns="http://www.w3.org/2000/svg" width="{size}" height="{size}">
  <defs>
    <linearGradient id="g" x1="0" y1="0" x2="1" y2="1">
      <stop offset="0" stop-color="#E3F7DE"/>
      <stop offset="1" stop-color="#FDF0C6"/>
    </linearGradient>
  </defs>
  <rect width="100%" height="100%" rx="{size//2}" fill="url(#g)"/>
  <text x="50%" y="55%" text-anchor="middle" font-family="Inter, system-ui" font-size="{int(size*0.42)}" font-weight="800" fill="#396041">{initials}</text>
</svg>""".encode("utf-8")

def _with_google_size(href: str, size: int) -> str:
    u = urlparse(href)
    if u.hostname and (u.hostname.endswith("googleusercontent.com") or u.hostname.endswith("ggpht.com")):
        qs = dict(parse_qsl(u.query))
        qs.setdefault("sz", str(size))
        u = u._replace(query=urlencode(qs))
        return urlunparse(u)
    return href
# site_backend/api/account/__init__ (or wherever your avatar route lives)
from site_backend.core.paths import UPLOAD_ROOT  # import this

@router.get("/u/{user_id}/avatar/{size}")
def public_user_avatar(user_id: str, size: int = 80, s: Session = Depends(session_dep)):
    size = max(24, min(size, 256))

    prof = get_public_profile(s, user_id)
    display = (prof or {}).get("display_name") or user_id
    avatar_url: Optional[str] = (prof or {}).get("avatar_url")

    headers = {"Cache-Control": "public, max-age=600"}

    if not avatar_url:
        return Response(_initials_svg(display, size), media_type="image/svg+xml",
                        headers={"Cache-Control": "public, max-age=86400"})

    if re.match(r"^https?://", avatar_url, re.I):
        return RedirectResponse(_with_google_size(avatar_url, size), status_code=302, headers=headers)

    # Normalize local path → always starts with /uploads/…
    local_path = avatar_url if avatar_url.startswith("/") else f"/{avatar_url}"
    p = Path(local_path.lstrip("/"))  # e.g. uploads/avatars/59/76/<sha>.webp

    # Map URL path → filesystem path rooted at UPLOAD_ROOT
    def to_fs(path_under_uploads: Path) -> Path:
        parts = path_under_uploads.parts
        if parts and parts[0] == "uploads":
            # /uploads/<...> → UPLOAD_ROOT/<...>
            return UPLOAD_ROOT / Path(*parts[1:])
        # fallback (shouldn’t really happen)
        return UPLOAD_ROOT / path_under_uploads

    # Try exact path first
    fs_exact = to_fs(p)
    if fs_exact.is_file():
        return RedirectResponse(local_path, status_code=302, headers=headers)

    # Try size bucket variant: /uploads/avatars/{size}/…
    parts = p.parts
    if len(parts) >= 2 and parts[0] == "uploads" and parts[1] == "avatars":
        candidate = Path("uploads/avatars") / str(size) / Path(*parts[2:])
        fs_candidate = to_fs(candidate)
        if fs_candidate.is_file():
            return RedirectResponse("/" + str(candidate).replace("\\", "/"), status_code=302, headers=headers)

    # Fallback → initials
    return Response(_initials_svg(display, size), media_type="image/svg+xml",
                    headers={"Cache-Control": "public, max-age=86400"})
