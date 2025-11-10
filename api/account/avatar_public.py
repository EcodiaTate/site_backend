from __future__ import annotations

from typing import Optional
from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import RedirectResponse
from neo4j import Session

from site_backend.core.neo_driver import session_dep

router = APIRouter(tags=["account-avatars"])

CY_Q = """
MATCH (u:User {id:$uid})
RETURN
  u.avatar_url        AS avatar_url,
  u.avatar_sha        AS avatar_sha,
  u.avatar_rev        AS avatar_rev,
  u.avatar_updated_at AS avatar_updated_at
LIMIT 1
"""

# ---- helpers ---------------------------------------------------------------

def _normalize_upload_url(raw: Optional[str]) -> Optional[str]:
    """
    Accepts:
      - http(s) absolute → pass through
      - '/uploads/avatars/...', 'uploads/avatars/...' (plural)
      - '/uploads/avatar/...',  'uploads/avatar/...'  (singular, legacy)
      - other relative → return '/<raw>'
    Ensures leading slash; preserves query (?v=...).
    """
    if not raw:
        return None
    s = str(raw).strip()
    if not s:
        return None
    if s.startswith("http://") or s.startswith("https://"):
        return s

    # ensure leading slash for relative
    if not s.startswith("/"):
        s = "/" + s

    # normalize legacy singular → plural
    if s.startswith("/uploads/avatar/"):
        s = s.replace("/uploads/avatar/", "/uploads/avatars/", 1)

    # accept both plural and (if it slips in) singular
    if s.startswith("/uploads/avatars/") or s.startswith("/uploads/avatar/"):
        return s

    # unknown relative path – still return normalized absolute-ish
    return s

def _build_from_sha(sha: str, rev: Optional[str]) -> str:
    """
    Canonical sharded path:
      /uploads/avatars/aa/bb/<sha>.webp[?v=<rev>]
    """
    aa, bb = sha[0:2], sha[2:4]
    path = f"/uploads/avatars/{aa}/{bb}/{sha}.webp"
    if rev:
        join = "&" if "?" in path else "?"
        path = f"{path}{join}v={rev}"
    return path

def _cache_headers(resp: RedirectResponse, rev: Optional[str]) -> None:
    if rev:
        resp.headers["Cache-Control"] = "public, max-age=31536000, immutable"
    else:
        resp.headers["Cache-Control"] = "public, max-age=3600"

# ---- routes ----------------------------------------------------------------

@router.get("/u/{user_id}/avatar/{size}")
@router.head("/u/{user_id}/avatar/{size}")
def user_avatar_redirect(
    user_id: str,
    size: int,  # kept for URL parity/caching, not used server-side
    session: Session = Depends(session_dep),
):
    """
    Resolves a user's avatar to a concrete asset URL via 307 redirect.
    - Prefer explicit u.avatar_url (handles flat/sharded, singular/plural, and http(s)).
    - Fallback to u.avatar_sha → canonical sharded path.
    """
    rec = session.run(CY_Q, {"uid": user_id}).single()
    if not rec:
        raise HTTPException(status_code=404, detail="User not found")

    row = rec.data()  # <-- IMPORTANT: get dict
    url: Optional[str] = row.get("avatar_url")
    sha: Optional[str] = row.get("avatar_sha")
    rev: Optional[str] = row.get("avatar_rev")

    # 1) Use explicit URL if present
    u = _normalize_upload_url(url)
    if u:
        # ensure rev is appended for immutable caching if not present
        if u.startswith("/uploads/") and rev and "v=" not in u:
            sep = "&" if "?" in u else "?"
            u = f"{u}{sep}v={rev}"
        resp = RedirectResponse(u, status_code=307)
        _cache_headers(resp, rev)
        return resp

    # 2) Derive from sha (supports sharded layout)
    if sha:
        path = _build_from_sha(sha, rev)
        resp = RedirectResponse(path, status_code=307)
        _cache_headers(resp, rev)
        return resp

    # 3) No avatar set
    raise HTTPException(status_code=404, detail="Avatar not set")
