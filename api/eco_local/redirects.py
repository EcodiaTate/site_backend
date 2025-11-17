# api/media/redirects.py (new or alongside your avatar router)
from __future__ import annotations
from typing import Optional
from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import RedirectResponse
from neo4j import Session
from site_backend.core.neo_driver import session_dep

router = APIRouter(tags=["media-redirects"])

# ------- avatars (existing) -------

CY_AVATAR = """
MATCH (u:User {id:$uid})
RETURN
  u.avatar_url AS avatar_url,
  u.avatar_sha AS avatar_sha,
  u.avatar_rev AS avatar_rev
LIMIT 1
"""

def _normalize_upload_url(raw: Optional[str]) -> Optional[str]:
    if not raw:
        return None
    s = str(raw).strip()
    if not s:
        return None
    if s.startswith("http://") or s.startswith("https://"):
        return s
    if not s.startswith("/"):
        s = "/" + s
    if s.startswith("/uploads/avatar/"):
        s = s.replace("/uploads/avatar/", "/uploads/avatars/", 1)
    return s

def _cache_headers(resp: RedirectResponse, rev: Optional[str]) -> None:
    resp.headers["Cache-Control"] = (
        "public, max-age=31536000, immutable" if rev else "public, max-age=3600"
    )

def _build_avatar_from_sha(sha: str, rev: Optional[str]) -> str:
    aa, bb = sha[0:2], sha[2:4]
    path = f"/uploads/avatars/{aa}/{bb}/{sha}.webp"
    if rev:
        path += ("&" if "?" in path else "?") + f"v={rev}"
    return path

@router.get("/u/{user_id}/avatar/{size}")
@router.head("/u/{user_id}/avatar/{size}")
def user_avatar_redirect(user_id: str, size: int, session: Session = Depends(session_dep)):
    rec = session.run(CY_AVATAR, {"uid": user_id}).single()
    if not rec:
        raise HTTPException(status_code=404, detail="User not found")
    row = rec.data()
    url = row.get("avatar_url")
    sha = row.get("avatar_sha")
    rev = row.get("avatar_rev")

    u = _normalize_upload_url(url)
    if u:
        if u.startswith("/uploads/") and rev and "v=" not in u:
            u += ("&" if "?" in u else "?") + f"v={rev}"
        resp = RedirectResponse(u, status_code=307)
        _cache_headers(resp, rev)
        return resp

    if sha:
        path = _build_avatar_from_sha(sha, rev)
        resp = RedirectResponse(path, status_code=307)
        _cache_headers(resp, rev)
        return resp

    raise HTTPException(status_code=404, detail="Avatar not set")

# ------- heroes (NEW, same pattern) -------

CY_HERO = """
MATCH (b:BusinessProfile {id:$bid})
RETURN
  b.hero_url AS hero_url,
  b.hero_sha AS hero_sha,
  b.hero_rev AS hero_rev
LIMIT 1
"""

def _normalize_hero_url(raw: Optional[str]) -> Optional[str]:
    if not raw:
        return None
    s = str(raw).strip()
    if not s:
        return None
    if s.startswith("http://") or s.startswith("https://"):
        return s
    if not s.startswith("/"):
        s = "/" + s
    # legacy singular
    if s.startswith("/uploads/hero/"):
        s = s.replace("/uploads/hero/", "/uploads/heroes/", 1)
    # keep legacy asset route working without rewrites
    # (/eco-local/assets/hero/abc.png) - your old endpoint can still serve it
    return s

def _build_hero_from_sha(sha: str, rev: Optional[str]) -> str:
    aa, bb = sha[0:2], sha[2:4]
    path = f"/uploads/heroes/{aa}/{bb}/{sha}.webp"
    if rev:
        path += ("&" if "?" in path else "?") + f"v={rev}"
    return path

@router.get("/b/{business_id}/hero/{size}")
@router.head("/b/{business_id}/hero/{size}")
def business_hero_redirect(business_id: str, size: int, session: Session = Depends(session_dep)):
    rec = session.run(CY_HERO, {"bid": business_id}).single()
    if not rec:
        raise HTTPException(status_code=404, detail="Business not found")
    row = rec.data()
    url = row.get("hero_url")
    sha = row.get("hero_sha")
    rev = row.get("hero_rev")

    u = _normalize_hero_url(url)
    if u:
        if u.startswith("/uploads/") and rev and "v=" not in u:
            u += ("&" if "?" in u else "?") + f"v={rev}"
        resp = RedirectResponse(u, status_code=307)
        _cache_headers(resp, rev)
        return resp

    if sha:
        path = _build_hero_from_sha(sha, rev)
        resp = RedirectResponse(path, status_code=307)
        _cache_headers(resp, rev)
        return resp

    raise HTTPException(status_code=404, detail="Hero not set")
