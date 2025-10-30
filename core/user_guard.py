from __future__ import annotations
from typing import Optional
import os, re
from fastapi import Header, Cookie, HTTPException, status
from jose import jwt, JWTError

# =========================
# Config (HS256 for now)
# =========================
ACCESS_JWT_SECRET = os.getenv("ACCESS_JWT_SECRET", os.getenv("JWT_SECRET", "dev-secret-change-me"))
ACCESS_JWT_ALGO   = os.getenv("ACCESS_JWT_ALGO", "HS256")
ACCESS_JWT_ISS    = os.getenv("ACCESS_JWT_ISS")      # e.g. "https://ecodia.au"
ACCESS_JWT_AUD    = os.getenv("ACCESS_JWT_AUD")      # e.g. "ecodia-site"

# TEMP: allow legacy cookie during migration (UUID-only; never JWT)
ALLOW_LEGACY_COOKIE = os.getenv("ALLOW_LEGACY_COOKIE", "true").lower() in {"1","true","yes"}

# Dev-only impersonation guard (must be explicitly enabled AND requested)
DEV_MODE = os.getenv("DEV_MODE", "false").lower() in {"1", "true", "yes"}

UUID_RE = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$", re.I)
def _looks_like_uuid(s: str) -> bool:
    return bool(UUID_RE.match(s or ""))

def _looks_like_jwt(s: str) -> bool:
    # very loose check: three dot-separated base64url-ish parts
    return isinstance(s, str) and s.count(".") == 2

def _unauth(detail: str = "Unauthorized") -> HTTPException:
    return HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail=detail,
        headers={"WWW-Authenticate": 'Bearer error="invalid_token"'},
    )

def _verify_access_and_get_sub(token: str) -> str:
    """
    Verify the access token and return the subject (uid).
    HS256 for now; drop-in upgradable to RS256 later.
    """
    try:
        opts = {"verify_aud": bool(ACCESS_JWT_AUD)}
        kwargs = {}
        if ACCESS_JWT_ISS: kwargs["issuer"] = ACCESS_JWT_ISS
        if ACCESS_JWT_AUD: kwargs["audience"] = ACCESS_JWT_AUD

        claims = jwt.decode(
            token,
            ACCESS_JWT_SECRET,
            algorithms=[ACCESS_JWT_ALGO],
            options=opts,
            **kwargs,
        )
        uid = str(claims.get("sub") or claims.get("uid") or "")
        if not uid:
            raise _unauth("Token missing subject")
        return uid
    except JWTError as e:
        raise _unauth(f"Invalid/expired access token: {e}")

# ---------------- Optional "maybe" dependency ----------------
async def maybe_current_user_id(
    authorization: Optional[str] = Header(default=None),
    session_token: Optional[str] = Cookie(default=None),   # legacy cookie
) -> Optional[str]:
    """
    Best-effort current user:
    - If no credentials ⇒ return None (do NOT raise)
    - If Bearer present but invalid ⇒ return None (treat as anonymous)
    - TEMP: accept legacy cookie only if it looks like a UUID (never JWT)
    """
    # 1) Bearer
    if authorization:
        auth = authorization.strip()
        if auth.lower().startswith("bearer "):
            token = auth.split(" ", 1)[1].strip()
            if token:
                try:
                    return _verify_access_and_get_sub(token)
                except HTTPException:
                    return None

    # 2) Legacy cookie (UUID only; reject JWT-shaped or anything else)
    if ALLOW_LEGACY_COOKIE and session_token and _looks_like_uuid(session_token) and not _looks_like_jwt(session_token):
        return session_token

    return None
# core/user_guard.py
from fastapi import Cookie

async def current_user_id(
    authorization: Optional[str] = Header(default=None),
    session_token: Optional[str] = Cookie(default=None),
    access_token: Optional[str] = Cookie(default=None),      # 👈 new
    x_dev_auth: Optional[str] = Header(default=None, alias="X-Dev-Auth"),
    x_user_id: Optional[str] = Header(default=None, alias="X-User-Id"),
) -> str:
    # 1) Bearer
    if authorization and authorization.lower().startswith("bearer "):
        token = authorization.split(" ", 1)[1].strip()
        if token:
            return _verify_access_and_get_sub(token)

    # 1b) Cookie JWT (if present)
    if access_token and _looks_like_jwt(access_token):
        return _verify_access_and_get_sub(access_token)

    # 2) Legacy UUID cookie
    if ALLOW_LEGACY_COOKIE and session_token and _looks_like_uuid(session_token) and not _looks_like_jwt(session_token):
        return session_token

    # 3) Dev override
    if DEV_MODE and x_dev_auth == "1" and x_user_id and _looks_like_uuid(x_user_id):
        return x_user_id

    raise _unauth()
