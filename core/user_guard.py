from __future__ import annotations
from typing import Optional
import os, time
from fastapi import Header, Cookie, HTTPException, status
from jose import jwt, JWTError

# =========================
# Config (HS256 for now)
# =========================
ACCESS_JWT_SECRET = os.getenv("ACCESS_JWT_SECRET", os.getenv("JWT_SECRET", "dev-secret-change-me"))
ACCESS_JWT_ALGO   = os.getenv("ACCESS_JWT_ALGO", "HS256")
ACCESS_JWT_ISS    = os.getenv("ACCESS_JWT_ISS", None)      # e.g. "https://ecodia.au"
ACCESS_JWT_AUD    = os.getenv("ACCESS_JWT_AUD", None)      # e.g. "ecodia-site"

# TEMP: allow legacy cookie during migration
ALLOW_LEGACY_COOKIE = os.getenv("ALLOW_LEGACY_COOKIE", "true").lower() in {"1","true","yes"}

# Dev-only impersonation guard (must be explicitly enabled AND requested)
DEV_MODE = os.getenv("DEV_MODE", "false").lower() in {"1", "true", "yes"}

def _invalid_token_401(detail: str = "Unauthorized") -> HTTPException:
    return HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail=detail,
        headers={"WWW-Authenticate": 'Bearer error="invalid_token"'},
    )
def _invalid_token_headers():
    return {"WWW-Authenticate": 'Bearer error="invalid_token"'}

async def maybe_current_user_id(
    authorization: Optional[str] = Header(default=None),
    session_token: Optional[str] = Cookie(default=None),
) -> Optional[str]:
    """
    Best-effort current user:
    - If no credentials ⇒ return None (do NOT raise)
    - If Bearer present but invalid ⇒ return None (treat as anonymous)
    - TEMP: accept legacy cookie if present
    """
    # Bearer path
    if authorization:
        auth = authorization.strip()
        if auth.lower().startswith("bearer "):
            token = auth.split(" ", 1)[1].strip()
            if token:
                try:
                    return _verify_access_and_get_sub(token)
                except HTTPException:
                    return None

    # Legacy cookie fallback (if enabled)
    if os.getenv("ALLOW_LEGACY_COOKIE", "true").lower() in {"1","true","yes"} and session_token:
        return session_token

    return None
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
            raise _invalid_token_401("Token missing subject")
        # Optional nbf/exp checks already enforced by jose
        return uid
    except JWTError as e:
        raise _invalid_token_401(f"Invalid/expired access token: {e}")

async def current_user_id(
    authorization: Optional[str] = Header(default=None),
    # TEMP legacy cookie (keep a week or two only):
    session_token: Optional[str] = Cookie(default=None),
    # Dev backdoor must be explicit and opt-in:
    x_dev_auth: Optional[str] = Header(default=None, alias="X-Dev-Auth"),
    x_user_id: Optional[str] = Header(default=None, alias="X-User-Id"),
) -> str:
    """
    Contract hierarchy (final target):
      1) Authorization: Bearer <access_jwt>  ✅

    Temporary migration paths:
      2) Cookie: session_token=<uid>         ⚠️ legacy (remove soon)
      3) Dev override: X-Dev-Auth: 1 + DEV_MODE=true + X-User-Id=<uid>
    """
    # --- 1) Proper Bearer flow ---
    if authorization:
        auth = authorization.strip()
        if auth.lower().startswith("bearer "):
            token = auth.split(" ", 1)[1].strip()
            if token:
                return _verify_access_and_get_sub(token)

    # --- 2) Legacy cookie fallback (accept raw uid string) ---
    if ALLOW_LEGACY_COOKIE and session_token:
        return session_token

    # --- 3) Dev-only override (never active in prod) ---
    if DEV_MODE and x_dev_auth == "1" and x_user_id:
        return x_user_id

    # Advertise Bearer invalid so FE knows to refresh
    raise _invalid_token_401()
