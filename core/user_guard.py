# site_backend/core/user_guard.py
from __future__ import annotations
from typing import Optional
import os
import re

from fastapi import Header, Cookie, HTTPException, status
from jose import jwt, JWTError

# ────────────────────────────────────────────────────────────────────────────
# Config (env-driven, with sane defaults)
# ────────────────────────────────────────────────────────────────────────────

ACCESS_JWT_SECRET = os.getenv("ACCESS_JWT_SECRET", os.getenv("JWT_SECRET", "dev-secret-change-me"))
ACCESS_JWT_ALGO = os.getenv("ACCESS_JWT_ALGO", "HS256")
ACCESS_JWT_ISS = os.getenv("ACCESS_JWT_ISS")  # optional
ACCESS_JWT_AUD = os.getenv("ACCESS_JWT_AUD")  # optional

# Names for cookies (so FE/BE can evolve without breaking)
ACCESS_COOKIE_NAME = os.getenv("ACCESS_COOKIE_NAME", "access")  # JWT-bearing cookie
LEGACY_COOKIE_NAME = os.getenv("LEGACY_SESSION_COOKIE", "session_token")  # UUID session id (legacy)

ALLOW_LEGACY_COOKIE = os.getenv("ALLOW_LEGACY_COOKIE", "true").lower() in {"1", "true", "yes"}
DEV_MODE = os.getenv("DEV_MODE", "false").lower() in {"1", "true", "yes"}

UUID_RE = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$",
    re.I,
)


def _looks_like_uuid(s: str) -> bool:
    return bool(UUID_RE.match(s or ""))


def _looks_like_jwt(s: str) -> bool:
    return isinstance(s, str) and s.count(".") == 2


def _unauth(detail: str = "Unauthorized") -> HTTPException:
    return HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail=detail,
        headers={"WWW-Authenticate": 'Bearer error="invalid_token"'},
    )


# ────────────────────────────────────────────────────────────────────────────
# Helpers
# ────────────────────────────────────────────────────────────────────────────


def _extract_bearer(authorization: Optional[str]) -> Optional[str]:
    """
    Accepts typical auth header variants:
      - "Bearer <token>"
      - "bearer <token>"
      - "JWT <token>"
      - "Token <token>"
    """
    if not authorization:
        return None
    auth = authorization.strip()
    if not auth:
        return None
    lower = auth.lower()
    for prefix in ("bearer ", "jwt ", "token "):
        if lower.startswith(prefix):
            return auth[len(prefix) :].strip()
    # If header has no prefix but *is* a JWT, accept it (some proxies forward raw token)
    if _looks_like_jwt(auth):
        return auth
    return None


def _verify_access_and_get_sub(token: str) -> str:
    """
    Decode + validate an *access* JWT and return its subject (user id).
    Respects optional ISS/AUD if provided.
    """
    try:
        options = {"verify_aud": bool(ACCESS_JWT_AUD)}
        kwargs = {}
        if ACCESS_JWT_ISS:
            kwargs["issuer"] = ACCESS_JWT_ISS
        if ACCESS_JWT_AUD:
            kwargs["audience"] = ACCESS_JWT_AUD

        claims = jwt.decode(
            token,
            ACCESS_JWT_SECRET,
            algorithms=[ACCESS_JWT_ALGO],
            options=options,
            **kwargs,
        )
        uid = str(claims.get("sub") or claims.get("uid") or "").strip()
        if not uid:
            raise _unauth("Token missing subject")
        return uid
    except JWTError as e:
        # keep detail terse in prod; jose already strips secrets
        raise _unauth(f"Invalid/expired access token: {e}")


# ────────────────────────────────────────────────────────────────────────────
# Public dependencies
# ────────────────────────────────────────────────────────────────────────────


async def maybe_current_user_id(
    authorization: Optional[str] = Header(default=None),
    # Legacy UUID session cookie (optional)
    session_token: Optional[str] = Cookie(default=None, alias=LEGACY_COOKIE_NAME),
    # Access JWT cookie (optional)
    access_cookie: Optional[str] = Cookie(default=None, alias=ACCESS_COOKIE_NAME),
) -> Optional[str]:
    """
    Soft auth: return user id if present/valid, else None.
    Order: Authorization header → access cookie → legacy UUID cookie (if allowed).
    """
    # 1) Authorization header (preferred)
    bearer = _extract_bearer(authorization)
    if bearer:
        try:
            return _verify_access_and_get_sub(bearer)
        except HTTPException:
            return None

    # 2) Access JWT cookie
    if access_cookie and _looks_like_jwt(access_cookie):
        try:
            return _verify_access_and_get_sub(access_cookie)
        except HTTPException:
            return None

    # 3) Legacy UUID cookie (session id)
    if (
        ALLOW_LEGACY_COOKIE
        and session_token
        and _looks_like_uuid(session_token)
        and not _looks_like_jwt(session_token)
    ):
        return session_token

    return None


async def current_user_id(
    authorization: Optional[str] = Header(default=None),
    session_token: Optional[str] = Cookie(default=None, alias=LEGACY_COOKIE_NAME),
    access_cookie: Optional[str] = Cookie(default=None, alias=ACCESS_COOKIE_NAME),
    x_dev_auth: Optional[str] = Header(default=None, alias="X-Dev-Auth"),
    x_user_id: Optional[str] = Header(default=None, alias="X-User-Id"),
) -> str:
    """
    Hard auth: must return a valid user id or raise 401.
    Order: Authorization header → access cookie → legacy UUID cookie → dev override.
    """
    # 1) Authorization header (preferred; works for FE fetch with Bearer)
    bearer = _extract_bearer(authorization)
    if bearer:
        return _verify_access_and_get_sub(bearer)

    # 2) Access JWT cookie (supports SSR/cookie flows)
    if access_cookie and _looks_like_jwt(access_cookie):
        return _verify_access_and_get_sub(access_cookie)

    # 3) Legacy UUID cookie (pre-JWT sessions)
    if (
        ALLOW_LEGACY_COOKIE
        and session_token
        and _looks_like_uuid(session_token)
        and not _looks_like_jwt(session_token)
    ):
        return session_token

    # 4) Dev override (useful for local tooling/curl)
    if DEV_MODE and x_dev_auth == "1" and x_user_id and _looks_like_uuid(x_user_id):
        return x_user_id

    # 5) No luck
    raise _unauth()
