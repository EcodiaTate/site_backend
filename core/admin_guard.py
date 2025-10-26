# site_backend/core/admin_guard.py
from __future__ import annotations
from typing import Optional
import os, hashlib, logging
from fastapi import Header, HTTPException, status, Cookie
from jose import jwt, JWTError

JWT_SECRET = os.getenv("JWT_SECRET", "dev-secret-change-me")
JWT_ALGO = os.getenv("JWT_ALGO", "HS256")
ADMIN_EMAIL = (os.getenv("ADMIN_EMAIL") or "tate@ecodia.au").lower()

logging.warning(
    "ADMIN_GUARD JWT_SECRET length=%d sha256=%s algo=%s",
    len(JWT_SECRET),
    hashlib.sha256(JWT_SECRET.encode("utf-8")).hexdigest(),
    JWT_ALGO,
)

def _decode_token(token: str) -> dict:
    try:
        return jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGO], audience="admin")
    except JWTError as e_with_aud:
        try:
            return jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGO], options={"verify_aud": False})
        except JWTError:
            raise e_with_aud

async def require_admin(
    x_auth_token: Optional[str] = Header(default=None, alias="X-Auth-Token"),
    authorization: Optional[str] = Header(default=None),
    admin_cookie: Optional[str] = Cookie(default=None, alias="admin_token"),
) -> str:
    """
    Accepts either:
      - X-Auth-Token: <jwt>
      - Authorization: Bearer <jwt>
      - Cookie: admin_token=<jwt>   (HttpOnly cookie set by /auth/admin-cookie)
    """
    token = x_auth_token

    if not token and authorization:
        parts = authorization.split()
        if len(parts) == 2 and parts[0].lower() == "bearer":
            token = parts[1]

    if not token and admin_cookie:
        token = admin_cookie

    if not token or not isinstance(token, str):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Admin token required")

    try:
        payload = _decode_token(token)
    except JWTError as e:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail=f"Invalid admin token: {e}")

    scope = payload.get("scope")
    aud = payload.get("aud")
    sub = (payload.get("sub") or "").lower()

    if scope != "admin":
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Not an admin token (scope mismatch)")
    if aud not in (None, "admin"):
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Invalid token audience")
    if not sub:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Token missing subject")

    return sub
