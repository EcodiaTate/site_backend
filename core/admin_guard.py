# site_backend/core/admin_guard.py
from __future__ import annotations
from typing import Optional
import os

from fastapi import Header, HTTPException, status
from jose import jwt, JWTError

JWT_SECRET = os.getenv("JWT_SECRET", "dev-secret-change-me")
JWT_ALGO = os.getenv("JWT_ALGO", "HS256")  # keep in sync with api.auth
ADMIN_EMAIL = (os.getenv("ADMIN_EMAIL") or "tate@ecodia.au").lower()
# site_backend/core/admin_guard.py
import hashlib, logging
logging.warning(
    "ADMIN_GUARD JWT_SECRET length=%d sha256=%s algo=%s",
    len(JWT_SECRET),
    hashlib.sha256(JWT_SECRET.encode("utf-8")).hexdigest(),
    JWT_ALGO,
)

def _decode_token(token: str) -> dict:
    """
    Decode an admin JWT. If the token includes an 'aud' claim, verify it as 'admin'.
    If no 'aud' is present, fall back to a decode without audience verification.
    """
    # First try: expect aud="admin" (your token includes this)
    try:
        return jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGO], audience="admin")
    except JWTError as e_with_aud:
        # Fallback: accept tokens without 'aud' (legacy)
        try:
            return jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGO], options={"verify_aud": False})
        except JWTError:
            # Re-raise the more specific first error
            raise e_with_aud

async def require_admin(
    x_auth_token: Optional[str] = Header(default=None, alias="X-Auth-Token"),
    authorization: Optional[str] = Header(default=None),
) -> str:
    """
    Validates the admin token produced by your minting function.
    Accepts either:
      - X-Auth-Token: <jwt>
      - Authorization: Bearer <jwt>
    Returns the admin's email (sub).
    """
    token = x_auth_token
    if not token and authorization:
        parts = authorization.split()
        if len(parts) == 2 and parts[0].lower() == "bearer":
            token = parts[1]

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
    # Accept aud=None (legacy) or "admin" (current)
    if aud not in (None, "admin"):
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Invalid token audience")
    if not sub:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Token missing subject")

    return sub
