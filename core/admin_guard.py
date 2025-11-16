# site_backend/core/admin_guard.py
from __future__ import annotations

from typing import Optional, Any
import os
import hashlib
import logging

from fastapi import Header, HTTPException, status
from jose import jwt, JWTError

JWT_SECRET = os.getenv("JWT_SECRET", "dev-secret-change-me")
JWT_ALGO = os.getenv("JWT_ALGO", "HS256")  # keep in sync with api.auth
ADMIN_EMAIL = (os.getenv("ADMIN_EMAIL") or "tate@ecodia.au").lower()

logging.warning(
    "ADMIN_GUARD JWT_SECRET length=%d sha256=%s algo=%s admin_email=%s",
    len(JWT_SECRET),
    hashlib.sha256(JWT_SECRET.encode("utf-8")).hexdigest(),
    JWT_ALGO,
    ADMIN_EMAIL,
)


def _decode_token(token: str) -> dict:
    """
    Decode an admin JWT. If the token includes an 'aud' claim, verify it as 'admin'.
    If no 'aud' is present, fall back to a decode without audience verification.

    This lets you use:
      - dedicated admin tokens with aud="admin" (minted by /auth/admin-cookie)
      - legacy / normal access tokens without an aud claim.
    """
    try:
        # preferred: aud="admin"
        return jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGO], audience="admin")
    except JWTError as e_with_aud:
        # legacy: no aud
        try:
            return jwt.decode(
                token,
                JWT_SECRET,
                algorithms=[JWT_ALGO],
                options={"verify_aud": False},
            )
        except JWTError:
            # re-raise the more specific error
            raise e_with_aud


def _has_admin_scope(scope: Any) -> bool:
    """
    Accept flexible scope formats:
      - "admin"
      - "user admin"
      - "admin sidequests"
      - ["user", "admin"]
    """
    if scope is None:
        return False

    # string form: space/comma separated
    if isinstance(scope, str):
        parts = [
            p.strip().lower()
            for p in scope.replace(",", " ").split()
            if p.strip()
        ]
        return "admin" in parts

    # iterable form: list/tuple/set of scopes
    if isinstance(scope, (list, tuple, set)):
        return any(str(p).lower() == "admin" for p in scope)

    return False


async def require_admin(
    x_auth_token: Optional[str] = Header(default=None, alias="X-Auth-Token"),
    authorization: Optional[str] = Header(default=None),
) -> str:
    """
    Validates the admin token produced by your minting function.
    Accepts either:
      - X-Auth-Token: <jwt>
      - Authorization: Bearer <jwt>

    A token is considered admin if EITHER:
      - its scope contains "admin" (flexible string/list handling), OR
      - its email/sub resolves to ADMIN_EMAIL (allows your current access token to work)

    This keeps the gate strict to your admin email while being lenient
    about whether the token is an "access" or "admin" token.
    """
    token = x_auth_token
    if not token and authorization:
        parts = authorization.split()
        if len(parts) == 2 and parts[0].lower() == "bearer":
            token = parts[1]

    if not token or not isinstance(token, str):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Admin token required",
        )

    try:
        payload = _decode_token(token)
    except JWTError as e:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=f"Invalid admin token: {e}",
        )

    logging.warning("ADMIN_GUARD payload=%r", payload)

    scope = payload.get("scope")
    aud = payload.get("aud")
    sub_raw = payload.get("sub") or ""
    email_raw = payload.get("email") or ""

    sub = str(sub_raw).lower()
    email = str(email_raw).lower()

    if not sub and not email:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token missing subject/email",
        )

    # Flexible admin detection:
    is_admin_by_scope = _has_admin_scope(scope)
    # accept either sub or email claim matching ADMIN_EMAIL
    is_admin_by_email = (sub == ADMIN_EMAIL) or (email == ADMIN_EMAIL)

    if not (is_admin_by_scope or is_admin_by_email):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Not an admin token (scope mismatch)",
        )

    # Accept aud=None (legacy) or "admin" (current). Reject anything else.
    if aud not in (None, "admin"):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Invalid token audience",
        )

    # Return a canonical admin identity (email if available, else sub)
    return email or sub
