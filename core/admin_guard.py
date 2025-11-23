from __future__ import annotations

from typing import Optional, Any, Iterable
import os
import hashlib
import logging

from fastapi import Header, HTTPException, status
from jose import jwt, JWTError

JWT_SECRET = os.getenv("JWT_SECRET", "dev-secret-change-me")
JWT_ALGO = os.getenv("JWT_ALGO", "HS256")  # keep in sync with api.auth

# --- Admin email(s): prefer ADMIN_EMAILS, fallback to legacy ADMIN_EMAIL ---
_legacy = (os.getenv("ADMIN_EMAIL") or "").strip().lower()
_list = [
    e.strip().lower()
    for e in os.getenv("ADMIN_EMAILS", "").split(",")
    if e.strip()
]
ADMIN_EMAILS = sorted({*(_list or []), *([_legacy] if _legacy else [])})

logging.warning(
    "ADMIN_GUARD JWT_SECRET length=%d sha256=%s algo=%s admin_emails=%s",
    len(JWT_SECRET),
    hashlib.sha256(JWT_SECRET.encode("utf-8")).hexdigest(),
    JWT_ALGO,
    ",".join(ADMIN_EMAILS) or "(none)",
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


def _email_in_list(candidate: Optional[str], emails: Iterable[str]) -> bool:
    if not candidate:
        return False
    c = str(candidate).strip().lower()
    return c in set(e.strip().lower() for e in emails)


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
      - its email/sub is in ADMIN_EMAILS (allows current access tokens to work)

    Audience:
      - Accept aud=None (legacy) or aud="admin" (current). Reject anything else.

    Returns a canonical admin identity (email if available, else sub).
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
    # accept either sub or email claim being in ADMIN_EMAILS
    is_admin_by_email = _email_in_list(sub, ADMIN_EMAILS) or _email_in_list(email, ADMIN_EMAILS)

    if not (is_admin_by_scope or is_admin_by_email):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Not an admin token (scope/email mismatch)",
        )

    # Accept aud=None (legacy) or "admin" (current). Reject anything else.
    if aud not in (None, "admin"):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Invalid token audience",
        )

    # Return a canonical admin identity (email if available, else sub)
    return email or sub
