# site_backend/core/admin_guard.py
from __future__ import annotations
import os, time
from fastapi import HTTPException, Request, status
from jose import jwt, JWTError, ExpiredSignatureError  # python-jose

JWT_SECRET = os.getenv("JWT_SECRET", "dev-secret-change-me")
JWT_ALGO   = os.getenv("JWT_ALGO", "HS256")

ADMIN_EMAIL  = (os.getenv("ADMIN_EMAIL") or "").lower().strip() or None
ADMIN_EMAILS = {"tate@ecodia.au"} | ({ADMIN_EMAIL} if ADMIN_EMAIL else set())

def is_admin_email(email: str) -> bool:
    if not email:
        return False
    e = email.lower()
    return (e in ADMIN_EMAILS) or e.endswith("@ecodia.au")

def mint_admin_token(email: str, ttl_secs: int = 60 * 60) -> str:
    now = int(time.time())
    claims = {"sub": email, "scope": "admin", "iat": now, "exp": now + ttl_secs}
    return jwt.encode(claims, JWT_SECRET, algorithm=JWT_ALGO)

def _decode_admin_token(token: str) -> dict:
    """
    Decode an admin token. python-jose in this env doesn't support a 'leeway' kwarg,
    so we implement a 90s grace manually: if the token is expired but within 90s,
    accept it and let the route re-issue/rotate as needed.
    """
    try:
        # Normal strict decode (exp/iat enforced)
        return jwt.decode(
            token,
            JWT_SECRET,
            algorithms=[JWT_ALGO],
            options={"verify_aud": False, "require_iat": True, "require_exp": True},
        )
    except ExpiredSignatureError:
        # Manual 90s grace: decode without exp verification, check ourselves
        try:
            claims = jwt.decode(
                token,
                JWT_SECRET,
                algorithms=[JWT_ALGO],
                options={
                    "verify_aud": False,
                    "verify_exp": False,   # allow reading claims
                    "require_iat": True,
                    "require_exp": True,
                },
            )
            now = int(time.time())
            exp = int(claims.get("exp", 0))
            if exp and (now - exp) <= 90:
                return claims  # within grace
        except JWTError:
            pass
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid admin token: expired",
            headers={"WWW-Authenticate": 'Bearer error="invalid_token"'},
        )
    except JWTError as e:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=f"Invalid admin token: {e}",
            headers={"WWW-Authenticate": 'Bearer error="invalid_token"'},
        )

def _bearer_from_auth_header(request: Request) -> str | None:
    auth = request.headers.get("Authorization") or request.headers.get("authorization")
    if not auth:
        return None
    parts = auth.split()
    if len(parts) == 2 and parts[0].lower() == "bearer":
        return parts[1]
    return None

def require_admin(request: Request) -> str:
    """
    Accept admin credentials from, in order of preference:
    1) HttpOnly cookie 'admin_token'
    2) Authorization: Bearer <admin-token>
    3) X-Auth-Token (legacy)
    """
    cookie = request.cookies.get("admin_token")
    if cookie:
        claims = _decode_admin_token(cookie)
        if claims.get("scope") == "admin":
            return claims.get("sub") or "admin"

    bearer = _bearer_from_auth_header(request)
    if bearer:
        try:
            claims = _decode_admin_token(bearer)
            if claims.get("scope") == "admin":
                return claims.get("sub") or "admin"
        except HTTPException:
            pass

    legacy = request.headers.get("X-Auth-Token") or request.headers.get("x-auth-token")
    if legacy:
        try:
            claims = _decode_admin_token(legacy)
            if claims.get("scope") == "admin":
                return claims.get("sub") or "admin"
        except HTTPException:
            pass

    raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Admin token required",
        headers={"WWW-Authenticate": 'Bearer realm="admin"'},
    )
