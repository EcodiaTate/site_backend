# site_backend/api/auth/admin_cookie.py
from __future__ import annotations
from datetime import datetime, timedelta

from fastapi import APIRouter, Depends, Response, HTTPException, status, Request
from jose import jwt
from neo4j import Session

from site_backend.core.user_guard import current_user_id
from site_backend.core.neo_driver import session_dep
from site_backend.core.admin_guard import JWT_SECRET, JWT_ALGO, ADMIN_EMAIL
from site_backend.core.cookies import (
    set_scoped_cookie,
    delete_scoped_cookie,
    ADMIN_COOKIE_NAME,
)

router = APIRouter(prefix="/auth", tags=["auth"])


def _mint_admin_token(email: str) -> str:
    now = datetime.utcnow()
    payload = {
        "sub": email,
        "scope": "admin",
        "aud": "admin",  # 🔑 matches admin_guard expectation (or is allowed as None)
        "iat": int(now.timestamp()),
        "exp": int((now + timedelta(days=7)).timestamp()),
    }
    return jwt.encode(payload, JWT_SECRET, algorithm=JWT_ALGO)


@router.post("/admin-cookie")
def r_admin_cookie(
    response: Response,
    request: Request,
    session: Session = Depends(session_dep),
    uid: str = Depends(current_user_id),
):
    # Look up email by uid (adjust to your user model)
    rec = session.run(
        "MATCH (u:User {id:$id}) RETURN toLower(coalesce(u.email,'')) AS email",
        {"id": uid},
    ).single()
    email = (rec["email"] or "").lower() if rec else ""

    # Only the configured ADMIN_EMAIL can get an admin token
    if email != ADMIN_EMAIL:
        delete_scoped_cookie(response, name=ADMIN_COOKIE_NAME, request=request)
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Admin only",
        )

    token = _mint_admin_token(email)

    # HttpOnly cookie with correct localhost/prod behaviour
    set_scoped_cookie(
        response,
        name=ADMIN_COOKIE_NAME,
        value=token,
        max_age=7 * 24 * 3600,
        http_only=True,
        request=request,
    )

    # 🔑 IMPORTANT: return the token so ensureAdminCookie() can stash it in localStorage
    return {"ok": True, "admin_token": token}
