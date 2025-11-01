from __future__ import annotations
from fastapi import APIRouter, Depends, Response, HTTPException, status
from datetime import datetime, timedelta
from jose import jwt
from neo4j import Session
import os

from site_backend.core.user_guard import current_user_id
from site_backend.core.neo_driver import session_dep
from site_backend.core.admin_guard import JWT_SECRET, JWT_ALGO, is_admin_email
from site_backend.core.cookies import set_scoped_cookie, delete_scoped_cookie, ADMIN_COOKIE_NAME

router = APIRouter(prefix="/auth", tags=["auth"])

ADMIN_TTL_DAYS = int(os.getenv("ADMIN_TTL_DAYS", "7"))

def _mint_admin_token(email: str) -> str:
    now = datetime.utcnow()
    payload = {
        "sub": email,
        "scope": "admin",
        "iat": int(now.timestamp()),
        "exp": int((now + timedelta(days=ADMIN_TTL_DAYS)).timestamp()),
    }
    return jwt.encode(payload, JWT_SECRET, algorithm=JWT_ALGO)
@router.post("/admin-cookie")
def r_admin_cookie(
    response: Response,
    session: Session = Depends(session_dep),
    uid: str = Depends(current_user_id),
):
    rec = session.run(
        "MATCH (u:User {id:$id}) RETURN toLower(coalesce(u.email,'')) AS email",
        {"id": uid},
    ).single()
    email = (rec["email"] or "").lower() if rec else ""

    if not is_admin_email(email):
        delete_scoped_cookie(response, name=ADMIN_COOKIE_NAME)
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Admin only")

    token = _mint_admin_token(email)

    # still set the cookie (works in prod same-site), but we'll ALSO return the token
    set_scoped_cookie(
        response,
        name=ADMIN_COOKIE_NAME,
        value=token,
        max_age=ADMIN_TTL_DAYS * 24 * 3600,
        http_only=True,
    )

    # 👇 return token so FE can Bearer it for cross-site POSTs in dev
    return {"ok": True, "admin_token": token}