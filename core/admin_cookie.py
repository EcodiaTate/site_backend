# site_backend/api/auth/admin_cookie.py
from __future__ import annotations
from fastapi import APIRouter, Depends, Response, HTTPException, status
from datetime import datetime, timedelta
from jose import jwt

from site_backend.core.user_guard import current_user_id
from site_backend.core.neo_driver import session_dep
from neo4j import Session
from site_backend.core.admin_guard import JWT_SECRET, JWT_ALGO, ADMIN_EMAIL

router = APIRouter(prefix="/auth", tags=["auth"])

def _mint_admin_token(email: str) -> str:
    now = datetime.utcnow()
    payload = {
        "sub": email,
        "scope": "admin",
        "iat": int(now.timestamp()),
        "exp": int((now + timedelta(days=7)).timestamp()),
    }
    return jwt.encode(payload, JWT_SECRET, algorithm=JWT_ALGO)

@router.post("/admin-cookie")
def r_admin_cookie(
    response: Response,
    session: Session = Depends(session_dep),
    uid: str = Depends(current_user_id),
):
    # look up email by uid (adjust to your user model)
    rec = session.run("MATCH (u:User {id:$id}) RETURN toLower(coalesce(u.email,'')) AS email", {"id": uid}).single()
    email = (rec["email"] or "").lower() if rec else ""

    if email != ADMIN_EMAIL:
        # clear cookie if present
        response.delete_cookie("admin_token", path="/", httponly=True, secure=False, samesite="lax")
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Admin only")

    token = _mint_admin_token(email)
    # Set HttpOnly cookie (adjust secure=True if behind HTTPS)
    response.set_cookie(
        key="admin_token",
        value=token,
        path="/",
        httponly=True,
        secure=False,   # set True in prod over HTTPS
        samesite="lax",
        max_age=7*24*3600,
    )
    return {"ok": True}
