from __future__ import annotations
from fastapi import APIRouter, Depends, Request, Response, HTTPException, status
from jose import jwt, JWTError
import os, time

router = APIRouter(tags=["auth"])

# Match auth_main settings
ACCESS_JWT_SECRET = os.getenv("ACCESS_JWT_SECRET", os.getenv("JWT_SECRET", "dev-secret-change-me"))
ACCESS_JWT_ALGO   = os.getenv("ACCESS_JWT_ALGO", "HS256")
ACCESS_JWT_TTL_S  = int(os.getenv("ACCESS_JWT_TTL_S", "900"))
ACCESS_JWT_ISS    = os.getenv("ACCESS_JWT_ISS", None)
ACCESS_JWT_AUD    = os.getenv("ACCESS_JWT_AUD", None)

REFRESH_JWT_SECRET = os.getenv("REFRESH_JWT_SECRET", os.getenv("JWT_SECRET", "dev-secret-change-me"))
REFRESH_JWT_ALGO   = os.getenv("REFRESH_JWT_ALGO", "HS256")
REFRESH_COOKIE_NAME = os.getenv("REFRESH_COOKIE_NAME", "refresh_token")
REFRESH_TTL_DAYS   = int(os.getenv("REFRESH_TTL_DAYS", "90"))

def _now_s() -> int:
    return int(time.time())

def _mint_access(uid: str, email: str | None = None) -> tuple[str, int]:
    now = _now_s()
    exp = now + ACCESS_JWT_TTL_S
    payload = {"sub": uid, "iat": now, "exp": exp}
    if email: payload["email"] = email
    if ACCESS_JWT_ISS: payload["iss"] = ACCESS_JWT_ISS
    if ACCESS_JWT_AUD: payload["aud"] = ACCESS_JWT_AUD
    tok = jwt.encode(payload, ACCESS_JWT_SECRET, algorithm=ACCESS_JWT_ALGO)
    return tok, exp

def _mint_refresh(uid: str) -> str:
    now = _now_s()
    exp = now + REFRESH_TTL_DAYS * 24 * 3600
    payload = {"sub": uid, "iat": now, "exp": exp, "typ": "refresh"}
    return jwt.encode(payload, REFRESH_JWT_SECRET, algorithm=REFRESH_JWT_ALGO)

@router.post("/refresh")
def refresh(request: Request, response: Response):
    """
    Rotate refresh cookie and return a fresh short-lived access token.
    Uses only the HttpOnly cookie; no Authorization header needed.
    """
    cookie_name = REFRESH_COOKIE_NAME
    raw = request.cookies.get(cookie_name)
    if not raw:
        raise HTTPException(status_code=401, detail="No refresh token")

    try:
        claims = jwt.decode(raw, REFRESH_JWT_SECRET, algorithms=[REFRESH_JWT_ALGO])
        uid = str(claims.get("sub") or "")
        if not uid:
            raise HTTPException(status_code=401, detail="Refresh missing subject")
    except JWTError:
        # clear the bad cookie
        response.delete_cookie(cookie_name, path="/", httponly=True, secure=False, samesite="lax")
        raise HTTPException(status_code=401, detail="Invalid refresh token")

    # Rotate refresh
    new_refresh = _mint_refresh(uid)
    response.set_cookie(
        key=cookie_name,
        value=new_refresh,
        path="/",
        httponly=True,
        secure=False,  # set True in prod (HTTPS)
        samesite="lax",
        max_age=REFRESH_TTL_DAYS * 24 * 3600,
    )

    access, exp = _mint_access(uid)
    return {"access": access, "exp": exp}
