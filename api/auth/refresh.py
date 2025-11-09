from __future__ import annotations
from fastapi import APIRouter, Request, Response, HTTPException
from jose import jwt, JWTError
import os, time
from site_backend.core.cookies import set_scoped_cookie, delete_scoped_cookie, REFRESH_COOKIE_NAME

router = APIRouter(tags=["auth"])

ACCESS_JWT_SECRET = os.getenv("ACCESS_JWT_SECRET", os.getenv("JWT_SECRET", "dev-secret-change-me"))
ACCESS_JWT_ALGO   = os.getenv("ACCESS_JWT_ALGO", "HS256")
ACCESS_JWT_TTL_S  = int(os.getenv("ACCESS_JWT_TTL_S", "900"))
ACCESS_JWT_ISS    = os.getenv("ACCESS_JWT_ISS")
ACCESS_JWT_AUD    = os.getenv("ACCESS_JWT_AUD")

REFRESH_JWT_SECRET = os.getenv("REFRESH_JWT_SECRET", os.getenv("JWT_SECRET", "dev-secret-change-me"))
REFRESH_JWT_ALGO   = os.getenv("REFRESH_JWT_ALGO", "HS256")
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
    return jwt.encode(payload, ACCESS_JWT_SECRET, algorithm=ACCESS_JWT_ALGO), exp

def _mint_refresh(uid: str) -> str:
    now = _now_s()
    exp = now + REFRESH_TTL_DAYS * 24 * 3600
    payload = {"sub": uid, "iat": now, "exp": exp, "typ": "refresh"}
    return jwt.encode(payload, REFRESH_JWT_SECRET, algorithm=REFRESH_JWT_ALGO)

@router.post("/refresh")
def refresh(request: Request, response: Response):
    raw = request.cookies.get(REFRESH_COOKIE_NAME)
    if not raw:
        raise HTTPException(status_code=401, detail="No refresh token")
    try:
        # small leeway helps around clock skew in dev/prod
        claims = jwt.decode(
            raw, REFRESH_JWT_SECRET, algorithms=[REFRESH_JWT_ALGO], options={"leeway": 10}
        )
        if claims.get("typ") not in (None, "refresh"):
            # 1. Pass request to delete
            delete_scoped_cookie(response, name=REFRESH_COOKIE_NAME, request=request)
            raise HTTPException(status_code=401, detail="Wrong token type")
        uid = str(claims.get("sub") or "")
        if not uid:
            # 2. Pass request to delete
            delete_scoped_cookie(response, name=REFRESH_COOKIE_NAME, request=request)
            raise HTTPException(status_code=401, detail="Refresh missing subject")
    except JWTError:
        # 3. Pass request to delete
        delete_scoped_cookie(response, name=REFRESH_COOKIE_NAME, request=request)
        raise HTTPException(status_code=401, detail="Invalid refresh token")

    # rotate refresh and return fresh access
    new_refresh = _mint_refresh(uid)
    
    # 4. Pass request to set
    set_scoped_cookie(
        response,
        name=REFRESH_COOKIE_NAME,
        value=new_refresh,
        max_age=REFRESH_TTL_DAYS * 24 * 3600,
        request=request,
    )
    access, exp = _mint_access(uid)
    return {"access": access, "exp": exp}