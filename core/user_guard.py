# site_backend/core/user_guard.py
from __future__ import annotations
from typing import Optional
import os
from fastapi import Header, Cookie, HTTPException, status

# Default to OFF in real dev; you can re-enable per-request via a header below.
DEV_MODE = (os.getenv("DEV_MODE", "false").lower() in {"1", "true", "yes"})

async def current_user_id(
    x_user_id: Optional[str] = Header(default=None),
    authorization: Optional[str] = Header(default=None),
    eyba_user_token: Optional[str] = Cookie(default=None),
    x_dev_auth: Optional[str] = Header(default=None, alias="X-Dev-Auth"),
) -> str:
    """
    Accept multiple auth sources, in order:
    1) X-User-Id: <user-id>                (explicit override; dev tools)
    2) Authorization: Bearer <token>       (SPA header; token == user_id)
    3) Cookie eyba_user_token              (HttpOnly user cookie)
    4) Dev fallback (opt-in via X-Dev-Auth: 1, and DEV_MODE=true)
    """
    if x_user_id:
        return x_user_id

    if authorization:
        auth = authorization.strip()
        if auth.lower().startswith("bearer "):
            token = auth.split(" ", 1)[1].strip()
            if token:
                return token

    if eyba_user_token:
        return eyba_user_token

    # Opt-in dev fallback, not global
    if DEV_MODE and (x_dev_auth == "1"):
        return "user-dev-1"

    raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Unauthorized")
