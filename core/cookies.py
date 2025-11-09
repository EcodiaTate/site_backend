from __future__ import annotations
import os
from fastapi import Response, Request

REFRESH_COOKIE_NAME = os.getenv("REFRESH_COOKIE_NAME", "refresh_token")
ADMIN_COOKIE_NAME   = os.getenv("ADMIN_COOKIE_NAME", "admin_token")
ACCESS_COOKIE_NAME  = os.getenv("ACCESS_COOKIE_NAME", "access_token")

COOKIE_DOMAIN   = os.getenv("AUTH_COOKIE_DOMAIN") or None
COOKIE_PATH     = os.getenv("AUTH_COOKIE_PATH", "/")
COOKIE_SAMESITE = (os.getenv("AUTH_COOKIE_SAMESITE", "lax") or "lax").lower()  # "lax"|"none"|"strict"
COOKIE_SECURE   = os.getenv("AUTH_COOKIE_SECURE", "true").lower() in {"1","true","yes"}

def _is_local(req: Request | None) -> bool:
    if not req:
        return False
    host = (req.headers.get("host") or "").split(":")[0].lower()
    scheme = req.url.scheme.lower()
    return scheme == "http" and host in ("localhost", "127.0.0.1")

def set_scoped_cookie(
    response: Response,
    *,
    name: str,
    value: str,
    max_age: int,
    http_only: bool = True,
    request: Request | None = None,
):
    local = _is_local(request)

    # On localhost over HTTP: no Secure, no Domain, avoid SameSite=None (it requires Secure)
    secure   = False if local else COOKIE_SECURE
    samesite = ("lax" if local and COOKIE_SAMESITE == "none" else COOKIE_SAMESITE).lower()
    domain   = None if local else COOKIE_DOMAIN

    response.set_cookie(
        key=name,
        value=value,
        path=COOKIE_PATH,
        domain=domain,
        httponly=http_only,
        secure=secure,
        samesite=samesite,  # "lax" | "none" | "strict"
        max_age=max_age,
    )

def delete_scoped_cookie(response: Response, *, name: str, request: Request | None = None):
    local = _is_local(request)
    domain = None if local else COOKIE_DOMAIN
    response.delete_cookie(
        key=name,
        path=COOKIE_PATH,
        domain=domain,
    )
