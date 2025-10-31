from __future__ import annotations
import os
from fastapi import Response

REFRESH_COOKIE_NAME = os.getenv("REFRESH_COOKIE_NAME", "refresh_token")
ADMIN_COOKIE_NAME   = os.getenv("ADMIN_COOKIE_NAME", "admin_token")
ACCESS_COOKIE_NAME  = os.getenv("ACCESS_COOKIE_NAME", "access_token")  # optional, if you ever set it

COOKIE_DOMAIN   = os.getenv("AUTH_COOKIE_DOMAIN") or None
COOKIE_PATH     = os.getenv("AUTH_COOKIE_PATH", "/")
COOKIE_SAMESITE = (os.getenv("AUTH_COOKIE_SAMESITE", "lax") or "lax").lower()  # "lax"|"none"|"strict"
COOKIE_SECURE   = os.getenv("AUTH_COOKIE_SECURE", "true").lower() in {"1","true","yes"}

def set_scoped_cookie(response: Response, *, name: str, value: str, max_age: int, http_only: bool = True):
    response.set_cookie(
        key=name,
        value=value,
        path=COOKIE_PATH,
        domain=COOKIE_DOMAIN,
        httponly=http_only,
        secure=COOKIE_SECURE,
        samesite=COOKIE_SAMESITE,
        max_age=max_age,
    )

def delete_scoped_cookie(response: Response, *, name: str):
    response.delete_cookie(
        key=name,
        path=COOKIE_PATH,
        domain=COOKIE_DOMAIN,
        httponly=True,
        secure=COOKIE_SECURE,
        samesite=COOKIE_SAMESITE,
    )
