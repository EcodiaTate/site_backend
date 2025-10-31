from __future__ import annotations
from fastapi import APIRouter, Response
from site_backend.core.cookies import (
    delete_scoped_cookie,
    REFRESH_COOKIE_NAME,
    ADMIN_COOKIE_NAME,
    ACCESS_COOKIE_NAME,
)

router = APIRouter(tags=["auth"])

@router.post("/logout")
def logout(response: Response):
    # Clear HttpOnly cookies using the exact same scope attributes as set
    delete_scoped_cookie(response, name=REFRESH_COOKIE_NAME)
    delete_scoped_cookie(response, name=ADMIN_COOKIE_NAME)
    # If you ever set an access cookie, clear it too
    delete_scoped_cookie(response, name=ACCESS_COOKIE_NAME)
    # (If you also store any non-HttpOnly front-end cookies, clear them client-side)
    return {"ok": True}
