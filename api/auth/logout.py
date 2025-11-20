# site_backend/api/auth/logout.py
from fastapi import APIRouter, Response, Request
from site_backend.core.cookies import (
    delete_scoped_cookie,
    REFRESH_COOKIE_NAME,
    ADMIN_COOKIE_NAME,
    ACCESS_COOKIE_NAME,  # 👈 add this
)

router = APIRouter(tags=["auth"])

@router.post("/logout")
def logout(response: Response, request: Request):
    # Kill all auth cookies that might exist
    delete_scoped_cookie(response, name=REFRESH_COOKIE_NAME, request=request)
    delete_scoped_cookie(response, name=ACCESS_COOKIE_NAME, request=request)
    delete_scoped_cookie(response, name=ADMIN_COOKIE_NAME, request=request)

    # Also delete the old hardcoded one just in case
    response.delete_cookie(
        "eco_local_user_token",
        path="/",
        httponly=True,
        secure=False,
        samesite="lax",
    )

    return {"ok": True}
