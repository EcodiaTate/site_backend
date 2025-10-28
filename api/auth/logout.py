from fastapi import APIRouter, Response
import os

router = APIRouter(tags=["auth"])

REFRESH_COOKIE_NAME = os.getenv("REFRESH_COOKIE_NAME", "refresh_token")

@router.post("/logout")
def logout(response: Response):
    # Clear all auth-related cookies
    response.delete_cookie("eyba_user_token", path="/", httponly=True, secure=False, samesite="lax")
    response.delete_cookie("admin_token", path="/", httponly=True, secure=False, samesite="lax")
    response.delete_cookie(REFRESH_COOKIE_NAME, path="/", httponly=True, secure=False, samesite="lax")
    return {"ok": True}
