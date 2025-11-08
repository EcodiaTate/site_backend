# site_backend/api/auth/logout.py
from fastapi import APIRouter, Response

router = APIRouter(tags=["auth"])

@router.post("/logout")
def logout(response: Response):
    # Clear both possible cookies
    response.delete_cookie("eco_local_user_token", path="/", httponly=True, secure=False, samesite="lax")
    response.delete_cookie("admin_token", path="/", httponly=True, secure=False, samesite="lax")
    return {"ok": True}
