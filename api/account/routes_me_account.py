from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, status, UploadFile, File
from pydantic import BaseModel, EmailStr, Field
from typing import Optional
from neo4j import Session

from site_backend.core.neo_driver import session_dep
from site_backend.core.user_guard import current_user_id
from site_backend.core.urls import abs_media

from .service import (
    get_me_account,
    update_display_name,
    begin_change_email,
    trigger_verify_email,
    change_password as svc_change_password,
    trigger_password_reset,
    set_user_avatar_from_bytes,
    clear_user_avatar,
)

router = APIRouter(prefix="/me/account", tags=["account"])


# -------------------- Schemas --------------------

class MeAccount(BaseModel):
    id: str
    email: EmailStr
    display_name: str | None = None
    role: str = Field(default="public")
    email_verified: bool = False
    avatar_url: Optional[str] = None  # NEW


class DisplayNameIn(BaseModel):
    display_name: str = Field(min_length=1, max_length=80)


class ChangeEmailIn(BaseModel):
    new_email: EmailStr


class ChangeEmailOut(BaseModel):
    pending_verification: bool = True


class VerifyEmailOut(BaseModel):
    sent: bool = True


class ChangePasswordIn(BaseModel):
    current_password: str = Field(min_length=6)
    new_password: str = Field(min_length=8)


class ChangePasswordOut(BaseModel):
    ok: bool = True


class PasswordResetOut(BaseModel):
    sent: bool = True


class AvatarOut(BaseModel):
    avatar_url: Optional[str] = None


# -------------------- Routes --------------------

@router.get("", response_model=MeAccount)
def r_get_me_account(uid: str = Depends(current_user_id), s: Session = Depends(session_dep)):
    acc = get_me_account(s, uid)
    if not acc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="User not found")
    # Ensure absolute media URL if PUBLIC_API_ORIGIN is set
    acc["avatar_url"] = abs_media(acc.get("avatar_url"))
    return acc


@router.patch("/display-name", response_model=dict)
def r_update_display_name(
    p: DisplayNameIn,
    uid: str = Depends(current_user_id),
    s: Session = Depends(session_dep),
):
    update_display_name(s, uid, p.display_name.strip())
    return {"ok": True}


@router.post("/change-email", response_model=ChangeEmailOut)
def r_change_email(
    p: ChangeEmailIn,
    uid: str = Depends(current_user_id),
    s: Session = Depends(session_dep),
):
    begin_change_email(s, uid, p.new_email)
    # We always require verification; client expects "pending_verification"
    return ChangeEmailOut(pending_verification=True)


@router.post("/send-verify-email", response_model=VerifyEmailOut)
def r_send_verify_email(uid: str = Depends(current_user_id), s: Session = Depends(session_dep)):
    sent = trigger_verify_email(s, uid)
    return VerifyEmailOut(sent=sent)


@router.post("/change-password", response_model=ChangePasswordOut)
def r_change_password(
    p: ChangePasswordIn,
    uid: str = Depends(current_user_id),
    s: Session = Depends(session_dep),
):
    svc_change_password(s, uid, p.current_password, p.new_password)
    return ChangePasswordOut(ok=True)


@router.post("/send-password-reset", response_model=PasswordResetOut)
def r_send_password_reset(uid: str = Depends(current_user_id), s: Session = Depends(session_dep)):
    sent = trigger_password_reset(s, uid)
    return PasswordResetOut(sent=sent)


# -------- Avatar upload/remove (NEW) --------

def _guess_ext(name: str) -> str:
    n = (name or "").lower()
    if n.endswith(".png"):  return ".png"
    if n.endswith(".webp"): return ".webp"
    return ".jpg"

@router.post("/avatar", response_model=AvatarOut)
def r_upload_avatar(
    f: UploadFile = File(...),
    uid: str = Depends(current_user_id),
    s: Session = Depends(session_dep),
):
    if not f.content_type or not f.content_type.startswith("image/"):
        raise HTTPException(status_code=400, detail="Please upload an image.")
    data = f.file.read()
    rel = set_user_avatar_from_bytes(s, uid, data, _guess_ext(f.filename or "avatar.jpg"))
    return AvatarOut(avatar_url=abs_media(rel))

@router.delete("/avatar", response_model=AvatarOut)
def r_delete_avatar(
    uid: str = Depends(current_user_id),
    s: Session = Depends(session_dep),
):
    clear_user_avatar(s, uid)
    return AvatarOut(avatar_url=None)
