# api/me/routes_me_account.py
from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, status, UploadFile, File
from pydantic import BaseModel, EmailStr, Field
from typing import Optional
from neo4j import Session

import io
import os
import logging

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
    set_user_avatar_from_bytes,  # keep existing storage path/versioning logic
    clear_user_avatar,
)

router = APIRouter(prefix="/me/account", tags=["account"])
log = logging.getLogger("account.avatar")

# ==================== Schemas ====================

class MeAccount(BaseModel):
    id: str
    email: EmailStr
    display_name: str | None = None
    role: str = Field(default="public")
    email_verified: bool = False
    avatar_url: Optional[str] = None  # absolute via abs_media

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


# ==================== Upload helpers ====================

# Max avatar size (MB) -> bytes (defaults to 15 MB)
_AVATAR_MAX_MB = float(os.getenv("AVATAR_MAX_MB", "15"))
_AVATAR_MAX_BYTES = int(_AVATAR_MAX_MB * 1024 * 1024)

# Content-type / extension sets
_HEIC_CTS = {"image/heic", "image/heif", "image/avif"}
_BASIC_EXTS = (".png", ".jpg", ".jpeg", ".webp")
_HEIC_EXTS = (".heic", ".heif", ".avif")

def _register_heif_plugins() -> bool:
    """
    Try all known pillow-heif registration entry points across versions.
    Returns True if any registration succeeded.
    """
    try:
        import pillow_heif  # type: ignore
    except Exception as e:
        log.info("pillow-heif not importable: %s", e)
        return False

    registered = False
    for fn_name in ("register_heif", "register_heif_opener", "register_avif_opener"):
        try:
            fn = getattr(pillow_heif, fn_name, None)
            if callable(fn):
                fn()
                registered = True
                log.info("pillow-heif: %s() succeeded", fn_name)
        except Exception as e:
            log.warning("pillow-heif: %s() failed: %s", fn_name, e)
    if not registered:
        log.warning("pillow-heif present but no registration function succeeded")
    return registered

def _lazy_pillow():
    """
    Import PIL lazily so the app still starts if Pillow isn't installed.
    Also attempt to register HEIF/AVIF openers if pillow-heif is present.
    Returns the PIL Image module or None.
    """
    try:
        from PIL import Image as PILImage  # type: ignore
    except Exception as e:
        log.info("Pillow not importable: %s", e)
        return None
    _register_heif_plugins()
    return PILImage

def _normalize_orientation_pil(im):
    # EXIF orientation normalization; safe no-op if no EXIF
    try:
        exif = getattr(im, "getexif", lambda: {})()
        orientation = exif.get(274)
        if orientation == 3:
            return im.rotate(180, expand=True)
        if orientation == 6:
            return im.rotate(270, expand=True)
        if orientation == 8:
            return im.rotate(90, expand=True)
    except Exception:
        pass
    return im

def _to_webp_bytes_pil(PILImage, im) -> bytes:
    im = _normalize_orientation_pil(im)
    if im.mode not in ("RGB", "RGBA"):
        im = im.convert("RGB")
    buf = io.BytesIO()
    im.save(buf, "WEBP", quality=85, method=6)
    return buf.getvalue()

def _guess_ext_from_ct_or_name(content_type: Optional[str], filename: Optional[str]) -> str:
    ct = (content_type or "").lower()
    fn = (filename or "").lower()
    if "png" in ct or fn.endswith(".png"):
        return ".png"
    if "webp" in ct or fn.endswith(".webp"):
        return ".webp"
    if "jpeg" in ct or "jpg" in ct or fn.endswith(".jpg") or fn.endswith(".jpeg"):
        return ".jpg"
    if fn.endswith(_HEIC_EXTS):
        return ".heic"  # marker; we won't passthrough this without conversion
    return ".jpg"


# ==================== Routes ====================

@router.get("", response_model=MeAccount)
def r_get_me_account(uid: str = Depends(current_user_id), s: Session = Depends(session_dep)):
    acc = get_me_account(s, uid)
    if not acc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="User not found")
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


# -------- Avatar upload/remove (HEIC-safe, tolerant, lazy Pillow) --------
@router.post("/avatar", response_model=AvatarOut)
def r_upload_avatar(
    f: UploadFile = File(...),
    uid: str = Depends(current_user_id),
    s: Session = Depends(session_dep),
):
    # Tolerant: allow empty/odd content-types if filename looks like an image
    ct = (f.content_type or "").lower()
    fn = (f.filename or "").lower()
    looks_like_image = (
        ct.startswith("image/")
        or fn.endswith(_BASIC_EXTS)
        or fn.endswith(_HEIC_EXTS)
    )
    if not looks_like_image:
        raise HTTPException(status_code=400, detail="Please upload an image (PNG/JPG/WebP/HEIC/AVIF).")

    data = f.file.read()
    if not data:
        raise HTTPException(status_code=400, detail="Empty upload.")
    if len(data) > _AVATAR_MAX_BYTES:
        raise HTTPException(
            status_code=413,
            detail=f"Image too large (max {int(_AVATAR_MAX_MB)} MB).",
        )

    # Try Pillow first regardless of provided content-type.
    PILImage = _lazy_pillow()
    if PILImage is not None:
        try:
            im = PILImage.open(io.BytesIO(data))
            im.load()  # force decode; works for HEIC/AVIF if pillow-heif registered
        except Exception as e:
            # If decode failed, fall back to passthrough for basic types; otherwise explain.
            ext = _guess_ext_from_ct_or_name(ct, f.filename)
            if ext in (".jpg", ".png", ".webp"):
                rel = set_user_avatar_from_bytes(s, uid, data, ext)
                return AvatarOut(avatar_url=abs_media(rel))
            if ext in (".heic",):
                raise HTTPException(
                    status_code=415,
                    detail="HEIC/AVIF not supported by the running server. Ensure pillow-heif is registered and libheif is available; then restart the server."
                )
            raise HTTPException(status_code=415, detail=f"Unsupported image format: {e}")

        # With a decoded image, normalize and convert to WEBP
        try:
            webp_bytes = _to_webp_bytes_pil(PILImage, im)
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"Could not convert to WEBP: {e}")

        rel = set_user_avatar_from_bytes(s, uid, webp_bytes, ".webp")
        return AvatarOut(avatar_url=abs_media(rel))

    # No Pillow available → allow passthrough for PNG/JPG/WebP; block HEIC/AVIF with a clear message.
    if ct in _HEIC_CTS or fn.endswith(_HEIC_EXTS):
        raise HTTPException(
            status_code=415,
            detail="HEIC/AVIF not supported on this server. Install Pillow and pillow-heif (plus libheif on Linux) and restart."
        )
    ext = _guess_ext_from_ct_or_name(ct, f.filename)
    if ext not in (".jpg", ".png", ".webp"):
        raise HTTPException(status_code=415, detail=f"Unsupported image type: {ct or f.filename or 'unknown'}")

    rel = set_user_avatar_from_bytes(s, uid, data, ext)
    return AvatarOut(avatar_url=abs_media(rel))


@router.delete("/avatar", response_model=AvatarOut)
def r_delete_avatar(
    uid: str = Depends(current_user_id),
    s: Session = Depends(session_dep),
):
    clear_user_avatar(s, uid)
    return AvatarOut(avatar_url=None)
