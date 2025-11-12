from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, status, UploadFile, File
from pydantic import BaseModel, EmailStr, Field
from typing import Optional, Tuple, Dict
from neo4j import Session

import io, os, time, hashlib
from pathlib import Path
from PIL import Image

# HEIC/AVIF support (safe no-op if not installed)
try:
    from pillow_heif import register_heif
    register_heif()
except Exception:
    pass

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
    avatar_url: Optional[str] = None  # absolute (via abs_media)

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

# -------------------- Constants / helpers --------------------

# GCSFuse mount root; app serves /uploads -> {UPLOAD_ROOT}/uploads
UPLOAD_ROOT = Path(os.getenv("UPLOAD_ROOT", "/data/uploads"))
MEDIA_UPLOADS_ROOT = UPLOAD_ROOT / "uploads"           # .../uploads
AVATARS_ROOT = MEDIA_UPLOADS_ROOT / "avatars"          # .../uploads/avatars

def _ensure_dirs(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)

def _normalize_orientation(im: Image.Image) -> Image.Image:
    try:
        exif = im.getexif()
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

def _atomic_write(path: Path, data: bytes) -> None:
    _ensure_dirs(path.parent)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with open(tmp, "wb") as f:
        f.write(data)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)

def _guess_orig_ext(fmt: Optional[str], content_type: Optional[str], filename: Optional[str]) -> str:
    ct = (content_type or "").lower()
    fn = (filename or "").lower()
    f = (fmt or "").upper()
    # prefer explicit types
    if "png" in ct or fn.endswith(".png") or f == "PNG":
        return ".png"
    if "webp" in ct or fn.endswith(".webp") or f == "WEBP":
        return ".webp"
    if "jpeg" in ct or "jpg" in ct or fn.endswith(".jpg") or fn.endswith(".jpeg") or f in ("JPEG", "JPG"):
        return ".jpg"
    # HEIC/AVIF → store original as .jpg alongside webp
    if "heic" in ct or "heif" in ct or "avif" in ct or f in ("HEIC", "HEIF", "AVIF"):
        return ".jpg"
    return ".jpg"

def _to_webp_bytes(im: Image.Image) -> bytes:
    im = _normalize_orientation(im)
    if im.mode not in ("RGB", "RGBA"):
        im = im.convert("RGB")
    buf = io.BytesIO()
    im.save(buf, "WEBP", quality=85, method=6)
    return buf.getvalue()

def _encode_as(im: Image.Image, ext: str) -> bytes:
    im = _normalize_orientation(im)
    if ext == ".png":
        if im.mode not in ("RGB", "RGBA"):
            im = im.convert("RGB")
        buf = io.BytesIO()
        im.save(buf, "PNG", optimize=True)
        return buf.getvalue()
    # default to JPEG
    if im.mode not in ("RGB", "RGBA"):
        im = im.convert("RGB")
    buf = io.BytesIO()
    im.save(buf, "JPEG", quality=90, optimize=True)
    return buf.getvalue()

def _shard_dir_for_sha(sha: str) -> Path:
    aa, bb = sha[:2], sha[2:4]
    return AVATARS_ROOT / aa / bb

def _build_avatar_url(sha: str, rev: str) -> str:
    aa, bb = sha[:2], sha[2:4]
    return f"/uploads/avatars/{aa}/{bb}/{sha}.webp?v={rev}"

# -------------------- Routes --------------------

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

# -------- Avatar upload/remove (HEIC-safe) --------

@router.post("/avatar", response_model=AvatarOut)
def r_upload_avatar(
    f: UploadFile = File(...),
    uid: str = Depends(current_user_id),
    s: Session = Depends(session_dep),
):
    # Basic type guard
    if not f.content_type or not f.content_type.startswith("image/"):
        raise HTTPException(status_code=400, detail="Please upload an image.")

    # 1) read bytes and decode any format (HEIC/AVIF supported via pillow-heif)
    data = f.file.read()
    try:
        im = Image.open(io.BytesIO(data))
        im.load()  # force decode
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Could not read image: {e}")

    # 2) produce canonical WEBP
    try:
        webp_bytes = _to_webp_bytes(im)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Could not convert to WEBP: {e}")

    # 3) sha from webp (stable key) + rev for cache busting
    sha = hashlib.sha256(webp_bytes).hexdigest()
    rev = str(int(time.time()))

    # 4) compute shard paths and write atomically
    shard_dir = _shard_dir_for_sha(sha)
    webp_path = shard_dir / f"{sha}.webp"

    # original ext (store alongside webp: .jpg for HEIC/AVIF)
    orig_ext = _guess_orig_ext(getattr(im, "format", None), f.content_type, f.filename)
    orig_path = shard_dir / f"{sha}{orig_ext}"

    try:
        # write original (re-encode if necessary)
        orig_bytes = _encode_as(im, orig_ext)
        _atomic_write(orig_path, orig_bytes)
        # write webp
        _atomic_write(webp_path, webp_bytes)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to store avatar: {e}")

    # 5) commit DB only after successful writes
    cy = """
    MATCH (u:User {id:$uid})
    SET   u.avatar_sha = $sha,
          u.avatar_rev = $rev,
          u.avatar_url = NULL,
          u.avatar_updated_at = datetime()
    RETURN u.avatar_sha AS avatar_sha, u.avatar_rev AS avatar_rev
    """
    rec = s.run(cy, {"uid": uid, "sha": sha, "rev": rev}).single()
    if not rec:
        # roll forward: keep files, but signal user not found
        raise HTTPException(status_code=404, detail="User not found")

    url = _build_avatar_url(sha, rev)
    return AvatarOut(avatar_url=abs_media(url))

@router.delete("/avatar", response_model=AvatarOut)
def r_delete_avatar(
    uid: str = Depends(current_user_id),
    s: Session = Depends(session_dep),
):
    clear_user_avatar(s, uid)
    return AvatarOut(avatar_url=None)
