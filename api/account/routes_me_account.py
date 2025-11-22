# api/me/routes_me_account.py
from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, status, UploadFile, File
from pydantic import BaseModel, EmailStr, Field
from typing import Optional
from neo4j import Session

import io
import os
import logging
import datetime as dt

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
log = logging.getLogger("account.avatar")

ALLOWED_ROLES: set[str] = {"youth", "business", "creative", "partner", "public"}

# ==================== Schemas ====================


class MeAccount(BaseModel):
    id: str
    email: EmailStr
    display_name: str | None = None

    # Primary routing / experience role
    role: str = Field(default="public")

    email_verified: bool = False
    avatar_url: Optional[str] = None  # absolute via abs_media

    # Does this account have a local password hash?
    has_password: bool = False

    # ── Legal flags (read-only) ───────────────────────────────────
    legal_onboarding_complete: bool | None = None
    tos_version: str | None = None
    tos_accepted_at: str | None = None
    privacy_accepted_at: str | None = None
    over18_confirmed: bool | None = None
    birth_year: int | None = None


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


# ── Legal onboarding I/O ─────────────────────────────────────────


class LegalStatusOut(BaseModel):
    legal_onboarding_complete: bool
    tos_version: str | None = None
    tos_accepted_at: str | None = None
    privacy_accepted_at: str | None = None
    over18_confirmed: bool | None = None
    birth_year: int | None = None


class LegalAcceptIn(BaseModel):
    agreed_tos: bool
    agreed_privacy: bool
    tos_version: str | None = None
    is_adult: bool | None = None
    birth_year: int | None = None


class LegalAcceptOut(BaseModel):
    ok: bool = True
    legal_onboarding_complete: bool = True


class CompleteOnboardingIn(LegalAcceptIn):
    """
    Used by the new /account setup flow to atomically:
    - set / confirm legal flags
    - set primary role
    """
    role: str


# ==================== Upload helpers ====================

_AVATAR_MAX_MB = float(os.getenv("AVATAR_MAX_MB", "15"))
_AVATAR_MAX_BYTES = int(_AVATAR_MAX_MB * 1024 * 1024)

_HEIC_CTS = {"image/heic", "image/heif", "image/avif"}
_BASIC_EXTS = (".png", ".jpg", ".jpeg", ".webp")
_HEIC_EXTS = (".heic", ".heif", ".avif")


def _register_heif_plugins() -> bool:
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
    try:
        from PIL import Image as PILImage  # type: ignore
    except Exception as e:
        log.info("Pillow not importable: %s", e)
        return None
    _register_heif_plugins()
    return PILImage


def _normalize_orientation_pil(im):
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
    if "jpeg" in ct or "jpg" in ct or fn.endswith((".jpg", ".jpeg")):
        return ".jpg"
    if fn.endswith(_HEIC_EXTS):
        return ".heic"
    return ".jpg"


# ==================== Internal helpers ====================


def _persist_legal(
    s: Session,
    uid: str,
    p: LegalAcceptIn,
    *,
    role: Optional[str] = None,
) -> None:
    """Core logic to set legal flags and optionally a primary role."""

    if not (p.agreed_tos and p.agreed_privacy):
        raise HTTPException(
            status_code=400,
            detail="You must agree to Terms and Privacy Policy.",
        )

    now = dt.datetime.now(dt.timezone.utc)
    now_iso = now.isoformat()
    current_year = now.year

    tos_version = (p.tos_version or os.getenv("DEFAULT_TOS_VERSION", "v1")).strip() or "v1"

    # determine adulthood (either explicit is_adult OR infer from birth_year)
    over18: bool | None = None
    if p.is_adult is not None:
        over18 = bool(p.is_adult)
    elif p.birth_year is not None and 1900 <= p.birth_year <= current_year:
        over18 = (current_year - int(p.birth_year)) >= 18

    if p.birth_year is not None and not (1900 <= p.birth_year <= current_year):
        raise HTTPException(status_code=400, detail="Invalid birth year.")

    params: dict = {
        "uid": uid,
        "tos_version": tos_version,
        "now_iso": now_iso,
        "over18": over18,
        "birth_year": int(p.birth_year) if p.birth_year is not None else None,
    }

    cypher = """
        MATCH (u:User {id:$uid})
        SET u.tos_version = $tos_version,
            u.tos_accepted_at = $now_iso,
            u.privacy_accepted_at = $now_iso,
            u.over18_confirmed = coalesce($over18, u.over18_confirmed),
            u.birth_year = coalesce($birth_year, u.birth_year),
            u.legal_onboarding_complete = true,
            u.updated_at = datetime()
    """

    if role is not None:
        cypher += ", u.role = $role"
        params["role"] = role

    s.run(cypher, **params)


# ==================== Routes ====================


@router.get("", response_model=MeAccount)
def r_get_me_account(
    uid: str = Depends(current_user_id),
    s: Session = Depends(session_dep),
):
    acc = get_me_account(s, uid)
    if not acc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="User not found")

    acc["avatar_url"] = abs_media(acc.get("avatar_url"))

    # ensure role always has a sensible default
    acc.setdefault("role", "public")

    # surface legal flags (safe defaults)
    acc.setdefault(
        "legal_onboarding_complete",
        bool(acc.get("legal_onboarding_complete") or False),
    )
    acc.setdefault("tos_version", acc.get("tos_version"))
    acc.setdefault("tos_accepted_at", acc.get("tos_accepted_at"))
    acc.setdefault("privacy_accepted_at", acc.get("privacy_accepted_at"))
    acc.setdefault("over18_confirmed", acc.get("over18_confirmed"))
    acc.setdefault("birth_year", acc.get("birth_year"))

    return acc


@router.get("/legal-status", response_model=LegalStatusOut)
def r_get_legal_status(
    uid: str = Depends(current_user_id),
    s: Session = Depends(session_dep),
):
    row = s.run(
        """
        MATCH (u:User {id:$uid})
        RETURN coalesce(u.legal_onboarding_complete,false) AS complete,
               coalesce(u.tos_version, NULL)              AS tos_version,
               toString(u.tos_accepted_at)                AS tos_at,
               toString(u.privacy_accepted_at)            AS priv_at,
               coalesce(u.over18_confirmed, NULL)         AS over18,
               coalesce(u.birth_year, NULL)               AS birth_year
        """,
        uid=uid,
    ).single()
    if not row:
        raise HTTPException(status_code=404, detail="User not found")

    return {
        "legal_onboarding_complete": bool(row["complete"]),
        "tos_version": row["tos_version"],
        "tos_accepted_at": row["tos_at"],
        "privacy_accepted_at": row["priv_at"],
        "over18_confirmed": row["over18"],
        "birth_year": row["birth_year"],
    }


@router.post("/legal-accept", response_model=LegalAcceptOut)
def r_post_legal_accept(
    p: LegalAcceptIn,
    uid: str = Depends(current_user_id),
    s: Session = Depends(session_dep),
):
    """
    Legacy / existing route – just accepts legal flags.
    The new unified onboarding flow will usually use /complete-onboarding instead.
    """
    _persist_legal(s, uid, p, role=None)
    return LegalAcceptOut(ok=True, legal_onboarding_complete=True)


@router.post("/complete-onboarding", response_model=LegalAcceptOut)
def r_post_complete_onboarding(
    p: CompleteOnboardingIn,
    uid: str = Depends(current_user_id),
    s: Session = Depends(session_dep),
):
    """
    New unified endpoint for the /account first-time card.

    Does, in one go:
    - validates role against ALLOWED_ROLES
    - sets role on the User node
    - sets legal flags, timestamps, and legal_onboarding_complete
    """
    role = (p.role or "").strip().lower()
    if role not in ALLOWED_ROLES:
        raise HTTPException(status_code=400, detail="Invalid role.")

    _persist_legal(s, uid, p, role=role)
    return LegalAcceptOut(ok=True, legal_onboarding_complete=True)


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
def r_send_verify_email(
    uid: str = Depends(current_user_id),
    s: Session = Depends(session_dep),
):
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
def r_send_password_reset(
    uid: str = Depends(current_user_id),
    s: Session = Depends(session_dep),
):
    sent = trigger_password_reset(s, uid)
    return PasswordResetOut(sent=sent)


# -------- Avatar upload/remove (HEIC-safe, tolerant, lazy Pillow) --------


@router.post("/avatar", response_model=AvatarOut)
def r_upload_avatar(
    f: UploadFile = File(...),
    uid: str = Depends(current_user_id),
    s: Session = Depends(session_dep),
):
    ct = (f.content_type or "").lower()
    fn = (f.filename or "").lower()
    looks_like_image = (
        ct.startswith("image/") or fn.endswith(_BASIC_EXTS) or fn.endswith(_HEIC_EXTS)
    )
    if not looks_like_image:
        raise HTTPException(
            status_code=400,
            detail="Please upload an image (PNG/JPG/WebP/HEIC/AVIF).",
        )

    data = f.file.read()
    if not data:
        raise HTTPException(status_code=400, detail="Empty upload.")
    if len(data) > _AVATAR_MAX_BYTES:
        raise HTTPException(
            status_code=413,
            detail=f"Image too large (max {int(_AVATAR_MAX_MB)} MB).",
        )

    PILImage = _lazy_pillow()
    if PILImage is not None:
        try:
            im = PILImage.open(io.BytesIO(data))
            im.load()
        except Exception as e:
            ext = _guess_ext_from_ct_or_name(ct, f.filename)
            if ext in (".jpg", ".png", ".webp"):
                rel = set_user_avatar_from_bytes(s, uid, data, ext)
                return AvatarOut(avatar_url=abs_media(rel))
            if ext in (".heic",):
                raise HTTPException(
                    status_code=415,
                    detail=(
                        "HEIC/AVIF not supported by the running server. Ensure pillow-heif "
                        "is registered and libheif is available; then restart the server."
                    ),
                )
            raise HTTPException(status_code=415, detail=f"Unsupported image format: {e}")

        try:
            from PIL import Image as PIL  # type: ignore

            im = im.convert("RGB")
            buf = io.BytesIO()
            im.save(buf, "WEBP", quality=85, method=6)
            webp_bytes = buf.getvalue()
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"Could not convert to WEBP: {e}")

        rel = set_user_avatar_from_bytes(s, uid, webp_bytes, ".webp")
        return AvatarOut(avatar_url=abs_media(rel))

    if ct in _HEIC_CTS or fn.endswith(_HEIC_EXTS):
        raise HTTPException(
            status_code=415,
            detail=(
                "HEIC/AVIF not supported on this server. Install Pillow and pillow-heif "
                "(plus libheif on Linux) and restart."
            ),
        )
    ext = _guess_ext_from_ct_or_name(ct, f.filename)
    if ext not in (".jpg", ".png", ".webp"):
        raise HTTPException(
            status_code=415,
            detail=f"Unsupported image type: {ct or f.filename or 'unknown'}",
        )

    rel = set_user_avatar_from_bytes(s, uid, data, ext)
    return AvatarOut(avatar_url=abs_media(rel))


@router.delete("/avatar", response_model=AvatarOut)
def r_delete_avatar(
    uid: str = Depends(current_user_id),
    s: Session = Depends(session_dep),
):
    clear_user_avatar(s, uid)
    return AvatarOut(avatar_url=None)
