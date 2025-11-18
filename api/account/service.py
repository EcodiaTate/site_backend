from __future__ import annotations

import os
import io
import time
import hashlib
from typing import Optional, Dict, Any
from uuid import uuid4
from datetime import datetime, timezone
from pathlib import Path

from fastapi import HTTPException
from neo4j import Session
from argon2 import PasswordHasher
from argon2.exceptions import VerifyMismatchError

from PIL import Image, ImageOps

from site_backend.core.paths import UPLOAD_ROOT  # must resolve to .../uploads

# =========================================================
# Config
# =========================================================
PUBLIC_BASE_URL = os.getenv("PUBLIC_BASE_URL", "https://ecodia.au").rstrip("/")
VERIFY_EMAIL_PATH = os.getenv("VERIFY_EMAIL_PATH", "/auth/verify-email")
RESET_PASSWORD_PATH = os.getenv("RESET_PASSWORD_PATH", "/auth/reset-password")

# Avatar storage policy
AVATAR_MAX_DIM       = int(os.getenv("AVATAR_MAX_DIM", "1024"))     # px
AVATAR_WEBP_QUALITY  = int(os.getenv("AVATAR_WEBP_QUALITY", "92"))
AVATAR_RETENTION_DAYS = int(os.getenv("AVATAR_RETENTION_DAYS", "14"))

# Paths
AVATAR_DIR: Path = (UPLOAD_ROOT / "avatars")
AVATAR_DIR.mkdir(parents=True, exist_ok=True)

# Security
ph = PasswordHasher()


# =========================================================
# Utilities
# =========================================================

def _now_s() -> int:
    return int(time.time())

def _send_email(to_email: str, subject: str, body: str) -> bool:
    try:
        print(f"[MAIL] To: {to_email}\nSubj: {subject}\n\n{body}\n")
        return True
    except Exception:
        return False

def _normalize_avatar(img: Image.Image) -> Image.Image:
    """Square center-crop, EXIF transpose, RGB, clamp to AVATAR_MAX_DIM."""
    img = ImageOps.exif_transpose(img)

    if img.mode not in ("RGB", "RGBA"):
        img = img.convert("RGB")
    if img.mode == "RGBA":
        bg = Image.new("RGB", img.size, (255, 255, 255))
        bg.paste(img, mask=img.split()[-1])
        img = bg

    w, h = img.size
    side = min(w, h)
    left = max(0, (w - side) // 2)
    top  = max(0, (h - side) // 2)
    img = img.crop((left, top, left + side, top + side))

    if side > AVATAR_MAX_DIM:
        img = img.resize((AVATAR_MAX_DIM, AVATAR_MAX_DIM), Image.LANCZOS)
    return img

def _encode_webp_bytes(img: Image.Image) -> bytes:
    out = io.BytesIO()
    img.save(out, format="WEBP", method=6, quality=AVATAR_WEBP_QUALITY)
    return out.getvalue()

def _sha256(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()

def _avatar_rel_path(sha: str) -> str:
    # /uploads/avatars/aa/bb/<sha>.webp
    return f"/uploads/avatars/{sha[:2]}/{sha[2:4]}/{sha}.webp"

def _avatar_fs_path(sha: str) -> Path:
    return AVATAR_DIR / sha[:2] / sha[2:4] / f"{sha}.webp"

def _ensure_parent(p: Path) -> None:
    p.parent.mkdir(parents=True, exist_ok=True)

def _parse_sha_from_url(url: Optional[str]) -> Optional[str]:
    if not url:
        return None
    try:
        base = url.split("?", 1)[0]
        name = Path(base).name  # <sha>.webp
        sha = name.split(".")[0]
        if len(sha) == 64 and all(c in "0123456789abcdef" for c in sha):
            return sha
    except Exception:
        pass
    return None


# =========================================================
# Account Reads
# =========================================================
def get_me_account(s: Session, uid: str) -> Optional[Dict[str, Any]]:
    cy = """
    MATCH (u:User {id:$uid})
    RETURN u.id AS id,
           toLower(coalesce(u.email,'')) AS email,
           toLower(coalesce(u.role,'public')) AS role,
           coalesce(u.display_name, NULL) AS display_name,
           coalesce(u.email_verified, false) AS email_verified,
           coalesce(u.avatar_url, NULL) AS avatar_url,

           // legal flags
           coalesce(u.legal_onboarding_complete,false) AS legal_onboarding_complete,
           coalesce(u.tos_version, NULL)               AS tos_version,
           coalesce(u.tos_accepted_at, NULL)           AS tos_accepted_at,
           coalesce(u.privacy_accepted_at, NULL)       AS privacy_accepted_at,
           coalesce(u.over18_confirmed, NULL)          AS over18_confirmed,
           coalesce(u.birth_year, NULL)                AS birth_year
    """
    rec = s.run(cy, uid=uid).single()
    if not rec:
        return None
    return {
        "id": rec["id"],
        "email": rec["email"],
        "display_name": rec["display_name"],
        "role": rec["role"],
        "email_verified": bool(rec["email_verified"]),
        "avatar_url": rec["avatar_url"],

        "legal_onboarding_complete": bool(rec["legal_onboarding_complete"]),
        "tos_version": rec["tos_version"],
        "tos_accepted_at": rec["tos_accepted_at"],
        "privacy_accepted_at": rec["privacy_accepted_at"],
        "over18_confirmed": rec["over18_confirmed"],
        "birth_year": rec["birth_year"],
    }


def get_public_profile(s: Session, uid: str) -> Optional[Dict[str, Any]]:
    """
    Minimal public shape for the avatar endpoint and public displays.
    """
    cy = """
    MATCH (u:User {id:$uid})
    RETURN coalesce(u.display_name, NULL) AS display_name,
           coalesce(u.avatar_url, NULL)    AS avatar_url
    """
    rec = s.run(cy, uid=uid).single()
    if not rec:
        return None
    return {
        "display_name": rec["display_name"],
        "avatar_url": rec["avatar_url"],
    }


# =========================================================
# Account Mutations (email/name/password)
# =========================================================

def update_display_name(s: Session, uid: str, display_name: str) -> None:
    if not display_name:
        raise HTTPException(status_code=400, detail="Display name required")
    cy = """
    MATCH (u:User {id:$uid})
    SET u.display_name = $display_name,
        u.updated_at = datetime()
    RETURN u
    """
    if not s.run(cy, uid=uid, display_name=display_name).single():
        raise HTTPException(status_code=404, detail="User not found")

def begin_change_email(s: Session, uid: str, new_email: str) -> None:
    new_email_l = new_email.lower().strip()
    if not new_email_l:
        raise HTTPException(status_code=400, detail="Email required")

    # Soft uniqueness check (optional: enforce)
    s.run("MATCH (x:User {email:$e}) RETURN x.id AS id LIMIT 1", e=new_email_l).single()

    token = str(uuid4())
    exp = _now_s() + 60 * 60 * 24  # 24h

    cy = """
    MATCH (u:User {id:$uid})
    SET u.pending_email = $new_email,
        u.email_verify_token = $token,
        u.email_verify_expires = $exp,
        u.email_verified = false,
        u.updated_at = datetime()
    RETURN u.email AS current_email, u.pending_email AS pending_email
    """
    rec = s.run(cy, uid=uid, new_email=new_email_l, token=token, exp=exp).single()
    if not rec:
        raise HTTPException(status_code=404, detail="User not found")

    link = f"{PUBLIC_BASE_URL}{VERIFY_EMAIL_PATH}?token={token}"
    subj = "Confirm your new Ecodia email"
    body = (
        f"Hey!\n\nWe received a request to change your Ecodia email to {new_email_l}.\n"
        f"Please confirm by clicking the link below:\n\n{link}\n\n"
        f"This link expires in 24 hours. If you didn’t request this, ignore this email."
    )
    if not _send_email(new_email_l, subj, body):
        raise HTTPException(status_code=500, detail="Could not send verification email")

def trigger_verify_email(s: Session, uid: str) -> bool:
    row = s.run(
        "MATCH (u:User {id:$uid}) "
        "RETURN toLower(coalesce(u.pending_email,'')) AS pending_email, "
        "       toLower(coalesce(u.email,'')) AS email",
        uid=uid,
    ).single()
    if not row:
        raise HTTPException(status_code=404, detail="User not found")

    dest = (row["pending_email"] or row["email"] or "").strip().lower()
    if not dest:
        raise HTTPException(status_code=400, detail="No email on file to verify")

    token = str(uuid4())
    exp = _now_s() + 60 * 60 * 24

    s.run(
        "MATCH (u:User {id:$uid}) "
        "SET u.email_verify_token = $token, "
        "    u.email_verify_expires = $exp, "
        "    u.email_verified = false, "
        "    u.updated_at = datetime()",
        uid=uid, token=token, exp=exp,
    )

    link = f"{PUBLIC_BASE_URL}{VERIFY_EMAIL_PATH}?token={token}"
    subj = "Verify your Ecodia email"
    body = f"Hi!\n\nPlease verify your email for Ecodia by clicking the link below:\n\n{link}\n\nThis link expires in 24 hours."
    return _send_email(dest, subj, body)

def change_password(s: Session, uid: str, current_password: str, new_password: str) -> None:
    rec = s.run("MATCH (u:User {id:$uid}) RETURN u.password_hash AS hash", uid=uid).single()
    if not rec:
        raise HTTPException(status_code=404, detail="User not found")

    hash_ = rec["hash"]
    if hash_:
        try:
            ph.verify(hash_, current_password)
        except VerifyMismatchError:
            raise HTTPException(status_code=401, detail="Current password is incorrect")
    elif current_password:
        raise HTTPException(status_code=400, detail="Password change not allowed for SSO-only account")

    new_hash = ph.hash(new_password)
    s.run(
        "MATCH (u:User {id:$uid}) "
        "SET u.password_hash = $hash, u.updated_at = datetime()",
        uid=uid, hash=new_hash,
    )

def trigger_password_reset(s: Session, uid: str) -> bool:
    rec = s.run("MATCH (u:User {id:$uid}) RETURN toLower(coalesce(u.email,'')) AS email", uid=uid).single()
    if not rec:
        raise HTTPException(status_code=404, detail="User not found")

    email = (rec["email"] or "").strip().lower()
    if not email:
        return False

    token = str(uuid4())
    exp = _now_s() + 60 * 60 * 2  # 2h

    s.run(
        "MATCH (u:User {id:$uid}) "
        "SET u.reset_token = $token, u.reset_expires = $exp, u.updated_at = datetime()",
        uid=uid, token=token, exp=exp,
    )

    link = f"{PUBLIC_BASE_URL}{RESET_PASSWORD_PATH}?token={token}"
    subj = "Reset your Ecodia password"
    body = (
        f"We received a request to reset your Ecodia password.\n\n"
        f"Reset it here:\n{link}\n\n"
        f"This link expires in 2 hours. If you didn’t request this, you can ignore this email."
    )
    return _send_email(email, subj, body)


# =========================================================
# Avatar: Content-Addressed Storage + Refcounts + GC
# =========================================================

def _upsert_blob_inc_ref(s: Session, sha: str, size_bytes: int) -> None:
    s.run(
        """
        MERGE (b:AvatarBlob {sha:$sha})
        ON CREATE SET b.bytes = $bytes,
                      b.refcount = 1,
                      b.created_at = datetime(),
                      b.last_ref_at = datetime(),
                      b.purge_at = null
        ON MATCH SET b.bytes = coalesce(b.bytes, $bytes),
                     b.refcount = coalesce(b.refcount,0) + 1,
                     b.last_ref_at = datetime(),
                     b.purge_at = null
        """,
        sha=sha, bytes=size_bytes,
    )

def _dec_ref_and_maybe_schedule_purge(s: Session, sha: str) -> None:
    s.run(
        """
        MATCH (b:AvatarBlob {sha:$sha})
        SET b.refcount = coalesce(b.refcount,0) - 1
        WITH b
        WHERE b.refcount <= 0
        SET b.refcount = 0,
            b.purge_at = datetime() + duration({days:$days})
        """,
        sha=sha, days=AVATAR_RETENTION_DAYS,
    )

def _store_blob_if_absent(sha: str, data: bytes) -> None:
    p = _avatar_fs_path(sha)
    if not p.exists():
        _ensure_parent(p)
        with open(p, "wb") as f:
            f.write(data)

def _delete_blob_file(sha: str) -> None:
    p = _avatar_fs_path(sha)
    try:
        p.unlink()
        try:
            p.parent.rmdir()
            p.parent.parent.rmdir()
        except Exception:
            pass
    except FileNotFoundError:
        pass

def _build_avatar_url(sha: str, rev: str) -> str:
    rel = _avatar_rel_path(sha)
    return f"{rel}?v={rev}"

def set_user_avatar_from_bytes(s: Session, user_id: str, file_bytes: bytes, ext_hint: str = ".jpg") -> str:
    """
    Process, content-hash, store if new, track refcounts, update user with cache-busting avatar_url.
    Returns the (relative) avatar_url with ?v=rev.
    """
    try:
        img = Image.open(io.BytesIO(file_bytes))
    except Exception as e:
        raise HTTPException(status_code=400, detail="Invalid image") from e

    img = _normalize_avatar(img)
    data = _encode_webp_bytes(img)
    sha = _sha256(data)
    size = len(data)

    _store_blob_if_absent(sha, data)

    row = s.run("MATCH (u:User {id:$uid}) RETURN coalesce(u.avatar_sha, NULL) AS old_sha",
                uid=user_id).single()
    old_sha = row["old_sha"] if row else None

    _upsert_blob_inc_ref(s, sha, size)

    if old_sha and old_sha != sha:
        _dec_ref_and_maybe_schedule_purge(s, old_sha)

    rev = sha[:8]
    avatar_url_rel = _build_avatar_url(sha, rev)
    s.run(
        """
        MATCH (u:User {id:$uid})
        SET u.avatar_sha = $sha,
            u.avatar_url = $url,
            u.avatar_rev = $rev,
            u.avatar_updated_at = timestamp(),
            u.updated_at = datetime()
        """,
        uid=user_id, sha=sha, url=avatar_url_rel, rev=rev,
    )
    return avatar_url_rel

def clear_user_avatar(s: Session, user_id: str) -> None:
    row = s.run("MATCH (u:User {id:$uid}) RETURN coalesce(u.avatar_sha,NULL) AS sha", uid=user_id).single()
    old_sha = row["sha"] if row else None

    s.run(
        """
        MATCH (u:User {id:$uid})
        REMOVE u.avatar_sha
        REMOVE u.avatar_url
        SET u.avatar_rev = $rev,
            u.avatar_updated_at = timestamp(),
            u.updated_at = datetime()
        """,
        uid=user_id, rev=str(int(time.time())),
    )

    if old_sha:
        _dec_ref_and_maybe_schedule_purge(s, old_sha)

def gc_avatar_blobs(s: Session) -> int:
    rows = s.run(
        """
        MATCH (b:AvatarBlob)
        WHERE coalesce(b.refcount,0) = 0 AND b.purge_at IS NOT NULL AND b.purge_at <= datetime()
        RETURN b.sha AS sha
        """
    ).data()

    count = 0
    for r in rows:
        sha = r["sha"]
        _delete_blob_file(sha)
        s.run("MATCH (b:AvatarBlob {sha:$sha}) DETACH DELETE b", sha=sha)
        count += 1
    return count

def backfill_user_avatar_sha(s: Session, user_id: str) -> None:
    row = s.run(
        "MATCH (u:User {id:$uid}) "
        "RETURN coalesce(u.avatar_sha,NULL) AS sha, coalesce(u.avatar_url,NULL) AS url",
        uid=user_id,
    ).single()
    if not row:
        return
    if row["sha"]:
        return
    sha = _parse_sha_from_url(row["url"])
    if not sha:
        return
    p = _avatar_fs_path(sha)
    size = p.stat().st_size if p.exists() else 0
    _upsert_blob_inc_ref(s, sha, size)
    s.run(
        "MATCH (u:User {id:$uid}) SET u.avatar_sha = $sha, u.avatar_rev = coalesce(u.avatar_rev, substring($sha,0,8))",
        uid=user_id, sha=sha,
    )