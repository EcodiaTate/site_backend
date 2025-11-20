# api/auth/routes_email_links.py
from __future__ import annotations

import time
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query, status
from pydantic import BaseModel, EmailStr, Field
from neo4j import Session
from argon2 import PasswordHasher

from site_backend.core.neo_driver import session_dep

router = APIRouter(tags=["auth-email-links"])

# Local helpers (duplicated small bits on purpose – keeps it self-contained)
def _now_s() -> int:
  return int(time.time())

_ph = PasswordHasher()


# ───────────────────────────
# Email verification
# ───────────────────────────

class VerifyEmailOut(BaseModel):
  ok: bool
  email: Optional[EmailStr] = None
  message: str | None = None


@router.get("/verify-email", response_model=VerifyEmailOut)
def r_verify_email(
  token: str = Query(..., min_length=8),
  s: Session = Depends(session_dep),
):
  """
  Public endpoint hit from email links like:
  https://ecodia.au/auth/verify-email?token=...

  It finalises either:
  - change-email flow (pending_email -> email)
  - or initial verification of existing email.
  """
  rec = s.run(
    """
    MATCH (u:User)
    WHERE u.email_verify_token = $token
    RETURN u.id                               AS id,
           toLower(coalesce(u.email,''))      AS email,
           toLower(coalesce(u.pending_email,'')) AS pending_email,
           coalesce(u.email_verify_expires,0) AS exp
    """,
    token=token,
  ).single()

  if not rec:
    raise HTTPException(
      status_code=status.HTTP_400_BAD_REQUEST,
      detail="This verification link is invalid or has already been used.",
    )

  now = _now_s()
  exp = int(rec["exp"] or 0)
  if exp and now > exp:
    raise HTTPException(
      status_code=status.HTTP_400_BAD_REQUEST,
      detail="This verification link has expired. Please request a new one from your account page.",
    )

  new_email = (rec["pending_email"] or rec["email"] or "").strip().lower()
  if not new_email:
    raise HTTPException(
      status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
      detail="No email found to verify for this link.",
    )

  s.run(
    """
    MATCH (u:User {id:$id})
    SET u.email                = $email,
        u.pending_email        = null,
        u.email_verified       = true,
        u.email_verify_token   = null,
        u.email_verify_expires = null,
        u.updated_at           = datetime()
    """,
    id=rec["id"],
    email=new_email,
  )

  return VerifyEmailOut(
    ok=True,
    email=new_email,
    message="Your email has been verified. You can close this tab and continue using Ecodia.",
  )


# ───────────────────────────
# Reset password
# ───────────────────────────

class ResetPasswordIn(BaseModel):
  token: str = Field(min_length=8)
  new_password: str = Field(min_length=8)


class ResetPasswordOut(BaseModel):
  ok: bool
  message: str | None = None


@router.post("/reset-password", response_model=ResetPasswordOut)
def r_reset_password(
  payload: ResetPasswordIn,
  s: Session = Depends(session_dep),
):
  """
  Public endpoint hit from the reset-password page.

  Consumes the reset token, sets a new password_hash, and clears the token.
  """
  rec = s.run(
    """
    MATCH (u:User)
    WHERE u.reset_token = $token
    RETURN u.id                    AS id,
           coalesce(u.reset_expires, 0) AS exp
    """,
    token=payload.token,
  ).single()

  if not rec:
    raise HTTPException(
      status_code=status.HTTP_400_BAD_REQUEST,
      detail="This reset link is invalid or has already been used.",
    )

  now = _now_s()
  exp = int(rec["exp"] or 0)
  if exp and now > exp:
    raise HTTPException(
      status_code=status.HTTP_400_BAD_REQUEST,
      detail="This reset link has expired. Please request a new reset email.",
    )

  # Hash and persist the new password
  new_hash = _ph.hash(payload.new_password)

  s.run(
    """
    MATCH (u:User {id:$id})
    SET u.password_hash = $hash,
        u.reset_token   = null,
        u.reset_expires = null,
        u.updated_at    = datetime()
    """,
    id=rec["id"],
    hash=new_hash,
  )

  return ResetPasswordOut(
    ok=True,
    message="Your password has been updated. You can now sign in with your email and new password.",
  )
