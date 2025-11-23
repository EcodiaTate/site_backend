# site_backend/api/routers/market_connect.py
from __future__ import annotations

import os
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, status
from neo4j import Session  # type: ignore
from pydantic import BaseModel, HttpUrl

from site_backend.core.neo_driver import session_dep
from site_backend.core.user_guard import current_user_id

# Stripe config
try:
    import stripe  # type: ignore
except ImportError:  # pragma: no cover
    stripe = None  # type: ignore

STRIPE_SECRET_KEY = os.getenv("STRIPE_SECRET_KEY")
PUBLIC_ECODIA_URL = os.getenv("PUBLIC_ECODIA_URL", "http://localhost:3001").rstrip("/")

router = APIRouter(
    prefix="/market/connect",
    tags=["market_connect"],
)


# ============================================================
# Models
# ============================================================

class ConnectOnboardOut(BaseModel):
  url: HttpUrl


class ConnectStatusOut(BaseModel):
  has_store: bool
  stripe_account_id: Optional[str] = None
  payouts_enabled: bool = False
  charges_enabled: bool = False
  details_submitted: bool = False
  # Basic hints, not full Stripe object
  requirements_currently_due: Optional[list[str]] = None
  requirements_disabled_reason: Optional[str] = None


# ============================================================
# Helpers
# ============================================================

def _require_stripe() -> None:
  if stripe is None:
    raise HTTPException(
      status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
      detail="Stripe SDK not installed on server.",
    )
  if not STRIPE_SECRET_KEY:
    raise HTTPException(
      status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
      detail="Stripe is not configured (STRIPE_SECRET_KEY missing).",
    )
  stripe.api_key = STRIPE_SECRET_KEY


def _get_store_for_user(session: Session, uid: str) -> Optional[dict]:
  row = session.run(
    """
    MATCH (u:User {id:$uid})-[:OWNS_UPCYCLING_STORE]->(s:UpcyclingStore)
    RETURN s.id AS store_id, s.stripe_account_id AS stripe_account_id
    """,
    uid=uid,
  ).single()
  return row.data() if row else None


# ============================================================
# Routes
# ============================================================

@router.post(
  "/onboard",
  response_model=ConnectOnboardOut,
  status_code=status.HTTP_201_CREATED,
)
def start_connect_onboarding(
  session: Session = Depends(session_dep),
  uid: str = Depends(current_user_id),
):
  """
  Start or continue Stripe Connect onboarding for the current user's UpcyclingStore.

  - Ensures a Connect Express account exists.
  - Returns a Stripe-hosted onboarding link URL.
  """
  _require_stripe()

  store = _get_store_for_user(session, uid)
  if not store:
    raise HTTPException(
      status_code=status.HTTP_404_NOT_FOUND,
      detail="No upcycling store found for this user.",
    )

  store_id = store["store_id"]
  acct_id = store.get("stripe_account_id")

  # 1) Create Connect account if we don't have one yet
  if not acct_id:
    try:
      account = stripe.Account.create(  # type: ignore[attr-defined]
        type="express",
        country="AU",
        capabilities={
          "transfers": {"requested": True},
        },
        business_type="individual",  # can evolve later if needed
      )
      acct_id = account.id
    except Exception as e:
      raise HTTPException(
        status_code=status.HTTP_502_BAD_GATEWAY,
        detail=f"Failed to create Stripe Connect account: {e}",
      )

    # Persist on store
    session.run(
      """
      MATCH (s:UpcyclingStore {id:$sid})
      SET s.stripe_account_id = $acct
      """,
      sid=store_id,
      acct=acct_id,
    )

  # 2) Create Stripe AccountLink for onboarding / updating details
  refresh_url = f"{PUBLIC_ECODIA_URL}/market/settings"
  return_url = f"{PUBLIC_ECODIA_URL}/market/settings?onboard=done"

  try:
    link = stripe.AccountLink.create(  # type: ignore[attr-defined]
      account=acct_id,
      refresh_url=refresh_url,
      return_url=return_url,
      type="account_onboarding",
    )
  except Exception as e:
    raise HTTPException(
      status_code=status.HTTP_502_BAD_GATEWAY,
      detail=f"Failed to create Stripe onboarding link: {e}",
    )

  return ConnectOnboardOut(url=link.url)


@router.get(
  "/status",
  response_model=ConnectStatusOut,
)
def get_connect_status(
  session: Session = Depends(session_dep),
  uid: str = Depends(current_user_id),
):
  """
  Basic Stripe Connect status for the current creator.

  Used by the store settings page to show whether payouts are ready.
  """
  _require_stripe()

  store = _get_store_for_user(session, uid)
  if not store:
    return ConnectStatusOut(has_store=False)

  acct_id = store.get("stripe_account_id")
  if not acct_id:
    return ConnectStatusOut(
      has_store=True,
      stripe_account_id=None,
    )

  try:
    account = stripe.Account.retrieve(acct_id)  # type: ignore[attr-defined]
  except Exception as e:
    raise HTTPException(
      status_code=status.HTTP_502_BAD_GATEWAY,
      detail=f"Failed to fetch Connect account: {e}",
    )

  # Be conservative: only expose a few safe fields
  requirements = getattr(account, "requirements", None) or {}
  return ConnectStatusOut(
    has_store=True,
    stripe_account_id=acct_id,
    payouts_enabled=bool(getattr(account, "payouts_enabled", False)),
    charges_enabled=bool(getattr(account, "charges_enabled", False)),
    details_submitted=bool(getattr(account, "details_submitted", False)),
    requirements_currently_due=requirements.get("currently_due") or [],
    requirements_disabled_reason=requirements.get("disabled_reason"),
  )
