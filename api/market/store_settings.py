# site_backend/api/routers/market_store_settings.py
from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, status
from neo4j import Session  # type: ignore
from pydantic import BaseModel

from site_backend.core.neo_driver import session_dep
from site_backend.core.user_guard import current_user_id

router = APIRouter(
    prefix="/market/store",
    tags=["market_store_settings"],
)


# ============================================================
# Models
# ============================================================


class UpcyclingStorePayoutSettingsIn(BaseModel):
    # Billing address
    billing_name: Optional[str] = None
    billing_line1: Optional[str] = None
    billing_line2: Optional[str] = None
    billing_suburb: Optional[str] = None
    billing_state: Optional[str] = None
    billing_postcode: Optional[str] = None
    billing_country: Optional[str] = None

    # Bank / payout details
    payout_account_name: Optional[str] = None
    payout_bsb: Optional[str] = None
    payout_account_number: Optional[str] = None
    payout_bank_name: Optional[str] = None
    payout_email: Optional[str] = None  # for payout notices, optional

    # Optional ABN
    abn: Optional[str] = None


class UpcyclingStorePayoutSettingsOut(UpcyclingStorePayoutSettingsIn):
    store_id: str
    user_id: str
    handle: str
    display_name: str


# ============================================================
# Routes
# ============================================================


@router.get(
    "/me/settings",
    response_model=UpcyclingStorePayoutSettingsOut,
)
async def get_store_settings(
    session: Session = Depends(session_dep),
    user_id: str = Depends(current_user_id),
):
    """
    Fetch payout + billing settings for the current user's upcycling store.
    """
    cypher = """
    MATCH (u:User {id: $user_id})-[:OWNS_UPCYCLING_STORE]->(s:UpcyclingStore)
    RETURN {
        store_id: s.id,
        user_id: s.user_id,
        handle: s.handle,
        display_name: s.display_name,
        billing_name: coalesce(s.billing_name, null),
        billing_line1: coalesce(s.billing_line1, null),
        billing_line2: coalesce(s.billing_line2, null),
        billing_suburb: coalesce(s.billing_suburb, null),
        billing_state: coalesce(s.billing_state, null),
        billing_postcode: coalesce(s.billing_postcode, null),
        billing_country: coalesce(s.billing_country, null),
        payout_account_name: coalesce(s.payout_account_name, null),
        payout_bsb: coalesce(s.payout_bsb, null),
        payout_account_number: coalesce(s.payout_account_number, null),
        payout_bank_name: coalesce(s.payout_bank_name, null),
        payout_email: coalesce(s.payout_email, null),
        abn: coalesce(s.abn, null)
    } AS data
    """
    result = session.run(cypher, {"user_id": user_id})
    record = result.single()
    if not record:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="No upcycling store found for this user",
        )

    data = record["data"] or {}
    return UpcyclingStorePayoutSettingsOut(**data)


@router.put(
    "/me/settings",
    response_model=UpcyclingStorePayoutSettingsOut,
)
async def update_store_settings(
    body: UpcyclingStorePayoutSettingsIn,
    session: Session = Depends(session_dep),
    user_id: str = Depends(current_user_id),
):
    """
    Update billing address, bank details, and optional ABN
    for the current user's upcycling store.
    """
    params = {"user_id": user_id, **body.dict()}

    cypher = """
    MATCH (u:User {id: $user_id})-[:OWNS_UPCYCLING_STORE]->(s:UpcyclingStore)
    SET
      s.billing_name = $billing_name,
      s.billing_line1 = $billing_line1,
      s.billing_line2 = $billing_line2,
      s.billing_suburb = $billing_suburb,
      s.billing_state = $billing_state,
      s.billing_postcode = $billing_postcode,
      s.billing_country = $billing_country,
      s.payout_account_name = $payout_account_name,
      s.payout_bsb = $payout_bsb,
      s.payout_account_number = $payout_account_number,
      s.payout_bank_name = $payout_bank_name,
      s.payout_email = $payout_email,
      s.abn = $abn,
      s.updated_at = datetime()
    RETURN {
        store_id: s.id,
        user_id: s.user_id,
        handle: s.handle,
        display_name: s.display_name,
        billing_name: coalesce(s.billing_name, null),
        billing_line1: coalesce(s.billing_line1, null),
        billing_line2: coalesce(s.billing_line2, null),
        billing_suburb: coalesce(s.billing_suburb, null),
        billing_state: coalesce(s.billing_state, null),
        billing_postcode: coalesce(s.billing_postcode, null),
        billing_country: coalesce(s.billing_country, null),
        payout_account_name: coalesce(s.payout_account_name, null),
        payout_bsb: coalesce(s.payout_bsb, null),
        payout_account_number: coalesce(s.payout_account_number, null),
        payout_bank_name: coalesce(s.payout_bank_name, null),
        payout_email: coalesce(s.payout_email, null),
        abn: coalesce(s.abn, null)
    } AS data
    """

    result = session.run(cypher, params)
    record = result.single()
    if not record:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="No upcycling store found for this user",
        )

    data = record["data"] or {}
    return UpcyclingStorePayoutSettingsOut(**data)
