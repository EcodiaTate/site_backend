# api/routers/eyba_offers.py
from __future__ import annotations

from datetime import date
from typing import List, Optional, Literal, Dict, Any

from fastapi import APIRouter, Depends, HTTPException, Query
from neo4j import Session
from pydantic import BaseModel, Field

from site_backend.core.neo_driver import session_dep
from site_backend.core.user_guard import current_user_id
from site_backend.api.eyba.neo_business import (
    create_offer,
    list_offers,
    patch_offer,
    delete_offer,
    get_business_metrics,
    stripe_record_contribution,
)

router = APIRouter(prefix="/eyba", tags=["eyba"])

# ------------------------------------------------------------
# Helpers: user → business scoping (+ ownership checks)
# ------------------------------------------------------------

# If your relationships are different, update this set:
_OWNS_EDGES = (":OWNS|:MANAGES",)  # used in Cypher string

def _get_user_business_ids(s: Session, user_id: str) -> List[str]:
    """
    Returns all business ids the user owns/manages.
    """
    recs = s.run(
        f"""
        MATCH (u:User {{id:$uid}})-[{_OWNS_EDGES[0]}]->(b:BusinessProfile)
        RETURN b.id AS id
        ORDER BY id
        """,
        uid=user_id,
    )
    return [r["id"] for r in recs]

def _resolve_user_business_id(
    s: Session,
    user_id: str,
    requested_business_id: Optional[str],
) -> str:
    """
    If a business_id is given: verify ownership.
    Else: if exactly one business, use it.
          if none: 404
          if many: 400 (caller must specify ?business_id=...)
    """
    if requested_business_id:
        rec = s.run(
            f"""
            MATCH (u:User {{id:$uid}})-[{_OWNS_EDGES[0]}]->(b:BusinessProfile {{id:$bid}})
            RETURN b.id AS id
            LIMIT 1
            """,
            uid=user_id,
            bid=requested_business_id,
        ).single()
        if not rec:
            raise HTTPException(status_code=403, detail="You don't have access to that business")
        return requested_business_id

    # No business_id provided; infer
    ids = _get_user_business_ids(s, user_id)
    if len(ids) == 0:
        raise HTTPException(status_code=404, detail="You don't have a business yet or you arent a business!")
    if len(ids) == 1:
        return ids[0]
    # Multiple businesses: ask the caller to specify
    raise HTTPException(
        status_code=400,
        detail={"message": "Multiple businesses found; specify ?business_id=...", "your_business_ids": ids},
    )

def _assert_offer_belongs_to_user(s: Session, user_id: str, offer_id: str) -> str:
    """
    Ensures the offer belongs to a business owned/managed by user.
    Returns the business_id if OK.
    """
    rec = s.run(
        f"""
        MATCH (u:User {{id:$uid}})-[{_OWNS_EDGES[0]}]->(b:BusinessProfile)<-[:OF]-(o:Offer {{id:$oid}})
        RETURN b.id AS bid
        LIMIT 1
        """,
        uid=user_id,
        oid=offer_id,
    ).single()
    if not rec:
        raise HTTPException(status_code=403, detail="Offer not found or not yours")
    return rec["bid"]


# ------------------------------------------------------------
# Models
# ------------------------------------------------------------

# Create uses the user's current business; no business_id in body
class OfferIn(BaseModel):
    title: str = Field(..., min_length=2, max_length=120)
    blurb: str = Field(..., min_length=2, max_length=280)
    type: Literal["discount", "perk", "info"] = "discount"
    visible: bool = True
    redeem_eco: Optional[int] = Field(default=None, ge=1)
    url: Optional[str] = None
    valid_until: Optional[date] = None
    tags: List[str] = Field(default_factory=list)

class OfferOut(OfferIn):
    id: str
    business_id: str

class OfferPatch(BaseModel):
    title: Optional[str] = Field(None, min_length=2, max_length=120)
    blurb: Optional[str] = Field(None, min_length=2, max_length=280)
    type: Optional[Literal["discount", "perk", "info"]] = None
    visible: Optional[bool] = None
    redeem_eco: Optional[int] = Field(None, ge=1, le=100000)
    url: Optional[str] = None
    valid_until: Optional[date] = None
    tags: Optional[List[str]] = None


# ------------------------------------------------------------
# Offers (scoped to current user's business)
# ------------------------------------------------------------

@router.get("/offers", response_model=List[OfferOut])
def list_offers_api(
    visible_only: bool = Query(False),
    business_id: Optional[str] = Query(None, description="Optional if you own multiple"),
    s: Session = Depends(session_dep),
    user_id: str = Depends(current_user_id),
):
    bid = _resolve_user_business_id(s, user_id, business_id)
    arr = list_offers(s, business_id=bid, visible_only=visible_only)
    # ensure business_id present in response
    return [OfferOut(**o, business_id=bid) for o in arr]

@router.post("/offers", response_model=OfferOut, status_code=201)
def create_offer_api(
    payload: OfferIn,
    business_id: Optional[str] = Query(None, description="Optional if you own multiple"),
    s: Session = Depends(session_dep),
    user_id: str = Depends(current_user_id),
):
    bid = _resolve_user_business_id(s, user_id, business_id)
    try:
        o = create_offer(
            s,
            business_id=bid,
            title=payload.title,
            blurb=payload.blurb,
            offtype=payload.type,
            visible=payload.visible,
            redeem_eco=payload.redeem_eco,
            url=payload.url,
            valid_until=str(payload.valid_until) if payload.valid_until else None,
            tags=payload.tags,
        )
        return OfferOut(**o, business_id=bid)
    except Exception:
        # keep it generic; create_offer already ensures Biz exists
        raise HTTPException(status_code=404, detail="Business not found")

@router.patch("/offers/{offer_id}", response_model=OfferOut)
def patch_offer_api(
    offer_id: str,
    patch: OfferPatch,
    s: Session = Depends(session_dep),
    user_id: str = Depends(current_user_id),
):
    # ownership check (also yields business_id for response)
    bid = _assert_offer_belongs_to_user(s, user_id, offer_id)

    fields = {
        k: (str(v) if k == "valid_until" and v is not None else v)
        for k, v in patch.model_dump(exclude_unset=True).items()
    }
    o = patch_offer(s, offer_id=offer_id, fields=fields)
    return OfferOut(**o, business_id=bid)

@router.delete("/offers/{offer_id}", response_model=dict)
def delete_offer_api(
    offer_id: str,
    s: Session = Depends(session_dep),
    user_id: str = Depends(current_user_id),
):
    _assert_offer_belongs_to_user(s, user_id, offer_id)
    delete_offer(s, offer_id=offer_id)
    return {"ok": True}


# ------------------------------------------------------------
# Business Metrics (scoped)
# ------------------------------------------------------------

class BusinessMetricsOut(BaseModel):
    business_id: str
    name: Optional[str] = None
    pledge_tier: Optional[str] = None
    eco_mint_ratio: Optional[int] = None
    eco_contributed_total: int
    eco_given_total: int
    minted_eco: int
    eco_velocity_30d: float

@router.get("/business/metrics", response_model=BusinessMetricsOut)
def get_business_metrics_api(
    business_id: Optional[str] = Query(None, description="Optional if you own multiple"),
    s: Session = Depends(session_dep),
    user_id: str = Depends(current_user_id),
):
    bid = _resolve_user_business_id(s, user_id, business_id)
    try:
        data = get_business_metrics(s, business_id=bid)
        # ensure the response always contains the resolved business_id
        if isinstance(data, dict):
            data = {**data, "business_id": bid}
        return data
    except ValueError:
        raise HTTPException(status_code=404, detail="Business not found")


# ------------------------------------------------------------
# Stripe → ECO contribution mint (manual) (scoped)
# ------------------------------------------------------------

class ContributionIn(BaseModel):
    aud_cents: int = Field(..., ge=100)  # min $1
    eco_mint_ratio: Optional[int] = Field(None, ge=1, le=100)

class ContributionOut(BaseModel):
    ok: bool
    tx_id: str
    eco: int
    business_id: str

@router.post("/business/contribute", response_model=ContributionOut, status_code=201)
def post_contribution_api(
    payload: ContributionIn,
    business_id: Optional[str] = Query(None, description="Optional if you own multiple"),
    s: Session = Depends(session_dep),
    user_id: str = Depends(current_user_id),
):
    bid = _resolve_user_business_id(s, user_id, business_id)
    out = stripe_record_contribution(
        s,
        business_id=bid,
        aud_cents=payload.aud_cents,
        override_eco_mint_ratio=payload.eco_mint_ratio,
    )
    # enforce business_id on the way out
    if isinstance(out, dict):
        out = {**out, "business_id": bid}
    return out
