# api/routers/eyba_business.py
from __future__ import annotations

from typing import Optional, List

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field
from neo4j import Session

from site_backend.core.neo_driver import session_dep
from site_backend.core.user_guard import current_user_id
from site_backend.api.eyba.neo_business import (
    business_by_owner,
    business_update_public_profile,
    get_business_metrics,
    get_business_activity,
)

router = APIRouter(prefix="/eyba/business", tags=["eyba-business"])

# --------- Schemas ----------
class BusinessMineOut(BaseModel):
    id: str
    name: Optional[str] = None
    industry_group: Optional[str] = None
    size: Optional[str] = None
    area: Optional[str] = None
    pledge_tier: Optional[str] = None
    eco_mint_ratio: Optional[int] = None
    website: Optional[str] = None
    tagline: Optional[str] = None
    address: Optional[str] = None
    hours: Optional[str] = None
    description: Optional[str] = None
    hero_url: Optional[str] = None
    lat: Optional[float] = None
    lng: Optional[float] = None
    visible_on_map: bool = True
    tags: Optional[List[str]] = None
    qr_code: Optional[str] = None

class BusinessProfilePatch(BaseModel):
    name: Optional[str] = Field(None, min_length=2, max_length=120)
    tagline: Optional[str] = Field(None, min_length=0, max_length=160)
    website: Optional[str] = None
    address: Optional[str] = None
    hours: Optional[str] = None
    description: Optional[str] = None
    hero_url: Optional[str] = None
    lat: Optional[float] = Field(None, ge=-90, le=90)
    lng: Optional[float] = Field(None, ge=-180, le=180)
    visible_on_map: Optional[bool] = None
    tags: Optional[List[str]] = None

class MetricsOut(BaseModel):
    business_id: str
    name: Optional[str] = None
    pledge_tier: Optional[str] = None
    eco_mint_ratio: Optional[int] = None
    eco_contributed_total: int
    eco_given_total: int
    minted_eco: int
    eco_velocity_30d: float

class ActivityRow(BaseModel):
    id: str
    kind: str
    source: Optional[str] = None
    amount: int
    createdAt: int
    user_id: Optional[str] = None

# --------- Routes ----------
@router.get("/mine", response_model=BusinessMineOut)
def get_mine(
    s: Session = Depends(session_dep),
    uid: str = Depends(current_user_id),
):
    b = business_by_owner(s, user_id=uid)
    if not b:
        raise HTTPException(status_code=404, detail="No business found for this account")
    return BusinessMineOut(**b)
@router.patch("/profile", response_model=BusinessMineOut)
def patch_profile(
    patch: BusinessProfilePatch,
    s: Session = Depends(session_dep),
    uid: str = Depends(current_user_id),
):
    b = business_by_owner(s, user_id=uid)
    if not b:
        raise HTTPException(status_code=404, detail="No business found for this account")
    out = business_update_public_profile(
        s,
        business_id=b["id"],
        owner_user_id=uid,
        fields={k: v for k, v in patch.model_dump(exclude_unset=True).items()}
    )
    merged = {**b, **out}
    return BusinessMineOut(**merged)

@router.get("/metrics", response_model=MetricsOut)
def metrics(
    s: Session = Depends(session_dep),
    uid: str = Depends(current_user_id),
):
    b = business_by_owner(s, user_id=uid)
    if not b:
        raise HTTPException(status_code=404, detail="No business found for this account")
    try:
        m = get_business_metrics(s, business_id=b["id"])
        return MetricsOut(**m)
    except ValueError:
        raise HTTPException(status_code=404, detail="Business not found")

@router.get("/activity", response_model=List[ActivityRow])
def activity(
    limit: int = Query(50, ge=1, le=500),
    s: Session = Depends(session_dep),
    uid: str = Depends(current_user_id),
):
    b = business_by_owner(s, user_id=uid)
    if not b:
        raise HTTPException(status_code=404, detail="No business found for this account")
    rows = get_business_activity(s, business_id=b["id"], limit=limit)
    return [ActivityRow(**r) for r in rows]
