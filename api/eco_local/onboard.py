# api/routers/eco-local_onboard.py
from __future__ import annotations
from typing import Optional, Dict, Any

from fastapi import APIRouter, Depends, HTTPException, Query, Body
from pydantic import BaseModel, Field
from neo4j import Session

from site_backend.core.neo_driver import session_dep
from site_backend.core.user_guard import current_user_id
from site_backend.api.eco_local.neo_business import (
    business_init,
    business_update_standards,
)

router = APIRouter(prefix="/eco-local", tags=["onboarding"])

# ---------------- Models ----------------
class InitIn(BaseModel):
    business_name: str = Field(..., min_length=2)
    industry_group: str = Field(..., min_length=2)
    size: str
    area: str
    pledge: str  # "starter" | "builder" | "leader"

class InitOut(BaseModel):
    business_id: str
    qr_code: str

class RecommendIn(BaseModel):
    business_name: Optional[str] = None
    industry_group: str
    size: str
    area: str
    pledge: str

class RecommendOut(BaseModel):
    recommended_monthly_aud: int
    breakdown: Dict[str, Any]
    pay_what_you_want: bool
    min_aud: int
    max_aud: int

class ProfileIn(BaseModel):
    standards_eco: str = Field(..., min_length=10)
    standards_sustainability: str = Field(..., min_length=10)
    standards_social: str = Field(..., min_length=10)
    certifications: Optional[list[str]] = None
    links: Optional[list[str]] = None

class StatusOut(BaseModel):
    business_id: str
    completed: bool

# ---------------- Helpers ----------------
def _resolve_user_business_id(s: Session, user_id: str, requested: Optional[str]) -> str:
    if requested:
        ok = s.run(
            """
            MATCH (u:User {id:$uid})-[r]->(b:BusinessProfile {id:$bid})
            WHERE type(r) IN ['OWNS','MANAGES']
            RETURN 1 AS ok
            """,
            uid=user_id, bid=requested,
        ).single()
        if not ok:
            raise HTTPException(status_code=403, detail="You don't have access to that business")
        return requested

    ids = [r["id"] for r in s.run(
        """
        MATCH (u:User {id:$uid})-[r]->(b:BusinessProfile)
        WHERE type(r) IN ['OWNS','MANAGES']
        RETURN b.id AS id
        ORDER BY id
        """,
        uid=user_id,
    )]
    if not ids:
        raise HTTPException(status_code=404, detail="You don't have a business yet")
    if len(ids) > 1:
        raise HTTPException(
            status_code=400,
            detail={"message": "Multiple businesses; specify ?business_id=...", "your_business_ids": ids},
        )
    return ids[0]

# ---------------- Endpoints ----------------
@router.post("/business/init", response_model=InitOut, status_code=201)
def business_init_api(
    payload: InitIn,
    s: Session = Depends(session_dep),
    user_id: str = Depends(current_user_id),
):
    out = business_init(
        s,
        user_id=user_id,
        business_name=payload.business_name.strip(),
        industry_group=payload.industry_group.strip(),
        size=payload.size.strip(),
        area=payload.area.strip(),
        pledge_tier=payload.pledge.strip(),
    )
    bid = out["business_id"]

    # Mark onboarding not completed yet (idempotent)
    s.run(
        """
        MATCH (b:BusinessProfile {id:$bid})
        SET b.onboarding_completed = coalesce(b.onboarding_completed,false)
        """,
        bid=bid,
    )
    return InitOut(business_id=bid, qr_code=out["qr_code"])

@router.post("/business/profile", response_model=dict)
def business_profile_api(
    payload: ProfileIn,
    s: Session = Depends(session_dep),
    user_id: str = Depends(current_user_id),
    business_id: Optional[str] = Query(None),
):
    bid = _resolve_user_business_id(s, user_id, business_id)
    business_update_standards(
        s,
        business_id=bid,
        standards_eco=payload.standards_eco,
        standards_sustainability=payload.standards_sustainability,
        standards_social=payload.standards_social,
        certifications=payload.certifications,
        links=payload.links,
    )
    return {"ok": True, "business_id": bid}

@router.post("/business/recommend", response_model=RecommendOut)
def business_recommend_api(payload: RecommendIn):
    base_by_size = {"1-5": 25, "6-20": 49, "21-50": 99, "50+": 149}
    area_mult = {"cbd": 1.15, "suburb": 1.0, "regional": 0.85}
    pledge_mult = {"starter": 0.9, "builder": 1.0, "leader": 1.15}

    base = base_by_size.get(payload.size, 49)
    amt = int(round(base * area_mult.get(payload.area, 1.0) * pledge_mult.get(payload.pledge, 1.0)))
    amt = max(10, min(999, amt))

    breakdown = {
        "size_base": base,
        "area_multiplier": area_mult.get(payload.area, 1.0),
        "pledge_multiplier": pledge_mult.get(payload.pledge, 1.0),
        "policy": "1 AUD = 1 ECO; scan rewards mint as needed.",
        "inputs": payload.model_dump(),
    }
    return RecommendOut(
        recommended_monthly_aud=amt,
        breakdown=breakdown,
        pay_what_you_want=True,
        min_aud=10,
        max_aud=999,
    )

@router.get("/business/onboarding_status", response_model=StatusOut)
def onboarding_status_api(
    s: Session = Depends(session_dep),
    user_id: str = Depends(current_user_id),
    business_id: Optional[str] = Query(None),
):
    bid = _resolve_user_business_id(s, user_id, business_id)
    rec = s.run(
        """
        MATCH (b:BusinessProfile {id:$bid})
        RETURN b.id AS bid, coalesce(b.onboarding_completed,false) AS ok
        """,
        bid=bid,
    ).single()
    if not rec:
        raise HTTPException(status_code=404, detail="Business not found")
    return StatusOut(business_id=rec["bid"], completed=bool(rec["ok"]))

@router.post("/business/onboarding_complete", response_model=dict)
def onboarding_complete_api(
    s: Session = Depends(session_dep),
    user_id: str = Depends(current_user_id),
    business_id: Optional[str] = Query(None),
):
    bid = _resolve_user_business_id(s, user_id, business_id)
    s.run(
        """
        MATCH (b:BusinessProfile {id:$bid})
        SET b.onboarding_completed = true, b.onboarding_completed_at = timestamp()
        """,
        bid=bid,
    )
    return {"ok": True, "business_id": bid}

# ---- Dev helper: mock checkout ----
@router.post("/dev/mock_checkout", response_model=dict)
def dev_mock_checkout_api(
    payload: Optional[dict] = Body(default=None),
    monthly_aud: Optional[int] = Query(default=None),
    s: Session = Depends(session_dep),
    user_id: str = Depends(current_user_id),
    business_id: Optional[str] = Query(None),
):
    amt = None
    if payload and "monthly_aud" in payload:
        try:
            amt = int(payload["monthly_aud"])
        except Exception:
            pass
    if amt is None and monthly_aud is not None:
        amt = int(monthly_aud)
    if amt is None:
        raise HTTPException(status_code=400, detail="monthly_aud is required")

    bid = _resolve_user_business_id(s, user_id, business_id)
    if amt < 5:
        raise HTTPException(status_code=400, detail="Min $5")

    s.run(
        """
        MATCH (b:BusinessProfile {id:$bid})
        SET b.latest_unit_amount_aud=$amt,
            b.subscription_status='active'
        """,
        bid=bid, amt=int(amt),
    )
    return {"ok": True, "business_id": bid, "monthly_aud": int(amt)}
