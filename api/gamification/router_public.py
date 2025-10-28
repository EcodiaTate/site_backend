# site_backend/api/gamification/router_public.py
from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query
from neo4j import Session

from site_backend.core.neo_driver import session_dep
from site_backend.core.user_guard import current_user_id
from site_backend.core.user_bootstrap import ensure_user_exists  # ← runs MERGE(User) automatically

from site_backend.api.gamification import service
from site_backend.api.gamification.schema import (
    MeBadgesResponse,
    BusinessAwardsResponse,
    ProgressPreviewOut,
    ClaimRequest,
    ClaimResponse,
    PrestigeResponse,
    StreakFreezeResponse,
    ReferralLinkRequest,
    ReferralLinkResponse,
    LeaderboardResponse,
    Period,
    Scope,
)

# Ensure the user node exists before any handler runs in this router.
router = APIRouter(
    prefix="/gamification",
    tags=["gamification"],
    dependencies=[Depends(ensure_user_exists)],
)

# ── Public endpoints ──────────────────────────────────────────────────────────
@router.get("/me", response_model=MeBadgesResponse)
def me_badges_awards(
    uid: str = Depends(current_user_id),
    s: Session = Depends(session_dep),
):
    return MeBadgesResponse(**service.get_user_badges_and_awards(s, uid=uid))

@router.get("/business/{business_id}/awards", response_model=BusinessAwardsResponse)
def business_awards(
    business_id: str,
    s: Session = Depends(session_dep),
):
    return BusinessAwardsResponse(**service.get_business_awards(s, bid=business_id))

@router.get("/progress/preview", response_model=ProgressPreviewOut)
def progress_preview(
    uid: str = Depends(current_user_id),
    s: Session = Depends(session_dep),
):
    return ProgressPreviewOut(**service.get_progress_preview(s, uid=uid))

@router.post("/claim", response_model=ClaimResponse)
def claim(
    body: ClaimRequest,
    uid: str = Depends(current_user_id),
    s: Session = Depends(session_dep),
):
    try:
        res = service.claim_quest(
            s,
            uid=uid,
            quest_type_id=body.quest_type_id,
            amount=body.amount,
            metadata=body.metadata,
        )
        return ClaimResponse(
            claim_id=res["claim_id"],
            tx_id=res["tx_id"],
            awarded=res["awarded"],
            badges_granted=res["badges_granted"],
            stats=res["stats"],
            window=res["window"],
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e

@router.post("/prestige", response_model=PrestigeResponse)
def prestige(
    uid: str = Depends(current_user_id),
    s: Session = Depends(session_dep),
):
    try:
        out = service.grant_prestige(s, uid=uid)
        return PrestigeResponse(ok=True, new_prestige=out["new_prestige"])
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e

@router.post("/streak/freeze", response_model=StreakFreezeResponse)
def streak_freeze(
    uid: str = Depends(current_user_id),
    s: Session = Depends(session_dep),
):
    try:
        out = service.use_streak_freeze(s, uid=uid)
        return StreakFreezeResponse(**out)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e

@router.post("/referrals/link", response_model=ReferralLinkResponse)
def referrals_link(
    body: ReferralLinkRequest,
    uid: str = Depends(current_user_id),
    s: Session = Depends(session_dep),
):
    if body.referee_id != uid:
        raise HTTPException(status_code=403, detail="referee_must_be_current_user")
    try:
        out = service.link_referral(s, referrer_id=body.referrer_id, referee_id=body.referee_id)
        return ReferralLinkResponse(ok=True, awarded=out["awarded"], amounts=out.get("amounts"))
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
# site_backend/api/gamification/router_public.py
from typing import Optional
from site_backend.core.user_guard import current_user_id, maybe_current_user_id
# keep the router dependency ensure_user_exists as-is if you made it optional-safe

@router.get("/leaderboard", response_model=LeaderboardResponse)
def leaderboard(
    period: Period = Query(..., description="weekly | monthly | total"),
    scope: Scope = Query(..., description="youth | business"),
    start: str | None = Query(None),
    end: str | None = Query(None),
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=200),
    cohort_school_id: str | None = Query(None),
    cohort_team_id: str | None = Query(None),
    cohort_region: str | None = Query(None),
    include_me: bool = Query(False, description="include requesting user's rank"),
    uid: Optional[str] = Depends(maybe_current_user_id),   # ⬅️ optional now
    s: Session = Depends(session_dep),
):
    # If the caller asked to include their own rank but they are anonymous, signal that
    if include_me and not uid:
        raise HTTPException(
            status_code=401,
            detail="login_required_for_include_me",
            headers={"WWW-Authenticate": 'Bearer error="invalid_token"'},
        )

    res = service.get_leaderboard(
        s,
        period=period,
        scope=scope,
        start=start,
        end=end,
        page=page,
        page_size=page_size,
        cohort_school_id=cohort_school_id,
        cohort_team_id=cohort_team_id,
        cohort_region=cohort_region,
        include_me=include_me,
        uid=uid,  # service should handle None => no “me” row
    )
    return LeaderboardResponse(**res)
