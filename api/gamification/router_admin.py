# site_backend/api/gamification/router_admin.py
from __future__ import annotations
from typing import List
from fastapi import APIRouter, Depends
from neo4j import Session

from site_backend.core.neo_driver import session_dep
from site_backend.core.admin_guard import require_admin  # <-- your admin guard

from .schema import (
    BadgeTypeUpsert, BadgeTypeOut,
    AwardTypeUpsert, AwardTypeOut,
    SeasonUpsert, SeasonOut,
    EvaluateUserReq, MintMonthlyAwardsReq,
)
from .service import (
    list_badge_types, list_award_types, list_seasons,
    upsert_badge_type, delete_badge_type,
    upsert_award_type, delete_award_type,
    upsert_season, delete_season,
    evaluate_badges_for_user, mint_monthly_awards,
)

# Enforce admin on ALL routes under this prefix
router = APIRouter(
    prefix="/admin/gamification",
    tags=["admin-gamification"],
    dependencies=[Depends(require_admin)],
)

# ---------- Catalogue ----------
@router.get("/badge-types", response_model=List[BadgeTypeOut])
def get_badge_types(session: Session = Depends(session_dep)):
    return list_badge_types(session)

@router.post("/badge-types", response_model=BadgeTypeOut)
def put_badge_type(body: BadgeTypeUpsert, session: Session = Depends(session_dep)):
    return upsert_badge_type(session, payload=body.model_dump())

@router.delete("/badge-types/{bid}")
def del_badge_type(bid: str, session: Session = Depends(session_dep)):
    delete_badge_type(session, id=bid)
    return {"ok": True}

@router.get("/award-types", response_model=List[AwardTypeOut])
def get_award_types(session: Session = Depends(session_dep)):
    return list_award_types(session)

@router.post("/award-types", response_model=AwardTypeOut)
def put_award_type(body: AwardTypeUpsert, session: Session = Depends(session_dep)):
    return upsert_award_type(session, payload=body.model_dump())

@router.delete("/award-types/{aid}")
def del_award_type(aid: str, session: Session = Depends(session_dep)):
    delete_award_type(session, id=aid)
    return {"ok": True}

@router.get("/seasons", response_model=List[SeasonOut])
def get_seasons(session: Session = Depends(session_dep)):
    return list_seasons(session)

@router.post("/seasons", response_model=SeasonOut)
def put_season(body: SeasonUpsert, session: Session = Depends(session_dep)):
    return upsert_season(session, payload=body.model_dump())

@router.delete("/seasons/{sid}")
def del_season(sid: str, session: Session = Depends(session_dep)):
    delete_season(session, id=sid)
    return {"ok": True}

# ---------- Evaluation / Minting ----------
@router.post("/evaluate-user")
def post_evaluate_user(body: EvaluateUserReq, session: Session = Depends(session_dep)):
    return evaluate_badges_for_user(session, uid=body.user_id, season_id=body.season_id)

@router.post("/mint-monthly-awards")
def post_mint_monthly_awards(body: MintMonthlyAwardsReq, session: Session = Depends(session_dep)):
    return mint_monthly_awards(
        session,
        start=body.start, end=body.end, season_id=body.season_id,
        youth_award_type_id=body.youth_award_type_id, business_award_type_id=body.business_award_type_id,
        youth_limit=body.youth_limit, business_limit=body.business_limit,
    )
