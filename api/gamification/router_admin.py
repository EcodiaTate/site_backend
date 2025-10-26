from __future__ import annotations
from fastapi import APIRouter, Depends
from neo4j import Session

from site_backend.core.neo_driver import session_dep
from site_backend.core.user_guard import current_user_id
from site_backend.api.gamification import service
from site_backend.api.gamification.schema import (
    BadgeTypeIn, BadgeTypeOut,
    AwardTypeIn, AwardTypeOut,
    SeasonIn, SeasonOut,
    MultiplierConfigIn, MultiplierConfigOut,
    QuestTypeIn, QuestTypeOut,
)

router = APIRouter(prefix="/admin/gamification", tags=["admin-gamification"])

# Optional: swap with your real admin policy
def _ensure_admin(uid: str) -> None:
    # if not user_is_admin(uid): raise HTTPException(403, "forbidden")
    return

# ── Badge Types ───────────────────────────────────────────────────────────────
@router.get("/badge-types", response_model=list[BadgeTypeOut])
def list_badge_types(uid: str = Depends(current_user_id), s: Session = Depends(session_dep)):
    _ensure_admin(uid)
    return [BadgeTypeOut(**r) for r in service.list_badge_types(s)]

@router.post("/badge-types", response_model=BadgeTypeOut)
def upsert_badge_type(body: BadgeTypeIn, uid: str = Depends(current_user_id), s: Session = Depends(session_dep)):
    _ensure_admin(uid)
    return BadgeTypeOut(**service.upsert_badge_type(s, payload=body.model_dump()))

@router.delete("/badge-types/{badge_type_id}")
def delete_badge_type(badge_type_id: str, uid: str = Depends(current_user_id), s: Session = Depends(session_dep)):
    _ensure_admin(uid)
    service.delete_badge_type(s, id=badge_type_id)
    return {"ok": True}

# ── Award Types ───────────────────────────────────────────────────────────────
@router.get("/award-types", response_model=list[AwardTypeOut])
def list_award_types(uid: str = Depends(current_user_id), s: Session = Depends(session_dep)):
    _ensure_admin(uid)
    return [AwardTypeOut(**r) for r in service.list_award_types(s)]

@router.post("/award-types", response_model=AwardTypeOut)
def upsert_award_type(body: AwardTypeIn, uid: str = Depends(current_user_id), s: Session = Depends(session_dep)):
    _ensure_admin(uid)
    return AwardTypeOut(**service.upsert_award_type(s, payload=body.model_dump()))

@router.delete("/award-types/{award_type_id}")
def delete_award_type(award_type_id: str, uid: str = Depends(current_user_id), s: Session = Depends(session_dep)):
    _ensure_admin(uid)
    service.delete_award_type(s, id=award_type_id)
    return {"ok": True}

# ── Seasons ───────────────────────────────────────────────────────────────────
@router.get("/seasons", response_model=list[SeasonOut])
def list_seasons(uid: str = Depends(current_user_id), s: Session = Depends(session_dep)):
    _ensure_admin(uid)
    return [SeasonOut(**r) for r in service.list_seasons(s)]

@router.post("/seasons", response_model=SeasonOut)
def upsert_season(body: SeasonIn, uid: str = Depends(current_user_id), s: Session = Depends(session_dep)):
    _ensure_admin(uid)
    return SeasonOut(**service.upsert_season(s, payload=body.model_dump()))

@router.delete("/seasons/{season_id}")
def delete_season(season_id: str, uid: str = Depends(current_user_id), s: Session = Depends(session_dep)):
    _ensure_admin(uid)
    service.delete_season(s, id=season_id)
    return {"ok": True}

# ── Multipliers ───────────────────────────────────────────────────────────────
@router.get("/multipliers", response_model=list[MultiplierConfigOut])
def list_multipliers(uid: str = Depends(current_user_id), s: Session = Depends(session_dep)):
    _ensure_admin(uid)
    return [MultiplierConfigOut(**r) for r in service.list_multiplier_configs(s)]

@router.post("/multipliers", response_model=MultiplierConfigOut)
def upsert_multiplier(body: MultiplierConfigIn, uid: str = Depends(current_user_id), s: Session = Depends(session_dep)):
    _ensure_admin(uid)
    return MultiplierConfigOut(**service.upsert_multiplier_config(s, payload=body.model_dump()))

@router.delete("/multipliers/{multiplier_id}")
def delete_multiplier(multiplier_id: str, uid: str = Depends(current_user_id), s: Session = Depends(session_dep)):
    _ensure_admin(uid)
    service.delete_multiplier_config(s, id=multiplier_id)
    return {"ok": True}

# ── Quest Types ───────────────────────────────────────────────────────────────
@router.get("/quest-types", response_model=list[QuestTypeOut])
def list_quest_types(uid: str = Depends(current_user_id), s: Session = Depends(session_dep)):
    _ensure_admin(uid)
    return [QuestTypeOut(**r) for r in service.list_quest_types(s)]

@router.post("/quest-types", response_model=QuestTypeOut)
def upsert_quest_type(body: QuestTypeIn, uid: str = Depends(current_user_id), s: Session = Depends(session_dep)):
    _ensure_admin(uid)
    return QuestTypeOut(**service.upsert_quest_type(s, payload=body.model_dump()))

@router.delete("/quest-types/{quest_type_id}")
def delete_quest_type(quest_type_id: str, uid: str = Depends(current_user_id), s: Session = Depends(session_dep)):
    _ensure_admin(uid)
    service.delete_quest_type(s, id=quest_type_id)
    return {"ok": True}

# ── Admin utilities ───────────────────────────────────────────────────────────
@router.post("/utility/backfill-titles")
def backfill_titles(uid: str = Depends(current_user_id), s: Session = Depends(session_dep)):
    _ensure_admin(uid)
    return service.backfill_titles_from_badges(s)

@router.post("/utility/recompute-streaks")
def recompute_streaks(uid: str = Depends(current_user_id), s: Session = Depends(session_dep)):
    _ensure_admin(uid)
    return service.recompute_all_streaks(s)
