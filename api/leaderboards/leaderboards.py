from __future__ import annotations
from typing import List, Optional
from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel
from neo4j import Session

from site_backend.core.neo_driver import session_dep
from .service import top_youth_eco, top_business_eco, top_youth_actions

router = APIRouter(prefix="/leaderboards", tags=["leaderboards"])

# ───────────────────────────────────────────────────────────────────────────────
# Schemas
# ───────────────────────────────────────────────────────────────────────────────

class LBYouthEcoRow(BaseModel):
    user_id: str
    eco: int = 0

class LBBusinessEcoRow(BaseModel):
    business_id: str
    name: str
    eco: int = 0

class LBYouthActionsRow(BaseModel):
    user_id: str
    completed: int = 0

# ───────────────────────────────────────────────────────────────────────────────
# Endpoints
# ───────────────────────────────────────────────────────────────────────────────

@router.get("/youth/eco", response_model=List[LBYouthEcoRow])
def leaderboard_youth_eco(
    period: str = Query("total", pattern="^(total|weekly|monthly)$"),
    limit: int = Query(20, ge=1, le=200),
    offset: int = Query(0, ge=0),
    session: Session = Depends(session_dep),
):
    """Youth ECO leaderboard (sum of EcoTx earned)."""
    return top_youth_eco(session, period=period, limit=limit, offset=offset)

@router.get("/business/eco", response_model=List[LBBusinessEcoRow])
def leaderboard_business_eco(
    period: str = Query("total", pattern="^(total|weekly|monthly)$"),
    limit: int = Query(20, ge=1, le=200),
    offset: int = Query(0, ge=0),
    session: Session = Depends(session_dep),
):
    """Business ECO leaderboard (sum of EcoTx minted FROM each business)."""
    return top_business_eco(session, period=period, limit=limit, offset=offset)

@router.get("/youth/actions", response_model=List[LBYouthActionsRow])
def leaderboard_youth_actions(
    period: str = Query("total", pattern="^(total|weekly|monthly)$"),
    type: Optional[str] = Query(None, pattern="^(eco_action|sidequest|all)?$"),
    limit: int = Query(20, ge=1, le=200),
    offset: int = Query(0, ge=0),
    session: Session = Depends(session_dep),
):
    """
    Youth mission completions (approved submissions) leaderboard.
    - period: total | weekly | monthly
    - type: eco_action | sidequest | all (default all)
    """
    return top_youth_actions(session, period=period, mission_type=type, limit=limit, offset=offset)

# Convenience alias
@router.get("/youth/sidequests", response_model=List[LBYouthActionsRow])
def leaderboard_youth_sidequests(
    period: str = Query("total", pattern="^(total|weekly|monthly)$"),
    limit: int = Query(20, ge=1, le=200),
    offset: int = Query(0, ge=0),
    session: Session = Depends(session_dep),
):
    return top_youth_actions(session, period=period, mission_type="sidequest", limit=limit, offset=offset)
