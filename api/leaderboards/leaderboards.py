# site_backend/api/leaderboards/router.py
from __future__ import annotations
from typing import Literal, Optional, List, Dict, Any
from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel, Field
from neo4j import Session

from site_backend.core.neo_driver import session_dep
from .service import (  # ← your provided service file (top_youth_eco, etc.)
    top_youth_eco,
    top_youth_contributed,
    top_business_eco,
    top_youth_actions,
)

Period = Literal["total", "weekly", "monthly"]

router = APIRouter(prefix="/leaderboards", tags=["leaderboards"])

# ---------- Pydantic shapes (unified across endpoints) ----------

class LBMetaMy(BaseModel):
    id: str
    value: int
    rank: int
    display_name: str
    avatar_url: Optional[str] = None

class LBMeta(BaseModel):
    period: Period
    since_ms: Optional[int] = None
    limit: int
    offset: int
    has_more: bool
    total_estimate: int
    top_value: int
    my: Optional[LBMetaMy] = None

class LBUserEcoItem(BaseModel):
    user_id: str
    display_name: str
    eco: int
    avatar_url: Optional[str] = None

class LBBusinessEcoItem(BaseModel):
    business_id: str
    display_name: str
    eco: int

class LBUserActionsItem(BaseModel):
    user_id: str
    display_name: str
    completed: int
    avatar_url: Optional[str] = None

class LBResponse(BaseModel):
    items: List[Dict[str, Any]] = Field(default_factory=list)
    meta: LBMeta

# ---------- Routes ----------

@router.get("/youth/eco", response_model=LBResponse)
def lb_youth_eco(
    s: Session = Depends(session_dep),
    period: Period = Query("monthly", regex="^(total|weekly|monthly)$"),
    limit: int = Query(20, ge=1, le=100),
    offset: int = Query(0, ge=0),
    me_user_id: Optional[str] = Query(None),
):
    data = top_youth_eco(s, period=period, limit=limit, offset=offset, me_user_id=me_user_id)
    return data

@router.get("/youth/contributed", response_model=LBResponse)
def lb_youth_contributed(
    s: Session = Depends(session_dep),
    period: Period = Query("monthly", regex="^(total|weekly|monthly)$"),
    limit: int = Query(20, ge=1, le=100),
    offset: int = Query(0, ge=0),
    me_user_id: Optional[str] = Query(None),
):
    data = top_youth_contributed(s, period=period, limit=limit, offset=offset, me_user_id=me_user_id)
    return data

@router.get("/business/eco", response_model=LBResponse)
def lb_business_eco(
    s: Session = Depends(session_dep),
    period: Period = Query("monthly", regex="^(total|weekly|monthly)$"),
    limit: int = Query(20, ge=1, le=100),
    offset: int = Query(0, ge=0),
    me_business_id: Optional[str] = Query(None),
):
    data = top_business_eco(s, period=period, limit=limit, offset=offset, me_business_id=me_business_id)
    return data

@router.get("/youth/actions", response_model=LBResponse)
def lb_youth_actions(
    s: Session = Depends(session_dep),
    period: Period = Query("monthly", regex="^(total|weekly|monthly)$"),
    kind: Optional[str] = Query(None),  # 'eco_action' | 'sidequest' | 'all' | None
    limit: int = Query(20, ge=1, le=100),
    offset: int = Query(0, ge=0),
    me_user_id: Optional[str] = Query(None),
):
    data = top_youth_actions(s, period=period, mission_type=kind, limit=limit, offset=offset, me_user_id=me_user_id)
    return data
