# site_backend/api/leaderboards/leaderboards.py
from __future__ import annotations
from typing import List, Optional
import os

from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel
from neo4j import Session, GraphDatabase
from neo4j.exceptions import ServiceUnavailable

from site_backend.core.neo_driver import session_dep  # your existing dep
from .service import top_youth_eco, top_business_eco, top_youth_actions

router = APIRouter(prefix="/leaderboards", tags=["leaderboards"])

# ───────────────────────────────────────────────────────────────────────────────
# Schemas (rows)
# ───────────────────────────────────────────────────────────────────────────────

class LBYouthEcoRow(BaseModel):
    user_id: str
    display_name: str
    eco: int = 0
    avatar_url: Optional[str] = None

class LBBusinessEcoRow(BaseModel):
    business_id: str
    name: str
    eco: int = 0

class LBYouthActionsRow(BaseModel):
    user_id: str
    display_name: str
    completed: int = 0
    avatar_url: Optional[str] = None

# ───────────────────────────────────────────────────────────────────────────────
# Schemas (meta + wrappers)
# ───────────────────────────────────────────────────────────────────────────────

class MetaMy(BaseModel):
    id: str
    value: int
    rank: int
    display_name: Optional[str] = None
    avatar_url: Optional[str] = None

class MetaBlock(BaseModel):
    period: str
    since_ms: Optional[int]
    limit: int
    offset: int
    has_more: bool
    total_estimate: Optional[int] = None
    top_value: Optional[int] = None
    my: Optional[MetaMy] = None

class LBWrapYouthEco(BaseModel):
    items: List[LBYouthEcoRow]
    meta: MetaBlock

class LBWrapBizEco(BaseModel):
    items: List[LBBusinessEcoRow]
    meta: MetaBlock

class LBWrapYouthActions(BaseModel):
    items: List[LBYouthActionsRow]
    meta: MetaBlock

# ───────────────────────────────────────────────────────────────────────────────
# Local fallback helper (ONLY used if routing explodes for this call)
# ───────────────────────────────────────────────────────────────────────────────

def _with_direct_bolt_retry(fn, *args, **kwargs):
    """
    Run the given service function with the provided session first.
    If routing blows up, retry once using a temporary bolt:// driver + pinned DB.
    """
    # First attempt: normal session (already in args[0])
    try:
        return fn(*args, **kwargs)
    except ServiceUnavailable as e:
        # Retry with temporary bolt driver
        uri_env = os.getenv("NEO4J_URI", "bolt://localhost:7687").strip()
        # Rewrite routing schemes to direct bolt only for this fallback
        if uri_env.startswith("neo4j://") or uri_env.startswith("neo4j+ssc://") or uri_env.startswith("neo4j+s://"):
            host = uri_env.split("://", 1)[1]
            uri_env = "bolt://" + host
        user = os.getenv("NEO4J_USER", "neo4j")
        pwd  = os.getenv("NEO4J_PASS", "neo4j")
        db   = os.getenv("NEO4J_DATABASE", "neo4j").strip()

        tmp_driver = GraphDatabase.driver(uri_env, auth=(user, pwd))
        try:
            with tmp_driver.session(database=db) as s2:
                # Replace the session argument (first arg) with the fallback session
                args2 = (s2, *args[1:])
                return fn(*args2, **kwargs)
        finally:
            tmp_driver.close()

# ───────────────────────────────────────────────────────────────────────────────
# Endpoints
# ───────────────────────────────────────────────────────────────────────────────

@router.get("/youth/eco", response_model=LBWrapYouthEco)
def leaderboard_youth_eco(
    period: str = Query("total", pattern="^(total|weekly|monthly)$"),
    limit: int = Query(20, ge=1, le=200),
    offset: int = Query(0, ge=0),
    me_user_id: Optional[str] = Query(None),
    session: Session = Depends(session_dep),
):
    """Youth ECO leaderboard (sum of settled EcoTx earned in period)."""
    # Normal path uses the injected session; on routing error only, retry direct bolt
    return _with_direct_bolt_retry(
        top_youth_eco, session, period=period, limit=limit, offset=offset, me_user_id=me_user_id
    )

@router.get("/business/eco", response_model=LBWrapBizEco)
def leaderboard_business_eco(
    period: str = Query("total", pattern="^(total|weekly|monthly)$"),
    limit: int = Query(20, ge=1, le=200),
    offset: int = Query(0, ge=0),
    me_business_id: Optional[str] = Query(None),
    session: Session = Depends(session_dep),
):
    """Business ECO leaderboard (sum of settled EcoTx TRIGGERED by business in period)."""
    return _with_direct_bolt_retry(
        top_business_eco, session, period=period, limit=limit, offset=offset, me_business_id=me_business_id
    )

@router.get("/youth/actions", response_model=LBWrapYouthActions)
def leaderboard_youth_actions(
    period: str = Query("total", pattern="^(total|weekly|monthly)$"),
    type: Optional[str] = Query(None, pattern="^(eco_action|sidequest|all)?$"),
    limit: int = Query(20, ge=1, le=200),
    offset: int = Query(0, ge=0),
    me_user_id: Optional[str] = Query(None),
    session: Session = Depends(session_dep),
):
    """
    Youth sidequest completions (approved submissions) leaderboard.
    - period: total | weekly | monthly
    - type: eco_action | sidequest | all (default all)
    """
    return _with_direct_bolt_retry(
        top_youth_actions, session, period=period, mission_type=type, limit=limit, offset=offset, me_user_id=me_user_id
    )
