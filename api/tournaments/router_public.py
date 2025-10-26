from __future__ import annotations
from typing import List, Optional
from fastapi import APIRouter, Depends, Query, Body
from neo4j import Session
from site_backend.core.neo_driver import session_dep
from site_backend.core.user_guard import current_user_id
from site_backend.core.admin_guard import require_admin

from .schema import (
    Tournament, TournamentCreate, TournamentUpdate, TournamentEnrollResult,
    StandingRow, EnrollmentOut, LeaderboardOut
)
from .service import (
    list_tournaments, create_tournament, update_tournament,
    enroll, withdraw, enrollment, standings, leaderboard
)

router = APIRouter(prefix="/tournaments", tags=["tournaments"])

# ---------- Public ----------
@router.get("", response_model=List[Tournament])
def r_list_tournaments(
    status: Optional[str] = Query(default=None),
    visibility: Optional[str] = Query(default=None),
    division: Optional[str] = Query(default=None),
    session: Session = Depends(session_dep),
):
    return list_tournaments(session, status=status, visibility=visibility, division=division)

@router.post("/{tid}/enroll", response_model=TournamentEnrollResult)
def r_enroll(
    tid: str,
    scope: str = Query(pattern="^(team|solo)$"),
    team_id: str | None = None,
    session: Session = Depends(session_dep),
    uid: str = Depends(current_user_id),
):
    return enroll(session, uid, tid, scope, team_id)

@router.post("/{tid}/withdraw", response_model=dict)
def r_withdraw(
    tid: str,
    scope: str = Query(pattern="^(team|solo)$"),
    team_id: str | None = None,
    session: Session = Depends(session_dep),
    uid: str = Depends(current_user_id),
):
    return withdraw(session, uid, tid, scope, team_id)

@router.get("/{tid}/enrollment", response_model=EnrollmentOut)
def r_enrollment(tid: str, session: Session = Depends(session_dep)):
    return enrollment(session, tid)

@router.get("/{tid}/standings", response_model=List[StandingRow])
def r_standings(tid: str, session: Session = Depends(session_dep)):
    # Back-compat endpoint
    return standings(session, tid)

@router.get("/{tid}/leaderboard", response_model=LeaderboardOut)
def r_leaderboard(
    tid: str,
    metric: Optional[str] = Query(default=None, pattern="^(eco|completions|eco_per_member)$"),
    session: Session = Depends(session_dep),
):
    return leaderboard(session, tid, metric)

# ---------- Admin (optional but handy) ----------
@router.post("", response_model=Tournament)
def r_create_tournament(
    payload: TournamentCreate,
    session: Session = Depends(session_dep),
    admin: str = Depends(require_admin),
):
    return create_tournament(session, payload.model_dump())

from fastapi import Body  # already imported in your file

@router.patch("/{tid}", response_model=Tournament)
def r_update_tournament(
    tid: str,
    payload: TournamentUpdate = Body(...),
    session: Session = Depends(session_dep),
    admin: str = Depends(require_admin),
):
    return update_tournament(session, tid, payload.model_dump(exclude_none=True))
