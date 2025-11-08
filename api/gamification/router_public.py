from __future__ import annotations
from fastapi import APIRouter, Depends, Query
from neo4j import Session

from site_backend.core.neo_driver import session_dep
from .schema import MeBadgesResponse, BusinessAwardsResponse
from .service import get_user_badges_and_awards, get_business_awards

router = APIRouter(prefix="/gamification", tags=["gamification"])

@router.get("/users/{uid}/me", response_model=MeBadgesResponse)
def my_badges_and_awards(uid: str, session: Session = Depends(session_dep)):
    data = get_user_badges_and_awards(session, uid=uid)
    return data

@router.get("/business/{bid}/awards", response_model=BusinessAwardsResponse)
def business_awards(bid: str, session: Session = Depends(session_dep)):
    data = get_business_awards(session, bid=bid)
    return data
