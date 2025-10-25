from __future__ import annotations
from fastapi import APIRouter, Depends, HTTPException
from neo4j import Session
from pydantic import BaseModel
from site_backend.core.neo_driver import session_dep

router = APIRouter()

# ------------------ Schemas ------------------

class YouthProfileOut(BaseModel):
    user_id: str
    birth_year: int | None = None
    eyba_points: int = 0
    actions_completed: int = 0
    total_eco_actions: int | None = None
    total_pledges_supported: int | None = None


# ------------------ Routes ------------------

@router.get("/profile/{user_id}", response_model=YouthProfileOut)
def get_youth_profile(user_id: str, s: Session = Depends(session_dep)):
    """
    Fetch a youth's profile with their EYBA points, actions completed,
    and derived stats from any linked nodes (eco actions, pledges, etc.)
    """
    cypher = """
    MATCH (u:User {id:$id})-[:HAS_PROFILE]->(p:YouthProfile)
    OPTIONAL MATCH (p)-[:COMPLETED]->(a:EcoAction)
    OPTIONAL MATCH (p)-[:SUPPORTED]->(biz:BusinessProfile)
    WITH p,
         count(DISTINCT a) AS total_eco_actions,
         count(DISTINCT biz) AS total_pledges_supported
    RETURN {
        user_id: p.user_id,
        birth_year: p.birth_year,
        eyba_points: coalesce(p.eyba_points, 0),
        actions_completed: coalesce(p.actions_completed, 0),
        total_eco_actions: total_eco_actions,
        total_pledges_supported: total_pledges_supported
    } AS out
    """
    rec = s.run(cypher, id=user_id).single()
    if not rec:
        raise HTTPException(status_code=404, detail="Profile not found")

    return rec["out"]
