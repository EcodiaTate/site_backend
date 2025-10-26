# site_backend/api/auth/set_role.py
from __future__ import annotations
from typing import Literal
from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field
from neo4j import Session

from site_backend.core.neo_driver import session_dep
from site_backend.core.user_guard import current_user_id
from site_backend.core.admin_guard import ADMIN_EMAIL

router = APIRouter(tags=["auth"])
RoleLiteral = Literal["youth", "business", "creative", "partner", "public"]

class SetRoleIn(BaseModel):
    role: RoleLiteral = Field(...)

ROLE_CREATE_PROFILE_CYPHER: dict[str, str] = {
    "youth": """
        MATCH (u:User {id:$uid})
        MERGE (p:YouthProfile { user_id:$uid })
          ON CREATE SET p.birth_year = coalesce(p.birth_year, 2006),
                        p.eyba_points = 0,
                        p.actions_completed = 0
        MERGE (u)-[:HAS_PROFILE]->(p)
    """,
    "creative": """
        MATCH (u:User {id:$uid})
        MERGE (p:CreativeProfile { user_id:$uid })
          ON CREATE SET p.display_name = coalesce(p.display_name, ""),
                        p.portfolio_url = coalesce(p.portfolio_url, ""),
                        p.collabs_started = 0
        MERGE (u)-[:HAS_PROFILE]->(p)
    """,
    "partner": """
        MATCH (u:User {id:$uid})
        MERGE (p:PartnerProfile { user_id:$uid })
          ON CREATE SET p.org_name = coalesce(p.org_name, ""),
                        p.org_type = coalesce(p.org_type, "community"),
                        p.active_projects = 0
        MERGE (u)-[:HAS_PROFILE]->(p)
    """,
    "public": """
        MATCH (u:User {id:$uid})
        MERGE (p:PublicProfile { user_id:$uid })
          ON CREATE SET p.display_name = coalesce(p.display_name, ""),
                        p.following = 0
        MERGE (u)-[:HAS_PROFILE]->(p)
    """,
}

def _read_role(s: Session, uid: str) -> str:
    rec = s.run("MATCH (u:User {id:$uid}) RETURN toLower(coalesce(u.role,'')) AS role", uid=uid).single()
    return (rec and rec["role"]) or ""

@router.post("/set-role")
def set_role(
    p: SetRoleIn,
    force: bool = Query(False),
    uid: str = Depends(current_user_id),
    s: Session = Depends(session_dep),
):
    rec = s.run("MATCH (u:User {id:$uid}) RETURN u", uid=uid).single()
    if not rec or not rec.get("u"):
        raise HTTPException(status_code=404, detail="User not found")

    u = rec["u"]
    cur_role = (u.get("role") or "").lower()
    email = (u.get("email") or "").lower()
    is_admin = bool(ADMIN_EMAIL and email == ADMIN_EMAIL.lower())
    req_role = p.role.lower()

    # Always allow switching to 'public'
    if req_role == "public":
        s.run("MATCH (u:User {id:$uid}) SET u.role=$role", uid=uid, role=req_role).consume()
        s.run(ROLE_CREATE_PROFILE_CYPHER[req_role], uid=uid).consume()
        new_role = _read_role(s, uid)
        return {"ok": True, "role": new_role, "prev_role": cur_role, "uid": uid}

    # No-op (already that role)
    if cur_role == req_role:
        if req_role in ROLE_CREATE_PROFILE_CYPHER:
            s.run(ROLE_CREATE_PROFILE_CYPHER[req_role], uid=uid).consume()
        new_role = _read_role(s, uid)
        return {"ok": True, "role": new_role, "prev_role": cur_role, "uid": uid}

    # Neutral/missing → any role is fine (do NOT create BusinessProfile here)
    if cur_role in ("", "user", "public"):
        s.run("MATCH (u:User {id:$uid}) SET u.role=$role", uid=uid, role=req_role).consume()
        if req_role in ROLE_CREATE_PROFILE_CYPHER:
            s.run(ROLE_CREATE_PROFILE_CYPHER[req_role], uid=uid).consume()
        new_role = _read_role(s, uid)
        return {"ok": True, "role": new_role, "prev_role": cur_role, "uid": uid}

    # Specific role → another specific role requires admin+force
    if not (is_admin and force):
        raise HTTPException(status_code=409, detail=f"Account is already '{cur_role}'")

    s.run("MATCH (u:User {id:$uid}) SET u.role=$role", uid=uid, role=req_role).consume()
    if req_role in ROLE_CREATE_PROFILE_CYPHER:
        s.run(ROLE_CREATE_PROFILE_CYPHER[req_role], uid=uid).consume()
    new_role = _read_role(s, uid)
    return {"ok": True, "role": new_role, "prev_role": cur_role, "uid": uid}
