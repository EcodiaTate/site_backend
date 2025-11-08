from __future__ import annotations
from typing import Literal

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field
from neo4j import Session

from site_backend.core.neo_driver import session_dep
from site_backend.core.user_guard import current_user_id
from site_backend.core.admin_guard import ADMIN_EMAIL  # 👈 add

router = APIRouter(tags=["auth"])

# Unified role literal
RoleLiteral = Literal["youth", "business", "creative", "partner", "public"]

class SetRoleIn(BaseModel):
    role: RoleLiteral = Field(...)

ROLE_CREATE_PROFILE_CYPHER: dict[str, str] = {
    "youth": """
        MATCH (u:User {id:$uid})
        MERGE (p:YouthProfile { user_id:$uid })
          ON CREATE SET p.birth_year = coalesce(p.birth_year, 2006),
                        p.eco_local_points = 0,
                        p.actions_completed = 0
        MERGE (u)-[:HAS_PROFILE]->(p)
    """,
    "business": """
        MATCH (u:User {id:$uid})
        MERGE (p:BusinessProfile { user_id:$uid })
          ON CREATE SET p.store_name = coalesce(p.store_name, ""),
                        p.pay_model = coalesce(p.pay_model, "pwyw"),
                        p.pledge = coalesce(p.pledge, 0),
                        p.eco_score = coalesce(p.eco_score, 0)
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

@router.post("/set-role")
def set_role(
    p: SetRoleIn,
    force: bool = Query(False),                       # 👈 new: admin override
    uid: str = Depends(current_user_id),
    s: Session = Depends(session_dep),
):
    # Fetch current user + email + role
    cy_get = """
    MATCH (u:User {id:$uid})
    RETURN u
    """
    rec = s.run(cy_get, uid=uid).single()
    if not rec or not rec.get("u"):
        raise HTTPException(status_code=404, detail="User not found")

    u = rec["u"]
    cur_role = (u.get("role") or "").lower()
    email = (u.get("email") or "").lower()
    is_admin = bool(ADMIN_EMAIL and email == ADMIN_EMAIL.lower())

    req_role = p.role.lower()

    # Always allow switching to 'public'
    if req_role == "public":
        s.run("MATCH (u:User {id:$uid}) SET u.role=$role", uid=uid, role=req_role)
        s.run(ROLE_CREATE_PROFILE_CYPHER[req_role], uid=uid)
        return {"ok": True, "role": req_role}

    # No-op (already that role)
    if cur_role == req_role:
        s.run(ROLE_CREATE_PROFILE_CYPHER[req_role], uid=uid)  # ensure profile exists
        return {"ok": True, "role": req_role}

    # Neutral/missing → any role is fine
    if cur_role in ("", "user", "public"):
        s.run("MATCH (u:User {id:$uid}) SET u.role=$role", uid=uid, role=req_role)
        s.run(ROLE_CREATE_PROFILE_CYPHER[req_role], uid=uid)
        return {"ok": True, "role": req_role}

    # Specific role → another specific role:
    # - allow only if admin & force=true
    if not (is_admin and force):
        raise HTTPException(status_code=409, detail=f"Account is already '{cur_role}'")

    # Admin-forced flip
    s.run("MATCH (u:User {id:$uid}) SET u.role=$role", uid=uid, role=req_role)
    s.run(ROLE_CREATE_PROFILE_CYPHER[req_role], uid=uid)
    return {"ok": True, "role": req_role}
