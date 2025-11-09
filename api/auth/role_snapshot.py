# site_backend/api/auth/role_snapshot.py
from __future__ import annotations

from typing import Optional, Literal, Any
from fastapi import APIRouter, Depends, Body, HTTPException, status, Request, Response
from pydantic import BaseModel
from neo4j import Session

from site_backend.core.neo_driver import session_dep
from site_backend.core.user_guard import current_user_id

Role = Literal["youth", "business", "creative", "partner", "public"]
NON_PUBLIC: set[str] = {"youth", "business", "creative", "partner"}

router = APIRouter(tags=["auth"])

ROLE_DEFAULT_CAPS: dict[str, dict[str, Any]] = {
    "youth": {"max_redemptions_per_week": 5, "streak_bonus_enabled": True},
    "business": {"max_active_offers": 3, "can_issue_qr": True},
    "creative": {"max_active_collabs": 3},
    "partner": {"max_workspaces": 2},
    "public": {},
}

class RoleSnapshotIn(BaseModel):
    # Optional hint from FE; ignored if it would *downgrade*
    role_hint: Optional[Role] = None

@router.post("/role-snapshot")
def role_snapshot(
    request: Request,
    response: Response,
    s: Session = Depends(session_dep),
    uid: str = Depends(current_user_id),
    p: Optional[RoleSnapshotIn] = Body(default=None),   # ← tolerate {} or no body
):
    # 1) Load user + attached profile presence
    rec = s.run(
        """
        MATCH (u:User {id:$uid})
        OPTIONAL MATCH (u)-[:HAS_PROFILE]->(yp:YouthProfile)
        OPTIONAL MATCH (u)-[:HAS_PROFILE]->(cp:CreativeProfile)
        OPTIONAL MATCH (u)-[:HAS_PROFILE]->(pp:PartnerProfile)
        OPTIONAL MATCH (u)-[:HAS_PROFILE]->(bp:BusinessProfile)
        OPTIONAL MATCH (u)-[:OWNS|:MANAGES]->(bOwned:BusinessProfile)
        WITH u,
             yp IS NOT NULL AS has_yp,
             cp IS NOT NULL AS has_cp,
             pp IS NOT NULL AS has_pp,
             (bp IS NOT NULL OR bOwned IS NOT NULL) AS has_bp
        RETURN u, has_yp, has_cp, has_pp, has_bp
        """,
        {"uid": uid},
    ).single()

    if not rec or not rec.get("u"):
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="User not found")

    u = rec["u"]
    cur_role: Role = (u.get("role") or "public").lower()  # type: ignore
    has_bp = bool(rec["has_bp"])
    has_yp = bool(rec["has_yp"])
    has_cp = bool(rec["has_cp"])
    has_pp = bool(rec["has_pp"])

    # 2) Infer best role from DB attachments (no admin handling here)
    inferred: Role = (
        "business" if has_bp
        else "youth" if has_yp
        else "creative" if has_cp
        else "partner" if has_pp
        else "public"
    )

    # 3) Apply optional hint (never downgrade non-public → public)
    hint: Optional[Role] = (p.role_hint if p else None)
    if hint and hint in NON_PUBLIC:
        # If current is public and inferred is public, accept hint
        if cur_role == "public" and inferred == "public":
            inferred = hint
        # If current/inferred are non-public, we choose the "stronger" one (keep business > others)
        elif inferred == "public" and cur_role in NON_PUBLIC:
            inferred = cur_role  # keep current non-public
        elif inferred in NON_PUBLIC and cur_role == "public":
            # already good; leave inferred
            pass
        # If both non-public, prefer business, else keep inferred
        elif inferred in NON_PUBLIC and hint in NON_PUBLIC:
            if hint == "business":
                inferred = "business"

    # Never downgrade an existing non-public current role to public
    if cur_role in NON_PUBLIC and inferred == "public":
        inferred = cur_role

    # 4) Persist if changed
    did_update = False
    if inferred != cur_role:
        s.run("MATCH (u:User {id:$uid}) SET u.role=$role", uid=uid, role=inferred)
        did_update = True

    # 5) Return a compact summary for FE
    caps = ROLE_DEFAULT_CAPS.get(inferred, {})
    return {
        "id": u["id"],
        "role": inferred,
        "did_update": did_update,
        "caps": caps,
    }
