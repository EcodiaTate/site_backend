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

@router.post("/set-role")
def set_role(
    p: SetRoleIn,
    force: bool = Query(False),
    uid: str = Depends(current_user_id),
    s: Session = Depends(session_dep),
):
    # Load current user
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
        s.run("MATCH (u:User {id:$uid}) SET u.role=$role", uid=uid, role=req_role)
        return {"ok": True, "role": req_role}

    # No-op if unchanged
    if cur_role == req_role:
        return {"ok": True, "role": req_role}

    # Neutral/missing → any role is fine
    if cur_role in ("", "user", "public"):
        s.run("MATCH (u:User {id:$uid}) SET u.role=$role", uid=uid, role=req_role)
        return {"ok": True, "role": req_role}

    # Switching between specific roles requires admin + force
    if not (is_admin and force):
        raise HTTPException(status_code=409, detail=f"Account is already '{cur_role}'")

    s.run("MATCH (u:User {id:$uid}) SET u.role=$role", uid=uid, role=req_role)
    return {"ok": True, "role": req_role}
