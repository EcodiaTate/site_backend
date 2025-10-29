# site_backend/api/routers/sidequest_safety.py
from __future__ import annotations

from typing import Optional
from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException, Query, Body, Request
from pydantic import BaseModel, Field
from neo4j import Session

from site_backend.core.neo_driver import session_dep
from site_backend.core.user_guard import current_user_id

router = APIRouter(prefix="/sidequests", tags=["sidequests", "safety"])

# ---------- models ----------

class SafetyAckOut(BaseModel):
    acknowledged: bool
    version: str
    at: Optional[int] = None  # epoch ms of first ack for this version

class SafetyAckPostIn(BaseModel):
    legal_version: str = Field(..., min_length=3, max_length=64)

class SafetyAckPostOut(BaseModel):
    acknowledged: bool = True
    version: str
    at: int  # epoch ms
    message: str = "Acknowledged"

# ---------- helpers ----------

def now_ms() -> int:
    return int(datetime.now(tz=timezone.utc).timestamp() * 1000)

# ---------- GET: has user acknowledged this sidequest's safety for version v? ----------

@router.get("/{sidequest_id}/safety_ack", response_model=SafetyAckOut)
def get_safety_ack(
    sidequest_id: str,
    v: str = Query("safety-v1.0", alias="legal_version"),
    s: Session = Depends(session_dep),
    uid: str = Depends(current_user_id),
):
    """
    Returns whether the current user has acknowledged safety for this sidequest at version `v`.
    Uses a versioned relationship:
        (u:User)-[r:ACKED_SAFETY {version:v}]->(sq:Sidequest)
    """
    rec = s.run(
        """
        MATCH (sq:Sidequest {id:$sid})
        WITH sq
        MATCH (u:User {id:$uid})
        OPTIONAL MATCH (u)-[r:ACKED_SAFETY {version:$v}]->(sq)
        RETURN r.created_at AS created_at
        """,
        sid=sidequest_id, uid=uid, v=v
    ).single()

    if rec is None:
        # If sidequest didn't exist, Neo4j would still return a row with nulls only if MATCH found u.
        # We do an explicit existence check to give a clear 404.
        exists = s.run("MATCH (sq:Sidequest {id:$sid}) RETURN sq LIMIT 1", sid=sidequest_id).single()
        if not exists:
            raise HTTPException(status_code=404, detail="Sidequest not found")
        # User existence is guaranteed by auth guard.

    created_at = rec["created_at"] if rec else None
    return SafetyAckOut(
        acknowledged=created_at is not None,
        version=v,
        at=int(created_at) if created_at is not None else None,
    )

# ---------- POST: record acknowledgement for version v (idempotent) ----------

@router.post("/{sidequest_id}/safety_ack", response_model=SafetyAckPostOut)
def post_safety_ack(
    sidequest_id: str,
    payload: SafetyAckPostIn = Body(...),
    request: Request = None,
    s: Session = Depends(session_dep),
    uid: str = Depends(current_user_id),
):
    """
    Idempotently records the user's acknowledgement for `legal_version`.
    Multiple versions can coexist (new MERGE per version).
    """
    v = payload.legal_version
    now = now_ms()
    ip = None
    try:
        ip = request.client.host if request and request.client else None
    except Exception:
        ip = None

    # Ensure sidequest exists
    exists = s.run("MATCH (sq:Sidequest {id:$sid}) RETURN 1 AS ok LIMIT 1", sid=sidequest_id).single()
    if not exists:
        raise HTTPException(status_code=404, detail="Sidequest not found")

    # MERGE versioned ack relation; set created_at once, updated_at always
    rec = s.run(
        """
        MATCH (u:User {id:$uid}), (sq:Sidequest {id:$sid})
        MERGE (u)-[r:ACKED_SAFETY {version:$v}]->(sq)
        ON CREATE SET
          r.created_at = $now,
          r.updated_at = $now,
          r.count = 1,
          r.ip = $ip
        ON MATCH SET
          r.updated_at = $now,
          r.count = coalesce(r.count,0) + 1,
          r.ip = coalesce($ip, r.ip)
        RETURN r.created_at AS created_at
        """,
        uid=uid, sid=sidequest_id, v=v, now=now, ip=ip
    ).single()

    created_at = int(rec["created_at"])
    return SafetyAckPostOut(acknowledged=True, version=v, at=created_at)
