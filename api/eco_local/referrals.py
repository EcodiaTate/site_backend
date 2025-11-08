# api/routers/referrals.py
from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Optional, Dict, Any, Callable

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field
from neo4j import Session, Transaction
from neo4j.exceptions import Neo4jError

from site_backend.core.neo_driver import session_dep

router = APIRouter(prefix="/referrals", tags=["referrals"])

# ---------- models ----------
class SubmitReferralIn(BaseModel):
    youth_id: str = Field(..., description="Your internal user id")
    youth_name: str = Field(..., description="Display name")
    store_name: str
    location: str
    notes: Optional[str] = None

class ReferralRecord(BaseModel):
    id: str
    submitted_at: str
    youth_id: str
    youth_name: str
    store_name: str
    location: str
    notes: Optional[str] = None
    website: Optional[str] = None
    email: Optional[str] = None
    phone: Optional[str] = None
    place_id: Optional[str] = None
    status: str
    outreach_sent_at: Optional[str] = None
    joined_at: Optional[str] = None
    partner_id: Optional[str] = None

class SubmitReferralOut(BaseModel):
    referral: ReferralRecord

# ---------- helpers ----------
def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()

def _record_to_model(r: Dict[str, Any]) -> ReferralRecord:
    return ReferralRecord(
        id=r.get("id"),
        submitted_at=r.get("submitted_at"),
        youth_id=r.get("youth_id"),
        youth_name=r.get("youth_name"),
        store_name=r.get("store_name"),
        location=r.get("location"),
        notes=r.get("notes"),
        website=r.get("website"),
        email=r.get("email"),
        phone=r.get("phone"),
        place_id=r.get("place_id"),
        status=r.get("status", "submitted"),
        outreach_sent_at=r.get("outreach_sent_at"),
        joined_at=r.get("joined_at"),
        partner_id=r.get("partner_id"),
    )

# ---------- cypher ----------
CREATE_WITH_ID_CY = """
MERGE (u:User {id: $youth_id})
ON CREATE SET u.name = $youth_name
WITH u
CREATE (r:YouthReferral {
  id: $rid,
  submitted_at: datetime($submitted_at),
  youth_id: $youth_id,
  youth_name: $youth_name,
  store_name: $store_name,
  location: $location,
  notes: $notes,
  website: NULL,
  email: NULL,
  phone: NULL,
  place_id: NULL,
  status: 'submitted'
})
MERGE (u)-[:REFERRED]->(r)
RETURN 1 AS ok
"""

READ_BY_ID_CY = """
MATCH (r:YouthReferral {id: $id})
RETURN r {
  .*,
  id: r.id,
  submitted_at: toString(r.submitted_at),
  outreach_sent_at: CASE WHEN r.outreach_sent_at IS NOT NULL THEN toString(r.outreach_sent_at) ELSE NULL END,
  joined_at:        CASE WHEN r.joined_at        IS NOT NULL THEN toString(r.joined_at)        ELSE NULL END
} AS r
"""

# ---------- tx helpers ----------
def _write(s: Session, fn: Callable[[Transaction], Any]) -> Any:
    return s.execute_write(lambda tx: fn(tx))

def _read(s: Session, fn: Callable[[Transaction], Any]) -> Any:
    return s.execute_read(lambda tx: fn(tx))

# ---------- endpoints ----------
@router.post("", response_model=SubmitReferralOut, summary="Youth submits a referral")
def create_referral(body: SubmitReferralIn, s: Session = Depends(session_dep)) -> SubmitReferralOut:
    rid = str(uuid.uuid4())
    submitted_at = _iso_now()

    params = {
        "rid": rid,
        "submitted_at": submitted_at,
        "youth_id": body.youth_id.strip(),
        "youth_name": body.youth_name.strip(),
        "store_name": body.store_name.strip(),
        "location": body.location.strip(),
        "notes": (body.notes or None),
    }

    try:
        def _create_only(tx: Transaction):
            result = tx.run(CREATE_WITH_ID_CY, **params)
            # Ensure the query is sent and summarized
            summary = result.consume()
            # We expect at least 1 node created (the YouthReferral)
            if not summary.counters.contains_updates:
                raise RuntimeError("No updates reported by Neo4j")
            return True

        _write(s, _create_only)

        # Build the response from data we just wrote.
        # (We don't depend on an immediate MATCH/RETURN which has been flaky.)
        response = {
            "id": rid,
            "submitted_at": submitted_at,
            "youth_id": params["youth_id"],
            "youth_name": params["youth_name"],
            "store_name": params["store_name"],
            "location": params["location"],
            "notes": params["notes"],
            "website": None,
            "email": None,
            "phone": None,
            "place_id": None,
            "status": "submitted",
            "outreach_sent_at": None,
            "joined_at": None,
            "partner_id": None,
        }
        return SubmitReferralOut(referral=_record_to_model(response))

    except Neo4jError as e:
        raise HTTPException(status_code=500, detail={"error": "neo4j_error", "code": e.code, "msg": str(e)})
    except Exception as e:
        raise HTTPException(status_code=500, detail={"error": "create_failed", "msg": str(e)})

@router.get("/{referral_id}", response_model=SubmitReferralOut, summary="Get a referral by id")
def read_referral(referral_id: str, s: Session = Depends(session_dep)) -> SubmitReferralOut:
    try:
        rec = _read(s, lambda tx: tx.run(READ_BY_ID_CY, id=referral_id).single())
        if not rec or "r" not in rec:
            raise HTTPException(status_code=404, detail="Not found")
        return SubmitReferralOut(referral=_record_to_model(rec["r"]))
    except Neo4jError as e:
        raise HTTPException(status_code=500, detail={"error": "neo4j_error", "code": e.code, "msg": str(e)})
