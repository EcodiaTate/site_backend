# api/eco_local/impact.py
from __future__ import annotations
from typing import Optional, Dict, Any, List

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from pydantic import BaseModel, Field
from neo4j import Session

from site_backend.core.neo_driver import session_dep
from site_backend.core.user_guard import current_user_id
from site_backend.api.services.impact import (
    IMPACT_EVENTS,
    upsert_impact_inputs,
    compute_and_store_bis,
    enable_events_for_business,
    record_scan_mint,
)
from site_backend.api.eco_local.onboard import _resolve_user_business_id

router = APIRouter(prefix="/eco_local/impact", tags=["impact"])

# ----- Models -----
class ImpactInputs(BaseModel):
    practices: Dict[str, Any] = Field(default_factory=dict)     # { energy:0..5, waste:0..5, ... }
    social: Dict[str, Any] = Field(default_factory=dict)        # { inclusive_hiring, youth_support, ... }
    certifications: List[str] = Field(default_factory=list)     # ["b_corp","climate_active",...]
    transparency: Dict[str, Any] = Field(default_factory=dict)  # { impact_report, third_party_audit, ... }

class EnableEventsIn(BaseModel):
    event_keys: List[str]

class ScanIn(BaseModel):
    event_key: str
    youth_id: str
    evidence: Optional[Dict[str, Any]] = None
    device_id: Optional[str] = None

@router.get("/events", response_model=Dict[str, Dict[str, Any]])
def list_event_catalog():
    return IMPACT_EVENTS

@router.post("/inputs", response_model=dict)
def submit_impact_inputs(
    payload: ImpactInputs,
    s: Session = Depends(session_dep),
    user_id: str = Depends(current_user_id),
    business_id: Optional[str] = Query(None),
):
    bid = _resolve_user_business_id(s, user_id, business_id)
    upsert_impact_inputs(
        s,
        business_id=bid,
        practices=payload.practices,
        social=payload.social,
        certifications=payload.certifications,
        transparency=payload.transparency,
    )
    out = compute_and_store_bis(s, business_id=bid)
    return {"ok": True, "business_id": bid, **(out or {})}

@router.post("/enable_events", response_model=dict)
def enable_events(
    payload: EnableEventsIn,
    s: Session = Depends(session_dep),
    user_id: str = Depends(current_user_id),
    business_id: Optional[str] = Query(None),
):
    bid = _resolve_user_business_id(s, user_id, business_id)
    return enable_events_for_business(s, business_id=bid, event_keys=payload.event_keys)

@router.post("/scan", response_model=dict)
def scan_and_mint(
    req: Request,
    payload: ScanIn,
    s: Session = Depends(session_dep),
    user_id: str = Depends(current_user_id),  # business/staff account performing the scan
    business_id: Optional[str] = Query(None),
):
    """
    Records an impact-scoped scan → standardized EcoTx in the unified ledger.
    Service is responsible for: event validation, award calculation, and
    writing (User)-[:EARNED]->(EcoTx)<-[:TRIGGERED]-(BusinessProfile),
    plus optional evidence.
    """
    bid = _resolve_user_business_id(s, user_id, business_id)
    try:
        out = record_scan_mint(
            s,
            business_id=bid,
            youth_id=payload.youth_id,
            event_key=payload.event_key,
            evidence=payload.evidence,
            client_ip=(req.client.host if req.client else None),
            device_id=payload.device_id,
        )
        return out or {"ok": True, "business_id": bid}
    except PermissionError as e:
        raise HTTPException(status_code=403, detail=str(e))
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
