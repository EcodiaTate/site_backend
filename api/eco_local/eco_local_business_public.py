# api/routers/eco-local_business_public.py
from __future__ import annotations
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from typing import Optional, List
from neo4j import Session
from site_backend.core.neo_driver import session_dep

router = APIRouter(prefix="/eco-local/business/public", tags=["eco_local-business-public"])

class BusinessPublicOut(BaseModel):
  id: str
  name: Optional[str] = None
  tagline: Optional[str] = None
  website: Optional[str] = None
  address: Optional[str] = None
  hours: Optional[str] = None
  description: Optional[str] = None
  hero_url: Optional[str] = None
  lat: Optional[float] = None
  lng: Optional[float] = None
  tags: Optional[List[str]] = None

@router.get("/{business_id}", response_model=BusinessPublicOut)
def public_profile(business_id: str, s: Session = Depends(session_dep)):
  rec = s.run("""
    MATCH (b:BusinessProfile {id:$bid})
    RETURN b.id AS id,
           b.name AS name,
           b.tagline AS tagline,
           b.website AS website,
           b.address AS address,
           b.hours AS hours,
           b.description AS description,
           b.hero_url AS hero_url,
           b.lat AS lat,
           b.lng AS lng,
           coalesce(b.tags, []) AS tags
    """, bid=business_id).single()
  if not rec:
    raise HTTPException(status_code=404, detail="Business not found")
  return BusinessPublicOut(**rec.data())
