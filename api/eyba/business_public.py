from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Optional, List

from fastapi import APIRouter, Depends, HTTPException
from neo4j import Session
from pydantic import BaseModel

from site_backend.core.neo_driver import session_dep

router = APIRouter(prefix="/eyba/business/public", tags=["eyba-business-public"])

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
    rec = s.run(
        """
        MATCH (b:BusinessProfile {id:$bid})
        WHERE coalesce(b.visible_on_map, true) = true   // remove this line if you want to expose all
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
        """,
        bid=business_id,
    ).single()
    if not rec:
        raise HTTPException(status_code=404, detail="Business not found")
    return BusinessPublicOut(**rec.data())

# ---------- Public stats for a business ----------
class BusinessPublicStatsOut(BaseModel):
    business_id: str
    minted_eco: int
    eco_contributed_total: int
    eco_given_total: int
    claims_30d: int
    minted_30d: int
    last_tx_at: Optional[str] = None  # ISO string if any

@router.get("/{business_id}/stats", response_model=BusinessPublicStatsOut)
def business_public_stats(business_id: str, s: Session = Depends(session_dep)):
    end_dt = datetime.now(timezone.utc)
    start_dt = end_dt - timedelta(days=30)
    start_iso = start_dt.isoformat()
    end_iso = end_dt.isoformat()
    start_ms = int(start_dt.timestamp() * 1000)
    end_ms = int(end_dt.timestamp() * 1000)

    rec = s.run(
        """
        MATCH (b:BusinessProfile {id:$bid})
        OPTIONAL MATCH (b)-[:TRIGGERED]->(t:EcoTx)
        WITH b, t,
             toInteger(coalesce(t.amount, t.eco, 0)) AS eco_val,
             CASE
               WHEN t.at IS NOT NULL THEN t.at
               ELSE datetime({epochMillis: toInteger(t.createdAt)})
             END AS tat
        WITH b,
             sum(eco_val) AS minted_total,
             collect(t) AS txs,
             [tx IN collect({tat: tat, eco: eco_val})
                WHERE tat >= datetime($start_iso) AND tat < datetime($end_iso)] AS last30
        RETURN
          toInteger(coalesce(b.minted_eco, minted_total, 0)) AS minted_eco,
          toInteger(coalesce(b.eco_contributed_total,0)) AS eco_contributed_total,
          toInteger(coalesce(b.eco_given_total,0)) AS eco_given_total,
          toInteger(reduce(s=0, x IN last30 | s + toInteger(x.eco))) AS minted_30d,
          toInteger(size(last30)) AS claims_30d,
          CASE WHEN size(txs) > 0
            THEN toString( max( coalesce(t.at, datetime({epochMillis: toInteger(t.createdAt)})) ) )
            ELSE NULL
          END AS last_tx_at
        """,
        {"bid": business_id, "start_iso": start_iso, "end_iso": end_iso, "start_ms": start_ms, "end_ms": end_ms},
    ).single()

    if not rec:
        raise HTTPException(status_code=404, detail="Business not found")

    return BusinessPublicStatsOut(
        business_id=business_id,
        minted_eco=int(rec["minted_eco"] or 0),
        eco_contributed_total=int(rec["eco_contributed_total"] or 0),
        eco_given_total=int(rec["eco_given_total"] or 0),
        minted_30d=int(rec["minted_30d"] or 0),
        claims_30d=int(rec["claims_30d"] or 0),
        last_tx_at=rec["last_tx_at"],
    )


# ---------- Public offers for a business ----------
from typing import Literal, Dict, Any  # add to imports if missing

OfferStatus = Literal["active", "paused", "hidden"]

class OfferPublicOut(BaseModel):
    id: str
    business_id: str
    title: str
    blurb: Optional[str] = None
    status: OfferStatus = "active"
    eco_price: Optional[int] = None
    fiat_cost_cents: Optional[int] = None
    stock: Optional[int] = None
    url: Optional[str] = None
    valid_until: Optional[str] = None
    tags: Optional[List[str]] = None
    createdAt: Optional[int] = None

@router.get("/{business_id}/offers", response_model=List[OfferPublicOut])
def public_offers_for_business(business_id: str, s: Session = Depends(session_dep)):
    recs = s.run(
        """
        MATCH (b:BusinessProfile {id:$bid})
        WHERE coalesce(b.visible_on_map, true) = true
        MATCH (o:Offer)-[:OF]->(b)
        WHERE o.status = 'active'
          AND (o.stock IS NULL OR o.stock > 0)
          AND (o.valid_until IS NULL OR date(o.valid_until) >= date())
        RETURN o{
          .id, .title, .blurb, .status, .eco_price, .fiat_cost_cents, .stock,
          .url, .valid_until, .tags, .createdAt,
          business_id: $bid
        } AS offer
        ORDER BY coalesce(o.updated_at, o.created_at) DESC
        """,
        bid=business_id,
    )
    out: List[OfferPublicOut] = []
    for r in recs:
        o: Dict[str, Any] = dict(r["offer"])
        # keep shape stable even if missing
        o.setdefault("status", "active")
        out.append(OfferPublicOut(**o))
    return out
