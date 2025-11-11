from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Optional, List, Literal, Dict, Any

from fastapi import APIRouter, Depends, HTTPException
from neo4j import Session
from pydantic import BaseModel

from site_backend.core.neo_driver import session_dep

router = APIRouter(prefix="/eco-local/business/public", tags=["eco_local-business-public"])

# ---------------------------------------------------------
# Models
# ---------------------------------------------------------

class BusinessPublicOut(BaseModel):
    id: str
    name: Optional[str] = None
    tagline: Optional[str] = None
    website: Optional[str] = None
    address: Optional[str] = None
    hours: Optional[str] = None
    description: Optional[str] = None
    # media
    hero_url: Optional[str] = None
    avatar_url: Optional[str] = None
    logo_url: Optional[str] = None
    photo_url: Optional[str] = None
    image_small: Optional[str] = None
    # owner hint
    owner_user_id: Optional[str] = None
    # geo + tags
    lat: Optional[float] = None
    lng: Optional[float] = None
    tags: Optional[List[str]] = None
    # optional runtime info
    open_now: Optional[bool] = None

class BusinessPublicStatsOut(BaseModel):
    business_id: str
    eco_collected_total: int
    eco_collected_30d: int
    contributions_30d: int
    last_collected_at: Optional[str] = None  # ISO datetime

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

# ---------------------------------------------------------
# Public business profile (with avatar fields!)
# ---------------------------------------------------------

@router.get("/{business_id}", response_model=BusinessPublicOut)
def public_profile(business_id: str, s: Session = Depends(session_dep)):
    rec = s.run(
        """
        MATCH (b:BusinessProfile {id:$bid})
        // comment out the next line to expose all businesses
        WHERE coalesce(b.visible_on_map, true) = true

        // Optional owner relation fallback for avatar route
        OPTIONAL MATCH (u:User)-[r:OWNS|MANAGES]->(b)

        RETURN
          b.id                         AS id,
          b.name                       AS name,
          b.tagline                    AS tagline,
          b.website                    AS website,
          b.address                    AS address,
          b.hours                      AS hours,
          b.description                AS description,

          // media fields (as stored; frontend will absolutize)
          b.hero_url                   AS hero_url,
          b.avatar_url                 AS avatar_url,
          b.logo_url                   AS logo_url,
          b.photo_url                  AS photo_url,
          b.image_small                AS image_small,

          // if business node doesn’t have owner_user_id, use related user id
          coalesce(b.owner_user_id, u.id) AS owner_user_id,

          b.lat                        AS lat,
          b.lng                        AS lng,
          coalesce(b.tags, [])         AS tags,

          // if you track store hours → expose "open_now" if present
          coalesce(b.open_now, NULL)   AS open_now
        """,
        bid=business_id,
    ).single()

    if not rec:
        raise HTTPException(status_code=404, detail="Business not found")

    return BusinessPublicOut(**rec.data())

# ---------------------------------------------------------
# Public stats (unchanged)
# ---------------------------------------------------------

@router.get("/{business_id}/stats", response_model=BusinessPublicStatsOut)
def business_public_stats(business_id: str, s: Session = Depends(session_dep)):
    end_dt = datetime.now(timezone.utc)
    start_dt = end_dt - timedelta(days=30)

    start_iso = start_dt.isoformat()
    end_iso = end_dt.isoformat()

    rec = s.run(
        """
        MATCH (b:BusinessProfile {id:$bid})

        OPTIONAL MATCH (tx:EcoTx)-[:TO]->(b)
        WHERE coalesce(tx.kind, tx.type) IN ['CONTRIBUTE_TO_BIZ','CONTRIBUTION','BIZ_COLLECT']

        WITH b, tx,
             toInteger(coalesce(tx.amount, tx.eco, 0)) AS eco_val,
             CASE
               WHEN tx.at IS NOT NULL THEN tx.at
               WHEN tx.createdAt IS NOT NULL THEN datetime({epochMillis: toInteger(tx.createdAt)})
               WHEN tx.created_at IS NOT NULL THEN datetime(tx.created_at)
               ELSE datetime.transaction()
             END AS tat

        WITH b,
             sum(eco_val) AS collected_total,
             max(tat)     AS lastTat,
             collect({tat: tat, eco: eco_val}) AS alltx

        WITH b, collected_total, lastTat,
             [t IN alltx WHERE t.tat >= datetime($start_iso) AND t.tat < datetime($end_iso)] AS last30

        RETURN
          toInteger(coalesce(b.eco_collected_total, collected_total, 0)) AS eco_collected_total,
          toInteger(reduce(s=0, x IN last30 | s + toInteger(x.eco)))     AS eco_collected_30d,
          toInteger(size(last30))                                         AS contributions_30d,
          CASE WHEN lastTat IS NULL THEN NULL ELSE toString(lastTat) END  AS last_collected_at
        """,
        {"bid": business_id, "start_iso": start_iso, "end_iso": end_iso},
    ).single()

    if not rec:
        raise HTTPException(status_code=404, detail="Business not found")

    return BusinessPublicStatsOut(
        business_id=business_id,
        eco_collected_total=int(rec["eco_collected_total"] or 0),
        eco_collected_30d=int(rec["eco_collected_30d"] or 0),
        contributions_30d=int(rec["contributions_30d"] or 0),
        last_collected_at=rec["last_collected_at"],
    )

# ---------------------------------------------------------
# Public offers (unchanged)
# ---------------------------------------------------------

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
        o.setdefault("status", "active")
        if o.get("business_id") is None:
            o["business_id"] = business_id
        out.append(OfferPublicOut(**o))
    return out
