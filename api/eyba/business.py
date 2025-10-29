# api/eyba/business.py
from __future__ import annotations

from typing import Optional, List, Dict, Any

from fastapi import APIRouter, Depends, HTTPException, Query, Body, status
from pydantic import BaseModel, Field
from neo4j import Session
import secrets
import string

from site_backend.core.neo_driver import session_dep
from site_backend.core.user_guard import current_user_id
from site_backend.api.services.neo_business import (
    business_by_owner,
    business_update_public_profile,
)

router = APIRouter(prefix="/eyba/business", tags=["eyba-business"])

# --------- Schemas ----------
class BusinessMineOut(BaseModel):
    id: str
    name: Optional[str] = None
    industry_group: Optional[str] = None
    size: Optional[str] = None
    area: Optional[str] = None
    pledge_tier: Optional[str] = None
    website: Optional[str] = None
    tagline: Optional[str] = None
    address: Optional[str] = None
    hours: Optional[str] = None
    description: Optional[str] = None
    hero_url: Optional[str] = None
    lat: Optional[float] = None
    lng: Optional[float] = None
    visible_on_map: bool = True
    tags: Optional[List[str]] = None
    qr_code: Optional[str] = None

class BusinessProfilePatch(BaseModel):
    name: Optional[str] = Field(None, min_length=2, max_length=120)
    tagline: Optional[str] = Field(None, min_length=0, max_length=160)
    website: Optional[str] = None
    address: Optional[str] = None
    hours: Optional[str] = None
    description: Optional[str] = None
    hero_url: Optional[str] = None
    lat: Optional[float] = Field(None, ge=-90, le=90)
    lng: Optional[float] = Field(None, ge=-180, le=180)
    visible_on_map: Optional[bool] = None
    tags: Optional[List[str]] = None

class MetricsOut(BaseModel):
    business_id: str
    name: Optional[str] = None

    sponsor_balance_cents: int = 0

    eco_retired_total: int = 0
    eco_retired_30d: int = 0
    redemptions_30d: int = 0
    unique_claimants_30d: int = 0

    minted_eco_30d: int = 0  # optional: actions earned at/for this business

class ActivityRow(BaseModel):
    id: str
    kind: str                  # MINT_ACTION | BURN_REWARD | SPONSOR_DEPOSIT | SPONSOR_PAYOUT
    source: Optional[str] = None
    amount: int
    createdAt: int
    user_id: Optional[str] = None
    offer_id: Optional[str] = None


# --------- Helpers ----------
def _new_qr_code(s: Session) -> str:
    # 10 url-safe chars; ensure unique
    alphabet = string.ascii_uppercase + string.digits
    for _ in range(20):
        code = "".join(secrets.choice(alphabet) for _ in range(10))
        if not s.run("MATCH (q:QR {code:$c}) RETURN q", c=code).single():
            return code
    raise HTTPException(500, "Failed to allocate QR code")

def _ensure_qr_for_business(s: Session, *, bid: str) -> str:
    rec = s.run(
        """
        MATCH (bp:BusinessProfile {id:$bid})
        OPTIONAL MATCH (q:QR)-[:OF]->(bp)
        RETURN q.code AS code
        """,
        bid=bid
    ).single()
    if rec and rec["code"]:
        return rec["code"]
    # create new code
    code = _new_qr_code(s)
    s.run(
        """
        MATCH (bp:BusinessProfile {id:$bid})
        MERGE (q:QR {code:$code})
        MERGE (q)-[:OF]->(bp)
        """,
        bid=bid, code=code
    )
    return code

def _new_business_id() -> str:
    # biz_ + 12 hex
    return "biz_" + secrets.token_hex(6)

@router.get("/onboarding_status")
def onboarding_status(
    s: Session = Depends(session_dep),
    user_id: str = Depends(current_user_id),
    business_id: str | None = Query(None)
):
    rec = s.run(
        """
        MATCH (u:User {id:$uid})-[r]->(b:BusinessProfile)
        WHERE type(r) IN ['OWNS','MANAGES','HAS_PROFILE']
          AND ($bid IS NULL OR b.id = $bid)
        OPTIONAL MATCH (q:QR)-[:OF]->(b)
        RETURN b.id AS bid, b.name AS name, q.code AS qcode, b.hero_url AS hero
        ORDER BY bid
        LIMIT 1
        """,
        uid=user_id, bid=business_id
    ).single()
    if not rec:
        raise HTTPException(404, "No business found")

    # Simple criteria: has name + QR; tweak as you wish (hero, area, etc.)
    completed = bool((rec.get("name") or "").strip()) and bool(rec.get("qcode"))
    return {"business_id": rec["bid"], "qr_code": rec["qcode"], "completed": completed}

@router.get("/mine", response_model=BusinessMineOut)
def get_mine(
    s: Session = Depends(session_dep),
    uid: str = Depends(current_user_id),
):
    b = business_by_owner(s, user_id=uid)
    if not b:
        raise HTTPException(status_code=404, detail="No business found for this account")

    qr = s.run(
        """
        MATCH (u:User {id:$uid})-[r]->(b:BusinessProfile)
        WHERE type(r) IN ['OWNS','MANAGES']
        OPTIONAL MATCH (q:QR)-[:OF]->(b)
        RETURN q.code AS code
        LIMIT 1
        """,
        uid=uid
    ).single()
    qr_code = qr["code"] if qr else None

    return BusinessMineOut(**{**b, "qr_code": qr_code})


@router.patch("/profile", response_model=BusinessMineOut)
def patch_profile(
    patch: BusinessProfilePatch,
    s: Session = Depends(session_dep),
    uid: str = Depends(current_user_id),
):
    b = business_by_owner(s, user_id=uid)
    if not b:
        raise HTTPException(status_code=404, detail="No business found for this account")
    out = business_update_public_profile(
        s,
        business_id=b["id"],
        owner_user_id=uid,
        fields={k: v for k, v in patch.model_dump(exclude_unset=True).items()}
    )
    merged = {**b, **out}

    # ensure QR exists (idempotent, APOC-free)
    merged["qr_code"] = _ensure_qr_for_business(s, bid=merged["id"])
    return BusinessMineOut(**merged)


@router.get("/metrics", response_model=MetricsOut)
def metrics(
    s: Session = Depends(session_dep),
    uid: str = Depends(current_user_id),
):
    """
    Retirements-first analytics:
      - ECO retired via this business' offers (burns linked to offers of this business)
      - Redemptions, unique claimants (30d)
      - Sponsor wallet balance
      - Optional minted_eco_30d (actions earned at/for this business)
    """
    b = business_by_owner(s, user_id=uid)
    if not b:
        raise HTTPException(status_code=404, detail="No business found for this account")

    from datetime import datetime, timedelta, timezone
    since_ms = int((datetime.now(tz=timezone.utc) - timedelta(days=30)).timestamp() * 1000)

    rec = s.run(
        """
        MATCH (b:BusinessProfile {id:$bid})

        OPTIONAL MATCH (b)<-[:OF]-(o:Offer)<-[:FOR_OFFER]-(t:EcoTx {kind:'BURN_REWARD', status:'settled'})
        WITH b, sum(coalesce(t.amount,0)) AS eco_retired_total

        OPTIONAL MATCH (b)<-[:OF]-(o2:Offer)<-[:FOR_OFFER]-(t2:EcoTx {kind:'BURN_REWARD', status:'settled'})
        WHERE t2.createdAt >= $since
        WITH b, eco_retired_total,
             sum(coalesce(t2.amount,0)) AS eco_retired_30d,
             count(t2) AS redemptions_30d

        OPTIONAL MATCH (b)<-[:OF]-(o3:Offer)<-[:FOR_OFFER]-(t3:EcoTx {kind:'BURN_REWARD', status:'settled'})<-[:SPENT]-(u3:User)
        WHERE t3.createdAt >= $since
        WITH b, eco_retired_total, eco_retired_30d, redemptions_30d, count(DISTINCT u3) AS unique_claimants_30d

        OPTIONAL MATCH (t4:EcoTx {kind:'MINT_ACTION', status:'settled'})-[:AT|:FOR]->(b)
        WHERE t4.createdAt >= $since
        RETURN
          b.id AS bid,
          b.name AS name,
          toInteger(coalesce(b.sponsor_balance_cents,0)) AS sponsor_balance_cents,
          toInteger(eco_retired_total) AS eco_retired_total,
          toInteger(eco_retired_30d) AS eco_retired_30d,
          toInteger(redemptions_30d) AS redemptions_30d,
          toInteger(unique_claimants_30d) AS unique_claimants_30d,
          toInteger(coalesce(sum(t4.amount),0)) AS minted_eco_30d
        """,
        bid=b["id"], since=since_ms
    ).single()

    if not rec:
        raise HTTPException(status_code=404, detail="Business not found")

    return MetricsOut(
        business_id=rec["bid"],
        name=rec.get("name"),
        sponsor_balance_cents=int(rec.get("sponsor_balance_cents") or 0),
        eco_retired_total=int(rec.get("eco_retired_total") or 0),
        eco_retired_30d=int(rec.get("eco_retired_30d") or 0),
        redemptions_30d=int(rec.get("redemptions_30d") or 0),
        unique_claimants_30d=int(rec.get("unique_claimants_30d") or 0),
        minted_eco_30d=int(rec.get("minted_eco_30d") or 0),
    )

@router.get("/activity", response_model=List[ActivityRow])
def activity(
    limit: int = Query(50, ge=1, le=500),
    s: Session = Depends(session_dep),
    uid: str = Depends(current_user_id),
):
    """
    Unified activity feed for a business:
      - MINT_ACTION earned at/for this business (via [:AT] or [:FOR])
      - BURN_REWARD redemptions attached to offers of this business
      - SPONSOR_DEPOSIT / SPONSOR_PAYOUT wallet events
    """
    b = business_by_owner(s, user_id=uid)
    if not b:
        raise HTTPException(status_code=404, detail="No business found for this account")

    rows = s.run(
        """
        MATCH (t:EcoTx {kind:'MINT_ACTION', status:'settled'})-[:AT|:FOR]->(b:BusinessProfile {id:$bid})
        OPTIONAL MATCH (u:User)-[:EARNED]->(t)
        WITH collect({
          id: t.id, kind: t.kind, source: t.source, amount: toInteger(t.amount),
          createdAt: toInteger(t.createdAt), user_id: u.id, offer_id: null
        }) AS a

        MATCH (t2:EcoTx {kind:'BURN_REWARD', status:'settled'})-[:FOR_OFFER]->(o:Offer)-[:OF]->(b2:BusinessProfile {id:$bid})
        OPTIONAL MATCH (u2:User)-[:SPENT]->(t2)
        WITH a + collect({
          id: t2.id, kind: t2.kind, source: t2.source, amount: toInteger(t2.amount),
          createdAt: toInteger(t2.createdAt), user_id: u2.id, offer_id: o.id
        }) AS a

        MATCH (b3:BusinessProfile {id:$bid})
        OPTIONAL MATCH (b3)-[:FUNDED]->(td:EcoTx {kind:'SPONSOR_DEPOSIT'})
        WITH a + collect({
          id: td.id, kind: td.kind, source: td.source, amount: toInteger(td.amount),
          createdAt: toInteger(td.createdAt), user_id: null, offer_id: null
        }) AS a, b3
        OPTIONAL MATCH (b3)-[:PAID]->(tp:EcoTx {kind:'SPONSOR_PAYOUT'})
        WITH a + collect({
          id: tp.id, kind: tp.kind, source: tp.source, amount: toInteger(tp.amount),
          createdAt: toInteger(tp.createdAt), user_id: null, offer_id: null
        }) AS a

        UNWIND a AS row
        WITH row
        WHERE row.id IS NOT NULL
        ORDER BY row.createdAt DESC
        LIMIT $limit
        RETURN row
        """,
        bid=b["id"], limit=limit,
    ).data() or []

    return [
        ActivityRow(
            id=r["row"]["id"],
            kind=r["row"]["kind"],
            source=r["row"].get("source"),
            amount=int(r["row"]["amount"] or 0),
            createdAt=int(r["row"]["createdAt"] or 0),
            user_id=r["row"].get("user_id"),
            offer_id=r["row"].get("offer_id"),
        )
        for r in rows
    ]
