# site_backend/api/eco-local/owner.py
from __future__ import annotations

from typing import List, Optional, Literal, Any, Dict
from uuid import uuid4
from datetime import datetime, timezone, timedelta

from fastapi import APIRouter, Depends, HTTPException, UploadFile, File, status
from pydantic import BaseModel
import os
import shutil
import json  # still here in case you reintroduce templates/criteria later

from site_backend.core.neo_driver import session_dep   # yields a neo4j.Session
from site_backend.core.user_guard import current_user_id  # validates Bearer or legacy cookie
from neo4j import Session

# ─────────────────────────────────────────────────────────────────────────────
# Offer models – aligned with new offers setup (eco_price, fiat, stock, status)
# ─────────────────────────────────────────────────────────────────────────────

OfferStatus = Literal["active", "paused", "hidden"]


class OfferOut(BaseModel):
    id: str
    title: str
    blurb: Optional[str] = None
    status: OfferStatus = "active"
    eco_price: int = 0
    fiat_cost_cents: int = 0
    stock: Optional[int] = None
    url: Optional[str] = None
    # stored as string date (YYYY-MM-DD or ISO) from graph
    valid_until: Optional[str] = None
    tags: List[str] = []


class OfferIn(BaseModel):
    title: str
    blurb: str
    status: OfferStatus = "active"
    eco_price: int = 0
    fiat_cost_cents: int = 0
    stock: Optional[int] = None
    url: Optional[str] = None
    # FE sends a plain date string; we just persist it as-is
    valid_until: Optional[str] = None
    tags: List[str] = []


# ─────────────────────────────────────────────────────────────────────────────
# Business + metrics models (unchanged)
# ─────────────────────────────────────────────────────────────────────────────

class BusinessMine(BaseModel):
    id: str
    name: Optional[str] = None
    tagline: Optional[str] = None
    website: Optional[str] = None
    address: Optional[str] = None
    hours: Optional[str] = None  # JSON string of HoursMap (graph stores as map; we surface string)
    description: Optional[str] = None
    hero_url: Optional[str] = None
    lat: Optional[float] = None
    lng: Optional[float] = None
    visible_on_map: bool = True
    tags: Optional[List[str]] = None
    qr_code: Optional[str] = None

    # Rule/config fields used by FE autosave
    pledge_tier: Optional[Literal["starter", "builder", "leader"]] = None
    rules_first_visit: Optional[int] = None
    rules_return_visit: Optional[int] = None
    rules_cooldown_hours: Optional[int] = None
    rules_daily_cap_per_user: Optional[int] = None
    rules_geofence_radius_m: Optional[int] = None


class Metrics(BaseModel):
    minted_eco: float = 0
    eco_contributed_total: float = 0
    eco_given_total: float = 0
    eco_velocity_30d: float = 0


class ActivityRow(BaseModel):
    id: str
    createdAt: str
    user_id: Optional[str] = None
    kind: str
    amount: float
    offer_id: Optional[str] = None


class PatchProfile(BaseModel):
    # Existing editable fields
    name: Optional[str] = None
    tagline: Optional[str] = None
    website: Optional[str] = None
    address: Optional[str] = None
    hours: Optional[str] = None
    description: Optional[str] = None
    hero_url: Optional[str] = None
    lat: Optional[float] = None
    lng: Optional[float] = None
    visible_on_map: Optional[bool] = None
    tags: Optional[List[str]] = None

    # Accept rule/config fields from FE to avoid 422
    pledge_tier: Optional[Literal["starter", "builder", "leader"]] = None
    rules_first_visit: Optional[int] = None
    rules_return_visit: Optional[int] = None
    rules_cooldown_hours: Optional[int] = None
    rules_daily_cap_per_user: Optional[int] = None
    # FE may send "" to disable; normalize in handler
    rules_geofence_radius_m: Optional[int] = None


router = APIRouter(prefix="/eco-local/owner", tags=["eco_local.owner"])
assets_router = APIRouter(prefix="/eco-local/assets", tags=["eco_local.assets"])

# Where to drop hero files (served by your StaticFiles mount)
UPLOAD_DIR = os.getenv("ECO_LOCAL_UPLOAD_DIR", "uploads/hero")

# ─────────────────────────────────────────────────────────────────────────────
# Small helpers for Neo4j session access
# ─────────────────────────────────────────────────────────────────────────────

def _one(s: Session, cypher: str, params: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    rec = s.run(cypher, **params).single()
    return rec.data() if rec else None


def _all(s: Session, cypher: str, params: Dict[str, Any]) -> List[Dict[str, Any]]:
    return [r.data() for r in s.run(cypher, **params)]

# ─────────────────────────────────────────────────────────────────────────────
# Graph helpers - aligned to your constraints
# ─────────────────────────────────────────────────────────────────────────────

def _ensure_owner_business(s: Session, user_id: str) -> str:
    """
    Ensure a BusinessProfile exists for this user_id; return business_id.
    """
    cy = """
    MERGE (b:BusinessProfile {user_id: $uid})
      ON CREATE SET
        b.id = coalesce(b.id, randomUUID()),
        b.visible_on_map = true,
        b.created_at = datetime()
    WITH b
    MERGE (u:User {id: $uid})
    MERGE (u)-[:OWNS]->(b)
    RETURN b.id AS id
    """
    rec = _one(s, cy, {"uid": user_id})
    return rec["id"]


def _get_business(s: Session, user_id: str) -> Optional[BusinessMine]:
    cy = """
    MATCH (b:BusinessProfile {user_id: $uid})
    OPTIONAL MATCH (q:QR)-[:OF]->(b)
    RETURN b {
      .id, .name, .tagline, .website, .address, .hours, .description,
      .hero_url, .lat, .lng, .visible_on_map, .tags,
      .pledge_tier, .rules_first_visit, .rules_return_visit, .rules_cooldown_hours,
      .rules_daily_cap_per_user, .rules_geofence_radius_m
    } AS b, q.code AS qr
    """
    rec = _one(s, cy, {"uid": user_id})
    if not rec:
        return None
    b = rec.get("b") or {}
    return BusinessMine(
        id=b.get("id"),
        name=b.get("name"),
        tagline=b.get("tagline"),
        website=b.get("website"),
        address=b.get("address"),
        hours=b.get("hours"),
        description=b.get("description"),
        hero_url=b.get("hero_url"),
        lat=b.get("lat"),
        lng=b.get("lng"),
        visible_on_map=b.get("visible_on_map", True),
        tags=b.get("tags") or [],
        qr_code=rec.get("qr"),
        pledge_tier=b.get("pledge_tier"),
        rules_first_visit=b.get("rules_first_visit"),
        rules_return_visit=b.get("rules_return_visit"),
        rules_cooldown_hours=b.get("rules_cooldown_hours"),
        rules_daily_cap_per_user=b.get("rules_daily_cap_per_user"),
        rules_geofence_radius_m=b.get("rules_geofence_radius_m"),
    )


def _offer_record_to_out(rec: Dict[str, Any]) -> OfferOut:
    """
    Normalise graph Offer → API shape.
    - Map legacy `visible` → `status` when `status` missing.
    - Map legacy `redeem_eco` → `eco_price` when needed.
    """
    o = rec["o"]

    status = o.get("status")
    visible = o.get("visible")
    if not status:
        # Legacy mapping: visible True -> active, False -> hidden
        status = "active" if (visible is None or visible is True) else "hidden"

    eco_price = o.get("eco_price")
    if eco_price is None and o.get("redeem_eco") is not None:
        eco_price = int(o["redeem_eco"])

    fiat_cost = o.get("fiat_cost_cents") or 0

    return OfferOut(
        id=o.get("id"),
        title=o.get("title") or "",
        blurb=o.get("blurb"),
        status=status,  # type: ignore[arg-type]
        eco_price=int(eco_price or 0),
        fiat_cost_cents=int(fiat_cost),
        stock=o.get("stock"),
        url=o.get("url"),
        valid_until=o.get("valid_until"),
        tags=o.get("tags") or [],
    )

# ─────────────────────────────────────────────────────────────────────────────
# Endpoints
# ─────────────────────────────────────────────────────────────────────────────

@router.get("/mine")
def owner_mine(
    uid: str = Depends(current_user_id),
    s: Session = Depends(session_dep),
):
    cy = """
    MATCH (b:BusinessProfile {user_id: $uid})
    OPTIONAL MATCH (q:QR)-[:OF]->(b)
    RETURN {
      id: b.id,
      name: b.name,
      tagline: coalesce(b.tagline, ''),
      website: coalesce(b.website, ''),
      address: coalesce(b.address, ''),
      hours: coalesce(b.hours, {}),
      description: coalesce(b.description, ''),
      hero_url: coalesce(b.hero_url, ''),
      lat: coalesce(b.lat, 0.0),
      lng: coalesce(b.lng, 0.0),
      visible_on_map: coalesce(b.visible_on_map, true),
      tags: coalesce(b.tags, []),
      pledge_tier: coalesce(b.pledge_tier, 'starter'),
      rules_first_visit: coalesce(b.rules_first_visit, ''),
      rules_return_visit: coalesce(b.rules_return_visit, ''),
      rules_cooldown_hours: toInteger(coalesce(b.rules_cooldown_hours, 0)),
      rules_daily_cap_per_user: toInteger(coalesce(b.rules_daily_cap_per_user, 0)),
      rules_geofence_radius_m: toInteger(coalesce(b.rules_geofence_radius_m, 0)),
      qr: q.code
    } AS result
    """
    rec = s.run(cy, uid=uid).single()
    if not rec:
        return {"result": None}
    return rec["result"]


@router.get("/metrics", response_model=Dict[str, Any])
def get_metrics(
    user_id: str = Depends(current_user_id),
    s: Session = Depends(session_dep),
):
    _ensure_owner_business(s, user_id)

    cy = """
    MATCH (b:BusinessProfile {user_id:$uid})
    WITH b, toInteger(timestamp(datetime() - duration({days:30}))) AS cutoff_ms

    // -------- scans triggered at this business (QR check-ins) --------
    OPTIONAL MATCH (b)-[:TRIGGERED]->(tscan:EcoTx {status:'settled'})
    WHERE coalesce(tscan.kind,'')='scan'
    WITH b, cutoff_ms,
         collect({
           ms:  toInteger(coalesce(tscan.createdAt, timestamp(tscan.at), timestamp())),
           eco: toInteger(coalesce(tscan.eco, tscan.amount, 0)),
           uid: tscan.user_id
         }) AS scans

    WITH b, cutoff_ms,
         reduce(s=0, r IN scans | s + r.eco) AS eco_triggered_total,
         [r IN scans WHERE r.ms >= cutoff_ms] AS scans30
    WITH b, cutoff_ms, eco_triggered_total, scans30,
         size(scans30) AS claims_30d,
         reduce(s=0, r IN scans30 | s + r.eco) AS eco_triggered_30d,
         // make a scalar max(ms) without APOC:
         reduce(mx = -1, r IN scans30 | CASE WHEN r.ms > mx THEN r.ms ELSE mx END) AS last_ms_raw,
         reduce(acc=[], r IN scans30 |
            CASE WHEN r.uid IS NULL OR r.uid IN acc THEN acc ELSE acc + r.uid END
         ) AS uniqs
    WITH b, cutoff_ms, eco_triggered_total, claims_30d, eco_triggered_30d, last_ms_raw,
         size(uniqs) AS unique_claimants_30d
    WITH b, cutoff_ms, eco_triggered_total, claims_30d, eco_triggered_30d,
         unique_claimants_30d,
         CASE WHEN last_ms_raw < 0 THEN NULL ELSE last_ms_raw END AS last_ms

    // -------- inbound contributions to the business --------
    OPTIONAL MATCH (b)-[:COLLECTED|EARNED]->(tin:EcoTx {status:'settled'})
    WHERE coalesce(tin.kind,'') IN ['CONTRIBUTE','SPONSOR_DEPOSIT']
       OR coalesce(tin.source,'')='contribution'
    WITH b, cutoff_ms, eco_triggered_total, claims_30d, eco_triggered_30d, unique_claimants_30d, last_ms,
         collect({
           ms:  toInteger(coalesce(tin.createdAt, timestamp(tin.at), timestamp())),
           eco: toInteger(coalesce(tin.eco, tin.amount, 0))
         }) AS ins

    WITH b, cutoff_ms, eco_triggered_total, claims_30d, eco_triggered_30d, unique_claimants_30d, last_ms,
         reduce(s=0, r IN ins | s + r.eco) AS contributions_total,
         [r IN ins WHERE r.ms >= cutoff_ms] AS ins30
    WITH b, cutoff_ms, eco_triggered_total, claims_30d, eco_triggered_30d, unique_claimants_30d, last_ms,
         contributions_total,
         reduce(s=0, r IN ins30 | s + r.eco) AS contributions_30d

    // -------- retirements via offer redemptions --------
    OPTIONAL MATCH (b)-[:SPENT]->(tout:EcoTx {status:'settled'})
    WHERE coalesce(tout.kind,'')='BURN_REWARD'
    WITH b, cutoff_ms, eco_triggered_total, claims_30d, eco_triggered_30d, unique_claimants_30d, last_ms,
         contributions_total, contributions_30d,
         collect({
           ms:  toInteger(coalesce(tout.createdAt, timestamp(tout.at), timestamp())),
           eco: toInteger(coalesce(tout.eco, tout.amount, 0))
         }) AS outs

    WITH b, cutoff_ms, eco_triggered_total, claims_30d, eco_triggered_30d, unique_claimants_30d, last_ms,
         contributions_total, contributions_30d,
         reduce(s=0, r IN outs | s + r.eco) AS eco_retired_total,
         [r IN outs WHERE r.ms >= cutoff_ms] AS outs30

    RETURN {
      business_id: b.id,
      sponsor_balance_cents: toInteger(coalesce(b.sponsor_balance_cents,0)),

      // scans
      eco_triggered_total: toInteger(coalesce(eco_triggered_total,0)),
      eco_triggered_30d:   toInteger(coalesce(eco_triggered_30d,0)),
      claims_30d:          toInteger(coalesce(claims_30d,0)),
      unique_claimants_30d:toInteger(coalesce(unique_claimants_30d,0)),
      last_claim_at:       (CASE WHEN last_ms IS NULL THEN NULL ELSE toString(datetime({epochMillis:last_ms})) END),

      // inbound contributions
      contributions_total: toInteger(coalesce(contributions_total,0)),
      contributions_30d:   toInteger(coalesce(contributions_30d,0)),

      // retirements (offers)
      eco_retired_total:   toInteger(coalesce(eco_retired_total,0)),
      eco_retired_30d:     toInteger(coalesce(reduce(s=0, r IN outs30 | s + r.eco),0)),
      redemptions_30d:     toInteger(size(outs30))
    } AS m
    """

    try:
        rec = _one(s, cy, {"uid": user_id}) or {"m": {}}
        m = rec["m"] or {}

        eco_velocity_30d = (m.get("eco_triggered_30d") or 0) / 30.0

        return {
            "business_id": m.get("business_id"),
            "sponsor_balance_cents": int(m.get("sponsor_balance_cents") or 0),

            # Triggered (scans)
            "eco_triggered_total": int(m.get("eco_triggered_total") or 0),
            "eco_triggered_30d": int(m.get("eco_triggered_30d") or 0),
            "claims_30d": int(m.get("claims_30d") or 0),
            "unique_claimants_30d": int(m.get("unique_claimants_30d") or 0),
            "last_claim_at": m.get("last_claim_at"),

            # Contributions in
            "contributions_total": int(m.get("contributions_total") or 0),
            "contributions_30d": int(m.get("contributions_30d") or 0),

            # Retirements (offers)
            "eco_retired_total": int(m.get("eco_retired_total") or 0),
            "eco_retired_30d": int(m.get("eco_retired_30d") or 0),
            "redemptions_30d": int(m.get("redemptions_30d") or 0),

            # Derived rate
            "eco_velocity_30d": round(float(eco_velocity_30d), 2),
        }
    except Exception as e:
        from fastapi import HTTPException
        raise HTTPException(status_code=500, detail=f"/owner/metrics failed: {e}")


@router.get("/offers", response_model=List[OfferOut])
def list_offers(
    user_id: str = Depends(current_user_id),
    s: Session = Depends(session_dep),
):
    """
    Business-owner view of their offers.

    Pulls offers attached via either:
    - (b)-[:HAS_OFFER]->(o)
    - (o)-[:OF]->(b)

    and normalises legacy fields into the new eco_price / status shape.
    """
    _ensure_owner_business(s, user_id)
    cy = """
    MATCH (b:BusinessProfile {user_id: $uid})
    OPTIONAL MATCH (b)-[:HAS_OFFER]->(a:Offer)
    OPTIONAL MATCH (o:Offer)-[:OF]->(b)
    WITH b, coalesce(a, o) AS o
    WHERE o IS NOT NULL
    WITH collect(DISTINCT o) AS os
    UNWIND os AS o
    ORDER BY coalesce(o.valid_until, '') DESC, o.title
    RETURN o {
      .id,
      .title,
      .blurb,
      .status,
      .eco_price,
      .fiat_cost_cents,
      .stock,
      .url,
      .valid_until,
      .tags,
      .visible,        // for legacy mapping to status
      .redeem_eco      // for legacy mapping to eco_price
    } AS o
    """
    rows = _all(s, cy, {"uid": user_id})
    return [_offer_record_to_out(r) for r in rows]


@router.post("/offers", response_model=OfferOut, status_code=status.HTTP_201_CREATED)
def create_offer(
    payload: OfferIn,
    user_id: str = Depends(current_user_id),
    s: Session = Depends(session_dep),
):
    """
    Create a new business offer with the upgraded schema:
    - status (active/paused/hidden)
    - eco_price (required by redeem logic)
    - fiat_cost_cents (for sponsor payouts)
    - stock, url, valid_until, tags
    """
    _ensure_owner_business(s, user_id)
    oid = str(uuid4())
    cy = """
    MATCH (b:BusinessProfile {user_id: $uid})
    CREATE (o:Offer {
      id: $oid,
      title: $title,
      blurb: $blurb,
      status: $status,
      eco_price: $eco_price,
      fiat_cost_cents: $fiat_cost_cents,
      stock: $stock,
      url: $url,
      valid_until: $vu,
      tags: $tags,
      claims: coalesce($claims, 0),
      created_at: datetime(),
      updated_at: datetime()
    })
    MERGE (b)-[:HAS_OFFER]->(o)
    MERGE (o)-[:OF]->(b)        // keep both directions for compatibility
    RETURN o {
      .id,
      .title,
      .blurb,
      .status,
      .eco_price,
      .fiat_cost_cents,
      .stock,
      .url,
      .valid_until,
      .tags
    } AS o
    """
    rec = _one(
        s,
        cy,
        {
            "uid": user_id,
            "oid": oid,
            "title": payload.title.strip(),
            "blurb": payload.blurb.strip() if payload.blurb else None,
            "status": payload.status,
            "eco_price": int(payload.eco_price or 0),
            "fiat_cost_cents": int(payload.fiat_cost_cents or 0),
            "stock": payload.stock,
            "url": payload.url,
            "vu": payload.valid_until,
            "tags": payload.tags or [],
            "claims": 0,
        },
    )
    return _offer_record_to_out(rec)


@router.patch("/offers/{offer_id}", response_model=OfferOut)
def patch_offer(
    offer_id: str,
    payload: OfferIn,
    user_id: str = Depends(current_user_id),
    s: Session = Depends(session_dep),
):
    """
    Full update of an offer (FE sends the complete offer payload).
    """
    _ensure_owner_business(s, user_id)
    cy = """
    MATCH (b:BusinessProfile {user_id: $uid})
    OPTIONAL MATCH (b)-[:HAS_OFFER]->(o1:Offer {id:$oid})
    OPTIONAL MATCH (o2:Offer {id:$oid})-[:OF]->(b)
    WITH coalesce(o1, o2) AS o
    WHERE o IS NOT NULL
    SET o.title           = $title,
        o.blurb           = $blurb,
        o.status          = $status,
        o.eco_price       = $eco_price,
        o.fiat_cost_cents = $fiat_cost_cents,
        o.stock           = $stock,
        o.url             = $url,
        o.valid_until     = $vu,
        o.tags            = $tags,
        o.updated_at      = datetime()
    RETURN o {
      .id,
      .title,
      .blurb,
      .status,
      .eco_price,
      .fiat_cost_cents,
      .stock,
      .url,
      .valid_until,
      .tags
    } AS o
    """
    rec = _one(
        s,
        cy,
        {
            "uid": user_id,
            "oid": offer_id,
            "title": payload.title.strip(),
            "blurb": payload.blurb.strip() if payload.blurb else None,
            "status": payload.status,
            "eco_price": int(payload.eco_price or 0),
            "fiat_cost_cents": int(payload.fiat_cost_cents or 0),
            "stock": payload.stock,
            "url": payload.url,
            "vu": payload.valid_until,
            "tags": payload.tags or [],
        },
    )
    if not rec:
        raise HTTPException(status_code=404, detail="Offer not found")
    return _offer_record_to_out(rec)


@router.delete("/offers/{offer_id}", response_model=dict)
def delete_offer(
    offer_id: str,
    user_id: str = Depends(current_user_id),
    s: Session = Depends(session_dep),
):
    """
    Delete an offer owned by this business using a pure-Cypher FOREACH pattern
    (no APOC), to avoid the `Variable o not defined` errors.
    """
    _ensure_owner_business(s, user_id)
    cy = """
    MATCH (b:BusinessProfile {user_id:$uid})
    OPTIONAL MATCH (b)-[:HAS_OFFER]->(o1:Offer {id:$oid})
    OPTIONAL MATCH (o2:Offer {id:$oid})-[:OF]->(b)
    WITH coalesce(o1, o2) AS o
    WITH o,
         CASE WHEN o IS NULL THEN 0 ELSE 1 END AS deleted
    FOREACH (_ IN CASE WHEN o IS NULL THEN [] ELSE [1] END |
      DETACH DELETE o
    )
    RETURN deleted AS deleted
    """
    rec = _one(s, cy, {"uid": user_id, "oid": offer_id})
    if not rec or int(rec.get("deleted") or 0) == 0:
        raise HTTPException(status_code=404, detail="Offer not found")
    return {"ok": True}


@router.get("/activity", response_model=List[ActivityRow])
def business_recent_activity(
    limit: int = 50,
    user_id: str = Depends(current_user_id),
    s: Session = Depends(session_dep),
):
    cy = """
    MATCH (b:BusinessProfile {user_id:$uid})

    CALL {
      WITH b
      // Inbound: contributions collected by the business
      MATCH (b)-[:COLLECTED]->(tin:EcoTx)
      WHERE coalesce(tin.status,'settled')='settled'
        AND (coalesce(tin.kind,'')='CONTRIBUTE' OR tin.source='contribution' OR tin.source='eco_local')
      RETURN
        tin.id AS id,
        toString(datetime({epochMillis: toInteger(coalesce(tin.createdAt, timestamp(tin.at), timestamp()))})) AS createdAt,
        tin.user_id AS user_id,
        coalesce(tin.kind,'CONTRIBUTE') AS kind,
        toFloat(coalesce(tin.eco, tin.amount)) AS amount,
        NULL AS offer_id
      UNION ALL
      WITH b
      // Outbound: rewards/payouts initiated by the business
      MATCH (b)-[r]->(tout:EcoTx)
      WHERE coalesce(tout.status,'settled')='settled'
        AND type(r) IN ['SPENT','COLLECTED','EARNED']
        AND coalesce(tout.kind,'') IN ['BURN_REWARD','SPONSOR_PAYOUT']
      RETURN
        tout.id AS id,
        toString(datetime({epochMillis: toInteger(coalesce(tout.createdAt, timestamp(tout.at), timestamp()))})) AS createdAt,
        tout.user_id AS user_id,
        coalesce(tout.kind,'BURN_REWARD') AS kind,
        toFloat(coalesce(tout.eco, tout.amount)) AS amount,
        tout.offer_id AS offer_id
    }
    RETURN id, createdAt, user_id, kind, amount, offer_id
    ORDER BY datetime(createdAt) DESC
    LIMIT $limit
    """
    rows = _all(s, cy, {"uid": user_id, "limit": int(limit)})

    out: list[ActivityRow] = []
    for r in rows:
        out.append(
            ActivityRow(
                id=r.get("id") or str(uuid4()),
                createdAt=r.get("createdAt") or datetime.now(timezone.utc).isoformat(),
                user_id=r.get("user_id"),
                kind=r.get("kind") or "event",
                amount=float(r.get("amount") or 0.0),
                offer_id=r.get("offer_id"),
            )
        )

    return out


@router.patch("/profile", response_model=BusinessMine)
def patch_profile(
    patch: PatchProfile,
    user_id: str = Depends(current_user_id),
    s: Session = Depends(session_dep),
):
    """
    No APOC: explicitly SET only provided fields (accepts rule fields to avoid 422).
    """
    _ensure_owner_business(s, user_id)

    # Build dynamic SETs based on provided keys
    fields_map = patch.dict(exclude_unset=True)

    # Normalise FE "disable" semantics for geofence: FE may send null/""; if "", drop it.
    if "rules_geofence_radius_m" in fields_map and fields_map["rules_geofence_radius_m"] in ("", None):
        fields_map["rules_geofence_radius_m"] = None

    if not fields_map:
        b = _get_business(s, user_id)
        if not b:
            raise HTTPException(status_code=404, detail="Business not found")
        return b

    set_lines = []
    params: Dict[str, Any] = {"uid": user_id}
    for k, v in fields_map.items():
        set_lines.append(f"b.{k} = ${k}")
        params[k] = v

    cy = f"""
    MATCH (b:BusinessProfile {{user_id:$uid}})
    SET {", ".join(set_lines)}
    RETURN b {{
      .id, .name, .tagline, .website, .address, .hours, .description,
      .hero_url, .lat, .lng, .visible_on_map, .tags,
      .pledge_tier, .rules_first_visit, .rules_return_visit, .rules_cooldown_hours,
      .rules_daily_cap_per_user, .rules_geofence_radius_m
    }} AS b
    """
    rec = _one(s, cy, params)
    if not rec:
        raise HTTPException(status_code=404, detail="Business not found")

    # Add QR (if any)... canonical shape (:QR)-[:OF]->(b)
    cy_qr = """
    MATCH (b:BusinessProfile {user_id:$uid})
    OPTIONAL MATCH (q:QR)-[:OF]->(b)
    RETURN q.code AS qr
    """
    qr = (_one(s, cy_qr, {"uid": user_id}) or {}).get("qr")

    b = rec.get("b") or {}
    return BusinessMine(
        id=b.get("id"),
        name=b.get("name"),
        tagline=b.get("tagline"),
        website=b.get("website"),
        address=b.get("address"),
        hours=b.get("hours"),
        description=b.get("description"),
        hero_url=b.get("hero_url"),
        lat=b.get("lat"),
        lng=b.get("lng"),
        visible_on_map=b.get("visible_on_map", True),
        tags=b.get("tags") or [],
        qr_code=qr,
        pledge_tier=b.get("pledge_tier"),
        rules_first_visit=b.get("rules_first_visit"),
        rules_return_visit=b.get("rules_return_visit"),
        rules_cooldown_hours=b.get("rules_cooldown_hours"),
        rules_daily_cap_per_user=b.get("rules_daily_cap_per_user"),
        rules_geofence_radius_m=b.get("rules_geofence_radius_m"),
    )

# ─────────────────────────────────────────────────────────────────────────────
# Asset upload (hero image)
# ─────────────────────────────────────────────────────────────────────────────

@assets_router.post("/hero_upload")
def hero_upload(
    file: UploadFile = File(...),
    user_id: str = Depends(current_user_id),
    s: Session = Depends(session_dep),
):
    os.makedirs(UPLOAD_DIR, exist_ok=True)
    ext = os.path.splitext(file.filename or "")[1] or ".bin"
    name = f"{uuid4().hex}{ext}"
    disk_path = os.path.join(UPLOAD_DIR, name)
    with open(disk_path, "wb") as f:
        shutil.copyfileobj(file.file, f)

    # Path served by your StaticFiles (adjust if your static mount differs)
    public_path = f"/{UPLOAD_DIR}/{name}".replace("//", "/")

    cy = """
    MATCH (b:BusinessProfile {user_id:$uid})
    SET b.hero_url = $url
    """
    s.run(cy, uid=user_id, url=public_path).consume()

    return {"path": public_path, "url": public_path}

# ── Analytics models (unchanged) ─────────────────────────────

class DayPoint(BaseModel):
    day: str
    minted: int
    retired: int
    net: int


class DailySeriesOut(BaseModel):
    points: list[DayPoint]


class BusyBucket(BaseModel):
    label: str
    claims: int
    eco: int


class BusyOut(BaseModel):
    by_hour: list[BusyBucket]
    by_weekday: list[BusyBucket]


class VisitorRow(BaseModel):
    id: str
    first_at: int
    last_at: int
    claims: int
    minted_eco: int


class VisitorsOut(BaseModel):
    items: list[VisitorRow]


class AbuseOut(BaseModel):
    cooldown_hits: int
    daily_cap_hits: int


def _utc_day(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).date().isoformat()


@router.get("/analytics/daily", response_model=DailySeriesOut)
def analytics_daily(
    days: int = 90,
    user_id: str = Depends(current_user_id),
    s: Session = Depends(session_dep),
):
    """
    Roll up per-UTC-day for inbound COLLECTED and outbound BURN_REWARD.
    """
    _ensure_owner_business(s, user_id)
    cy = """
    MATCH (b:BusinessProfile {user_id:$uid})
    WITH b, datetime() - duration({days:$days}) AS since

    // inbound minted
    MATCH (b)-[:COLLECTED]->(tin:EcoTx {status:'settled'})
    WHERE coalesce(tin.at, datetime({epochMillis:tin.createdAt})) >= since
    WITH b, since,
         date(coalesce(tin.at, datetime({epochMillis:tin.createdAt}))) AS d,
         toInteger(coalesce(tin.eco, tin.amount, 0)) AS eco_in
    WITH b, since, d, sum(eco_in) AS minted

    // outbound retired on same day d
    OPTIONAL MATCH (b)-[:SPENT]->(tout:EcoTx {status:'settled'})
    WHERE coalesce(tout.kind,'')='BURN_REWARD'
      AND date(coalesce(tout.at, datetime({epochMillis:tout.createdAt}))) = d
      AND coalesce(tout.at, datetime({epochMillis:tout.createdAt})) >= since
    WITH d, minted,
         sum( toInteger(coalesce(tout.eco, tout.amount, 0)) ) AS retired
    RETURN d AS day, toInteger(minted) AS minted, toInteger(coalesce(retired,0)) AS retired
    ORDER BY day ASC
    """
    rows = _all(s, cy, {"uid": user_id, "days": int(days)})
    points = []
    for r in rows:
        day = str(r["day"])
        minted = int(r["minted"] or 0)
        retired = int(r["retired"] or 0)
        points.append({"day": day, "minted": minted, "retired": retired, "net": minted - retired})
    return {"points": points}


@router.get("/analytics/busy", response_model=BusyOut)
def analytics_busy(
    days: int = 90,
    user_id: str = Depends(current_user_id),
    s: Session = Depends(session_dep),
):
    """
    Busiest hours and weekdays (claims + ECO).
    - Parenthesized WHERE to ensure the time-window applies to both kind/source branches.
    """
    _ensure_owner_business(s, user_id)
    cy = """
    MATCH (b:BusinessProfile {user_id:$uid})
    WITH b, datetime() - duration({days:$days}) AS since
    MATCH (b)-[:COLLECTED]->(t:EcoTx {status:'settled'})
    WHERE (
      coalesce(t.kind,'')='MINT_ACTION'
      OR coalesce(t.source,'') IN ['qr','contribution','sidequest','eco_local']
    ) AND coalesce(t.at, datetime({epochMillis:t.createdAt})) >= since
    WITH
      time(coalesce(t.at, datetime({epochMillis:t.createdAt}))).hour AS hr,
      coalesce(t.user_id, substring(coalesce(t.source,"anon"),0,16)) AS who,
      toInteger(coalesce(t.eco, t.amount, 0)) AS eco,
      date(coalesce(t.at, datetime({epochMillis:t.createdAt}))).weekday AS wd
    RETURN
      hr, wd,
      count(*) AS claims,
      sum(eco) AS eco
    """
    rows = _all(s, cy, {"uid": user_id, "days": int(days)})

    hour = {i: {"claims": 0, "eco": 0} for i in range(24)}
    dow = {i: {"claims": 0, "eco": 0} for i in range(7)}

    for r in rows:
        hr = int(r["hr"] or 0)
        wd = int(r["wd"] or 0)
        hour[hr]["claims"] += int(r["claims"] or 0)
        hour[hr]["eco"] += int(r["eco"] or 0)
        dow[wd]["claims"] += int(r["claims"] or 0)
        dow[wd]["eco"] += int(r["eco"] or 0)

    names = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"]
    return {
        "by_hour": [
            {"label": f"{h:02d}:00", "claims": hour[h]["claims"], "eco": hour[h]["eco"]}
            for h in range(24)
        ],
        "by_weekday": [
            {"label": names[d], "claims": dow[d]["claims"], "eco": dow[d]["eco"]}
            for d in range(7)
        ],
    }


@router.get("/analytics/visitors", response_model=VisitorsOut)
def analytics_visitors(
    days: int = 90,
    limit: int = 50,
    user_id: str = Depends(current_user_id),
    s: Session = Depends(session_dep),
):
    """
    Unique visitors over window; groups by user_id when present, else a device-ish surrogate.
    """
    _ensure_owner_business(s, user_id)
    cy = """
    MATCH (b:BusinessProfile {user_id:$uid})
    WITH b, datetime() - duration({days:$days}) AS since
    MATCH (b)-[:COLLECTED]->(t:EcoTx {status:'settled'})
    WHERE coalesce(t.at, datetime({epochMillis:t.createdAt})) >= since
    WITH coalesce(t.user_id, "device:" + substring(coalesce(t.source,"anon"),0,16)) AS id,
         toInteger(coalesce(t.createdAt, timestamp(t.at), timestamp())) AS ms,
         toInteger(coalesce(t.eco, t.amount, 0)) AS eco
    RETURN id,
           min(ms) AS first_at,
           max(ms) AS last_at,
           count(*) AS claims,
           sum(eco) AS minted_eco
    ORDER BY claims DESC, last_at DESC
    LIMIT $limit
    """
    rows = _all(s, cy, {"uid": user_id, "days": int(days), "limit": int(limit)})
    return {
        "items": [
            {
                "id": r["id"],
                "first_at": int(r["first_at"] or 0),
                "last_at": int(r["last_at"] or 0),
                "claims": int(r["claims"] or 0),
                "minted_eco": int(r["minted_eco"] or 0),
            }
            for r in rows
        ]
    }


@router.get("/analytics/abuse", response_model=AbuseOut)
def analytics_abuse(
    days: int = 30,
    user_id: str = Depends(current_user_id),
    s: Session = Depends(session_dep),
):
    _ensure_owner_business(s, user_id)
    # TODO: replace with your real reject logs when available
    return {"cooldown_hits": 0, "daily_cap_hits": 0}
