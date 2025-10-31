# site_backend/api/eyba/owner.py
from __future__ import annotations

from typing import List, Optional, Literal, Any, Dict
from uuid import uuid4
from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException, UploadFile, File, status
from pydantic import BaseModel
import os
import shutil
import json

# ─────────────────────────────────────────────────────────────────────────────
# Auth / DB deps — use your real ones
# ─────────────────────────────────────────────────────────────────────────────
from site_backend.core.neo_driver import session_dep   # yields a neo4j.Session
from site_backend.core.user_guard import current_user_id  # validates Bearer or legacy cookie
from neo4j import Session

# ─────────────────────────────────────────────────────────────────────────────
# Pydantic models (mirror the FE types you’re using)
# ─────────────────────────────────────────────────────────────────────────────

class OfferOut(BaseModel):
    id: str
    title: str
    blurb: Optional[str] = None
    visible: bool = True
    url: Optional[str] = None
    valid_until: Optional[str] = None
    type: Optional[Literal["discount", "perk", "info"]] = "perk"
    tags: Optional[List[str]] = None

class OfferIn(BaseModel):
    title: str
    blurb: str
    type: Literal["discount", "perk", "info"] = "perk"
    visible: bool = True
    url: Optional[str] = None
    valid_until: Optional[str] = None
    tags: Optional[List[str]] = None
    template_id: Optional[str] = None
    criteria: Optional[dict] = None  # kept generic; stored as JSON string

class BusinessMine(BaseModel):
    id: str
    name: Optional[str] = None
    tagline: Optional[str] = None
    website: Optional[str] = None
    address: Optional[str] = None
    hours: Optional[str] = None  # JSON string of HoursMap (your graph stores it as map; we surface string)
    description: Optional[str] = None
    hero_url: Optional[str] = None
    lat: Optional[float] = None
    lng: Optional[float] = None
    visible_on_map: bool = True
    tags: Optional[List[str]] = None
    qr_code: Optional[str] = None

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
    offer_id: Optional[str] = None   # ← add


class PatchProfile(BaseModel):
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

router = APIRouter(prefix="/eyba/owner", tags=["eyba.owner"])
assets_router = APIRouter(prefix="/eyba/assets", tags=["eyba.assets"])

# Where to drop hero files (served by your StaticFiles mount)
UPLOAD_DIR = os.getenv("EYBA_UPLOAD_DIR", "uploads/hero")

# ─────────────────────────────────────────────────────────────────────────────
# Small helpers for Neo4j session access
# ─────────────────────────────────────────────────────────────────────────────

def _one(s: Session, cypher: str, params: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    rec = s.run(cypher, **params).single()
    return rec.data() if rec else None

def _all(s: Session, cypher: str, params: Dict[str, Any]) -> List[Dict[str, Any]]:
    return [r.data() for r in s.run(cypher, **params)]

# ─────────────────────────────────────────────────────────────────────────────
# Graph helpers – aligned to your constraints:
# - BusinessProfile has unique (id) and unique (user_id)
# - We also connect a User node for relationships, but source of truth
#   for ownership is BusinessProfile.user_id = <uid>.
# - Canonical QR relation is (:QR)-[:OF]->(b) with q.code.
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
      .hero_url, .lat, .lng, .visible_on_map, .tags
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
    )

def _offer_record_to_out(rec: Dict[str, Any]) -> OfferOut:
    o = rec["o"]
    return OfferOut(
        id=o.get("id"),
        title=o.get("title") or "",
        blurb=o.get("blurb"),
        visible=o.get("visible", True),
        url=o.get("url"),
        valid_until=o.get("valid_until"),
        type=o.get("type") or "perk",
        tags=o.get("tags") or [],
    )

# ─────────────────────────────────────────────────────────────────────────────
# Endpoints
# ─────────────────────────────────────────────────────────────────────────────

@router.get("/mine", response_model=BusinessMine)
def get_mine(
    user_id: str = Depends(current_user_id),
    s: Session = Depends(session_dep),
):
    _ensure_owner_business(s, user_id)
    b = _get_business(s, user_id)
    if not b:
        raise HTTPException(status_code=404, detail="Business not found")
    return b
@router.get("/metrics", response_model=Dict[str, Any])
def get_metrics(
    user_id: str = Depends(current_user_id),
    s: Session = Depends(session_dep),
):
    _ensure_owner_business(s, user_id)
    cy = """
    MATCH (b:BusinessProfile {user_id:$uid})
    WITH b, datetime() - duration({days:30}) AS d30

    // inbound collected
    OPTIONAL MATCH (b)-[:COLLECTED]->(tin:EcoTx {status:'settled'})
    WITH b, d30, collect(tin) AS ins

    // outbound spent
    OPTIONAL MATCH (b)-[:SPENT]->(tout:EcoTx {status:'settled'})
    WITH b, d30, ins, collect(tout) AS outs

    // Build 30d slices with robust timestamp coercion
    WITH b, d30, ins, outs,
         [t IN ins  WHERE coalesce(t.at,  datetime({epochMillis: toInteger(coalesce(t.createdAt, timestamp(t.at), timestamp()))})) >= d30] AS ins30,
         [t IN outs WHERE coalesce(t.at,  datetime({epochMillis: toInteger(coalesce(t.createdAt, timestamp(t.at), timestamp()))})) >= d30] AS outs30

    // Totals
    WITH b, ins, outs, ins30, outs30,
         reduce(s=0, t IN ins    | s + toInteger(coalesce(t.eco, t.amount, 0))) AS minted_total,
         reduce(s=0, t IN ins30  | s + toInteger(coalesce(t.eco, t.amount, 0))) AS minted_30d,
         reduce(s=0, t IN [t IN outs WHERE coalesce(t.kind,'')='BURN_REWARD']
                         | s + toInteger(coalesce(t.eco, t.amount, 0))) AS retired_total,
         reduce(s=0, t IN [t IN outs30 WHERE coalesce(t.kind,'')='BURN_REWARD']
                         | s + toInteger(coalesce(t.eco, t.amount, 0))) AS retired_30d,
         size(ins30) AS claims_30d,
         [t IN ins30 WHERE t.user_id IS NOT NULL | t.user_id] AS claimants30_raw

    // Last claim: UNWIND then max(datetime)
    UNWIND ins30 AS tlast
    WITH b, outs30, minted_30d, retired_30d, claims_30d, claimants30_raw,
         max(coalesce(tlast.at, datetime({epochMillis: toInteger(coalesce(tlast.createdAt, timestamp(tlast.at), timestamp()))}))) AS last_claim_dt

    // Unique claimants (pure Cypher distinct)
    WITH b, outs30, minted_30d, retired_30d, claims_30d, last_claim_dt, claimants30_raw
    UNWIND claimants30_raw AS c
    WITH b, outs30, minted_30d, retired_30d, claims_30d, last_claim_dt, collect(DISTINCT c) AS uniqs
    WITH b, outs30, minted_30d, retired_30d, claims_30d, last_claim_dt,
         size([u IN uniqs WHERE u IS NOT NULL | u]) AS unique_30d

    // Redemptions count (30d)
    WITH b, minted_30d, retired_30d, claims_30d, last_claim_dt, unique_30d,
         size([t IN outs30 WHERE coalesce(t.kind,'')='BURN_REWARD' | 1]) AS redemptions_30d

    RETURN {
      business_id: b.id,
      minted_eco_30d: minted_30d,
      eco_retired_30d: retired_30d,
      unique_claimants_30d: unique_30d,
      claims_30d: claims_30d,
      last_claim_at: (CASE WHEN last_claim_dt IS NULL THEN NULL ELSE toString(last_claim_dt) END),
      redemptions_30d: redemptions_30d,
      sponsor_balance_cents: toInteger(coalesce(b.sponsor_balance_cents, 0))
    } AS m
    """
    rec = _one(s, cy, {"uid": user_id}) or {"m": {}}
    m = rec.get("m") or {}

    return {
        "business_id": m.get("business_id"),
        "sponsor_balance_cents": int(m.get("sponsor_balance_cents") or 0),
        "eco_retired_30d": int(m.get("eco_retired_30d") or 0),
        "redemptions_30d": int(m.get("redemptions_30d") or 0),
        "unique_claimants_30d": int(m.get("unique_claimants_30d") or 0),
        "minted_eco_30d": int(m.get("minted_eco_30d") or 0),
        "claims_30d": int(m.get("claims_30d") or 0),
        "last_claim_at": m.get("last_claim_at"),
        # compat placeholders
        "name": None,
        "pledge_tier": None,
        "eco_retired_total": 0,
        "minted_eco_month": 0,
    }

@router.get("/offers", response_model=List[OfferOut])
def list_offers(
    user_id: str = Depends(current_user_id),
    s: Session = Depends(session_dep),
):
    _ensure_owner_business(s, user_id)
    cy = """
    MATCH (b:BusinessProfile {user_id: $uid})-[:HAS_OFFER]->(o:Offer)
    WITH o
    ORDER BY coalesce(o.valid_until, '') DESC, o.title
    RETURN o {
      .id, .title, .blurb, .visible, .url, .valid_until, .type, .tags,
      .criteria_json
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
    _ensure_owner_business(s, user_id)
    oid = str(uuid4())
    cy = """
    MATCH (b:BusinessProfile {user_id: $uid})
    CREATE (o:Offer {
      id:$oid, title:$title, blurb:$blurb, visible:$visible, url:$url,
      valid_until:$vu, type:$type, tags:$tags, template_id:$tid, criteria_json:$crit_json,
      created_at: datetime()
    })
    MERGE (b)-[:HAS_OFFER]->(o)
    RETURN o {
      .id, .title, .blurb, .visible, .url, .valid_until, .type, .tags
    } AS o
    """
    rec = _one(s, cy, {
        "uid": user_id,
        "oid": oid,
        "title": payload.title.strip(),
        "blurb": payload.blurb.strip() if payload.blurb else None,
        "visible": bool(payload.visible),
        "url": payload.url,
        "vu": payload.valid_until,
        "type": payload.type,
        "tags": payload.tags or [],
        "tid": payload.template_id,
        "crit_json": json.dumps(payload.criteria) if payload.criteria is not None else None,
    })
    return _offer_record_to_out(rec)

@router.patch("/offers/{offer_id}", response_model=OfferOut)
def patch_offer(
    offer_id: str,
    payload: OfferIn,
    user_id: str = Depends(current_user_id),
    s: Session = Depends(session_dep),
):
    _ensure_owner_business(s, user_id)
    cy = """
    MATCH (b:BusinessProfile {user_id:$uid})-[:HAS_OFFER]->(o:Offer {id:$oid})
    SET o.title         = $title,
        o.blurb         = $blurb,
        o.visible       = $visible,
        o.url           = $url,
        o.valid_until   = $vu,
        o.type          = $type,
        o.tags          = $tags,
        o.template_id   = $tid,
        o.criteria_json = $crit_json
    RETURN o {
      .id, .title, .blurb, .visible, .url, .valid_until, .type, .tags
    } AS o
    """
    rec = _one(s, cy, {
        "uid": user_id,
        "oid": offer_id,
        "title": payload.title.strip(),
        "blurb": payload.blurb.strip() if payload.blurb else None,
        "visible": bool(payload.visible),
        "url": payload.url,
        "vu": payload.valid_until,
        "type": payload.type,
        "tags": payload.tags or [],
        "tid": payload.template_id,
        "crit_json": json.dumps(payload.criteria) if payload.criteria is not None else None,
    })
    if not rec:
        raise HTTPException(status_code=404, detail="Offer not found")
    return _offer_record_to_out(rec)

@router.delete("/offers/{offer_id}", response_model=dict)
def delete_offer(
    offer_id: str,
    user_id: str = Depends(current_user_id),
    s: Session = Depends(session_dep),
):
    _ensure_owner_business(s, user_id)
    cy = """
    MATCH (b:BusinessProfile {user_id:$uid})-[:HAS_OFFER]->(o:Offer {id:$oid})
    DETACH DELETE o
    """
    s.run(cy, uid=user_id, oid=offer_id).consume()
    return {"ok": True}


def _all(s: Session, cypher: str, params: Dict[str, Any]) -> list[Dict[str, Any]]:
    return [r.data() for r in s.run(cypher, **params)]

@router.get("/activity", response_model=List[ActivityRow])
def business_recent_activity(
    limit: int = 50,
    user_id: str = Depends(current_user_id),
    s: Session = Depends(session_dep),
):
    # If you need to ensure the business node exists for this user, do it here.
    cy = """
    MATCH (b:BusinessProfile {user_id:$uid})

  CALL {
  WITH b
  // Inbound: contributions collected by the business
  MATCH (b)-[:COLLECTED]->(tin:EcoTx)
  WHERE coalesce(tin.status,'settled')='settled'
    AND (coalesce(tin.kind,'')='CONTRIBUTE' OR tin.source='contribution' OR tin.source='eyba')
  RETURN
    tin.id AS id,
    toString(datetime({epochMillis: toInteger(coalesce(tin.createdAt, timestamp(tin.at), timestamp()))})) AS createdAt,
    tin.user_id AS user_id,
    coalesce(tin.kind,'CONTRIBUTE') AS kind,
    toFloat(coalesce(tin.eco, tin.amount)) AS amount,
    NULL AS offer_id                                     // ← new
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
    tout.offer_id AS offer_id                              // ← new
}
RETURN id, createdAt, user_id, kind, amount, offer_id
ORDER BY datetime(createdAt) DESC
LIMIT $limit

    """
    rows = _all(s, cy, {"uid": user_id, "limit": int(limit)})

    out: list[ActivityRow] = []
    for r in rows:
        out.append(ActivityRow(
            id=r.get("id") or str(uuid4()),
            createdAt=r.get("createdAt") or datetime.now(timezone.utc).isoformat(),
            user_id=r.get("user_id"),
            kind=r.get("kind") or "event",
            amount=float(r.get("amount") or 0.0),
            offer_id=r.get("offer_id"),                            # ← new
        ))

    return out

@router.patch("/profile", response_model=BusinessMine)
def patch_profile(
    patch: PatchProfile,
    user_id: str = Depends(current_user_id),
    s: Session = Depends(session_dep),
):
    """
    No APOC: explicitly SET only provided fields.
    """
    _ensure_owner_business(s, user_id)

    # Build dynamic SETs based on provided keys
    fields_map = patch.dict(exclude_unset=True)
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
      .hero_url, .lat, .lng, .visible_on_map, .tags
    }} AS b
    """
    rec = _one(s, cy, params)
    if not rec:
        raise HTTPException(status_code=404, detail="Business not found")

    # Add QR (if any) — canonical shape (:QR)-[:OF]->(b)
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

# ── Analytics models (append near existing models) ─────────────────────────────

class DayPoint(BaseModel):
    day: str          # YYYY-MM-DD (UTC)
    minted: int       # ECO minted to this business that day
    retired: int      # ECO retired by this business that day
    net: int          # minted - retired

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
    id: str               # user_id or device hash
    first_at: int
    last_at: int
    claims: int
    minted_eco: int

class VisitorsOut(BaseModel):
    items: list[VisitorRow]

class AbuseOut(BaseModel):
    cooldown_hits: int
    daily_cap_hits: int

from datetime import timedelta

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
      OR coalesce(t.source,'') IN ['qr','contribution','sidequest','eyba']
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
    dow  = {i: {"claims": 0, "eco": 0} for i in range(7)}

    for r in rows:
        hr = int(r["hr"] or 0)
        wd = int(r["wd"] or 0)
        hour[hr]["claims"]  += int(r["claims"] or 0)
        hour[hr]["eco"]     += int(r["eco"] or 0)
        dow[wd]["claims"]   += int(r["claims"] or 0)
        dow[wd]["eco"]      += int(r["eco"] or 0)

    names = ["Sun","Mon","Tue","Wed","Thu","Fri","Sat"]
    return {
        "by_hour": [{"label": f"{h:02d}:00", "claims": hour[h]["claims"], "eco": hour[h]["eco"]} for h in range(24)],
        "by_weekday": [{"label": names[d], "claims": dow[d]["claims"], "eco": dow[d]["eco"]} for d in range(7)],
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
    return {"items": [
        {
            "id": r["id"],
            "first_at": int(r["first_at"] or 0),
            "last_at": int(r["last_at"] or 0),
            "claims": int(r["claims"] or 0),
            "minted_eco": int(r["minted_eco"] or 0),
        } for r in rows
    ]}

@router.get("/analytics/abuse", response_model=AbuseOut)
def analytics_abuse(
    days: int = 30,
    user_id: str = Depends(current_user_id),
    s: Session = Depends(session_dep),
):
    _ensure_owner_business(s, user_id)
    # TODO: replace with your real reject logs when available
    return {"cooldown_hits": 0, "daily_cap_hits": 0}
