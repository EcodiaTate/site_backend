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
# Auth / DB deps — use your real ones (you provided these)
# ─────────────────────────────────────────────────────────────────────────────
from site_backend.core.neo_driver import session_dep  # yields a neo4j.Session
from site_backend.core.user_guard import current_user_id  # validates Bearer or legacy cookie

# ─────────────────────────────────────────────────────────────────────────────
# Pydantic models (mirror the front-end types you’re using)
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
    criteria: Optional[dict] = None

class BusinessMine(BaseModel):
    id: str
    name: Optional[str] = None
    tagline: Optional[str] = None
    website: Optional[str] = None
    address: Optional[str] = None
    hours: Optional[str] = None  # JSON string of HoursMap
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

# Separate router for assets (hero uploads)
assets_router = APIRouter(prefix="/eyba/assets", tags=["eyba.assets"])

# Where to drop hero files (served by your StaticFiles mount)
UPLOAD_DIR = os.getenv("EYBA_UPLOAD_DIR", "uploads/hero")


# ─────────────────────────────────────────────────────────────────────────────
# Small helpers for Neo4j session access
# ─────────────────────────────────────────────────────────────────────────────

def _one(s, cypher: str, params: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    rec = s.run(cypher, **params).single()
    return rec.data() if rec else None

def _all(s, cypher: str, params: Dict[str, Any]) -> List[Dict[str, Any]]:
    return [r.data() for r in s.run(cypher, **params)]


# ─────────────────────────────────────────────────────────────────────────────
# Graph helpers – aligned to your constraints:
# - BusinessProfile has unique (id) and unique (user_id)
# - We also create/connect a User node for relationships, but source of truth
#   for ownership is BusinessProfile.user_id = <uid>.
# - Canonical QR relation is (:QR)-[:OF]->(b) with q.code.
# ─────────────────────────────────────────────────────────────────────────────

def _ensure_owner_business(s, user_id: str) -> str:
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

def _get_business(s, user_id: str) -> Optional[BusinessMine]:
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
    # crit = o.get("criteria_json")  # if you later expose criteria on OfferOut
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
    s = Depends(session_dep),
):
    _ensure_owner_business(s, user_id)
    b = _get_business(s, user_id)
    if not b:
        raise HTTPException(status_code=404, detail="Business not found")
    return b


@router.get("/metrics", response_model=Metrics)
def get_metrics(
    user_id: str = Depends(current_user_id),
    s = Depends(session_dep),
):
    _ensure_owner_business(s, user_id)
    # Example aggregation (replace with your real ledger logic)
    # Sums incoming EcoTx amounts to the business.
    cy = """
    MATCH (b:BusinessProfile {user_id: $uid})
    OPTIONAL MATCH (e:EcoTx)-[:ECO_TO]->(b)
    WITH collect(e) AS txs
    WITH
      reduce(sum=0.0, t IN txs | sum + coalesce(t.amount,0.0)) AS minted,
      [tx IN txs WHERE tx.kind='contribution'] AS contribs,
      [tx IN txs WHERE tx.kind='redemption']   AS redemps
    RETURN {
      minted_eco: minted,
      eco_contributed_total: reduce(sum=0.0, t IN contribs | sum + coalesce(t.amount,0.0)),
      eco_given_total:       reduce(sum=0.0, t IN redemps  | sum + coalesce(t.amount,0.0)),
      eco_velocity_30d: 0.0
    } AS m
    """
    rec = _one(s, cy, {"uid": user_id}) or {"m": {}}
    m = rec.get("m") or {}
    return Metrics(
        minted_eco=float(m.get("minted_eco", 0.0)),
        eco_contributed_total=float(m.get("eco_contributed_total", 0.0)),
        eco_given_total=float(m.get("eco_given_total", 0.0)),
        eco_velocity_30d=float(m.get("eco_velocity_30d", 0.0)),
    )


@router.get("/offers", response_model=List[OfferOut])
def list_offers(
    user_id: str = Depends(current_user_id),
    s = Depends(session_dep),
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
    s = Depends(session_dep),
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
    s = Depends(session_dep),
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
    s = Depends(session_dep),
):
    _ensure_owner_business(s, user_id)
    cy = """
    MATCH (b:BusinessProfile {user_id:$uid})-[:HAS_OFFER]->(o:Offer {id:$oid})
    DETACH DELETE o
    """
    s.run(cy, uid=user_id, oid=offer_id).consume()
    return {"ok": True}


@router.get("/activity", response_model=List[ActivityRow])
def recent_activity(
    limit: int = 50,
    user_id: str = Depends(current_user_id),
    s = Depends(session_dep),
):
    _ensure_owner_business(s, user_id)
    # Replace with your real ledger model as needed
    cy = """
    MATCH (b:BusinessProfile {user_id:$uid})
    OPTIONAL MATCH (e:EcoTx)-[:ECO_TO]->(b)
    WITH e
    ORDER BY e.created_at DESC
    LIMIT $limit
    RETURN e { .id, .created_at, .user_id, .kind, .amount } AS e
    """
    rows = _all(s, cy, {"uid": user_id, "limit": int(limit)})
    out: List[ActivityRow] = []
    for r in rows:
        e = r.get("e") or {}
        ts = e.get("created_at")
        if isinstance(ts, datetime):
            iso = ts.astimezone(timezone.utc).isoformat()
        else:
            iso = str(ts or datetime.now(timezone.utc).isoformat())
        out.append(ActivityRow(
            id=e.get("id") or str(uuid4()),
            createdAt=iso,
            user_id=e.get("user_id"),
            kind=e.get("kind") or "event",
            amount=float(e.get("amount") or 0.0),
        ))
    return out


@router.patch("/profile", response_model=BusinessMine)
def patch_profile(
    patch: PatchProfile,
    user_id: str = Depends(current_user_id),
    s = Depends(session_dep),
):
    """
    No APOC: explicitly SET only provided fields.
    """
    _ensure_owner_business(s, user_id)

    # Build dynamic SETs based on provided keys
    fields_map = patch.dict(exclude_unset=True)
    if not fields_map:
        # Still return current profile
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
    s = Depends(session_dep),
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
