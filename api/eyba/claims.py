# api/routers/eyba_claims.py
from __future__ import annotations

import hashlib
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Optional, Literal, Dict, Any, List

from fastapi import (
    APIRouter,
    Body,
    Depends,
    Header,
    HTTPException,
    Request,
    status,
    Query,
)
from neo4j import Session
from pydantic import BaseModel

from site_backend.core.neo_driver import session_dep
from site_backend.api.eyba.neo_business import new_id  # id helper

router = APIRouter(prefix="/eyba", tags=["eyba"])

PledgeTier = Literal["starter", "builder", "leader"]

# ---------- Request / Response ----------
class ClaimRequest(BaseModel):
    lat: Optional[float] = None
    lng: Optional[float] = None
    # If the UI sends this, we'll validate and attach before minting
    offer_id: Optional[str] = None

class ClaimResponse(BaseModel):
    ok: bool
    awarded_eco: int = 0
    balance: int = 0
    reason: Optional[str] = None
    business_name: Optional[str] = None
    location_name: Optional[str] = None
    tx_id: Optional[str] = None
    business_id: Optional[str] = None
    debug: Optional[Dict[str, Any]] = None  # optional debug payload

class AttachOfferRequest(BaseModel):
    offer_id: str

class AttachOfferResponse(BaseModel):
    ok: bool

# ---------- helpers ----------
def _now() -> datetime:
    return datetime.now(timezone.utc)

def _now_ms() -> int:
    return int(_now().timestamp() * 1000)

def _ms_at_utc_day_start(dt: datetime) -> int:
    d0 = datetime(dt.year, dt.month, dt.day, tzinfo=timezone.utc)
    return int(d0.timestamp() * 1000)

def _device_hash(ip: str, ua: str) -> str:
    h = hashlib.sha256()
    h.update((ip or "-").encode())
    h.update((ua or "-").encode())
    return h.hexdigest()[:16]

def _haversine_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    import math as m
    R = 6371000.0
    phi1 = m.radians(lat1)
    phi2 = m.radians(lat2)
    dphi = m.radians(lat2 - lat1)
    dl = m.radians(lon2 - lon1)
    a = m.sin(dphi / 2) ** 2 + m.cos(phi1) * m.cos(phi2) * m.sin(dl / 2) ** 2
    c = 2 * m.atan2(m.sqrt(a), m.sqrt(1 - a))
    return R * c

def get_youth_id(req: Request) -> str:
    ip = req.client.host if req.client else "0.0.0.0"
    ua = req.headers.get("user-agent", "")
    return f"y_{_device_hash(ip, ua)}"

def _day_bucket_ms(dt: datetime) -> int:
    d0 = datetime(dt.year, dt.month, dt.day, tzinfo=timezone.utc)
    return int(d0.timestamp() * 1000)

def _cooldown_bucket_ms(now: datetime, hours: int) -> int:
    slot_ms = max(1, hours) * 3600 * 1000
    return (int(now.timestamp() * 1000) // slot_ms) * slot_ms

def _dedupe_id(prefix: str, uid: str, bid: str, bucket_ms: int) -> str:
    h = hashlib.sha256(f"{prefix}|{uid}|{bid}|{bucket_ms}".encode()).hexdigest()[:24]
    return f"{prefix}_{h}"

# ---------- DB lookups (QR + Business + Rules) ----------
@dataclass
class DBQRMeta:
    code: str
    business_id: str
    business_name: Optional[str]
    location_name: Optional[str]
    lat: Optional[float]
    lng: Optional[float]
    active: bool
    pledge_tier: PledgeTier
    rules_first_visit: int
    rules_return_visit: int
    rules_cooldown_hours: int
    rules_daily_cap_per_user: int
    rules_geofence_radius_m: Optional[int]

def _fetch_qr_meta(s: Session, code: str) -> Optional[DBQRMeta]:
    rows = s.run(
        """
        MATCH (q:QR {code:$code})-[:OF]->(b:BusinessProfile)
        WITH properties(q) AS q, properties(b) AS b
        RETURN
          q['code'] AS code,
          b['id'] AS bid,
          b['name'] AS bname,
          coalesce(b['area'], b['location'], b['suburb']) AS locname,
          toFloat(q['lat']) AS qlat,
          toFloat(q['lng']) AS qlng,
          coalesce(q['active'], true) AS qactive,
          coalesce(b['pledge_tier'], 'starter') AS pledge_tier,
          toInteger(coalesce(b['rules_first_visit'], 12)) AS first_visit,
          toInteger(coalesce(b['rules_return_visit'], 4)) AS return_visit,
          toInteger(coalesce(b['rules_cooldown_hours'], 20)) AS cooldown_hours,
          toInteger(coalesce(b['rules_daily_cap_per_user'], 1)) AS daily_cap,
          toInteger(coalesce(b['rules_geofence_radius_m'], 150)) AS geofence_m
        """,
        code=code,
    ).data()
    if not rows:
        return None
    rec = rows[0]
    return DBQRMeta(
        code=rec["code"],
        business_id=rec["bid"],
        business_name=rec["bname"],
        location_name=rec["locname"],
        lat=rec["qlat"],
        lng=rec["qlng"],
        active=bool(rec["qactive"]),
        pledge_tier=rec["pledge_tier"],
        rules_first_visit=int(rec["first_visit"]),
        rules_return_visit=int(rec["return_visit"]),
        rules_cooldown_hours=int(rec["cooldown_hours"]),
        rules_daily_cap_per_user=int(rec["daily_cap"]),
        rules_geofence_radius_m=int(rec["geofence_m"]) if rec["geofence_m"] is not None else None,
    )

def _compute_eps(pledge_tier: PledgeTier, first_visit: bool) -> float:
    base = 1.5 if first_visit else 1.0
    tier_bonus = {"starter": 1.0, "builder": 1.15, "leader": 1.3}[pledge_tier]
    return round(base * tier_bonus, 2)

# Optional current-user dependency that safely awaits your existing one
from site_backend.core.user_guard import current_user_id as _current_user_id

async def current_user_id_optional_dep(req: Request) -> Optional[str]:
    """
    Return the authenticated user id as a string, or None.
    Never returns complex objects (e.g. Request).
    """
    try:
        val = await _current_user_id(req)
        return val if isinstance(val, str) and val else None
    except Exception:
        return None

def _last_scan_ms(s: Session, device_id: str, business_id: str) -> Optional[int]:
    rows = s.run(
        """
        MATCH (:User)-[:EARNED]->(t:EcoTx)<-[:TRIGGERED]-(:BusinessProfile {id:$bid})
        WHERE t.kind = 'scan' AND t.device_id = $did
        RETURN max(t.createdAt) AS last
        """,
        did=device_id, bid=business_id,
    ).data()
    if not rows:
        return None
    last = rows[0].get("last")
    return int(last) if last is not None else None

def _today_scans_count(s: Session, device_id: str, business_id: str, today0_ms: int) -> int:
    rows = s.run(
        """
        MATCH (:User)-[:EARNED]->(t:EcoTx)<-[:TRIGGERED]-(:BusinessProfile {id:$bid})
        WHERE t.kind='scan' AND t.device_id=$did AND t.createdAt >= $today0
        RETURN count(t) AS c
        """,
        did=device_id, bid=business_id, today0=today0_ms,
    ).data()
    return int(rows[0].get("c", 0)) if rows else 0

# Visible offers for a business (id/title/blurb/redeem_eco)
def _business_visible_offers(s: Session, business_id: str) -> List[Dict[str, Any]]:
    return s.run(
        """
        MATCH (b:BusinessProfile {id:$bid})<-[:OF]-(o:Offer)
        WHERE coalesce(o.visible, true) = true
        RETURN o.id AS id,
               o.title AS title,
               o.blurb AS blurb,
               toInteger(o.redeem_eco) AS redeem_eco
        ORDER BY o.title
        """,
        bid=business_id,
    ).data() or []

# ---------- Youth balance (ledger) ----------
def _youth_balance(s: Session, youth_id: str) -> int:
    """
    Balance = sum(EARNED) - sum(SPENT) over settled EcoTx.
    Neo4j-5 safe (no implicit grouping).
    """
    row = s.run(
        """
        // Earned
        CALL {
          WITH $uid AS uid
          MATCH (u:User {id: uid})
          OPTIONAL MATCH (u)-[:EARNED]->(te:EcoTx)
            WHERE coalesce(te.status,'settled') = 'settled'
          RETURN coalesce(sum(toInteger(te.amount)), 0) AS earned
        }
        // Spent
        CALL {
          WITH $uid AS uid
          MATCH (u:User {id: uid})
          OPTIONAL MATCH (u)-[:SPENT]->(ts:EcoTx)
            WHERE coalesce(ts.status,'settled') = 'settled'
          RETURN coalesce(sum(toInteger(ts.amount)), 0) AS spent
        }
        RETURN toInteger(earned - spent) AS balance
        """,
        uid=youth_id,
    ).single()
    return int(row["balance"]) if row and row["balance"] is not None else 0

def _attach_offer_to_tx(s: Session, tx_id: str, offer_id: str):
    s.run(
        """
        MATCH (t:EcoTx {id:$tx_id})
        MATCH (o:Offer {id:$offer_id})<-[:OF]-(b:BusinessProfile)
        MERGE (t)-[:FOR_OFFER]->(o)
        SET o.claims = coalesce(o.claims,0) + 1
        """,
        tx_id=tx_id, offer_id=offer_id,
    )

# ---------- quick QR debug ----------
@router.get("/qr/{code}/debug", response_model=Dict[str, Any])
def qr_debug(code: str, s: Session = Depends(session_dep)):
    meta = _fetch_qr_meta(s, code)
    if not meta:
        raise HTTPException(status_code=404, detail="QR not found")
    return {
        "code": meta.code,
        "business_id": meta.business_id,
        "business_name": meta.business_name,
        "location_name": meta.location_name,
        "qr_lat": meta.lat,
        "qr_lng": meta.lng,
        "active": meta.active,
        "pledge_tier": meta.pledge_tier,
        "rules": {
            "first_visit": meta.rules_first_visit,
            "return_visit": meta.rules_return_visit,
            "cooldown_hours": meta.rules_cooldown_hours,
            "daily_cap_per_user": meta.rules_daily_cap_per_user,
            "geofence_radius_m": meta.rules_geofence_radius_m,
        },
    }
@router.post("/qr/{code}/claim", response_model=ClaimResponse)
async def claim_eco(
    code: str,
    req: Request,
    payload: ClaimRequest = Body(...),
    x_forwarded_for: Optional[str] = Header(default=None, alias="X-Forwarded-For"),
    user_agent: Optional[str] = Header(default=None, alias="User-Agent"),
    debug: int = Query(0, description="Set to 1 to return debug payload"),
    account_uid: Optional[str] = Depends(current_user_id_optional_dep),
    s: Session = Depends(session_dep),
):
    # ----- identity (no duplicates; normalize account_uid) -------------------
    if not isinstance(account_uid, str) or not account_uid.strip():
        account_uid = None

    device_uid = get_youth_id(req)             # stable device identity
    resolved_uid = account_uid or device_uid   # ledger owner

    # ----- QR + business meta ------------------------------------------------
    meta = _fetch_qr_meta(s, code)
    if not meta:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="QR not found")
    if not meta.active:
        raise HTTPException(status_code=status.HTTP_410_GONE, detail="QR inactive")

    dbg: Dict[str, Any] = {}
    if debug:
        dbg["meta"] = {
            "code": meta.code,
            "business_id": meta.business_id,
            "business_name": meta.business_name,
            "location_name": meta.location_name,
            "qr_lat": meta.lat,
            "qr_lng": meta.lng,
            "active": meta.active,
            "pledge_tier": meta.pledge_tier,
            "rules": {
                "first_visit": meta.rules_first_visit,
                "return_visit": meta.rules_return_visit,
                "cooldown_hours": meta.rules_cooldown_hours,
                "daily_cap_per_user": meta.rules_daily_cap_per_user,
                "geofence_radius_m": meta.rules_geofence_radius_m,
            },
        }
        dbg["identity"] = {
            "device_uid": device_uid,
            "account_uid": account_uid,
            "resolved_uid": resolved_uid,
        }

    try:
        # ----- require a visible offer + validate it -------------------------
        offers = _business_visible_offers(s, meta.business_id)
        if debug:
            dbg["offers_preview"] = [{"id": o["id"], "title": o.get("title")} for o in offers]

        if len(offers) == 0:
            return ClaimResponse(
                ok=False, reason="no_offers",
                business_name=meta.business_name, location_name=meta.location_name,
                business_id=meta.business_id, debug=dbg if debug else None,
            )

        if payload.offer_id is None:
            return ClaimResponse(
                ok=False, reason="offer_required",
                business_name=meta.business_name, location_name=meta.location_name,
                business_id=meta.business_id,
                debug=(dbg | {"offers": offers}) if debug else None,
            )

        if not any(o["id"] == payload.offer_id for o in offers):
            return ClaimResponse(
                ok=False, reason="offer_invalid",
                business_name=meta.business_name, location_name=meta.location_name,
                business_id=meta.business_id, debug=dbg if debug else None,
            )

        # ----- geofence check (if coords on both sides + radius configured) ---
        if (
            meta.rules_geofence_radius_m
            and payload.lat is not None and payload.lng is not None
            and meta.lat is not None and meta.lng is not None
        ):
            dist = _haversine_m(payload.lat, payload.lng, meta.lat, meta.lng)
            if debug:
                dbg["geo"] = {
                    "provided": {"lat": payload.lat, "lng": payload.lng},
                    "qr": {"lat": meta.lat, "lng": meta.lng},
                    "distance_m": round(dist, 2),
                    "allowed_radius_m": meta.rules_geofence_radius_m,
                }
            if dist > float(meta.rules_geofence_radius_m):
                return ClaimResponse(
                    ok=False, reason="geofence",
                    business_name=meta.business_name, location_name=meta.location_name,
                    business_id=meta.business_id, debug=dbg if debug else None,
                )

        # ----- soft analytics + guard rails -----------------------------------
        now = _now()
        last_ms = _last_scan_ms(s, device_uid, meta.business_id)
        today0_ms = _ms_at_utc_day_start(now)
        todays = _today_scans_count(s, device_uid, meta.business_id, today0_ms)  # uses settled scans
        if debug:
            dbg["timing"] = {"now_ms": _now_ms(), "last_ms": last_ms}
            dbg["count"] = {"today_scans_for_business": todays, "today0_ms": today0_ms}

        # DAILY CAP: obey per-user-per-day cap from business rules
        if meta.rules_daily_cap_per_user is not None and todays >= int(meta.rules_daily_cap_per_user):
            return ClaimResponse(
                ok=False, reason="daily_cap",
                business_name=meta.business_name, location_name=meta.location_name,
                business_id=meta.business_id, debug=dbg if debug else None,
            )

        # COOLDOWN: durable bucket per device/business
        cool0 = _cooldown_bucket_ms(now, meta.rules_cooldown_hours)
        cool_id = _dedupe_id("cool", device_uid, meta.business_id, cool0)
        rows = s.run(
            """
            MERGE (c:CooldownClaim {id:$id})
            ON CREATE SET c.device_id=$did, c.business_id=$bid, c.bucket_ms=$bucket, c.createdAt=$now, c._is_new=true
            ON MATCH  SET c._is_new=false
            RETURN c._is_new AS is_new
            """,
            id=cool_id, did=device_uid, bid=meta.business_id, bucket=cool0, now=_now_ms(),
        ).data()
        if rows and rows[0]["is_new"] is False:
            return ClaimResponse(
                ok=False, reason="cooldown",
                business_name=meta.business_name, location_name=meta.location_name,
                business_id=meta.business_id, debug=dbg if debug else None,
            )

        # ----- award math ------------------------------------------------------
        first_visit = (last_ms is None)
        eps = _compute_eps(meta.pledge_tier, first_visit)
        base = meta.rules_first_visit if first_visit else meta.rules_return_visit
        reward = max(1, int(round(base * eps)))
        if debug:
            dbg["award_math"] = {"first_visit": first_visit, "base": base, "eps": eps, "reward": reward}

        # ----- write ledger ----------------------------------------------------
        tx_id = new_id("eco_tx")
        s.run(
            """
            MERGE (u:User {id:$uid})
            MERGE (b:BusinessProfile {id:$bid})
            MERGE (d:Device {id:$did})
            MERGE (u)-[:USES_DEVICE]->(d)

            MERGE (t:EcoTx {id:$tx_id})
              ON CREATE SET
                t.amount      = $eco,
                t.kind        = "scan",
                t.source      = "eyba",
                t.status      = "settled",
                t.createdAt   = $now,
                t.qr_code     = $qr,
                t.device_id   = $did,
                t.account_id  = $uid,
                t.first_visit = $first_visit,
                t.lat         = $lat,
                t.lng         = $lng
            MERGE (u)-[:EARNED]->(t)
            MERGE (b)-[:TRIGGERED]->(t)
            """,
            uid=resolved_uid,
            did=device_uid,
            bid=meta.business_id,
            tx_id=tx_id,
            eco=reward,
            now=_now_ms(),
            qr=meta.code,
            first_visit=first_visit,
            lat=payload.lat,
            lng=payload.lng,
        )

        # link chosen offer
        _attach_offer_to_tx(s, tx_id, payload.offer_id)

        # bump business counters
        s.run(
            """
            MATCH (b:BusinessProfile {id:$bid})
            SET b.eco_given_total = coalesce(b.eco_given_total,0) + $eco,
                b.minted_eco      = coalesce(b.minted_eco,0) + $eco
            """,
            bid=meta.business_id, eco=reward,
        )

        # return balance for resolved user
        balance_after = _youth_balance(s, resolved_uid)

        return ClaimResponse(
            ok=True,
            awarded_eco=reward,
            balance=balance_after,
            business_name=meta.business_name,
            location_name=meta.location_name,
            tx_id=tx_id,
            business_id=meta.business_id,
            debug=dbg if debug else None,
        )

    except Exception as e:
        logging.exception("claim_eco failed")
        if debug:
            import traceback as _tb
            tb = _tb.format_exc()
            raise HTTPException(status_code=500, detail=f"{type(e).__name__}: {e}\n{tb}")
        raise HTTPException(status_code=500, detail=f"{type(e).__name__}: {e}")


# ---------- attach offer (post-claim) ----------
@router.post("/tx/{tx_id}/attach_offer", response_model=AttachOfferResponse)
def attach_offer(tx_id: str, payload: AttachOfferRequest, s: Session = Depends(session_dep)):
    _attach_offer_to_tx(s, tx_id, payload.offer_id)
    return AttachOfferResponse(ok=True)
