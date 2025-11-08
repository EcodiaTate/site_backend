# api/routers/eco_local_claims.py
from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Optional, Literal

from fastapi import APIRouter, Body, Depends, Header, HTTPException, Request, status
from neo4j import Session
from pydantic import BaseModel

from site_backend.core.neo_driver import session_dep
from site_backend.api.eco_local.neo_business import new_id  # id helper

router = APIRouter(prefix="/eco_local", tags=["eco_local"])

PledgeTier = Literal["starter", "builder", "leader"]

# ---------- Request / Response ----------
class ClaimRequest(BaseModel):
    lat: Optional[float] = None
    lng: Optional[float] = None

class ClaimResponse(BaseModel):
    ok: bool
    awarded_eco: int = 0
    balance: int = 0
    reason: Optional[str] = None
    business_name: Optional[str] = None
    location_name: Optional[str] = None
    tx_id: Optional[str] = None

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
    phi1=m.radians(lat1); phi2=m.radians(lat2)
    dphi=m.radians(lat2-lat1); dl=m.radians(lon2-lon1)
    a=m.sin(dphi/2)**2 + m.cos(phi1)*m.cos(phi2)*m.sin(dl/2)**2
    c=2*m.atan2(m.sqrt(a), m.sqrt(1-a))
    return R*c

def get_youth_id(req: Request) -> str:
    ip = req.client.host if req.client else "0.0.0.0"
    ua = req.headers.get("user-agent", "")
    return f"y_{_device_hash(ip, ua)}"

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
    """
    Expects:
      (q:QR {code})-[:OF]->(b:BusinessProfile)
    Optional q: lat, lng, active
    Optional b: name, area/suburb/location (for display), pledge_tier, and rules fields.
    Defaults via coalesce.
    """
    rec = s.run(
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
    ).single()
    if not rec:
        return None
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

def _last_scan_ms(s: Session, youth_id: str, business_id: str) -> Optional[int]:
    rec = s.run(
        """
        MATCH (:User {id:$uid})-[:EARNED]->(t:EcoTx)<-[:TRIGGERED]-(:BusinessProfile {id:$bid})
        WHERE t.kind = 'scan'
        RETURN max(t.createdAt) AS last
        """,
        uid=youth_id, bid=business_id,
    ).single()
    return rec["last"] if rec and rec["last"] is not None else None

def _today_scans_count(s: Session, youth_id: str, business_id: str, today0_ms: int) -> int:
    rec = s.run(
        """
        MATCH (:User {id:$uid})-[:EARNED]->(t:EcoTx)<-[:TRIGGERED]-(:BusinessProfile {id:$bid})
        WHERE t.kind = 'scan' AND t.createdAt >= $today0
        RETURN count(t) AS c
        """,
        uid=youth_id, bid=business_id, today0=today0_ms,
    ).single()
    return int(rec["c"] or 0)

# ---------- Youth balance (ledger) ----------
def _youth_balance(s: Session, youth_id: str) -> int:
    """
    Balance = sum(EARNED) - sum(SPENT) over settled EcoTx.
    Relationship-driven so it's robust to different t.kind values.
    """
    rec = s.run(
        """
        MATCH (u:User {id:$uid})
        OPTIONAL MATCH (u)-[:EARNED]->(te:EcoTx)
          WHERE coalesce(te.status,'settled') = 'settled'
        WITH u, coalesce(sum(toInteger(te.amount)),0) AS earned
        OPTIONAL MATCH (u)-[:SPENT]->(ts:EcoTx)
          WHERE coalesce(ts.status,'settled') = 'settled'
        RETURN toInteger(earned - coalesce(sum(toInteger(ts.amount)),0)) AS balance
        """,
        uid=youth_id,
    ).single()
    return int(rec["balance"] or 0)

# ---------- endpoint ----------
@router.post("/qr/{code}/claim", response_model=ClaimResponse)
def claim_eco(
    code: str,
    req: Request,
    payload: ClaimRequest = Body(...),
    x_forwarded_for: Optional[str] = Header(default=None, alias="X-Forwarded-For"),
    user_agent: Optional[str] = Header(default=None, alias="User-Agent"),
    s: Session = Depends(session_dep),
):
    youth_id = get_youth_id(req)

    meta = _fetch_qr_meta(s, code)
    if not meta or not meta.active:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="QR not found or inactive")

    # Geofence (only if both sides have coordinates)
    if (
        meta.rules_geofence_radius_m
        and payload.lat is not None and payload.lng is not None
        and meta.lat is not None and meta.lng is not None
    ):
        if _haversine_m(payload.lat, payload.lng, meta.lat, meta.lng) > float(meta.rules_geofence_radius_m):
            return ClaimResponse(ok=False, reason="geofence",
                                 business_name=meta.business_name, location_name=meta.location_name)

    # Cooldown + daily cap
    now = _now()
    last_ms = _last_scan_ms(s, youth_id, meta.business_id)
    if last_ms is not None:
        if (now - datetime.fromtimestamp(last_ms / 1000, tz=timezone.utc)) < timedelta(hours=meta.rules_cooldown_hours):
            return ClaimResponse(ok=False, reason="cooldown",
                                 business_name=meta.business_name, location_name=meta.location_name)

    today0_ms = _ms_at_utc_day_start(now)
    if _today_scans_count(s, youth_id, meta.business_id, today0_ms) >= meta.rules_daily_cap_per_user:
        return ClaimResponse(ok=False, reason="daily_cap",
                             business_name=meta.business_name, location_name=meta.location_name)

    # Dynamic EPS + reward
    first_visit = last_ms is None
    eps = _compute_eps(meta.pledge_tier, first_visit)
    base = meta.rules_first_visit if first_visit else meta.rules_return_visit
    reward = max(1, int(round(base * eps)))

    # ---- canonical unified ledger write ----
    tx_id = new_id("eco_tx")
    s.run(
        """
        MERGE (u:User {id:$uid})
        MERGE (b:BusinessProfile {id:$bid})
        MERGE (t:EcoTx {id:$tx_id})
          ON CREATE SET
            t.amount      = $eco,
            t.kind        = "scan",
            t.source      = "eco_local",
            t.status      = "settled",
            t.createdAt   = $now,
            t.qr_code     = $qr
        MERGE (u)-[:EARNED]->(t)
        MERGE (b)-[:TRIGGERED]->(t)
        // bump business counters
        SET b.eco_given_total = coalesce(b.eco_given_total,0) + $eco,
            b.minted_eco       = coalesce(b.minted_eco,0) + $eco
        """,
        uid=youth_id, bid=meta.business_id, tx_id=tx_id, eco=reward, now=_now_ms(), qr=meta.code,
    )

    # Compute post-claim balance from ledger
    balance_after = _youth_balance(s, youth_id)

    return ClaimResponse(
        ok=True,
        awarded_eco=reward,
        balance=balance_after,
        business_name=meta.business_name,
        location_name=meta.location_name,
        tx_id=tx_id,
    )
