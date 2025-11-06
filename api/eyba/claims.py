from __future__ import annotations

import hashlib
import logging
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional, Literal, Dict, Any

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
from site_backend.core.user_guard import current_user_id  # strict, same as /eco_local/wallet

router = APIRouter(prefix="/eco_local", tags=["eco_local"])

PledgeTier = Literal["starter", "builder", "leader"]

# ---------- Request / Response ----------
class ClaimRequest(BaseModel):
    lat: Optional[float] = None
    lng: Optional[float] = None

class ClaimResponse(BaseModel):
    ok: bool
    awarded_eco: int = 0           # UI compatibility; here: “contributed ECO”
    balance: int = 0
    reason: Optional[str] = None   # geofence | cooldown | daily_cap | insufficient_balance
    business_name: Optional[str] = None
    location_name: Optional[str] = None
    tx_id: Optional[str] = None
    business_id: Optional[str] = None
    debug: Optional[Dict[str, Any]] = None

# Deprecated (kept for compatibility, now a 410)
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

def _cooldown_bucket_ms(now: datetime, hours: int) -> int:
    slot_ms = max(1, hours) * 3600 * 1000
    return (int(now.timestamp() * 1000) // slot_ms) * slot_ms

def _dedupe_id(prefix: str, uid: str, bid: str, bucket_ms: int) -> str:
    h = hashlib.sha256(f"{prefix}|{uid}|{bid}|{bucket_ms}".encode()).hexdigest()[:24]
    return f"{prefix}_{h}"

# ── Dev/test guard bypass ─────────────────────────────────────────────────────
def _bypass_requested(req: Request, debug: int) -> bool:
    """
    True if caller asked to bypass guard rails.
    - Query:   ?nocap=1
    - Query:   ?debug=2
    - Header:  X-ECO Local-Dev-Bypass: 1
    - Env:     ECO_LOCAL_DEV_DISABLE_GUARDS=1
    """
    if os.getenv("ECO_LOCAL_DEV_DISABLE_GUARDS") == "1":
        return True
    if debug and int(debug) >= 2:
        return True
    if req.query_params.get("nocap") == "1":
        return True
    if req.headers.get("X-ECO Local-Dev-Bypass") == "1":
        return True
    return False

def _bypass_geofence(req: Request, debug: int) -> bool:
    """
    True if caller asked to bypass geofence.
    - Query:  ?nogeo=1
    - Header: X-ECO Local-NoGeo: 1
    - Env:    ECO_LOCAL_DEV_DISABLE_GUARDS=1
    - Or debug>=2
    """
    if os.getenv("ECO_LOCAL_DEV_DISABLE_GUARDS") == "1":
        return True
    if debug and int(debug) >= 2:
        return True
    if req.query_params.get("nogeo") == "1":
        return True
    if req.headers.get("X-ECO Local-NoGeo") == "1":
        return True
    return False

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
async def current_user_id_optional_dep(req: Request) -> Optional[str]:
    try:
        val = await current_user_id(req)
        return val if isinstance(val, str) and val else None
    except Exception:
        return None

# ---------- small read helpers (warning-free rel types) ----------
def _last_visit_ms(s: Session, device_id: str, business_id: str) -> Optional[int]:
    row = s.run(
        """
        MATCH (b:BusinessProfile {id:$bid})-[rel]->(t:EcoTx)
        WHERE type(rel)='COLLECTED'
          AND t.kind IN ['CONTRIBUTE','BURN_REWARD']
          AND t.device_id = $did
        RETURN max(toInteger(t.createdAt)) AS last
        """,
        did=device_id,
        bid=business_id,
    ).single()
    last = row["last"] if row else None
    return int(last) if last is not None else None

def _today_visits_count(s: Session, device_id: str, business_id: str, today0_ms: int) -> int:
    row = s.run(
        """
        MATCH (b:BusinessProfile {id:$bid})-[rel]->(t:EcoTx)
        WHERE type(rel)='COLLECTED'
          AND t.kind IN ['CONTRIBUTE','BURN_REWARD']
          AND t.device_id=$did
          AND toInteger(t.createdAt) >= $today0
        RETURN count(t) AS c
        """,
        did=device_id,
        bid=business_id,
        today0=today0_ms,
    ).single()
    return int(row["c"] or 0) if row else 0

def _youth_balance(s: Session, youth_id: Optional[str]) -> int:
    if not youth_id:
        return 0
    row = s.run(
        """
        // Earned
        CALL () {
          WITH $uid AS uid
          OPTIONAL MATCH (:User {id: uid})-[r]->(te:EcoTx {status:'settled'})
          WHERE type(r) = 'EARNED' AND te.kind IN ['MINT_ACTION']
          RETURN coalesce(sum(toInteger(coalesce(te.amount, te.eco, 0))), 0) AS earned
        }
        // Spent (retired / contributed)
        CALL () {
          WITH $uid AS uid
          OPTIONAL MATCH (:User {id: uid})-[r2]->(ts:EcoTx {status:'settled'})
          WHERE type(r2) = 'SPENT' AND ts.kind IN ['BURN_REWARD','CONTRIBUTE']
          RETURN coalesce(sum(toInteger(coalesce(ts.amount, ts.eco, 0))), 0) AS spent
        }
        RETURN toInteger(earned - spent) AS balance
        """,
        uid=youth_id,
    ).single()
    return int(row["balance"]) if row and row["balance"] is not None else 0

# ---------- Season controls ----------
def _season_multiplier(s: Session) -> float:
    rec = s.run(
        """
        MATCH (se:Season)
        WITH se, coalesce(se.emission_multiplier, 1.0) AS mul
        RETURN mul
        ORDER BY coalesce(se.created_at, datetime({epochMillis:0})) DESC
        LIMIT 1
        """
    ).single()
    try:
        return float(rec["mul"]) if rec and rec["mul"] is not None else 1.0
    except Exception:
        return 1.0

# ---------- Balance helper to mirror the wallet/counter ----------
def _wallet_balance_like_counter(s: Session, resolved_uid: Optional[str], device_uid: str) -> int:
    """
    EXACTLY like the wallet/counter:
    - If we have a resolved user id → use that user id.
    - Else, if we can infer a user who uses this device → use that user id.
    - Else, return 0 (counter shows a user balance, not device).
    """
    if resolved_uid:
        return _youth_balance(s, resolved_uid)

    rec = s.run(
        """
        MATCH (d:Device {id:$did})<-[:USES_DEVICE]-(u:User)
        RETURN u.id AS uid
        ORDER BY u.id
        LIMIT 1
        """,
        did=device_uid,
    ).single()
    inferred_uid = rec["uid"] if rec and rec.get("uid") else None
    return _youth_balance(s, inferred_uid) if inferred_uid else 0

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
        "dev_bypass_note": "Pass ?nocap=1 or debug=2 to bypass cap/cooldown; ?nogeo=1 to bypass geofence.",
    }
@router.post("/qr/{code}/claim", response_model=ClaimResponse)
async def claim_eco(
    code: str,
    req: Request,
    payload: Optional[ClaimRequest] = Body(default=None),  # body still optional for easy dev
    x_forwarded_for: Optional[str] = Header(default=None, alias="X-Forwarded-For"),
    user_agent: Optional[str] = Header(default=None, alias="User-Agent"),
    debug: int = Query(0, description="Set to 1 to include debug payload; 2 = plus dev bypass"),
    # 🚨 make auth STRICT: same resolver EcoCounter/Wallet uses
    uid: str = Depends(current_user_id),
    s: Session = Depends(session_dep),
):
    # ----- identity (strict UID; no device fallback) -------------------------
    device_uid = get_youth_id(req)
    resolved_uid: Optional[str] = uid  # always present due to strict dep

    # Optional relink last 30d device txs to this user (idempotent)
    s.run(
        """
        MERGE (u:User {id:$uid})
        MERGE (d:Device {id:$did})
        MERGE (u)-[:USES_DEVICE]->(d)
        WITH u, d
        MATCH (t:EcoTx)
        WHERE t.device_id = $did AND toInteger(t.createdAt) >= $since
        FOREACH (_ IN CASE WHEN t.kind IN ['MINT_ACTION'] THEN [1] ELSE [] END |
          MERGE (u)-[:EARNED]->(t)
        )
        FOREACH (_ IN CASE WHEN t.kind IN ['BURN_REWARD','CONTRIBUTE'] THEN [1] ELSE [] END |
          MERGE (u)-[:SPENT]->(t)
        )
        """,
        did=device_uid, uid=uid, since=_now_ms() - 30 * 24 * 3600 * 1000,
    )

    # ----- QR + business meta ------------------------------------------------
    meta = _fetch_qr_meta(s, code)
    if not meta:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="QR not found")
    if not meta.active:
        raise HTTPException(status_code=status.HTTP_410_GONE, detail="QR inactive")

    # Balance BEFORE... EXACTLY like wallet/counter (strict user id)
    balance_before = _youth_balance(s, uid)

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
            "resolved_uid": uid,
            "auth_header": ("present" if (req.headers.get("authorization") or req.headers.get("Authorization")) else "missing"),
        }
        dbg["balance_before"] = balance_before

    # ----- guard rails -------------------------------------------------------
    bypass = _bypass_requested(req, debug)
    bypass_geo = _bypass_geofence(req, debug)
    if debug:
        dbg["bypass"] = {"nocap_cooldown": bool(bypass), "nogeo": bool(bypass_geo)}

    # Geofence (only if QR has coords + radius and not bypassed)
    if not bypass_geo and meta.lat is not None and meta.lng is not None and meta.rules_geofence_radius_m:
        if payload is None or payload.lat is None or payload.lng is None:
            return ClaimResponse(
                ok=False, reason="geofence",
                business_name=meta.business_name, location_name=meta.location_name,
                business_id=meta.business_id, debug=dbg if debug else None,
            )
        dist = _haversine_m(meta.lat, meta.lng, float(payload.lat), float(payload.lng))
        if debug:
            dbg["geofence"] = {"qr": [meta.lat, meta.lng], "scan": [payload.lat, payload.lng],
                               "radius_m": meta.rules_geofence_radius_m, "distance_m": round(dist, 2)}
        if dist > float(meta.rules_geofence_radius_m):
            return ClaimResponse(
                ok=False, reason="geofence",
                business_name=meta.business_name, location_name=meta.location_name,
                business_id=meta.business_id, debug=dbg if debug else None,
            )

    # Cooldown + daily cap (per device per business)
    now_dt = _now()
    if not bypass:
        last_ms = _last_visit_ms(s, device_uid, meta.business_id)
        cooldown_ms = max(1, meta.rules_cooldown_hours) * 3600 * 1000
        if last_ms is not None and (_now_ms() - int(last_ms)) < cooldown_ms:
            if debug:
                dbg["cooldown"] = {"last_ms": last_ms, "cooldown_ms": cooldown_ms}
            return ClaimResponse(
                ok=False, reason="cooldown",
                business_name=meta.business_name, location_name=meta.location_name,
                business_id=meta.business_id, debug=dbg if debug else None,
            )

        today0 = _ms_at_utc_day_start(now_dt)
        count_today = _today_visits_count(s, device_uid, meta.business_id, today0)
        if debug:
            dbg["today"] = {"count": count_today, "cap": meta.rules_daily_cap_per_user}
        if count_today >= max(1, meta.rules_daily_cap_per_user):
            return ClaimResponse(
                ok=False, reason="daily_cap",
                business_name=meta.business_name, location_name=meta.location_name,
                business_id=meta.business_id, debug=dbg if debug else None,
            )

    # ----- contribution math --------------------------------------------------
    first_visit = (_last_visit_ms(s, device_uid, meta.business_id) is None)
    eps = _compute_eps(meta.pledge_tier, first_visit)
    base = meta.rules_first_visit if first_visit else meta.rules_return_visit
    target = max(1, int(round(base * eps)))
    mul = _season_multiplier(s)
    target = max(1, int(round(target * mul)))

    if debug:
        dbg["contribution_math"] = {
            "first_visit": first_visit, "base": base, "eps": eps,
            "season_multiplier": mul, "target_contribution": target
        }

    # Balance check
    if balance_before <= 0:
        return ClaimResponse(
            ok=False, reason="insufficient_balance",
            business_name=meta.business_name, location_name=meta.location_name,
            business_id=meta.business_id, debug=dbg if debug else None,
        )

    contributed = min(balance_before, target)
    if contributed <= 0:
        return ClaimResponse(
            ok=False, reason="insufficient_balance",
            business_name=meta.business_name, location_name=meta.location_name,
            business_id=meta.business_id, debug=dbg if debug else None,
        )

    # ----- write ledger -------------------------------------------------------
    now_ms = _now_ms()
    tx_id = hashlib.sha256(f"tx|contrib|{device_uid}|{meta.business_id}|{now_ms}".encode()).hexdigest()[:24]

    s.run(
        """
        MERGE (u:User {id:$uid})
        MERGE (b:BusinessProfile {id:$bid})
        MERGE (d:Device {id:$did})
        MERGE (u)-[:USES_DEVICE]->(d)

        MERGE (t:EcoTx {id:$tx_id})
          ON CREATE SET
            t.amount      = $eco,
            t.kind        = "CONTRIBUTE",
            t.source      = "eco_local",
            t.status      = "settled",
            t.createdAt   = $now_ms,
            t.at          = datetime($now_iso),
            t.qr_code     = $qr,
            t.device_id   = $did,
            t.account_id  = $uid,
            t.first_visit = $first_visit,
            t.lat         = $lat,
            t.lng         = $lng

        MERGE (u)-[:SPENT]->(t)
        MERGE (b)-[:COLLECTED]->(t)
        """,
        uid=uid,
        did=device_uid,
        bid=meta.business_id,
        tx_id=tx_id,
        eco=contributed,
        now_ms=now_ms,
        now_iso=_now().isoformat(),
        qr=meta.code,
        first_visit=first_visit,
        lat=(payload.lat if payload else None),
        lng=(payload.lng if payload else None),
    )

    # AFTER... exactly like wallet/counter
    balance_after = _youth_balance(s, uid)
    if debug:
        dbg["balance_after"] = balance_after

    return ClaimResponse(
        ok=True,
        awarded_eco=contributed,
        balance=balance_after,
        business_name=meta.business_name,
        location_name=meta.location_name,
        tx_id=tx_id,
        business_id=meta.business_id,
        debug=dbg if debug else None,
    )

# ---------- attach offer (post-claim) [DEPRECATED] ----------
@router.post("/tx/{tx_id}/attach_offer", response_model=AttachOfferResponse)
def attach_offer(tx_id: str, payload: AttachOfferRequest, s: Session = Depends(session_dep)):
    """
    Deprecated under the new mechanics. ECO Local claims are contributions to businesses.
    Offers redemption should be handled by separate endpoints if/when reintroduced.
    """
    raise HTTPException(
        status_code=410,
        detail="Deprecated: attach_offer is no longer supported under unilateral contributions.",
    )
