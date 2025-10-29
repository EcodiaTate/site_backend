# api/eyba/claims.py
from __future__ import annotations

import hashlib
import logging
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
from site_backend.core.user_guard import current_user_id as _current_user_id

router = APIRouter(prefix="/eyba", tags=["eyba"])

PledgeTier = Literal["starter", "builder", "leader"]


# ---------- Request / Response ----------
class ClaimRequest(BaseModel):
    lat: Optional[float] = None
    lng: Optional[float] = None


class ClaimResponse(BaseModel):
    ok: bool
    awarded_eco: int = 0
    balance: int = 0
    reason: Optional[str] = None  # geofence | cooldown | daily_cap
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
    """
    Return the authenticated user id as a string, or None.
    Never returns complex objects (e.g. Request).
    """
    try:
        val = await _current_user_id(req)
        return val if isinstance(val, str) and val else None
    except Exception:
        return None


def _last_visit_ms(s: Session, device_id: str, business_id: str) -> Optional[int]:
    row = s.run(
        """
        MATCH (:BusinessProfile {id:$bid})-[:TRIGGERED]->(t:EcoTx)
        WHERE t.kind = 'MINT_ACTION' AND t.device_id = $did
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
        MATCH (:BusinessProfile {id:$bid})-[:TRIGGERED]->(t:EcoTx)
        WHERE t.kind='MINT_ACTION' AND t.device_id=$did AND toInteger(t.createdAt) >= $today0
        RETURN count(t) AS c
        """,
        did=device_id,
        bid=business_id,
        today0=today0_ms,
    ).single()
    return int(row["c"] or 0) if row else 0


# ---------- Youth balance (ledger) ----------
def _youth_balance(s: Session, youth_id: str) -> int:
    """
    Balance = sum(EARNED kind=MINT_ACTION) - sum(SPENT kind=BURN_REWARD) over settled EcoTx.
    """
    row = s.run(
        """
        // Earned
        CALL {
          WITH $uid AS uid
          MATCH (u:User {id: uid})
          OPTIONAL MATCH (u)-[:EARNED]->(te:EcoTx {kind:'MINT_ACTION', status:'settled'})
          RETURN coalesce(sum(toInteger(te.amount)), 0) AS earned
        }
        // Spent (retired)
        CALL {
          WITH $uid AS uid
          MATCH (u:User {id: uid})
          OPTIONAL MATCH (u)-[:SPENT]->(ts:EcoTx {kind:'BURN_REWARD', status:'settled'})
          RETURN coalesce(sum(toInteger(ts.amount)), 0) AS spent
        }
        RETURN toInteger(earned - spent) AS balance
        """,
        uid=youth_id,
    ).single()
    return int(row["balance"]) if row and row["balance"] is not None else 0


# ---------- Season controls ----------
def _season_multiplier(s: Session) -> float:
    """
    Reads Season.emission_multiplier. Defaults to 1.0.
    """
    rec = s.run(
        """
        MATCH (se:Season)
        RETURN coalesce(se.emission_multiplier, 1.0) AS mul
        ORDER BY se.created_at DESC
        LIMIT 1
        """
    ).single()
    try:
        return float(rec["mul"]) if rec and rec["mul"] is not None else 1.0
    except Exception:
        return 1.0


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


# ---------- claim ----------
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
        today0_ms = _ms_at_utc_day_start(now)
        last_ms = _last_visit_ms(s, device_uid, meta.business_id)
        todays = _today_visits_count(s, device_uid, meta.business_id, today0_ms)
        if debug:
            dbg["timing"] = {"now_ms": _now_ms(), "last_ms": last_ms}
            dbg["count"] = {"today_visits_for_business": todays, "today0_ms": today0_ms}

        # DAILY CAP per-user-per-day
        if meta.rules_daily_cap_per_user is not None and todays >= int(meta.rules_daily_cap_per_user):
            return ClaimResponse(
                ok=False, reason="daily_cap",
                business_name=meta.business_name, location_name=meta.location_name,
                business_id=meta.business_id, debug=dbg if debug else None,
            )

        # COOLDOWN bucket per device/business
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

        # seasonal multiplier
        mul = _season_multiplier(s)
        reward = max(1, int(round(reward * mul)))

        if debug:
            dbg["award_math"] = {
                "first_visit": first_visit, "base": base, "eps": eps,
                "season_multiplier": mul, "reward_final": reward
            }

        # ----- write ledger (MINT_ACTION associated to business) --------------
        tx_id = hashlib.sha256(f"tx|{device_uid}|{meta.business_id}|{_now_ms()}".encode()).hexdigest()[:24]
        s.run(
            """
            MERGE (u:User {id:$uid})
            MERGE (b:BusinessProfile {id:$bid})
            MERGE (d:Device {id:$did})
            MERGE (u)-[:USES_DEVICE]->(d)

            MERGE (t:EcoTx {id:$tx_id})
              ON CREATE SET
                t.amount      = $eco,
                t.kind        = "MINT_ACTION",
                t.source      = "eyba",
                t.status      = "settled",
                t.createdAt   = $now_ms,
                t.at          = datetime($now_iso),
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
            now_ms=_now_ms(),
            now_iso=_now().isoformat(),
            qr=meta.code,
            first_visit=first_visit,
            lat=payload.lat,
            lng=payload.lng,
        )

        # If user has just authenticated, relink last 30d device txs to their account
        if account_uid:
            s.run(
                """
                MATCH (d:Device {id:$did})<-[:USES_DEVICE]-(:User)-[:EARNED]->(t:EcoTx)
                WHERE toInteger(t.createdAt) >= $since
                WITH DISTINCT t
                MERGE (u:User {id:$uid})-[:EARNED]->(t)
                """,
                did=device_uid, uid=account_uid, since=_now_ms() - 30*24*3600*1000,
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


# ---------- attach offer (post-claim) [DEPRECATED] ----------
@router.post("/tx/{tx_id}/attach_offer", response_model=AttachOfferResponse)
def attach_offer(tx_id: str, payload: AttachOfferRequest, s: Session = Depends(session_dep)):
    """
    Deprecated under the new mechanics. Mint (MINT_ACTION) transactions must NOT be linked to Offers.
    Offer linkage happens only when retiring ECO on redemption (BURN_REWARD via /offers/{id}/redeem).
    """
    raise HTTPException(
        status_code=410,
        detail="Deprecated: attach_offer is no longer supported. Use /offers/{id}/redeem to burn ECO when claiming a reward.",
    )
