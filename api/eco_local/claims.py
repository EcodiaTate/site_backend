from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional, Literal, List, Dict, Any

from fastapi import APIRouter, Body, Depends, HTTPException, Request, status
from neo4j import Session
from pydantic import BaseModel

from site_backend.core.neo_driver import session_dep
from site_backend.core.user_guard import current_user_id  # 👈 use real logged-in youth

router = APIRouter(prefix="/eco-local", tags=["eco-local"])

PledgeTier = Literal["starter", "builder", "leader"]


# ---------- Request / Response models ----------

class ClaimRequest(BaseModel):
    """
    Payload from the client when scanning the QR.
    We only need location for geofencing.
    """
    lat: Optional[float] = None
    lng: Optional[float] = None


class ClaimableOffer(BaseModel):
    """
    Offer that can potentially be claimed at this business.
    eco_price is what the youth must spend.
    can_claim is computed from the youth's ECO balance.
    """
    id: str
    title: str
    blurb: Optional[str] = None
    eco_price: int
    stock: Optional[int] = None
    valid_until: Optional[str] = None
    can_claim: bool


class QRScanOffersResponse(BaseModel):
    """
    Returned to the FE when a youth scans the QR:
      - which business they’re at
      - their current ECO balance (offer wallet)
      - list of active offers with affordability.
    """
    ok: bool
    reason: Optional[str] = None  # e.g. "geofence"
    business_id: Optional[str] = None
    business_name: Optional[str] = None
    location_name: Optional[str] = None
    balance: int = 0
    offers: List[ClaimableOffer] = []


# ---------- helpers ----------

def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _device_hash(ip: str, ua: str) -> str:
    """
    Kept for possible analytics / anti-abuse later,
    but NOT used for wallet identity anymore.
    """
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
    rules_geofence_radius_m: Optional[int]


def _fetch_qr_meta(s: Session, code: str) -> Optional[DBQRMeta]:
    """
    Expects:
      (q:QR {code})-[:OF]->(b:BusinessProfile)

    We only keep what we need for the QR → offers flow:
      - business id/name/location
      - QR coords + geofence radius (if configured)
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
        rules_geofence_radius_m=int(rec["geofence_m"]) if rec["geofence_m"] is not None else None,
    )


# ---------- Wallet balance (parity with offers.py) ----------

def _wallet_balance_for_offers(s: Session, user_id: str) -> int:
    """
    EXACT same wallet math as offers.py::_user_wallet_balance:

      Earned: MINT_ACTION (settled)
      Spent:  BURN_REWARD, CONTRIBUTE (settled)

    This ensures the balance shown in the QR modal matches what
    /offers/{offer_id}/redeem will actually use.
    """
    row = s.run(
        """
        // Earned (posted)
        CALL {
          WITH $uid AS uid
          OPTIONAL MATCH (:User {id: uid})-[:EARNED]->(te:EcoTx {status:'settled'})
          WHERE te.kind IN ['MINT_ACTION']
          RETURN coalesce(sum(toInteger(coalesce(te.amount, te.eco, 0))), 0) AS earned
        }
        // Spent (posted)
        CALL {
          WITH $uid AS uid
          OPTIONAL MATCH (:User {id: uid})-[:SPENT]->(ts:EcoTx {status:'settled'})
          WHERE ts.kind IN ['BURN_REWARD','CONTRIBUTE']
          RETURN coalesce(sum(toInteger(coalesce(ts.amount, ts.eco, 0))), 0) AS spent
        }
        RETURN toInteger(earned - spent) AS balance
        """,
        uid=user_id,
    ).single()
    return int(row["balance"]) if row and row["balance"] is not None else 0


# ---------- Active offers for the business ----------

def _active_offers_for_business(s: Session, business_id: str) -> List[Dict[str, Any]]:
    """
    Active, ECO-priced, in-stock, non-expired offers for a business.
    Mirrors the visibility logic in offers.py::_is_visible but done here in Cypher.
    """
    today_iso = _now_utc().date().isoformat()
    recs = s.run(
        """
        MATCH (o:Offer)-[:OF]->(b:BusinessProfile {id:$bid})
        WHERE coalesce(o.status,'active') = 'active'
          AND toInteger(coalesce(o.eco_price,0)) > 0
          AND (o.stock IS NULL OR toInteger(o.stock) > 0)
          AND (
               o.valid_until IS NULL
            OR o.valid_until = ''
            OR date(o.valid_until) >= date($today)
          )
        RETURN
          o.id AS id,
          o.title AS title,
          o.blurb AS blurb,
          toInteger(coalesce(o.eco_price,0)) AS eco_price,
          toInteger(coalesce(o.stock, -1)) AS stock,
          o.valid_until AS valid_until
        ORDER BY o.title ASC
        """,
        bid=business_id,
        today=today_iso,
    )
    out: List[Dict[str, Any]] = []
    for r in recs:
        out.append(
            {
                "id": r["id"],
                "title": r["title"],
                "blurb": r.get("blurb"),
                "eco_price": int(r["eco_price"] or 0),
                "stock": int(r["stock"]) if r.get("stock") is not None else None,
                "valid_until": r.get("valid_until"),
            }
        )
    return out


# ---------- Primary endpoint: QR scan → offers ----------

@router.post("/qr/{code}/offers", response_model=QRScanOffersResponse)
def qr_scan_offers(
    code: str,
    req: Request,
    payload: ClaimRequest = Body(...),
    user_id: str = Depends(current_user_id),  # 👈 must be a logged-in youth
    s: Session = Depends(session_dep),
):
    """
    Scanning a QR does NOT mint ECO.

    This endpoint:
      - Identifies the business from the QR.
      - Optionally enforces geofence (if radius set).
      - Computes the youth's spendable ECO balance (offer wallet).
      - Returns all active offers for that business with `eco_price`
        and `can_claim` flags.

    Flow:
      1. Youth scans QR at the business.
      2. FE calls POST /eco-local/qr/{code}/offers with lat/lng.
      3. This returns business + offers + balance.
      4. FE shows modal: youth chooses one offer.
      5. FE calls POST /eco-local/offers/{offer_id}/redeem (in offers.py).
         That endpoint performs the actual transaction:
           - ECO debited from youth
           - business balances/sponsor updated
           - ECO retired (BURN_REWARD)
    """
    meta = _fetch_qr_meta(s, code)
    if not meta or not meta.active:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="QR not found or inactive")

    # Offer-wallet balance for this authenticated youth
    balance = _wallet_balance_for_offers(s, user_id)

    # Optional geofence check (only if both sides have coordinates + radius)
    if (
        meta.rules_geofence_radius_m
        and payload.lat is not None
        and payload.lng is not None
        and meta.lat is not None
        and meta.lng is not None
    ):
        dist = _haversine_m(payload.lat, payload.lng, meta.lat, meta.lng)
        if dist > float(meta.rules_geofence_radius_m):
            # No offers if you're too far away
            return QRScanOffersResponse(
                ok=False,
                reason="geofence",
                business_id=meta.business_id,
                business_name=meta.business_name,
                location_name=meta.location_name,
                balance=balance,
                offers=[],
            )

    # Fetch visible offers for this business
    offers_raw = _active_offers_for_business(s, meta.business_id)
    offers_out: List[ClaimableOffer] = []
    for o in offers_raw:
        eco_price = int(o["eco_price"] or 0)
        offers_out.append(
            ClaimableOffer(
                id=o["id"],
                title=o["title"] or "",
                blurb=o.get("blurb"),
                eco_price=eco_price,
                stock=o.get("stock"),
                valid_until=o.get("valid_until"),
                can_claim=(balance >= eco_price),
            )
        )

    return QRScanOffersResponse(
        ok=True,
        business_id=meta.business_id,
        business_name=meta.business_name,
        location_name=meta.location_name,
        balance=balance,
        offers=offers_out,
    )
