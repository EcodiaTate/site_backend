
# api/routers/offers.py
from __future__ import annotations

from datetime import date, timedelta, datetime, timezone
from typing import List, Optional, Literal, Dict, Any

from fastapi import APIRouter, Depends, HTTPException, Query
from neo4j import Session
from pydantic import BaseModel, Field
from uuid import uuid4

from site_backend.core.neo_driver import session_dep
from site_backend.core.user_guard import current_user_id

# If you keep service wrappers, you can still import them.
# This router mostly uses Cypher directly to ensure atomicity.
from site_backend.api.services.neo_business import (
    create_offer as svc_create_offer,
    list_offers as svc_list_offers,
    patch_offer as svc_patch_offer,
    delete_offer as svc_delete_offer,
)

router = APIRouter(prefix="/eco-local", tags=["eco-local"])

# ============================================================
# Helpers
# ============================================================

_OWNS_EDGES = (":OWNS|:MANAGES",)  # used in Cypher string
VOUCHER_TTL_MIN = 15               # one-time QR lifetime

def now_ms() -> int:
    return int(datetime.now(tz=timezone.utc).timestamp() * 1000)

def now_iso() -> str:
    return datetime.now(tz=timezone.utc).isoformat()

def short_code() -> str:
    # short voucher-ish code (you can swap to ULID/nanoid)
    return uuid4().hex[:10].upper()

def _get_user_business_ids(s: Session, user_id: str) -> List[str]:
    recs = s.run(
        f"""
        MATCH (u:User {{id:$uid}})-[{_OWNS_EDGES[0]}]->(b:BusinessProfile)
        RETURN b.id AS id
        ORDER BY id
        """,
        uid=user_id,
    )
    return [r["id"] for r in recs]

def _resolve_user_business_id(
    s: Session,
    user_id: str,
    requested_business_id: Optional[str],
) -> str:
    if requested_business_id:
        rec = s.run(
            f"""
            MATCH (u:User {{id:$uid}})-[{_OWNS_EDGES[0]}]->(b:BusinessProfile {{id:$bid}})
            RETURN b.id AS id
            LIMIT 1
            """,
            uid=user_id,
            bid=requested_business_id,
        ).single()
        if not rec:
            raise HTTPException(status_code=403, detail="You don't have access to that business")
        return requested_business_id

    ids = _get_user_business_ids(s, user_id)
    if len(ids) == 0:
        raise HTTPException(status_code=404, detail="You don't have a business yet or you arent a business!")
    if len(ids) == 1:
        return ids[0]
    raise HTTPException(
        status_code=400,
        detail={"message": "Multiple businesses found; specify ?business_id=...", "your_business_ids": ids},
    )

def _assert_offer_belongs_to_user(s: Session, user_id: str, offer_id: str) -> str:
    rec = s.run(
        f"""
        MATCH (u:User {{id:$uid}})-[{_OWNS_EDGES[0]}]->(b:BusinessProfile)<-[:OF]-(o:Offer {{id:$oid}})
        RETURN b.id AS bid
        LIMIT 1
        """,
        uid=user_id,
        oid=offer_id,
    ).single()
    if not rec:
        raise HTTPException(status_code=403, detail="Offer not found or not yours")
    return rec["bid"]

def _assert_voucher_belongs_to_user_business(s: Session, user_id: str, voucher_code: str) -> Dict[str, Any]:
    """
    Ensure a voucher belongs to a business owned/managed by the user.
    Returns {code, status, expiresAt, offer_id, offer_title, eco_price, business_id}
    """
    rec = s.run(
        f"""
        MATCH (u:User {{id:$uid}})-[{_OWNS_EDGES[0]}]->(b:BusinessProfile)
        MATCH (v:Voucher {{code:$code}})-[:FOR_OFFER]->(o:Offer)-[:OF]->(b)
        RETURN v.code AS code,
               v.status AS status,
               toInteger(v.expiresAt) AS expiresAt,
               o.id AS offer_id,
               o.title AS offer_title,
               toInteger(o.eco_price) AS eco_price,
               b.id AS business_id
        LIMIT 1
        """,
        uid=user_id,
        code=voucher_code,
    ).single()
    if not rec:
        raise HTTPException(status_code=404, detail="Voucher not found or not for your business")
    return {
        "code": rec["code"],
        "status": rec["status"],
        "expiresAt": int(rec["expiresAt"] or 0),
        "offer_id": rec["offer_id"],
        "offer_title": rec["offer_title"],
        "eco_price": int(rec["eco_price"] or 0),
        "business_id": rec["business_id"],
    }

# -------- balance helper (PARITY with wallet/counter) --------
def _user_wallet_balance(s: Session, user_id: str) -> int:
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

# ============================================================
# Models (updated to ECO retire + vouchers)
# ============================================================

OfferStatus = Literal["active", "paused", "hidden"]

class OfferIn(BaseModel):
    title: str = Field(..., min_length=2, max_length=120)
    blurb: str = Field(..., min_length=2, max_length=280)
    status: OfferStatus = "active"
    eco_price: int = Field(..., ge=1)            # Required ECO to retire
    fiat_cost_cents: int = Field(0, ge=0)        # 0 for in-kind
    stock: Optional[int] = Field(None, ge=0)     # None = unlimited
    url: Optional[str] = None
    valid_until: Optional[date] = None
    tags: List[str] = Field(default_factory=list)


class OfferOut(BaseModel):
    id: str
    business_id: Optional[str] = None
    title: str
    blurb: Optional[str] = None
    status: Optional[str] = "active"
    eco_price: Optional[int] = None
    fiat_cost_cents: Optional[int] = None
    stock: Optional[int] = None
    url: Optional[str] = None
    valid_until: Optional[str] = None
    tags: Optional[List[str]] = None
    createdAt: Optional[int] = None

def _normalize_offer(o: Dict[str, Any]) -> Dict[str, Any]:
    # prefer eco_price but fallback to redeem_eco
    eco_price = o.get("eco_price")
    if eco_price is None and o.get("redeem_eco") is not None:
        eco_price = int(o["redeem_eco"])
    return {
        "id": o.get("id"),
        "business_id": o.get("business_id"),
        "title": o.get("title"),
        "blurb": o.get("blurb"),
        "status": o.get("status") or "active",
        "eco_price": eco_price,
        "fiat_cost_cents": o.get("fiat_cost_cents"),
        "stock": o.get("stock"),
        "url": o.get("url"),
        "valid_until": o.get("valid_until"),
        "tags": o.get("tags"),
        "createdAt": o.get("createdAt"),
    }


class OfferPatch(BaseModel):
    title: Optional[str] = Field(None, min_length=2, max_length=120)
    blurb: Optional[str] = Field(None, min_length=2, max_length=280)
    status: Optional[OfferStatus] = None
    eco_price: Optional[int] = Field(None, ge=1)
    fiat_cost_cents: Optional[int] = Field(None, ge=0)
    stock: Optional[int] = Field(None, ge=0)
    url: Optional[str] = None
    valid_until: Optional[date] = None
    tags: Optional[List[str]] = None

class RedeemResponse(BaseModel):
    offer_id: str
    eco_retired: int
    balance_after: int
    voucher_code: str
    message: str  # "Retired N ECO • Offer: ... • Voucher: ..."

# Voucher I/O
class VoucherVerifyIn(BaseModel):
    voucher_code: str

class VoucherVerifyOut(BaseModel):
    ok: bool
    offer: Dict[str, Any]
    status: Literal["issued", "verified", "consumed", "expired", "void"]
    expires_in_sec: int

class VoucherConsumeIn(BaseModel):
    voucher_code: str

class VoucherConsumeOut(BaseModel):
    ok: bool
    voucher_id: str
    consumedAt: int

# Suggest price (rich POST body)
OfferType = Literal["discount", "perk", "info"]
PledgeTier = Literal["starter", "builder", "leader"]

class SuggestPriceIn(BaseModel):
    type: Optional[OfferType] = None
    fiat_cost_cents: Optional[int] = Field(None, ge=0)
    avg_basket_cents: Optional[int] = Field(None, ge=0)
    percent: Optional[int] = Field(None, ge=0, le=100)
    pledge: Optional[PledgeTier] = None

class SuggestPriceOut(BaseModel):
    suggested_eco_price: int
    rule: str  # description of how we computed it

# ============================================================
# Offers (owner-scoped)
# ============================================================

def _is_visible(o: Dict[str, Any]) -> bool:
    # visible means: active, eco_price>0, stock not exhausted, not expired
    if (o.get("status") or "active") != "active":
        return False
    eco_price = o.get("eco_price") or 0
    if int(eco_price) <= 0:
        return False
    stock = o.get("stock")
    if stock is not None and int(stock) <= 0:
        return False
    vu = o.get("valid_until")
    if vu:
        try:
            # accept YYYY-MM-DD or ISO date
            d = datetime.fromisoformat(vu).date() if "T" in vu else date.fromisoformat(vu)
            if d < date.today():
                return False
        except Exception:
            # if bad date string, treat as not visible
            return False
    return True

@router.get("/offers", response_model=List[OfferOut])
def list_offers_api(
    s: Session = Depends(session_dep),
    business_id: Optional[str] = Query(None, alias="business_id"),
    status: Optional[str] = Query(None, pattern="^(active|paused|hidden)$"),
    visible_only: bool = Query(False),
):
    """
    - Call the service with the required keyword-only `visible_only`.
    - Then do API-layer filtering for `status` and visibility rules if requested.
    """
    # ✅ FIX: pass visible_only to satisfy service signature
    raw = svc_list_offers(s, business_id=business_id, visible_only=visible_only)

    offers = [_normalize_offer(dict(o) if not isinstance(o, dict) else o) for o in (raw or [])]

    # API-side status filtering (service may or may not support it)
    if status:
        offers = [o for o in offers if (o.get("status") or "active") == status]

    # If the service returned more than "visible" despite visible_only, enforce again just in case
    if visible_only:
        offers = [o for o in offers if _is_visible(o)]

    return [OfferOut(**o) for o in offers]

@router.get("/offers/{offer_id}", response_model=OfferOut)
def get_offer_api(
    offer_id: str,
    s: Session = Depends(session_dep),
    user_id: str = Depends(current_user_id),
):
    bid = _assert_offer_belongs_to_user(s, user_id, offer_id)
    rec = s.run(
        """
        MATCH (o:Offer {id:$oid})-[:OF]->(b:BusinessProfile {id:$bid})
        RETURN o{.*, business_id:b.id} AS offer
        """,
        oid=offer_id, bid=bid
    ).single()
    if not rec:
        raise HTTPException(status_code=404, detail="Offer not found")
    o = dict(rec["offer"])
    o.setdefault("claims", 0)
    return OfferOut(**o)

@router.post("/offers", response_model=OfferOut, status_code=201)
def create_offer_api(
    payload: OfferIn,
    business_id: Optional[str] = Query(None, description="Optional if you own multiple"),
    s: Session = Depends(session_dep),
    user_id: str = Depends(current_user_id),
):
    bid = _resolve_user_business_id(s, user_id, business_id)
    if svc_create_offer:
        o = svc_create_offer(
            s,
            business_id=bid,
            title=payload.title,
            blurb=payload.blurb,
            status=payload.status,
            eco_price=payload.eco_price,
            fiat_cost_cents=payload.fiat_cost_cents,
            stock=payload.stock,
            url=payload.url,
            valid_until=str(payload.valid_until) if payload.valid_until else None,
            tags=payload.tags,
        )
        return OfferOut(**o, business_id=bid)

    oid = str(uuid4())
    s.run(
        """
        MATCH (b:BusinessProfile {id:$bid})
        MERGE (o:Offer {id:$oid})
        SET o.title=$title,
            o.blurb=$blurb,
            o.status=$status,
            o.eco_price=$eco_price,
            o.fiat_cost_cents=$fiat_cost_cents,
            o.stock=$stock,
            o.url=$url,
            o.valid_until=$valid_until,
            o.tags=$tags,
            o.claims=coalesce(o.claims,0),
            o.created_at=$now,
            o.updated_at=$now
        MERGE (o)-[:OF]->(b)
        """,
        bid=bid,
        oid=oid,
        title=payload.title,
        blurb=payload.blurb,
        status=payload.status,
        eco_price=int(payload.eco_price),
        fiat_cost_cents=int(payload.fiat_cost_cents),
        stock=payload.stock,
        url=payload.url,
        valid_until=str(payload.valid_until) if payload.valid_until else None,
        tags=payload.tags,
        now=now_ms(),
    )
    # Note: `claims` not in OfferOut schema; extra is ignored but we avoid passing it here.
    return OfferOut(id=oid, business_id=bid, **payload.model_dump())

@router.patch("/offers/{offer_id}", response_model=OfferOut)
def patch_offer_api(
    offer_id: str,
    patch: OfferPatch,
    s: Session = Depends(session_dep),
    user_id: str = Depends(current_user_id),
):
    bid = _assert_offer_belongs_to_user(s, user_id, offer_id)
    fields = patch.model_dump(exclude_unset=True)

    if svc_patch_offer:
        if "valid_until" in fields and fields["valid_until"] is not None:
            fields["valid_until"] = str(fields["valid_until"])
        o = svc_patch_offer(s, offer_id=offer_id, fields=fields)
        return OfferOut(**o, business_id=bid)

    sets = []
    params: Dict[str, Any] = {"oid": offer_id, "now": now_ms(), "bid": bid}
    for k, v in fields.items():
        if k == "valid_until" and v is not None:
            v = str(v)
        params[k] = v
        sets.append(f"o.{k} = ${k}")
    set_clause = ", ".join(sets + ["o.updated_at = $now"]) if sets else "o.updated_at = $now"

    rec = s.run(
        f"""
        MATCH (o:Offer {{id:$oid}})-[:OF]->(b:BusinessProfile {{id:$bid}})
        SET {set_clause}
        RETURN o{{.*, business_id:b.id}} AS offer
        """,
        **params
    ).single()
    if not rec:
        raise HTTPException(status_code=404, detail="Offer not found")
    o = dict(rec["offer"])
    o.setdefault("claims", 0)
    return OfferOut(**o)

@router.delete("/offers/{offer_id}", response_model=dict)
def delete_offer_api(
    offer_id: str,
    s: Session = Depends(session_dep),
    user_id: str = Depends(current_user_id),
):
    _assert_offer_belongs_to_user(s, user_id, offer_id)
    if svc_delete_offer:
        svc_delete_offer(s, offer_id=offer_id)
    else:
        s.run(
            """
            MATCH (o:Offer {id:$oid})
            DETACH DELETE o
            """,
            oid=offer_id,
        )
    return {"ok": True}

# ============================================================
# Redemption → create Voucher + burn ECO (atomic)
# ============================================================

@router.post("/offers/{offer_id}/redeem", response_model=RedeemResponse)
def redeem_offer_api(
    offer_id: str,
    s: Session = Depends(session_dep),
    user_id: str = Depends(current_user_id),
):
    """
    Steps (single unit of work):
      - Check user ECO balance >= eco_price  (wallet math)
      - Offer active & (stock > 0 if finite) & not expired
      - Create Voucher(code, status='issued', expiresAt=now+TTL)
      - Create BURN_REWARD linked to Offer, Business, User (settled)
      - Decrement stock if finite (>0)
      - If fiat_cost_cents > 0: decrement sponsor_balance_cents and write SPONSOR_PAYOUT paired to burn
      - Return voucher + balance_after (wallet math)
    """
    tx_id = str(uuid4())
    payout_id = str(uuid4())
    now = now_ms()
    vcode = short_code()
    expires_at = now + VOUCHER_TTL_MIN * 60 * 1000

    # Load offer + business meta + user's wallet balance (PARITY)
    rec = s.run(
        """
        // Load offer + business
        MATCH (o:Offer {id:$oid})-[:OF]->(b:BusinessProfile)
        WITH o,b

        // Wallet balance (posted, parity with counter)
        OPTIONAL MATCH (:User {id:$uid})-[:EARNED]->(m:EcoTx {kind:'MINT_ACTION', status:'settled'})
        WITH o,b, coalesce(sum(m.amount),0) AS earned
        OPTIONAL MATCH (:User {id:$uid})-[:SPENT]->(sx:EcoTx {status:'settled'})
        WHERE sx.kind IN ['BURN_REWARD','CONTRIBUTE']
        WITH o,b, toInteger(earned - coalesce(sum(sx.amount),0)) AS user_balance

        RETURN
          o.id AS oid,
          o.title AS title,
          o.status AS status,
          toInteger(o.eco_price) AS eco_price,
          toInteger(coalesce(o.fiat_cost_cents,0)) AS fiat_cost_cents,
          toInteger(coalesce(o.stock,-1)) AS stock,
          o.valid_until AS valid_until,
          b.id AS bid,
          toInteger(coalesce(b.sponsor_balance_cents,0)) AS sponsor_balance_cents,
          user_balance AS user_balance
        """,
        oid=offer_id, uid=user_id
    ).single()

    if not rec:
        raise HTTPException(status_code=404, detail="Offer not found")

    if rec["status"] != "active":
        raise HTTPException(status_code=409, detail="Offer unavailable")

    # Expiry check for offer (optional)
    if rec["valid_until"]:
        try:
            vu = datetime.fromisoformat(str(rec["valid_until"]))
            if vu.tzinfo is None:
                vu = vu.replace(tzinfo=timezone.utc)
            if datetime.now(timezone.utc) > vu:
                raise HTTPException(status_code=409, detail="Offer expired")
        except Exception:
            pass

    eco_price = int(rec["eco_price"])
    fiat_cost = int(rec["fiat_cost_cents"])
    stock     = int(rec["stock"])
    bid       = rec["bid"]
    sponsor_balance = int(rec["sponsor_balance_cents"])
    user_balance    = int(rec["user_balance"] or 0)

    if user_balance < eco_price:
        raise HTTPException(status_code=400, detail="Insufficient ECO")

    # Stock check
    if stock == 0:
        raise HTTPException(status_code=409, detail="Out of stock")

    # Sponsor balance (cash-reimbursed only)
    if fiat_cost > 0 and sponsor_balance < fiat_cost:
        raise HTTPException(status_code=402, detail="Temporarily unavailable")

    # Atomic mutations
    s.run(
        """
        MATCH (o:Offer {id:$oid})-[:OF]->(b:BusinessProfile {id:$bid})
        MATCH (u:User {id:$uid})

        // Voucher
        MERGE (v:Voucher {code:$vcode})
          ON CREATE SET
            v.status      = 'issued',
            v.createdAt   = $now,
            v.expiresAt   = $expires,
            v.eco_spent   = $eco_price
        MERGE (v)-[:FOR_OFFER]->(o)
        MERGE (v)-[:FOR_BUSINESS]->(b)
        MERGE (u)-[:HAS_VOUCHER]->(v)

        // Burn tx (retire ECO)... include parity fields
        MERGE (t:EcoTx {id:$tx})
          ON CREATE SET
            t.kind        = 'BURN_REWARD',
            t.amount      = $eco_price,
            t.burn        = true,
            t.voucher     = $vcode,
            t.source      = 'offer',
            t.status      = 'settled',
            t.account_id  = $uid,
            t.createdAt   = $now,
            t.at          = datetime($now_iso)

        MERGE (u)-[:SPENT]->(t)
        MERGE (t)-[:FOR_OFFER]->(o)
        MERGE (b)-[:REDEEMED]->(t)

        // stock decrement if finite and positive
        WITH o,b,t,v
        CALL apoc.do.when(
          o.stock IS NOT NULL AND o.stock > 0,
          'SET o.stock = o.stock - 1 RETURN o',
          'RETURN o',
          {o:o}
        ) YIELD value

        // claims counter
        SET o.claims = coalesce(o.claims,0) + 1

        // sponsor payout (cash-reimbursed)
        WITH o,b,t,v
        CALL apoc.do.when(
          $fiat_cost > 0,
          '
          SET b.sponsor_balance_cents = coalesce(b.sponsor_balance_cents,0) - $fiat_cost
          MERGE (p:EcoTx {id:$payout})
            ON CREATE SET p.kind="SPONSOR_PAYOUT",
                          p.amount=$fiat_cost,
                          p.status="settled",
                          p.createdAt=$now
          MERGE (b)-[:PAID]->(p)
          MERGE (p)-[:PAIRS]->(t)
          RETURN b
          ',
          'RETURN b',
          {fiat_cost:$fiat_cost, payout:$payout, now:$now, t:t, b:b}
        ) YIELD value AS _
        RETURN 1 AS ok
        """,
        oid=offer_id,
        bid=bid,
        uid=user_id,
        tx=tx_id,
        payout=str(uuid4()),
        vcode=vcode,
        eco_price=eco_price,
        fiat_cost=fiat_cost,
        now=now,
        now_iso=now_iso(),
        expires=expires_at,
    )

    # Return fresh balance-after (PARITY helper)
    balance_after = _user_wallet_balance(s, user_id)

    return RedeemResponse(
        offer_id=offer_id,
        eco_retired=eco_price,
        balance_after=balance_after,
        voucher_code=vcode,
        message=f"Retired {eco_price} ECO • Offer: {rec['title']} • Voucher: {vcode}",
    )

# ============================================================
# Vouchers (owner: verify & consume)
# ============================================================

@router.post("/owner/vouchers/verify", response_model=VoucherVerifyOut)
def verify_voucher_api(
    body: VoucherVerifyIn,
    s: Session = Depends(session_dep),
    user_id: str = Depends(current_user_id),
):
    meta = _assert_voucher_belongs_to_user_business(s, user_id, body.voucher_code)
    now = now_ms()
    expires_in = max(0, (meta["expiresAt"] - now) // 1000)

    # Expired?
    if meta["expiresAt"] and now > meta["expiresAt"] and meta["status"] in ("issued", "verified"):
        s.run(
            """
            MATCH (v:Voucher {code:$code})
            SET v.status='expired', v.expiredAt=$now
            """,
            code=body.voucher_code, now=now
        )
        return VoucherVerifyOut(
            ok=False,
            offer={"id": meta["offer_id"], "title": meta["offer_title"], "eco_price": meta["eco_price"]},
            status="expired",
            expires_in_sec=0,
        )

    # Idempotent move to verified
    s.run(
        """
        MATCH (v:Voucher {code:$code})
        WITH v
        CALL apoc.do.when(
          v.status = 'issued',
          'SET v.status="verified", v.verifiedAt=$now RETURN v',
          'RETURN v',
          {now:$now}
        ) YIELD value
        RETURN 1 AS ok
        """,
        code=body.voucher_code, now=now
    )

    return VoucherVerifyOut(
        ok=True,
        offer={"id": meta["offer_id"], "title": meta["offer_title"], "eco_price": meta["eco_price"]},
        status="verified" if meta["status"] in ("issued", "verified") else meta["status"],
        expires_in_sec=expires_in,
    )

@router.post("/owner/vouchers/consume", response_model=VoucherConsumeOut)
def consume_voucher_api(
    body: VoucherConsumeIn,
    s: Session = Depends(session_dep),
    user_id: str = Depends(current_user_id),
):
    meta = _assert_voucher_belongs_to_user_business(s, user_id, body.voucher_code)
    now = now_ms()

    # Expiry gate
    if meta["expiresAt"] and now > meta["expiresAt"]:
        s.run(
            """
            MATCH (v:Voucher {code:$code})
            WHERE v.status IN ['issued','verified']
            SET v.status='expired', v.expiredAt=$now
            """,
            code=body.voucher_code, now=now
        )
        raise HTTPException(status_code=410, detail="Voucher expired")

    # Idempotent verify then consume
    rec = s.run(
        """
        MATCH (v:Voucher {code:$code})
        WITH v
        // auto-verify if still issued
        CALL apoc.do.when(
          v.status = 'issued',
          'SET v.status="verified", v.verifiedAt=$now RETURN v',
          'RETURN v',
          {now:$now}
        ) YIELD value
        WITH v
        CALL apoc.do.when(
          v.status <> 'consumed' AND v.status <> 'void',
          'SET v.status="consumed", v.consumedAt=$now RETURN v',
          'RETURN v',
          {now:$now}
        ) YIELD value AS vv
        RETURN vv AS v
        """,
        code=body.voucher_code, now=now
    ).single()

    if not rec:
        raise HTTPException(status_code=404, detail="Voucher not found")

    return VoucherConsumeOut(ok=True, voucher_id=meta["code"], consumedAt=now)

# ============================================================
# Suggest ECO price (rich POST body)
# ============================================================

@router.post("/offers/suggest_price", response_model=SuggestPriceOut)
def suggest_offer_price_api(
    body: SuggestPriceIn,
    s: Session = Depends(session_dep),
    user_id: str = Depends(current_user_id),  # auth required even though no scope
):
    """
    Deterministic rule:
      eco_price = clamp( round( (fiat_value_aud * k_by_pledge * k_by_type) ), MIN, MAX )
    With fallbacks when fiat is missing (percent/avg basket for discounts; fixed perk estimate).
    """
    # --- Derive fiat value ---
    fiat_cents = int(body.fiat_cost_cents or 0)

    if fiat_cents == 0 and (body.type == "discount") and (body.percent is not None):
        avg_basket = int(body.avg_basket_cents or 2000)  # default $20
        fiat_cents = max(0, round(avg_basket * (body.percent / 100)))

    if fiat_cents == 0 and (body.type == "perk"):
        # simple default perk value if not provided
        fiat_cents = 500  # $5 default

    # --- Multipliers ---
    k_by_pledge = {"starter": 4.0, "builder": 3.0, "leader": 2.5}
    k_by_type   = {"discount": 1.0, "perk": 0.7, "info": 0.0}
    MIN_ECO, MAX_ECO = 5, 250

    pledge_k = k_by_pledge.get(body.pledge or "starter", 4.0)
    type_k   = k_by_type.get(body.type or "perk", 0.7)

    # info offers cost 0 ECO
    if (body.type == "info"):
        return SuggestPriceOut(suggested_eco_price=0, rule="info_type_zero_eco")

    eco = round((fiat_cents / 100.0) * pledge_k * type_k)
    eco = max(MIN_ECO, min(MAX_ECO, eco))
    rule = f"pledge_{body.pledge or 'starter'}*type_{body.type or 'perk'}_clamped_{MIN_ECO}_{MAX_ECO}"

    return SuggestPriceOut(suggested_eco_price=int(eco), rule=rule)

# ============================================================
# Business Metrics (retirements, redemptions, sponsor)
# ============================================================

class BusinessMetricsOut(BaseModel):
    business_id: str
    name: Optional[str] = None
    sponsor_balance_cents: int = 0
    eco_retired_total: int = 0
    eco_retired_30d: int = 0
    redemptions_30d: int = 0
    unique_claimants_30d: int = 0
    minted_eco_30d: int = 0

@router.get("/business/metrics", response_model=BusinessMetricsOut)
def get_business_metrics_api(
    business_id: Optional[str] = Query(None, description="Optional if you own multiple"),
    s: Session = Depends(session_dep),
    user_id: str = Depends(current_user_id),
):
    bid = _resolve_user_business_id(s, user_id, business_id)

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

        // Optional: minted at/for this business in last 30d (if such relation exists)
        OPTIONAL MATCH (b)<-[:AT|:FOR]-(m:EcoTx {kind:'MINT_ACTION', status:'settled'})
        WHERE m.createdAt >= $since
        RETURN
          b.id AS bid,
          b.name AS name,
          toInteger(coalesce(b.sponsor_balance_cents,0)) AS sponsor_balance_cents,
          toInteger(eco_retired_total) AS eco_retired_total,
          toInteger(eco_retired_30d) AS eco_retired_30d,
          toInteger(redemptions_30d) AS redemptions_30d,
          toInteger(unique_claimants_30d) AS unique_claimants_30d,
          toInteger(coalesce(sum(m.amount),0)) AS minted_eco_30d
        """,
        bid=bid,
        since=since_ms,
    ).single()

    if not rec:
        raise HTTPException(status_code=404, detail="Business not found")

    return BusinessMetricsOut(
        business_id=bid,
        name=rec.get("name"),
        sponsor_balance_cents=int(rec.get("sponsor_balance_cents") or 0),
        eco_retired_total=int(rec.get("eco_retired_total") or 0),
        eco_retired_30d=int(rec.get("eco_retired_30d") or 0),
        redemptions_30d=int(rec.get("redemptions_30d") or 0),
        unique_claimants_30d=int(rec.get("unique_claimants_30d") or 0),
        minted_eco_30d=int(rec.get("minted_eco_30d") or 0),
    )

# ============================================================
# Public: Places (businesses) with offers for map/listing
# ============================================================

class PlaceItemOut(BaseModel):
    id: str                # duplicate of business_id (for your FE shape)
    business_id: str
    name: str
    lat: float
    lng: float
    pledge_tier: Literal["starter", "builder", "leader"] | None = None
    industry_group: Optional[str] = None
    area_type: Optional[str] = None
    has_offers: bool
    distance_km: Optional[float] = None

class PlacesResponseOut(BaseModel):
    items: List[PlaceItemOut]
    total: int
    page: int
    page_size: int

@router.get("/places/offers", response_model=PlacesResponseOut)
def public_places_with_offers(
    # bounding box (optional)
    min_lat: float | None = Query(None, ge=-90, le=90),
    min_lng: float | None = Query(None, ge=-180, le=180),
    max_lat: float | None = Query(None, ge=-90, le=90),
    max_lng: float | None = Query(None, ge=-180, le=180),

    # free text & filters
    q: str | None = Query(None, description="Case-insensitive search on name or tags"),
    pledges: List[Literal["starter","builder","leader"]] = Query(default_factory=list),
    industries: List[str] = Query(default_factory=list),
    areas: List[str] = Query(default_factory=list),

    # sorting & user location
    sort: Literal["distance","name","tier"] = Query("name"),
    lat: float | None = Query(None, ge=-90, le=90, description="Required for sort=distance"),
    lng: float | None = Query(None, ge=-180, le=180),

    # paging
    page: int = Query(1, ge=1),
    page_size: int = Query(100, ge=1, le=500),

    s: Session = Depends(session_dep),
):
    """
    Public, unauthenticated listing of businesses that are visible on the map,
    with an 'has_offers' flag (true if the business has at least one active offer).
    Supports bbox filtering, simple text search, filters, sorting, and pagination.
    """
    use_distance = sort == "distance" and lat is not None and lng is not None
    if sort == "distance" and not use_distance:
        sort = "name"

    where_clauses = [
        "b.visible_on_map = true",
        "b.lat IS NOT NULL",
        "b.lng IS NOT NULL",
    ]
    params: Dict[str, Any] = {}

    if all(v is not None for v in (min_lat, min_lng, max_lat, max_lng)):
        where_clauses.append("b.lat >= $min_lat AND b.lat <= $max_lat AND b.lng >= $min_lng AND b.lng <= $max_lng")
        params.update(dict(min_lat=min_lat, min_lng=min_lng, max_lat=max_lat, max_lng=max_lng))

    if q:
        where_clauses.append("(toLower(b.name) CONTAINS $q OR any(t IN coalesce(b.tags,[]) WHERE toLower(t) CONTAINS $q))")
        params["q"] = q.lower()

    if pledges:
        where_clauses.append("b.pledge_tier IN $pledges")
        params["pledges"] = pledges

    if industries:
        where_clauses.append("b.industry_group IN $industries")
        params["industries"] = industries

    if areas:
        where_clauses.append("b.area IN $areas")
        params["areas"] = areas

    where_sql = "WHERE " + " AND ".join(where_clauses) if where_clauses else ""

    distance_expr = "distance(point({latitude:b.lat, longitude:b.lng}), point({latitude:$lat, longitude:$lng}))" if use_distance else "null"
    if use_distance:
        params.update(dict(lat=lat, lng=lng))

    if sort == "distance":
        order_by = "ORDER BY dist ASC, b.name ASC"
    elif sort == "tier":
        order_by = "ORDER BY (CASE b.pledge_tier WHEN 'leader' THEN 3 WHEN 'builder' THEN 2 WHEN 'starter' THEN 1 ELSE 0 END) DESC, b.name ASC"
    else:
        order_by = "ORDER BY b.name ASC"

    skip = (page - 1) * page_size
    limit = page_size

    total_rec = s.run(
        f"""
        MATCH (b:BusinessProfile)
        {where_sql}
        RETURN count(b) AS total
        """,
        **params
    ).single()
    total = int(total_rec["total"] if total_rec else 0)

    if total == 0:
        return PlacesResponseOut(items=[], total=0, page=page, page_size=page_size)

    recs = s.run(
        f"""
        MATCH (b:BusinessProfile)
        {where_sql}

        OPTIONAL MATCH (b)<-[:OF]-(o:Offer)
        WITH b, any(x IN collect(o) WHERE x.status = 'active') AS has_active_offer

        WITH b, has_active_offer,
             {distance_expr} AS dist

        {order_by}
        SKIP $skip LIMIT $limit

        RETURN
          b.id AS business_id,
          b.name AS name,
          toFloat(b.lat) AS lat,
          toFloat(b.lng) AS lng,
          b.pledge_tier AS pledge_tier,
          b.industry_group AS industry_group,
          b.area AS area_type,
          has_active_offer AS has_offers,
          (CASE WHEN dist IS NULL THEN NULL ELSE toFloat(dist) / 1000.0 END) AS distance_km
        """,
        **params, skip=skip, limit=limit
    )

    items: List[PlaceItemOut] = []
    for r in recs:
        items.append(
            PlaceItemOut(
                id=r["business_id"],
                business_id=r["business_id"],
                name=r["name"],
                lat=float(r["lat"]),
                lng=float(r["lng"]),
                pledge_tier=r.get("pledge_tier"),
                industry_group=r.get("industry_group"),
                area_type=r.get("area_type"),
                has_offers=bool(r.get("has_offers")),
                distance_km=(float(r["distance_km"]) if r.get("distance_km") is not None else None),
            )
        )

    return PlacesResponseOut(items=items, total=total, page=page, page_size=page_size)
