from __future__ import annotations

import hashlib
from datetime import datetime, timezone, timedelta
from typing import Optional, List, Literal, Dict, Any, Tuple

from fastapi import APIRouter, Depends, Query, Request, status
from neo4j import Session
from pydantic import BaseModel, Field

from site_backend.core.neo_driver import session_dep
from site_backend.core.user_guard import current_user_id as _current_user_id

router = APIRouter(prefix="/eco-local", tags=["eco-local"])

# ---------- helpers ----------
def _now_ms() -> int:
    return int(datetime.now(timezone.utc).timestamp() * 1000)


def _device_hash(ip: str, ua: str) -> str:
    h = hashlib.sha256()
    h.update((ip or "-").encode())
    h.update((ua or "-").encode())
    return h.hexdigest()[:16]


def _guest_user_id(req: Request) -> str:
    ip = req.client.host if req.client else "0.0.0.0"
    ua = req.headers.get("user-agent", "")
    return f"y_{_device_hash(ip, ua)}"


async def _resolve_user_id(req: Request, force_uid: Optional[str] = None) -> Dict[str, str]:
    """
    Prefer explicit override header, else authenticated, else guest.
    Returns both the chosen uid and the candidates for debug visibility.
    """
    guest = _guest_user_id(req)
    authed = None
    try:
        v = await _current_user_id(req)
        if isinstance(v, str) and v.strip():
            authed = v.strip()
    except Exception:
        authed = None

    final = (force_uid.strip() if force_uid else None) or authed or guest
    return {"final": final, "authed": authed or "", "guest": guest}


# ---------- models ----------
class WalletTx(BaseModel):
    id: str
    kind: Literal["MINT_ACTION", "BURN_REWARD", "CONTRIBUTE", "SPONSOR_DEPOSIT", "SPONSOR_PAYOUT"]
    direction: Literal["earned", "spent"]
    amount: int                    # ECO amount (unchanged)
    createdAt: int
    source: Optional[str] = None
    business: Optional[dict] = None
    offer_id: Optional[str] = None
    xp: int = 0                    # XP tied to this tx (usually only for earned rows)


class WalletOut(BaseModel):
    balance: int
    earned_total: int
    spent_total: int
    xp_total: int = 0              # lifetime XP (earned)
    xp_30d: int = 0                # last 30 days XP
    txs: List[WalletTx] = Field(default_factory=list)
    next_before_ms: Optional[int] = None
    debug: Optional[Dict[str, Any]] = None


# ---------- youth wallet (EcoTx-only, Submissions = connectors only) ----------

def _youth_totals(s: Session, uid: str) -> Tuple[int, int, int, int]:
    """
    Returns: (eco_earned, eco_spent, xp_total, xp_30d)

    Source of truth: settled EcoTx only.

    - ECO earned = sum of EcoTx (EARNED) with:
        kind in [MINT_ACTION, CONTRIBUTE, SPONSOR_DEPOSIT, SPONSOR_PAYOUT]
        or source='sidequest' or reason='sidequest_reward'
    - ECO spent  = sum of EcoTx (SPENT) with:
        kind in [BURN_REWARD, CONTRIBUTE, SPONSOR_PAYOUT]
        or source='sidequest' or reason='sidequest_reward'
      (we still filter via rel type so only SPENT edges count here)
    - XP total   = sum(t.xp on earned txs only
    - XP 30d     = same as XP total but only last 30 days

    Submissions are *not* counted directly; they only exist as proof
    and connectors that may have minted those EcoTx.
    """
    thirty_days_ms = int((datetime.now(timezone.utc) - timedelta(days=30)).timestamp() * 1000)

    rec = s.run(
        """
        MATCH (u:User {id:$uid})
        OPTIONAL MATCH (u)-[rel:EARNED|SPENT]->(t:EcoTx {status:'settled'})
        WHERE
          coalesce(t.kind,'') IN ['MINT_ACTION','BURN_REWARD','CONTRIBUTE','SPONSOR_DEPOSIT','SPONSOR_PAYOUT']
          OR t.source = 'sidequest'
          OR t.reason = 'sidequest_reward'

        WITH
          rel,
          t,
          toInteger(coalesce(t.eco, t.amount)) AS eco_amt,
          toInteger(coalesce(t.xp, 0)) AS xp_amt,
          toInteger(coalesce(t.createdAt, timestamp(t.at), timestamp())) AS t_ms

        RETURN
          toInteger(
            sum( CASE WHEN type(rel) = 'EARNED' THEN eco_amt ELSE 0 END )
          ) AS eco_earned,
          toInteger(
            sum( CASE WHEN type(rel) = 'SPENT' THEN eco_amt ELSE 0 END )
          ) AS eco_spent,
          toInteger(
            sum( CASE WHEN type(rel) = 'EARNED' THEN xp_amt ELSE 0 END )
          ) AS xp_total,
          toInteger(
            sum(
              CASE
                WHEN type(rel) = 'EARNED' AND t_ms >= $cutoff_ms
                THEN xp_amt
                ELSE 0
              END
            )
          ) AS xp_30d
        """,
        {"uid": uid, "cutoff_ms": thirty_days_ms},
    ).single() or {}

    return (
        int(rec.get("eco_earned") or 0),
        int(rec.get("eco_spent") or 0),
        int(rec.get("xp_total") or 0),
        int(rec.get("xp_30d") or 0),
    )


def _youth_txs(s: Session, uid: str, limit: int, before_ms: Optional[int]) -> List[WalletTx]:
    """
    Return a reverse-chronological list of EcoTx-backed wallet rows.

    Only real EcoTx are returned. Approved Submissions are NOT turned into
    virtual transactions anymore; they just act as connectors (e.g. PROOF).
    """
    recs = s.run(
        """
        MATCH (u:User {id:$uid})-[rel:EARNED|SPENT]->(t:EcoTx)
        WHERE coalesce(t.status,'settled') = 'settled'
          AND (
                t.kind IN ['MINT_ACTION','BURN_REWARD','CONTRIBUTE','SPONSOR_DEPOSIT','SPONSOR_PAYOUT']
             OR t.source = 'sidequest'
             OR t.reason = 'sidequest_reward'
          )
        WITH
          rel,
          t,
          CASE type(rel)
            WHEN 'EARNED' THEN 'earned'
            ELSE 'spent'
          END AS direction,
          toInteger(coalesce(t.eco, t.amount)) AS amount,
          toInteger(coalesce(t.createdAt, timestamp(t.at), timestamp())) AS createdAt,
          toInteger(coalesce(t.xp,0)) AS xp
        WHERE $before IS NULL OR createdAt < toInteger($before)
        RETURN
          t.id AS id,
          t.kind AS kind,
          direction,
          amount,
          createdAt,
          t.source AS source,
          xp,
          NULL AS business,
          NULL AS offer_id
        ORDER BY createdAt DESC
        LIMIT $limit
        """,
        {"uid": uid, "before": before_ms, "limit": limit},
    )

    items: List[WalletTx] = []
    for r in recs:
        row = r.data()
        items.append(
            WalletTx(
                id=row["id"],
                kind=row["kind"],
                direction=row["direction"],
                amount=int(row["amount"] or 0),
                createdAt=int(row["createdAt"] or 0),
                source=row.get("source"),
                business=row.get("business"),
                offer_id=row.get("offer_id"),
                xp=int(row.get("xp") or 0),
            )
        )
    return items


@router.get("/wallet", response_model=WalletOut)
async def get_wallet(
    req: Request,
    s: Session = Depends(session_dep),
    limit: int = Query(25, ge=1, le=100),
    before_ms: Optional[int] = Query(None),
    uid: str = Depends(_current_user_id),
):
    eco_earned, eco_spent, xp_total, xp_30d = _youth_totals(s, uid)
    balance = eco_earned - eco_spent

    txs = _youth_txs(s, uid, limit=limit, before_ms=before_ms)
    next_before = txs[-1].createdAt if txs else None

    dbg = None
    if req.query_params.get("debug") == "0":
        authed = uid
        guest = _guest_user_id(req)
        dbg = {
            "resolve": {"final": uid, "authed": authed, "guest": guest},
            "totals": {
                "earned_total": eco_earned,
                "spent_total": eco_spent,
                "balance": balance,
                "xp_total": xp_total,
                "xp_30d": xp_30d,
            },
        }

    return WalletOut(
        balance=balance,
        earned_total=eco_earned,
        spent_total=eco_spent,
        xp_total=xp_total,
        xp_30d=xp_30d,
        txs=txs,
        next_before_ms=next_before,
        debug=dbg,
    )


# ---------- business wallet (mirrors youth) ----------

class BizWalletTx(WalletTx):
    pass  # same shape


class BizWalletOut(WalletOut):
    business_id: str


def _biz_id_for_user(s: Session, uid: str, requested: Optional[str]) -> str:
    """
    If a business_id is provided, verify the caller owns it.
    Otherwise, resolve the caller's business (single-owner model).
    """
    if requested:
        row = s.run(
            "MATCH (b:BusinessProfile {id:$bid}) RETURN b.user_id AS owner",
            {"bid": requested},
        ).single()
        if not row or (row["owner"] or "") != uid:
            from fastapi import HTTPException

            raise HTTPException(status_code=403, detail="Not your business")
        return requested

    row = s.run(
        "MATCH (b:BusinessProfile {user_id:$uid}) RETURN b.id AS bid LIMIT 1",
        {"uid": uid},
    ).single()
    if not row or not row["bid"]:
        from fastapi import HTTPException

        raise HTTPException(status_code=404, detail="No business found for user")
    return row["bid"]


def _biz_totals(s: Session, bid: str) -> tuple[int, int]:
    rec = s.run(
        """
        MATCH (b:BusinessProfile {id:$bid})

        // Money/ECO flowing *to* the business
        OPTIONAL MATCH (b)-[:COLLECTED|EARNED]->(te:EcoTx {status:'settled'})
        WHERE coalesce(te.kind,'') IN ['CONTRIBUTE','SPONSOR_DEPOSIT','MINT_ACTION']
              OR te.source IN ['contribution','sidequest']
        WITH b, coalesce(sum(toInteger(coalesce(te.eco, te.amount))),0) AS earned

        // Money/ECO flowing *from* the business
        OPTIONAL MATCH (b)-[:SPENT]->(ts:EcoTx {status:'settled'})
        WHERE coalesce(ts.kind,'') IN ['SPONSOR_PAYOUT','BURN_REWARD']
        RETURN toInteger(earned) AS earned,
               toInteger(coalesce(sum(toInteger(coalesce(ts.eco, ts.amount))),0)) AS spent
        """,
        {"bid": bid},
    ).single() or {}
    return int(rec.get("earned") or 0), int(rec.get("spent") or 0)


def _biz_txs(s: Session, bid: str, limit: int, before_ms: Optional[int]) -> list[BizWalletTx]:
    recs = s.run(
        """
        MATCH (b:BusinessProfile {id:$bid})-[rel:COLLECTED|EARNED|SPENT]->(t:EcoTx)
        WHERE coalesce(t.status,'settled')='settled'
          AND (
                t.kind IN ['MINT_ACTION','BURN_REWARD','CONTRIBUTE','SPONSOR_DEPOSIT','SPONSOR_PAYOUT']
                OR t.source IN ['contribution','sidequest']
              )
          AND ($before IS NULL OR toInteger(coalesce(t.createdAt, timestamp(t.at), timestamp())) < toInteger($before))

        OPTIONAL MATCH (t)-[:FOR_OFFER]->(o:Offer)
        WITH t, rel, o
        RETURN
          t.id AS id,
          t.kind AS kind,
          CASE type(rel)
            WHEN 'COLLECTED' THEN 'earned'
            WHEN 'EARNED'    THEN 'earned'
            ELSE 'spent'
          END AS direction,
          toInteger(coalesce(t.eco, t.amount)) AS amount,
          toInteger(coalesce(t.createdAt, timestamp(t.at), timestamp())) AS createdAt,
          t.source AS source,
          NULL AS business,                           // implicit (this is the biz wallet)
          CASE WHEN o IS NULL THEN NULL ELSE o.id END AS offer_id
        ORDER BY createdAt DESC
        LIMIT $limit
        """,
        {"bid": bid, "before": before_ms, "limit": limit},
    )
    return [BizWalletTx(**r.data()) for r in recs]


@router.get("/business/wallet", response_model=BizWalletOut, status_code=status.HTTP_200_OK)
async def get_business_wallet(
    req: Request,
    s: Session = Depends(session_dep),
    limit: int = Query(25, ge=1, le=100),
    before_ms: Optional[int] = Query(None),
    business_id: Optional[str] = Query(None),
    uid: str = Depends(_current_user_id),
):
    bid = _biz_id_for_user(s, uid, business_id)
    earned_total, spent_total = _biz_totals(s, bid)
    balance = earned_total - spent_total
    txs = _biz_txs(s, bid, limit=limit, before_ms=before_ms)
    next_before = txs[-1].createdAt if txs else None

    dbg = None
    if req.query_params.get("debug") == "1":
        dbg = {
            "resolve": {"owner_uid": uid, "business_id": bid},
            "totals": {"earned_total": earned_total, "spent_total": spent_total, "balance": balance},
        }

    return BizWalletOut(
        business_id=bid,
        balance=balance,
        earned_total=earned_total,
        spent_total=spent_total,
        txs=txs,
        next_before_ms=next_before,
        debug=dbg,
    )
