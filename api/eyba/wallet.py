from __future__ import annotations

import hashlib
from datetime import datetime, timezone
from typing import Optional, List, Literal, Dict, Any, Tuple

from fastapi import APIRouter, Depends, Query, Request, status
from neo4j import Session
from pydantic import BaseModel, Field

from site_backend.core.neo_driver import session_dep
from site_backend.core.user_guard import current_user_id as _current_user_id

router = APIRouter(prefix="/eyba", tags=["eyba"])

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
    # include CONTRIBUTE so the feed types match the new flow
    kind: Literal["MINT_ACTION", "BURN_REWARD", "CONTRIBUTE", "SPONSOR_DEPOSIT", "SPONSOR_PAYOUT"]
    direction: Literal["earned", "spent"]
    amount: int
    createdAt: int
    source: Optional[str] = None
    business: Optional[dict] = None  # {id,name} if available
    offer_id: Optional[str] = None   # when spent on a reward

class WalletOut(BaseModel):
    balance: int
    earned_total: int
    spent_total: int
    txs: List[WalletTx] = Field(default_factory=list)
    next_before_ms: Optional[int] = None  # for paging
    debug: Optional[Dict[str, Any]] = None  # returned when ?debug=1

# ---------- queries ----------
def _youth_totals(s: Session, uid: str) -> Tuple[int, int]:
    """
    Earned = settled EcoTx (MINT_ACTION or sidequest-sourced) + approved Submissions
             that don't yet have a PROOF-linked EcoTx (virtual earnings).
    Spent   = settled EcoTx with kind in (BURN_REWARD, CONTRIBUTE).
    """
    rec = s.run(
        """
        // ---------- Earned from real EcoTx ----------
        MATCH (u:User {id:$uid})
        OPTIONAL MATCH (u)-[:EARNED]->(te:EcoTx {status:'settled'})
        WHERE coalesce(te.kind,'') = 'MINT_ACTION'
           OR te.source = 'sidequest'
           OR te.reason = 'sidequest_reward'
        WITH u, coalesce(sum(toInteger(coalesce(te.eco, te.amount))),0) AS earned_tx

        // ---------- PLUS approved submissions w/o a PROOF-linked EcoTx (virtual) ----------
        OPTIONAL MATCH (u)-[:SUBMITTED]->(sub:Submission {state:'approved'})-[:FOR]->(sq:Sidequest)
        WHERE NOT (sub)<-[:PROOF]-(:EcoTx)
        WITH u, earned_tx, coalesce(sum(toInteger(coalesce(sq.reward_eco,0))),0) AS earned_subs

        // ---------- Spent ----------
        OPTIONAL MATCH (u)-[:SPENT]->(ts:EcoTx {status:'settled'})
        WHERE coalesce(ts.kind,'') IN ['BURN_REWARD','CONTRIBUTE']

        RETURN
          toInteger(earned_tx + earned_subs) AS earned,
          toInteger(coalesce(sum(toInteger(coalesce(ts.eco, ts.amount))),0)) AS spent
        """,
        {"uid": uid},
    ).single() or {}

    return int(rec.get("earned") or 0), int(rec.get("spent") or 0)

def _youth_txs(s: Session, uid: str, limit: int, before_ms: Optional[int]) -> List[WalletTx]:
    recs = s.run(
        """
        CALL {
          // ---------- A) Real EcoTx rows ----------
          WITH $uid AS uid, $before AS before
          MATCH (u:User {id:uid})-[rel:EARNED|SPENT]->(t:EcoTx)
          WHERE coalesce(t.status,'settled')='settled'
            AND (
                  t.kind IN ['MINT_ACTION','BURN_REWARD','CONTRIBUTE','SPONSOR_DEPOSIT','SPONSOR_PAYOUT']
               OR t.source='sidequest'
               OR t.reason='sidequest_reward'
            )
          WITH
            t.id AS id,
            t.kind AS kind,
            CASE type(rel) WHEN 'EARNED' THEN 'earned' ELSE 'spent' END AS direction,
            toInteger(coalesce(t.eco, t.amount)) AS amount,
            toInteger(coalesce(t.createdAt, timestamp(t.at), timestamp())) AS createdAt,
            t.source AS source,
            null AS business,
            null AS offer_id
          WHERE before IS NULL OR createdAt < toInteger(before)
          RETURN id, kind, direction, amount, createdAt, source, business, offer_id

          UNION ALL

          // ---------- B) Virtual rows from approved Submissions lacking a tx ----------
          WITH $uid AS uid, $before AS before
          MATCH (u:User {id:uid})-[:SUBMITTED]->(sub:Submission {state:'approved'})-[:FOR]->(sq:Sidequest)
          WHERE NOT (sub)<-[:PROOF]-(:EcoTx)
          WITH
            'vtx:' + sub.id AS id,
            'MINT_ACTION' AS kind,
            'earned' AS direction,
            toInteger(coalesce(sq.reward_eco,0)) AS amount,
            toInteger(timestamp(coalesce(sub.reviewed_at, sub.created_at, datetime()))) AS createdAt,
            'sidequest' AS source,
            null AS business,
            null AS offer_id
          WHERE before IS NULL OR createdAt < toInteger(before)
          RETURN id, kind, direction, amount, createdAt, source, business, offer_id
        }
        RETURN id, kind, direction, amount, createdAt, source, business, offer_id
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
            )
        )
    return items


# ---------- youth wallet route ----------
@router.get("/wallet", response_model=WalletOut)
async def get_wallet(
    req: Request,
    s: Session = Depends(session_dep),
    limit: int = Query(25, ge=1, le=100),
    before_ms: Optional[int] = Query(None),
    uid: str = Depends(_current_user_id),
):
    earned_total, spent_total = _youth_totals(s, uid)
    balance = earned_total - spent_total
    txs = _youth_txs(s, uid, limit=limit, before_ms=before_ms)
    next_before = txs[-1].createdAt if txs else None

    # Intentionally returns debug when ?debug=0 (matches your existing client)
    dbg = None
    if req.query_params.get("debug") == "0":
        authed = uid
        guest = _guest_user_id(req)
        dbg = {
            "resolve": {"final": uid, "authed": authed, "guest": guest},
            "totals": {"earned_total": earned_total, "spent_total": spent_total, "balance": balance},
        }

    return WalletOut(
        balance=balance,
        earned_total=earned_total,
        spent_total=spent_total,
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
# --- replace _biz_totals and _biz_txs with these ---

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
