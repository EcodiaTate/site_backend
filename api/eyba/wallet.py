# api/eyba/wallet.py
from __future__ import annotations

import hashlib
from datetime import datetime, timezone
from typing import Optional, List, Literal, Dict, Any

from fastapi import APIRouter, Depends, Query, Request
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

async def _resolve_user_id(req: Request) -> str:
    """
    Prefer authenticated user id; fallback to device-hash guest id.
    """
    try:
        uid = await _current_user_id(req)
        if isinstance(uid, str) and uid.strip():
            return uid
    except Exception:
        pass
    return _guest_user_id(req)

# ---------- models ----------
class WalletTx(BaseModel):
    id: str
    kind: Literal["MINT_ACTION", "BURN_REWARD", "SPONSOR_DEPOSIT", "SPONSOR_PAYOUT"]
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

# ---------- internal queries ----------
def _youth_totals(s: Session, uid: str) -> tuple[int, int]:
    """
    Earned = sum(MINT_ACTION), Spent = sum(BURN_REWARD), settled only.
    """
    rec = s.run(
        """
        MATCH (u:User {id:$uid})
        OPTIONAL MATCH (u)-[:EARNED]->(te:EcoTx {kind:'MINT_ACTION', status:'settled'})
        WITH u, coalesce(sum(toInteger(te.amount)),0) AS earned
        OPTIONAL MATCH (u)-[:SPENT]->(ts:EcoTx {kind:'BURN_REWARD', status:'settled'})
        WITH u, earned, coalesce(sum(toInteger(ts.amount)),0) AS spent
        RETURN toInteger(earned) AS earned, toInteger(spent) AS spent
        """,
        uid=uid,
    ).single()
    earned = int(rec["earned"] or 0)
    spent  = int(rec["spent"] or 0)
    return earned, spent

def _youth_txs(s: Session, uid: str, limit: int, before_ms: Optional[int]) -> List[WalletTx]:
    """
    Unified list:
      - Earned: (u)-[:EARNED]->(t:EcoTx {kind:'MINT_ACTION'})
        business via (t)-[:AT]->(b)
      - Spent:  (u)-[:SPENT]->(t:EcoTx {kind:'BURN_REWARD'})
        business via (t)-[:FOR_OFFER]->(o)-[:OF]->(b)
    """
    recs = s.run(
        """
        MATCH (u:User {id:$uid})-[rel:EARNED|SPENT]->(t:EcoTx)
        WHERE coalesce(t.status,'settled')='settled'
          AND t.kind IN ['MINT_ACTION','BURN_REWARD','SPONSOR_DEPOSIT','SPONSOR_PAYOUT']
          AND ($before IS NULL OR toInteger(t.createdAt) < toInteger($before))

        // business for mints
        OPTIONAL MATCH (t)-[:AT]->(b1:BusinessProfile)

        // business for burns via offer
        OPTIONAL MATCH (t)-[:FOR_OFFER]->(o:Offer)-[:OF]->(b2:BusinessProfile)

        WITH t, rel,
             CASE WHEN b1 IS NOT NULL THEN b1 ELSE b2 END AS b,
             o

        RETURN
          t.id AS id,
          t.kind AS kind,
          CASE type(rel) WHEN 'EARNED' THEN 'earned' ELSE 'spent' END AS direction,
          toInteger(t.amount) AS amount,
          toInteger(t.createdAt) AS createdAt,
          t.source AS source,
          CASE WHEN b IS NULL THEN NULL ELSE {id: b.id, name: b.name} END AS business,
          CASE WHEN o IS NULL THEN NULL ELSE o.id END AS offer_id
        ORDER BY createdAt DESC
        LIMIT $limit
        """,
        uid=uid, before=before_ms, limit=limit,
    )
    return [WalletTx(**r.data()) for r in recs]

# ---------- endpoint ----------
@router.get("/wallet", response_model=WalletOut)
async def get_wallet(
    req: Request,
    s: Session = Depends(session_dep),
    limit: int = Query(25, ge=1, le=100),
    before_ms: Optional[int] = Query(None),
):
    uid = await _resolve_user_id(req)

    earned_total, spent_total = _youth_totals(s, uid)
    balance = earned_total - spent_total

    txs = _youth_txs(s, uid, limit=limit, before_ms=before_ms)
    next_before = txs[-1].createdAt if txs else None

    return WalletOut(
        balance=balance,
        earned_total=earned_total,
        spent_total=spent_total,
        txs=txs,
        next_before_ms=next_before,
    )
