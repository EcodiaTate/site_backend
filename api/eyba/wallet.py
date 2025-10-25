# api/routers/eyba_wallet.py
from __future__ import annotations

import hashlib
from datetime import datetime, timezone
from typing import Optional, List, Literal

from fastapi import APIRouter, Depends, Query, Request
from neo4j import Session
from pydantic import BaseModel, Field

from site_backend.core.neo_driver import session_dep

router = APIRouter(prefix="/eyba", tags=["eyba"])

# ---------- helpers ----------
def _now_ms() -> int:
    return int(datetime.now(timezone.utc).timestamp() * 1000)

def _device_hash(ip: str, ua: str) -> str:
    import hashlib
    h = hashlib.sha256()
    h.update((ip or "-").encode())
    h.update((ua or "-").encode())
    return h.hexdigest()[:16]

def get_youth_id(req: Request) -> str:
    ip = req.client.host if req.client else "0.0.0.0"
    ua = req.headers.get("user-agent", "")
    return f"y_{_device_hash(ip, ua)}"

# ---------- models ----------
class WalletTx(BaseModel):
    id: str
    kind: Optional[str] = None
    direction: Literal["earned", "spent"]
    amount: int
    createdAt: int
    source: Optional[str] = None
    business: Optional[dict] = None  # {id,name} if available

class WalletOut(BaseModel):
    balance: int
    earned_total: int
    spent_total: int
    txs: List[WalletTx] = Field(default_factory=list)
    next_before_ms: Optional[int] = None  # for paging

# ---------- internal queries ----------
def _youth_totals(s: Session, uid: str) -> tuple[int, int]:
    rec = s.run(
        """
        MATCH (u:User {id:$uid})
        OPTIONAL MATCH (u)-[:EARNED]->(te:EcoTx)
          WHERE coalesce(te.status,'settled')='settled'
        WITH u, coalesce(sum(toInteger(te.amount)),0) AS earned
        OPTIONAL MATCH (u)-[:SPENT]->(ts:EcoTx)
          WHERE coalesce(ts.status,'settled')='settled'
        RETURN toInteger(earned) AS earned, toInteger(coalesce(sum(toInteger(ts.amount)),0)) AS spent
        """,
        uid=uid,
    ).single()
    earned = int(rec["earned"] or 0)
    spent  = int(rec["spent"] or 0)
    return earned, spent

def _youth_txs(s: Session, uid: str, limit: int, before_ms: Optional[int]) -> list[WalletTx]:
    recs = s.run(
        """
        MATCH (u:User {id:$uid})-[r:EARNED|SPENT]->(t:EcoTx)
        WHERE coalesce(t.status,'settled')='settled'
          AND ($before IS NULL OR toInteger(t.createdAt) < toInteger($before))
        OPTIONAL MATCH (b:BusinessProfile)-[:TRIGGERED]->(t)
        WITH t, r, b
        RETURN
          t.id AS id,
          t.kind AS kind,
          CASE type(r) WHEN 'EARNED' THEN 'earned' ELSE 'spent' END AS direction,
          toInteger(t.amount) AS amount,
          toInteger(t.createdAt) AS createdAt,
          t.source AS source,
          CASE WHEN b IS NULL THEN NULL ELSE {id: b.id, name: b.name} END AS business
        ORDER BY createdAt DESC
        LIMIT $limit
        """,
        uid=uid, before=before_ms, limit=limit,
    )
    return [WalletTx(**r.data()) for r in recs]

# ---------- endpoint ----------
@router.get("/wallet", response_model=WalletOut)
def get_wallet(
    req: Request,
    s: Session = Depends(session_dep),
    limit: int = Query(25, ge=1, le=100),
    before_ms: Optional[int] = Query(None),
):
    uid = get_youth_id(req)

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
