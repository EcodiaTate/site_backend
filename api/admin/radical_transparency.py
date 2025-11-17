from __future__ import annotations

from datetime import datetime, timezone
from typing import List, Literal, Optional, Dict, Any, Set
import os
import logging

from fastapi import APIRouter, Depends, Query
from neo4j import Session
from pydantic import BaseModel, Field

from site_backend.core.neo_driver import session_dep

log = logging.getLogger(__name__)

router = APIRouter(prefix="/transparency", tags=["transparency"])


# ---------------------- admin email source of truth ----------------------


def _admin_emails_from_env() -> Set[str]:
    """
    Shared source of truth for 'who counts as admin' by email.

    - Prefer ADMIN_EMAILS (comma-separated list)
    - Fallback to legacy ADMIN_EMAIL (single email)
    """
    raw = os.getenv("ADMIN_EMAILS")
    if raw:
        parts = [p.strip().lower() for p in raw.split(",")]
        return {p for p in parts if p}
    legacy = os.getenv("ADMIN_EMAIL")
    if legacy:
        return {legacy.strip().lower()}
    return set()


# ---------------------- response models ----------------------


class AdminEcoLogItem(BaseModel):
    # tx identity + basic info
    id: str = Field(..., description="EcoTx ID")
    tx_kind: str = Field(..., description="EcoTx.kind (e.g. MINT_ACTION, BURN_REWARD)")
    direction: Literal["earned", "spent"] = Field(
        ..., description="Earned vs spent from the admin's perspective"
    )
    eco_delta: int = Field(
        ...,
        description="Signed ECO amount (earned > 0, spent < 0), matches wallet math",
    )
    eco_abs: int = Field(..., description="Absolute ECO amount for this tx")
    xp: int = Field(0, description="XP associated with this transaction (if any)")

    created_at: datetime = Field(..., description="When this tx happened (ISO datetime)")
    created_at_ms: int = Field(..., description="When this tx happened (ms since epoch)")

    source: Optional[str] = Field(
        None, description="t.source (e.g. 'sidequest', 'offer', 'contribution')"
    )
    reason: Optional[str] = Field(
        None, description="t.reason (e.g. 'sidequest_reward', etc.)"
    )

    # high-level human label
    title: Optional[str] = Field(
        None, description="Short human-facing label (e.g. sidequest title)"
    )
    description: Optional[str] = Field(
        None, description="Human-friendly explanation of what this entry represents"
    )

    # user (admin) info
    user_id: Optional[str] = None
    user_email: Optional[str] = None
    user_name: Optional[str] = None

    # Optional context for sidequests / offers
    sidequest_title: Optional[str] = None
    submission_id: Optional[str] = None
    voucher_code: Optional[str] = None

    # Ledger snapshot for that admin at that moment (within log window)
    balance_after: Optional[int] = Field(
        None,
        description=(
            "Admin's ECO wallet balance immediately after this tx, "
            "computed from all prior admin EcoTx in this log window."
        ),
    )


class AdminEcoLogSummary(BaseModel):
    eco_earned_total: int = Field(
        ..., description="Total ECO earned by all admins in this log window (sum of earned txs)"
    )
    eco_spent_total: int = Field(
        ..., description="Total ECO spent/retired by all admins in this log window (sum of spent txs)"
    )
    eco_net: int = Field(
        ..., description="Net ECO (earned - spent) for all admins in this log window"
    )


class AdminEcoLogResponse(BaseModel):
    admins: List[str]
    items: List[AdminEcoLogItem]
    summary: AdminEcoLogSummary


# ---------------------- main endpoint ----------------------


@router.get("/admin-eco-log", response_model=AdminEcoLogResponse)
def get_admin_eco_log(
    limit: int = Query(200, ge=1, le=1000),
    session: Session = Depends(session_dep),
):
    """
    Public, read-only log of *all* EcoTx for admin accounts, based solely on:

      (u:User)-[rel:EARNED|SPENT]->(t:EcoTx)

    That means:

    - Sidequest mints (MINT_ACTION, source:'sidequest', reason:'sidequest_reward')
    - Offer spends (BURN_REWARD, source:'offer', voucher:<code>)
    - Any other EcoTx types attached to those admin users.

    This is exactly the same primitive your wallets use. Vouchers themselves
    do not change balances; the BURN_REWARD EcoTx does, and that is what we log.
    """
    admin_emails = _admin_emails_from_env()
    if not admin_emails:
        log.warning("AdminEcoLog: no admin emails configured; returning empty log")
        return AdminEcoLogResponse(
            admins=[],
            items=[],
            summary=AdminEcoLogSummary(
                eco_earned_total=0,
                eco_spent_total=0,
                eco_net=0,
            ),
        )

    records = session.run(
        """
        MATCH (u:User)
        WHERE toLower(u.email) IN $admin_emails

        MATCH (u)-[rel:EARNED|SPENT]->(t:EcoTx)
        WHERE coalesce(t.status,'settled') = 'settled'
          AND (
                t.kind IN ['MINT_ACTION','BURN_REWARD','CONTRIBUTE','SPONSOR_DEPOSIT','SPONSOR_PAYOUT']
             OR t.source IN ['sidequest','offer','contribution']
             OR t.reason = 'sidequest_reward'
          )

        WITH u, rel, t,
             CASE type(rel) WHEN 'EARNED' THEN 'earned' ELSE 'spent' END AS direction,
             toInteger(coalesce(t.eco, t.amount, 0)) AS eco_amt,
             toInteger(coalesce(t.createdAt, timestamp(t.at), timestamp())) AS createdAtMs

        // optional sidequest context
        OPTIONAL MATCH (t)-[:PROOF]->(sub:Submission)
        OPTIONAL MATCH (sub)-[:FOR]->(sq:Sidequest)

        RETURN
          u.id               AS user_id,
          u.email            AS user_email,
          coalesce(u.display_name, u.email) AS user_name,

          t.id               AS tx_id,
          coalesce(t.kind,'') AS tx_kind,
          direction,
          eco_amt,
          createdAtMs,
          t.source           AS source,
          t.reason           AS reason,
          toInteger(coalesce(t.xp,0)) AS xp,

          sub.id             AS submission_id,
          sq.title           AS sidequest_title,
          t.voucher          AS voucher_code
        ORDER BY createdAtMs DESC
        LIMIT $limit
        """,
        {
            "admin_emails": [e.lower() for e in admin_emails],
            "limit": limit,
        },
    )

    # Pull all rows into memory so we can compute per-admin running balances.
    rows: List[Dict[str, Any]] = [rec.data() for rec in records]

    # Group rows by user_id and compute running balance (ascending time) for each admin.
    balances_after_by_tx: Dict[str, int] = {}
    eco_earned_total = 0
    eco_spent_total = 0

    by_user: Dict[str, List[Dict[str, Any]]] = {}
    for d in rows:
        uid = d.get("user_id") or "__unknown__"
        by_user.setdefault(uid, []).append(d)

    for uid, lst in by_user.items():
        # Sort ascending by time so we can walk forward and accumulate.
        lst.sort(key=lambda d: int(d.get("createdAtMs") or 0))
        running_balance = 0
        for d in lst:
            eco_abs = int(d.get("eco_amt") or 0)
            direction = d.get("direction") or "earned"
            delta = eco_abs if direction == "earned" else -eco_abs
            running_balance += delta

            tx_id = d.get("tx_id")
            if tx_id:
                balances_after_by_tx[tx_id] = running_balance

            if direction == "earned":
                eco_earned_total += eco_abs
            else:
                eco_spent_total += eco_abs

    eco_net = eco_earned_total - eco_spent_total

    items: List[AdminEcoLogItem] = []
    # NOTE: rows are still in DESC order (because the query ordered them that way).
    for data in rows:
        created_ms = int(data.get("createdAtMs") or 0)
        created_dt = datetime.fromtimestamp(
            created_ms / 1000.0, tz=timezone.utc
        )

        direction: Literal["earned", "spent"] = data["direction"]
        eco_abs = int(data.get("eco_amt") or 0)
        eco_delta = eco_abs if direction == "earned" else -eco_abs

        tx_kind = (data.get("tx_kind") or "").upper()
        source = data.get("source") or None
        reason = data.get("reason") or None
        sidequest_title = data.get("sidequest_title") or None
        voucher_code = data.get("voucher_code") or None

        # Human-facing label/description
        title: Optional[str] = None
        desc: Optional[str] = None

        if sidequest_title:
            title = sidequest_title
            desc = "Sidequest reward"
        elif source == "offer":
            title = "Offer claim"
            if voucher_code:
                desc = f"Voucher {voucher_code}"
            else:
                desc = "Offer redemption"
        elif tx_kind == "CONTRIBUTE":
            title = "Contribution"
            desc = "Admin contributed ECO"
        elif tx_kind in ("SPONSOR_DEPOSIT", "SPONSOR_PAYOUT"):
            title = "Sponsor flow"
            desc = tx_kind.replace("_", " ").title()
        else:
            # Fallback to source/reason
            if source and reason:
                title = source
                desc = reason
            elif source:
                title = source
            elif reason:
                title = reason

        tx_id = data["tx_id"]
        balance_after = balances_after_by_tx.get(tx_id)

        items.append(
            AdminEcoLogItem(
                id=tx_id,
                tx_kind=tx_kind,
                direction=direction,
                eco_delta=eco_delta,
                eco_abs=eco_abs,
                xp=int(data.get("xp") or 0),
                created_at=created_dt,
                created_at_ms=created_ms,
                source=source,
                reason=reason,
                title=title,
                description=desc,
                user_id=data.get("user_id"),
                user_email=data.get("user_email"),
                user_name=data.get("user_name"),
                sidequest_title=sidequest_title,
                submission_id=data.get("submission_id"),
                voucher_code=voucher_code,
                balance_after=balance_after,
            )
        )

    return AdminEcoLogResponse(
        admins=sorted(admin_emails),
        items=items,
        summary=AdminEcoLogSummary(
            eco_earned_total=eco_earned_total,
            eco_spent_total=eco_spent_total,
            eco_net=eco_net,
        ),
    )
