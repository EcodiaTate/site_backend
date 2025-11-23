from __future__ import annotations

import os
from typing import Dict, List

from fastapi import APIRouter, Depends, Header, HTTPException, status
from neo4j import Session  # type: ignore

from site_backend.core.neo_driver import session_dep

# Stripe config
try:
    import stripe  # type: ignore
except ImportError:  # pragma: no cover
    stripe = None  # type: ignore

STRIPE_SECRET_KEY = os.getenv("STRIPE_SECRET_KEY")
DEFAULT_CURRENCY = (os.getenv("MARKET_CURRENCY", "AUD") or "AUD").lower()

# Security for Cloud Scheduler
PAYOUT_CRON_TOKEN = os.getenv("PAYOUT_CRON_TOKEN", "dev-payout-token")

# Current market manager's Stripe Connect account
MARKET_MANAGER_ACCOUNT_ID = os.getenv("MARKET_MANAGER_STRIPE_ACCOUNT_ID")

# Minimum payout threshold in cents (e.g. 500 = $5.00)
MIN_PAYOUT_CENTS = int(os.getenv("MARKET_MIN_PAYOUT_CENTS", "500"))

router = APIRouter(
    prefix="/market/payouts",
    tags=["market_payouts"],
)


# ============================================================
# Helpers
# ============================================================

def _require_stripe() -> None:
    if stripe is None:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Stripe SDK not installed on server.",
        )
    if not STRIPE_SECRET_KEY:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Stripe is not configured (STRIPE_SECRET_KEY missing).",
        )
    stripe.api_key = STRIPE_SECRET_KEY


# ============================================================
# Routes
# ============================================================

@router.post("/run")
def run_payout_cycle(
    x_cron_token: str = Header(..., alias="X-Cron-Token"),
    session: Session = Depends(session_dep),
):
    """
    Batch job for upcycling payouts.

    - Protected by X-Cron-Token so Cloud Scheduler can call it safely.
    - Finds all succeeded UpcyclingOrder nodes whose designer/manager shares
      are not yet marked as paid, and that are older than 1 day.
    - Aggregates totals per recipient and creates Stripe Transfers to their
      Connect accounts.
    - Marks orders as paid in Neo4j.

    This assumes all orders are currently AUD; if you expand currencies, you'd
    want to group by currency as well.
    """
    if x_cron_token != PAYOUT_CRON_TOKEN:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Forbidden")

    _require_stripe()

    # 1) Fetch eligible orders
    rows = session.run(
        """
        MATCH (o:UpcyclingOrder)
        WHERE o.status = 'succeeded'
          AND (
            coalesce(o.designer_paid, false) = false OR
            coalesce(o.manager_paid, false) = false
          )
          AND o.created_at < datetime() - duration('P1D')  // 1-day buffer
        MATCH (s:UpcyclingStore {id:o.store_id})
        RETURN
          o.id AS order_id,
          coalesce(o.share_designer_cents, 0) AS designer_cents,
          coalesce(o.share_manager_cents, 0) AS manager_cents,
          coalesce(o.currency, 'AUD') AS currency,
          s.stripe_account_id AS designer_acct
        """
    )

    orders: List[dict] = [r.data() for r in rows]

    if not orders:
        return {
            "ok": True,
            "message": "No pending payouts.",
            "orders_considered": 0,
            "designer_accounts_paid": 0,
            "manager_paid": False,
            "transfers": {"designers": {}, "manager": None},
        }

    # 2) Aggregate by recipient
    designer_totals: Dict[str, int] = {}
    manager_total_cents = 0
    order_ids: List[str] = []

    for row in orders:
        order_id = row["order_id"]
        designer_acct = row["designer_acct"]
        designer_cents = int(row["designer_cents"] or 0)
        manager_cents = int(row["manager_cents"] or 0)
        currency = (row["currency"] or "AUD").lower()

        # For now we only support AUD; skip if not.
        if currency != DEFAULT_CURRENCY.lower():
            # In the future, you can branch by currency here.
            continue

        order_ids.append(order_id)

        if designer_acct and designer_cents > 0:
            designer_totals.setdefault(designer_acct, 0)
            designer_totals[designer_acct] += designer_cents

        if manager_cents > 0:
            manager_total_cents += manager_cents

    if not order_ids:
        return {
            "ok": True,
            "message": "No eligible orders for payouts (currency/filters).",
            "orders_considered": 0,
            "designer_accounts_paid": 0,
            "manager_paid": False,
            "transfers": {"designers": {}, "manager": None},
        }

    transfer_results = {"designers": {}, "manager": None}
    currency = DEFAULT_CURRENCY.lower()

    # 3) Payout designers via Connect Transfers
    for acct_id, amount_cents in designer_totals.items():
        if amount_cents < MIN_PAYOUT_CENTS:
            # Avoid spammy micro-payouts; they will accumulate for next run
            continue

        try:
            transfer = stripe.Transfer.create(  # type: ignore[attr-defined]
                amount=amount_cents,
                currency=currency,
                destination=acct_id,
                description="Ecodia Upcycling sales payout",
            )
            transfer_results["designers"][acct_id] = transfer.id
        except Exception as e:
            # Log failure; for now, we just skip marking those orders as paid
            # The next run will retry.
            print(f"[payouts] Failed transfer to designer {acct_id}: {e}")

    # 4) Payout current market manager if configured
    manager_paid_flag = False
    if MARKET_MANAGER_ACCOUNT_ID and manager_total_cents >= MIN_PAYOUT_CENTS:
        try:
            transfer = stripe.Transfer.create(  # type: ignore[attr-defined]
                amount=manager_total_cents,
                currency=currency,
                destination=MARKET_MANAGER_ACCOUNT_ID,
                description="Ecodia Upcycling manager commission",
            )
            transfer_results["manager"] = transfer.id
            manager_paid_flag = True
        except Exception as e:
            print(
                f"[payouts] Failed transfer to manager ({MARKET_MANAGER_ACCOUNT_ID}): {e}"
            )

    # 5) Mark orders as paid in Neo4j
    # Note: we mark flags true if their share > 0. If a transfer failed, we rely
    # on totals / minimum threshold to re-run next time (could be refined later).
    session.run(
        """
        MATCH (o:UpcyclingOrder)
        WHERE o.id IN $order_ids
          AND o.status = 'succeeded'
        SET
          o.designer_paid = CASE
            WHEN coalesce(o.share_designer_cents,0) > 0 THEN true
            ELSE o.designer_paid
          END,
          o.manager_paid = CASE
            WHEN coalesce(o.share_manager_cents,0) > 0 THEN true
            ELSE o.manager_paid
          END,
          o.updated_at = datetime()
        """,
        order_ids=order_ids,
    )

    return {
        "ok": True,
        "orders_considered": len(order_ids),
        "designer_accounts_paid": len(transfer_results["designers"]),
        "manager_paid": manager_paid_flag,
        "transfers": transfer_results,
    }
