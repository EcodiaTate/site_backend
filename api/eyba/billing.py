# api/routers/eyba_billing.py
from __future__ import annotations

import os
from typing import Optional, Dict, Any, List

import stripe
from fastapi import APIRouter, HTTPException, Request, Depends, Query
from pydantic import BaseModel, Field
from neo4j import Session

from site_backend.core.neo_driver import session_dep
from site_backend.core.user_guard import current_user_id
from site_backend.api.eyba.neo_business import stripe_record_contribution  # webhook path uses UNSCOPED mint

router = APIRouter(prefix="/eyba", tags=["billing"])

stripe.api_key = os.environ.get("STRIPE_SECRET_KEY", "")
WEBHOOK_SECRET = os.environ.get("STRIPE_WEBHOOK_SECRET", "")
PUBLIC_BASE = os.environ.get("PUBLIC_BASE_URL", "http://localhost:3000")

# --------------------------------------------------------------------
# Ownership helpers (user → business)
# --------------------------------------------------------------------

# Edit here if your edge names differ (e.g., :ADMIN_OF)
_OWNS_RELS = ":OWNS|MANAGES"


def _user_business_ids(s: Session, user_id: str) -> List[str]:
    recs = s.run(
        f"""
        MATCH (u:User {{id:$uid}})-[{_OWNS_RELS}]->(b:BusinessProfile)
        RETURN b.id AS id
        ORDER BY id
        """,
        uid=user_id,
    )
    return [r["id"] for r in recs]


def _resolve_user_business_id(
    s: Session, *, user_id: str, requested_business_id: Optional[str]
) -> str:
    """
    If requested_business_id is provided, verify ownership; else infer:
      - 0 owned -> 404
      - 1 owned -> choose it
      - many   -> 400 with list so caller can choose
    """
    if requested_business_id:
        ok = s.run(
            f"""
            MATCH (u:User {{id:$uid}})-[{_OWNS_RELS}]->(b:BusinessProfile {{id:$bid}})
            RETURN 1 AS ok
            """,
            uid=user_id,
            bid=requested_business_id,
        ).single()
        if not ok:
            raise HTTPException(status_code=403, detail="You don't have access to that business")
        return requested_business_id

    ids = _user_business_ids(s, user_id)
    if len(ids) == 0:
        raise HTTPException(status_code=404, detail="You don't have a business yet")
    if len(ids) == 1:
        return ids[0]
    raise HTTPException(
        status_code=400,
        detail={"message": "Multiple businesses found; specify ?business_id=...", "your_business_ids": ids},
    )


# --------------------------------------------------------------------
# Neo helpers (no ownership assumptions)
# --------------------------------------------------------------------
def _get_business(s: Session, business_id: str) -> Dict[str, Any]:
    rec = s.run(
        """
        MATCH (b:BusinessProfile {id:$bid})
        WITH b, properties(b) AS p
        RETURN {
          id:                      p['id'],
          name:                    p['name'],
          pledge_tier:             p['pledge_tier'],
          eco_mint_ratio:          coalesce(p['eco_mint_ratio'], 10),
          stripe_customer_id:      p['stripe_customer_id'],
          stripe_subscription_id:  p['stripe_subscription_id'],
          stripe_subscription_item_id: p['stripe_subscription_item_id'],
          subscription_status:     p['subscription_status'],
          latest_unit_amount_aud:  p['latest_unit_amount_aud']
        } AS b
        """,
        bid=business_id,
    ).single()
    if not rec:
        raise HTTPException(status_code=404, detail="Business not found")
    return rec["b"]


def _attach_stripe_to_business(
    s: Session,
    *,
    business_id: str,
    customer_id: Optional[str],
    subscription_id: Optional[str],
    subscription_item_id: Optional[str],
    subscription_status: Optional[str],
    latest_unit_amount_aud: Optional[int] = None,
) -> None:
    s.run(
        """
        MATCH (b:BusinessProfile {id:$bid})
        SET  b.stripe_customer_id          = $cid,
             b.stripe_subscription_id      = $sid,
             b.stripe_subscription_item_id = $siid,
             b.subscription_status         = $status
        WITH b
        SET  b.latest_unit_amount_aud      = coalesce($latest, b.latest_unit_amount_aud)
        """,
        bid=business_id,
        cid=customer_id,
        sid=subscription_id,
        siid=subscription_item_id,
        status=subscription_status,
        latest=latest_unit_amount_aud,
    )


def _set_subscription_status_by_sid(s: Session, subscription_id: str, status_str: str) -> None:
    s.run(
        """
        MATCH (b:BusinessProfile {stripe_subscription_id:$sid})
        SET b.subscription_status=$st
        """,
        sid=subscription_id,
        st=status_str,
    )


def _hosted_return_url(path: str) -> str:
    return f"{PUBLIC_BASE.rstrip('/')}{path}"


# --------------------------------------------------------------------
# Models (scoped: no business_id in bodies; pass via query if multi-biz)
# --------------------------------------------------------------------
class CheckoutIn(BaseModel):
    monthly_aud: int = Field(..., ge=5, le=999)
    email: Optional[str] = None


class CheckoutOut(BaseModel):
    url: str


class PortalIn(BaseModel):
    # If not supplied, we'll use the resolved business' stored stripe_customer_id
    customer_id: Optional[str] = None
    # match new path
    return_path: str = "/my-eco-bizz/billing"


class PortalOut(BaseModel):
    url: str


class UpdateAmountIn(BaseModel):
    monthly_aud: int = Field(..., ge=5, le=999)  # set new monthly in Stripe


class UpdateAmountOut(BaseModel):
    ok: bool
    business_id: str
    monthly_aud: int


class BillingStatusOut(BaseModel):
    business_id: str
    has_subscription: bool
    subscription_status: Optional[str] = None
    customer_id: Optional[str] = None
    pledge_tier: Optional[str] = None
    eco_mint_ratio: int
    latest_unit_amount_aud: Optional[int] = None  # last known monthly amount we stored


# --------------------------------------------------------------------
# Create Checkout Session (SCOPED)
# --------------------------------------------------------------------
@router.post("/billing/checkout", response_model=CheckoutOut)
def create_checkout(
    payload: CheckoutIn,
    s: Session = Depends(session_dep),
    user_id: str = Depends(current_user_id),
    business_id: Optional[str] = Query(None, description="Optional if you own multiple"),
):
    bid = _resolve_user_business_id(s, user_id=user_id, requested_business_id=business_id)
    try:
        session = stripe.checkout.Session.create(
            mode="subscription",
            success_url=_hosted_return_url("/my-eco-bizz/billing?success=1"),
            cancel_url=_hosted_return_url("/my-eco-bizz/billing?canceled=1"),
            customer_email=payload.email,
            line_items=[
                {
                    "price_data": {
                        "currency": "aud",
                        "product_data": {
                            "name": "EYBA Monthly Contribution",
                            "metadata": {"business_id": bid},
                        },
                        "recurring": {"interval": "month"},
                        "unit_amount": int(payload.monthly_aud * 100),
                    },
                    "quantity": 1,
                }
            ],
            metadata={"business_id": bid},
        )
        return CheckoutOut(url=session.url)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Stripe error: {e}")


# --------------------------------------------------------------------
# Billing Portal (SCOPED)
# --------------------------------------------------------------------
@router.post("/billing/portal", response_model=PortalOut)
def create_portal(
    payload: PortalIn,
    s: Session = Depends(session_dep),
    user_id: str = Depends(current_user_id),
    business_id: Optional[str] = Query(None, description="Optional if you own multiple"),
):
    bid = _resolve_user_business_id(s, user_id=user_id, requested_business_id=business_id)
    b = _get_business(s, bid)

    # If client didn't send a customer_id, use the stored one; else verify ownership.
    customer_id = payload.customer_id or b.get("stripe_customer_id")
    if not customer_id:
        raise HTTPException(status_code=400, detail="No Stripe customer set up yet for this business")

    if payload.customer_id and payload.customer_id != b.get("stripe_customer_id"):
        raise HTTPException(status_code=403, detail="That customer id does not belong to your business")

    try:
        portal = stripe.billing_portal.Session.create(
            customer=customer_id,
            return_url=_hosted_return_url(payload.return_path),
        )
        return PortalOut(url=portal.url)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Stripe error: {e}")


# --------------------------------------------------------------------
# Update subscription amount (no proration) (SCOPED)
# --------------------------------------------------------------------
@router.post("/billing/update_amount", response_model=UpdateAmountOut)
def update_amount(
    payload: UpdateAmountIn,
    s: Session = Depends(session_dep),
    user_id: str = Depends(current_user_id),
    business_id: Optional[str] = Query(None, description="Optional if you own multiple"),
):
    bid = _resolve_user_business_id(s, user_id=user_id, requested_business_id=business_id)
    # Validate + get IDs from Neo
    biz = _get_business(s, bid)
    sub_id = biz.get("stripe_subscription_id")
    item_id = biz.get("stripe_subscription_item_id")
    if not sub_id or not item_id:
        raise HTTPException(status_code=400, detail="No active subscription to update")

    # Update Stripe
    try:
        sub = stripe.Subscription.retrieve(sub_id, expand=["items.data.price.product"])
        first_item = (sub.get("items", {}).get("data") or [None])[0]
        product_id = first_item["price"]["product"]["id"] if first_item else None
        if not product_id:
            raise HTTPException(status_code=500, detail="Stripe subscription item missing product")

        stripe.Subscription.modify(
            sub_id,
            proration_behavior="none",
            items=[
                {
                    "id": item_id,
                    "price_data": {
                        "currency": "aud",
                        "product": product_id,
                        "recurring": {"interval": "month"},
                        "unit_amount": int(payload.monthly_aud * 100),
                    },
                }
            ],
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Stripe error: {e}")

    # Persist the new amount to Neo for UI convenience
    _attach_stripe_to_business(
        s,
        business_id=bid,
        customer_id=biz.get("stripe_customer_id"),
        subscription_id=biz.get("stripe_subscription_id"),
        subscription_item_id=biz.get("stripe_subscription_item_id"),
        subscription_status=biz.get("subscription_status"),
        latest_unit_amount_aud=int(payload.monthly_aud),
    )

    return UpdateAmountOut(ok=True, business_id=bid, monthly_aud=int(payload.monthly_aud))


# --------------------------------------------------------------------
# Lightweight billing status for UI (SCOPED)
# --------------------------------------------------------------------
@router.get("/business/billing_status", response_model=BillingStatusOut)
def billing_status(
    s: Session = Depends(session_dep),
    user_id: str = Depends(current_user_id),
    business_id: Optional[str] = Query(None, description="Optional if you own multiple"),
):
    bid = _resolve_user_business_id(s, user_id=user_id, requested_business_id=business_id)
    b = _get_business(s, bid)
    return BillingStatusOut(
        business_id=bid,
        has_subscription=bool(b.get("stripe_subscription_id")),
        subscription_status=b.get("subscription_status"),
        customer_id=b.get("stripe_customer_id"),
        pledge_tier=b.get("pledge_tier"),
        eco_mint_ratio=int(b.get("eco_mint_ratio", 10)),
        latest_unit_amount_aud=(int(b["latest_unit_amount_aud"]) if b.get("latest_unit_amount_aud") is not None else None),
    )


# --------------------------------------------------------------------
# Webhook (GLOBAL - not scoped)
# --------------------------------------------------------------------
@router.post("/webhooks/stripe")
async def stripe_webhook(
    req: Request,
    s: Session = Depends(session_dep),
):
    # Verify (if secret configured)
    if not WEBHOOK_SECRET:
        event = await req.json()
    else:
        payload = await req.body()
        sig = req.headers.get("stripe-signature")
        try:
            event = stripe.Webhook.construct_event(payload, sig, WEBHOOK_SECRET)
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"Webhook signature error: {e}")

    t = event.get("type")
    data = event.get("data", {}).get("object", {}) or {}

    # Checkout completed → store Stripe IDs and last unit amount
    if t == "checkout.session.completed":
        biz_id = (data.get("metadata") or {}).get("business_id")
        customer = data.get("customer")
        subscription_id = data.get("subscription")
        if biz_id and subscription_id:
            try:
                sub = stripe.Subscription.retrieve(subscription_id, expand=["items.data.price.product"])
                first_item = (sub.get("items", {}).get("data") or [None])[0]
                item_id = first_item["id"] if first_item else None
                unit_amount = int(first_item["price"]["unit_amount"]) if first_item else 0
                status_str = sub.get("status")
                _attach_stripe_to_business(
                    s,
                    business_id=biz_id,
                    customer_id=customer,
                    subscription_id=subscription_id,
                    subscription_item_id=item_id,
                    subscription_status=status_str,
                    latest_unit_amount_aud=(unit_amount // 100 if unit_amount else None),
                )
            except Exception:
                # swallow and still 200, Stripe will retry if needed
                pass

    # Invoice paid → mint ECO contribution
    if t == "invoice.paid":
        subscription_id = data.get("subscription")
        amount_paid_cents = int(data.get("amount_paid") or 0)
        if subscription_id and amount_paid_cents > 0:
            rec = s.run(
                "MATCH (b:BusinessProfile {stripe_subscription_id:$sid}) RETURN b.id AS bid",
                sid=subscription_id,
            ).single()
            if rec and rec.get("bid"):
                bid = rec["bid"]
                try:
                    # Mint ECO into unified ledger (UNSCOPED canonical path)
                    stripe_record_contribution(s, business_id=bid, aud_cents=amount_paid_cents)
                    # Also store latest amount on business for UI
                    _attach_stripe_to_business(
                        s,
                        business_id=bid,
                        customer_id=None,
                        subscription_id=subscription_id,
                        subscription_item_id=None,
                        subscription_status=data.get("status") or "active",
                        latest_unit_amount_aud=amount_paid_cents // 100,
                    )
                except Exception:
                    pass  # keep webhook idempotent-friendly

    # Subscription lifecycle → update status
    if t in (
        "customer.subscription.updated",
        "customer.subscription.deleted",
        "customer.subscription.paused",
        "customer.subscription.resumed",
    ):
        sub = data
        subscription_id = sub.get("id")
        status_str = sub.get("status")
        if subscription_id and status_str:
            _set_subscription_status_by_sid(s, subscription_id, status_str)

    return {"ok": True, "received": t}
