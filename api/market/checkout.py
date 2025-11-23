from __future__ import annotations

import os
import logging
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, status
from neo4j import Session  # type: ignore
from pydantic import BaseModel

import boto3
import botocore.exceptions

from site_backend.core.neo_driver import session_dep
from site_backend.core.user_guard import current_user_id
from site_backend.core.urls import abs_media

# Stripe config
try:
    import stripe  # type: ignore
except ImportError:  # pragma: no cover
    stripe = None  # type: ignore

STRIPE_SECRET_KEY = os.getenv("STRIPE_SECRET_KEY")
DEFAULT_CURRENCY = (os.getenv("MARKET_CURRENCY", "AUD") or "AUD").lower()

# Revenue split config (fractions of total in cents)
# Defaults: 85% designer, 10% manager, 5% platform
try:
    MARKET_DESIGNER_SHARE = float(os.getenv("MARKET_DESIGNER_SHARE", "0.85"))
    MARKET_MANAGER_SHARE = float(os.getenv("MARKET_MANAGER_SHARE", "0.10"))
    MARKET_PLATFORM_SHARE = float(os.getenv("MARKET_PLATFORM_SHARE", "0.05"))
except ValueError:
    MARKET_DESIGNER_SHARE = 0.85
    MARKET_MANAGER_SHARE = 0.10
    MARKET_PLATFORM_SHARE = 0.05

router = APIRouter(
    prefix="/market/checkout",
    tags=["market_checkout"],
)

log = logging.getLogger("market.checkout")

# ============================================================
# Email (SES)
# ============================================================

SES_REGION = os.getenv("SES_REGION", "ap-southeast-2")
SES_FROM = os.getenv("SES_FROM", "connect@ecodia.au")
_ses_client = boto3.client("ses", region_name=SES_REGION)


def _send_email(to_email: str, subject: str, body: str) -> bool:
    """
    Send a plain-text email via AWS SES.

    Returns True on success, False on failure (callers may log/warn).
    """
    to_email = (to_email or "").strip()
    if not to_email:
        log.warning("[MAIL][SES] No destination email provided; skipping send.")
        return False

    try:
        log.info(
            "[MAIL][SES] Sending email to=%r from=%r subj=%r",
            to_email,
            SES_FROM,
            subject,
        )
        _ses_client.send_email(
            Source=SES_FROM,
            Destination={"ToAddresses": [to_email]},
            Message={
                "Subject": {"Data": subject, "Charset": "UTF-8"},
                "Body": {
                    "Text": {"Data": body, "Charset": "UTF-8"},
                },
            },
        )
        return True
    except botocore.exceptions.ClientError as e:
        log.warning("[MAIL][SES][ERROR] ClientError sending to %s: %s", to_email, e)
        return False
    except Exception as e:
        log.warning("[MAIL][SES][ERROR] Unexpected error sending to %s: %s", to_email, e)
        return False


# ============================================================
# Models
# ============================================================


class UpcyclingCheckoutCreateIn(BaseModel):
    item_id: str
    # Use plain string so Stripe's {CHECKOUT_SESSION_ID} placeholder is allowed
    success_url: str
    cancel_url: str

    buyer_name: str
    buyer_email: Optional[str] = None
    delivery_notes: Optional[str] = None


class UpcyclingCheckoutSessionOut(BaseModel):
    id: str
    url: str


class UpcyclingCheckoutFinalizeIn(BaseModel):
    session_id: str


class UpcyclingCheckoutFinalizeOut(BaseModel):
    order_id: str
    item_id: str
    store_id: str
    status: str


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
    stripe.api_key = STRIPE_SECRET_KEY  # type: ignore[attr-defined]


def _send_store_order_email(
    *,
    to_email: Optional[str],
    store_name: str,
    item_title: str,
    amount_cents: int,
    currency: str,
    buyer_name: Optional[str],
    buyer_email: Optional[str],
    delivery_notes: Optional[str],
) -> None:
    """
    Notify the store owner that they've received a new paid order.
    Best-effort via SES (non-fatal on failure).
    """
    to_email_clean = (to_email or "").strip()
    if not to_email_clean:
        log.warning("No store owner email for order; skipping notification email.")
        return

    amount = amount_cents / 100.0
    currency_upper = (currency or "AUD").upper()

    subject = f"New Ecodia upcycling order – {item_title}"
    body = f"""Hey {store_name},

You just received a new Ecodia upcycling order.

Item: {item_title}
Amount: {currency_upper} {amount:.2f}

Buyer name: {buyer_name or 'Not provided'}
Buyer email: {buyer_email or 'Not provided'}

Delivery / pickup details:
{delivery_notes or '(none provided)'}

You can also view this order in your Ecodia store dashboard.

– Ecodia
"""

    if not _send_email(to_email_clean, subject, body):
        log.warning("Failed to send upcycling order email via SES to %s", to_email_clean)


def _send_buyer_order_email(
    *,
    to_email: Optional[str],
    buyer_name: Optional[str],
    store_name: str,
    item_title: str,
    amount_cents: int,
    currency: str,
    status: str,
    tracking_number: Optional[str] = None,
    tracking_url: Optional[str] = None,
) -> None:
    """
    Notify the buyer about their order / status updates.
    This can be imported by market_store.py as well when sellers update orders.
    """
    to_email_clean = (to_email or "").strip()
    if not to_email_clean:
        log.info("No buyer email for order; skipping buyer email.")
        return

    amount = amount_cents / 100.0
    currency_upper = (currency or "AUD").upper()
    pretty_status = (status or "").replace("_", " ").title() or "Updated"

    tracking_lines = ""
    if tracking_number or tracking_url:
        tracking_lines = "\nTracking details:\n"
        if tracking_number:
            tracking_lines += f"- Tracking number: {tracking_number}\n"
        if tracking_url:
            tracking_lines += f"- Track online: {tracking_url}\n"

    subject = f"Your Ecodia order – {item_title} ({pretty_status})"
    body = f"""Hey {buyer_name or 'there'},

This is an update about your Ecodia upcycling order.

Store: {store_name}
Item: {item_title}
Amount: {currency_upper} {amount:.2f}
Status: {pretty_status}
{tracking_lines}
If anything looks off, you can reply to this email or message Ecodia on Instagram.

– Ecodia
"""

    if not _send_email(to_email_clean, subject, body):
        log.warning("Failed to send buyer order email via SES to %s", to_email_clean)


# ============================================================
# Routes
# ============================================================


@router.post(
    "/session",
    response_model=UpcyclingCheckoutSessionOut,
    status_code=status.HTTP_201_CREATED,
)
async def create_checkout_session(
    p: UpcyclingCheckoutCreateIn,
    session: Session = Depends(session_dep),
    buyer_user_id: str = Depends(current_user_id),
):
    """
    Create a Stripe Checkout session for a single UpcyclingItem.

    - Requires logged-in buyer.
    - Charges full item price to Ecodia's Stripe account (payouts later).
    - Records an UpcyclingOrder node with split shares for designer/manager/platform
      and status 'pending' until Stripe payment is confirmed.
    """
    _require_stripe()

    # 1) Fetch item + store from Neo4j
    cypher = """
    MATCH (item:UpcyclingItem {id: $item_id})<-[:HAS_ITEM]-(store:UpcyclingStore)
    MATCH (u:User {id: $buyer_id})
    RETURN item, store, u AS buyer
    """
    result = session.run(
        cypher,
        {"item_id": p.item_id, "buyer_id": buyer_user_id},
    )
    record = result.single()
    if not record:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Item or store not found.",
        )

    item = record["item"]
    store = record["store"]

    price: Optional[float] = item.get("price")
    currency: str = (item.get("currency") or DEFAULT_CURRENCY or "aud").lower()
    status_val: str = (item.get("status") or "active").lower()

    if price is None:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Item has no price set.",
        )

    if status_val != "active":
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Item is not available for purchase.",
        )

    # Convert to smallest currency unit (e.g. cents)
    try:
        unit_amount = int(round(float(price) * 100))
    except Exception:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid item price.",
        )
    if unit_amount <= 0:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Item price must be greater than zero.",
        )

    # Compute revenue shares in cents
    total_cents = unit_amount
    designer_cents = int(round(total_cents * MARKET_DESIGNER_SHARE))
    manager_cents = int(round(total_cents * MARKET_MANAGER_SHARE))
    platform_cents = total_cents - designer_cents - manager_cents
    if platform_cents < 0:
        # In case env vars are misconfigured and sums > 1.0, clamp
        platform_cents = 0

    # Build product info
    title = str(item.get("title") or "Upcycled piece")
    description = str(item.get("description") or "")
    store_name = str(store.get("display_name") or "").strip() or "Ecodia Upcycling"
    image_url_raw = item.get("image_url") or store.get("hero_image_url")
    image_url_abs: Optional[str] = abs_media(image_url_raw) if image_url_raw else None

    buyer_name = p.buyer_name.strip()
    buyer_email = (p.buyer_email or "").strip() or None
    delivery_notes = (p.delivery_notes or "").strip() or None

    # 2) Create Stripe Checkout Session
    try:
        line_item: dict = {
            "price_data": {
                "currency": currency,
                "product_data": {
                    "name": f"{title} – {store_name}",
                },
                "unit_amount": unit_amount,
            },
            "quantity": 1,
        }
        if description:
            line_item["price_data"]["product_data"]["description"] = description
        if image_url_abs:
            line_item["price_data"]["product_data"]["images"] = [image_url_abs]

        checkout_session = stripe.checkout.Session.create(  # type: ignore[attr-defined]
            mode="payment",
            payment_method_types=["card"],
            line_items=[line_item],
            success_url=p.success_url,
            cancel_url=p.cancel_url,
            metadata={
                "upcycling_item_id": str(item["id"]),
                "upcycling_store_id": str(store["id"]),
                "buyer_user_id": buyer_user_id,
                "buyer_name": buyer_name,
                "buyer_email": buyer_email or "",
                "delivery_notes": delivery_notes or "",
            },
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail=f"Failed to create Stripe checkout session: {e}",
        )

    # 3) Record an UpcyclingOrder node (pending, with split shares)
    order_cypher = """
    MATCH (item:UpcyclingItem {id: $item_id})<-[:HAS_ITEM]-(store:UpcyclingStore)
    MATCH (buyer:User {id: $buyer_id})
    CREATE (order:UpcyclingOrder {
        id: randomUUID(),
        stripe_session_id: $session_id,
        stripe_payment_intent_id: $payment_intent_id,
        item_id: item.id,
        store_id: store.id,
        buyer_id: buyer.id,
        amount: $amount,
        currency: $currency,
        status: 'pending',

        // revenue splits
        share_designer_cents: $share_designer_cents,
        share_manager_cents: $share_manager_cents,
        share_platform_cents: $share_platform_cents,

        // payout flags
        designer_paid: false,
        manager_paid: false,

        buyer_name: $buyer_name,
        buyer_email: $buyer_email,
        delivery_notes: $delivery_notes,
        created_at: datetime(),
        updated_at: datetime()
    })
    MERGE (buyer)-[:PLACED_ORDER]->(order)
    MERGE (store)-[:RECEIVED_ORDER]->(order)
    RETURN order.id AS order_id
    """

    session.run(
        order_cypher,
        {
            "item_id": p.item_id,
            "buyer_id": buyer_user_id,
            "session_id": checkout_session.id,
            "payment_intent_id": getattr(checkout_session, "payment_intent", None),
            "amount": total_cents,
            "currency": currency.upper(),
            "share_designer_cents": designer_cents,
            "share_manager_cents": manager_cents,
            "share_platform_cents": platform_cents,
            "buyer_name": buyer_name,
            "buyer_email": buyer_email,
            "delivery_notes": delivery_notes,
        },
    )

    return UpcyclingCheckoutSessionOut(id=checkout_session.id, url=checkout_session.url)


@router.post(
    "/finalize",
    response_model=UpcyclingCheckoutFinalizeOut,
)
async def finalize_checkout_session(
    p: UpcyclingCheckoutFinalizeIn,
    session: Session = Depends(session_dep),
    buyer_user_id: str = Depends(current_user_id),
):
    """
    Finalize a checkout session after redirect to /market/checkout/success.

    - Verifies the Stripe session is paid.
    - Marks the order as 'succeeded' and sets paid_at (ready for payout cron).
    - Marks the item as 'sold_out'.
    - Sends an email to the store owner and the buyer with order + delivery details.
    """
    _require_stripe()

    # 1) Retrieve session from Stripe and ensure it's paid
    try:
        stripe_session = stripe.checkout.Session.retrieve(p.session_id)  # type: ignore[attr-defined]
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Stripe session not found: {e}",
        )

    payment_status = getattr(stripe_session, "payment_status", None)
    if payment_status != "paid":
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Payment not completed yet.",
        )

    md = getattr(stripe_session, "metadata", {}) or {}
    buyer_name = (md.get("buyer_name") or "").strip() or None
    buyer_email = (md.get("buyer_email") or "").strip() or None
    delivery_notes = (md.get("delivery_notes") or "").strip() or None

    # 2) Update order + item, ensure it belongs to the current user
    cypher = """
    MATCH (buyer:User {id: $buyer_id})-[:PLACED_ORDER]->(order:UpcyclingOrder {stripe_session_id: $session_id})
    MATCH (store:UpcyclingStore {id: order.store_id})
    MATCH (owner:User)-[:OWNS_UPCYCLING_STORE]->(store)
    MATCH (item:UpcyclingItem {id: order.item_id})
    SET
      order.status = 'succeeded',
      order.updated_at = datetime(),
      order.paid_at = coalesce(order.paid_at, datetime()),
      order.buyer_name = coalesce($buyer_name, order.buyer_name),
      order.buyer_email = coalesce($buyer_email, order.buyer_email),
      order.delivery_notes = coalesce($delivery_notes, order.delivery_notes),
      item.status = 'sold_out',
      item.updated_at = datetime()
    RETURN order, item, store, owner
    """
    result = session.run(
        cypher,
        {
            "buyer_id": buyer_user_id,
            "session_id": p.session_id,
            "buyer_name": buyer_name,
            "buyer_email": buyer_email,
            "delivery_notes": delivery_notes,
        },
    )
    record = result.single()
    if not record:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Order not found for this session/user.",
        )

    order = record["order"]
    item = record["item"]
    store = record["store"]
    owner = record["owner"]

    # 3) Email store owner (best-effort, non-fatal)
    try:
        _send_store_order_email(
            to_email=owner.get("email"),
            store_name=str(store.get("display_name") or ""),
            item_title=str(item.get("title") or ""),
            amount_cents=int(order.get("amount") or 0),
            currency=str(order.get("currency") or "AUD"),
            buyer_name=order.get("buyer_name") or buyer_name,
            buyer_email=order.get("buyer_email") or buyer_email,
            delivery_notes=order.get("delivery_notes") or delivery_notes,
        )
    except Exception as e:
        log.warning("Error while sending store order email (non-fatal): %s", e)

    # 4) Email buyer with confirmation (best-effort)
    try:
        _send_buyer_order_email(
            to_email=order.get("buyer_email") or buyer_email,
            buyer_name=order.get("buyer_name") or buyer_name,
            store_name=str(store.get("display_name") or ""),
            item_title=str(item.get("title") or ""),
            amount_cents=int(order.get("amount") or 0),
            currency=str(order.get("currency") or "AUD"),
            status=str(order.get("status") or "succeeded"),
        )
    except Exception as e:
        log.warning("Error while sending buyer order email (non-fatal): %s", e)

    return UpcyclingCheckoutFinalizeOut(
        order_id=str(order["id"]),
        item_id=str(item["id"]),
        store_id=str(store["id"]),
        status=str(order.get("status", "succeeded")),
    )
