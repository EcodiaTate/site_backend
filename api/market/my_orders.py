from __future__ import annotations

import logging
from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, status
from neo4j import Session  # type: ignore
from pydantic import BaseModel

from site_backend.core.neo_driver import session_dep
from site_backend.core.user_guard import current_user_id

log = logging.getLogger("market.my_orders")

router = APIRouter(
    prefix="/market/my",
    tags=["market_my_orders"],
)


# ============================================================
# Models
# ============================================================


class UpcyclingOrderStatus(str):
    """
    Kept as simple str type to avoid FastAPI Enum serialization weirdness.
    Must align with frontend type:
      'pending' | 'paid' | 'preparing' | 'shipped' | 'delivered' | 'cancelled'
    """
    pass


class UpcyclingOrderOut(BaseModel):
    """
    Buyer-facing view of upcycling orders.
    Mirrors the TS UpcyclingOrder interface (plus store metadata).
    """

    id: str
    item_id: str
    item_title: str
    item_image_url: Optional[str] = None

    amount_cents: int
    currency: str
    status: str  # we’ll keep as string; frontend narrows to UpcyclingOrderStatus

    buyer_name: Optional[str] = None
    buyer_email: Optional[str] = None
    buyer_instagram: Optional[str] = None
    delivery_notes: Optional[str] = None

    tracking_number: Optional[str] = None
    tracking_carrier: Optional[str] = None
    tracking_url: Optional[str] = None

    # Optional grouping key so sellers can ship multiple pieces in one package.
    group_id: Optional[str] = None

    # Store metadata for buyer UI
    store_id: Optional[str] = None
    store_name: Optional[str] = None
    store_handle: Optional[str] = None

    created_at: str
    paid_at: Optional[str] = None
    last_status_update_at: Optional[str] = None


# ============================================================
# Helpers
# ============================================================


def _normalize_status(raw: Optional[str]) -> str:
    """
    Clamp weird statuses into the known set so the frontend union stays sane.
    """
    if not raw:
        return "pending"
    raw_l = raw.lower()
    allowed = {"pending", "paid", "preparing", "shipped", "delivered", "cancelled"}
    if raw_l in allowed:
        return raw_l
    # Fallback: if old 'packed' is present, treat it as 'preparing'
    if raw_l == "packed":
        return "preparing"
    return "pending"


# ============================================================
# Routes – buyer "my orders"
# ============================================================


@router.get(
    "/orders",
    response_model=List[UpcyclingOrderOut],
)
async def get_my_orders(
    session: Session = Depends(session_dep),
    user_id: str = Depends(current_user_id),
):
    """
    Buyer: list orders placed by the current user.

    Includes:
      - item title + hero image
      - amount + status
      - delivery notes
      - tracking info
      - store name / handle (for linking back to catalogue)
    """
    cypher = """
    MATCH (buyer:User {id: $user_id})-[:PLACED_ORDER]->(o:UpcyclingOrder)
    OPTIONAL MATCH (item:UpcyclingItem {id: o.item_id})
    OPTIONAL MATCH (store:UpcyclingStore {id: o.store_id})
    WITH o, item, store
    ORDER BY o.created_at DESC
    RETURN collect({
        id: o.id,
        item_id: o.item_id,
        item_title: coalesce(item.title, '(deleted item)'),
        item_image_url: item.image_url,

        amount_cents: coalesce(o.amount, 0),
        currency: coalesce(o.currency, 'AUD'),
        status: coalesce(o.status, 'pending'),

        buyer_name: o.buyer_name,
        buyer_email: o.buyer_email,
        buyer_instagram: o.buyer_instagram,
        delivery_notes: o.delivery_notes,

        tracking_number: o.tracking_number,
        tracking_carrier: o.tracking_carrier,
        tracking_url: o.tracking_url,

        group_id: coalesce(o.group_id, coalesce(o.buyer_email, o.buyer_id)),

        store_id: store.id,
        store_name: store.display_name,
        store_handle: store.handle,

        created_at: toString(o.created_at),
        paid_at: CASE
          WHEN o.paid_at IS NULL THEN NULL
          ELSE toString(o.paid_at)
        END,
        last_status_update_at: CASE
          WHEN o.last_status_update_at IS NULL THEN NULL
          ELSE toString(o.last_status_update_at)
        END
    }) AS orders
    """

    rec = session.run(cypher, {"user_id": user_id}).single()
    if not rec:
        return []

    data = rec["orders"] or []

    out: List[UpcyclingOrderOut] = []
    for o in data:
        o["status"] = _normalize_status(o.get("status"))
        out.append(UpcyclingOrderOut(**o))
    return out


@router.get(
    "/orders/{order_id}",
    response_model=UpcyclingOrderOut,
)
async def get_my_order_by_id(
    order_id: str,
    session: Session = Depends(session_dep),
    user_id: str = Depends(current_user_id),
):
    """
    Buyer: fetch a single order by id, ensuring it belongs to the current user.
    """
    cypher = """
    MATCH (buyer:User {id: $user_id})-[:PLACED_ORDER]->(o:UpcyclingOrder {id: $order_id})
    OPTIONAL MATCH (item:UpcyclingItem {id: o.item_id})
    OPTIONAL MATCH (store:UpcyclingStore {id: o.store_id})
    RETURN {
        id: o.id,
        item_id: o.item_id,
        item_title: coalesce(item.title, '(deleted item)'),
        item_image_url: item.image_url,

        amount_cents: coalesce(o.amount, 0),
        currency: coalesce(o.currency, 'AUD'),
        status: coalesce(o.status, 'pending'),

        buyer_name: o.buyer_name,
        buyer_email: o.buyer_email,
        buyer_instagram: o.buyer_instagram,
        delivery_notes: o.delivery_notes,

        tracking_number: o.tracking_number,
        tracking_carrier: o.tracking_carrier,
        tracking_url: o.tracking_url,

        group_id: coalesce(o.group_id, coalesce(o.buyer_email, o.buyer_id)),

        store_id: store.id,
        store_name: store.display_name,
        store_handle: store.handle,

        created_at: toString(o.created_at),
        paid_at: CASE
          WHEN o.paid_at IS NULL THEN NULL
          ELSE toString(o.paid_at)
        END,
        last_status_update_at: CASE
          WHEN o.last_status_update_at IS NULL THEN NULL
          ELSE toString(o.last_status_update_at)
        END
    } AS order
    """

    rec = session.run(
        cypher,
        {"user_id": user_id, "order_id": order_id},
    ).single()

    if not rec:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Order not found for this account.",
        )

    data = rec["order"]
    data["status"] = _normalize_status(data.get("status"))
    return UpcyclingOrderOut(**data)
