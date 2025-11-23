from __future__ import annotations

from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, status
from neo4j import Session  # type: ignore
from pydantic import BaseModel

from site_backend.core.neo_driver import session_dep
from site_backend.core.user_guard import current_user_id

router = APIRouter(
    prefix="/market/admin",
    tags=["market_admin"],
)

# NOTE:
# - This router is meant for admin-only usage.
# - Swap `current_user_id` for your stricter admin guard if you have one
#   (e.g. require_admin_user_id) or enforce admin in a dependency.


# ============================================================
# Models
# ============================================================


class UpcyclingOrderAdminOut(BaseModel):
    id: str

    item_id: str
    item_title: Optional[str] = None
    item_image_url: Optional[str] = None

    amount_cents: int
    currency: str
    status: str

    buyer_id: Optional[str] = None
    buyer_name: Optional[str] = None
    buyer_email: Optional[str] = None

    delivery_notes: Optional[str] = None

    tracking_number: Optional[str] = None
    tracking_carrier: Optional[str] = None
    tracking_url: Optional[str] = None

    seller_note: Optional[str] = None

    store_id: Optional[str] = None
    store_name: Optional[str] = None
    store_handle: Optional[str] = None

    # Revenue splits / payout info
    share_designer_cents: int
    share_manager_cents: int
    share_platform_cents: int
    designer_paid: bool
    manager_paid: bool

    created_at: Optional[str] = None
    paid_at: Optional[str] = None
    shipped_at: Optional[str] = None
    delivered_at: Optional[str] = None
    cancelled_at: Optional[str] = None
    last_status_update_at: Optional[str] = None


class UpcyclingOrderAdminList(BaseModel):
    items: List[UpcyclingOrderAdminOut]
    total: int


# ============================================================
# Helpers
# ============================================================


def _ensure_admin(user_id: str, session: Session) -> None:
    """
    Very simple admin check. Replace with your existing admin guard
    if you already have one (preferred).
    """
    row = session.run(
        """
        MATCH (u:User {id: $uid})
        RETURN coalesce(u.role, '') AS role
        """,
        uid=user_id,
    ).single()

    role = (row["role"] if row else "") or ""
    if role != "admin":
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Admin access required.",
        )


def _neo_time_to_str(value) -> Optional[str]:
    if value is None:
        return None
    # Neo4j datetime has iso_format(); fall back to str(..) if needed
    try:
        iso = value.iso_format()  # type: ignore[attr-defined]
    except Exception:
        iso = str(value)
    return iso


# ============================================================
# Routes
# ============================================================


@router.get(
    "/orders",
    response_model=UpcyclingOrderAdminList,
    status_code=status.HTTP_200_OK,
)
def list_admin_upcycling_orders(
    status_filter: Optional[str] = Query(
        None,
        description="Optional status filter (pending, succeeded, shipped, etc.)",
    ),
    store_id: Optional[str] = Query(
        None, description="Optional store id/handle filter"
    ),
    buyer_q: Optional[str] = Query(
        None,
        description="Optional buyer search (name/email contains, case-insensitive)",
    ),
    skip: int = Query(0, ge=0),
    limit: int = Query(50, ge=1, le=200),
    current_admin_id: str = Depends(current_user_id),
    session: Session = Depends(session_dep),
):
    """
    Admin: List all upcycling orders across all stores.

    - Supports filtering by status, store, and buyer text search.
    - Returns item, store, buyer, and payout info for each order.
    """
    # Enforce admin
    _ensure_admin(current_admin_id, session)

    filters = []
    params: dict = {
        "skip": skip,
        "limit": limit,
    }

    if status_filter:
        filters.append("o.status = $status_filter")
        params["status_filter"] = status_filter

    if store_id:
        # Store id OR handle match
        filters.append("(o.store_id = $store_id OR store.handle = $store_id)")
        params["store_id"] = store_id

    if buyer_q:
        filters.append(
            "(toLower(o.buyer_name) CONTAINS toLower($buyer_q) "
            "OR toLower(o.buyer_email) CONTAINS toLower($buyer_q))"
        )
        params["buyer_q"] = buyer_q

    where_clause = ""
    if filters:
        where_clause = "WHERE " + " AND ".join(filters)

    # Count query
    count_cypher = f"""
    MATCH (o:UpcyclingOrder)
    OPTIONAL MATCH (s:UpcyclingStore {{id: o.store_id}})
    {where_clause}
    RETURN count(DISTINCT o) AS total
    """

    count_row = session.run(count_cypher, params).single()
    total = int(count_row["total"] if count_row and count_row["total"] is not None else 0)

    # Data query
    cypher = f"""
    MATCH (o:UpcyclingOrder)
    OPTIONAL MATCH (item:UpcyclingItem {{id: o.item_id}})
    OPTIONAL MATCH (store:UpcyclingStore {{id: o.store_id}})
    OPTIONAL MATCH (buyer:User {{id: o.buyer_id}})
    {where_clause}
    RETURN o, item, store, buyer
    ORDER BY o.created_at DESC
    SKIP $skip
    LIMIT $limit
    """

    rows = session.run(cypher, params)

    items: List[UpcyclingOrderAdminOut] = []

    for r in rows:
        o = r["o"]
        item = r.get("item")
        store = r.get("store")
        buyer = r.get("buyer")

        # core order fields
        amount_cents = int(o.get("amount") or 0)
        currency = str(o.get("currency") or "AUD")

        # payout info with sensible defaults
        share_designer_cents = int(o.get("share_designer_cents") or 0)
        share_manager_cents = int(o.get("share_manager_cents") or 0)
        share_platform_cents = int(o.get("share_platform_cents") or 0)
        designer_paid = bool(o.get("designer_paid") or False)
        manager_paid = bool(o.get("manager_paid") or False)

        # datetimes -> strings
        created_at = _neo_time_to_str(o.get("created_at"))
        paid_at = _neo_time_to_str(o.get("paid_at"))
        shipped_at = _neo_time_to_str(o.get("shipped_at"))
        delivered_at = _neo_time_to_str(o.get("delivered_at"))
        cancelled_at = _neo_time_to_str(o.get("cancelled_at"))
        last_status_update_at = _neo_time_to_str(o.get("last_status_update_at"))

        items.append(
            UpcyclingOrderAdminOut(
                id=str(o["id"]),
                item_id=str(o.get("item_id") or ""),
                item_title=(item.get("title") if item else None),
                item_image_url=(item.get("image_url") if item else None),
                amount_cents=amount_cents,
                currency=currency,
                status=str(o.get("status") or ""),

                buyer_id=str(o.get("buyer_id") or "") or None,
                buyer_name=o.get("buyer_name"),
                buyer_email=o.get("buyer_email"),

                delivery_notes=o.get("delivery_notes"),
                tracking_number=o.get("tracking_number"),
                tracking_carrier=o.get("tracking_carrier"),
                tracking_url=o.get("tracking_url"),
                seller_note=o.get("seller_note"),

                store_id=str(o.get("store_id") or "") or None,
                store_name=(store.get("display_name") if store else None),
                store_handle=(store.get("handle") if store else None),

                share_designer_cents=share_designer_cents,
                share_manager_cents=share_manager_cents,
                share_platform_cents=share_platform_cents,
                designer_paid=designer_paid,
                manager_paid=manager_paid,

                created_at=created_at,
                paid_at=paid_at,
                shipped_at=shipped_at,
                delivered_at=delivered_at,
                cancelled_at=cancelled_at,
                last_status_update_at=last_status_update_at,
            )
        )

    return UpcyclingOrderAdminList(items=items, total=total)
