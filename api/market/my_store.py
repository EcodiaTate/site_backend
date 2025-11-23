from __future__ import annotations

import logging
import os
from enum import Enum
from pathlib import Path
from typing import List, Optional

from fastapi import (
    APIRouter,
    Depends,
    File,
    Form,
    HTTPException,
    UploadFile,
    status,
)
from neo4j import Session  # type: ignore
from pydantic import BaseModel

from site_backend.core.neo_driver import session_dep
from site_backend.core.user_guard import current_user_id
from site_backend.core.paths import UPLOAD_ROOT
from site_backend.core.urls import abs_media

import boto3
import botocore.exceptions

router = APIRouter(
    prefix="/market/store",
    tags=["market_store"],
)

log = logging.getLogger("market.store")

# ============================================================
# Email (SES) for order updates
# ============================================================

SES_REGION = os.getenv("SES_REGION", "ap-southeast-2")
SES_FROM = os.getenv("SES_FROM", "connect@ecodia.au")
_ses_client = boto3.client("ses", region_name=SES_REGION)


def _send_email(to_email: str, subject: str, body: str) -> bool:
    """
    Best-effort plain-text email via SES.
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
                "Body": {"Text": {"Data": body, "Charset": "UTF-8"}},
            },
        )
        return True
    except botocore.exceptions.ClientError as e:
        log.warning("[MAIL][SES][ERROR] ClientError sending to %s: %s", to_email, e)
        return False
    except Exception as e:
        log.warning("[MAIL][SES][ERROR] Unexpected error sending to %s: %s", to_email, e)
        return False


def _format_order_status_email_body(
    *,
    heading: str,
    store_name: str,
    item_title: str,
    status: str,
    amount_cents: int,
    currency: str,
    tracking_number: Optional[str],
    tracking_url: Optional[str],
    delivery_notes: Optional[str],
    seller_note: Optional[str],
) -> str:
    amount = (amount_cents or 0) / 100.0
    currency_upper = (currency or "AUD").upper()

    tracking_block = "Tracking: (none provided)"
    if tracking_number or tracking_url:
        tracking_block = "Tracking:\n"
        if tracking_number:
            tracking_block += f"- Number: {tracking_number}\n"
        if tracking_url:
            tracking_block += f"- Link: {tracking_url}\n"

    note_block = ""
    if seller_note:
        note_block = f"\nSeller note:\n{seller_note}\n"

    pretty_status = status.replace("_", " ").title() if status else "Pending"

    return f"""{heading}

Store: {store_name}
Item: {item_title}
Amount: {currency_upper} {amount:.2f}
Status: {pretty_status}

{tracking_block}

Delivery / pickup details:
{delivery_notes or '(none provided)'}{note_block}

You can always log into Ecodia to view the latest status.

– Ecodia
"""


# ============================================================
# Models
# ============================================================


class ItemStatus(str, Enum):
    active = "active"
    draft = "draft"
    sold_out = "sold_out"


class UpcyclingItemOut(BaseModel):
    id: str
    store_id: str
    title: str
    description: Optional[str] = None
    price: Optional[float] = None
    currency: str
    size: Optional[str] = None
    status: ItemStatus
    tags: List[str]
    image_url: Optional[str] = None
    created_at: str
    updated_at: str


class UpcyclingStoreProfileOut(BaseModel):
    id: str
    user_id: str
    handle: str
    display_name: str
    bio: Optional[str] = None
    hero_image_url: Optional[str] = None
    style_tags: List[str]
    created_at: str
    updated_at: Optional[str] = None


class UpcyclingStoreDetailOut(BaseModel):
    store: UpcyclingStoreProfileOut
    items: List[UpcyclingItemOut]


class UpcyclingOrderStatus(str, Enum):
    pending = "pending"
    paid = "paid"
    preparing = "preparing"  # matches frontend 'preparing'
    shipped = "shipped"
    delivered = "delivered"
    cancelled = "cancelled"


class UpcyclingOrderOut(BaseModel):
    """
    Seller-facing + frontend-aligned view of upcycling orders.
    """

    id: str
    item_id: str
    item_title: str
    item_image_url: Optional[str] = None

    amount_cents: int
    currency: str
    status: UpcyclingOrderStatus

    buyer_name: Optional[str] = None
    buyer_email: Optional[str] = None
    buyer_instagram: Optional[str] = None
    delivery_notes: Optional[str] = None

    tracking_number: Optional[str] = None
    tracking_carrier: Optional[str] = None
    tracking_url: Optional[str] = None

    # grouping key for multi-piece shipments
    group_id: Optional[str] = None

    created_at: str
    paid_at: Optional[str] = None
    last_status_update_at: Optional[str] = None


class UpcyclingOrderUpdateIn(BaseModel):
    """
    Seller-side patch payload.
    Matches the frontend UpcyclingOrderUpdateInput shape.
    """

    status: Optional[UpcyclingOrderStatus] = None
    tracking_number: Optional[str] = None
    tracking_carrier: Optional[str] = None
    tracking_url: Optional[str] = None
    # Optional internal note for emails / your own reference (not in TS type but harmless)
    seller_note: Optional[str] = None


# ============================================================
# File helpers
# ============================================================

# Store hero images: <UPLOAD_ROOT>/upcycling_store_hero
STORE_HERO_DIR = UPLOAD_ROOT / "upcycling_store_hero"

# Item images: <UPLOAD_ROOT>/upcycling_items
ITEM_IMAGE_DIR = UPLOAD_ROOT / "upcycling_items"


async def save_store_hero(photo: UploadFile) -> str:
    """
    Save a store hero image under <UPLOAD_ROOT>/upcycling_store_hero
    and return a path served from /uploads.
    """
    STORE_HERO_DIR.mkdir(parents=True, exist_ok=True)
    suffix = Path(photo.filename or "image").suffix or ".jpg"

    import uuid

    file_id = uuid.uuid4().hex
    filename = f"{file_id}{suffix}"
    dest = STORE_HERO_DIR / filename

    content = await photo.read()
    dest.write_bytes(content)

    # Browsers fetch this at /uploads/...
    return f"/uploads/upcycling_store_hero/{filename}"


async def save_item_image(photo: UploadFile) -> str:
    """
    Save an item image under <UPLOAD_ROOT>/upcycling_items
    and return a path served from /uploads.
    """
    ITEM_IMAGE_DIR.mkdir(parents=True, exist_ok=True)
    suffix = Path(photo.filename or "image").suffix or ".jpg"

    import uuid

    file_id = uuid.uuid4().hex
    filename = f"{file_id}{suffix}"
    dest = ITEM_IMAGE_DIR / filename

    content = await photo.read()
    dest.write_bytes(content)

    return f"/uploads/upcycling_items/{filename}"


# ============================================================
# Mapping helpers
# ============================================================


def map_store_record(store: dict) -> UpcyclingStoreProfileOut:
    hero = store.get("hero_image_url")
    return UpcyclingStoreProfileOut(
        id=store["id"],
        user_id=store["user_id"],
        handle=store["handle"],
        display_name=store["display_name"],
        bio=store.get("bio"),
        hero_image_url=abs_media(hero) if hero else None,
        style_tags=store.get("style_tags") or [],
        created_at=store["created_at"],
        updated_at=store.get("updated_at"),
    )


def map_item_record(item: dict) -> UpcyclingItemOut:
    img = item.get("image_url")
    return UpcyclingItemOut(
        id=item["id"],
        store_id=item["store_id"],
        title=item["title"],
        description=item.get("description"),
        price=item.get("price"),
        currency=item.get("currency") or "AUD",
        size=item.get("size"),
        status=ItemStatus(item.get("status", "active")),
        tags=item.get("tags") or [],
        image_url=abs_media(img) if img else None,
        created_at=item["created_at"],
        updated_at=item["updated_at"],
    )


# ============================================================
# Routes – owner / dashboard
# ============================================================


@router.get(
    "/me",
    response_model=UpcyclingStoreDetailOut,
)
async def get_my_store(
    session: Session = Depends(session_dep),
    user_id: str = Depends(current_user_id),
):
    """
    Fetch the current user's store and all its items.
    """
    query = """
    MATCH (u:User {id: $user_id})-[:OWNS_UPCYCLING_STORE]->(s:UpcyclingStore)
    OPTIONAL MATCH (s)-[:HAS_ITEM]->(item:UpcyclingItem)
    WITH s, collect(item) AS items
    RETURN {
        id: s.id,
        user_id: s.user_id,
        handle: s.handle,
        display_name: s.display_name,
        bio: s.bio,
        hero_image_url: s.hero_image_url,
        style_tags: coalesce(s.style_tags, []),
        created_at: toString(s.created_at),
        updated_at: CASE WHEN s.updated_at IS NULL THEN NULL ELSE toString(s.updated_at) END
    } AS store,
    [i IN items | {
        id: i.id,
        store_id: i.store_id,
        title: i.title,
        description: i.description,
        price: i.price,
        currency: coalesce(i.currency, 'AUD'),
        size: i.size,
        status: coalesce(i.status, 'active'),
        tags: coalesce(i.tags, []),
        image_url: i.image_url,
        created_at: toString(i.created_at),
        updated_at: toString(i.updated_at)
    }] AS items
    """
    result = session.run(query, {"user_id": user_id})
    record = result.single()
    if not record:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="No upcycling store found for this user",
        )

    store_data = record["store"]
    items_data = record["items"] or []

    store = map_store_record(store_data)
    items = [map_item_record(i) for i in items_data]

    return UpcyclingStoreDetailOut(store=store, items=items)


@router.put(
    "/me",
    response_model=UpcyclingStoreProfileOut,
)
async def update_my_store(
    display_name: Optional[str] = Form(None),
    bio: Optional[str] = Form(None),
    style_tags: Optional[str] = Form(None),  # comma-separated
    hero_image: Optional[UploadFile] = File(None),
    session: Session = Depends(session_dep),
    user_id: str = Depends(current_user_id),
):
    """
    Update store profile for the current user.
    """
    hero_image_url: Optional[str] = None
    if hero_image is not None:
        hero_image_url = await save_store_hero(hero_image)

    tags_list: List[str] = []
    if style_tags:
        tags_list = [t.strip() for t in style_tags.split(",") if t.strip()]

    query = """
    MATCH (u:User {id: $user_id})-[:OWNS_UPCYCLING_STORE]->(s:UpcyclingStore)
    SET
      s.display_name = coalesce($display_name, s.display_name),
      s.bio = $bio,
      s.style_tags = $style_tags,
      s.hero_image_url = coalesce($hero_image_url, s.hero_image_url),
      s.updated_at = datetime()
    RETURN {
        id: s.id,
        user_id: s.user_id,
        handle: s.handle,
        display_name: s.display_name,
        bio: s.bio,
        hero_image_url: s.hero_image_url,
        style_tags: coalesce(s.style_tags, []),
        created_at: toString(s.created_at),
        updated_at: toString(s.updated_at)
    } AS store
    """

    result = session.run(
        query,
        {
            "user_id": user_id,
            "display_name": display_name,
            "bio": bio,
            "style_tags": tags_list,
            "hero_image_url": hero_image_url,
        },
    )
    record = result.single()
    if not record:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="No upcycling store found for this user",
        )

    store_data = record["store"]
    return map_store_record(store_data)


@router.post(
    "/items",
    response_model=UpcyclingItemOut,
    status_code=status.HTTP_201_CREATED,
)
async def create_item(
    title: str = Form(...),
    description: Optional[str] = Form(None),
    price: Optional[float] = Form(None),
    size: Optional[str] = Form(None),
    tags: Optional[str] = Form(None),  # comma-separated
    status_value: ItemStatus = Form(ItemStatus.active),
    image: Optional[UploadFile] = File(None),
    session: Session = Depends(session_dep),
    user_id: str = Depends(current_user_id),
):
    """
    Create a new UpcyclingItem under the current user's store.
    """
    image_url: Optional[str] = None
    if image is not None:
        image_url = await save_item_image(image)

    tags_list: List[str] = []
    if tags:
        tags_list = [t.strip() for t in tags.split(",") if t.strip()]

    query = """
    MATCH (u:User {id: $user_id})-[:OWNS_UPCYCLING_STORE]->(s:UpcyclingStore)
    WITH s
    CREATE (item:UpcyclingItem {
        id: randomUUID(),
        store_id: s.id,
        title: $title,
        description: $description,
        price: $price,
        currency: 'AUD',
        size: $size,
        status: $status,
        tags: $tags,
        image_url: $image_url,
        created_at: datetime(),
        updated_at: datetime()
    })
    MERGE (s)-[:HAS_ITEM]->(item)
    RETURN {
        id: item.id,
        store_id: item.store_id,
        title: item.title,
        description: item.description,
        price: item.price,
        currency: coalesce(item.currency, 'AUD'),
        size: item.size,
        status: item.status,
        tags: coalesce(item.tags, []),
        image_url: item.image_url,
        created_at: toString(item.created_at),
        updated_at: toString(item.updated_at)
    } AS item
    """

    result = session.run(
        query,
        {
            "user_id": user_id,
            "title": title,
            "description": description,
            "price": price,
            "size": size,
            "status": status_value.value,
            "tags": tags_list,
            "image_url": image_url,
        },
    )
    record = result.single()
    if not record:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="No upcycling store found for this user",
        )

    item_data = record["item"]
    return map_item_record(item_data)


@router.patch(
    "/items/{item_id}",
    response_model=UpcyclingItemOut,
)
async def update_item(
    item_id: str,
    title: Optional[str] = Form(None),
    description: Optional[str] = Form(None),
    price: Optional[float] = Form(None),
    size: Optional[str] = Form(None),
    tags: Optional[str] = Form(None),  # comma-separated
    status_value: Optional[ItemStatus] = Form(None),
    image: Optional[UploadFile] = File(None),
    session: Session = Depends(session_dep),
    user_id: str = Depends(current_user_id),
):
    """
    Update an existing item owned by the current user's store.
    """
    image_url: Optional[str] = None
    if image is not None:
        image_url = await save_item_image(image)

    tags_list: Optional[List[str]] = None
    if tags is not None:
        tags_list = [t.strip() for t in tags.split(",") if t.strip()]

    query = """
    MATCH (u:User {id: $user_id})-[:OWNS_UPCYCLING_STORE]->(s:UpcyclingStore)-[:HAS_ITEM]->(item:UpcyclingItem {id: $item_id})
    SET
      item.title = coalesce($title, item.title),
      item.description = coalesce($description, item.description),
      item.price = coalesce($price, item.price),
      item.size = coalesce($size, item.size),
      item.status = coalesce($status, item.status),
      item.tags = CASE WHEN $tags IS NULL THEN item.tags ELSE $tags END,
      item.image_url = coalesce($image_url, item.image_url),
      item.updated_at = datetime()
    RETURN {
        id: item.id,
        store_id: item.store_id,
        title: item.title,
        description: item.description,
        price: item.price,
        currency: coalesce(item.currency, 'AUD'),
        size: item.size,
        status: item.status,
        tags: coalesce(item.tags, []),
        image_url: item.image_url,
        created_at: toString(item.created_at),
        updated_at: toString(item.updated_at)
    } AS item
    """

    result = session.run(
        query,
        {
            "user_id": user_id,
            "item_id": item_id,
            "title": title,
            "description": description,
            "price": price,
            "size": size,
            "status": status_value.value if status_value else None,
            "tags": tags_list,
            "image_url": image_url,
        },
    )
    record = result.single()
    if not record:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Item not found or not owned by this user",
        )

    item_data = record["item"]
    return map_item_record(item_data)


@router.delete(
    "/items/{item_id}",
    status_code=status.HTTP_204_NO_CONTENT,
)
async def delete_item(
    item_id: str,
    session: Session = Depends(session_dep),
    user_id: str = Depends(current_user_id),
):
    """
    Delete an item owned by the current user's store.
    """
    query = """
    MATCH (u:User {id: $user_id})-[:OWNS_UPCYCLING_STORE]->(s:UpcyclingStore)-[r:HAS_ITEM]->(item:UpcyclingItem {id: $item_id})
    DETACH DELETE item
    RETURN count(item) AS deleted
    """
    result = session.run(
        query,
        {
            "user_id": user_id,
            "item_id": item_id,
        },
    )
    record = result.single()
    if not record or not record["deleted"]:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Item not found or not owned by this user",
        )
    return None


# ============================================================
# Routes – PUBLIC store detail (id or handle)
# ============================================================


@router.get(
    "/{store_id}",
    response_model=UpcyclingStoreDetailOut,
)
async def get_store_public(
    store_id: str,
    session: Session = Depends(session_dep),
):
    """
    Public: fetch an upcycling store + its items by id or handle.

    `store_id` can be:
      - the store's UUID (s.id)
      - the store's handle (case-insensitive)
    """
    query = """
    MATCH (s:UpcyclingStore)
    WHERE s.id = $store_id
       OR toLower(s.handle) = toLower($store_id)
    OPTIONAL MATCH (s)-[:HAS_ITEM]->(item:UpcyclingItem)
    WITH s, collect(item) AS items
    RETURN {
        id: s.id,
        user_id: s.user_id,
        handle: s.handle,
        display_name: s.display_name,
        bio: s.bio,
        hero_image_url: s.hero_image_url,
        style_tags: coalesce(s.style_tags, []),
        created_at: toString(s.created_at),
        updated_at: CASE WHEN s.updated_at IS NULL THEN NULL ELSE toString(s.updated_at) END
    } AS store,
    [i IN items | {
        id: i.id,
        store_id: i.store_id,
        title: i.title,
        description: i.description,
        price: i.price,
        currency: coalesce(i.currency, 'AUD'),
        size: i.size,
        status: coalesce(i.status, 'active'),
        tags: coalesce(i.tags, []),
        image_url: i.image_url,
        created_at: toString(i.created_at),
        updated_at: toString(i.updated_at)
    }] AS items
    """

    result = session.run(query, {"store_id": store_id})
    record = result.single()
    if not record:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Upcycling store not found",
        )

    store_data = record["store"]
    items_data = record["items"] or []

    store = map_store_record(store_data)
    items = [map_item_record(i) for i in items_data]

    return UpcyclingStoreDetailOut(store=store, items=items)


# ============================================================
# Orders – seller dashboard
# ============================================================


@router.get(
    "/me/orders",
    response_model=List[UpcyclingOrderOut],
)
async def get_my_store_orders(
    session: Session = Depends(session_dep),
    user_id: str = Depends(current_user_id),
):
    """
    List orders for the current user's upcycling store.
    Includes buyer name/email + delivery/pickup notes + tracking + status.
    Amount is returned in cents.
    """
    query = """
    MATCH (u:User {id: $user_id})-[:OWNS_UPCYCLING_STORE]->(s:UpcyclingStore)
    OPTIONAL MATCH (s)-[:RECEIVED_ORDER]->(o:UpcyclingOrder)
    OPTIONAL MATCH (item:UpcyclingItem {id: o.item_id})
    WITH o, item
    WHERE o IS NOT NULL
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
        created_at: toString(o.created_at),
        paid_at: CASE WHEN o.paid_at IS NULL THEN NULL ELSE toString(o.paid_at) END,
        last_status_update_at: CASE WHEN o.last_status_update_at IS NULL THEN NULL ELSE toString(o.last_status_update_at) END
    }) AS orders
    """
    record = session.run(query, {"user_id": user_id}).single()
    if not record:
        return []

    orders_data = record["orders"] or []
    # Cast status string -> enum where possible
    out: List[UpcyclingOrderOut] = []
    for o in orders_data:
        status_val = o.get("status") or "pending"
        try:
            o["status"] = UpcyclingOrderStatus(status_val)
        except ValueError:
            o["status"] = UpcyclingOrderStatus.pending
        out.append(UpcyclingOrderOut(**o))
    return out


@router.patch(
    "/me/orders/{order_id}",
    response_model=UpcyclingOrderOut,
)
async def update_my_store_order(
    order_id: str,
    body: UpcyclingOrderUpdateIn,
    session: Session = Depends(session_dep),
    user_id: str = Depends(current_user_id),
):
    """
    Seller updates order status / tracking.

    - Only the owner of the store receiving the order may update it.
    - Updates status / tracking fields.
    - Records last_status_update_at.
    - Sends SES emails to buyer & seller when status or tracking change.
    """
    cypher = """
    MATCH (owner:User {id: $user_id})-[:OWNS_UPCYCLING_STORE]->(s:UpcyclingStore)-[:RECEIVED_ORDER]->(o:UpcyclingOrder {id: $order_id})
    OPTIONAL MATCH (item:UpcyclingItem {id: o.item_id})
    OPTIONAL MATCH (buyer:User {id: o.buyer_id})
    WITH o, s, owner, item, buyer,
         o.status AS old_status,
         o.tracking_number AS old_tracking_number,
         o.tracking_carrier AS old_tracking_carrier,
         o.tracking_url AS old_tracking_url
    SET
      o.status = coalesce($status, o.status),
      o.tracking_number = $tracking_number,
      o.tracking_carrier = $tracking_carrier,
      o.tracking_url = $tracking_url,
      o.seller_note = $seller_note,
      o.last_status_update_at = datetime(),
      o.updated_at = datetime()
    WITH o, s, owner, item, buyer,
         old_status,
         old_tracking_number,
         old_tracking_carrier,
         old_tracking_url
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
        group_id: o.group_id,
        created_at: toString(o.created_at),
        paid_at: CASE
          WHEN o.paid_at IS NULL THEN NULL
          ELSE toString(o.paid_at)
        END,
        last_status_update_at: CASE
          WHEN o.last_status_update_at IS NULL THEN NULL
          ELSE toString(o.last_status_update_at)
        END
    } AS order,
    old_status,
    old_tracking_number,
    old_tracking_carrier,
    old_tracking_url,
    s.display_name AS store_name,
    owner.email AS owner_email,
    coalesce(o.buyer_email, buyer.email) AS buyer_email_effective
    """
    params = {
        "user_id": user_id,
        "order_id": order_id,
        "status": body.status.value if body.status else None,
        "tracking_number": body.tracking_number,
        "tracking_carrier": body.tracking_carrier,
        "tracking_url": body.tracking_url,
        "seller_note": body.seller_note,
    }

    record = session.run(cypher, params).single()
    if not record:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Order not found for this store or you don't own it.",
        )

    order_data = record["order"]
    status_val = order_data.get("status") or "pending"
    try:
        order_data["status"] = UpcyclingOrderStatus(status_val)
    except ValueError:
        order_data["status"] = UpcyclingOrderStatus.pending

    out = UpcyclingOrderOut(**order_data)

    # Decide whether to send emails (only if something meaningful changed)
    old_status = record["old_status"]
    old_tracking_number = record["old_tracking_number"]
    old_tracking_url = record["old_tracking_url"]

    status_changed = (old_status or "") != (out.status.value if out.status else "")
    tracking_changed = (
        (old_tracking_number or "") != (out.tracking_number or "")
        or (old_tracking_url or "") != (out.tracking_url or "")
    )

    if status_changed or tracking_changed or body.seller_note:
        store_name = record["store_name"] or "Ecodia Upcycling"
        owner_email = record["owner_email"]
        buyer_email = record["buyer_email_effective"]

        # Buyer email
        if buyer_email:
            buyer_subject = (
                f"Your Ecodia order update – {out.status.value.capitalize()}"
                if out.status
                else "Your Ecodia order update"
            )
            buyer_body = _format_order_status_email_body(
                heading="Your Ecodia upcycling order has been updated.",
                store_name=store_name,
                item_title=out.item_title,
                status=out.status.value if out.status else "pending",
                amount_cents=out.amount_cents,
                currency=out.currency,
                tracking_number=out.tracking_number,
                tracking_url=out.tracking_url,
                delivery_notes=out.delivery_notes,
                seller_note=body.seller_note,
            )
            _send_email(buyer_email, buyer_subject, buyer_body)

        # Seller email
        if owner_email:
            seller_subject = f"You updated an Ecodia order – {out.item_title}"
            seller_body = _format_order_status_email_body(
                heading="You just updated an Ecodia upcycling order.",
                store_name=store_name,
                item_title=out.item_title,
                status=out.status.value if out.status else "pending",
                amount_cents=out.amount_cents,
                currency=out.currency,
                tracking_number=out.tracking_number,
                tracking_url=out.tracking_url,
                delivery_notes=out.delivery_notes,
                seller_note=body.seller_note,
            )
            _send_email(owner_email, seller_subject, seller_body)

    return out


@router.patch(
    "/orders/{order_id}",
    response_model=UpcyclingOrderOut,
)
async def update_store_order(
    order_id: str,
    p: UpcyclingOrderUpdateIn,
    session: Session = Depends(session_dep),
    user_id: str = Depends(current_user_id),
):
    """
    Backwards-compatible alias for `/me/orders/{order_id}`.
    """
    return await update_my_store_order(
        order_id=order_id,
        body=p,
        session=session,
        user_id=user_id,
    )
