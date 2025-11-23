# site_backend/api/routers/market_apply.py
from __future__ import annotations

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

router = APIRouter(
    prefix="/market/apply",
    tags=["market_apply"],
)

# ============================================================
# Models
# ============================================================


class UpcycleApplicationStatus(str, Enum):
    pending = "pending"
    approved = "approved"
    rejected = "rejected"


class UpcycleApplicationOut(BaseModel):
    id: str
    user_id: str
    full_name: str
    instagram_handle: Optional[str] = None
    location: Optional[str] = None
    style_notes: Optional[str] = None
    primary_style: Optional[str] = None
    price_range: Optional[str] = None
    shipping_options: Optional[str] = None
    # NOTE: relative URL (e.g. "/uploads/..."), so plain str not HttpUrl
    photo_url: Optional[str] = None
    status: UpcycleApplicationStatus
    created_at: str
    updated_at: str


class UpcycleApplicationList(BaseModel):
    items: List[UpcycleApplicationOut]
    total: int


class UpcyclingStoreOut(BaseModel):
    id: str
    user_id: str
    handle: str
    display_name: str
    created_at: str


# ============================================================
# Helpers: file storage + cypher mapping
# ============================================================
from pathlib import Path
from site_backend.core.paths import UPLOAD_ROOT  # NEW

# All upcycling application photos go under: <UPLOAD_ROOT>/upcycling_applications
APPLICATION_UPLOAD_DIR = UPLOAD_ROOT / "upcycling_applications"


async def save_application_photo(photo: UploadFile) -> str:
    """
    Save an uploaded photo for an application and return a web path
    under /uploads, using the shared UPLOAD_ROOT (same root as avatars).
    """
    APPLICATION_UPLOAD_DIR.mkdir(parents=True, exist_ok=True)

    suffix = Path(photo.filename or "image").suffix or ".jpg"
    import uuid

    file_id = uuid.uuid4().hex
    filename = f"{file_id}{suffix}"
    dest = APPLICATION_UPLOAD_DIR / filename

    content = await photo.read()
    dest.write_bytes(content)

    # This is served by FastAPI at /uploads from UPLOAD_ROOT
    return f"/uploads/upcycling_applications/{filename}"



def map_app_record(record) -> UpcycleApplicationOut:
    app = record["app"]
    return UpcycleApplicationOut(
        id=app["id"],
        user_id=app["user_id"],
        full_name=app.get("full_name") or "",
        instagram_handle=app.get("instagram_handle"),
        location=app.get("location"),
        style_notes=app.get("style_notes"),
        primary_style=app.get("primary_style"),
        price_range=app.get("price_range"),
        shipping_options=app.get("shipping_options"),
        photo_url=app.get("photo_url"),
        status=UpcycleApplicationStatus(app["status"]),
        created_at=app["created_at"],
        updated_at=app["updated_at"],
    )


async def require_admin(
    uid: str = Depends(current_user_id),
    session: Session = Depends(session_dep),
) -> str:
    """
    Simple admin gate: user must have role 'admin' or is_admin = true.
    Adjust to match your actual User schema if needed.
    """
    rec = session.run(
        "MATCH (u:User {id: $id}) RETURN u.role AS role, u.is_admin AS is_admin",
        {"id": uid},
    ).single()

    if not rec:
        # Auth says they exist, but graph doesn't – treat as unauthorized
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Unauthorized")

    role = rec.get("role")
    is_admin_flag = rec.get("is_admin")
    if not (is_admin_flag or role == "admin"):
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Admin only")

    return uid


# ============================================================
# Routes
# ============================================================


@router.post(
    "",
    response_model=UpcycleApplicationOut,
    status_code=status.HTTP_201_CREATED,
)
async def apply_to_sell(
    full_name: str = Form(...),
    instagram_handle: Optional[str] = Form(None),
    location: Optional[str] = Form(None),
    style_notes: Optional[str] = Form(None),
    primary_style: Optional[str] = Form(None),
    price_range: Optional[str] = Form(None),
    shipping_options: Optional[str] = Form(None),
    photo: Optional[UploadFile] = File(None),
    session: Session = Depends(session_dep),
    user_id: str = Depends(current_user_id),
):
    """
    Create / replace a seller application for the current user.
    If an existing application exists, it is marked as 'superseded'.
    """
    photo_url: Optional[str] = None
    if photo is not None:
        photo_url = await save_application_photo(photo)

    query = """
    MATCH (u:User {id: $user_id})
    WITH u
    OPTIONAL MATCH (u)-[:SUBMITTED_UPCYCLE_APP]->(existing:UpcycleApplication)
    FOREACH (_ IN CASE WHEN existing IS NOT NULL THEN [1] ELSE [] END |
        SET existing.status = 'superseded',
            existing.updated_at = datetime()
    )
    WITH u
    CREATE (app:UpcycleApplication {
        id: randomUUID(),
        user_id: $user_id,
        full_name: $full_name,
        instagram_handle: $instagram_handle,
        location: $location,
        style_notes: $style_notes,
        primary_style: $primary_style,
        price_range: $price_range,
        shipping_options: $shipping_options,
        photo_url: $photo_url,
        status: 'pending',
        created_at: datetime(),
        updated_at: datetime()
    })
    MERGE (u)-[:SUBMITTED_UPCYCLE_APP]->(app)
    RETURN {
        id: app.id,
        user_id: app.user_id,
        full_name: app.full_name,
        instagram_handle: app.instagram_handle,
        location: app.location,
        style_notes: app.style_notes,
        primary_style: app.primary_style,
        price_range: app.price_range,
        shipping_options: app.shipping_options,
        photo_url: app.photo_url,
        status: app.status,
        created_at: toString(app.created_at),
        updated_at: toString(app.updated_at)
    } AS app
    """

    params = {
        "user_id": user_id,
        "full_name": full_name,
        "instagram_handle": instagram_handle,
        "location": location,
        "style_notes": style_notes,
        "primary_style": primary_style,
        "price_range": price_range,
        "shipping_options": shipping_options,
        "photo_url": photo_url,
    }

    result = session.run(query, params)
    record = result.single()
    if not record:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to create application",
        )
    return map_app_record(record)


@router.get(
    "/applications",
    response_model=UpcycleApplicationList,
    dependencies=[Depends(require_admin)],
)
async def list_applications(
    session: Session = Depends(session_dep),
    status_filter: Optional[UpcycleApplicationStatus] = None,
    skip: int = 0,
    limit: int = 50,
):
    """
    Admin: list upcycling applications.
    """
    status_clause = ""
    params: dict = {"skip": skip, "limit": limit}
    if status_filter:
        status_clause = "WHERE app.status = $status"
        params["status"] = status_filter.value

    query = f"""
    MATCH (app:UpcycleApplication)
    {status_clause}
    WITH app
    ORDER BY app.created_at DESC
    SKIP $skip LIMIT $limit
    RETURN collect({{
        id: app.id,
        user_id: app.user_id,
        full_name: app.full_name,
        instagram_handle: app.instagram_handle,
        location: app.location,
        style_notes: app.style_notes,
        primary_style: app.primary_style,
        price_range: app.price_range,
        shipping_options: app.shipping_options,
        photo_url: app.photo_url,
        status: app.status,
        created_at: toString(app.created_at),
        updated_at: toString(app.updated_at)
    }}) AS apps,
    size([a IN collect(app) | 1]) AS total
    """

    result = session.run(query, params)
    record = result.single()
    if not record:
        return UpcycleApplicationList(items=[], total=0)

    apps_raw = record["apps"] or []
    items = [
        UpcycleApplicationOut(
            id=a["id"],
            user_id=a["user_id"],
            full_name=a.get("full_name") or "",
            instagram_handle=a.get("instagram_handle"),
            location=a.get("location"),
            style_notes=a.get("style_notes"),
            primary_style=a.get("primary_style"),
            price_range=a.get("price_range"),
            shipping_options=a.get("shipping_options"),
            photo_url=a.get("photo_url"),
            status=UpcycleApplicationStatus(a["status"]),
            created_at=a["created_at"],
            updated_at=a["updated_at"],
        )
        for a in apps_raw
    ]
    return UpcycleApplicationList(items=items, total=record["total"])


@router.post(
    "/applications/{app_id}/approve",
    response_model=UpcyclingStoreOut,
    dependencies=[Depends(require_admin)],
)
async def approve_application(
    app_id: str,
    session: Session = Depends(session_dep),
):
    """
    Admin: approve an application and create an UpcyclingStore node linked to the user.
    """
    query = """
    MATCH (app:UpcycleApplication {id: $app_id})
    MATCH (u:User {id: app.user_id})
    SET app.status = 'approved',
        app.updated_at = datetime()
    WITH app, u
    MERGE (store:UpcyclingStore {id: app.id})
      ON CREATE SET
        store.user_id = app.user_id,
        store.handle = coalesce(app.instagram_handle, toLower(replace(app.full_name, ' ', ''))),
        store.display_name = app.full_name,
        store.created_at = datetime()
      ON MATCH SET
        store.display_name = app.full_name
    MERGE (u)-[:OWNS_UPCYCLING_STORE]->(store)
    RETURN {
        id: store.id,
        user_id: store.user_id,
        handle: store.handle,
        display_name: store.display_name,
        created_at: toString(store.created_at)
    } AS store
    """

    result = session.run(query, {"app_id": app_id})
    record = result.single()
    if not record:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Application not found",
        )

    store = record["store"]
    return UpcyclingStoreOut(**store)


@router.post(
    "/applications/{app_id}/reject",
    response_model=UpcycleApplicationOut,
    dependencies=[Depends(require_admin)],
)
async def reject_application(
    app_id: str,
    session: Session = Depends(session_dep),
):
    """
    Admin: reject application (no store created).
    """
    query = """
    MATCH (app:UpcycleApplication {id: $app_id})
    SET app.status = 'rejected',
        app.updated_at = datetime()
    RETURN {
        id: app.id,
        user_id: app.user_id,
        full_name: app.full_name,
        instagram_handle: app.instagram_handle,
        location: app.location,
        style_notes: app.style_notes,
        primary_style: app.primary_style,
        price_range: app.price_range,
        shipping_options: app.shipping_options,
        photo_url: app.photo_url,
        status: app.status,
        created_at: toString(app.created_at),
        updated_at: toString(app.updated_at)
    } AS app
    """

    result = session.run(query, {"app_id": app_id})
    record = result.single()
    if not record:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Application not found",
        )
    return map_app_record(record)
