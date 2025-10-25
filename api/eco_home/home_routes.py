# systems/api/routers/home.py
from fastapi import APIRouter, Depends, HTTPException, Request
from datetime import datetime
from typing import List, Dict, Any
from site_backend.core.neo_driver import session_dep
from pydantic import BaseModel, Field
from urllib.parse import urlparse
import json

router = APIRouter(prefix="/users", tags=["home"])

# ---- Pydantic models ----
ItemType = str  # or use Literal[...] if you want strict types


class HomeItem(BaseModel):
    id: str
    type: ItemType
    x: float
    y: float
    w: float | None = None
    h: float | None = None
    z: int | None = None
    skin: str | None = None
    meta: Dict[str, Any] = Field(default_factory=dict)


class HomeLayout(BaseModel):
    id: str  # user id
    bg: str
    items: List[HomeItem] = Field(default_factory=list)
    version: int = 1
    updated_at: datetime


def _clamp01(v: float) -> float:
    return max(0.0, min(1.0, float(v)))


# ---- Helpers ----
DEFAULT_FRONTEND_BG = "/img/my-eco-home-bg.png"


def _to_frontend_img_path(bg: str | None) -> str:
    """
    Always return a relative path that Next.js will serve from /public.
    - If bg is an absolute URL (http/https), strip the origin and return its path.
    - If bg is empty, return the default.
    - Ensure it starts with '/'.
    """
    if not bg:
        return DEFAULT_FRONTEND_BG

    if bg.startswith("http://") or bg.startswith("https://"):
        try:
            p = urlparse(bg)
            path = p.path or DEFAULT_FRONTEND_BG
            return path if path.startswith("/") else f"/{path}"
        except Exception:
            return DEFAULT_FRONTEND_BG

    return bg if bg.startswith("/") else f"/{bg}"


def _record_to_layout(user_id: str, hl_props: dict | None, items: list) -> HomeLayout:
    # fallback if no layout found
    if not hl_props:
        return HomeLayout(
            id=user_id,
            bg=DEFAULT_FRONTEND_BG,
            items=[],
            version=1,
            updated_at=datetime.utcnow(),
        )

    # Parse item list
    parsed_items: list[HomeItem] = []
    for it in items or []:
        props = dict(it)
        meta_json = props.pop("meta_json", None)
        meta = {}
        if meta_json:
            try:
                meta = json.loads(meta_json)
            except Exception:
                meta = {}
        parsed_items.append(HomeItem(**props, meta=meta))

    # Normalise background to a frontend-served path
    bg = _to_frontend_img_path(hl_props.get("bg", DEFAULT_FRONTEND_BG))

    return HomeLayout(
        id=hl_props.get("id", user_id),
        bg=bg,
        items=parsed_items,
        version=int(hl_props.get("version", 1)),
        updated_at=datetime.fromisoformat(hl_props["updated_at"])
        if isinstance(hl_props.get("updated_at"), str)
        else datetime.utcnow(),
    )


# ---- Routes ----
@router.get("/{user_id}/home_layout", response_model=HomeLayout)
def get_home_layout(user_id: str, request: Request, session=Depends(session_dep)):
    q = """
    MATCH (u:User {id:$uid})-[:HAS_HOME_LAYOUT]->(hl:HomeLayout)
    OPTIONAL MATCH (hl)-[:HAS_ITEM]->(hi:HomeItem)
    RETURN hl AS hl, collect(hi) AS items
    """
    rec = session.run(q, uid=user_id).single()
    if not rec:
        return _record_to_layout(user_id, None, [])
    hl_node = rec["hl"]
    items_nodes = rec["items"]
    hl_props = dict(hl_node) if hl_node else None
    items_props = [dict(n) for n in items_nodes] if items_nodes else []
    return _record_to_layout(user_id, hl_props, items_props)


@router.put("/{user_id}/home_layout", response_model=HomeLayout)
def put_home_layout(user_id: str, layout: HomeLayout, request: Request, session=Depends(session_dep)):
    if layout.id != user_id:
        raise HTTPException(status_code=400, detail="layout.id must match user_id")

    # Always store bg as a frontend path (relative), regardless of input
    store_bg = _to_frontend_img_path(layout.bg)

    # clamp and prep items
    safe_items = []
    for it in layout.items:
        safe_items.append(
            {
                "id": it.id,
                "type": it.type,
                "x": _clamp01(it.x),
                "y": _clamp01(it.y),
                "w": it.w,
                "h": it.h,
                "z": it.z,
                "skin": it.skin,
                "meta_json": json.dumps(it.meta or {}),
            }
        )

    # Upsert layout node
    q_layout = """
    MERGE (u:User {id:$uid})
    MERGE (u)-[:HAS_HOME_LAYOUT]->(hl:HomeLayout {id:$uid})
    SET hl.bg=$bg,
        hl.version=$version,
        hl.updated_at=$updated_at
    RETURN hl
    """
    rec = session.run(
        q_layout,
        uid=user_id,
        bg=store_bg,
        version=layout.version,
        updated_at=datetime.utcnow().isoformat(),
    ).single()
    _ = rec["hl"]

    # Delete old items
    q_delete_items = """
    MATCH (hl:HomeLayout {id:$uid})-[r:HAS_ITEM]->(old:HomeItem)
    DELETE r, old
    """
    session.run(q_delete_items, uid=user_id)

    # Create new items
    if safe_items:
        q_create_items = """
        MATCH (hl:HomeLayout {id:$uid})
        UNWIND $items AS it
        MERGE (hi:HomeItem {id: it.id})
        SET hi.type = it.type,
            hi.x = it.x,
            hi.y = it.y,
            hi.w = it.w,
            hi.h = it.h,
            hi.z = it.z,
            hi.skin = it.skin,
            hi.meta_json = it.meta_json
        MERGE (hl)-[:HAS_ITEM]->(hi)
        """
        session.run(q_create_items, uid=user_id, items=safe_items)

    # Return updated layout (normalised to frontend path again)
    q_return = """
    MATCH (hl:HomeLayout {id:$uid})
    OPTIONAL MATCH (hl)-[:HAS_ITEM]->(hi:HomeItem)
    RETURN hl AS hl, collect(hi) AS items
    """
    rec2 = session.run(q_return, uid=user_id).single()
    hl_props = dict(rec2["hl"])
    items_props = [dict(n) for n in rec2["items"]]
    return _record_to_layout(user_id, hl_props, items_props)
