# site_backend/api/routers/market_catalogue.py
from __future__ import annotations

from typing import List, Optional

from fastapi import APIRouter, Depends, Query
from neo4j import Session  # type: ignore
from pydantic import BaseModel

from site_backend.core.neo_driver import session_dep
from site_backend.core.urls import abs_media

router = APIRouter(
    prefix="/market/catalogue",
    tags=["market_catalogue"],
)


# ============================================================
# Models
# ============================================================

class CatalogueStoreOut(BaseModel):
    id: str
    handle: str
    display_name: str
    bio: Optional[str] = None
    hero_image_url: Optional[str] = None
    style_tags: List[str]
    item_count: int
    created_at: str


class CatalogueStoreList(BaseModel):
    items: List[CatalogueStoreOut]
    total: int


# ============================================================
# Routes
# ============================================================

@router.get(
    "",
    response_model=CatalogueStoreList,
)
async def list_upcycling_stores(
    session: Session = Depends(session_dep),
    q: Optional[str] = Query(None, description="Keyword search"),
    tag: List[str] = Query(default=[], description="Filter by style tags"),
    sort: str = Query(
        "newest",
        pattern="^(newest|name|random)$",
        description="Sort mode: newest | name | random",
    ),
    skip: int = Query(0, ge=0),
    limit: int = Query(24, ge=1, le=100),
):
    """
    Public catalogue listing of upcycling stores.

    Search:
      - display_name, handle, bio, style_tags (case-insensitive substring)

    Filters:
      - tag=... (can be repeated) matches any of the store's style_tags

    Sorting:
      - newest: created_at DESC
      - name: display_name A–Z
      - random: randomized order
    """
    q_norm = (q or "").strip().lower()
    tags_norm = [t.strip() for t in tag if t.strip()]

    params = {
        "q": q_norm if q_norm else None,
        "tags": tags_norm,
        "sort": sort,
        "skip": skip,
        "limit": limit,
    }

    query = """
    MATCH (s:UpcyclingStore)
    OPTIONAL MATCH (s)-[:HAS_ITEM]->(item:UpcyclingItem)
    WITH s, count(item) AS item_count
    WHERE
      (
        $q IS NULL OR
        toLower(s.display_name) CONTAINS $q OR
        toLower(s.handle) CONTAINS $q OR
        (s.bio IS NOT NULL AND toLower(s.bio) CONTAINS $q) OR
        any(tag IN coalesce(s.style_tags, []) WHERE toLower(tag) CONTAINS $q)
      )
      AND
      (
        size($tags) = 0 OR
        any(t IN coalesce(s.style_tags, []) WHERE t IN $tags)
      )
    WITH s, item_count,
         CASE $sort WHEN 'name' THEN toLower(s.display_name) ELSE '' END AS sort_name,
         CASE $sort WHEN 'newest' THEN s.created_at ELSE datetime({epochMillis: 0}) END AS sort_created
    ORDER BY
      CASE $sort WHEN 'name' THEN sort_name END ASC,
      CASE $sort WHEN 'newest' THEN sort_created END DESC,
      CASE $sort WHEN 'random' THEN rand() ELSE 0 END
    WITH collect({
      id: s.id,
      handle: s.handle,
      display_name: s.display_name,
      bio: s.bio,
      hero_image_url: s.hero_image_url,
      style_tags: coalesce(s.style_tags, []),
      item_count: item_count,
      created_at: toString(s.created_at)
    }) AS stores
    RETURN
      stores[$skip..($skip + $limit)] AS items,
      size(stores) AS total
    """

    result = session.run(query, params)
    record = result.single()
    if not record:
        return CatalogueStoreList(items=[], total=0)

    raw_items = record["items"] or []
    total = int(record["total"] or 0)

    items: List[CatalogueStoreOut] = []
    for s in raw_items:
        hero = s.get("hero_image_url")
        items.append(
            CatalogueStoreOut(
                id=s["id"],
                handle=s["handle"],
                display_name=s["display_name"],
                bio=s.get("bio"),
                hero_image_url=abs_media(hero) if hero else None,
                style_tags=s.get("style_tags") or [],
                item_count=int(s.get("item_count") or 0),
                created_at=s["created_at"],
            )
        )

    return CatalogueStoreList(items=items, total=total)
