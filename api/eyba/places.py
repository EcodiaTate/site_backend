# api/routers/eyba_places.py
from __future__ import annotations

from typing import Optional, List, Literal, Tuple, Dict, Any
from fastapi import APIRouter, Depends, Query, HTTPException
from pydantic import BaseModel, Field
from math import radians, sin, cos, atan2, sqrt
from neo4j import Session
from inspect import signature

from site_backend.core.neo_driver import session_dep

router = APIRouter(prefix="/eyba", tags=["eyba"])

AreaType = Literal["cbd", "suburb", "regional"]
PledgeTier = Literal["starter", "builder", "leader"]

# -------- Models --------
class PlaceOut(BaseModel):
    id: str
    business_id: str
    name: str
    lat: float
    lng: float
    pledge_tier: PledgeTier
    industry_group: Optional[str] = None
    area_type: Optional[AreaType] = None
    has_offers: bool = False

    # Optional enrichments
    address: Optional[str] = None
    url: Optional[str] = None
    tags: List[str] = Field(default_factory=list)
    open_now: Optional[bool] = None

    # Calculated
    distance_km: Optional[float] = None

class PlacesResponse(BaseModel):
    items: List[PlaceOut]
    total: int
    page: int
    page_size: int

# -------- Helpers --------
def _haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    R = 6371.0
    phi1 = radians(lat1); phi2 = radians(lat2)
    dphi = radians(lat2 - lat1)
    dl   = radians(lon2 - lon1)
    a = sin(dphi / 2) ** 2 + cos(phi1) * cos(phi2) * sin(dl / 2) ** 2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))
    return R * c

def _tier_rank(t: Optional[str]) -> int:
    return {"starter": 1, "builder": 2, "leader": 3}.get((t or "").lower(), 0)

def _parse_bbox_str(bbox: Optional[str]) -> Optional[Tuple[float, float, float, float]]:
    if not bbox:
        return None
    try:
        parts = [float(x) for x in bbox.split(",")]
        if len(parts) != 4: raise ValueError
        min_lat, min_lng, max_lat, max_lng = parts
        if min_lat >= max_lat or min_lng >= max_lng: raise ValueError
        return (min_lat, min_lng, max_lat, max_lng)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid bbox param. Use minLat,minLng,maxLat,maxLng")

def _merge_multi_and_csv(multi: Optional[List[str]], csv: Optional[str]) -> Optional[List[str]]:
    out: List[str] = []
    if multi: out.extend([s for s in multi if s is not None])
    if csv:   out.extend([s.strip() for s in csv.split(",") if s.strip()])
    return out or None

def _call_service_safely(svc, s: Session, kwargs: Dict[str, Any]):
    sig = signature(svc)
    accepted = set(sig.parameters.keys())
    filtered = {k: v for k, v in kwargs.items() if k in accepted and v is not None}
    return svc(s, **filtered)

# -------- Endpoint --------
@router.get("/places", response_model=PlacesResponse)
def get_places(
    s: Session = Depends(session_dep),

    # pagination
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=200),

    # bbox
    bbox: Optional[str] = Query(default=None, description="minLat,minLng,maxLat,maxLng"),
    min_lat: Optional[float] = None,
    min_lng: Optional[float] = None,
    max_lat: Optional[float] = None,
    max_lng: Optional[float] = None,

    # search + filters
    q: Optional[str] = Query(None, description="free text: name/address/tags"),
    pledge: Optional[List[str]] = Query(None, description="multi: ?pledge=starter&pledge=leader"),
    pledge_csv: Optional[str] = Query(None, alias="pledge", description="CSV legacy"),
    industry: Optional[List[str]] = Query(None, description="multi: ?industry=retail&industry=hospitality"),
    industry_csv: Optional[str] = Query(None, alias="industry", description="CSV legacy"),
    area: Optional[List[str]] = Query(None, description="multi: ?area=cbd&area=suburb"),
    area_csv: Optional[str] = Query(None, alias="area", description="CSV legacy"),

    # optional hours hint
    open_now: Optional[bool] = None,

    # sorting + distance
    sort: Optional[str] = Query("name", pattern="^(distance|name|tier)$"),
    lat: Optional[float] = None,
    lng: Optional[float] = None,
):
    bbox_tuple = _parse_bbox_str(bbox)
    if not bbox_tuple and (min_lat is not None and min_lng is not None and max_lat is not None and max_lng is not None):
        if min_lat >= max_lat or min_lng >= max_lng:
            raise HTTPException(status_code=400, detail="Invalid bbox min/max values.")
        bbox_tuple = (min_lat, min_lng, max_lat, max_lng)

    pledges    = _merge_multi_and_csv(pledge, pledge_csv)
    industries = _merge_multi_and_csv(industry, industry_csv)
    areas      = _merge_multi_and_csv(area, area_csv)

    svc_kwargs: Dict[str, Any] = dict(
        min_lat=bbox_tuple[0] if bbox_tuple else None,
        min_lng=bbox_tuple[1] if bbox_tuple else None,
        max_lat=bbox_tuple[2] if bbox_tuple else None,
        max_lng=bbox_tuple[3] if bbox_tuple else None,
        q=q,
        pledges=pledges,
        industries=industries,
        areas=areas,
        open_now=open_now,
        sort=sort,
        lat=lat, lng=lng, user_lat=lat, user_lng=lng,
        page=page, page_size=page_size,
    )
    from site_backend.api.services.neo_places import list_places_with_offer_flag
    raw = _call_service_safely(list_places_with_offer_flag, s, svc_kwargs)

    if isinstance(raw, dict):
        raw_items = raw.get("items", []) or []
        items: List[PlaceOut] = []
        for it in raw_items:
            items.append(PlaceOut(**it) if isinstance(it, dict) else it)
        total = int(raw.get("total", len(items)))
        data = PlacesResponse(items=items, total=total, page=page, page_size=page_size)
    else:
        data = raw  # type: ignore

    if sort == "distance" and lat is not None and lng is not None:
        for i, p in enumerate(data.items):
            if p.distance_km is None:
                try:
                    data.items[i].distance_km = round(_haversine_km(lat, lng, p.lat, p.lng), 2)
                except Exception:
                    pass
        data.items.sort(key=lambda x: (x.distance_km if x.distance_km is not None else 1e9, (x.name or "").lower()))
    elif sort == "name":
        data.items.sort(key=lambda x: (x.name or "").lower())
    elif sort == "tier":
        data.items.sort(key=lambda x: (-_tier_rank(x.pledge_tier), (x.name or "").lower()))

    if data.total is None:
        data.total = len(data.items)
    data.page = page
    data.page_size = page_size
    return data
