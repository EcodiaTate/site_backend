# api/eyba/places.py
from __future__ import annotations

from typing import Optional, List, Literal, Tuple, Dict, Any
from fastapi import APIRouter, Depends, Query, HTTPException
from pydantic import BaseModel, Field
from math import radians, sin, cos, atan2, sqrt
from neo4j import Session

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
    has_offers: bool = False  # active & in-stock offers with eco_price>0
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

    # IMPORTANT: no alias collisions anymore
    pledge: Optional[List[str]] = Query(None, description="multi: ?pledge=starter&pledge=leader"),
    pledge_csv: Optional[str] = Query(None, description="CSV legacy: ?pledge_csv=starter,leader"),

    industry: Optional[List[str]] = Query(None, description="multi: ?industry=retail&industry=hospitality"),
    industry_csv: Optional[str] = Query(None, description="CSV legacy: ?industry_csv=retail,hospitality"),

    area: Optional[List[str]] = Query(None, description="multi: ?area=cbd&area=suburb"),
    area_csv: Optional[str] = Query(None, description="CSV legacy: ?area_csv=cbd,suburb"),

    open_now: Optional[bool] = None,  # hint only – not enforced unless you store hours

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

    # ---- Build Cypher (no external service dependency) ----
    # Only visible businesses by default.
    # `has_offers` means: at least one active, in-stock offer with eco_price>0 and valid_until >= today (or null).
    cypher = """
      // base set
      MATCH (b:BusinessProfile)
      WHERE coalesce(b.visible_on_map, true) = true

      // optional spatial bbox
      WITH b
      WHERE
        $min_lat IS NULL OR $min_lng IS NULL OR $max_lat IS NULL OR $max_lng IS NULL OR
        (b.lat IS NOT NULL AND b.lng IS NOT NULL AND
         toFloat(b.lat) >= $min_lat AND toFloat(b.lat) <= $max_lat AND
         toFloat(b.lng) >= $min_lng AND toFloat(b.lng) <= $max_lng)

      // free-text search over a few properties (case-insensitive)
      WITH b
      WHERE
        $q IS NULL OR
        toLower(coalesce(b.name,'')) CONTAINS toLower($q) OR
        toLower(coalesce(b.address,'')) CONTAINS toLower($q) OR
        ($q IN coalesce([x IN coalesce(b.tags,[]) | toLower(x)], []))

      // multi-filters
      WITH b
      WHERE
        $pledges IS NULL OR toLower(coalesce(b.pledge_tier,'')) IN [x IN $pledges | toLower(x)]
      WITH b
      WHERE
        $industries IS NULL OR toLower(coalesce(b.industry_group,'')) IN [x IN $industries | toLower(x)]
      WITH b
      WHERE
        $areas IS NULL OR toLower(coalesce(b.area,'')) IN [x IN $areas | toLower(x)]

      // compute has_offers flag
      OPTIONAL MATCH (o:Offer)-[:OF]->(b)
      WHERE coalesce(o.status,'active')='active'
        AND toInteger(coalesce(o.eco_price,0)) > 0
        AND (o.stock IS NULL OR toInteger(o.stock) > 0)
        AND (o.valid_until IS NULL OR date(o.valid_until) >= date())

      WITH b, count(o) AS offer_count

      // collect rows for post-sorting/paging in Python (distance calc)
      RETURN
        b.id AS business_id,
        coalesce(b.name,'') AS name,
        toFloat(b.lat) AS lat,
        toFloat(b.lng) AS lng,
        toLower(coalesce(b.pledge_tier,'')) AS pledge_tier,
        toLower(coalesce(b.industry_group,'')) AS industry_group,
        toLower(coalesce(b.area,'')) AS area_type,
        coalesce(b.address,'') AS address,
        coalesce(b.website,'') AS url,
        coalesce(b.tags, []) AS tags,
        (offer_count > 0) AS has_offers
    """

    recs = s.run(
        cypher,
        {
            "q": q,
            "pledges": pledges,
            "industries": industries,
            "areas": areas,
            "min_lat": bbox_tuple[0] if bbox_tuple else None,
            "min_lng": bbox_tuple[1] if bbox_tuple else None,
            "max_lat": bbox_tuple[2] if bbox_tuple else None,
            "max_lng": bbox_tuple[3] if bbox_tuple else None,
        },
    )

    rows = [r.data() for r in recs]  # list[dict]
    items: List[PlaceOut] = []
    for r in rows:
        # Skip rows missing essentials
        if not r.get("name") or r.get("lat") is None or r.get("lng") is None:
            continue

        # Pydantic expects a separate "id"
        item = PlaceOut(
            id=r["business_id"],
            business_id=r["business_id"],
            name=r["name"],
            lat=float(r["lat"]),
            lng=float(r["lng"]),
            pledge_tier=(r["pledge_tier"] or "starter"),  # default for safety
            industry_group=(r["industry_group"] or None),
            area_type=(r["area_type"] or None),
            has_offers=bool(r.get("has_offers", False)),
            address=(r["address"] or None),
            url=(r["url"] or None),
            tags=r.get("tags") or [],
            open_now=None,
            distance_km=None,
        )
        items.append(item)

    # Sorting in Python (to support distance)
    if sort == "distance" and lat is not None and lng is not None:
        for it in items:
            try:
                it.distance_km = round(_haversine_km(lat, lng, it.lat, it.lng), 2)
            except Exception:
                it.distance_km = None
        items.sort(key=lambda x: (x.distance_km if x.distance_km is not None else 1e9, x.name.lower()))
    elif sort == "name":
        items.sort(key=lambda x: x.name.lower())
    elif sort == "tier":
        items.sort(key=lambda x: (-_tier_rank(x.pledge_tier), x.name.lower()))

    total = len(items)

    # Paging (1-based page)
    start = (page - 1) * page_size
    end = start + page_size
    page_items = items[start:end]

    return PlacesResponse(
        items=page_items,
        total=total,
        page=page,
        page_size=page_size,
    )
