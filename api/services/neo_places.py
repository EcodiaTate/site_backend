# api/services/neo_places.py
from __future__ import annotations
from typing import Any, Dict, List, Optional
from neo4j import Session

VALID_SORTS = {"distance", "name", "tier"}

def list_places_with_offer_flag(
    s: Session,
    *,
    min_lat: Optional[float] = None,
    min_lng: Optional[float] = None,
    max_lat: Optional[float] = None,
    max_lng: Optional[float] = None,
    q: Optional[str] = None,
    pledges: Optional[List[str]] = None,      # ["starter","builder","leader"]
    industries: Optional[List[str]] = None,   # ["hospitality","retail",...]
    areas: Optional[List[str]] = None,        # ["cbd","suburb","regional"]
    sort: Optional[str] = None,               # "distance" | "name" | "tier"
    lat: Optional[float] = None,              # user lat for distance
    lng: Optional[float] = None,              # user lng for distance
    page: int = 1,
    page_size: int = 50,
) -> Dict[str, Any]:

    if sort not in VALID_SORTS and sort is not None:
        sort = "name"
    sort = sort or "name"

    params: Dict[str, Any] = {
        "skip": max(0, (page - 1) * page_size),
        "limit": min(200, page_size),
        "q": q,
        "pledges": pledges or [],
        "industries": industries or [],
        "areas": areas or [],
        "ulat": lat,
        "ulng": lng,
        "min_lat": min_lat,
        "min_lng": min_lng,
        "max_lat": max_lat,
        "max_lng": max_lng,
    }

    # Filters for each shape
    filters_p: List[str] = []  # matching (p:Place)-[:OF]->(b)
    filters_b: List[str] = []  # matching (b:BusinessProfile) with lat/lng

    # BBox
    if None not in (min_lat, min_lng, max_lat, max_lng):
        filters_p += [
            "p.lat IS NOT NULL", "p.lng IS NOT NULL",
            "p.lat >= $min_lat", "p.lat <= $max_lat",
            "p.lng >= $min_lng", "p.lng <= $max_lng",
        ]
        filters_b += [
            "b.lat IS NOT NULL", "b.lng IS NOT NULL",
            "b.lat >= $min_lat", "b.lat <= $max_lat",
            "b.lng >= $min_lng", "b.lng <= $max_lng",
        ]

    # Search by name (place or business)
    if q:
        filters_p += ["(toLower(p.name) CONTAINS toLower($q) OR toLower(coalesce(b.name,'')) CONTAINS toLower($q))"]
        filters_b += ["toLower(coalesce(b.name,'')) CONTAINS toLower($q)"]

    # Multi-filters
    if pledges:
        filters_p += ["coalesce(b.pledge,'') IN $pledges"]
        filters_b += ["coalesce(b.pledge,'') IN $pledges"]
    if industries:
        filters_p += ["coalesce(b.industry_group,'') IN $industries"]
        filters_b += ["coalesce(b.industry_group,'') IN $industries"]
    if areas:
        filters_p += ["coalesce(b.area,'') IN $areas"]
        filters_b += ["coalesce(b.area,'') IN $areas"]

    where_p = ("WHERE " + " AND ".join(filters_p)) if filters_p else ""
    where_b = ("WHERE " + " AND ".join(filters_b)) if filters_b else ""

    # ORDER BY clause
    if sort == "distance":
        order_clause = "ORDER BY (distance_km IS NULL) ASC, distance_km ASC, toLower(place.name) ASC"
    elif sort == "tier":
        order_clause = (
            "ORDER BY CASE place.pledge_tier "
            "WHEN 'leader' THEN 0 WHEN 'builder' THEN 1 ELSE 2 END, "
            "toLower(place.name) ASC"
        )
    else:
        order_clause = "ORDER BY toLower(place.name) ASC"

    cypher = f"""
    // UNION two shapes then collapse to one row per business (bid)
    CALL {{
      // Shape A: Place → Business
      MATCH (p:Place)-[:OF]->(b:BusinessProfile)
      {where_p}
      RETURN b.id AS bid,
             p{{.*,
                business_id:b.id,
                pledge_tier:coalesce(b.pledge,'starter'),
                industry_group:b.industry_group,
                area_type:b.area}} AS P,
             b
      UNION
      // Shape B: Business with lat/lng
      MATCH (b:BusinessProfile)
      WHERE b.lat IS NOT NULL AND b.lng IS NOT NULL
      {("AND " + " AND ".join(filters_b)) if filters_b else ""}
      WITH b, b AS p
      RETURN b.id AS bid,
             p{{ id: coalesce(b.place_id, b.id),
                 name: coalesce(b.name,''),
                 lat: b.lat, lng: b.lng,
                 business_id:b.id,
                 pledge_tier:coalesce(b.pledge,'starter'),
                 industry_group:b.industry_group,
                 area_type:b.area }} AS P,
             b
    }}
    WITH bid, P, b
    // collapse duplicates per business id (prefer first projection)
    WITH bid, b, collect(P) AS plist
    WITH bid, b, head(plist) AS P

    // offers
    OPTIONAL MATCH (o:Offer)-[:OF]->(b)
    WITH P AS place, b, count(CASE WHEN coalesce(o.visible,true) THEN 1 END) AS visible_offers

    // compute distance if user coords present
    WITH place, b, (visible_offers > 0) AS has_offers,
         CASE
           WHEN $ulat IS NOT NULL AND $ulng IS NOT NULL
                AND place.lat IS NOT NULL AND place.lng IS NOT NULL
           THEN point.distance(
                  point({{latitude:place.lat, longitude:place.lng}}),
                  point({{latitude:$ulat, longitude:$ulng}})
                ) / 1000.0
           ELSE NULL
         END AS distance_km

    RETURN place, has_offers, distance_km
    {order_clause}
    SKIP $skip LIMIT $limit
    """

    rows = list(s.run(cypher, **params))
    items: List[Dict[str, Any]] = []
    for r in rows:
        p = dict(r["place"])
        item: Dict[str, Any] = {
            "id": p.get("id"),
            "business_id": p.get("business_id"),
            "name": p.get("name"),
            "lat": float(p.get("lat")),
            "lng": float(p.get("lng")),
            "pledge_tier": p.get("pledge_tier", "starter"),
            "industry_group": p.get("industry_group"),
            "area_type": p.get("area_type"),
            "has_offers": bool(r["has_offers"]),
        }
        dk = r.get("distance_km")
        if dk is not None:
            item["distance_km"] = float(dk)
        items.append(item)

    # total - count per shape, summed, using the same filters
    count_cypher = f"""
    CALL {{
      MATCH (p:Place)-[:OF]->(b:BusinessProfile)
      {where_p}
      RETURN count(DISTINCT b.id) AS c
      UNION
      MATCH (b:BusinessProfile)
      WHERE b.lat IS NOT NULL AND b.lng IS NOT NULL
      {("AND " + " AND ".join(filters_b)) if filters_b else ""}
      RETURN count(DISTINCT b.id) AS c
    }}
    RETURN sum(c) AS total
    """
    total = s.run(count_cypher, **params).single()["total"] or 0

    return {"items": items, "total": int(total), "page": page, "page_size": params["limit"]}
