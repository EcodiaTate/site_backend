from __future__ import annotations

from datetime import datetime, timezone
from typing import Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from neo4j import Session
from pydantic import BaseModel, Field

from site_backend.core.neo_driver import session_dep

router = APIRouter(prefix="/eyba/admin/business/stats", tags=["eyba-admin-stats"])

# ---------- Helpers ----------
def _month_bounds(iso_month: str) -> tuple[str, str]:
    # iso_month = "YYYY-MM"
    try:
        y, m = map(int, iso_month.split("-"))
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid month. Use 'YYYY-MM'.")
    start = datetime(y, m, 1, tzinfo=timezone.utc)
    end = datetime(y + (m // 12), (m % 12) + 1, 1, tzinfo=timezone.utc)
    return start.isoformat(), end.isoformat()

def _month_bounds_ms(iso_month: str) -> tuple[int, int]:
    s_iso, e_iso = _month_bounds(iso_month)
    s = int(datetime.fromisoformat(s_iso).timestamp() * 1000)
    e = int(datetime.fromisoformat(e_iso).timestamp() * 1000)
    return s, e

def _iter_year_months(start_ym: str, end_ym: str) -> List[str]:
    sy, sm = map(int, start_ym.split("-"))
    ey, em = map(int, end_ym.split("-"))
    if (ey, em) < (sy, sm):
        raise HTTPException(status_code=400, detail="'to' must be >= 'from' (YYYY-MM).")
    y, m = sy, sm
    out = []
    while (y, m) <= (ey, em):
        out.append(f"{y:04d}-{m:02d}")
        y, m = (y + 1, 1) if m == 12 else (y, m + 1)
    return out

# ---------- Models ----------
class SummaryOut(BaseModel):
    month_start: str
    month_end: str
    businesses_total: int
    by_pledge: Dict[str, int] = Field(default_factory=dict)
    by_industry: Dict[str, int] = Field(default_factory=dict)
    by_area: Dict[str, int] = Field(default_factory=dict)
    offers_total: int
    minted_eco_month: int
    claims_month: int
    unique_youth_month: int

class IndustryRow(BaseModel):
    industry_group: str
    businesses: int
    offers: int
    minted_eco_month: int
    claims_month: int
    unique_youth_month: int

class IndustryBreakdownOut(BaseModel):
    month_start: str
    month_end: str
    rows: List[IndustryRow]

class TopBizRow(BaseModel):
    business_id: str
    name: str
    industry_group: Optional[str] = None
    pledge: Optional[str] = None
    area: Optional[str] = None
    minted_eco_month: int
    claims_month: int
    unique_youth_month: int

class TopBizOut(BaseModel):
    month_start: str
    month_end: str
    items: List[TopBizRow]

class Point(BaseModel):
    month: str   # YYYY-MM
    minted_eco: int

class TimeSeriesOut(BaseModel):
    from_month: str
    to_month: str
    points: List[Point]

# ---------- Endpoints ----------
@router.get("/summary", response_model=SummaryOut)
def summary(month: str = Query(..., description="YYYY-MM"), s: Session = Depends(session_dep)):
    start_iso, end_iso = _month_bounds(month)
    start_ms, end_ms = _month_bounds_ms(month)

    total_biz = int(s.run("MATCH (b:BusinessProfile) RETURN count(b) AS c").single()["c"])

    pledge_map = {r["k"]: int(r["v"]) for r in s.run(
        "MATCH (b:BusinessProfile) WITH toLower(coalesce(b.pledge_tier,'unknown')) AS k, count(b) AS v RETURN k,v"
    )}
    industry_map = {r["k"]: int(r["v"]) for r in s.run(
        "MATCH (b:BusinessProfile) WITH toLower(coalesce(b.industry_group,'unknown')) AS k, count(b) AS v RETURN k,v"
    )}
    area_map = {r["k"]: int(r["v"]) for r in s.run(
        "MATCH (b:BusinessProfile) WITH toLower(coalesce(b.area,'unknown')) AS k, count(b) AS v RETURN k,v"
    )}

    offers_total = int(s.run(
        "MATCH (o:Offer)<-[:OF]-(:BusinessProfile) RETURN count(o) AS c"
    ).single()["c"])

    rec = s.run(
        """
        MATCH (b:BusinessProfile)-[:TRIGGERED]->(t:EcoTx)
        WHERE
          (t.at IS NOT NULL AND t.at >= datetime($start_iso) AND t.at < datetime($end_iso))
          OR
          (t.at IS NULL AND toInteger(t.createdAt) >= $start_ms AND toInteger(t.createdAt) < $end_ms)
        WITH collect(t) AS txs, sum(toInteger(coalesce(t.amount, t.eco, 0))) AS minted, count(t) AS claims
        OPTIONAL MATCH (u:User)-[:EARNED]->(t2:EcoTx) WHERE t2 IN txs
        RETURN toInteger(coalesce(minted,0)) AS minted, toInteger(coalesce(claims,0)) AS claims, toInteger(count(DISTINCT u)) AS uniq
        """,
        {"start_iso": start_iso, "end_iso": end_iso, "start_ms": start_ms, "end_ms": end_ms},
    ).single()
    minted = int(rec["minted"] or 0)
    claims = int(rec["claims"] or 0)
    uniq = int(rec["uniq"] or 0)

    return SummaryOut(
        month_start=start_iso, month_end=end_iso,
        businesses_total=total_biz,
        by_pledge=pledge_map, by_industry=industry_map, by_area=area_map,
        offers_total=offers_total, minted_eco_month=minted, claims_month=claims, unique_youth_month=uniq
    )

@router.get("/industry", response_model=IndustryBreakdownOut)
def by_industry(month: str = Query(..., description="YYYY-MM"), s: Session = Depends(session_dep)):
    start_iso, end_iso = _month_bounds(month)
    start_ms, end_ms = _month_bounds_ms(month)

    rows: List[IndustryRow] = []
    for r in s.run(
        """
        MATCH (b:BusinessProfile)
        WITH toLower(coalesce(b.industry_group,'unknown')) AS industry, collect(b) AS bs
        WITH industry, bs, size(bs) AS businesses

        OPTIONAL MATCH (o:Offer)<-[:OF]-(b2:BusinessProfile)
        WHERE toLower(coalesce(b2.industry_group,'unknown')) = industry
        WITH industry, businesses, count(o) AS offers

        OPTIONAL MATCH (b3:BusinessProfile)-[:TRIGGERED]->(t:EcoTx)
        WHERE toLower(coalesce(b3.industry_group,'unknown')) = industry
          AND (
            (t.at IS NOT NULL AND t.at >= datetime($start_iso) AND t.at < datetime($end_iso))
            OR
            (t.at IS NULL AND toInteger(t.createdAt) >= $start_ms AND toInteger(t.createdAt) < $end_ms)
          )
        WITH industry, businesses, offers, sum(toInteger(coalesce(t.amount, t.eco, 0))) AS minted, count(t) AS claims, collect(t) AS txs
        OPTIONAL MATCH (u:User)-[:EARNED]->(t2:EcoTx) WHERE t2 IN txs
        RETURN industry, toInteger(businesses) AS businesses, toInteger(offers) AS offers,
               toInteger(coalesce(minted,0)) AS minted, toInteger(coalesce(claims,0)) AS claims, toInteger(count(DISTINCT u)) AS uniq
        ORDER BY industry ASC
        """,
        {"start_iso": start_iso, "end_iso": end_iso, "start_ms": start_ms, "end_ms": end_ms},
    ):
        rows.append(IndustryRow(
            industry_group=r["industry"], businesses=int(r["businesses"]), offers=int(r["offers"]),
            minted_eco_month=int(r["minted"]), claims_month=int(r["claims"]), unique_youth_month=int(r["uniq"])
        ))

    return IndustryBreakdownOut(month_start=start_iso, month_end=end_iso, rows=rows)

@router.get("/top", response_model=TopBizOut)
def top_businesses(month: str = Query(..., description="YYYY-MM"),
                   limit: int = Query(20, ge=1, le=200),
                   s: Session = Depends(session_dep)):
    start_iso, end_iso = _month_bounds(month)
    start_ms, end_ms = _month_bounds_ms(month)

    items: List[TopBizRow] = []
    for r in s.run(
        """
        MATCH (b:BusinessProfile)
        OPTIONAL MATCH (b)-[:TRIGGERED]->(t:EcoTx)
        WHERE
          (t.at IS NOT NULL AND t.at >= datetime($start_iso) AND t.at < datetime($end_iso))
          OR
          (t.at IS NULL AND toInteger(t.createdAt) >= $start_ms AND toInteger(t.createdAt) < $end_ms)
        WITH b, sum(toInteger(coalesce(t.amount, t.eco, 0))) AS minted, count(t) AS claims, collect(t) AS txs
        OPTIONAL MATCH (u:User)-[:EARNED]->(t2:EcoTx) WHERE t2 IN txs
        WITH b, minted, claims, count(DISTINCT u) AS uniq
        RETURN b.id AS bid,
               coalesce(b.name, b.id) AS name,
               toLower(coalesce(b.industry_group,'unknown')) AS industry,
               toLower(coalesce(b.pledge_tier,'unknown')) AS pledge,
               toLower(coalesce(b.area,'unknown')) AS area,
               toInteger(coalesce(minted,0)) AS minted,
               toInteger(coalesce(claims,0)) AS claims,
               toInteger(uniq) AS uniq
        ORDER BY minted DESC, claims DESC, name ASC
        LIMIT $limit
        """,
        {"start_iso": start_iso, "end_iso": end_iso, "start_ms": start_ms, "end_ms": end_ms, "limit": limit},
    ):
        items.append(TopBizRow(
            business_id=r["bid"], name=r["name"],
            industry_group=r["industry"], pledge=r["pledge"], area=r["area"],
            minted_eco_month=int(r["minted"]), claims_month=int(r["claims"]), unique_youth_month=int(r["uniq"])
        ))

    return TopBizOut(month_start=start_iso, month_end=end_iso, items=items)

@router.get("/timeseries", response_model=TimeSeriesOut)
def minted_timeseries(from_month: str = Query(..., description="YYYY-MM"),
                      to_month: str = Query(..., description="YYYY-MM"),
                      s: Session = Depends(session_dep)):
    months = _iter_year_months(from_month, to_month)
    points: List[Point] = []
    for ym in months:
        start_iso, end_iso = _month_bounds(ym)
        start_ms, end_ms = _month_bounds_ms(ym)
        rec = s.run(
            """
            MATCH (b:BusinessProfile)-[:TRIGGERED]->(t:EcoTx)
            WHERE
              (t.at IS NOT NULL AND t.at >= datetime($start_iso) AND t.at < datetime($end_iso))
              OR
              (t.at IS NULL AND toInteger(t.createdAt) >= $start_ms AND toInteger(t.createdAt) < $end_ms)
            RETURN toInteger(sum(toInteger(coalesce(t.amount, t.eco, 0)))) AS minted
            """,
            {"start_iso": start_iso, "end_iso": end_iso, "start_ms": start_ms, "end_ms": end_ms},
        ).single()
        points.append(Point(month=ym, minted_eco=int(rec["minted"] or 0)))
    return TimeSeriesOut(from_month=from_month, to_month=to_month, points=points)
