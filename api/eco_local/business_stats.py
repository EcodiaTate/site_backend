# site_backend/api/routers/eco_local_business_stats.py
from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from neo4j import Session
from pydantic import BaseModel, Field

from site_backend.core.neo_driver import session_dep

router = APIRouter(prefix="/eco_local/admin/business/stats", tags=["eco_local_admin_stats"])


# ---------- Helpers ----------
def _month_bounds(iso_month: str) -> tuple[str, str]:
    # iso_month = "YYYY-MM"
    try:
        y, m = map(int, iso_month.split("-"))
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid month. Use 'YYYY-MM'.")
    start = datetime(y, m, 1, tzinfo=timezone.utc)
    if m == 12:
        end = datetime(y + 1, 1, 1, tzinfo=timezone.utc)
    else:
        end = datetime(y, m + 1, 1, tzinfo=timezone.utc)
    return start.isoformat(), end.isoformat()


def _iter_year_months(start_ym: str, end_ym: str) -> List[str]:
    sy, sm = map(int, start_ym.split("-"))
    ey, em = map(int, end_ym.split("-"))
    if (ey, em) < (sy, sm):
        raise HTTPException(status_code=400, detail="'to' must be >= 'from' (YYYY-MM).")
    cur_y, cur_m = sy, sm
    out = []
    while (cur_y, cur_m) <= (ey, em):
        out.append(f"{cur_y:04d}-{cur_m:02d}")
        if cur_m == 12:
            cur_y += 1
            cur_m = 1
        else:
            cur_m += 1
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
    monthly_budget: Optional[int] = None  # computed from price * ratio * budget_ratio


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
def summary(
    month: str = Query(..., description="YYYY-MM"),
    s: Session = Depends(session_dep),
):
    start, end = _month_bounds(month)

    # 1) Business totals & groupings
    rec = s.run(
        """
        MATCH (b:BusinessProfile)
        WITH collect(b) as bs
        RETURN size(bs) as total
        """
    ).single()
    total_biz = int(rec["total"]) if rec else 0

    # Group by pledge/industry/area
    pledge_map = {}
    for row in s.run(
        """
        MATCH (b:BusinessProfile)
        WITH toLower(coalesce(b.pledge, "unknown")) AS p, count(b) AS c
        RETURN p AS k, c AS v
        """
    ):
        pledge_map[row["k"]] = int(row["v"])

    industry_map = {}
    for row in s.run(
        """
        MATCH (b:BusinessProfile)
        WITH toLower(coalesce(b.industry_group, "unknown")) AS i, count(b) AS c
        RETURN i AS k, c AS v
        """
    ):
        industry_map[row["k"]] = int(row["v"])

    area_map = {}
    for row in s.run(
        """
        MATCH (b:BusinessProfile)
        WITH toLower(coalesce(b.area, "unknown")) AS a, count(b) AS c
        RETURN a AS k, c AS v
        """
    ):
        area_map[row["k"]] = int(row["v"])

    # 2) Offers count (visible or all; here we count all)
    offers_total = 0
    rec2 = s.run(
        """
        MATCH (o:Offer)-[:OF]->(:BusinessProfile)
        RETURN count(o) AS c
        """
    ).single()
    offers_total = int(rec2["c"]) if rec2 else 0

    # 3) Current month minted & claims & unique youth
    rec3 = s.run(
        """
        // Minted + claims + unique youth for this month
        MATCH (t:EcoTx)-[:FROM]->(b:BusinessProfile)
        WHERE t.at >= datetime($start) AND t.at < datetime($end)
        WITH collect(t) AS txs, sum(coalesce(t.eco,0)) AS minted, count(t) AS claims
        OPTIONAL MATCH (u:User)-[:EARNED]->(t2:EcoTx)
        WHERE t2 IN txs
        RETURN minted AS minted, claims AS claims, count(DISTINCT u) AS uniq
        """,
        {"start": start, "end": end},
    ).single()
    minted = int(rec3["minted"]) if rec3 and rec3["minted"] is not None else 0
    claims = int(rec3["claims"]) if rec3 and rec3["claims"] is not None else 0
    uniq = int(rec3["uniq"]) if rec3 and rec3["uniq"] is not None else 0

    return SummaryOut(
        month_start=start,
        month_end=end,
        businesses_total=total_biz,
        by_pledge=pledge_map,
        by_industry=industry_map,
        by_area=area_map,
        offers_total=offers_total,
        minted_eco_month=minted,
        claims_month=claims,
        unique_youth_month=uniq,
    )


@router.get("/industry", response_model=IndustryBreakdownOut)
def by_industry(
    month: str = Query(..., description="YYYY-MM"),
    s: Session = Depends(session_dep),
):
    start, end = _month_bounds(month)

    rows: List[IndustryRow] = []
    # Per-industry counts + offers + minted/claims/unique youth
    for row in s.run(
        """
        // Baseline per-industry counts
        MATCH (b:BusinessProfile)
        WITH toLower(coalesce(b.industry_group, "unknown")) AS industry, collect(b) AS bs
        WITH industry, bs, size(bs) AS businesses
        // Offers in industry
        OPTIONAL MATCH (o:Offer)-[:OF]->(b2:BusinessProfile)
        WHERE toLower(coalesce(b2.industry_group, "unknown")) = industry
        WITH industry, businesses, count(o) AS offers
        // Current month tx
        OPTIONAL MATCH (t:EcoTx)-[:FROM]->(b3:BusinessProfile)
        WHERE toLower(coalesce(b3.industry_group, "unknown")) = industry
          AND t.at >= datetime($start) AND t.at < datetime($end)
        WITH industry, businesses, offers,
             sum(coalesce(t.eco,0)) AS minted, count(t) AS claims, collect(t) AS txs
        OPTIONAL MATCH (u:User)-[:EARNED]->(t2:EcoTx)
        WHERE t2 IN txs
        RETURN industry, businesses, offers,
               toInteger(coalesce(minted,0)) AS minted,
               toInteger(coalesce(claims,0)) AS claims,
               toInteger(count(DISTINCT u)) AS uniq
        ORDER BY industry ASC
        """,
        {"start": start, "end": end},
    ):
        rows.append(
            IndustryRow(
                industry_group=row["industry"],
                businesses=int(row["businesses"]),
                offers=int(row["offers"]),
                minted_eco_month=int(row["minted"]),
                claims_month=int(row["claims"]),
                unique_youth_month=int(row["uniq"]),
            )
        )

    return IndustryBreakdownOut(month_start=start, month_end=end, rows=rows)


@router.get("/top", response_model=TopBizOut)
def top_businesses(
    month: str = Query(..., description="YYYY-MM"),
    limit: int = Query(20, ge=1, le=200),
    s: Session = Depends(session_dep),
):
    start, end = _month_bounds(month)

    items: List[TopBizRow] = []
    for row in s.run(
        """
        MATCH (b:BusinessProfile)
        OPTIONAL MATCH (t:EcoTx)-[:FROM]->(b)
        WHERE t.at >= datetime($start) AND t.at < datetime($end)
        WITH b,
             toInteger(coalesce(b.monthly_price_cents,0)) AS price_cents,
             toInteger(coalesce(b.eco_mint_ratio,10))     AS ratio,
             coalesce(b.monthly_budget_ratio,0.9)         AS bratio,
             sum(coalesce(t.eco,0)) AS minted,
             count(t) AS claims,
             collect(t) AS txs
        OPTIONAL MATCH (u:User)-[:EARNED]->(t2:EcoTx)
        WHERE t2 IN txs
        WITH b, price_cents, ratio, bratio, minted, claims, count(DISTINCT u) AS uniq
        WITH b, minted, claims, uniq,
             toInteger( round((price_cents/100.0) * ratio * bratio) ) AS monthly_budget
        RETURN b.id AS bid,
               coalesce(b.name, b.id) AS name,
               toLower(coalesce(b.industry_group, "unknown")) AS industry,
               toLower(coalesce(b.pledge, "unknown")) AS pledge,
               toLower(coalesce(b.area, "unknown")) AS area,
               toInteger(coalesce(minted,0)) AS minted,
               toInteger(coalesce(claims,0)) AS claims,
               toInteger(uniq) AS uniq,
               monthly_budget
        ORDER BY minted DESC, claims DESC, name ASC
        LIMIT $limit
        """,
        {"start": start, "end": end, "limit": limit},
    ):
        items.append(
            TopBizRow(
                business_id=row["bid"],
                name=row["name"],
                industry_group=row["industry"],
                pledge=row["pledge"],
                area=row["area"],
                minted_eco_month=int(row["minted"]),
                claims_month=int(row["claims"]),
                unique_youth_month=int(row["uniq"]),
                monthly_budget=int(row["monthly_budget"]) if row["monthly_budget"] is not None else None,
            )
        )

    return TopBizOut(month_start=start, month_end=end, items=items)


@router.get("/timeseries", response_model=TimeSeriesOut)
def minted_timeseries(
    from_month: str = Query(..., description="YYYY-MM"),
    to_month: str = Query(..., description="YYYY-MM"),
    s: Session = Depends(session_dep),
):
    months = _iter_year_months(from_month, to_month)
    points: List[Point] = []

    # Query per-month to keep Cypher simple & predictable (data volume is small for admin)
    for ym in months:
        start, end = _month_bounds(ym)
        rec = s.run(
            """
            MATCH (t:EcoTx)-[:FROM]->(:BusinessProfile)
            WHERE t.at >= datetime($start) AND t.at < datetime($end)
            RETURN toInteger(sum(coalesce(t.eco,0))) AS minted
            """,
            {"start": start, "end": end},
        ).single()
        minted = int(rec["minted"]) if rec and rec["minted"] is not None else 0
        points.append(Point(month=ym, minted_eco=minted))

    return TimeSeriesOut(from_month=from_month, to_month=to_month, points=points)
