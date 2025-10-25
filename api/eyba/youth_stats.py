# site_backend/api/eyba/youth_stats.py
from __future__ import annotations

from datetime import datetime, timezone
from typing import List, Literal, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from neo4j import Session
from pydantic import BaseModel

from site_backend.core.neo_driver import session_dep
from site_backend.core.user_guard import current_user_id


# =========================================================
# Helpers
# =========================================================

def _month_bounds(iso_month: str) -> tuple[str, str]:
    """
    iso_month = 'YYYY-MM'
    Returns (start_iso, end_iso) with UTC tz.
    """
    try:
        year, mon = map(int, iso_month.split("-"))
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid month. Use 'YYYY-MM'.")
    start = datetime(year, mon, 1, tzinfo=timezone.utc)
    if mon == 12:
        end = datetime(year + 1, 1, 1, tzinfo=timezone.utc)
    else:
        end = datetime(year, mon + 1, 1, tzinfo=timezone.utc)
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


# =========================================================
# Models (public + admin)
# =========================================================

class YouthStats(BaseModel):
    user_id: str
    total_eco: int
    eco_from_missions: int
    eco_from_eyba: int
    missions_completed: int
    last_earn_at: Optional[str] = None


class YouthStatsRow(YouthStats):
    pass


class YouthStatsList(BaseModel):
    total: int
    rows: List[YouthStatsRow]


class OverviewStats(BaseModel):
    total_youth: int
    active_youth_30d: int
    eco_minted_total: int
    minted_24h: int
    missions_completed_total: int
    last_event_at: Optional[str] = None


class TopYouthRow(BaseModel):
    user_id: str
    total_eco_period: int
    missions_completed_period: int
    last_earn_at: Optional[str] = None


class TopYouthOut(BaseModel):
    month_start: Optional[str] = None
    month_end: Optional[str] = None
    items: List[TopYouthRow]


class Point(BaseModel):
    month: str   # YYYY-MM
    minted_eco: int
    active_youth: int
    missions_completed: int


class TimeSeriesOut(BaseModel):
    from_month: str
    to_month: str
    points: List[Point]


OrderParam = Literal[
    "total_eco_desc", "total_eco_asc",
    "last_earn_at_desc", "last_earn_at_asc",
    "missions_desc", "missions_asc"
]


def _order_clause(order: OrderParam) -> str:
    mapping = {
        "total_eco_desc":     "total_eco DESC",
        "total_eco_asc":      "total_eco ASC",
        "last_earn_at_desc":  "last_earn_at DESC",
        "last_earn_at_asc":   "last_earn_at ASC",
        "missions_desc":      "missions_completed DESC",
        "missions_asc":       "missions_completed ASC",
    }
    return mapping.get(order, "last_earn_at DESC")


# =========================================================
# Routers
# =========================================================

public_router = APIRouter(prefix="/stats", tags=["youth_stats"])
admin_router  = APIRouter(prefix="/eyba/admin/youth/stats", tags=["eyba_admin_stats"])


# =========================================================
# Public endpoint - My Stats
# =========================================================

@public_router.get("/youth/{user_id}", response_model=YouthStats)
def get_youth_stats(user_id: str, s: Session = Depends(session_dep)):
    rec = s.run(
    """
    MATCH (u:User {id:$uid})

    // 1) aggregate Eco once
    OPTIONAL MATCH (u)-[:EARNED]->(t:EcoTx)
    WITH u,
         sum(coalesce(t.eco,0)) AS total_eco,
         sum(CASE WHEN t.source = "mission" THEN coalesce(t.eco,0) ELSE 0 END) AS eco_from_missions,
         sum(CASE WHEN t.source = "eyba"    THEN coalesce(t.eco,0) ELSE 0 END) AS eco_from_eyba,
         max(t.at) AS last_earn_at

    // 2) do the second aggregation separately, then freeze all values
    OPTIONAL MATCH (u)-[:SUBMITTED]->(s:Submission {state:"approved"})
    WITH u, total_eco, eco_from_missions, eco_from_eyba, last_earn_at,
         count(s) AS missions_completed

    // 3) return a pure map (no more aggregations here)
    RETURN {
      user_id: u.id,
      total_eco: toInteger(total_eco),
      eco_from_missions: toInteger(eco_from_missions),
      eco_from_eyba: toInteger(eco_from_eyba),
      missions_completed: toInteger(missions_completed),
      last_earn_at: CASE WHEN last_earn_at IS NULL THEN NULL ELSE toString(last_earn_at) END
    } AS stats
    """,
    {"uid": user_id},  # user_id in the path handler, or `me` from current_user_id() in /youth/me
).single()


    if not rec:
        raise HTTPException(status_code=404, detail="User not found")
    return YouthStats(**dict(rec["stats"]))


# =========================================================
# Admin endpoints - lists, overview, top, timeseries
# =========================================================

@admin_router.get("/list", response_model=YouthStatsList)
def list_youth_stats(
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
    order: OrderParam = Query("last_earn_at_desc"),
    s: Session = Depends(session_dep),
):
    # Build the ORDER BY safely without f-strings breaking braces.
    base_query = """
    CALL {
      MATCH (u:User)
      OPTIONAL MATCH (u)-[:EARNED]->(t:EcoTx)
      WITH u,
           sum(coalesce(t.eco,0)) AS total_eco,
           sum(CASE WHEN t.source = "mission" THEN coalesce(t.eco,0) ELSE 0 END) AS eco_from_missions,
           sum(CASE WHEN t.source = "eyba"    THEN coalesce(t.eco,0) ELSE 0 END) AS eco_from_eyba,
           max(t.at) AS last_earn_at
      OPTIONAL MATCH (u)-[:SUBMITTED]->(s:Submission {state:"approved"})
      WITH u, total_eco, eco_from_missions, eco_from_eyba, last_earn_at, count(s) AS missions_completed
      RETURN u.id AS user_id,
             toInteger(total_eco) AS total_eco,
             toInteger(eco_from_missions) AS eco_from_missions,
             toInteger(eco_from_eyba) AS eco_from_eyba,
             toInteger(missions_completed) AS missions_completed,
             last_earn_at
    }
    WITH *
    ORDER BY {ORDER}
    SKIP $offset LIMIT $limit
    RETURN collect({
      user_id: user_id,
      total_eco: total_eco,
      eco_from_missions: eco_from_missions,
      eco_from_eyba: eco_from_eyba,
      missions_completed: missions_completed,
      last_earn_at: CASE WHEN last_earn_at IS NULL THEN NULL ELSE toString(last_earn_at) END
    }) AS rows
    """
    query = base_query.replace("{ORDER}", _order_clause(order))

    total_query = """
    MATCH (u:User)
    RETURN count(u) AS total
    """

    rows_rec = s.run(query, {"limit": limit, "offset": offset}).single()
    total_rec = s.run(total_query).single()

    rows = rows_rec["rows"] if rows_rec else []
    total = int(total_rec["total"]) if total_rec else 0
    return YouthStatsList(total=total, rows=rows)


@admin_router.get("/summary", response_model=OverviewStats)
def overview(s: Session = Depends(session_dep)):
    rec = s.run(
        """
        // total users
        MATCH (u:User)
        WITH count(u) AS total_youth

        // totals across all EcoTx
        OPTIONAL MATCH (t:EcoTx)
        WITH total_youth,
             toInteger(sum(coalesce(t.eco,0))) AS eco_minted_total,
             max(t.at) AS last_event_at

        // minted in last 24h
        CALL {
          WITH 1 AS _
          OPTIONAL MATCH (t24:EcoTx)
          WHERE t24.at >= datetime() - duration('P1D')
          RETURN toInteger(sum(coalesce(t24.eco,0))) AS minted_24h
        }

        // active youth last 30d
        CALL {
          WITH 1 AS _
          OPTIONAL MATCH (uu:User)-[:EARNED]->(tt:EcoTx)
          WHERE tt.at >= datetime() - duration('P30D')
          RETURN count(DISTINCT uu) AS active_youth_30d
        }

        // total approved missions
        CALL {
          WITH 1 AS _
          OPTIONAL MATCH (:User)-[:SUBMITTED]->(ss:Submission {state:'approved'})
          RETURN toInteger(count(ss)) AS missions_completed_total
        }

        RETURN {
          total_youth: toInteger(total_youth),
          active_youth_30d: toInteger(coalesce(active_youth_30d,0)),
          eco_minted_total: toInteger(coalesce(eco_minted_total,0)),
          minted_24h: toInteger(coalesce(minted_24h,0)),
          missions_completed_total: toInteger(coalesce(missions_completed_total,0)),
          last_event_at: CASE WHEN last_event_at IS NULL THEN NULL ELSE toString(last_event_at) END
        } AS stats
        """
    ).single()

    if not rec:
        return OverviewStats(
            total_youth=0, active_youth_30d=0, eco_minted_total=0,
            minted_24h=0, missions_completed_total=0, last_event_at=None
        )

    return OverviewStats(**dict(rec["stats"]))


@admin_router.get("/top", response_model=TopYouthOut)
def top_youth(
    month: Optional[str] = Query(None, description="YYYY-MM; if omitted = all-time"),
    limit: int = Query(20, ge=1, le=200),
    s: Session = Depends(session_dep),
):
    if month:
        start, end = _month_bounds(month)
        rows = []
        for row in s.run(
            """
            MATCH (u:User)
            OPTIONAL MATCH (u)-[:EARNED]->(t:EcoTx)
            WHERE t.at >= datetime($start) AND t.at < datetime($end)
            WITH u,
                 sum(coalesce(t.eco,0)) AS eco_sum,
                 max(t.at) AS last_earn_at,
                 collect(t) AS txs
            OPTIONAL MATCH (u)-[:SUBMITTED]->(s:Submission {state:'approved'})
            WHERE s.at >= datetime($start) AND s.at < datetime($end)
            WITH u, eco_sum, last_earn_at, size([x IN txs WHERE x IS NOT NULL]) AS tx_count, count(s) AS missions_count
            RETURN u.id AS uid,
                   toInteger(coalesce(eco_sum,0)) AS eco_period,
                   toInteger(coalesce(missions_count,0)) AS missions_period,
                   last_earn_at
            ORDER BY eco_period DESC, missions_period DESC, uid ASC
            LIMIT $limit
            """,
            {"start": start, "end": end, "limit": limit},
        ):
            rows.append(
                TopYouthRow(
                    user_id=row["uid"],
                    total_eco_period=int(row["eco_period"]),
                    missions_completed_period=int(row["missions_period"]),
                    last_earn_at=(str(row["last_earn_at"]) if row["last_earn_at"] else None),
                )
            )
        return TopYouthOut(month_start=start, month_end=end, items=rows)

    # All-time
    rows = []
    for row in s.run(
        """
        MATCH (u:User)
        OPTIONAL MATCH (u)-[:EARNED]->(t:EcoTx)
        WITH u,
             sum(coalesce(t.eco,0)) AS eco_sum,
             max(t.at) AS last_earn_at
        OPTIONAL MATCH (u)-[:SUBMITTED]->(s:Submission {state:'approved'})
        WITH u, eco_sum, last_earn_at, count(s) AS missions_count
        RETURN u.id AS uid,
               toInteger(coalesce(eco_sum,0)) AS eco_period,
               toInteger(coalesce(missions_count,0)) AS missions_period,
               last_earn_at
        ORDER BY eco_period DESC, missions_period DESC, uid ASC
        LIMIT $limit
        """,
        {"limit": limit},
    ):
        rows.append(
            TopYouthRow(
                user_id=row["uid"],
                total_eco_period=int(row["eco_period"]),
                missions_completed_period=int(row["missions_period"]),
                last_earn_at=(str(row["last_earn_at"]) if row["last_earn_at"] else None),
            )
        )
    return TopYouthOut(month_start=None, month_end=None, items=rows)


@admin_router.get("/timeseries", response_model=TimeSeriesOut)
def youth_timeseries(
    from_month: str = Query(..., description="YYYY-MM"),
    to_month: str = Query(..., description="YYYY-MM"),
    s: Session = Depends(session_dep),
):
    months = _iter_year_months(from_month, to_month)
    points: List[Point] = []

    for ym in months:
        start, end = _month_bounds(ym)
        rec = s.run(
            """
            // monthly eco minted (all sources)
            OPTIONAL MATCH (t:EcoTx)
            WHERE t.at >= datetime($start) AND t.at < datetime($end)
            WITH toInteger(sum(coalesce(t.eco,0))) AS minted

            // active youth this month (earned at least once)
            OPTIONAL MATCH (u:User)-[:EARNED]->(tt:EcoTx)
            WHERE tt.at >= datetime($start) AND tt.at < datetime($end)
            WITH minted, count(DISTINCT u) AS active_youth

            // total approved missions this month
            OPTIONAL MATCH (:User)-[:SUBMITTED]->(s:Submission {state:'approved'})
            WHERE s.at >= datetime($start) AND s.at < datetime($end)
            RETURN minted AS minted,
                   toInteger(active_youth) AS active_youth,
                   toInteger(count(s)) AS missions_completed
            """,
            {"start": start, "end": end},
        ).single()

        minted = int(rec["minted"]) if rec and rec["minted"] is not None else 0
        active = int(rec["active_youth"]) if rec and rec["active_youth"] is not None else 0
        missions = int(rec["missions_completed"]) if rec and rec["missions_completed"] is not None else 0

        points.append(Point(month=ym, minted_eco=minted, active_youth=active, missions_completed=missions))

    return TimeSeriesOut(from_month=from_month, to_month=to_month, points=points)

@public_router.get("/youth/me", response_model=YouthStats)
def get_my_youth_stats(
    me: str = Depends(current_user_id),
    s: Session = Depends(session_dep),
):
    rec = s.run(
    """
    MATCH (u:User {id:$uid})

    // 1) aggregate Eco once
    OPTIONAL MATCH (u)-[:EARNED]->(t:EcoTx)
    WITH u,
         sum(coalesce(t.eco,0)) AS total_eco,
         sum(CASE WHEN t.source = "mission" THEN coalesce(t.eco,0) ELSE 0 END) AS eco_from_missions,
         sum(CASE WHEN t.source = "eyba"    THEN coalesce(t.eco,0) ELSE 0 END) AS eco_from_eyba,
         max(t.at) AS last_earn_at

    // 2) do the second aggregation separately, then freeze all values
    OPTIONAL MATCH (u)-[:SUBMITTED]->(s:Submission {state:"approved"})
    WITH u, total_eco, eco_from_missions, eco_from_eyba, last_earn_at,
         count(s) AS missions_completed

    // 3) return a pure map (no more aggregations here)
    RETURN {
      user_id: u.id,
      total_eco: toInteger(total_eco),
      eco_from_missions: toInteger(eco_from_missions),
      eco_from_eyba: toInteger(eco_from_eyba),
      missions_completed: toInteger(missions_completed),
      last_earn_at: CASE WHEN last_earn_at IS NULL THEN NULL ELSE toString(last_earn_at) END
    } AS stats
    """,
    {"uid": me},  # user_id in the path handler, or `me` from current_user_id() in /youth/me
).single()

    if not rec:
        raise HTTPException(status_code=404, detail="User not found")
    return YouthStats(**dict(rec["stats"]))

# =========================================================
# Export routers for inclusion in app
# =========================================================
# In your FastAPI app, include both:
#   app.include_router(public_router)  # /stats/...
#   app.include_router(admin_router)   # /eyba/admin/youth/stats/...
