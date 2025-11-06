# api/eco_local/youth_stats.py
from __future__ import annotations

from datetime import datetime, timezone
from typing import List, Literal, Optional, Tuple

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from neo4j import Session
from pydantic import BaseModel

from site_backend.core.neo_driver import session_dep
from site_backend.core.user_guard import current_user_id as _current_user_id

# =========================================================
# Helpers
# =========================================================

def _month_bounds_ms(iso_month: str) -> Tuple[int, int]:
    """
    iso_month = 'YYYY-MM'
    Returns (start_ms, end_ms) UTC (ms since epoch).
    """
    try:
        year, mon = map(int, iso_month.split("-"))
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid month. Use 'YYYY-MM'.")
    start = datetime(year, mon, 1, tzinfo=timezone.utc)
    end = datetime(year + (1 if mon == 12 else 0), 1 if mon == 12 else mon + 1, 1, tzinfo=timezone.utc)
    return int(start.timestamp() * 1000), int(end.timestamp() * 1000)

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

async def _resolve_me(req: Request) -> str:
    uid = await _current_user_id(req)
    if isinstance(uid, str) and uid.strip():
        return uid
    raise HTTPException(status_code=401, detail="Not authenticated")

# =========================================================
# Models (clean, retire-era)
# =========================================================

class YouthStats(BaseModel):
    user_id: str
    balance: int
    earned_total: int
    retired_total: int
    actions_completed: int
    last_earn_at: Optional[int] = None   # ms since epoch
    last_spend_at: Optional[int] = None  # ms since epoch

class YouthStatsRow(YouthStats):
    pass

class YouthStatsList(BaseModel):
    total: int
    rows: List[YouthStatsRow]

class OverviewStats(BaseModel):
    total_youth: int
    active_youth_30d: int
    eco_minted_total: int
    eco_retired_total: int
    last_event_at: Optional[int] = None  # ms

class TopYouthRow(BaseModel):
    user_id: str
    total_earned_period: int
    total_retired_period: int
    last_earn_at: Optional[int] = None

class TopYouthOut(BaseModel):
    month_start: Optional[str] = None
    month_end: Optional[str] = None
    items: List[TopYouthRow]

class Point(BaseModel):
    month: str   # YYYY-MM
    minted_eco: int
    retired_eco: int
    active_youth: int
    actions_completed: int

class TimeSeriesOut(BaseModel):
    from_month: str
    to_month: str
    points: List[Point]

OrderParam = Literal[
    "balance_desc", "balance_asc",
    "earned_desc", "earned_asc",
    "retired_desc", "retired_asc",
    "last_earn_desc", "last_earn_asc"
]

def _order_clause(order: OrderParam) -> str:
    mapping = {
        "balance_desc":   "balance DESC",
        "balance_asc":    "balance ASC",
        "earned_desc":    "earned_total DESC",
        "earned_asc":     "earned_total ASC",
        "retired_desc":   "retired_total DESC",
        "retired_asc":    "retired_total ASC",
        "last_earn_desc": "last_earn_at DESC",
        "last_earn_asc":  "last_earn_at ASC",
    }
    return mapping.get(order, "last_earn_at DESC")

# =========================================================
# Routers
# =========================================================

public_router = APIRouter(prefix="/stats", tags=["youth_stats"])
admin_router  = APIRouter(prefix="/eco_local/admin/youth/stats", tags=["eco_local_admin_stats"])

# =========================================================
# Public endpoint - My Stats / By User
# =========================================================

@public_router.get("/youth/{user_id}", response_model=YouthStats)
def get_youth_stats(user_id: str, s: Session = Depends(session_dep)):
    row = s.run(
        """
        MATCH (u:User {id:$uid})

        // earned totals & last earn
        OPTIONAL MATCH (u)-[:EARNED]->(te:EcoTx {kind:'MINT_ACTION', status:'settled'})
        WITH u, coalesce(sum(toInteger(te.amount)),0) AS earned_total, max(toInteger(te.createdAt)) AS last_earn_at

        // retired totals & last spend
        OPTIONAL MATCH (u)-[:SPENT]->(ts:EcoTx {kind:'BURN_REWARD', status:'settled'})
        WITH u, earned_total, last_earn_at,
             coalesce(sum(toInteger(ts.amount)),0) AS retired_total,
             max(toInteger(ts.createdAt)) AS last_spend_at

        // actions completed (approved submissions)
        OPTIONAL MATCH (u)-[:SUBMITTED]->(sub:Submission {state:'approved'})
        RETURN u.id AS user_id,
               toInteger(earned_total) AS earned_total,
               toInteger(retired_total) AS retired_total,
               toInteger(earned_total - retired_total) AS balance,
               toInteger(count(sub)) AS actions_completed,
               last_earn_at,
               last_spend_at
        """,
        uid=user_id
    ).single()

    if not row:
        raise HTTPException(status_code=404, detail="User not found")

    return YouthStats(
        user_id=row["user_id"],
        balance=int(row["balance"] or 0),
        earned_total=int(row["earned_total"] or 0),
        retired_total=int(row["retired_total"] or 0),
        actions_completed=int(row["actions_completed"] or 0),
        last_earn_at=int(row["last_earn_at"]) if row["last_earn_at"] is not None else None,
        last_spend_at=int(row["last_spend_at"]) if row["last_spend_at"] is not None else None,
    )

@public_router.get("/youth/me", response_model=YouthStats)
async def get_my_youth_stats(
    req: Request,
    s: Session = Depends(session_dep),
):
    me = await _resolve_me(req)
    return get_youth_stats(me, s)

# =========================================================
# Admin endpoints - list, overview, top, timeseries
# =========================================================

@admin_router.get("/list", response_model=YouthStatsList)
def list_youth_stats(
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
    order: OrderParam = Query("last_earn_desc"),
    s: Session = Depends(session_dep),
):
    base = f"""
    CALL {{
      MATCH (u:User)
      OPTIONAL MATCH (u)-[:EARNED]->(te:EcoTx {{kind:'MINT_ACTION', status:'settled'}})
      WITH u, coalesce(sum(toInteger(te.amount)),0) AS earned_total, max(toInteger(te.createdAt)) AS last_earn_at
      OPTIONAL MATCH (u)-[:SPENT]->(ts:EcoTx {{kind:'BURN_REWARD', status:'settled'}})
      WITH u, earned_total, last_earn_at, coalesce(sum(toInteger(ts.amount)),0) AS retired_total
      OPTIONAL MATCH (u)-[:SUBMITTED]->(sub:Submission {{state:'approved'}})
      RETURN u.id AS user_id,
             toInteger(earned_total) AS earned_total,
             toInteger(retired_total) AS retired_total,
             toInteger(earned_total - retired_total) AS balance,
             toInteger(count(sub)) AS actions_completed,
             last_earn_at,
             // derive last_spend_at only for ordering payload completeness
             0 AS last_spend_at
    }}
    WITH *
    ORDER BY {_order_clause(order)}
    SKIP $offset LIMIT $limit
    RETURN collect({{
      user_id: user_id,
      balance: balance,
      earned_total: earned_total,
      retired_total: retired_total,
      actions_completed: actions_completed,
      last_earn_at: last_earn_at,
      last_spend_at: last_spend_at
    }}) AS rows
    """
    total_query = "MATCH (u:User) RETURN count(u) AS total"

    rows_rec = s.run(base, {"limit": limit, "offset": offset}).single()
    total_rec = s.run(total_query).single()

    rows = [YouthStatsRow(**r) for r in (rows_rec["rows"] or [])]
    total = int(total_rec["total"]) if total_rec else 0
    return YouthStatsList(total=total, rows=rows)

@admin_router.get("/summary", response_model=OverviewStats)
def overview(s: Session = Depends(session_dep)):
    from datetime import timedelta
    since_30d = int((datetime.now(timezone.utc) - timedelta(days=30)).timestamp() * 1000)

    rec = s.run(
        """
        MATCH (u:User)
        WITH count(u) AS total_youth

        OPTIONAL MATCH (tm:EcoTx {kind:'MINT_ACTION', status:'settled'})
        WITH total_youth, toInteger(sum(coalesce(tm.amount,0))) AS eco_minted_total, max(toInteger(tm.createdAt)) AS last_mint

        OPTIONAL MATCH (tb:EcoTx {kind:'BURN_REWARD', status:'settled'})
        WITH total_youth, eco_minted_total, last_mint,
             toInteger(sum(coalesce(tb.amount,0))) AS eco_retired_total,
             max(toInteger(tb.createdAt)) AS last_burn

        WITH total_youth, eco_minted_total, eco_retired_total, coalesce(max(last_mint,last_burn), NULL) AS last_event_at

        OPTIONAL MATCH (ua:User)-[:EARNED]->(t30:EcoTx {kind:'MINT_ACTION', status:'settled'})
        WHERE toInteger(t30.createdAt) >= $since
        RETURN
          toInteger(total_youth) AS total_youth,
          toInteger(eco_minted_total) AS eco_minted_total,
          toInteger(eco_retired_total) AS eco_retired_total,
          toInteger(coalesce(count(DISTINCT ua),0)) AS active_youth_30d,
          last_event_at
        """,
        since=since_30d
    ).single()

    if not rec:
        return OverviewStats(total_youth=0, active_youth_30d=0, eco_minted_total=0, eco_retired_total=0, last_event_at=None)

    return OverviewStats(
        total_youth=int(rec["total_youth"] or 0),
        active_youth_30d=int(rec["active_youth_30d"] or 0),
        eco_minted_total=int(rec["eco_minted_total"] or 0),
        eco_retired_total=int(rec["eco_retired_total"] or 0),
        last_event_at=int(rec["last_event_at"]) if rec["last_event_at"] is not None else None,
    )

@admin_router.get("/top", response_model=TopYouthOut)
def top_youth(
    month: Optional[str] = Query(None, description="YYYY-MM; if omitted = all-time"),
    limit: int = Query(20, ge=1, le=200),
    s: Session = Depends(session_dep),
):
    if month:
        start_ms, end_ms = _month_bounds_ms(month)
        rows = s.run(
            """
            MATCH (u:User)
            OPTIONAL MATCH (u)-[:EARNED]->(te:EcoTx {kind:'MINT_ACTION', status:'settled'})
            WHERE toInteger(te.createdAt) >= $start AND toInteger(te.createdAt) < $end
            WITH u, coalesce(sum(toInteger(te.amount)),0) AS earned_total, max(toInteger(te.createdAt)) AS last_earn_at
            OPTIONAL MATCH (u)-[:SPENT]->(ts:EcoTx {kind:'BURN_REWARD', status:'settled'})
            WHERE toInteger(ts.createdAt) >= $start AND toInteger(ts.createdAt) < $end
            RETURN u.id AS user_id,
                   toInteger(earned_total) AS total_earned_period,
                   toInteger(coalesce(sum(toInteger(ts.amount)),0)) AS total_retired_period,
                   last_earn_at
            ORDER BY total_earned_period DESC, user_id ASC
            LIMIT $limit
            """,
            start=start_ms, end=end_ms, limit=limit
        ).data() or []
        return TopYouthOut(
            month_start=month, month_end=month,  # human label only
            items=[
                TopYouthRow(
                    user_id=r["user_id"],
                    total_earned_period=int(r["total_earned_period"] or 0),
                    total_retired_period=int(r["total_retired_period"] or 0),
                    last_earn_at=int(r["last_earn_at"]) if r["last_earn_at"] is not None else None,
                )
                for r in rows
            ]
        )

    # All-time
    rows = s.run(
        """
        MATCH (u:User)
        OPTIONAL MATCH (u)-[:EARNED]->(te:EcoTx {kind:'MINT_ACTION', status:'settled'})
        WITH u, coalesce(sum(toInteger(te.amount)),0) AS earned_total, max(toInteger(te.createdAt)) AS last_earn_at
        OPTIONAL MATCH (u)-[:SPENT]->(ts:EcoTx {kind:'BURN_REWARD', status:'settled'})
        RETURN u.id AS user_id,
               toInteger(earned_total) AS total_earned_period,
               toInteger(coalesce(sum(toInteger(ts.amount)),0)) AS total_retired_period,
               last_earn_at
        ORDER BY total_earned_period DESC, user_id ASC
        LIMIT $limit
        """,
        limit=limit
    ).data() or []
    return TopYouthOut(
        month_start=None, month_end=None,
        items=[
            TopYouthRow(
                user_id=r["user_id"],
                total_earned_period=int(r["total_earned_period"] or 0),
                total_retired_period=int(r["total_retired_period"] or 0),
                last_earn_at=int(r["last_earn_at"]) if r["last_earn_at"] is not None else None,
            )
            for r in rows
        ]
    )

@admin_router.get("/timeseries", response_model=TimeSeriesOut)
def youth_timeseries(
    from_month: str = Query(..., description="YYYY-MM"),
    to_month: str = Query(..., description="YYYY-MM"),
    s: Session = Depends(session_dep),
):
    months = _iter_year_months(from_month, to_month)
    points: List[Point] = []

    for ym in months:
        start_ms, end_ms = _month_bounds_ms(ym)
        rec = s.run(
            """
            // minted & retired in this month
            OPTIONAL MATCH (tm:EcoTx {kind:'MINT_ACTION', status:'settled'})
            WHERE toInteger(tm.createdAt) >= $start AND toInteger(tm.createdAt) < $end
            WITH toInteger(sum(coalesce(tm.amount,0))) AS minted

            OPTIONAL MATCH (tb:EcoTx {kind:'BURN_REWARD', status:'settled'})
            WHERE toInteger(tb.createdAt) >= $start AND toInteger(tb.createdAt) < $end
            WITH minted, toInteger(sum(coalesce(tb.amount,0))) AS retired

            // active youth = any earned this month
            OPTIONAL MATCH (u:User)-[:EARNED]->(t:EcoTx {kind:'MINT_ACTION', status:'settled'})
            WHERE toInteger(t.createdAt) >= $start AND toInteger(t.createdAt) < $end
            WITH minted, retired, toInteger(count(DISTINCT u)) AS active

            // approved submissions this month
            OPTIONAL MATCH (:User)-[:SUBMITTED]->(s:Submission {state:'approved'})
            WHERE toInteger(s.at) >= $start AND toInteger(s.at) < $end
            RETURN minted AS minted, retired AS retired, active AS active, toInteger(count(s)) AS actions_completed
            """,
            start=start_ms, end=end_ms
        ).single()

        points.append(
            Point(
                month=ym,
                minted_eco=int(rec["minted"]) if rec and rec["minted"] is not None else 0,
                retired_eco=int(rec["retired"]) if rec and rec["retired"] is not None else 0,
                active_youth=int(rec["active"]) if rec and rec["active"] is not None else 0,
                actions_completed=int(rec["actions_completed"]) if rec and rec["actions_completed"] is not None else 0,
            )
        )

    return TimeSeriesOut(from_month=from_month, to_month=to_month, points=points)

# =========================================================
# Export routers for inclusion in app
# =========================================================
# In your FastAPI app, include both:
#   app.include_router(public_router)   # /stats/...
#   app.include_router(admin_router)    # /eco_local/admin/youth/stats/...
