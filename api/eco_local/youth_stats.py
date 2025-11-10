# site_backend/api/eco-local/youth_stats.py
from __future__ import annotations

from datetime import datetime, timezone
from typing import List, Literal, Optional, Tuple

from fastapi import APIRouter, Depends, HTTPException, Query
from neo4j import Session
from pydantic import BaseModel

from site_backend.core.neo_driver import session_dep
from site_backend.core.user_guard import current_user_id

# =========================================================
# Helpers
# =========================================================

def _month_bounds(iso_month: str) -> tuple[str, str, int, int]:
    """
    iso_month = 'YYYY-MM'
    Returns (start_iso, end_iso, start_ms, end_ms) in UTC.
    We use both ms and ISO because some rows only have createdAt (ms),
    others may rely on at (datetime), and we want robust windows.
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
    s_iso, e_iso = start.isoformat(), end.isoformat()
    s_ms = int(start.timestamp() * 1000)
    e_ms = int(end.timestamp() * 1000)
    return s_iso, e_iso, s_ms, e_ms


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
# Models
# =========================================================

class YouthStats(BaseModel):
    user_id: str
    # Lifetime earned ECO (posted/settled) from ledger — NOT balance.
    total_eco: int
    # From missions (sidequests): earned tx with t.source='sidequest' or t.reason='sidequest_reward'
    # plus virtual approved submissions which lack a PROOF-linked EcoTx.
    eco_from_missions: int
    # From Eco-Local (QR/scan etc): earned tx with source='eco-local' or kind='scan'
    eco_from_eco_local: int
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
admin_router  = APIRouter(prefix="/eco-local/admin/youth/stats", tags=["eco_local_admin_stats"])


# =========================================================
# Core sub-queries (reuse everywhere)
# =========================================================

# NOTE: We always compute a canonical tx timestamp in ms:
#   t_ms = coalesce(t.createdAt, timestamp(t.at), 0)
# and we consider earned if:
#   rel = :EARNED and coalesce(t.status,'settled')='settled'
# Amount is:
#   eco_amt = toInteger(coalesce(t.amount, t.eco, 0))

# Earned lifetime + sources breakdown + last earn time
_YOUTH_EARN_LIFETIME = """
MATCH (u:User {id:$uid})

// Earned (settled) lifetime
OPTIONAL MATCH (u)-[rel:EARNED]->(t:EcoTx)
WHERE coalesce(t.status,'settled')='settled'
WITH u,
     toInteger(coalesce(sum(toInteger(coalesce(t.amount, t.eco, 0))),0)) AS total_earned,
     // missions (tx-based)
     toInteger(coalesce(sum(CASE WHEN (toLower(coalesce(t.source,''))='sidequest' OR toLower(coalesce(t.reason,''))='sidequest_reward')
                                 THEN toInteger(coalesce(t.amount,t.eco,0)) ELSE 0 END),0)) AS missions_tx_earned,
     // eco-local scans (include either explicit source or kind)
     toInteger(coalesce(sum(CASE WHEN (toLower(coalesce(t.source,''))='eco-local' OR toLower(coalesce(t.kind,''))='scan')
                                 THEN toInteger(coalesce(t.amount,t.eco,0)) ELSE 0 END),0)) AS eco_local_tx_earned,
     coalesce(max(coalesce(t.createdAt, timestamp(t.at))), null) AS last_ms

// Virtual sidequests (approved submissions w/o PROOF tx)
OPTIONAL MATCH (u)-[:SUBMITTED]->(sub:Submission {state:'approved'})-[:FOR]->(sq:Sidequest)
WHERE NOT (sub)<-[:PROOF]-(:EcoTx)
WITH u, total_earned, missions_tx_earned, eco_local_tx_earned, last_ms,
     toInteger(coalesce(sum(toInteger(coalesce(sq.reward_eco,0))),0)) AS missions_virtual_earned,

     // raw mission count (approved submissions lifetime)
     toInteger(count(sub)) AS missions_count

RETURN
  toInteger(total_earned) AS total_earned,
  toInteger(missions_tx_earned + missions_virtual_earned) AS from_missions,
  toInteger(eco_local_tx_earned) AS from_eco_local,
  toInteger(missions_count) AS missions_completed,
  CASE WHEN last_ms IS NULL THEN NULL ELSE datetime({epochMillis:last_ms}) END AS last_dt
"""


# Period-bounded earned and missions
_YOUTH_EARN_IN_RANGE = """
MATCH (u:User {id:$uid})
WITH u

// Earned tx in window
OPTIONAL MATCH (u)-[:EARNED]->(t:EcoTx)
WHERE coalesce(t.status,'settled')='settled'
  AND toInteger(coalesce(t.createdAt, timestamp(t.at), -1)) >= $start_ms
  AND toInteger(coalesce(t.createdAt, timestamp(t.at), -1)) <  $end_ms
WITH u,
     toInteger(coalesce(sum(toInteger(coalesce(t.amount,t.eco,0))),0)) AS eco_sum,
     coalesce(max(coalesce(t.createdAt, timestamp(t.at))), null) AS last_ms

// Missions in window (approved)
OPTIONAL MATCH (u)-[:SUBMITTED]->(s:Submission {state:'approved'})
WHERE toInteger(timestamp(coalesce(s.reviewed_at, s.created_at, datetime()))) >= $start_ms
  AND toInteger(timestamp(coalesce(s.reviewed_at, s.created_at, datetime()))) <  $end_ms
WITH u, eco_sum, last_ms, toInteger(count(s)) AS missions_count

RETURN
  toInteger(eco_sum) AS eco_sum,
  toInteger(missions_count) AS missions_count,
  CASE WHEN last_ms IS NULL THEN NULL ELSE datetime({epochMillis:last_ms}) END AS last_dt
"""


# =========================================================
# Public endpoint - My Stats (by user_id and /me)
# =========================================================

def _load_user_stats(s: Session, user_id: str) -> YouthStats:
    rec = s.run(_YOUTH_EARN_LIFETIME, {"uid": user_id}).single()
    if not rec:
        raise HTTPException(status_code=404, detail="User not found")

    last_dt = rec["last_dt"]
    return YouthStats(
        user_id=user_id,
        total_eco=int(rec["total_earned"] or 0),
        eco_from_missions=int(rec["from_missions"] or 0),
        eco_from_eco_local=int(rec["from_eco_local"] or 0),
        missions_completed=int(rec["missions_completed"] or 0),
        last_earn_at=(str(last_dt) if last_dt else None),
    )


@public_router.get("/youth/{user_id}", response_model=YouthStats)
def get_youth_stats(user_id: str, s: Session = Depends(session_dep)):
    return _load_user_stats(s, user_id)


@public_router.get("/youth/me", response_model=YouthStats)
def get_my_youth_stats(me: str = Depends(current_user_id), s: Session = Depends(session_dep)):
    return _load_user_stats(s, me)


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
    # Build rows via per-user rollup (efficient enough with SKIP/LIMIT)
    base_query = f"""
    CALL {{
      MATCH (u:User)
      WITH u
      // Earned lifetime + breakdown
      OPTIONAL MATCH (u)-[:EARNED]->(t:EcoTx)
      WHERE coalesce(t.status,'settled')='settled'
      WITH u,
           toInteger(coalesce(sum(toInteger(coalesce(t.amount,t.eco,0))),0)) AS total_earned,
           toInteger(coalesce(sum(CASE WHEN (toLower(coalesce(t.source,''))='sidequest' OR toLower(coalesce(t.reason,''))='sidequest_reward')
                                       THEN toInteger(coalesce(t.amount,t.eco,0)) ELSE 0 END),0)) AS missions_tx_earned,
           toInteger(coalesce(sum(CASE WHEN (toLower(coalesce(t.source,''))='eco-local' OR toLower(coalesce(t.kind,''))='scan')
                                       THEN toInteger(coalesce(t.amount,t.eco,0)) ELSE 0 END),0)) AS eco_local_tx_earned,
           coalesce(max(coalesce(t.createdAt, timestamp(t.at))), null) AS last_ms

      // Virtual sidequests
      OPTIONAL MATCH (u)-[:SUBMITTED]->(sub:Submission {{state:'approved'}})-[:FOR]->(sq:Sidequest)
      WHERE NOT (sub)<-[:PROOF]-(:EcoTx)
      WITH u, total_earned, missions_tx_earned, eco_local_tx_earned, last_ms,
           toInteger(coalesce(sum(toInteger(coalesce(sq.reward_eco,0))),0)) AS missions_virtual_earned,
           toInteger(count(sub)) AS missions_count

      RETURN
        u.id AS user_id,
        toInteger(total_earned) AS total_eco,
        toInteger(missions_tx_earned + missions_virtual_earned) AS eco_from_missions,
        toInteger(eco_local_tx_earned) AS eco_from_eco_local,
        toInteger(missions_count) AS missions_completed,
        CASE WHEN last_ms IS NULL THEN NULL ELSE datetime({{epochMillis:last_ms}}) END AS last_dt
    }}
    WITH *
    ORDER BY {_order_clause(order).replace("last_earn_at", "last_dt")}
    SKIP $offset LIMIT $limit
    RETURN collect({{
      user_id: user_id,
      total_eco: total_eco,
      eco_from_missions: eco_from_missions,
      eco_from_eco_local: eco_from_eco_local,
      missions_completed: missions_completed,
      last_earn_at: CASE WHEN last_dt IS NULL THEN NULL ELSE toString(last_dt) END
    }}) AS rows
    """

    total_query = "MATCH (u:User) RETURN count(u) AS total"

    rows_rec = s.run(base_query, {"limit": limit, "offset": offset}).single()
    total_rec = s.run(total_query).single()

    rows = rows_rec["rows"] if rows_rec else []
    total = int(total_rec["total"]) if total_rec else 0
    return YouthStatsList(total=total, rows=rows)


@admin_router.get("/summary", response_model=OverviewStats)
def overview(s: Session = Depends(session_dep)):
    # Active = earned tx in last 30d OR approved submission in last 30d
    rec = s.run(
        """
        // total users
        MATCH (u:User)
        WITH count(u) AS total_youth

        // totals across all earned EcoTx (settled)
        OPTIONAL MATCH (:User)-[:EARNED]->(t:EcoTx)
        WHERE coalesce(t.status,'settled')='settled'
        WITH total_youth,
             toInteger(coalesce(sum(toInteger(coalesce(t.amount,t.eco,0))),0)) AS eco_minted_total,
             coalesce(max(coalesce(t.createdAt, timestamp(t.at))), null) AS last_ms

        // minted in last 24h
        CALL {
          WITH 1 AS _
          OPTIONAL MATCH (:User)-[:EARNED]->(t24:EcoTx)
          WHERE coalesce(t24.status,'settled')='settled'
            AND toInteger(coalesce(t24.createdAt, timestamp(t24.at), -1)) >= toInteger(timestamp(datetime()) - duration('P1D')) * 1000
          RETURN toInteger(coalesce(sum(toInteger(coalesce(t24.amount,t24.eco,0))),0)) AS minted_24h
        }

        // active youth last 30d: earned tx or approved submission
        CALL {
          WITH 1 AS _
          OPTIONAL MATCH (uu:User)
          OPTIONAL MATCH (uu)-[:EARNED]->(te:EcoTx)
          WHERE coalesce(te.status,'settled')='settled'
            AND toInteger(coalesce(te.createdAt, timestamp(te.at), -1)) >= toInteger(timestamp(datetime()) - duration('P30D')) * 1000
          WITH uu, count(te) AS cte
          OPTIONAL MATCH (uu)-[:SUBMITTED]->(ss:Submission {state:'approved'})
          WHERE toInteger(timestamp(coalesce(ss.reviewed_at, ss.created_at, datetime()))) >= toInteger(timestamp(datetime()) - duration('P30D')) * 1000
          WITH uu, cte, count(ss) AS css
          RETURN count(DISTINCT CASE WHEN cte>0 OR css>0 THEN uu END) AS active_youth_30d
        }

        // total approved missions (lifetime)
        CALL {
          WITH 1 AS _
          OPTIONAL MATCH (:User)-[:SUBMITTED]->(s:Submission {state:'approved'})
          RETURN toInteger(count(s)) AS missions_completed_total
        }

        RETURN {
          total_youth: toInteger(total_youth),
          active_youth_30d: toInteger(coalesce(active_youth_30d,0)),
          eco_minted_total: toInteger(coalesce(eco_minted_total,0)),
          minted_24h: toInteger(coalesce(minted_24h,0)),
          missions_completed_total: toInteger(coalesce(missions_completed_total,0)),
          last_event_at: CASE WHEN last_ms IS NULL THEN NULL ELSE toString(datetime({epochMillis:last_ms})) END
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
        s_iso, e_iso, s_ms, e_ms = _month_bounds(month)
        rows: List[TopYouthRow] = []
        for row in s.run(
            _YOUTH_EARN_IN_RANGE + """
            WITH eco_sum, missions_count, last_dt, u
            RETURN u.id AS uid,
                   toInteger(eco_sum) AS eco_period,
                   toInteger(missions_count) AS missions_period,
                   last_dt
            ORDER BY eco_period DESC, missions_period DESC, uid ASC
            LIMIT $limit
            """,
            {"uid": None, "start_ms": s_ms, "end_ms": e_ms, "limit": limit},
        ):
            # The above query needs per-user; run as a list-based rollup:
            pass  # placeholder (we'll run a set-based variant below)
        # Set-based variant (faster and correct)
        rows = []
        for r in s.run(
            """
            MATCH (u:User)
            // earned in window
            OPTIONAL MATCH (u)-[:EARNED]->(t:EcoTx)
            WHERE coalesce(t.status,'settled')='settled'
              AND toInteger(coalesce(t.createdAt, timestamp(t.at), -1)) >= $start_ms
              AND toInteger(coalesce(t.createdAt, timestamp(t.at), -1)) <  $end_ms
            WITH u,
                 toInteger(coalesce(sum(toInteger(coalesce(t.amount,t.eco,0))),0)) AS eco_sum,
                 coalesce(max(coalesce(t.createdAt, timestamp(t.at))), null) AS last_ms
            // missions in window
            OPTIONAL MATCH (u)-[:SUBMITTED]->(s1:Submission {state:'approved'})
            WHERE toInteger(timestamp(coalesce(s1.reviewed_at, s1.created_at, datetime()))) >= $start_ms
              AND toInteger(timestamp(coalesce(s1.reviewed_at, s1.created_at, datetime()))) <  $end_ms
            WITH u, eco_sum, last_ms, toInteger(count(s1)) AS missions_count
            RETURN u.id AS uid,
                   toInteger(eco_sum) AS eco_period,
                   toInteger(missions_count) AS missions_period,
                   CASE WHEN last_ms IS NULL THEN NULL ELSE datetime({epochMillis:last_ms}) END AS last_dt
            ORDER BY eco_period DESC, missions_period DESC, uid ASC
            LIMIT $limit
            """,
            {"start_ms": s_ms, "end_ms": e_ms, "limit": limit},
        ):
            rows.append(
                TopYouthRow(
                    user_id=r["uid"],
                    total_eco_period=int(r["eco_period"] or 0),
                    missions_completed_period=int(r["missions_period"] or 0),
                    last_earn_at=(str(r["last_dt"]) if r["last_dt"] else None),
                )
            )
        return TopYouthOut(month_start=s_iso, month_end=e_iso, items=rows)

    # All-time
    rows: List[TopYouthRow] = []
    for r in s.run(
        """
        MATCH (u:User)
        OPTIONAL MATCH (u)-[:EARNED]->(t:EcoTx)
        WHERE coalesce(t.status,'settled')='settled'
        WITH u,
             toInteger(coalesce(sum(toInteger(coalesce(t.amount,t.eco,0))),0)) AS eco_sum,
             coalesce(max(coalesce(t.createdAt, timestamp(t.at))), null) AS last_ms
        OPTIONAL MATCH (u)-[:SUBMITTED]->(s:Submission {state:'approved'})
        WITH u, eco_sum, last_ms, toInteger(count(s)) AS missions_count
        RETURN u.id AS uid,
               toInteger(eco_sum) AS eco_period,
               toInteger(missions_count) AS missions_period,
               CASE WHEN last_ms IS NULL THEN NULL ELSE datetime({epochMillis:last_ms}) END AS last_dt
        ORDER BY eco_period DESC, missions_period DESC, uid ASC
        LIMIT $limit
        """,
        {"limit": limit},
    ):
        rows.append(
            TopYouthRow(
                user_id=r["uid"],
                total_eco_period=int(r["eco_period"] or 0),
                missions_completed_period=int(r["missions_period"] or 0),
                last_earn_at=(str(r["last_dt"]) if r["last_dt"] else None),
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
        s_iso, e_iso, s_ms, e_ms = _month_bounds(ym)
        rec = s.run(
            """
            // monthly eco minted (earned side of ledger)
            OPTIONAL MATCH (u:User)-[:EARNED]->(t:EcoTx)
            WHERE coalesce(t.status,'settled')='settled'
              AND toInteger(coalesce(t.createdAt, timestamp(t.at), -1)) >= $start_ms
              AND toInteger(coalesce(t.createdAt, timestamp(t.at), -1)) <  $end_ms
            WITH toInteger(coalesce(sum(toInteger(coalesce(t.amount,t.eco,0))),0)) AS minted

            // active youth this month (earned tx OR approved submission)
            OPTIONAL MATCH (uu:User)
            OPTIONAL MATCH (uu)-[:EARNED]->(tt:EcoTx)
            WHERE coalesce(tt.status,'settled')='settled'
              AND toInteger(coalesce(tt.createdAt, timestamp(tt.at), -1)) >= $start_ms
              AND toInteger(coalesce(tt.createdAt, timestamp(tt.at), -1)) <  $end_ms
            WITH minted, uu, count(tt) AS cte
            OPTIONAL MATCH (uu)-[:SUBMITTED]->(ss:Submission {state:'approved'})
            WHERE toInteger(timestamp(coalesce(ss.reviewed_at, ss.created_at, datetime()))) >= $start_ms
              AND toInteger(timestamp(coalesce(ss.reviewed_at, ss.created_at, datetime()))) <  $end_ms
            WITH minted, count(DISTINCT CASE WHEN cte>0 OR count(ss)>0 THEN uu END) AS active_youth

            // total approved missions this month
            OPTIONAL MATCH (:User)-[:SUBMITTED]->(s:Submission {state:'approved'})
            WHERE toInteger(timestamp(coalesce(s.reviewed_at, s.created_at, datetime()))) >= $start_ms
              AND toInteger(timestamp(coalesce(s.reviewed_at, s.created_at, datetime()))) <  $end_ms
            RETURN toInteger(minted) AS minted,
                   toInteger(active_youth) AS active_youth,
                   toInteger(count(s)) AS missions_completed
            """,
            {"start_ms": s_ms, "end_ms": e_ms},
        ).single()

        points.append(Point(
            month=ym,
            minted_eco=int(rec["minted"] or 0),
            active_youth=int(rec["active_youth"] or 0),
            missions_completed=int(rec["missions_completed"] or 0),
        ))

    return TimeSeriesOut(from_month=from_month, to_month=to_month, points=points)
