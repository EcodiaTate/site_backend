from __future__ import annotations
from typing import List, Dict, Optional, Tuple
from datetime import datetime, timedelta, timezone
from neo4j import Session

# ───────────────────────────────────────────────────────────────────────────────
# Time helpers
# ───────────────────────────────────────────────────────────────────────────────

def _now_utc() -> datetime:
    return datetime.now(timezone.utc)

def _week_bounds_utc(dt: datetime) -> Tuple[str, str]:
    # ISO week starting Monday
    start = (dt - timedelta(days=dt.weekday())).replace(hour=0, minute=0, second=0, microsecond=0)
    end = start + timedelta(days=7)
    return start.isoformat(), end.isoformat()

def _month_bounds_utc(dt: datetime) -> Tuple[str, str]:
    start = dt.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    if start.month == 12:
        end = start.replace(year=start.year + 1, month=1)
    else:
        end = start.replace(month=start.month + 1)
    return start.isoformat(), end.isoformat()

def _period_bounds(period: str) -> Tuple[Optional[str], Optional[str]]:
    """
    period: 'total' | 'weekly' | 'monthly'
    Returns (start_iso, end_iso) or (None, None) for 'total'.
    """
    p = (period or "total").lower()
    now = _now_utc()
    if p == "weekly":
        return _week_bounds_utc(now)
    if p == "monthly":
        return _month_bounds_utc(now)
    return None, None

# ───────────────────────────────────────────────────────────────────────────────
# Leaderboards
# ───────────────────────────────────────────────────────────────────────────────

def top_youth_eco(s: Session, *, period: str, limit: int = 20, offset: int = 0) -> List[Dict]:
    start, end = _period_bounds(period)
    recs = s.run(
        """
        MATCH (u:User)
        OPTIONAL MATCH (u)-[:EARNED]->(t:EcoTx)
          WHERE $start IS NULL OR (t.at >= datetime($start) AND t.at < datetime($end))
        WITH u, toInteger(sum(coalesce(t.eco,0))) AS eco
        RETURN { user_id: u.id, eco: eco } AS row
        ORDER BY row.eco DESC, row.user_id ASC
        SKIP $offset LIMIT $limit
        """,
        start=start, end=end, limit=limit, offset=offset,
    )
    return [dict(r["row"]) for r in recs]

# site_backend/api/leaderboards/service.py

def top_business_eco(s: Session, *, period: str, limit: int = 20, offset: int = 0) -> List[Dict]:
    start, end = _period_bounds(period)
    recs = s.run(
        """
        // Pull businesses, compute safe string id/name, then aggregate
        MATCH (b:BusinessProfile)
        WITH b,
             coalesce(toString(b.id), '') AS bid,
             coalesce(toString(b.name), toString(b.id), 'Business') AS bname
        WHERE bid <> ''  // ensure we never emit null/empty ids

        OPTIONAL MATCH (t:EcoTx)-[:FROM]->(b)
          // guard null timestamps; only time-filter when t.at exists
          WHERE $start IS NULL
             OR (t.at IS NOT NULL AND t.at >= datetime($start) AND t.at < datetime($end))

        WITH bid, bname, toInteger(sum(coalesce(t.eco,0))) AS eco
        RETURN {
          business_id: bid,
          name: bname,
          eco: eco
        } AS row
        ORDER BY row.eco DESC, row.business_id ASC
        SKIP $offset LIMIT $limit
        """,
        start=start, end=end, limit=limit, offset=offset,
    )
    return [dict(r["row"]) for r in recs]

def top_youth_actions(
    s: Session,
    *,
    period: str,
    mission_type: Optional[str] = None,  # 'eco_action' | 'sidequest' | None/'all'
    limit: int = 20,
    offset: int = 0,
) -> List[Dict]:
    start, end = _period_bounds(period)
    mtype = None if (mission_type in (None, "", "all")) else mission_type

    recs = s.run(
        """
        MATCH (u:User)
        OPTIONAL MATCH (u)-[:SUBMITTED]->(sub:Submission {state:'approved'})-[:FOR]->(m:Mission)
          WHERE ($start IS NULL OR (datetime(sub.created_at) >= datetime($start) AND datetime(sub.created_at) < datetime($end)))
            AND ($mtype IS NULL OR m.type = $mtype)
        WITH u, count(sub) AS completed
        RETURN { user_id: u.id, completed: toInteger(completed) } AS row
        ORDER BY row.completed DESC, row.user_id ASC
        SKIP $offset LIMIT $limit
        """,
        start=start, end=end, mtype=mtype, limit=limit, offset=offset,
    )
    return [dict(r["row"]) for r in recs]
