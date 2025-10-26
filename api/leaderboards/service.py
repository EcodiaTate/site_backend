# site_backend/api/leaderboards/service.py
from __future__ import annotations
from typing import List, Optional, Literal, Dict, Any
from datetime import datetime, timedelta, timezone
from neo4j import Session

Period = Literal["total", "weekly", "monthly"]

def _now() -> datetime:
    return datetime.now(timezone.utc)

def _since_ms(period: Period) -> Optional[int]:
    if period == "total":
        return None
    days = 7 if period == "weekly" else 30
    dt = _now() - timedelta(days=days)
    # UTC day start for stable windows
    d0 = datetime(dt.year, dt.month, dt.day, tzinfo=timezone.utc)
    return int(d0.timestamp() * 1000)

def _display_name_expr() -> str:
    return (
        "coalesce("
        "  u.display_name, "
        "  u.first_name, "
        "  u.given_name, "
        "  (CASE WHEN u.email IS NOT NULL THEN split(u.email,'@')[0] END), "
        "  right(u.id, 6)"
        ")"
    )

def _has_more(count_page: int, limit: int) -> bool:
    return count_page == limit

# ───────────────────────────────────────────────────────────────────────────────
# Youth ECO leaderboard (EARNED)
# ───────────────────────────────────────────────────────────────────────────────

def top_youth_eco(
    s: Session,
    period: Period = "total",
    limit: int = 20,
    offset: int = 0,
    me_user_id: Optional[str] = None,
) -> Dict[str, Any]:
    since = _since_ms(period)

    rows = s.run(
        f"""
        CALL {{
          WITH *
          MATCH (u:User)
          OPTIONAL MATCH (u)-[:EARNED]->(t:EcoTx)
            WHERE coalesce(t.status,'settled')='settled'
              AND ($since IS NULL OR t.createdAt >= $since)
          RETURN u, coalesce(sum(toInteger(t.amount)),0) AS eco
        }}
        WITH u, toInteger(eco) AS eco
        WITH u, eco, {_display_name_expr()} AS display_name, u.avatar_url AS avatar_url
        RETURN u.id AS user_id, display_name, eco, avatar_url
        ORDER BY eco DESC, user_id ASC
        SKIP $offset LIMIT $limit
        """,
        since=since, offset=offset, limit=limit,
    ).data()

    items = [
        {
            "user_id": r["user_id"],
            "display_name": r.get("display_name") or r["user_id"][-6:],
            "eco": int(r.get("eco", 0) or 0),
            "avatar_url": r.get("avatar_url"),
        } for r in rows
    ]

    top_q = s.run(
        """
        CALL {
          WITH *
          MATCH (u:User)
          OPTIONAL MATCH (u)-[:EARNED]->(t:EcoTx)
            WHERE coalesce(t.status,'settled')='settled'
              AND ($since IS NULL OR t.createdAt >= $since)
          RETURN toInteger(coalesce(sum(toInteger(t.amount)),0)) AS eco
        }
        RETURN coalesce(max(eco), 0) AS top_value
        """,
        since=since,
    ).single()
    top_value = int(top_q["top_value"] or 0)

    tot_q = s.run("MATCH (u:User) RETURN count(u) AS n").single()
    total_estimate = int(tot_q["n"] or 0)

    has_more = _has_more(len(items), limit)

    meta_my = None
    if me_user_id:
        my_row = s.run(
            f"""
            // my value
            MATCH (u:User {{id: $uid}})
            OPTIONAL MATCH (u)-[:EARNED]->(t:EcoTx)
              WHERE coalesce(t.status,'settled')='settled'
                AND ($since IS NULL OR t.createdAt >= $since)
            WITH u, toInteger(coalesce(sum(toInteger(t.amount)),0)) AS my_eco
            WITH u, my_eco, {_display_name_expr()} AS display_name, u.avatar_url AS avatar_url

            // strictly higher than me in same period
            CALL {{
              WITH my_eco, $since AS since
              MATCH (u2:User)
              OPTIONAL MATCH (u2)-[:EARNED]->(t2:EcoTx)
                WHERE coalesce(t2.status,'settled')='settled'
                  AND (since IS NULL OR t2.createdAt >= since)
              WITH toInteger(coalesce(sum(toInteger(t2.amount)),0)) AS eco2, my_eco
              WHERE eco2 > my_eco
              RETURN count(*) AS higher
            }}
            RETURN u.id AS user_id, display_name, avatar_url, my_eco AS value, (1 + higher) AS rank
            """,
            uid=me_user_id, since=since,
        ).single()
        if my_row:
            meta_my = {
                "id": my_row["user_id"],
                "value": int(my_row["value"] or 0),
                "rank": int(my_row["rank"] or 1),
                "display_name": my_row.get("display_name"),
                "avatar_url": my_row.get("avatar_url"),
            }

    return {
        "items": items,
        "meta": {
            "period": period,
            "since_ms": since,
            "limit": limit,
            "offset": offset,
            "has_more": has_more,
            "total_estimate": total_estimate,
            "top_value": top_value,
            "my": meta_my,
        },
    }

# ───────────────────────────────────────────────────────────────────────────────
# Business ECO leaderboard (TRIGGERED)
# ───────────────────────────────────────────────────────────────────────────────

def top_business_eco(
    s: Session,
    period: Period = "total",
    limit: int = 20,
    offset: int = 0,
    me_business_id: Optional[str] = None,
) -> Dict[str, Any]:
    since = _since_ms(period)

    rows = s.run(
        """
        MATCH (b:BusinessProfile)
        WHERE b.id IS NOT NULL
        OPTIONAL MATCH (b)-[:TRIGGERED]->(t:EcoTx)
          WHERE coalesce(t.status,'settled')='settled'
            AND ($since IS NULL OR t.createdAt >= $since)
        RETURN b.id AS business_id,
               coalesce(b.name,'(Unnamed Business)') AS name,
               toInteger(coalesce(sum(toInteger(t.amount)),0)) AS eco
        ORDER BY eco DESC, business_id ASC
        SKIP $offset LIMIT $limit
        """,
        since=since, offset=offset, limit=limit,
    ).data()

    items = [
        {
            "business_id": r["business_id"],  # guaranteed non-null due to WHERE b.id IS NOT NULL
            "name": r.get("name") or "(Unnamed Business)",
            "eco": int(r.get("eco", 0) or 0),
        } for r in rows
    ]

    top_value = int(s.run(
        """
        CALL {
          WITH *
          MATCH (b:BusinessProfile)
          WHERE b.id IS NOT NULL
          OPTIONAL MATCH (b)-[:TRIGGERED]->(t:EcoTx)
            WHERE coalesce(t.status,'settled')='settled'
              AND ($since IS NULL OR t.createdAt >= $since)
          RETURN toInteger(coalesce(sum(toInteger(t.amount)),0)) AS eco
        }
        RETURN coalesce(max(eco),0) AS top_value
        """, since=since).single()["top_value"] or 0)

    total_estimate = int(s.run("MATCH (b:BusinessProfile) WHERE b.id IS NOT NULL RETURN count(b) AS n").single()["n"] or 0)
    has_more = _has_more(len(items), limit)

    meta_my = None
    if me_business_id:
        my_row = s.run(
            """
            MATCH (b:BusinessProfile {id: $bid})
            OPTIONAL MATCH (b)-[:TRIGGERED]->(t:EcoTx)
              WHERE coalesce(t.status,'settled')='settled'
                AND ($since IS NULL OR t.createdAt >= $since)
            WITH b, toInteger(coalesce(sum(toInteger(t.amount)),0)) AS my_eco, coalesce(b.name,'(Unnamed Business)') AS name
            CALL {
              WITH my_eco, $since AS since
              MATCH (b2:BusinessProfile)
              WHERE b2.id IS NOT NULL
              OPTIONAL MATCH (b2)-[:TRIGGERED]->(t2:EcoTx)
                WHERE coalesce(t2.status,'settled')='settled'
                  AND (since IS NULL OR t2.createdAt >= since)
              WITH toInteger(coalesce(sum(toInteger(t2.amount)),0)) AS eco2, my_eco
              WHERE eco2 > my_eco
              RETURN count(*) AS higher
            }
            RETURN b.id AS business_id, name, my_eco AS value, (1 + higher) AS rank
            """,
            bid=me_business_id, since=since
        ).single()
        if my_row:
            meta_my = {
                "id": my_row["business_id"],
                "value": int(my_row["value"] or 0),
                "rank": int(my_row["rank"] or 1),
                "display_name": my_row["name"],
            }

    return {
        "items": items,
        "meta": {
            "period": period,
            "since_ms": since,
            "limit": limit,
            "offset": offset,
            "has_more": has_more,
            "total_estimate": total_estimate,
            "top_value": top_value,
            "my": meta_my,
        },
    }

# ───────────────────────────────────────────────────────────────────────────────
# Youth Actions leaderboard (sidequests)
# ───────────────────────────────────────────────────────────────────────────────

def top_youth_actions(
    s: Session,
    period: Period = "total",
    mission_type: Optional[str] = None,  # 'eco_action' | 'sidequest' | 'all' | None
    limit: int = 20,
    offset: int = 0,
    me_user_id: Optional[str] = None,
) -> Dict[str, Any]:
    since = _since_ms(period)

    rows = s.run(
        f"""
        CALL {{
          WITH *
          MATCH (u:User)
          OPTIONAL MATCH (u)-[:SUBMITTED]->(ms:MissionSubmission)
            WHERE coalesce(ms.status,'approved')='approved'
              AND ($since IS NULL OR coalesce(ms.completedAt, ms.approvedAt, ms.createdAt, 0) >= $since)
              AND ($kind IS NULL OR $kind='all' OR ms.kind = $kind)
          RETURN u, toInteger(count(ms)) AS completed
        }}
        WITH u, completed, {_display_name_expr()} AS display_name, u.avatar_url AS avatar_url
        RETURN u.id AS user_id, display_name, completed, avatar_url
        ORDER BY completed DESC, user_id ASC
        SKIP $offset LIMIT $limit
        """,
        since=since, kind=mission_type, offset=offset, limit=limit,
    ).data()

    items = [
        {
            "user_id": r["user_id"],
            "display_name": r.get("display_name") or r["user_id"][-6:],
            "completed": int(r.get("completed", 0) or 0),
            "avatar_url": r.get("avatar_url"),
        } for r in rows
    ]

    top_value = int(s.run(
        """
        CALL {
          WITH *
          MATCH (u:User)
          OPTIONAL MATCH (u)-[:SUBMITTED]->(ms:MissionSubmission)
            WHERE coalesce(ms.status,'approved')='approved'
              AND ($since IS NULL OR coalesce(ms.completedAt, ms.approvedAt, ms.createdAt, 0) >= $since)
              AND ($kind IS NULL OR $kind='all' OR ms.kind = $kind)
          RETURN toInteger(count(ms)) AS c
        }
        RETURN coalesce(max(c),0) AS top_value
        """, since=since, kind=mission_type).single()["top_value"] or 0)

    total_estimate = int(s.run("MATCH (u:User) RETURN count(u) AS n").single()["n"] or 0)
    has_more = _has_more(len(items), limit)

    meta_my = None
    if me_user_id:
        my_row = s.run(
            f"""
            MATCH (u:User {{id: $uid}})
            OPTIONAL MATCH (u)-[:SUBMITTED]->(ms:MissionSubmission)
              WHERE coalesce(ms.status,'approved')='approved'
                AND ($since IS NULL OR coalesce(ms.completedAt, ms.approvedAt, ms.createdAt, 0) >= $since)
                AND ($kind IS NULL OR $kind='all' OR ms.kind = $kind)
            WITH u, toInteger(count(ms)) AS my_completed
            WITH u, my_completed, {_display_name_expr()} AS display_name, u.avatar_url AS avatar_url
            CALL {{
              WITH my_completed, $since AS since, $kind AS kind
              MATCH (u2:User)
              OPTIONAL MATCH (u2)-[:SUBMITTED]->(ms2:MissionSubmission)
                WHERE coalesce(ms2.status,'approved')='approved'
                  AND (since IS NULL OR coalesce(ms2.completedAt, ms2.approvedAt, ms2.createdAt, 0) >= since)
                  AND (kind IS NULL OR kind='all' OR ms2.kind = kind)
              WITH toInteger(count(ms2)) AS c2, my_completed
              WHERE c2 > my_completed
              RETURN count(*) AS higher
            }}
            RETURN u.id AS user_id, display_name, avatar_url, my_completed AS value, (1 + higher) AS rank
            """,
            uid=me_user_id, since=since, kind=mission_type
        ).single()
        if my_row:
            meta_my = {
                "id": my_row["user_id"],
                "value": int(my_row["value"] or 0),
                "rank": int(my_row["rank"] or 1),
                "display_name": my_row.get("display_name"),
                "avatar_url": my_row.get("avatar_url"),
            }

    return {
        "items": items,
        "meta": {
            "period": period,
            "since_ms": since,
            "limit": limit,
            "offset": offset,
            "has_more": has_more,
            "total_estimate": total_estimate,
            "top_value": top_value,
            "my": meta_my,
        },
    }
