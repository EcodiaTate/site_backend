from __future__ import annotations
from typing import Optional, Literal, Dict, Any
from datetime import datetime, timedelta, timezone
from neo4j import Session

Period = Literal["total", "weekly", "monthly"]

# ───────────────────────────────────────────────────────────────────────────────
# Time helpers
# ───────────────────────────────────────────────────────────────────────────────

def _now() -> datetime:
    return datetime.now(timezone.utc)

def _since_ms(period: Period) -> Optional[int]:
    if period == "total":
        return None
    days = 7 if period == "weekly" else 30
    dt = _now() - timedelta(days=days)
    d0 = datetime(dt.year, dt.month, dt.day, tzinfo=timezone.utc)  # UTC midnight for stable windows
    return int(d0.timestamp() * 1000)

def _has_more(count_page: int, limit: int) -> bool:
    return count_page == limit

# Robust, APOC-free millisecond extraction for a tx node variable
def _tx_ms_expr(var: str = "tx") -> str:
    v = var
    return (
        "coalesce("
        f"  toInteger({v}.createdAt),"
        f"  CASE "
        f"    WHEN {v}.created_at IS NULL THEN NULL "
        f"    WHEN toString({v}.created_at) =~ '^[0-9]+$' THEN toInteger({v}.created_at) "
        f"    ELSE toInteger(datetime({v}.created_at).epochMillis) "
        f"  END,"
        f"  toInteger(timestamp({v}.at)),"
        "  0"
        ")"
    )

# ───────────────────────────────────────────────────────────────────────────────
# Display name / role helpers (canonical)
# ───────────────────────────────────────────────────────────────────────────────

def _display_name_expr_user() -> str:
    return (
        "coalesce("
        "  u.display_name, "
        "  u.first_name, "
        "  u.given_name, "
        "  (CASE WHEN u.email IS NOT NULL THEN split(u.email,'@')[0] END), "
        "  right(u.id, 6)"
        ")"
    )

def _display_name_expr_business() -> str:
    return (
        "coalesce("
        "  b.display_name, "
        "  b.name, "
        "  right(b.id, 6)"
        ")"
    )

# Any sign a user is actually a business actor
def _user_is_business_predicate(alias: str = "u") -> str:
    a = alias
    return (
        "("
        f"  ({a})-[:OWNS|MANAGES|WORKS_AT|STAFF_OF|REPRESENTS]->(:BusinessProfile)"
        f"  OR coalesce({a}.is_business, false) = true"
        f"  OR coalesce({a}.has_business, false) = true"
        f"  OR coalesce({a}.business_id, '') <> ''"
        ")"
    )

def _where_user_is_youth(alias: str = "u") -> str:
    return f"WHERE NOT {_user_is_business_predicate(alias)}"

# ───────────────────────────────────────────────────────────────────────────────
# Youth ECO leaderboard (EARNED) — wallet parity for earned side
# ───────────────────────────────────────────────────────────────────────────────

def top_youth_eco(
    s: Session,
    period: Period = "total",
    limit: int = 20,
    offset: int = 0,
    me_user_id: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Youth ECO *earned* leaderboard with wallet parity:
    A) Real settled EARNED EcoTx (MINT_ACTION | source=sidequest | reason=sidequest_reward)
    B) Virtual: approved Submissions with no PROOF-linked EcoTx (sum sq.reward_eco)
    Both constrained by the period window.
    """
    since = _since_ms(period)

    # ---------- main rows ----------
    rows = s.run(
        f"""
        // Compute per-user eco_real + eco_virtual (wallet parity)
        CALL () {{
          MATCH (u:User)
          {_where_user_is_youth('u')}

          // A) Real earned EcoTx
          OPTIONAL MATCH (u)-[:EARNED]->(t:EcoTx)
          WITH u,
               coalesce(
                 toInteger(t.createdAt),
                 CASE
                   WHEN t.created_at IS NULL THEN NULL
                   WHEN toString(t.created_at) =~ '^[0-9]+$' THEN toInteger(t.created_at)
                   ELSE toInteger(datetime(t.created_at).epochMillis)
                 END,
                 toInteger(timestamp(t.at)),
                 0
               ) AS t_ms,
               t
          WITH u,
               CASE
                 WHEN t IS NULL THEN 0
                 WHEN coalesce(t.status,'settled')='settled'
                      AND (
                           t.kind   IN ['MINT_ACTION'] OR
                           t.source =  'sidequest'    OR
                           t.reason =  'sidequest_reward'
                          )
                      AND ($since IS NULL OR t_ms >= $since)
                 THEN toInteger(coalesce(t.eco, t.amount))
                 ELSE 0
               END AS eco_real_piece
          WITH u, sum(eco_real_piece) AS eco_real

          // B) Virtual sidequests (approved, no PROOF EcoTx)
          OPTIONAL MATCH (u)-[:SUBMITTED]->(sub:Submission {{state:'approved'}})-[:FOR]->(sq:Sidequest)
          WHERE NOT (sub)<-[:PROOF]-(:EcoTx)
          WITH u, eco_real,
               toInteger(timestamp(coalesce(sub.reviewed_at, sub.created_at, datetime()))) AS sub_ms,
               toInteger(coalesce(sq.reward_eco,0)) AS reward_eco
          WITH u, eco_real,
               CASE WHEN $since IS NULL OR sub_ms >= $since THEN reward_eco ELSE 0 END AS eco_virtual_piece
          WITH u, eco_real, sum(eco_virtual_piece) AS eco_virtual

          RETURN u, toInteger(coalesce(eco_real,0) + coalesce(eco_virtual,0)) AS eco
        }}

        WITH u, eco,
             {_display_name_expr_user()} AS display_name,
             u.avatar_url AS avatar_url
        RETURN u.id AS user_id, display_name, eco, avatar_url
        ORDER BY eco DESC, user_id ASC
        SKIP $offset LIMIT $limit
        """,
        since=since, offset=offset, limit=limit,
    ).data()

    items = [
        {
            "user_id": r["user_id"],
            "display_name": (r.get("display_name") or r["user_id"][-6:]),
            "eco": int(r.get("eco", 0) or 0),
            "avatar_url": r.get("avatar_url"),
        } for r in rows
    ]

    # ---------- top_value ----------
    top_q = s.run(
        f"""
        CALL () {{
          MATCH (u:User)
          {_where_user_is_youth('u')}

          // A) Real
          OPTIONAL MATCH (u)-[:EARNED]->(t:EcoTx)
          WITH u,
               coalesce(
                 toInteger(t.createdAt),
                 CASE
                   WHEN t.created_at IS NULL THEN NULL
                   WHEN toString(t.created_at) =~ '^[0-9]+$' THEN toInteger(t.created_at)
                   ELSE toInteger(datetime(t.created_at).epochMillis)
                 END,
                 toInteger(timestamp(t.at)),
                 0
               ) AS t_ms,
               t
          WITH u,
               CASE
                 WHEN t IS NULL THEN 0
                 WHEN coalesce(t.status,'settled')='settled'
                      AND (
                           t.kind   IN ['MINT_ACTION'] OR
                           t.source =  'sidequest'    OR
                           t.reason =  'sidequest_reward'
                          )
                      AND ($since IS NULL OR t_ms >= $since)
                 THEN toInteger(coalesce(t.eco, t.amount))
                 ELSE 0
               END AS eco_real_piece
          WITH u, sum(eco_real_piece) AS eco_real

          // B) Virtual
          OPTIONAL MATCH (u)-[:SUBMITTED]->(sub:Submission {{state:'approved'}})-[:FOR]->(sq:Sidequest)
          WHERE NOT (sub)<-[:PROOF]-(:EcoTx)
          WITH u, eco_real,
               toInteger(timestamp(coalesce(sub.reviewed_at, sub.created_at, datetime()))) AS sub_ms,
               toInteger(coalesce(sq.reward_eco,0)) AS reward_eco
          WITH u, eco_real,
               CASE WHEN $since IS NULL OR sub_ms >= $since THEN reward_eco ELSE 0 END AS eco_virtual_piece
          WITH u, eco_real, sum(eco_virtual_piece) AS eco_virtual

          RETURN toInteger(coalesce(eco_real,0) + coalesce(eco_virtual,0)) AS eco
        }}
        RETURN coalesce(max(eco), 0) AS top_value
        """,
        since=since,
    ).single()
    top_value = int(top_q["top_value"] or 0)

    tot_q = s.run(f"MATCH (u:User) {_where_user_is_youth('u')} RETURN count(u) AS n").single()
    total_estimate = int(tot_q["n"] or 0)
    has_more = _has_more(len(items), limit)

    # ---------- my ----------
    meta_my = None
    if me_user_id:
        elig = s.run(
            f"""
            MATCH (u:User {{id:$uid}})
            RETURN {_user_is_business_predicate('u')} AS has_biz
            """,
            uid=me_user_id,
        ).single()
        if elig and not elig["has_biz"]:
            my_row = s.run(
                f"""
                // my value (real + virtual)
                CALL () {{
                  MATCH (u:User {{id: $uid}})
                  {_where_user_is_youth('u')}

                  // A) Real
                  OPTIONAL MATCH (u)-[:EARNED]->(t:EcoTx)
                  WITH u,
                      coalesce(
                        toInteger(t.createdAt),
                        CASE
                          WHEN t.created_at IS NULL THEN NULL
                          WHEN toString(t.created_at) =~ '^[0-9]+$' THEN toInteger(t.created_at)
                          ELSE toInteger(datetime(t.created_at).epochMillis)
                        END,
                        toInteger(timestamp(t.at)),
                        0
                      ) AS t_ms,
                      t
                  WITH u,
                      CASE
                        WHEN t IS NULL THEN 0
                        WHEN coalesce(t.status,'settled')='settled'
                              AND (
                                  t.kind   IN ['MINT_ACTION'] OR
                                  t.source =  'sidequest'    OR
                                  t.reason =  'sidequest_reward'
                                  )
                              AND ($since IS NULL OR t_ms >= $since)
                        THEN toInteger(coalesce(t.eco, t.amount))
                        ELSE 0
                      END AS eco_real_piece
                  WITH u, sum(eco_real_piece) AS eco_real

                  // B) Virtual
                  OPTIONAL MATCH (u)-[:SUBMITTED]->(sub:Submission {{state:'approved'}})-[:FOR]->(sq:Sidequest)
                  WHERE NOT (sub)<-[:PROOF]-(:EcoTx)
                  WITH u, eco_real,
                      toInteger(timestamp(coalesce(sub.reviewed_at, sub.created_at, datetime()))) AS sub_ms,
                      toInteger(coalesce(sq.reward_eco,0)) AS reward_eco
                  WITH u, eco_real,
                      CASE WHEN $since IS NULL OR sub_ms >= $since THEN reward_eco ELSE 0 END AS eco_virtual_piece
                  WITH u, eco_real, sum(eco_virtual_piece) AS eco_virtual

                  RETURN u, toInteger(coalesce(eco_real,0) + coalesce(eco_virtual,0)) AS my_eco
                }}

                WITH u, my_eco, {_display_name_expr_user()} AS display_name, u.avatar_url AS avatar_url

                // rank = 1 + number of eligible youth strictly higher than me
                CALL {{
                  WITH my_eco
                  MATCH (u2:User)
                  {_where_user_is_youth('u2')}

                  // A) Real for others
                  OPTIONAL MATCH (u2)-[:EARNED]->(t2:EcoTx)
                  WITH u2, my_eco,
                      coalesce(
                        toInteger(t2.createdAt),
                        CASE
                          WHEN t2.created_at IS NULL THEN NULL
                          WHEN toString(t2.created_at) =~ '^[0-9]+$' THEN toInteger(t2.created_at)
                          ELSE toInteger(datetime(t2.created_at).epochMillis)
                        END,
                        toInteger(timestamp(t2.at)),
                        0
                      ) AS t2_ms,
                      t2
                  WITH u2, my_eco,
                      CASE
                        WHEN t2 IS NULL THEN 0
                        WHEN coalesce(t2.status,'settled')='settled'
                              AND (
                                  t2.kind   IN ['MINT_ACTION'] OR
                                  t2.source =  'sidequest'    OR
                                  t2.reason =  'sidequest_reward'
                                  )
                              AND ($since IS NULL OR t2_ms >= $since)
                        THEN toInteger(coalesce(t2.eco, t2.amount))
                        ELSE 0
                      END AS eco_real_piece
                  WITH u2, my_eco, sum(eco_real_piece) AS eco_real2

                  // B) Virtual for others (approved, no PROOF EcoTx) — windowed sum of reward_eco
                  OPTIONAL MATCH (u2)-[:SUBMITTED]->(sub2:Submission {{state:'approved'}})-[:FOR]->(sq2:Sidequest)
                  WHERE NOT (sub2)<-[:PROOF]-(:EcoTx)
                  WITH u2, my_eco, eco_real2, sub2, sq2,
                      toInteger(timestamp(coalesce(sub2.reviewed_at, sub2.created_at, datetime()))) AS sub2_ms,
                      toInteger(coalesce(sq2.reward_eco,0)) AS reward_eco2
                  WITH my_eco, eco_real2,
                      CASE WHEN $since IS NULL OR sub2_ms >= $since THEN reward_eco2 ELSE 0 END AS eco_virtual_piece
                  WITH my_eco, eco_real2, sum(eco_virtual_piece) AS eco_virtual2

                  WITH my_eco, toInteger(eco_real2) + toInteger(eco_virtual2) AS eco2
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
                    "display_name": my_row.get("display_name") or str(my_row["user_id"])[-6:],
                    "avatar_url": my_row.get("avatar_url"),
                }

    return {
        "items": items,
        "meta": {
            "period": period,
            "since_ms": since,
            "limit": limit,
            "offset": offset,
            "has_more": _has_more(len(items), limit),
            "total_estimate": total_estimate,
            "top_value": top_value,
            "my": meta_my,
        },
    }


# ───────────────────────────────────────────────────────────────────────────────
# Youth ECO leaderboard (CONTRIBUTED → businesses)
# ───────────────────────────────────────────────────────────────────────────────

def top_youth_contributed(
    s: Session,
    period: Period = "total",
    limit: int = 20,
    offset: int = 0,
    me_user_id: Optional[str] = None,
) -> Dict[str, Any]:
    since = _since_ms(period)
    tx_ms = _tx_ms_expr("tx")

    rows = s.run(
        f"""
        CALL () {{
          MATCH (u:User)
          {_where_user_is_youth('u')}
          OPTIONAL MATCH (u)-[:SPENT|SENT|FROM|CONTRIBUTED]->(tx:EcoTx)
          OPTIONAL MATCH (b:BusinessProfile)-[:COLLECTED]->(tx)
          WITH u, tx, b, {tx_ms} AS tx_ms
          WHERE tx IS NULL OR (
            b IS NOT NULL
            AND coalesce(tx.status,'settled')='settled'
            AND (
              coalesce(tx.kind,'') IN ['CONTRIBUTE'] OR
              tx.source = 'contribution'
            )
            AND ($since IS NULL OR tx_ms >= $since)
          )
          RETURN u, toInteger(coalesce(sum(toInteger(coalesce(tx.amount, tx.eco, 0))),0)) AS eco
        }}
        WITH u, eco, {_display_name_expr_user()} AS display_name, u.avatar_url AS avatar_url
        RETURN u.id AS user_id, display_name, toInteger(eco) AS eco, avatar_url
        ORDER BY eco DESC, user_id ASC
        SKIP $offset LIMIT $limit
        """,
        since=since, offset=offset, limit=limit,
    ).data()

    items = [
        {
            "user_id": r["user_id"],
            "display_name": (r.get("display_name") or r["user_id"][-6:]),
            "eco": int(r.get("eco", 0) or 0),
            "avatar_url": r.get("avatar_url"),
        } for r in rows
    ]

    top_value = int(s.run(
        f"""
        CALL () {{
          MATCH (u:User)
          {_where_user_is_youth('u')}
          OPTIONAL MATCH (u)-[:SPENT|SENT|FROM|CONTRIBUTED]->(tx:EcoTx)
          OPTIONAL MATCH (b:BusinessProfile)-[:COLLECTED]->(tx)
          WITH tx, b, {tx_ms} AS tx_ms
          WHERE tx IS NULL OR (
            b IS NOT NULL
            AND coalesce(tx.status,'settled')='settled'
            AND (
              coalesce(tx.kind,'') IN ['CONTRIBUTE'] OR
              tx.source = 'contribution'
            )
            AND ($since IS NULL OR tx_ms >= $since)
          )
          RETURN toInteger(coalesce(sum(toInteger(coalesce(tx.amount, tx.eco, 0))),0)) AS eco
        }}
        RETURN coalesce(max(eco),0) AS top_value
        """,
        since=since,
    ).single()["top_value"] or 0)

    total_estimate = int(s.run(
        f"MATCH (u:User) {_where_user_is_youth('u')} RETURN count(u) AS n"
    ).single()["n"] or 0)

    meta_my = None
    if me_user_id:
        elig = s.run(
            f"""
            MATCH (u:User {{id:$uid}})
            RETURN {_user_is_business_predicate('u')} AS has_biz
            """,
            uid=me_user_id,
        ).single()
        if elig and not elig["has_biz"]:
            my = s.run(
                f"""
                MATCH (u:User {{id:$uid}})
                {_where_user_is_youth('u')}
                OPTIONAL MATCH (u)-[:SPENT|SENT|FROM|CONTRIBUTED]->(tx:EcoTx)
                OPTIONAL MATCH (b:BusinessProfile)-[:COLLECTED]->(tx)
                WITH u, tx, b, {_tx_ms_expr('tx')} AS tx_ms
                WHERE tx IS NULL OR (
                  b IS NOT NULL
                  AND coalesce(tx.status,'settled')='settled'
                  AND (
                    coalesce(tx.kind,'') IN ['CONTRIBUTE'] OR
                    tx.source = 'contribution'
                  )
                  AND ($since IS NULL OR tx_ms >= $since)
                )
                WITH u, toInteger(coalesce(sum(toInteger(coalesce(tx.amount, tx.eco, 0))),0)) AS my_eco,
                     {_display_name_expr_user()} AS display_name, u.avatar_url AS avatar_url
                CALL {{
                  WITH my_eco
                  MATCH (u2:User)
                  {_where_user_is_youth('u2')}
                  OPTIONAL MATCH (u2)-[:SPENT|SENT|FROM|CONTRIBUTED]->(tx2:EcoTx)
                  OPTIONAL MATCH (b2:BusinessProfile)-[:COLLECTED]->(tx2)
                  WITH tx2, b2, my_eco, {_tx_ms_expr('tx2')} AS tx2_ms
                  WHERE tx2 IS NULL OR (
                    b2 IS NOT NULL
                    AND coalesce(tx2.status,'settled')='settled'
                    AND (
                      coalesce(tx2.kind,'') IN ['CONTRIBUTE'] OR
                      tx2.source = 'contribution'
                    )
                    AND ($since IS NULL OR tx2_ms >= $since)
                  )
                  WITH toInteger(coalesce(sum(toInteger(coalesce(tx2.amount, tx2.eco, 0))),0)) AS eco2, my_eco
                  WHERE eco2 > my_eco
                  RETURN count(*) AS higher
                }}
                RETURN u.id AS user_id, display_name, avatar_url, my_eco AS value, (1 + higher) AS rank
                """,
                uid=me_user_id, since=since
            ).single()
            if my:
                meta_my = {
                    "id": my["user_id"],
                    "value": int(my["value"] or 0),
                    "rank": int(my["rank"] or 1),
                    "display_name": my.get("display_name") or str(my["user_id"])[-6:],
                    "avatar_url": my.get("avatar_url"),
                }

    return {
        "items": items,
        "meta": {
            "period": period,
            "since_ms": since,
            "limit": limit,
            "offset": offset,
            "has_more": _has_more(len(items), limit),
            "total_estimate": total_estimate,
            "top_value": top_value,
            "my": meta_my,
        },
    }

# ───────────────────────────────────────────────────────────────────────────────
# Business ECO leaderboard — COLLECTED (wallet parity)
# ───────────────────────────────────────────────────────────────────────────────
# ⛑️ CHANGE: coalesce business avatar from (b.avatar_url) or owner/manager user’s avatar

def top_business_eco(
    s: Session,
    period: Period = "total",
    limit: int = 20,
    offset: int = 0,
    me_business_id: Optional[str] = None,
) -> Dict[str, Any]:
    since = _since_ms(period)
    tx_ms = _tx_ms_expr("tx")

    rows = s.run(
        f"""
        MATCH (b:BusinessProfile)
        WHERE b.id IS NOT NULL
        OPTIONAL MATCH (b)-[:COLLECTED|EARNED]->(tx:EcoTx)
        WITH b, tx, {tx_ms} AS tx_ms
        WHERE tx IS NULL OR (
          coalesce(tx.status,'settled')='settled'
          AND (
            coalesce(tx.kind,'') IN ['CONTRIBUTE','SPONSOR_DEPOSIT','MINT_ACTION']
            OR tx.source IN ['contribution','sidequest']
          )
          AND ($since IS NULL OR tx_ms >= $since)
        )
        WITH b, toInteger(coalesce(sum(toInteger(coalesce(tx.amount, tx.eco, 0))),0)) AS eco

        // Find a representative owner/manager user to source avatar fallback from
        OPTIONAL MATCH (b)<-[:OWNS|MANAGES|REPRESENTS|STAFF_OF|WORKS_AT]-(owner:User)
        WITH b, eco, owner
        ORDER BY coalesce(owner.createdAt, 0) ASC  // deterministic pick if multiple
        WITH b, eco, head(collect(owner)) AS o

        WITH b, eco,
             {_display_name_expr_business()} AS display_name,
             coalesce(b.avatar_url, o.avatar_url) AS avatar_url
        RETURN b.id AS business_id,
               display_name,
               toInteger(eco) AS eco,
               avatar_url
        ORDER BY eco DESC, business_id ASC
        SKIP $offset LIMIT $limit
        """,
        since=since, offset=offset, limit=limit,
    ).data()

    items = [
        {
            "business_id": r["business_id"],
            "display_name": (r.get("display_name") or str(r["business_id"])[-6:]),
            "eco": int(r.get("eco", 0) or 0),
            "avatar_url": r.get("avatar_url"),
        } for r in rows
    ]

    top_value = int(s.run(
        f"""
        CALL () {{
          MATCH (b:BusinessProfile)
          WHERE b.id IS NOT NULL
          OPTIONAL MATCH (b)-[:COLLECTED|EARNED]->(tx:EcoTx)
          WITH tx, {tx_ms} AS tx_ms
          WHERE tx IS NULL OR (
            coalesce(tx.status,'settled')='settled'
            AND (
              coalesce(tx.kind,'') IN ['CONTRIBUTE','SPONSOR_DEPOSIT','MINT_ACTION']
              OR tx.source IN ['contribution','sidequest']
            )
            AND ($since IS NULL OR tx_ms >= $since)
          )
          RETURN toInteger(coalesce(sum(toInteger(coalesce(tx.amount, tx.eco, 0))),0)) AS eco
        }}
        RETURN coalesce(max(eco),0) AS top_value
        """, since=since
    ).single()["top_value"] or 0)

    total_estimate = int(s.run(
        "MATCH (b:BusinessProfile) WHERE b.id IS NOT NULL RETURN count(b) AS n"
    ).single()["n"] or 0)

    meta_my = None
    if me_business_id:
        my = s.run(
            f"""
            MATCH (b:BusinessProfile {{id:$bid}})
            OPTIONAL MATCH (b)-[:COLLECTED|EARNED]->(tx:EcoTx)
            WITH b, tx, {_tx_ms_expr('tx')} AS tx_ms
            WHERE tx IS NULL OR (
              coalesce(tx.status,'settled')='settled'
              AND (
                coalesce(tx.kind,'') IN ['CONTRIBUTE','SPONSOR_DEPOSIT','MINT_ACTION']
                OR tx.source IN ['contribution','sidequest']
              )
              AND ($since IS NULL OR tx_ms >= $since)
            )
            WITH b, toInteger(coalesce(sum(toInteger(coalesce(tx.amount, tx.eco, 0))),0)) AS my_eco

            OPTIONAL MATCH (b)<-[:OWNS|MANAGES|REPRESENTS|STAFF_OF|WORKS_AT]-(owner:User)
            WITH b, my_eco, owner
            ORDER BY coalesce(owner.createdAt, 0) ASC
            WITH b, my_eco, head(collect(owner)) AS o

            WITH b, my_eco,
                 {_display_name_expr_business()} AS display_name,
                 coalesce(b.avatar_url, o.avatar_url) AS avatar_url

            CALL {{
              WITH my_eco
              MATCH (b2:BusinessProfile)
              WHERE b2.id IS NOT NULL
              OPTIONAL MATCH (b2)-[:COLLECTED|EARNED]->(tx2:EcoTx)
              WITH tx2, my_eco, {_tx_ms_expr('tx2')} AS tx2_ms
              WHERE tx2 IS NULL OR (
                coalesce(tx2.status,'settled')='settled'
                AND (
                  coalesce(tx2.kind,'') IN ['CONTRIBUTE','SPONSOR_DEPOSIT','MINT_ACTION']
                  OR tx2.source IN ['contribution','sidequest']
                )
                AND ($since IS NULL OR tx2_ms >= $since)
              )
              WITH toInteger(coalesce(sum(toInteger(coalesce(tx2.amount, tx2.eco, 0))),0)) AS eco2, my_eco
              WHERE eco2 > my_eco
              RETURN count(*) AS higher
            }}
            RETURN b.id AS business_id, display_name, avatar_url, my_eco AS value, (1 + higher) AS rank
            """,
            bid=me_business_id, since=since
        ).single()
        if my:
            meta_my = {
                "id": my["business_id"],
                "value": int(my["value"] or 0),
                "rank": int(my["rank"] or 1),
                "display_name": my.get("display_name") or str(my["business_id"])[-6:],
                "avatar_url": my.get("avatar_url"),
            }

    return {
        "items": items,
        "meta": {
            "period": period,
            "since_ms": since,
            "limit": limit,
            "offset": offset,
            "has_more": _has_more(len(items), limit),
            "total_estimate": total_estimate,
            "top_value": top_value,
            "my": meta_my,
        },
    }

# ───────────────────────────────────────────────────────────────────────────────
# Youth Actions leaderboard (approved submissions count)
# ───────────────────────────────────────────────────────────────────────────────

def top_youth_actions(
    s: Session,
    period: Period = "total",
    mission_type: Optional[str] = None,
    limit: int = 20,
    offset: int = 0,
    me_user_id: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Counts APPROVED sidequests per user.
    We measure *approved* Submission nodes (wallet parity).
    """
    since = _since_ms(period)

    rows = s.run(
        f"""
        CALL () {{
          MATCH (u:User)
          {_where_user_is_youth('u')}
          OPTIONAL MATCH (u)-[:SUBMITTED]->(sub:Submission {{state:'approved'}})-[:FOR]->(:Sidequest)
          WITH u, sub,
               toInteger(timestamp(coalesce(sub.reviewed_at, sub.created_at, datetime()))) AS sub_ms
          WHERE sub IS NULL OR ($since IS NULL OR sub_ms >= $since)
          RETURN u, toInteger(count(sub)) AS completed
        }}
        WITH u, completed, {_display_name_expr_user()} AS display_name, u.avatar_url AS avatar_url
        RETURN u.id AS user_id, display_name, completed, avatar_url
        ORDER BY completed DESC, user_id ASC
        SKIP $offset LIMIT $limit
        """,
        since=since, offset=offset, limit=limit,
    ).data()

    items = [
        {
            "user_id": r["user_id"],
            "display_name": (r.get("display_name") or r["user_id"][-6:]),
            "completed": int(r.get("completed", 0) or 0),
            "avatar_url": r.get("avatar_url"),
        } for r in rows
    ]

    top_value = int(s.run(
        f"""
        CALL () {{ 
          MATCH (u:User)
          {_where_user_is_youth('u')}
          OPTIONAL MATCH (u)-[:SUBMITTED]->(sub:Submission {{state:'approved'}})-[:FOR]->(:Sidequest)
          WITH u, sub,
               toInteger(timestamp(coalesce(sub.reviewed_at, sub.created_at, datetime()))) AS sub_ms
          WHERE sub IS NULL OR ($since IS NULL OR sub_ms >= $since)
          RETURN toInteger(count(sub)) AS c
        }}
        RETURN coalesce(max(c),0) AS top_value
        """, since=since).single()["top_value"] or 0)

    total_estimate = int(s.run(
        f"MATCH (u:User) {_where_user_is_youth('u')} RETURN count(u) AS n"
    ).single()["n"] or 0)

    meta_my = None
    if me_user_id:
        elig = s.run(
            f"""
            MATCH (u:User {{id:$uid}})
            RETURN {_user_is_business_predicate('u')} AS has_biz
            """,
            uid=me_user_id,
        ).single()
        if elig and not elig["has_biz"]:
            my_row = s.run(
                f"""
                MATCH (u:User {{id: $uid}})
                {_where_user_is_youth('u')}
                OPTIONAL MATCH (u)-[:SUBMITTED]->(sub:Submission {{state:'approved'}})-[:FOR]->(:Sidequest)
                WITH u, sub,
                     toInteger(timestamp(coalesce(sub.reviewed_at, sub.created_at, datetime()))) AS sub_ms
                WHERE sub IS NULL OR ($since IS NULL OR sub_ms >= $since)
                WITH u, toInteger(count(sub)) AS my_completed, {_display_name_expr_user()} AS display_name, u.avatar_url AS avatar_url
                CALL {{
                  WITH my_completed
                  MATCH (u2:User)
                  {_where_user_is_youth('u2')}
                  OPTIONAL MATCH (u2)-[:SUBMITTED]->(sub2:Submission {{state:'approved'}})-[:FOR]->(:Sidequest)
                  WITH u2, sub2, my_completed,
                       toInteger(timestamp(coalesce(sub2.reviewed_at, sub2.created_at, datetime()))) AS sub2_ms
                  WHERE sub2 IS NULL OR ($since IS NULL OR sub2_ms >= $since)
                  WITH toInteger(count(sub2)) AS c2, my_completed
                  WHERE c2 > my_completed
                  RETURN count(*) AS higher
                }}
                RETURN u.id AS user_id, display_name, avatar_url, my_completed AS value, (1 + higher) AS rank
                """,
                uid=me_user_id, since=since
            ).single()
            if my_row:
                meta_my = {
                    "id": my_row["user_id"],
                    "value": int(my_row["value"] or 0),
                    "rank": int(my_row["rank"] or 1),
                    "display_name": my_row.get("display_name") or str(my_row["user_id"])[-6:],
                    "avatar_url": my_row.get("avatar_url"),
                }

    return {
        "items": items,
        "meta": {
            "period": period,
            "since_ms": since,
            "limit": limit,
            "offset": offset,
            "has_more": _has_more(len(items), limit),
            "total_estimate": total_estimate,
            "top_value": top_value,
            "my": meta_my,
        },
    }