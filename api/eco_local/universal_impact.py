from __future__ import annotations

from datetime import datetime, timezone, timedelta
from typing import Optional

from fastapi import APIRouter, Depends
from neo4j import Session
from pydantic import BaseModel

from site_backend.core.neo_driver import session_dep

router = APIRouter(
    prefix="/eco-local/universal-impact",
    tags=["eco-local-universal-impact"],
)


class UniversalImpactOut(BaseModel):
    # Youth / missions
    total_youth: int
    active_youth_30d: int
    missions_completed_total: int

    # ECO minting to youth ledger
    eco_minted_total: int
    minted_24h: int
    last_event_at: Optional[str] = None  # ISO string or null

    # Businesses / offers / contributions / retirements
    businesses_total: int
    offers_total: int
    eco_contributed_total: int
    eco_contributed_30d: int
    eco_retired_total: int
    eco_retired_30d: int


@router.get("/overview", response_model=UniversalImpactOut)
def universal_overview(s: Session = Depends(session_dep)) -> UniversalImpactOut:
    """
    Global, privacy-safe impact rollup across the whole graph.

    Semantics:
    - "Youth" = User nodes that do NOT OWNS/MANAGES a BusinessProfile.
    - ECO minting = ECO earned by those youth via (:User)-[:EARNED]->(EcoTx),
      treating missing status as 'settled'.
    - "ECO contributed by partners" on the UI is actually ECO contributed *to*
      partners, which is the same as ECO retired into offers via
      business BURN_REWARD spend.
    """
    now = datetime.now(timezone.utc)
    cutoff_24h_ms = int((now - timedelta(days=1)).timestamp() * 1000)
    cutoff_30d_ms = int((now - timedelta(days=30)).timestamp() * 1000)

    # ---------- 1) Total youth + minted + last event (youth only) ----------
    rec = (
        s.run(
            """
            // youth are users that are not business owners/managers
            MATCH (u:User)
            WHERE NOT (u)-[:OWNS|MANAGES]->(:BusinessProfile)
            WITH collect(u) AS youth, count(u) AS total_youth

            // ECO minted into youth wallets
            OPTIONAL MATCH (yu:User)-[:EARNED]->(t:EcoTx)
            WHERE yu IN youth
              AND coalesce(t.status,'settled') = 'settled'
            WITH total_youth,
                 toInteger(
                   coalesce(
                     sum(toInteger(coalesce(t.amount, t.eco, 0))),
                     0
                   )
                 ) AS eco_minted_total,
                 coalesce(
                   max(
                     toInteger(
                       coalesce(t.createdAt, timestamp(t.at), timestamp())
                     )
                   ),
                   null
                 ) AS last_ms

            RETURN total_youth, eco_minted_total, last_ms
            """
        ).single()
        or {}
    )

    total_youth = int(rec.get("total_youth") or 0)
    eco_minted_total = int(rec.get("eco_minted_total") or 0)
    last_ms = rec.get("last_ms")
    if last_ms is None or last_ms == "":
        last_event_at: Optional[str] = None
    else:
        try:
            last_event_at = datetime.fromtimestamp(
                int(last_ms) / 1000.0, tz=timezone.utc
            ).isoformat()
        except Exception:
            last_event_at = None

    # ---------- 2) Minted in last 24h (youth only) ----------
    rec_24 = (
        s.run(
            """
            MATCH (u:User)
            WHERE NOT (u)-[:OWNS|MANAGES]->(:BusinessProfile)
            WITH collect(u) AS youth

            OPTIONAL MATCH (u2:User)-[:EARNED]->(t:EcoTx)
            WHERE u2 IN youth
              AND coalesce(t.status,'settled') = 'settled'
              AND toInteger(
                    coalesce(
                      t.createdAt,
                      timestamp(t.at),
                      timestamp()
                    )
                  ) >= $cutoff_24h_ms

            RETURN toInteger(
                     coalesce(
                       sum(toInteger(coalesce(t.amount, t.eco, 0))),
                       0
                     )
                   ) AS minted_24h
            """,
            {"cutoff_24h_ms": cutoff_24h_ms},
        ).single()
        or {}
    )
    minted_24h = int(rec_24.get("minted_24h") or 0)

    # ---------- 3) Active youth in last 30 days ----------
    rec_active = (
        s.run(
            """
            MATCH (uu:User)
            WHERE NOT (uu)-[:OWNS|MANAGES]->(:BusinessProfile)

            // ECO earns in last 30d
            OPTIONAL MATCH (uu)-[:EARNED]->(te:EcoTx)
            WHERE coalesce(te.status,'settled') = 'settled'
              AND toInteger(
                    coalesce(
                      te.createdAt,
                      timestamp(te.at),
                      timestamp()
                    )
                  ) >= $cutoff_30d_ms
            WITH uu, count(te) AS cte

            // approved submissions in last 30d
            OPTIONAL MATCH (uu)-[:SUBMITTED]->(ss:Submission {state:'approved'})
            WHERE toInteger(
                    timestamp(
                      coalesce(ss.reviewed_at, ss.created_at, datetime())
                    )
                  ) >= $cutoff_30d_ms
            WITH uu, cte, count(ss) AS css

            RETURN count(
              DISTINCT CASE
                WHEN cte > 0 OR css > 0 THEN uu
                ELSE null
              END
            ) AS active_youth_30d
            """,
            {"cutoff_30d_ms": cutoff_30d_ms},
        ).single()
        or {}
    )
    active_youth_30d = int(rec_active.get("active_youth_30d") or 0)

    # ---------- 4) Missions completed total (youth only) ----------
    rec_missions = (
        s.run(
            """
            MATCH (u:User)
            WHERE NOT (u)-[:OWNS|MANAGES]->(:BusinessProfile)
            OPTIONAL MATCH (u)-[:SUBMITTED]->(s:Submission {state:'approved'})
            RETURN toInteger(count(s)) AS missions_completed_total
            """
        ).single()
        or {}
    )
    missions_completed_total = int(rec_missions.get("missions_completed_total") or 0)

    # ---------- 5) Businesses + offers ----------
    rec_biz = (
        s.run(
            """
            OPTIONAL MATCH (b:BusinessProfile)
            WITH toInteger(count(b)) AS businesses_total
            OPTIONAL MATCH (o:Offer)
            RETURN businesses_total,
                   toInteger(count(o)) AS offers_total
            """
        ).single()
        or {}
    )
    businesses_total = int(rec_biz.get("businesses_total") or 0)
    offers_total = int(rec_biz.get("offers_total") or 0)

    # ---------- 6) ECO retired into offers (lifetime + 30d) ----------
    rec_flow = (
        s.run(
            """
            WITH $cutoff_30d_ms AS cutoff_ms

            // retirements via offer redemptions (BURN_REWARD) from businesses
            OPTIONAL MATCH (b:BusinessProfile)-[:SPENT]->(tout:EcoTx)
            WHERE coalesce(tout.status,'settled') = 'settled'
              AND coalesce(tout.kind,'') = 'BURN_REWARD'
            WITH cutoff_ms,
                 collect({
                   ms:  toInteger(
                          coalesce(
                            tout.createdAt,
                            timestamp(tout.at),
                            timestamp()
                          )
                        ),
                   eco: toInteger(coalesce(tout.eco, tout.amount, 0))
                 }) AS outs

            RETURN
              toInteger(
                coalesce(
                  reduce(s=0, r IN outs | s + r.eco),
                  0
                )
              ) AS eco_retired_total,
              toInteger(
                coalesce(
                  reduce(
                    s=0,
                    r IN [r IN outs WHERE r.ms >= cutoff_ms] |
                      s + r.eco
                  ),
                  0
                )
              ) AS eco_retired_30d
            """,
            {"cutoff_30d_ms": cutoff_30d_ms},
        ).single()
        or {}
    )

    eco_retired_total = int(rec_flow.get("eco_retired_total") or 0)
    eco_retired_30d = int(rec_flow.get("eco_retired_30d") or 0)

    # Semantics: ECO contributed BY -> ECO contributed TO partners,
    # which is the same as ECO retired into offers (value that has
    # actually landed with partners).
    eco_contributed_total = eco_retired_total
    eco_contributed_30d = eco_retired_30d

    return UniversalImpactOut(
        total_youth=total_youth,
        active_youth_30d=active_youth_30d,
        missions_completed_total=missions_completed_total,
        eco_minted_total=eco_minted_total,
        minted_24h=minted_24h,
        last_event_at=last_event_at,
        businesses_total=businesses_total,
        offers_total=offers_total,
        eco_contributed_total=eco_contributed_total,
        eco_contributed_30d=eco_contributed_30d,
        eco_retired_total=eco_retired_total,
        eco_retired_30d=eco_retired_30d,
    )
