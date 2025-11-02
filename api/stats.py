# site_backend/api/routers/stats.py
from __future__ import annotations

from datetime import datetime, timezone
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from neo4j import Session

from site_backend.core.neo_driver import session_dep

router = APIRouter(prefix="/stats", tags=["stats"])

# =========================
# Models (FE-aligned)
# =========================
class YouthStats(BaseModel):
    user_id: str
    minted_eco: int                    # total ECO earned via MINT_ACTION
    eco_contributed_total: int         # ECO spent as contributions
    eco_given_total: int               # ECO spent on offers (BURN_REWARD)
    missions_completed: int
    last_earn_at: str | None = None    # ISO datetime (from t.at/createdAt)


class BizStats(BaseModel):
    business_id: str
    minted_eco: int                    # ECO minted *at/for* this biz (MINT_ACTION via AT/FOR)
    eco_contributed_total: int         # ECO collected from youth contributions
    eco_given_total: int               # ECO retired via this biz's offers (BURN_REWARD)
    unique_youth_month: int            # distinct youth who redeemed this month
    last_tx_at: str | None = None      # latest tx (earn/collect/burn) ISO


# =========================
# Helpers
# =========================
def _month_bounds_utc_now() -> tuple[str, str]:
    """Start of current UTC month (inclusive) → start of next month (exclusive) as ISO."""
    now = datetime.now(timezone.utc)
    start = datetime(now.year, now.month, 1, tzinfo=timezone.utc)
    if now.month == 12:
        end = datetime(now.year + 1, 1, 1, tzinfo=timezone.utc)
    else:
        end = datetime(now.year, now.month + 1, 1, tzinfo=timezone.utc)
    return start.isoformat(), end.isoformat()


# =========================
# Routes
# =========================
@router.get("/youth/{user_id}", response_model=YouthStats)
def get_youth_stats(user_id: str, s: Session = Depends(session_dep)):
    """
    - minted_eco: sum of settled MINT_ACTION earned by the user
    - eco_contributed_total: sum of settled CONTRIBUTE spent by the user
    - eco_given_total: sum of settled BURN_REWARD spent by the user (offers)
    - missions_completed: approved submissions count
    - last_earn_at: latest earn timestamp (t.at preferred, else createdAt)
    """
    rec = s.run(
        """
        MATCH (u:User {id:$uid})

        // ------- Earned (MINT_ACTION) -------
        OPTIONAL MATCH (u)-[:EARNED]->(te:EcoTx {status:'settled'})
        WHERE coalesce(te.kind,'') = 'MINT_ACTION'
        WITH u,
             sum(toInteger(coalesce(te.eco, te.amount, 0))) AS minted_eco,
             max(
               CASE
                 WHEN te.at IS NOT NULL THEN te.at
                 WHEN te.createdAt IS NOT NULL THEN datetime({epochMillis: toInteger(te.createdAt)})
                 ELSE NULL
               END
             ) AS lastEarn

        // ------- Spent buckets -------
        OPTIONAL MATCH (u)-[:SPENT]->(ts:EcoTx {status:'settled'})
        WHERE coalesce(ts.kind,'') IN ['BURN_REWARD','CONTRIBUTE']
        WITH u, minted_eco, lastEarn,
             sum( CASE WHEN ts.kind='CONTRIBUTE'  THEN toInteger(coalesce(ts.eco, ts.amount, 0)) ELSE 0 END )
               AS eco_contributed_total,
             sum( CASE WHEN ts.kind='BURN_REWARD' THEN toInteger(coalesce(ts.eco, ts.amount, 0)) ELSE 0 END )
               AS eco_given_total

        // ------- Missions completed -------
        OPTIONAL MATCH (u)-[:SUBMITTED]->(sub:Submission {state:'approved'})
        RETURN
          u.id AS user_id,
          toInteger(coalesce(minted_eco,0)) AS minted_eco,
          toInteger(coalesce(eco_contributed_total,0)) AS eco_contributed_total,
          toInteger(coalesce(eco_given_total,0)) AS eco_given_total,
          toInteger(count(sub)) AS missions_completed,
          CASE WHEN lastEarn IS NULL THEN NULL ELSE toString(lastEarn) END AS last_earn_at
        """,
        {"uid": user_id},
    ).single()

    if not rec:
        raise HTTPException(status_code=404, detail="User not found")

    return YouthStats(**dict(rec))

@router.get("/business/{business_id}", response_model=BizStats)
def get_business_stats(business_id: str, s: Session = Depends(session_dep)):
    from datetime import datetime, timezone

    def _month_bounds_utc_now() -> tuple[str, str, int, int]:
        now = datetime.now(timezone.utc)
        start = datetime(now.year, now.month, 1, tzinfo=timezone.utc)
        end = datetime(now.year + (1 if now.month == 12 else 0),
                       1 if now.month == 12 else now.month + 1,
                       1, tzinfo=timezone.utc)
        return (start.isoformat(), end.isoformat(),
                int(start.timestamp() * 1000), int(end.timestamp() * 1000))

    mstart, mend, mstart_ms, mend_ms = _month_bounds_utc_now()

    rec = s.run(
        """
        MATCH (b:BusinessProfile {id:$bid})

        // A) Minted at/for this business (settled MINT_ACTION)
        OPTIONAL MATCH (m:EcoTx {status:'settled', kind:'MINT_ACTION'})-[:AT|:FOR]->(b)
        WITH b,
             sum(toInteger(coalesce(m.eco, m.amount, 0))) AS minted_eco,
             collect(m) AS mintRows

        // B) Contributions collected by this business
        OPTIONAL MATCH (b)-[:COLLECTED|EARNED]->(c:EcoTx {status:'settled'})
        WHERE (coalesce(c.kind,'') = 'CONTRIBUTE') OR (toLower(coalesce(c.source,'')) = 'contribution')
        WITH b, minted_eco, mintRows,
             sum(toInteger(coalesce(c.eco, c.amount, 0))) AS eco_contributed_total,
             collect(c) AS contribRows

        // C) Offer burns for this business (retire ECO)
        OPTIONAL MATCH (b)<-[:OF]-(o:Offer)<-[:FOR_OFFER]-(br:EcoTx {status:'settled', kind:'BURN_REWARD'})
        WITH b, minted_eco, eco_contributed_total, mintRows, contribRows,
             sum(toInteger(coalesce(br.eco, br.amount, 0))) AS eco_given_total,
             collect(br) AS burnRows

        // Unique youth who redeemed this month (by BURN_REWARD)
        OPTIONAL MATCH (u:User)-[:SPENT]->(tmo:EcoTx {status:'settled', kind:'BURN_REWARD'})-[:FOR_OFFER]->(:Offer)-[:OF]->(b)
        WHERE (tmo.at IS NOT NULL AND tmo.at >= datetime($mstart) AND tmo.at < datetime($mend))
           OR (tmo.at IS NULL AND toInteger(coalesce(tmo.createdAt,0)) >= $mstart_ms AND toInteger(coalesce(tmo.createdAt,0)) < $mend_ms)
        WITH b, minted_eco, eco_contributed_total, eco_given_total, count(DISTINCT u) AS unique_youth_month,
             mintRows + contribRows + burnRows AS allRows

        // Build list of datetimes (prefer t.at, else createdAt ms)
        WITH b, minted_eco, eco_contributed_total, eco_given_total, unique_youth_month,
             [t IN allRows |
               CASE
                 WHEN t.at IS NOT NULL THEN t.at
                 WHEN t.createdAt IS NOT NULL THEN datetime({epochMillis: toInteger(t.createdAt)})
                 ELSE NULL
               END
             ] AS times

        // Keep a row even if empty: UNWIND a single NULL when size=0, then take max()
        UNWIND (CASE WHEN size(times)=0 THEN [NULL] ELSE [x IN times WHERE x IS NOT NULL] END) AS t
        WITH b, minted_eco, eco_contributed_total, eco_given_total, unique_youth_month, max(t) AS latest
        RETURN
          b.id AS business_id,
          toInteger(coalesce(minted_eco,0)) AS minted_eco,
          toInteger(coalesce(eco_contributed_total,0)) AS eco_contributed_total,
          toInteger(coalesce(eco_given_total,0)) AS eco_given_total,
          toInteger(coalesce(unique_youth_month,0)) AS unique_youth_month,
          CASE WHEN latest IS NULL THEN NULL ELSE toString(latest) END AS last_tx_at
        """,
        {"bid": business_id, "mstart": mstart, "mend": mend, "mstart_ms": mstart_ms, "mend_ms": mend_ms},
    ).single()

    if not rec:
        raise HTTPException(status_code=404, detail="Business not found")

    return BizStats(**dict(rec))
