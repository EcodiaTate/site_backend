# site_backend/api/routers/stats.py
from __future__ import annotations

from datetime import datetime, timezone
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from neo4j import Session

from site_backend.core.neo_driver import session_dep

router = APIRouter(prefix="/stats", tags=["stats"])


# ---------- Models ----------
class YouthStats(BaseModel):
    user_id: str
    total_eco: int
    eco_from_missions: int
    eco_from_eyba: int
    missions_completed: int
    last_earn_at: str | None = None


class BizStats(BaseModel):
    business_id: str
    month_start: str
    month_end: str
    monthly_budget: int
    minted: int
    remaining: int
    claims: int
    unique_youth: int


# ---------- Helpers ----------
def month_bounds(iso_month: str) -> tuple[str, str]:
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


# ---------- Routes ----------
@router.get("/youth/{user_id}", response_model=YouthStats)
def get_youth_stats(user_id: str, s: Session = Depends(session_dep)):
    rec = s.run(
        """
        MATCH (u:User {id:$uid})
        OPTIONAL MATCH (u)-[:EARNED]->(t:EcoTx)
        WITH u,
             sum(coalesce(t.eco,0)) AS total_eco,
             sum(CASE WHEN t.source = "mission" THEN coalesce(t.eco,0) ELSE 0 END) AS eco_from_missions,
             sum(CASE WHEN t.source = "eyba"    THEN coalesce(t.eco,0) ELSE 0 END) AS eco_from_eyba,
             max(t.at) AS last_earn_at
        OPTIONAL MATCH (u)-[:SUBMITTED]->(s:Submission {state:"approved"})
        RETURN {
          user_id: u.id,
          total_eco: toInteger(total_eco),
          eco_from_missions: toInteger(eco_from_missions),
          eco_from_eyba: toInteger(eco_from_eyba),
          missions_completed: count(s),
          last_earn_at: CASE WHEN last_earn_at IS NULL THEN NULL ELSE toString(last_earn_at) END
        } AS stats
        """,
        {"uid": user_id},
    ).single()

    if not rec:
        raise HTTPException(status_code=404, detail="User not found")
    return YouthStats(**dict(rec["stats"]))


@router.get("/business/{business_id}", response_model=BizStats)
def get_business_stats(business_id: str, month: str, s: Session = Depends(session_dep)):
    start_iso, end_iso = month_bounds(month)
    rec = s.run(
        """
        MATCH (b:BusinessProfile {id:$bid})
        WITH b,
             toInteger(coalesce(b.monthly_price_cents,0)) AS price_cents,
             toInteger(coalesce(b.eco_mint_ratio,10))     AS ratio,
             coalesce(b.monthly_budget_ratio, 0.9)        AS budget_ratio
        WITH b, toInteger( round( (price_cents/100.0) * ratio * budget_ratio ) ) AS monthly_budget

        OPTIONAL MATCH (t:EcoTx)-[:FROM]->(b)
        WHERE t.at >= datetime($start) AND t.at < datetime($end)
        WITH b, monthly_budget,
             sum(coalesce(t.eco,0)) AS minted,
             count(t)               AS claim_count,
             collect(t)             AS txs

        OPTIONAL MATCH (u:User)-[:EARNED]->(t:EcoTx)
        WHERE t IN txs
        WITH monthly_budget, coalesce(minted,0) AS minted, claim_count, count(DISTINCT u) AS unique_youth

        RETURN {
          business_id: $bid,
          month_start: $start,
          month_end: $end,
          monthly_budget: toInteger(monthly_budget),
          minted: toInteger(minted),
          remaining: toInteger(monthly_budget - minted),
          claims: toInteger(coalesce(claim_count,0)),
          unique_youth: toInteger(unique_youth)
        } AS stats
        """,
        {"bid": business_id, "start": start_iso, "end": end_iso},
    ).single()

    if not rec:
        raise HTTPException(status_code=404, detail="Business not found")

    return BizStats(**dict(rec["stats"]))
