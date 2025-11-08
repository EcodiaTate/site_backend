# app/routers/eco-local_recruiting.py
from __future__ import annotations

import os
from datetime import datetime, date
from typing import Any, Dict, Optional

from fastapi import APIRouter, HTTPException, Query
from neo4j import GraphDatabase
from neo4j.graph import Node, Relationship, Path
try:
    # neo4j temporal helpers
    from neo4j.time import DateTime as NeoDateTime, Date as NeoDate, Time as NeoTime, Duration as NeoDuration
except Exception:  # pragma: no cover
    NeoDateTime = NeoDate = NeoTime = NeoDuration = tuple()  # type: ignore


# ─────────────────────────────────────────────────────────
# Neo4j driver (self-contained; uses env vars)
# ─────────────────────────────────────────────────────────
NEO4J_URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "password")

_driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

def _iso(v: Any) -> Any:
    """Convert temporal-ish values to ISO strings."""
    # neo4j.time.* instances usually have .to_native()
    if hasattr(v, "to_native"):
        nat = v.to_native()
        try:
            return nat.isoformat()
        except Exception:
            return str(nat)
    # plain python datetime/date/time
    if hasattr(v, "isoformat"):
        try:
            return v.isoformat()
        except Exception:
            return str(v)
    return str(v)

def _coerce_neo(v: Any) -> Any:
    """Recursively convert Neo4j driver values into JSON-safe types."""
    if v is None:
        return None

    # Temporal types
    if isinstance(v, (NeoDateTime, NeoDate, NeoTime, NeoDuration)):
        return _iso(v)

    # Graph types
    if isinstance(v, Node):
        # Just return properties; if you want labels/id, add as needed
        # e.g., {"_id": v.element_id, "_labels": list(v.labels), **dict(v)}
        return {k: _coerce_neo(v[k]) for k in v.keys()}
    if isinstance(v, Relationship):
        # As with Node, return only properties by default
        return {k: _coerce_neo(v[k]) for k in v.keys()}
    if isinstance(v, Path):
        # Represent as list of nodes (properties only)
        return [_coerce_neo(n) for n in v.nodes]

    # Containers
    if isinstance(v, dict):
        return {str(k): _coerce_neo(val) for k, val in v.items()}
    if isinstance(v, (list, tuple, set)):
        return [_coerce_neo(x) for x in v]

    # Fallback for other neo4j types that stringify cleanly
    return v

def _run(cy: str, params: Dict[str, Any] | None = None):
    with _driver.session() as s:
        rs = s.run(cy, **(params or {}))
        rows = [r.data() for r in rs]
        # Coerce every row to JSON-safe
        return [_coerce_neo(r) for r in rows]


# ─────────────────────────────────────────────────────────
# Router
# ─────────────────────────────────────────────────────────
router = APIRouter()

def _safe_date(s: Optional[str]) -> date:
    if not s:
        return datetime.now().date()
    try:
        return datetime.fromisoformat(s[:10]).date()
    except Exception:
        return datetime.now().date()

# ─────────────────────────────────────────────────────────
# GET /api/eco-local/recruiting/overview
# ─────────────────────────────────────────────────────────
@router.get("/overview")
def overview() -> Dict[str, Any]:
    q_totals = """
    CALL { MATCH (p:Prospect) RETURN count(p) AS prospects }
    CALL { MATCH (p:Prospect) WHERE coalesce(p.qualified,false)=true RETURN count(p) AS qualified }
    CALL { MATCH (t:Thread) RETURN count(t) AS active_threads }
    CALL { MATCH (p:Prospect) WHERE coalesce(p.unsubscribed,false)=true RETURN count(p) AS unsubscribed }
    CALL { MATCH (p:Prospect) WHERE coalesce(p.won,false)=true RETURN count(p) AS won }
    RETURN prospects, qualified, active_threads, unsubscribed, won
    """
    totals = _run(q_totals)[0]

    # Rewritten: no MATCH inside list comps; use subqueries
    q_outreach = """
    CALL { MATCH (p:Prospect) WHERE coalesce(p.outreach_started,false)=true RETURN count(p) AS started }
    CALL {
      MATCH (p:Prospect)
      RETURN sum(coalesce(p.attempt_count,0)) AS attempts_total,
             max(coalesce(p.last_outreach_at, datetime({epochMillis:0}))) AS last_outreach_at
    }
    RETURN { started: started, attempts_total: attempts_total, last_outreach_at: last_outreach_at } AS outreach
    """
    outreach = _run(q_outreach)[0]["outreach"]

    q_success = """
    CALL { MATCH (p:Prospect) WHERE coalesce(p.won,false)=true RETURN count(p) AS wins }
    CALL { MATCH (p:Prospect) RETURN count(p) AS total }
    WITH wins, total, CASE WHEN total=0 THEN 0.0 ELSE toFloat(wins)/toFloat(total) END AS rate
    CALL {
      MATCH (p:Prospect)
      WHERE coalesce(p.won,false)=true AND coalesce(p.won_at, datetime({epochMillis:0})) >= datetime() - duration({days:7})
      RETURN count(p) AS last7
    }
    RETURN {win_rate: rate, last_7d_wins: last7} AS success
    """
    success = _run(q_success)[0]["success"]
    q_holds = """
    WITH date() AS today
    OPTIONAL MATCH (h:CalendarHold)
    WITH today,
        // Normalize start into a datetime (handles strings, maps, or already-temporal)
        date(datetime(h.start)) AS d
    WITH today,
        sum(CASE WHEN d = today THEN 1 ELSE 0 END) AS today_count,
        sum(CASE WHEN d > today AND d <= today + duration({days:7}) THEN 1 ELSE 0 END) AS next_7d_count
    RETURN {today_count: coalesce(today_count,0), next_7d_count: coalesce(next_7d_count,0)} AS holds
    """


    holds = _run(q_holds)[0]["holds"]

    inbox = {"last_poll_at": None, "last_poll_processed": None}
    return {"totals": totals, "outreach": outreach, "success": success, "holds": holds, "inbox": inbox}

# ─────────────────────────────────────────────────────────
# GET /api/eco-local/recruiting/prospects
# ─────────────────────────────────────────────────────────
@router.get("/prospects")
def list_prospects(
    q: Optional[str] = Query(None),
    status: str = Query("all", pattern="^(all|new|started|won|unsub)$"),
    cursor: Optional[str] = None,  # reserved for future
    limit: int = 100,
) -> Dict[str, Any]:
    where = []
    params: Dict[str, Any] = {"limit": int(limit)}
    if q:
        where.append("(toLower(p.email) CONTAINS toLower($q) OR toLower(coalesce(p.name,'')) CONTAINS toLower($q))")
        params["q"] = q
    if status == "new":
        where.append("coalesce(p.outreach_started,false)=false AND coalesce(p.unsubscribed,false)=false AND coalesce(p.won,false)=false")
    elif status == "started":
        where.append("coalesce(p.outreach_started,false)=true AND coalesce(p.unsubscribed,false)=false AND coalesce(p.won,false)=false")
    elif status == "won":
        where.append("coalesce(p.won,false)=true")
    elif status == "unsub":
        where.append("coalesce(p.unsubscribed,false)=true")

    w = ("WHERE " + " AND ".join(where)) if where else ""
    cy = f"""
    MATCH (p:Prospect)
    {w}
    RETURN p
    ORDER BY coalesce(p.updated_at, p.created_at) DESC
    LIMIT $limit
    """
    rows = _run(cy, params)
    items = [r["p"] for r in rows]  # already coerced
    return {"items": items, "next_cursor": None}

# ─────────────────────────────────────────────────────────
# GET /api/eco-local/recruiting/threads/:email
# ─────────────────────────────────────────────────────────
@router.get("/threads/{email}")
def get_thread(email: str) -> Dict[str, Any]:
    params = {"email": email}

    trows = _run("MATCH (t:Thread {email:$email}) RETURN t", params)
    thread = trows[0]["t"] if trows else {"email": email}

    inbound = [r["m"] for r in _run(
        "MATCH (:Thread {email:$email})<-[:IN_THREAD]-(m:InboundEmail) RETURN m ORDER BY coalesce(m.received_at,m.created_at) DESC LIMIT 200",
        params
    )]
    replies = [r["r"] for r in _run(
        "MATCH (:Thread {email:$email})<-[:IN_THREAD]-(r:Reply) RETURN r ORDER BY coalesce(r.created_at,r.updated_at) DESC LIMIT 200",
        params
    )]
    holds = [r["h"] for r in _run(
        "MATCH (p:Prospect {email:$email})-[:HAS_HOLD]->(h:CalendarHold) RETURN h ORDER BY coalesce(h.start,h.created_at) DESC LIMIT 50",
        params
    )]
    return {"thread": thread, "inbound": inbound, "replies": replies, "holds": holds}

# ─────────────────────────────────────────────────────────
# GET /api/eco-local/recruiting/runs
# ─────────────────────────────────────────────────────────
@router.get("/runs")
def get_runs() -> Dict[str, Any]:
    rows = _run("MATCH (r:ECO LocalRun) RETURN r ORDER BY r.date DESC LIMIT 90")
    return {"runs": [r["r"] for r in rows]}

# ─────────────────────────────────────────────────────────
# GET /api/eco-local/recruiting/runs/:dateISO/drafts
# ─────────────────────────────────────────────────────────
@router.get("/runs/{dateISO}/drafts")
def get_run_drafts(dateISO: str) -> Dict[str, Any]:
    rows = _run("MATCH (m:Draft {run_date: date($d)}) RETURN m ORDER BY m.email", {"d": dateISO})
    return {"drafts": [r["m"] for r in rows]}

# ─────────────────────────────────────────────────────────
# GET /api/eco-local/recruiting/activity
# ─────────────────────────────────────────────────────────
@router.get("/activity")
def activity() -> Dict[str, Any]:
    # Use a single CALL { … UNION ALL … } scope and return 'item' to avoid shadowing
    cy = """
    CALL {
      MATCH (m:InboundEmail)-[:IN_THREAD]->(t:Thread)
      RETURN { ts: coalesce(m.received_at, m.created_at), action:'inbound', prospect:t.email, subject:m.subject, external_id:m.key } AS item
      UNION ALL
      MATCH (r:Reply)-[:IN_THREAD]->(t:Thread)
      RETURN { ts: coalesce(r.created_at, r.updated_at), action:'reply', prospect:t.email, subject:r.subject, external_id:r.message_id } AS item
    }
    RETURN item ORDER BY item.ts DESC LIMIT 200
    """
    items = [r["item"] for r in _run(cy)]
    return {"items": items}

# ─────────────────────────────────────────────────────────
# POST /api/eco-local/recruiting/inbox/poll  (no external deps)
# ─────────────────────────────────────────────────────────
@router.post("/inbox/poll")
def inbox_poll() -> Dict[str, int]:
    return {"processed": 0}

# ─────────────────────────────────────────────────────────
# POST /api/eco-local/recruiting/prospects/:id/mark-won
# ─────────────────────────────────────────────────────────
@router.post("/prospects/{pid}/mark-won")
def set_won(pid: str) -> Dict[str, Any]:
    rows = _run("MATCH (p:Prospect {id:$pid}) RETURN p", {"pid": pid})
    if not rows:
        raise HTTPException(404, "Prospect not found")
    _run("MATCH (p:Prospect {id:$pid}) SET p.won = true, p.won_at = datetime(), p.updated_at = datetime()", {"pid": pid})
    return {"ok": True}

# ─────────────────────────────────────────────────────────
# POST /api/eco-local/recruiting/prospects/:id/unsubscribe
# ─────────────────────────────────────────────────────────
@router.post("/prospects/{pid}/unsubscribe")
def set_unsub(pid: str) -> Dict[str, Any]:
    rows = _run("MATCH (p:Prospect {id:$pid}) RETURN p", {"pid": pid})
    if not rows:
        raise HTTPException(404, "Prospect not found")
    _run("MATCH (p:Prospect {id:$pid}) SET p.unsubscribed = true, p.unsubscribed_at = datetime(), p.updated_at = datetime()", {"pid": pid})
    return {"ok": True}

# ─────────────────────────────────────────────────────────
# POST /api/eco-local/recruiting/threads/:email/nudge
# (graph-only placeholder: records a Reply; no email send)
# ─────────────────────────────────────────────────────────
@router.post("/threads/{email}/nudge")
def nudge(email: str) -> Dict[str, Any]:
    subject = "Quick follow-up - Ecodia"
    html = "<p>Just checking in to see if you had a moment to chat about Ecodia’s local value loops.</p>"
    mid = f"nudged-{datetime.utcnow().isoformat()}"
    cy = """
    MERGE (t:Thread {email:$email})
      ON CREATE SET t.created_at = datetime()
    SET t.last_outbound_at = datetime(), t.last_outbound_date = date(datetime())
    MERGE (r:Reply {message_id:$mid})
      ON CREATE SET r.created_at = datetime()
    SET r.subject = $subject, r.html = $html, r.updated_at = datetime()
    MERGE (r)-[:IN_THREAD]->(t)
    """
    _run(cy, {"email": email, "mid": mid, "subject": subject, "html": html})
    return {"ok": True, "message_id": mid}

# ─────────────────────────────────────────────────────────
# POST /api/eco-local/recruiting/threads/:email/cancel-holds
# (graph-only: detaches HAS_HOLD and marks holds canceled)
# ─────────────────────────────────────────────────────────
@router.post("/threads/{email}/cancel-holds")
def cancel_holds(email: str) -> Dict[str, Any]:
    cy = """
    MATCH (p:Prospect {email:$email})-[rel:HAS_HOLD]->(h:CalendarHold)
    DELETE rel
    SET h.status = 'canceled', h.updated_at = datetime()
    RETURN count(h) AS touched
    """
    touched = _run(cy, {"email": email})[0]["touched"]
    return {"ok": True, "touched": touched}

# ─────────────────────────────────────────────────────────
# POST /api/eco-local/recruiting/runs/:date/create
# ─────────────────────────────────────────────────────────
@router.post("/runs/{dateISO}/create")
def runs_create(dateISO: str) -> Dict[str, Any]:
    d = _safe_date(dateISO).isoformat()
    cy = """
    MERGE (r:ECO LocalRun {date: date($d)})
      ON CREATE SET r.created_at = datetime()
    RETURN r
    """
    row = _run(cy, {"d": d})[0]["r"]
    return {"run": row}

# ─────────────────────────────────────────────────────────
# POST /api/eco-local/recruiting/runs/:date/freeze
# ─────────────────────────────────────────────────────────
@router.post("/runs/{dateISO}/freeze")
def runs_freeze(dateISO: str) -> Dict[str, Any]:
    d = _safe_date(dateISO).isoformat()
    _run("MERGE (r:ECO LocalRun {date: date($d)}) SET r.frozen = true, r.frozen_at = datetime()", {"d": d})
    return {"ok": True}

# ─────────────────────────────────────────────────────────
# POST /api/eco-local/recruiting/runs/:date/send
# (graph-only: mark drafts as sent; bump thread/prospect)
# ─────────────────────────────────────────────────────────
@router.post("/runs/{dateISO}/send")
def runs_send(dateISO: str) -> Dict[str, Any]:
    d = _safe_date(dateISO).isoformat()
    cy = """
    MATCH (m:Draft {run_date: date($d)})
    SET m.sent = true, m.sent_at = datetime()
    WITH m
    MERGE (t:Thread {email: m.email})
      ON CREATE SET t.created_at = datetime()
    SET t.last_outbound_at = datetime(), t.last_outbound_date = date(datetime())
    MERGE (m)-[:IN_THREAD]->(t)
    WITH m, t
    MATCH (p:Prospect {email: m.email})
    SET p.outreach_started = true,
        p.attempt_count = coalesce(p.attempt_count, 0) + 1,
        p.last_outreach_at = datetime(),
        p.updated_at = datetime()
    RETURN count(m) AS sent
    """
    sent = _run(cy, {"d": d})[0]["sent"]
    return {"date": d, "sent": int(sent)}
