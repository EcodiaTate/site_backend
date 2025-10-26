from __future__ import annotations
from typing import Dict, Any, List, Optional, Tuple
from uuid import uuid4
from datetime import datetime
from neo4j import Session

# Back-compat note:
# - We keep the original behavior (list, enroll, standings) but add stronger checks,
#   richer metrics, and extra utilities. The router continues to expose the old routes.

# ----------------- helpers -----------------
def _shape_tournament(alias: str = "tr") -> str:
    return f"""{alias}{{
      .*, start: toString({alias}.start), end: toString({alias}.end),
      created_at: toString({alias}.created_at), updated_at: toString({alias}.updated_at)
    }} AS {alias}"""

def _now_iso() -> str:
    return datetime.utcnow().isoformat()

def _enrollment_count(session: Session, tid: str) -> int:
    row = session.run("""
      MATCH (:User)-[:ENROLLED]->(tr:Tournament {id:$tid}) RETURN count(*) AS c1
    """, tid=tid).single()
    solo = row["c1"] or 0
    row2 = session.run("""
      MATCH (:Team)-[:ENROLLED]->(tr:Tournament {id:$tid}) RETURN count(*) AS c2
    """, tid=tid).single()
    teams = row2["c2"] or 0
    return int(solo + teams)

def _window_of(tr: Dict[str, Any]) -> Tuple[str, str]:
    return tr["start"], tr["end"]

def _team_size(session: Session, team_id: str) -> int:
    return session.run("""
      MATCH (:User)-[:MEMBER_OF]->(t:Team {id:$tid}) RETURN count(*) AS n
    """, tid=team_id).single()["n"] or 0

# ----------------- CRUD / listing -----------------
def list_tournaments(session: Session,
                     status: Optional[str] = None,
                     visibility: Optional[str] = None,
                     division: Optional[str] = None) -> List[Dict[str, Any]]:
    where = []
    params: Dict[str, Any] = {}
    if status:
        where.append("coalesce(tr.status,'draft') = $status"); params["status"] = status
    if visibility:
        where.append("coalesce(tr.visibility,'public') = $visibility"); params["visibility"] = visibility
    if division:
        where.append("toLower(coalesce(tr.division,'')) = toLower($division)"); params["division"] = division
    wc = "WHERE " + " AND ".join(where) if where else ""
    rows = session.run(f"""
      MATCH (tr:Tournament)
      {wc}
      RETURN {_shape_tournament("tr")}
      ORDER BY toString(tr.start) DESC
    """, **params).data()
    return [dict(r["tr"]) for r in rows]

def create_tournament(session: Session, payload: Dict[str, Any]) -> Dict[str, Any]:
    tid = uuid4().hex
    now = _now_iso()
    rec = session.run(f"""
      CREATE (tr:Tournament {{
        id:$tid, name:$name, season:$season, start:datetime($start), end:datetime($end),
        mode:$mode, metric:$metric, visibility:$visibility, status:$status,
        division:$division, max_participants:$maxp, allow_late_join:$late,
        min_team_size:$minsz, max_team_size:$maxsz, tie_breaker:$tb,
        rules:$rules, prizes:$prizes, created_at:datetime($now), updated_at:datetime($now)
      }})
      RETURN {_shape_tournament("tr")}
    """, {
      "tid": tid, "now": now,
      "name": payload["name"], "season": payload.get("season"),
      "start": payload["start"], "end": payload["end"],
      "mode": payload["mode"], "metric": payload.get("metric","eco"),
      "visibility": payload.get("visibility","public"),
      "status": payload.get("status","upcoming"),
      "division": payload.get("division"),
      "maxp": payload.get("max_participants"),
      "late": bool(payload.get("allow_late_join", False)),
      "minsz": payload.get("min_team_size"),
      "maxsz": payload.get("max_team_size"),
      "tb": payload.get("tie_breaker","highest_single_day"),
      "rules": payload.get("rules"),
      "prizes": payload.get("prizes", []),
    }).single()
    return dict(rec["tr"])

def update_tournament(session: Session, tid: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    sets = ["tr.updated_at = datetime($now)"]
    params: Dict[str, Any] = {"tid": tid, "now": _now_iso()}
    for k in ["name","season","start","end","metric","visibility","status","division",
              "max_participants","allow_late_join","min_team_size","max_team_size","tie_breaker","rules","prizes"]:
        if k in payload and payload[k] is not None:
            v = payload[k]
            if k in ("start","end"):
                params[k] = v; sets.append(f"tr.{k} = datetime(${k})")
            else:
                params[k] = v; sets.append(f"tr.{k} = ${k}")
    if not sets: sets.append("tr.id = tr.id")
    rec = session.run(f"""
      MATCH (tr:Tournament {{id:$tid}})
      SET {", ".join(sets)}
      RETURN {_shape_tournament("tr")}
    """, params).single()
    if not rec: raise ValueError("not_found")
    return dict(rec["tr"])

# ----------------- enroll / withdraw -----------------
def enroll(session: Session, uid: str, tid: str, scope: str, team_id: Optional[str]) -> Dict[str, Any]:
    tr = session.run("MATCH (tr:Tournament {id:$tid}) RETURN tr", tid=tid).single()
    if not tr: raise ValueError("Tournament not found")
    trd = dict(tr["tr"])
    start, end = _window_of(trd)

    # capacity guard
    cap = trd.get("max_participants")
    if cap is not None and _enrollment_count(session, tid) >= cap:
        return {"ok": False, "already_enrolled": False, "note": "full"}

    # mode guard
    if trd.get("mode") == "team":
        if scope != "team" or not team_id:
            return {"ok": False, "note": "team_id_required", "scope": "team"}
        # team size guard
        minsz = trd.get("min_team_size") or 1
        maxsz = trd.get("max_team_size")
        sz = _team_size(session, team_id)
        if sz < minsz: return {"ok": False, "note": "team_too_small"}
        if maxsz and sz > maxsz: return {"ok": False, "note": "team_too_large"}

        # dedupe
        dedupe = session.run("""
          MATCH (t:Team {id:$tidTeam})-[:ENROLLED]->(tr:Tournament {id:$tid}) RETURN count(*) AS c
        """, tidTeam=team_id, tid=tid).single()["c"]
        if dedupe and int(dedupe) > 0:
            return {"ok": True, "already_enrolled": True, "scope": "team", "entrant_id": team_id}

        session.run("""
          MATCH (t:Team {id:$team_id}), (tr:Tournament {id:$tid})
          MERGE (t)-[:ENROLLED]->(tr)
        """, {"team_id": team_id, "tid": tid})
        return {"ok": True, "already_enrolled": False, "scope": "team", "entrant_id": team_id}

    # SOLO mode
    else:
        # dedupe
        dedupe = session.run("""
          MATCH (u:User {id:$uid})-[:ENROLLED]->(tr:Tournament {id:$tid}) RETURN count(*) AS c
        """, uid=uid, tid=tid).single()["c"]
        if dedupe and int(dedupe) > 0:
            return {"ok": True, "already_enrolled": True, "scope": "solo", "entrant_id": uid}

        session.run("""
          MATCH (u:User {id:$uid}), (tr:Tournament {id:$tid})
          MERGE (u)-[:ENROLLED]->(tr)
        """, {"uid": uid, "tid": tid})
        return {"ok": True, "already_enrolled": False, "scope": "solo", "entrant_id": uid}

def withdraw(session: Session, uid: str, tid: str, scope: str, team_id: Optional[str]) -> Dict[str, Any]:
    if scope == "team" and team_id:
        session.run("""
          MATCH (:Team {id:$tidTeam})-[r:ENROLLED]->(:Tournament {id:$tid}) DELETE r
        """, tidTeam=team_id, tid=tid)
        return {"ok": True}
    session.run("""
      MATCH (:User {id:$uid})-[r:ENROLLED]->(:Tournament {id:$tid}) DELETE r
    """, uid=uid, tid=tid)
    return {"ok": True}

def enrollment(session: Session, tid: str) -> Dict[str, Any]:
    cap = session.run("MATCH (tr:Tournament {id:$tid}) RETURN tr.max_participants AS cap", tid=tid).single()["cap"]
    cnt = _enrollment_count(session, tid)
    return {"entrants": int(cnt), "capacity": cap}

# ----------------- standings / leaderboard -----------------
def _score_for(metric: str, eco: int, completions: int, members: Optional[int]) -> float:
    if metric == "eco": return float(eco)
    if metric == "completions": return float(completions)
    if metric == "eco_per_member":
        m = max(1, members or 1)
        return float(eco) / float(m)
    return float(eco)

def standings(session: Session, tid: str) -> List[Dict[str, Any]]:
    # Back-compat call: default metric window = tournament window, metric = tr.metric
    tr = session.run("MATCH (tr:Tournament {id:$tid}) RETURN tr", {"tid": tid}).single()
    if not tr:
        raise ValueError("Tournament not found")
    d = dict(tr["tr"])
    start, end = _window_of(d)
    metric = d.get("metric","eco")
    mode = d.get("mode","solo")

    if mode == "team":
        rows = session.run("""
          MATCH (tm:Team)-[:ENROLLED]->(tr:Tournament {id:$tid})
          OPTIONAL MATCH (u:User)-[:MEMBER_OF]->(tm)
          WITH tr, tm, count(u) AS members
          OPTIONAL MATCH (u2:User)-[:MEMBER_OF]->(tm)
          OPTIONAL MATCH (u2)-[:EARNED]->(tx:EcoTx)
            WHERE tx.at >= datetime($start) AND tx.at < datetime($end)
          WITH tm, members, toInteger(sum(coalesce(tx.eco,0))) AS eco
          OPTIONAL MATCH (sub:Submission {state:'approved'})
            WHERE sub.team_id = tm.id AND sub.created_at >= datetime($start) AND sub.created_at < datetime($end)
          WITH tm, members, eco, count(sub) AS completions
          RETURN tm.id AS id, tm.name AS name, members, eco, completions,
                 toString(max(sub.created_at)) AS last_activity_at
          ORDER BY eco DESC, id ASC LIMIT 100
        """, {"tid": tid, "start": start, "end": end}).data()
    else:
        rows = session.run("""
          MATCH (u:User)-[:ENROLLED]->(tr:Tournament {id:$tid})
          OPTIONAL MATCH (u)-[:EARNED]->(tx:EcoTx)
            WHERE tx.at >= datetime($start) AND tx.at < datetime($end)
          WITH u, toInteger(sum(coalesce(tx.eco,0))) AS eco
          OPTIONAL MATCH (u)-[:SUBMITTED]->(sub:Submission {state:'approved'})
            WHERE sub.created_at >= datetime($start) AND sub.created_at < datetime($end)
          RETURN u.id AS id, coalesce(u.display_name, u.id) AS name, eco, count(sub) AS completions,
                 toString(max(sub.created_at)) AS last_activity_at
          ORDER BY eco DESC, id ASC LIMIT 100
        """, {"tid": tid, "start": start, "end": end}).data()

    out, last_score, rank = [], None, 0
    for i, r in enumerate(rows, start=1):
        eco = int(r.get("eco") or 0)
        comps = int(r.get("completions") or 0)
        members = r.get("members")
        score = _score_for(metric, eco, comps, members)
        if last_score is None or score < last_score:
            rank = i
        last_score = score
        out.append({
            "id": r.get("id"),
            "name": r.get("name"),
            "eco": eco,
            "completions": comps,
            "score": float(round(score, 4)),
            "rank": rank,
            "members": int(members) if members is not None else None,
            "last_activity_at": r.get("last_activity_at"),
        })
    return out

def leaderboard(session: Session, tid: str, metric: Optional[str] = None) -> Dict[str, Any]:
    tr = session.run("MATCH (tr:Tournament {id:$tid}) RETURN tr", {"tid": tid}).single()
    if not tr: raise ValueError("Tournament not found")
    d = dict(tr["tr"])
    metric = metric or d.get("metric","eco")
    start, end = _window_of(d)
    return {"tid": tid, "metric": metric, "window": {"start": start, "end": end}, "rows": standings(session, tid)}
