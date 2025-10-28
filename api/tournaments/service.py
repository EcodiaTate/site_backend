from __future__ import annotations
from typing import Dict, Any, List, Optional, Tuple
from uuid import uuid4
from datetime import datetime, timezone
from neo4j import Session

# ----------------- helpers -----------------
def _utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def _to_iso(v: Any) -> str:
    return str(v) if v is not None else ""

def _shape_tournament(alias: str = "tr") -> str:
    # Keep everything as strings for SSR alignment
    return f"""{alias}{{
      .*, 
      start: toString({alias}.start), 
      end: toString({alias}.end),
      created_at: toString({alias}.created_at), 
      updated_at: toString({alias}.updated_at)
    }} AS {alias}"""

def _compute_status(raw_status: str, start_iso: str, end_iso: str) -> str:
    if raw_status in ("draft", "archived"):
        return raw_status
    now = datetime.now(timezone.utc)
    try:
        start = datetime.fromisoformat(start_iso.replace("Z", "+00:00"))
        end = datetime.fromisoformat(end_iso.replace("Z", "+00:00"))
    except Exception:
        return raw_status or "upcoming"
    if now < start:
        return "upcoming"
    if start <= now < end:
        return "active"
    return "ended"

def _is_join_open(tr: Dict[str, Any], entrants: int) -> bool:
    status = tr.get("computed_status") or tr.get("status")
    if status in ("draft", "archived", "ended"):
        return False
    cap = tr.get("max_participants")
    if cap is not None and entrants >= int(cap):
        return False
    if status == "active":
        return bool(tr.get("allow_late_join", False))
    return True

def _enrollment_count(session: Session, tid: str) -> int:
    solo = int(session.run(
        "MATCH (:User)-[:ENROLLED]->(tr:Tournament {id:$tid}) RETURN count(*) AS c", tid=tid
    ).single()["c"] or 0)
    teams = int(session.run(
        "MATCH (:Team)-[:ENROLLED]->(tr:Tournament {id:$tid}) RETURN count(*) AS c", tid=tid
    ).single()["c"] or 0)
    return solo + teams

def _window_of(tr: Dict[str, Any]) -> Tuple[str, str]:
    return tr["start"], tr["end"]

def _team_size(session: Session, team_id: str) -> int:
    return int(session.run(
        "MATCH (:User)-[:MEMBER_OF]->(t:Team {id:$tid}) RETURN count(*) AS n", tid=team_id
    ).single()["n"] or 0)

def _exists_user(session: Session, uid: str) -> bool:
    rec = session.run("MATCH (u:User {id:$uid}) RETURN 1 AS ok", uid=uid).single()
    return bool(rec)

def _exists_team(session: Session, team_id: str) -> bool:
    rec = session.run("MATCH (t:Team {id:$tid}) RETURN 1 AS ok", tid=team_id).single()
    return bool(rec)

def _fetch_tournament_core(session: Session, tid: str) -> Dict[str, Any]:
    rec = session.run(f"""
      MATCH (tr:Tournament {{id:$tid}})
      RETURN {_shape_tournament("tr")}
    """, tid=tid).single()
    if not rec:
        raise ValueError("not_found")
    return dict(rec["tr"])

def _fetch_tournament_with_prizes(session: Session, tid: str) -> Dict[str, Any]:
    tr = _fetch_tournament_core(session, tid)
    prizes = session.run("""
      MATCH (t:Tournament {id:$tid})-[:HAS_PRIZE]->(p:Prize)
      RETURN
        p.place AS place,
        p.title AS title,
        CASE WHEN 'badge_key' IN keys(p) THEN p.badge_key ELSE null END AS badge_key,
        CASE WHEN 'description' IN keys(p) THEN p.description ELSE null END AS description
      ORDER BY place ASC
    """, tid=tid).data()

    entrants = _enrollment_count(session, tid)
    cap = tr.get("max_participants")
    cap_left = None if cap is None else max(0, int(cap) - entrants)

    computed = _compute_status(tr.get("status") or "upcoming", tr["start"], tr["end"])

    tr.update({
        "prizes": [{
            "place": int(r.get("place") or 0),
            "title": r.get("title"),
            "badge_key": r.get("badge_key"),
            "description": r.get("description"),
        } for r in prizes],
        "computed_status": computed,
        "is_active_now": (computed == "active"),
        "entrants": int(entrants),
        "capacity_left": cap_left,
        "is_join_open": _is_join_open({**tr, "computed_status": computed}, entrants),
    })
    return tr

def _normalize_prizes(prizes: Any) -> List[Dict[str, Any]]:
    if not prizes:
        return []
    out: List[Dict[str, Any]] = []
    for p in prizes:
        if isinstance(p, dict):
            out.append({
                "place": int(p.get("place") or 0),
                "title": p.get("title"),
                "badge_key": p.get("badge_key"),
                "description": p.get("description"),
            })
    out.sort(key=lambda x: x["place"])
    return out

def _upsert_prizes(session: Session, tid: str, prizes: List[Dict[str, Any]]) -> None:
    prizes = _normalize_prizes(prizes)
    session.run("""
      MATCH (t:Tournament {id:$tid})
      OPTIONAL MATCH (t)-[r:HAS_PRIZE]->(old:Prize)
      DELETE r, old
    """, tid=tid)
    if not prizes:
        return
    session.run("""
      MATCH (t:Tournament {id:$tid})
      WITH t, $prizes AS prizes
      UNWIND prizes AS p
      CREATE (pr:Prize {
        id: toString(randomUUID()),
        place: toInteger(p.place),
        title: p.title,
        badge_key: p.badge_key,
        description: p.description
      })
      MERGE (t)-[:HAS_PRIZE]->(pr)
    """, tid=tid, prizes=prizes)

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

    out = []
    for r in rows:
        tr = dict(r["tr"])
        entrants = _enrollment_count(session, tr["id"])
        computed = _compute_status(tr.get("status") or "upcoming", tr["start"], tr["end"])
        cap = tr.get("max_participants")
        cap_left = None if cap is None else max(0, int(cap) - entrants)
        tr.update({
            "computed_status": computed,
            "is_active_now": (computed == "active"),
            "entrants": int(entrants),
            "capacity_left": cap_left,
            "is_join_open": _is_join_open({**tr, "computed_status": computed}, entrants),
            "prizes": [],  # hydrate prizes via GET /{tid}
        })
        out.append(tr)
    return out

def create_tournament(session: Session, payload: Dict[str, Any]) -> Dict[str, Any]:
    tid = uuid4().hex
    now = _utcnow_iso()

    prizes = payload.get("prizes") or []
    rules = payload.get("rules") or {}

    session.run("""
      CREATE (tr:Tournament {
        id:$tid, name:$name, season:$season,
        start:datetime($start), end:datetime($end),
        mode:$mode, metric:$metric, visibility:$visibility, status:$status,
        division:$division, max_participants:$maxp, allow_late_join:$late,
        min_team_size:$minsz, max_team_size:$maxsz, tie_breaker:$tb,
        rules_url:$rules_url,
        rules_text_md:$rules_text_md,
        anti_cheat:$anti_cheat,
        allowed_sidequest_kinds:$allowed_kinds,
        created_at:datetime($now), updated_at:datetime($now)
      })
    """, {
      "tid": tid, "now": now,
      "name": payload["name"],
      "season": payload.get("season"),
      "start": payload["start"],
      "end": payload["end"],
      "mode": payload["mode"],
      "metric": payload.get("metric","eco"),
      "visibility": payload.get("visibility","public"),
      "status": payload.get("status","upcoming"),
      "division": payload.get("division"),
      "maxp": payload.get("max_participants"),
      "late": bool(payload.get("allow_late_join", False)),
      "minsz": payload.get("min_team_size"),
      "maxsz": payload.get("max_team_size"),
      "tb": payload.get("tie_breaker","highest_single_day"),
      "rules_url": rules.get("rules_url"),
      "rules_text_md": rules.get("text_md"),
      "anti_cheat": rules.get("anti_cheat") or [],
      "allowed_kinds": rules.get("allowed_sidequest_kinds") or [],
    })

    _upsert_prizes(session, tid, prizes)
    return _fetch_tournament_with_prizes(session, tid)

def update_tournament(session: Session, tid: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    sets = ["tr.updated_at = datetime($now)"]
    params: Dict[str, Any] = {"tid": tid, "now": _utcnow_iso()}

    prizes_present = "prizes" in payload
    prizes = payload.get("prizes") if prizes_present else None
    rules_present = "rules" in payload
    rules = payload.get("rules") or {}

    for k in ["name","season","start","end","metric","visibility","status","division",
              "max_participants","allow_late_join","min_team_size","max_team_size","tie_breaker"]:
        if k in payload and payload[k] is not None:
            v = payload[k]
            if k in ("start","end"):
                params[k] = v; sets.append(f"tr.{k} = datetime(${k})")
            else:
                params[k] = v; sets.append(f"tr.{k} = ${k}")

    if rules_present:
        params["rules_url"] = rules.get("rules_url")
        params["rules_text_md"] = rules.get("text_md")
        params["anti_cheat"] = rules.get("anti_cheat") or []
        params["allowed_kinds"] = rules.get("allowed_sidequest_kinds") or []
        sets += [
            "tr.rules_url = $rules_url",
            "tr.rules_text_md = $rules_text_md",
            "tr.anti_cheat = $anti_cheat",
            "tr.allowed_sidequest_kinds = $allowed_kinds",
        ]

    rec = session.run(f"""
      MATCH (tr:Tournament {{id:$tid}})
      SET {", ".join(sets)}
      RETURN tr.id AS id
    """, params).single()
    if not rec:
        raise ValueError("not_found")

    if prizes_present:
        _upsert_prizes(session, tid, prizes)

    return _fetch_tournament_with_prizes(session, tid)

# ----------------- enroll / withdraw -----------------
def enroll(session: Session, uid: str, tid: str, scope: str, team_id: Optional[str]) -> Dict[str, Any]:
    # Ensure tournament exists first (and get window/status/capacity)
    try:
        tr = _fetch_tournament_core(session, tid)
    except ValueError:
        return {"ok": False, "note": "tournament_missing"}

    # Compute guards
    start, end = _window_of(tr)
    entrants = _enrollment_count(session, tid)
    computed = _compute_status(tr.get("status") or "upcoming", start, end)

    cap = tr.get("max_participants")
    if cap is not None and entrants >= int(cap):
        return {"ok": False, "already_enrolled": False, "note": "full"}

    if computed == "ended":
        return {"ok": False, "already_enrolled": False, "note": "ended"}
    if computed == "active" and not bool(tr.get("allow_late_join", False)):
        return {"ok": False, "already_enrolled": False, "note": "late_join_disabled"}

    mode = tr.get("mode", "solo")

    # TEAM
    if mode == "team":
        if scope != "team" or not team_id:
            return {"ok": False, "note": "team_id_required", "scope": "team"}

        if not _exists_team(session, team_id):
            return {"ok": False, "note": "team_or_tournament_missing", "scope": "team"}

        # Team size enforcement
        minsz = tr.get("min_team_size") or 1
        maxsz = tr.get("max_team_size")
        sz = _team_size(session, team_id)
        if sz < minsz:
            return {"ok": False, "note": "team_too_small", "scope": "team"}
        if maxsz and sz > maxsz:
            return {"ok": False, "note": "team_too_large", "scope": "team"}

        # Create/ensure rel
        rec = session.run("""
          MATCH (t:Team {id:$team_id}), (tr:Tournament {id:$tid})
          MERGE (t)-[r:ENROLLED]->(tr)
          ON CREATE SET r.created_at = datetime()
          RETURN t.id AS team_id, r.created_at IS NOT NULL AS created
        """, {"team_id": team_id, "tid": tid}).single()

        if not rec:
            return {"ok": False, "note": "team_or_tournament_missing", "scope": "team"}

        return {
            "ok": True,
            "already_enrolled": not bool(rec["created"]),
            "scope": "team",
            "entrant_id": rec["team_id"],
        }

    # SOLO
    if not _exists_user(session, uid):
        return {"ok": False, "note": "user_or_tournament_missing", "scope": "solo"}

    rec = session.run("""
      MATCH (u:User {id:$uid}), (tr:Tournament {id:$tid})
      MERGE (u)-[r:ENROLLED]->(tr)
      ON CREATE SET r.created_at = datetime()
      RETURN u.id AS uid, r.created_at IS NOT NULL AS created
    """, {"uid": uid, "tid": tid}).single()

    if not rec:
        return {"ok": False, "note": "user_or_tournament_missing", "scope": "solo"}

    return {
        "ok": True,
        "already_enrolled": not bool(rec["created"]),
        "scope": "solo",
        "entrant_id": rec["uid"],
    }

def withdraw(session: Session, uid: str, tid: str, scope: str, team_id: Optional[str]) -> Dict[str, Any]:
    if scope == "team" and team_id:
        session.run("""
          MATCH (:Team {id:$team_id})-[r:ENROLLED]->(:Tournament {id:$tid}) DELETE r
        """, team_id=team_id, tid=tid)
        return {"ok": True}
    session.run("""
      MATCH (:User {id:$uid})-[r:ENROLLED]->(:Tournament {id:$tid}) DELETE r
    """, uid=uid, tid=tid)
    return {"ok": True}

def enrollment(session: Session, tid: str) -> Dict[str, Any]:
    cap = session.run(
        "MATCH (tr:Tournament {id:$tid}) RETURN tr.max_participants AS cap", tid=tid
    ).single()["cap"]
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

def _tie_break(a: Dict[str, Any], b: Dict[str, Any], tie_breaker: str) -> int:
    if tie_breaker == "most_completions":
        return (b["completions"] - a["completions"]) or 0
    if tie_breaker == "earliest_finish":
        la = a.get("last_activity_at"); lb = b.get("last_activity_at")
        if la and lb:
            return -1 if la < lb else (1 if la > lb else 0)
        return 0
    return 0

def standings(session: Session, tid: str) -> List[Dict[str, Any]]:
    tr = _fetch_tournament_core(session, tid)
    start, end = _window_of(tr)
    start_s, end_s = _to_iso(start), _to_iso(end)
    metric = tr.get("metric","eco")
    mode = tr.get("mode","solo")

    if mode == "team":
        rows = session.run("""
        MATCH (tm:Team)-[:ENROLLED]->(tr:Tournament {id:$tid})

        OPTIONAL MATCH (u:User)-[:MEMBER_OF]->(tm)
        WITH tr, tm, collect(u) AS members_list, count(u) AS members

        UNWIND coalesce(members_list, []) AS m1
        OPTIONAL MATCH (m1)-[:EARNED]->(tx:EcoTx)
            WHERE tx.at >= datetime($start) AND tx.at < datetime($end)
        WITH tr, tm, members, toInteger(sum(coalesce(tx.eco,0))) AS eco, members_list

        UNWIND coalesce(members_list, []) AS m2
        OPTIONAL MATCH (m2)-[:SUBMITTED]->(sub:Submission {state:'approved'})
            WHERE sub.created_at >= datetime($start)
              AND sub.created_at < datetime($end)
        WITH tm, members, eco,
             count(DISTINCT sub) AS completions,
             toString(max(sub.created_at)) AS last_activity_at

        RETURN
            tm.id AS id,
            coalesce(tm.name, tm.slug, tm.id) AS name,   // <-- was tm.name
            members,
            eco,
            completions,
            last_activity_at
        ORDER BY eco DESC, id ASC
        LIMIT 100

        """, {"tid": tid, "start": start_s, "end": end_s}).data()
    else:
        rows = session.run("""
          MATCH (u:User)-[:ENROLLED]->(tr:Tournament {id:$tid})
          OPTIONAL MATCH (u)-[:EARNED]->(tx:EcoTx)
            WHERE tx.at >= datetime($start) AND tx.at < datetime($end)
          WITH u, toInteger(sum(coalesce(tx.eco,0))) AS eco
          OPTIONAL MATCH (u)-[:SUBMITTED]->(sub:Submission {state:'approved'})
            WHERE sub.created_at >= datetime($start) AND sub.created_at < datetime($end)
          RETURN
            u.id AS id,
            coalesce(u.display_name, u.displayName, u.username, u.handle, u.id) AS name, 
            eco,
            count(sub) AS completions,
            toString(max(sub.created_at)) AS last_activity_at
          ORDER BY eco DESC, id ASC LIMIT 100
        """, {"tid": tid, "start": start_s, "end": end_s}).data()

    tb = tr.get("tie_breaker", "highest_single_day")
    enriched = []
    for r in rows:
        eco = int(r.get("eco") or 0)
        comps = int(r.get("completions") or 0)
        members = r.get("members")
        score = _score_for(metric, eco, comps, members)
        enriched.append({
            "id": r.get("id"),
            "name": r.get("name"),
            "eco": eco,
            "completions": comps,
            "score": float(round(score, 4)),
            "members": int(members) if members is not None else None,
            "last_activity_at": r.get("last_activity_at"),
        })

    enriched.sort(key=lambda x: (-x["score"], x["id"]))
    if tb != "highest_single_day":
        i = 0
        while i + 1 < len(enriched):
            a, b = enriched[i], enriched[i+1]
            if a["score"] == b["score"]:
                cmp = _tie_break(a, b, tb)
                if cmp > 0:
                    enriched[i], enriched[i+1] = b, a
            i += 1

    out, last_score, rank = [], None, 0
    for i, row in enumerate(enriched, start=1):
        if last_score is None or row["score"] < last_score:
            rank = i
        last_score = row["score"]
        out.append({**row, "rank": rank})
    return out

def leaderboard(session: Session, tid: str, metric: Optional[str] = None) -> Dict[str, Any]:
    tr = _fetch_tournament_core(session, tid)
    metric = metric or tr.get("metric","eco")
    start, end = _window_of(tr)
    return {
        "tid": tid,
        "metric": metric,
        "window": {"start": _to_iso(start), "end": _to_iso(end)},
        "rows": standings(session, tid),
    }

def get_tournament(session: Session, tid: str) -> Dict[str, Any]:
    return _fetch_tournament_with_prizes(session, tid)
