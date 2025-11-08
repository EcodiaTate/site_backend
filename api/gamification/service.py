from __future__ import annotations
from typing import List, Dict, Optional, Tuple
from neo4j import Session

# ───────────────────────────────────────────────────────────────────────────────
# READ: public-facing
# ───────────────────────────────────────────────────────────────────────────────

def get_user_badges_and_awards(s: Session, *, uid: str) -> Dict[str, List[Dict]]:
    badges_rec = s.run("""
      MATCH (u:User {id:$uid})-[:EARNED_BADGE]->(ba:BadgeAward)-[:OF]->(bt:BadgeType)
      OPTIONAL MATCH (ba)-[:IN_SEASON]->(ss:Season)
      RETURN collect({
        id: ba.id,
        at: toString(ba.at),
        tier: ba.tier,
        badge_id: bt.id,
        season: ss.id
      }) AS badges
    """, uid=uid).single()
    awards_rec = s.run("""
      MATCH (u:User {id:$uid})-[:WON]->(aw:Award)-[:OF]->(at:AwardType)
      OPTIONAL MATCH (aw)-[:IN_SEASON]->(ss:Season)
      RETURN collect({
        id: aw.id,
        at: toString(aw.at),
        rank: aw.rank,
        period: aw.period,
        award_type_id: at.id,
        season: ss.id
      }) AS awards
    """, uid=uid).single()
    return {"badges": badges_rec["badges"], "awards": awards_rec["awards"]}

def get_business_awards(s: Session, *, bid: str) -> Dict[str, List[Dict]]:
    awards_rec = s.run("""
      MATCH (b:BusinessProfile {id:$bid})-[:WON]->(aw:Award)-[:OF]->(at:AwardType {scope:'business'})
      OPTIONAL MATCH (aw)-[:IN_SEASON]->(ss:Season)
      RETURN collect({
        id: aw.id,
        at: toString(aw.at),
        rank: aw.rank,
        period: aw.period,
        award_type_id: at.id,
        season: ss.id
      }) AS awards
    """, bid=bid).single()
    return {"awards": awards_rec["awards"]}

# ───────────────────────────────────────────────────────────────────────────────
# READ: admin (catalog & seasons)
# ───────────────────────────────────────────────────────────────────────────────

def list_badge_types(s: Session) -> List[Dict]:
    recs = s.run("MATCH (t:BadgeType) RETURN t ORDER BY toLower(t.name) ASC")
    out: List[Dict] = []
    for r in recs:
        t = dict(r["t"])
        out.append({
            "id": t.get("id"), "name": t.get("name"),
            "icon": t.get("icon"), "color": t.get("color"),
            "kind": t.get("kind"), "rule": t.get("rule"),
            "tier": t.get("tier"), "max_tier": t.get("max_tier"),
        })
    return out

def list_award_types(s: Session) -> List[Dict]:
    recs = s.run("MATCH (t:AwardType) RETURN t ORDER BY toLower(t.name) ASC")
    out: List[Dict] = []
    for r in recs:
        t = dict(r["t"])
        out.append({
            "id": t.get("id"), "name": t.get("name"),
            "icon": t.get("icon"), "color": t.get("color"),
            "scope": t.get("scope"), "rank_limit": t.get("rank_limit"),
        })
    return out

def list_seasons(s: Session) -> List[Dict]:
    recs = s.run("MATCH (ss:Season) RETURN ss ORDER BY ss.start DESC")
    out: List[Dict] = []
    for r in recs:
        x = dict(r["ss"])
        out.append({
            "id": x.get("id"), "label": x.get("label"),
            "start": str(x.get("start")), "end": str(x.get("end")),
            "theme": x.get("theme"),
        })
    return out

# ───────────────────────────────────────────────────────────────────────────────
# UPSERT: admin
# ───────────────────────────────────────────────────────────────────────────────

def upsert_badge_type(s: Session, payload: Dict) -> Dict:
    rec = s.run("""
      MERGE (t:BadgeType {id:$id})
      SET t.name=$name, t.icon=$icon, t.color=$color,
          t.kind=$kind, t.rule=$rule, t.tier=$tier, t.max_tier=$max_tier
      RETURN t
    """, **payload).single()
    return dict(rec["t"])

def delete_badge_type(s: Session, *, id: str) -> None:
    s.run("MATCH (t:BadgeType {id:$id}) DETACH DELETE t", id=id)

def upsert_award_type(s: Session, payload: Dict) -> Dict:
    rec = s.run("""
      MERGE (t:AwardType {id:$id})
      SET t.name=$name, t.icon=$icon, t.color=$color,
          t.scope=$scope, t.rank_limit=$rank_limit
      RETURN t
    """, **payload).single()
    return dict(rec["t"])

def delete_award_type(s: Session, *, id: str) -> None:
    s.run("MATCH (t:AwardType {id:$id}) DETACH DELETE t", id=id)

def upsert_season(s: Session, payload: Dict) -> Dict:
    rec = s.run("""
      MERGE (ss:Season {id:$id})
      SET ss.label=$label, ss.start=datetime($start), ss.end=datetime($end), ss.theme=$theme
      RETURN ss
    """, **payload).single()
    return dict(rec["ss"])

def delete_season(s: Session, *, id: str) -> None:
    s.run("MATCH (ss:Season {id:$id}) DETACH DELETE ss", id=id)

# ───────────────────────────────────────────────────────────────────────────────
# BADGE EVALUATION (user)
# ───────────────────────────────────────────────────────────────────────────────

def _get_user_stats_for_rules(s: Session, *, uid: str, season_id: Optional[str]) -> Dict:
    # total_eco, actions_total (approved submissions), season_actions (within season) and simple streak
    stats = s.run("""
      MATCH (u:User {id:$uid})
      OPTIONAL MATCH (u)-[:EARNED]->(t:EcoTx)
      WITH u, toInteger(sum(coalesce(t.eco,0))) AS total_eco
      OPTIONAL MATCH (u)-[:SUBMITTED]->(s1:Submission {state:'approved'})
      WITH u, total_eco, count(s1) AS actions_total
      WITH u, total_eco, actions_total
      RETURN { total_eco: total_eco, actions_total: actions_total } AS base
    """, uid=uid).single()["base"]

    season_actions = None
    if season_id:
        # Bound by season window
        rec = s.run("""
          MATCH (ss:Season {id:$sid})
          WITH ss
          MATCH (:User {id:$uid})-[:SUBMITTED]->(s1:Submission {state:'approved'})
          WITH ss, s1 WHERE
            (s1.reviewed_at IS NOT NULL AND datetime(s1.reviewed_at) >= ss.start AND datetime(s1.reviewed_at) < ss.end)
            OR (s1.reviewed_at IS NULL AND s1.created_at IS NOT NULL AND datetime(s1.created_at) >= ss.start AND datetime(s1.created_at) < ss.end)
          RETURN count(s1) AS c
        """, sid=season_id, uid=uid).single()
        season_actions = int(rec["c"])

    # streak_days (very light heuristic: distinct dates of activity in the last 30 days)
    streak_rec = s.run("""
      MATCH (u:User {id:$uid})
      OPTIONAL MATCH (u)-[:EARNED]->(t:EcoTx)
      WITH u, collect(date(t.at)) AS d1
      OPTIONAL MATCH (u)-[:SUBMITTED]->(s1:Submission {state:'approved'})
      WITH u, d1 + collect(date(datetime(coalesce(s1.reviewed_at, s1.created_at)))) AS days
      WITH [d IN days WHERE d IS NOT NULL AND d >= date() - duration('P30D')] AS recent
      RETURN size(apoc.coll.toSet(recent)) AS active_days
    """, uid=uid).single()
    streak_days = int(streak_rec["active_days"]) if streak_rec and streak_rec["active_days"] is not None else 0

    stats["season_actions"] = season_actions or 0
    stats["streak_days"] = streak_days
    return stats

def _should_grant(rule: Dict, stats: Dict) -> bool:
    # Only support a single simple rule: threshold gte on a field
    if not rule: 
        return False
    if rule.get("type") == "threshold":
        field = rule.get("field")
        gte = rule.get("gte")
        try:
            return stats.get(field, 0) >= int(gte)
        except Exception:
            return False
    return False

def evaluate_badges_for_user(s: Session, *, uid: str, season_id: Optional[str]) -> Dict:
    stats = _get_user_stats_for_rules(s, uid=uid, season_id=season_id)
    # fetch all badge types
    types = s.run("MATCH (t:BadgeType) RETURN t").value("t")

    granted: List[str] = []
    for t in types:
        bt = dict(t)
        if not _should_grant(bt.get("rule"), stats):
            continue
        # already granted?
        already = s.run("""
          MATCH (:User {id:$uid})-[:EARNED_BADGE]->(:BadgeAward)-[:OF]->(t:BadgeType {id:$bid})
          RETURN count(*) AS c
        """, uid=uid, bid=bt["id"]).single()["c"]
        if already and int(already) > 0:
            continue
        # grant
        rec = s.run("""
          MATCH (u:User {id:$uid}), (t:BadgeType {id:$bid})
          CREATE (ba:BadgeAward {id: randomUUID(), at: datetime(), tier: coalesce($tier, null)})
          MERGE (u)-[:EARNED_BADGE]->(ba)
          MERGE (ba)-[:OF]->(t)
          WITH ba
          OPTIONAL MATCH (ss:Season {id:$sid})
          FOREACH (_ IN CASE WHEN ss IS NULL THEN [] ELSE [1] END | MERGE (ba)-[:IN_SEASON]->(ss))
          RETURN ba.id AS id
        """, uid=uid, bid=bt["id"], sid=season_id, tier=bt.get("tier")).single()
        granted.append(rec["id"])
    return {"granted": granted, "stats": stats}

# ───────────────────────────────────────────────────────────────────────────────
# MONTHLY AWARDS (users & businesses)
# ───────────────────────────────────────────────────────────────────────────────

def mint_monthly_awards(s: Session, *, start: str, end: str,
                        season_id: str,
                        youth_award_type_id: str, business_award_type_id: str,
                        youth_limit: int, business_limit: int) -> Dict:
    # Youth winners by ECO
    youth = s.run("""
      CALL {
        MATCH (u:User)
        OPTIONAL MATCH (u)-[:EARNED]->(t:EcoTx)
          WHERE t.at IS NOT NULL AND t.at >= datetime($start) AND t.at < datetime($end)
        WITH u, toInteger(sum(coalesce(t.eco,0))) AS eco
        RETURN u.id AS uid, eco
        ORDER BY eco DESC, uid ASC
        LIMIT $y_lim
      }
      RETURN collect({uid: uid, eco: eco}) AS rows
    """, start=start, end=end, y_lim=youth_limit).single()["rows"]

    # Business winners by ECO minted FROM business
    biz = s.run("""
      CALL {
        MATCH (b:BusinessProfile)
        OPTIONAL MATCH (t:EcoTx)-[:FROM]->(b)
          WHERE t.at IS NOT NULL AND t.at >= datetime($start) AND t.at < datetime($end)
        WITH b, toInteger(sum(coalesce(t.eco,0))) AS eco
        RETURN b.id AS bid, eco
        ORDER BY eco DESC, bid ASC
        LIMIT $b_lim
      }
      RETURN collect({bid: bid, eco: eco}) AS rows
    """, start=start, end=end, b_lim=business_limit).single()["rows"]

    # Grant youth awards
    granted_youth = []
    for idx, row in enumerate(youth):
        rec = s.run("""
          MATCH (u:User {id:$uid})
          MATCH (awt:AwardType {id:$awid, scope:'youth'})
          MATCH (ss:Season {id:$sid})
          CREATE (aw:Award {id: randomUUID(), at: datetime(), rank: $rank, period:'monthly'})
          MERGE (u)-[:WON]->(aw)
          MERGE (aw)-[:OF]->(awt)
          MERGE (aw)-[:IN_SEASON]->(ss)
          RETURN aw.id AS id
        """, uid=row["uid"], awid=youth_award_type_id, sid=season_id, rank=idx+1).single()
        granted_youth.append(rec["id"])

    # Grant business awards
    granted_biz = []
    for idx, row in enumerate(biz):
        rec = s.run("""
          MATCH (b:BusinessProfile {id:$bid})
          MATCH (awt:AwardType {id:$awid, scope:'business'})
          MATCH (ss:Season {id:$sid})
          CREATE (aw:Award {id: randomUUID(), at: datetime(), rank: $rank, period:'monthly'})
          MERGE (b)-[:WON]->(aw)
          MERGE (aw)-[:OF]->(awt)
          MERGE (aw)-[:IN_SEASON]->(ss)
          RETURN aw.id AS id
        """, bid=row["bid"], awid=business_award_type_id, sid=season_id, rank=idx+1).single()
        granted_biz.append(rec["id"])

    # Optional: store leaderboard snapshots (simple)
    s.run("""
      MATCH (awt:AwardType {id:$y_awid})
      CREATE (:LeaderboardSnapshot {
        id: randomUUID(), at: datetime(), period:'monthly', scope:'youth',
        start: datetime($start), end: datetime($end)
      })-[:OF]->(awt)
    """, y_awid=youth_award_type_id, start=start, end=end)
    s.run("""
      MATCH (awt:AwardType {id:$b_awid})
      CREATE (:LeaderboardSnapshot {
        id: randomUUID(), at: datetime(), period:'monthly', scope:'business',
        start: datetime($start), end: datetime($end)
      })-[:OF]->(awt)
    """, b_awid=business_award_type_id, start=start, end=end)

    return {"granted_youth": granted_youth, "granted_business": granted_biz}
