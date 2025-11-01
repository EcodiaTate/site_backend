from __future__ import annotations
from typing import List, Dict, Optional, Tuple, Any
from neo4j import Session
import json

# ───────────────────────────────────────────────────────────────────────────────
# Level math (derived from total_xp)
# Quadratic ramp with soft prestige reset:
# next_level_xp = 100 * (level^2) * (1 + 0.15 * prestige_level)
# ───────────────────────────────────────────────────────────────────────────────
def _level_for_xp(total_xp: int, prestige: int = 0) -> tuple[int, int, int]:
    lvl = 1
    def req(l: int) -> int:
        return int(100 * (l ** 2) * (1 + 0.15 * prestige))
    while total_xp >= req(lvl):
        lvl += 1
    next_level_xp = req(lvl)
    xp_to_next = max(0, next_level_xp - total_xp)
    return lvl, next_level_xp, xp_to_next

# Utility: clamp
def _clamp(n: int, lo: int, hi: int) -> int:
    return lo if n < lo else hi if n > hi else n

# ───────────────────────────────────────────────────────────────────────────────
# Time windows (claim cadence)
# ───────────────────────────────────────────────────────────────────────────────
def _window_bounds(s: Session, cadence: str) -> tuple[str, str]:
    """
    Returns (start_iso, end_iso) UTC ISO strings for a claim window by cadence.
    Cadences: 'once', 'daily', 'weekly', 'monthly', 'seasonal'
    """
    cadence = (cadence or "").lower()
    if cadence == "daily":
        rec = s.run("""
          RETURN toString(datetime().date()) + 'T00:00:00Z' AS start,
                 toString(datetime().date() + duration('P1D')) + 'T00:00:00Z' AS end
        """).single()
    elif cadence == "weekly":
        # ISO week: start Monday
        rec = s.run("""
          WITH datetime().week AS w, datetime().year AS y
          WITH datetime({year:y, week:w, weekday:1}) AS ws
          RETURN toString(ws) AS start, toString(ws + duration('P7D')) AS end
        """).single()
    elif cadence == "monthly":
        rec = s.run("""
        WITH datetime.truncate('month', datetime()) AS ms
        RETURN toString(ms) AS start,
                toString(ms + duration('P1M')) AS end
        """).single()
    elif cadence == "seasonal":
        rec = s.run("""
          MATCH (ss:Season)
          WHERE ss.start <= datetime() AND ss.end > datetime()
          RETURN toString(ss.start) AS start, toString(ss.end) AS end
        """).single() or {"start": None, "end": None}
    else:  # 'once' (or unknown) → current moment to +100y (acts as no-reset)
        rec = s.run("""
          RETURN toString(datetime()) AS start,
                 toString(datetime() + duration('P100Y')) AS end
        """).single()
    return rec["start"], rec["end"]

# ───────────────────────────────────────────────────────────────────────────────
# READ: public-facing
# ───────────────────────────────────────────────────────────────────────────────
def _extract_rule(bt: Dict) -> Dict:
    # Try legacy embedded dict first
    rule = (bt.get("rule") or {}) if isinstance(bt.get("rule"), dict) else {}
    if rule:
        return rule
    # Fall back to flattened
    rt = bt.get("rule_type")
    if rt == "threshold":
        return {"type":"threshold", "field": bt.get("rule_field"), "gte": bt.get("rule_gte")}
    if rt == "title":
        return {"type":"title", "title_id": bt.get("rule_title_id")}
    return {}

def _nearest_badge_progress(s: Session, stats: Dict) -> Tuple[int, Optional[str]]:
    types = s.run("MATCH (t:BadgeType) RETURN t").value("t")
    nxt = []
    for t in types:
        bt = dict(t)
        rule = _extract_rule(bt)
        if rule.get("type") not in ("threshold", "title"):
            continue
        if rule.get("type") == "threshold":
            field = rule.get("field")
            try:
                target = int(rule.get("gte") or 0)
            except:
                continue
            val = int(stats.get(field or "", 0))
            if val < target:
                nxt.append((target, bt.get("name") or bt.get("id"), field))
    if not nxt:
        return 100, None
    target, name, field = sorted(nxt, key=lambda x: x[0])[0]
    current = int(stats.get(field or "", 0))
    pct = int(min(100, max(0, (current / max(1, target)) * 100)))
    return pct, name

def _user_banned(s: Session, uid: str) -> bool:
    rec = s.run("MATCH (u:User {id:$uid}) RETURN coalesce(u.banned,false) AS b", uid=uid).single()
    return bool(rec and rec.get("b"))

def _active_season(s: Session) -> Optional[Dict]:
    rec = s.run("""
      OPTIONAL MATCH (ss:Season)
      WHERE ss.start <= datetime() AND ss.end > datetime()
      RETURN ss
      LIMIT 1
    """).single()
    if not rec or not rec.get("ss"):
        return None
    try:
        return dict(rec["ss"])
    except Exception:
        return None

def _user_prestige(s: Session, uid: str) -> int:
    rec = s.run("MATCH (u:User {id:$uid}) RETURN toInteger(coalesce(u.prestige,0)) AS p", uid=uid).single()
    return int(rec["p"]) if rec and "p" in rec else 0

def _season_actions(s: Session, uid: str, season: Dict) -> int:
    if not season:
        return 0
    return int(s.run("""
        MATCH (u:User {id:$uid})-[:SUBMITTED]->(s1:Submission {state:'approved'})
        WHERE datetime(coalesce(s1.reviewed_at, s1.created_at)) >= $start
          AND datetime(coalesce(s1.reviewed_at, s1.created_at)) <  $end
        RETURN toInteger(count(s1)) AS c
    """, uid=uid, start=season.get("start"), end=season.get("end")).single()["c"] or 0)

def _collect_multipliers(s: Session, uid: str) -> Dict[str, float]:
    """
    Aggregate active multiplicative bonuses based on party size, referrals, season, titles, etc.
    """
    out: Dict[str, float] = {}

    # Season XP boost
    season = _active_season(s)
    if season and season.get("xp_boost"):
        out["season"] = float(season.get("xp_boost") or 1.0)

    # Party bonus (users connected via PARTY_WITH edge in last 3 hours)
    party = s.run("""
      MATCH (u:User {id:$uid})-[:PARTY_WITH]->(p:User)
      WHERE datetime(coalesce(p.party_at, datetime())) >= datetime() - duration('PT3H')
      RETURN count(p) AS c
    """, uid=uid).single()
    party_size = int(party["c"] or 0) + 1  # include self
    if party_size >= 2:
        conf = s.run("MATCH (m:MultiplierConfig {id:'party_bonus'}) RETURN m.value AS v, m.max_stack AS ms").single()
        if conf:
            v = float(conf["v"] or 1.1)
            ms = int(conf["ms"] or 3)
            stacks = _clamp(party_size - 1, 1, ms)
            out["party"] = v ** stacks

    # Referral boost (first 30 days after being referred)
    ref = s.run("""
      MATCH (u:User {id:$uid})<-[:REFERRED]-(r:User)
      WHERE date(u.created_at) >= date() - duration('P30D')
      RETURN count(r) AS c
    """, uid=uid).single()
    if ref and int(ref["c"] or 0) > 0:
        rb = s.run("MATCH (m:MultiplierConfig {id:'referral_boost'}) RETURN m.value AS v").single()
        out["referral"] = float(rb["v"] if rb else 1.1)

    # Title boost (best single title multiplier)
    tb = s.run("""
      MATCH (u:User {id:$uid})-[:HAS_TITLE]->(t:Title)
      RETURN max(coalesce(t.xp_boost, 1.0)) AS v
    """, uid=uid).single()
    if tb and tb["v"]:
        out["title"] = float(tb["v"])

    return out

def _apply_multipliers(base_xp: int, base_eco: int, mults: Dict[str, float]) -> tuple[int, int]:
    m = 1.0
    for v in mults.values():
        try:
            m *= float(v)
        except Exception:
            continue
    # XP fully multiplicative; cap ECO a bit to avoid runaway inflation
    return int(round(base_xp * m)), int(round(base_eco * max(1.0, min(m, 3.0))))
def get_user_badges_and_awards(s: Session, *, uid: str) -> Dict[str, Any]:
    if _user_banned(s, uid):
        return {
            "badges": [],
            "awards": [],
            "stats": {
                "total_eco": 0, "total_xp": 0, "actions_total": 0, "season_actions": 0,
                "streak_days": 0, "level": 1, "next_level_xp": 100, "xp_to_next": 100,
                "progress_pct": 0, "next_badge_hint": None, "prestige_level": 0,
                "active_multipliers": {}, "anomaly_flag": "banned",
                # explicit ECO model
                "eco_balance": 0, "eco_earned_total": 0, "eco_spent_total": 0,
                "eco_retired_total": 0, "last_tx_at": None,
            }
        }


    badges_rec = s.run("""
      MATCH (u:User {id:$uid})-[:EARNED_BADGE]->(ba:BadgeAward)-[:OF]->(bt:BadgeType)
      OPTIONAL MATCH (ba)-[:IN_SEASON]->(ss:Season)
      RETURN collect({
        id: ba.id, at: toString(ba.at), tier: ba.tier, badge_id: bt.id, season: ss.id
      }) AS badges
    """, uid=uid).single() or {"badges": []}

    awards_rec = s.run("""
      MATCH (u:User {id:$uid})-[:WON]->(aw:Award)-[:OF]->(at:AwardType)
      OPTIONAL MATCH (aw)-[:IN_SEASON]->(ss:Season)
      RETURN collect({
        id: aw.id, at: toString(aw.at), rank: aw.rank, period: aw.period, award_type_id: at.id, season: ss.id
      }) AS awards
    """, uid=uid).single() or {"awards": []}

    base = s.run("""
     MATCH (u:User {id:$uid})
OPTIONAL MATCH (u)-[:EARNED]->(t:EcoTx)
WITH u,
  toInteger(sum(coalesce(t.eco,0))) AS total_eco_ledger,
  toInteger(sum(coalesce(t.xp,0)))  AS total_xp_ledger

// Virtual gains from approved submissions lacking a ledger tx
OPTIONAL MATCH (u)-[:SUBMITTED]->(sub:Submission {state:'approved'})-[:FOR]->(sq:Sidequest)
WHERE NOT (sub)<-[:PROOF]-(:EcoTx)
WITH u, total_eco_ledger, total_xp_ledger,
  toInteger(sum(coalesce(sq.reward_eco,0))) AS eco_virtual,
  toInteger(sum(coalesce(sq.xp_reward,0)))  AS xp_virtual

WITH u,
  toInteger(coalesce(total_eco_ledger,0) + coalesce(eco_virtual,0)) AS total_eco,
  toInteger(coalesce(total_xp_ledger,0)  + coalesce(xp_virtual,0))  AS total_xp

// keep actions_total as before
OPTIONAL MATCH (u)-[:SUBMITTED]->(s1:Submission {state:'approved'})
WITH u, total_eco, total_xp, count(s1) AS actions_total
RETURN toInteger(coalesce(u.prestige,0)) AS prestige, total_eco, total_xp, actions_total

    """, uid=uid).single()

    prestige = int(base.get("prestige") or 0) if base else 0
    total_eco = int(base.get("total_eco") or 0) if base else 0
    total_xp = int(base.get("total_xp") or 0) if base else 0
    actions_total = int(base.get("actions_total") or 0) if base else 0

    season = _active_season(s)
    season_actions = _season_actions(s, uid, season) if season else 0

    # streak (fixed)
    streak_rec = s.run("""
      MATCH (u:User {id:$uid})
      OPTIONAL MATCH (u)-[:EARNED]->(t:EcoTx)
      WITH u, collect(date(t.at)) AS d1
      OPTIONAL MATCH (u)-[:SUBMITTED]->(s1:Submission {state:'approved'})
      WITH u, d1, s1
      WITH u, d1,
           collect(DISTINCT date(datetime(
             CASE WHEN (s1.reviewed_at) IS NOT NULL AND s1.reviewed_at IS NOT NULL
                  THEN s1.reviewed_at ELSE s1.created_at END
           ))) AS d2
      WITH [d IN (d1 + d2) WHERE d IS NOT NULL AND d >= date() - duration('P30D')] AS recent
      RETURN size(apoc.coll.toSet(recent)) AS active_days
    """, uid=uid).single()
    streak_days = int((streak_rec and streak_rec.get("active_days")) or 0)

    stats = {
        "total_eco": total_eco,
        "total_xp": total_xp,
        "actions_total": actions_total,
        "season_actions": season_actions,
        "streak_days": streak_days,
    }

    lvl, next_level_xp, xp_to_next = _level_for_xp(total_xp, prestige)
    stats["level"] = lvl
    stats["next_level_xp"] = next_level_xp
    stats["xp_to_next"] = xp_to_next

    pct, hint = _nearest_badge_progress(s, stats)
    stats["progress_pct"] = pct
    stats["next_badge_hint"] = hint
    stats["prestige_level"] = prestige

    mults = _collect_multipliers(s, uid)
    stats["active_multipliers"] = mults

    # anomaly: XP spike
    spike = s.run("""
      MATCH (:User {id:$uid})-[:EARNED]->(t:EcoTx)
      WHERE date(t.at) = date()
      RETURN toInteger(sum(coalesce(t.xp,0))) AS dxp
    """, uid=uid).single()
    if int((spike and spike.get("dxp")) or 0) > 10000:
        stats["anomaly_flag"] = "xp_spike"

    # NEW explicit ECO fields
    ledger = _eco_ledger_for_user(s, uid)
    stats["eco_balance"]       = ledger["eco_balance"]
    stats["eco_earned_total"]  = ledger["eco_earned_total"]
    stats["eco_spent_total"]   = ledger["eco_spent_total"]  # 0 until EYBA burn integrated here
    stats["eco_retired_total"] = 0                          # business metric; not per-user here
    stats["last_tx_at"]        = ledger["last_tx_at"]

    return {
        "badges": badges_rec.get("badges", []),
        "awards": awards_rec.get("awards", []),
        "stats": stats
    }

def _eco_ledger_for_user(s: Session, uid: str) -> dict:
    """
    Returns:
      {
        eco_earned_total: int,
        eco_spent_total:  int,         # 0 for now (spend lives in EYBA wallet)
        eco_balance:      int,         # earned - spent
        last_tx_at:       str | None,  # ISO
      }
    """
    rec = s.run("""
      MATCH (:User {id:$uid})-[:EARNED]->(t:EcoTx)
      WITH sum(coalesce(t.eco,0)) AS earned, max(t.at) AS last_at
      RETURN toInteger(coalesce(earned,0)) AS earned, 
             CASE WHEN last_at IS NULL THEN NULL ELSE toString(last_at) END AS last_at
    """, uid=uid).single()

    earned = int((rec and rec.get("earned")) or 0)
    # NOTE: Redemptions / burns are tracked in the EYBA wallet, not EcoTx here.
    spent = 0
    return {
        "eco_earned_total": earned,
        "eco_spent_total": spent,
        "eco_balance": earned - spent,
        "last_tx_at": (rec and rec.get("last_at")) or None,
    }


def get_business_awards(s: Session, *, bid: str) -> Dict[str, Any]:
    """
    Public awards listing for a business profile.
    Matches Award <-[:OF]- AwardType scoped to 'business' and linked to BusinessProfile.
    """
    rows = s.run("""
      MATCH (b:BusinessProfile {id:$bid})
      OPTIONAL MATCH (b)<-[:FOR]-(aw:Award)-[:OF]->(at:AwardType)
      WHERE coalesce(at.scope,'business') IN ['business','global']
      OPTIONAL MATCH (aw)-[:IN_SEASON]->(ss:Season)
      RETURN collect({
        id: aw.id, at: toString(aw.at), rank: aw.rank,
        period: aw.period, award_type_id: at.id, season: ss.id
      }) AS awards
    """, bid=bid).single()
    return {"awards": (rows and rows.get("awards")) or []}


def list_badge_types(s: Session) -> List[Dict]:
    recs = s.run("MATCH (t:BadgeType) RETURN t ORDER BY toLower(t.name) ASC")
    out: List[Dict] = []
    for r in recs:
        t = dict(r["t"])
        # Try to expose "rule" like before, reconstructed from JSON or flats
        rule = None
        if t.get("rule_json"):
            try:
                rule = json.loads(t["rule_json"])
            except Exception:
                rule = None
        if not rule:
            # fallback from flattened fields
            if t.get("rule_type") == "threshold":
                rule = {"type": "threshold", "field": t.get("rule_field"), "gte": t.get("rule_gte")}
            elif t.get("rule_type") == "title":
                rule = {"type": "title", "title_id": t.get("rule_title_id")}

        out.append({
            "id": t.get("id"), "name": t.get("name"),
            "icon": t.get("icon"), "color": t.get("color"),
            "kind": t.get("kind"), "rule": rule,
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
            "theme": x.get("theme"), "xp_boost": x.get("xp_boost", 1.0),
        })
    return out

def upsert_badge_type(s: Session, payload: Dict) -> Dict:
    # Accept payload["rule"] as dict or None
    rule = payload.get("rule") or {}
    rule_type = rule.get("type")
    rule_field = rule.get("field")
    rule_gte = rule.get("gte")
    rule_title_id = rule.get("title_id")

    # JSON encode for round-trip fidelity
    rule_json = json.dumps(rule) if rule else None

    params = {
        **payload,
        "rule_json": rule_json,
        "rule_type": rule_type,
        "rule_field": rule_field,
        "rule_gte": int(rule_gte) if rule_type == "threshold" and rule_gte is not None else None,
        "rule_title_id": rule_title_id if rule_type == "title" else None,
    }

    rec = s.run("""
      MERGE (t:BadgeType {id:$id})
      SET t.name=$name, t.icon=$icon, t.color=$color,
          t.kind=$kind, t.tier=$tier, t.max_tier=$max_tier,
          t.rule_json=$rule_json,
          t.rule_type=$rule_type,
          t.rule_field=$rule_field,
          t.rule_gte=$rule_gte,
          t.rule_title_id=$rule_title_id
      RETURN t
    """, **params).single()
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
      SET ss.label=$label, ss.start=datetime($start), ss.end=datetime($end),
          ss.theme=$theme, ss.xp_boost=coalesce($xp_boost,1.0)
      RETURN ss
    """, **payload).single()
    return dict(rec["ss"])

def delete_season(s: Session, *, id: str) -> None:
    s.run("MATCH (ss:Season {id:$id}) DETACH DELETE ss", id=id)

# ───────────────────────────────────────────────────────────────────────────────
# TUNING (Multipliers)
# ───────────────────────────────────────────────────────────────────────────────
def list_multiplier_configs(s: Session) -> List[Dict]:
    recs = s.run("MATCH (m:MultiplierConfig) RETURN m ORDER BY toLower(m.label) ASC")
    return [dict(r["m"]) for r in recs]

def upsert_multiplier_config(s: Session, payload: Dict) -> Dict:
    rec = s.run("""
      MERGE (m:MultiplierConfig {id:$id})
      SET m.label=$label, m.value=$value, m.max_stack=$max_stack, m.conditions=$conditions
      RETURN m
    """, **payload).single()
    return dict(rec["m"])

def delete_multiplier_config(s: Session, *, id: str) -> None:
    s.run("MATCH (m:MultiplierConfig {id:$id}) DETACH DELETE m", id=id)

# ───────────────────────────────────────────────────────────────────────────────
# QUESTS (catalog)
# ───────────────────────────────────────────────────────────────────────────────
def list_quest_types(s: Session) -> List[Dict]:
    recs = s.run("MATCH (q:QuestType) RETURN q ORDER BY toLower(q.label)")
    return [dict(r["q"]) for r in recs]

def upsert_quest_type(s: Session, payload: Dict) -> Dict:
    xr = payload.get("extra_rules") or {}
    rule_json = json.dumps(xr) if xr else None
    cap_xp = xr.get("cap_xp_per_claim")
    cap_eco = xr.get("cap_eco_per_claim")

    params = {
        **payload,
        "extra_rules_json": rule_json,
        "cap_xp_per_claim": int(cap_xp) if cap_xp is not None else None,
        "cap_eco_per_claim": int(cap_eco) if cap_eco is not None else None,
    }

    rec = s.run("""
      MERGE (q:QuestType {id:$id})
      SET q.label=$label, q.cadence=$cadence, q.base_xp=$base_xp, q.base_eco=$base_eco,
          q.limit_per_window=$limit_per_window, q.icon=$icon, q.color=$color,
          q.extra_rules_json=$extra_rules_json,
          q.cap_xp_per_claim=$cap_xp_per_claim,
          q.cap_eco_per_claim=$cap_eco_per_claim
      RETURN q
    """, **params).single()
    return dict(rec["q"])

def delete_quest_type(s: Session, *, id: str) -> None:
    s.run("MATCH (q:QuestType {id:$id}) DETACH DELETE q", id=id)

def _get_user_stats_for_rules(s: Session, *, uid: str, season_id: Optional[str]) -> Dict:
    base = s.run("""
      MATCH (u:User {id:$uid})
OPTIONAL MATCH (u)-[:EARNED]->(t:EcoTx)
WITH u,
  toInteger(sum(coalesce(t.eco,0))) AS total_eco_ledger,
  toInteger(sum(coalesce(t.xp,0)))  AS total_xp_ledger

OPTIONAL MATCH (u)-[:SUBMITTED]->(sub:Submission {state:'approved'})-[:FOR]->(sq:Sidequest)
WHERE NOT (sub)<-[:PROOF]-(:EcoTx)
WITH u, total_eco_ledger, total_xp_ledger,
  toInteger(sum(coalesce(sq.reward_eco,0))) AS eco_virtual,
  toInteger(sum(coalesce(sq.xp_reward,0)))  AS xp_virtual

WITH u,
  toInteger(coalesce(total_eco_ledger,0) + coalesce(eco_virtual,0)) AS total_eco,
  toInteger(coalesce(total_xp_ledger,0)  + coalesce(xp_virtual,0))  AS total_xp

OPTIONAL MATCH (u)-[:SUBMITTED]->(s1:Submission {state:'approved'})
WITH total_eco, total_xp, count(s1) AS actions_total
RETURN total_eco, total_xp, actions_total

    """, uid=uid).single()

    stats = {
        "total_eco": int((base and base.get("total_eco")) or 0),
        "total_xp": int((base and base.get("total_xp")) or 0),
        "actions_total": int((base and base.get("actions_total")) or 0),
        "season_actions": 0,
        "streak_days": 0,
    }

    if season_id:
        rec = s.run("""
          MATCH (ss:Season {id:$sid})
          WITH ss
          MATCH (:User {id:$uid})-[:SUBMITTED]->(s1:Submission {state:'approved'})
          WHERE datetime(coalesce(s1.reviewed_at, s1.created_at)) >= ss.start
            AND datetime(coalesce(s1.reviewed_at, s1.created_at)) <  ss.end
          RETURN toInteger(count(s1)) AS c
        """, sid=season_id, uid=uid).single()
        stats["season_actions"] = int((rec and rec.get("c")) or 0)

    # -------- FIXED STREAK QUERY HERE TOO --------
    streak_rec = s.run("""
      MATCH (u:User {id:$uid})
      OPTIONAL MATCH (u)-[:EARNED]->(t:EcoTx)
      WITH u, collect(date(t.at)) AS d1
      OPTIONAL MATCH (u)-[:SUBMITTED]->(s1:Submission {state:'approved'})
      WITH u, d1, s1
      WITH u, d1,
           collect(DISTINCT date(datetime(
             CASE WHEN (s1.reviewed_at) IS NOT NULL AND s1.reviewed_at IS NOT NULL
                  THEN s1.reviewed_at
                  ELSE s1.created_at
             END
           ))) AS d2
      WITH [d IN (d1 + d2) WHERE d IS NOT NULL AND d >= date() - duration('P30D')] AS recent
      RETURN size(apoc.coll.toSet(recent)) AS active_days
    """, uid=uid).single()
    stats["streak_days"] = int((streak_rec and streak_rec.get("active_days")) or 0)

    return stats

def _should_grant(rule: Dict, stats: Dict, bt: Optional[Dict] = None) -> bool:
    """
    rule: dict from legacy 'rule' OR None
    bt: full BadgeType dict, used to fall back to flattened fields if rule is None
    """
    # Prefer explicit rule dict if present
    if rule and rule.get("type") == "threshold":
        field = rule.get("field")
        try:
            return stats.get(field, 0) >= int(rule.get("gte"))
        except Exception:
            return False
    if rule and rule.get("type") == "title":
        return True

    # Fall back to flattened properties on bt
    if bt:
        rt = bt.get("rule_type")
        if rt == "threshold":
            field = bt.get("rule_field")
            gte = bt.get("rule_gte")
            if field is None or gte is None:
                return False
            return stats.get(field, 0) >= int(gte)
        if rt == "title":
            return True

    return False

def evaluate_badges_for_user(s: Session, *, uid: str, season_id: Optional[str]) -> Dict:
    stats = _get_user_stats_for_rules(s, uid=uid, season_id=season_id)
    types = s.run("MATCH (t:BadgeType) RETURN t").value("t")

    granted: List[str] = []
    for t in types:
        bt = dict(t)
        if not _should_grant(bt.get("rule"), stats, bt):
            continue
        already = s.run("""
          MATCH (:User {id:$uid})-[:EARNED_BADGE]->(:BadgeAward)-[:OF]->(t:BadgeType {id:$bid})
          RETURN count(*) AS c
        """, uid=uid, bid=bt["id"]).single()["c"]
        if already and int(already) > 0:
            continue
        rec = s.run("""
          MATCH (u:User {id:$uid}), (t:BadgeType {id:$bid})
          CREATE (ba:BadgeAward {id: randomUUID(), at: datetime(), tier: coalesce($tier, null)})
          MERGE (u)-[:EARNED_BADGE]->(ba)
          MERGE (ba)-[:OF]->(t)
          WITH ba
          OPTIONAL MATCH (ss:Season)
            WHERE ss.start <= datetime() AND ss.end > datetime()
          FOREACH (_ IN CASE WHEN ss IS NULL THEN [] ELSE [1] END | MERGE (ba)-[:IN_SEASON]->(ss))
          RETURN ba.id AS id
        """, uid=uid, bid=bt["id"], tier=bt.get("tier")).single()
        granted.append(rec["id"])
        # Title unlocks if rule.type == "title"
        rule = bt.get("rule") or {}
        if rule.get("type") == "title" and rule.get("title_id"):
            s.run("""
              MATCH (u:User {id:$uid})
              MERGE (t:Title {id:$tid})
              ON CREATE SET t.label=$tid, t.xp_boost=1.05
              MERGE (u)-[:HAS_TITLE]->(t)
            """, uid=uid, tid=rule["title_id"])
    return {"granted": granted, "stats": stats}

# ───────────────────────────────────────────────────────────────────────────────
# LEADERBOARDS (with cohorts & pagination)
# ───────────────────────────────────────────────────────────────────────────────
def _compute_leader_rows(s: Session, *, period: str, scope: str,
                         start: Optional[str], end: Optional[str],
                         cohort_school_id: Optional[str], cohort_team_id: Optional[str], cohort_region: Optional[str],
                         limit: int) -> List[Dict]:
    params = {"period": period, "start": start, "end": end}
    cohort_match = ""
    if scope == "youth":
        if cohort_school_id:
            cohort_match += "MATCH (u)-[:ENROLLED_AT]->(:School {id:$school})\n"
            params["school"] = cohort_school_id
        if cohort_team_id:
            cohort_match += "MATCH (u)-[:MEMBER_OF]->(:Team {id:$team})\n"
            params["team"] = cohort_team_id
        if cohort_region:
            cohort_match += "MATCH (u)-[:LOCATED_IN]->(:Region {id:$region})\n"
            params["region"] = cohort_region

        q = f"""
          MATCH (u:User)
          {cohort_match}
          OPTIONAL MATCH (u)-[:EARNED]->(t:EcoTx)
            WHERE $period = 'total' OR (t.at >= datetime($start) AND t.at < datetime($end))
          WITH u, toInteger(sum(coalesce(t.eco,0))) AS eco
          RETURN u.id AS id, eco
          ORDER BY eco DESC, id ASC
          LIMIT $limit
        """
    else:
        if cohort_region:
            cohort_match += "MATCH (b:BusinessProfile)-[:LOCATED_IN]->(:Region {id:$region})\n"
            params["region"] = cohort_region
        q = f"""
          MATCH (b:BusinessProfile)
          {cohort_match}
          OPTIONAL MATCH (t:EcoTx)-[:FROM]->(b)
            WHERE $period = 'total' OR (t.at >= datetime($start) AND t.at < datetime($end))
          WITH b, toInteger(sum(coalesce(t.eco,0))) AS eco
          RETURN b.id AS id, eco
          ORDER BY eco DESC, id ASC
          LIMIT $limit
        """
    params["limit"] = limit
    return s.run(q, **params).data()

def get_leaderboard(
    s: Session, *,
    period: str,
    scope: str,
    start: Optional[str],
    end: Optional[str],
    page: int = 1,
    page_size: int = 50,
    cohort_school_id: Optional[str] = None,
    cohort_team_id: Optional[str] = None,
    cohort_region: Optional[str] = None,
    include_me: bool = False,                 # NEW
    uid: Optional[str] = None,                # NEW (required when include_me=True for youth scope)
) -> Dict:
    """
    Returns a leaderboard page for users (scope='youth') or businesses (scope='business').
    Supports cohorts, pagination, and optional 'me' rank computation for youth scope.
    Ties are handled with stable competition ranking (1, 2, 2, 4...).

    Response:
      {
        "period": str, "scope": str, "start": str|None, "end": str|None,
        "rows": [{"id": str, "eco": int, "rank": int}, ...],
        "page": int, "page_size": int,
        "me": {"id": str, "eco": int, "rank": int} | None
      }
    """
    # Default windows for weekly / monthly
    if period == "weekly" and not (start and end):
        res = s.run(
            "RETURN toString(datetime() - duration('P7D')) AS start, "
            "toString(datetime()) AS end"
        ).single()
        start, end = res["start"], res["end"]
    elif period == "monthly" and not (start and end):
        res = s.run("""
        WITH datetime.truncate('month', datetime()) AS ms
        RETURN toString(ms - duration('P1M')) AS start,
                toString(ms)                   AS end
        """).single()
        start, end = res["start"], res["end"]
    # For "total", start/end may remain None and the Cypher uses the OR guard.

    fetch = min(page_size * page, 500)
    rows = _compute_leader_rows(
        s,
        period=period,
        scope=scope,
        start=start,
        end=end,
        cohort_school_id=cohort_school_id,
        cohort_team_id=cohort_team_id,
        cohort_region=cohort_region,
        limit=fetch,
    )

    # Stable competition ranking with ties
    out: List[Dict] = []
    last_eco: Optional[int] = None
    rank = 0
    for idx, r in enumerate(rows, start=1):
        e = int(r["eco"] or 0)
        if last_eco is None or e < last_eco:
            rank = idx
        last_eco = e
        out.append({"id": r["id"], "eco": e, "rank": rank})

    # Slice for requested page
    start_i = max(0, (page - 1) * page_size)
    end_i = start_i + page_size
    page_rows = out[start_i:end_i]

    result: Dict = {
        "period": period,
        "scope": scope,
        "start": start,
        "end": end,
        "rows": page_rows,
        "page": page,
        "page_size": page_size,
    }

    # Optional "me" computation (youth scope only)
    if include_me and uid and scope == "youth":
        params: Dict = {"period": period, "start": start, "end": end, "uid": uid}

        cohort_match = ""
        if cohort_school_id:
            cohort_match += "MATCH (u)-[:ENROLLED_AT]->(:School {id:$school})\n"
            params["school"] = cohort_school_id
        if cohort_team_id:
            cohort_match += "MATCH (u)-[:MEMBER_OF]->(:Team {id:$team})\n"
            params["team"] = cohort_team_id
        if cohort_region:
            cohort_match += "MATCH (u)-[:LOCATED_IN]->(:Region {id:$region})\n"
            params["region"] = cohort_region

        # 1) My ECO in the same window + cohorts
        my_row = s.run(
            f"""
            MATCH (u:User {{id:$uid}})
            {cohort_match}
            OPTIONAL MATCH (u)-[:EARNED]->(t:EcoTx)
              WHERE $period = 'total' OR (t.at >= datetime($start) AND t.at < datetime($end))
            RETURN toInteger(sum(coalesce(t.eco,0))) AS eco
            """,
            **params,
        ).single()
        my_eco = int((my_row and my_row["eco"]) or 0)

        # 2) Count strictly higher ECO to get competition rank (ties share rank)
        higher_row = s.run(
            f"""
            MATCH (u:User)
            {cohort_match}
            OPTIONAL MATCH (u)-[:EARNED]->(t:EcoTx)
              WHERE $period = 'total' OR (t.at >= datetime($start) AND t.at < datetime($end))
            WITH toInteger(sum(coalesce(t.eco,0))) AS eco
            WHERE eco > $my_eco
            RETURN toInteger(count(*)) AS higher
            """,
            **{**params, "my_eco": my_eco},
        ).single()
        higher = int((higher_row and higher_row["higher"]) or 0)
        my_rank = higher + 1

        result["me"] = {"id": uid, "eco": my_eco, "rank": my_rank}

    return result

# ───────────────────────────────────────────────────────────────────────────────
# PROGRESS PREVIEW + CLAIMS + PRESTIGE + REFERRALS + STREAK FREEZE
# ───────────────────────────────────────────────────────────────────────────────
def _recommended_title(s: Session, uid: str) -> Optional[str]:
    stats = get_user_badges_and_awards(s, uid=uid)["stats"]
    pct, hint = _nearest_badge_progress(s, stats)

    # Pull all types once, filter in Python
    types = [dict(t) for t in s.run("MATCH (t:BadgeType) RETURN t").value("t")]

    def as_rule(bt: Dict) -> Dict:
        return _extract_rule(bt)

    if hint:
        for bt in types:
            if (bt.get("name") or "").lower() == hint.lower():
                r = as_rule(bt)
                tid = r.get("title_id") if r.get("type") == "title" else None
                if tid:
                    return tid

    # Otherwise suggest easiest title rule (lowest tier first)
    title_types = [bt for bt in types if as_rule(bt).get("type") == "title" and as_rule(bt).get("title_id")]
    if not title_types:
        return None
    title_types.sort(key=lambda bt: (bt.get("tier") or 0))
    return as_rule(title_types[0]).get("title_id")

def get_progress_preview(s: Session, *, uid: str) -> Dict:
    if _user_banned(s, uid):
        return {
            "level": 1, "xp_to_next": 100, "next_badge_hint": None,
            "daily_available": False, "weekly_available": False,
            "monthly_available": False, "recommended_title": None
        }

    stats = get_user_badges_and_awards(s, uid=uid)["stats"]

    # availability checks via idempotency edges this-window
    daily_ok = s.run("""
      MATCH (u:User {id:$uid})
      OPTIONAL MATCH (u)-[:CLAIMED]->(c:QuestClaim {cadence:'daily'})
        WHERE date(c.at)=date()
      RETURN count(c)=0 AS ok
    """, uid=uid).single()["ok"]

    weekly_ok = s.run("""
      WITH datetime().week AS w, datetime().year AS y
      MATCH (u:User {id:$uid})
      OPTIONAL MATCH (u)-[:CLAIMED]->(c:QuestClaim {cadence:'weekly'})
      WHERE c.window_week = w AND c.window_year = y
      RETURN count(c)=0 AS ok
    """, uid=uid).single()["ok"]

    monthly_ok = s.run("""
      WITH datetime().year AS y, datetime().month AS m
      MATCH (u:User {id:$uid})
      OPTIONAL MATCH (u)-[:CLAIMED]->(c:QuestClaim {cadence:'monthly'})
      WHERE c.window_year = y AND c.window_month = m
      RETURN count(c)=0 AS ok
    """, uid=uid).single()["ok"]

    _, _, xp_to_next = _level_for_xp(stats["total_xp"], stats.get("prestige_level", 0))
    return {
        "level": stats["level"],
        "xp_to_next": xp_to_next,
        "next_badge_hint": stats.get("next_badge_hint"),
        "daily_available": bool(daily_ok),
        "weekly_available": bool(weekly_ok),
        "monthly_available": bool(monthly_ok),
        "recommended_title": _recommended_title(s, uid),
    }

def _safe_positive(n: Optional[int]) -> int:
    try:
        n = int(n or 0)
        return max(0, n)
    except Exception:
        return 0

def _write_ecotx_and_claim(s: Session, *, uid: str, qtype: Dict, amount: int,
                           mults: Dict[str, float], meta: Optional[Dict],
                           wstart: Optional[str], wend: Optional[str]) -> Dict:
    base_xp = _safe_positive(qtype.get("base_xp"))
    base_eco = _safe_positive(qtype.get("base_eco"))
    per_xp, per_eco = _apply_multipliers(base_xp, base_eco, mults)

    total_xp = per_xp * amount
    total_eco = per_eco * amount

    # Prefer flat properties, fallback to JSON if present
    cap_xp = qtype.get("cap_xp_per_claim")
    cap_eco = qtype.get("cap_eco_per_claim")

    if cap_xp is None or cap_eco is None:
        xr = qtype.get("extra_rules") or {}
        if not xr and qtype.get("extra_rules_json"):
            try:
                xr = json.loads(qtype["extra_rules_json"])
            except Exception:
                xr = {}
        cap_xp = cap_xp if cap_xp is not None else xr.get("cap_xp_per_claim")
        cap_eco = cap_eco if cap_eco is not None else xr.get("cap_eco_per_claim")

    if cap_xp is not None:
        total_xp = min(total_xp, int(cap_xp))
    if cap_eco is not None:
        total_eco = min(total_eco, int(cap_eco))

    # Final write + compute balance_after (sum of ECO after insert)
    res = s.run("""
      MATCH (u:User {id:$uid}), (q:QuestType {id:$qid})
      CREATE (tx:EcoTx {
        id: randomUUID(),
        at: datetime(),
        xp: toInteger($xp),
        eco: toInteger($eco),
        kind: 'quest',
        quest_type_id: $qid,
        metadata: $meta
      })
      MERGE (u)-[:EARNED]->(tx)
      CREATE (c:QuestClaim {
        id: randomUUID(),
        at: datetime(),
        cadence: q.cadence,
        quest_type_id: q.id,
        amount: toInteger($amount),
        window_start: $wstart,
        window_end: $wend,
        window_year: datetime().year,
        window_month: datetime().month,
        window_week: datetime().week
      })
      MERGE (u)-[:CLAIMED]->(c)
      MERGE (c)-[:OF]->(q)
      OPTIONAL MATCH (ss:Season)
        WHERE ss.start <= datetime() AND ss.end > datetime()
      FOREACH (_ IN CASE WHEN ss IS NULL THEN [] ELSE [1] END | MERGE (c)-[:IN_SEASON]->(ss))
      // New: compute balance_after compatibly with frontend ClaimResponse
      WITH tx, c, u
      OPTIONAL MATCH (u)-[:EARNED]->(t2:EcoTx)
      RETURN tx.id AS txid, c.id AS cid, toInteger(sum(coalesce(t2.eco,0))) AS balance_after
    """, uid=uid, qid=qtype["id"], xp=total_xp, eco=total_eco, amount=amount, meta=meta,
       wstart=wstart, wend=wend).single()

    return {
        "tx_id": res["txid"],
        "claim_id": res["cid"],
        "balance_after": int(res.get("balance_after") or 0),
        "awarded": {"xp": total_xp, "eco": total_eco, "per_xp": per_xp, "per_eco": per_eco}
    }

def _quest_type_by_id(s: Session, qid: str) -> Optional[Dict]:
    rec = s.run("MATCH (q:QuestType {id:$id}) RETURN q", id=qid).single()
    return dict(rec["q"]) if rec else None

def _claims_used_in_window(s: Session, uid: str, qid: str, start: str, end: str) -> int:
    rec = s.run("""
      MATCH (u:User {id:$uid})-[:CLAIMED]->(c:QuestClaim)-[:OF]->(q:QuestType {id:$qid})
      WHERE datetime(c.at) >= datetime($start) AND datetime(c.at) < datetime($end)
      RETURN toInteger(sum(coalesce(c.amount,1))) AS used
    """, uid=uid, qid=qid, start=start, end=end).single()
    return int(rec["used"] or 0)

def _user_anomaly_log(s: Session, uid: str, code: str, details: Dict) -> None:
    s.run("""
      MATCH (u:User {id:$uid})
      CREATE (a:Anomaly {
        id: randomUUID(), code:$code, at: datetime(), details:$details
      })
      MERGE (u)-[:FLAGGED]->(a)
    """, uid=uid, code=code, details=details)
def claim_quest(s: Session, *, uid: str, quest_type_id: str, amount: int = 1, metadata: Optional[Dict] = None) -> Dict:
    if _user_banned(s, uid):
        raise ValueError("user_banned")

    qtype = _quest_type_by_id(s, quest_type_id)
    if not qtype:
        raise ValueError("unknown_quest")

    cadence = (qtype.get("cadence") or "daily").lower()
    limit_per_window = _safe_positive(qtype.get("limit_per_window") or 1)
    amount = _clamp(int(amount or 1), 1, 1000)

    wstart, wend = _window_bounds(s, cadence)
    if not (wstart and wend):
        raise ValueError("no_active_window")

    used = _claims_used_in_window(s, uid, quest_type_id, wstart, wend)
    if used >= limit_per_window:
        raise ValueError("limit_reached")

    remaining = max(0, limit_per_window - used)
    take = min(remaining, amount)

    # Anti-spam: tiny cooldown (30s)
    recent = s.run("""
      MATCH (u:User {id:$uid})-[:CLAIMED]->(c:QuestClaim {quest_type_id:$qid})
      WHERE c.at >= datetime() - duration('PT30S')
      RETURN count(c) AS c
    """, uid=uid, qid=quest_type_id).single()
    if int(recent["c"] or 0) > 3:
        _user_anomaly_log(s, uid, "spam_claims", {"quest_type_id": quest_type_id})
        raise ValueError("cooldown")

    mults = _collect_multipliers(s, uid)
    result = _write_ecotx_and_claim(
        s, uid=uid, qtype=qtype, amount=take, mults=mults, meta=metadata, wstart=wstart, wend=wend
    )

    # Evaluate badges post-claim
    season = _active_season(s)
    _ = evaluate_badges_for_user(s, uid=uid, season_id=season["id"] if season else None)

    # Mark activity day
    s.run("""
      MATCH (u:User {id:$uid})
      MERGE (d:ActivityDay {id: toString(date())})
      MERGE (u)-[:ACTIVE_ON]->(d)
    """, uid=uid)

    # Post-claim stats + balance_after
    stats_now = get_user_badges_and_awards(s, uid=uid)["stats"]
    balance_after = int(stats_now.get("eco_balance") or 0)

    return {
        "claim_id": result["claim_id"],
        "tx_id": result["tx_id"],
        "awarded": result["awarded"],
        "badges_granted": _["granted"] if isinstance(_, dict) else [],
        "stats": stats_now,
        "window": {"start": wstart, "end": wend, "used": used + take, "limit": limit_per_window},
        "balance_after": balance_after,   # <...  NEW optional field (frontend reads if present)
    }

def grant_prestige(s: Session, *, uid: str) -> Dict:
    """
    Soft reset XP to 0, increment prestige by 1. Requires level >= threshold or explicit total_xp threshold.
    Default threshold: level >= 20 or total_xp >= 50_000 (configurable via MultiplierConfig/Settings nodes).
    """
    if _user_banned(s, uid):
        raise ValueError("user_banned")

    # Read totals
    base = s.run("""
      MATCH (u:User {id:$uid})
      OPTIONAL MATCH (u)-[:EARNED]->(t:EcoTx)
      RETURN toInteger(coalesce(u.prestige,0)) AS prestige,
             toInteger(sum(coalesce(t.xp,0))) AS total_xp
    """, uid=uid).single()
    prestige = int(base["prestige"] or 0)
    total_xp = int(base["total_xp"] or 0)
    lvl, _, _ = _level_for_xp(total_xp, prestige)

    # Thresholds (allow Settings override)
    cfg = s.run("""
      OPTIONAL MATCH (c:Settings {id:'prestige'})
      RETURN toInteger(coalesce(c.lvl_threshold, 20)) AS lt,
             toInteger(coalesce(c.xp_threshold, 50000)) AS xt
    """).single()
    need_lvl = int(cfg["lt"])
    need_xp = int(cfg["xt"])

    if lvl < need_lvl and total_xp < need_xp:
        raise ValueError("insufficient_for_prestige")

    # Increase prestige & mark reset by inserting a Prestige node (audit) and XP reset marker
    s.run("""
      MATCH (u:User {id:$uid})
      SET u.prestige = toInteger(coalesce(u.prestige,0)) + 1
      CREATE (p:Prestige {id: randomUUID(), at: datetime(), old_total_xp: toInteger($txp)})
      MERGE (u)-[:PRESTIGED]->(p)
    """, uid=uid, txp=total_xp)

    # Optional: write a "Reset" EcoTx of 0 that documents new prestige context
    s.run("""
      MATCH (u:User {id:$uid})
      CREATE (m:EcoTx {id: randomUUID(), at: datetime(), xp: 0, eco: 0,
                       kind: 'prestige_reset', metadata: { prestige: toInteger(u.prestige) } })
      MERGE (u)-[:EARNED]->(m)
    """, uid=uid)

    # Titles for prestige milestones
    s.run("""
      MATCH (u:User {id:$uid})
      WITH u, toInteger(u.prestige) AS p
      MERGE (t:Title {id: 'Prestige-' + toString(p)})
      ON CREATE SET t.label = 'Prestige ' + toString(p), t.xp_boost = 1.03 + (0.01 * p)
      MERGE (u)-[:HAS_TITLE]->(t)
    """, uid=uid)

    return {"ok": True, "new_prestige": prestige + 1}

def use_streak_freeze(s: Session, *, uid: str) -> Dict:
    """
    Allows user to protect today's streak day once per weekly window.
    Creates a StreakFreeze node (idempotent per week).
    """
    if _user_banned(s, uid):
        raise ValueError("user_banned")

    r = s.run("""
      WITH datetime().week AS w, datetime().year AS y
      MATCH (u:User {id:$uid})
      OPTIONAL MATCH (u)-[:USED]->(f:StreakFreeze)
      WHERE f.window_week = w AND f.window_year = y
      RETURN count(f) AS used
    """, uid=uid).single()
    if int(r["used"] or 0) > 0:
        raise ValueError("already_used")

    res = s.run("""
      WITH datetime().week AS w, datetime().year AS y
      MATCH (u:User {id:$uid})
      CREATE (f:StreakFreeze {id: randomUUID(), at: datetime(), window_week:w, window_year:y})
      MERGE (u)-[:USED]->(f)
      RETURN f.id AS id
    """, uid=uid).single()
    return {"freeze_id": res["id"]}

def link_referral(s: Session, *, referrer_id: str, referee_id: str) -> Dict:
    """
    Creates a RECOMMENDED/REFERRED relationship if not exists, awards both sides once.
    """
    if referrer_id == referee_id:
        raise ValueError("self_referral")

    # Create link if doesn't exist
    s.run("""
      MATCH (a:User {id:$referrer}), (b:User {id:$referee})
      MERGE (a)-[:REFERRED]->(b)
    """, referrer=referrer_id, referee=referee_id)

    # Check if already awarded
    chk = s.run("""
      MATCH (b:User {id:$referee})-[:EARNED]->(t:EcoTx {kind:'referral_bonus'})
      WHERE t.metadata.referrer_id = $referrer
      RETURN count(t) AS c
    """, referrer=referrer_id, referee=referee_id).single()
    if int(chk["c"] or 0) > 0:
        return {"ok": True, "awarded": False}

    # Default amounts (configurable)
    cfg = s.run("""
      OPTIONAL MATCH (m:MultiplierConfig {id:'referral_bonus'})
      RETURN toInteger(coalesce(m.base_xp, 500)) AS xp, toInteger(coalesce(m.base_eco, 250)) AS eco
    """).single()
    xp = int(cfg["xp"])
    eco = int(cfg["eco"])

    # Referee
    s.run("""
      MATCH (b:User {id:$referee})
      CREATE (t:EcoTx {id: randomUUID(), at: datetime(), xp: $xp, eco: $eco,
                       kind:'referral_bonus', metadata:{referrer_id:$referrer, side:'referee'}})
      MERGE (b)-[:EARNED]->(t)
    """, referrer=referrer_id, referee=referee_id, xp=xp, eco=eco)

    # Referrer
    s.run("""
      MATCH (a:User {id:$referrer})
      CREATE (t:EcoTx {id: randomUUID(), at: datetime(), xp: $xp, eco: $eco,
                       kind:'referral_bonus', metadata:{referee_id:$referee, side:'referrer'}})
      MERGE (a)-[:EARNED]->(t)
    """, referrer=referrer_id, referee=referee_id, xp=xp, eco=eco)

    # Mark both as ACTIVE_ON today (helps streaks)
    s.run("""
      MATCH (a:User {id:$referrer}), (b:User {id:$referee})
      MERGE (d:ActivityDay {id: toString(date())})
      MERGE (a)-[:ACTIVE_ON]->(d)
      MERGE (b)-[:ACTIVE_ON]->(d)
    """, referrer=referrer_id, referee=referee_id)

    return {"ok": True, "awarded": True, "amounts": {"xp": xp, "eco": eco}}

def backfill_titles_from_badges(s: Session) -> Dict:
    s.run("""
      MATCH (u:User)-[:EARNED_BADGE]->(:BadgeAward)-[:OF]->(bt:BadgeType)
      WHERE bt.rule_type='title' AND bt.rule_title_id IS NOT NULL
      WITH u, bt
      MERGE (t:Title {id: bt.rule_title_id})
      ON CREATE SET t.label = coalesce(bt.name, bt.id), t.xp_boost = 1.05
      MERGE (u)-[:HAS_TITLE]->(t)
    """)
    return {"ok": True}

def recompute_all_streaks(s: Session) -> Dict:
    """
    Recomputes the 'ACTIVE_ON' days from EcoTx & approved Submissions for last 30 days.
    """
    s.run("""
      MATCH (d:ActivityDay) WHERE d.id >= toString(date() - duration('P30D')) DETACH DELETE d;
    """)
    s.run("""
      // Earned days
      MATCH (u:User)-[:EARNED]->(t:EcoTx)
      WITH u, date(t.at) AS d WHERE d >= date() - duration('P30D')
      MERGE (ad:ActivityDay {id: toString(d)})
      MERGE (u)-[:ACTIVE_ON]->(ad)
    """)
    s.run("""
      // Approved submission days
      MATCH (u:User)-[:SUBMITTED]->(s1:Submission {state:'approved'})
      WITH u, date(datetime(coalesce(s1.reviewed_at, s1.created_at))) AS d
      WHERE d >= date() - duration('P30D')
      MERGE (ad:ActivityDay {id: toString(d)})
      MERGE (u)-[:ACTIVE_ON]->(ad)
    """)
    return {"ok": True}
