# site_backend/api/missions/service.py
from __future__ import annotations
from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime, timedelta
from uuid import uuid4
from math import radians, sin, cos, asin, sqrt

from neo4j import Session

from .schema import (
    MissionCreate, MissionUpdate, MissionOut,
    SubmissionCreate, SubmissionOut, ModerationDecision
)

# -------- utils --------
def _now_iso() -> str:
    return datetime.utcnow().isoformat()

def _haversine_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    R = 6371_000.0
    dlat = radians(lat2 - lat1)
    dlon = radians(lon2 - lon1)
    a = sin(dlat/2)**2 + cos(radians(lat1)) * cos(radians(lat2)) * sin(dlon/2)**2
    return 2 * R * asin(sqrt(a))

def _within_radius(geo: Optional[Dict[str, Any]], u_lat: Optional[float], u_lon: Optional[float]) -> bool:
    if not geo or u_lat is None or u_lon is None:
        return True  # no targeting configured or no client coords
    d = _haversine_m(geo["lat"], geo["lon"], u_lat, u_lon)
    return d <= (geo.get("radius_m") or 0)

def _to_mission_out(d: Dict[str, Any]) -> MissionOut:
    return MissionOut(
        id=d["id"],
        type=d["type"],
        title=d.get("title"),
        subtitle=d.get("subtitle"),
        description_md=d.get("description_md"),
        tags=d.get("tags"),
        reward_eco=d.get("reward_eco", 0),
        max_completions_per_user=d.get("max_completions_per_user"),
        cooldown_days=d.get("cooldown_days"),
        pills=d.get("pills"),
        geo=d.get("geo"),
        verification_methods=d.get("verification_methods") or ["photo_upload"],
        start_at=d.get("start_at"),
        end_at=d.get("end_at"),
        status=d.get("status", "draft"),
        hero_image=d.get("hero_image"),
        card_accent=d.get("card_accent"),
        created_at=d.get("created_at"),
        updated_at=d.get("updated_at"),
    )

def _to_submission_out(d: Dict[str, Any]) -> SubmissionOut:
    return SubmissionOut(
        id=d["id"],
        mission_id=d.get("mission_id", d.get("mid", "")),
        user_id=d.get("user_id", d.get("uid", "")),
        method=d.get("method"),
        state=d.get("state"),
        created_at=d.get("created_at"),
        reviewed_at=d.get("reviewed_at"),
        auto_checks=d.get("auto_checks") or {},
        notes=d.get("notes"),
        media_url=d.get("media_url"),
        instagram_url=d.get("instagram_url"),
    )

# -------- flatten helpers (write-side) --------
def _flatten_from_create(m: MissionCreate) -> Dict[str, Any]:
    pills = m.pills.model_dump() if m.pills else {}
    geo = m.geo.model_dump() if m.geo else {}
    return {
        # core
        "type": m.type,
        "title": m.title,
        "subtitle": m.subtitle,
        "description_md": m.description_md,
        "tags": m.tags,
        "reward_eco": m.reward_eco,
        "max_completions_per_user": m.max_completions_per_user,
        "cooldown_days": m.cooldown_days,
        "verification_methods": m.verification_methods,
        "start_at": m.start_at,
        "end_at": m.end_at,
        "status": m.status,
        "hero_image": m.hero_image,
        "card_accent": m.card_accent,
        # pills (flattened)
        "pills_difficulty": pills.get("difficulty"),
        "pills_impact": pills.get("impact"),
        "pills_time_estimate_min": pills.get("time_estimate_min"),
        "pills_materials": pills.get("materials"),
        "pills_facts": pills.get("facts"),
        # geo (flattened)
        "geo_lat": geo.get("lat"),
        "geo_lon": geo.get("lon"),
        "geo_radius_m": geo.get("radius_m"),
        "geo_locality": geo.get("locality"),
    }

def _flatten_from_update(u: MissionUpdate) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    # core (only provided)
    if u.title is not None: out["title"] = u.title
    if u.subtitle is not None: out["subtitle"] = u.subtitle
    if u.description_md is not None: out["description_md"] = u.description_md
    if u.tags is not None: out["tags"] = u.tags
    if u.reward_eco is not None: out["reward_eco"] = u.reward_eco
    if u.max_completions_per_user is not None: out["max_completions_per_user"] = u.max_completions_per_user
    if u.cooldown_days is not None: out["cooldown_days"] = u.cooldown_days
    if u.verification_methods is not None: out["verification_methods"] = u.verification_methods
    if u.start_at is not None: out["start_at"] = u.start_at
    if u.end_at is not None: out["end_at"] = u.end_at
    if u.status is not None: out["status"] = u.status
    if u.hero_image is not None: out["hero_image"] = u.hero_image
    if u.card_accent is not None: out["card_accent"] = u.card_accent
    # pills
    if u.pills is not None:
        pills = u.pills.model_dump()
        out["pills_difficulty"] = pills.get("difficulty")
        out["pills_impact"] = pills.get("impact")
        out["pills_time_estimate_min"] = pills.get("time_estimate_min")
        out["pills_materials"] = pills.get("materials")
        out["pills_facts"] = pills.get("facts")
    # geo
    if u.geo is not None:
        geo = u.geo.model_dump()
        out["geo_lat"] = geo.get("lat")
        out["geo_lon"] = geo.get("lon")
        out["geo_radius_m"] = geo.get("radius_m")
        out["geo_locality"] = geo.get("locality")
    return out

# -------- missions --------
def create_mission(session: Session, m: MissionCreate) -> MissionOut:
    mid = uuid4().hex
    now = _now_iso()
    flat = _flatten_from_create(m)
    rec = session.run(
        """
        MERGE (mi:Mission {id:$id})
          ON CREATE SET mi.created_at = $now
        SET mi.type                       = $type,
            mi.title                      = $title,
            mi.subtitle                   = $subtitle,
            mi.description_md             = $description_md,
            mi.tags                       = $tags,
            mi.reward_eco                 = $reward_eco,
            mi.max_completions_per_user   = $max_completions_per_user,
            mi.cooldown_days              = $cooldown_days,
            mi.verification_methods       = $verification_methods,
            mi.start_at                   = $start_at,
            mi.end_at                     = $end_at,
            mi.status                     = $status,
            mi.hero_image                 = $hero_image,
            mi.card_accent                = $card_accent,
            mi.updated_at                 = $now,

            // flattened pills
            mi.pills_difficulty           = $pills_difficulty,
            mi.pills_impact               = $pills_impact,
            mi.pills_time_estimate_min    = $pills_time_estimate_min,
            mi.pills_materials            = $pills_materials,
            mi.pills_facts                = $pills_facts,

            // flattened geo
            mi.geo_lat                    = $geo_lat,
            mi.geo_lon                    = $geo_lon,
            mi.geo_radius_m               = $geo_radius_m,
            mi.geo_locality               = $geo_locality
        RETURN mi{
          .*,
          pills: CASE
            WHEN mi.pills_difficulty IS NULL
              AND mi.pills_impact IS NULL
              AND mi.pills_time_estimate_min IS NULL
              AND mi.pills_materials IS NULL
              AND mi.pills_facts IS NULL
            THEN NULL
            ELSE {
              difficulty: mi.pills_difficulty,
              impact: mi.pills_impact,
              time_estimate_min: mi.pills_time_estimate_min,
              materials: mi.pills_materials,
              facts: mi.pills_facts
            }
          END,
          geo: CASE
            WHEN mi.geo_lat IS NULL OR mi.geo_lon IS NULL THEN NULL
            ELSE {
              lat: mi.geo_lat,
              lon: mi.geo_lon,
              radius_m: coalesce(mi.geo_radius_m, 0),
              locality: mi.geo_locality
            }
          END,
          created_at: toString(mi.created_at),
          updated_at: toString(mi.updated_at)
        } AS mi
        """,
        {"id": mid, "now": now, **flat}
    ).single()
    return _to_mission_out(dict(rec["mi"]))

def update_mission(session: Session, mission_id: str, u: MissionUpdate) -> MissionOut:
    now = _now_iso()
    fields = _flatten_from_update(u)
    set_lines = [f"mi.{k} = ${k}" for k in fields.keys()]
    if set_lines:
        set_lines.append("mi.updated_at = $now")
    else:
        set_lines = ["mi.updated_at = $now"]
    q = f"""
    MATCH (mi:Mission {{id:$id}})
    SET {", ".join(set_lines)}
    RETURN mi{{
      .*,
      pills: CASE
        WHEN mi.pills_difficulty IS NULL
          AND mi.pills_impact IS NULL
          AND mi.pills_time_estimate_min IS NULL
          AND mi.pills_materials IS NULL
          AND mi.pills_facts IS NULL
        THEN NULL
        ELSE {{
          difficulty: mi.pills_difficulty,
          impact: mi.pills_impact,
          time_estimate_min: mi.pills_time_estimate_min,
          materials: mi.pills_materials,
          facts: mi.pills_facts
        }}
      END,
      geo: CASE
        WHEN mi.geo_lat IS NULL OR mi.geo_lon IS NULL THEN NULL
        ELSE {{
          lat: mi.geo_lat,
          lon: mi.geo_lon,
          radius_m: coalesce(mi.geo_radius_m, 0),
          locality: mi.geo_locality
        }}
      END,
      created_at: toString(mi.created_at),
      updated_at: toString(mi.updated_at)
    }} AS mi
    """
    params = {"id": mission_id, "now": now, **fields}
    rec = session.run(q, params).single()
    return _to_mission_out(dict(rec["mi"]))

def get_mission(session: Session, mission_id: str) -> MissionOut:
    rec = session.run(
        """
        MATCH (mi:Mission {id:$id})
        RETURN mi{
          .*,
          pills: CASE
            WHEN mi.pills_difficulty IS NULL
              AND mi.pills_impact IS NULL
              AND mi.pills_time_estimate_min IS NULL
              AND mi.pills_materials IS NULL
              AND mi.pills_facts IS NULL
            THEN NULL
            ELSE {
              difficulty: mi.pills_difficulty,
              impact: mi.pills_impact,
              time_estimate_min: mi.pills_time_estimate_min,
              materials: mi.pills_materials,
              facts: mi.pills_facts
            }
          END,
          geo: CASE
            WHEN mi.geo_lat IS NULL OR mi.geo_lon IS NULL THEN NULL
            ELSE {
              lat: mi.geo_lat,
              lon: mi.geo_lon,
              radius_m: coalesce(mi.geo_radius_m, 0),
              locality: mi.geo_locality
            }
          END,
          created_at: toString(mi.created_at),
          updated_at: toString(mi.updated_at)
        } AS mi
        """,
        {"id": mission_id}
    ).single()
    if not rec:
        raise ValueError(f"Mission not found: {mission_id}")
    return _to_mission_out(dict(rec["mi"]))

def list_missions(session: Session, mtype: Optional[str], status: Optional[str],
                  q: Optional[str], limit: int, skip: int) -> List[MissionOut]:
    where = []
    params: Dict[str, Any] = {"limit": limit, "skip": skip}
    if mtype:
        where.append("mi.type = $type"); params["type"] = mtype
    if status:
        where.append("mi.status = $status"); params["status"] = status
    if q:
        where.append("(toLower(mi.title) CONTAINS toLower($q) OR toLower(mi.description_md) CONTAINS toLower($q))")
        params["q"] = q
    where_clause = "WHERE " + " AND ".join(where) if where else ""
    recs = session.run(f"""
        MATCH (mi:Mission)
        {where_clause}
        WITH mi
        ORDER BY coalesce(toString(mi.updated_at), toString(mi.created_at)) DESC
        SKIP $skip LIMIT $limit
        RETURN mi{{
          .*,
          pills: CASE
            WHEN mi.pills_difficulty IS NULL
              AND mi.pills_impact IS NULL
              AND mi.pills_time_estimate_min IS NULL
              AND mi.pills_materials IS NULL
              AND mi.pills_facts IS NULL
            THEN NULL
            ELSE {{
              difficulty: mi.pills_difficulty,
              impact: mi.pills_impact,
              time_estimate_min: mi.pills_time_estimate_min,
              materials: mi.pills_materials,
              facts: mi.pills_facts
            }}
          END,
          geo: CASE
            WHEN mi.geo_lat IS NULL OR mi.geo_lon IS NULL THEN NULL
            ELSE {{
              lat: mi.geo_lat,
              lon: mi.geo_lon,
              radius_m: coalesce(mi.geo_radius_m, 0),
              locality: mi.geo_locality
            }}
          END,
          created_at: toString(mi.created_at),
          updated_at: toString(mi.updated_at)
        }} AS mi
    """, params)
    return [_to_mission_out(dict(r["mi"])) for r in recs]

# -------- submissions & rewards --------
def create_submission(session: Session, user_id: str, s: SubmissionCreate, media_meta: Optional[Dict[str, Any]]) -> SubmissionOut:
    # fetch mission for geo & limits
    m = get_mission(session, s.mission_id)
    auto_checks: Dict[str, bool] = {}

    # geo radius
    auto_checks["within_radius"] = _within_radius(m.geo.model_dump() if m.geo else None, s.user_lat, s.user_lon)

    # duplicate media pHash (per user+mission)
    phash = (media_meta or {}).get("phash")
    if s.method == "photo_upload" and phash:
        rows = session.run("""
            MATCH (:User {id:$uid})-[:SUBMITTED]->(sub:Submission)-[:FOR]->(:Mission {id:$mid})
            WHERE sub.phash IS NOT NULL
            RETURN sub.phash AS phash
        """, {"uid": user_id, "mid": s.mission_id}).value("phash")
        auto_checks["duplicate_media"] = phash in set(rows)
    else:
        auto_checks["duplicate_media"] = False

    # instagram heuristic (no API)
    tag_ok = False
    if s.method == "instagram_link":
        if s.instagram_url:
            u = str(s.instagram_url)
            tag_ok = any(t in u for t in ["#Ecodia", "ecodia", "eco_district", "ecopoints"])
        if s.caption:
            cap = s.caption.lower()
            tag_ok = tag_ok or any(t in cap for t in ["#ecodia", "#ecodistrict", "#eco", "#wattle", "ecopoints"])
    auto_checks["insta_tag_heuristic"] = tag_ok

    sid = uuid4().hex
    now = _now_iso()
    rec = session.run("""
        MATCH (u:User {id:$uid}), (m:Mission {id:$mid})
        MERGE (sub:Submission {id:$sid})
        SET sub.method      = $method,
            sub.state       = 'pending',
            sub.created_at  = $now,
            sub.auto_checks = $auto,
            sub.media_url   = $media_url,
            sub.instagram_url = $insta_url,
            sub.caption     = $caption,
            sub.user_lat    = $ulat,
            sub.user_lon    = $ulon,
            sub.phash       = $phash
        MERGE (u)-[:SUBMITTED]->(sub)
        MERGE (sub)-[:FOR]->(m)
        RETURN sub{.*, uid:u.id, mid:m.id} AS sub
    """, {
        "uid": user_id, "mid": s.mission_id, "sid": sid, "now": now,
        "method": s.method, "auto": auto_checks,
        "media_url": (media_meta or {}).get("path"),
        "phash": phash,
        "insta_url": s.instagram_url, "caption": s.caption,
        "ulat": s.user_lat, "ulon": s.user_lon,
    }).single()

    return _to_submission_out(dict(rec["sub"]))

def moderate_submission(session: Session, submission_id: str, moderator_id: str, decision: ModerationDecision) -> SubmissionOut:
    now = _now_iso()
    rec = session.run("""
        MATCH (sub:Submission {id:$sid})-[:FOR]->(m:Mission)
        SET sub.state = $state, sub.reviewed_at = $now, sub.notes = $notes
        RETURN sub{.*, mid:m.id} AS sub, m
    """, {"sid": submission_id, "state": decision.state, "now": now, "notes": decision.notes}).single()

    sub = dict(rec["sub"])
    if decision.state == "approved":
        _award_on_approval(session, submission_id)

    return _to_submission_out(sub)

def _award_on_approval(session: Session, submission_id: str) -> None:
    # Read mission + guardrails
    rec = session.run("""
        MATCH (u:User)-[:SUBMITTED]->(sub:Submission {id:$sid})-[:FOR]->(m:Mission)
        RETURN u.id AS uid, m.id AS mid, coalesce(m.reward_eco,0) AS eco,
               m.max_completions_per_user AS max_c, coalesce(m.cooldown_days,0) AS cd
    """, {"sid": submission_id}).single()
    uid, mid, eco, max_c, cd = rec["uid"], rec["mid"], rec["eco"], rec["max_c"], rec["cd"]

    # Enforce per-user limits for this mission
    row = session.run("""
        MATCH (:User {id:$uid})-[:SUBMITTED]->(s:Submission {state:'approved'})-[:FOR]->(:Mission {id:$mid})
        RETURN count(s) AS c, max(toString(s.created_at)) AS last_ts
    """, {"uid": uid, "mid": mid}).single()
    count, last_ts = row["c"], row["last_ts"]

    if max_c and count >= max_c:
        return
    if cd and last_ts:
        last_dt = datetime.fromisoformat(last_ts)
        if datetime.utcnow() < last_dt + timedelta(days=cd):
            return

    # Write canonical Eco transaction in the unified ledger
    tid = uuid4().hex
    now = _now_iso()
    session.run("""
        MATCH (u:User {id:$uid}), (sub:Submission {id:$sid})-[:FOR]->(m:Mission {id:$mid})
        MERGE (t:EcoTransaction:EcoTx {id:$tid})
        SET t.eco    = $eco,
            t.at     = datetime($now),
            t.source = "mission",
            t.reason = "mission_reward",
            t.status = "settled"
        MERGE (u)-[:EARNED]->(t)
        MERGE (t)-[:FOR]->(m)
        MERGE (t)-[:PROOF]->(sub)
    """, {"uid": uid, "sid": submission_id, "mid": mid, "tid": tid, "eco": eco, "now": now})

# -------- bulk upsert --------
def bulk_upsert(session: Session, missions: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Upsert a list of mission dicts.
    - If 'id' present -> update that mission
    - Else -> create new mission
    Returns: {"created": int, "updated": int, "errors": [str, ...]}
    """
    created = 0
    updated = 0
    errors: List[str] = []

    for idx, raw in enumerate(missions, start=1):
        try:
            if "id" in raw and raw["id"]:
                # Update existing
                mid = str(raw["id"])
                payload = {k: v for k, v in raw.items() if k != "id"}
                mu = MissionUpdate(**payload)
                update_mission(session, mid, mu)
                updated += 1
            else:
                # Create new
                mc = MissionCreate(**raw)
                create_mission(session, mc)
                created += 1
        except Exception as e:
            errors.append(f"row {idx}: {type(e).__name__}: {e}")

    return {"created": created, "updated": updated, "errors": errors}
