from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple, TypedDict
from datetime import date, datetime, timedelta
from uuid import uuid4
from math import radians, sin, cos, asin, sqrt

from neo4j import Session

from .schema import (
    SidequestCreate, SidequestUpdate, SidequestOut,
    SubmissionCreate, SubmissionOut, ModerationDecision,
    UserProgressOut, RotationRequest, RotationResult,
)

# NEW: hook into gamification after awards
try:
    # soft import to avoid circular import issues if modules load order changes
    from site_backend.api.gamification.service import evaluate_badges_for_user as _eval_badges
except Exception:  # pragma: no cover
    _eval_badges = None


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
        return True
    d = _haversine_m(geo["lat"], geo["lon"], u_lat, u_lon)
    return d <= (geo.get("radius_m") or 0)


# ---------- Legacy mapping ----------
def _public_kind_from_legacy(legacy_type: Optional[str], legacy_sub_type: Optional[str]) -> str:
    if (legacy_type or "").lower() == "sidequest" and (legacy_sub_type or "").lower() == "eco_action":
        return "eco_action"
    return "core"


def _title_key(s: Optional[str]) -> Optional[str]:
    if not s:
        return None
    # lower + trim + collapse all internal whitespace to single spaces
    return " ".join(s.strip().lower().split())


# -------- helpers → SidequestOut --------
def _to_sidequest_out(d: Dict[str, Any]) -> SidequestOut:
    outward_kind = d.get("kind") or _public_kind_from_legacy(d.get("type"), d.get("sub_type"))
    return SidequestOut(
        id=d["id"],
        kind=outward_kind,
        title=d.get("title"),
        subtitle=d.get("subtitle"),
        description_md=d.get("description_md"),
        tags=d.get("tags"),
        reward_eco=d.get("reward_eco", 0),
        xp_reward=d.get("xp_reward", 0),
        max_completions_per_user=d.get("max_completions_per_user"),
        cooldown_days=d.get("cooldown_days"),
        pills=d.get("pills"),
        geo=d.get("geo"),
        streak=d.get("streak"),
        rotation=d.get("rotation"),
        chain=d.get("chain"),
        chain_index=d.get("chain_index"),
        chain_length=d.get("chain_length"),
        chain_slug=d.get("chain_slug"),
        team=d.get("team"),
        verification_methods=d.get("verification_methods") or ["photo_upload"],
        start_at=d.get("start_at"),
        end_at=d.get("end_at"),
        status=d.get("status", "draft"),
        hero_image=d.get("hero_image"),
        card_accent=d.get("card_accent"),
        created_at=d.get("created_at"),
        updated_at=d.get("updated_at"),
        legacy_type=d.get("type"),
        legacy_sub_type=d.get("sub_type"),
    )


def _to_submission_out(d: Dict[str, Any]) -> SubmissionOut:
    return SubmissionOut(
        id=d["id"],
        sidequest_id=d.get("sidequest_id", d.get("mid", "")),
        user_id=d.get("user_id", d.get("uid", "")),
        method=d.get("method"),
        state=d.get("state"),
        created_at=d.get("created_at"),
        reviewed_at=d.get("reviewed_at"),
        auto_checks=d.get("auto_checks") or {},
        notes=d.get("notes"),
        media_url=d.get("media_url"),
        instagram_url=d.get("instagram_url"),
        team_id=d.get("team_id"),
    )


# -------- list all (picker/feed) --------
def list_sidequests_all(
    session: Session,
    *,
    kind: Optional[str],
    status: Optional[str],
    q: Optional[str],
    tag: Optional[str],
    locality: Optional[str],
    cap: int = 5000,  # hard safety cap; tune as needed
) -> List[SidequestOut]:
    where = []
    params: Dict[str, Any] = {"cap": cap}

    if kind:
        where.append("sq.kind = $kind"); params["kind"] = kind
    else:
        where.append("sq.kind IS NOT NULL")

    if status:
        where.append("sq.status = $status"); params["status"] = status
    if q:
        where.append("(toLower(sq.title) CONTAINS toLower($q) OR toLower(sq.description_md) CONTAINS toLower($q))")
        params["q"] = q
    if tag:
        where.append("$tag IN coalesce(sq.tags, [])"); params["tag"] = tag
    if locality:
        where.append("toLower(coalesce(sq.geo_locality,'')) = toLower($locality)"); params["locality"] = locality

    where_clause = "WHERE " + " AND ".join(where) if where else ""
    recs = session.run(f"""
        MATCH (sq:Sidequest)
        {where_clause}
        // compute chain length for this sq
        OPTIONAL MATCH (sib:Sidequest {{chain_id: sq.chain_id}})
        WITH sq, count(sib) AS _chain_len
        ORDER BY coalesce(toString(sq.updated_at), toString(sq.created_at)) DESC
        LIMIT $cap
        RETURN sq{{
          .*,
          pills: CASE
            WHEN sq.pills_difficulty IS NULL
              AND sq.pills_impact IS NULL
              AND sq.pills_time_estimate_min IS NULL
              AND sq.pills_materials IS NULL
              AND sq.pills_facts IS NULL
            THEN NULL
            ELSE {{
              difficulty: sq.pills_difficulty,
              impact: sq.pills_impact,
              time_estimate_min: sq.pills_time_estimate_min,
              materials: sq.pills_materials,
              facts: sq.pills_facts
            }}
          END,
          geo: CASE
            WHEN sq.geo_lat IS NULL OR sq.geo_lon IS NULL THEN NULL
            ELSE {{
              lat: sq.geo_lat,
              lon: sq.geo_lon,
              radius_m: coalesce(sq.geo_radius_m, 0),
              locality: sq.geo_locality
            }}
          END,
          streak: CASE
            WHEN sq.streak_name IS NULL THEN NULL
            ELSE {{
              name: sq.streak_name,
              period: sq.streak_period,
              bonus_eco_per_step: coalesce(sq.streak_bonus_eco_per_step,0),
              max_steps: sq.streak_max_steps
            }}
          END,
          rotation: CASE
            WHEN sq.rot_is_weekly_slot IS NULL THEN NULL
            ELSE {{
              is_weekly_slot: sq.rot_is_weekly_slot,
              iso_year: sq.rot_iso_year,
              iso_week: sq.rot_iso_week,
              slot_index: sq.rot_slot_index,
              starts_on: CASE WHEN sq.rot_starts_on IS NULL THEN NULL ELSE toString(sq.rot_starts_on) END,
              ends_on:   CASE WHEN sq.rot_ends_on   IS NULL THEN NULL ELSE toString(sq.rot_ends_on)   END
            }}
          END,
          // NEW: normalized chain object + client hints
          chain: CASE
            WHEN sq.chain_id IS NULL THEN NULL
            ELSE {{
              id: sq.chain_id,
              order: sq.chain_order,
              requires_prev_approved: coalesce(sq.chain_requires_prev, false)
            }}
          END,
          chain_index: CASE WHEN sq.chain_id IS NULL THEN NULL ELSE sq.chain_order END,
          chain_length: CASE WHEN sq.chain_id IS NULL THEN NULL ELSE _chain_len END,
        chain_slug: CASE
  WHEN sq.chain_id IS NULL THEN NULL
  WHEN sq.chain_slug IS NOT NULL THEN sq.chain_slug
  ELSE replace(coalesce(sq.title_key, toLower(sq.title)), " ", "-")
END,

          // override top-level temporal fields as ISO strings for Pydantic
          start_at: CASE WHEN sq.start_at IS NULL THEN NULL ELSE toString(sq.start_at) END,
          end_at:   CASE WHEN sq.end_at   IS NULL THEN NULL ELSE toString(sq.end_at)   END,
          created_at: toString(sq.created_at),
          updated_at: toString(sq.updated_at)
        }} AS sq
    """, params)
    return [_to_sidequest_out(dict(r["sq"])) for r in recs]


# -------- flatten helpers (write-side) --------
def _flatten_from_create(m: SidequestCreate) -> Dict[str, Any]:
    pills = m.pills.model_dump() if m.pills else {}
    geo = m.geo.model_dump() if m.geo else {}
    streak = m.streak.model_dump() if m.streak else {}
    rotation = m.rotation.model_dump() if m.rotation else {}
    chain = m.chain.model_dump() if m.chain else {}
    team = m.team.model_dump() if m.team else {}
    return {
        "kind": m.kind,
        "title": m.title,
        "title_key": _title_key(m.title),   # <— add
        "subtitle": m.subtitle,
        "description_md": m.description_md,
        "tags": m.tags,
        "reward_eco": m.reward_eco,
        "xp_reward": m.xp_reward,
        "max_completions_per_user": m.max_completions_per_user,
        "cooldown_days": m.cooldown_days,
        "verification_methods": m.verification_methods,
        "start_at": m.start_at,
        "end_at": m.end_at,
        "status": m.status,
        "hero_image": m.hero_image,
        "card_accent": m.card_accent,
        # pills
        "pills_difficulty": pills.get("difficulty"),
        "pills_impact": pills.get("impact"),
        "pills_time_estimate_min": pills.get("time_estimate_min"),
        "pills_materials": pills.get("materials"),
        "pills_facts": pills.get("facts"),
        # geo
        "geo_lat": geo.get("lat"),
        "geo_lon": geo.get("lon"),
        "geo_radius_m": geo.get("radius_m"),
        "geo_locality": geo.get("locality"),
        # streak
        "streak_name": streak.get("name"),
        "streak_period": streak.get("period"),
        "streak_bonus_eco_per_step": streak.get("bonus_eco_per_step"),
        "streak_max_steps": streak.get("max_steps"),
        # rotation
        "rot_is_weekly_slot": rotation.get("is_weekly_slot"),
        "rot_iso_year": rotation.get("iso_year"),
        "rot_iso_week": rotation.get("iso_week"),
        "rot_slot_index": rotation.get("slot_index"),
        "rot_starts_on": rotation.get("starts_on"),
        "rot_ends_on": rotation.get("ends_on"),
        # chain
        "chain_id": chain.get("chain_id"),
        "chain_order": chain.get("chain_order"),
        "chain_requires_prev": chain.get("requires_prev_approved"),
        # team
        "team_allowed": team.get("allowed"),
        "team_min_size": team.get("min_size"),
        "team_max_size": team.get("max_size"),
        "team_bonus_eco": team.get("team_bonus_eco"),
    }


def _flatten_from_update(u: SidequestUpdate) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    if u.title is not None:
        out["title"] = u.title
        out["title_key"] = _title_key(u.title)
    if u.subtitle is not None: out["subtitle"] = u.subtitle
    if u.description_md is not None: out["description_md"] = u.description_md
    if u.tags is not None: out["tags"] = u.tags
    if u.reward_eco is not None: out["reward_eco"] = u.reward_eco
    if u.xp_reward is not None: out["xp_reward"] = u.xp_reward
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
        p = u.pills.model_dump()
        out["pills_difficulty"] = p.get("difficulty")
        out["pills_impact"] = p.get("impact")
        out["pills_time_estimate_min"] = p.get("time_estimate_min")
        out["pills_materials"] = p.get("materials")
        out["pills_facts"] = p.get("facts")
    # geo
    if u.geo is not None:
        g = u.geo.model_dump()
        out["geo_lat"] = g.get("lat")
        out["geo_lon"] = g.get("lon")
        out["geo_radius_m"] = g.get("radius_m")
        out["geo_locality"] = g.get("locality")
    # streak
    if u.streak is not None:
        st = u.streak.model_dump()
        out["streak_name"] = st.get("name")
        out["streak_period"] = st.get("period")
        out["streak_bonus_eco_per_step"] = st.get("bonus_eco_per_step")
        out["streak_max_steps"] = st.get("max_steps")
    # rotation
    if u.rotation is not None:
        rm = u.rotation.model_dump()
        out["rot_is_weekly_slot"] = rm.get("is_weekly_slot")
        out["rot_iso_year"] = rm.get("iso_year")
        out["rot_iso_week"] = rm.get("iso_week")
        out["rot_slot_index"] = rm.get("slot_index")
        out["rot_starts_on"] = rm.get("starts_on")
        out["rot_ends_on"] = rm.get("ends_on")
    # chain
    if u.chain is not None:
        ch = u.chain.model_dump()
        out["chain_id"] = ch.get("chain_id")
        out["chain_order"] = ch.get("chain_order")
        out["chain_requires_prev"] = ch.get("requires_prev_approved")
    # team
    if u.team is not None:
        tm = u.team.model_dump()
        out["team_allowed"] = tm.get("allowed")
        out["team_min_size"] = tm.get("min_size")
        out["team_max_size"] = tm.get("max_size")
        out["team_bonus_eco"] = tm.get("team_bonus_eco")
    return out


# -------- existence helper --------
def _sidequest_exists(session: Session, sid: str) -> bool:
    row = session.run(
        "MATCH (sq:Sidequest {id:$id}) RETURN count(sq) AS c",
        {"id": sid},
    ).single()
    return bool(row and row["c"] and int(row["c"]) > 0)


# -------- sidequests CRUD --------
def create_sidequest(session: Session, m: SidequestCreate, forced_id: Optional[str] = None) -> SidequestOut:
    """
    Create a sidequest. If forced_id is provided, use that id (for CSV upsert with explicit ids).
    """
    mid = forced_id or uuid4().hex
    now = _now_iso()
    flat = _flatten_from_create(m)
    rec = session.run(
        """
        MERGE (sq:Sidequest {id:$id})
          ON CREATE SET sq.created_at = $now
        SET
            sq.kind                      = $kind,
            sq.title                     = $title,
            sq.title_key                 = $title_key,
            sq.subtitle                  = $subtitle,
            sq.description_md            = $description_md,
            sq.tags                      = $tags,
            sq.reward_eco                = $reward_eco,
            sq.xp_reward                 = $xp_reward,
            sq.max_completions_per_user  = $max_completions_per_user,
            sq.cooldown_days             = $cooldown_days,
            sq.verification_methods      = $verification_methods,

            // time window (coerce to datetime or NULL)
            sq.start_at                  = CASE WHEN $start_at IS NULL OR $start_at = '' THEN NULL ELSE datetime($start_at) END,
            sq.end_at                    = CASE WHEN $end_at   IS NULL OR $end_at   = '' THEN NULL ELSE datetime($end_at)   END,

            sq.status                    = $status,
            sq.hero_image                = $hero_image,
            sq.card_accent               = $card_accent,
            sq.updated_at                = $now,

            // flattened pills
            sq.pills_difficulty          = $pills_difficulty,
            sq.pills_impact              = $pills_impact,
            sq.pills_time_estimate_min   = $pills_time_estimate_min,
            sq.pills_materials           = $pills_materials,
            sq.pills_facts               = $pills_facts,

            // flattened geo
            sq.geo_lat                   = $geo_lat,
            sq.geo_lon                   = $geo_lon,
            sq.geo_radius_m              = $geo_radius_m,
            sq.geo_locality              = $geo_locality,

            // flattened streak
            sq.streak_name               = $streak_name,
            sq.streak_period             = $streak_period,
            sq.streak_bonus_eco_per_step = $streak_bonus_eco_per_step,
            sq.streak_max_steps          = $streak_max_steps,

            // flattened rotation (dates coerced)
            sq.rot_is_weekly_slot        = $rot_is_weekly_slot,
            sq.rot_iso_year              = $rot_iso_year,
            sq.rot_iso_week              = $rot_iso_week,
            sq.rot_slot_index            = $rot_slot_index,
            sq.rot_starts_on             = CASE
                                              WHEN $rot_starts_on IS NULL OR $rot_starts_on = '' THEN NULL
                                              ELSE date($rot_starts_on)
                                            END,
            sq.rot_ends_on               = CASE
                                              WHEN $rot_ends_on IS NULL OR $rot_ends_on = '' THEN NULL
                                              ELSE date($rot_ends_on)
                                            END,

            // flattened chain
            sq.chain_id                  = $chain_id,
            sq.chain_order               = $chain_order,
            sq.chain_requires_prev       = $chain_requires_prev,

            // flattened team
            sq.team_allowed              = $team_allowed,
            sq.team_min_size             = $team_min_size,
            sq.team_max_size             = $team_max_size,
            sq.team_bonus_eco            = $team_bonus_eco

        // compute chain len for projection
        WITH sq
        OPTIONAL MATCH (sib:Sidequest {chain_id: sq.chain_id})
        WITH sq, count(sib) AS _chain_len

        RETURN sq{
          .*,
          pills: CASE
            WHEN sq.pills_difficulty IS NULL
              AND sq.pills_impact IS NULL
              AND sq.pills_time_estimate_min IS NULL
              AND sq.pills_materials IS NULL
              AND sq.pills_facts IS NULL
            THEN NULL
            ELSE {
              difficulty: sq.pills_difficulty,
              impact: sq.pills_impact,
              time_estimate_min: sq.pills_time_estimate_min,
              materials: sq.pills_materials,
              facts: sq.pills_facts
            }
          END,
          geo: CASE
            WHEN sq.geo_lat IS NULL OR sq.geo_lon IS NULL THEN NULL
            ELSE {
              lat: sq.geo_lat,
              lon: sq.geo_lon,
              radius_m: coalesce(sq.geo_radius_m, 0),
              locality: sq.geo_locality
            }
          END,
          streak: CASE
            WHEN sq.streak_name IS NULL THEN NULL
            ELSE {
              name: sq.streak_name,
              period: sq.streak_period,
              bonus_eco_per_step: coalesce(sq.streak_bonus_eco_per_step,0),
              max_steps: sq.streak_max_steps
            }
          END,
          rotation: CASE
            WHEN sq.rot_is_weekly_slot IS NULL THEN NULL
            ELSE {
              is_weekly_slot: sq.rot_is_weekly_slot,
              iso_year: sq.rot_iso_year,
              iso_week: sq.rot_iso_week,
              slot_index: sq.rot_slot_index,
              starts_on: CASE WHEN sq.rot_starts_on IS NULL THEN NULL ELSE toString(sq.rot_starts_on) END,
              ends_on:   CASE WHEN sq.rot_ends_on   IS NULL THEN NULL ELSE toString(sq.rot_ends_on)   END
            }
          END,
          // NEW: chain projection + hints
          chain: CASE
            WHEN sq.chain_id IS NULL THEN NULL
            ELSE {
              id: sq.chain_id,
              order: sq.chain_order,
              requires_prev_approved: coalesce(sq.chain_requires_prev, false)
            }
          END,
          chain_index: CASE WHEN sq.chain_id IS NULL THEN NULL ELSE sq.chain_order END,
          chain_length: CASE WHEN sq.chain_id IS NULL THEN NULL ELSE _chain_len END,
        chain_slug: CASE
  WHEN sq.chain_id IS NULL THEN NULL
  WHEN sq.chain_slug IS NOT NULL THEN sq.chain_slug
  ELSE replace(coalesce(sq.title_key, toLower(sq.title)), " ", "-")
END,

          // override top-level temporal fields as ISO strings for Pydantic
          start_at: CASE WHEN sq.start_at IS NULL THEN NULL ELSE toString(sq.start_at) END,
          end_at:   CASE WHEN sq.end_at   IS NULL THEN NULL ELSE toString(sq.end_at)   END,
          created_at: toString(sq.created_at),
          updated_at: toString(sq.updated_at)
        } AS sq
        """,
        {"id": mid, "now": now, **flat}
    ).single()

    if not rec or not rec["sq"]:
        raise ValueError("create_sidequest: failed to create or return node")

    return _to_sidequest_out(dict(rec["sq"]))


def update_sidequest(session: Session, sidequest_id: str, u: SidequestUpdate) -> SidequestOut:
    if not _sidequest_exists(session, sidequest_id):
        # Cleaner error (caught by bulk_upsert to fall back to create-with-id)
        raise ValueError(f"Sidequest not found for update: {sidequest_id}")

    now = _now_iso()
    fields = _flatten_from_update(u)

    # Build SET lines, with special handling to coerce rotation/start_end dates
    set_lines: List[str] = []
    params: Dict[str, Any] = {"id": sidequest_id, "now": now}

    for k, v in fields.items():
        if k == "rot_starts_on":
            set_lines.append(
                "sq.rot_starts_on = CASE WHEN $rot_starts_on IS NULL OR $rot_starts_on = '' "
                "THEN NULL ELSE date($rot_starts_on) END"
            )
            params["rot_starts_on"] = v
        elif k == "rot_ends_on":
            set_lines.append(
                "sq.rot_ends_on = CASE WHEN $rot_ends_on IS NULL OR $rot_ends_on = '' "
                "THEN NULL ELSE date($rot_ends_on) END"
            )
            params["rot_ends_on"] = v
        elif k == "start_at":
            set_lines.append(
                "sq.start_at = CASE WHEN $start_at IS NULL OR $start_at = '' "
                "THEN NULL ELSE datetime($start_at) END"
            )
            params["start_at"] = v
        elif k == "end_at":
            set_lines.append(
                "sq.end_at = CASE WHEN $end_at IS NULL OR $end_at = '' "
                "THEN NULL ELSE datetime($end_at) END"
            )
            params["end_at"] = v
        else:
            set_lines.append(f"sq.{k} = ${k}")
            params[k] = v

    set_lines.append("sq.updated_at = $now")

    q = f"""
    MATCH (sq:Sidequest {{id:$id}})
    SET {", ".join(set_lines)}
    // compute chain len for projection
    WITH sq
    OPTIONAL MATCH (sib:Sidequest {{chain_id: sq.chain_id}})
    WITH sq, count(sib) AS _chain_len
    RETURN sq{{
      .*,
      pills: CASE
        WHEN sq.pills_difficulty IS NULL
          AND sq.pills_impact IS NULL
          AND sq.pills_time_estimate_min IS NULL
          AND sq.pills_materials IS NULL
          AND sq.pills_facts IS NULL
        THEN NULL
        ELSE {{
          difficulty: sq.pills_difficulty,
          impact: sq.pills_impact,
          time_estimate_min: sq.pills_time_estimate_min,
          materials: sq.pills_materials,
          facts: sq.pills_facts
        }}
      END,
      geo: CASE
        WHEN sq.geo_lat IS NULL OR sq.geo_lon IS NULL THEN NULL
        ELSE {{
          lat: sq.geo_lat,
          lon: sq.geo_lon,
          radius_m: coalesce(sq.geo_radius_m, 0),
          locality: sq.geo_locality
        }}
      END,
      streak: CASE
        WHEN sq.streak_name IS NULL THEN NULL
        ELSE {{
          name: sq.streak_name,
          period: sq.streak_period,
          bonus_eco_per_step: coalesce(sq.streak_bonus_eco_per_step,0),
          max_steps: sq.streak_max_steps
        }}
      END,
      rotation: CASE
        WHEN sq.rot_is_weekly_slot IS NULL THEN NULL
        ELSE {{
          is_weekly_slot: sq.rot_is_weekly_slot,
          iso_year: sq.rot_iso_year,
          iso_week: sq.rot_iso_week,
          slot_index: sq.rot_slot_index,
          starts_on: CASE WHEN sq.rot_starts_on IS NULL THEN NULL ELSE toString(sq.rot_starts_on) END,
          ends_on:   CASE WHEN sq.rot_ends_on   IS NULL THEN NULL ELSE toString(sq.rot_ends_on)   END
        }}
      END,
      // NEW: chain projection + hints
      chain: CASE
        WHEN sq.chain_id IS NULL THEN NULL
        ELSE {{
          id: sq.chain_id,
          order: sq.chain_order,
          requires_prev_approved: coalesce(sq.chain_requires_prev, false)
        }}
      END,
      chain_index: CASE WHEN sq.chain_id IS NULL THEN NULL ELSE sq.chain_order END,
      chain_length: CASE WHEN sq.chain_id IS NULL THEN NULL ELSE _chain_len END,
     chain_slug: CASE
  WHEN sq.chain_id IS NULL THEN NULL
  WHEN sq.chain_slug IS NOT NULL THEN sq.chain_slug
  ELSE replace(coalesce(sq.title_key, toLower(sq.title)), " ", "-")
END,

      // override top-level temporal fields as ISO strings for Pydantic
      start_at: CASE WHEN sq.start_at IS NULL THEN NULL ELSE toString(sq.start_at) END,
      end_at:   CASE WHEN sq.end_at   IS NULL THEN NULL ELSE toString(sq.end_at)   END,
      created_at: toString(sq.created_at),
      updated_at: toString(sq.updated_at)
    }} AS sq
    """
    rec = session.run(q, params).single()
    if not rec or not rec["sq"]:
        raise ValueError(f"update_sidequest: failed to return node for {sidequest_id}")

    return _to_sidequest_out(dict(rec["sq"]))


def get_sidequest(session: Session, sidequest_id: str) -> SidequestOut:
    rec = session.run(
        """
        MATCH (sq:Sidequest {id:$id})
        OPTIONAL MATCH (sib:Sidequest {chain_id: sq.chain_id})
        WITH sq, count(sib) AS _chain_len
        RETURN sq{
          .*,
          pills: CASE
            WHEN sq.pills_difficulty IS NULL
              AND sq.pills_impact IS NULL
              AND sq.pills_time_estimate_min IS NULL
              AND sq.pills_materials IS NULL
              AND sq.pills_facts IS NULL
            THEN NULL
            ELSE {
              difficulty: sq.pills_difficulty,
              impact: sq.pills_impact,
              time_estimate_min: sq.pills_time_estimate_min,
              materials: sq.pills_materials,
              facts: sq.pills_facts
            }
          END,
          geo: CASE
            WHEN sq.geo_lat IS NULL OR sq.geo_lon IS NULL THEN NULL
            ELSE {
              lat: sq.geo_lat,
              lon: sq.geo_lon,
              radius_m: coalesce(sq.geo_radius_m, 0),
              locality: sq.geo_locality
            }
          END,
          streak: CASE
            WHEN sq.streak_name IS NULL THEN NULL
            ELSE {
              name: sq.streak_name,
              period: sq.streak_period,
              bonus_eco_per_step: coalesce(sq.streak_bonus_eco_per_step,0),
              max_steps: sq.streak_max_steps
            }
          END,
          rotation: CASE
            WHEN sq.rot_is_weekly_slot IS NULL THEN NULL
            ELSE {
              is_weekly_slot: sq.rot_is_weekly_slot,
              iso_year: sq.rot_iso_year,
              iso_week: sq.rot_iso_week,
              slot_index: sq.rot_slot_index,
              starts_on: CASE WHEN sq.rot_starts_on IS NULL THEN NULL ELSE toString(sq.rot_starts_on) END,
              ends_on:   CASE WHEN sq.rot_ends_on   IS NULL THEN NULL ELSE toString(sq.rot_ends_on)   END
            }
          END,
          // NEW: chain projection + hints
          chain: CASE
            WHEN sq.chain_id IS NULL THEN NULL
            ELSE {
              id: sq.chain_id,
              order: sq.chain_order,
              requires_prev_approved: coalesce(sq.chain_requires_prev, false)
            }
          END,
          chain_index: CASE WHEN sq.chain_id IS NULL THEN NULL ELSE sq.chain_order END,
          chain_length: CASE WHEN sq.chain_id IS NULL THEN NULL ELSE _chain_len END,
         chain_slug: CASE
  WHEN sq.chain_id IS NULL THEN NULL
  WHEN sq.chain_slug IS NOT NULL THEN sq.chain_slug
  ELSE replace(coalesce(sq.title_key, toLower(sq.title)), " ", "-")
END,

          // override top-level temporal fields as ISO strings for Pydantic
          start_at: CASE WHEN sq.start_at IS NULL THEN NULL ELSE toString(sq.start_at) END,
          end_at:   CASE WHEN sq.end_at   IS NULL THEN NULL ELSE toString(sq.end_at)   END,
          created_at: toString(sq.created_at),
          updated_at: toString(sq.updated_at)
        } AS sq
        """,
        {"id": sidequest_id}
    ).single()
    if not rec or not rec["sq"]:
        raise ValueError(f"Sidequest not found: {sidequest_id}")
    return _to_sidequest_out(dict(rec["sq"]))


def list_sidequests(
    session: Session,
    *,
    kind: Optional[str],
    status: Optional[str],
    q: Optional[str],
    tag: Optional[str],
    locality: Optional[str],
    limit: int,
    skip: int,
) -> List[SidequestOut]:
    where = []
    params: Dict[str, Any] = {"limit": limit, "skip": skip}

    if kind:
        where.append("sq.kind = $kind"); params["kind"] = kind
    else:
        # default to the sidequest universe (all kinds)
        where.append("sq.kind IS NOT NULL")

    if status:
        where.append("sq.status = $status"); params["status"] = status
    if q:
        where.append("(toLower(sq.title) CONTAINS toLower($q) OR toLower(sq.description_md) CONTAINS toLower($q))")
        params["q"] = q
    if tag:
        where.append("$tag IN coalesce(sq.tags, [])"); params["tag"] = tag
    if locality:
        where.append("toLower(coalesce(sq.geo_locality,'')) = toLower($locality)"); params["locality"] = locality

    where_clause = "WHERE " + " AND ".join(where) if where else ""
    recs = session.run(f"""
        MATCH (sq:Sidequest)
        {where_clause}
        OPTIONAL MATCH (sib:Sidequest {{chain_id: sq.chain_id}})
        WITH sq, count(sib) AS _chain_len
        ORDER BY coalesce(toString(sq.updated_at), toString(sq.created_at)) DESC
        SKIP $skip LIMIT $limit
        RETURN sq{{
          .*,
          pills: CASE
            WHEN sq.pills_difficulty IS NULL
              AND sq.pills_impact IS NULL
              AND sq.pills_time_estimate_min IS NULL
              AND sq.pills_materials IS NULL
              AND sq.pills_facts IS NULL
            THEN NULL
            ELSE {{
              difficulty: sq.pills_difficulty,
              impact: sq.pills_impact,
              time_estimate_min: sq.pills_time_estimate_min,
              materials: sq.pills_materials,
              facts: sq.pills_facts
            }}
          END,
          geo: CASE
            WHEN sq.geo_lat IS NULL OR sq.geo_lon IS NULL THEN NULL
            ELSE {{
              lat: sq.geo_lat,
              lon: sq.geo_lon,
              radius_m: coalesce(sq.geo_radius_m, 0),
              locality: sq.geo_locality
            }}
          END,
          streak: CASE
            WHEN sq.streak_name IS NULL THEN NULL
            ELSE {{
              name: sq.streak_name,
              period: sq.streak_period,
              bonus_eco_per_step: coalesce(sq.streak_bonus_eco_per_step,0),
              max_steps: sq.streak_max_steps
            }}
          END,
          rotation: CASE
            WHEN sq.rot_is_weekly_slot IS NULL THEN NULL
            ELSE {{
              is_weekly_slot: sq.rot_is_weekly_slot,
              iso_year: sq.rot_iso_year,
              iso_week: sq.rot_iso_week,
              slot_index: sq.rot_slot_index,
              starts_on: CASE WHEN sq.rot_starts_on IS NULL THEN NULL ELSE toString(sq.rot_starts_on) END,
              ends_on:   CASE WHEN sq.rot_ends_on   IS NULL THEN NULL ELSE toString(sq.rot_ends_on)   END
            }}
          END,
          // NEW: chain projection + hints
          chain: CASE
            WHEN sq.chain_id IS NULL THEN NULL
            ELSE {{
              id: sq.chain_id,
              order: sq.chain_order,
              requires_prev_approved: coalesce(sq.chain_requires_prev, false)
            }}
          END,
          chain_index: CASE WHEN sq.chain_id IS NULL THEN NULL ELSE sq.chain_order END,
          chain_length: CASE WHEN sq.chain_id IS NULL THEN NULL ELSE _chain_len END,
        chain_slug: CASE
  WHEN sq.chain_id IS NULL THEN NULL
  WHEN sq.chain_slug IS NOT NULL THEN sq.chain_slug
  ELSE replace(coalesce(sq.title_key, toLower(sq.title)), " ", "-")
END,

          // override top-level temporal fields as ISO strings for Pydantic
          start_at: CASE WHEN sq.start_at IS NULL THEN NULL ELSE toString(sq.start_at) END,
          end_at:   CASE WHEN sq.end_at   IS NULL THEN NULL ELSE toString(sq.end_at)   END,
          created_at: toString(sq.created_at),
          updated_at: toString(sq.updated_at)
        }} AS sq
    """, params)
    return [_to_sidequest_out(dict(r["sq"])) for r in recs]


# --- Add near other helpers ---
def ensure_chain_prereq(session: Session, sidequest_id: str, user_id: str):
    """
    Raise HTTP 403 if this sidequest is part of a chain that requires previous approval
    and the user hasn't got the previous step approved yet.
    """
    rec = session.run("""
        MATCH (sq:Sidequest {id:$sid})
        RETURN sq.chain_id AS cid, sq.chain_order AS ord, coalesce(sq.chain_requires_prev,false) AS req
    """, {"sid": sidequest_id}).single()

    if not rec:
        return  # unknown sidequest → let general 404/validation handle

    cid = rec["cid"]; ord_ = rec["ord"]; requires = bool(rec["req"])
    if not cid or not requires:
        return  # not a chain or rule disabled

    # If it's the first in the chain, nothing to enforce
    if ord_ is None or int(ord_) <= 0:
        return

    prev_ord = int(ord_) - 1

    # Does user have an APPROVED submission on the previous step?
    ok = session.run("""
        MATCH (prev:Sidequest {chain_id:$cid, chain_order:$prev})
        OPTIONAL MATCH (:User {id:$uid})-[:SUBMITTED]->(sub:Submission {state:'approved'})-[:FOR]->(prev)
        RETURN prev.id AS pid, COUNT(sub) > 0 AS has_approved
    """, {"cid": cid, "prev": prev_ord, "uid": user_id}).single()

    if not ok or not ok["pid"]:
        # chain broken/malformed; be strict
        from fastapi import HTTPException, status
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN,
                            detail="Chain prerequisite not found")

    if not ok["has_approved"]:
        from fastapi import HTTPException, status
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN,
                            detail="Finish the previous chain step first")


# -------- submissions & rewards --------
def create_submission(session: Session, user_id: str, s: SubmissionCreate, media_meta: Optional[Dict[str, Any]]) -> SubmissionOut:
    # Accept either 'sidequest_id' or legacy 'mission_id'
    sidequest_id = getattr(s, "sidequest_id", None) or getattr(s, "mission_id", None)
    if not sidequest_id:
        raise ValueError("Missing sidequest id")

    # Enforce chain prerequisite (server-side authority)
    ensure_chain_prereq(session, sidequest_id=sidequest_id, user_id=user_id)

    # self import safe in same module
    from .service import get_sidequest  # type: ignore
    m = get_sidequest(session, sidequest_id)
    auto_checks: Dict[str, bool] = {}

    auto_checks["within_radius"] = _within_radius(m.geo.model_dump() if m.geo else None, s.user_lat, s.user_lon)

    phash = (media_meta or {}).get("phash")
    if s.method == "photo_upload" and phash:
        rows = session.run("""
            MATCH (:User {id:$uid})-[:SUBMITTED]->(sub:Submission {state:'approved'})-[:FOR]->(:Sidequest {id:$mid})
            WHERE sub.phash IS NOT NULL
            RETURN sub.phash AS phash
        """, {"uid": user_id, "mid": sidequest_id}).value("phash")
        auto_checks["duplicate_media"] = phash in set(rows)
    else:
        auto_checks["duplicate_media"] = False

    tag_ok = False
    if s.method == "instagram_link":
        if s.instagram_url:
            u = str(s.instagram_url)
            tag_ok = any(t in u for t in ["#Ecodia", "ecodia", "eco_district", "ecopoints"])
        if s.caption:
            cap = s.caption.lower()
            tag_ok = tag_ok or any(t in cap for t in ["#ecodia", "#ecodistrict", "#eco", "#wattle", "ecopoints"])
    auto_checks["insta_tag_heuristic"] = tag_ok

    team_id = None
    if s.team_id and m.team and m.team.allowed:
        team_id = s.team_id

    sid = uuid4().hex
    now = _now_iso()
    rec = session.run("""
        MATCH (u:User {id:$uid}), (sq:Sidequest {id:$mid})
        MERGE (sub:Submission {id:$sid})
        SET sub.method      = $method,
            sub.state       = 'pending',
            sub.created_at  = datetime($now),  // <- store as datetime
            sub.auto_checks = $auto,
            sub.media_url   = $media_url,
            sub.instagram_url = $insta_url,
            sub.caption     = $caption,
            sub.user_lat    = $ulat,
            sub.user_lon    = $ulon,
            sub.phash       = $phash,
            sub.team_id     = $team_id
        MERGE (u)-[:SUBMITTED]->(sub)
        MERGE (sub)-[:FOR]->(sq)
        RETURN sub{.*, uid:u.id, mid:sq.id} AS sub
    """, {
        "uid": user_id, "mid": sidequest_id, "sid": sid, "now": now,
        "method": s.method, "auto": auto_checks,
        "media_url": (media_meta or {}).get("path"),
        "phash": phash,
        "insta_url": s.instagram_url, "caption": s.caption,
        "ulat": s.user_lat, "ulon": s.user_lon,
        "team_id": team_id,
    }).single()

    return _to_submission_out(dict(rec["sub"]))


def moderate_submission(session: Session, submission_id: str, moderator_id: str, decision: ModerationDecision) -> SubmissionOut:
    now = _now_iso()
    rec = session.run("""
        MATCH (sub:Submission {id:$sid})-[:FOR]->(sq:Sidequest)
        SET sub.state = $state, sub.reviewed_at = $now, sub.notes = $notes
        RETURN sub{.*, mid:sq.id} AS sub, sq
    """, {"sid": submission_id, "state": decision.state, "now": now, "notes": decision.notes}).single()

    sub = dict(rec["sub"])
    if decision.state == "approved":
        _award_on_approval(session, submission_id)

        # 🔥 Gamification hook: evaluate badges after every approval.
        try:
            if _eval_badges:
                # Pass current active Season if present
                cur_ss = session.run(
                    "MATCH (ss:Season) WHERE ss.start <= datetime() AND ss.end > datetime() RETURN ss LIMIT 1"
                ).single()
                season_id = cur_ss["ss"]["id"] if cur_ss else None
                uid = sub.get("uid")
                if uid:
                    _ = _eval_badges(session, uid=uid, season_id=season_id)
        except Exception:
            # don’t block approval if eval fails; logs can capture this server-side
            pass

    return _to_submission_out(sub)


def _award_on_approval(session: Session, submission_id: str) -> None:
    rec = session.run("""
        MATCH (u:User)-[:SUBMITTED]->(sub:Submission {id:$sid})-[:FOR]->(sq:Sidequest)
        RETURN u.id AS uid, sq.id AS mid, coalesce(sq.reward_eco,0) AS eco,
               coalesce(sq.xp_reward,0) AS xp,
               sq.max_completions_per_user AS max_c, coalesce(sq.cooldown_days,0) AS cd,
               sq.streak_period AS streak_period,
               coalesce(sq.streak_bonus_eco_per_step,0) AS streak_bonus,
               sq.streak_max_steps AS streak_cap,
               sq.team_allowed AS team_allowed,
               sub.team_id AS sub_team
    """, {"sid": submission_id}).single()

    uid = rec["uid"]; mid = rec["mid"]
    eco = rec["eco"]; xp = rec["xp"]
    max_c = rec["max_c"]; cd = rec["cd"]
    streak_period = rec["streak_period"]; streak_bonus = rec["streak_bonus"]; streak_cap = rec["streak_cap"]
    team_allowed = rec["team_allowed"]; sub_team = rec["sub_team"]

    row = session.run("""
        MATCH (:User {id:$uid})-[:SUBMITTED]->(s:Submission {state:'approved'})-[:FOR]->(:Sidequest {id:$mid})
        RETURN count(s) AS c, toString(max(datetime(s.created_at))) AS last_ts
    """, {"uid": uid, "mid": mid}).single()
    count, last_ts = row["c"], row["last_ts"]

    if max_c and count >= max_c:
        return
    if cd and last_ts:
        last_dt = datetime.fromisoformat(last_ts)
        if datetime.utcnow() < last_dt + timedelta(days=cd):
            return

    # Streak bonus (light heuristic)
    bonus = 0
    if streak_period and streak_bonus:
        if streak_period == "weekly":
            prev = session.run("""
                MATCH (:User {id:$uid})-[:SUBMITTED]->(s:Submission {state:'approved'})
                WITH s, date(datetime(s.created_at)) AS d
                RETURN d.year AS y, d.week AS w
                ORDER BY y DESC, w DESC
                LIMIT 1
            """, {"uid": uid}).single()
            if prev:
                ty, tw, _ = date.today().isocalendar()
                if (prev["y"], prev["w"] + 1) == (ty, tw):
                    bonus = streak_bonus
        elif streak_period == "daily":
            prev = session.run("""
                MATCH (:User {id:$uid})-[:SUBMITTED]->(s:Submission {state:'approved'})
                RETURN max(date(datetime(s.created_at))) AS d
            """, {"uid": uid}).single()
            if prev and prev["d"]:
                if prev["d"] == date.today() - timedelta(days=1):
                    bonus = streak_bonus

    eco_total = int(eco) + int(bonus)

    # Canonical ECO/XP transaction
    tid = uuid4().hex
    now = _now_iso()
    session.run("""
        MATCH (u:User {id:$uid}), (sub:Submission {id:$sid})-[:FOR]->(sq:Sidequest {id:$mid})
        MERGE (t:EcoTransaction:EcoTx {id:$tid})
        SET t.eco    = $eco,
            t.xp     = $xp,
            t.bonus  = $bonus,
            t.at     = datetime($now),
            t.source = "sidequest",
            t.reason = "sidequest_reward",
            t.status = "settled"
        MERGE (u)-[:EARNED]->(t)
        MERGE (t)-[:FOR]->(sq)
        MERGE (t)-[:PROOF]->(sub)
    """, {"uid": uid, "sid": submission_id, "mid": mid, "tid": tid, "eco": eco_total, "xp": xp, "bonus": bonus, "now": now})

    if team_allowed and sub_team:
        tbid = uuid4().hex
        session.run("""
            MATCH (t:EcoTx {id:$tid})
            MERGE (tb:TeamBonus {id:$tbid})
            SET tb.team_id = $team_id, tb.eco = 0, tb.at = datetime($now)
            MERGE (t)-[:TEAM_BONUS]->(tb)
        """, {"tid": tid, "tbid": tbid, "team_id": sub_team, "now": now})


# -------- progress & rotation --------
def parse_iso_utc(s: str) -> datetime:
    return datetime.fromisoformat(s.replace("Z", "+00:00"))


def get_user_progress(session: Session, user_id: str) -> UserProgressOut:
    # recent approvals (last 30)
    recs = session.run("""
        MATCH (:User {id:$uid})-[:SUBMITTED]->(s:Submission {state:'approved'})-[:FOR]->(sq:Sidequest)
        WITH s, sq
        ORDER BY datetime(s.created_at) DESC
        LIMIT 30
        RETURN collect(sq.id) AS ids
    """, {"uid": user_id}).single()
    recent_ids = (recs and recs["ids"]) or []

    # cooldowns
    cd_rows = session.run("""
        MATCH (:User {id:$uid})-[:SUBMITTED]->(s:Submission {state:'approved'})-[:FOR]->(sq:Sidequest)
        WITH s, sq
        WHERE coalesce(sq.cooldown_days,0) > 0
        WITH sq.id AS id,
             max(datetime(s.created_at)) AS last_at,
             max(coalesce(sq.cooldown_days,0)) AS cd
        RETURN id AS sid, toString(last_at) AS last_ts, cd AS cd
    """, {"uid": user_id})
    cooldowns = []
    for r in cd_rows:
        last_ts = r["last_ts"]
        until = None
        if last_ts:
            last_dt = parse_iso_utc(last_ts)
            until_dt = last_dt + timedelta(days=r["cd"] or 0)
            if until_dt > datetime.utcnow():
                until = until_dt
        if until:
            cooldowns.append({"sidequest_id": r["sid"], "until": until})

    # streak steps (weekly heuristic)
    nsr = session.run("""
        MATCH (:User {id:$uid})-[:SUBMITTED]->(s:Submission {state:'approved'})
        WITH date(datetime(s.created_at)) AS d
        RETURN collect({y:d.year, w:d.week}) AS weeks
    """, {"uid": user_id}).single()
    weeks = (nsr and nsr["weeks"]) or []
    streak_steps = 0
    if weeks:
        seen = {(w["y"], w["w"]) for w in weeks}
        ty, tw, _ = datetime.utcnow().date().isocalendar()
        # count back consecutive presence ending this week
        while (ty, tw - streak_steps) in seen:
            streak_steps += 1

    # eligible weekly ids (active+current rotation window)
    ewr = session.run("""
        MATCH (sq:Sidequest {status:'active'})
        WHERE sq.rot_is_weekly_slot = true
          AND sq.rot_starts_on IS NOT NULL AND sq.rot_ends_on IS NOT NULL
        WITH sq WHERE date() >= sq.rot_starts_on AND date() <= sq.rot_ends_on
        RETURN collect(sq.id) AS ids
    """).single()
    eligible_weekly = (ewr and ewr["ids"]) or []

    # next streak reset (end of current ISO week, Sunday 23:59:59 UTC)
    today_d = datetime.utcnow().date()
    wd = today_d.isoweekday()  # 1=Mon .. 7=Sun
    week_start = today_d - timedelta(days=wd - 1)
    next_reset = datetime.combine(week_start + timedelta(days=7), datetime.min.time()) - timedelta(seconds=1)

    return UserProgressOut(
        recent_approved_ids=list(recent_ids),
        cooldowns=cooldowns,
        streak_steps=streak_steps,
        next_streak_reset_at=next_reset,
        eligible_weekly_ids=list(eligible_weekly),
    )


def rotate_weekly_sidequests(session: Session, payload: RotationRequest) -> RotationResult:
    """
    Activates all eligible weekly sidequests (or up to max_slots) for the given ISO window.
    We do not archive; we only stamp rotation fields and status='active'.
    """
    params = {
        "y": payload.iso_year,
        "w": payload.iso_week,
        "start": payload.starts_on.isoformat(),
        "end": payload.ends_on.isoformat(),
        "limit": payload.max_slots or 999_999,
        "now": _now_iso(),
    }
    recs = session.run("""
        MATCH (sq:Sidequest {kind:'weekly'})
        WHERE coalesce(sq.status,'draft') <> 'archived'
        WITH sq
        // Prefer those not already assigned to this week
        ORDER BY CASE WHEN sq.rot_iso_year = $y AND sq.rot_iso_week = $w THEN 1 ELSE 0 END ASC,
                 coalesce(sq.updated_at, sq.created_at) DESC
        LIMIT $limit
        WITH collect(sq) AS picks
        UNWIND picks AS sq
        SET sq.rot_is_weekly_slot = true,
            sq.rot_iso_year = $y,
            sq.rot_iso_week = $w,
            sq.rot_starts_on = date($start),
            sq.rot_ends_on = date($end),
            sq.status = 'active',
            sq.updated_at = $now
        RETURN collect(sq.id) AS ids
    """, params).single()

    activated = recs["ids"] or []
    return RotationResult(
        iso_year=payload.iso_year,
        iso_week=payload.iso_week,
        activated_ids=list(activated),
        window=payload,
    )


# -------- bulk upsert (sidequests) --------
def bulk_upsert(session: Session, sidequests: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Upsert a list of sidequest dicts.
    Behavior:
      - If 'id' present AND exists → update
      - If 'id' present AND missing → create with that same id
      - If 'id' absent → create (auto id)
    Returns: {"created": int, "updated": int, "errors": [str, ...]}
    """
    import json, traceback

    created = 0
    updated = 0
    errors: List[str] = []

    for idx, raw in enumerate(sidequests, start=1):
        try:
            if "id" in raw and raw["id"]:
                sid = str(raw["id"])
                payload = {k: v for k, v in raw.items() if k != "id"}
                try:
                    if _sidequest_exists(session, sid):
                        mu = SidequestUpdate(**payload)
                        update_sidequest(session, sid, mu)
                        updated += 1
                    else:
                        # Create with the caller-provided id
                        mc = SidequestCreate(**payload)
                        create_sidequest(session, mc, forced_id=sid)
                        created += 1
                except Exception as e:
                    tb = traceback.format_exc().strip().splitlines()[-2:]
                    errors.append(
                        f"row {idx}: upsert:{type(e).__name__}: {e} | keys={sorted(list(raw.keys()))} | trace_tail={' | '.join(tb)}"
                    )
            else:
                mc = SidequestCreate(**raw)
                create_sidequest(session, mc)
                created += 1
        except Exception as e:
            tb = traceback.format_exc().strip().splitlines()[-2:]
            errors.append(f"row {idx}: unknown:{type(e).__name__}: {e} | trace_tail={' | '.join(tb)}")

    return {"created": created, "updated": updated, "errors": errors}


# -------- Chain Context (user-aware) --------
class ChainStepOut(TypedDict, total=False):
    id: str
    title: str
    href: str
    done: bool
    locked: bool
    order: int

class ChainContextOut(TypedDict, total=False):
    chain_id: str
    slug: str
    steps: List[ChainStepOut]
    current_index: int


def get_chain_context(
    session: Session,
    *,
    chain_id: str,
    user_id: Optional[str] = None,
    current_sidequest_id: Optional[str] = None,
) -> Dict[str, Any]:
    rec = session.run("""
        // gather ordered steps for chain
        MATCH (sq:Sidequest {chain_id:$cid})
        WITH sq
        ORDER BY coalesce(sq.chain_order, 0) ASC, toString(coalesce(sq.updated_at, sq.created_at)) ASC
        WITH collect(sq) AS steps
        // which of these steps has the user approved?
        OPTIONAL MATCH (:User {id:$uid})-[:SUBMITTED]->(sub:Submission {state:'approved'})-[:FOR]->(dsq:Sidequest)
        WHERE $uid IS NOT NULL AND dsq.chain_id = $cid
        WITH steps, collect(dsq.id) AS done_ids
        // derive slug from first step's title_key/title (kept for later if you add a chain page)
        WITH steps, done_ids,
             CASE
               WHEN size(steps) = 0 THEN 'chain'
               ELSE coalesce(steps[0].title_key, toLower(steps[0].title))
             END AS base_slug
        WITH steps, done_ids, replace(base_slug, ' ', '-') AS slug
        WITH steps, done_ids, slug,
             [i IN range(0, size(steps)-1) |
               {
                 id:      steps[i].id,
                 title:   steps[i].title,
                 // SAFE: use an in-page anchor the UI already handles
                 href:    '#sq-' + steps[i].id,
                 order:   coalesce(steps[i].chain_order, i),
                 done:    steps[i].id IN done_ids,
                 locked:  coalesce(steps[i].chain_requires_prev, false)
                          AND (i > 0 AND NOT (steps[i-1].id IN done_ids))
               }
             ] AS mapped
        RETURN slug, mapped AS steps
    """, {"cid": chain_id, "uid": user_id}).single()

    if not rec:
        return {"chain_id": chain_id, "slug": "chain", "steps": [], "current_index": 0}

    steps = [dict(s) for s in rec["steps"]]
    slug = rec["slug"] or "chain"

    idx = 0
    if current_sidequest_id:
        for i, s in enumerate(steps):
            if s["id"] == current_sidequest_id:
                idx = i
                break

    return {
        "chain_id": chain_id,
        "slug": slug,
        "steps": steps,
        "current_index": idx,
    }
