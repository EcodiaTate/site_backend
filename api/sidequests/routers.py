# site_backend/api/sidequests/routes.py
from __future__ import annotations

from typing import List, Optional, Any, Dict
from fastapi import APIRouter, Depends, File, UploadFile, Query, HTTPException, status
from neo4j import Session
import csv, io, json
from datetime import date, datetime

from .schema import (
    SidequestCreate, SidequestUpdate, SidequestOut,
    SubmissionCreate, SubmissionOut, ModerationDecision,
    UserProgressOut, RotationRequest, RotationResult, BulkUpsertResult,
)
from .service import (
    create_sidequest, update_sidequest, get_sidequest, list_sidequests,
    create_submission, moderate_submission, bulk_upsert,
    get_user_progress, rotate_weekly_sidequests, list_sidequests_all, get_chain_context,
    ensure_chain_prereq,  # ← enforce chain rules before accepting submissions
)
from .media import save_image_and_fingerprints
from site_backend.core.neo_driver import session_dep
from site_backend.core.admin_guard import require_admin
from site_backend.core.user_guard import current_user_id

router = APIRouter(prefix="/sidequests", tags=["sidequests"])

# -----------------------------------------------------------------------------
# Admin-only
# -----------------------------------------------------------------------------

@router.post("", response_model=SidequestOut)
def r_create_sidequest(
    payload: SidequestCreate,
    session: Session = Depends(session_dep),
    admin_email: str = Depends(require_admin),
):
    return create_sidequest(session, payload)


@router.patch("/{sidequest_id}", response_model=SidequestOut)
def r_update_sidequest(
    sidequest_id: str,
    payload: SidequestUpdate,
    session: Session = Depends(session_dep),
    admin_email: str = Depends(require_admin),
):
    return update_sidequest(session, sidequest_id, payload)


@router.get("/all", response_model=List[SidequestOut])
def r_list_sidequests_all(
    kind: Optional[str] = Query(default=None, pattern="^(core|eco_action|daily|weekly|tournament|team|chain)$"),
    status: Optional[str] = Query(default=None),
    q: Optional[str] = Query(default=None),
    tag: Optional[str] = Query(default=None),
    locality: Optional[str] = Query(default=None),
    cap: int = Query(default=5000, ge=1, le=10000),
    session: Session = Depends(session_dep),
):
    """
    Return up to `cap` sidequests (no paging). Intended for client pickers/search.
    """
    return list_sidequests_all(
        session,
        kind=kind,
        status=status,
        q=q,
        tag=tag,
        locality=locality,
        cap=cap,
    )


@router.post("/bulk", response_model=BulkUpsertResult)
def r_bulk_upsert(
    sidequests: List[dict],
    session: Session = Depends(session_dep),
    admin_email: str = Depends(require_admin),
):
    return BulkUpsertResult(**bulk_upsert(session, sidequests))


@router.post("/rotate-weekly", response_model=RotationResult)
def r_rotate_weekly(
    payload: RotationRequest,
    session: Session = Depends(session_dep),
    admin_email: str = Depends(require_admin),
):
    """
    Admin trigger to (re)assign weekly sidequests for the given ISO week.
    Non-destructive: sets rotation flags on Sidequest nodes and carries forward chains.
    """
    return rotate_weekly_sidequests(session, payload)


# -----------------------------------------------------------------------------
# Chain Context
# -----------------------------------------------------------------------------

@router.get("/{sidequest_id}/chain")
def r_get_chain_context(
    sidequest_id: str,
    session: Session = Depends(session_dep),
    uid: Optional[str] = Depends(current_user_id),
):
    """
    Return an ordered, user-aware view of a chain for the given sidequest.
    """
    rec = session.run(
        "MATCH (sq:Sidequest {id:$id}) RETURN sq.chain_id AS cid",
        {"id": sidequest_id},
    ).single()
    if not rec or not rec["cid"]:
        return {"chain_id": None, "slug": "chain", "steps": [], "current_index": 0}

    return get_chain_context(
        session,
        chain_id=rec["cid"],
        user_id=uid,
        current_sidequest_id=sidequest_id,
    )


# -----------------------------------------------------------------------------
# Public / User
# -----------------------------------------------------------------------------

@router.get("", response_model=List[SidequestOut])
def r_list_sidequests(
    kind: Optional[str] = Query(default=None, pattern="^(core|eco_action|daily|weekly|tournament|team|chain)$"),
    status: Optional[str] = Query(default=None),
    q: Optional[str] = Query(default=None),
    tag: Optional[str] = Query(default=None),
    locality: Optional[str] = Query(default=None),
    limit: int = 50,
    skip: int = 0,
    session: Session = Depends(session_dep),
):
    return list_sidequests(
        session,
        kind=kind,
        status=status,
        q=q,
        tag=tag,
        locality=locality,
        limit=limit,
        skip=skip,
    )


@router.get("/{sidequest_id}", response_model=SidequestOut)
def r_get_sidequest(sidequest_id: str, session: Session = Depends(session_dep)):
    return get_sidequest(session, sidequest_id)


@router.post("/media/upload")
async def r_upload_media(file: UploadFile = File(...)):
    """
    Upload an image and compute fingerprints (e.g., phash) server-side.
    Returns an opaque upload_id (path) and metadata.
    """
    data = await file.read()
    path, meta = save_image_and_fingerprints(
        data,
        ext_hint=f".{file.filename.split('.')[-1]}",
    )
    return {"upload_id": path, "meta": meta}


@router.post("/submissions", response_model=SubmissionOut)
def r_submit_verification(
    payload: SubmissionCreate,
    session: Session = Depends(session_dep),
    user_id: str = Depends(current_user_id),
):
    """
    Submit proof for a sidequest. Enforces 'requires_prev_approved' on chains.
    """
    # Enforce chain prerequisites (403 if previous step not approved)
    try:
        ensure_chain_prereq(session, sidequest_id=payload.sidequest_id, user_id=user_id)
    except HTTPException:
        # propagate FastAPI HTTPException from ensure_chain_prereq
        raise

    media_meta: Dict[str, Any] = {}
    if payload.image_upload_id:
        media_meta = {"path": payload.image_upload_id}

    return create_submission(session, user_id, payload, media_meta)


@router.get("/me/progress", response_model=UserProgressOut)
def r_my_progress(
    session: Session = Depends(session_dep),
    user_id: str = Depends(current_user_id),
):
    """
    Snapshot of user progress (recent approvals, cooldowns, streaks, etc).
    """
    try:
        return get_user_progress(session, user_id)
    except Exception as e:
        import logging
        logging.getLogger("api").exception("progress failed for %s", user_id)
        raise HTTPException(
            status_code=500,
            detail={"ok": False, "error": "Database error", "message": str(e).splitlines()[0]},
        )


# -----------------------------------------------------------------------------
# CSV Bulk Upload (full schema, tolerant)
# -----------------------------------------------------------------------------

def _parse_bool(v: Optional[str]) -> Optional[bool]:
    if v is None:
        return None
    s = str(v).strip().lower()
    if s in {"1", "true", "yes", "y", "t"}:
        return True
    if s in {"0", "false", "no", "n", "f"}:
        return False
    return None


def _parse_int(v: Optional[str]) -> Optional[int]:
    if v is None or str(v).strip() == "":
        return None
    try:
        return int(str(v).strip())
    except Exception:
        return None


def _parse_float(v: Optional[str]) -> Optional[float]:
    if v is None or str(v).strip() == "":
        return None
    try:
        return float(str(v).strip())
    except Exception:
        return None


def _parse_list(v: Optional[str]) -> Optional[List[str]]:
    if v is None:
        return None
    s = str(v).strip()
    if not s:
        return None
    parts = [p.strip() for p in s.replace("|", ",").split(",")]
    return [p for p in parts if p]


def _maybe_json(v: Optional[str]) -> Optional[dict]:
    if v is None:
        return None
    s = str(v).strip()
    if not s:
        return None
    try:
        obj = json.loads(s)
        return obj if isinstance(obj, dict) else None
    except Exception:
        return None


def _clean_nested(d: Dict[str, Any]) -> Dict[str, Any]:
    """Drop keys that are None/''/[]/{} so we can decide if the group is actually present."""
    return {k: v for k, v in d.items() if v not in (None, "", [], {})}


_DIFFICULTY_MAP = {
    "1": "easy", "easy": "easy", "e": "easy",
    "2": "moderate", "moderate": "moderate", "med": "moderate", "m": "moderate",
    "3": "hard", "hard": "hard", "h": "hard",
}
_IMPACT_MAP = {
    "1": "low", "low": "low", "l": "low",
    "2": "medium", "medium": "medium", "med": "medium", "m": "medium",
    "3": "high", "high": "high", "h": "high",
}
_PERIOD_MAP = {"daily": "daily", "d": "daily", "weekly": "weekly", "w": "weekly"}


def _norm_difficulty(v: Optional[str]) -> Optional[str]:
    if v is None or str(v).strip() == "":
        return None
    s = str(v).strip().lower()
    return _DIFFICULTY_MAP.get(s, s)


def _norm_impact(v: Optional[str]) -> Optional[str]:
    if v is None or str(v).strip() == "":
        return None
    s = str(v).strip().lower()
    return _IMPACT_MAP.get(s, s)


def _norm_period(v: Optional[str]) -> Optional[str]:
    if v is None or str(v).strip() == "":
        return None
    s = str(v).strip().lower()
    return _PERIOD_MAP.get(s, s)


def _row_to_sidequest_dict(row: Dict[str, str]) -> Dict[str, Any]:
    """
    Convert one CSV row into a SidequestCreate/Update-compatible dict.

    Supports either:
      - Prefixed columns like "pills.difficulty", "geo.lat", etc.
      - Or full JSON in a single column e.g. "pills", "geo", etc.

    Lists: "tags", "verification_methods" accept comma/pipe separated values.
    Empty nested groups are ignored (not included in the output), so validation won't fire.
    """
    out: Dict[str, Any] = {}

    # --- top-level simple fields ---
    out["id"] = (row.get("id") or "").strip() or None
    out["kind"] = (row.get("kind") or "").strip() or None
    out["title"] = (row.get("title") or "").strip() or None
    out["subtitle"] = (row.get("subtitle") or "").strip() or None
    out["description_md"] = row.get("description_md") or None
    out["reward_eco"] = _parse_int(row.get("reward_eco"))
    out["xp_reward"] = _parse_int(row.get("xp_reward"))
    out["max_completions_per_user"] = _parse_int(row.get("max_completions_per_user"))
    out["cooldown_days"] = _parse_int(row.get("cooldown_days"))
    out["start_at"] = (row.get("start_at") or "").strip() or None   # ISO string
    out["end_at"] = (row.get("end_at") or "").strip() or None       # ISO string
    out["status"] = (row.get("status") or "").strip() or None
    out["hero_image"] = (row.get("hero_image") or "").strip() or None
    out["card_accent"] = (row.get("card_accent") or "").strip() or None

    tags = _parse_list(row.get("tags"))
    if tags is not None:
        out["tags"] = tags

    vms = _parse_list(row.get("verification_methods"))
    if vms is not None:
        out["verification_methods"] = vms

    # --- nested helpers ---
    def prefixed(ns: str) -> Dict[str, str]:
        p = f"{ns}."
        # keep only non-empty string values
        return {k[len(p):]: v for k, v in row.items() if k.startswith(p) and str(v or "").strip() != ""}

    # pills
    pills_raw = _maybe_json(row.get("pills")) or {}
    pills_raw |= prefixed("pills")
    pills_norm = {
        "difficulty": _norm_difficulty(pills_raw.get("difficulty")),
        "impact": _norm_impact(pills_raw.get("impact")),
        "time_estimate_min": _parse_int(pills_raw.get("time_estimate_min")),
        "materials": _parse_list(pills_raw.get("materials")),
        "facts": _parse_list(pills_raw.get("facts")),
    }
    pills_final = _clean_nested(pills_norm)
    if pills_final:
        out["pills"] = pills_final

    # geo (strict: only include when BOTH lat & lon are present; special-case Anywhere)
    geo_raw = _maybe_json(row.get("geo")) or {}
    geo_raw |= prefixed("geo")
    locality_val = (geo_raw.get("locality") or "").strip()
    lat_val = _parse_float(geo_raw.get("lat"))
    lon_val = _parse_float(geo_raw.get("lon"))

    if any(k in geo_raw for k in ("lat", "lon", "radius_m", "locality")):
        if (locality_val.lower() in ("anywhere", "") and (lat_val is None or lon_val is None)):
            # user is saying Anywhere → drop geo completely
            pass
        else:
            if lat_val is None or lon_val is None:
                # hard error so it surfaces as a row error (but doesn't abort the whole job)
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="geo.lat and geo.lon are required when any geo.* is provided (unless locality is 'Anywhere')",
                )
            geo_norm = {
                "lat": lat_val,
                "lon": lon_val,
                "radius_m": _parse_int(geo_raw.get("radius_m")),
                "locality": locality_val or None,
            }
            geo_final = _clean_nested(geo_norm)
            if geo_final:
                out["geo"] = geo_final

    # streak
    streak_raw = _maybe_json(row.get("streak")) or {}
    streak_raw |= prefixed("streak")
    streak_norm = {
        "name": (streak_raw.get("name") or "").strip() or None,
        "period": _norm_period(streak_raw.get("period")),
        "bonus_eco_per_step": _parse_int(streak_raw.get("bonus_eco_per_step")),
        "max_steps": _parse_int(streak_raw.get("max_steps")),
    }
    streak_final = _clean_nested(streak_norm)
    if streak_final:
        out["streak"] = streak_final

    # rotation (only meaningful if is_weekly_slot true)
    rotation_raw = _maybe_json(row.get("rotation")) or {}
    rotation_raw |= prefixed("rotation")
    is_weekly = _parse_bool(rotation_raw.get("is_weekly_slot"))
    if is_weekly is True:
        rotation_norm = {
            "is_weekly_slot": True,
            "iso_year": _parse_int(rotation_raw.get("iso_year")),
            "iso_week": _parse_int(rotation_raw.get("iso_week")),
            "slot_index": _parse_int(rotation_raw.get("slot_index")),
            "starts_on": (rotation_raw.get("starts_on") or "").strip() or None,  # YYYY-MM-DD
            "ends_on": (rotation_raw.get("ends_on") or "").strip() or None,      # YYYY-MM-DD
        }
        rotation_final = _clean_nested(rotation_norm)
        if rotation_final:
            out["rotation"] = rotation_final

    # chain
    chain_raw = _maybe_json(row.get("chain")) or {}
    chain_raw |= prefixed("chain")
    chain_id = (chain_raw.get("chain_id") or "").strip()
    if chain_id:
        chain_norm = {
            "chain_id": chain_id,
            "chain_order": _parse_int(chain_raw.get("chain_order")),
            "requires_prev_approved": _parse_bool(chain_raw.get("requires_prev_approved")),
        }
        chain_final = _clean_nested(chain_norm)
        if chain_final:
            out["chain"] = chain_final

    # team (only if allowed true)
    team_raw = _maybe_json(row.get("team")) or {}
    team_raw |= prefixed("team")
    allowed = _parse_bool(team_raw.get("allowed"))
    if allowed is True:
        team_norm = {
            "allowed": True,
            "min_size": _parse_int(team_raw.get("min_size")),
            "max_size": _parse_int(team_raw.get("max_size")),
            "team_bonus_eco": _parse_int(team_raw.get("team_bonus_eco")),
        }
        team_final = _clean_nested(team_norm)
        if team_final:
            out["team"] = team_final

    # prune top-level empties
    return {k: v for k, v in out.items() if v is not None}


@router.post("/bulk/csv", response_model=BulkUpsertResult)
async def r_bulk_upsert_csv(
    file: UploadFile = File(...),
    session: Session = Depends(session_dep),
    admin_email: str = Depends(require_admin),
):
    """
    CSV bulk upsert for Sidequests.

    Accepts columns:
      - Top-level: id, kind, title, subtitle, description_md, reward_eco, xp_reward,
                   max_completions_per_user, cooldown_days, tags, verification_methods,
                   start_at, end_at, status, hero_image, card_accent
      - Nested via prefixes or JSON:
          pills.*      (difficulty, impact, time_estimate_min, materials, facts)
          geo.*        (lat, lon, radius_m, locality)
          streak.*     (name, period, bonus_eco_per_step, max_steps)
          rotation.*   (is_weekly_slot, iso_year, iso_week, slot_index, starts_on, ends_on)
          chain.*      (chain_id, chain_order, requires_prev_approved)
          team.*       (allowed, min_size, max_size, team_bonus_eco)

    Lists accept comma or pipe separators. Booleans accept 1/0, true/false, yes/no.
    If 'id' is present → update; else → create.
    """
    try:
        raw = await file.read()
        text = raw.decode("utf-8-sig")
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Cannot read CSV: {e}")

    try:
        reader = csv.DictReader(io.StringIO(text))
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Invalid CSV: {e}")

    rows: List[Dict[str, Any]] = []
    for i, row in enumerate(reader, start=2):  # start=2 to account for header line=1
        try:
            payload = _row_to_sidequest_dict(row)
            rows.append(payload)
        except HTTPException as e:
            rows.append({"__row_error__": f"line {i}: {type(e).__name__}: {e.detail}"})
        except Exception as e:
            nonempty_keys = [k for k, v in row.items() if str(v or '').strip() != ""]
            rows.append({"__row_error__": f"line {i}: {type(e).__name__}: {e} (keys={nonempty_keys})"})

    # Separate valid rows vs converter errors
    valid_payloads: List[Dict[str, Any]] = []
    conversion_errors: List[str] = []
    for payload in rows:
        if "__row_error__" in payload:
            conversion_errors.append(payload["__row_error__"])
        else:
            valid_payloads.append(payload)

    # Use existing JSON bulk upsert
    result = bulk_upsert(session, valid_payloads)
    if conversion_errors:
        result["errors"] = (result.get("errors") or []) + conversion_errors

    return BulkUpsertResult(**result)
