from typing import List, Optional
from fastapi import APIRouter, Depends, File, UploadFile, Query
from neo4j import Session

from .schema import (
    SidequestCreate, SidequestUpdate, SidequestOut,
    SubmissionCreate, SubmissionOut, ModerationDecision,
    UserProgressOut, RotationRequest, RotationResult, BulkUpsertResult,
)
from .service import (
    create_sidequest, update_sidequest, get_sidequest, list_sidequests,
    create_submission, moderate_submission, bulk_upsert,
    get_user_progress, rotate_weekly_sidequests,
)
from .media import save_image_and_fingerprints
from site_backend.core.neo_driver import session_dep
from site_backend.core.admin_guard import require_admin
from site_backend.core.user_guard import current_user_id

router = APIRouter(prefix="/sidequests", tags=["sidequests"])

# ---------- Admin-only ----------
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

@router.post("/bulk", response_model=BulkUpsertResult)
def r_bulk_upsert(
    sidequests: List[dict],
    session: Session = Depends(session_dep),
    admin_email: str = Depends(require_admin),
):
    return BulkUpsertResult(**bulk_upsert(session, sidequests))

@router.post("/submissions/{submission_id}/moderate", response_model=SubmissionOut)
def r_moderate_submission(
    submission_id: str,
    decision: ModerationDecision,
    session: Session = Depends(session_dep),
    admin_email: str = Depends(require_admin),  # acts as moderator_id
):
    return moderate_submission(session, submission_id, moderator_id=admin_email, decision=decision)

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

# ---------- Public / user ----------
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
    data = await file.read()
    path, meta = save_image_and_fingerprints(data, ext_hint=f".{file.filename.split('.')[-1]}")
    return {"upload_id": path, "meta": meta}

@router.post("/submissions", response_model=SubmissionOut)
def r_submit_verification(
    payload: SubmissionCreate,
    session: Session = Depends(session_dep),
    user_id: str = Depends(current_user_id),
):
    media_meta = {}
    if payload.image_upload_id:
        media_meta = {"path": payload.image_upload_id}
    return create_submission(session, user_id, payload, media_meta)
from fastapi import HTTPException

@router.get("/me/progress", response_model=UserProgressOut)
def r_my_progress(session: Session = Depends(session_dep), user_id: str = Depends(current_user_id)):
    try:
        return get_user_progress(session, user_id)
    except Exception as e:
        import logging; logging.getLogger("api").exception("progress failed for %s", user_id)
        raise HTTPException(status_code=500, detail={
            "ok": False, "error": "Database error", "message": str(e).splitlines()[0]
        })

# site_backend/api/sidequests/router.py  (append)

from fastapi import HTTPException
import csv, io, json

def _parse_bool(v: str | None) -> Optional[bool]:
    if v is None: return None
    s = str(v).strip().lower()
    if s in {"1","true","yes","y","t"}: return True
    if s in {"0","false","no","n","f"}: return False
    return None

def _parse_int(v: str | None) -> Optional[int]:
    if v is None or str(v).strip() == "": return None
    try: return int(str(v).strip())
    except Exception: return None

def _parse_float(v: str | None) -> Optional[float]:
    if v is None or str(v).strip() == "": return None
    try: return float(str(v).strip())
    except Exception: return None

def _parse_list(v: str | None) -> Optional[list[str]]:
    if v is None: return None
    s = str(v).strip()
    if not s: return None
    # Accept comma or pipe separators
    parts = [p.strip() for p in s.replace("|", ",").split(",")]
    return [p for p in parts if p]

def _maybe_json(v: str | None) -> Optional[dict]:
    if v is None: return None
    s = str(v).strip()
    if not s: return None
    try:
        obj = json.loads(s)
        return obj if isinstance(obj, dict) else None
    except Exception:
        return None

def _row_to_sidequest_dict(row: dict[str, str]) -> dict[str, any]:
    """
    Convert a CSV row (flat dict of strings) into a SidequestCreate/Update-compatible dict.
    Supports either:
      - Prefixed columns like "pills.difficulty", "geo.lat", etc.
      - Or full JSON in a single column e.g. "pills", "geo", etc.
    Lists: "tags", "verification_methods" accept comma/pipe separated values.
    Numbers/booleans are coerced where sensible.
    """
    out: dict[str, any] = {}

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
    if tags is not None: out["tags"] = tags

    vms = _parse_list(row.get("verification_methods"))
    if vms is not None: out["verification_methods"] = vms

    # --- nested helpers ---
    def prefixed(ns: str) -> dict[str, str]:
        p = f"{ns}."
        return {k[len(p):]: v for k, v in row.items() if k.startswith(p) and v is not None}

    # pills
    pills = _maybe_json(row.get("pills")) or {}
    pills |= prefixed("pills")
    if pills:
        out["pills"] = {
            "difficulty": _parse_int(pills.get("difficulty")),
            "impact": _parse_int(pills.get("impact")),
            "time_estimate_min": _parse_int(pills.get("time_estimate_min")),
            "materials": _parse_list(pills.get("materials")),
            "facts": _parse_list(pills.get("facts")),
        }

    # geo
    geo = _maybe_json(row.get("geo")) or {}
    geo |= prefixed("geo")
    if geo:
        out["geo"] = {
            "lat": _parse_float(geo.get("lat")),
            "lon": _parse_float(geo.get("lon")),
            "radius_m": _parse_int(geo.get("radius_m")),
            "locality": (geo.get("locality") or "").strip() or None,
        }

    # streak
    streak = _maybe_json(row.get("streak")) or {}
    streak |= prefixed("streak")
    if streak:
        out["streak"] = {
            "name": (streak.get("name") or "").strip() or None,
            "period": (streak.get("period") or "").strip() or None,  # "daily" | "weekly"
            "bonus_eco_per_step": _parse_int(streak.get("bonus_eco_per_step")),
            "max_steps": _parse_int(streak.get("max_steps")),
        }

    # rotation
    rotation = _maybe_json(row.get("rotation")) or {}
    rotation |= prefixed("rotation")
    if rotation:
        out["rotation"] = {
            "is_weekly_slot": _parse_bool(rotation.get("is_weekly_slot")),
            "iso_year": _parse_int(rotation.get("iso_year")),
            "iso_week": _parse_int(rotation.get("iso_week")),
            "slot_index": _parse_int(rotation.get("slot_index")),
            "starts_on": (rotation.get("starts_on") or "").strip() or None,  # YYYY-MM-DD
            "ends_on": (rotation.get("ends_on") or "").strip() or None,      # YYYY-MM-DD
        }

    # chain
    chain = _maybe_json(row.get("chain")) or {}
    chain |= prefixed("chain")
    if chain:
        out["chain"] = {
            "chain_id": (chain.get("chain_id") or "").strip() or None,
            "chain_order": _parse_int(chain.get("chain_order")),
            "requires_prev_approved": _parse_bool(chain.get("requires_prev_approved")),
        }

    # team
    team = _maybe_json(row.get("team")) or {}
    team |= prefixed("team")
    if team:
        out["team"] = {
            "allowed": _parse_bool(team.get("allowed")),
            "min_size": _parse_int(team.get("min_size")),
            "max_size": _parse_int(team.get("max_size")),
            "team_bonus_eco": _parse_int(team.get("team_bonus_eco")),
        }

    # prune Nones that would violate Pydantic optionality cleanly
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

    rows: list[dict[str, any]] = []
    for i, row in enumerate(reader, start=2):  # start=2 to account for header line=1
        try:
            rows.append(_row_to_sidequest_dict(row))
        except Exception as e:
            # Keep going, report in errors
            rows.append({"__row_error__": f"line {i}: {type(e).__name__}: {e}"})

    # Separate valid rows vs errors captured by converter
    valid_payloads: list[dict] = []
    conversion_errors: list[str] = []
    for idx, payload in enumerate(rows, start=1):
        if "__row_error__" in payload:
            conversion_errors.append(payload["__row_error__"])
        else:
            valid_payloads.append(payload)

    # Reuse existing JSON bulk upsert for the valid rows
    result = bulk_upsert(session, valid_payloads)
    if conversion_errors:
        result["errors"] = (result.get("errors") or []) + conversion_errors

    return BulkUpsertResult(**result)
