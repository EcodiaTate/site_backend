# site_backend/api/missions/routers.py
from typing import List, Optional
from fastapi import APIRouter, Depends, File, UploadFile, Query
from neo4j import Session

from .schema import (
    MissionCreate, MissionUpdate, MissionOut,
    SubmissionCreate, SubmissionOut, ModerationDecision
)
from .service import (
    create_mission, update_mission, get_mission, list_missions,
    create_submission, moderate_submission, bulk_upsert
)
from .media import save_image_and_fingerprints
from site_backend.core.neo_driver import session_dep
from site_backend.core.admin_guard import require_admin
from site_backend.core.user_guard import current_user_id

router = APIRouter(prefix="/missions", tags=["missions"])

# ---------- Admin-only ----------
@router.post("", response_model=MissionOut)
def r_create_mission(
    payload: MissionCreate,
    session: Session = Depends(session_dep),
    admin_email: str = Depends(require_admin),
):
    return create_mission(session, payload)

@router.patch("/{mission_id}", response_model=MissionOut)
def r_update_mission(
    mission_id: str,
    payload: MissionUpdate,
    session: Session = Depends(session_dep),
    admin_email: str = Depends(require_admin),
):
    return update_mission(session, mission_id, payload)

@router.post("/bulk", response_model=dict)
def r_bulk_upsert(
    missions: List[dict],
    session: Session = Depends(session_dep),
    admin_email: str = Depends(require_admin),
):
    return bulk_upsert(session, missions)

@router.post("/submissions/{submission_id}/moderate", response_model=SubmissionOut)
def r_moderate_submission(
    submission_id: str,
    decision: ModerationDecision,
    session: Session = Depends(session_dep),
    admin_email: str = Depends(require_admin),  # will serve as moderator_id
):
    return moderate_submission(session, submission_id, moderator_id=admin_email, decision=decision)

# ---------- Public / user ----------
@router.get("", response_model=List[MissionOut])
def r_list_missions(
    type: Optional[str] = Query(default=None, pattern="^(sidequest|eco_action)$"),
    status: Optional[str] = Query(default=None),
    q: Optional[str] = Query(default=None),
    limit: int = 50,
    skip: int = 0,
    session: Session = Depends(session_dep),
):
    return list_missions(session, mtype=type, status=status, q=q, limit=limit, skip=skip)

@router.get("/{mission_id}", response_model=MissionOut)
def r_get_mission(mission_id: str, session: Session = Depends(session_dep)):
    return get_mission(session, mission_id)

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
