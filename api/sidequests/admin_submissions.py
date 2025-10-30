# site_backend/api/admin_submissions.py
from __future__ import annotations

from typing import List, Optional, Literal
from fastapi import APIRouter, Depends, Query, HTTPException, status
from pydantic import BaseModel
from neo4j import Session

# Core deps
from site_backend.core.neo_driver import session_dep
from site_backend.core.admin_guard import require_admin          # validates admin token/cookie
from site_backend.core.user_guard import current_user_id         # actor uid (the admin user)
from site_backend.core.urls import abs_media                     # absolutize /uploads/* if needed

# Domain service + schema (sidequests module)
from site_backend.api.sidequests.service import moderate_submission
from site_backend.api.sidequests.schema import ModerationDecision

router = APIRouter(prefix="/admin/submissions", tags=["Admin Submissions"])


# ───────────────────────────────────────────────────────────────────────────────
# Lite joined shapes for admin UI
# ───────────────────────────────────────────────────────────────────────────────
class SidequestLite(BaseModel):
    id: str
    title: Optional[str] = None
    subtitle: Optional[str] = None


class UserLite(BaseModel):
    id: str
    display_name: Optional[str] = None
    email: Optional[str] = None
    avatar_url: Optional[str] = None


class AdminSubmissionOut(BaseModel):
    # submission core
    id: str
    state: Literal["pending", "approved", "rejected"]
    created_at: str
    reviewed_at: Optional[str] = None
    method: Optional[str] = None
    caption: Optional[str] = None
    instagram_url: Optional[str] = None
    media_url: Optional[str] = None
    notes: Optional[str] = None
    uid: Optional[str] = None   # author
    mid: str                    # sidequest id
    # joins
    user: Optional[UserLite] = None
    sidequest: Optional[SidequestLite] = None


def _project_row(r) -> AdminSubmissionOut:
    sub = dict(r["sub"])
    # Ensure media URL is absolute (prefix PUBLIC_API_ORIGIN if relative)
    sub["media_url"] = abs_media(sub.get("media_url"))

    sq = r.get("sq") or None
    u = r.get("u") or None
    return AdminSubmissionOut(
        id=sub["id"],
        state=sub.get("state") or "pending",
        created_at=sub.get("created_at"),
        reviewed_at=sub.get("reviewed_at"),
        method=sub.get("method"),
        caption=sub.get("caption"),
        instagram_url=sub.get("instagram_url"),
        media_url=sub.get("media_url"),
        notes=sub.get("notes"),
        uid=sub.get("uid"),
        mid=sub.get("mid"),
        user=(UserLite(**u) if u else None),
        sidequest=(SidequestLite(**sq) if sq else None),
    )


# ───────────────────────────────────────────────────────────────────────────────
# GET /admin/submissions  — list (default: pending)
# Filters: state, q, sid, uid; Pagination: skip, limit
# ───────────────────────────────────────────────────────────────────────────────
@router.get("", response_model=List[AdminSubmissionOut])
def list_admin_submissions(
    s: Session = Depends(session_dep),
    _admin_email: str = Depends(require_admin),   # validates admin credentials
    state: Optional[str] = Query("pending", description='Default "pending"'),
    q: Optional[str] = Query(None),
    sid: Optional[str] = Query(None, description="Filter by sidequest id"),
    uid: Optional[str] = Query(None, description="Filter by user id"),
    skip: int = Query(0, ge=0),
    limit: int = Query(50, ge=1, le=200),
):
    # Treat blank state as “no filter”
    if state is not None and str(state).strip() == "":
        state = None

    recs = s.run(
        """
        // 1) Base match + STATE filter first (avoid filtering after optional matches)
        MATCH (sub:Submission)
        WHERE ($state IS NULL OR sub.state = $state)

        // 2) Optional joins
        OPTIONAL MATCH (sub)-[:FOR]->(sq:Sidequest)
        OPTIONAL MATCH (u:User)-[:SUBMITTED]->(sub)

        // 3) Apply remaining filters after joins
        WITH sub, sq, u
        WHERE ($sid IS NULL OR sq.id = $sid)
          AND ($uid IS NULL OR u.id = $uid)
          AND (
            $q IS NULL OR
            toLower(coalesce(sub.caption,''))    CONTAINS toLower($q) OR
            toLower(coalesce(sq.title,''))       CONTAINS toLower($q) OR
            toLower(coalesce(u.display_name,'')) CONTAINS toLower($q) OR
            toLower(coalesce(u.email,''))        CONTAINS toLower($q)
          )

        // 4) Project (normalize media URL)
        RETURN
          sub{
            .*,
            created_at: toString(sub.created_at),
            reviewed_at: CASE WHEN sub.reviewed_at IS NULL THEN NULL ELSE toString(sub.reviewed_at) END,
            mid: sq.id,
            uid: u.id,
            media_url: CASE
              WHEN sub.media_upload_id IS NOT NULL AND sub.media_upload_id <> '' THEN '/uploads/' + sub.media_upload_id
              WHEN sub.media_url IS NULL OR sub.media_url = '' THEN NULL
              WHEN sub.media_url STARTS WITH '/uploads/' THEN sub.media_url
              WHEN sub.media_url STARTS WITH 'http' THEN sub.media_url
              ELSE '/uploads/' + replace(toString(sub.media_url), '/data/uploads/', '')
            END
          } AS sub,
          sq{ .id, .title, .subtitle } AS sq,
          u{ .id, .display_name, .email, .avatar_url } AS u

        // 5) Sort & paginate
        ORDER BY sub.created_at DESC
        SKIP $skip LIMIT $limit
        """,
        {"state": state, "q": q, "sid": sid, "uid": uid, "skip": skip, "limit": limit},
    )
    return [_project_row(r) for r in recs]


# ───────────────────────────────────────────────────────────────────────────────
# GET /admin/submissions/{id} — single
# ───────────────────────────────────────────────────────────────────────────────
@router.get("/{submission_id}", response_model=AdminSubmissionOut)
def get_admin_submission(
    submission_id: str,
    s: Session = Depends(session_dep),
    _admin_email: str = Depends(require_admin),
):
    rec = s.run(
        """
        MATCH (sub:Submission {id:$sid})
        OPTIONAL MATCH (sub)-[:FOR]->(sq:Sidequest)
        OPTIONAL MATCH (u:User)-[:SUBMITTED]->(sub)
        RETURN
          sub{
            .*,
            created_at: toString(sub.created_at),
            reviewed_at: CASE WHEN sub.reviewed_at IS NULL THEN NULL ELSE toString(sub.reviewed_at) END,
            mid: sq.id,
            uid: u.id,
            media_url: CASE
              WHEN sub.media_upload_id IS NOT NULL AND sub.media_upload_id <> '' THEN '/uploads/' + sub.media_upload_id
              WHEN sub.media_url IS NULL OR sub.media_url = '' THEN NULL
              WHEN sub.media_url STARTS WITH '/uploads/' THEN sub.media_url
              WHEN sub.media_url STARTS WITH 'http' THEN sub.media_url
              ELSE '/uploads/' + replace(toString(sub.media_url), '/data/uploads/', '')
            END
          } AS sub,
          sq{ .id, .title, .subtitle } AS sq,
          u{ .id, .display_name, .email, .avatar_url } AS u
        """,
        {"sid": submission_id},
    ).single()

    if not rec:
        raise HTTPException(status_code=404, detail="Submission not found")
    return _project_row(rec)


# ───────────────────────────────────────────────────────────────────────────────
# POST /admin/submissions/{id}/moderate — approve/reject + notes
# Uses domain service (awards/badges), then re-reads joins.
# ───────────────────────────────────────────────────────────────────────────────
@router.post("/{submission_id}/moderate", response_model=AdminSubmissionOut)
def moderate_admin_submission(
    submission_id: str,
    payload: ModerationDecision,
    s: Session = Depends(session_dep),
    moderator_uid: str = Depends(current_user_id),   # actor id (must be valid user)
    _admin_email: str = Depends(require_admin),       # must be admin
):
    # Apply the decision (updates state, reviewed_at, notes, triggers awards on approval)
    _ = moderate_submission(s, submission_id=submission_id, moderator_id=moderator_uid, decision=payload)

    # Return joined view for the admin panel
    rec = s.run(
        """
        MATCH (sub:Submission {id:$sid})
        OPTIONAL MATCH (sub)-[:FOR]->(sq:Sidequest)
        OPTIONAL MATCH (u:User)-[:SUBMITTED]->(sub)
        RETURN
          sub{
            .*,
            created_at: toString(sub.created_at),
            reviewed_at: CASE WHEN sub.reviewed_at IS NULL THEN NULL ELSE toString(sub.reviewed_at) END,
            mid: sq.id,
            uid: u.id,
            media_url: CASE
              WHEN sub.media_upload_id IS NOT NULL AND sub.media_upload_id <> '' THEN '/uploads/' + sub.media_upload_id
              WHEN sub.media_url IS NULL OR sub.media_url = '' THEN NULL
              WHEN sub.media_url STARTS WITH '/uploads/' THEN sub.media_url
              WHEN sub.media_url STARTS WITH 'http' THEN sub.media_url
              ELSE '/uploads/' + replace(toString(sub.media_url), '/data/uploads/', '')
            END
          } AS sub,
          sq{ .id, .title, .subtitle } AS sq,
          u{ .id, .display_name, .email, .avatar_url } AS u
        """,
        {"sid": submission_id},
    ).single()

    if not rec:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Submission disappeared after moderation")

    return _project_row(rec)
