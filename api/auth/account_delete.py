# site_backend/routers/account_delete.py
from __future__ import annotations
from datetime import datetime, timezone
from fastapi import APIRouter, Depends, HTTPException, status
from neo4j import Session

from site_backend.core.user_guard import current_user_id
from site_backend.core.neo_driver import session_dep  # <- your session dependency

router = APIRouter(prefix="/account", tags=["account-delete"])

DELETE_WAIT_DAYS = 30

def _now_ms() -> int:
    return int(datetime.now(timezone.utc).timestamp() * 1000)

def _ms_in_days(days: int) -> int:
    return days * 24 * 60 * 60 * 1000

@router.post("/delete-request")
def request_account_delete(
    user_id: str = Depends(current_user_id),
    s: Session = Depends(session_dep),
):
    """
    Schedule the authenticated user's account for deletion in 30 days.
    Idempotent: re-calling updates the schedule from 'now'.
    """
    now = _now_ms()
    after = now + _ms_in_days(DELETE_WAIT_DAYS)

    rec = s.run(
        """
        MATCH (u:User {id:$uid})
        SET u.delete_status = 'pending',
            u.delete_requested_at = $now,
            u.delete_after_ms = $after
        RETURN u.id AS id, u.delete_status AS status, u.delete_after_ms AS delete_after_ms
        """,
        uid=user_id, now=now, after=after
    ).single()

    if not rec:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="User not found")

    delete_after_ms = rec["delete_after_ms"]
    return {
        "user_id": rec["id"],
        "status": rec["status"],
        "delete_after_ms": delete_after_ms,
        "delete_after_iso": datetime.fromtimestamp(delete_after_ms / 1000, tz=timezone.utc).isoformat(),
        "wait_days": DELETE_WAIT_DAYS,
    }

@router.post("/delete-cancel")
def cancel_account_delete(
    user_id: str = Depends(current_user_id),
    s: Session = Depends(session_dep),
):
    """
    Cancel a pending deletion for the authenticated user.
    """
    rec = s.run(
        """
        MATCH (u:User {id:$uid})
        REMOVE u.delete_status, u.delete_requested_at, u.delete_after_ms
        RETURN u.id AS id
        """,
        uid=user_id
    ).single()

    if not rec:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="User not found")

    return {"user_id": rec["id"], "status": "cancelled"}

@router.get("/delete-status")
def get_delete_status(
    user_id: str = Depends(current_user_id),
    s: Session = Depends(session_dep),
):
    """
    Get current delete scheduling status for the authenticated user.
    """
    rec = s.run(
        """
        MATCH (u:User {id:$uid})
        RETURN
          u.id AS id,
          u.delete_status AS status,
          u.delete_requested_at AS requested_at_ms,
          u.delete_after_ms AS delete_after_ms
        """,
        uid=user_id
    ).single()

    if not rec:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="User not found")

    out = {
        "user_id": rec["id"],
        "status": rec["status"] or "none",
        "requested_at_ms": rec["requested_at_ms"],
        "delete_after_ms": rec["delete_after_ms"],
    }
    if rec["requested_at_ms"]:
        out["requested_at_iso"] = datetime.fromtimestamp(rec["requested_at_ms"] / 1000, tz=timezone.utc).isoformat()
    if rec["delete_after_ms"]:
        out["delete_after_iso"] = datetime.fromtimestamp(rec["delete_after_ms"] / 1000, tz=timezone.utc).isoformat()
    return out

@router.post("/_sweep-deletions")
def sweep_due_deletions(
    # Protect via Cloud Run OIDC (Cloud Scheduler) or add your own admin guard.
    s: Session = Depends(session_dep),
):
    """
    Delete all users where:
      - delete_status = 'pending'
      - delete_after_ms <= now
    Uses DETACH DELETE to remove the user node and its relationships.
    """
    now = _now_ms()
    rec = s.run(
        """
        MATCH (u:User)
        WHERE u.delete_status = 'pending' AND coalesce(u.delete_after_ms, 0) <= $now
        WITH collect(u) AS users
        CALL {
          WITH users
          UNWIND users AS u
          DETACH DELETE u
          RETURN count(*) AS c
        }
        RETURN c AS deleted_count
        """,
        now=now
    ).single()

    deleted = rec["deleted_count"] if rec and "deleted_count" in rec else 0
    return {"deleted_count": deleted, "as_of_ms": now}
