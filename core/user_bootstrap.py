# site_backend/core/user_bootstrap.py
from __future__ import annotations

from typing import Optional
from fastapi import Depends
from neo4j import Session

from .neo_driver import session_dep
from .user_guard import maybe_current_user_id

def ensure_user_exists(
    s: Session = Depends(session_dep),
    uid: Optional[str] = Depends(maybe_current_user_id),
) -> Optional[str]:
    """
    Best-effort bootstrap:
    - If a user id is present (Bearer / allowed legacy cookie), MERGE the User node.
    - If anonymous, do nothing (keeps public endpoints public).
    Returns the uid (or None) so routers can still inject it if desired.
    """
    if uid:
        s.run(
            """
            MERGE (u:User {id:$uid})
            ON CREATE SET u.created_at = datetime(),
                          u.prestige   = 0,
                          u.banned     = false
            """,
            uid=uid,
        ).consume()
    return uid
