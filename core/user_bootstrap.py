# site_backend/core/user_bootstrap.py
from fastapi import Depends
from neo4j import Session
from .neo_driver import session_dep
from .user_guard import current_user_id

def ensure_user_exists(
    s: Session = Depends(session_dep),
    uid: str = Depends(current_user_id),
) -> str:
    s.run("""
        MERGE (u:User {id:$uid})
        ON CREATE SET u.created_at = datetime(),
                      u.prestige   = 0,
                      u.banned     = false
    """, uid=uid).consume()
    return uid  # pass uid along to route handlers
