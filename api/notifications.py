from fastapi import APIRouter, Depends, HTTPException
from neo4j import Session
from site_backend.core.user_guard import current_user_id
from site_backend.core.neo_driver import session_dep
from site_backend.api.models.prefs import NotificationPrefsIn, NotificationPrefsOut

router = APIRouter(prefix="/notifications", tags=["notifications"])

@router.get("/preferences", response_model=NotificationPrefsOut)
def get_prefs(
    user_id: str = Depends(current_user_id),
    s: Session = Depends(session_dep),
):
    rec = s.run("""
        MERGE (u:User {id: $uid})
        MERGE (p:NotificationPrefs {user_id: $uid})
        MERGE (u)-[:HAS_NOTIFICATION_PREFS]->(p)
        WITH p
        RETURN p
    """, uid=user_id).single()
    if not rec:
        raise HTTPException(status_code=404, detail="Not found")
    p = rec["p"]
    return {
        "user_id": p["user_id"],
        "productNews": bool(p.get("productNews", False)),
        "announcements": bool(p.get("announcements", True)),
        "offers": bool(p.get("offers", False)),
        "securityOnly": bool(p.get("securityOnly", True)),
        "email": bool(p.get("email", True)),
        "inapp": bool(p.get("inapp", True)),
        "sms": bool(p.get("sms", False)),
    }

@router.put("/preferences", response_model=NotificationPrefsOut)
def put_prefs(
    body: NotificationPrefsIn,
    user_id: str = Depends(current_user_id),
    s: Session = Depends(session_dep),
):
    # only set provided fields
    updates = {k: v for k, v in body.dict(exclude_unset=True).items()}
    rec = s.run("""
        MERGE (u:User {id: $uid})
        MERGE (p:NotificationPrefs {user_id: $uid})
        MERGE (u)-[:HAS_NOTIFICATION_PREFS]->(p)
        SET p += $updates
        RETURN p
    """, uid=user_id, updates=updates).single()
    p = rec["p"]
    return {
        "user_id": p["user_id"],
        "productNews": bool(p.get("productNews", False)),
        "announcements": bool(p.get("announcements", True)),
        "offers": bool(p.get("offers", False)),
        "securityOnly": bool(p.get("securityOnly", True)),
        "email": bool(p.get("email", True)),
        "inapp": bool(p.get("inapp", True)),
        "sms": bool(p.get("sms", False)),
    }
