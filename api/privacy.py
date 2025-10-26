from fastapi import APIRouter, Depends
from neo4j import Session
from datetime import datetime, timezone
from site_backend.core.user_guard import current_user_id
from site_backend.core.neo_driver import session_dep
from site_backend.api.models.prefs import PrivacyPrefsIn, PrivacyPrefsOut

router = APIRouter(prefix="/privacy", tags=["privacy"])

@router.get("/settings", response_model=PrivacyPrefsOut)
def get_privacy(
    user_id: str = Depends(current_user_id),
    s: Session = Depends(session_dep),
):
    rec = s.run("""
        MERGE (u:User {id: $uid})
        MERGE (p:PrivacyPrefs {user_id: $uid})
        MERGE (u)-[:HAS_PRIVACY_PREFS]->(p)
        WITH p
        RETURN p
    """, uid=user_id).single()
    p = rec["p"]
    return {
        "user_id": p["user_id"],
        "analyticsConsent": bool(p.get("analyticsConsent", False)),
        "essentialOnly": bool(p.get("essentialOnly", False)),
        "studentTargeting": bool(p.get("studentTargeting", False)),
        "shareForResearch": bool(p.get("shareForResearch", False)),
        "lastConsentAt": p.get("lastConsentAt"),
    }

@router.put("/settings", response_model=PrivacyPrefsOut)
def put_privacy(
    body: PrivacyPrefsIn,
    user_id: str = Depends(current_user_id),
    s: Session = Depends(session_dep),
):
    updates = {k: v for k, v in body.dict(exclude_unset=True).items()}
    if any(k in updates for k in ("analyticsConsent","essentialOnly","shareForResearch")):
        updates["lastConsentAt"] = datetime.now(timezone.utc).isoformat()

    rec = s.run("""
        MERGE (u:User {id: $uid})
        MERGE (p:PrivacyPrefs {user_id: $uid})
        MERGE (u)-[:HAS_PRIVACY_PREFS]->(p)
        SET p += $updates
        RETURN p
    """, uid=user_id, updates=updates).single()
    p = rec["p"]
    return {
        "user_id": p["user_id"],
        "analyticsConsent": bool(p.get("analyticsConsent", False)),
        "essentialOnly": bool(p.get("essentialOnly", False)),
        "studentTargeting": bool(p.get("studentTargeting", False)),
        "shareForResearch": bool(p.get("shareForResearch", False)),
        "lastConsentAt": p.get("lastConsentAt"),
    }
