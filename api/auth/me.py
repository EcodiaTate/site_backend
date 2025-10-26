from fastapi import APIRouter, Depends
from neo4j import Session
from site_backend.core.neo_driver import session_dep
from site_backend.core.user_guard import current_user_id

router = APIRouter(tags=["auth"])

@router.get("/me")
def read_me(uid: str = Depends(current_user_id), s: Session = Depends(session_dep)):
    rec = s.run(
        "MATCH (u:User {id:$uid}) RETURN u.id AS id, toLower(coalesce(u.role,'')) AS role, toLower(coalesce(u.email,'')) AS email",
        uid=uid,
    ).single()
    if not rec:
        return {"id": uid, "role": "", "email": ""}
    return {"id": rec["id"], "role": rec["role"], "email": rec["email"]}
