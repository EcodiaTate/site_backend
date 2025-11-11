from __future__ import annotations

from typing import List, Literal, Optional, Dict, Any
from fastapi import APIRouter, Depends, HTTPException, Body, Query
from pydantic import BaseModel
from neo4j import Session

from site_backend.core.neo_driver import session_dep

router = APIRouter(prefix="/identity", tags=["identity"])

Kind = Literal["user", "business"]


class IdentityOut(BaseModel):
    id: str                   # echo of the input id you asked for
    kind: Kind
    user_id: Optional[str] = None
    business_id: Optional[str] = None
    display_name: str
    avatar_url: Optional[str] = None
    owner_user_id: Optional[str] = None
    mutuals_subject_id: Optional[str] = None
    has_avatar: bool


# ─────────────────────────────────────────────────────────
# Name helpers (kept inline for clarity)
# ─────────────────────────────────────────────────────────

# Youth/user display name precedence:
# display_name → first_name → given_name → email localpart → right(id,6)
DISPLAY_USER = (
    "coalesce("
    "  u.display_name, "
    "  u.first_name, "
    "  u.given_name, "
    "  (CASE WHEN u.email IS NOT NULL THEN split(u.email,'@')[0] END), "
    "  right(u.id, 6)"
    ")"
)

# Business display name precedence: display_name → name → right(id,6)
DISPLAY_BIZ = "coalesce(b.display_name, b.name, right(b.id, 6))"


# ─────────────────────────────────────────────────────────
# Low-level resolvers (single id)
# ─────────────────────────────────────────────────────────

def _resolve_business(s: Session, business_id: str) -> Optional[Dict[str, Any]]:
    rec = s.run(
        f"""
        MATCH (b:BusinessProfile {{id:$bid}})
        OPTIONAL MATCH (o:User)-[:OWNS|MANAGES|REPRESENTS|STAFF_OF|WORKS_AT]->(b)
        WITH b, o
        ORDER BY coalesce(o.createdAt, 0) ASC
        WITH b, head(collect(o)) AS owner

        RETURN
          b.id                                AS business_id,
          {DISPLAY_BIZ}                       AS display_name,
          coalesce(b.avatar_url, owner.avatar_url) AS avatar_url,
          coalesce(b.owner_user_id, owner.id) AS owner_user_id
        """,
        {"bid": business_id},
    ).single()

    if not rec:
        return None

    owner_user_id = rec["owner_user_id"]
    avatar_url = rec["avatar_url"]

    return {
        "id": business_id,
        "kind": "business",
        "user_id": owner_user_id,
        "business_id": business_id,
        "display_name": rec["display_name"],
        "avatar_url": avatar_url,
        "owner_user_id": owner_user_id,
        "mutuals_subject_id": owner_user_id,
        "has_avatar": bool(avatar_url),
    }


def _resolve_user(s: Session, user_id: str, prefer_business_name_if_owner: bool = True) -> Optional[Dict[str, Any]]:
    rec = s.run(
        f"""
        MATCH (u:User {{id:$uid}})
        OPTIONAL MATCH (u)-[:OWNS|MANAGES|REPRESENTS|STAFF_OF|WORKS_AT]->(b:BusinessProfile)
        WITH u, b
        ORDER BY coalesce(b.created_at, 0) ASC
        WITH u, head(collect(b)) AS b

        WITH
          u, b,
          {DISPLAY_USER} AS user_name,
          CASE
            WHEN b IS NULL THEN NULL
            ELSE {DISPLAY_BIZ}
          END AS business_name

        RETURN
          u.id         AS user_id,
          coalesce(u.role,'') AS role,
          user_name    AS user_name,
          business_name AS business_name,
          coalesce(u.avatar_url, b.avatar_url) AS avatar_url,
          CASE WHEN b IS NULL THEN NULL ELSE b.id END AS business_id
        """,
        {"uid": user_id},
    ).single()

    if not rec:
        return None

    role = (rec["role"] or "").lower()
    biz_name = rec["business_name"]
    user_name = rec["user_name"]

    # If this user is a business actor and has a linked BusinessProfile,
    # prefer the business display name when asked for the *user* identity
    # (this makes owner accounts render as the venue name in UI lists).
    display_name = biz_name if (prefer_business_name_if_owner and role == "business" and biz_name) else user_name

    avatar_url = rec["avatar_url"]
    business_id = rec["business_id"]

    return {
        "id": user_id,
        "kind": "user",
        "user_id": user_id,
        "business_id": business_id,
        "display_name": display_name,
        "avatar_url": avatar_url,
        "owner_user_id": user_id,
        "mutuals_subject_id": user_id,
        "has_avatar": bool(avatar_url),
    }


# ─────────────────────────────────────────────────────────
# Public endpoints
# ─────────────────────────────────────────────────────────

@router.get("/{id}", response_model=IdentityOut)
def identity_single(
    id: str,
    kind: Optional[Kind] = Query(None, description="Hint what you expect: user or business"),
    s: Session = Depends(session_dep),
):
    """
    Resolve a single identity.
    - If kind=business or id matches BusinessProfile → return Business identity (name from BusinessProfile).
    - Else resolve User; if role='business' and linked BusinessProfile exists, we *prefer* the business name.
    """
    # If the caller hints business, try that path first
    if kind == "business":
        out = _resolve_business(s, id)
        if out:
            return IdentityOut(**out)
        # fall back to user if business id not found
        out_user = _resolve_user(s, id, prefer_business_name_if_owner=True)
        if out_user:
            return IdentityOut(**out_user)
        raise HTTPException(status_code=404, detail="Identity not found")

    # No hint: try business first (common for map/leaderboards), then user
    out = _resolve_business(s, id)
    if out:
        return IdentityOut(**out)

    out = _resolve_user(s, id, prefer_business_name_if_owner=True)
    if out:
        return IdentityOut(**out)

    raise HTTPException(status_code=404, detail="Identity not found")


class ResolveItemIn(BaseModel):
    id: str
    kind: Optional[Kind] = None


class ResolveBatchOut(BaseModel):
    items: List[IdentityOut]


@router.post("/resolve", response_model=ResolveBatchOut)
def identity_batch(
    payload: Dict[str, List[ResolveItemIn]] = Body(..., example={"items": [{"id": "uuid"}, {"id": "biz-id", "kind": "business"}]}),
    s: Session = Depends(session_dep),
):
    """
    Batch resolve identities. For each item:
    - Try kind hint if present; otherwise business first, then user.
    """
    items_in = payload.get("items") or []
    out: List[IdentityOut] = []

    for it in items_in:
        iid = it.id
        hint = it.kind

        rec: Optional[Dict[str, Any]] = None

        if hint == "business":
            rec = _resolve_business(s, iid) or _resolve_user(s, iid, prefer_business_name_if_owner=True)
        elif hint == "user":
            rec = _resolve_user(s, iid, prefer_business_name_if_owner=True) or _resolve_business(s, iid)
        else:
            # Unknown → business first (most of your lists use business ids), then user
            rec = _resolve_business(s, iid) or _resolve_user(s, iid, prefer_business_name_if_owner=True)

        if rec:
            out.append(IdentityOut(**rec))
        # If not found, skip or append a 404-like sentinel (skipping keeps output clean)

    return ResolveBatchOut(items=out)
