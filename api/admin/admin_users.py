from __future__ import annotations

from typing import Any, Dict, List, Literal, Optional
from datetime import datetime, timezone
import json

from fastapi import APIRouter, Depends, HTTPException, Query, Body, status, Header
from neo4j import Session
from neo4j.time import DateTime as NeoDateTime
from pydantic import BaseModel, Field, RootModel

from site_backend.core.neo_driver import session_dep
from site_backend.core.admin_guard import _decode_token  # or your own admin dependency

router = APIRouter(prefix="/admin", tags=["admin:users"])

# ─────────────────────────────────────────────────────────
# Admin guard (accepts either Bearer or X-Auth-Token)
# ─────────────────────────────────────────────────────────
def current_admin(
    authorization: Optional[str] = Header(default=None),
    x_auth_token: Optional[str] = Header(default=None, alias="X-Auth-Token"),
) -> dict:
    token = None
    if authorization and authorization.lower().startswith("bearer "):
        token = authorization.split(" ", 1)[1].strip()
    elif x_auth_token:
        token = x_auth_token.strip()
    if not token:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Missing admin token")

    try:
        claims = _decode_token(token)
    except Exception:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token")

    aud = claims.get("aud")
    if aud and aud != "admin":
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Not an admin token")

    return claims


# ─────────────────────────────────────────────────────────
# Helpers: normalize Neo4j values to API model types
# ─────────────────────────────────────────────────────────
def _iso(v: Any) -> Optional[str]:
    if v is None:
        return None
    try:
        if isinstance(v, NeoDateTime):
            # neo4j.time.DateTime → ISO string with Z
            return v.isoformat().replace("+00:00", "Z")
        if isinstance(v, datetime):
            if v.tzinfo is None:
                v = v.replace(tzinfo=timezone.utc)
            return v.isoformat().replace("+00:00", "Z")
        if isinstance(v, (int, float)):
            # support seconds or ms
            s = v / 1000.0 if v > 1e12 else float(v)
            return datetime.fromtimestamp(s, tz=timezone.utc).isoformat().replace("+00:00", "Z")
        # assume already stringy
        return str(v)
    except Exception:
        return str(v)

def _caps_to_dict(val: Any) -> Optional[dict]:
    if val is None or val == "":
        return None
    if isinstance(val, dict):
        return val
    if isinstance(val, (bytes, bytearray)):
        try:
            return json.loads(val.decode("utf-8"))
        except Exception:
            return None
    if isinstance(val, str):
        # Some nodes store JSON as a string
        try:
            return json.loads(val)
        except Exception:
            return None
    return None

def serialize_user(u: Dict[str, Any] | None) -> Dict[str, Any] | None:
    if not u:
        return None
    out = dict(u)
    out["created_at"] = _iso(out.get("created_at"))
    out["updated_at"] = _iso(out.get("updated_at"))
    out["caps_json"] = _caps_to_dict(out.get("caps_json"))
    # keep avatar_updated_at as int if possible
    try:
        if out.get("avatar_updated_at") is not None:
            out["avatar_updated_at"] = int(out["avatar_updated_at"])
    except Exception:
        pass
    return out

def serialize_bp(bp: Dict[str, Any] | None) -> Dict[str, Any] | None:
    if not bp:
        return None
    out = dict(bp)
    out["created_at"] = _iso(out.get("created_at"))
    # onboarding_completed_at looks like ms epoch in your sample; keep as int if it is
    try:
        if out.get("onboarding_completed_at") is not None:
            out["onboarding_completed_at"] = int(out["onboarding_completed_at"])
    except Exception:
        out["onboarding_completed_at"] = None
    return out


# ─────────────────────────────────────────────────────────
# Models
# ─────────────────────────────────────────────────────────
class UserOut(BaseModel):
    id: str
    email: str
    role: Optional[str] = None
    display_name: Optional[str] = None
    created_at: Optional[str] = None
    updated_at: Optional[str] = None
    email_verified: Optional[bool] = None
    caps_json: Optional[dict] = None
    avatar_url: Optional[str] = None
    avatar_sha: Optional[str] = None
    avatar_rev: Optional[str] = None
    avatar_updated_at: Optional[int] = None

class BusinessProfileOut(BaseModel):
    id: str
    user_id: Optional[str] = None
    name: Optional[str] = None
    tagline: Optional[str] = None
    address: Optional[str] = None
    area: Optional[str] = None
    industry_group: Optional[str] = None
    size: Optional[str] = None
    lat: Optional[float] = None
    lng: Optional[float] = None
    latest_unit_amount_aud: Optional[int] = None
    pledge_tier: Optional[str] = None
    subscription_status: Optional[str] = None
    visible_on_map: Optional[bool] = None
    website: Optional[str] = None
    created_at: Optional[str] = None
    onboarding_completed: Optional[bool] = None
    onboarding_completed_at: Optional[int] = None
    rules_cooldown_hours: Optional[int] = None
    rules_daily_cap_per_user: Optional[int] = None
    rules_geofence_radius_m: Optional[int] = None
    hero_url: Optional[str] = None
    hero_sha: Optional[str] = None
    hero_rev: Optional[str] = None
    hours: Optional[str] = None
    standards_eco: Optional[str] = None
    standards_social: Optional[str] = None
    standards_sustainability: Optional[str] = None
    area_type: Optional[str] = Field(default=None, alias="area")

class AdminUserRecord(BaseModel):
    user: UserOut
    business: Optional[BusinessProfileOut] = None


# Whitelists for patches
ALLOWED_USER_FIELDS = {
    "display_name",
    "role",
    "email_verified",
    "caps_json",
    "avatar_url",
    "avatar_sha",
    "avatar_rev",
    "avatar_updated_at",
}
ALLOWED_BUSINESS_FIELDS = {
    "name",
    "tagline",
    "address",
    "area",
    "industry_group",
    "size",
    "lat",
    "lng",
    "latest_unit_amount_aud",
    "pledge_tier",
    "subscription_status",
    "visible_on_map",
    "website",
    "onboarding_completed",
    "onboarding_completed_at",
    "rules_cooldown_hours",
    "rules_daily_cap_per_user",
    "rules_geofence_radius_m",
    "hero_url",
    "hero_sha",
    "hero_rev",
    "hours",
    "standards_eco",
    "standards_social",
    "standards_sustainability",
}

def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

# Pydantic v2 RootModel payloads
class UserPatch(RootModel[Dict[str, Any]]):
    pass

class BusinessPatch(RootModel[Dict[str, Any]]):
    pass


# ─────────────────────────────────────────────────────────
# GET /admin/users : list
# ─────────────────────────────────────────────────────────
@router.get("/users")
def list_users(
    q: Optional[str] = Query(default=None, description="Search email or display_name"),
    role: Optional[str] = Query(default=None),
    email_verified: Optional[bool] = Query(default=None),
    has_business: Optional[bool] = Query(default=None),
    pledge_tier: Optional[str] = Query(default=None, description="Filter by business pledge tier"),
    subscription_status: Optional[str] = Query(default=None, description="Filter by business subscription"),
    sort: Literal["created_at", "updated_at", "email", "role", "business_name"] = Query(default="created_at"),
    order: Literal["asc", "desc"] = Query(default="desc"),
    limit: int = Query(default=25, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
    _: dict = Depends(current_admin),
    db: Session = Depends(session_dep),
):
    params: Dict[str, Any] = {"limit": limit, "offset": offset}
    predicates: List[str] = []

    if q:
        predicates.append("(toLower(u.email) CONTAINS toLower($q) OR toLower(u.display_name) CONTAINS toLower($q))")
        params["q"] = q
    if role:
        predicates.append("u.role = $role")
        params["role"] = role
    if email_verified is not None:
        predicates.append("coalesce(u.email_verified, false) = $email_verified")
        params["email_verified"] = email_verified
    if has_business is True:
        predicates.append("bp IS NOT NULL")
    elif has_business is False:
        predicates.append("bp IS NULL")
    if pledge_tier:
        predicates.append("bp.pledge_tier = $pledge_tier")
        params["pledge_tier"] = pledge_tier
    if subscription_status:
        predicates.append("bp.subscription_status = $subscription_status")
        params["subscription_status"] = subscription_status

    where_sql = f"WHERE {' AND '.join(predicates)}" if predicates else ""

    sort_field_map = {
        "created_at": "u.created_at",
        "updated_at": "u.updated_at",
        "email": "u.email",
        "role": "u.role",
        "business_name": "bp.name",
    }
    order_sql = "ASC" if order == "asc" else "DESC"
    sort_sql = f"ORDER BY {sort_field_map.get(sort, 'u.created_at')} {order_sql}"

    cypher = f"""
    MATCH (u:User)
    OPTIONAL MATCH (u)-[:OWNS]->(bp:BusinessProfile)
    WITH u, bp
    {where_sql}
    {sort_sql}
    SKIP $offset
    LIMIT $limit
    RETURN u{{.*}} AS u, bp{{.*}} AS bp
    """

    items: List[Dict[str, Any]] = []
    with db.begin_transaction() as tx:
        res = tx.run(cypher, **params)
        for row in res:
            u = serialize_user(row["u"] or {})
            bp = serialize_bp(row["bp"] or None)
            items.append({"user": u, "business": bp})

    return {
        "items": items,
        "limit": limit,
        "offset": offset,
        "count": len(items),
    }


# ─────────────────────────────────────────────────────────
# GET /admin/users/{user_id}
# ─────────────────────────────────────────────────────────
@router.get("/users/{user_id}", response_model=AdminUserRecord)
def get_user(
    user_id: str,
    _: dict = Depends(current_admin),
    db: Session = Depends(session_dep),
):
    cypher = """
    MATCH (u:User {id: $user_id})
    OPTIONAL MATCH (u)-[:OWNS]->(bp:BusinessProfile)
    RETURN u{.*} AS u, bp{.*} AS bp
    """
    with db.begin_transaction() as tx:
        rec = tx.run(cypher, user_id=user_id).single()
        if not rec:
            raise HTTPException(status_code=404, detail="User not found")
        return {"user": serialize_user(rec["u"]), "business": serialize_bp(rec["bp"])}


# ─────────────────────────────────────────────────────────
# PATCH /admin/users/{user_id}
# ─────────────────────────────────────────────────────────
@router.patch("/users/{user_id}", response_model=UserOut)
def patch_user(
    user_id: str,
    patch: UserPatch = Body(...),
    _: dict = Depends(current_admin),
    db: Session = Depends(session_dep),
):
    payload = patch.root
    updates = {k: v for k, v in payload.items() if k in ALLOWED_USER_FIELDS}
    if not updates:
        raise HTTPException(status_code=400, detail="No allowed fields to update")

    updates["updated_at"] = _now_iso()
    set_lines = [f"u.{k} = ${k}" for k in updates.keys()]
    params = {"user_id": user_id, **updates}

    cypher = f"""
    MATCH (u:User {{id: $user_id}})
    SET {", ".join(set_lines)}
    RETURN u{{.*}} AS u
    """

    with db.begin_transaction() as tx:
        rec = tx.run(cypher, **params).single()
        if not rec:
            raise HTTPException(status_code=404, detail="User not found")
        return serialize_user(rec["u"])  # ← normalize before returning


# ─────────────────────────────────────────────────────────
# PATCH /admin/business-profiles/{bp_id}
# ─────────────────────────────────────────────────────────
@router.patch("/business-profiles/{bp_id}", response_model=BusinessProfileOut)
def patch_business_profile(
    bp_id: str,
    patch: BusinessPatch = Body(...),
    _: dict = Depends(current_admin),
    db: Session = Depends(session_dep),
):
    payload = patch.root
    updates = {k: v for k, v in payload.items() if k in ALLOWED_BUSINESS_FIELDS}
    if not updates:
        raise HTTPException(status_code=400, detail="No allowed fields to update")

    set_lines = [f"bp.{k} = ${k}" for k in updates.keys()]
    params = {"bp_id": bp_id, **updates}

    cypher = f"""
    MATCH (bp:BusinessProfile {{id: $bp_id}})
    SET {", ".join(set_lines)}
    RETURN bp{{.*}} AS bp
    """

    with db.begin_transaction() as tx:
        rec = tx.run(cypher, **params).single()
        if not rec:
            raise HTTPException(status_code=404, detail="BusinessProfile not found")
        return serialize_bp(rec["bp"])  # ← normalize
