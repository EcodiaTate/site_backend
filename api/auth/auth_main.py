# site_backend/api/auth/auth_routes.py  (example path for your big file)
from __future__ import annotations
from uuid import uuid4
from typing import Any, Optional
from fastapi import APIRouter, Depends, HTTPException, Response, status
from pydantic import BaseModel, EmailStr, Field
from argon2 import PasswordHasher
from neo4j import Session
import os, time, json
from jose import jwt
from neo4j.exceptions import ConstraintError

from site_backend.core.neo_driver import session_dep
from site_backend.core.cookies import set_scoped_cookie, REFRESH_COOKIE_NAME, ADMIN_COOKIE_NAME
from site_backend.core.user_guard import current_user_id

router = APIRouter()
ph = PasswordHasher()

JWT_SECRET = os.getenv("JWT_SECRET", "dev-secret-change-me")
JWT_ALGO = "HS256"
ADMIN_EMAIL = (os.getenv("ADMIN_EMAIL") or "tate@ecodia.au").lower()

ACCESS_JWT_SECRET = os.getenv("ACCESS_JWT_SECRET", JWT_SECRET)
ACCESS_JWT_ALGO   = os.getenv("ACCESS_JWT_ALGO", "HS256")
ACCESS_JWT_TTL_S  = int(os.getenv("ACCESS_JWT_TTL_S", "900"))
ACCESS_JWT_ISS    = os.getenv("ACCESS_JWT_ISS")
ACCESS_JWT_AUD    = os.getenv("ACCESS_JWT_AUD")

REFRESH_JWT_SECRET  = os.getenv("REFRESH_JWT_SECRET", JWT_SECRET)
REFRESH_JWT_ALGO    = os.getenv("REFRESH_JWT_ALGO", "HS256")
REFRESH_TTL_DAYS    = int(os.getenv("REFRESH_TTL_DAYS", "90"))

def _now_s() -> int: return int(time.time())

def _mint_access(uid: str, email: Optional[str] = None) -> tuple[str, int]:
    now = _now_s()
    exp = now + ACCESS_JWT_TTL_S
    payload = {"sub": uid, "iat": now, "exp": exp}
    if email: payload["email"] = email
    if ACCESS_JWT_ISS: payload["iss"] = ACCESS_JWT_ISS
    if ACCESS_JWT_AUD: payload["aud"] = ACCESS_JWT_AUD
    return jwt.encode(payload, ACCESS_JWT_SECRET, algorithm=ACCESS_JWT_ALGO), exp

def _mint_refresh(uid: str) -> str:
    now = _now_s()
    exp = now + REFRESH_TTL_DAYS * 24 * 3600
    payload = {"sub": uid, "iat": now, "exp": exp, "typ": "refresh"}
    return jwt.encode(payload, REFRESH_JWT_SECRET, algorithm=REFRESH_JWT_ALGO)

def _safe_caps(caps_raw: Any, role: str) -> dict[str, Any]:
    try:
        caps = json.loads(caps_raw) if isinstance(caps_raw, str) else (caps_raw or {})
    except Exception:
        caps = {}
    if not caps:
        caps = ROLE_DEFAULT_CAPS.get(role, {})
    return caps

def is_admin_email(email: str) -> bool:
    if not email: return False
    e = email.lower()
    return e == ADMIN_EMAIL or e.endswith("@ecodia.au")

def mint_admin_token(email: str, ttl_secs: int = 6 * 60 * 60) -> str:
    now = _now_s()
    payload = {"sub": email, "scope": "admin", "iat": now, "exp": now + ttl_secs}
    return jwt.encode(payload, JWT_SECRET, algorithm=JWT_ALGO)

def get_user_by_id(session: Session, uid: str) -> Optional[dict]:
    rec = session.run("MATCH (u:User {id:$id}) RETURN u.email AS email", {"id": uid}).single()
    if not rec: return None
    return {"email": rec["email"]}

ROLE_DEFAULT_CAPS: dict[str, dict[str, Any]] = {
    "youth": {"max_redemptions_per_week": 5, "streak_bonus_enabled": True, "max_active_sidequests": 7},
    "business": {"max_active_offers": 3, "can_issue_qr": True, "analytics_enabled": True},
    "creative": {"max_active_collabs": 3, "portfolio_required": False},
    "partner": {"max_workspaces": 2, "can_host_events": True},
    "public": {},
}

class LoginIn(BaseModel):
    email: EmailStr
    password: str

class UserOut(BaseModel):
    id: str
    email: EmailStr
    role: str
    caps: dict[str, Any]
    profile: dict[str, Any] = {}

# ... join endpoints unchanged ...

@router.post("/login")
def login(p: LoginIn, response: Response, s: Session = Depends(session_dep)):
    cypher = """
    MATCH (u:User {email:$email})
    OPTIONAL MATCH (u)-[:HAS_PROFILE]->(yp:YouthProfile)
    OPTIONAL MATCH (u)-[:HAS_PROFILE]->(bp:BusinessProfile)
    OPTIONAL MATCH (u)-[:HAS_PROFILE]->(cp:CreativeProfile)
    OPTIONAL MATCH (u)-[:HAS_PROFILE]->(pp:PartnerProfile)
    OPTIONAL MATCH (u)-[:HAS_PROFILE]->(pub:PublicProfile)
    RETURN u, yp, bp, cp, pp, pub
    """
    rec = s.run(cypher, email=p.email.lower()).single()
    if not rec or not rec.get("u"):
        raise HTTPException(status_code=401, detail="Invalid credentials")

    u = rec["u"]
    from argon2.exceptions import VerifyMismatchError
    from argon2 import PasswordHasher
    ph = PasswordHasher()
    try:
        ph.verify(u["password_hash"], p.password)
    except VerifyMismatchError:
        raise HTTPException(status_code=401, detail="Invalid credentials")

    role = (u.get("role") or "public").lower()
    profile = {}
    if role == "youth" and rec.get("yp"): profile = dict(rec["yp"])
    elif role == "business" and rec.get("bp"): profile = dict(rec["bp"])
    elif role == "creative" and rec.get("cp"): profile = dict(rec["cp"])
    elif role == "partner" and rec.get("pp"): profile = dict(rec["pp"])
    elif role == "public" and rec.get("pub"): profile = dict(rec["pub"])

    caps = _safe_caps(u.get("caps_json") or "{}", role)

    access, exp = _mint_access(u["id"], u["email"])
    refresh = _mint_refresh(u["id"])
    # Set refresh cookie with shared helper
    set_scoped_cookie(response, name=REFRESH_COOKIE_NAME, value=refresh, max_age=REFRESH_TTL_DAYS * 24 * 3600)

    resp = {
        "id": u["id"],
        "email": u["email"],
        "role": role,
        "caps": caps,
        "profile": profile,
        "user_token": u["id"],  # legacy, slated for deprecation
        "token": access,
        "exp": exp,
    }

    if ADMIN_EMAIL and u["email"].lower() == ADMIN_EMAIL:
        now = _now_s()
        admin_payload = {"sub": u["email"], "scope": "admin", "iat": now, "exp": now + 60*60*12, "aud": "admin"}
        resp["admin_token"] = jwt.encode(admin_payload, JWT_SECRET, algorithm=JWT_ALGO)

    return resp

@router.post("/admin-cookie")
def r_admin_cookie_here(
    response: Response,
    s: Session = Depends(session_dep),
    uid: str = Depends(current_user_id),
):
    # If you keep this duplicate, ensure it matches api/auth/admin_cookie.py OR remove this one.
    user = get_user_by_id(s, uid)
    email = (user or {}).get("email") or ""
    if not is_admin_email(email):
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Not an admin")

    token = mint_admin_token(email, ttl_secs=6*60*60)
    set_scoped_cookie(response, name=ADMIN_COOKIE_NAME, value=token, max_age=6*60*60)
    return {"ok": True}
