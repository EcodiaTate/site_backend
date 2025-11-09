# site_backend/api/auth/sso_login.py
from __future__ import annotations
from typing import Any
import json, os, time

from fastapi import APIRouter, Depends, HTTPException, Request, Response
from pydantic import BaseModel, EmailStr
from neo4j import Session
from jose import jwt

from site_backend.core.neo_driver import session_dep
from site_backend.core.admin_guard import ADMIN_EMAIL, JWT_SECRET as ADMIN_JWT_SECRET, JWT_ALGO as ADMIN_JWT_ALGO
from site_backend.core.cookies import set_scoped_cookie, REFRESH_COOKIE_NAME, ACCESS_COOKIE_NAME

router = APIRouter(tags=["auth"])

ACCESS_JWT_SECRET = os.getenv("ACCESS_JWT_SECRET", os.getenv("JWT_SECRET", "dev-secret-change-me"))
ACCESS_JWT_ALGO   = os.getenv("ACCESS_JWT_ALGO", "HS256")
ACCESS_JWT_TTL_S  = int(os.getenv("ACCESS_JWT_TTL_S", "900"))
ACCESS_JWT_ISS    = os.getenv("ACCESS_JWT_ISS")
ACCESS_JWT_AUD    = os.getenv("ACCESS_JWT_AUD")

REFRESH_JWT_SECRET = os.getenv("REFRESH_JWT_SECRET", os.getenv("JWT_SECRET", "dev-secret-change-me"))
REFRESH_JWT_ALGO   = os.getenv("REFRESH_JWT_ALGO", "HS256")
REFRESH_TTL_DAYS   = int(os.getenv("REFRESH_TTL_DAYS", "90"))

def _now_s() -> int:
    return int(time.time())

def _mint_access(uid: str, email: str | None = None) -> tuple[str, int]:
    now = _now_s()
    exp = now + ACCESS_JWT_TTL_S
    payload: dict[str, Any] = {"sub": uid, "iat": now, "exp": exp}
    if email: payload["email"] = email
    if ACCESS_JWT_ISS: payload["iss"] = ACCESS_JWT_ISS
    if ACCESS_JWT_AUD: payload["aud"] = ACCESS_JWT_AUD
    return jwt.encode(payload, ACCESS_JWT_SECRET, algorithm=ACCESS_JWT_ALGO), exp

def _mint_refresh(uid: str) -> str:
    now = _now_s()
    exp = now + REFRESH_TTL_DAYS * 24 * 3600
    payload = {"sub": uid, "iat": now, "exp": exp, "typ": "refresh"}
    return jwt.encode(payload, REFRESH_JWT_SECRET, algorithm=REFRESH_JWT_ALGO)

def _mint_admin_token(email: str) -> str:
    now = _now_s()
    exp = now + 60 * 60 * 12
    payload = {"sub": email, "scope": "admin", "iat": now, "exp": exp, "aud": "admin"}
    return jwt.encode(payload, ADMIN_JWT_SECRET, algorithm=ADMIN_JWT_ALGO)

class SsoLoginIn(BaseModel):
    email: EmailStr

ROLE_DEFAULT_CAPS: dict[str, dict[str, int]] = {
    "youth": {"max_redemptions_per_week": 5},
    "business": {"max_active_offers": 3},
    "creative": {"max_active_collabs": 3},
    "partner": {"max_workspaces": 2},
    "public": {},
}

def _safe_caps(caps_raw: Any, role: str) -> dict[str, Any]:
    try:
        caps = json.loads(caps_raw) if isinstance(caps_raw, str) else (caps_raw or {})
    except Exception:
        caps = {}
    if not caps:
        caps = ROLE_DEFAULT_CAPS.get(role, {})
    return caps

def _determine_role(rec: dict, email: str, current_role: str) -> str:
    # precedence: admin → business → youth → creative → partner → public
    has_bp = bool(rec.get("has_bp"))
    has_yp = bool(rec.get("has_yp"))
    has_cp = bool(rec.get("has_cp"))
    has_pp = bool(rec.get("has_pp"))

    if ADMIN_EMAIL and email == ADMIN_EMAIL.lower():
        return "admin"
    if has_bp: return "business"
    if has_yp: return "youth"
    if has_cp: return "creative"
    if has_pp: return "partner"
    return current_role or "public"

def _issue_tokens_and_build_response(u: dict, role: str, caps: dict[str, Any], request: Request, response: Response) -> dict[str, Any]:
    access, exp = _mint_access(u["id"], u.get("email"))
    refresh = _mint_refresh(u["id"])

    set_scoped_cookie(response, name=REFRESH_COOKIE_NAME, value=refresh,
                      max_age=REFRESH_TTL_DAYS * 24 * 3600, request=request)
    set_scoped_cookie(response, name=ACCESS_COOKIE_NAME, value=access,
                      max_age=ACCESS_JWT_TTL_S, request=request)

    out: dict[str, Any] = {
        "id": u["id"],
        "email": u["email"],
        "role": role,
        "caps": caps,
        "profile": {},
        "user_token": u["id"],  # legacy
        "token": access,
        "exp": exp,
    }
    if ADMIN_EMAIL and (u.get("email","").lower() == ADMIN_EMAIL.lower()):
        out["admin_token"] = _mint_admin_token(u["email"])
    return out

def _upsert_and_login(email: str, request: Request, response: Response, s: Session) -> dict[str, Any]:
    # MERGE user if missing (upsert), then read attached profiles to derive final role.
    rec = s.run("""
        MERGE (u:User {email:$email})
          ON CREATE SET
            u.id = coalesce(u.id, randomUUID()),
            u.role = coalesce(u.role, "public"),
            u.created_at = datetime()
        WITH u
        OPTIONAL MATCH (u)-[:HAS_PROFILE]->(yp:YouthProfile)
        OPTIONAL MATCH (u)-[:HAS_PROFILE]->(cp:CreativeProfile)
        OPTIONAL MATCH (u)-[:HAS_PROFILE]->(pp:PartnerProfile)
        OPTIONAL MATCH (u)-[:HAS_PROFILE]->(bp:BusinessProfile)
        OPTIONAL MATCH (u)-[:OWNS|MANAGES]->(bOwned:BusinessProfile)
        RETURN u,
               yp IS NOT NULL AS has_yp,
               cp IS NOT NULL AS has_cp,
               pp IS NOT NULL AS has_pp,
               (bp IS NOT NULL OR bOwned IS NOT NULL) AS has_bp
    """, email=email).single()

    if not rec or not rec.get("u"):
        # Shouldn't happen, but be explicit.
        raise HTTPException(status_code=500, detail="SSO upsert failed")

    u = rec["u"]
    current_role = (u.get("role") or "public").lower()
    final_role = _determine_role(rec, email, current_role)

    if final_role != current_role:
        s.run("MATCH (u:User {id:$id}) SET u.role=$role", id=u["id"], role=final_role)

    caps = _safe_caps(u.get("caps_json") or "{}", final_role)
    return _issue_tokens_and_build_response(u, final_role, caps, request, response)

@router.post("/sso-login")
def sso_login(p: SsoLoginIn, request: Request, response: Response, s: Session = Depends(session_dep)):
    email = p.email.lower()
    # Upsert + login (always 200 on success)
    return _upsert_and_login(email, request, response, s)

# Optional alias: some FE builds still try /auth/sso-register on 404 from old flows.
# Keep it for compatibility; it does the exact same thing.
@router.post("/sso-register")
def sso_register(p: SsoLoginIn, request: Request, response: Response, s: Session = Depends(session_dep)):
    email = p.email.lower()
    return _upsert_and_login(email, request, response, s)
