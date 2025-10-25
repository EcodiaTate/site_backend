from __future__ import annotations
from typing import Any

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, EmailStr
from neo4j import Session
import json
from jose import jwt

from site_backend.core.neo_driver import session_dep
from site_backend.core.admin_guard import ADMIN_EMAIL, JWT_SECRET as ADMIN_JWT_SECRET, JWT_ALGO as ADMIN_JWT_ALGO

router = APIRouter(tags=["auth"])

# If you want the same admin token format as your /auth/login flow:
def _mint_admin_token(email: str) -> str:
    import time
    now = int(time.time())
    exp = now + 60 * 60 * 12  # 12h
    payload = {"sub": email, "scope": "admin", "iat": now, "exp": exp, "aud": "admin"}
    return jwt.encode(payload, ADMIN_JWT_SECRET, algorithm=ADMIN_JWT_ALGO)

class SsoLoginIn(BaseModel):
    email: EmailStr

# Centralized role→caps defaults
ROLE_DEFAULT_CAPS: dict[str, dict[str, int]] = {
    "youth": {"max_redemptions_per_week": 5},
    "business": {"max_active_offers": 3},
    "creative": {"max_active_collabs": 3},
    "partner": {"max_workspaces": 2},
    "public": {},
}

# Which profile label to read for each role
ROLE_PROFILE_LABEL = {
    "youth": "YouthProfile",
    "business": "BusinessProfile",
    "creative": "CreativeProfile",
    "partner": "PartnerProfile",
    "public": "PublicProfile",
}

@router.post("/sso-login")
def sso_login(p: SsoLoginIn, s: Session = Depends(session_dep)):
    """
    Called by NextAuth after Google login (email verified by Google).
    Upsert user by email, return app-specific fields for the NextAuth JWT/session.
    """
    email = p.email.lower()

    cy = """
    MERGE (u:User {email:$email})
      ON CREATE SET
        u.id = coalesce(u.id, randomUUID()),
        u.role = coalesce(u.role, "public"),   // default neutral role
        u.created_at = datetime()
    WITH u
    OPTIONAL MATCH (u)-[:HAS_PROFILE]->(yp:YouthProfile)
    OPTIONAL MATCH (u)-[:HAS_PROFILE]->(bp:BusinessProfile)
    OPTIONAL MATCH (u)-[:HAS_PROFILE]->(cp:CreativeProfile)
    OPTIONAL MATCH (u)-[:HAS_PROFILE]->(pp:PartnerProfile)
    OPTIONAL MATCH (u)-[:HAS_PROFILE]->(pub:PublicProfile)
    RETURN u, yp, bp, cp, pp, pub
    """
    rec = s.run(cy, email=email).single()
    if not rec:
        raise HTTPException(status_code=500, detail="Upsert failed")

    u = rec["u"]
    role = (u.get("role") or "public").lower()
    caps_raw = u.get("caps_json") or "{}"
    try:
        caps = json.loads(caps_raw) if isinstance(caps_raw, str) else (caps_raw or {})
    except Exception:
        caps = {}

    # Sensible defaults if caps_json is missing
    if not caps:
        caps = ROLE_DEFAULT_CAPS.get(role, {})

    # Minimal profile passthrough (optional)
    profile: dict[str, Any] = {}
    if role == "business" and rec.get("bp"):
        profile = dict(rec["bp"])
    elif role == "youth" and rec.get("yp"):
        profile = dict(rec["yp"])
    elif role == "creative" and rec.get("cp"):
        profile = dict(rec["cp"])
    elif role == "partner" and rec.get("pp"):
        profile = dict(rec["pp"])
    elif role == "public" and rec.get("pub"):
        profile = dict(rec["pub"])

    out: dict[str, Any] = {
        "id": u["id"],
        "email": u["email"],
        "role": role,
        "caps": caps,
        "profile": profile,
        "user_token": u["id"],  # your SPA uses this as a bearer for dev APIs
    }

    if ADMIN_EMAIL and email == ADMIN_EMAIL:
        out["admin_token"] = _mint_admin_token(email)

    return out
