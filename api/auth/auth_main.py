# site_backend/api/auth/auth_routes.py
from __future__ import annotations

from uuid import uuid4
from typing import Any, Optional

from fastapi import APIRouter, Depends, HTTPException, Response, status, Request
from pydantic import BaseModel, EmailStr, Field
from argon2 import PasswordHasher
from argon2.exceptions import VerifyMismatchError
from neo4j import Session
from neo4j.exceptions import ConstraintError
from jose import jwt
import os
import time
import json
import datetime as _dt

from site_backend.core.neo_driver import session_dep
from site_backend.core.cookies import (
    set_scoped_cookie,
    REFRESH_COOKIE_NAME,
    ADMIN_COOKIE_NAME,
    ACCESS_COOKIE_NAME,
)
from site_backend.core.user_guard import current_user_id

router = APIRouter()
ph = PasswordHasher()

JWT_SECRET = os.getenv("JWT_SECRET", "dev-secret-change-me")
JWT_ALGO = "HS256"
ADMIN_EMAIL = (os.getenv("ADMIN_EMAIL") or "tate@ecodia.au").lower()

ACCESS_JWT_SECRET = os.getenv("ACCESS_JWT_SECRET", JWT_SECRET)
ACCESS_JWT_ALGO = os.getenv("ACCESS_JWT_ALGO", "HS256")
ACCESS_JWT_TTL_S = int(os.getenv("ACCESS_JWT_TTL_S", "900"))
ACCESS_JWT_ISS = os.getenv("ACCESS_JWT_ISS")
ACCESS_JWT_AUD = os.getenv("ACCESS_JWT_AUD")

REFRESH_JWT_SECRET = os.getenv("REFRESH_JWT_SECRET", JWT_SECRET)
REFRESH_JWT_ALGO = os.getenv("REFRESH_JWT_ALGO", "HS256")
REFRESH_TTL_DAYS = int(os.getenv("REFRESH_TTL_DAYS", "90"))

DEFAULT_TOS_VERSION = os.getenv("DEFAULT_TOS_VERSION", "v1")


# ── Signup payloads ────────────────────────────────────────────────────────


class YouthSignupIn(BaseModel):
    email: EmailStr
    password: str
    birth_year: int


class CreativeSignupIn(BaseModel):
    email: EmailStr
    password: str
    display_name: str
    portfolio_url: str | None = None


class PartnerSignupIn(BaseModel):
    email: EmailStr
    password: str
    org_name: str
    org_type: str | None = None


class PublicSignupIn(BaseModel):
    email: EmailStr
    password: str
    display_name: str | None = None


class MinimalSignupIn(BaseModel):
    email: EmailStr
    password: str
    # Pydantic v2: use `pattern` instead of `regex`
    role: str = Field(pattern="^(youth|business|creative|partner|public)$")


# ── Role caps ──────────────────────────────────────────────────────────────


ROLE_DEFAULT_CAPS: dict[str, dict[str, Any]] = {
    "youth": {
        "max_redemptions_per_week": 5,
        "streak_bonus_enabled": True,
        "max_active_sidequests": 7,
    },
    "business": {
        "max_active_offers": 3,
        "can_issue_qr": True,
        "analytics_enabled": True,
    },
    "creative": {
        "max_active_collabs": 3,
        "portfolio_required": False,
    },
    "partner": {
        "max_workspaces": 2,
        "can_host_events": True,
    },
    "public": {},
}


# ── Simple models ──────────────────────────────────────────────────────────


class LoginIn(BaseModel):
    email: EmailStr
    password: str


class UserOut(BaseModel):
    id: str
    email: EmailStr
    role: str
    caps: dict[str, Any]
    profile: dict[str, Any] = {}


# ── Time / token helpers ───────────────────────────────────────────────────


def _now_s() -> int:
    return int(time.time())


def _now_iso() -> str:
    return _dt.datetime.now(_dt.timezone.utc).isoformat()


def _mint_access(uid: str, email: Optional[str] = None) -> tuple[str, int]:
    now = _now_s()
    exp = now + ACCESS_JWT_TTL_S
    payload: dict[str, Any] = {"sub": uid, "iat": now, "exp": exp}
    if email:
        payload["email"] = email
    if ACCESS_JWT_ISS:
        payload["iss"] = ACCESS_JWT_ISS
    if ACCESS_JWT_AUD:
        payload["aud"] = ACCESS_JWT_AUD
    return jwt.encode(payload, ACCESS_JWT_SECRET, algorithm=ACCESS_JWT_ALGO), exp


def _mint_refresh(uid: str) -> str:
    now = _now_s()
    exp = now + REFRESH_TTL_DAYS * 24 * 3600
    payload = {"sub": uid, "iat": now, "exp": exp, "typ": "refresh"}
    return jwt.encode(payload, REFRESH_JWT_SECRET, algorithm=REFRESH_JWT_ALGO)


def _infer_over18(payload: dict) -> bool:
    """
    Prefer explicit over18_confirmed from payload.
    If absent, infer from birth_year when present (youth flow).
    Otherwise default False (caller may choose to hard-enforce at the route).
    """
    if "over18_confirmed" in payload:
        return bool(payload.get("over18_confirmed"))

    by = payload.get("birth_year")
    if isinstance(by, int) and 1900 <= by <= _dt.datetime.now().year:
        return (_dt.datetime.now().year - by) >= 18
    return False


def _extract_tos_age_fields(payload: dict) -> dict[str, Any]:
    """
    Collects TOS + age fields robustly from mixed signup payloads.
    - tos_version: payload.tos_version or DEFAULT_TOS_VERSION
    - tos_accepted_at: now
    - over18_confirmed: explicit flag OR infer from birth_year (if available)
    """
    tos_version = (payload.get("tos_version") or DEFAULT_TOS_VERSION).strip()
    tos_accepted_at = _now_iso()
    over18_confirmed = _infer_over18(payload)
    return {
        "tos_version": tos_version,
        "tos_accepted_at": tos_accepted_at,
        "over18_confirmed": bool(over18_confirmed),
    }


def _safe_caps(caps_raw: Any, role: str) -> dict[str, Any]:
    try:
        caps = json.loads(caps_raw) if isinstance(caps_raw, str) else (caps_raw or {})
    except Exception:
        caps = {}
    if not caps:
        caps = ROLE_DEFAULT_CAPS.get(role, {})
    return caps


def is_admin_email(email: str) -> bool:
    if not email:
        return False
    e = email.lower()
    return e == ADMIN_EMAIL or e.endswith("@ecodia.au")


def mint_admin_token(email: str, ttl_secs: int = 6 * 60 * 60) -> str:
    now = _now_s()
    payload = {
        "sub": email,
        "scope": "admin",
        "iat": now,
        "exp": now + ttl_secs,
        "aud": "admin",
    }
    return jwt.encode(payload, JWT_SECRET, algorithm=JWT_ALGO)


def get_user_by_id(session: Session, uid: str) -> Optional[dict]:
    rec = session.run(
        "MATCH (u:User {id:$id}) RETURN u.email AS email", {"id": uid}
    ).single()
    if not rec:
        return None
    return {"email": rec["email"]}


def _issue_tokens_and_build_response_for_user(
    u: Any,
    role: str,
    profile: dict[str, Any],
    request: Request,
    response: Response,
) -> dict[str, Any]:
    """
    Shared response shaper for login + signup flows.
    Issues access/refresh cookies and returns UserOut-compatible payload.
    """
    caps = _safe_caps(u.get("caps_json") or "{}", role)

    access, exp = _mint_access(u["id"], u.get("email"))
    refresh = _mint_refresh(u["id"])

    set_scoped_cookie(
        response,
        name=REFRESH_COOKIE_NAME,
        value=refresh,
        max_age=REFRESH_TTL_DAYS * 24 * 3600,
        request=request,
    )
    set_scoped_cookie(
        response,
        name=ACCESS_COOKIE_NAME,
        value=access,
        max_age=ACCESS_JWT_TTL_S,
        request=request,
    )

    legal_complete = bool(u.get("legal_onboarding_complete") or False)

    resp: dict[str, Any] = {
        "id": u["id"],
        "email": u["email"],
        "role": role,
        "caps": caps,
        "profile": profile or {},
        "user_token": u["id"],
        "token": access,
        "exp": exp,
        "legal_onboarding_complete": legal_complete,
        "needs_legal": (not legal_complete),
        "tos_version": u.get("tos_version"),
        "tos_accepted_at": u.get("tos_accepted_at"),
        "privacy_accepted_at": u.get("privacy_accepted_at"),
        "over18_confirmed": u.get("over18_confirmed"),
        "birth_year": u.get("birth_year"),
    }

    if ADMIN_EMAIL and u.get("email", "").lower() == ADMIN_EMAIL:
        admin_token = mint_admin_token(u["email"], ttl_secs=60 * 60 * 12)
        resp["admin_token"] = admin_token

    return resp


# ── Routes ─────────────────────────────────────────────────────────────────


@router.post("/login")
def login(
    p: LoginIn,
    response: Response,
    request: Request,
    s: Session = Depends(session_dep),
):
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

    try:
        ph.verify(u["password_hash"], p.password)
    except VerifyMismatchError:
        raise HTTPException(status_code=401, detail="Invalid credentials")

    role = (u.get("role") or "public").lower()
    profile: dict[str, Any] = {}

    if role == "youth" and rec.get("yp"):
        profile = dict(rec["yp"])
    elif role == "business" and rec.get("bp"):
        profile = dict(rec["bp"])
    elif role == "creative" and rec.get("cp"):
        profile = dict(rec["cp"])
    elif role == "partner" and rec.get("pp"):
        profile = dict(rec["pp"])
    elif role == "public" and rec.get("pub"):
        profile = dict(rec["pub"])

    prof_avatar = profile.get("avatar_url") if isinstance(profile, dict) else None
    unified_avatar = prof_avatar or u.get("avatar_url")
    if isinstance(profile, dict):
        profile["avatar_url"] = unified_avatar

    return _issue_tokens_and_build_response_for_user(
        u=u,
        role=role,
        profile=profile,
        request=request,
        response=response,
    )


@router.post("/admin-cookie")
def r_admin_cookie_here(
    response: Response,
    request: Request,
    s: Session = Depends(session_dep),
    uid: str = Depends(current_user_id),
):
    # If you keep this duplicate, ensure it matches api/auth/admin_cookie.py,
    # or remove the other one.
    user = get_user_by_id(s, uid)
    email = (user or {}).get("email") or ""
    if not is_admin_email(email):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN, detail="Not an admin"
        )

    token = mint_admin_token(email, ttl_secs=6 * 60 * 60)

    set_scoped_cookie(
        response,
        name=ADMIN_COOKIE_NAME,
        value=token,
        max_age=6 * 60 * 60,
        request=request,
    )
    # Also return admin_token so FE can store it in localStorage if desired
    return {"ok": True, "admin_token": token}


@router.post("/login/youth")
def signup_youth(
    p: YouthSignupIn,
    response: Response,
    request: Request,
    s: Session = Depends(session_dep),
):
    email = p.email.lower()
    pwd_hash = ph.hash(p.password)

    tos_bits = _extract_tos_age_fields({"birth_year": p.birth_year})
    uid = str(uuid4())
    yp_id = str(uuid4())

    try:
        s.run(
            """
            CREATE (u:User {
              id: $id,
              email: $email,
              password_hash: $password_hash,
              role: 'youth',
              caps_json: $caps_json,
              legal_onboarding_complete: false,
              tos_version: $tos_version,
              tos_accepted_at: $tos_accepted_at,
              over18_confirmed: $over18_confirmed,
              birth_year: $birth_year,
              created_at: datetime(),
              updated_at: datetime()
            })
            CREATE (yp:YouthProfile {
              id: $yp_id,
              created_at: datetime(),
              updated_at: datetime()
            })
            MERGE (u)-[:HAS_PROFILE]->(yp)
            """,
            {
                "id": uid,
                "email": email,
                "password_hash": pwd_hash,
                "caps_json": json.dumps(ROLE_DEFAULT_CAPS["youth"]),
                "birth_year": int(p.birth_year),
                "yp_id": yp_id,
                **tos_bits,
            },
        )
    except ConstraintError:
        raise HTTPException(status_code=400, detail="Email already registered")

    # Refetch minimal user row
    rec = s.run("MATCH (u:User {id:$id}) RETURN u", id=uid).single()
    if not rec or not rec.get("u"):
        raise HTTPException(status_code=500, detail="Signup failed")

    u = rec["u"]
    profile: dict[str, Any] = {}

    return _issue_tokens_and_build_response_for_user(
        u=u,
        role="youth",
        profile=profile,
        request=request,
        response=response,
    )


@router.post("/login/creative")
def signup_creative(
    p: CreativeSignupIn,
    response: Response,
    request: Request,
    s: Session = Depends(session_dep),
):
    email = p.email.lower()
    pwd_hash = ph.hash(p.password)

    # For now, legal onboarding is completed via /me/account/legal-accept.
    tos_bits = _extract_tos_age_fields({"over18_confirmed": True})
    uid = str(uuid4())
    cp_id = str(uuid4())

    try:
        s.run(
            """
            CREATE (u:User {
              id: $id,
              email: $email,
              password_hash: $password_hash,
              role: 'creative',
              caps_json: $caps_json,
              legal_onboarding_complete: false,
              tos_version: $tos_version,
              tos_accepted_at: $tos_accepted_at,
              over18_confirmed: $over18_confirmed,
              created_at: datetime(),
              updated_at: datetime()
            })
            CREATE (cp:CreativeProfile {
              id: $cp_id,
              display_name: $display_name,
              portfolio_url: $portfolio_url,
              created_at: datetime(),
              updated_at: datetime()
            })
            MERGE (u)-[:HAS_PROFILE]->(cp)
            """,
            {
                "id": uid,
                "email": email,
                "password_hash": pwd_hash,
                "caps_json": json.dumps(ROLE_DEFAULT_CAPS["creative"]),
                "cp_id": cp_id,
                "display_name": p.display_name.strip(),
                "portfolio_url": (p.portfolio_url or "").strip(),
                **tos_bits,
            },
        )
    except ConstraintError:
        raise HTTPException(status_code=400, detail="Email already registered")

    rec = s.run(
        """
        MATCH (u:User {id:$id})
        OPTIONAL MATCH (u)-[:HAS_PROFILE]->(cp:CreativeProfile)
        RETURN u, cp
        """,
        id=uid,
    ).single()
    if not rec or not rec.get("u"):
        raise HTTPException(status_code=500, detail="Signup failed")

    u = rec["u"]
    profile = dict(rec["cp"]) if rec.get("cp") else {}

    return _issue_tokens_and_build_response_for_user(
        u=u,
        role="creative",
        profile=profile,
        request=request,
        response=response,
    )


@router.post("/login/partner")
def signup_partner(
    p: PartnerSignupIn,
    response: Response,
    request: Request,
    s: Session = Depends(session_dep),
):
    email = p.email.lower()
    pwd_hash = ph.hash(p.password)

    tos_bits = _extract_tos_age_fields({"over18_confirmed": True})
    uid = str(uuid4())
    pp_id = str(uuid4())

    org_type = (p.org_type or "community").strip()

    try:
        s.run(
            """
            CREATE (u:User {
              id: $id,
              email: $email,
              password_hash: $password_hash,
              role: 'partner',
              caps_json: $caps_json,
              legal_onboarding_complete: false,
              tos_version: $tos_version,
              tos_accepted_at: $tos_accepted_at,
              over18_confirmed: $over18_confirmed,
              created_at: datetime(),
              updated_at: datetime()
            })
            CREATE (pp:PartnerProfile {
              id: $pp_id,
              org_name: $org_name,
              org_type: $org_type,
              created_at: datetime(),
              updated_at: datetime()
            })
            MERGE (u)-[:HAS_PROFILE]->(pp)
            """,
            {
                "id": uid,
                "email": email,
                "password_hash": pwd_hash,
                "caps_json": json.dumps(ROLE_DEFAULT_CAPS["partner"]),
                "pp_id": pp_id,
                "org_name": p.org_name.strip(),
                "org_type": org_type or "community",
                **tos_bits,
            },
        )
    except ConstraintError:
        raise HTTPException(status_code=400, detail="Email already registered")

    rec = s.run(
        """
        MATCH (u:User {id:$id})
        OPTIONAL MATCH (u)-[:HAS_PROFILE]->(pp:PartnerProfile)
        RETURN u, pp
        """,
        id=uid,
    ).single()
    if not rec or not rec.get("u"):
        raise HTTPException(status_code=500, detail="Signup failed")

    u = rec["u"]
    profile = dict(rec["pp"]) if rec.get("pp") else {}

    return _issue_tokens_and_build_response_for_user(
        u=u,
        role="partner",
        profile=profile,
        request=request,
        response=response,
    )


@router.post("/login/public")
def signup_public(
    p: PublicSignupIn,
    response: Response,
    request: Request,
    s: Session = Depends(session_dep),
):
    email = p.email.lower()
    pwd_hash = ph.hash(p.password)

    tos_bits = _extract_tos_age_fields({"over18_confirmed": True})
    uid = str(uuid4())
    pub_id = str(uuid4())

    display_name = (p.display_name or "").strip()

    try:
        s.run(
            """
            CREATE (u:User {
              id: $id,
              email: $email,
              password_hash: $password_hash,
              role: 'public',
              caps_json: $caps_json,
              legal_onboarding_complete: false,
              tos_version: $tos_version,
              tos_accepted_at: $tos_accepted_at,
              over18_confirmed: $over18_confirmed,
              created_at: datetime(),
              updated_at: datetime()
            })
            CREATE (pub:PublicProfile {
              id: $pub_id,
              display_name: $display_name,
              created_at: datetime(),
              updated_at: datetime()
            })
            MERGE (u)-[:HAS_PROFILE]->(pub)
            """,
            {
                "id": uid,
                "email": email,
                "password_hash": pwd_hash,
                "caps_json": json.dumps(ROLE_DEFAULT_CAPS["public"]),
                "pub_id": pub_id,
                "display_name": display_name,
                **tos_bits,
            },
        )
    except ConstraintError:
        raise HTTPException(status_code=400, detail="Email already registered")

    rec = s.run(
        """
        MATCH (u:User {id:$id})
        OPTIONAL MATCH (u)-[:HAS_PROFILE]->(pub:PublicProfile)
        RETURN u, pub
        """,
        id=uid,
    ).single()
    if not rec or not rec.get("u"):
        raise HTTPException(status_code=500, detail="Signup failed")

    u = rec["u"]
    profile = dict(rec["pub"]) if rec.get("pub") else {}

    return _issue_tokens_and_build_response_for_user(
        u=u,
        role="public",
        profile=profile,
        request=request,
        response=response,
    )


@router.post("/login/minimal")
def signup_minimal(
    p: MinimalSignupIn,
    response: Response,
    request: Request,
    s: Session = Depends(session_dep),
):
    role = (p.role or "public").lower()
    if role not in ROLE_DEFAULT_CAPS:
        raise HTTPException(status_code=400, detail="Invalid role")

    email = p.email.lower()
    pwd_hash = ph.hash(p.password)

    # If user already exists, just verify password and log them in.
    rec = s.run("MATCH (u:User {email:$email}) RETURN u", email=email).single()
    if rec and rec.get("u"):
        u = rec["u"]
        existing_hash = u.get("password_hash")
        if not existing_hash:
            raise HTTPException(
                status_code=400,
                detail="Account exists as SSO-only. Use Sign in with Apple/Google.",
            )
        try:
            ph.verify(existing_hash, p.password)
        except VerifyMismatchError:
            raise HTTPException(status_code=401, detail="Invalid credentials")
        # ensure role is at least the requested role
        if u.get("role") != role:
            s.run(
                "MATCH (u:User {id:$id}) SET u.role=$role",
                id=u["id"],
                role=role,
            )
            u["role"] = role
        return _issue_tokens_and_build_response_for_user(
            u=u,
            role=role,
            profile={},
            request=request,
            response=response,
        )

    # Otherwise create a bare-bones user (business flow will attach BusinessProfile via /eco-local/business/init).
    tos_bits = _extract_tos_age_fields({"over18_confirmed": True})
    uid = str(uuid4())

    try:
        s.run(
            """
            CREATE (u:User {
              id: $id,
              email: $email,
              password_hash: $password_hash,
              role: $role,
              caps_json: $caps_json,
              legal_onboarding_complete: false,
              tos_version: $tos_version,
              tos_accepted_at: $tos_accepted_at,
              over18_confirmed: $over18_confirmed,
              created_at: datetime(),
              updated_at: datetime()
            })
            """,
            {
                "id": uid,
                "email": email,
                "password_hash": pwd_hash,
                "role": role,
                "caps_json": json.dumps(ROLE_DEFAULT_CAPS[role]),
                **tos_bits,
            },
        )
    except ConstraintError:
        raise HTTPException(status_code=400, detail="Email already registered")

    rec2 = s.run("MATCH (u:User {id:$id}) RETURN u", id=uid).single()
    if not rec2 or not rec2.get("u"):
        raise HTTPException(status_code=500, detail="Signup failed")

    u2 = rec2["u"]

    return _issue_tokens_and_build_response_for_user(
        u=u2,
        role=role,
        profile={},
        request=request,
        response=response,
    )
