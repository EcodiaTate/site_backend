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
from site_backend.core.user_guard import current_user_id  # <-- needed for /auth/admin-cookie

router = APIRouter()
ph = PasswordHasher()

# ------------------ Config & helpers ------------------
JWT_SECRET = os.getenv("JWT_SECRET", "dev-secret-change-me")
JWT_ALGO = "HS256"
ADMIN_EMAIL = (os.getenv("ADMIN_EMAIL") or "tate@ecodia.au").lower()

# Access/Refresh config (HS256; upgradeable to RS256 later)
ACCESS_JWT_SECRET = os.getenv("ACCESS_JWT_SECRET", JWT_SECRET)
ACCESS_JWT_ALGO   = os.getenv("ACCESS_JWT_ALGO", "HS256")
ACCESS_JWT_TTL_S  = int(os.getenv("ACCESS_JWT_TTL_S", "900"))    # 15m
ACCESS_JWT_ISS    = os.getenv("ACCESS_JWT_ISS", None)
ACCESS_JWT_AUD    = os.getenv("ACCESS_JWT_AUD", None)

REFRESH_JWT_SECRET  = os.getenv("REFRESH_JWT_SECRET", JWT_SECRET)
REFRESH_JWT_ALGO    = os.getenv("REFRESH_JWT_ALGO", "HS256")
REFRESH_TTL_DAYS    = int(os.getenv("REFRESH_TTL_DAYS", "90"))
REFRESH_COOKIE_NAME = os.getenv("REFRESH_COOKIE_NAME", "refresh_token")

def _now_s() -> int:
    return int(time.time())

def _mint_access(uid: str, email: Optional[str] = None) -> tuple[str, int]:
    now = _now_s()
    exp = now + ACCESS_JWT_TTL_S
    payload = {
        "sub": uid,
        "iat": now,
        "exp": exp,
    }
    if email: payload["email"] = email
    if ACCESS_JWT_ISS: payload["iss"] = ACCESS_JWT_ISS
    if ACCESS_JWT_AUD: payload["aud"] = ACCESS_JWT_AUD
    token = jwt.encode(payload, ACCESS_JWT_SECRET, algorithm=ACCESS_JWT_ALGO)
    return token, exp

def _mint_refresh(uid: str) -> str:
    now = _now_s()
    exp = now + REFRESH_TTL_DAYS * 24 * 3600
    payload = {
        "sub": uid,
        "iat": now,
        "exp": exp,
        "typ": "refresh",
    }
    return jwt.encode(payload, REFRESH_JWT_SECRET, algorithm=REFRESH_JWT_ALGO)

def _safe_caps(caps_raw: Any, role: str) -> dict[str, Any]:
    """
    Parse caps from DB (stringified JSON or map). If missing/empty,
    fall back to ROLE_DEFAULT_CAPS[role].
    """
    try:
        caps = json.loads(caps_raw) if isinstance(caps_raw, str) else (caps_raw or {})
    except Exception:
        caps = {}
    if not caps:
        caps = ROLE_DEFAULT_CAPS.get(role, {})
    return caps

# ---- Admin helpers (cookie-based) ----
def is_admin_email(email: str) -> bool:
    if not email:
        return False
    e = email.lower()
    # keep simple: exact match or whole domain
    return e == ADMIN_EMAIL or e.endswith("@ecodia.au")

def mint_admin_token(email: str, ttl_secs: int = 6 * 60 * 60) -> str:
    now = _now_s()
    payload = {
        "sub": email,
        "scope": "admin",
        "iat": now,
        "exp": now + ttl_secs,
    }
    return jwt.encode(payload, JWT_SECRET, algorithm=JWT_ALGO)

def get_user_by_id(session: Session, uid: str) -> Optional[dict]:
    rec = session.run("MATCH (u:User {id:$id}) RETURN u.email AS email", {"id": uid}).single()
    if not rec:
        return None
    return {"email": rec["email"]}

# ------------------ Role default capabilities ------------------
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

# ------------------ Schemas ------------------
class YouthJoinIn(BaseModel):
    email: EmailStr
    password: str = Field(min_length=6)
    birth_year: int = Field(ge=1900, le=2100)

class BusinessJoinIn(BaseModel):
    email: EmailStr
    password: str = Field(min_length=6)
    store_name: str = Field(min_length=2, max_length=120)
    pledge: Optional[float] = Field(default=None, ge=0)

class CreativeJoinIn(BaseModel):
    email: EmailStr
    password: str = Field(min_length=6)
    display_name: str = Field(min_length=2, max_length=120)
    portfolio_url: Optional[str] = Field(default=None, max_length=300)

class PartnerJoinIn(BaseModel):
    email: EmailStr
    password: str = Field(min_length=6)
    org_name: str = Field(min_length=2, max_length=160)
    org_type: str = Field(default="community", max_length=80)

class PublicJoinIn(BaseModel):
    email: EmailStr
    password: str = Field(min_length=6)
    display_name: Optional[str] = Field(default="", max_length=120)

class LoginIn(BaseModel):
    email: EmailStr
    password: str

class UserOut(BaseModel):
    id: str
    email: EmailStr
    role: str
    caps: dict[str, Any]
    profile: dict[str, Any] = {}

# ------------------ Joins ------------------
@router.post("/join/youth", response_model=UserOut)
def join_youth(p: YouthJoinIn, s: Session = Depends(session_dep)):
    uid = str(uuid4())
    hash_ = ph.hash(p.password)
    caps_json = json.dumps(ROLE_DEFAULT_CAPS["youth"])
    cypher = """
    CREATE (u:User {
      id:$id, email:$email, password_hash:$hash, role:"youth",
      created_at:datetime(), caps_json:$caps_json
    })
    CREATE (yp:YouthProfile {
      user_id:$id, birth_year:$birth_year, eyba_points:0, actions_completed:0
    })
    CREATE (u)-[:HAS_PROFILE]->(yp)
    RETURN u, yp
    """
    try:
        rec = s.run(
            cypher,
            id=uid,
            email=p.email.lower(),
            hash=hash_,
            caps_json=caps_json,
            birth_year=p.birth_year,
        ).single()
        if not rec:
            raise HTTPException(status_code=500, detail="No record returned from Neo4j")
        u, yp = rec["u"], rec["yp"]
        out = {
            "id": u["id"],
            "email": u["email"],
            "role": u["role"],
            "caps": _safe_caps(u.get("caps_json"), "youth"),
            "profile": dict(yp),
        }
        return out
    except ConstraintError:
        raise HTTPException(status_code=409, detail="That email is already registered. Try logging in, or reset your password.")
    except Exception:
        raise HTTPException(status_code=400, detail="We couldn't create your account. Please try again.")
@router.post("/join/business", response_model=UserOut)
def join_business(p: BusinessJoinIn, s: Session = Depends(session_dep)):
    uid = str(uuid4())
    hash_ = ph.hash(p.password)
    caps_json = json.dumps(ROLE_DEFAULT_CAPS["business"])
    cypher = """
    // --- Create user (unique email constraint should exist) ---
    CREATE (u:User {
      id:$id, email:$email, password_hash:$hash, role:"business",
      created_at:datetime(), caps_json:$caps_json
    })

    // --- New-style BusinessProfile (has stable id + name) ---
    CREATE (b:BusinessProfile {
      id: 'biz_' + replace(toString(randomUUID()),'-','')[..12],
      user_id:$id,
      name:$store_name,
      pledge_tier: CASE WHEN $pledge IS NULL THEN NULL ELSE toString($pledge) END,
      eco_score:0,
      pay_model:'pwyw',
      created_at:datetime()
    })

    // --- Normalize relationship used everywhere else ---
    MERGE (u)-[:OWNS]->(b)

    // --- Ensure a QR exists and is linked via :OF ---
    CALL {
      WITH b
      OPTIONAL MATCH (q0:QR)-[:OF]->(b)
      WITH b, q0
      WHERE q0 IS NOT NULL
      RETURN q0.code AS code
      UNION
      WITH b
      WITH b, apoc.text.random('ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789', 10) AS code
      MERGE (q:QR {code: code})
      MERGE (q)-[:OF]->(b)
      RETURN code
    } AS qr

    RETURN u, b, qr.code AS qr_code
    """
    try:
        rec = s.run(
            cypher,
            id=uid,
            email=p.email.lower(),
            hash=hash_,
            caps_json=caps_json,
            store_name=p.store_name,
            pledge=p.pledge,
        ).single()
        if not rec:
            raise HTTPException(status_code=500, detail="No record returned from Neo4j")
        u, b, qr_code = rec["u"], rec["b"], rec["qr_code"]

        return {
            "id": u["id"],
            "email": u["email"],
            "role": u["role"],
            "caps": _safe_caps(u.get("caps_json"), "business"),
            "profile": {**dict(b), "qr_code": qr_code},
        }
    except ConstraintError:
        raise HTTPException(status_code=409, detail="That email is already registered. Try logging in, or reset your password.")
    except Exception:
        raise HTTPException(status_code=400, detail="We couldn't create your account. Please try again.")

@router.post("/join/creative", response_model=UserOut)
def join_creative(p: CreativeJoinIn, s: Session = Depends(session_dep)):
    uid = str(uuid4())
    hash_ = ph.hash(p.password)
    caps_json = json.dumps(ROLE_DEFAULT_CAPS["creative"])
    cypher = """
    CREATE (u:User {
      id:$id, email:$email, password_hash:$hash, role:"creative",
      created_at:datetime(), caps_json:$caps_json
    })
    CREATE (cp:CreativeProfile {
      user_id:$id, display_name:$display_name, portfolio_url:$portfolio_url,
      collabs_started:0
    })
    CREATE (u)-[:HAS_PROFILE]->(cp)
    RETURN u, cp
    """
    try:
        rec = s.run(
            cypher,
            id=uid,
            email=p.email.lower(),
            hash=hash_,
            caps_json=caps_json,
            display_name=p.display_name,
            portfolio_url=p.portfolio_url or "",
        ).single()
        if not rec:
            raise HTTPException(status_code=500, detail="No record returned from Neo4j")
        u, cp = rec["u"], rec["cp"]
        return {
            "id": u["id"],
            "email": u["email"],
            "role": u["role"],
            "caps": _safe_caps(u.get("caps_json"), "creative"),
            "profile": dict(cp),
        }
    except ConstraintError:
        raise HTTPException(status_code=409, detail="That email is already registered. Try logging in, or reset your password.")
    except Exception:
        raise HTTPException(status_code=400, detail="We couldn't create your account. Please try again.")

@router.post("/join/partner", response_model=UserOut)
def join_partner(p: PartnerJoinIn, s: Session = Depends(session_dep)):
    uid = str(uuid4())
    hash_ = ph.hash(p.password)
    caps_json = json.dumps(ROLE_DEFAULT_CAPS["partner"])
    cypher = """
    CREATE (u:User {
      id:$id, email:$email, password_hash:$hash, role:"partner",
      created_at:datetime(), caps_json:$caps_json
    })
    CREATE (pp:PartnerProfile {
      user_id:$id, org_name:$org_name, org_type:$org_type, active_projects:0
    })
    CREATE (u)-[:HAS_PROFILE]->(pp)
    RETURN u, pp
    """
    try:
        rec = s.run(
            cypher,
            id=uid,
            email=p.email.lower(),
            hash=hash_,
            caps_json=caps_json,
            org_name=p.org_name,
            org_type=p.org_type,
        ).single()
        if not rec:
            raise HTTPException(status_code=500, detail="No record returned from Neo4j")
        u, pp = rec["u"], rec["pp"]
        return {
            "id": u["id"],
            "email": u["email"],
            "role": u["role"],
            "caps": _safe_caps(u.get("caps_json"), "partner"),
            "profile": dict(pp),
        }
    except ConstraintError:
        raise HTTPException(status_code=409, detail="That email is already registered. Try logging in, or reset your password.")
    except Exception:
        raise HTTPException(status_code=400, detail="We couldn't create your account. Please try again.")

@router.post("/join/public", response_model=UserOut)
def join_public(p: PublicJoinIn, s: Session = Depends(session_dep)):
    uid = str(uuid4())
    hash_ = ph.hash(p.password)
    caps_json = json.dumps(ROLE_DEFAULT_CAPS["public"])
    cypher = """
    CREATE (u:User {
      id:$id, email:$email, password_hash:$hash, role:"public",
      created_at:datetime(), caps_json:$caps_json
    })
    CREATE (pub:PublicProfile { user_id:$id, display_name:$display_name, following:0 })
    CREATE (u)-[:HAS_PROFILE]->(pub)
    RETURN u, pub
    """
    try:
        rec = s.run(
            cypher,
            id=uid,
            email=p.email.lower(),
            hash=hash_,
            caps_json=caps_json,
            display_name=p.display_name or "",
        ).single()
        if not rec:
            raise HTTPException(status_code=500, detail="No record returned from Neo4j")
        u, pub = rec["u"], rec["pub"]
        return {
            "id": u["id"],
            "email": u["email"],
            "role": u["role"],
            "caps": _safe_caps(u.get("caps_json"), "public"),
            "profile": dict(pub),
        }
    except ConstraintError:
        raise HTTPException(status_code=409, detail="That email is already registered. Try logging in, or reset your password.")
    except Exception:
        raise HTTPException(status_code=400, detail="We couldn't create your account. Please try again.")

# ------------------ Login ------------------
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
    try:
        ph.verify(u["password_hash"], p.password)
    except VerifyMismatchError:
        raise HTTPException(status_code=401, detail="Invalid credentials")

    role = (u.get("role") or "public").lower()

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
    else:
        profile = {}

    caps = _safe_caps(u.get("caps_json") or "{}", role)

    # --- Mint tokens ---
    access, exp = _mint_access(u["id"], u["email"])
    refresh = _mint_refresh(u["id"])

    # --- Set refresh cookie (HttpOnly) ---
    # NOTE: set secure=True in production behind HTTPS
    response.set_cookie(
        key=os.getenv("REFRESH_COOKIE_NAME", REFRESH_COOKIE_NAME),
        value=refresh,
        path="/",
        httponly=True,
        secure=False,
        samesite="lax",
        max_age=REFRESH_TTL_DAYS * 24 * 3600,
    )

    resp = {
        "id": u["id"],
        "email": u["email"],
        "role": role,
        "caps": caps,
        "profile": profile,
        # Frontend expects these today:
        "user_token": u["id"],  # legacy convenience; slated for deprecation
        # Canonical access token for FE:
        "token": access,
        "exp": exp,
    }

    # Keep header-mode admin token for backward-compat (optional)
    if ADMIN_EMAIL and u["email"].lower() == ADMIN_EMAIL:
        now = _now_s()
        admin_payload = {"sub": u["email"], "scope": "admin", "iat": now, "exp": now + 60*60*12, "aud": "admin"}
        resp["admin_token"] = jwt.encode(admin_payload, JWT_SECRET, algorithm=JWT_ALGO)

    return resp


# ------------------ Admin cookie mint/rotate ------------------
@router.post("/admin-cookie")
def r_admin_cookie(
    response: Response,
    s: Session = Depends(session_dep),
    uid: str = Depends(current_user_id),
):
    user = get_user_by_id(s, uid)
    email = (user or {}).get("email") or ""
    if not is_admin_email(email):
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Not an admin")

    token = mint_admin_token(email, ttl_secs=6*60*60)  # 6h
    response.set_cookie(
        key="admin_token",
        value=token,
        httponly=True,
        samesite="Lax",
        secure=False,   # True in prod HTTPS
        path="/",
        max_age=6*60*60,
    )
    return {"ok": True}

# --- Add with your other Pydantic models ---
class MinimalJoinIn(BaseModel):
    email: EmailStr
    password: str = Field(min_length=6)
    role: str = Field(pattern="^(youth|business|creative|partner|public)$")


# --- New endpoint: creates only (u:User {role}) with no profile node ---
@router.post("/join/minimal", response_model=UserOut)
def join_minimal(p: MinimalJoinIn, s: Session = Depends(session_dep)):
    role = p.role.lower()
    if role not in ROLE_DEFAULT_CAPS:
        raise HTTPException(status_code=400, detail="Unknown role")

    uid = str(uuid4())
    hash_ = ph.hash(p.password)
    caps_json = json.dumps(ROLE_DEFAULT_CAPS[role])

    cypher = """
    CREATE (u:User {
      id:$id, email:$email, password_hash:$hash, role:$role,
      created_at:datetime(), caps_json:$caps_json
    })
    RETURN u
    """
    try:
        rec = s.run(
            cypher,
            id=uid,
            email=p.email.lower(),
            hash=hash_,
            role=role,
            caps_json=caps_json,
        ).single()
        if not rec:
            raise HTTPException(status_code=500, detail="No record returned from Neo4j")
        u = rec["u"]
        return {
            "id": u["id"],
            "email": u["email"],
            "role": u["role"],
            "caps": _safe_caps(u.get("caps_json"), role),
            "profile": {},  # deliberately empty: profiles are created later
        }
    except ConstraintError:
        raise HTTPException(status_code=409, detail="That email is already registered. Try logging in, or reset your password.")
    except Exception:
        raise HTTPException(status_code=400, detail="We couldn't create your account. Please try again.")
