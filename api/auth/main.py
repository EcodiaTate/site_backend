from __future__ import annotations
from uuid import uuid4
from typing import Any, Optional

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, EmailStr, Field
from argon2 import PasswordHasher
from neo4j import Session
import os, time, json
from jose import jwt
from neo4j.exceptions import ConstraintError

from site_backend.core.neo_driver import session_dep

router = APIRouter()
ph = PasswordHasher()

# ------------------ Config & helpers ------------------
JWT_SECRET = os.getenv("JWT_SECRET", "dev-secret-change-me")
JWT_ALGO = "HS256"
ADMIN_EMAIL = (os.getenv("ADMIN_EMAIL") or "tate@ecodia.au").lower()

def _now_ms() -> int:
    return int(time.time() * 1000)

def _mint_admin_token(email: str) -> str:
    now = int(time.time())
    exp = now + 60 * 60 * 12  # 12h
    payload = {"sub": email, "scope": "admin", "iat": now, "exp": exp, "aud": "admin"}
    token = jwt.encode(payload, JWT_SECRET, algorithm=JWT_ALGO)
    return token

# ------------------ Shared role defaults ------------------
ROLE_DEFAULT_CAPS: dict[str, dict[str, int]] = {
    "youth": {"max_redemptions_per_week": 5},
    "business": {"max_active_offers": 3},
    "creative": {"max_active_collabs": 3},
    "partner": {"max_workspaces": 2},
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

# ------------------ Helpers ------------------
def _safe_caps(caps_raw: Any, role: str) -> dict[str, Any]:
    try:
        caps = json.loads(caps_raw) if isinstance(caps_raw, str) else (caps_raw or {})
    except Exception:
        caps = {}
    if not caps:
        caps = ROLE_DEFAULT_CAPS.get(role, {})
    return caps

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
    CREATE (u:User {
      id:$id, email:$email, password_hash:$hash, role:"business",
      created_at:datetime(), caps_json:$caps_json
    })
    CREATE (bp:BusinessProfile {
      user_id:$id, store_name:$store_name, pay_model:"pwyw", pledge:$pledge, eco_score:0
    })
    CREATE (u)-[:HAS_PROFILE]->(bp)
    RETURN u, bp
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
        u, bp = rec["u"], rec["bp"]
        out = {
            "id": u["id"],
            "email": u["email"],
            "role": u["role"],
            "caps": _safe_caps(u.get("caps_json"), "business"),
            "profile": dict(bp),
        }
        return out
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
def login(p: LoginIn, s: Session = Depends(session_dep)):
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

    resp = {"id": u["id"], "email": u["email"], "role": role, "caps": caps, "profile": profile}

    if ADMIN_EMAIL and u["email"].lower() == ADMIN_EMAIL:
        resp["admin_token"] = _mint_admin_token(u["email"])

    resp["user_token"] = u["id"]  # optional convenience

    return resp
