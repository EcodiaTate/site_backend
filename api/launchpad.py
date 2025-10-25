from __future__ import annotations

import os
import uuid
import datetime as dt
from typing import Optional, Literal, List, Dict, Any

from fastapi import APIRouter, HTTPException, Header, status, Query, Request, Depends
from pydantic import BaseModel, Field, EmailStr, HttpUrl
from jose import jwt, JWTError
from neo4j import Session
from neo4j.exceptions import Neo4jError

from site_backend.core.admin_guard import require_admin, JWT_SECRET, JWT_ALGO
from site_backend.core.neo_driver import session_dep

router = APIRouter(prefix="/launchpad", tags=["launchpad"])

# ------------------------------------------------------------------------------#
# Config
# ------------------------------------------------------------------------------#
LAUNCHPAD_JWT_AUD = "launchpad-owner"
LAUNCHPAD_OWNER_TOKEN_TTL_DAYS = int(os.getenv("LAUNCHPAD_OWNER_TOKEN_TTL_DAYS", "90"))
ACTION_COOLDOWN_SECONDS = int(os.getenv("LAUNCHPAD_ACTION_COOLDOWN_SECONDS", "8"))
_last_action_by_key: Dict[str, float] = {}

# ------------------------------------------------------------------------------#
# Models
# ------------------------------------------------------------------------------#
Category = Literal["education", "tech", "arts", "community", "environment", "other"]
Need = Literal["development", "design", "publicity", "funding", "research", "mentorship"]
StatusType = Literal["new", "draft", "triage", "greenhouse", "incubation", "declined", "showcased"]

# Frontend expects these fields for admin list:
class AdminProposalRow(BaseModel):
    id: str
    slug: str
    title: str
    one_liner: str
    category: Category
    status: StatusType
    region: Optional[str] = None
    readiness_score: int
    owners: List[str] = []
    applause: int = 0
    followers: int = 0
    updated_at: str

# ------------------------------------------------------------------------------#
# Helpers: token extractors & value sanitizers
# ------------------------------------------------------------------------------#
def _extract_owner_token(request: Request, x_owner_token: Optional[str]) -> Optional[str]:
    # Prefer header, fallback to ?owner_token=...
    if x_owner_token:
        return x_owner_token
    q = request.query_params.get("owner_token")
    return q or None

def _extract_admin_token(request: Request, x_auth_token: Optional[str]) -> Optional[str]:
    # Prefer header (X-Auth-Token), fallback to ?admin_token=...
    if x_auth_token:
        return x_auth_token
    q = request.query_params.get("admin_token")
    return q or None

async def _require_admin_from_any(
    request: Request,
    x_auth_token: Optional[str] = Header(default=None, alias="X-Auth-Token"),
) -> str:
    tok = _extract_admin_token(request, x_auth_token)
    if not tok:
        raise HTTPException(status_code=401, detail="Admin token required")
    return await require_admin(tok)

def _to_primitive(val: Any) -> Any:
    """Convert Pydantic/URL/datetime types into plain Python types."""
    if isinstance(val, list):
        return [_to_primitive(x) for x in val]
    if isinstance(val, dict):
        return {k: _to_primitive(v) for k, v in val.items()}
    tname = type(val).__name__
    if tname in ("HttpUrl", "AnyUrl", "Url"):
        return str(val)
    if isinstance(val, (dt.datetime, dt.date)):
        return val.isoformat()
    return val

def _sanitize_patch_dict(d: dict) -> dict:
    return {k: _to_primitive(v) for k, v in d.items()}

class ProposalCreate(BaseModel):
    title: str = Field(..., min_length=2, max_length=120)
    one_liner: str = Field(..., min_length=10, max_length=160)
    category: Category
    needs: List[Need] = Field(default_factory=list)
    impact_summary: Optional[str] = Field(default=None, max_length=1200)
    region: Optional[str] = Field(default=None, max_length=120)
    contact_email: EmailStr
    links: Optional[List[HttpUrl]] = None
    consent_public: bool = False
    cover_url: Optional[HttpUrl] = None

class ProposalUpdate(BaseModel):
    title: Optional[str] = Field(default=None, min_length=2, max_length=120)
    one_liner: Optional[str] = Field(default=None, min_length=10, max_length=200)
    category: Optional[Category] = None
    needs: Optional[List[Need]] = None
    impact_summary: Optional[str] = Field(default=None, max_length=4000)
    problem: Optional[str] = Field(default=None, max_length=4000)
    solution: Optional[str] = Field(default=None, max_length=6000)
    milestones: Optional[List[str]] = None
    evidence_links: Optional[List[HttpUrl]] = None
    team: Optional[List[str]] = None
    region: Optional[str] = Field(default=None, max_length=120)
    links: Optional[List[HttpUrl]] = None
    consent_public: Optional[bool] = None
    cover_url: Optional[HttpUrl] = None

class ProposalOwnerView(BaseModel):
    id: str
    slug: str
    status: StatusType
    title: str
    one_liner: str
    category: Category
    needs: List[Need]
    impact_summary: Optional[str]
    problem: Optional[str]
    solution: Optional[str]
    milestones: List[str]
    evidence_links: List[str]
    team: List[str]
    region: Optional[str]
    links: List[str]
    consent_public: bool
    readiness_score: int
    created_at: str
    updated_at: str
    cover_url: Optional[str] = None

class ReviewScores(BaseModel):
    clarity: int = Field(ge=0, le=5)
    feasibility: int = Field(ge=0, le=5)
    alignment: int = Field(ge=0, le=5)
    impact: int = Field(ge=0, le=5)
    commitment: int = Field(ge=0, le=5)

    @property
    def avg(self) -> float:
        return (self.clarity + self.feasibility + self.alignment + self.impact + self.commitment) / 5.0

class ReviewCreate(BaseModel):
    proposal_id: str
    notes: Optional[str] = Field(default=None, max_length=4000)
    scores: ReviewScores

class StatusChange(BaseModel):
    proposal_id: str
    status: StatusType

class PublicCard(BaseModel):
    id: str
    slug: str
    title: str
    one_liner: str
    category: Category
    region: Optional[str]
    status: StatusType
    applause: int
    followers: int
    cover_url: Optional[str] = None

class FollowRequest(BaseModel):
    proposal_id: str
    email: Optional[EmailStr] = None
    client_fingerprint: Optional[str] = None

class ApplaudRequest(BaseModel):
    proposal_id: str
    email: Optional[EmailStr] = None
    client_fingerprint: Optional[str] = None

# ------------------------------------------------------------------------------#
# Tokens
# ------------------------------------------------------------------------------#
def mint_owner_token(proposal_id: str) -> str:
    now = dt.datetime.utcnow()
    exp = now + dt.timedelta(days=LAUNCHPAD_OWNER_TOKEN_TTL_DAYS)
    payload = {"sub": proposal_id, "aud": LAUNCHPAD_JWT_AUD, "scope": "owner", "iat": int(now.timestamp()), "exp": int(exp.timestamp())}
    return jwt.encode(payload, JWT_SECRET, algorithm=JWT_ALGO)

def verify_owner_token(token: str) -> str:
    try:
        payload = jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGO], audience=LAUNCHPAD_JWT_AUD)
        sub = payload.get("sub")
        if not sub:
            raise JWTError("No subject in token")
        return sub
    except JWTError as e:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail=f"Invalid owner link: {e}")

# ------------------------------------------------------------------------------#
# Utils
# ------------------------------------------------------------------------------#
def _now_iso() -> str:
    return dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"

def _slugify(title: str) -> str:
    base = "".join(ch.lower() if ch.isalnum() else "-" for ch in title).strip("-")
    base = "-".join([p for p in base.split("-") if p])
    return f"{base}-{uuid.uuid4().hex[:6]}"

def compute_readiness(p: dict) -> int:
    score = 0
    fields = [
        ("title", 10), ("one_liner", 10), ("impact_summary", 10),
        ("problem", 15), ("solution", 20),
        ("milestones", 10), ("evidence_links", 10), ("team", 10), ("links", 5),
    ]
    for key, pts in fields:
        val = p.get(key)
        if isinstance(val, list):
            if val:
                score += pts
        elif val:
            score += pts
    return max(0, min(score, 100))

def _rate_key(ip: str, fp: Optional[str], action: str, pid: str) -> str:
    return f"{ip}:{fp or '-'}:{action}:{pid}"

def _check_rate_limit(ip: str, fp: Optional[str], action: str, pid: str):
    import time
    key = _rate_key(ip, fp, action, pid)
    now = time.time()
    last = _last_action_by_key.get(key, 0.0)
    if now - last < ACTION_COOLDOWN_SECONDS:
        raise HTTPException(status_code=429, detail="Please wait a moment before trying again.")
    _last_action_by_key[key] = now

# ------------------------------------------------------------------------------#
# Cypher
# ------------------------------------------------------------------------------#
CYPHER_GET_PROPOSAL = "MATCH (p:ProjectProposal {id: $id}) RETURN p"
CYPHER_GET_BY_SLUG = "MATCH (p:ProjectProposal {slug: $slug}) RETURN p.id AS id, p.slug AS slug"

CYPHER_OWNER_CHECK = """
MATCH (u:User {email: $email})-[:OWNS]->(p:ProjectProposal {id: $proposal_id})
RETURN p LIMIT 1
"""

CYPHER_PATCH_PROPOSAL = """
MATCH (p:ProjectProposal {id: $id})
SET p += $patch,
    p.updated_at = datetime($now),
    p.readiness_score = $readiness
RETURN p
"""

CYPHER_REQUEST_REVIEW = """
MATCH (p:ProjectProposal {id: $id})
SET p.status = 'triage', p.updated_at = datetime($now)
RETURN p
"""

CYPHER_CREATE_REVIEW = """
MATCH (p:ProjectProposal {id: $proposal_id})
WITH p
CREATE (r:Review {
  id: $id,
  clarity: $scores.clarity,
  feasibility: $scores.feasibility,
  alignment: $scores.alignment,
  impact: $scores.impact,
  commitment: $commitment,
  notes: $notes,
  avg: $avg,
  created_at: datetime($now)
})
MERGE (p)<-[:FOR]-(r)
WITH p, r
MATCH (admin:User {email: $admin_email})
MERGE (admin)-[:REVIEWED]->(r)
RETURN r
""".replace("$commitment", "$scores.commitment")

CYPHER_STATUS_CHANGE = """
MATCH (p:ProjectProposal {id: $proposal_id})
SET p.status = $status, p.updated_at = datetime($now)
RETURN p
"""

CYPHER_PUBLIC_LIST = """
MATCH (p:ProjectProposal)
WHERE p.status IN ['greenhouse','incubation','showcased']
OPTIONAL MATCH (f:Follow)-[:FOR]->(p)
WITH p, count(f) AS followers
OPTIONAL MATCH (a:Applaud)-[:FOR]->(p)
WITH p, followers, count(a) AS applause
RETURN p {.*, followers: followers, applause: applause}
ORDER BY p.updated_at DESC
SKIP $skip
LIMIT $limit
"""

CYPHER_PUBLIC_TRENDING = """
MATCH (p:ProjectProposal)
WHERE p.status IN ['greenhouse','incubation','showcased']
OPTIONAL MATCH (a:Applaud)-[:FOR]->(p)
WITH p, a
WHERE a IS NULL OR a.created_at >= datetime($since)
WITH p, count(a) As recent_applause
OPTIONAL MATCH (f:Follow)-[:FOR]->(p)
WITH p, recent_applause, count(f) AS followers
WITH p, (recent_applause * 3) + toInteger(followers * 0.5) AS score, followers
RETURN p {.*, followers: followers, applause: score} AS row
ORDER BY score DESC, p.updated_at DESC
SKIP $skip
LIMIT $limit
"""

# New: admin listing with filters (status/q), counts, owners
CYPHER_ADMIN_LIST = """
MATCH (p:ProjectProposal)
WHERE ($status IS NULL OR p.status = $status)
  AND (
    $q IS NULL OR
    toLower(p.title) CONTAINS toLower($q) OR
    toLower(p.one_liner) CONTAINS toLower($q) OR
    (p.region IS NOT NULL AND toLower(p.region) CONTAINS toLower($q))
  )
OPTIONAL MATCH (o:User)-[:OWNS]->(p)
WITH p, collect(DISTINCT o.email) AS owners
OPTIONAL MATCH (f:Follow)-[:FOR]->(p)
WITH p, owners, count(f) AS followers
OPTIONAL MATCH (a:Applaud)-[:FOR]->(p)
WITH p, owners, followers, count(a) AS applause
RETURN {
  id: p.id,
  slug: p.slug,
  title: p.title,
  one_liner: p.one_liner,
  category: p.category,
  status: p.status,
  region: p.region,
  readiness_score: coalesce(p.readiness_score, 0),
  owners: owners,
  applause: applause,
  followers: followers,
  updated_at: toString(p.updated_at)
} AS row
ORDER BY p.updated_at DESC
SKIP $skip
LIMIT $limit
"""

CYPHER_FOLLOW_FOREACH = """
MATCH (p:ProjectProposal {id: $proposal_id})
WITH p, $email AS email
FOREACH (_ IN CASE WHEN email IS NULL OR email = '' THEN [] ELSE [1] END |
  MERGE (u:User {email: email})
  ON CREATE SET u.id = coalesce(u.id, randomUUID()), u.created_at = timestamp()
  MERGE (u)-[:FOLLOWS]->(p)
)
CREATE (f:Follow {id: $id, created_at: datetime($now)})
MERGE (f)-[:FOR]->(p)
RETURN p
"""

CYPHER_APPLAUD_EMAIL_DEDUPE = """
MATCH (p:ProjectProposal {id: $proposal_id})
WITH p, date(datetime($now)) AS d
MERGE (u:User {email: $email})
  ON CREATE SET u.id = coalesce(u.id, randomUUID()), u.created_at = timestamp()
MERGE (a:Applaud {email: $email, for_date: toString(d), proposal_id: $proposal_id})
  ON CREATE SET a.id = $id, a.created_at = datetime($now)
MERGE (a)-[:FOR]->(p)
RETURN p, a
"""

CYPHER_APPLAUD_ANON = """
MATCH (p:ProjectProposal {id: $proposal_id})
CREATE (a:Applaud {id: $id, created_at: datetime($now)})
MERGE (a)-[:FOR]->(p)
RETURN p, a
"""

# ------------------------------------------------------------------------------#
# Helpers
# ------------------------------------------------------------------------------#
def _proposal_to_owner_view(rec: dict) -> ProposalOwnerView:
    return ProposalOwnerView(
        id=rec["id"],
        slug=rec["slug"],
        status=rec.get("status", "new"),
        title=rec["title"],
        one_liner=rec["one_liner"],
        category=rec["category"],
        needs=list(rec.get("needs", []) or []),
        impact_summary=rec.get("impact_summary"),
        problem=rec.get("problem"),
        solution=rec.get("solution"),
        milestones=list(rec.get("milestones", []) or []),
        evidence_links=[str(u) for u in (rec.get("evidence_links") or [])],
        team=list(rec.get("team", []) or []),
        region=rec.get("region"),
        links=[str(u) for u in (rec.get("links") or [])],
        consent_public=bool(rec.get("consent_public", False)),
        readiness_score=int(rec.get("readiness_score", 0)),
        created_at=str(rec.get("created_at")),
        updated_at=str(rec.get("updated_at")),
        cover_url=rec.get("cover_url"),
    )

def _load_proposal(session: Session, proposal_id: str) -> dict:
    rec = session.run(CYPHER_GET_PROPOSAL, id=proposal_id).single()
    if not rec:
        raise HTTPException(status_code=404, detail="Proposal not found")
    return dict(rec["p"])

def _is_owner(session: Session, proposal_id: str, email: Optional[str]) -> bool:
    if not email:
        return False
    row = session.run(CYPHER_OWNER_CHECK, proposal_id=proposal_id, email=email.lower()).single()
    return bool(row)

# ------------------------------------------------------------------------------#
# Endpoints
# ------------------------------------------------------------------------------#
@router.post("/proposals", status_code=201)
def create_proposal(
    body: ProposalCreate,
    request: Request,
    session: Session = Depends(session_dep),
):
    proposal_id = str(uuid.uuid4())
    slug = _slugify(body.title)
    now = _now_iso()

    auth_email = (request.headers.get("X-User-Email") or "").lower().strip() or None
    contact_email = str(body.contact_email).lower()

    try:
        rec = session.run(
            """
            MERGE (contact:User {email: $contact_email})
            ON CREATE SET contact.id = coalesce(contact.id, randomUUID()), contact.created_at = timestamp()
            WITH contact
            CREATE (p:ProjectProposal {
              id: $id, slug: $slug, title: $title, one_liner: $one_liner,
              category: $category, needs: $needs, impact_summary: $impact_summary,
              region: $region, links: $links, consent_public: $consent_public,
              status: 'new', readiness_score: 0, created_at: datetime($now), updated_at: datetime($now),
              cover_url: $cover_url
            })
            MERGE (contact)-[:SUBMITTED]->(p)
            WITH p, $auth_email AS auth_email
            FOREACH (_ IN CASE WHEN auth_email IS NULL OR auth_email = '' THEN [] ELSE [1] END |
              MERGE (auth:User {email: auth_email})
              ON CREATE SET auth.id = coalesce(auth.id, randomUUID()), auth.created_at = timestamp()
              MERGE (auth)-[:OWNS]->(p)
            )
            RETURN p
            """,
            id=proposal_id,
            slug=slug,
            title=body.title,
            one_liner=body.one_liner,
            category=body.category,
            needs=list(body.needs),
            impact_summary=body.impact_summary,
            region=body.region,
            links=[str(u) for u in (body.links or [])],
            consent_public=bool(body.consent_public),
            contact_email=contact_email,
            auth_email=auth_email,
            now=now,
            cover_url=str(body.cover_url) if body.cover_url else None,
        ).single()
    except Neo4jError:
        raise HTTPException(status_code=500, detail="Database error while saving your proposal. Please try again.")

    if not rec:
        raise HTTPException(status_code=500, detail="Failed to create proposal. Please try again.")

    token = mint_owner_token(proposal_id)
    owner_url = f"/launchpad/p/{slug}?id={proposal_id}&owner_token={token}"

    return {"id": proposal_id, "slug": slug, "owner_token": token, "owner_url": owner_url, "status": "new"}

class ClaimBody(BaseModel):
    owner_token: Optional[str] = None
# --- ADMIN: list proposals -----------------------------------------------------
from typing import Optional, List, Dict, Any
from fastapi import Depends, Query, HTTPException
from neo4j import Session

# Cypher for admin list (adjust to your schema):
CYPHER_ADMIN_LIST = """
MATCH (p:ProjectProposal)
OPTIONAL MATCH (p)<-[:OWNS]-(u:User)
WITH p, collect(DISTINCT u.email) AS owners
OPTIONAL MATCH (f:Follow)-[:FOR]->(p)
WITH p, owners, count(f) AS followers
OPTIONAL MATCH (a:Applaud)-[:FOR]->(p)
WITH p, owners, followers, count(a) AS applause
WHERE ($status IS NULL OR p.status = $status)
  AND (
    $q IS NULL OR $q = '' OR
    toLower(p.title) CONTAINS toLower($q) OR
    toLower(p.one_liner) CONTAINS toLower($q) OR
    toLower(coalesce(p.region,'')) CONTAINS toLower($q)
  )
RETURN p {
  .id, .slug, .title, .one_liner, .category, .region, .status,
  .readiness_score, .created_at, .updated_at, .cover_url
} AS p,
owners AS owners,
followers AS followers,
applause AS applause
ORDER BY p.updated_at DESC
SKIP $skip
LIMIT $limit
"""

@router.get("/proposals/admin")
async def admin_list_proposals(
    # ✅ this is the only auth path: header -> require_admin
    admin_email: str = Depends(require_admin),
    status: Optional[str] = Query(default=None),
    q: Optional[str] = Query(default=None),
    skip: int = Query(default=0, ge=0),
    limit: int = Query(default=100, ge=1, le=200),
    session: Session = Depends(session_dep),
):
    # Optionally use admin_email for auditing/logging
    rows = session.run(
        CYPHER_ADMIN_LIST,
        status=status,
        q=q,
        skip=skip,
        limit=limit,
    ).data()

    out: List[Dict[str, Any]] = []
    for r in rows:
        p = r["p"]
        out.append({
            "id": p["id"],
            "slug": p["slug"],
            "title": p["title"],
            "one_liner": p["one_liner"],
            "category": p["category"],
            "region": p.get("region"),
            "status": p["status"],
            "readiness_score": int(p.get("readiness_score", 0)),
            "created_at": str(p.get("created_at")),
            "updated_at": str(p.get("updated_at")),
            "owners": r.get("owners") or [],
            "followers": int(r.get("followers") or 0),
            "applause": int(r.get("applause") or 0),
            "cover_url": p.get("cover_url"),
        })
    return out

# --------------------------- Admin REQUIRED endpoints --------------------------
@router.post("/proposals/status")
async def set_status(
    body: StatusChange,
    _admin_email: str = Depends(_require_admin_from_any),
    session: Session = Depends(session_dep),
):
    rec = session.run(
        CYPHER_STATUS_CHANGE, proposal_id=body.proposal_id, status=body.status, now=_now_iso()
    ).single()
    if not rec:
        raise HTTPException(status_code=404, detail="Proposal not found")
    return {"ok": True, "status": body.status}

@router.post("/reviews")
async def create_review(
    body: ReviewCreate,
    admin_email: str = Depends(_require_admin_from_any),
    session: Session = Depends(session_dep),
):
    now = _now_iso()
    review_id = str(uuid.uuid4())
    avg = body.scores.avg
    rec = session.run(
        """
        MATCH (p:ProjectProposal {id: $proposal_id})
        WITH p
        CREATE (r:Review {
          id: $id,
          clarity: $scores.clarity,
          feasibility: $scores.feasibility,
          alignment: $scores.alignment,
          impact: $scores.impact,
          commitment: $scores.commitment,
          notes: $notes,
          avg: $avg,
          created_at: datetime($now)
        })
        MERGE (p)<-[:FOR]-(r)
        WITH p, r
        MERGE (admin:User {email: $admin_email})
        MERGE (admin)-[:REVIEWED]->(r)
        RETURN r
        """,
        proposal_id=body.proposal_id,
        id=review_id,
        scores=body.scores.model_dump(),
        notes=body.notes,
        avg=avg,
        now=now,
        admin_email=admin_email.lower(),
    ).single()
    if not rec:
        raise HTTPException(status_code=404, detail="Proposal not found")
    return {"ok": True, "review_id": review_id, "avg": avg}

@router.post("/proposals/{proposal_id}/claim")
async def claim_proposal(
    proposal_id: str,
    body: ClaimBody,
    admin_email: str = Depends(_require_admin_from_any),
    session: Session = Depends(session_dep),
):
    if body.owner_token:
        sub = verify_owner_token(body.owner_token)
        if sub != proposal_id:
            raise HTTPException(status_code=403, detail="That owner link is for another proposal")
    rec = session.run(
        """
        MATCH (p:ProjectProposal {id: $proposal_id})
        MERGE (u:User {email: $user_email})
        ON CREATE SET u.id = coalesce(u.id, randomUUID()), u.created_at = timestamp()
        MERGE (u)-[:OWNS]->(p)
        RETURN p
        """,
        proposal_id=proposal_id,
        user_email=admin_email.lower(),
    ).single()
    if not rec:
        raise HTTPException(status_code=404, detail="Proposal not found")
    return {"ok": True}

# --------------------------- Admin OR Owner endpoints --------------------------
@router.post("/proposals/{proposal_id}/owner_link")
async def mint_owner_link(
    proposal_id: str,
    request: Request,
    x_auth_token: Optional[str] = Header(default=None, alias="X-Auth-Token"),
    session: Session = Depends(session_dep),
):
    email = (request.headers.get("X-User-Email") or "").lower().strip() or None
    is_admin = False
    if x_auth_token:
        try:
            await require_admin(x_auth_token)
            is_admin = True
        except HTTPException:
            is_admin = False

    if not is_admin and not _is_owner(session, proposal_id, email):
        raise HTTPException(status_code=401, detail="Provide an owner link or be signed-in as an owner")

    p = _load_proposal(session, proposal_id)
    token = mint_owner_token(proposal_id)
    owner_url = f"/launchpad/p/{p['slug']}?id={proposal_id}&owner_token={token}"
    return {"owner_token": token, "owner_url": owner_url}

@router.get("/proposals/{proposal_id}")
async def get_owner_proposal(
    proposal_id: str,
    request: Request,
    x_owner_token: Optional[str] = Header(default=None, convert_underscores=False),
    x_auth_token: Optional[str] = Header(default=None, alias="X-Auth-Token"),
    session: Session = Depends(session_dep),
):
    owner_tok = _extract_owner_token(request, x_owner_token)
    if owner_tok:
        sub = verify_owner_token(owner_tok)
        if sub != proposal_id:
            raise HTTPException(status_code=403, detail="Your owner link is for a different proposal")
    else:
        admin_tok = _extract_admin_token(request, x_auth_token)
        if admin_tok:
            _ = await require_admin(admin_tok)
        else:
            email = (request.headers.get("X-User-Email") or "").lower().strip() or None
            if not _is_owner(session, proposal_id, email):
                raise HTTPException(status_code=401, detail="Provide an owner link or be signed-in as an owner")

    rec = _load_proposal(session, proposal_id)
    return _proposal_to_owner_view(rec)

@router.get("/proposals/resolve_slug/{slug}")
def resolve_slug(slug: str, session: Session = Depends(session_dep)):
    row = session.run(CYPHER_GET_BY_SLUG, slug=slug).single()
    if not row:
        raise HTTPException(status_code=404, detail="Proposal not found")
    return {"id": row["id"], "slug": row["slug"]}

@router.patch("/proposals/{proposal_id}")
async def patch_owner_proposal(
    proposal_id: str,
    body: ProposalUpdate,
    request: Request,
    x_owner_token: Optional[str] = Header(default=None, convert_underscores=False),
    x_auth_token: Optional[str] = Header(default=None, alias="X-Auth-Token"),
    session: Session = Depends(session_dep),
):
    owner_tok = _extract_owner_token(request, x_owner_token)
    if owner_tok:
        sub = verify_owner_token(owner_tok)
        if sub != proposal_id:
            raise HTTPException(status_code=403, detail="Your owner link is for a different proposal")
    else:
        admin_tok = _extract_admin_token(request, x_auth_token)
        if admin_tok:
            _ = await require_admin(admin_tok)
        else:
            email = (request.headers.get("X-User-Email") or "").lower().strip() or None
            if not _is_owner(session, proposal_id, email):
                raise HTTPException(status_code=401, detail="Provide an owner link or be signed-in as an owner")

    current = _load_proposal(session, proposal_id)
    patch = body.model_dump(exclude_none=True)
    patch = _sanitize_patch_dict(patch)
    merged = {**current, **patch}
    readiness = compute_readiness(merged)

    rec = session.run(
        CYPHER_PATCH_PROPOSAL, id=proposal_id, patch=patch, readiness=readiness, now=_now_iso()
    ).single()

    if not rec:
        raise HTTPException(status_code=500, detail="Failed to update proposal. Please try again.")
    return _proposal_to_owner_view(dict(rec["p"]))

@router.post("/proposals/{proposal_id}/request_review")
async def request_review(
    proposal_id: str,
    request: Request,
    x_owner_token: Optional[str] = Header(default=None, convert_underscores=False),
    x_auth_token: Optional[str] = Header(default=None, alias="X-Auth-Token"),
    session: Session = Depends(session_dep),
):
    owner_tok = _extract_owner_token(request, x_owner_token)
    if owner_tok:
        sub = verify_owner_token(owner_tok)
        if sub != proposal_id:
            raise HTTPException(status_code=403, detail="Your owner link is for a different proposal")
    else:
        admin_tok = _extract_admin_token(request, x_auth_token)
        if admin_tok:
            _ = await require_admin(admin_tok)
        else:
            email = (request.headers.get("X-User-Email") or "").lower().strip() or None
            if not _is_owner(session, proposal_id, email):
                raise HTTPException(status_code=401, detail="Provide an owner link or be signed-in as an owner")

    rec = session.run(CYPHER_REQUEST_REVIEW, id=proposal_id, now=_now_iso()).single()
    if not rec:
        raise HTTPException(status_code=404, detail="Proposal not found")
    return {"ok": True, "status": "triage"}

# ------------------------------ Public lists ----------------------------------#
@router.get("/public", response_model=List[PublicCard])
def public_list(
    skip: int = Query(0, ge=0),
    limit: int = Query(24, ge=1, le=48),
    session: Session = Depends(session_dep),
):
    rows = session.run(CYPHER_PUBLIC_LIST, skip=skip, limit=limit).data()
    out: List[PublicCard] = []
    for r in rows:
        p = r["p"]
        out.append(
            PublicCard(
                id=p["id"], slug=p["slug"], title=p["title"], one_liner=p["one_liner"],
                category=p["category"], region=p.get("region"), status=p["status"],
                applause=int(p.get("applause", 0)), followers=int(p.get("followers", 0)),
                cover_url=p.get("cover_url"),
            )
        )
    return out

@router.get("/public/trending", response_model=List[PublicCard])
def public_trending(
    days: int = Query(21, ge=1, le=60),
    skip: int = Query(0, ge=0),
    limit: int = Query(24, ge=1, le=48),
    session: Session = Depends(session_dep),
):
    since = (dt.datetime.utcnow() - dt.timedelta(days=days)).isoformat() + "Z"
    rows = session.run(CYPHER_PUBLIC_TRENDING, since=since, skip=skip, limit=limit).data()
    out: List[PublicCard] = []
    for r in rows:
        p = r["row"]
        out.append(
            PublicCard(
                id=p["id"], slug=p["slug"], title=p["title"], one_liner=p["one_liner"],
                category=p["category"], region=p.get("region"), status=p["status"],
                applause=int(p.get("applause", 0)), followers=int(p.get("followers", 0)),
                cover_url=p.get("cover_url"),
            )
        )
    return out

# ----------------------------- Social signals ---------------------------------#
@router.post("/proposals/follow")
def follow(
    body: FollowRequest,
    request: Request,
    session: Session = Depends(session_dep),
):
    ip = request.client.host if request.client else "0.0.0.0"
    _check_rate_limit(ip, body.client_fingerprint, "follow", body.proposal_id)

    session.run(
        CYPHER_FOLLOW_FOREACH,
        proposal_id=body.proposal_id,
        id=str(uuid.uuid4()),
        now=_now_iso(),
        email=(body.email.lower() if body.email else None),
    )
    stats = session.run(
        """
        MATCH (p:ProjectProposal {id: $id})
        OPTIONAL MATCH (f:Follow)-[:FOR]->(p)
        WITH p, count(f) AS followers
        OPTIONAL MATCH (a:Applaud)-[:FOR]->(p)
        WITH p, followers, count(a) AS applause
        RETURN followers AS followers, applause AS applause
        """,
        id=body.proposal_id,
    ).single()

    return {"ok": True, "followers": int(stats["followers"]), "applause": int(stats["applause"])}

@router.post("/proposals/applaud")
def applaud(
    body: ApplaudRequest,
    request: Request,
    session: Session = Depends(session_dep),
):
    ip = request.client.host if request.client else "0.0.0.0"
    _check_rate_limit(ip, body.client_fingerprint, "applaud", body.proposal_id)

    if body.email:
        session.run(
            CYPHER_APPLAUD_EMAIL_DEDUPE,
            proposal_id=body.proposal_id,
            id=str(uuid.uuid4()),
            now=_now_iso(),
            email=body.email.lower(),
        )
    else:
        session.run(
            CYPHER_APPLAUD_ANON,
            proposal_id=body.proposal_id,
            id=str(uuid.uuid4()),
            now=_now_iso(),
        )

    stats = session.run(
        """
        MATCH (p:ProjectProposal {id: $id})
        OPTIONAL MATCH (f:Follow)-[:FOR]->(p)
        WITH p, count(f) AS followers
        OPTIONAL MATCH (a:Applaud)-[:FOR]->(p)
        WITH p, followers, count(a) AS applause
        RETURN followers AS followers, applause AS applause
        """,
        id=body.proposal_id,
    ).single()

    return {"ok": True, "followers": int(stats["followers"]), "applause": int(stats["applause"])}

# ------------------------------ How it works ----------------------------------#
class HowItWorksStep(BaseModel):
    title: str
    body: str
    icon: Optional[str] = None

@router.get("/how_it_works", response_model=List[HowItWorksStep])
def how_it_works():
    return [
        HowItWorksStep(title="Plant your idea", body="Share a 60-second seed: title, one-liner, what you need. We’ll create your private workspace.", icon="🌱"),
        HowItWorksStep(title="Grow your proposal", body="Use your workspace to add milestones, evidence, and your team. A readiness meter guides you.", icon="🪴"),
        HowItWorksStep(title="Request triage", body="When ready, tap Request Review. We review for clarity, feasibility, impact and alignment.", icon="🧭"),
        HowItWorksStep(title="Enter the Greenhouse", body="Accepted projects join the curated Garden with follow and applaud signals. We can help with dev, design, and outreach.", icon="🏡"),
        HowItWorksStep(title="Incubation & launch", body="Select projects move to hands-on incubation with EOS/Wattle integrations, storytelling, and seasonal showcases.", icon="🚀"),
    ]
