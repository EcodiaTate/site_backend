# site_backend/api/auth_dep.py
from __future__ import annotations

import os
import base64
from typing import Optional, Tuple

import jwt  # PyJWT
from fastapi import HTTPException, Request, Depends
from neo4j import Session

from site_backend.core.neo_driver import session_dep

# This app's identity tag (kept for symmetry; not used for graph writes here)
SITE_NAME = os.getenv("SITE_NAME", "ecodia")

# JWT verification envs (RS256 preferred; HS256 for dev)
raw = os.getenv("ECODIA_JWT_PUBLIC_KEY", "")
JWT_PUBLIC_KEY = raw.replace("\\n", "\n") if raw else None
JWT_SECRET     = os.getenv("ECODIA_JWT_SECRET", "dev-secret")
JWT_ALGS       = (os.getenv("ECODIA_JWT_ALG") or "RS256").split(",")

# Optional stricter verification (set if you use them)
JWT_ISS = os.getenv("ECODIA_JWT_ISS")       # e.g., "https://ecodia.au"
JWT_AUD = os.getenv("ECODIA_JWT_AUD")       # e.g., "ecodia-site"


def _extract_bearer(req: Request) -> Optional[str]:
    auth = req.headers.get("authorization") or req.headers.get("Authorization")
    if not auth or not auth.lower().startswith("bearer "):
        return None
    return auth.split(" ", 1)[1].strip() or None


def _normalize_pem(val: Optional[str]) -> Optional[str]:
    """Accept PEM, base64-encoded PEM, or \\n-escaped PEM from env."""
    if not val:
        return None
    v = val.strip()
    try:
        if not v.startswith("-----BEGIN"):
            return base64.b64decode(v, validate=True).decode()
    except Exception:
        pass
    return v.replace("\\n", "\n")


def _uid_from_token_or_header(request: Request) -> Tuple[Optional[str], Optional[str]]:
    """
    Return (uid, email) for current caller.
    Prefer Bearer JWT; fallback to X-User-Id / X-User-Email for dev.
    """
    token = _extract_bearer(request)
    if token:
        try:
            key = _normalize_pem(JWT_PUBLIC_KEY)
            options = {"verify_aud": bool(JWT_AUD)}
            kwargs = {}
            if JWT_ISS:
                kwargs["issuer"] = JWT_ISS
            if JWT_AUD:
                kwargs["audience"] = JWT_AUD

            if key:
                claims = jwt.decode(token, key, algorithms=JWT_ALGS, options=options, **kwargs)
            else:
                # HS256 fallback for local/dev
                claims = jwt.decode(token, JWT_SECRET, algorithms=["HS256"], options=options, **kwargs)

            uid = str(claims.get("sub") or claims.get("uid") or claims.get("user_id") or "")
            email = claims.get("email") or claims.get("user_email")
            if uid:
                return uid, email
        except Exception:
            # Fall through to header-based dev identity
            pass

    uid = request.headers.get("X-User-Id")
    email = request.headers.get("X-User-Email")
    return (uid, email) if uid else (None, None)


def _ensure_person(s: Session, *, uid: str, email: Optional[str]) -> str:
    """
    Single-source identity in Neo4j without Account nodes.
    - Prefer existing Person by uid; else by primary_email; else create.
    - If found, backfill missing primary_email/uid.
    - Do NOT merge two existing Persons here; linking flow handles cross-site merges.
    """
    rec = s.run(
        """
        OPTIONAL MATCH (p1:Person {uid:$uid})
        OPTIONAL MATCH (p2:Person {primary_email:$email})
        WITH coalesce(p1, p2) AS p, $uid AS uid, $email AS email
        CALL apoc.do.when(
          p IS NULL,
          'CREATE (np:Person {pid: randomUUID(), uid:$uid, primary_email:$email, created_at: datetime()})
           RETURN np.pid AS pid',
          'SET p.uid = coalesce(p.uid, $uid)
           SET p.primary_email = coalesce(p.primary_email, $email)
           RETURN p.pid AS pid',
          {uid:uid, email:email, p:p}
        ) YIELD value
        RETURN value.pid AS pid
        """,
        {"uid": uid, "email": email},
    ).single()
    return rec["pid"]


def auth_person_pid(request: Request, s: Session = Depends(session_dep)) -> str:
    uid, email = _uid_from_token_or_header(request)
    if not uid:
        raise HTTPException(401, "Unauthenticated")
    pid = _ensure_person(s, uid=uid, email=email)
    if not pid:
        raise HTTPException(401, "Could not resolve Person")
    return pid
