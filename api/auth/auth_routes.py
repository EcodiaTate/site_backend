from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Literal, Optional, List
import os
import uuid

from fastapi import APIRouter, HTTPException, status, Depends, Header
from pydantic import BaseModel, EmailStr
from jose import jwt, JWTError
from neo4j import Session
from passlib.context import CryptContext

from site_backend.core.neo_driver import session_dep

# -------------------------------------------------------------------
# Config – only JWT_SECRET / JWT_ALGO + optional ISS/AUD
# -------------------------------------------------------------------

JWT_SECRET = os.getenv("JWT_SECRET", "dev-secret-change-me")
JWT_ALGO = os.getenv("JWT_ALGO", "HS256")

# Access + refresh both use the same secret/algo
ACCESS_JWT_SECRET = JWT_SECRET
REFRESH_JWT_SECRET = JWT_SECRET
ACCESS_JWT_ALGO = JWT_ALGO
REFRESH_JWT_ALGO = JWT_ALGO

# Optional ISS/AUD
ACCESS_JWT_ISS = os.getenv("ACCESS_JWT_ISS")  # optional
ACCESS_JWT_AUD = os.getenv("ACCESS_JWT_AUD")  # optional

ACCESS_TOKEN_MINUTES = int(os.getenv("ACCESS_TOKEN_MINUTES", "15"))
REFRESH_TOKEN_DAYS = int(os.getenv("REFRESH_TOKEN_DAYS", "30"))

# Admin email(s) – supports old ADMIN_EMAIL plus newer comma list ADMIN_EMAILS
_admin_email_single = (os.getenv("ADMIN_EMAIL") or "").strip().lower()
_admin_email_list = [
    e.strip().lower()
    for e in os.getenv("ADMIN_EMAILS", "").split(",")
    if e.strip()
]

ADMIN_EMAILS: List[str] = sorted(
    {
        e
        for e in (
            _admin_email_list
            + ([_admin_email_single] if _admin_email_single else [])
        )
        if e
    }
)

Role = Literal["youth", "business", "creative", "partner", "admin"]

# -------------------------------------------------------------------
# Password hashing – use pbkdf2_sha256 (no bcrypt bugs / 72-byte limit)
# -------------------------------------------------------------------

# replace current pwd_context line
pwd_context = CryptContext(
    schemes=["pbkdf2_sha256", "argon2"],  # pbkdf2 as default; still verify argon2
    deprecated="auto",
)

def _check_password(session: Session, email: str, plain_password: str) -> bool:
    hashed = _get_password_hash(session, email)
    if not hashed:
        # legacy records with no hash: keep allowing (until you finish migration),
        # then flip this to `return False`.
        return True

    try:
        ok = pwd_context.verify(plain_password, hashed)
    except Exception:
        return False

    if ok and pwd_context.needs_update(hashed):
        # transparently rehash to pbkdf2 on next successful login
        _set_password_hash(session, email, plain_password)
    return ok

# -------------------------------------------------------------------
# Models
# -------------------------------------------------------------------

class UserOut(BaseModel):
    id: str
    email: EmailStr
    display_name: str
    role: Role
    avatar_url: Optional[str] = None

    legal_onboarding_complete: bool = False
    tos_accepted_at: Optional[datetime] = None
    privacy_accepted_at: Optional[datetime] = None
    over18_confirmed: bool = False

    @property
    def is_admin(self) -> bool:
        email = (self.email or "").lower()
        return self.role == "admin" or email in ADMIN_EMAILS


class BackendTokens(BaseModel):
    access_token: str
    refresh_token: str
    access_expires_at: int  # Unix epoch seconds


class LoginResponse(BaseModel):
    user: UserOut
    backend_tokens: BackendTokens


class LoginRequest(BaseModel):
    email: EmailStr
    password: str


class RegisterRequest(BaseModel):
    email: EmailStr
    password: str
    display_name: Optional[str] = None
    role: Optional[Role] = "youth"


class SSOExchangeRequest(BaseModel):
    email: EmailStr
    display_name: Optional[str] = None
    image_url: Optional[str] = None
    default_role: Optional[Role] = "youth"


class RefreshRequest(BaseModel):
    refresh_token: str


class RefreshResponse(BaseModel):
    access_token: str
    access_expires_at: int


class AcceptLegalRequest(BaseModel):
    tos_version: str = "v1"
    over18_confirmed: bool = False

# site_backend/api/auth/auth_routes.py
from datetime import datetime, timedelta, timezone
# add (near the other imports)
try:
    from neo4j.time import DateTime as NeoDateTime
except Exception:
    NeoDateTime = None

def _to_py_dt(v):
    """Coerce Neo4j DateTime or ISO string to Python datetime (UTC)."""
    if v is None:
        return None
    if isinstance(v, datetime):
        return v
    # Neo4j driver returns neo4j.time.DateTime which has .to_native()
    if NeoDateTime and isinstance(v, NeoDateTime):
        return v.to_native()
    if isinstance(v, str):
        try:
            # handle '...Z'
            if v.endswith("Z"):
                v = v[:-1] + "+00:00"
            return datetime.fromisoformat(v)
        except Exception:
            return None
    return None

# -------------------------------------------------------------------
# DB helpers – Neo4j-based, no cookies
# -------------------------------------------------------------------
def _row_to_user_out(row: dict) -> UserOut:
    return UserOut(
        id=row["id"],
        email=row["email"],
        display_name=row["display_name"],
        role=row["role"],
        avatar_url=row.get("avatar_url"),
        legal_onboarding_complete=bool(row.get("legal_onboarding_complete", False)),
        tos_accepted_at=_to_py_dt(row.get("tos_accepted_at")),
        privacy_accepted_at=_to_py_dt(row.get("privacy_accepted_at")),
        over18_confirmed=bool(row.get("over18_confirmed", False)),
    )



def _load_user_by_email(session: Session, email: str) -> Optional[UserOut]:
    """
    Case-insensitive lookup of a user by email in Neo4j.
    """
    rec = session.run(
        """
        MATCH (u:User)
        WHERE toLower(u.email) = $email
        RETURN
          coalesce(u.id, u.uid)                        AS id,
          toLower(u.email)                             AS email,
          coalesce(u.display_name, u.email)            AS display_name,
          coalesce(u.role, 'youth')                    AS role,
          u.avatar_url                                 AS avatar_url,
          coalesce(u.legal_onboarding_complete,false)  AS legal_onboarding_complete,
          u.tos_accepted_at                            AS tos_accepted_at,
          u.privacy_accepted_at                        AS privacy_accepted_at,
          coalesce(u.over18_confirmed,false)           AS over18_confirmed
        """,
        {"email": email.lower()},
    ).single()

    if not rec:
        return None

    return _row_to_user_out(rec.data())


def _get_password_hash(session: Session, email: str) -> Optional[str]:
    rec = session.run(
        """
        MATCH (u:User)
        WHERE toLower(u.email) = $email
        RETURN u.password_hash AS password_hash
        """,
        {"email": email.lower()},
    ).single()
    if not rec:
        return None
    return rec.get("password_hash")


def _set_password_hash(session: Session, email: str, plain_password: str) -> None:
    hashed = pwd_context.hash(plain_password)
    session.run(
        """
        MATCH (u:User)
        WHERE toLower(u.email) = $email
        SET u.password_hash = $password_hash
        """,
        {"email": email.lower(), "password_hash": hashed},
    )


def _check_password(session: Session, email: str, plain_password: str) -> bool:
    """
    Password check with **legacy fallback**:
    - If password_hash exists → verify via pbkdf2_sha256
    - If password_hash is missing → allow login (for old records) for now.
    """
    hashed = _get_password_hash(session, email)
    if not hashed:
        # DEV / legacy: no hash stored yet → don't block login.
        # Once you've migrated data, flip this to `return False`.
        return True

    try:
        return pwd_context.verify(plain_password, hashed)
    except Exception:
        return False


def _create_user_in_db(
    session: Session,
    *,
    email: str,
    password: str,
    display_name: Optional[str],
    role: Role,
    avatar_url: Optional[str] = None,
) -> UserOut:
    """
    Create a new User node with a hashed password and return UserOut.
    """
    user_id = str(uuid.uuid4())
    pwd_hash = pwd_context.hash(password)
    display = display_name or email.split("@")[0]

    rec = session.run(
        """
        CREATE (u:User {
          id: $id,
          email: $email,
          display_name: $display_name,
          role: $role,
          avatar_url: $avatar_url,
          legal_onboarding_complete: false,
          tos_accepted_at: null,
          privacy_accepted_at: null,
          over18_confirmed: false,
          password_hash: $password_hash
        })
        RETURN
          u.id                              AS id,
          toLower(u.email)                  AS email,
          coalesce(u.display_name, u.email) AS display_name,
          coalesce(u.role, 'youth')         AS role,
          u.avatar_url                      AS avatar_url,
          coalesce(u.legal_onboarding_complete, false) AS legal_onboarding_complete,
          u.tos_accepted_at                 AS tos_accepted_at,
          u.privacy_accepted_at             AS privacy_accepted_at,
          coalesce(u.over18_confirmed, false) AS over18_confirmed
        """,
        {
            "id": user_id,
            "email": email.lower(),
            "display_name": display,
            "role": role,
            "avatar_url": avatar_url,
            "password_hash": pwd_hash,
        },
    ).single()

    if not rec:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to create user",
        )

    return _row_to_user_out(rec.data())


# -------------------------------------------------------------------
# JWT helpers (no cookies)
# -------------------------------------------------------------------

def _now() -> datetime:
    return datetime.now(timezone.utc)


def _encode_access_payload(user: UserOut, exp: datetime) -> dict:
    payload: dict = {
        "sub": user.id,
        "email": user.email,
        "role": user.role,
        "legal_onboarding_complete": user.legal_onboarding_complete,
        "exp": exp,
    }
    if ACCESS_JWT_ISS:
        payload["iss"] = ACCESS_JWT_ISS
    if ACCESS_JWT_AUD:
        payload["aud"] = ACCESS_JWT_AUD
    return payload


def create_access_and_refresh(user: UserOut) -> BackendTokens:
    now = _now()
    access_exp = now + timedelta(minutes=ACCESS_TOKEN_MINUTES)
    refresh_exp = now + timedelta(days=REFRESH_TOKEN_DAYS)

    access_payload = _encode_access_payload(user, access_exp)
    refresh_payload = {
        "sub": user.id,
        "email": user.email,
        "role": user.role,
        "legal_onboarding_complete": user.legal_onboarding_complete,
        "exp": refresh_exp,
        "type": "refresh",
    }

    access_token = jwt.encode(
        access_payload, ACCESS_JWT_SECRET, algorithm=ACCESS_JWT_ALGO
    )
    refresh_token = jwt.encode(
        refresh_payload, REFRESH_JWT_SECRET, algorithm=REFRESH_JWT_ALGO
    )

    return BackendTokens(
        access_token=access_token,
        refresh_token=refresh_token,
        access_expires_at=int(access_exp.timestamp()),
    )


def rotate_access_token(refresh_token: str) -> RefreshResponse:
    try:
        payload = jwt.decode(
            refresh_token,
            REFRESH_JWT_SECRET,
            algorithms=[REFRESH_JWT_ALGO],
        )
    except JWTError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid refresh token",
        )

    if payload.get("type") != "refresh":
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid refresh token type",
        )

    now = _now()
    user_id = payload.get("sub")
    email = payload.get("email")
    role = payload.get("role")
    legal = bool(payload.get("legal_onboarding_complete", False))

    if not user_id or not email or not role:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Malformed refresh token",
        )

    access_exp = now + timedelta(minutes=ACCESS_TOKEN_MINUTES)
    access_payload = _encode_access_payload(
        UserOut(
            id=str(user_id),
            email=email,
            display_name=str(email).split("@")[0],
            role=role,
            avatar_url=None,
            legal_onboarding_complete=legal,
        ),
        access_exp,
    )

    access_token = jwt.encode(
        access_payload, ACCESS_JWT_SECRET, algorithm=ACCESS_JWT_ALGO
    )

    return RefreshResponse(
        access_token=access_token,
        access_expires_at=int(access_exp.timestamp()),
    )


# -------------------------------------------------------------------
# Current user + admin guard (Authorization header only)
# -------------------------------------------------------------------

def _extract_bearer(authorization: Optional[str]) -> Optional[str]:
    if not authorization:
        return None
    auth = authorization.strip()
    if not auth:
        return None
    lower = auth.lower()
    for prefix in ("bearer ", "jwt ", "token "):
        if lower.startswith(prefix):
            return auth[len(prefix):].strip()
    if auth.count(".") == 2:
        return auth
    return None


async def get_current_user(
    authorization: Optional[str] = Header(None),
    session: Session = Depends(session_dep),
) -> UserOut:
    token_str = _extract_bearer(authorization)
    if not token_str:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing Authorization token",
        )

    try:
        options = {"verify_aud": bool(ACCESS_JWT_AUD)}
        kwargs = {}
        if ACCESS_JWT_ISS:
            kwargs["issuer"] = ACCESS_JWT_ISS
        if ACCESS_JWT_AUD:
            kwargs["audience"] = ACCESS_JWT_AUD

        payload = jwt.decode(
            token_str,
            ACCESS_JWT_SECRET,
            algorithms=[ACCESS_JWT_ALGO],
            options=options,
            **kwargs,
        )
    except JWTError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid access token",
        )

    user_id = payload.get("sub")
    email = payload.get("email")
    role = payload.get("role")
    legal = bool(payload.get("legal_onboarding_complete", False))

    if not user_id or not email or not role:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Malformed access token",
        )

    db_user = _load_user_by_email(session, email)
    if db_user:
        return db_user

    # Fallback to token info if DB lookup fails (should be rare)
    return UserOut(
        id=str(user_id),
        email=email,
        display_name=(str(email).split("@")[0] if email else str(user_id)),
        role=role,
        avatar_url=None,
        legal_onboarding_complete=legal,
        tos_accepted_at=None,
        privacy_accepted_at=None,
        over18_confirmed=False,
    )


async def require_admin(user: UserOut = Depends(get_current_user)) -> UserOut:
    if not user.is_admin:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Admin access required",
        )
    return user


# -------------------------------------------------------------------
# Routes – pure JSON (server-side token exchange)
# -------------------------------------------------------------------

router = APIRouter(prefix="/auth", tags=["auth"])
from fastapi import APIRouter, HTTPException, status, Depends, Header, Request, Form
from pydantic import BaseModel, EmailStr
# site_backend/api/auth/auth_routes.py
from fastapi import Request

class LoginRequest(BaseModel):
    email: EmailStr
    password: str

@router.post("/login", response_model=LoginResponse)
async def login(
    request: Request,
    session: Session = Depends(session_dep),
) -> LoginResponse:
    # 1) Try JSON
    data = None
    try:
        data = await request.json()
        if not isinstance(data, dict):
            data = None
    except Exception:
        data = None

    # 2) Fallback to form-encoded (Credentials providers often send this)
    if data is None:
        form = await request.form()
        data = {"email": form.get("email"), "password": form.get("password")}

    # 3) Normalize & validate (avoid 422 on minor issues)
    email = (data.get("email") or "").strip().lower()
    password = (data.get("password") or "").strip()
    if not email or not password:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Email & password required")

    # If you depend on EmailStr validation, check explicitly:
    try:
        _ = LoginRequest(email=email, password=password)
    except Exception:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid email/password format")

    # 4) Proceed with auth
    user = _load_user_by_email(session, email)
    if not user or not _check_password(session, email, password):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid email or password")

    tokens = create_access_and_refresh(user)
    return LoginResponse(user=user, backend_tokens=tokens)


@router.post("/register", response_model=LoginResponse)
async def register(
    payload: RegisterRequest,
    session: Session = Depends(session_dep),
) -> LoginResponse:
    existing = _load_user_by_email(session, payload.email)
    if existing:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Email already registered",
        )

    user = _create_user_in_db(
        session,
        email=payload.email,
        password=payload.password,
        display_name=payload.display_name,
        role=payload.role or "youth",
    )
    tokens = create_access_and_refresh(user)
    return LoginResponse(user=user, backend_tokens=tokens)


@router.post("/sso-exchange", response_model=LoginResponse)
async def sso_exchange(
    payload: SSOExchangeRequest,
    session: Session = Depends(session_dep),
) -> LoginResponse:
    existing = _load_user_by_email(session, payload.email)

    if existing:
        user = existing
    else:
        # SSO users get a random password (never used directly)
        user = _create_user_in_db(
            session,
            email=payload.email,
            password=str(uuid.uuid4()),
            display_name=payload.display_name or payload.email.split("@")[0],
            role=payload.default_role or "youth",
            avatar_url=payload.image_url,
        )

    tokens = create_access_and_refresh(user)
    return LoginResponse(user=user, backend_tokens=tokens)


@router.post("/refresh", response_model=RefreshResponse)
async def refresh(payload: RefreshRequest) -> RefreshResponse:
    return rotate_access_token(payload.refresh_token)


@router.post("/accept-legal", response_model=UserOut)
async def accept_legal(
    payload: AcceptLegalRequest,
    user: UserOut = Depends(get_current_user),
    session: Session = Depends(session_dep),
) -> UserOut:
    now = _now()

    # Persist to DB
    session.run(
        """
        MATCH (u:User {id: $id})
        SET u.legal_onboarding_complete = true,
            u.tos_accepted_at = datetime($now),
            u.privacy_accepted_at = datetime($now),
            u.over18_confirmed = $over18
        """,
        {
            "id": user.id,
            "now": now.isoformat(),
            "over18": payload.over18_confirmed,
        },
    )

    data = user.dict()
    data.update(
        {
            "legal_onboarding_complete": True,
            "tos_accepted_at": now,
            "privacy_accepted_at": now,
            "over18_confirmed": payload.over18_confirmed,
        }
    )
    updated = UserOut(**data)
    return updated

class RoleSnapshotRequest(BaseModel):
    role: Role

@router.post("/role-snapshot", response_model=LoginResponse)
async def role_snapshot(
    payload: RoleSnapshotRequest,
    user: UserOut = Depends(get_current_user),
    session: Session = Depends(session_dep),
) -> LoginResponse:
    """
    Update the user's role based on UI choice and return a fresh
    LoginResponse (user + backend_tokens) so the frontend can
    refresh its snapshot if it wants.
    """
    # Persist role change in Neo4j
    rec = session.run(
        """
        MATCH (u:User {id: $id})
        SET u.role = $role
        RETURN
          coalesce(u.id, u.uid)                        AS id,
          toLower(u.email)                             AS email,
          coalesce(u.display_name, u.email)            AS display_name,
          coalesce(u.role, 'youth')                    AS role,
          u.avatar_url                                 AS avatar_url,
          coalesce(u.legal_onboarding_complete,false)  AS legal_onboarding_complete,
          u.tos_accepted_at                            AS tos_accepted_at,
          u.privacy_accepted_at                        AS privacy_accepted_at,
          coalesce(u.over18_confirmed,false)           AS over18_confirmed
        """,
        {"id": user.id, "role": payload.role},
    ).single()

    if rec:
        updated_user = _row_to_user_out(rec.data())
    else:
        # Fallback (should be rare): update in-memory user only
        updated_user = user.copy(update={"role": payload.role})

    # Mint fresh tokens with the new role embedded
    tokens = create_access_and_refresh(updated_user)
    return LoginResponse(user=updated_user, backend_tokens=tokens)
