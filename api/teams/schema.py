# site_backend/routers/teams/schema.py
from __future__ import annotations
from typing import List, Literal, Optional
from pydantic import BaseModel, Field

Visibility = Literal["public", "private"]
MemberRole = Literal["owner", "admin", "member"]
RequestStatus = Literal["pending", "approved", "rejected", "cancelled", "expired"]

# ---------------- Team core ----------------
class Team(BaseModel):
    id: str
    name: str
    slug: str
    created_at: str
    join_code: str
    visibility: Visibility = "public"
    avatar_url: Optional[str] = None
    bio: Optional[str] = None
    max_members: int = 50
    # NEW:
    banner_url: Optional[str] = None
    theme_color: Optional[str] = None  # HEX string like "#7fd069"
    timezone: Optional[str] = None     # e.g. "Australia/Brisbane"
    lat: Optional[float] = None
    lng: Optional[float] = None
    tags: List[str] = Field(default_factory=list)
    rules_md: Optional[str] = None
    socials: Optional["TeamSocials"] = None
    allow_auto_join_public: bool = True            # public-only
    require_approval_private: bool = True          # private-only
    join_questions: List[str] = Field(default_factory=list)

class TeamSocials(BaseModel):
    website: Optional[str] = None
    instagram: Optional[str] = None
    facebook: Optional[str] = None
    x: Optional[str] = None
    discord: Optional[str] = None

Team.model_rebuild()

class TeamCreate(BaseModel):
    name: str
    slug: str
    visibility: Visibility = "public"
    avatar_url: Optional[str] = None
    bio: Optional[str] = None
    max_members: int = Field(default=50, ge=2, le=500)
    # NEW:
    banner_url: Optional[str] = None
    theme_color: Optional[str] = None
    timezone: Optional[str] = None
    lat: Optional[float] = None
    lng: Optional[float] = None
    tags: List[str] = Field(default_factory=list)
    rules_md: Optional[str] = None
    socials: Optional[TeamSocials] = None
    allow_auto_join_public: bool = True
    require_approval_private: bool = True
    join_questions: List[str] = Field(default_factory=list)

class TeamUpdate(BaseModel):
    name: Optional[str] = None
    slug: Optional[str] = None
    visibility: Optional[Visibility] = None
    avatar_url: Optional[str] = None
    bio: Optional[str] = None
    max_members: Optional[int] = Field(default=None, ge=2, le=500)
    # NEW:
    banner_url: Optional[str] = None
    theme_color: Optional[str] = None
    timezone: Optional[str] = None
    lat: Optional[float] = None
    lng: Optional[float] = None
    tags: Optional[List[str]] = None
    rules_md: Optional[str] = None
    socials: Optional[TeamSocials] = None
    allow_auto_join_public: Optional[bool] = None
    require_approval_private: Optional[bool] = None
    join_questions: Optional[List[str]] = None

class TeamMember(BaseModel):
    id: str
    role: MemberRole
    joined_at: str

class TeamDetail(BaseModel):
    team: Team
    members: List[TeamMember] = Field(default_factory=list)

# ---- Invites / Join Requests ----
class TeamInviteCreate(BaseModel):
    to_user_id: str

class TeamInvite(BaseModel):
    id: str
    team_id: str
    to_user_id: str
    from_user_id: str
    status: RequestStatus
    created_at: str

class JoinRequestCreate(BaseModel):
    team_id: str
    message: Optional[str] = None
    # NEW: optional answers to join questions
    answers: Optional[List[str]] = None

class JoinRequest(BaseModel):
    id: str
    team_id: str
    from_user_id: str
    status: RequestStatus
    created_at: str
    message: Optional[str] = None

# ---- Stats / Leaderboard / Feed ----
class TeamStats(BaseModel):
    team_id: str
    members_count: int
    eco_total: int
    eco_week: int
    eco_month: int
    submissions_approved: int

class TeamFeedItem(BaseModel):
    id: str
    at: str
    type: Literal[
        "submission_approved",
        "member_joined",
        "announcement_posted",
        "milestone_reached",
        "badge_awarded",
        "award_won",
    ]
    title: str
    eco_delta: int = 0
    by_user_id: Optional[str] = None
    submission_id: Optional[str] = None
    # NEW:
    ref_user_id: Optional[str] = None  # who was invited / announced, etc.

class TeamLeaderboardEntry(BaseModel):
    team_id: str
    team_name: str
    eco: int
    rank: int

class TeamLeaderboard(BaseModel):
    period: Literal["weekly", "monthly", "total"]
    rows: List[TeamLeaderboardEntry] = Field(default_factory=list)

# ---- NEW: Member leaderboard (within a team) ----
class MemberLeaderboardRow(BaseModel):
    user_id: str
    user_name: Optional[str] = None
    eco: int
    rank: int

class MemberLeaderboard(BaseModel):
    team_id: str
    period: Literal["weekly", "monthly", "total"]
    rows: List[MemberLeaderboardRow] = Field(default_factory=list)

# ---- NEW: Invite Links (referrals) ----
class InviteLinkCreate(BaseModel):
    max_uses: Optional[int] = Field(default=None, ge=1, le=9999)
    expires_days: Optional[int] = Field(default=None, ge=1, le=365)

class InviteLink(BaseModel):
    code: str
    team_id: str
    created_at: str
    created_by: str
    uses: int
    max_uses: Optional[int] = None
    expires_at: Optional[str] = None

# ---- NEW: Announcements ----
class AnnouncementCreate(BaseModel):
    title: str
    body_md: Optional[str] = None

class Announcement(BaseModel):
    id: str
    team_id: str
    at: str
    title: str
    body_md: Optional[str] = None
    by_user_id: str
