from __future__ import annotations

from typing import Annotated, Dict, List, Literal, Optional
from datetime import date, datetime
from pydantic import BaseModel, Field, HttpUrl

SidequestKind = Literal["core", "eco_action", "daily", "weekly", "tournament", "team", "chain"]
SidequestStatus = Literal["draft", "active", "archived"]
VerificationMethod = Literal["photo_upload", "instagram_link"]
SubmissionState = Literal["pending", "approved", "rejected"]

# ---- Reusable constrained aliases ----
Lat = Annotated[float, Field(ge=-90, le=90)]
Lon = Annotated[float, Field(ge=-180, le=180)]
NonNegInt = Annotated[int, Field(ge=0)]
PosInt = Annotated[int, Field(gt=0)]

class GeoTarget(BaseModel):
    lat: Lat
    lon: Lon
    radius_m: NonNegInt = 0
    locality: Optional[str] = None  # e.g. "Brisbane", "QLD"

class PillMeta(BaseModel):
    difficulty: Optional[Literal["easy", "moderate", "hard"]] = None
    impact: Optional[str] = None                 # short line for pill
    facts: Optional[List[str]] = None            # bullet tidbits
    time_estimate_min: Optional[PosInt] = None
    materials: Optional[List[str]] = None        # optional checklist

class StreakPolicy(BaseModel):
    name: str = "Weekly Eco Streak"
    period: Literal["daily", "weekly"] = "weekly"
    bonus_eco_per_step: NonNegInt = 0          # extra ECO granted per consecutive period
    max_steps: Optional[PosInt] = None         # cap bonuses (None = no cap)

class RotationMeta(BaseModel):
    is_weekly_slot: bool = False               # true if this is managed by the weekly rotation
    iso_year: Optional[int] = None
    iso_week: Optional[int] = None
    slot_index: Optional[int] = None           # e.g., 0..N-1 for layout
    starts_on: Optional[date] = None
    ends_on: Optional[date] = None

class ChainMeta(BaseModel):
    chain_id: Optional[str] = None             # logical group id for chained quests
    chain_order: Optional[PosInt] = None       # 1..n
    requires_prev_approved: bool = True

class TeamMeta(BaseModel):
    allowed: bool = False                      # if True, team completions allowed
    min_size: Optional[PosInt] = None
    max_size: Optional[PosInt] = None
    team_bonus_eco: NonNegInt = 0              # bonus to split among members

class SidequestBase(BaseModel):
    kind: SidequestKind = "core"
    title: str
    subtitle: Optional[str] = None
    description_md: Optional[str] = None
    tags: Optional[List[str]] = None

    reward_eco: NonNegInt = 0
    xp_reward: NonNegInt = 0                    # future-proof gamification
    max_completions_per_user: Optional[PosInt] = 1
    cooldown_days: Optional[NonNegInt] = 0

    pills: Optional[PillMeta] = None
    geo: Optional[GeoTarget] = None
    streak: Optional[StreakPolicy] = None
    rotation: Optional[RotationMeta] = None
    chain: Optional[ChainMeta] = None
    team: Optional[TeamMeta] = None

    verification_methods: List[VerificationMethod] = Field(
        default_factory=lambda: ["photo_upload"]
    )
    start_at: Optional[datetime] = None
    end_at: Optional[datetime] = None
    status: SidequestStatus = "draft"

    hero_image: Optional[str] = None   # CDN path for listing card
    card_accent: Optional[str] = None  # hex/color token for UI

class SidequestCreate(SidequestBase):
    pass

class SidequestUpdate(BaseModel):
    # note: kind is fixed on create to keep analytics consistent
    title: Optional[str] = None
    subtitle: Optional[str] = None
    description_md: Optional[str] = None
    tags: Optional[List[str]] = None

    reward_eco: Optional[NonNegInt] = None
    xp_reward: Optional[NonNegInt] = None
    max_completions_per_user: Optional[PosInt] = None
    cooldown_days: Optional[NonNegInt] = None

    pills: Optional[PillMeta] = None
    geo: Optional[GeoTarget] = None
    streak: Optional[StreakPolicy] = None
    rotation: Optional[RotationMeta] = None
    chain: Optional[ChainMeta] = None
    team: Optional[TeamMeta] = None

    verification_methods: Optional[List[VerificationMethod]] = None
    start_at: Optional[datetime] = None
    end_at: Optional[datetime] = None
    status: Optional[SidequestStatus] = None

    hero_image: Optional[str] = None
    card_accent: Optional[str] = None

class SidequestOut(SidequestBase):
    id: str
    created_at: datetime
    updated_at: datetime
    # back-compat visibility fields (don’t break clients)
    # expose a view mapping from any legacy storage if present
    legacy_type: Optional[str] = None
    legacy_sub_type: Optional[str] = None

class SubmissionCreate(BaseModel):
    sidequest_id: str
    method: VerificationMethod
    # When method == "photo_upload"
    image_upload_id: Optional[str] = None  # from /sidequests/media/upload
    # When method == "instagram_link"
    instagram_url: Optional[HttpUrl] = None
    caption: Optional[str] = None
    # Optional geo proof from client
    user_lat: Optional[Lat] = None
    user_lon: Optional[Lon] = None
    # Optional team context (if team completions are allowed)
    team_id: Optional[str] = None

class SubmissionOut(BaseModel):
    id: str
    sidequest_id: str
    user_id: str
    method: VerificationMethod
    state: SubmissionState
    created_at: datetime
    reviewed_at: Optional[datetime] = None
    auto_checks: Dict[str, bool] = {}
    notes: Optional[str] = None
    media_url: Optional[str] = None
    instagram_url: Optional[HttpUrl] = None
    team_id: Optional[str] = None

class ModerationDecision(BaseModel):
    state: Literal["approved", "rejected"]
    notes: Optional[str] = None

class BulkUpsertResult(BaseModel):
    created: int
    updated: int
    errors: List[str] = []

# ---------- User progress / rotation ----------
class CooldownLock(BaseModel):
    sidequest_id: str
    until: Optional[datetime] = None

class UserProgressOut(BaseModel):
    recent_approved_ids: List[str] = []
    cooldowns: List[CooldownLock] = []
    streak_steps: NonNegInt = 0
    next_streak_reset_at: Optional[datetime] = None
    eligible_weekly_ids: List[str] = []

class RotationRequest(BaseModel):
    iso_year: int
    iso_week: int
    starts_on: date
    ends_on: date
    # optionally limit how many weekly slots we activate (default: all eligible)
    max_slots: Optional[PosInt] = None

class RotationResult(BaseModel):
    iso_year: int
    iso_week: int
    activated_ids: List[str]
    window: RotationRequest
