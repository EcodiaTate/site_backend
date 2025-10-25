# site_backend/api/missions/schema.py
from __future__ import annotations

from typing import Annotated, Dict, List, Literal, Optional
from datetime import datetime

from pydantic import BaseModel, Field, HttpUrl

MissionType = Literal["sidequest", "eco_action"]
MissionStatus = Literal["draft", "active", "archived"]
VerificationMethod = Literal["photo_upload", "instagram_link"]
SubmissionState = Literal["pending", "approved", "rejected"]

# ---- Reusable constrained aliases (Pylance-friendly) ----
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

class MissionBase(BaseModel):
    type: MissionType
    title: str
    subtitle: Optional[str] = None
    description_md: Optional[str] = None
    tags: Optional[List[str]] = None

    reward_eco: NonNegInt = 0
    max_completions_per_user: Optional[PosInt] = 1
    cooldown_days: Optional[NonNegInt] = 0

    pills: Optional[PillMeta] = None
    geo: Optional[GeoTarget] = None

    verification_methods: List[VerificationMethod] = Field(
        default_factory=lambda: ["photo_upload"]
    )
    start_at: Optional[datetime] = None
    end_at: Optional[datetime] = None
    status: MissionStatus = "draft"

    hero_image: Optional[str] = None   # CDN path for listing card
    card_accent: Optional[str] = None  # hex/color token for UI

class MissionCreate(MissionBase):
    pass

class MissionUpdate(BaseModel):
    title: Optional[str] = None
    subtitle: Optional[str] = None
    description_md: Optional[str] = None
    tags: Optional[List[str]] = None
    reward_eco: Optional[NonNegInt] = None
    max_completions_per_user: Optional[PosInt] = None
    cooldown_days: Optional[NonNegInt] = None
    pills: Optional[PillMeta] = None
    geo: Optional[GeoTarget] = None
    verification_methods: Optional[List[VerificationMethod]] = None
    start_at: Optional[datetime] = None
    end_at: Optional[datetime] = None
    status: Optional[MissionStatus] = None
    hero_image: Optional[str] = None
    card_accent: Optional[str] = None

class MissionOut(MissionBase):
    id: str
    created_at: datetime
    updated_at: datetime

class SubmissionCreate(BaseModel):
    mission_id: str
    method: VerificationMethod
    # When method == "photo_upload"
    image_upload_id: Optional[str] = None  # returned by /missions/media/upload
    # When method == "instagram_link"
    instagram_url: Optional[HttpUrl] = None
    caption: Optional[str] = None

    # Optional geo proof from client
    user_lat: Optional[Lat] = None
    user_lon: Optional[Lon] = None

class SubmissionOut(BaseModel):
    id: str
    mission_id: str
    user_id: str
    method: VerificationMethod
    state: SubmissionState
    created_at: datetime
    reviewed_at: Optional[datetime] = None
    auto_checks: Dict[str, bool] = {}
    notes: Optional[str] = None
    media_url: Optional[str] = None
    instagram_url: Optional[HttpUrl] = None

class ModerationDecision(BaseModel):
    state: Literal["approved", "rejected"]
    notes: Optional[str] = None

class BulkUpsertResult(BaseModel):
    created: int
    updated: int
    errors: List[str] = []
