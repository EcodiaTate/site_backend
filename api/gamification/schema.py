from __future__ import annotations
from typing import Optional, List, Literal
from pydantic import BaseModel, Field

Period = Literal["weekly", "monthly", "total"]

# ---------- Types (catalog) ----------
class BadgeTypeUpsert(BaseModel):
    id: str
    name: str
    icon: Optional[str] = None
    color: Optional[str] = None
    kind: Literal["milestone", "streak", "seasonal"] = "milestone"
    # very simple rule format for now:
    # {"type":"threshold","field":"total_eco|actions_total|streak_days|season_actions","gte":100}
    rule: Optional[dict] = None
    tier: Optional[int] = None
    max_tier: Optional[int] = None

class BadgeTypeOut(BadgeTypeUpsert):
    pass

class AwardTypeUpsert(BaseModel):
    id: str
    name: str
    icon: Optional[str] = None
    color: Optional[str] = None
    scope: Literal["youth", "business"]
    rank_limit: int = 10  # e.g., 1, 3, 10

class AwardTypeOut(AwardTypeUpsert):
    pass

class SeasonUpsert(BaseModel):
    id: str               # e.g., "2025-10"
    label: str
    start: str            # ISO datetime
    end: str              # ISO datetime
    theme: Optional[str] = None

class SeasonOut(SeasonUpsert):
    pass

# ---------- Earned instances ----------
class BadgeAwardOut(BaseModel):
    id: str
    at: str
    tier: Optional[int] = None
    badge_id: str
    season: Optional[str] = None

class AwardOut(BaseModel):
    id: str
    at: str
    rank: int
    period: Literal["monthly", "weekly"]
    award_type_id: str
    season: Optional[str] = None

# ---------- Public responses ----------
class MeBadgesResponse(BaseModel):
    badges: List[BadgeAwardOut] = Field(default_factory=list)
    awards: List[AwardOut] = Field(default_factory=list)

class BusinessAwardsResponse(BaseModel):
    awards: List[AwardOut] = Field(default_factory=list)

# ---------- Admin ops ----------
class EvaluateUserReq(BaseModel):
    user_id: str
    season_id: Optional[str] = None

class MintMonthlyAwardsReq(BaseModel):
    start: str   # ISO
    end: str     # ISO
    season_id: str
    youth_award_type_id: str   # e.g., "youth_top10"
    business_award_type_id: str  # e.g., "biz_top10"
    youth_limit: int = 10
    business_limit: int = 10
